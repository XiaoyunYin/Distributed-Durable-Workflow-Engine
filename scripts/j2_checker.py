"""Build and verify the locked J2 checker independently of the worker image."""

import argparse
import hashlib
import json
import os
import re
import subprocess
import tarfile
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOCK_PATH = ROOT / "experiments/portfolio/load-envelope/checker.lock.json"
POD_CHECKER = "/tmp/checker"
MOUNTED_CHECKER = "/j2-tooling/checker"

INSTALL = """
import hashlib, os, shutil, sys
path, digest = sys.argv[1:]
with open(path + '.incoming', 'wb') as output:
    shutil.copyfileobj(sys.stdin.buffer, output)
with open(path + '.incoming', 'rb') as source:
    if hashlib.file_digest(source, 'sha256').hexdigest() != digest:
        sys.exit('observer checker digest mismatch')
os.chmod(path + '.incoming', 0o555)
os.replace(path + '.incoming', path)
print('observer checker SHA-256 ' + digest)
"""

# The same wrapper runs in the observer and fresh Docker cgroups. Only the
# full-scale pilot needs it to sample the child. Qualification already has a
# node-side sampler that survives a lost observer.
CHECKER = """
import hashlib, json, os, resource, subprocess, sys, time
from pathlib import Path
path, digest, memory_limit, *args = sys.argv[1:]
with open(path, 'rb') as checker:
    if hashlib.file_digest(checker, 'sha256').hexdigest() != digest:
        sys.exit('checker digest mismatch')
env = os.environ.copy()
if memory_limit:
    env['GOMEMLIMIT'] = memory_limit
else:
    env.pop('GOMEMLIMIT', None)
env['J2_CHECKER_PHASES'] = '1'
started_at_ns = time.time_ns()
start = time.monotonic()
with subprocess.Popen([path, *args], env=env) as process:
    if env.get('J2_CHECKER_SAMPLE') == '1':
        while process.poll() is None:
            try:
                status = Path('/proc', str(process.pid), 'status').read_text()
                peak = next(line.split()[1] for line in status.splitlines()
                            if line.startswith('VmHWM:'))
                current = Path('/sys/fs/cgroup/memory.current').read_text()
                print('J2_SAMPLE ' + json.dumps({'at_ns': time.time_ns(),
                      'vmhwm_kib': int(peak), 'memory_current_bytes': int(current)}),
                      file=sys.stderr, flush=True)
            except (ProcessLookupError, FileNotFoundError, StopIteration):
                pass
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                pass
    status = process.wait()
measurement = {'exit_status': status, 'started_at_ns': started_at_ns,
               'wall_seconds': time.monotonic() - start,
               'peak_rss_kib': resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
               'checker_sha256': digest, 'gomemlimit': env.get('GOMEMLIMIT')}
print('J2_CHECKER ' + json.dumps(measurement), file=sys.stderr, flush=True)
sys.exit(status if status >= 0 else 128 - status)
"""

CONTAINER_CHECKER = (
    """
import json, sys
from pathlib import Path
try:
"""
    + "\n".join("    " + line for line in CHECKER.strip().splitlines())
    + """
finally:
    root = Path('/sys/fs/cgroup')
    files = {'max': 'memory.max', 'peak': 'memory.peak', 'events': 'memory.events',
             'swap_max': 'memory.swap.max', 'cpu_max': 'cpu.max'}
    cgroup = {name: (root / file).read_text().strip() for name, file in files.items()}
    print('J2_CGROUP ' + json.dumps(cgroup), file=sys.stderr, flush=True)
"""
)


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def verify_binary(path, manifest=LOCK_PATH):
    lock = json.loads(manifest.read_bytes())
    if sha256(path) != lock["sha256"]:
        raise ValueError(f"J2 checker SHA-256 mismatch: {path}")
    return lock


def comparison_manifest(checker):
    return checker.with_suffix(".comparison.json")


def build(target, comparison_source=None):
    lock = json.loads(LOCK_PATH.read_bytes())
    if comparison_source is not None:
        if not re.fullmatch(r"[0-9a-f]{40}", comparison_source):
            raise ValueError("comparison requires the full dispatched source commit")
        lock["source"] = comparison_source
        lock.pop("gomemlimit", None)
    tree = subprocess.check_output(
        ["git", "rev-parse", lock["source"] + "^{tree}"], cwd=ROOT, text=True
    ).strip()
    if comparison_source is not None:
        lock["tree"] = tree
        for name, key in (("go.mod", "go_mod_sha256"), ("go.sum", "go_sum_sha256")):
            blob = subprocess.check_output(
                ["git", "cat-file", "blob", f"{comparison_source}:{name}"], cwd=ROOT
            )
            lock[key] = hashlib.sha256(blob).hexdigest()
    elif tree != lock["tree"]:
        raise ValueError("J2 checker source tree mismatch")
    env = {**os.environ, **lock["env"]}
    target = target.resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=target.parent) as temp:
        source = Path(temp)
        archive = source / "source.tar"
        subprocess.run(
            [
                "git",
                "-c",
                "core.autocrlf=false",
                "-c",
                "core.eol=lf",
                "archive",
                f"--output={archive}",
                lock["source"],
                "go.mod",
                "go.sum",
                "cmd/dur049-checker",
                "internal",
                "migrations",
            ],
            cwd=ROOT,
            check=True,
        )
        with tarfile.open(archive) as data:
            data.extractall(source, filter="data")
        for name, key in (("go.mod", "go_mod_sha256"), ("go.sum", "go_sum_sha256")):
            if sha256(source / name) != lock[key]:
                raise ValueError(f"J2 checker {name} mismatch")
        version = subprocess.check_output(
            ["go", "env", "GOVERSION"], cwd=source, env=env, text=True
        )
        if version.strip() != lock["go_version"]:
            raise ValueError(f"unexpected checker toolchain: {version.strip()}")
        binary = source / "checker"
        subprocess.run(
            ["go", "build", *lock["flags"], "-o", binary, lock["package"]],
            cwd=source,
            env=env,
            check=True,
        )
        if comparison_source is not None:
            lock["sha256"] = sha256(binary)
        else:
            verify_binary(binary)
        binary.replace(target)
    if comparison_source is not None:
        comparison_manifest(target).write_text(json.dumps(lock, indent=2) + "\n", encoding="utf-8")
    print(f"{target} SHA-256 {lock['sha256']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--comparison-source")
    args = parser.parse_args()
    build(args.output, args.comparison_source)
