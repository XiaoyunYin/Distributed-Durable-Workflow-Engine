#!/usr/bin/env python3
"""Run DUR-046 mutations in disposable source exports.

The runner never edits the checkout. Each mutation starts from a verified, restored ``git archive
HEAD`` export (optionally overlaid with the working tree), applies exactly one
textual change, and runs the check declared by the manifest. Behavioral mutations require the named semantic test to fail with
its expected message; configuration tripwires require their configuration
assertion to fail. Build failures, skips, and unrelated failures do not count
as detections.
"""

from __future__ import annotations

import argparse
import io
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path


def run(command: list[str], cwd: Path, env: dict[str, str]) -> tuple[int, str]:
    process = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=180,
    )
    return process.returncode, process.stdout


def case_command(command: list[str], python_exe: str, basetemp: Path) -> list[str]:
    if "pytest" in command:
        # pytest creates basetemp lazily, but requires its parent to exist.
        basetemp.parent.mkdir(parents=True, exist_ok=True)
        # Reuse the already-resolved project interpreter, including the legacy
        # uv-run canary case. No nested sync or source-export .venv is needed.
        return [python_exe, "-m", "pytest", *command[command.index("pytest") + 1:],
                "--basetemp", str(basetemp), "-p", "no:cacheprovider"]
    result = list(command)
    if result[0] == "python":
        result[0] = python_exe
    return result


def source_fingerprint(root: Path) -> dict[str, str]:
    return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in root.rglob("*") if path.is_file()}


def baseline_test_command(command: list[str]) -> list[str] | None:
    """Return a verbose Go test command for proving a manifest case selects tests."""
    if not command or command[0] != "go" or len(command) < 2 or command[1] != "test":
        return None
    result = list(command)
    if "-v" not in result:
        result.insert(2, "-v")
    return result


def validate_case_baseline(
    case_id: str, command: list[str], cwd: Path, env: dict[str, str]
) -> tuple[bool, str]:
    test_command = baseline_test_command(command)
    if test_command is None:
        if "pytest" not in command:
            return True, ""
        code, output = run(command, cwd, env)
        valid = code == 0 and " passed" in output and "skipped" not in output.lower()
        return valid, output if valid else f"{case_id}: Python baseline failed or skipped\n{output}"
    code, output = run(test_command, cwd, env)
    executed = any(
        line.startswith(("--- PASS:", "--- FAIL:", "--- SKIP:")) for line in output.splitlines()
    )
    invalid = (
        code != 0 or not executed or "--- SKIP:" in output or "no tests to run" in output.lower()
    )
    if not invalid:
        return True, output
    return False, (
        f"{case_id}: named-test baseline failed, skipped, or selected no tests; "
        f"command={test_command!r}\n{output}"
    )


def export_head(repo: Path, destination: Path, working_tree: bool = False) -> None:
    archive = subprocess.check_output(["git", "-C", str(repo), "archive", "--format=tar", "HEAD"])
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as handle:
        # Python 3.12+ supports the safe extraction filter; retain a
        # compatibility fallback for older runners used by local developers.
        try:
            handle.extractall(destination, filter="data")
        except TypeError:
            handle.extractall(destination)

    if working_tree:
        # Reviewable uncommitted source must be tested as well as committed CI.
        deleted = subprocess.check_output(
            ["git", "diff", "--name-only", "--diff-filter=D", "-z", "HEAD"], cwd=repo
        )
        for raw in deleted.split(b"\0"):
            if raw:
                (destination / Path(os.fsdecode(raw))).unlink(missing_ok=True)
        changed = subprocess.check_output(["git", "diff", "--name-only", "--diff-filter=ACMRT", "-z", "HEAD"], cwd=repo)
        added = subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "-z"], cwd=repo)
        for raw in changed.split(b"\0") + added.split(b"\0"):
            if not raw:
                continue
            relative = Path(os.fsdecode(raw))
            source = repo / relative
            if source.is_file():
                target = destination / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)


def main() -> int:
    started = time.perf_counter()
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", default="mutations/manifest.json")
    parser.add_argument("--output", default="mutations/results.json")
    parser.add_argument("--working-tree", action="store_true")
    args = parser.parse_args()

    repo = Path(__file__).resolve().parents[1]
    manifest_path = repo / args.manifest
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    cases = manifest["cases"]
    python_exe = subprocess.check_output(
        ["uv", "run", "--no-sync", "python", "-c", "import sys; print(sys.executable)"],
        cwd=repo, text=True,
    ).strip()
    database_url = os.environ.get("DURABLE_MUTATION_DATABASE_URL", "")
    if any(case["requires_database"] for case in cases) and not database_url:
        print(
            "DURABLE_MUTATION_DATABASE_URL is required for database-backed mutations",
            file=sys.stderr,
        )
        return 2

    base_env = os.environ.copy()
    owned_cache = not bool(base_env.get("GOCACHE"))
    if owned_cache:
        base_env["GOCACHE"] = str(Path(tempfile.mkdtemp(prefix="dur046-go-cache-")))
    base_env["PYTHONDONTWRITEBYTECODE"] = "1"
    base_env["PYTHONIOENCODING"] = "utf-8"
    if database_url:
        base_env["DURABLE_RUN_INTEGRATION"] = "1"
        base_env["DURABLE_REQUIRE_DATABASE"] = "1"
        base_env["DURABLE_DATABASE_URL"] = database_url
    else:
        base_env.pop("DURABLE_RUN_INTEGRATION", None)
        base_env.pop("DURABLE_REQUIRE_DATABASE", None)

    results: list[dict[str, object]] = []
    scratch_root = Path(tempfile.mkdtemp(prefix="dur046-mutation-"))
    setup_duration = time.perf_counter() - started
    try:
        baseline = [
            "go",
            "test",
            "-p",
            "1",
            "./internal/reference",
            "./internal/api",
            "./internal/engine",
            "./internal/invariants",
            "./internal/state",
            "-count=1",
        ]
        baseline_started = time.perf_counter()
        code, output = run(baseline, repo, base_env)
        baseline_duration = time.perf_counter() - baseline_started
        if code != 0 or "SKIP" in output or "no tests to run" in output.lower():
            print(output, file=sys.stderr)
            print(
                "DUR-046 baseline failed, skipped, or selected no tests; "
                "no mutation result is valid",
                file=sys.stderr,
            )
            return 1

        setup_started = time.perf_counter()
        scratch = scratch_root / "source"
        scratch.mkdir()
        export_head(repo, scratch, args.working_tree)
        original_fingerprint = source_fingerprint(scratch)
        setup_duration += time.perf_counter() - setup_started
        for case in cases:
            case_started = time.perf_counter()
            if source_fingerprint(scratch) != original_fingerprint:
                raise RuntimeError("mutation source drift: restored export differs from its baseline")
            kind = str(case.get("kind", "behavioral"))
            if kind not in {"behavioral", "configuration_tripwire"}:
                raise RuntimeError(f"{case['id']}: unsupported mutation kind {kind!r}")
            path = scratch / str(case["file"])
            original_bytes = path.read_bytes()
            source = path.read_text(encoding="utf-8")
            old = str(case["old"])
            new = str(case["new"])
            count = source.count(old)
            if count != 1:
                raise RuntimeError(f"{case['id']}: expected one patch site, found {count}")
            command = case_command([str(item) for item in case["command"]], python_exe,
                                   scratch_root / "pytest" / str(case["id"]))
            env = dict(base_env)
            # Prove the exact selector passes on the unmodified source before
            # applying the mutation. This catches misspelled -run patterns and
            # skipped/no-test cases; running this after patching would confuse
            # an expected mutation failure with a broken baseline.
            case_setup_duration = time.perf_counter() - case_started
            baseline_started = time.perf_counter()
            baseline_ok, baseline_output = validate_case_baseline(
                str(case["id"]), command, scratch, env
            )
            case_baseline_duration = time.perf_counter() - baseline_started
            if not baseline_ok:
                print(baseline_output, file=sys.stderr)
                print(
                    "DUR-046 mutation case has no passing, executed baseline; "
                    "refusing to apply mutation",
                    file=sys.stderr,
                )
                return 1
            path.write_text(source.replace(old, new, 1), encoding="utf-8", newline="\n")
            mutation_started = time.perf_counter()
            code, output = run(command, scratch, env)
            mutation_duration = time.perf_counter() - mutation_started
            expected = str(case["expected"])
            detected = (
                code != 0
                and expected in output
                and "SKIP" not in output
                and "no tests to run" not in output.lower()
            )
            result = {
                "id": case["id"],
                "kind": kind,
                "status": "PASS" if detected else "FAIL",
                "exit_code": code,
                "baseline_status": "PASS",
                "baseline_output_tail": baseline_output[-2000:],
                "source_sha256": hashlib.sha256(source.encode()).hexdigest(),
                "expected_failure": expected,
                "output_tail": output[-4000:],
                "duration_seconds": time.perf_counter() - case_started,
                "setup_duration_seconds": case_setup_duration,
                "baseline_duration_seconds": case_baseline_duration,
                "mutation_duration_seconds": mutation_duration,
            }
            results.append(result)
            if not detected:
                print(json.dumps(result, indent=2), file=sys.stderr)
                return 1
            path.write_bytes(original_bytes)
            result["duration_seconds"] = time.perf_counter() - case_started
            if kind == "configuration_tripwire":
                print(f"PASS {case['id']}: configuration tripwire detected the removed setting")
            else:
                print(f"PASS {case['id']}: mutation was rejected by its named semantic test")

        # The mutation exports are disposable, so prove the unmodified
        # checkout still passes after the complete campaign as well.
        baseline_started = time.perf_counter()
        code, output = run(baseline, repo, base_env)
        restored_baseline_duration = time.perf_counter() - baseline_started
        if code != 0 or "SKIP" in output or "no tests to run" in output.lower():
            print(output, file=sys.stderr)
            print(
                "DUR-046 restored baseline failed, skipped, or selected no tests",
                file=sys.stderr,
            )
            return 1

        commit = subprocess.check_output(
            ["git", "-C", str(repo), "rev-parse", "HEAD"], text=True
        ).strip()
        output_path = repo / args.output
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(
                {
                    "schema": "dur-046-mutation-results.v1",
                    "status": "PASS",
                    "git_commit": commit,
                    "source_basis": "working-tree overlay" if args.working_tree else "HEAD",
                    "generated_at_utc": datetime.now(UTC).isoformat(),
                    "duration_seconds": time.perf_counter() - started,
                    "setup_duration_seconds": setup_duration,
                    "baseline_duration_seconds": baseline_duration,
                    "restored_baseline_duration_seconds": restored_baseline_duration,
                    "cases": results,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        return 0
    finally:
        shutil.rmtree(scratch_root, ignore_errors=True)
        if owned_cache:
            shutil.rmtree(Path(base_env["GOCACHE"]), ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
