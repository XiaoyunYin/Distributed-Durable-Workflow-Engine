#!/usr/bin/env python3
"""Run DUR-046 mutations in disposable source exports.

The runner never edits the checkout. Each mutation starts from ``git archive
HEAD``, applies exactly one textual change, and runs the check declared by the
manifest. Behavioral mutations require the named semantic test to fail with
its expected message; configuration tripwires require their configuration
assertion to fail. Build failures, skips, and unrelated failures do not count
as detections.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
from datetime import UTC, datetime
from pathlib import Path


def run(command: list[str], cwd: Path, env: dict[str, str]) -> tuple[int, str]:
    process = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=180,
    )
    return process.returncode, process.stdout


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
        return True, ""
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


def export_head(repo: Path, destination: Path) -> None:
    archive = subprocess.check_output(["git", "-C", str(repo), "archive", "--format=tar", "HEAD"])
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as handle:
        # Python 3.12+ supports the safe extraction filter; retain a
        # compatibility fallback for older runners used by local developers.
        try:
            handle.extractall(destination, filter="data")
        except TypeError:
            handle.extractall(destination)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", default="mutations/manifest.json")
    parser.add_argument("--output", default="mutations/results.json")
    args = parser.parse_args()

    repo = Path(__file__).resolve().parents[1]
    manifest_path = repo / args.manifest
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    cases = manifest["cases"]
    database_url = os.environ.get("DURABLE_MUTATION_DATABASE_URL", "")
    if any(case["requires_database"] for case in cases) and not database_url:
        print(
            "DURABLE_MUTATION_DATABASE_URL is required for database-backed mutations",
            file=sys.stderr,
        )
        return 2

    base_env = os.environ.copy()
    base_env["GOCACHE"] = str(Path(tempfile.mkdtemp(prefix="dur046-go-cache-")))
    if database_url:
        base_env["DURABLE_RUN_INTEGRATION"] = "1"
        base_env["DURABLE_REQUIRE_DATABASE"] = "1"
        base_env["DURABLE_DATABASE_URL"] = database_url
    else:
        base_env.pop("DURABLE_RUN_INTEGRATION", None)
        base_env.pop("DURABLE_REQUIRE_DATABASE", None)

    results: list[dict[str, object]] = []
    scratch_root = Path(tempfile.mkdtemp(prefix="dur046-mutation-"))
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
        code, output = run(baseline, repo, base_env)
        if code != 0 or "SKIP" in output or "no tests to run" in output.lower():
            print(output, file=sys.stderr)
            print(
                "DUR-046 baseline failed, skipped, or selected no tests; "
                "no mutation result is valid",
                file=sys.stderr,
            )
            return 1

        for case in cases:
            kind = str(case.get("kind", "behavioral"))
            if kind not in {"behavioral", "configuration_tripwire"}:
                raise RuntimeError(f"{case['id']}: unsupported mutation kind {kind!r}")
            scratch = scratch_root / str(case["id"])
            scratch.mkdir()
            export_head(repo, scratch)
            path = scratch / str(case["file"])
            source = path.read_text(encoding="utf-8")
            old = str(case["old"])
            new = str(case["new"])
            count = source.count(old)
            if count != 1:
                raise RuntimeError(f"{case['id']}: expected one patch site, found {count}")
            command = [str(item) for item in case["command"]]
            env = dict(base_env)
            if command[0] == "go":
                env["GOCACHE"] = str(Path(tempfile.mkdtemp(prefix="dur046-case-cache-")))
            # Prove the exact selector passes on the unmodified source before
            # applying the mutation. This catches misspelled -run patterns and
            # skipped/no-test cases; running this after patching would confuse
            # an expected mutation failure with a broken baseline.
            baseline_ok, baseline_output = validate_case_baseline(
                str(case["id"]), command, scratch, env
            )
            if not baseline_ok:
                print(baseline_output, file=sys.stderr)
                print(
                    "DUR-046 mutation case has no passing, executed baseline; "
                    "refusing to apply mutation",
                    file=sys.stderr,
                )
                return 1
            path.write_text(source.replace(old, new, 1), encoding="utf-8", newline="\n")
            code, output = run(command, scratch, env)
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
                "expected_failure": expected,
                "output_tail": output[-4000:],
            }
            results.append(result)
            if not detected:
                print(json.dumps(result, indent=2), file=sys.stderr)
                return 1
            if kind == "configuration_tripwire":
                print(f"PASS {case['id']}: configuration tripwire detected the removed setting")
            else:
                print(f"PASS {case['id']}: mutation was rejected by its named semantic test")

        # The mutation exports are disposable, so prove the unmodified
        # checkout still passes after the complete campaign as well.
        code, output = run(baseline, repo, base_env)
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
                    "generated_at_utc": datetime.now(UTC).isoformat(),
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
        shutil.rmtree(Path(base_env["GOCACHE"]), ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
