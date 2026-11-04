#!/usr/bin/env python3
"""Compare effective candidate image registries; mismatch exits nonzero.

CI checks the default and DUR048 deployment profiles. Image verification can
pass --runtime/--worker JSON files exported from the exact candidate images.
The database pre-rollout check is /runtime compatibility-check, not this check.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


def registry_set(entries: object) -> set[tuple[str, str]]:
    if not isinstance(entries, list) or not entries:
        raise ValueError("empty or invalid registry")
    result: set[tuple[str, str]] = set()
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {"name", "version"}:
            raise ValueError("invalid registry entry")
        name, version = entry["name"], entry["version"]
        if not isinstance(name, str) or not name or not isinstance(version, str) or not version:
            raise ValueError("invalid activity name/version")
        if (name, version) in result:
            raise ValueError("duplicate registry entry")
        result.add((name, version))
    return result


def check(runtime: object, worker: object) -> None:
    left, right = registry_set(runtime), registry_set(worker)
    if left != right:
        raise ValueError(f"registry mismatch: Go-only={sorted(left-right)}, Python-only={sorted(right-left)}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime", type=Path)
    parser.add_argument("--worker", type=Path)
    args = parser.parse_args()
    if args.runtime or args.worker:
        if not args.runtime or not args.worker:
            parser.error("both candidate manifests required")
        check(json.loads(args.runtime.read_text()), json.loads(args.worker.read_text()))
    else:
        for fixture in (False, True):
            env = dict(os.environ)
            env["DUR048_ALLOW_FIXTURE_ACTIVITY"] = "1" if fixture else "0"
            env["DUR048_ACTIVITY_DELAY_MS"] = "1" if fixture else "0"
            env["DUR050_ENABLE_SHA256_ACTIVITY"] = "0"
            env["PYTHONPATH"] = "python"
            runtime = subprocess.check_output(["go", "run", "./cmd/runtime", "activity-registry"], env=env)
            worker = subprocess.check_output([sys.executable, "-m", "workers", "activity-registry"], env=env)
            check(json.loads(runtime), json.loads(worker))
            print(f"activity registry parity PASS: fixture={fixture}")


if __name__ == "__main__":
    main()
