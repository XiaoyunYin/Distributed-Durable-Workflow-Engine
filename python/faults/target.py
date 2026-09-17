"""Small target process used by the named-boundary controller tests."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys


def emit(event: dict[str, object]) -> None:
    print(json.dumps(event, sort_keys=True), flush=True)


def run() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--boundary", required=True)
    parser.add_argument("--skip-boundary", action="store_true")
    args = parser.parse_args()

    if args.skip_boundary:
        # A controller timeout is the only exit path for this fixture. It is
        # intentionally event-free so the test cannot pass by sleeping.
        sys.stdin.readline()
        return 0

    fingerprint = hashlib.sha256(f"{args.seed}:{args.boundary}".encode()).hexdigest()[:16]
    emit(
        {
            "event": "boundary_reached",
            "boundary": args.boundary,
            "seed": args.seed,
            "work_fingerprint": fingerprint,
        }
    )

    command_line = sys.stdin.readline()
    if not command_line:
        return 2
    command = json.loads(command_line)
    if not isinstance(command, dict):
        return 2
    if command.get("command") != "release" or command.get("boundary") != args.boundary:
        return 2
    emit(
        {
            "event": "released",
            "boundary": args.boundary,
            "seed": args.seed,
            "work_fingerprint": fingerprint,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
