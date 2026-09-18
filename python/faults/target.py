"""Seeded Python fake activity used by the reusable fault harness."""

from __future__ import annotations

import argparse
import os
import sys

from faults.fixtures import fake_activity, fixture_for


def run() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--boundary", required=True)
    parser.add_argument("--skip-boundary", action="store_true")
    parser.add_argument("--exit-before-boundary", action="store_true")
    parser.add_argument("--noise", action="store_true")
    parser.add_argument("--malformed-output", action="store_true")
    parser.add_argument("--stderr-lines", type=int, default=0)
    args = parser.parse_args()

    if args.noise:
        print("starting up: ordinary target output", flush=True)
    if args.malformed_output:
        print("{not-json output from target", flush=True)
    for index in range(args.stderr_lines):
        print(f"diagnostic line {index}", file=sys.stderr, flush=True)
    if args.exit_before_boundary:
        return 17
    if args.skip_boundary:
        sys.stdin.readline()
        return 0

    fixture = fixture_for(args.seed)
    if not os.getenv("DURABLE_FAULT_ENDPOINT"):
        print(f"fixture {fixture.workflow_id} result={fixture.expected_sum}", flush=True)
        return 0
    result = fake_activity(fixture, args.boundary)
    print(f"fixture {fixture.workflow_id} result={result}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
