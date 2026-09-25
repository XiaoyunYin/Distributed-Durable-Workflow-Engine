"""Filter runtime logs into immutable DUR-050 transaction-timing JSONL."""

from __future__ import annotations

import json
import re
import socket
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) < 4:
        print(
            "usage: dur050-filter-txn-logs.py OUTPUT_FILE NAMESPACE RUN_ID [RUN_ID ...]",
            file=sys.stderr,
        )
        return 2
    output_path = Path(sys.argv[1])
    namespace = sys.argv[2]
    run_ids = sys.argv[3:]
    if re.fullmatch(r"dur050-[A-Za-z0-9-]+", namespace) is None:
        print("namespace must be a valid dur050-* identifier", file=sys.stderr)
        return 2
    if any(re.fullmatch(r"[A-Za-z0-9-]{1,48}", run_id) is None for run_id in run_ids):
        print("run IDs must be 1-48 ASCII letters, digits, or hyphens", file=sys.stderr)
        return 2
    run_patterns = [
        (re.compile(rf"^{re.escape(namespace)}-{re.escape(run_id)}-\d{{6}}$"), run_id)
        for run_id in run_ids
    ]
    count = 0
    try:
        with output_path.open("x", encoding="utf-8") as output:
            for line in sys.stdin:
                marker = "DUR050_TXN "
                position = line.find(marker)
                if position < 0:
                    continue
                try:
                    row = json.loads(line[position + len(marker) :].strip())
                except json.JSONDecodeError:
                    print(
                        f"malformed DUR050_TXN row in runtime logs: {line.rstrip()}",
                        file=sys.stderr,
                    )
                    return 1
                if not isinstance(row, dict):
                    print("DUR050_TXN row must be a JSON object", file=sys.stderr)
                    return 1
                workflow_id = row.get("workflow_id", "")
                if not isinstance(workflow_id, str):
                    print("DUR050_TXN workflow_id must be a string", file=sys.stderr)
                    return 1
                matching_run = next(
                    (run_id for pattern, run_id in run_patterns if pattern.fullmatch(workflow_id)),
                    None,
                )
                if matching_run is None:
                    continue
                if row.get("schema") != "dur050-transaction-timing.v1":
                    print(
                        f"unexpected transaction timing schema for {workflow_id}", file=sys.stderr
                    )
                    return 1
                if row.get("namespace") != namespace:
                    print(f"transaction namespace mismatch for {workflow_id}", file=sys.stderr)
                    return 1
                if row.get("operation") not in {"submission", "terminal_transition"}:
                    print(f"unexpected transaction operation for {workflow_id}", file=sys.stderr)
                    return 1
                if (
                    row.get("measurement_clock") != "runtime_process_monotonic"
                    or row.get("measurement_scope")
                    != "pool_begin_to_commit_return_including_pool_wait_network"
                ):
                    print(
                        f"unexpected transaction measurement contract for {workflow_id}",
                        file=sys.stderr,
                    )
                    return 1
                if (
                    not isinstance(row.get("transaction_duration_us"), int)
                    or row["transaction_duration_us"] < 0
                ):
                    print(f"invalid transaction duration for {workflow_id}", file=sys.stderr)
                    return 1
                row["run_id"] = matching_run
                row["log_source_host"] = socket.gethostname()
                output.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
                count += 1
    except OSError as error:
        print(f"could not write timing rows: {error}", file=sys.stderr)
        return 1
    if count == 0:
        print("no DUR050_TXN rows matched the requested measurement run IDs", file=sys.stderr)
        return 1
    print(f"DUR050_TRANSACTION_ROWS={count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
