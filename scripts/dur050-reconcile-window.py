"""Fail closed unless every accepted DUR-050 submission has terminal evidence."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header: {path.name}")
        return list(reader)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_id_file(path: Path) -> list[str]:
    ids = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if len(ids) != len(set(ids)):
        raise ValueError("accepted workflow ID file contains duplicates")
    return ids


def reconcile(
    loadgen_path: Path,
    accepted_ids_path: Path,
    observer_path: Path,
    database_reconciliation_path: Path,
) -> dict[str, Any]:
    submissions = read_csv(loadgen_path)
    if not submissions:
        raise ValueError("load-generator CSV has no scheduled submission rows")
    submission_by_id: dict[str, dict[str, str]] = {}
    for row in submissions:
        workflow_id = row.get("workflow_id", "")
        if row.get("record_type") != "submission" or not workflow_id:
            raise ValueError("load-generator CSV contains a malformed submission row")
        if workflow_id in submission_by_id:
            raise ValueError(f"load-generator CSV repeats workflow ID {workflow_id}")
        submission_by_id[workflow_id] = row

    accepted_ids = read_id_file(accepted_ids_path)
    accepted_from_loadgen = {
        workflow_id
        for workflow_id, row in submission_by_id.items()
        if row.get("outcome") == "accepted"
    }
    if set(accepted_ids) != accepted_from_loadgen:
        raise ValueError("accepted-workflow-ids.txt disagrees with load-generator outcomes")

    observer_rows = read_csv(observer_path)
    terminal_by_id: dict[str, list[dict[str, str]]] = {}
    not_found_by_id: set[str] = set()
    for row in observer_rows:
        workflow_id = row.get("workflow_id", "")
        if row.get("record_type") == "first_terminal_observation":
            terminal_by_id.setdefault(workflow_id, []).append(row)
        if row.get("record_type") == "snapshot" and row.get("state") == "NOT_FOUND":
            if row.get("completion_marker_seen", "").lower() != "true":
                raise ValueError(
                    f"NOT_FOUND for {workflow_id} was emitted before the completion "
                    "marker was observed"
                )
            not_found_by_id.add(workflow_id)

    for workflow_id in accepted_ids:
        terminal_count = len(terminal_by_id.get(workflow_id, []))
        if terminal_count != 1:
            raise ValueError(
                f"accepted workflow {workflow_id} has {terminal_count} "
                "first_terminal_observation rows; want exactly one"
            )
        if workflow_id in not_found_by_id:
            raise ValueError(f"accepted workflow {workflow_id} has a NOT_FOUND observation")

    database_rows = read_csv(database_reconciliation_path)
    database_by_id: dict[str, dict[str, str]] = {}
    for row in database_rows:
        workflow_id = row.get("workflow_id", "")
        if row.get("record_type") != "submission_reconciliation" or not workflow_id:
            raise ValueError("database reconciliation contains a malformed row")
        if workflow_id in database_by_id:
            raise ValueError(f"database reconciliation repeats workflow ID {workflow_id}")
        database_by_id[workflow_id] = row
    if set(database_by_id) != set(submission_by_id):
        raise ValueError("direct database reconciliation IDs differ from scheduled submissions")
    for workflow_id, submission in submission_by_id.items():
        database_row = database_by_id[workflow_id]
        if database_row.get("submission_outcome") != submission.get("outcome"):
            raise ValueError(f"direct database reconciliation outcome differs for {workflow_id}")
        exists = database_row.get("exists", "").lower()
        if exists not in {"true", "false"}:
            raise ValueError(
                f"direct database reconciliation lacks exists status for {workflow_id}"
            )
        if workflow_id in accepted_from_loadgen and exists != "true":
            raise ValueError(
                f"accepted workflow {workflow_id} is absent from direct database lookup"
            )
        if exists == "true" and not database_row.get("state"):
            raise ValueError(f"existing workflow {workflow_id} has no directly observed state")
        if workflow_id in accepted_from_loadgen:
            observed_terminal_state = terminal_by_id[workflow_id][0].get("state", "")
            if database_row.get("state") != observed_terminal_state:
                raise ValueError(
                    f"direct database state differs from terminal observation for {workflow_id}"
                )

    return {
        "schema": "dur050-window-reconciliation.v1",
        "status": "PASS",
        "scheduled_workflows": len(submission_by_id),
        "accepted_workflows": len(accepted_ids),
        "accepted_terminal_observations": sum(len(terminal_by_id.get(i, [])) for i in accepted_ids),
        "not_found_ids": len(not_found_by_id),
        "direct_database_lookups": len(database_by_id),
        "nonaccepted_direct_lookups": sum(
            row.get("outcome") != "accepted" for row in submission_by_id.values()
        ),
        "found_nonaccepted_workflows": sum(
            database_by_id[workflow_id].get("exists", "").lower() == "true"
            for workflow_id, row in submission_by_id.items()
            if row.get("outcome") != "accepted"
        ),
        "input_sha256": {
            path.name: sha256_file(path)
            for path in (
                loadgen_path,
                accepted_ids_path,
                observer_path,
                database_reconciliation_path,
            )
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("loadgen_csv", type=Path)
    parser.add_argument("accepted_ids", type=Path)
    parser.add_argument("observer_csv", type=Path)
    parser.add_argument("database_reconciliation_csv", type=Path)
    parser.add_argument("output_json", type=Path)
    args = parser.parse_args()

    try:
        report = reconcile(
            args.loadgen_csv,
            args.accepted_ids,
            args.observer_csv,
            args.database_reconciliation_csv,
        )
    except (OSError, ValueError) as error:
        report = {
            "schema": "dur050-window-reconciliation.v1",
            "status": "FAIL",
            "error": str(error),
        }
        status = 1
    else:
        status = 0
    with args.output_json.open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(report, stream, indent=2, sort_keys=True)
        stream.write("\n")
    print(json.dumps({"status": report["status"], "error": report.get("error")}, sort_keys=True))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
