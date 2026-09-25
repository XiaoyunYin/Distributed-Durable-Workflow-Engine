from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "dur050-reconcile-window.py"


def write_csv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def make_window_evidence(
    root: Path, *, terminal_for_accepted: bool
) -> tuple[Path, Path, Path, Path, Path]:
    root.mkdir()
    loadgen = root / "submissions.csv"
    accepted_ids = root / "accepted-workflow-ids.txt"
    observer = root / "observer.csv"
    direct = root / "submission-reconciliation.csv"
    output = root / "window-reconciliation.json"
    write_csv(
        loadgen,
        ["record_type", "workflow_id", "outcome", "http_status"],
        [
            {
                "record_type": "submission",
                "workflow_id": "accepted-1",
                "outcome": "accepted",
                "http_status": "201",
            },
            {
                "record_type": "submission",
                "workflow_id": "rejected-1",
                "outcome": "rejected",
                "http_status": "503",
            },
            {
                "record_type": "submission",
                "workflow_id": "uncertain-1",
                "outcome": "ambiguous",
                "http_status": "",
            },
        ],
    )
    accepted_ids.write_text("accepted-1\n", encoding="utf-8")
    observer_rows = [
        {
            "record_type": "snapshot",
            "workflow_id": "accepted-1",
            "state": "SUCCEEDED" if terminal_for_accepted else "RUNNABLE",
            "completion_marker_seen": "true",
        },
        {
            "record_type": "snapshot",
            "workflow_id": "rejected-1",
            "state": "NOT_FOUND",
            "completion_marker_seen": "true",
        },
    ]
    if terminal_for_accepted:
        observer_rows.append(
            {
                "record_type": "first_terminal_observation",
                "workflow_id": "accepted-1",
                "state": "SUCCEEDED",
            }
        )
    write_csv(
        observer, ["record_type", "workflow_id", "state", "completion_marker_seen"], observer_rows
    )
    write_csv(
        direct,
        ["record_type", "workflow_id", "submission_outcome", "http_status", "exists", "state"],
        [
            {
                "record_type": "submission_reconciliation",
                "workflow_id": "accepted-1",
                "submission_outcome": "accepted",
                "http_status": "201",
                "exists": "true",
                "state": "SUCCEEDED" if terminal_for_accepted else "RUNNABLE",
            },
            {
                "record_type": "submission_reconciliation",
                "workflow_id": "rejected-1",
                "submission_outcome": "rejected",
                "http_status": "503",
                "exists": "false",
                "state": "",
            },
            {
                "record_type": "submission_reconciliation",
                "workflow_id": "uncertain-1",
                "submission_outcome": "ambiguous",
                "http_status": "",
                "exists": "true",
                "state": "RUNNABLE",
            },
        ],
    )
    return loadgen, accepted_ids, observer, direct, output


def invoke(evidence: tuple[Path, Path, Path, Path, Path]) -> subprocess.CompletedProcess[str]:
    loadgen, accepted_ids, observer, direct, output = evidence
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            str(loadgen),
            str(accepted_ids),
            str(observer),
            str(direct),
            str(output),
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def test_accepted_id_without_terminal_observation_makes_window_reconciliation_fail(
    tmp_path: Path,
) -> None:
    evidence = make_window_evidence(tmp_path / "missing-terminal", terminal_for_accepted=False)

    result = invoke(evidence)

    assert result.returncode != 0
    report = json.loads(evidence[-1].read_text(encoding="utf-8"))
    assert report["status"] == "FAIL"
    assert "accepted workflow accepted-1 has 0 first_terminal_observation rows" in report["error"]


def test_window_reconciliation_accepts_complete_rows_and_records_nonaccepted_lookup(
    tmp_path: Path,
) -> None:
    evidence = make_window_evidence(tmp_path / "complete", terminal_for_accepted=True)

    result = invoke(evidence)

    assert result.returncode == 0, result.stderr + result.stdout
    report = json.loads(evidence[-1].read_text(encoding="utf-8"))
    assert report["status"] == "PASS"
    assert report["accepted_workflows"] == report["accepted_terminal_observations"] == 1
    assert report["nonaccepted_direct_lookups"] == 2
    assert report["found_nonaccepted_workflows"] == 1


def test_accepted_id_with_not_found_snapshot_fails_even_if_terminal_row_exists(
    tmp_path: Path,
) -> None:
    evidence = make_window_evidence(tmp_path / "accepted-not-found", terminal_for_accepted=True)
    observer = evidence[2]
    text = observer.read_text(encoding="utf-8").replace(
        "accepted-1,SUCCEEDED,true", "accepted-1,NOT_FOUND,true"
    )
    observer.write_text(text, encoding="utf-8")

    result = invoke(evidence)

    assert result.returncode != 0
    report = json.loads(evidence[-1].read_text(encoding="utf-8"))
    assert report["status"] == "FAIL"
    assert "accepted workflow accepted-1 has a NOT_FOUND observation" in report["error"]


def test_not_found_requires_marker_seen_in_the_same_or_earlier_poll(tmp_path: Path) -> None:
    evidence = make_window_evidence(tmp_path / "early-not-found", terminal_for_accepted=True)
    observer = evidence[2]
    text = observer.read_text(encoding="utf-8").replace(
        "rejected-1,NOT_FOUND,true", "rejected-1,NOT_FOUND,false"
    )
    observer.write_text(text, encoding="utf-8")

    result = invoke(evidence)

    assert result.returncode != 0
    report = json.loads(evidence[-1].read_text(encoding="utf-8"))
    assert "emitted before the completion marker" in report["error"]
