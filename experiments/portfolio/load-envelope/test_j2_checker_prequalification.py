import csv
import json
import subprocess
import sys
from collections import Counter
from hashlib import sha256
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

from j2_checker_report import indexed_report  # noqa: E402
from j2_prequal_fixture import LINKED, build_fixture, compact  # noqa: E402
from j2_prequalify_checker import (  # noqa: E402
    MEMORY_MAX,
    PEAK_LIMIT_BYTES,
    TRIAL_LIMITS,
    VMHWM_LIMIT_KIB,
    checker_gates,
    measure_trials,
)
from j2_qualification_evidence import read, verify, write  # noqa: E402


def test_comparison_reuses_snapshot_even_when_control_live_fails(tmp_path, monkeypatch):
    evidence, scratch = tmp_path / "evidence", tmp_path / "scratch"
    evidence.mkdir()
    scratch.mkdir()
    for name in ("arrivals.json", "witness.json", "clock.json", "plan.json", "terminals.json"):
        (evidence / name).write_text(name, encoding="utf-8")
    count = 21002
    tables = {f"engine.{name}": {"count": count * n} for name, (_, n) in LINKED.items()}
    tables["engine.lease_acquisitions"] = {"count": 26 * count}
    calls = []
    snapshots = []

    def checker(directory, binary, digest, population, path, limit):
        calls.append((path, limit))
        assert population == count and digest == "digest" and binary == Path("checker")
        assert (directory / "arrivals.json").samefile(evidence / "arrivals.json")
        if path == "offline":
            assert (directory / "snapshot.json").samefile(evidence / "snapshot.json")
        (directory / f"{path}-checker.stderr").write_text("gc telemetry", encoding="utf-8")
        passed = path == "offline" or limit is not None
        return {
            "gates": {"pass": passed},
            "tables": tables if passed else {},
            "process": {"gomemlimit": limit},
            "gc_count": 7,
        }

    def python_stage(directory, operation):
        if operation == "extract":
            snapshots.append(directory.name)
            (directory / "snapshot.json").write_text("snapshot", encoding="utf-8")
        return {"process_exit": 0}

    monkeypatch.setattr("j2_prequalify_checker.checker_run", checker)
    monkeypatch.setattr("j2_prequalify_checker.profile", python_stage)
    trials = measure_trials(count, evidence, scratch, Path("checker"), "digest", TRIAL_LIMITS)
    assert calls == [(path, limit) for path in ("live", "offline") for limit in TRIAL_LIMITS]
    assert snapshots == ["1100MiB"]
    assert {trial["offline_snapshot_from"] for trial in trials.values()} == {"1100MiB"}
    assert [label for label, trial in trials.items() if trial["pass"]] == ["1100MiB", "1200MiB"]
    assert trials["unset"]["paths"]["offline"]["gc_count"] == 7
    assert (evidence / "gomemlimit" / "1200MiB" / "live-checker.stderr").is_file()
    assert not list(scratch.iterdir())


def test_checker_lock_hashes_match_committed_go_module_blobs():
    root = Path(__file__).resolve().parents[3]
    lock_path = root / "experiments/portfolio/load-envelope/checker.lock.json"
    lock = json.loads(lock_path.read_bytes())
    for filename, key in (("go.mod", "go_mod_sha256"), ("go.sum", "go_sum_sha256")):
        blob = subprocess.check_output(
            ["git", "cat-file", "blob", f"{lock['source']}:{filename}"], cwd=root
        )
        assert sha256(blob).hexdigest() == lock[key]


def test_indexed_report_handles_raw_payloads_without_reencoding(tmp_path):
    rows = b'[{"a":{"trace":{"rows":["[}]\\"",1e+2]},"valid":false}},null]'
    digest = sha256(rows).hexdigest().encode()
    trace = (
        b'{"Snapshot":{"engine.leases":{"count":2,"sha256":"'
        + digest
        + b'","rows":'
        + rows
        + b"}}}"
    )
    live = tmp_path / "live.json"
    live.write_bytes(b'{"status":"PASS","trace":' + trace + b',"valid":true}\n')
    output = tmp_path / "snapshot.json"
    report, tables = indexed_report(live, snapshot=output)
    assert report["valid"] is True
    assert tables == {"engine.leases": {"bytes": len(rows), "count": 2, "sha256": digest.decode()}}
    assert output.read_bytes() == trace + b"\n"
    live.write_bytes(live.read_bytes()[:-3])
    with pytest.raises(ValueError):
        indexed_report(live)


def test_prequalification_gates_apply_all_four_thresholds():
    count, digest = 21002, "a" * 64
    report = {
        "status": "PASS",
        "valid": True,
        "campaign": {"valid": True, "accepted": count, "accepted_results": count},
    }
    process = {
        "exit_status": 0,
        "peak_rss_kib": VMHWM_LIMIT_KIB,
        "wall_seconds": 240,
        "checker_sha256": digest,
    }
    cgroup = {
        "max": str(MEMORY_MAX),
        "peak": str(PEAK_LIMIT_BYTES),
        "cpu_max": "100000 100000",
        "swap_max": "0",
    }
    for mode, first in (("live", "load"), ("offline", "offline_decode")):
        phases = [{"phase": name} for name in (first, "hash", "check", "campaign_check", "write")]
        assert all(checker_gates(0, report, process, cgroup, phases, digest, count, mode).values())
        gates = checker_gates(
            1,
            report,
            {**process, "peak_rss_kib": VMHWM_LIMIT_KIB + 1, "wall_seconds": 240.001},
            {**cgroup, "peak": str(PEAK_LIMIT_BYTES + 1)},
            phases,
            digest,
            count,
            mode,
        )
        assert {key for key, good in gates.items() if not good} == {
            "pass",
            "vmhwm",
            "cgroup_peak",
            "wall",
        }


def test_retained_fixture_rewrites_linked_identities_for_reconciliation(tmp_path, monkeypatch):
    count = 10
    build_fixture(tmp_path, tmp_path, count)
    sql = (tmp_path / "fixture.sql").read_text(encoding="utf-8")
    assert "generate_series(1, 260)" in sql
    tables = {}
    for block in sql.split("COPY j2_input FROM STDIN WITH (FORMAT csv);\n")[1:]:
        contents, insert = block.split("\\.\n", 1)
        table = insert.split()[2]
        rows = [json.loads(row[0]) for row in csv.reader(contents.splitlines())]
        tables[table] = {
            "rows": rows,
            "count": len(rows),
            "sha256": sha256(compact(rows).encode()).hexdigest(),
        }
    for table, (_, multiplicity) in LINKED.items():
        assert tables[f"engine.{table}"]["count"] == count * multiplicity
    events = {row["event_id"] for row in tables["engine.outbox"]["rows"]}
    for name in ("event_inbox", "outbox_publications", "scheduler_wakeups"):
        assert {row["event_id"] for row in tables[f"engine.{name}"]["rows"]} <= events
    inbox = tables["engine.event_inbox"]["rows"]
    assert len({(row["kafka_partition"], row["kafka_offset"]) for row in inbox}) == 6 * count
    arrivals = read(tmp_path / "arrivals.json")["rows"]
    assert Counter(row["activity"] for row in arrivals) == {
        "dur048.sleep": 8,
        "pure.echo": 1,
        "pure.add": 1,
    }
    accepted = {
        (row["workflow_id"], row["node_id"]): row["accepted_result"]
        for row in tables["engine.node_instances"]["rows"]
        if row["accepted_result"] is not None
    }
    assert all(accepted[row["workflow_id"], row["activity"]] == row["expected"] for row in arrivals)
    workflows = tables["engine.workflow_executions"]["rows"]
    ids = [row["workflow_id"] for row in workflows]
    submissions = [
        {
            "Namespace": row["namespace"],
            "Key": row["submission_key"],
            "Hash": row["submission_payload_hash"],
            "WorkflowID": row["workflow_id"],
        }
        for row in workflows
    ]
    report = {
        "status": "PASS",
        "valid": True,
        "workflow_ids": ids,
        "trace": {"Snapshot": tables, "Submissions": submissions},
        "campaign": {
            "valid": True,
            "accepted_results": count,
            "busy_seconds": 0,
            "reexecuted_seconds": 0,
            "executions": [
                {"workflow_id": identity, "winners": 1, "invocations": 1} for identity in ids
            ],
        },
    }
    for mode in ("live", "offline"):
        write(tmp_path / f"{mode}-checker.json", report)
    # Use a small local fixture while the production verifier retains the two registered volumes.
    monkeypatch.setattr("j2_qualification_evidence.VOLUMES", (count,))
    result = verify(tmp_path)
    assert result["identities"] == count
    assert result["phase_join_counts"] == {"observed": count}
