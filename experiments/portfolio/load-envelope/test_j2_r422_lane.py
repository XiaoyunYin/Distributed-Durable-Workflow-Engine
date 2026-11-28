import json
import sys
import zipfile
from contextlib import nullcontext
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import j2_control_evidence as control  # noqa: E402
import j2_integrated_evidence as integrated  # noqa: E402
import j2_measure as measure  # noqa: E402
import j2_r422_lane as lane  # noqa: E402
import j2_replay_storage as storage  # noqa: E402
import j2_schedule_rehearsal as driver  # noqa: E402
from j2_drain import STATE_READ_SCHEMA, cohort_counts, expected_arrivals  # noqa: E402
from j2_measure_replay import claim_candidates  # noqa: E402
from j2_qualification_evidence import file_record, read, write  # noqa: E402
from j2_schedule import (  # noqa: E402
    DISPATCH_JOURNAL,
    MEASURED_CAMPAIGN,
    SECOND,
    continuous_schedule,
    sender_report,
    timestamp,
)
from test_j2_dispatch import dispatch_fixture  # noqa: E402
from test_j2_schedule import ORIGIN  # noqa: E402


@pytest.mark.parametrize(
    ("identifier", "hold", "supplied", "ref"),
    [
        (MEASURED_CAMPAIGN, "A", "", "refs/heads/codex/j2-slice3"),
        ("eng-r422-ab-probe", "A", "", "refs/heads/codex/j2-slice3"),
        ("eng-r422-a-probe", "C", "", "refs/heads/codex/j2-slice3"),
        ("eng-r422-a-probe", "A", "j2-authorization/j2-load-envelope-v1", "branch"),
        ("eng-r422-a-probe", "A", "", "refs/heads/j2-authorization/j2-load-envelope-v1"),
    ],
)
def test_lane_rejects_registered_or_mismatched_input(identifier, hold, supplied, ref):
    with pytest.raises(ValueError, match="R422"):
        lane.gate(identifier, hold, supplied, ref)


@pytest.mark.parametrize(("hold", "count", "bound"), [("A", 21002, 21002), ("AB", 26677, 26678)])
def test_lane_schedule_is_full_clock_and_uses_the_two_segment_sender(hold, count, bound):
    identifier = f"eng-r422-{hold.lower()}-probe"
    lane.gate(identifier, hold, "", "refs/heads/codex/j2-slice3")
    row = lane.schedule_for(identifier, hold, 3, ORIGIN)
    assert row["warmup_end_ns"] - ORIGIN == 120 * SECOND
    assert row["hold_end_ns"] - row["warmup_end_ns"] == 1800 * SECOND
    assert row["stop_ns"] - row["hold_end_ns"] == 300 * SECOND
    assert row["cap"] == 16 and row["count"] == count and row["population_bound"] == bound
    assert row["dispatch_journal"] == DISPATCH_JOURNAL and row["scope"] == lane.SCOPE


@pytest.mark.parametrize("scope", [measure.ENGINEERING_GRID_SCOPE, measure.REGISTERED_GRID_SCOPE])
@pytest.mark.parametrize("cell", ["A", "soak"])
def test_measure_selects_precommit_for_both_shapes(scope, cell):
    context = SimpleNamespace(campaign="eng-r422-probe", scope=scope)
    schedule = measure.schedule_for(context, 1, cell, "A", ORIGIN, precommit=True)
    assert schedule["dispatch_journal"] == DISPATCH_JOURNAL
    assert ("periods" in schedule) == (cell == "soak")
    assert measure.PRECOMMIT_LEAD_SECONDS == 30


def test_install_and_run_bind_raw_schedule_bytes_and_opt_in(tmp_path, monkeypatch):
    commands = []
    executed = {}

    def kubectl(*args, **kwargs):
        commands.append((args, kwargs))
        return SimpleNamespace(stdout=file_record(tmp_path / "schedule.json")["sha256"] + "\n")

    def execute(*args, **kwargs):
        executed.update(args=args, kwargs=kwargs)

    monkeypatch.setattr(driver, "execute_sender", execute)
    observer = SimpleNamespace(output=tmp_path, observer="one", k=kubectl)
    schedule = lane.schedule_for("eng-r422-a-probe", "A", 1, ORIGIN)
    driver.run_precommitted(observer, schedule, partial=False)
    path = tmp_path / "schedule.json"
    assert read(path) == schedule and commands[0][1]["data"].encode() == path.read_bytes()
    assert executed["args"][3][:4] == [
        driver.CONTINUOUS,
        "-dispatch-journal=precommit-v1",
        "-schedule",
        driver.POD_SCHEDULE,
    ]
    assert executed["kwargs"]["lead"] == driver.PRECOMMIT_LEAD_SECONDS
    assert read(tmp_path / "sender-schedule-install.json")["sha256"] == file_record(path)["sha256"]
    observer.k = lambda *_args, **_kw: SimpleNamespace(stdout="tampered")
    with pytest.raises(ValueError, match="schedule digest"):
        driver.run_precommitted(observer, schedule, partial=False)
    assert len(commands) == 1


def test_archived_install_identifies_exact_source_build_binary_and_schedule(tmp_path):
    head = "a" * 40
    write(tmp_path / "schedule.json", {"test": "schedule"})
    build = tmp_path / "dispatch-build.json"
    binary, source = {"sha256": "b" * 64}, {"sha256": "c" * 64}
    common = {
        "source": head,
        "package": "./cmd/j2-sender",
        "dispatch_journal": "precommit-v1",
        "source_archive": source,
    }
    write(build, {**common, "binary": binary, "source_files": {"cmd/j2-sender/main.go": source}})
    write(
        tmp_path / "sender-install.json",
        {**common, "build": file_record(build), "sha256": binary["sha256"]},
    )
    write(
        tmp_path / "sender-schedule-install.json",
        {"path": driver.POD_SCHEDULE, "sha256": file_record(tmp_path / "schedule.json")["sha256"]},
    )
    driver.verify_precommit_install(tmp_path, build, head)
    write(tmp_path / "schedule.json", {"test": "tampered"})
    with pytest.raises(ValueError, match="does not bind"):
        driver.verify_precommit_install(tmp_path, build, head)


@pytest.fixture
def acceptance_input():
    schedule = continuous_schedule(
        "eng-r422-a-fixture",
        "job-1",
        "soak",
        "run",
        "j2-harness",
        ORIGIN,
        ("1", 2),
        ("40", 1),
        warmup=1,
    )
    schedule["dispatch_journal"] = DISPATCH_JOURNAL
    prefix = f"dur053-run-{schedule['seed']}-"
    samples = [
        {
            "key": prefix + f"{n:06d}",
            "dispatch_intent_ns": at - ORIGIN,
            "transport_entry_ns": at - ORIGIN + 3 * lane.MS,
            "request_written_ns": at - ORIGIN + lane.P999_NS,
            "outcome": "accepted",
            "error": "",
        }
        for n, (at, _) in enumerate(lane.schedule_slots(schedule))
    ]
    sender = {
        "schema": DISPATCH_JOURNAL,
        "sender_samples": samples,
        "sent_keys": [row["key"] for row in samples],
        "durable_admissions": [row["key"] for row in samples],
        "explicitly_unsent": [],
        "closed": True,
        "torn_suffix": False,
        "remaining_intents": [],
        "outside_slot": [],
        "timing_exceptions": [],
        "flushes": [
            {
                "batch": 0,
                "precommit_start_ns": -4 * SECOND,
                "directory_sync_return_ns": -2 * SECOND,
                "precommit_bytes": 12345,
                "peak_rss_kib": 19,
            }
        ],
    }
    summary = {
        "sender_valid_by_window": {"hold": True, "step": True},
        "all_accepted_verdict": "PASS",
        "drain": {"status": "drain_failure"},
    }
    return schedule, sender, summary


def test_margins_include_warmup_transport_and_checked_expiry(acceptance_input):
    report = lane.acceptance(*acceptance_input)
    assert report["status"] == "PASS" and report["drain"]["status"] == "drain_failure"
    assert report["intent_to_write"]["p99_9_ns"] == 12 * lane.MS
    assert report["intent_to_transport_entry"]["max_ns"] == 3 * lane.MS
    assert report["transport_entry_to_write"]["max_ns"] == 9 * lane.MS
    assert report["precommit"]["time_ns"] == 2 * SECOND
    assert all(w["intent_to_write"]["p99_9_ns"] == 12 * lane.MS for w in report["windows"].values())
    assert lane.latency([0] * 999 + [20 * lane.MS])["p99_9_ns"] == 0


@pytest.mark.parametrize(
    ("row", "field", "value", "reason"),
    [
        (0, "request_written_ns", None, "missing_write"),
        (0, "outcome", "unsent", "refused"),
        (-1, "request_written_ns", 3 * SECOND, "outside_slot"),
        (-1, "request_written_ns", 3 * SECOND - 1, "slot_margin"),
        (0, "error", "cutoff-after-intent", "cutoff_after_intent"),
        (-1, "error", "missed-send-deadline", "cutoff_after_intent"),
    ],
)
def test_engineering_rejects_bad_key_even_when_summary_is_otherwise_valid(
    acceptance_input,
    row,
    field,
    value,
    reason,
):
    _, sender, _ = acceptance_input
    sample = sender["sender_samples"][row]
    sample[field] = value
    report = lane.acceptance(*acceptance_input)
    assert report["status"] == "FAIL" and report["timing_failures"][reason] == [sample["key"]]


def test_engineering_missing_precommit_rss_fails(acceptance_input):
    acceptance_input[1]["flushes"][0].pop("peak_rss_kib")
    assert lane.acceptance(*acceptance_input)["status"] == "FAIL"


@pytest.mark.parametrize(
    ("sequence", "reason", "in_flight", "hold_valid", "lane_pass"),
    [
        (7, "capacity-missed", 16, True, True),
        (1, "capacity-missed", 16, False, False),
        (0, "capacity-missed", 16, False, False),
        (7, "capacity-missed", 15, True, False),
        (7, "cutoff-after-intent", 16, True, False),
    ],
)
def test_window_delivery_and_lane_replay_agree(
    tmp_path, monkeypatch, sequence, reason, in_flight, hold_valid, lane_pass
):
    root = tmp_path / "soak"
    root.mkdir()
    plan = continuous_schedule(
        "eng-r422-a-fixture",
        "job-1",
        "soak",
        "run",
        "j2-harness",
        ORIGIN,
        ("1", 7),
        ("40", 1),
        warmup=1,
    )
    plan.update(dispatch_journal=DISPATCH_JOURNAL, campaign_clock=True)
    schedule = {**plan, "state_read_evidence": STATE_READ_SCHEMA}
    schedule_path = root / "schedule.json"
    write(schedule_path, schedule)
    schedule_hash = file_record(schedule_path)["sha256"]

    def refuse(index, at, cutoff, row):
        if index == sequence:
            row.update(
                outcome="unsent",
                error=reason,
                in_flight_at_release=in_flight,
                refusal_ns=at,
                transport_entry_ns=None,
                request_written_ns=None,
                status=0,
                write_callbacks=0,
                acknowledged_workflow_id=None,
            )

    head, raw = dispatch_fixture(schedule, schedule_hash, changed=refuse, resources=True)
    (root / "sender-journal.jsonl").write_bytes(raw)
    missed = head["plan"]["rows"][sequence]["key"]
    checked = [
        {
            "workflow_id": row["key"],
            "scheduled_ns": ORIGIN + row["scheduled_ns"],
            "terminal_ns": ORIGIN + row["scheduled_ns"] + lane.MS,
            "phase_ns": [lane.MS // 4] * 4,
            "polling_minus_durable_ns": lane.MS,
        }
        for row in head["plan"]["rows"]
        if row["key"] != missed
    ]
    ids = [row["workflow_id"] for row in checked]
    sender = sender_report(raw, schedule, ids, schedule_sha256=schedule_hash)
    telemetry = {"coverage": {}}
    write(root / "prometheus-samples.json", {})
    write(root / "cell-scrape-evidence.json", telemetry)
    cut = {
        "db": {"submissions": [{"id": key} for key in ids]},
        "witness": [],
        "upper_unix_ns": schedule["stop_ns"],
    }
    write(root / "final.json", cut)
    write(root / "drain-result.json", {"pending": []})
    write(root / "phase-rows.json", checked)
    write(root / "cohorts.json", cohort_counts(schedule, checked))
    write(root / "sender-report.json", sender)
    write(root / "journal-read.json", {"exit": 0})
    write(root / "sender-exit.json", {"exec_exit": 1})
    write(root / "live-checker-read.json", {"exit": 0})
    write(root / "checker-resource-summary.json", {"exit": 0})
    write(root / "witness.json", {"ledgers": []})
    write(
        root / "step-transition.json",
        {"worker_uids_before": ["worker"], "worker_uids_after": ["worker"]},
    )
    write(root / "clock.json", [])
    monkeypatch.setattr(integrated, "replay_episode", lambda _root: (checked, 0))
    monkeypatch.setattr(
        integrated,
        "indexed_report",
        lambda *_args, **_kwargs: (
            {"trace": {"Snapshot": {"engine.transition_history": {"rows": []}}}},
            {},
        ),
    )
    monkeypatch.setattr(
        integrated, "checker_resource_summary", lambda *_: {"checker_peak_rss_kib": 1}
    )
    monkeypatch.setattr(integrated, "require_checker_resources", lambda _: None)
    monkeypatch.setattr(integrated, "checked_cut", lambda *_: cut)
    monkeypatch.setattr(integrated, "quiet_evidence", lambda *_: {"status": "PASS"})
    monkeypatch.setattr(integrated, "verified_crossings", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(integrated, "clock_evidence", lambda *_: {"uncertainty_ns": 0})
    monkeypatch.setattr(
        integrated,
        "gates_samples",
        lambda _: [
            {"observed_at": timestamp(at), "outstanding": 0, "unpublished": 0}
            for at in range(ORIGIN, schedule["stop_ns"], SECOND // 10)
        ],
    )
    monkeypatch.setattr(lane, "cell_samples", lambda *_: telemetry)

    summary = lane.summarize_episode(root)
    assert not summary["sender_valid"] and not summary["soak_evidence_eligible"]
    assert summary["sender_valid_by_window"] == {"hold": hold_valid, "step": sequence < 7}
    assert summary["cohorts"]["hold"]["gate"]["status"] == (
        "pass" if hold_valid else "sender_invalid"
    )
    if hold_valid:
        assert summary["cohorts"]["step"]["gate"]["status"] == "sender_invalid"
        assert summary["hold_diagnostic"]["in_window_completed"] == 6
        assert measure.cell_record(summary, telemetry)[0]["reason"] == "step_sender_invalid"
    else:
        assert "hold_diagnostic" not in summary
        assert measure.cell_record(summary, telemetry)[0]["reason"] == "sender_invalid"
    measured = SimpleNamespace(campaign=MEASURED_CAMPAIGN)
    jobs = {
        job: {"cells": {"soak": {"episode": summary, "row": {"rate": "A"}}}, "errors": {}}
        for job in (1, 2, 3)
    }
    assert (
        claim_candidates(
            measured, jobs, {"status": "verified", "decision": {"selected_rate": "A"}}, True
        )
        == []
    )

    live = lane.episode_report(root, schedule, summary)
    assert live["status"] == ("PASS" if lane_pass else "FAIL")
    if lane_pass:
        (capacity,) = live["step_capacity_misses"]
        assert capacity["key"] == missed and capacity["in_flight_at_release"] == 16
        assert live["intent_to_write"]["samples"] == schedule["count"] - 1
        assert live["sender_reconciled"] and live["timing_exception_count"] == 0
        assert not any(live["timing_failures"].values())
    else:
        assert live["timing_failures"]["refused"] == [missed]
        assert live["timing_failures"]["missing_write"] == [missed]
        assert live["step_capacity_misses"] == []

    write(root / "episode-replay.json", summary)
    write(root / "engineering-acceptance.json", live)
    campaign = SimpleNamespace(campaign=plan["campaign"], head="head", run=123)
    write(tmp_path / "runner-inventory.json", {})
    write(root / "pre-traffic-telemetry.json", {"freshness": {"distinct_samples": 5}})
    write(
        root / "sender-release.json",
        {
            "run_id": plan["run_id"],
            "unix_ns": ORIGIN - 3 * SECOND,
            "telemetry": file_record(root / "pre-traffic-telemetry.json"),
            "inventory": file_record(tmp_path / "runner-inventory.json"),
        },
    )
    write(
        root / "fresh-db.json",
        {
            "counts": {},
            "settings": ["on", "on", "on", "100"],
            "engine_partitions": list(range(16)),
        },
    )
    write(root / "before-workloads-transport.json", {"latest": {}})
    write(tmp_path / "cleanup.json", {"status": "PASS"})
    write(
        tmp_path / "status.json",
        {
            "scope": lane.SCOPE,
            "head": "head",
            "run_id": "123",
            "run_attempt": "1",
            "cleanup_verified": True,
        },
    )
    monkeypatch.setattr(lane, "schedule_for", lambda *_: plan)
    monkeypatch.setattr(lane, "verify_precommit_install", lambda *_: None)
    monkeypatch.setattr(lane, "checker_replay", lambda *_: {"identities": len(ids)})
    budget = SimpleNamespace(stage=lambda *_: nullcontext(lambda: 240))
    replayed = lane.verify_lane_episode(tmp_path, campaign, 1, "A", None, budget, None)
    assert {key: replayed[key] for key in live} == live
    write(root / "engineering-acceptance.json", {**live, "step_capacity_misses": ["tampered"]})
    with pytest.raises(ValueError, match="independent engineering evidence differs"):
        lane.verify_lane_episode(tmp_path, campaign, 1, "A", None, budget, None)


@pytest.fixture
def supplemental():
    run = 123
    return storage.validate_supplemental(
        {
            "schema": "j2.r422-controls.v1",
            "run": run,
            "attempt": 1,
            "head": "a" * 40,
            "artifacts": {
                "bundle": {
                    "id": 1001,
                    "name": f"j2-controls-r422-{run}-1",
                    "bytes": 1234,
                    "sha256": "b" * 64,
                },
                "receipt": {
                    "id": 1002,
                    "name": f"j2-controls-r422-receipt-{run}-1",
                    "bytes": 4321,
                    "sha256": "c" * 64,
                },
            },
        }
    )


def artifact(supplemental, role):
    row = supplemental["artifacts"][role]
    return {
        "name": row["name"],
        "id": row["id"],
        "size_in_bytes": row["bytes"],
        "digest": "sha256:" + row["sha256"],
        "source_attempt": 1,
        "workflow_run": {"id": supplemental["run"], "head_sha": supplemental["head"]},
    }


@pytest.mark.parametrize(
    "field", ["id", "name", "size_in_bytes", "digest", "workflow_run", "source_attempt"]
)
def test_supplemental_archive_binding_detects_tamper(supplemental, field):
    meta = artifact(supplemental, "bundle")
    assert storage.verify_prerequisite(meta["name"], meta, supplemental) == (123, "controls-r422")
    assert len(storage.prerequisite_archives(supplemental)) == 5
    altered = {**meta, field: "tampered"}
    with pytest.raises(ValueError, match="source differs|bytes or attempt differ"):
        storage.verify_prerequisite(meta["name"], altered, supplemental)


def test_supplemental_receipt_binds_component_coverage_and_phase_parity(supplemental, tmp_path):
    original = artifact(supplemental, "bundle")
    digest = {"sender": "d" * 64}
    report = {
        "schema": supplemental["schema"],
        "campaign": "eng-controls-123-1",
        "covered_components": digest,
        "withheld_terminal": {"status": "PASS"},
        "healthy_low_load": {"status": "PASS", "phase_join_parity": "PASS"},
        "capped_sender": {"status": "sender_invalid"},
    }
    path = tmp_path / "receipt.zip"

    def write_receipt(replay):
        with zipfile.ZipFile(path, "w") as bundle:
            bundle.writestr("artifact.json", json.dumps(original))
            bundle.writestr("offline-replay.json", json.dumps(replay))

    write_receipt(report)
    metadata = {original["name"]: original}
    assert storage.supplemental_verdict(path, original["name"], metadata, digest) == report
    with pytest.raises(ValueError, match="exact components"):
        storage.supplemental_verdict(path, original["name"], metadata, {"sender": "e" * 64})
    drift = deepcopy(report)
    drift["healthy_low_load"]["phase_join_parity"] = "FAIL"
    write_receipt(drift)
    with pytest.raises(ValueError, match="verdicts"):
        storage.supplemental_verdict(path, original["name"], metadata, digest)


def test_fresh_healthy_phase_projection_preserves_all_join_inputs():
    schedule = control.control_schedule(control.HEALTHY, "eng-controls-123-1", "j2-harness", ORIGIN)
    schedule.update(count=12, warmup_end_ns=ORIGIN + 2 * SECOND, stop_ns=ORIGIN + 12 * SECOND)
    schedule["dispatch_journal"] = DISPATCH_JOURNAL
    digest = "a" * 64
    _, raw = dispatch_fixture(schedule, digest)
    expected, sender = expected_arrivals(raw, schedule, schedule_sha256=digest)
    old_expected, old_sender = control.legacy_phase_projection(raw, schedule, expected, sender)
    assert expected == old_expected
    for field in ("acknowledged", "explicitly_unsent", "unresolved"):
        assert sender[field] == old_sender[field]
    assert old_sender["stock_verdict"] == "PASS"
