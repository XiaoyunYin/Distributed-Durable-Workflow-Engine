import json
import sys
import zipfile
from contextlib import contextmanager, nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import j2_integrated_rehearsal as rehearsal  # noqa: E402
import j2_integrated_report as report  # noqa: E402
from j2_barrier import (  # noqa: E402
    INTEGRATED_SCOPE,
    Barrier,
    BarrierStop,
    Campaign,
    RetryLater,
    checkpoint_name,
)
from j2_checker_report import IntegrityError  # noqa: E402
from j2_drain import DrainExpired  # noqa: E402
from j2_harness_evidence import seal  # noqa: E402
from j2_qualification_evidence import read, write  # noqa: E402
from j2_schedule import ORDERS, RATES  # noqa: E402


@pytest.fixture
def attempt(monkeypatch):
    for key, value in {
        "GITHUB_SHA": "a" * 40,
        "GITHUB_RUN_ID": "54",
        "GITHUB_RUN_ATTEMPT": "2",
    }.items():
        monkeypatch.setenv(key, value)


def test_forecast_reserves_every_registered_drain_and_one_full_barrier():
    for job, order in enumerate(ORDERS, 1):
        budget = rehearsal.forecast(job)
        assert budget["order"] == order
        assert budget["matrix_seconds"] == 10573
        assert budget["replay_seconds"] == 5370
        assert budget["matrix"]["grid_drain"] == 5 * 600
        assert budget["matrix"]["continuous_drain"] == 600
        assert budget["matrix"]["barrier"] == 45 * 60
    with pytest.raises(ValueError, match="matrix forecast"):
        rehearsal.forecast(1, matrix_limit=10572)
    with pytest.raises(ValueError, match="replay forecast"):
        rehearsal.forecast(1, replay_limit=5369)


def test_barrier_refuses_before_publication_if_the_full_successor_will_not_fit(
    tmp_path, monkeypatch
):
    receipts = tmp_path / "receipts"
    receipts.mkdir()
    for rate in RATES:
        write(receipts / f"{rate}.json", {})
    now = 1000
    reserve = rehearsal.EPISODE_SECONDS + rehearsal.SOAK_SETUP
    barrier_end = now + rehearsal.BARRIER_SECONDS
    monkeypatch.setattr(rehearsal.time, "monotonic", lambda: now)
    write(tmp_path / "job.json", {"work_end": barrier_end + reserve - 1})
    with pytest.raises(BarrierStop, match="less than the registered barrier allowance"):
        rehearsal.prepare_barrier(tmp_path, 1)
    assert not (tmp_path / "barrier.json").exists()
    write(tmp_path / "job.json", {"work_end": barrier_end + reserve})
    rehearsal.prepare_barrier(tmp_path, 1)
    assert read(tmp_path / "barrier.json") == {"ready": now, "deadline": barrier_end}


@pytest.mark.usefixtures("attempt")
def test_engineering_clock_keeps_registered_rates_and_separate_ids():
    for job, order in enumerate(ORDERS, 1):
        for cell in order:
            plan = rehearsal.episode_schedule(job, cell, cell, 0)
            assert plan["rate"] == RATES[cell] and plan["stop_ns"] == 75 * 10**9
            assert plan["warmup_end_ns"] == 15 * 10**9
            assert not plan["measured_eligible"] and "eng-integrated" in plan["run_id"]
    soak = rehearsal.episode_schedule(1, "soak", "AB", 0)
    assert [period["rate"] for period in soak["periods"]] == [RATES["AB"], RATES["C"]]
    assert [period["seconds"] for period in soak["periods"]] == [130, 60]
    assert soak["hold_end_ns"] == 130 * 10**9 and soak["stop_ns"] == 190 * 10**9


@pytest.mark.parametrize("classification", ["drain_failure", "integrity_stop", "unchecked"])
def test_checked_expiry_checkpoints_and_continues_but_integrity_stops(classification, monkeypatch):
    calls = []
    driver = SimpleNamespace(
        stage=lambda name: nullcontext(),
        fresh_episode=lambda *args: calls.append(("reset", args[-1])) or {"new": "identity"},
        pod=lambda *args: {"metadata": {"name": "observer"}},
        kube=lambda *args: {"items": []},
        install_checker=lambda: None,
    )
    monkeypatch.setattr(rehearsal, "install_sender", lambda *args, **kwargs: None)
    monkeypatch.setattr(rehearsal, "attribution_samples", lambda *_: nullcontext())
    monkeypatch.setattr(
        rehearsal, "checkpoint", lambda *args: calls.append(("checkpoint", args[2])) or "index"
    )

    def exercise(*args, **kwargs):
        assert kwargs == {"stepped": False, "visibility_delay": 585}
        if classification == "integrity_stop":
            raise IntegrityError("checker violation")
        raise DrainExpired(
            "checked fixture",
            {"check_status": "PASS" if classification == "drain_failure" else "FAIL"},
        )

    monkeypatch.setattr(rehearsal, "exercise_drain", exercise)

    def cell():
        return rehearsal.exercise(driver, 1, "B", "B", {}, "", "old", {False: (None, None)})

    if classification == "drain_failure":
        assert cell() == ({"new": "identity"}, "index")
        assert calls == [("reset", "old"), ("checkpoint", "B")]
    else:
        expected = IntegrityError if classification == "integrity_stop" else ValueError
        with pytest.raises(expected):
            cell()
        assert calls == [("reset", "old")]


def test_publish_bound_grid_checkpoints_before_allowing_a_start():
    campaign = Campaign("owner/repo", 54, 2, "a" * 40, "fixture", {}, 26678, INTEGRATED_SCOPE)
    barrier = Barrier(campaign, None)
    references = {
        cell: {
            "name": checkpoint_name(54, 2, 1, cell),
            "id": i,
            "sha256": "b" * 64,
            "index_sha256": "c" * 64,
        }
        for i, cell in enumerate(RATES, 1)
    }
    entries = {
        ref["name"]: {"id": ref["id"], "digest": f"sha256:{ref['sha256']}"}
        for ref in references.values()
    }
    barrier.checkpoints(1, references, entries)
    expected = entries.pop(references["B"]["name"])
    with pytest.raises(RetryLater, match="waiting for grid checkpoint"):
        barrier.checkpoints(1, references, entries)
    entries[references["B"]["name"]] = {**expected, "id": 77}
    with pytest.raises(BarrierStop, match="checkpoint artifact drift"):
        barrier.checkpoints(1, references, entries)


@pytest.mark.usefixtures("attempt")
@pytest.mark.parametrize("outcome", ["would_start", "integrity_stop", "no_eligible_rate"])
def test_only_the_published_eligible_decision_requests_the_continuous_episode(
    tmp_path, monkeypatch, outcome
):
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path))
    evidence = tmp_path / "final"
    evidence.mkdir()
    rows = {
        "integrity": {"outcome": "integrity_stop"},
        "no-eligible": {"outcome": "no_eligible_rate"},
        "eligible": {"outcome": outcome, "decision": {"selected_rate": "AB"}},
    }
    for name, row in rows.items():
        path = tmp_path / "j2-barrier" / name / "outcome" / "outcome.json"
        path.parent.mkdir(parents=True)
        write(path, row)
    calls = []
    monkeypatch.setattr(rehearsal, "request_episode", lambda *args: calls.append(args))
    if outcome == "would_start":
        rehearsal.start_soak(tmp_path, 1)
        assert calls[0][1:4] == (1, "soak", "AB")
        assert read(evidence / "barrier-outcomes.json") == rows
    else:
        with pytest.raises(ValueError, match="unexpected barrier outcome"):
            rehearsal.start_soak(tmp_path, 1)
        assert calls == []


@pytest.mark.usefixtures("attempt")
@pytest.mark.parametrize("mutation", [None, "digest", "membership", "attempt"])
def test_archive_verification_detects_tampering(tmp_path, mutation):
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    write(evidence / "identity.json", {"id": 1})
    seal(evidence)
    index = read(evidence / "freeze.json")
    archive = tmp_path / "checkpoint.zip"
    if mutation == "attempt":
        index["run_attempt"] = "1"
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr("freeze.json", json.dumps(index))
        original = (evidence / "identity.json").read_bytes()
        bundle.writestr(
            "identity.json", original.replace(b"1", b"2") if mutation == "digest" else original
        )
        if mutation == "membership":
            bundle.writestr("unexpected.json", "{}")
    if mutation:
        with pytest.raises(ValueError, match="drift|membership"):
            report.extract(archive, tmp_path / "extracted")
    else:
        result = report.extract(archive, tmp_path / "extracted")
        assert len(result) == 64
        assert read(tmp_path / "extracted" / "identity.json") == {"id": 1}


@pytest.mark.usefixtures("attempt")
def test_report_replays_available_checkpoints_after_final_archive_loss(tmp_path, monkeypatch):
    @contextmanager
    def evidence(*_):
        yield tmp_path, "a" * 64, 120

    monkeypatch.setattr(report, "artifact_evidence", evidence)
    monkeypatch.setattr(
        report,
        "verify_episode",
        lambda *args: (
            "identity",
            {"episode": {"sender_valid": True, "drain": {"status": "drain_failure"}}},
        ),
    )
    name = rehearsal.label_name(1, "A")
    meta = {"id": 42, "digest": "sha256:" + "b" * 64}
    result = report.replay_job(None, tmp_path, None, 1, {name: meta}, None)
    assert set(result["cells"]) == {"A"}
    assert set(result["errors"]) == {"barrier", "final", *RATES, "soak"}
    assert result["diagnostic"]["diagnostic_first_failure"] is None


def test_witness_duration_replay_accepts_only_the_underlying_event_sums(monkeypatch):
    starts = [
        {
            "event": "start",
            "workflow_id": "a",
            "node_id": "n",
            "iteration": 0,
            "timestamp": f"2026-11-27T00:00:0{i}Z",
            "invocation_id": str(i),
            "monotonic_ns": i * 10**9,
        }
        for i in range(2)
    ]
    ends = [
        {**first, "event": "end", "monotonic_ns": first["monotonic_ns"] + 10**9} for first in starts
    ]
    monkeypatch.setattr(report, "witness_events", lambda witness: witness)
    totals = {"busy_seconds": 2.0, "reexecuted_seconds": 1.0}
    assert report.check_witness_durations([*starts, *ends], {"campaign": totals}) == totals
    with pytest.raises(ValueError, match="differs from witness duration"):
        report.check_witness_durations(
            [*starts, *ends], {"campaign": {**totals, "busy_seconds": 3.0}}
        )
