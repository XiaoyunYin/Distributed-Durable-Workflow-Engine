import ast
import json
import subprocess
import sys
import time
import zipfile
from contextlib import contextmanager, nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import j2_integrated_rehearsal as rehearsal  # noqa: E402
import j2_integrated_report as report  # noqa: E402
from j2_attribution import diagnostic_pair  # noqa: E402
from j2_attribution_fixture import fixture_report  # noqa: E402
from j2_barrier import (  # noqa: E402
    INTEGRATED_SCOPE,
    Barrier,
    BarrierStop,
    Campaign,
    RetryLater,
    checkpoint_name,
)
from j2_checked_cut_fixture import exercise_fixture  # noqa: E402
from j2_checker_report import IntegrityError  # noqa: E402
from j2_drain import DrainExpired  # noqa: E402
from j2_harness import POD_IMPORTS  # noqa: E402
from j2_harness_evidence import seal  # noqa: E402
from j2_integrated_evidence import registered_boundary  # noqa: E402
from j2_measure import wait_for_soak  # noqa: E402
from j2_measure_replay import (  # noqa: E402
    ReplayBudget,
    claim_candidates,
    qualified_summary,
    replay_authorization,
)
from j2_qualification_evidence import read, write  # noqa: E402
from j2_schedule import ORDERS, RATES  # noqa: E402
from j2_schedule_rehearsal import PEER_SUPERVISOR  # noqa: E402


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


@pytest.fixture(scope="module")
def native_checker(tmp_path_factory):
    target = tmp_path_factory.mktemp("checked-cut-bin") / "checker"
    if sys.platform == "win32":
        target = target.with_suffix(".exe")
    subprocess.run(
        [
            "go",
            "build",
            "-p",
            "1",
            "-mod=readonly",
            "-buildvcs=false",
            "-o",
            str(target),
            "./cmd/dur049-checker",
        ],
        cwd=Path(__file__).resolve().parents[3],
        check=True,
        timeout=180,
    )
    return target


def test_full_clock_c_cut_checks_pending_identities_and_replays(native_checker, tmp_path):
    exercise_fixture(tmp_path, native_checker, "1200MiB", "eng-checked-cut-test")
    report_file = read(tmp_path / "fixture-report.json")
    expired, quiet = (report_file["cases"][name] for name in ("checked-expiry", "quiet"))
    assert expired["population"]["scheduled"] == quiet["population"]["scheduled"] == 17519
    assert expired["population"]["windows"]["grid"]["scheduled"] == 14599
    assert expired["pending_count"] == 5353
    assert expired["gate"] == "failed_gate" and expired["drain"] == "drain_failure"
    assert expired["checks"] == {"4a": False, "4b": None, "4c": False}
    assert quiet["gate"] == "pass" and quiet["drain"] == "PASS"
    assert expired["independent_replay"] == quiet["independent_replay"] == "PASS"
    assert report_file["boundary_with_passing_lower_inputs"]["saturation"] == "C"
    cut = read(tmp_path / "checked-expiry" / "episode-replay.json")
    grid = cut["cohorts"]["grid"]
    assert grid["phases"]["end_to_end_quantiles"]["censored"] == 5353
    assert len(grid["first_observed_minus_durable_ns"]) == 9246
    assert set(grid["first_observed_minus_durable_ns"].values()) == {1_000_000}
    assert report_file["clock_negatives"]["missing_clock"]["claim_eligible"] is False
    assert report_file["clock_negatives"]["bad_final_clock"]["claim_eligible"] is False
    assert (tmp_path / "bad-clock.json").is_file()


def test_boundary_uses_checked_gate_and_keeps_drain_verdict_separate():
    def cell(gate, drain, *, growth=False):
        return {
            "sender_valid": True,
            "cohorts": {"grid": {"gate": {"status": gate, "sustained_growth": growth}}},
            "drain": {"status": drain},
        }

    grid = {
        "AB": cell("failed_gate", "drain_failure", growth=True),
        "A": cell("pass", "drain_failure"),
        "B": cell("pass", "PASS"),
    }
    result = registered_boundary(grid)
    assert result == {
        "stable_rates": ["A", "B"],
        "drains": {"A": "drain_failure", "AB": "drain_failure", "B": "PASS"},
        "saturation": "AB",
    }
    grid["A"] = cell("invalid_gate_inputs", "PASS")
    result = registered_boundary(grid)
    assert result["saturation"] is None
    assert result["drains"]["AB"] == "drain_failure"
    assert diagnostic_pair(result, {})["component"] == "unresolved"


def verified_replay_jobs():
    choice = {"selected_rate": "A", "grid_sha256": ["1", "2", "3"]}
    states = {job: {"conclusion": "success"} for job in (1, 2, 3)}
    jobs = {
        job: {
            "grid": {
                "body": {
                    "cells": [
                        {"rate": cell, "outcome": "pass" if cell == "A" else "gate_failure"}
                        for cell in RATES
                    ]
                },
                "sha256": str(job),
            },
            "cells": {
                **dict.fromkeys(RATES, {"verified": True}),
                "soak": {
                    "row": {"rate": "A"},
                    "episode": {
                        "soak_evidence_eligible": True,
                        "cohorts": {
                            "hold": {
                                "in_window_success": ["one"],
                                "phases": {"end_to_end_quantiles": {"p95_ns": 1_000_000}},
                            }
                        },
                    },
                },
            },
            "errors": {},
            "integrity_stop": False,
            "decision": dict(choice),
            "final": {"runner": {"available_cpu_ids": [0, 1]}},
        }
        for job in states
    }
    return jobs, states


@pytest.mark.parametrize(
    "failure",
    [
        "missing_checkpoint",
        "integrity_stop",
        "missing_decision",
        "rate_disagreement",
        "digest_disagreement",
        "peer_failed",
    ],
)
def test_replay_withholds_every_dependent_sentence_but_keeps_observed_work(failure):
    jobs, states = verified_replay_jobs()
    original = jobs[1]["cells"]["soak"]
    if failure == "missing_checkpoint":
        jobs[3]["cells"].pop("C")
    elif failure == "integrity_stop":
        jobs[3]["integrity_stop"] = True
    elif failure == "missing_decision":
        jobs[3]["decision"] = None
    elif failure == "rate_disagreement":
        jobs[3]["decision"]["selected_rate"] = "AB"
    elif failure == "digest_disagreement":
        jobs[3]["decision"]["grid_sha256"] = ["1", "3", "2"]
    else:
        states[3]["conclusion"] = "failure"
        jobs[1]["final"]["peer_failure"] = {"sender_released": True}
    outcome = replay_authorization(jobs, states)
    assert outcome["status"] == "unverifiable"
    assert (
        claim_candidates(SimpleNamespace(campaign="j2-load-envelope-v1"), jobs, outcome, True) == []
    )
    assert jobs[1]["cells"]["soak"] is original


def test_only_independently_qualified_soaks_publish_and_engineering_has_no_sentence():
    jobs, states = verified_replay_jobs()
    jobs[3]["cells"]["soak"]["episode"]["soak_evidence_eligible"] = False
    auth = replay_authorization(jobs, states)
    assert auth["status"] == "verified"
    actual = claim_candidates(SimpleNamespace(campaign="j2-load-envelope-v1"), jobs, auth, True)
    assert [row["job"] for row in actual] == [1, 2]
    assert [row["qualification"] for row in actual] == ["2 of 3 jobs qualified"] * 2
    assert qualified_summary(actual) is None
    assert claim_candidates(SimpleNamespace(campaign="eng-fixture"), jobs, auth, True) == []

    jobs[3]["cells"]["soak"]["episode"]["soak_evidence_eligible"] = True
    three = claim_candidates(SimpleNamespace(campaign="j2-load-envelope-v1"), jobs, auth, True)
    assert [row["job"] for row in qualified_summary(three)["pairs"]] == [1, 2, 3]
    assert qualified_summary(three)["latency_ns_ranges"] == {"p95": [1_000_000, 1_000_000]}


def test_peer_failure_after_sender_release_does_not_discard_the_checkpoint(tmp_path, monkeypatch):
    import j2_measure

    for name in ("commands", "driver/soak", "final"):
        (tmp_path / name).mkdir(parents=True)
    write(tmp_path / "driver/soak/sender-release.json", {"run_id": "run"})
    when = 0
    campaign = object()

    def peer_failed(value):
        assert value is campaign
        raise BarrierStop("peer_failure", "job 3 failed during the soak")

    def sleep(delay):
        nonlocal when
        when += delay
        write(tmp_path / "commands/soak-done.json", {"index_sha256": "archive"})

    monkeypatch.setattr(
        j2_measure, "client", lambda *_: SimpleNamespace(api=SimpleNamespace(jobs=peer_failed))
    )
    wait_for_soak(tmp_path, campaign, 60, now=lambda: when, sleep=sleep)
    assert read(tmp_path / "commands/soak-done.json")["index_sha256"] == "archive"
    assert read(tmp_path / "final/peer-failure.json")["reason"] == "peer_failure"
    assert read(tmp_path / "final/peer-failure.json")["sender_released"] is True


def test_supervisor_stops_additional_sends_and_preserves_first_intent(tmp_path):
    imports = {
        name
        for node in ast.parse(PEER_SUPERVISOR).body
        for name in (
            [alias.name for alias in node.names]
            if isinstance(node, ast.Import)
            else [node.module]
            if isinstance(node, ast.ImportFrom)
            else []
        )
    }
    assert imports <= set(POD_IMPORTS)
    stop, journal = (tmp_path / name for name in ("stop", "journal"))
    fake_sender = (
        "from pathlib import Path; import sys,time; p=Path(sys.argv[1]); "
        "t=p.with_suffix('.tmp'); t.write_text('first intent'); t.replace(p); "
        "time.sleep(30); p.write_text('second intent')"
    )
    command = [
        sys.executable,
        "-c",
        PEER_SUPERVISOR,
        str(stop),
        sys.executable,
        "-c",
        fake_sender,
        str(journal),
    ]
    with subprocess.Popen(command, stdout=subprocess.PIPE, text=True) as supervisor:
        try:
            deadline = time.monotonic() + 5
            while not journal.is_file() and time.monotonic() < deadline:
                time.sleep(0.025)
            assert journal.read_text() == "first intent"
        finally:
            write(stop.with_suffix(".tmp"), {"reason": "peer_failure"})
            stop.with_suffix(".tmp").replace(stop)
            stdout, _ = supervisor.communicate(timeout=7)
    assert supervisor.returncode == 1
    assert json.loads(stdout) == {"reason": "peer_failure"}
    assert journal.read_text() == "first intent"


def test_raw_attribution_fixture_requires_the_competing_discriminators(tmp_path):
    findings = fixture_report(tmp_path / "raw-pairs")["cases"]
    assert findings["supported"]["component"] == "relay_publication"
    assert (
        findings["conflicting"]["component"] == findings["drain_only"]["component"] == "unresolved"
    )
    missing = findings["missing_discriminator"]
    assert missing["component"] == "unresolved" and missing["missing"] == [
        "broker timings/offsets or relay trace"
    ]


def test_reporter_reserves_upload_and_bounds_retries_and_scratch(tmp_path):
    budget = ReplayBudget(
        tmp_path,
        0,
        now=lambda: 0,
        disk=lambda _: SimpleNamespace(free=1 << 30),
    )
    assert budget.record()["reserved_minutes"] == 85
    assert budget.cutoff == 81 * 60
    with pytest.raises(ValueError, match="disk headroom"):
        budget.disk_for(1 << 30)
    budget.seconds["retry"] = 14 * 60 - 10

    def unavailable():
        raise RetryLater("test transport", 20)

    with pytest.raises(ValueError, match="retry reserve exhausted"):
        budget.retry(unavailable)
