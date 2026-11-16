"""R318 keeps observed HTTP drain cost visible without changing engine outcomes."""

import copy
import hashlib
import json
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dur053_k4_evidence as evidence
import dur053_k4_schema as schema
import dur053_record_k4 as recorder
from test_k4_offline import (  # noqa: F401
    checker,
    episode_fixture,
    freeze,
    harness_incident,
    write,
)


def http_row(role="worker-control", status=503):
    return dict(
        role=role,
        request="retry-id",
        body_hash="a" * 64,
        attempt=1,
        operation="result",
        at=120,
        status=status,
        error="CONTROL_UNAVAILABLE",
    )


def safety_row():
    return dict(
        episode="short-core-6-pair-0-baseline",
        unavailable=[],
        lost_accepted=0,
        duplicate_accepted=0,
        stale_committed=0,
        superseded_owner_transitions=0,
        failed_workflows=0,
        canceled_workflows=0,
        http_5xx=7,
        http_5xx_by_role=[dict(role="worker-control", arm="baseline", http_5xx=7)],
    )


def test_http_only_is_drain_cost():
    row = safety_row()
    result = evidence.safety_aggregate([row])
    schema.validate(result, schema.SAFETY)
    assert result["passed"] and result["http_5xx"] == 7, (
        "R318 HTTP-only drain cost failed safety or disappeared"
    )
    assert result["http_5xx_by_role"] == row["http_5xx_by_role"]


@pytest.mark.parametrize(
    "counter",
    [
        "stale_committed",
        "lost_accepted",
        "duplicate_accepted",
        "superseded_owner_transitions",
        "failed_workflows",
        "canceled_workflows",
    ],
)
def test_other_safety_counters_still_fail(counter):
    row = safety_row()
    row[counter] = 1
    assert not evidence.safety_aggregate([row])["passed"], "R318 hid a safety failure"
    row["http_5xx"] = 0
    row["http_5xx_by_role"] = []
    assert not evidence.safety_aggregate([row])["passed"]


def test_unavailable_audit_still_fails():
    row = safety_row()
    row["unavailable"] = ["lease audit unavailable"]
    assert not evidence.safety_aggregate([row])["passed"]


def test_report_retains_http_per_role_and_arm(tmp_path, freeze, checker):  # noqa: F811
    rows = []
    for arm, roles in [
        ("baseline", ["worker-control", "worker-control", "api"]),
        ("treatment", ["api"]),
    ]:
        name = "short-core-6-pair-0-" + arm
        _, docs = episode_fixture(tmp_path / name, freeze, episode=name)
        docs["timeline"]["http"] = [http_row(role) for role in roles]
        write(tmp_path / name / "timeline.json", docs["timeline"])
        row = evidence.safety_evidence(tmp_path / name, checker)
        assert row["http_5xx"] == len(roles), "R318 reported row dropped HTTP 5xx"
        rows.append(row)
    result = evidence.summaries([], safety_rows=rows)["safety"]
    assert result["passed"] and result["http_5xx"] == 4
    assert result["http_5xx_by_role"] == [
        dict(role="api", arm="baseline", http_5xx=1),
        dict(role="api", arm="treatment", http_5xx=1),
        dict(role="worker-control", arm="baseline", http_5xx=2),
    ]
    wrong = copy.deepcopy(result)
    wrong["http_5xx_by_role"][0]["hidden"] = 1
    with pytest.raises(ValueError, match="k4-schema"):
        schema.validate(wrong, schema.SAFETY)


def test_http_engine_outcome_cannot_be_excluded(tmp_path, freeze):  # noqa: F811
    name = "short-core-6-pair-0-baseline"
    ep, docs = episode_fixture(tmp_path / name, freeze, episode=name)
    ep.update(status="invalid", timeout=False)
    write(tmp_path / name / "episode.json", ep)
    d = harness_incident(
        tmp_path, name, raw=dict(component="collector", errors=["export failed"], missing=[])
    )
    # Prove the harness record alone is admissible: 503 is the sole outcome guard.
    recorder.validate_disposition(tmp_path, d)
    docs["timeline"]["http"].append(http_row())
    write(tmp_path / name / "timeline.json", docs["timeline"])
    with pytest.raises(ValueError, match="k4-engine-outcome-not-infrastructure"):
        recorder.validate_disposition(tmp_path, d)


def test_intake_accepts_only_matching_retained_reports(tmp_path, monkeypatch):
    name = safety_row()["episode"]
    metric = dict(episode=name, safety=safety_row(), valid=True)
    manifest = dict(
        job="k4-2",
        run_id="1",
        budget_scope="registered-campaign",
        job_id="2",
        head_sha="a" * 40,
        merge_ref_sha="a" * 40,
        tree_sha="a" * 40,
        status="CARRY_OVER",
        charged_seconds=400,
        episodes=[name],
        dispositions=[dict(episode=name, classification="valid")],
    )

    def row_for(rules):
        row = safety_row()
        if rules is evidence.Derivation.HISTORICAL:
            del row["http_5xx_by_role"]
        return row

    def summary_for(*args, rules=evidence.Derivation.CORRECTED, **kwargs):
        return dict(safety=evidence.safety_aggregate([row_for(rules)], rules=rules), other="bound")

    write(tmp_path / name / "episode.json", {})
    write(tmp_path / "manifest.json", manifest)
    monkeypatch.setattr(recorder, "frozen", lambda _: {})
    monkeypatch.setattr(recorder, "inventory", lambda _: ([], {"entries": []}))
    monkeypatch.setattr(recorder, "validate_job", lambda *args, **kwargs: manifest)
    monkeypatch.setattr(recorder, "validate_disposition", lambda *args, **kwargs: None)
    monkeypatch.setattr(recorder, "ledger_check", lambda *args: None)
    monkeypatch.setattr(
        evidence,
        "episode",
        lambda *args, rules=evidence.Derivation.CORRECTED: dict(
            copy.deepcopy(metric), safety=row_for(rules)
        ),
    )
    monkeypatch.setattr(
        evidence,
        "safety_evidence",
        lambda *args, rules=evidence.Derivation.CORRECTED: row_for(rules),
    )
    monkeypatch.setattr(evidence, "summaries", summary_for)

    for old_format in (True, False):
        manifest["run_id"] = "43714142099" if old_format else "1"
        write(tmp_path / "manifest.json", manifest)
        rules = evidence.Derivation.HISTORICAL if old_format else evidence.Derivation.CORRECTED
        stored_metric = dict(copy.deepcopy(metric), safety=row_for(rules))
        stored_summary = summary_for(rules=rules)
        write(tmp_path / name / "metrics.json", stored_metric)
        write(tmp_path / "summary.json", stored_summary)
        before = {p: p.read_bytes() for p in tmp_path.rglob("*.json")}
        _, _, _, intake, _, _ = recorder.prepare(tmp_path, tmp_path / "repo", Path("checker"))
        assert intake["metrics"] == [stored_metric], "R318 intake did not reproduce producer rules"
        assert intake["safety_rows"] == [row_for(rules)]
        assert all(p.read_bytes() == raw for p, raw in before.items())
        for document, guard in (
            (tmp_path / name / "metrics.json", "k4-intake-metrics"),
            (tmp_path / "summary.json", "k4-intake-summary"),
        ):
            original = json.loads(before[document])
            tampered = copy.deepcopy(original)
            if old_format:
                tampered["safety"]["stale_committed"] += 1
            else:
                tampered["safety"]["http_5xx_by_role"][0]["arm"] = "treatment"
            write(document, tampered)
            with pytest.raises(ValueError, match=guard):
                recorder.prepare(tmp_path, tmp_path / "repo", Path("checker"))
            write(document, original)


def test_verification_recomputes_historical_http_safety(
    tmp_path,
    freeze,  # noqa: F811
    checker,  # noqa: F811
    monkeypatch,
):
    name = "short-core-6-pair-0-baseline"
    ep, docs = episode_fixture(tmp_path / name, freeze, episode=name)
    docs["timeline"]["http"].extend(http_row() for _ in range(7))
    write(tmp_path / name / "timeline.json", docs["timeline"])
    ep["status"] = "unexercised"
    ep["files"]["timeline"] = hashlib.sha256(
        (tmp_path / name / "timeline.json").read_bytes()
    ).hexdigest()
    write(tmp_path / name / "episode.json", ep)
    write(
        tmp_path / name / "metrics.json",
        evidence.episode(tmp_path / name, freeze, checker, rules=evidence.Derivation.HISTORICAL),
    )
    old_metric = evidence.episode(
        tmp_path / name, freeze, checker, rules=evidence.Derivation.HISTORICAL
    )
    old_rows = [
        evidence.safety_evidence(tmp_path / name, checker, rules=evidence.Derivation.HISTORICAL)
    ]
    old = evidence.summaries(
        [old_metric], safety_rows=old_rows, rules=evidence.Derivation.HISTORICAL
    )
    assert old["safety"]["passed"] is False
    write(tmp_path / "summary.json", old)
    manifest = dict(
        job="k4-2",
        run_id="43714142099",
        budget_scope="registered-campaign",
        job_id="2",
        status="CARRY_OVER",
        charged_seconds=400,
        pending=[],
        episodes=[name],
        dispositions=[dict(episode=name, classification="unexercised", reason="", evidence=[])],
    )

    @contextmanager
    def materialize(*args, **kwargs):
        yield tmp_path

    monkeypatch.setattr(recorder, "materialize_job", materialize)
    monkeypatch.setattr(evidence, "frozen", lambda _: freeze)
    monkeypatch.setattr(
        recorder, "inventory", lambda _: ([(tmp_path / "manifest.json", manifest)], {"entries": []})
    )
    monkeypatch.setattr(recorder, "validate_job", lambda *args, **kwargs: manifest)
    result = evidence.verify_evidence(tmp_path, checker)
    assert result["safety"]["passed"] and result["safety"]["http_5xx"] == 7
    assert result["safety"]["http_5xx_by_role"] == [
        dict(role="worker-control", arm="baseline", http_5xx=7)
    ]
    assert not result["valid"] and not result["complete"]  # registry still incomplete


def test_next_group_rebalance_ends_generation():
    def row(change, event, at, generation, **extra):
        return dict(
            change=change,
            event=event,
            at=at,
            generation=generation,
            consumer="survivor",
            survivors=["survivor"],
            partitions=[0],
            trigger_uid="pod",
            arrival="",
            disposition_order=None,
            **extra,
        )

    rows = [
        row("old", "revoke", 0, 3),
        row("old", "stable", 4.57, 4),
        row("new", "revoke", 5.31, -1),
        row("new", "stable", 5.4, 5),
    ]
    disposed = row("new", "disposed", 7.63, 5)
    disposed.update(arrival="000007", disposition_order=1)
    rows.append(disposed)
    arrivals = [dict(id="000007", workflow="workflow", scheduled=5.53)]
    timeline = dict(
        history=[dict(order=1, kind="receipt", identity=dict(workflow="workflow"))], invocations=[]
    )
    corrected = evidence.assignment_stalls(rows, arrivals, timeline, 100)
    historical = evidence.assignment_stalls(
        rows, arrivals, timeline, 100, rules=evidence.Derivation.HISTORICAL
    )
    old = next(s for s in corrected if s["change"] == "old")
    assert old["censored"] and old["eligible_undisposed"] == [], (
        "R319 joining member revoke did not end the stable generation"
    )
    assert next(s for s in historical if s["change"] == "old")["eligible_undisposed"] == ["000007"]
    # A genuinely eligible arrival before the next rebalance must still gate.
    arrivals[0]["scheduled"] = 5.0
    old = next(
        s for s in evidence.assignment_stalls(rows, arrivals, timeline, 100) if s["change"] == "old"
    )
    assert old["censored"] and old["eligible_undisposed"] == ["000007"], (
        "R319 eligible undisposed arrival before rebalance escaped the gate"
    )
