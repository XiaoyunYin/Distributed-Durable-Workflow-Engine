"""R321 removal decisions use receipt time and a bounded preceding snapshot."""

import copy
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dur053_k4_evidence as evidence


@pytest.fixture
def removal():
    request = 1_794_811_360.3548
    deadline = int(request + 40)
    c = dict(targets=[dict(pod_uid="removed", deleted=deadline)])
    pods = [
        dict(pod_uid=uid, role="worker", ready=True, eligible=True)
        for uid in ("removed", "idle", "claimed", "unready")
    ]
    pods[-1]["ready"] = False
    sample = dict(
        at=request - 0.52,
        error="",
        pods=pods,
        active_claims=[
            dict(pod_uid="removed", identity="claim"),
            dict(pod_uid="claimed", identity="other"),
        ],
    )
    t = dict(
        invocations=[
            dict(id="invocation", identity="claim", start=request - 2, finish=request + 0.1)
        ],
        sessions=[],
    )
    event = dict(
        received=request + 0.37,
        type="MODIFIED",
        object=dict(
            metadata=dict(
                uid="removed",
                deletionTimestamp=datetime.fromtimestamp(deadline, UTC).isoformat(),
                deletionGracePeriodSeconds=40,
            )
        ),
    )
    # A future sample and an error sample must never be selected. The valid row
    # is offset from both the request and whole-second grace deadline.
    samples = [
        dict(sample, at=request - 1.52),
        sample,
        dict(sample, at=request + 0.2, error="observer failure"),
        dict(sample, at=request + 0.56, active_claims=[]),
    ]
    return c, samples, t, [event]


def classify(removal, **kwargs):
    return evidence.classify_k7_removals(*removal, busy_upper=12, clock_slack=0.1, **kwargs)


def test_realistic_busy_and_idle_and_no_exact_equality(removal):
    result = classify(removal)
    c, samples, t, _ = removal
    assert all(s["at"] != c["targets"][0]["deleted"] for s in samples)
    assert evidence.busy_removals(c, samples, t, {})["ambiguous"]
    assert result["busy_uids"] == ["removed"] and result["idle_survivors"] == ["idle"]
    assert result["interrupted_claims"] == 1 and not result["ambiguous"]
    assert result["targets"][0]["snapshot_age"] == pytest.approx(0.89, abs=1e-6)
    assert result["targets"][0]["snapshot_at"] == samples[1]["at"]
    # The invocation finished BEFORE receipt, but was active at the snapshot.
    assert t["invocations"][0]["finish"] < result["targets"][0]["removal"]
    assert result["busy_upper"] == 12 and result["snapshot_max_age"] == 2.0


@pytest.mark.parametrize(
    "change,issue",
    [
        ("stale", "missing-or-stale-snapshot"),
        ("missing", "missing-deletion-event"),
        ("absent", "target-not-worker-in-snapshot"),
        ("role", "target-not-worker-in-snapshot"),
        ("join", "claim-invocation-join"),
        ("clock", "deletion-clock-mismatch-or-missing"),
        ("grace", "deletion-clock-mismatch-or-missing"),
    ],
)
def test_ambiguous_evidence(removal, change, issue):
    c, samples, t, events = removal
    if change == "stale":
        samples[:] = [dict(samples[0], at=events[0]["received"] - 2.001)]
    elif change == "missing":
        events.clear()
    elif change == "absent":
        for sample in samples:
            sample["pods"] = sample["pods"][1:]
    elif change == "role":
        samples[1]["pods"][0]["role"] = "relay"
    elif change == "join":
        t["invocations"][0]["start"] = samples[1]["at"] + 0.1
    elif change == "clock":
        events[0]["received"] += 5
    else:
        del events[0]["object"]["metadata"]["deletionGracePeriodSeconds"]
    result = classify(removal)
    assert result["ambiguous"] and issue in result["targets"][0]["issues"]


def test_first_event_and_two_second_boundary(removal):
    c, samples, _, events = removal
    samples[:] = [dict(samples[1], at=events[0]["received"] - 2.0)]
    later = copy.deepcopy(events[0])
    later["received"] += 30
    events.append(later)
    c["targets"][0]["deleted"] += 500  # The retained deadline is not the anchor.
    assert not classify(removal)["ambiguous"]
    assert classify(removal)["targets"][0]["removal"] == events[0]["received"]


def test_clock_bracket_slack_and_censored_cost(removal):
    events = removal[3]
    events[0]["received"] = int(events[0]["received"]) + 1.05
    assert not classify(removal)["ambiguous"]
    result = evidence.classify_k7_removals(*removal, busy_upper=None, clock_slack=0.1)
    assert result["ambiguous"] and result["busy_upper"] is None


def test_unverified_summaries_cannot_report_stored_k7():
    result = evidence.summaries([])
    assert result["k7_decision"] == "verify-evidence-only"
    assert result["k7"]["long"] == dict(status="verify-evidence-only")
    assert evidence.summaries([], k7_removals={})["k7_decision"] == "NO-GO"


def test_summary_uses_versioned_classification_and_keeps_campaign_safety(removal):
    corrected = classify(removal)
    metrics, removals = [], {}
    for pair in range(5):
        name = f"long-core-1-pair-{pair}-treatment"
        metrics.append(
            dict(
                episode=name,
                replacement_for="",
                status="PASS",
                valid=True,
                complete=True,
                busy_removals=dict(busy_uids=[], idle_survivors=[], ambiguous=True, busy_upper=12),
                wasted_original_seconds=dict(lower=10, unknown=0),
                reexecuted_seconds=dict(lower=10, unknown=0),
            )
        )
        removals[name] = corrected
    result = evidence.summaries(metrics, safety_rows=[], k7_removals=removals)
    assert result["k7"]["long"]["eligible"] == 5
    assert result["k7"]["long"]["median_excess"] == 15
    # Partial history cannot pass campaign safety even though all five
    # treatment rows have good removals and strictly positive excess.
    assert not result["k7"]["long"]["safety"]
    assert result["k7_decision"] == "NO-GO"
    assert all(m["busy_removals"]["ambiguous"] for m in metrics)
    removals.pop(name)
    assert (
        evidence.summaries(metrics, safety_rows=[], k7_removals=removals)["k7_decision"]
        == "verify-evidence-only"
    )


def test_history_requires_raw_verification(tmp_path, monkeypatch):
    import dur053_record_k4 as recorder

    monkeypatch.setattr(recorder, "inventory", lambda _: ([], dict(entries=[])))
    monkeypatch.setattr(evidence, "frozen", lambda _: {})
    monkeypatch.setattr(evidence, "remaining", lambda *args: {})
    assert evidence.history_view(tmp_path)["k7_decision"] == "verify-evidence-only"


def test_scoped_verification_does_not_schedule_partial_progress(tmp_path, monkeypatch):
    import dur053_record_k4 as recorder

    monkeypatch.setattr(recorder, "inventory", lambda _: ([], dict(entries=[])))
    monkeypatch.setattr(evidence, "frozen", lambda _: {})
    monkeypatch.setattr(evidence, "remaining", lambda *args: pytest.fail("partial schedule"))
    result = evidence.verify_evidence(tmp_path, None, jobs=set())
    assert result["schedule"] is None and result["verification_scope"] == []
    assert not result["complete"] and not result["valid"]
