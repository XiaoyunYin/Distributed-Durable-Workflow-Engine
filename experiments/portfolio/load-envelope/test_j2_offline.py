import sys
from copy import deepcopy
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

from j2_offline import (  # noqa: E402
    SECOND,
    ClockEvidenceError,
    bins_gate,
    clock_bounds,
    clock_evidence,
    drain_identities,
    join,
    quantiles,
    sender,
    summarize,
    utc_ns,
)


def ts(second, ns=0):
    minutes, seconds = divmod(second, 60)
    return f"1970-01-01T00:{minutes:02}:{seconds:02}.{ns:09}Z"


@pytest.fixture
def episode():
    key = "a"
    identity = {"workflow_id": key, "node_id": "x", "iteration": 0, "attempt_number": 2}
    old = dict(identity, attempt_number=1)

    def event(identity, rev, at, reason, state="WAITING_ACTIVITY"):
        return dict(identity, revision=rev, created_at=ts(at), reason=reason, new_state=state)

    history = [
        event(identity, 5, 7, "ATTEMPT_RESULT_RECORDED"),
        event(old, 2, 1, "ATTEMPT_CREATED"),
        event(identity, 8, 8, "GRAPH_ADVANCED", "SUCCEEDED"),
        event(identity, 4, 2, "ATTEMPT_CREATED"),
        event(old, 3, 7, "ATTEMPT_RESULT_RECORDED"),
        event(old, 9, 8, "ATTEMPT_RESULT_RECORDED"),
    ]
    snapshot = {
        "Snapshot": {
            f"engine.{table}": {"rows": rows}
            for table, rows in {
                "workflow_executions": [{"workflow_id": key, "state": "SUCCEEDED", "revision": 8}],
                "transition_history": history,
                "activity_attempts": [
                    dict(identity, claim_token="winning", state="SUCCEEDED", result=5)
                ],
                "outbox": [
                    {
                        "workflow_id": key,
                        "aggregate_revision": 5,
                        "event_type": "activity.result",
                        "created_at": ts(7),
                        "payload": 5,
                    }
                ],
            }.items()
        },
        "Submissions": [{"WorkflowID": key}],
    }
    witnesses = [
        dict(identity, timestamp=ts(3), event="start", claim_token="winning"),
        dict(old, timestamp=ts(1), event="start", claim_token="old"),
    ]
    return [(key, SECOND + 1)], snapshot, witnesses, {key: 9 * SECOND}, 100, 10 * SECOND


def test_revision_order_and_stage_delay(episode):
    (baseline,) = join(*episode)
    assert baseline["winning_attempt"] == ("a", "x", 0, 2)
    assert baseline["attempts"] == 2
    assert baseline["phase_ns"] == [SECOND - 1, SECOND, 4 * SECOND, SECOND]
    assert baseline["end_to_end_ns"] - baseline["raw_end_to_end_ns"] == 100
    assert baseline["phase_sensitivity_ns"] == [100, 100, 100, 0]
    delayed = deepcopy(episode)
    for row in delayed[1]["Snapshot"]["engine.transition_history"]["rows"]:
        if row["revision"] >= 5:
            row["created_at"] = ts(9 if row["revision"] == 5 else 10)
    delayed[1]["Snapshot"]["engine.outbox"]["rows"][0]["created_at"] = ts(9)
    delayed[3]["a"] += 2 * SECOND
    (changed,) = join(*delayed[:-1], delayed[-1] + 2 * SECOND)
    assert changed["workflow_id"] == baseline["workflow_id"]
    assert [b - a for a, b in zip(baseline["phase_ns"], changed["phase_ns"], strict=True)] == [
        0,
        0,
        2 * SECOND,
        0,
    ]
    assert changed["raw_end_to_end_ns"] - baseline["raw_end_to_end_ns"] == 2 * SECOND


@pytest.mark.parametrize(
    "case, status",
    [
        ("duplicate_workflow", "duplicate_workflow"),
        ("pending", "censored"),
        ("lost_terminal", "missing_or_duplicate_terminal"),
        ("wrong_revision", "missing_or_duplicate_terminal"),
        ("poll_before_terminal", "terminal_clock_or_observation"),
        ("failed", "failed"),
        ("duplicate_winner", "missing_or_duplicate_winner"),
        ("missing_creation", "missing_or_duplicate_attempt_boundary"),
        ("duplicate_witness", "missing_or_duplicate_attempt_boundary"),
        ("missing_receipt", "missing_or_duplicate_attempt_boundary"),
        ("wrong_token", "attempt_receipt_mismatch"),
        ("wrong_result", "attempt_receipt_mismatch"),
        ("reversed_phase", "reversed_phase"),
    ],
)
def test_censoring_and_required_boundaries(episode, case, status):
    _, snapshot, starts, observed, *_ = episode
    tables = snapshot["Snapshot"]
    workflows = tables["engine.workflow_executions"]["rows"]
    history = tables["engine.transition_history"]["rows"]
    match case:
        case "duplicate_workflow":
            workflows.append(deepcopy(workflows[0]))
        case "pending":
            workflows[0]["state"] = "WAITING_ACTIVITY"
        case "lost_terminal":
            history.pop(2)
        case "wrong_revision":
            history[2]["revision"] = 9
        case "poll_before_terminal":
            observed["a"] = 8 * SECOND - 101
        case "failed":
            workflows[0]["state"] = history[2]["new_state"] = "FAILED"
        case "duplicate_winner":
            history.append(deepcopy(history[0]))
        case "missing_creation":
            history.pop(3)
        case "duplicate_witness":
            starts.append(deepcopy(starts[0]))
        case "missing_receipt":
            tables["engine.outbox"]["rows"].clear()
        case "wrong_token":
            starts[0]["claim_token"] = "another"
        case "wrong_result":
            tables["engine.outbox"]["rows"][0]["payload"] = 9
        case "reversed_phase":
            starts[0]["timestamp"] = ts(1)
    (result,) = join(*episode)
    assert result["status"] == status
    assert "phase_ns" not in result
    if status == "censored":
        assert result["lower_bound_ns"] == 9 * SECOND - 1
        assert summarize([result])["end_to_end_quantiles"]["censored"] == 1


def test_withheld_terminal_preserves_count_but_fails_drain(episode):
    assert drain_identities("ab", "ab", "ab", "ab")["valid"]
    episode[1]["Snapshot"]["engine.transition_history"]["rows"].pop(2)
    rows = list(join(*episode))
    successes = [r["workflow_id"] for r in rows if "end_to_end_ns" in r]
    report = drain_identities("ab", "ab", [*successes, "b", "b"], "ab")
    assert report == {"valid": False, "missing": ["a"], "unexpected": [], "duplicated": ["b"]}
    assert drain_identities("b", "ab", "b", "b")["missing"] == ["a"]


def test_p99_floor_uses_observed_stage_count():
    below = quantiles(range(199), 201, 1)
    assert below == {
        "eligible": 201,
        "observed": 199,
        "missing": 1,
        "censored": 1,
        "p50_ns": 99,
        "p95_ns": 189,
    }
    assert quantiles(range(200), 200, 0)["p99_ns"] == 197
    assert quantiles([], 0, 0)["p95_ns"] is None


def test_clock_uses_widest_spanning_bracket():
    probes = {
        "setup": dict(observer_before=ts(0, 1), database=ts(0, 6), observer_after=ts(0, 11)),
        "measured": dict(observer_before=ts(3), database=ts(3, 100), observer_after=ts(3, 500)),
    }
    assert clock_bounds(probes, SECOND, 3 * SECOND) == 500
    with pytest.raises(ValueError, match="clock bracket"):
        clock_bounds(probes, SECOND, 3 * SECOND + 1)
    probes["setup"]["database"] = ts(0, 12)
    with pytest.raises(ValueError, match="clock bracket"):
        clock_bounds(probes, SECOND, 3 * SECOND)


@pytest.mark.parametrize(
    "case", ["before_setup", "after_final", "unspanned", "missing", "malformed"]
)
def test_clock_evidence_rejects_bad_brackets(case):
    probes = {
        "setup": dict(observer_before=ts(0), database=ts(0, 100), observer_after=ts(0, 200)),
        "measured": dict(observer_before=ts(3), database=ts(3, 100), observer_after=ts(3, 500)),
    }
    assert clock_evidence(probes, SECOND, 3 * SECOND) == {
        "valid": True,
        "bracket_widths_ns": {"setup": 200, "measured": 500},
        "uncertainty_ns": 500,
    }
    match case:
        case "before_setup":
            probes["setup"]["observer_before"] = ts(0, 101)
        case "after_final":
            probes["measured"]["database"] = ts(3, 501)
        case "unspanned":
            probes["setup"] = {key: ts(2) for key in probes["setup"]}
        case "missing":
            del probes["measured"]
        case "malformed":
            probes["setup"]["database"] = "not UTC"
    with pytest.raises(ClockEvidenceError, match="clock bracket"):
        clock_evidence(probes, SECOND, 3 * SECOND)


@pytest.mark.parametrize(
    ("edge", "delta", "status"),
    [
        ("scheduled", 0, "reversed_phase"),
        ("scheduled", 1, "terminal_clock_or_observation"),
        ("observed", 0, "observed"),
        ("observed", -1, "terminal_clock_or_observation"),
        ("missing", 0, "terminal_clock_or_observation"),
    ],
)
def test_terminal_clock_inequality_and_equal_edges(episode, edge, delta, status):
    plan, snapshot, starts, first, width, end = episode
    terminal = 8 * SECOND
    if edge == "scheduled":
        plan = [("a", terminal + width + delta)]
    elif edge == "observed":
        first = {"a": terminal - width + delta}
    else:
        first = {}
    (row,) = join(plan, snapshot, starts, first, width, end)
    assert row["status"] == status
    if edge == "scheduled" and delta == 0:
        # The lower clock bound holds, but the raw phases are reversed.
        assert row["polling_minus_durable_ns"] == SECOND
    if edge == "observed" and delta == 0:
        assert row["polling_minus_durable_ns"] == -width


def test_p99_workflow_ties_use_schedule_then_identity_and_disclose_populations():
    rows = [
        dict(
            workflow_id=f"w{i:03}",
            scheduled_ns=i // 2,
            admitted=True,
            attempts=1,
            status="observed",
            phase_ns=[1, 2, 3, 4],
            end_to_end_ns=11,
        )
        for i in reversed(range(200))
    ]
    rows += [dict(admitted=True, attempts=0, status=s) for s in ("censored", "missing_workflow")]
    result = summarize(rows)
    assert result["end_to_end_p99_workflow"]["workflow_id"] == "w197"
    assert result["end_to_end_p99_workflow"]["phase_ns"] == [1, 2, 3, 4]
    for counts in [*result["phase_quantiles"], result["end_to_end_quantiles"]]:
        assert {k: counts[k] for k in ("eligible", "observed", "missing", "censored")} == {
            "eligible": 202,
            "observed": 200,
            "missing": 1,
            "censored": 1,
        }
    small = summarize(rows[1:])
    assert small["end_to_end_p99_workflow"] is None
    assert small["end_to_end_quantiles"]["p95_ns"] == 11


@pytest.fixture
def delivery():
    plan = {
        "origin": ts(0),
        "periods": [[0, 2, 1]],
        "run_id": "test",
        "seed": 17,
        "namespace": "j2",
    }
    rows, commits = [], []
    for i in range(2):
        key = f"dur053-test-17-{i:06d}"
        response = {
            "workflow": {
                "workflow_id": key,
                "submission_key": key,
                "namespace": "j2",
                "payload_hash": "hash",
            }
        }
        rows.append(
            dict(
                sequence=i,
                key=key,
                workflow_id=key,
                scheduled_ns=i * SECOND,
                scheduled_at=ts(i),
                send_ns=i * SECOND,
                sent_at=ts(i),
                outcome="ACCEPTED",
                http_status=201,
                response=response,
            )
        )
        commits.append(dict(Key=key, WorkflowID=key, Namespace="j2", Hash="hash"))
    return plan, {"status": "PASS", "rows": rows}, commits


@pytest.mark.parametrize(
    "case, problem",
    [
        ("missing", "missing_or_duplicate_journal"),
        ("late_final", "missing_or_late_send"),
        ("capacity", "missing_or_late_send"),
        ("bad_schedule", "schedule_mismatch"),
        ("missing_commit", "unreconciled_admission"),
        ("bad_hash", "acceptance_mismatch"),
        ("shedding", "http_429"),
        ("other_4xx", "http_4xx"),
        ("server_error", "http_5xx"),
        ("malformed", "malformed_acceptance"),
        ("transport", "other_transport_error"),
    ],
)
def test_invalid_sender(delivery, case, problem):
    plan, journal, commits = delivery
    row = journal["rows"][-1]
    key = row["key"]
    match case:
        case "missing":
            journal["rows"].pop()
        case "late_final":
            row.update(send_ns=2 * SECOND, sent_at=ts(2))
        case "capacity":
            row.update(send_ns=None, sent_at=None, outcome="CAPACITY_MISSED")
        case "bad_schedule":
            row["scheduled_at"] = ts(1, 1)
        case "missing_commit":
            commits.pop()
        case "bad_hash":
            commits[-1]["Hash"] = "wrong"
        case "shedding":
            row.update(http_status=429, outcome="REJECTED")
        case "other_4xx":
            row.update(http_status=403, outcome="REJECTED")
        case "server_error":
            row.update(http_status=500, outcome="UNCERTAIN")
        case "malformed":
            row.update(outcome="UNCERTAIN", response=None)
        case "transport":
            row.update(http_status=0, outcome="UNCERTAIN", error="TCP reset")
    _, result = sender(plan, journal, commits)
    assert not result["valid"]
    assert result["problems"][key] == problem


def test_timeout_with_commit_stays_in_transport_report(delivery):
    plan, journal, commits = delivery
    assert sender(*delivery)[1]["valid"]
    journal["status"] = "FAIL"
    journal["rows"][0].update(outcome="UNCERTAIN", http_status=0, error="context deadline exceeded")
    result = sender(plan, journal, commits)[1]
    assert result["valid"]
    assert result["stock_status"] == "FAIL"
    assert result["transport"] == {"accepted": 1, "timeout": 1}
    assert result["committed_timeout_keys"] == [commits[0]["Key"]]


@pytest.fixture
def gate_input():
    rows = [
        dict(
            workflow_id=str(i),
            scheduled_ns=i * SECOND,
            terminal_ns=i * SECOND + 1,
            terminal_state="SUCCEEDED",
            status="observed",
            raw_end_to_end_ns=1,
        )
        for i in range(24)
    ]
    samples = [dict(observed_at=ts(i), outstanding=1, unpublished=1) for i in range(0, 24, 2)]
    return rows, samples, 0, 24 * SECOND, True


def test_pooled_gate_allows_one_bad_bin(gate_input):
    rows, *_ = gate_input
    rows[4]["terminal_ns"] = 8 * SECOND
    result = bins_gate(*gate_input)
    assert result["bins"][1]["completions"] == 3
    assert result["pooled_completions"] == result["pooled_arrivals"] == 20
    assert result["checks"] == {"4a": True, "4b": True, "4c": True}
    rows[4]["terminal_ns"] = rows[5]["terminal_ns"] = 24 * SECOND
    assert bins_gate(*gate_input)["checks"]["4a"] is False


@pytest.mark.parametrize("lost", [1, 2])
def test_checked_censoring_counts_in_planned_denominator_and_cannot_pass(gate_input, lost):
    rows, samples, *_ = gate_input
    for row in rows[-lost:]:
        row.update(status="censored", terminal_state=None, admitted=True)
        del row["terminal_ns"], row["raw_end_to_end_ns"]
    assert bins_gate(*gate_input)["status"] == "invalid_gate_inputs"
    result = bins_gate(*gate_input, checked_expiry=True)
    assert result["pooled_arrivals"] == 20 and result["pooled_completions"] == 20 - lost
    assert result["checks"] == {"4a": lost == 1, "4b": None, "4c": True}
    assert result["status"] == ("unevaluable_gate" if lost == 1 else "failed_gate")
    for sample in samples[-2:]:
        sample["outstanding"] = 9
    for sample in samples[-4:-2]:
        sample["outstanding"] = 3
    result = bins_gate(*gate_input, checked_expiry=True)
    assert result["checks"]["4c"] is False and result["sustained_growth"]
    rows[-1]["admitted"] = False
    assert bins_gate(*gate_input, checked_expiry=True)["status"] == "invalid_gate_inputs"


def test_drift_backlog_and_strict_growth(gate_input):
    rows, samples, *_ = gate_input
    for row in rows[-8:]:
        row["raw_end_to_end_ns"] = SECOND // 2 + 2
    for i, sample in enumerate(samples):
        sample["outstanding"] = [1, 1, 1, 3, 4, 8][i // 2]
    result = bins_gate(*gate_input)
    assert result["checks"] == {"4a": True, "4b": False, "4c": False}
    assert result["sustained_growth"] is True
    for sample in samples[-2:]:
        sample["outstanding"] = 7
    assert bins_gate(*gate_input)["sustained_growth"] is False
    for sample in samples[-4:-2]:
        sample["outstanding"] = 3
    for sample in samples[-2:]:
        sample["outstanding"] = 8
    assert bins_gate(*gate_input)["sustained_growth"] is False


def test_unpublished_bound_and_missing_samples(gate_input):
    rows, samples, start, end, _ = gate_input
    for sample in samples[-2:]:
        sample["unpublished"] = 21
    assert bins_gate(*gate_input)["checks"]["4c"] is True
    samples[-1]["unpublished"] = 23
    assert bins_gate(*gate_input)["checks"]["4c"] is False
    assert (
        bins_gate(rows, samples[:1] + samples[3:], start, end, True)["status"]
        == "invalid_gate_inputs"
    )
    assert bins_gate(rows, samples, start, end, False)["status"] == "sender_invalid"


def test_gate_rejects_duplicate_invalid_and_empty_bin_samples(gate_input):
    rows, samples, start, end, _ = gate_input
    assert (
        bins_gate(rows, [samples[0], *samples], start, end, True)["status"] == "invalid_gate_inputs"
    )
    assert (
        bins_gate(rows, samples[:2] + samples[4:], start, end, True)["status"]
        == "invalid_gate_inputs"
    )
    samples[3]["outstanding"] = -1
    assert bins_gate(*gate_input)["status"] == "invalid_gate_inputs"


def test_sender_refuses_unexpected_or_duplicate_identity(delivery):
    plan, journal, commits = delivery
    commits.append(dict(commits[0], Key="extra", WorkflowID="extra"))
    assert sender(*delivery)[1]["unexpected_keys"] == ["extra"]
    assert not sender(*delivery)[1]["valid"]
    journal["rows"].append(journal["rows"][0])
    assert (
        sender(plan, journal, commits)[1]["problems"][commits[0]["Key"]]
        == "missing_or_duplicate_journal"
    )


def test_nanosecond_precision():
    assert utc_ns(ts(3, 987654321)) == 3_987_654_321
