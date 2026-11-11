from __future__ import annotations

import base64
import copy
import hashlib
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from dur053_k3 import save  # noqa: E402
from dur053_saturation import engine_path_metrics, saturation_triggers  # noqa: E402


def baseline() -> dict:
    return {
        "offered_rate": 10,
        "completion_throughput_first_to_last": 10,
        "completion_throughput_arrival_span": 10,
        "warmup_outbox_unpublished_at_start": 0,
        "latency_quartile_medians_seconds": [2, 2, 2, 2],
    }


@pytest.mark.parametrize(
    ("field", "value", "trigger"),
    [
        ("completion_throughput_first_to_last", 8.999, "throughput"),
        ("completion_throughput_arrival_span", 8.999, "throughput"),
        ("warmup_outbox_unpublished_at_start", 1, "warmup_backlog"),
        ("latency_quartile_medians_seconds", [2, 2, 2, 3.001], "latency_drift"),
    ],
)
def test_individual_triggers(field: str, value: object, trigger: str) -> None:
    metrics = baseline()
    metrics[field] = value
    assert saturation_triggers(metrics) == [trigger], "engine saturation trigger bypassed"


def test_boundaries() -> None:
    metrics = baseline()
    assert saturation_triggers(metrics) == []
    metrics.update(
        completion_throughput_first_to_last=9,
        completion_throughput_arrival_span=9,
        latency_quartile_medians_seconds=[2, 2, 2, 3],
    )
    assert saturation_triggers(metrics) == [], "strict engine saturation boundary changed"
    metrics.update(
        completion_throughput_first_to_last=8,
        warmup_outbox_unpublished_at_start=1,
        latency_quartile_medians_seconds=[2, 2, 2, 4],
    )
    assert saturation_triggers(metrics) == ["throughput", "warmup_backlog", "latency_drift"]


def reduced_snapshot(tmp_path: Path) -> None:
    """Independent four-workflow oracle, using real snapshot/witness field shapes."""
    epoch = datetime(2026, 11, 6, tzinfo=UTC)

    def at(seconds: float) -> str:
        return (epoch + timedelta(seconds=seconds)).isoformat()

    rows = [{"workflow_id": f"w{i}", "scheduled_at": at(i)} for i in range(4)]
    save(tmp_path / "measured-arrivals.json", {"rate": 1, "rows": rows})
    save(tmp_path / "warmup-arrivals.json", {"rows": [{"workflow_id": "warm"}]})
    history, workflows, outbox = [], [], []
    events = [{"event": "session", "sequence": 1, "session": "session", "pod_uid": "p1"}]
    # Latencies 2, 4, 6, 8; completions 2, 5, 8, 11; phases 0.5, varying, 0.5, 0.5.
    for i, row in enumerate(rows):
        key = row["workflow_id"]
        for reason, time, state, revision in [
            ("ATTEMPT_CREATED", i + 0.5, "WAITING_ACTIVITY", 3),
            ("ATTEMPT_RESULT_RECORDED", 3 * i + 1.5, "WAITING_ACTIVITY", 4),
            ("GRAPH_ADVANCED", 3 * i + 2, "SUCCEEDED", 7),
        ]:
            history.append(
                {
                    "workflow_id": key,
                    "created_at": at(time),
                    "reason": reason,
                    "new_state": state,
                    "revision": revision,
                    "node_id": "dur048.sleep",
                    "iteration": 0,
                    "attempt_number": 1,
                }
            )
        workflows.append({"workflow_id": key, "revision": 7})
        events.append(
            {
                "event": "start",
                "sequence": len(events) + 1,
                "session": "session",
                "pod_uid": "p1",
                "workflow_id": key,
                "node_id": "dur048.sleep",
                "iteration": 0,
                "attempt_number": 1,
                "timestamp": at(3 * i + 1),
            }
        )
        outbox.append(
            {
                "workflow_id": key,
                "created_at": at(i),
                "published_at": at(i + i + 1) if i < 3 else None,
            }
        )
    outbox.extend(
        [
            {"workflow_id": "warm", "created_at": at(-2), "published_at": at(0.1)},
            {"workflow_id": "warm", "created_at": at(-2), "published_at": None},
            {"workflow_id": "warm", "created_at": at(-2), "published_at": at(0)},
            {"workflow_id": "warm", "created_at": at(1), "published_at": None},
        ]
    )
    save(
        tmp_path / "snapshot.json",
        {
            "Snapshot": {
                "engine.transition_history": {"rows": history},
                "engine.workflow_executions": {"rows": workflows},
                "engine.outbox": {"rows": outbox},
            }
        },
    )
    second = [{"event": "session", "sequence": 1, "session": "other", "pod_uid": "p2"}]
    ledgers = []
    for pod, data in [("p1", events), ("p2", second)]:
        raw = ("\n".join(json.dumps(e) for e in data) + "\n").encode()
        ledgers.append(
            {
                "inventory": {
                    "pod_uid": pod,
                    "session": data[0]["session"],
                    "complete": True,
                    "errors": 0,
                    "attempted": len(data),
                    "written": len(data),
                    "sha256": hashlib.sha256(raw).hexdigest(),
                },
                "data": base64.b64encode(raw).decode(),
            }
        )
    save(tmp_path / "witness.json", {"expected_pods": ["p1", "p2"], "ledgers": ledgers})


def test_reduced_real_shaped_snapshot(tmp_path: Path) -> None:
    reduced_snapshot(tmp_path)
    result = engine_path_metrics(tmp_path)
    assert result["multi_attempt_workflows"] == 0
    assert result["max_attempts"] == 1
    assert result["offered_rate"] == 1
    assert result["completion_throughput_first_to_last"] == pytest.approx(1 / 3)
    assert result["completion_throughput_arrival_span"] == pytest.approx(1 / 3)
    assert result["first_completion_delay_seconds"] == 2
    assert result["warmup_outbox_unpublished_at_start"] == 2
    assert result["measured_outbox_publish_lag_seconds"] == {
        "p50": 2,
        "p99": 3,
        "max": 3,
        "published_count": 3,
        "unpublished_count": 1,
    }
    assert result["latency_quartile_medians_seconds"] == [2, 4, 6, 8]
    assert result["p99_latency_workflow"] == "w3"
    assert result["p99_latency_seconds"] == 8
    assert list(result["p99_latency_phase_seconds"].values()) == [0.5, 6.5, 0.5, 0.5]
    assert list(result["individual_phase_p99_seconds"].values()) == [0.5, 6.5, 0.5, 0.5]
    assert result["triggers"] == ["throughput", "warmup_backlog", "latency_drift"]


def rewrite_witness(directory: Path, events: list[dict]) -> None:
    export = json.loads((directory / "witness.json").read_bytes())
    for i, event in enumerate(events):
        event["sequence"] = i + 1
    raw = ("\n".join(json.dumps(e) for e in events) + "\n").encode()
    ledger = export["ledgers"][0]
    ledger["data"] = base64.b64encode(raw).decode()
    ledger["inventory"].update(
        sha256=hashlib.sha256(raw).hexdigest(), attempted=len(events), written=len(events)
    )
    save(directory / "witness.json", export)


def two_attempt_snapshot(directory: Path) -> None:
    reduced_snapshot(directory)
    path = directory / "snapshot.json"
    snapshot = json.loads(path.read_bytes())
    history = snapshot["Snapshot"]["engine.transition_history"]["rows"]
    winning = next(
        h for h in history if h["workflow_id"] == "w3" and h["reason"] == "ATTEMPT_CREATED"
    )
    losing_created = dict(winning, revision=1, created_at="2026-11-06T00:00:03.100000+00:00")
    losing_result = dict(
        losing_created,
        revision=2,
        reason="ATTEMPT_RESULT_RECORDED",
        created_at="2026-11-06T00:00:03.300000+00:00",
    )
    history.extend([losing_created, losing_result])
    for h in history:
        if h["workflow_id"] == "w3" and h["revision"] in (3, 4):
            h["attempt_number"] = 2
    winning["created_at"] = "2026-11-06T00:00:03.750000+00:00"
    # A post-terminal result and arbitrary storage order must not choose a winner.
    history.append(dict(losing_result, revision=8))
    history.reverse()
    save(path, snapshot)
    export = json.loads((directory / "witness.json").read_bytes())
    events = [
        json.loads(line) for line in base64.b64decode(export["ledgers"][0]["data"]).splitlines()
    ]
    for e in events[1:]:
        e.update(
            invocation_id=e["workflow_id"], worker_id="worker", claim_token="token", monotonic_ns=0
        )
        if e["workflow_id"] == "w3":
            e.update(attempt_number=2, timestamp="2026-11-06T00:00:09.750000+00:00")
    loser = dict(
        events[-1],
        invocation_id="loser",
        attempt_number=1,
        timestamp="2026-11-06T00:00:03.200000+00:00",
    )
    events.append(loser)
    for e in list(events[1:]):
        events.append(dict(e, event="end", outcome="SUCCEEDED", monotonic_ns=100000000))
    rewrite_witness(directory, events)


def test_winning_attempt_phases(tmp_path: Path) -> None:
    two_attempt_snapshot(tmp_path)
    # Both invocations are successful and witnessed, as pilot acceptance permits.
    from dur053_pilot import busy_times

    assert len(busy_times(json.loads((tmp_path / "witness.json").read_bytes()), {"w3"})) == 2
    try:
        metrics = engine_path_metrics(tmp_path)
    except ValueError as exc:
        pytest.fail(f"winning attempt match bypassed: {exc}")
    assert metrics["multi_attempt_workflows"] == 1
    assert metrics["max_attempts"] == 2
    assert metrics["p99_latency_workflow"] == "w3"
    assert metrics["p99_latency_seconds"] == 8
    assert list(metrics["p99_latency_phase_seconds"].values()) == [0.75, 6, 0.75, 0.5], (
        "winning attempt match bypassed"
    )
    assert list(metrics["individual_phase_p99_seconds"].values()) == [0.75, 6, 0.75, 0.5]
    # Attempt counts cannot become a fourth overload trigger.
    clean = baseline() | {"multi_attempt_workflows": 1, "max_attempts": 2}
    assert saturation_triggers(clean) == []
    assert saturation_triggers(metrics) == ["throughput", "warmup_backlog", "latency_drift"]


@pytest.mark.parametrize(
    "ambiguity", ["final", "result", "missing_result", "created", "start", "missing_start"]
)
def test_ambiguous_winner_refuses(tmp_path: Path, ambiguity: str) -> None:
    two_attempt_snapshot(tmp_path)
    path = tmp_path / "snapshot.json"
    snapshot = json.loads(path.read_bytes())
    history = snapshot["Snapshot"]["engine.transition_history"]["rows"]
    if ambiguity in {"final", "result", "missing_result", "created"}:
        revision = {"final": 7, "result": 4, "missing_result": 4, "created": 3}[ambiguity]
        target = next(h for h in history if h["workflow_id"] == "w3" and h["revision"] == revision)
        if ambiguity == "missing_result":
            history[:] = [
                h
                for h in history
                if h["workflow_id"] != "w3" or h["reason"] != "ATTEMPT_RESULT_RECORDED"
            ]
        else:
            history.append(copy.deepcopy(target))
        save(path, snapshot)
    else:
        export = json.loads((tmp_path / "witness.json").read_bytes())
        events = [
            json.loads(line) for line in base64.b64decode(export["ledgers"][0]["data"]).splitlines()
        ]
        target = next(
            e
            for e in events
            if e["event"] == "start" and e["workflow_id"] == "w3" and e["attempt_number"] == 2
        )
        if ambiguity == "start":
            events.append(dict(target, invocation_id="duplicate"))
        else:
            events.remove(target)
        rewrite_witness(tmp_path, events)
    guard = (
        "pilot-engine-path-unique-phases"
        if ambiguity in {"final", "result", "missing_result"}
        else "pilot-engine-path-phase-identity"
    )
    with pytest.raises(ValueError, match=guard):
        engine_path_metrics(tmp_path)
