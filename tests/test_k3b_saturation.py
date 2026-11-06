from __future__ import annotations

import base64
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


def test_reduced_real_shaped_snapshot(tmp_path: Path) -> None:
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
    result = engine_path_metrics(tmp_path)
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
