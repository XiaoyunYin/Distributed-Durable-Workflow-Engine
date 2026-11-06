"""Offline disclosure only: derive engine-path sensitivity from retained evidence."""

from __future__ import annotations

import json
import math
import statistics
from datetime import datetime
from pathlib import Path
from typing import Any

from dur053_k3_policy import require


def timestamp(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def percentile(values: list[float], fraction: float) -> float:
    require(bool(values), "pilot-engine-path-population")
    return sorted(values)[math.ceil(fraction * len(values)) - 1]


def saturation_triggers(metrics: dict[str, Any]) -> list[str]:
    triggers = []
    if (
        min(
            metrics["completion_throughput_first_to_last"],
            metrics["completion_throughput_arrival_span"],
        )
        < 0.9 * metrics["offered_rate"]
    ):
        triggers.append("throughput")
    if metrics["warmup_outbox_unpublished_at_start"] > 0:
        triggers.append("warmup_backlog")
    quartiles = metrics["latency_quartile_medians_seconds"]
    if quartiles[-1] > 1.5 * quartiles[0]:
        triggers.append("latency_drift")
    return triggers


def engine_path_metrics(directory: Path) -> dict[str, Any]:
    """Caller supplies a fully verified materialised cell; never used for execution."""
    snapshot = json.loads((directory / "snapshot.json").read_bytes())["Snapshot"]
    arrivals = json.loads((directory / "measured-arrivals.json").read_bytes())
    warmup = json.loads((directory / "warmup-arrivals.json").read_bytes())
    from dur053_pilot import witness_events

    events = witness_events(json.loads((directory / "witness.json").read_bytes()))
    rows = sorted(arrivals["rows"], key=lambda r: (timestamp(r["scheduled_at"]), r["workflow_id"]))
    require(len(rows) >= 4, "pilot-engine-path-population")
    ids = {r["workflow_id"] for r in rows}
    warm_ids = {r["workflow_id"] for r in warmup["rows"]}
    history: dict[str, list[dict[str, Any]]] = {key: [] for key in ids}
    for row in snapshot["engine.transition_history"]["rows"]:
        if row["workflow_id"] in ids:
            history[row["workflow_id"]].append(row)
    terminals = {r["workflow_id"]: r for r in snapshot["engine.workflow_executions"]["rows"]}
    starts: dict[str, list[dict[str, Any]]] = {key: [] for key in ids}
    for event in events:
        if event["event"] == "start" and event["workflow_id"] in ids:
            starts[event["workflow_id"]].append(event)
    latencies, completions, phases = [], [], []
    for row in rows:
        key = row["workflow_id"]
        records = history[key]
        created = [h for h in records if h["reason"] == "ATTEMPT_CREATED"]
        result = [h for h in records if h["reason"] == "ATTEMPT_RESULT_RECORDED"]
        final = [
            h
            for h in records
            if h["new_state"] == "SUCCEEDED" and h["revision"] == terminals[key]["revision"]
        ]
        require(
            len(created) == len(result) == len(final) == len(starts[key]) == 1,
            "pilot-engine-path-unique-phases",
        )
        witness_start = starts[key][0]
        require(
            all(
                created[0][field] == result[0][field] == witness_start[field]
                for field in ("node_id", "iteration", "attempt_number")
            ),
            "pilot-engine-path-phase-identity",
        )
        points = [
            timestamp(row["scheduled_at"]),
            timestamp(created[0]["created_at"]),
            timestamp(witness_start["timestamp"]),
            timestamp(result[0]["created_at"]),
            timestamp(final[0]["created_at"]),
        ]
        require(points == sorted(points), "pilot-engine-path-phase-order")
        phases.append([b - a for a, b in zip(points, points[1:], strict=False)])
        latencies.append(points[-1] - points[0])
        completions.append(points[-1])
    first, last = min(completions), max(completions)
    start, end = timestamp(rows[0]["scheduled_at"]), timestamp(rows[-1]["scheduled_at"])
    require(last > first and end > start, "pilot-engine-path-span")
    outbox = snapshot["engine.outbox"]["rows"]
    measured_outbox = [r for r in outbox if r["workflow_id"] in ids]
    require(bool(measured_outbox), "pilot-engine-path-outbox-required")
    lags = [
        timestamp(r["published_at"]) - timestamp(r["created_at"])
        for r in measured_outbox
        if r["published_at"] is not None
    ]
    require(all(lag >= 0 for lag in lags), "pilot-engine-path-publish-order")
    quartiles = [
        statistics.median(latencies[i * len(rows) // 4 : (i + 1) * len(rows) // 4])
        for i in range(4)
    ]
    p99_index = sorted(range(len(rows)), key=lambda i: (latencies[i], i))[
        math.ceil(0.99 * len(rows)) - 1
    ]
    labels = [
        "scheduled_to_attempt_created",
        "attempt_created_to_witness_start",
        "witness_start_to_result_recorded",
        "result_recorded_to_succeeded",
    ]
    metrics = {
        "offered_rate": arrivals["rate"],
        "completion_throughput_first_to_last": (len(rows) - 1) / (last - first),
        "completion_throughput_arrival_span": sum(start <= t <= end for t in completions)
        / (end - start),
        "first_completion_delay_seconds": first - start,
        "warmup_outbox_unpublished_at_start": sum(
            r["workflow_id"] in warm_ids
            and timestamp(r["created_at"]) <= start
            and (r["published_at"] is None or timestamp(r["published_at"]) > start)
            for r in outbox
        ),
        "measured_outbox_publish_lag_seconds": {
            "p50": percentile(lags, 0.5) if lags else None,
            "p99": percentile(lags, 0.99) if lags else None,
            "max": max(lags) if lags else None,
            "published_count": len(lags),
            "unpublished_count": len(measured_outbox) - len(lags),
        },
        "latency_quartile_medians_seconds": quartiles,
        "p99_latency_workflow": rows[p99_index]["workflow_id"],
        "p99_latency_seconds": latencies[p99_index],
        "p99_latency_phase_seconds": dict(zip(labels, phases[p99_index], strict=True)),
        "individual_phase_p99_seconds": {
            label: percentile([phase[i] for phase in phases], 0.99)
            for i, label in enumerate(labels)
        },
    }
    metrics["triggers"] = saturation_triggers(metrics)
    return metrics
