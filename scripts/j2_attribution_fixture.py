"""Synthetic raw attribution comparisons for the archive-only engineering job."""

import base64
import json

from j2_attribution import compare, observe_bins
from j2_barrier import digest
from j2_offline import SECOND, bins_gate
from j2_qualification_evidence import require, write
from j2_schedule import timestamp

ORIGIN = 1_800_000_000 * SECOND
STOP = ORIGIN + 60 * SECOND


def build(root, *, failing=False, missing_broker=False, conflict=False):
    root.mkdir(parents=True)
    write(root / "schedule.json", {"warmup_end_ns": ORIGIN, "stop_ns": STOP})
    pods = [f"fixture-{i}" for i in range(2)]
    ledgers = []
    for pod in pods:
        raw = (
            json.dumps({"event": "session", "sequence": 1, "pod_uid": pod, "session": pod}) + "\n"
        ).encode()
        ledgers.append(
            {
                "data": base64.b64encode(raw).decode(),
                "inventory": {
                    "pod_uid": pod,
                    "session": pod,
                    "complete": True,
                    "errors": 0,
                    "attempted": 1,
                    "written": 1,
                    "sha256": digest(raw),
                },
            }
        )
    write(root / "witness.json", {"expected_pods": pods, "ledgers": ledgers})
    write(
        root / "outbox-publications.json",
        {
            "start_unix_ns": ORIGIN,
            "end_exclusive_unix_ns": STOP,
            "published_rows_by_second": [4 if failing else 8] * 60,
        },
    )
    write(
        root / "relay-attribution.json",
        {"logs": [{"role": role, "exit": 0} for role in ("relay", "scheduler")]},
    )
    offsets = "\n".join(f"workers durable-agent.tasks.v1 {part} 5 5 0" for part in range(8))
    rows = []
    for offset in range(2, 60, 5):
        point = ORIGIN + offset * SECOND

        def bracket(value, point=point):
            return {"lower_unix_ns": point, "upper_unix_ns": point + 1, "value": value}

        row = {
            "unix_ns": point,
            "errors": {},
            "postgres": bracket({"pg_locks": [{"granted": not conflict}]}),
            "kafka_groups": bracket(offsets),
            "worker_partition_state": bracket({"paused_with_busy_consumer": False, "runnable": 0}),
            "postgres_pool": bracket(
                {
                    "in_use": 4 if conflict else 1,
                    "capacity": 4,
                    "acquisition_wait_ns": 1 if conflict else 0,
                    "transaction_latency_ns": 10 if conflict else 1,
                    "io_wait_ns": 5 if conflict else 0,
                }
            ),
            "runner": bracket({"stat": "cpu 10 0 0 90", "loadavg": "0.01 0.01 0.01 1/30 1"}),
            "container_cpu": bracket(
                {
                    "runnable": 1,
                    "available_cores": 4,
                    "pressure_delta_usec": 0,
                    "container_throttled_usec": [0, 0],
                }
            ),
        }
        if not missing_broker:
            row["broker_timings"] = bracket({"produce_latency_ns": 100, "fetch_latency_ns": 100})
        rows.append(row)
    (root / "attribution-samples.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )
    for name in ("relay", "observer"):
        (root / f"{name}-resources.tsv").write_text(
            "".join(
                f"{ORIGIN + second * SECOND}\tcpu.stat\tok\tusage_usec {second};throttled_usec 0\n"
                for second in range(60)
            ),
            encoding="utf-8",
        )
    arrivals = [
        {
            "workflow_id": str(i),
            "scheduled_ns": ORIGIN + i * SECOND,
            "terminal_ns": ORIGIN + i * SECOND + 1,
            "terminal_state": "SUCCEEDED",
            "raw_end_to_end_ns": 1,
            "status": "observed",
        }
        for i in range(60)
    ]
    samples = [
        {
            "observed_at": timestamp(ORIGIN + i * SECOND),
            "outstanding": (i // 10) * 4 if failing else 0,
            "unpublished": (i // 10) * 10 if failing else 0,
        }
        for i in range(60)
    ]
    gate = bins_gate(arrivals, samples, ORIGIN, STOP, True)
    return observe_bins(root, {"cohorts": {"grid": {"gate": gate}}})


def fixture_report(root):
    root.mkdir()
    base = build(root / "last-pass")
    fail = build(root / "first-fail", failing=True)
    missing = build(root / "missing-broker", failing=True, missing_broker=True)
    conflict = build(root / "conflict", failing=True, conflict=True)
    entries = {
        "supported": compare(base, fail),
        "missing_discriminator": compare(base, missing),
        "conflicting": compare(base, conflict),
        "drain_only": compare(base, base),
    }
    require(
        entries["supported"]["component"] == "relay_publication"
        and all(
            entries[key]["component"] == "unresolved"
            for key in ("missing_discriminator", "conflicting", "drain_only")
        ),
        "paired-bin raw-to-report fixture differs",
    )
    result = {"scope": "synthetic engineering attribution fixture", "cases": entries}
    write(root / "fixture-report.json", result)
    return result
