"""Reduce paired final-bin attribution evidence without filling missing discriminators."""

import json

from dur053_pilot import witness_events
from j2_offline import SECOND, utc_ns
from j2_qualification_evidence import read
from j2_schedule import RATES, schedule_slots

TASKS = "durable-agent.tasks.v1"
DISCRIMINATORS = ("worker_slots", "postgres", "kafka", "host_cpu")


def _read_lines(path):
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_bytes().splitlines()]


def _lag(raw):
    values = {}
    for line in raw.splitlines():
        fields = line.split()
        if len(fields) >= 6 and fields[1] == TASKS and fields[2].isdigit():
            partition = int(fields[2])
            current, end = (int(v) if v.isdigit() else None for v in fields[3:5])
            if partition in values or current is None or end is None or current > end:
                return None
            values[partition] = {
                "current": current,
                "end": end,
                "lag": end - current,
                "assignment": dict(
                    zip(("consumer_id", "host", "client_id"), fields[6:9], strict=False)
                ),
            }
    return values if set(values) == set(range(8)) else None


def _values(samples, source, lo, hi):
    for sample in samples:
        item = sample.get(source)
        if (
            isinstance(item, dict)
            and lo <= item["lower_unix_ns"] <= item["upper_unix_ns"] < hi
            and source not in sample.get("errors", {})
        ):
            yield item["value"]


def _counter_rows(path):
    if not path.is_file():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        at, name, status, text = line.split("\t", 3)
        if name == "cpu.stat" and status == "ok":
            rows.append(
                (int(at), dict((k, int(v)) for k, v in (s.split() for s in text.split(";"))))
            )
    return rows


def _deltas(rows, lo, hi):
    inside = [row for at, row in rows if lo <= at < hi]
    if len(inside) < 2:
        return None
    counters = ("usage_usec", "throttled_usec")
    delta = {key: inside[-1][key] - inside[0][key] for key in counters}
    return delta if min(delta.values()) >= 0 else None


def _pressure_rows(path):
    if not path.is_file():
        return []
    samples = []
    with path.open(encoding="utf-8") as source:
        for line in source:
            at, name, status, text = line.rstrip("\n").split("\t", 3)
            if name in {"io.stat", "io.pressure", "cpu.pressure"} and status == "ok":
                samples.append((int(at), name, text))
    return samples


def _sender_samples(root, schedule, edges):
    path = root / "sender-report.json"
    if not path.is_file():
        return [None] * (len(edges) - 1)
    sender = read(path)
    times = [at for at, _ in schedule_slots(schedule)]
    samples = [[] for _ in edges[1:]]
    for row in sender["sender_samples"]:
        at = times[int(row["key"].rsplit("-", 1)[1])]
        for i, (lo, hi) in enumerate(zip(edges, edges[1:], strict=False)):
            if lo <= at < hi:
                samples[i].append(row)
                break
    return [
        {
            "peak_in_flight": max((row["in_flight_from_journal"] for row in group), default=0),
            "peak_intent_lateness_ns": max(
                (
                    row["recorded_intent_lateness_ns"]
                    for row in group
                    if row["phase"] == "intent" or "dispatch_intent_ns" in row
                ),
                default=None,
            ),
            "unsent_records": sum(row["outcome"] == "unsent" for row in group),
            "concurrency_cap": schedule["cap"],
            **(
                {
                    "peak_request_write_lateness_ns": max(
                        (
                            row["request_write_lateness_ns"]
                            for row in group
                            if row["request_write_lateness_ns"] is not None
                        ),
                        default=None,
                    ),
                }
                if sender.get("schema")
                else {}
            ),
        }
        for group in samples
    ]


def _published(timeline, lo, hi):
    origin, end = timeline.get("start_unix_ns", hi), timeline.get("end_exclusive_unix_ns", lo)
    if not (origin <= lo < hi <= end):
        return None
    values = timeline["published_rows_by_second"]
    i, j = (lo - origin) // SECOND, (hi - origin) // SECOND
    return sum(values[i:j]) if 0 <= i < j <= len(values) else None


def _worker_intervals(root):
    path = root / "witness.json"
    if not path.is_file():
        return None
    events = witness_events(read(path))
    starts = {e["invocation_id"]: e for e in events if e["event"] == "start"}
    ends = {e["invocation_id"]: e for e in events if e["event"] == "end"}
    if starts.keys() != ends.keys() or len(events) != len(starts) + len(ends):
        return None
    return [(utc_ns(e["timestamp"]), utc_ns(ends[key]["timestamp"])) for key, e in starts.items()]


def observe_bins(root, episode):
    schedule = read(root / "schedule.json")
    samples = _read_lines(root / "attribution-samples.jsonl")
    relay = _counter_rows(root / "relay-resources.tsv")
    observer = _counter_rows(root / "observer-resources.tsv")
    pressure = _pressure_rows(root / "observer-resources.tsv")
    path = root / "outbox-publications.json"
    timeline = read(path) if path.is_file() else {}
    log_path = root / "relay-attribution.json"
    log_status = read(log_path) if log_path.is_file() else {}
    logs_clear = all(
        any(
            entry.get("role") == role and entry.get("exit") == 0 and "error" not in entry
            for entry in log_status.get("logs", [])
        )
        for role in ("relay", "scheduler")
    )
    intervals = _worker_intervals(root)
    start, stop = schedule["warmup_end_ns"], schedule["stop_ns"]
    edges = [start + (stop - start) * i // 6 for i in range(3, 7)]
    sender = _sender_samples(root, schedule, edges)
    gate = episode["cohorts"]["grid"]["gate"]
    rows = []
    for i, (lo, hi) in enumerate(zip(edges, edges[1:], strict=False), 3):
        pg = list(_values(samples, "postgres", lo, hi))
        lag = [_lag(value) for value in _values(samples, "kafka_groups", lo, hi)]
        lag = [value for value in lag if value is not None]
        raw_host = list(_values(samples, "runner", lo, hi))
        worker = list(_values(samples, "worker_partition_state", lo, hi))
        pool = list(_values(samples, "postgres_pool", lo, hi))
        broker = list(_values(samples, "broker_timings", lo, hi))
        cpu = list(_values(samples, "container_cpu", lo, hi))
        counters = {"relay": _deltas(relay, lo, hi), "observer": _deltas(observer, lo, hi)}
        busy = (
            None
            if intervals is None
            else sum(max(0, min(hi, b) - max(lo, a)) for a, b in intervals)
        )
        slots = None if busy is None else busy / (4 * (hi - lo))
        partition_lag = [max(row["lag"] for row in group.values()) for group in lag]
        blockers = [
            sum(lock.get("granted") is False for lock in db["pg_locks"])
            for db in pg
            if isinstance(db.get("pg_locks"), list)
        ]
        published = _published(timeline, lo, hi)
        rows.append(
            {
                "bin": i + 1,
                "window_unix_ns": [lo, hi],
                "outstanding": gate["bins"][i]["outstanding"],
                "unpublished": gate["bins"][i]["unpublished"],
                "worker_slots": {"busy_fraction": slots, "partition_state": worker, "lag": lag},
                "postgres": {"pool": pool, "blocked_locks": blockers, "activity": pg},
                "kafka": {
                    "broker": broker,
                    "maximum_task_lag": max(partition_lag, default=None),
                    "published": published,
                    "relay_logs_complete": logs_clear,
                    "relay_cpu": counters["relay"],
                },
                "host_cpu": {"runner": raw_host, "containers": cpu, "cgroup_cpu": counters},
                "observer_resource_samples": [row for row in pressure if lo <= row[0] < hi],
                "sender": sender[i - 3],
            }
        )
    return rows


def _signature(row, baseline):
    """Return explicit binding evidence and missing discriminators for one paired bin."""
    worker, pg, kafka, host = (row[key] for key in DISCRIMINATORS)
    old_kafka = baseline["kafka"]
    missing = []
    bound = []
    if worker["busy_fraction"] is None or not worker["partition_state"] or not worker["lag"]:
        missing.append("worker slot/partition state")
    elif any(
        item["paused_with_busy_consumer"] and item["runnable"] > 0
        for item in worker["partition_state"]
    ):
        bound.append("worker_slots")
    if (
        not pg["pool"]
        or not baseline["postgres"]["pool"]
        or not pg["blocked_locks"]
        or not pg["activity"]
    ):
        missing.append("DB pool waits/transaction and I/O measurements")
    elif any(
        point["acquisition_wait_ns"] > 0
        and point["in_use"] == point["capacity"]
        and point["transaction_latency_ns"]
        > max(p["transaction_latency_ns"] for p in baseline["postgres"]["pool"])
        and (point["io_wait_ns"] > 0 or any(pg["blocked_locks"]))
        for point in pg["pool"]
    ):
        bound.append("postgres")
    if (
        not kafka["broker"]
        or not old_kafka["broker"]
        or kafka["maximum_task_lag"] is None
        or kafka["published"] is None
        or kafka["relay_cpu"] is None
        or not kafka["relay_logs_complete"]
        or old_kafka["published"] is None
        or baseline["unpublished"] is None
        or row["unpublished"] is None
    ):
        missing.append("broker timings/offsets or relay trace")
    elif (
        any(
            point[key] > max(old[key] for old in old_kafka["broker"])
            for point in kafka["broker"]
            for key in ("produce_latency_ns", "fetch_latency_ns")
        )
        and kafka["maximum_task_lag"] > 0
    ):
        bound.append("kafka")
    elif (
        row["unpublished"] > baseline["unpublished"]
        and kafka["published"] <= old_kafka["published"]
        and kafka["maximum_task_lag"] == 0
    ):
        bound.append("relay_publication")
    if not host["runner"] or not host["containers"] or host["cgroup_cpu"]["observer"] is None:
        missing.append("host pressure and per-container CPU/quotas")
    elif any(
        point["runnable"] > point["available_cores"]
        and point["pressure_delta_usec"] > 0
        and sum(value > 0 for value in point["container_throttled_usec"]) != 1
        for point in host["containers"]
    ):
        bound.append("host_cpu")
    return bound, missing


def compare(passing, failing):
    if len(passing) != 3 or len(failing) != 3:
        return {"component": "unresolved", "reason": "missing paired final bins"}
    outstanding = [row["outstanding"] for row in failing]
    if not (
        all(value is not None for value in outstanding)
        and outstanding[0] < outstanding[1] < outstanding[2]
        and outstanding[2] - outstanding[0] > 4
    ):
        return {
            "component": "unresolved",
            "reason": "sustained measured growth was not established",
        }
    results = [_signature(after, before) for before, after in zip(passing, failing, strict=True)]
    missing = sorted({reason for _, gaps in results for reason in gaps})
    # Missing competing evidence from the reference cell is also disqualifying.
    missing = sorted(set(missing) | {reason for p in passing for reason in _signature(p, p)[1]})
    active = {item for signature, _ in results for item in signature}
    common = set.intersection(*(set(signature) for signature, _ in results))
    if common == {"relay_publication"}:
        pending = [row["unpublished"] for row in failing]
        if not pending[0] < pending[1] < pending[2]:
            return {
                "component": "unresolved",
                "reason": "relay backlog did not rise in the paired bins",
            }
    if missing or len(active) != 1 or active != common:
        return {
            "component": "unresolved",
            "missing": missing,
            "signatures": [row[0] for row in results],
        }
    return {"component": next(iter(common)), "signatures": [row[0] for row in results]}


def diagnostic_pair(boundary, observations):
    rate = boundary["saturation"]
    if rate is None:
        return {"component": "unresolved", "reason": "saturation unresolved within the grid"}
    order = tuple(RATES)
    lower = [
        item
        for item in order[: order.index(rate)]
        if item in boundary["stable_rates"] and item in observations
    ]
    before = lower[-1] if lower else None
    if before is None:
        return {"component": "unresolved", "reason": "no verified lower passing rate"}
    passing, failing = observations[before], observations[rate]
    return {
        "passing_rate": before,
        "failing_rate": rate,
        **compare(passing["bins"], failing["bins"]),
    }
