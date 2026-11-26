"""Check J3 qualification evidence independently of the serving rules."""

import json

from j2_offline import attempt_key, utc_ns
from j3_replay import ALERTS, DEADLINE, SECOND, require, unique


def notifications(rows, transitions, target, start_ns, end_ns):
    """Pair receiver deliveries to observed Prometheus transitions by incident."""
    require(len(rows) >= 2, "missing receiver heartbeats")
    first, last = rows[0], rows[-1]
    times = [row["received_ns"] for row in rows]
    require(
        first.get("heartbeat") == "start"
        and last.get("heartbeat") == "end"
        and times == sorted(times)
        and times[0] <= start_ns <= end_ns <= times[-1],
        "receiver evidence does not enclose the episode",
    )
    deliveries = []
    for row in rows[1:-1]:
        require(
            row.get("http_status") == 204
            and row.get("status") in {"firing", "resolved"}
            and row["alerts"],
            "invalid delivery",
        )
        for delivery in row["alerts"]:
            labels = delivery["labels"]
            require(
                all(labels.get(key) == value for key, value in target.items())
                and labels["alertname"] in ALERTS
                and bool(delivery["fingerprint"])
                and delivery["status"] in {"firing", "resolved"},
                "unexpected receiver labels, status or fingerprint",
            )
            deliveries.append({**delivery, "received_ns": row["received_ns"]})

    incidents = {}
    matched = []
    used = set()
    for transition in transitions:
        name, at = transition["alert"], transition["at_ns"]
        firing = transition["firing"]
        expected = "firing" if firing else "resolved"
        for i, d in enumerate(deliveries):
            received = d["received_ns"]
            if not (d["labels"]["alertname"] == name and d["status"] == expected and i not in used):
                continue
            if not at <= received <= at + 30 * SECOND:
                continue
            started = utc_ns(d["startsAt"])
            if firing:
                if started != at:
                    continue
                incidents[name] = (d["fingerprint"], started)
            elif (
                incidents.get(name) != (d["fingerprint"], started)
                or utc_ns(d["endsAt"]) < at
                or utc_ns(d["endsAt"]) > received
            ):
                continue
            used.add(i)
            matched.append({"transition": transition, "delivery": d})
            break
        else:
            raise ValueError(f"missing {expected} receiver delivery for {name} at {at}")
    allowed_repeats = {
        (d["labels"]["alertname"], d["status"], d["fingerprint"], d["startsAt"])
        for i, d in enumerate(deliveries)
        if i in used
    }
    require(
        all(
            (d["labels"]["alertname"], d["status"], d["fingerprint"], d["startsAt"])
            in allowed_repeats
            for d in deliveries
        ),
        "unexpected or unpaired receiver delivery",
    )
    return matched


def foreign_positive(witness, evaluations, seeds, foreign):
    at = witness["sample_ns"]
    require(witness["namespace"] == foreign, "wrong foreign witness scope")
    quiet = [r for r in evaluations if r["valid"] and r["sample_ns"] == at and not r["firing"]]
    require(quiet, "foreign witness lacks a simultaneous quiet target evaluation")
    all_seeds = unique(seeds, lambda row: row["workflow_id"])
    committed = {identity for identity, row in all_seeds.items() if row["commit_seen_ns"] <= at}
    seen = set(witness["admission_ids"])
    require(
        seen == committed and len(seen) == len(witness["admission_ids"]),
        "foreign witness differs from independently committed seeds",
    )
    require(
        0 < witness["claimable_attempts"] <= len(committed)
        and witness["oldest_claimable_age_ns"] > 28_540_000_000,
        "foreign claimable workload did not cross the age threshold",
    )
    windows = witness["windows"]
    require(set(windows) == {"2m", "10m"}, "missing foreign deadline window")
    for name, row in windows.items():
        require(
            100 <= row["eligible"] <= len(committed)
            and 0 <= row["bad"] <= row["eligible"]
            and row["bad"] * 125 > row["eligible"] * 18,
            f"foreign {name} did not cross the burn and population thresholds",
        )
    return {"foreign": witness, "quiet_target_evaluations": quiet}


def foreign_isolation(data, seeds, foreign):
    seeded = unique(seeds, lambda row: row["workflow_id"])
    require(seeded, "missing foreign seed receipts")
    workflows = unique(
        (r for r in data["workflows"] if r["namespace"] == foreign), lambda r: r["workflow_id"]
    )
    require(workflows.keys() == seeded.keys(), "foreign workflows differ from seed receipts")
    for identity, row in workflows.items():
        require(
            row["revision"] == seeded[identity]["revision"] == 1
            and row["state"] == "WAITING_ACTIVITY",
            f"foreign workflow changed: {identity}",
        )
    history = unique(
        (r for r in data["history"] if r["workflow_id"] in seeded), lambda r: r["workflow_id"]
    )
    attempts = unique(
        (r for r in data["attempts"] if r["workflow_id"] in seeded), lambda r: r["workflow_id"]
    )
    require(
        history.keys() == attempts.keys() == seeded.keys(), "foreign history or attempt missing"
    )
    for identity in seeded:
        a, h = attempts[identity], history[identity]
        require(
            h["revision"] == 1
            and h["reason"] == "J3_FOREIGN_SEED"
            and a["state"] == "DISPATCHABLE"
            and a["is_current"] is True
            and all(a[k] is None for k in ("token_hash", "heartbeat_deadline", "worker_id")),
            f"foreign history or attempt changed: {identity}",
        )
    require(not any(r["workflow_id"] in seeded for r in data["outbox"]), "foreign outbox creation")
    require(
        not any(r.get("workflow_id") in seeded for r in data["slot_events"]), "foreign slot use"
    )
    return {"seeded": len(seeded), "claims": 0, "revision_changes": 0, "outbox_rows": 0}


def target_only_attainment(data, namespace, width, end_ns):
    """Compute target counts directly from the independent durable tables."""
    plan = unique(
        (r for r in data["plan"] if r["namespace"] == namespace), lambda r: r["submission_key"]
    )
    workflows = unique(
        (r for r in data["workflows"] if r["namespace"] == namespace), lambda r: r["workflow_id"]
    )
    facts = unique(
        (r for r in data["history"] if r["workflow_id"] in workflows),
        lambda r: (r["workflow_id"], r["revision"]),
    )
    on_time = incomplete = 0
    for identity, row in workflows.items():
        deadline = plan[row["submission_key"]]["scheduled_ns"] + DEADLINE
        require(deadline <= end_ns, f"target-only deadline not matured: {identity}")
        if row["state"] not in {"SUCCEEDED", "FAILED", "CANCELED"}:
            incomplete += 1
            continue
        revision = facts[identity, row["revision"]]
        require(revision["new_state"] == row["state"], f"target-only history mismatch: {identity}")
        if row["state"] == "SUCCEEDED" and revision["created_ns"] + width <= deadline:
            on_time += 1
    eligible = len(workflows)
    return {
        "eligible": eligible,
        "on_time": on_time,
        "attainment": on_time / eligible if eligible else None,
        "incomplete": incomplete,
    }


def slot_occupancy(data, slots, target, *, exclude_foreign):
    """Join worker invocation intervals to durable attempt identities."""
    require(len(slots) == len(set(slots)) == 4, "expected four witnessed worker slots")
    scope = {row["workflow_id"]: row["namespace"] for row in data["workflows"]}
    if exclude_foreign:
        scope = {identity: ns for identity, ns in scope.items() if ns == target}
    attempts = unique(
        (row for row in data["attempts"] if row["workflow_id"] in scope and row["token_hash"]),
        attempt_key,
    )
    ledger = [row for row in data["slot_events"] if row.get("event") in {"start", "end"}]
    if exclude_foreign:
        ledger = [row for row in ledger if row["workflow_id"] in scope]
    starts, ends = (
        unique((row for row in ledger if row["event"] == event), lambda row: row["invocation_id"])
        for event in ("start", "end")
    )
    require(starts.keys() == ends.keys(), "unfinished or missing slot invocation")
    intervals = {slot: [] for slot in sorted(slots)}
    seen = set()
    for invocation, start in starts.items():
        end = ends[invocation]
        key = attempt_key(start)
        attempt = attempts[key]
        slot = start["worker_id"]
        begin_ns, end_ns = (utc_ns(row["timestamp"]) for row in (start, end))
        require(
            begin_ns <= end_ns
            and key == attempt_key(end)
            and slot == end["worker_id"]
            and start["token_hash"] == end["token_hash"] == attempt["token_hash"]
            and attempt["worker_request_id"].split(":", 1)[0] == slot
            and scope[key[0]] == target
            and slot in intervals
            and key not in seen,
            f"slot invocation/claim mismatch: {invocation}",
        )
        intervals[slot].append((begin_ns, end_ns, key))
        seen.add(key)
    require(seen == attempts.keys(), "missing durable claim in worker slot ledger")
    for slot, rows in intervals.items():
        rows.sort()
        require(
            all(left[1] <= right[0] for left, right in zip(rows, rows[1:], strict=False)),
            f"worker slot intervals overlap: {slot}",
        )
    return {
        "occupied_invocations": {slot: len(rows) for slot, rows in intervals.items()},
        "intervals": intervals,
    }


def jsonl(path):
    with path.open(encoding="utf-8") as stream:
        yield from map(json.loads, stream)
