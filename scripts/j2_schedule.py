"""J2 grid schedule and independent interpretation of the stock K4 journal."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, datetime

from j2_offline import SECOND, utc_ns
from j2_qualification_evidence import require

RATES = {
    "A": "7.136100570066256",
    "AB": "10.091970208646075",
    "B": "14.272201140132513",
    "BC": "18.63484081654246",
    "C": "24.33102566649034",
}
ORDERS = (
    ("A", "AB", "B", "BC", "C"),
    ("B", "BC", "C", "A", "AB"),
    ("C", "A", "AB", "B", "BC"),
)
WARMUP = 120
MEASURED = 600
CAP = 16
REQUEST_TIMEOUT = "10s"
ENGINEERING = "engineering timing only"


def timestamp(ns):
    seconds, nanos = divmod(ns, SECOND)
    return datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%S") + f".{nanos:09d}Z"


def stock_schedule(campaign, job, cell, run_id, namespace, origin_ns, rate, warmup, measured):
    seed = (
        int.from_bytes(hashlib.sha256(f"{campaign}/{job}/{cell}".encode()).digest()[:4], "big") >> 1
    )
    return {
        "scope": ENGINEERING,
        "campaign": campaign,
        "job": job,
        "cell": cell,
        "run_id": run_id,
        "namespace": namespace,
        "seed": seed,
        "rate": rate,
        "count": math.ceil((warmup + measured) * float(rate)),
        "cap": CAP,
        "request_timeout": REQUEST_TIMEOUT,
        "origin_ns": origin_ns,
        "warmup_end_ns": origin_ns + warmup * SECOND,
        "stop_ns": origin_ns + (warmup + measured) * SECOND,
    }


def admission_states(planned, finished, durable):
    """Preserve ambiguous sends, including NOT_SENT in a surviving pre-send plan."""
    ids = set(durable)
    require(
        len(ids) == len(durable) and ids <= planned, "unexpected or duplicate durable admission"
    )
    acknowledged = {key for key, row in finished.items() if row["outcome"] == "accepted"}
    unsent = {key for key, row in finished.items() if row["outcome"] == "unsent"}
    require(not (unsent & ids), "explicit unsent journal row has a durable admission")
    return {
        "durable_admissions": sorted(ids),
        "acknowledged": sorted(acknowledged),
        "missing_acknowledged": sorted(acknowledged - ids),
        "admitted_without_acknowledgment": sorted(ids - acknowledged),
        "explicitly_unsent": sorted(unsent),
        "unresolved": sorted(planned - ids - unsent),
    }


def k4_report(raw, schedule, durable):
    lines = raw.splitlines(keepends=True)
    torn = bool(lines and not lines[-1].endswith(b"\n"))
    if torn:
        lines.pop()
    require(lines, "missing complete K4 plan")
    events = [json.loads(line) for line in lines]
    head = events[0]
    require(head["event"] == "plan", "first K4 event is not a plan")
    plan = head["plan"]
    require(
        plan["schema"] == "dur053.arrivals.v1"
        and plan["status"] == "IN_PROGRESS"
        and plan["arm"] == "short"
        and all(plan[field] == schedule[field] for field in ("namespace", "run_id", "seed"))
        and plan["rate"] == float(schedule["rate"])
        and plan["concurrency"] == schedule["cap"]
        and plan["offered"] == len(plan["rows"]) == schedule["count"],
        "stock K4 plan differs from requested schedule",
    )
    origin, stop = schedule["origin_ns"], schedule["stop_ns"]
    rate = float(schedule["rate"])
    planned = {}
    for sequence, row in enumerate(plan["rows"]):
        offset = int(float(sequence) * SECOND / rate)
        at = origin + offset
        key = f"dur053-{schedule['run_id']}-{schedule['seed']}-{sequence:06d}"
        require(
            row["sequence"] == sequence
            and row["key"] == row["workflow_id"] == key
            and row["scheduled_ns"] == offset
            and utc_ns(row["scheduled_at"]) == at
            and at < stop,
            f"K4 scheduled row differs: {key}",
        )
        planned[key] = (at, min(stop, origin + int(float(sequence + 1) * SECOND / rate)))
    active = {}
    finished = {}
    sender_samples = []
    violations = []
    closed = False
    stopped = False
    for line, event in enumerate(events[1:], 2):
        require(not closed, "K4 journal has records after close")
        name = event["event"]
        if name == "closed":
            require(not active, "closed K4 journal has unresolved intents")
            closed = True
            continue
        if name == "stop":
            require(not stopped and utc_ns(event["stop"]) == stop, "unexpected K4 stop")
            stopped = True
            continue
        require(name == "arrival", f"unknown K4 event: {name}")
        key = event["key"]
        require(key in planned and key not in finished, f"unknown or repeated K4 arrival: {key}")
        at, cutoff = planned[key]
        require(
            event["id"] == event["workflow"] == key and utc_ns(event["scheduled"]) == at,
            f"K4 journal identity or scheduled time differs: {key}",
        )
        sent = utc_ns(event["sent"]) if event["sent"] is not None else None
        outcome = event["outcome"]
        lateness = None if sent is None else sent - at
        if sent is None:
            require(
                key not in active
                and outcome == "unsent"
                and event["attempts"] == 0
                and event["status"] == 0,
                f"invalid unsent K4 arrival: {key}",
            )
            finished[key] = event
            phase = "unsent"
        elif key in active:
            require(
                outcome in {"accepted", "rejected", "uncertain"}
                and event["sent"] == active[key]["sent"]
                and event["attempts"] == 1,
                f"invalid completed K4 arrival: {key}",
            )
            active.pop(key)
            finished[key] = event
            phase = "finished"
        else:
            require(
                not stopped
                and outcome == "uncertain"
                and event["status"] == 0
                and not event["error"]
                and event["attempts"] == 1,
                f"missing K4 send intent: {key}",
            )
            active[key] = event
            phase = "intent"
            if not at <= sent < cutoff:
                violations.append(key)
        sender_samples.append(
            {
                "line": line,
                "key": key,
                "phase": phase,
                "outcome": outcome,
                "in_flight_from_journal": len(active),
                "recorded_intent_lateness_ns": lateness,
            }
        )

    states = admission_states(set(planned), finished, durable)
    return {
        **states,
        "offered": len(planned),
        "warmup_offered": sum(at < schedule["warmup_end_ns"] for at, _ in planned.values()),
        "closed": closed,
        "torn_suffix": torn,
        "remaining_intents": sorted(active),
        "outside_slot": violations,
        "max_in_flight_from_journal": max(
            (row["in_flight_from_journal"] for row in sender_samples), default=0
        ),
        "stock_verdict": (
            "PASS"
            if closed and not torn and len(states["acknowledged"]) == len(planned)
            else "FAIL"
        ),
        "sender_samples": sender_samples,
    }
