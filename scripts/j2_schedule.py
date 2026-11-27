"""J2 grid schedule and independent interpretation of the stock K4 journal."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, datetime
from decimal import ROUND_CEILING, Decimal

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
HOLD = 1800
STEP = 300
MEASURED_CAMPAIGN = "j2-load-envelope-v1"
CAP = 16
REQUEST_TIMEOUT = "10s"
ENGINEERING = "engineering timing only"


def timestamp(ns):
    seconds, nanos = divmod(ns, SECOND)
    return datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%S") + f".{nanos:09d}Z"


def population(rate, seconds):
    return int((Decimal(RATES[rate]) * seconds).to_integral_value(rounding=ROUND_CEILING))


def soak_population(rate):
    return population(rate, WARMUP) + population(rate, HOLD) + population("C", STEP)


def registered_schedule(campaign, job, cell, run_id, namespace, origin_ns, rate):
    require(rate in RATES and (cell == rate or cell == "soak"), "invalid registered cell or rate")
    common = (campaign, job, cell, run_id, namespace, origin_ns)
    if cell == "soak":
        schedule = continuous_schedule(
            *common, (RATES[rate], WARMUP + HOLD), (RATES["C"], STEP), warmup=WARMUP
        )
        bound = soak_population(rate)
    else:
        schedule = stock_schedule(*common, RATES[rate], WARMUP, MEASURED)
        bound = population(rate, WARMUP + MEASURED)
    schedule.update(registered_clock=True, population_bound=bound)
    require(schedule["count"] <= bound, "actual schedule exceeds its registered population bound")
    return schedule


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


def continuous_schedule(campaign, job, cell, run_id, namespace, origin_ns, hold, step, *, warmup=0):
    hold_rate, hold_seconds = hold
    step_rate, step_seconds = step
    require(0 <= warmup < hold_seconds, "warm-up must end inside the hold")
    schedule = stock_schedule(
        campaign, job, cell, run_id, namespace, origin_ns, hold_rate, warmup, hold_seconds - warmup
    )
    hold_count = schedule["count"]
    step_count = math.ceil(step_seconds * float(step_rate))
    schedule.update(
        count=hold_count + step_count,
        hold_end_ns=schedule["stop_ns"],
        stop_ns=schedule["stop_ns"] + step_seconds * SECOND,
        periods=[
            {"rate": hold_rate, "seconds": hold_seconds, "count": hold_count},
            {"rate": step_rate, "seconds": step_seconds, "count": step_count},
        ],
    )
    return schedule


def schedule_slots(schedule):
    start = schedule["origin_ns"]
    stop = schedule["stop_ns"]
    periods = schedule.get("periods") or [
        {"rate": schedule["rate"], "count": schedule["count"], "seconds": (stop - start) / SECOND}
    ]
    for period in periods:
        rate = float(period["rate"])
        end = start + int(period["seconds"] * SECOND)
        for i in range(period["count"]):
            at = start + int(float(i) * SECOND / rate)
            cutoff = min(end, start + int(float(i + 1) * SECOND / rate))
            require(at < cutoff, "scheduled arrival outside its period")
            yield at, cutoff
        start = end
    require(start == stop, "periods do not reach scheduled stop")


def registered_population(schedule, cohorts):
    """Bind the planned and observed populations separately from the capacity bound."""
    rate = next((key for key, value in RATES.items() if value == schedule["rate"]), None)
    expected = registered_schedule(
        *(schedule[k] for k in ("campaign", "job", "cell", "run_id", "namespace", "origin_ns")),
        rate,
    )
    warmup = population(rate, WARMUP)
    window_counts = {"warmup": warmup}
    if expected["cell"] == "soak":
        window_counts.update(
            hold=population(rate, WARMUP + HOLD) - warmup,
            step=population("C", STEP),
        )
    else:
        window_counts["grid"] = expected["population_bound"] - warmup
    require(
        ("periods" in schedule) == ("periods" in expected)
        and all(schedule.get(key) == value for key, value in expected.items() if key != "scope")
        and {name: cohort["offered"] for name, cohort in cohorts.items()} == window_counts
        and sum(window_counts.values()) == expected["count"],
        "registered schedule or population bound differs",
    )
    return {
        "bound": expected["population_bound"],
        "scheduled": expected["count"],
        "windows": {
            name: {
                "scheduled": cohort["offered"],
                "checked_success": cohort["checked_success"],
                "in_window_success": len(cohort["in_window_success"]),
            }
            for name, cohort in cohorts.items()
        },
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
    planned = {}
    continuous = "periods" in schedule
    for sequence, (row, (at, cutoff)) in enumerate(
        zip(plan["rows"], schedule_slots(schedule), strict=True)
    ):
        key = f"dur053-{schedule['run_id']}-{schedule['seed']}-{sequence:06d}"
        require(
            row["sequence"] == sequence
            and row["key"] == row["workflow_id"] == key
            and row["scheduled_ns"] == at - origin
            and utc_ns(row["scheduled_at"]) == at
            and at < stop,
            f"K4 scheduled row differs: {key}",
        )
        planned[key] = (at, cutoff)
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
            cutoff_after_intent = (
                continuous and key in active and event["error"] == "cutoff-after-intent"
            )
            require(
                (key not in active or cutoff_after_intent)
                and outcome == "unsent"
                and event["attempts"] == 0
                and event["status"] == 0,
                f"invalid unsent K4 arrival: {key}",
            )
            if cutoff_after_intent:
                active.pop(key)
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
