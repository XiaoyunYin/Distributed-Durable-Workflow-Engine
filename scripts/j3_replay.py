"""Verify retained J3 alert inputs and compute conservative deadline attainment."""

import argparse
import json
from pathlib import Path

from j2_offline import SECOND, TERMINAL, index, utc_ns

DEADLINE = 2_854_000_000
WINDOWS = {"all": None, "10m": 600 * SECOND, "2m": 120 * SECOND}
DOMAIN = ("OldestClaimable", "DeadlineBudgetBurn")
ALERTS = (*DOMAIN, "ScrapeAbsent")


def require(condition, detail):
    if not condition:
        raise ValueError(detail)


def unique(rows, key):
    result = {}
    for row in rows:
        identity = key(row)
        require(identity not in result, f"duplicate identity: {identity}")
        result[identity] = row
    return result


def brackets(data):
    setup, closing = [data["clock"][which] for which in ("setup", "closing")]
    parts = [probe[key] for probe in (setup, closing) for key in ("before_ns", "db_ns", "after_ns")]
    a, b, c, d, e, f = parts
    require(
        a <= b <= c <= data["start_ns"] <= data["end_ns"] <= d <= e <= f,
        "invalid or unspanned clock probes",
    )
    return c - a, f - d


def reconcile(data):
    namespace = data["namespace"]
    plan = unique(
        (r for r in data["plan"] if r["namespace"] == namespace), lambda r: r["submission_key"]
    )
    durable = unique(
        (r for r in data["workflows"] if r["namespace"] == namespace), lambda r: r["workflow_id"]
    )
    by_key = unique(durable.values(), lambda r: r["submission_key"])
    journal = unique(
        (r for r in data["journal"] if r["namespace"] == namespace), lambda r: r["submission_key"]
    )
    require(
        plan.keys() == journal.keys() and by_key.keys() <= plan.keys(),
        "plan, journal or durable scope mismatch",
    )
    for key, row in journal.items():
        status = row["outcome"]
        require(status in {"accepted", "timeout"}, f"unexpected transport outcome: {key}")
        if status == "accepted":
            require(
                key in by_key and by_key[key]["workflow_id"] == row["workflow_id"],
                f"missing acknowledged row: {key}",
            )
    history = index(
        (r for r in data["history"] if r["workflow_id"] in durable),
        lambda r: (r["workflow_id"], r["revision"]),
    )
    result = {}
    for workflow_id, row in durable.items():
        key = row["submission_key"]
        state = row["state"]
        revision = row["revision"]
        final = history[workflow_id, revision]
        if state in TERMINAL:
            require(
                len(final) == 1 and final[0]["new_state"] == state,
                f"broken terminal history: {workflow_id}",
            )
        result[workflow_id] = {
            "submission_key": key,
            "scheduled_ns": plan[key]["scheduled_ns"],
            "state": state,
            "revision": revision,
            "terminal_ns": final[0]["created_ns"] if state in TERMINAL else None,
        }
    return result, sorted(k for k in by_key if journal[k]["outcome"] == "timeout")


def on_time(row, width, at):
    deadline = row["scheduled_ns"] + DEADLINE
    return (
        row["state"] == "SUCCEEDED"
        and row["terminal_ns"] <= at
        and row["terminal_ns"] + width <= deadline
    )


def attainment(rows, setup, closing, end):
    require(
        all(row["scheduled_ns"] + DEADLINE <= end for row in rows.values()),
        "closeout precedes a deadline",
    )
    width = max(setup, closing)
    lower = {key for key, row in rows.items() if on_time(row, setup, end)}
    published = {key for key, row in rows.items() if on_time(row, width, end)}
    return {
        "on_time": len(published),
        "eligible": len(rows),
        "attainment": len(published) / len(rows) if rows else None,
        "incomplete": sum(row["state"] not in TERMINAL for row in rows.values()),
        "setup_width_ns": setup,
        "closing_width_ns": closing,
        "width_sensitivity": len(lower ^ published),
        "raw_durable_latency_ns": {
            key: row["terminal_ns"] - row["scheduled_ns"]
            for key, row in rows.items()
            if row["terminal_ns"] is not None
        },
    }


def snapshots(stream):
    admissions, successes, last = {}, {}, -1
    for sample in stream:
        at = sample["sample_ns"]
        require(at > last, "unordered or duplicate visibility samples")
        last = at
        for key, current in (("admissions", admissions), ("successes", successes)):
            change = sample[key]
            deleted = set(change["delete"])
            require(
                len(deleted) == len(change["delete"])
                and deleted <= current.keys()
                and not deleted.intersection(change["set"]),
                "invalid visibility delta",
            )
            for identity in deleted:
                del current[identity]
            current.update(change["set"])
        require(successes.keys() <= admissions.keys(), "visible success without admission")
        yield sample, admissions, successes


def cohort_band(rows, sample, admissions, successes, setup):
    at = sample["sample_ns"]
    require(sample["u_ns"] == setup, "live clock width differs")
    require(
        admissions.keys() <= rows.keys()
        and all(rows[key]["submission_key"] == value for key, value in admissions.items()),
        "snapshot admission mismatch",
    )
    require(
        all(
            rows[key]["state"] == "SUCCEEDED"
            and rows[key]["revision"] == revision
            and rows[key]["terminal_ns"] <= at
            for key, revision in successes.items()
        ),
        "snapshot history mismatch",
    )
    require(set(sample["windows"]) == WINDOWS.keys(), "missing deadline window")
    result = {}
    prior_added = prior_uncertain = 0
    for window in ("2m", "10m", "all"):
        width = WINDOWS[window]
        included = [
            key
            for key in admissions
            if rows[key]["scheduled_ns"] + DEADLINE <= at
            and (width is None or rows[key]["scheduled_ns"] + DEADLINE > at - width)
        ]
        good = {key for key in included if on_time(rows[key], setup, at)}
        uncertainty = len(good - successes.keys())
        replay_bad = len(included) - len(good)
        actual = sample["windows"][window]
        added = actual["bad"] - replay_bad
        require(
            actual["eligible"] == len(included) and 0 <= added <= uncertainty,
            f"eligible count or visibility band mismatch: {at} {window}",
        )
        require(
            0 <= added - prior_added <= uncertainty - prior_uncertain,
            f"inconsistent visibility across nested windows: {at} {window}",
        )
        prior_added, prior_uncertain = added, uncertainty
        result[window] = {
            "eligible": len(included),
            "live_bad": actual["bad"],
            "lower": replay_bad,
            "upper": replay_bad + uncertainty,
        }
    return result


def burn(band, field):
    short, long = (band[w] for w in ("2m", "10m"))
    if min(short["eligible"], long["eligible"]) < 100:
        return False, False
    hot = all(w[field] * 125 > w["eligible"] * 18 for w in (short, long))
    return hot, not hot


def verify_alerts(rows, setup, stream, evaluations, first, last):
    require(
        bool(evaluations)
        and [r["at_ns"] for r in evaluations] == list(range(first, last + 1, 5 * SECOND))
        and evaluations[-1]["at_ns"] == last,
        "missing or unordered rule evaluations",
    )
    requests = {r["sample_ns"] for r in evaluations if r["valid"]}
    inputs = {}
    for sample, admissions, successes in snapshots(stream):
        at = sample["sample_ns"]
        band = cohort_band(rows, sample, admissions, successes, setup)
        if at in requests:
            inputs[at] = band, sample["demand"], sample["windows"]
    active, pending, ambiguous, transitions = set(), None, [], []
    for ev in evaluations:
        at = ev["at_ns"]
        before = active.copy()
        require(ev["rule_ok"] is True, f"rule evaluation failed: {at}")
        if ev["valid"]:
            source, scrape = ev["sample_ns"], ev["scrape_ns"]
            require(source in inputs, f"missing visibility evidence: {source}")
            band, demand, counts = inputs[source]
            require(
                demand is not None
                and ev["windows"] == counts
                and at - 5 * SECOND <= source <= scrape <= at
                and at - 5 * SECOND <= ev["demand_ns"] <= scrape,
                f"incoherent live input: {at}",
            )
            require(
                utc_ns(demand["sampled_at"]) == ev["demand_ns"]
                and demand["oldest_claimable_age_seconds"] == ev["oldest_claimable_age_seconds"]
                and demand["claimable_attempts"] == ev["claimable_attempts"],
                "demand sample mismatch",
            )
            age_fire = (
                demand["claimable_attempts"] > 0 and demand["oldest_claimable_age_seconds"] > 28.540
            )
            pending = None
            active.discard("ScrapeAbsent")
            live = burn(band, "live_bad")
            lower, upper = burn(band, "lower"), burn(band, "upper")
            # Both predicates are monotone in the identity statuses. Assigning
            # every ambiguous identity the same endpoint is feasible in both windows.
            if lower != upper:
                ambiguous.append(
                    {
                        "at_ns": at,
                        "sample_ns": source,
                        "windows": band,
                        "observed_fire": live[0],
                        "observed_clear": live[1],
                    }
                )
            else:
                require(live == lower, f"live/replay predicate mismatch: {at}")
            for alert, (fire, clear) in zip(DOMAIN, ((age_fire, not age_fire), live), strict=True):
                if fire:
                    active.add(alert)
                elif clear:
                    active.discard(alert)
        else:
            pending = at if pending is None else pending
            if at - pending >= 5 * SECOND:
                active.add("ScrapeAbsent")
        require(
            set(ev["firing"]) == active and len(ev["firing"]) == len(active),
            f"actual rule transitions differ from live inputs: {at}",
        )
        for name in sorted(before ^ active):
            transitions.append({"at_ns": at, "alert": name, "firing": name in active})
    return {
        "ambiguous_evaluations": ambiguous,
        "ambiguous_count": len(ambiguous),
        "transitions": transitions,
    }


def replay(data, visibility):
    setup, closing = brackets(data)
    rows, timeouts = reconcile(data)
    return {
        "alert_inputs": verify_alerts(
            rows,
            setup,
            visibility,
            data["evaluations"],
            data["first_eval_ns"],
            data["last_eval_ns"],
        ),
        "published_attainment": attainment(rows, setup, closing, data["end_ns"]),
        "committed_timeout_keys": timeouts,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    parser.add_argument("visibility", type=Path)
    args = parser.parse_args()
    with args.visibility.open(encoding="utf-8") as stream:
        print(
            json.dumps(
                replay(json.loads(args.evidence.read_bytes()), map(json.loads, stream)), indent=2
            )
        )
