"""Replay J2 evidence using integer UTC nanoseconds and durable revisions."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import zipfile
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean, median

SECOND = 10**9
TERMINAL = {"SUCCEEDED", "FAILED", "CANCELED"}
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def utc_ns(value: str) -> int:
    base, fraction, zone = re.fullmatch(
        r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)", value
    ).groups()
    delta = datetime.fromisoformat(base + zone) - EPOCH
    return (delta.days * 86400 + delta.seconds) * SECOND + int((fraction or "").ljust(9, "0"))


def clock_bounds(probes: dict, start: int, last_observation: int) -> int:
    setup, final = [
        [utc_ns(probes[phase][k]) for k in ("observer_before", "database", "observer_after")]
        for phase in ("setup", "measured")
    ]
    if not (
        setup[0]
        <= setup[1]
        <= setup[2]
        <= start
        <= last_observation
        <= final[0]
        <= final[1]
        <= final[2]
    ):
        raise ValueError("unspanned or invalid observer/DB clock bracket")
    return max(setup[2] - setup[0], final[2] - final[0])


def index(rows, key):
    grouped = defaultdict(list)
    for row in rows:
        grouped[key(row)].append(row)
    return grouped


def attempt_key(row):
    return tuple(row[field] for field in ("workflow_id", "node_id", "iteration", "attempt_number"))


def transport(row):
    status = row.get("http_status", 0)
    if row.get("send_ns") is None:
        return row["outcome"]
    if 400 <= status < 600:
        return "http_429" if status == 429 else f"http_{status // 100}xx"
    if row["outcome"] == "ACCEPTED":
        return "accepted"
    error = row.get("error", "").lower()
    if "deadline exceeded" in error or "timeout" in error:
        return "timeout"
    if 200 <= status < 300:
        return "malformed_acceptance"
    return "other_transport_error"


def sender(plan, journal, submissions):
    """Check against the independent UTC origin and rated periods (first ns, count, rate)."""
    origin = utc_ns(plan["origin"])
    rows = index(journal["rows"], lambda r: r["key"])
    commits = index(submissions, lambda r: r["Key"])
    problems = {}
    expected = set()
    sequence = 0
    planned = []
    send_discrepancies = []
    for first, count, rate in plan["periods"]:
        for i in range(count):
            key = f"dur053-{plan['run_id']}-{plan['seed']}-{sequence:06d}"
            slot, next_slot = (first + int(j * SECOND / rate) for j in (i, i + 1))
            expected.add(key)
            candidates = rows[key]
            planned.append((key, origin + slot))
            if len(candidates) != 1:
                problems[key] = "missing_or_duplicate_journal"
            else:
                row = candidates[0]
                sent = row.get("send_ns")
                if (row["sequence"], row["workflow_id"], row["scheduled_ns"]) != (
                    sequence,
                    key,
                    slot,
                ) or utc_ns(row["scheduled_at"]) != origin + slot:
                    problems[key] = "schedule_mismatch"
                elif sent is None or not slot <= sent < next_slot or not row.get("sent_at"):
                    problems[key] = "missing_or_late_send"
                elif len(commits[key]) != 1 or (
                    commits[key][0]["Namespace"],
                    commits[key][0]["WorkflowID"],
                ) != (plan["namespace"], key):
                    problems[key] = "unreconciled_admission"
                else:
                    send_discrepancies.append(utc_ns(row["sent_at"]) - origin - sent)
                    outcome = transport(row)
                    response = (row.get("response") or {}).get("workflow") or {}
                    if outcome == "accepted" and 200 <= row["http_status"] < 300:
                        if (
                            response.get("workflow_id"),
                            response.get("submission_key"),
                            response.get("namespace"),
                            response.get("payload_hash"),
                        ) != (key, key, plan["namespace"], commits[key][0]["Hash"]):
                            problems[key] = "acceptance_mismatch"
                    elif outcome != "timeout":
                        problems[key] = outcome
            sequence += 1
    unexpected = sorted((set(rows) | set(commits)) - expected)
    return planned, {
        "valid": bool(expected) and not problems and not unexpected,
        "problems": problems,
        "unexpected_keys": unexpected,
        "transport": dict(Counter(transport(row) for row in journal["rows"])),
        "committed_timeout_keys": sorted(
            {
                row["key"]
                for row in journal["rows"]
                if transport(row) == "timeout" and len(commits[row["key"]]) == 1
            }
        ),
        "stock_status": journal["status"],
        "max_abs_send_wall_minus_monotonic_ns": max(map(abs, send_discrepancies), default=0),
    }


def join(planned, snapshot, starts, observations, uncertainty, last_observation):
    tables = snapshot["Snapshot"]
    workflows = index(tables["engine.workflow_executions"]["rows"], lambda r: r["workflow_id"])
    history = index(tables["engine.transition_history"]["rows"], lambda r: r["workflow_id"])
    attempts = index(tables["engine.activity_attempts"]["rows"], attempt_key)
    receipts = index(
        (row for row in tables["engine.outbox"]["rows"] if row["event_type"] == "activity.result"),
        lambda row: (row["workflow_id"], row["aggregate_revision"]),
    )
    starts = index((row for row in starts if row["event"] == "start"), attempt_key)
    admissions = {row["WorkflowID"] for row in snapshot["Submissions"]}
    for key, scheduled in planned:
        matching = workflows[key]
        records = history[key]
        created = [row for row in records if row["reason"] == "ATTEMPT_CREATED"]
        row = {
            "workflow_id": key,
            "scheduled_ns": scheduled,
            "attempts": len(created),
            "attempt_history": [
                h for h in records if h["reason"] in {"ATTEMPT_CREATED", "ATTEMPT_RESULT_RECORDED"}
            ],
            "admitted": key in admissions,
        }
        if len(matching) != 1:
            yield {**row, "status": "missing_workflow" if not matching else "duplicate_workflow"}
            continue
        workflow = matching[0]
        state = workflow["state"]
        if state not in TERMINAL:
            yield {**row, "status": "censored", "lower_bound_ns": last_observation - scheduled}
            continue
        terminal = [
            h for h in records if h["new_state"] == state and h["revision"] == workflow["revision"]
        ]
        row["terminal_candidates"] = terminal
        if len(terminal) != 1:
            yield {**row, "status": "missing_or_duplicate_terminal"}
            continue
        durable = utc_ns(terminal[0]["created_at"])
        observed = observations.get(key)
        row.update(terminal_ns=durable, first_observed_ns=observed, terminal_state=state)
        if (
            observed is None
            or not scheduled - uncertainty <= durable <= observed + uncertainty
            or observed > last_observation
        ):
            yield {**row, "status": "terminal_clock_or_observation"}
            continue
        row["polling_minus_durable_ns"] = observed - durable
        if state != "SUCCEEDED":
            yield {**row, "status": state.lower()}
            continue
        row.update(
            raw_end_to_end_ns=durable - scheduled, end_to_end_ns=durable - scheduled + uncertainty
        )
        results = [
            h
            for h in records
            if h["reason"] == "ATTEMPT_RESULT_RECORDED" and h["revision"] < terminal[0]["revision"]
        ]
        revision = max((h["revision"] for h in results), default=-1)
        winners = [h for h in results if h["revision"] == revision]
        if len(winners) != 1:
            yield {**row, "status": "missing_or_duplicate_winner"}
            continue
        winner = winners[0]
        identity = attempt_key(winner)
        begun = [h for h in created if attempt_key(h) == identity and h["revision"] < revision]
        stored = attempts[identity]
        witnessed = starts[identity]
        result_receipts = receipts[key, revision]
        row.update(
            winning_attempt=identity, result_receipts=result_receipts, witness_starts=witnessed
        )
        if not len(begun) == len(stored) == len(witnessed) == len(result_receipts) == 1:
            yield {**row, "status": "missing_or_duplicate_attempt_boundary"}
            continue
        if (
            not stored[0]["claim_token"]
            or stored[0]["claim_token"] != witnessed[0]["claim_token"]
            or stored[0]["state"] != "SUCCEEDED"
            or stored[0]["result"] != result_receipts[0]["payload"]
        ):
            yield {**row, "status": "attempt_receipt_mismatch"}
            continue
        points = [
            scheduled,
            utc_ns(begun[0]["created_at"]),
            utc_ns(witnessed[0]["timestamp"]),
            utc_ns(winner["created_at"]),
            durable,
        ]
        row["phase_boundary_ns"] = points
        if points != sorted(points):
            yield {**row, "status": "reversed_phase"}
            continue
        yield {
            **row,
            "status": "observed",
            "phase_ns": [b - a for a, b in zip(points, points[1:], strict=False)],
            "phase_sensitivity_ns": [uncertainty, uncertainty, uncertainty, 0],
        }


def rank(count, percent):
    return (count * percent + 99) // 100 - 1


def quantiles(values, eligible, censored):
    values = sorted(values)
    n = len(values)
    result = {
        "eligible": eligible,
        "observed": n,
        "missing": eligible - n - censored,
        "censored": censored,
        "p50_ns": None,
        "p95_ns": None,
    }
    if n:
        result.update(p50_ns=values[rank(n, 50)], p95_ns=values[rank(n, 95)])
    if n >= 200:
        result["p99_ns"] = values[rank(n, 99)]
    return result


def summarize(rows):
    eligible = [
        row
        for row in rows
        if row["admitted"] and row.get("terminal_state") not in {"FAILED", "CANCELED"}
    ]
    complete = [row for row in eligible if row["status"] == "observed"]
    terminal = sorted(
        (row for row in eligible if "end_to_end_ns" in row),
        key=lambda r: (r["end_to_end_ns"], r["scheduled_ns"], r["workflow_id"]),
    )
    censored = sum(row["status"] == "censored" for row in eligible)
    p99 = terminal[rank(len(terminal), 99)] if len(terminal) >= 200 else None
    return {
        "outcomes": dict(Counter(row["status"] for row in rows)),
        "multiple_attempts": sum(row["attempts"] > 1 for row in rows),
        "maximum_attempts": max((row["attempts"] for row in rows), default=0),
        "phase_quantiles": [
            quantiles([row["phase_ns"][i] for row in complete], len(eligible), censored)
            for i in range(4)
        ],
        "end_to_end_quantiles": quantiles(
            [r["end_to_end_ns"] for r in terminal], len(eligible), censored
        ),
        "end_to_end_p99_workflow": p99,
    }


def drain_identities(admitted, acknowledged, succeeded, checked):
    admitted, acknowledged, succeeded, checked = map(
        list, (admitted, acknowledged, succeeded, checked)
    )
    expected = set(admitted) | set(acknowledged)
    missing = sorted(expected - (set(admitted) & set(succeeded) & set(checked)))
    unexpected = sorted((set(succeeded) | set(checked)) - set(admitted))
    duplicated = sorted(
        {
            key
            for rows in (admitted, acknowledged, succeeded, checked)
            for key, count in Counter(rows).items()
            if count > 1
        }
    )
    return {
        "valid": not (missing or unexpected or duplicated),
        "missing": missing,
        "unexpected": unexpected,
        "duplicated": duplicated,
    }


def bins_gate(rows, samples, start, end, sender_valid):
    measured = sorted(
        (row for row in rows if start <= row["scheduled_ns"] < end),
        key=lambda r: (r["scheduled_ns"], r["workflow_id"]),
    )
    edges = [start + (end - start) * i // 6 for i in range(7)]
    bins = []
    timed = sorted(((utc_ns(s["observed_at"]), s) for s in samples), key=lambda item: item[0])
    for lo, hi in zip(edges, edges[1:], strict=False):
        actual = [(t, s["outstanding"], s["unpublished"]) for t, s in timed if lo <= t < hi]
        valid = all(type(n) is int and n >= 0 for _, a, b in actual for n in (a, b))
        bins.append(
            {
                "arrivals": sum(lo <= r["scheduled_ns"] < hi for r in measured),
                "completions": sum(
                    lo <= r.get("terminal_ns", -1) < hi
                    for r in measured
                    if r.get("terminal_state") == "SUCCEEDED"
                ),
                "samples": actual,
                "outstanding": mean(a for _, a, _ in actual) if actual and valid else None,
                "unpublished": mean(b for _, _, b in actual) if actual and valid else None,
            }
        )
    times = [start, *(t for b in bins for t, _, _ in b["samples"]), end]
    maximum_gap = max(b - a for a, b in zip(times, times[1:], strict=False))
    eligible = (
        end > start
        and len(measured) >= 6
        and maximum_gap <= 5 * SECOND
        and len(times[1:-1]) == len(set(times[1:-1]))
        and all(b["outstanding"] is not None for b in bins)
        and all(row["status"] == "observed" for row in measured)
    )
    evidence = {"bins": bins, "maximum_sample_gap_ns": maximum_gap, "complete_inputs": eligible}
    if not sender_valid or not eligible:
        return {
            **evidence,
            "status": "sender_invalid" if not sender_valid else "invalid_gate_inputs",
        }
    arrivals = sum(b["arrivals"] for b in bins[1:])
    completions = sum(b["completions"] for b in bins[1:])
    latencies = [r["raw_end_to_end_ns"] for r in measured]
    n = len(latencies)
    first, last = [sorted(latencies[i * n // 3 : (i + 1) * n // 3]) for i in (0, 2)]
    first_stats, last_stats = [(median(xs), xs[rank(len(xs), 90)]) for xs in (first, last)]
    outstanding = [b["outstanding"] for b in bins]
    checks = {
        "4a": 100 * completions >= 95 * arrivals,
        "4b": all(
            y <= max(1.25 * x, x + SECOND // 2)
            for x, y in zip(first_stats, last_stats, strict=True)
        ),
        "4c": outstanding[-1] <= outstanding[0] + 4
        and bins[-1]["unpublished"] <= bins[0]["unpublished"] + 20,
    }
    growth = (
        outstanding[3] < outstanding[4] < outstanding[5] and outstanding[5] - outstanding[3] > 4
    )
    return {
        **evidence,
        "status": "pass" if all(checks.values()) else "failed_gate",
        "checks": checks,
        "pooled_arrivals": arrivals,
        "pooled_completions": completions,
        "first_third_ns": first_stats,
        "last_third_ns": last_stats,
        "sustained_growth": growth,
    }


def read_json(path: Path):
    return json.loads(path.read_bytes())


def replay(plan, journal, snapshot, checker, witness, polls, samples, probes):
    from dur053_pilot import witness_events

    if not (
        checker["valid"] is True
        and checker["campaign"]["valid"] is True
        and checker["trace"] == snapshot
    ):
        raise ValueError("checker verdict does not bind the snapshot")
    planned, delivery = sender(plan, journal, snapshot["Submissions"])
    observations = {}
    poll_times = [utc_ns(poll["observed_at"]) for poll in polls]
    if poll_times != sorted(set(poll_times)):
        raise ValueError("polls must have strictly increasing UTC timestamps")
    for poll, observed in zip(polls, poll_times, strict=True):
        for key in poll["terminal_ids"]:
            observations.setdefault(key, observed)
    uncertainty = clock_bounds(probes, utc_ns(plan["origin"]), poll_times[-1])
    rows = list(
        join(planned, snapshot, witness_events(witness), observations, uncertainty, poll_times[-1])
    )
    start, end = (utc_ns(plan[k]) for k in ("window_start", "window_end"))
    window = [r for r in rows if start <= r["scheduled_ns"] < end]
    drain = drain_identities(
        (r["WorkflowID"] for r in snapshot["Submissions"]),
        (r["key"] for r in journal["rows"] if r["outcome"] == "ACCEPTED"),
        (
            r["workflow_id"]
            for r in rows
            if r.get("terminal_state") == "SUCCEEDED" and "end_to_end_ns" in r
        ),
        (r["workflow_id"] for r in checker["campaign"]["executions"] if r["winners"] == 1),
    )
    return {
        "clock_uncertainty_ns": uncertainty,
        "sender": delivery,
        "drain_identities": drain,
        "poll_gaps_ns": [b - a for a, b in zip(poll_times, poll_times[1:], strict=False)],
        "window": summarize(window),
        "rows": rows,
        "gate": bins_gate(rows, samples, start, end, delivery["valid"]),
    }


def archived_replay(manifest_path, artifact, cell):
    """The historical journal includes scheduled offsets but predates a saved UTC plan."""
    manifest = read_json(manifest_path)
    with zipfile.ZipFile(artifact) as archive:

        def load(name, lines=False):
            path = f"{cell}/{name}"
            raw = archive.read(path)
            if hashlib.sha256(raw).hexdigest() != manifest["artifact_sha256"][path]:
                raise ValueError(f"archived input hash mismatch for {path}")
            return [json.loads(line) for line in raw.splitlines()] if lines else json.loads(raw)

        journal = load("continuous-arrivals.json")
        record = load("record.json")
        start = utc_ns(record["measurement_start_at"])
        end = start + int(record["measurement_seconds"] * SECOND)
        seconds, ns = divmod(end, SECOND)
        end_text = datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%S")
        plan = {
            "origin": journal["rows"][0]["scheduled_at"],
            "periods": [
                [0, record["warmup_count"] + record["measured_count"], record["measured_rate"]]
            ],
            "run_id": f"{cell}-continuous",
            "namespace": journal["namespace"],
            "seed": record["seed"],
            "window_start": record["measurement_start_at"],
            "window_end": f"{end_text}.{ns:09}Z",
        }
        report = replay(
            plan,
            journal,
            load("snapshot.json"),
            load("offline-checker.json"),
            load("witness.json"),
            load("samples.jsonl", True),
            load("engine-samples.jsonl", True),
            load("clock.json"),
        )
    return {
        "scope": "archived K3 evidence replay",
        "source_run": manifest["run_id"],
        "source_cell": cell,
        **report,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Replay archived K3 input through the J2 offline tooling"
    )
    parser.add_argument("manifest", type=Path)
    parser.add_argument("artifact", type=Path)
    parser.add_argument("cell", choices=("short-1", "short-2", "short-3"))
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    report = archived_replay(args.manifest, args.artifact, args.cell)
    args.output.write_bytes(
        (json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n").encode()
    )
