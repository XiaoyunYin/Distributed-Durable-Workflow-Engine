"""Recompute the J2 control verdicts from retained schedules, journals and observations."""

from __future__ import annotations

import base64
import hashlib
import json
from collections import Counter
from copy import deepcopy
from decimal import Decimal
from statistics import median

from dur053_pilot import witness_events
from j2_checker_report import (
    IntegrityError,
    ResourceEvidenceError,
    indexed_report,
    require_checker_exit,
)
from j2_control_proxy import HOLD_NS
from j2_drain import (
    QUIET_NS,
    DrainExpired,
    checked_cut,
    checked_results,
    expected_arrivals,
    expire_drain,
    fingerprint,
    quiet_drain,
    read_polls,
    readiness,
    verify_state_reads,
    with_state_read_schema,
)
from j2_offline import (
    SECOND,
    attempt_key,
    bins_gate,
    clock_bounds,
    drain_identities,
    join,
    summarize,
    utc_ns,
)
from j2_qualification_evidence import DRAIN_SECONDS, file_record, read, require
from j2_qualification_resources import checker_resource_summary, require_checker_resources
from j2_qualify_volume import OBSERVER_LIMIT
from j2_schedule import (
    DISPATCH_JOURNAL,
    MEASURED,
    REQUEST_TIMEOUT,
    WARMUP,
    sender_report,
    sent_keys,
    stock_schedule,
    timestamp,
)

DELAY_BASE = "delay-100"
DELAY_HIGH = "delay-2100"
HEALTHY = "healthy-low-load"
CAPPED = "capped-sender"
EPISODES = (DELAY_BASE, DELAY_HIGH, HEALTHY, CAPPED)
DELAY_NS = 2 * SECOND
DELAY_WINDOW = 300


def control_schedule(name, campaign, namespace, origin):
    require(name in EPISODES and campaign.startswith("eng-controls-"), "invalid control identity")
    delay = name in (DELAY_BASE, DELAY_HIGH)
    schedule = stock_schedule(
        campaign,
        "controls",
        name,
        f"j2-{campaign}-{name}",
        namespace,
        origin,
        "0.1" if delay else "1",
        0 if delay else WARMUP,
        DELAY_WINDOW if delay else MEASURED,
    )
    schedule["worker_sleep_ms"] = 2100 if name == DELAY_HIGH else 100
    if name == CAPPED:
        schedule["cap"] = 1
    return schedule


def read_lines(path):
    return [json.loads(line) for line in path.read_bytes().splitlines()]


def gates_samples(series):
    try:
        metrics = {}
        for name, groups in series.items():
            (group,) = groups
            points = group["values"]
            metrics[name] = {at: Decimal(value) for at, value in points}
            require(len(metrics[name]) == len(points), f"duplicate scrape timestamps for {name}")
        samples = {}
        for at, epoch in metrics["j2_sample_timestamp_seconds"].items():
            if not (
                metrics["up"].get(at) == metrics["j2_query_ok"].get(at) == 1
                and all(at in points for points in metrics.values())
            ):
                continue
            stamp = int(epoch * SECOND)
            values = [metrics[name][at] for name in ("j2_outstanding", "j2_unpublished")]
            require(all(x.is_finite() and x >= 0 and x == int(x) for x in values), "invalid gauge")
            counts = tuple(map(int, values))
            require(samples.setdefault(stamp, counts) == counts, "conflicting query samples")
        return [
            {"observed_at": timestamp(stamp), "outstanding": outstanding, "unpublished": outbox}
            for stamp, (outstanding, outbox) in sorted(samples.items())
        ]
    except (ArithmeticError, TypeError, KeyError, ValueError) as error:
        raise ResourceEvidenceError(f"invalid gate sample evidence: {error}") from error


def journal_arrivals(raw):
    events = [json.loads(line) for line in raw.splitlines()]
    if events[0].get("schema") == DISPATCH_JOURNAL:
        return events[0]["plan"]["rows"], [e for e in events[1:] if e["event"] == "outcome"]
    return events[0]["plan"]["rows"], [e for e in events[1:] if e["event"] == "arrival"]


def quiet_evidence(root, final, expected, sender, checker_read, state):
    anchor = read(root / "deadline-anchor.json")
    stop = anchor["stop_mono_ns"]
    deadline = stop + DRAIN_SECONDS * SECOND
    require(readiness(final, expected, sender)["ready"], "quiet final read is not ready")
    require(readiness(state, expected, sender)["ready"], "locked checker had unfinished work")
    require(
        stop <= checker_read["lower_mono_ns"] <= checker_read["upper_mono_ns"] <= deadline,
        "checker outside the drain clock",
    )
    digest = fingerprint(final)
    quiet = None
    last = None
    for entry in read_lines(root / "drain-polls.jsonl"):
        event = entry["event"]
        if event == "quiet-start":
            quiet = (
                entry["upper_mono_ns"]
                if readiness(entry, expected, sender)["ready"]
                and fingerprint(entry) == digest
                and checker_read["upper_mono_ns"] <= entry["lower_mono_ns"]
                else None
            )
        elif event == "poll":
            last = entry
            if not entry["ready"] or entry["fingerprint"] != digest:
                quiet = None
        else:
            quiet = None
    require(
        quiet is not None
        and quiet + QUIET_NS <= final["lower_mono_ns"] <= final["upper_mono_ns"] <= deadline
        and last is not None
        and last["lower_mono_ns"] == final["lower_mono_ns"]
        and last["upper_mono_ns"] == final["upper_mono_ns"],
        "missing continuous bracketed quiet interval",
    )
    return {
        "status": "PASS",
        "quiet_ns": final["lower_mono_ns"] - quiet,
        "drain_seconds": (final["upper_mono_ns"] - stop) / SECOND,
    }


def legacy_phase_projection(raw, schedule, expected, sender):
    """Project a reconciled healthy episode into the historical phase reducer."""
    plan = json.loads(raw.split(b"\n", 1)[0])["plan"]
    events = [{"event": "plan", "plan": plan}]
    for row in sender["sender_samples"]:
        key = row["key"]
        arrival = {
            "event": "arrival",
            "key": key,
            "id": key,
            "workflow": key,
            "request_hash": expected[key]["request_hash"],
            "scheduled": timestamp(expected[key]["scheduled_ns"]),
            "sent": timestamp(schedule["origin_ns"] + row["dispatch_intent_ns"]),
            "attempts": 1,
            "error": "",
        }
        events.extend(
            (
                {**arrival, "outcome": "uncertain", "status": 0},
                {**arrival, "outcome": row["outcome"], "status": row["status"]},
            )
        )
    events.append({"event": "closed"})
    historical = {key: value for key, value in schedule.items() if key != "dispatch_journal"}
    return expected_arrivals(
        b"".join(json.dumps(event).encode() + b"\n" for event in events), historical
    )


def read_episode(root, campaign, namespace, *, phase_parity=False):
    schedule = read(root / "schedule.json")
    expected_schedule = control_schedule(root.name, campaign, namespace, schedule["origin_ns"])
    if schedule.get("dispatch_journal") == DISPATCH_JOURNAL:
        expected_schedule["dispatch_journal"] = DISPATCH_JOURNAL
    binding = file_record(root / "schedule.json")
    require(
        schedule == with_state_read_schema(expected_schedule, schedule)
        and binding == read(root / "schedule-binding.json")
        and schedule["request_timeout"] == REQUEST_TIMEOUT,
        "control schedule or journal binding differs",
    )
    raw = (root / "sender-journal.jsonl").read_bytes()
    expected, _ = expected_arrivals(raw, schedule, schedule_sha256=binding["sha256"])
    plan, journal = journal_arrivals(raw)
    final = read(root / "final.json")
    witness = read(root / "witness.json")
    cut = read(root / "live-checker-read.json")
    require_checker_exit(cut.get("exit"))
    try:
        report, _ = indexed_report(
            root / "live-checker.json", join_trace=True, verify_tables=True, drain_counts=True
        )
        state = checked_cut(
            report,
            cut,
            sorted((x["inventory"] for x in witness["ledgers"]), key=lambda x: x["pod_uid"]),
        )
    except (KeyError, OSError, ValueError) as error:
        raise IntegrityError(f"locked checker cut is invalid: {error}") from error
    admitted = [row["id"] for row in state["db"]["submissions"]]
    sender = sender_report(raw, schedule, admitted, schedule_sha256=binding["sha256"])
    if "state_read_evidence" in schedule:
        verify_state_reads(
            root, schedule, read(root / "drain-result.json"), final, expected, sender
        )
    polls = read_polls(root / "terminal-polls.jsonl")
    width = clock_bounds(read(root / "clock.json"), schedule["origin_ns"], final["upper_unix_ns"])
    try:
        checked = checked_results(
            report, expected, sender, witness, polls, width, final["upper_unix_ns"]
        )
        require(
            {row["id"]: row for row in state["db"]["submissions"]}
            == {row["id"]: row for row in final["db"]["submissions"]}
            and state["db"]["succeeded"] == final["db"]["succeeded"]
            and state["witness"] == final["witness"],
            "checker cut differs from final bracket",
        )
    except (KeyError, ValueError) as error:
        raise IntegrityError(str(error)) from error
    try:
        sampler_exit = read(root / "checker-resource-summary.json")["exit"]
    except (KeyError, OSError, ValueError) as error:
        raise ResourceEvidenceError("missing checker sampler exit status") from error
    resources = checker_resource_summary(root, sampler_exit, OBSERVER_LIMIT)
    require_checker_resources(resources)
    quiet = quiet_evidence(root, final, expected, sender, cut, state)
    anchor = read(root / "deadline-anchor.json")
    require(
        anchor["stop_mono_ns"] - anchor["anchor_mono_ns"]
        == schedule["stop_ns"] - anchor["anchor_unix_ns"],
        "scheduled stop differs from retained clock anchor",
    )
    sent = sent_keys(sender)
    accepted = set(sender["acknowledged"])
    require(
        read(root / "sender-exit.json")["exec_exit"]
        == (1 if root.name == CAPPED and sender.get("schema") == DISPATCH_JOURNAL else 0)
        and sender["closed"]
        and not sender["torn_suffix"]
        and not (
            sender["outside_slot"]
            or sender["remaining_intents"]
            or sender["unresolved"]
            or [
                row
                for row in sender.get("timing_exceptions", ())
                if root.name != CAPPED or row["reason"] != "capacity-missed"
            ]
        )
        and sent == accepted == set(admitted),
        "control sends, acknowledgments and checked admissions do not reconcile",
    )
    if phase_parity:
        require(
            root.name == HEALTHY
            and sender["schema"] == DISPATCH_JOURNAL
            and sender["all_accepted_verdict"] == "PASS"
            and len(admitted) == schedule["count"],
            "phase parity requires a fully delivered precommit healthy episode",
        )
        legacy_expected, legacy_sender = legacy_phase_projection(raw, schedule, expected, sender)
        legacy_checked = checked_results(
            report, legacy_expected, legacy_sender, witness, polls, width, final["upper_unix_ns"]
        )
        require(
            legacy_expected == expected and legacy_checked == checked,
            "legacy and precommit winning-attempt phase joins differ",
        )
    attempts = Counter(
        row["workflow_id"]
        for row in report["trace"]["Snapshot"]["engine.transition_history"]["rows"]
        if row["reason"] == "ATTEMPT_CREATED"
    )
    rows = {
        r["workflow_id"]: {
            **r,
            "admitted": True,
            "status": "observed",
            "terminal_state": "SUCCEEDED",
            "attempts": attempts[r["workflow_id"]],
            "raw_end_to_end_ns": r["terminal_ns"] - r["scheduled_ns"],
            "end_to_end_ns": r["terminal_ns"] - r["scheduled_ns"] + width,
            "phase_sensitivity_ns": [width, width, width, 0],
        }
        for r in checked
    }
    return {
        "schedule": schedule,
        "expected": expected,
        "sender": sender,
        "plan": plan,
        "journal": journal,
        "rows": rows,
        "quiet": quiet,
        **({"phase_join_parity": "PASS"} if phase_parity else {}),
        "report": report,
        "cut": cut,
        "state": state,
        "width": width,
        "witness": witness,
        "sent": sent,
        "checker_peak_rss_kib": resources["checker_peak_rss_kib"],
        "checker_vmhwm_peak_kib": resources["checker_vmhwm_peak_kib"],
    }


def cohort_delivery(episode):
    schedule, expected, rows = (episode[k] for k in ("schedule", "expected", "rows"))
    start, middle, end = (schedule[k] for k in ("origin_ns", "warmup_end_ns", "stop_ns"))
    admitted = set(episode["sender"]["durable_admissions"])
    require(admitted == rows.keys(), "cohort admissions and checked results differ")
    result = {}
    for name, lo, hi in (("warmup", start, middle), ("measured", middle, end)):
        keys = {key for key, e in expected.items() if lo <= e["scheduled_ns"] < hi}
        completed = [rows[key] for key in sorted(keys & admitted)]
        phases = summarize(completed)
        p99 = phases["end_to_end_p99_workflow"]
        result[name] = {
            "offered": len(keys),
            "sent": len(keys & episode["sent"]),
            "admitted": len(completed),
            "terminal": len(completed),
            "succeeded": len(completed),
            "phases": phases,
            "end_to_end_p99_phase_ns": p99["phase_ns"] if p99 is not None else None,
            "in_window_success": [
                row["workflow_id"] for row in completed if lo <= row["terminal_ns"] < hi
            ],
            "carry_in_success": [
                key
                for key, row in sorted(rows.items())
                if row["scheduled_ns"] < lo <= row["terminal_ns"] < hi
            ],
            "post_window_success": [
                row["workflow_id"] for row in completed if row["terminal_ns"] >= hi
            ],
        }
    return result


def gate_control(root, episode):
    schedule, sender, rows = (episode[name] for name in ("schedule", "sender", "rows"))
    valid = sender.get("all_accepted_verdict", sender["stock_verdict"]) == "PASS"
    gate = bins_gate(
        [
            rows.get(
                key, {"workflow_id": key, "scheduled_ns": item["scheduled_ns"], "status": "unsent"}
            )
            for key, item in episode["expected"].items()
        ],
        gates_samples(read(root / "prometheus-samples.json")),
        schedule["warmup_end_ns"],
        schedule["stop_ns"],
        valid,
    )
    if not gate["complete_inputs"]:
        raise ResourceEvidenceError(f"{root.name} has incomplete measured gate inputs")
    return gate


def capped_control(episode, proxy):
    sender, plan, journal = (episode[k] for k in ("sender", "plan", "journal"))
    precommit = sender.get("schema") == DISPATCH_JOURNAL
    verdict_field = "all_accepted_verdict" if precommit else "stock_verdict"
    shedding = sum(row["status"] == 429 for row in journal)
    finished = {row["key"]: row for row in journal}
    misses = {
        key
        for key, row in finished.items()
        if row["error"] == "capacity-missed"
        and row["outcome"] == "unsent"
        and (
            row["transport_entry_ns"] is None and row["request_written_ns"] is None
            if precommit
            else row["sent"] is None and row["attempts"] == 0
        )
    }
    require(
        episode["schedule"]["cap"] == 1
        and sender["max_in_flight_from_journal"] == 1
        and sender[verdict_field] == "FAIL"
        and misses == set(sender["explicitly_unsent"])
        and not shedding
        and set(sender["durable_admissions"]) == episode["rows"].keys()
        and proxy
        and proxy[0]["key"] == plan[0]["key"]
        and len(proxy) == len(sender["durable_admissions"])
        and {row["key"] for row in proxy} == set(sender["durable_admissions"]),
        "capped contacts, real CAPACITY_MISSED rows or stock verdict differ",
    )
    first = proxy[0]
    require(
        first["release_mono_ns"] - first["upstream_end_mono_ns"] >= HOLD_NS
        and first["released_mono_ns"] - first["entered_mono_ns"] < 10 * SECOND
        and first["upstream_end_unix_ns"] - first["entered_unix_ns"]
        < first["release_unix_ns"] - first["entered_unix_ns"],
        "first acceptance was not held for 3 s inside the 10 s timeout",
    )
    held = set()
    elapsed_deadlines = set()
    for row, following in zip(plan, plan[1:], strict=False):
        key = row["key"]
        if key not in misses:
            continue
        at, cutoff = utc_ns(row["scheduled_at"]), utc_ns(following["scheduled_at"])
        if first["upstream_end_unix_ns"] <= at < first["release_unix_ns"]:
            held.add(key)
        if first["upstream_end_unix_ns"] < cutoff <= first["release_unix_ns"]:
            elapsed_deadlines.add(key)
    require(
        held == misses and len(elapsed_deadlines & held) >= 2, "misses outside the occupied slot"
    )
    for i, row in enumerate(proxy):
        key = row["key"]
        reply = base64.b64decode(row["upstream_body_base64"], validate=True)
        require(
            row["request_sha256"] == episode["expected"][key]["request_hash"]
            and finished[key]["outcome"] == "accepted"
            and (
                episode["schedule"]["origin_ns"] + finished[key]["transport_entry_ns"]
                if precommit
                else utc_ns(finished[key]["sent"])
            )
            <= row["entered_unix_ns"]
            and row["upstream_status"] == row["downstream_status"] == finished[key]["status"]
            and 200 <= row["upstream_status"] < 300
            and json.loads(reply)["workflow"]["workflow_id"] == key
            and hashlib.sha256(reply).hexdigest() == row["downstream_body_sha256"]
            and row["entered_mono_ns"]
            <= row["upstream_end_mono_ns"]
            <= row["release_mono_ns"]
            <= row["released_mono_ns"]
            and (not i or proxy[i - 1]["released_mono_ns"] <= row["entered_mono_ns"]),
            f"proxy forwarding, response or slot proof differs: {key}",
        )
    return {
        "status": "sender_invalid",
        verdict_field: "FAIL",
        "capacity_missed": sorted(misses),
        "deadlines_elapsed_during_hold": sorted(elapsed_deadlines & held),
        "durable_admissions": sender["durable_admissions"],
        "checked": sorted(episode["rows"]),
        "shedding_count": shedding,
        "saturation_attribution": None,
    }


def worker_spec(application):
    return next(
        doc["spec"]
        for doc in application
        if doc["kind"] == "Deployment" and doc["metadata"]["name"] == "worker"
    )


def worker_comparison(base, delayed):
    def without_delay(spec):
        copy = deepcopy(spec)
        (container,) = copy["template"]["spec"]["containers"]
        (delay,) = (env for env in container["env"] if env["name"] == "DUR048_ACTIVITY_DELAY_MS")
        return delay.pop("value"), copy

    before, baseline = without_delay(base)
    after, treatment = without_delay(delayed)
    require((before, after) == ("100", "2100") and baseline == treatment, "worker specs drifted")
    return {"setting": "DUR048_ACTIVITY_DELAY_MS", "before": before, "after": after}


def paired_delays(base, delayed):
    paired = []
    for a, b in zip(base["plan"], delayed["plan"], strict=True):
        require(a["sequence"] == b["sequence"] and a["activity"] == b["activity"], "mix drifted")
        if a["activity"] != "dur048.sleep":
            continue
        first, second = base["rows"][a["key"]], delayed["rows"][b["key"]]
        paired.append(
            {
                "sequence": a["sequence"],
                "baseline": first["workflow_id"],
                "delayed": second["workflow_id"],
                "phase_delta_ns": [
                    y - x for x, y in zip(first["phase_ns"], second["phase_ns"], strict=True)
                ],
                "raw_end_to_end_delta_ns": second["raw_end_to_end_ns"] - first["raw_end_to_end_ns"],
            }
        )
    require(len(paired) == 24, "missing paired sleep workflows")
    phases = [median(row["phase_delta_ns"][i] for row in paired) for i in range(4)]
    end_to_end = median(row["raw_end_to_end_delta_ns"] for row in paired)
    require(
        all(
            abs(x - goal) <= SECOND // 2
            for x, goal in zip(phases, (0, 0, DELAY_NS, 0), strict=True)
        )
        and abs(end_to_end - DELAY_NS) <= SECOND // 2,
        f"paired phase or end-to-end shift differs: {phases}, {end_to_end}",
    )
    return {"paired": paired, "median_phase_ns": phases, "median_raw_end_to_end_ns": end_to_end}


def fixture_join(fixture):
    first = {}
    for poll in fixture["polls"]:
        for key in poll["terminal_ids"]:
            first.setdefault(key, poll["upper_unix_ns"])
    return list(
        join(
            fixture["planned"],
            fixture["snapshot"],
            fixture["events"],
            first,
            fixture["clock_uncertainty_ns"],
            max(row["upper_unix_ns"] for row in fixture["polls"]),
        )
    )


def delay_fixture(source, snapshot, polls, samples):
    key = next(r["key"] for r in source["plan"] if r["activity"] == "dur048.sleep")
    event_ids = {
        row["event_id"]
        for row in snapshot["Snapshot"]["engine.outbox"]["rows"]
        if row["workflow_id"] == key
    }
    base = {
        "planned": [[key, source["expected"][key]["scheduled_ns"]]],
        "snapshot": {
            "Snapshot": {
                name: {
                    "rows": [
                        row
                        for row in table["rows"]
                        if row.get("workflow_id") == key or row.get("event_id") in event_ids
                    ]
                }
                for name, table in snapshot["Snapshot"].items()
            },
            "Submissions": [row for row in snapshot["Submissions"] if row["WorkflowID"] == key],
        },
        "events": [
            event for event in witness_events(source["witness"]) if event["workflow_id"] == key
        ],
        "polls": [{**p, "terminal_ids": [key] if key in p["terminal_ids"] else []} for p in polls],
        "samples": samples,
        "clock_uncertainty_ns": source["width"],
    }
    (before,) = fixture_join(base)
    require(before["status"] == "observed", "raw delay fixture baseline did not join")
    delayed = deepcopy(base)
    tables = delayed["snapshot"]["Snapshot"]
    boundaries = tables["engine.transition_history"]["rows"]
    win = before["phase_boundary_ns"][3]
    (winner,) = (
        row
        for row in boundaries
        if row["reason"] == "ATTEMPT_RESULT_RECORDED"
        and utc_ns(row["created_at"]) == win
        and attempt_key(row) == before["winning_attempt"]
        and row["revision"] < before["terminal_candidates"][0]["revision"]
    )

    def shift(row, names):
        for name in names:
            value = row.get(name)
            if value is not None and utc_ns(value) >= win:
                row[name] = timestamp(utc_ns(value) + DELAY_NS)

    for name, table in tables.items():
        for row in table["rows"]:
            names = (
                "updated_at",
                "published_at",
                "recorded_at",
                "consumed_at",
                "next_attempt_at",
                "finished_at",
                "accepted_at",
            )
            if name == "engine.transition_history":
                if row["revision"] >= winner["revision"]:
                    shift(row, ("created_at",))
            elif name in (
                "engine.outbox",
                "engine.attempt_result_evidence",
                "engine.scheduler_wakeups",
            ):
                shift(row, (*names, "created_at"))
            else:
                shift(row, names)
    for event in delayed["events"]:
        if event["event"] == "end" and utc_ns(event["timestamp"]) >= win:
            shift(event, ("timestamp",))
            event["monotonic_ns"] += DELAY_NS
    for poll in delayed["polls"]:
        if key in poll["terminal_ids"]:
            for bound in ("lower_unix_ns", "upper_unix_ns", "lower_mono_ns", "upper_mono_ns"):
                poll[bound] += DELAY_NS
    (after,) = fixture_join(delayed)
    phase_delta = [b - a for a, b in zip(before["phase_ns"], after["phase_ns"], strict=True)]
    e2e_delta = after["raw_end_to_end_ns"] - before["raw_end_to_end_ns"]
    require(
        after["status"] == "observed"
        and before["workflow_id"] == after["workflow_id"] == key
        and before["winning_attempt"] == after["winning_attempt"]
        and len(base["polls"]) == len(delayed["polls"])
        and delayed["samples"] == base["samples"]
        and phase_delta == [0, 0, DELAY_NS, 0]
        and e2e_delta == DELAY_NS,
        "raw fixture did not isolate exact 2 s to execution",
    )
    return {
        "baseline-input": base,
        "delayed-input": delayed,
        "baseline-output": before,
        "delayed-output": after,
        "verdict": {
            "phase_delta_ns": phase_delta,
            "raw_end_to_end_delta_ns": e2e_delta,
            "identities": 1,
            "polls": len(polls),
            "samples": len(samples),
        },
    }


def withheld_terminal(episode):
    report, state, sender, expected = (
        episode[name] for name in ("report", "state", "sender", "expected")
    )
    checked = report["workflow_ids"]
    revisions = {row["id"]: row["revision"] for row in state["db"]["submissions"]}
    history = report["trace"]["Snapshot"]["engine.transition_history"]["rows"]
    baseline = [
        row
        for row in history
        if row["new_state"] == "SUCCEEDED" and revisions.get(row["workflow_id"]) == row["revision"]
    ]
    missing, replacement = sorted(sender["acknowledged"])[:2]
    removed = [row for row in baseline if row["workflow_id"] != missing]
    duplicated = [*removed, next(row for row in removed if row["workflow_id"] == replacement)]
    verdicts = {}
    for name, rows in (("baseline", baseline), ("removed", removed), ("duplicated", duplicated)):
        ids = [row["workflow_id"] for row in rows]
        identities = drain_identities(revisions, sender["acknowledged"], ids, checked)
        cut = {**state, "db": {**state["db"], "succeeded": ids}}
        try:
            predicates = readiness(cut, expected, sender)
        except IntegrityError as error:
            require(
                name == "duplicated"
                and str(error) == "duplicated or unexpected final-revision success",
                "unexpected integrity stop in withheld-terminal control",
            )
            verdict = {"classification": error.classification, "reason": str(error)}
        else:
            require(name != "duplicated", "duplicate success did not trigger an integrity stop")
            require(predicates["ready"] == (name == "baseline"), "withheld readiness differs")
            verdict = {"classification": "pass"}
            if name == "removed":
                # The checker input stays intact. This is a downstream
                # mutation of its terminal read, under the expiry classifier.
                deadline = DRAIN_SECONDS * SECOND
                mutated = {**cut, "lower_mono_ns": deadline, "upper_mono_ns": deadline}
                try:
                    expire_drain(
                        0,
                        deadline,
                        (cut, predicates),
                        lambda mutated=mutated: mutated,
                        expected,
                        sender,
                        lambda *_: None,
                        now=lambda deadline=deadline: deadline,
                    )
                except DrainExpired as error:
                    require(
                        error.result["missing"] == [missing]
                        and error.result["check_status"] == "PASS",
                        "withheld read did not produce the expected expiry residual",
                    )
                    verdict = {"classification": error.classification}
        require(
            identities["missing"] == ([] if name == "baseline" else [missing])
            and identities["duplicated"] == ([replacement] if name == "duplicated" else [])
            and not identities["unexpected"]
            and len(rows) == len(baseline) - (name == "removed"),
            "withheld and duplicate terminal evidence does not match the specified cause",
        )
        verdicts[name] = {**identities, **verdict}
    return {
        "baseline": baseline,
        "removed": removed,
        "duplicated": duplicated,
        "verdicts": verdicts,
    }


def checked_expiry(episode):
    """Delay visibility of a genuine checked cut until less than 30 seconds remain."""
    state, expected, sender = (episode[k] for k in ("state", "expected", "sender"))
    stop = 0
    deadline = DRAIN_SECONDS * SECOND
    expose = deadline - QUIET_NS // 2
    bracket_width = state["upper_mono_ns"] - state["lower_mono_ns"]
    require(0 <= bracket_width < QUIET_NS // 2, "source cut is too wide for expiry fixture")
    clock = stop
    trace = []
    checks = []

    def read_cut():
        nonlocal clock
        bracket = {**episode["cut"], "lower_mono_ns": clock, "upper_mono_ns": clock + bracket_width}
        clock = bracket["upper_mono_ns"]
        return checked_cut(episode["report"], bracket, state["witness"])

    def observe():
        if clock < expose:
            raise TimeoutError("fixture withholds visibility of the locked checker cut")
        return read_cut()

    def check(cut):
        require(readiness(cut, expected, sender)["ready"], "checked expiry cut was not ready")
        checks.append(cut)

    def sleep(seconds):
        nonlocal clock
        clock += round(seconds * SECOND)

    try:
        quiet_drain(
            stop_ns=stop,
            observe=observe,
            check=check,
            check_expiry=read_cut,
            expected=expected,
            sender=sender,
            retain=lambda event, data: trace.append({"event": event, **data}),
            now=lambda: clock,
            sleep=sleep,
        )
    except DrainExpired as error:
        result = error.result
    else:
        raise ValueError("late checked cut unexpectedly passed quiet")
    quiet = [event for event in trace if event["event"] == "quiet-start"]
    require(
        len(checks) == len(quiet) == 1
        and 0 < deadline - quiet[0]["upper_mono_ns"] < QUIET_NS
        and result["check_status"] == "PASS"
        and result["classification"] == "drain_failure"
        and result["ready"]
        and not result["missing"]
        and not result["missing_acknowledged"]
        and result["drain_seconds"] == DRAIN_SECONDS,
        "checked expiry did not fail on the registered quiet interval",
    )
    return {"visible_after_ns": expose, "trace": trace, "verdict": result}
