"""Replay a full-clock engineering C fixture through the locked snapshot checker."""

import argparse
import base64
import hashlib
import os
import subprocess
import tempfile
from bisect import bisect_left
from collections import defaultdict
from pathlib import Path

from j2_checker import verify_binary
from j2_checker_report import indexed_report, require_checker_exit, require_checker_pass
from j2_drain import (
    checked_cut,
    cohort_counts,
    expected_arrivals,
    fingerprint,
    json_bytes,
    readiness,
    replay_episode,
)
from j2_harness import hosted_dispatch
from j2_harness_evidence import seal
from j2_integrated_evidence import registered_boundary, summarize_episode
from j2_integrated_rehearsal import episode_schedule
from j2_integrated_report import verify_episode_reduction
from j2_offline import SECOND, ClockEvidenceError
from j2_qualification_evidence import DRAIN_SECONDS, file_record, read, require, write
from j2_schedule import k4_report, schedule_slots, timestamp

ORIGIN = 1_800_000_000 * SECOND
BACKLOG_START = 500 * SECOND
TERMINAL_DELAY = 150_000_000
TOPIC = "durable-agent.events.v1"


def write_lines(path, records):
    with path.open("wb") as stream:
        for record in records:
            stream.write(json_bytes(record) + b"\n")


def make_plan(schedule, slots):
    rows = []
    for i, (at, _) in enumerate(slots):
        run, seed, namespace = (schedule[k] for k in ("run_id", "seed", "namespace"))
        key = f"dur053-{run}-{seed}-{i:06d}"
        activity = {8: "pure.echo", 9: "pure.add"}.get(i % 10, "dur048.sleep")
        result = 5 if activity == "pure.add" else {"run_id": run, "sequence": i}
        payload = [2, 3] if activity == "pure.add" else result
        request = {
            "definition_id": f"dur053-{run}-{activity}",
            "definition_version": 1,
            "initial_node_id": activity,
            "initial_input": payload,
            "payload": payload,
            "workflow_id": key,
            "submission_key": key,
            "namespace": namespace,
            "actor_id": "dur053-loadgen",
        }
        rows.append(
            dict(
                sequence=i,
                key=key,
                workflow_id=key,
                activity=activity,
                expected=result,
                scheduled_ns=at - ORIGIN,
                scheduled_at=timestamp(at),
                request=request,
            )
        )
    return {
        **{k: schedule[k] for k in ("namespace", "run_id", "seed")},
        "schema": "dur053.arrivals.v1",
        "status": "IN_PROGRESS",
        "arm": "short",
        "rate": float(schedule["rate"]),
        "concurrency": schedule["cap"],
        "offered": len(rows),
        "rows": rows,
    }


def journal(plan, stop):
    yield {"event": "plan", "plan": plan}
    for row in plan["rows"]:
        event = {
            "event": "arrival",
            "key": row["key"],
            "id": row["key"],
            "workflow": row["key"],
            "scheduled": row["scheduled_at"],
            "sent": row["scheduled_at"],
            "request_hash": hashlib.sha256(json_bytes(row["request"])).hexdigest(),
            "attempts": 1,
            "error": "",
        }
        yield {**event, "status": 0, "outcome": "uncertain"}
        yield {**event, "status": 201, "outcome": "accepted"}
    yield {"event": "stop", "stop": timestamp(stop)}
    yield {"event": "closed"}


def fixture_snapshot(plan, expected, *, overload):
    trace, tables = defaultdict(list), defaultdict(list)
    witnesses = [[{"event": "session"}] for _ in range(2)]
    terminal_polls = []
    for row in plan["rows"]:
        key, i = row["key"], row["sequence"]
        supplied = expected[key]
        at = supplied["scheduled_ns"]
        done = not overload or at < ORIGIN + BACKLOG_START
        state, revision = ("SUCCEEDED", 4) if done else ("RUNNABLE", 1)
        trace["Submissions"].append(
            dict(WorkflowID=key, Key=key, Namespace=supplied["namespace"], Hash=supplied["hash"])
        )
        trace["Workflows"].append(dict(WorkflowID=key, State=state, Revision=revision))
        tables["workflow_executions"].append(
            dict(
                workflow_id=key,
                submission_key=key,
                namespace=supplied["namespace"],
                submission_payload_hash=supplied["hash"],
                state=state,
                revision=revision,
            )
        )
        identity = dict(workflow_id=key, node_id=row["activity"], iteration=0, attempt_number=1)
        events = [("", "RUNNABLE", "WORKFLOW_CREATED", "api", 1_000_000, "workflow.created")]
        if done:
            events += [
                ("RUNNABLE", "WAITING_ACTIVITY", "ATTEMPT_CREATED", "scheduler", 10_000_000, None),
                (
                    "WAITING_ACTIVITY",
                    "RUNNABLE",
                    "ATTEMPT_RESULT_RECORDED",
                    "worker",
                    120_000_000,
                    "activity.result",
                ),
                (
                    "RUNNABLE",
                    "SUCCEEDED",
                    "GRAPH_ADVANCED",
                    "scheduler",
                    TERMINAL_DELAY,
                    "graph.advanced",
                ),
            ]
        for rev, (old, new, reason, actor, delay, event) in enumerate(events, 1):
            trace["History"].append(
                dict(
                    WorkflowID=key,
                    Revision=rev,
                    OldState=old,
                    NewState=new,
                    Reason=reason,
                    ActorKind=actor,
                    SchedulerEpoch=1 if actor == "scheduler" else None,
                )
            )
            tables["transition_history"].append(
                dict(
                    identity,
                    revision=rev,
                    new_state=new,
                    reason=reason,
                    created_at=timestamp(at + delay),
                )
            )
            if event:
                trace["Outbox"].append(
                    dict(
                        WorkflowID=key,
                        EventID=f"{key}/{rev}",
                        AggregateRevision=rev,
                        EventType=event,
                        Topic=TOPIC,
                        PublishState="PUBLISHED" if done else "PENDING",
                        PayloadValid=True,
                    )
                )
                tables["outbox"].append(
                    dict(
                        workflow_id=key,
                        aggregate_revision=rev,
                        event_type=event,
                        payload=row["expected"] if event == "activity.result" else {},
                        published_at=timestamp(at + delay) if done else None,
                    )
                )
        if done:
            trace["Attempts"].append(
                dict(
                    WorkflowID=key,
                    NodeID=row["activity"],
                    Iteration=0,
                    AttemptNumber=1,
                    State="SUCCEEDED",
                    ClaimToken=key,
                    ResultRecorded=True,
                )
            )
            trace["Results"].append(
                dict(WorkflowID=key, NodeID=row["activity"], Iteration=0, Accepted=True)
            )
            tables["activity_attempts"].append(
                dict(identity, claim_token=key, state="SUCCEEDED", result=row["expected"])
            )
            for event, offset in (("start", 20_000_000), ("end", 115_000_000)):
                witnesses[i % 2].append(
                    dict(
                        identity,
                        event=event,
                        invocation_id=key,
                        claim_token=key,
                        outcome="SUCCEEDED",
                        timestamp=timestamp(at + offset),
                        monotonic_ns=at - ORIGIN + offset,
                    )
                )
            terminal_polls.append(
                {"upper_unix_ns": at + TERMINAL_DELAY + 1_000_000, "terminal_ids": [key]}
            )
    trace["Snapshot"] = {
        f"engine.{name}": dict(
            rows=rows, count=len(rows), sha256=hashlib.sha256(json_bytes(rows)).hexdigest()
        )
        for name, rows in tables.items()
    }
    ledgers = []
    for i, events in enumerate(witnesses):
        pod = {"pod_uid": f"eng-fixture-pod-{i}", "session": f"eng-fixture-session-{i}"}
        data = b"".join(
            json_bytes(dict(event, **pod, sequence=j)) + b"\n" for j, event in enumerate(events, 1)
        )
        inventory = dict(
            pod,
            complete=True,
            errors=0,
            attempted=len(events),
            written=len(events),
            sha256=hashlib.sha256(data).hexdigest(),
        )
        ledgers.append({"inventory": inventory, "data": base64.b64encode(data).decode("ascii")})
    return (
        trace,
        {
            "expected_pods": [ledger["inventory"]["pod_uid"] for ledger in ledgers],
            "ledgers": ledgers,
        },
        terminal_polls,
    )


def run_checker(checker, path, output, *, gomemlimit, terminal):
    command = [str(checker), "-snapshot", str(path)]
    if terminal:
        command.append("-require-terminal")
    with output.open("wb") as stdout, output.with_suffix(".stderr").open("wb") as stderr:
        result = subprocess.run(
            command,
            stdout=stdout,
            stderr=stderr,
            timeout=240,
            check=False,
            env={
                **os.environ,
                "GOMEMLIMIT": gomemlimit,
                "GOMAXPROCS": "2",
                "J2_CHECKER_PHASES": "1",
            },
        )
    require_checker_exit(result.returncode)


def bracket(unix_ns):
    mono = unix_ns - ORIGIN
    return dict(
        lower_mono_ns=mono,
        upper_mono_ns=mono + 10_000,
        lower_unix_ns=unix_ns,
        upper_unix_ns=unix_ns + 10_000,
    )


def samples(schedule, slots, polls, *, overload):
    starts = [at for at, _ in slots]
    terminals = [poll["upper_unix_ns"] - 1_000_000 for poll in polls]
    cutoff = bisect_left(starts, ORIGIN + BACKLOG_START)
    series = defaultdict(list)
    for now in range(ORIGIN, schedule["stop_ns"] + 1, SECOND):
        admitted = bisect_left(starts, now)
        for name, value in {
            "up": 1,
            "j2_query_ok": 1,
            "j2_sample_timestamp_seconds": now // SECOND,
            "j2_outstanding": admitted - bisect_left(terminals, now),
            "j2_unpublished": max(0, admitted - cutoff) if overload else 0,
        }.items():
            series[name].append([now // SECOND, str(value)])
    return {name: [{"values": values}] for name, values in series.items()}


def build_case(root, checker, gomemlimit, *, campaign, overload):
    root.mkdir()
    schedule = episode_schedule(1, "C", "C", ORIGIN, campaign=campaign)
    stop = schedule["stop_ns"]
    slots = list(schedule_slots(schedule))
    plan = make_plan(schedule, slots)
    write(root / "schedule.json", schedule)
    write(root / "schedule-binding.json", file_record(root / "schedule.json"))
    write_lines(root / "sender-journal.jsonl", journal(plan, stop))
    raw = (root / "sender-journal.jsonl").read_bytes()
    expected, _ = expected_arrivals(raw, schedule)
    trace, witness, polls = fixture_snapshot(plan, expected, overload=overload)
    snapshot = root / "snapshot.json"
    snapshot.write_bytes(json_bytes(trace) + b"\n")
    del trace
    cut = root / "live-checker.json"
    run_checker(checker, snapshot, cut, gomemlimit=gomemlimit, terminal=not overload)
    checked, _ = indexed_report(cut, join_trace=True, verify_tables=True, drain_counts=True)
    require_checker_pass(checked)
    inventories = [ledger["inventory"] for ledger in witness["ledgers"]]
    read_at = stop + (DRAIN_SECONDS if overload else 1) * SECOND
    checked_read = {**bracket(read_at), "exit": 0}
    final_read = checked_read if overload else bracket(stop + 33 * SECOND)
    final = checked_cut(checked, final_read, inventories)
    del checked
    admitted = [row["id"] for row in final["db"]["submissions"]]
    sender = k4_report(raw, schedule, admitted)
    residuals = readiness(final, expected, sender)
    result = {
        **residuals,
        "status": "FAIL" if overload else "PASS",
        "check_status": "PASS",
        "deadline_mono_ns": stop - ORIGIN + DRAIN_SECONDS * SECOND,
        "drain_seconds": DRAIN_SECONDS if overload else 33.00001,
        "checked_export_seconds": (checked_read["upper_unix_ns"] - stop) / SECOND,
        "final_fingerprint": fingerprint(final),
    }
    if overload:
        result["classification"] = "drain_failure"
    else:
        quiet_start = {**final, **bracket(stop + 2 * SECOND)}
        write_lines(
            root / "drain-polls.jsonl",
            [
                {"event": "quiet-start", **quiet_start},
                {
                    "event": "poll",
                    **bracket(stop + 33 * SECOND),
                    "ready": True,
                    "fingerprint": fingerprint(final),
                },
            ],
        )
    write(root / "deadline-anchor.json", {"stop_mono_ns": stop - ORIGIN})
    write(root / "live-checker-read.json", checked_read)
    write(root / "final.json", final)
    write(root / "witness.json", witness)
    write_lines(root / "terminal-polls.jsonl", polls)
    write(
        root / "clock.json",
        {
            phase: {
                name: timestamp(at + i * 1000)
                for i, name in enumerate(("observer_before", "database", "observer_after"))
            }
            for phase, at in (("setup", ORIGIN - SECOND), ("measured", final["upper_unix_ns"]))
        },
    )
    write(root / "prometheus-samples.json", samples(schedule, slots, polls, overload=overload))
    write(root / "drain-result.json", result)
    write(root / "sender-report.json", sender)
    write(root / "journal-read.json", {"exit": 0})
    write(root / "sender-exit.json", {"exec_exit": 0})
    checked_rows, _ = replay_episode(root, snapshot_only=True)
    write(root / "phase-rows.json", checked_rows)
    write(root / "cohorts.json", cohort_counts(schedule, checked_rows))
    summary = summarize_episode(root, snapshot_only=True)
    write(root / "episode-replay.json", summary)
    write(root / "checkpoint.json", {"population": summary["population"]})


def verify_case(root, checker, gomemlimit, *, overload):
    second_report = root / "offline-checker.json"
    run_checker(
        checker, root / "snapshot.json", second_report, gomemlimit=gomemlimit, terminal=not overload
    )
    require(
        file_record(second_report) == file_record(root / "live-checker.json"),
        "independent locked checker report differs from the first snapshot check",
    )
    return verify_episode_reduction(root, snapshot_only=True)


def negative_clock_cases(source, destination):
    probes = read(source / "clock.json")
    broken = {**probes, "measured": {**probes["measured"], "database": probes["setup"]["database"]}}
    write(destination / "bad-clock.json", broken)
    outcomes = {}
    with tempfile.TemporaryDirectory(dir=destination) as scratch:
        view = Path(scratch)
        for path in source.iterdir():
            if path.name != "clock.json":
                (view / path.name).hardlink_to(path)
        for label in ("missing_clock", "bad_final_clock"):
            if label == "bad_final_clock":
                write(view / "clock.json", broken)
            try:
                verify_episode_reduction(view, snapshot_only=True)
            except ClockEvidenceError as error:
                outcomes[label] = {"claim_eligible": False, "reason": str(error)}
            else:
                raise ValueError(f"clock-negative fixture {label} did not reject the evidence")
    return outcomes


def exercise_fixture(output, checker, gomemlimit, campaign):
    cases = {}
    for name, overload in (("checked-expiry", True), ("quiet", False)):
        root = output / name
        build_case(root, checker, gomemlimit, campaign=campaign, overload=overload)
        summary = verify_case(root, checker, gomemlimit, overload=overload)
        grid = summary["cohorts"]["grid"]
        gate = grid["gate"]
        if overload:
            require(
                summary["drain"]["status"] == "drain_failure"
                and gate["status"] == "failed_gate"
                and gate["sustained_growth"]
                and gate["checks"] == {"4a": False, "4b": None, "4c": False}
                and bool(grid["phases"]["censored_keys"]),
                "wrong checked expiry verdict",
            )
        else:
            require(
                summary["drain"]["status"] == "PASS" and gate["status"] == "pass",
                "quiet comparison did not pass",
            )
        cases[name] = summary
    negative = negative_clock_cases(output / "quiet", output)
    # The lower-rate outcomes are labelled inputs to exercise the boundary reduction.
    boundary = registered_boundary(
        {**dict.fromkeys(("A", "AB", "B", "BC"), cases["quiet"]), "C": cases["checked-expiry"]}
    )
    require(boundary["saturation"] == "C", "censored gate failed to establish the fixture boundary")
    write(
        output / "fixture-report.json",
        {
            "status": "PASS",
            "campaign": campaign,
            "scope": "unmeasured offline engineering fixture",
            "method": "deterministic K4-shaped journal and checker-verified snapshot history",
            "boundary_with_passing_lower_inputs": boundary,
            "clock_negatives": negative,
            "cases": {
                name: {
                    "population": value["population"],
                    "drain": value["drain"]["status"],
                    "pending_count": len(value["drain"].get("pending", [])),
                    "gate": value["cohorts"]["grid"]["gate"]["status"],
                    "checks": value["cohorts"]["grid"]["gate"]["checks"],
                    "clock": value["clock"],
                    "independent_replay": "PASS",
                }
                for name, value in cases.items()
            },
        },
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("checker", type=Path)
    args = parser.parse_args()
    hosted_dispatch()
    lock = verify_binary(args.checker)
    args.output.mkdir(parents=True, exist_ok=True)
    write(args.output / "checker-lock.json", lock)
    campaign = f"eng-checked-cut-{os.environ['GITHUB_RUN_ID']}-{os.environ['GITHUB_RUN_ATTEMPT']}"
    exercise_fixture(args.output, args.checker.resolve(), lock["gomemlimit"], campaign)
    seal(args.output)


if __name__ == "__main__":
    main()
