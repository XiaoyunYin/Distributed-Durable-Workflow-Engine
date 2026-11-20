"""Independent confirmation, recovery and coverage predicates for Phase F2."""

from __future__ import annotations

import json
import re
from datetime import datetime
from hashlib import sha256

from dur053_k6_b_evidence import SmokeFailure
from dur053_k6_e1_join import (
    BASE,
    EVENTS,
    SCHEDULERS,
    TASKS,
    base_inputs,
    broker_records,
    by,
    expect,
    journal,
    one,
    relay_at_hold,
)
from dur053_k6_f1a_inputs import ns, terminal_winner

CELLS = ("medium-5", "medium-6", "medium-7")
PG_ROLES = ("api", "worker-control", "scheduler", "relay")
QUIET_NS = 30_000_000_000
PROBE_PREFIX = "dur053-k6-f2-probe-"
TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED", "COMPENSATED"}
UNCOVERED = {
    "I3": "No forced scheduler lease handoff and stale owner write",
    "I5": "No cooperating effect or sink receipt",
    "I7": "No offset-ack boundary intervention and complete lower-offset audit",
    "I8": "No timer workload",
    "I9": "No approval or grant workload",
    "I10": "No operator cancellation or reconciliation workload",
    "I12": "No retry after an uncertain external effect",
    "I13": "No clock or correlation completeness claim",
}


def docker_time(value: str) -> int:
    match = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?Z", value)
    if not match:
        raise SmokeFailure("F2_DOCKER_TIMESTAMP_INVALID")
    seconds = int(datetime.fromisoformat(match[1] + "+00:00").timestamp())
    return seconds * 10**9 + int((match[2] or "").ljust(9, "0"))


def container_row(raw: list[dict], name: str) -> dict:
    row = one("F2_DOCKER_INSPECT", raw)
    expect("F2_DOCKER_TARGET_NAME", row["Name"], "/" + name)
    expect("F2_DOCKER_TARGET_ID", bool(re.fullmatch(r"[a-f0-9]{64}", row["Id"])), True)
    state = row["State"]
    address = row["NetworkSettings"]["Networks"]["kind"]["IPAddress"]
    expect("F2_DOCKER_TARGET_RUNNING", (state["Running"], state["Status"]), (True, "running"))
    expect("F2_DOCKER_KIND_ADDRESS", bool(address), True)
    return {
        "id": row["Id"],
        "ip": address,
        "started_at": state["StartedAt"],
        "started_ns": docker_time(state["StartedAt"]),
        "exit_code": state["ExitCode"],
        "stop_signal": row["Config"].get("StopSignal") or "SIGTERM (Docker default)",
    }


def lifecycle(raw: str, container: str, issued: int, completed: int) -> dict:
    try:
        rows = [json.loads(line) for line in raw.splitlines()]
    except ValueError:
        raise SmokeFailure("F2_DOCKER_EVENTS_INVALID") from None
    expect(
        "F2_DOCKER_EVENT_TARGET",
        {r["Actor"]["ID"]: r["Type"] for r in rows},
        {container: "container"},
    )
    events = [r for r in rows if issued <= r["timeNano"] <= completed]
    die = one("F2_DOCKER_DIE", by(events, Action="die"))
    start = one("F2_DOCKER_START", by(events, Action="start"))
    expect("F2_DOCKER_EVENT_ORDER", die["timeNano"] < start["timeNano"], True)
    return {"die": die, "start": start, "exit_code": int(die["Actor"]["Attributes"]["exitCode"])}


def runtime_start(raw: str) -> dict:
    rows = []
    for line in raw.splitlines():
        stamp, _, body = line.partition(" ")
        if " INFO runtime started " in body:
            rows.append({"timestamp": stamp, "at_ns": docker_time(stamp), "message": body})
    return one("F2_TIMESTAMPED_RUNTIME_START", rows)


def marks(topics: dict) -> dict:
    expect("F2_ENGINE_TOPIC_INVENTORY", sorted(topics), sorted((EVENTS, TASKS)))
    result = {}
    for name, sample in topics.items():
        partitions = sample["value"]
        broker_records(partitions, 8)
        expect(
            "F2_ENGINE_PARTITION_INVENTORY",
            sorted(r["partition"] for r in partitions),
            list(range(8)),
        )
        result[name] = {str(r["partition"]): r["next"] for r in partitions}
    return result


def retained_marks(before: dict, after: dict) -> None:
    for topic, partitions in before.items():
        for part, offset in partitions.items():
            expect("F2_KAFKA_HIGH_WATER_REGRESSED", after[topic][part] >= offset, True)
    expect(
        "F2_KAFKA_NO_RETAINED_LOG_CHALLENGE", any(any(m.values()) for m in before.values()), True
    )


def healthy(sample: dict, topic: str) -> None:
    value = sample["value"]
    expect(
        "F2_PROBE_ISOLATED_TOPIC", (value["topic"], topic.startswith(PROBE_PREFIX)), (topic, True)
    )
    expect(
        "F2_PROBE_HEALTHY_BROKER",
        (sample["exit_code"], value["metadata"], value["produced"], value["read_back"]),
        (0, True, True, True),
    )
    expect("F2_PROBE_READBACK_ID", (value["nonce"], value["partition"]), (sample["nonce"], 0))
    expect("F2_PROBE_READBACK_OFFSET", type(value["offset"]) is int and value["offset"] >= 0, True)


def pending(cut: dict) -> set[str]:
    return {
        r["event_id"]
        for d in cut["workflows"].values()
        for r in d["outbox"]
        if r["published_at"] is None
    }


def published(cut: dict) -> dict:
    return {
        r["event_id"]: r["published_at"]
        for d in cut["workflows"].values()
        for r in d["outbox"]
        if r["published_at"] is not None
    }


def in_flight(cut: dict, workflow: str) -> dict:
    expect("F2_MONITOR_READ_ONLY", cut["read_only"], "on")
    row = cut["workflows"].get(workflow, {})
    state = row.get("workflow", {}).get("state")
    expect("F2_IN_FLIGHT_WORKFLOW", bool(state) and state not in TERMINAL, True)
    durable = row["outbox"] + row["attempts"]
    expect("F2_IN_FLIGHT_DURABLE_ROW", bool(durable), True)
    return {
        "state": state,
        "outbox_ids": [r["event_id"] for r in row["outbox"]],
        "attempt_tokens": [r["token_hash"] for r in row["attempts"]],
    }


def backlog(cut: dict, arrivals: list[dict]) -> list[str]:
    return [
        one(
            "F2_DURING_PENDING_OUTBOX",
            [
                r
                for r in cut["workflows"][offer["workflow"]]["outbox"]
                if r["event_type"] == "workflow.created" and r["published_at"] is None
            ],
        )["event_id"]
        for offer in arrivals
    ]


def relay_hold(fault: dict, arrivals: list[dict]) -> dict:
    arm, raw = fault["arm"], fault["journal"].encode()
    lines = [json.loads(line) for line in raw.splitlines()]
    # Reuse the existing byte/hash/sequence join for the durable ACK prefix.
    held_line = one("F2_RELAY_HELD", by(lines, arm=arm["arm"], event="held"))
    prefix = b"\n".join(raw.splitlines()[: held_line["sequence"]]) + b"\n"
    ack = journal(prefix, arm)
    actions = [row for row in by(lines, arm=arm["arm"]) if row["sequence"] > held_line["sequence"]]
    expect(
        "F2_RELAY_RELEASE_SEQUENCE",
        [row["event"] for row in actions],
        ["action", "command_issued", "release"],
    )
    action, issued, release = actions
    expect("F2_RELAY_RELEASE_ACK", [row["ack"] for row in actions], [arm["ack"]] * 3)
    expect(
        "F2_RELAY_RELEASE_COMMAND", [row["command"] for row in (action, issued)], ["release"] * 2
    )
    reach = ack["reach"]
    before, during, after = fault["relay"]
    expect("F2_RELAY_PROCESS_PERSISTED", (during, after), (before, before))
    expect(
        "F2_RELAY_HOOK_BINDING",
        (
            arm["episode"],
            reach["boundary"],
            reach["target"]["uid"],
            reach["target"]["process_start_id"],
        ),
        ("medium-5", "before_publish", before["uid"], before["process_start_id"]),
    )
    fields = reach["fields"]["objects"]
    expect(
        "F2_RELAY_HOOK_CELL",
        (fields["cell"], fields["owner"]),
        ("medium-5", before["session_uuid"]),
    )
    seed = fault["seed"]
    initial, held = fault["cuts"]
    relay_at_hold(
        "hook-0", seed, fields, initial["workflows"][seed], fault["marks_before"][EVENTS]["value"]
    )
    expect("F2_RELAY_PUBLISHED_ADVANCED", published(held), published(initial))
    expect(
        "F2_RELAY_HIGH_WATER_ADVANCED", marks(fault["marks_during"]), marks(fault["marks_before"])
    )
    grew = backlog(held, arrivals)
    expect(
        "F2_RELAY_PENDING_DID_NOT_GROW",
        pending(initial) < pending(held) and set(grew) <= pending(held) - pending(initial),
        True,
    )
    probe = fault["probe"]
    healthy(probe, fault["probe_topic"])
    start_ns = held_line["at_ns"]
    end_ns = release["at_ns"]
    bound_ns = arm["until_ms"] * 1_000_000
    expect(
        "F2_RELAY_HOLD_WINDOW",
        start_ns
        <= initial["started_ns"]
        <= held["at_ns"]
        <= action["at_ns"]
        <= issued["at_ns"]
        <= end_ns
        < bound_ns,
        True,
    )
    expect(
        "F2_RELAY_PROBE_OUTSIDE_HOLD",
        start_ns <= probe["started_ns"] <= probe["at_ns"] <= action["at_ns"],
        True,
    )
    expect(
        "F2_RELAY_MARKS_OUTSIDE_HOLD",
        all(
            start_ns <= r["started_ns"] <= r["observed_ns"] <= action["at_ns"]
            for pair in (fault["marks_before"], fault["marks_during"])
            for r in pair.values()
        ),
        True,
    )
    return {
        "intervention": in_flight(initial, seed),
        "issued_ns": start_ns,
        "recovered_ns": end_ns,
        "release_authorized_ns": action["at_ns"],
        "command_issued_ns": issued["at_ns"],
        "until_ns": bound_ns,
        "arm_id": arm["arm"],
        "hook_id": reach["hook_id"],
        "ack": arm["ack"],
        "pending_before": sorted(pending(initial)),
        "pending_during": sorted(pending(held)),
        "backlog_ids": grew,
        "high_water_before_and_during": marks(fault["marks_before"]),
        "probe": probe,
        "relay": before,
    }


def pg_retained(before: dict, after: dict) -> None:
    expect(
        "F2_PG_POSTMASTER_START_NOT_NEW",
        ns(after["postmaster_start"]) > ns(before["postmaster_start"]),
        True,
    )
    for name in ("workflows", "history"):
        expect(
            "F2_PG_RETAINED_COUNT",
            after["retained"][name]["count"],
            before["retained"][name]["count"],
        )
        expect(
            "F2_PG_RETAINED_HASH", after["retained"][name]["hash"], before["retained"][name]["hash"]
        )
        expect("F2_PG_EMPTY_RETAINED_CHALLENGE", before["retained"][name]["count"] > 0, True)


def restart_fault(cell: str, fault: dict, arrivals: list[dict]) -> dict:
    pg = cell == "medium-6"
    label = "PG" if pg else "KAFKA"
    before, after = [container_row(raw, fault["target"]) for raw in fault["inspect"]]
    expect("F2_" + label + "_ADDRESS_CHANGED", after["ip"], before["ip"])
    expect("F2_RESTART_CONTAINER_ID_CHANGED", after["id"], before["id"])
    expect("F2_RESTART_CONTAINER_START_NOT_NEW", after["started_ns"] > before["started_ns"], True)
    issued, completed = fault["issued_ns"], fault["completed_ns"]
    expect("F2_RESTART_COMMAND_EXIT", fault["exit_code"], 0)
    events = lifecycle(fault["events"], before["id"], issued, completed)
    die = events["die"]["timeNano"]
    samples = fault["samples"]
    expect("F2_RESTART_PROBE_FATAL", [s for s in samples if "fatal" in s], [])
    expect("F2_RESTART_SAMPLE_ORDER", samples, sorted(samples, key=lambda s: s["started_ns"]))
    pre = [s for s in samples if s["at_ns"] <= issued and s["ok"]]
    failed = [s for s in samples if issued <= s["at_ns"] and not s["ok"]]
    recovered = [s for s in samples if s["started_ns"] >= die and s["ok"]]
    expect("F2_RESTART_WATCH_BASELINE_MISSING", bool(pre), True)
    expect("F2_" + label + "_RECOVERY_SAMPLE_MISSING", bool(recovered), True)
    failed = [s for s in failed if s["started_ns"] < recovered[0]["started_ns"]]
    expect("F2_" + label + "_NO_OUTAGE_SAMPLE", bool(failed), True)
    cut = fault["before_cut"]
    expect("F2_RESTART_IN_FLIGHT_WINDOW", cut["at_ns"] <= issued <= cut["at_ns"] + 2 * 10**9, True)
    proof = {
        "intervention": in_flight(cut, fault["seed"]),
        "issued_ns": issued,
        "unavailable_ns": failed[0]["at_ns"],
        "recovered_ns": recovered[0]["at_ns"],
        "recovery_probe_started_ns": recovered[0]["started_ns"],
        "restart_completed_ns": completed,
        "container_before": before,
        "container_after": after,
        "lifecycle": events,
        "image_stop_signal": fault["image_stop_signal"],
        "restart_command_exit": fault["exit_code"],
        "stop_timeout_seconds": fault["stop_timeout_seconds"],
        "samples": len(samples),
        "failed_samples": len(failed),
        "outage_samples": failed,
        "recovery_sample": recovered[0],
        "backlog_ids": [],
    }
    if pg:
        old, new = fault["postgres"]
        pg_retained(old, new)
        expect(
            "F2_PG_SQL_QUERY_ACCEPTED",
            (recovered[0]["value"]["read_only"], recovered[0]["value"]["postmaster_start"]),
            ("on", new["postmaster_start"]),
        )
        old_apps = {r["application"] for r in old["backends"]}
        new_apps = {r["application"] for r in new["backends"]}
        applications = {"durable-" + role for role in PG_ROLES}
        expect("F2_PG_ENGINE_BACKENDS", (old_apps, new_apps), (applications, applications))
        expect(
            "F2_PG_ENGINE_BACKENDS_RECONNECTED",
            all(ns(r["backend_start"]) >= ns(new["postmaster_start"]) for r in new["backends"]),
            True,
        )
        engines = []
        for role, (old_pods, new_pods) in fault["engines"].items():
            expect("F2_PG_POD_PROCESSES", bool(old_pods) and bool(new_pods), True)
            engines.append(
                {
                    "role": role,
                    "before": old_pods,
                    "after": new_pods,
                    "persisted": [p for p in new_pods if p in old_pods],
                }
            )
        expect("F2_PG_RUNTIME_ROLES", sorted(fault["engines"]), sorted(PG_ROLES))
        proof.update(postgres_before=old, postgres_after=new, engine_processes=engines)
    else:
        old, new = fault["broker_process"]
        expect("F2_KAFKA_PROCESS_START_NOT_NEW", new["start_ticks"] > old["start_ticks"], True)
        for sample in (pre[-1], recovered[0]):
            healthy(sample, fault["probe_topic"])
        expect(
            "F2_KAFKA_OUTAGE_STAGE",
            any(not s["value"]["metadata"] or not s["value"]["produced"] for s in failed),
            True,
        )
        before_marks, after_marks = [marks(data) for data in fault["marks"]]
        retained_marks(before_marks, after_marks)
        pending_cut = fault["pending_cut"]
        expect(
            "F2_KAFKA_BACKLOG_OUTAGE_BRACKET",
            any(
                s["at_ns"] >= pending_cut["at_ns"]
                and (not s["value"]["metadata"] or not s["value"]["produced"])
                for s in failed
            ),
            True,
        )
        expect(
            "F2_KAFKA_BACKLOG_DURING_OUTAGE",
            failed[0]["started_ns"]
            <= pending_cut["started_ns"]
            <= pending_cut["at_ns"]
            <= recovered[0]["started_ns"],
            True,
        )
        proof.update(
            broker_before=old,
            broker_after=new,
            high_water_before=before_marks,
            high_water_after=after_marks,
            backlog_ids=backlog(pending_cut, arrivals),
        )
    return proof


def coverage(predicates: dict) -> dict:
    covered = [name for name in BASE if predicates.get(name)]
    expect("F2_B_COVERAGE_INCOMPLETE", (covered, sorted(predicates)), (list(BASE), sorted(BASE)))
    return {
        "invariants_covered": covered,
        "invariants_not_covered": list(UNCOVERED),
        "outcomes": {
            **{name: {"status": "covered/pass", "witnesses": predicates[name]} for name in covered},
            **{
                name: {"status": "NOT COVERED", "reason": reason}
                for name, reason in UNCOVERED.items()
            },
        },
    }


def quiet_digest(cut: dict, witness: list[dict]) -> str:
    value = {"workflows": cut["workflows"], "witness": witness}
    return sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def join(inputs: dict) -> dict:
    """Compute the cell verdict from observations, including every accepted workflow."""
    try:
        cell, fault, offers = (inputs[key] for key in ("cell", "fault", "offers"))
        expect("F2_REGISTERED_CELL", cell in CELLS, True)
        seed, *arrivals = offers
        expect("F2_OFFER_POPULATION", (seed["workflow"], len(arrivals)), (fault["seed"], 2))
        proof = (
            relay_hold(fault, arrivals)
            if cell == "medium-5"
            else restart_fault(cell, fault, arrivals)
        )
        for offer in arrivals:
            expect(
                "F2_OFFER_NOT_DURING_FAULT",
                any(
                    proof.get("unavailable_ns", proof["issued_ns"])
                    <= r["started_ns"]
                    < proof.get("recovery_probe_started_ns", proof["recovered_ns"])
                    for r in offer["samples"]
                ),
                True,
            )
        final = inputs["terminal"]
        expect("F2_CLOSEOUT_READ_ONLY", final["read_only"], "on")
        accepted = {offer["workflow"]: offer for offer in offers}
        expect(
            "F2_ACCEPTED_CENSUS",
            (len(accepted), sorted(final["workflows"])),
            (len(offers), sorted(accepted)),
        )
        expect("F2_OUTBOX_NOT_DRAINED", sorted(pending(final)), [])
        expect(
            "F2_BACKLOG_NOT_PUBLISHED", set(proof["backlog_ids"]) <= published(final).keys(), True
        )
        backlog_boundary = (
            proof["recovered_ns"]
            if cell == "medium-5"
            else fault.get("pending_cut", {}).get("at_ns", proof["issued_ns"])
        )
        expect(
            "F2_BACKLOG_PUBLISHED_TOO_EARLY",
            all(ns(published(final)[e]) >= backlog_boundary for e in proof["backlog_ids"]),
            True,
        )
        records = {
            name: broker_records(sample["value"], 8) for name, sample in inputs["topics"].items()
        }
        marks(inputs["topics"])
        predicates: dict[str, dict] = {name: {} for name in BASE}
        dispositions = []
        rows = inputs["witness"]
        for workflow, offer in accepted.items():
            receipt = offer["samples"][-1]
            expect(
                "F2_ACCEPTED_RESPONSE",
                (receipt["workflow_id"], receipt["status"], receipt["created"]),
                (workflow, 201 if receipt["created"] else 200, receipt["created"]),
            )
            cut = final["workflows"][workflow]
            expect(
                "F2_ACCEPTED_WORKFLOW_NAMESPACE", cut["workflow"]["namespace"], inputs["namespace"]
            )
            winner = terminal_winner(
                cut, rows, inputs["workers"], inputs["namespace"], inputs["marker"]
            )
            invocation = winner["invocation"]["id"]
            witness = [
                {
                    "event": "start",
                    "invocation_id": invocation,
                    "token_hash": winner["attempt"]["token_hash"],
                },
                {
                    "event": "end",
                    "invocation_id": invocation,
                    "outcome": winner["attempt"]["state"],
                },
            ]
            base = base_inputs(cell, workflow, offer["fingerprint"], cut, witness)["predicates"]
            for invariant, evidence in base.items():
                predicates[invariant][workflow] = evidence
            for row in by(cut["inbox"], disposition="ACCEPTED"):
                record = one(
                    "F2_DISPOSITION_BROKER_OFFSET",
                    by(
                        records[row["topic"]],
                        partition=row["kafka_partition"],
                        offset=row["kafka_offset"],
                        event=row["event_id"],
                    ),
                )
                expect(
                    "F2_DISPOSITION_BROKER_WORKFLOW",
                    (record["key"], record["workflow"]),
                    (row["event_id"], workflow),
                )
                dispositions.append(row)
            one(
                "F2_USEFUL_CREATED_DISPOSITION",
                by(
                    cut["inbox"],
                    event_id=one(
                        "F2_CREATED_EVENT", by(cut["outbox"], event_type="workflow.created")
                    )["event_id"],
                    consumer_id=SCHEDULERS,
                    disposition="ACCEPTED",
                ),
            )
        quiet = inputs["quiet"]
        expected_digest = quiet_digest(final, rows)
        expect("F2_QUIET_CHANGED", {s["digest"]: True for s in quiet}, {expected_digest: True})
        expect("F2_QUIET_TOO_SHORT", quiet[-1]["started_ns"] - quiet[0]["at_ns"] >= QUIET_NS, True)
        expect("F2_QUIET_TAIL_BINDING", quiet[-1]["cut_at_ns"], final["at_ns"])
        derived = coverage(predicates)
        expect(
            "F2_REGISTRY_COVERAGE_MISMATCH",
            (derived["invariants_covered"], sorted(derived["invariants_not_covered"])),
            (
                inputs["registered"]["invariants_covered"],
                sorted(inputs["registered"]["invariants_not_covered"]),
            ),
        )
        return {
            "cell": cell,
            "status": "PASS",
            "confirmation": "CONFIRMED",
            **derived,
            "fault": proof,
            "accepted": sorted(accepted),
            "dispositions": dispositions,
            "pending_final": 0,
            "quiet": quiet,
            "final_high_water": marks(inputs["topics"]),
        }
    except SmokeFailure as error:
        if str(error).startswith("F2_"):
            raise
        raise SmokeFailure(f"F2_EVIDENCE_JOIN {error}") from None
    except (IndexError, KeyError, TypeError, ValueError) as error:
        raise SmokeFailure(f"F2_EVIDENCE_SHAPE {error}") from None
