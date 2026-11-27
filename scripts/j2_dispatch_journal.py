"""Independent replay for the precommitted J2 request-write journal."""

import hashlib
import json

from j2_checker_report import IntegrityError
from j2_offline import utc_ns
from j2_qualification_evidence import require

DISPATCH_JOURNAL = "j2.dispatch-journal.v1"


def raw_digest(lines):
    digest = hashlib.sha256()
    for line in lines:
        digest.update(line)
    return digest.hexdigest()


def check_intents(plan, events, slots, schedule):
    origin = schedule["origin_ns"]
    require(len(events) == len(slots) == plan["offered"], "precommit coverage differs")
    ready_at = None
    for i, (row, intent, (at, cutoff)) in enumerate(zip(plan["rows"], events, slots, strict=True)):
        key = f"dur053-{schedule['run_id']}-{schedule['seed']}-{i:06d}"
        request = json.dumps(row["request"], sort_keys=True, separators=(",", ":"))
        require(
            row["sequence"] == i
            and row["key"] == row["workflow_id"] == key
            and row["outcome"] == "NOT_SENT"
            and row["scheduled_ns"] == at - origin
            and utc_ns(row["scheduled_at"]) == at
            and intent["event"] == "intent"
            and intent["sequence"] == i
            and intent["key"] == intent["workflow_id"] == key
            and intent["at_ns"] == at - origin
            and intent["cutoff_ns"] == cutoff - origin
            and intent["request_sha256"] == raw_digest((request.encode(),))
            and type(intent["prepared_ns"]) is int
            and (ready_at is None or ready_at <= intent["prepared_ns"]),
            f"precommit intent or immutable plan differs: {key}",
        )
        ready_at = intent["prepared_ns"]
    return ready_at


def check_outcome(row, intent, cap):
    key = intent["key"]
    attempt = row["dispatch_intent_ns"]
    entry, write, refusal, done = (
        row[field]
        for field in ("transport_entry_ns", "request_written_ns", "refusal_ns", "completed_ns")
    )
    count, status, result = (row[field] for field in ("write_callbacks", "status", "outcome"))
    require(
        row["key"] == key
        and type(attempt) is int
        and (done is None or (type(done) is int and attempt <= done))
        and type(row["in_flight_at_release"]) is int
        and 0 <= row["in_flight_at_release"] <= cap
        and type(count) is int
        and count >= 0
        and type(status) is int
        and (status == 0 or 100 <= status <= 599)
        and isinstance(row["error"], str),
        f"invalid dispatch evidence: {key}",
    )
    ack = row.get("acknowledged_workflow_id")
    if result == "unsent":
        require(
            entry is None
            and write is None
            and count == status == 0
            and ack is None
            and type(refusal) is int
            and attempt <= refusal
            and (done is None or refusal <= done)
            and bool(row["error"]),
            f"invalid pre-transport refusal: {key}",
        )
        return row["error"]
    require(
        refusal is None
        and type(entry) is int
        and type(done) is int
        and attempt <= entry <= done
        and 1 <= row["in_flight_at_release"] <= cap
        and result in {"accepted", "rejected", "uncertain"}
        and (result != "accepted" or (200 <= status < 300 and ack == key and not row["error"]))
        and (result != "rejected" or (400 <= status < 500 and not row["error"]))
        and (result == "accepted" or ack is None)
        and (write is None or (type(write) is int and entry <= write <= done and count > 0)),
        f"invalid transport outcome: {key}",
    )
    if count > 1:
        return "multiple-write-callbacks"
    if write is None:
        return "missing-request-write"
    if not intent["at_ns"] <= attempt <= write < intent["cutoff_ns"]:
        return "request-write-outside-slot"
    return None


def check_flush(record, batch, *, prepared=None):
    enter, finish = record["sync_entry_ns"], record["sync_return_ns"]
    require(
        type(record["batch"]) is int
        and record["batch"] == batch
        and record["lock_acquired_ns"] is None
        and type(enter) is int
        and type(finish) is int
        and enter <= finish,
        "invalid J2 journal flush timing",
    )
    if prepared is not None:
        directory = record["directory_sync_return_ns"]
        require(
            type(directory) is int and prepared <= enter <= finish <= directory <= -2_000_000_000,
            "precommit sync was incomplete or missed preflight",
        )


def replay_dispatch(raw, schedule, slots, schedule_sha256):
    lines = raw.splitlines(keepends=True)
    torn = bool(lines and not lines[-1].endswith(b"\n"))
    if torn:
        lines.pop()
    require(lines, "missing J2 dispatch plan")
    head = json.loads(lines[0])
    plan = head["plan"]
    count = schedule["count"]
    require(
        head["event"] == "plan"
        and head["schema"] == DISPATCH_JOURNAL
        and schedule_sha256
        and head["schedule_sha256"] == schedule_sha256
        and head["origin_ns"] == schedule["origin_ns"]
        and plan["schema"] == "dur053.arrivals.v1"
        and plan["status"] == "IN_PROGRESS"
        and plan["arm"] == "short"
        and all(plan[k] == schedule[k] for k in ("namespace", "run_id", "seed"))
        and plan["rate"] == float(schedule["rate"])
        and plan["concurrency"] == schedule["cap"]
        and (
            schedule["cap"] == 16
            or (
                schedule["cap"] == 1
                and schedule["cell"] == "capped-sender"
                and schedule["campaign"].startswith("eng-controls-")
                and schedule["run_id"] == f"j2-{schedule['campaign']}-capped-sender"
                and schedule["rate"] == "1"
                and "periods" not in schedule
            )
        )
        and 0 < count <= 26678
        and len(plan["rows"]) == count,
        "J2 dispatch plan or schedule binding differs",
    )
    seal_at = count + 1
    require(len(lines) > seal_at, "missing complete J2 precommit")
    seal = json.loads(lines[seal_at])
    require(
        seal["event"] == "precommit"
        and seal["count"] == count
        and seal["sha256"] == raw_digest(lines[:seal_at])
        and sum(map(len, lines[: seal_at + 1])) <= 64 << 20,
        "J2 precommit count, digest or size differs",
    )
    intents = [json.loads(line) for line in lines[1:seal_at]]
    prepared = check_intents(plan, intents, slots, schedule)
    outcomes, flushes, problems = {}, {}, {}
    next_batch, batch_rows = 0, 0
    closed = False
    for i, raw_line in enumerate(lines[seal_at + 1 :], seal_at + 1):
        row = json.loads(raw_line)
        kind = row["event"]
        require(not closed, "J2 journal has records after close")
        if kind == "flush":
            require(next_batch == 0 or batch_rows > 0, "empty outcome flush")
            check_flush(row, next_batch, prepared=prepared if next_batch == 0 else None)
            if next_batch == 0 and "precommit_bytes" in row:
                require(
                    row["precommit_bytes"] == sum(map(len, lines[: seal_at + 1]))
                    and type(row["precommit_start_ns"]) is int
                    and row["precommit_start_ns"] <= intents[0]["prepared_ns"],
                    "precommit resource evidence differs from the sealed prefix",
                )
            flushes[next_batch] = row
            next_batch += 1
            batch_rows = 0
        elif kind == "closed":
            require(
                len(outcomes) == row["count"] == count
                and next_batch > 1
                and not batch_rows
                and row["sha256"] == raw_digest(lines[seal_at + 1 : i]),
                "J2 close is missing sealed outcomes or digest differs",
            )
            closed = True
        else:
            seq = row["sequence"]
            require(
                kind == "outcome"
                and type(seq) is int
                and 0 <= seq < count
                and seq not in outcomes
                and next_batch > 0
                and row["batch"] == next_batch
                and batch_rows < 64,
                "duplicate or unexpected J2 outcome or batch",
            )
            reason = check_outcome(row, intents[seq], schedule["cap"])
            if reason is not None:
                problems[seq] = reason
            outcomes[seq] = row
            batch_rows += 1
    return intents, outcomes, flushes, problems, closed, torn


def dispatch_report(raw, schedule, slots, durable, schedule_sha256, admission_states):
    intents, outcomes, flushes, problems, closed, torn = replay_dispatch(
        raw, schedule, slots, schedule_sha256
    )
    finished = {row["key"]: row for row in outcomes.values()}
    if set(durable).intersection(
        row["key"] for row in finished.values() if row["outcome"] == "unsent"
    ):
        raise IntegrityError("explicitly refused J2 identity has a durable admission")
    states = admission_states({row["key"] for row in intents}, finished, durable)
    samples, exceptions = [], []
    for seq, row in sorted(outcomes.items()):
        write, intent = row["request_written_ns"], intents[seq]
        sample = {
            **row,
            "phase": "unsent" if row["outcome"] == "unsent" else "finished",
            "recorded_intent_lateness_ns": row["dispatch_intent_ns"] - intent["at_ns"],
            "request_write_lateness_ns": None if write is None else write - intent["at_ns"],
            "in_flight_from_journal": row["in_flight_at_release"],
        }
        samples.append(sample)
        if seq in problems:
            exceptions.append(
                {
                    **sample,
                    "reason": problems[seq],
                    "at_ns": intent["at_ns"],
                    "cutoff_ns": intent["cutoff_ns"],
                    "prepared_ns": intent["prepared_ns"],
                    "precommit_flush": flushes.get(0),
                    "outcome_flush": flushes.get(row["batch"]),
                }
            )
    sent = sorted(row["key"] for row in samples if row["request_written_ns"] is not None)
    return {
        **states,
        "schema": DISPATCH_JOURNAL,
        "offered": len(intents),
        "warmup_offered": sum(
            row["at_ns"] < schedule["warmup_end_ns"] - schedule["origin_ns"] for row in intents
        ),
        "closed": closed,
        "torn_suffix": torn,
        "remaining_intents": sorted(
            row["key"] for i, row in enumerate(intents) if i not in outcomes
        ),
        "outside_slot": sorted(
            intents[i]["key"]
            for i, reason in problems.items()
            if reason == "request-write-outside-slot"
        ),
        "max_in_flight_from_journal": max(
            (row["in_flight_from_journal"] for row in samples), default=0
        ),
        "all_accepted_verdict": "PASS"
        if closed and not torn and len(states["acknowledged"]) == len(intents)
        else "FAIL",
        "stock_verdict": None,
        "sent_keys": sent,
        "request_evidence_keys": sorted(
            row["key"] for row in samples if row["request_written_ns"] is not None or row["status"]
        ),
        "timing_exceptions": exceptions,
        "flushes": list(flushes.values()),
        "sender_samples": samples,
    }
