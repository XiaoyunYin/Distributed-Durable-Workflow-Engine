"""Join the F3 clock, database, Kubernetes, and terminal witnesses offline."""

from __future__ import annotations

from statistics import median_low

from dur053_k6_b_evidence import SmokeFailure
from dur053_k6_chaos import API
from dur053_k6_e1_join import SCHEDULERS, base_inputs, broker_records, by, expect, one
from dur053_k6_f1a_inputs import LEASE_MS, attempt_fields, ns, terminal_winner, worker_principal
from dur053_k6_f2_inputs import (
    BASE,
    QUIET_NS,
    UNCOVERED,
    marks,
    pending,
    quiet_digest,
    runtime_start,
)

CELLS = {
    "medium-8": ("scheduler", 30),
    "medium-9": ("scheduler", -30),
    "medium-10": ("worker", 30),
    "medium-11": ("worker", -30),
}
SECOND = 10**9
TOLERANCE_NS = SECOND
TTL_TOLERANCE_NS = 50_000_000
NOT_EXERCISED = "NOT EXERCISED"
CONFIRMED = "CONFIRMED"
IDENTITY_FIELDS = ("pod_uid", "pid", "process_start_id", "boot_id", "role")


def time_spec(target: dict, seconds: int) -> dict:
    return {
        "selector": {"pods": {target["namespace"]: [target["pod"]]}},
        "mode": "one",
        "timeOffset": f"{seconds}s",
        "clockIds": ["CLOCK_REALTIME"],
        "containerNames": [target["container"]],
        "duration": "60s",
    }


def acknowledge(obj: dict, target: dict, seconds: int) -> bool:
    # Compare the stored CR with the registration and the exact bound pod/container.
    expect("F3_CHAOS_API", (obj["apiVersion"], obj["kind"]), (API, "TimeChaos"))
    expect("F3_CHAOS_NAMESPACE", obj["metadata"]["namespace"], target["namespace"])
    expect("F3_CHAOS_SPEC", obj["spec"], time_spec(target, seconds))
    status = obj.get("status", {})
    conditions = {row["type"]: row["status"] for row in status.get("conditions", [])}
    if conditions.get("AllInjected") != "True":
        return False
    record = one("F3_CHAOS_CONTAINER_RECORD", status["experiment"]["containerRecords"])
    expect(
        "F3_CHAOS_CONTAINER_ID",
        (record["selectorKey"], record["id"]),
        (".", f"{target['namespace']}/{target['pod']}/{target['container']}"),
    )
    expect(
        "F3_CHAOS_ACK",
        (
            conditions.get("Selected"),
            conditions.get("Paused"),
            record["phase"],
            record["injectedCount"],
            record["recoveredCount"],
            status["experiment"]["desiredPhase"],
        ),
        ("True", "False", "Injected", 1, 0, "Run"),
    )
    one("F3_CHAOS_APPLY_EVENT", by(record["events"], operation="Apply", type="Succeeded"))
    return True


def recovery(deleted: dict, injected: dict) -> None:
    expect("F3_CHAOS_DELETED_UID", deleted["metadata"]["uid"], injected["metadata"]["uid"])
    old = one("F3_CHAOS_APPLIED_RECORD", injected["status"]["experiment"]["containerRecords"])
    record = one("F3_CHAOS_RECOVERED_RECORD", deleted["status"]["experiment"]["containerRecords"])
    expect(
        "F3_CHAOS_RECORD_RECOVERED",
        (record["id"], record["phase"], record["injectedCount"], record["recoveredCount"]),
        (old["id"], "Not Injected", 1, 1),
    )
    applied = one(
        "F3_CHAOS_RECOVER_APPLY", by(record["events"], operation="Apply", type="Succeeded")
    )
    recovered = one(
        "F3_CHAOS_RECOVER_EVENT", by(record["events"], operation="Recover", type="Succeeded")
    )
    expect("F3_CHAOS_RECOVER_ORDER", ns(applied["timestamp"]) <= ns(recovered["timestamp"]), True)


def bracket(send: dict, receive: dict, wall_ns: int) -> tuple[int, int]:
    elapsed = receive["monotonic_ns"] - send["monotonic_ns"]
    wall_elapsed = receive["wall_ns"] - send["wall_ns"]
    expect("F3_OBSERVER_BRACKET", 0 <= elapsed <= SECOND // 2 and wall_elapsed >= 0, True)
    expect("F3_OBSERVER_WALL_STABLE", abs(elapsed - wall_elapsed) <= 5_000_000, True)
    expect("F3_TARGET_CLOCK_TYPE", type(wall_ns) is int, True)
    return wall_ns - (send["wall_ns"] + receive["wall_ns"]) // 2, (wall_elapsed + 1) // 2


def summarize(points: list[tuple[int, int]]) -> dict:
    expect("F3_CLOCK_SAMPLE_COUNT", len(points) >= 3, True)
    center = median_low([offset for offset, _ in points])
    return {
        "offset_ns": center,
        "uncertainty_ns": max(abs(offset - center) + width for offset, width in points),
        "samples": len(points),
    }


def clock_proof(data: dict, seconds: int) -> dict:
    target = data["target"]
    for binding in data["bindings"]:
        expect("F3_CRI_IDENTITY_DRIFT", binding, target)
    expect("F3_CRI_BINDING_COUNT", len(data["bindings"]), 3)
    expect("F3_BACKEND_INJECTION_MISSING", acknowledge(data["injected"], target, seconds), True)
    expect(
        "F3_BACKEND_INJECTION_ENDED_EARLY", acknowledge(data["last_status"], target, seconds), True
    )
    expect(
        "F3_CHAOS_LIVE_UID",
        data["last_status"]["metadata"]["uid"],
        data["injected"]["metadata"]["uid"],
    )
    recovery(data["deleted"], data["injected"])
    expect("F3_CHAOS_REMAINING", data["remaining"], [])
    m = {key: value["monotonic_ns"] for key, value in data["marks"].items()}
    expect(
        "F3_INTERVENTION_ORDER",
        m["apply"] < m["ack"] < m["remove"] < m["absent"]
        and m["absent"] - m["apply"] < 60 * SECOND,
        True,
    )
    identity = data["identity"]
    expect(
        "F3_CLOCK_TARGET_BINDING",
        [identity[k] for k in IDENTITY_FIELDS],
        [
            target["uid"],
            int(target["process"].split(":")[0]),
            target["process"],
            data["node_boot_id"],
            target["role"],
        ],
    )
    points: dict[str, list] = {"baseline": [], "during": [], "after": []}
    during_brackets = []
    previous = None
    for row in data["samples"]:
        send, receive, answer = row["send"], row["receive"], row["answer"]
        expect("F3_CLOCK_PROCESS_DRIFT", {key: answer[key] for key in identity}, identity)
        start, end = send["monotonic_ns"], receive["monotonic_ns"]
        if previous is not None:
            expect("F3_CLOCK_SAMPLE_GAP", 0 < start - previous <= 1_500_000_000, True)
        previous = start
        estimate = bracket(send, receive, answer["wall_ns"])
        if end < m["apply"]:
            points["baseline"].append(estimate)
        elif m["ack"] < start and end < m["remove"]:
            points["during"].append(estimate)
            during_brackets.append((start, end))
        elif m["absent"] < start:
            points["after"].append(estimate)
    baseline, during, after = [summarize(phase) for phase in points.values()]
    base = baseline["offset_ns"]
    base_width = baseline["uncertainty_ns"]
    expect("F3_CLOCK_BASELINE", abs(base) + base_width <= TOLERANCE_NS, True)
    for offset, width in points["after"]:
        expect(
            "F3_CLOCK_BASELINE_NOT_RESTORED",
            abs(offset - base) <= width + base_width + TOLERANCE_NS,
            True,
        )
    deltas = [(offset - base, width + base_width) for offset, width in points["during"]]
    shifted = all(
        abs(delta - seconds * SECOND) <= width + TOLERANCE_NS and abs(delta) > width + TOLERANCE_NS
        for delta, width in deltas
    )
    flat = all(abs(delta) <= width for delta, width in deltas)
    expect("F3_CLOCK_DURING_AMBIGUOUS", shifted or flat, True)
    return {
        "confirmation": CONFIRMED if shifted else NOT_EXERCISED,
        "baseline": baseline,
        "during": during,
        "during_span": [during_brackets[0][1], during_brackets[-1][0]],
        "after": after,
        "removal": data["marks"]["absent"],
        "removal_requested": data["marks"]["remove"],
        "observer_elapsed_ns": m["absent"] - m["apply"],
        "identity": identity,
        "target": target,
        "chaos_uid": data["injected"]["metadata"]["uid"],
    }


def check_ttl(row: dict, ttl_ns: int) -> dict:
    elapsed = ns(row["deadline"]) - ns(row["written_at"])
    expect("F3_DB_DEADLINE_TTL", abs(elapsed - ttl_ns) <= TTL_TOLERANCE_NS, True)
    return {**row, "ttl_ns": ttl_ns, "deadline_minus_write_ns": elapsed}


def in_flight(row: dict, attempt: dict, worker: dict, workflow: str) -> None:
    claim = one("F3_DURABLE_CURRENT_CLAIM", row["claim"])
    expect("F3_DURABLE_CLAIM_IDENTITY", attempt_fields(claim), attempt_fields(attempt))
    expect(
        "F3_DURABLE_CLAIM_ACTIVE",
        (
            claim["workflow_id"],
            claim["state"],
            claim["is_current"],
            claim["token_hash"],
            claim["worker_id"],
        ),
        (workflow, "CLAIMED", True, attempt["token_hash"], worker_principal(worker)),
    )
    expect("F3_DURABLE_CLAIM_EXPIRED", ns(claim["deadline"]) > ns(row["db_at"]), True)


def database_proof(data: dict, clock: dict, attempt: dict, worker: dict) -> dict:
    db = data["db"]
    marks_by_phase = data["marks"]
    phases = (
        ("baseline", None, "apply"),
        ("during_start", "ack", "remove"),
        ("during_end", "ack", "remove"),
        ("after", "absent", None),
    )
    for name, lower, upper in phases:
        sample = db[name]
        begin, end = sample["send"], sample["receive"]
        expect("F3_DB_SAMPLE_READ_ONLY", sample["value"]["read_only"], "on")
        if lower:
            expect(
                "F3_DB_SAMPLE_LOWER_BOUND",
                marks_by_phase[lower]["monotonic_ns"] < begin["monotonic_ns"],
                True,
            )
        if upper:
            expect(
                "F3_DB_SAMPLE_UPPER_BOUND",
                end["monotonic_ns"] < marks_by_phase[upper]["monotonic_ns"],
                True,
            )
        offset, width = bracket(begin, end, ns(sample["value"]["db_at"]))
        expect("F3_DB_CLOCK_INDEPENDENT", abs(offset) <= width + 5_000_000, True)
        in_flight(sample["value"], attempt, worker, attempt["workflow_id"])
    baseline, start, end, after = [db[p]["value"] for p, _, _ in phases]
    first, last = clock["during_span"]
    expect(
        "F3_DB_WRITE_WINDOW_CLOCK_BRACKET",
        first
        < db["during_start"]["send"]["monotonic_ns"]
        < db["during_end"]["receive"]["monotonic_ns"]
        < last,
        True,
    )
    lo, hi = ns(baseline["db_at"]), ns(after["db_at"])
    during_lo, during_hi = ns(start["db_at"]), ns(end["db_at"])
    expect("F3_DB_CLOCK_WINDOW_ORDER", lo < during_lo < during_hi < hi, True)
    rows = data["writes"]
    expect("F3_DB_WRITE_IDS_UNIQUE", len({row["id"] for row in rows}), len(rows))
    workflow = attempt["workflow_id"]
    role = clock["target"]["role"]
    scheduler = role == "scheduler"
    leases = []
    if scheduler:
        leases = [
            one("F3_DB_PARTITION_LEASE", cut["lease"]) for cut in (baseline, start, end, after)
        ]
        owner_key = tuple(leases[0][k] for k in ("partition_id", "owner_id", "epoch"))
        expect(
            "F3_DB_OWNER_BINDING", data["held_owner"]["session_id"], clock["identity"]["session"]
        )
        expect("F3_DB_OWNER_UID", data["held_owner"]["pod_uid"], clock["target"]["uid"])
        expect(
            "F3_DB_OWNER_PROOF",
            tuple(data["held_owner"][k] for k in ("partition_id", "owner_id", "epoch")),
            owner_key,
        )
        for lease, cut in zip(leases, (baseline, start, end, after), strict=True):
            expect(
                "F3_NO_SPURIOUS_TAKEOVER",
                tuple(lease[k] for k in ("partition_id", "owner_id", "epoch")),
                owner_key,
            )
            expect("F3_NO_STALE_OWNER", ns(lease["lease_expires_at"]) > ns(cut["db_at"]), True)
    primary, all_rows = [], []
    required_branch = "renewal" if scheduler else "heartbeat"
    expected_worker = worker_principal(worker)
    for row in rows:
        written = ns(row["written_at"])
        branch = row["branch"]
        if branch.startswith("lease-") or branch == "renewal":
            if (
                scheduler
                and lo <= written <= hi
                and row["partition_id"] == leases[0]["partition_id"]
            ):
                expect(
                    "F3_NO_SPURIOUS_TAKEOVER_WRITE",
                    (branch, row["owner_id"], row["epoch"]),
                    ("renewal", leases[0]["owner_id"], leases[0]["epoch"]),
                )
                expect(
                    "F3_RENEWAL_PROCESS",
                    (row["application"], row["session"]),
                    ("durable-scheduler", clock["identity"]["session"]),
                )
                expect("F3_NO_EARLY_LEASE_EXPIRY", ns(row["old_deadline"]) > written, True)
            if (
                not scheduler
                or row["session"] != clock["identity"]["session"]
                or branch == "lease-release"
            ):
                continue
            ttl = 15 * SECOND
        elif row["workflow_id"] == workflow and branch in {
            "dispatch",
            "claim",
            "heartbeat",
            "attempt-update",
        }:
            if lo <= written <= hi:
                expect(
                    "F3_CLAIM_NOT_HELD_THROUGH_WINDOW",
                    (branch, row["token_hash"]),
                    ("heartbeat", attempt["token_hash"]),
                )
            if row["deadline"] is None or (
                branch == "attempt-update" and row["deadline"] == row["old_deadline"]
            ):
                continue
            if branch == "dispatch":
                expect("F3_SCHEDULER_DISPATCH_SOURCE", row["application"], "durable-scheduler")
                ttl = 30 * SECOND
            else:
                expect(
                    "F3_WORKER_CONTROL_WRITE_SOURCE",
                    (row["application"], row["worker_id"]),
                    ("durable-worker-control", expected_worker),
                )
                expect(
                    "F3_WORKER_CONTROL_ATTEMPT_BINDING",
                    (attempt_fields(row), row["token_hash"]),
                    (attempt_fields(attempt), attempt["token_hash"]),
                )
                ttl = LEASE_MS * 1_000_000
                if branch == "heartbeat" and lo <= written <= hi:
                    expect("F3_NO_EARLY_CLAIM_EXPIRY", ns(row["old_deadline"]) > written, True)
        else:
            if lo <= written <= hi and (
                row["application"] == "durable-scheduler" or row["worker_id"] == expected_worker
            ):
                raise SmokeFailure("F3_UNACCOUNTED_DEADLINE_WRITE")
            continue
        # Include the initial dispatch and claim in the retained branch trace.
        if written <= hi:
            checked = check_ttl(row, ttl)
            all_rows.append(checked)
            if during_lo <= written <= during_hi and branch == required_branch:
                primary.append(checked)
    expect("F3_DURING_ROLE_WRITE_MISSING", bool(primary), True)
    expect(
        "F3_DISPATCH_AND_CLAIM_CAPTURED",
        {"dispatch", "claim"} <= {row["branch"] for row in all_rows},
        True,
    )
    return {"during": primary, "deadline_rows": all_rows, "leases": leases}


def coverage(predicates: dict, confirmation: str, deadline_ids: list[int]) -> dict:
    expect("F3_B_COVERAGE_INCOMPLETE", [name for name in BASE if predicates.get(name)], list(BASE))
    if confirmation == CONFIRMED:
        expect("F3_I12_WITNESSES", bool(deadline_ids), True)
        witnesses = {**predicates, "I12": deadline_ids}
    else:
        expect("F3_UNKNOWN_CONFIRMATION", confirmation, NOT_EXERCISED)
        witnesses = predicates
    reasons = {**UNCOVERED, "I12": "Target clock shift was NOT EXERCISED"}
    return {
        "invariants_covered": list(witnesses),
        "invariants_not_covered": [name for name in reasons if name not in witnesses],
        "outcomes": {
            **{
                name: {"status": "covered/pass", "witnesses": ids}
                for name, ids in witnesses.items()
            },
            **{
                name: {"status": "NOT COVERED", "reason": reason}
                for name, reason in reasons.items()
                if name not in witnesses
            },
        },
    }


def join(inputs: dict) -> dict:
    """Run the same closeout and coverage predicates on hosted or offline evidence."""
    try:
        cell = inputs["cell"]
        role, seconds = CELLS[cell]
        fault = inputs["fault"]
        expect("F3_REGISTERED_ROLE", fault["target"]["role"], role)
        clock = clock_proof(fault, seconds)
        confirmation = clock["confirmation"]
        control = None
        if confirmation == NOT_EXERCISED:
            control = inputs["control"]
            expect("F3_CONTROL_LABEL", control["purpose"], "harness validation only")
            expect(
                "F3_CONTROL_DISTINCT_TARGET",
                (
                    control["target"]["role"],
                    control["target"]["uid"] != fault["target"]["uid"],
                    control["target"]["namespace"],
                ),
                ("clock-control", True, fault["target"]["namespace"]),
            )
            expect(
                "F3_HARNESS_SENSITIVITY", clock_proof(control, seconds)["confirmation"], CONFIRMED
            )
        static = inputs["static"]
        expect(
            "F3_STATIC_DIGEST",
            (len(static["sha256"]), static["sha256"]),
            (64, static["build_sha256"]),
        )
        expect(
            "F3_STATIC_BUILD",
            all(
                value in static["go_version_m"]
                for value in ("CGO_ENABLED=0", "-tags=dur053_k6", "GOOS=linux")
            ),
            True,
        )
        expect(
            "F3_STATIC_LINKAGE",
            "statically linked" in static["file"] and "INTERP" not in static["program_headers"],
            True,
        )
        expect(
            "F3_STATIC_NODE_CONFIG",
            all(static[key] for key in ("containerd", "cni", "calico")),
            True,
        )
        runtime = runtime_start(inputs["scheduler_log"])
        expect("F3_SCHEDULER_RUNTIME_LOG", "roles=scheduler,ingestor" in runtime["message"], True)
        final = inputs["terminal"]
        offer = inputs["offer"]
        workflow = offer["workflow"]
        expect(
            "F3_ACCEPTANCE",
            offer["response"],
            {"status": 201, "workflow_id": workflow, "created": True},
        )
        expect(
            "F3_TERMINAL_CENSUS", (final["read_only"], list(final["workflows"])), ("on", [workflow])
        )
        expect("F3_OUTBOX_NOT_DRAINED", sorted(pending(final)), [])
        cut = final["workflows"][workflow]
        expect(
            "F3_TERMINAL_STATE",
            (cut["workflow"]["namespace"], cut["workflow"]["state"]),
            (fault["target"]["namespace"], "SUCCEEDED"),
        )
        witness = inputs["witness"]
        marker = inputs["marker"]
        expect(
            "F3_MARKER_AFTER_RESTORATION",
            marker["before"]["monotonic_ns"]
            > max(s["receive"]["monotonic_ns"] for s in fault["samples"]),
            True,
        )
        winner = terminal_winner(
            cut, witness, inputs["workers"], fault["target"]["namespace"], marker
        )
        attempt = inputs["attempt"]
        expect(
            "F3_HELD_ATTEMPT_WON",
            (attempt_fields(winner["attempt"]), winner["attempt"]["token_hash"]),
            (attempt_fields(attempt), attempt["token_hash"]),
        )
        worker = one(
            "F3_WINNING_WORKER_POD",
            [pod for pod in inputs["workers"] if worker_principal(pod) == attempt["worker_id"]],
        )
        if role == "worker":
            expect(
                "F3_TARGET_WORKER_PRINCIPAL",
                (worker["metadata"]["uid"], winner["result"]["process"]),
                (fault["target"]["uid"], fault["target"]["process"]),
            )
        deadlines = database_proof(fault, clock, attempt, worker)
        invocation = winner["invocation"]["id"]
        base = base_inputs(
            cell,
            workflow,
            offer["fingerprint"],
            cut,
            [
                {
                    "event": "start",
                    "invocation_id": invocation,
                    "token_hash": attempt["token_hash"],
                },
                {
                    "event": "end",
                    "invocation_id": invocation,
                    "outcome": winner["attempt"]["state"],
                },
            ],
        )["predicates"]
        topics = {name: broker_records(row["value"], 8) for name, row in inputs["topics"].items()}
        marks(inputs["topics"])
        for row in cut["outbox"]:
            accepted = by(cut["inbox"], event_id=row["event_id"], disposition="ACCEPTED")
            expect("F3_UNDISPOSED_OUTBOX", bool(accepted), True)
            for event in accepted:
                record = one(
                    "F3_BROKER_DISPOSITION",
                    by(
                        topics[event["topic"]],
                        partition=event["kafka_partition"],
                        offset=event["kafka_offset"],
                        event=event["event_id"],
                    ),
                )
                expect(
                    "F3_BROKER_WORKFLOW",
                    (record["workflow"], record["key"]),
                    (workflow, row["event_id"]),
                )
        expect(
            "F3_BROKER_EVENT_CENSUS",
            sorted(row["event"] for rows in topics.values() for row in rows),
            sorted(row["event_id"] for row in cut["outbox"]),
        )
        created = one("F3_CREATED_EVENT", by(cut["outbox"], event_type="workflow.created"))
        one(
            "F3_SCHEDULER_DISPOSITION",
            by(
                cut["inbox"],
                event_id=created["event_id"],
                consumer_id=SCHEDULERS,
                disposition="ACCEPTED",
            ),
        )
        quiet = inputs["quiet"]
        expect("F3_QUIET_SAMPLE_COUNT", len(quiet) >= 3, True)
        expect(
            "F3_QUIET_SAMPLE_ORDER",
            all(
                a["started"]["monotonic_ns"]
                <= a["ended"]["monotonic_ns"]
                < b["started"]["monotonic_ns"]
                for a, b in zip(quiet, quiet[1:], strict=False)
            ),
            True,
        )
        expect(
            "F3_QUIET_DIGEST",
            {sample["digest"] for sample in quiet},
            {quiet_digest(final, witness)},
        )
        expect(
            "F3_QUIET_DURATION",
            quiet[-1]["started"]["monotonic_ns"] - quiet[0]["ended"]["monotonic_ns"] >= QUIET_NS,
            True,
        )
        expect("F3_QUIET_TERMINAL_CUT", quiet[-1]["db_at"], final["db_at"])
        derived = coverage(base, confirmation, [row["id"] for row in deadlines["during"]])
        registered = inputs["registered"]
        expect(
            "F3_REGISTRY_COVERAGE",
            derived["invariants_covered"],
            [
                key
                for key in registered["invariants_covered"]
                if key != "I12" or confirmation == CONFIRMED
            ],
        )
        expect(
            "F3_REGISTRY_NOT_COVERED",
            sorted(derived["invariants_not_covered"]),
            sorted(
                set(registered["invariants_not_covered"])
                | ({"I12"} if confirmation == NOT_EXERCISED else set())
            ),
        )
        return {
            "cell": cell,
            "outcome": "CONFIRMED-PASS" if confirmation == CONFIRMED else NOT_EXERCISED,
            "passing_replicate": confirmation == CONFIRMED,
            "clock": clock,
            "deadlines": deadlines,
            "control": clock_proof(control, seconds) if control else None,
            "workflow": workflow,
            "arm": registered["arm"],
            **derived,
        }
    except (IndexError, KeyError, TypeError, ValueError) as error:
        raise SmokeFailure(f"F3_EVIDENCE_SHAPE {type(error).__name__} {error}") from None
