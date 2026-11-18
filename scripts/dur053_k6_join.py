"""Join the independent Phase C ingress inventory to the standalone sink export."""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import datetime


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def identity(value: str) -> str:
    return digest(value.encode())


def require(condition: bool, code: str) -> None:
    if not condition:
        raise ValueError(code)


def unique(rows: list[dict], key, code: str) -> dict:
    by_key = {}
    for row in rows:
        identifier = key(row)
        require(identifier not in by_key, code)
        by_key[identifier] = row
    return by_key


def valid_hash(value: str) -> bool:
    return bool(re.fullmatch(r"[0-9a-f]{64}", value))


LABELS = ("invocation_hash", "key_hash", "request_id_hash", "intent_hash", "resource_hash")
META = (
    *LABELS,
    "method",
    "operation",
    "connection_hash",
    "worker_binding",
    "relay_peer",
    "sink_peer",
)
EXCHANGE = ("inbound", "sink_send", "sink_response", "worker_delivery", "close")
RECEIPT_FIELDS = (
    "workflow_id",
    "logical_key",
    "resource_id",
    "canonical_hash",
    "scope_hash",
    "intent_id",
    "expected_revision",
    "applied_revision",
    "application_id",
)


def receipt_bytes(receipt: dict) -> bytes:
    # Match encoding/json on the C1 Receipt struct, including HTML escaping.
    raw = json.dumps(
        {k: receipt[k] for k in RECEIPT_FIELDS}, ensure_ascii=False, separators=(",", ":")
    )
    for char in ("<", ">", "&", "\u2028", "\u2029"):
        raw = raw.replace(char, f"\\u{ord(char):04x}")
    return raw.encode()


def sessions(records: list[dict], relay_peer: str, sink_peer: str) -> tuple[dict, dict]:
    seq: Counter[str] = Counter()
    ordinal: Counter[str] = Counter()
    marks: dict[str, int] = {}
    deliveries: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for row in records:
        session = row["session"]
        require(bool(re.fullmatch(r"[0-9a-f]{32}", session)), "JOURNAL_SESSION")
        require(session not in marks and row["sequence"] == seq[session] + 1, "JOURNAL_GAP")
        require(
            row["relay_peer"] == relay_peer
            and row["sink_peer"] == sink_peer
            and datetime.fromisoformat(row["time"]).tzinfo is not None,
            "JOURNAL_PEER_OR_TIME",
        )
        seq[session] += 1
        if row["event"] == "seal":
            require(row.get("high_water", 0) == row["sequence"] - 1, "JOURNAL_WATERMARK")
            marks[session] = row.get("high_water", 0)
        else:
            if row["event"] == "inbound":
                ordinal[session] += 1
                require(row["ordinal"] == ordinal[session], "JOURNAL_ORDINAL_GAP")
            deliveries[session, row["ordinal"]].append(row)
    require(bool(marks) and set(marks) == set(seq), "JOURNAL_UNSEALED")
    return deliveries, marks


def verified_exchange(rows: list[dict]) -> tuple[dict, dict]:
    inbound = rows[0]
    rejected = inbound["operation"] == "rejected"
    expected = ("inbound", "local_response", "worker_delivery", "close") if rejected else EXCHANGE
    require(tuple(row["event"] for row in rows) == expected, "JOURNAL_EXCHANGE_INCOMPLETE")
    response, worker, close = rows[-3:]
    require(
        (
            inbound["method"] in {"POST", "OTHER"} and not inbound.get("connection_hash")
            if rejected
            else inbound["method"] == "POST"
            and inbound["operation"] in {"apply", "lookup"}
            and valid_hash(inbound["connection_hash"])
        )
        and valid_hash(inbound["worker_binding"])
        and all(valid_hash(inbound[key]) for key in LABELS)
        and all(all(row.get(key) == inbound.get(key) for key in META) for row in rows),
        "JOURNAL_EXCHANGE_IDENTITY",
    )
    for row in rows[:-1]:
        require(
            0 <= row["length"] <= 65536
            and row["length"] < row["wire_length"] <= 90112
            and valid_hash(row["body_hash"])
            and valid_hash(row["wire_hash"]),
            "JOURNAL_BYTES",
        )
    pairs = [(response, worker)]
    if not rejected:
        pairs.append((inbound, rows[1]))
    require(
        all(
            all(
                left.get(key) == right.get(key)
                for key in ("length", "body_hash", "wire_length", "wire_hash", "status")
            )
            for left, right in pairs
        )
        and close["length"] == 0,
        "JOURNAL_FORWARD_CHANGED",
    )
    return inbound, response


def verify_request_bindings(snapshot: dict) -> None:
    bindings = unique(
        snapshot["request_bindings"], lambda r: r["request_id"], "SINK_REQUEST_ID_REUSED"
    )
    require(
        {call["request_id"] for call in snapshot["calls"]} == set(bindings),
        "SINK_REQUEST_INVENTORY",
    )
    for call in snapshot["calls"]:
        bound = bindings[call["request_id"]]["body_hash"]
        observed = identity(call["operation"] + "\0" + call["body_hash"])
        require(
            (bound == observed) == (call["outcome"] != "REQUEST_CONFLICT"), "SINK_REQUEST_BINDING"
        )


def match_call(call: dict, inbound: dict) -> None:
    require(
        call["operation"] == inbound["operation"]
        and call["body_hash"] == inbound["body_hash"]
        and all(
            identity(call[field]) == inbound[label]
            for field, label in (
                ("logical_key", "key_hash"),
                ("request_id", "request_id_hash"),
                ("intent_id", "intent_hash"),
                ("resource_id", "resource_hash"),
            )
        ),
        "SINK_REQUEST_MISMATCH",
    )


def match_apply(call: dict, response: dict, receipts: dict) -> str | None:
    outcome = call["outcome"]
    app_id = call["application_id"]
    if outcome in {"APPLIED", "DUPLICATE", "FOUND"}:
        require(
            outcome in ({"APPLIED", "DUPLICATE"} if call["operation"] == "apply" else {"FOUND"}),
            "SINK_OPERATION_OUTCOME",
        )
        require(app_id in receipts, "SINK_RECEIPT_ABSENT")
        receipt = receipts[app_id]
        body = receipt_bytes(receipt)
        require(
            all(
                receipt[k] == call[k]
                for k in ("workflow_id", "logical_key", "resource_id", "intent_id")
            )
            and response["status"] == 200
            and response["length"] == len(body)
            and response["body_hash"] == call["response_hash"] == digest(body),
            "SINK_REPLY_HASH_MISMATCH",
        )
        return app_id if outcome == "APPLIED" else None
    require(
        outcome
        in {"MISSING", "REQUEST_CONFLICT", "KEY_CONFLICT", "STALE_FENCE", "REVISION_CONFLICT"}
        and (outcome != "MISSING" or call["operation"] == "lookup")
        and app_id is None
        and call["response_hash"] is None,
        "SINK_CALL_OUTCOME",
    )
    body = f"K6_SINK_{outcome}\n".encode()
    require(
        response["status"] == (404 if outcome == "MISSING" else 409)
        and response["length"] == len(body)
        and response["body_hash"] == digest(body),
        "SINK_REPLY_HASH_MISMATCH",
    )
    return None


def reconcile_mutations(snapshot: dict, receipts: dict, applied: Counter) -> None:
    resources = unique(snapshot["resources"], lambda r: r["resource_id"], "SINK_RESOURCE_DUPLICATE")
    grouped: dict[str, list[dict]] = defaultdict(list)
    writes = unique(
        snapshot["journal"], lambda r: (r["resource_id"], r["revision"]), "SINK_MUTATION_DUPLICATE"
    )
    for write in writes.values():
        grouped[write["resource_id"]].append(write)
    require(set(grouped) <= resources.keys(), "SINK_MUTATION_RESOURCE_ABSENT")
    for key, resource in resources.items():
        revision, state_hash = resource["initial_revision"], resource["initial_state_hash"]
        require(revision == 0 and valid_hash(state_hash), "SINK_INITIAL_STATE")
        for write in sorted(grouped[key], key=lambda r: r["revision"]):
            require(
                write["old_revision"] == revision
                and write["revision"] == revision + 1
                and write["old_state_hash"] == state_hash
                and valid_hash(write["state_hash"]),
                "SINK_JOURNAL_REVISION_GAP",
            )
            revision, state_hash = write["revision"], write["state_hash"]
        require(
            (revision, state_hash) == (resource["revision"], resource["final_state_hash"]),
            "SINK_FINAL_STATE_MISMATCH",
        )

    require(applied == Counter({key: 1 for key in receipts}), "SINK_APPLIED_RECEIPT_COUNT")
    for receipt in receipts.values():
        write = writes.pop((receipt["resource_id"], receipt["applied_revision"]), None)
        require(
            write is not None
            and write["logical_key"] == receipt["logical_key"]
            and write["intent_id"] == receipt["intent_id"]
            and write["old_revision"] == receipt["expected_revision"],
            "SINK_RECEIPT_MUTATION_MISMATCH",
        )
    require(not writes, "SINK_MUTATION_UNJOINED")


def verify(
    records: list[dict],
    snapshot: dict,
    *,
    engine_peer: str,
    relay_peer: str,
    sink_peer: str,
    expected_applications: int,
    allocations: list[dict],
    denied_requests: list[tuple[str, str]],
    worker_observations: list[dict],
    exchange_reader=None,
) -> dict:
    """Every sink call must have exactly one independently observed arrival."""
    deliveries, marks = sessions(records, relay_peer, sink_peer)
    calls = unique(snapshot["calls"], lambda r: r["connection_hash"], "SINK_CONNECTION_REUSED")
    unique(snapshot["calls"], lambda r: r["call_id"], "SINK_CALL_ID_REUSED")
    receipts = unique(snapshot["receipts"], lambda r: r["application_id"], "SINK_RECEIPT_DUPLICATE")
    unique(snapshot["receipts"], lambda r: (r["workflow_id"], r["logical_key"]), "SINK_RECEIPT_KEY")
    application_count = len(snapshot["journal"])
    require(application_count == expected_applications, "SINK_APPLICATION_COUNT")
    # Direct allocator calls are admitted only against separate engine-side
    # TLS observations. A call merely labeled allocate receives no exemption.
    issued = unique(
        snapshot["allocations"],
        lambda r: (r["intent_id"], r["attempt"], r["resource_id"], r["fence"]),
        "SINK_ALLOCATION_DUPLICATE",
    )
    witnessed = set()
    for observation in allocations:
        call = calls.pop(observation["connection_hash"], None)
        body = json.dumps({"fence": observation["fence"]}, separators=(",", ":")).encode()
        require(
            call is not None
            and observation["engine_peer"] == engine_peer
            and valid_hash(observation["connection_hash"])
            and observation["response_hash"] == digest(body)
            and observation["response_length"] == len(body)
            and call["operation"] == "allocate"
            and call["outcome"] == "ALLOCATED"
            and call["application_id"] is None
            and call["response_hash"] is None
            and all(
                call[k] == observation[k]
                for k in (
                    "request_id",
                    "body_hash",
                    "intent_id",
                    "resource_id",
                    "attempt",
                    "logical_key",
                )
            ),
            "SINK_ALLOCATOR_UNWITNESSED",
        )
        key = (call["intent_id"], call["attempt"], call["resource_id"], observation["fence"])
        require(key in issued, "SINK_FENCE_UNJOINED")
        witnessed.add(key)
    require(witnessed == set(issued), "SINK_ALLOCATION_UNJOINED")
    expected_denials = Counter(denied_requests)
    workers = unique(
        worker_observations, lambda row: row["connection_hash"], "WORKER_CONNECTION_REUSED"
    )
    applied: Counter[str] = Counter()
    joined = []
    bindings = set()
    exchanges = (
        exchange_reader(deliveries)
        if exchange_reader is not None
        else ((*verified_exchange(rows), True) for rows in deliveries.values())
    )
    for inbound, response, delivered in exchanges:
        if delivered:
            worker = workers.pop(inbound["worker_binding"], None)
            require(
                worker is not None
                and worker["request_hash"] == inbound["body_hash"]
                and worker["response_hash"] == response["body_hash"]
                and worker["response_length"] == response["length"]
                and worker["status"] == response["status"],
                "WORKER_OBSERVATION_MISMATCH",
            )
        if inbound["operation"] == "rejected":
            require(
                response["status"] == 404
                and response["body_hash"] == digest(b"")
                and response["length"] == 0,
                "RELAY_LOCAL_DENIAL_MISMATCH",
            )
            continue
        binding = inbound["connection_hash"]
        require(binding not in bindings, "JOURNAL_CONNECTION_REUSED")
        bindings.add(binding)
        call = calls.pop(binding, None)
        if call is None:
            denial = (inbound["request_id_hash"], inbound["body_hash"])
            require(
                response["status"] == 403
                and response["body_hash"] == digest(b"K6_AUTH_DENIED\n")
                and response["length"] == len(b"K6_AUTH_DENIED\n")
                and expected_denials[denial] > 0,
                "SINK_DELIVERY_UNJOINED",
            )
            expected_denials[denial] -= 1
            continue
        match_call(call, inbound)
        if exchange_reader is not None:
            require(
                identity(call["message_id"]) == inbound["invocation_hash"],
                "SINK_INVOCATION_MISMATCH",
            )
        app_id = match_apply(call, response, receipts)
        if app_id is not None:
            applied[app_id] += 1
        joined.append(
            {
                "session": inbound["session"],
                "ordinal": inbound["ordinal"],
                "call_id": call["call_id"],
                "connection_hash": binding,
                "request_id_hash": inbound["request_id_hash"],
                "request_hash": inbound["body_hash"],
                "response_hash": response["body_hash"],
                "application_id": call["application_id"],
            }
        )
    require(not calls, "SINK_UNWITNESSED_CALL")
    require(not workers, "WORKER_DELIVERY_UNWITNESSED")
    require(not +expected_denials, "SINK_DENIAL_UNWITNESSED")
    verify_request_bindings(snapshot)
    reconcile_mutations(snapshot, receipts, applied)
    return {"applications": application_count, "joined": joined, "session_high_water": marks}


def load_records(path) -> list[dict]:
    data = path.read_bytes()
    require(data.endswith(b"\n"), "JOURNAL_TRUNCATED")
    return [json.loads(line) for line in data.splitlines()]
