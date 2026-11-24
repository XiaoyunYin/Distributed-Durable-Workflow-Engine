"""Offline corruption, budget, credential scan and explicit handoff checks."""

from __future__ import annotations

import json
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dur053_k3  # noqa: E402
import dur053_k6_b_smoke as phase_b  # noqa: E402
import dur053_k6_c_smoke as phase_c  # noqa: E402
from dur053_k6_b_evidence import SmokeFailure  # noqa: E402
from dur053_k6_c_deploy import EXPORTER, NS, SINK_ROLE, Deployment  # noqa: E402
from dur053_k6_c_render import CLIENT, ENGINE, RELAY, SINK, budget_for, render  # noqa: E402
from dur053_k6_join import digest, identity, receipt_bytes, verify  # noqa: E402
from test_k6_phase_b import StubK3Driver  # noqa: E402

ENGINE_PEER = "spiffe://episode/engine"
RELAY_PEER = "spiffe://episode/relay"
SINK_PEER = "spiffe://episode/sink"

ENDPOINTS = {
    "sink_db": "192.0.2.1",
    "engine_db": "192.0.2.2",
    "api_service": "192.0.2.3",
    "api_endpoint": "192.0.2.4",
    "witness": "192.0.2.5",
    "witness_port": "6543",
}


def fixture() -> tuple[list[dict], dict, dict]:
    session = "a" * 32
    peers = {"relay_peer": RELAY_PEER, "sink_peer": SINK_PEER}
    base = {"session": session, "time": "2026-11-18T00:00:00Z", **peers}
    labels = {
        "workflow_id": "workflow",
        "logical_key": "key",
        "intent_id": "intent",
        "resource_id": "resource",
    }
    receipt = {
        **labels,
        "expected_revision": 0,
        "applied_revision": 1,
        "application_id": "app",
        "canonical_hash": identity("args"),
        "scope_hash": identity("scope"),
    }
    request_hash = identity("body")
    reply = receipt_bytes(receipt)
    reply_hash = digest(reply)
    entries, calls, observations = [], [], []
    for ordinal, outcome in enumerate(("APPLIED", "DUPLICATE"), 1):
        binding = identity(f"connection-{ordinal}")
        worker = identity(f"worker-{ordinal}")
        calls.append(
            {
                **labels,
                "call_id": ordinal,
                "operation": "apply",
                "request_id": "request",
                "body_hash": request_hash,
                "connection_hash": binding,
                "outcome": outcome,
                "application_id": "app",
                "response_hash": reply_hash,
            }
        )
        per_delivery = {
            "connection_hash": binding,
            "worker_binding": worker,
            "ordinal": ordinal,
            "method": "POST",
            "operation": "apply",
            "request_id_hash": identity("request"),
            "invocation_hash": identity("invocation"),
            "key_hash": identity("key"),
            "intent_hash": identity("intent"),
            "resource_hash": identity("resource"),
        }
        request = {
            "length": 4,
            "body_hash": request_hash,
            "wire_length": 444,
            "wire_hash": identity("request wire"),
        }
        response = {
            "length": len(reply),
            "body_hash": reply_hash,
            "status": 200,
            "wire_length": len(reply) + 100,
            "wire_hash": identity("response wire"),
        }
        for event, data in (
            ("inbound", request),
            ("sink_send", request),
            ("sink_response", response),
            ("worker_delivery", response),
            ("close", {"length": 0}),
        ):
            entries.append(
                {**base, **per_delivery, **data, "event": event, "sequence": len(entries) + 1}
            )
        observations.append(
            {
                "connection_hash": worker,
                "request_hash": request_hash,
                "response_hash": reply_hash,
                "response_length": len(reply),
                "status": 200,
            }
        )
    entries.append({**base, "event": "seal", "sequence": 11, "high_water": 10, "length": 0})
    snapshot = {
        "resources": [
            {
                "resource_id": "resource",
                "initial_revision": 0,
                "revision": 1,
                "initial_state_hash": identity("initial"),
                "final_state_hash": identity("final"),
            }
        ],
        "journal": [
            {
                "resource_id": "resource",
                "old_revision": 0,
                "revision": 1,
                "old_state_hash": identity("initial"),
                "state_hash": identity("final"),
                "intent_id": "intent",
                "logical_key": "key",
            }
        ],
        "receipts": [receipt],
        "calls": calls,
        "allocations": [],
        "request_bindings": [
            {"request_id": "request", "body_hash": identity("apply\0" + request_hash)}
        ],
    }
    return (
        entries,
        snapshot,
        {
            **peers,
            "engine_peer": ENGINE_PEER,
            "expected_applications": 1,
            "allocations": [],
            "denied_requests": [],
            "worker_observations": observations,
        },
    )


def test_clean_join() -> None:
    journal, snapshot, options = fixture()
    result = verify(journal, snapshot, **options)
    assert result["applications"] == 1
    assert [row["call_id"] for row in result["joined"]] == [1, 2]
    assert result["session_high_water"] == {"a" * 32: 10}


@pytest.mark.parametrize(
    ("damage", "expected"),
    [
        ("sequence", "JOURNAL_GAP"),
        ("ordinal", "JOURNAL_ORDINAL_GAP"),
        ("high_water", "JOURNAL_WATERMARK"),
        ("unsealed", "JOURNAL_UNSEALED"),
        ("extra", "SINK_CONNECTION_REUSED"),
        ("reply", "SINK_REPLY_HASH_MISMATCH"),
        ("worker", "WORKER_OBSERVATION_MISMATCH"),
        ("hidden_write", "SINK_FINAL_STATE_MISMATCH"),
        ("mutation_key", "SINK_RECEIPT_MUTATION_MISMATCH"),
        ("binding", "SINK_REQUEST_BINDING"),
    ],
)
def test_join_corruption(damage: str, expected: str) -> None:
    journal, snapshot, options = fixture()
    match damage:
        case "sequence":
            journal.pop(1)
        case "ordinal":
            journal[5]["ordinal"] = 3
        case "high_water":
            journal[-1]["high_water"] = 9
        case "unsealed":
            journal.pop()
        case "extra":
            snapshot["calls"].append({**snapshot["calls"][0], "call_id": 5})
        case "reply":
            snapshot["calls"][0]["response_hash"] = identity("forged reply")
        case "worker":
            options["worker_observations"][0]["response_hash"] = identity("worker forgery")
        case "hidden_write":
            snapshot["resources"][0]["revision"] = 2
        case "mutation_key":
            snapshot["journal"][0]["logical_key"] = "unrelated"
        case "binding":
            snapshot["request_bindings"][0]["body_hash"] = identity("unrelated")
    with pytest.raises(ValueError, match=expected):
        verify(journal, snapshot, **options)


def test_join_rejects_bypass_call_without_ingress() -> None:
    journal, snapshot, options = fixture()
    snapshot["calls"].append(
        {**snapshot["calls"][1], "call_id": 3, "connection_hash": identity("bypass")}
    )
    try:
        verify(journal, snapshot, **options)
    except ValueError as error:
        assert str(error) == "SINK_UNWITNESSED_CALL"
    else:
        pytest.fail("accepted a sink call with no ingress record")


def test_join_counts_database_mutations_not_relay_sends() -> None:
    journal, snapshot, options = fixture()
    options["expected_applications"] = 2
    try:
        verify(journal, snapshot, **options)
    except ValueError as error:
        assert str(error) == "SINK_APPLICATION_COUNT"
    else:
        pytest.fail("counted two relay sends as two database applications")


def test_join_requires_independent_engine_observation_for_allocation() -> None:
    journal, snapshot, options = fixture()
    call = {
        **snapshot["calls"][0],
        "operation": "allocate",
        "outcome": "ALLOCATED",
        "call_id": 3,
        "request_id": "allocate",
        "connection_hash": identity("engine"),
        "application_id": None,
        "response_hash": None,
        "attempt": 1,
    }
    snapshot["calls"].append(call)
    snapshot["allocations"].append({**call, "fence": 1})
    snapshot["request_bindings"].append(
        {"request_id": "allocate", "body_hash": identity("allocate\0" + call["body_hash"])}
    )
    with pytest.raises(ValueError, match="SINK_ALLOCATION_UNJOINED"):
        verify(journal, snapshot, **options)
    observation = {
        **call,
        "fence": 1,
        "engine_peer": ENGINE_PEER,
        "response_hash": digest(b'{"fence":1}'),
        "response_length": 11,
    }
    options["allocations"].append(observation)
    assert verify(journal, snapshot, **options)["applications"] == 1
    observation["engine_peer"] = RELAY_PEER
    with pytest.raises(ValueError, match="SINK_ALLOCATOR_UNWITNESSED"):
        verify(journal, snapshot, **options)


def test_join_requires_observed_prevalidation_and_local_denials() -> None:
    journal, snapshot, options = fixture()
    seal = journal.pop()
    for ordinal, local in enumerate((False, True), 3):
        status, body = (404, b"") if local else (403, b"K6_AUTH_DENIED\n")
        fields = {
            **journal[0],
            "ordinal": ordinal,
            "operation": "rejected" if local else "apply",
            "worker_binding": identity(f"worker-{ordinal}"),
        }
        if local:
            fields.pop("connection_hash")
        else:
            fields["connection_hash"] = identity(f"connection-{ordinal}")
            options["denied_requests"].append((fields["request_id_hash"], fields["body_hash"]))
        request = dict(fields)
        reply = {
            **fields,
            "status": status,
            "length": len(body),
            "body_hash": digest(body),
            "wire_hash": identity(f"reply-{ordinal}"),
        }
        closed = {
            key: val
            for key, val in fields.items()
            if key not in {"wire_hash", "wire_length", "body_hash"}
        }
        closed["length"] = 0
        itinerary = (
            [("inbound", request), ("local_response", reply)]
            if local
            else [("inbound", request), ("sink_send", request), ("sink_response", reply)]
        )
        for event, row in (*itinerary, ("worker_delivery", reply), ("close", closed)):
            journal.append({**row, "event": event, "sequence": len(journal) + 1})
        options["worker_observations"].append(
            {
                "connection_hash": fields["worker_binding"],
                "request_hash": request["body_hash"],
                "response_hash": reply["body_hash"],
                "response_length": len(body),
                "status": status,
            }
        )
    journal.append({**seal, "sequence": len(journal) + 1, "high_water": len(journal)})
    assert len(verify(journal, snapshot, **options)["joined"]) == 2
    options["denied_requests"].clear()
    with pytest.raises(ValueError, match="SINK_DELIVERY_UNJOINED"):
        verify(journal, snapshot, **options)


def topology() -> list[dict]:
    return render("phase-c", "localhost:5001/k6@sha256:" + "a" * 64, ENDPOINTS)


def test_phase_c_topology_and_surge() -> None:
    objects = topology()
    budget = budget_for(objects)
    assert budget == {
        "engine": {"committed": 82, "max_connections": 100, "headroom": 18},
        "sink": {"committed": 19, "max_connections": 30, "headroom": 11},
    }
    by_name = {(o["kind"], o["metadata"]["name"]): o["spec"] for o in objects}
    for name in (SINK, RELAY):
        assert by_name["Service", name]["type"] == "ClusterIP"
    assert by_name["NetworkPolicy", "worker-effect"]["egress"][0]["to"] == [
        {"podSelector": {"matchLabels": {"app": RELAY}}}
    ]
    assert by_name["NetworkPolicy", "sink-accept"]["ingress"][0]["from"] == [
        {"podSelector": {"matchLabels": {"app": RELAY}}},
        {"podSelector": {"matchLabels": {"app": ENGINE}}},
    ]
    assert by_name["NetworkPolicy", "relay-forward-and-witness"]["egress"][1]["to"] == [
        {"ipBlock": {"cidr": ENDPOINTS["witness"] + "/32"}}
    ]
    assert ENDPOINTS["engine_db"] not in json.dumps(
        by_name["NetworkPolicy", "relay-forward-and-witness"]
    )
    identity_volume = by_name["Pod", CLIENT]["volumes"][0]["projected"]["sources"][0]
    assert identity_volume["serviceAccountToken"]["audience"] == "dur053-k6-sink"
    by_name["Deployment", SINK]["strategy"]["rollingUpdate"]["maxSurge"] = 2
    with pytest.raises(ValueError, match="K6_C_SURGE_BUDGET"):
        budget_for(objects)


def test_phase_c_render_rejects_unaccounted_sink_db_client() -> None:
    objects = topology()
    objects.append({"kind": "Job", "metadata": {"name": "unaccounted-client"}})
    try:
        budget_for(objects)
    except ValueError as error:
        assert str(error) == "UNACCOUNTED_SINK_DB_CLIENT"
    else:
        pytest.fail("renderer accepted an unaccounted sink DB client")


def test_phase_c_trace_rejects_unaccounted_or_excess_connections() -> None:
    sample: dict[str, Any] = {
        "limit": 30,
        "reserved": 3,
        "clients": [[SINK_ROLE, "dur053-k6-sink", 8]],
    }
    assert phase_c.trace_count("sink", sample) == 8
    sample["clients"][0][2] = 9
    with pytest.raises(SmokeFailure, match="DB_CLIENT_BUDGET_SINK"):
        phase_c.trace_count("sink", sample)
    sample["clients"][0][1] = "unregistered"
    with pytest.raises(SmokeFailure, match="DB_UNACCOUNTED_CLIENT_SINK"):
        phase_c.trace_count("sink", sample)


def trace_backend(
    pid: int,
    user: str | None = None,
    app: str | None = "",
    *,
    start: str | None = "2026-11-22T00:00:00Z",
) -> dict[str, Any]:
    return {
        "pid": pid,
        "backend_start": start,
        "state": "starting" if user is None else "idle",
        "usename": user,
        "application_name": app,
        "wait_event_type": "Client",
        "wait_event": "ClientRead",
        "client_addr": "10.7.0.1",
        "client_port": 45678,
    }


def trace_snapshot(
    elapsed_us: int, *backends: dict[str, Any], server: str = "sink"
) -> dict[str, Any]:
    counts = Counter((row["usename"], row["application_name"]) for row in backends)
    return {
        "captured_at_us": 100_000_000 + elapsed_us,
        "limit": 30 if server == "sink" else 100,
        "reserved": 3,
        "clients": [[user, app, count] for (user, app), count in counts.items()],
        "backends": backends,
    }


class TraceClock:
    def __init__(self) -> None:
        self.now = 10.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def synthetic_trace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *snapshots: dict[str, Any],
    clock: TraceClock | None = None,
) -> tuple[phase_c.Trace, Mock]:
    monkeypatch.setattr(phase_c, "time", clock or TraceClock())
    smoke = Mock(spec=Deployment)
    smoke.evidence = phase_c.PhaseCEvidence(tmp_path)
    smoke.namespace = NS
    query = Mock(side_effect=[json.dumps(sample) for sample in snapshots])
    smoke.trace_session.return_value = nullcontext(query)
    smoke.k.return_value = registered_sink()
    smoke.endpoint_addresses = ENDPOINTS
    smoke.command.return_value = b""
    trace = phase_c.Trace(smoke)
    trace.rows.append({"time": "2026-11-22T00:00:00Z"})
    return trace, query


def sample_synthetic(trace: phase_c.Trace, server: str = "sink") -> None:
    with ThreadPoolExecutor(max_workers=2) as executor:
        trace.sample_server(server, "admin" if server == "sink" else "monitor", 0, executor)


@pytest.mark.parametrize(
    ("server", "user", "app"),
    [
        ("sink", SINK_ROLE, "dur053-k6-sink"),
        ("engine", "durable", "dur053-k6-c-monitor"),
    ],
)
def test_phase_c_starting_trace_resolves_within_bound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, server: str, user: str, app: str
) -> None:
    trace, query = synthetic_trace(
        tmp_path,
        monkeypatch,
        trace_snapshot(0, trace_backend(42), server=server),
        trace_snapshot(100_000, trace_backend(42, user, app), server=server),
    )
    try:
        sample_synthetic(trace, server)
    except SmokeFailure:
        pytest.fail("registered startup failed to resolve")
    assert query.call_count == 2
    assert trace.rejections == []
    assert trace.rows[0][server]["total"] == 1
    startup = trace.startups[0]
    assert (startup["outcome"], startup["server"]) == ("resolved", server)
    initial, resolved = (sample["backends"][0] for sample in startup["observations"])
    assert initial["usename"] is None and initial["application_name"] == ""
    assert (initial["pid"], initial["backend_start"]) == (
        resolved["pid"],
        resolved["backend_start"],
    )
    assert (resolved["usename"], resolved["application_name"], resolved["pod_name"]) == (
        user,
        app,
        "sink-pod",
    )


def test_phase_c_starting_trace_fails_if_unresolved_at_bound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    trace, _ = synthetic_trace(
        tmp_path,
        monkeypatch,
        *(trace_snapshot(us, trace_backend(42)) for us in range(0, 2_000_001, 200_000)),
    )
    try:
        sample_synthetic(trace)
    except SmokeFailure as error:
        assert str(error) == "DB_STARTUP_UNRESOLVED_SINK"
    else:
        pytest.fail("accepted startup unresolved at the bound")
    assert trace.rejections[0]["backends"][0]["usename"] is None


@pytest.mark.parametrize(
    "replacement",
    [
        (),
        (trace_backend(42, SINK_ROLE, "dur053-k6-sink", start="2026-11-22T00:00:01Z"),),
        (trace_backend(43, SINK_ROLE, "dur053-k6-sink"),),
    ],
)
def test_phase_c_starting_trace_fails_if_backend_vanishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, replacement: tuple[dict[str, Any], ...]
) -> None:
    trace, _ = synthetic_trace(
        tmp_path,
        monkeypatch,
        trace_snapshot(0, {**trace_backend(42), "client_addr": "10.7.0.2"}),
        trace_snapshot(100_000, *replacement),
    )
    try:
        sample_synthetic(trace)
    except SmokeFailure as error:
        assert str(error) == "DB_STARTUP_VANISHED_SINK", "lost original startup identity"
    else:
        pytest.fail("accepted disappearance of a starting backend")
    first, second = trace.startups[0]["observations"]
    assert first["backends"][0]["pid"] == 42
    assert second["backends"] == []


def registered_sink() -> str:
    return json.dumps(
        {
            "items": [
                {
                    "metadata": {"name": "sink-pod", "uid": "sink-uid", "labels": {"app": SINK}},
                    "spec": {"nodeName": "kind-node"},
                    "status": {"podIPs": [{"ip": "10.7.0.1"}]},
                }
            ]
        }
    )


@pytest.mark.parametrize("translated", [False, True])
def test_phase_c_attributed_startup_can_vanish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, translated: bool
) -> None:
    address = "172.19.0.2" if translated else "10.7.0.1"
    trace, _ = synthetic_trace(
        tmp_path,
        monkeypatch,
        trace_snapshot(0, {**trace_backend(42), "client_addr": address}),
        trace_snapshot(100_000),
    )
    smoke: Any = trace.smoke
    smoke.k.return_value = registered_sink()
    smoke.endpoint_addresses = {"sink_db": "172.19.0.3"}
    smoke.command.return_value = (
        b"tcp 6 9 CLOSE src=10.7.0.1 dst=172.19.0.3 sport=54321 dport=5432 "
        b"src=172.19.0.3 dst=172.19.0.2 sport=5432 dport=45678 [ASSURED]\n"
    )
    sample_synthetic(trace)
    assert trace.rows[0]["sink"]["total"] == 1
    [record] = trace.startups[0]["attributed_vanishes"]
    assert (
        record["client_addr"],
        record["client_port"],
        record["pod_ip"],
        record["pod"]["uid"],
    ) == (address, 45678, "10.7.0.1", "sink-uid")


def test_phase_c_unregistered_pod_cannot_explain_startup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    trace, _ = synthetic_trace(
        tmp_path, monkeypatch, trace_snapshot(0, trace_backend(42)), trace_snapshot(100_000)
    )
    smoke: Any = trace.smoke
    smoke.k.return_value = registered_sink().replace(f'"app": "{SINK}"', '"app": "other"')
    try:
        sample_synthetic(trace)
    except SmokeFailure as error:
        assert str(error) == "DB_STARTUP_VANISHED_SINK"
    else:
        pytest.fail("accepted a vanished startup from an unregistered pod")


def test_phase_c_attributed_startup_vanish_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    trace, _ = synthetic_trace(
        tmp_path,
        monkeypatch,
        trace_snapshot(0, *(trace_backend(pid) for pid in (42, 43, 44))),
        trace_snapshot(100_000),
    )
    smoke: Any = trace.smoke
    smoke.k.return_value = registered_sink()
    try:
        sample_synthetic(trace)
    except SmokeFailure as error:
        assert str(error) == "DB_STARTUP_VANISH_BUDGET_SINK"
    else:
        pytest.fail("accepted more than two vanished startups in one window")


def test_phase_c_starting_trace_requires_backend_start(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    trace, query = synthetic_trace(
        tmp_path,
        monkeypatch,
        trace_snapshot(0, trace_backend(42, start=None)),
        trace_snapshot(100_000, trace_backend(42, SINK_ROLE, "dur053-k6-sink", start=None)),
    )
    try:
        sample_synthetic(trace)
    except SmokeFailure as error:
        assert str(error) == "DB_STARTUP_IDENTITY_SINK"
    else:
        pytest.fail("accepted a starting backend without a start time")
    assert query.call_count == 1
    assert trace.rejections[0]["backends"][0]["backend_start"] is None


def test_phase_c_starting_trace_fails_late_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    trace, _ = synthetic_trace(
        tmp_path,
        monkeypatch,
        trace_snapshot(0, trace_backend(42)),
        *(trace_snapshot(us, trace_backend(42)) for us in range(150_000, 2_000_000, 150_000)),
        trace_snapshot(2_000_001, trace_backend(42, SINK_ROLE, "dur053-k6-sink")),
    )
    try:
        sample_synthetic(trace)
    except SmokeFailure as error:
        assert str(error) == "DB_STARTUP_UNRESOLVED_SINK"
    else:
        pytest.fail("accepted startup resolution after the bound")


@pytest.mark.parametrize(("user", "app"), [("unknown", ""), (None, "unknown"), (None, None)])
def test_phase_c_trace_fails_unknown_pair_immediately(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, user: str | None, app: str | None
) -> None:
    trace, query = synthetic_trace(
        tmp_path,
        monkeypatch,
        trace_snapshot(0, trace_backend(42, user, app)),
        trace_snapshot(100_000, trace_backend(42, SINK_ROLE, "dur053-k6-sink")),
    )
    try:
        sample_synthetic(trace)
    except SmokeFailure as error:
        assert str(error) == "DB_UNACCOUNTED_CLIENT_SINK"
    else:
        pytest.fail("accepted an unregistered pair")
    assert query.call_count == 1
    trace.finish()
    artifact = json.loads((tmp_path / phase_c.TRACE_FILE).read_bytes())
    assert artifact["samples"][0]["sink"].get("clients") == [[user, app, 1]], (
        "discarded the rejected sample"
    )
    assert artifact["rejections"] == [
        {
            "sample": 0,
            "server": "sink",
            "reason": "DB_UNACCOUNTED_CLIENT_SINK",
            "captured_at_us": 100_000_000,
            "backends": [
                {
                    "pid": 42,
                    "backend_start": "2026-11-22T00:00:00Z",
                    "state": "starting" if user is None else "idle",
                    "usename": user,
                    "application_name": app,
                    "wait_event_type": "Client",
                    "wait_event": "ClientRead",
                    "client_addr": "10.7.0.1",
                    "client_port": 45678,
                    "pod_name": "sink-pod",
                }
            ],
        }
    ], "rejection record lost backend or socket details"


def test_phase_c_starting_trace_counts_toward_total_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clients = [
        trace_backend(pid, user, app)
        for pid, (user, app) in enumerate(
            pair for pair, budget in phase_c.SINK_CLIENTS.items() for _ in range(budget)
        )
    ]
    replaced = next(client for client in clients if client["usename"] == SINK_ROLE)
    remaining_clients = [client for client in clients if client is not replaced]
    trace, query = synthetic_trace(
        tmp_path,
        monkeypatch,
        trace_snapshot(0, *clients, trace_backend(42)),
        trace_snapshot(100_000, *remaining_clients, trace_backend(42, SINK_ROLE, "dur053-k6-sink")),
    )
    try:
        sample_synthetic(trace)
    except SmokeFailure as error:
        assert str(error) == "DB_TOTAL_BUDGET_SINK"
    else:
        pytest.fail("excluded starting backends from the total budget")
    assert query.call_count == 1


def test_phase_c_starting_trace_rejects_resolution_over_pair_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = "dur053-k6-sink-exporter"
    existing = trace_backend(41, EXPORTER, app)
    trace, _ = synthetic_trace(
        tmp_path,
        monkeypatch,
        trace_snapshot(0, existing, trace_backend(42)),
        trace_snapshot(100_000, existing, trace_backend(42, EXPORTER, app)),
    )
    try:
        sample_synthetic(trace)
    except SmokeFailure as error:
        assert str(error) == "DB_CLIENT_BUDGET_SINK"
    else:
        pytest.fail("accepted a resolved pair over budget")


def test_phase_c_starting_trace_rejects_gap_over_200_ms(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    trace, _ = synthetic_trace(
        tmp_path,
        monkeypatch,
        trace_snapshot(0, trace_backend(42)),
        trace_snapshot(200_001, trace_backend(42, SINK_ROLE, "dur053-k6-sink")),
    )
    try:
        sample_synthetic(trace)
    except SmokeFailure as error:
        assert str(error) == "DB_STARTUP_SAMPLE_GAP_SINK"
    else:
        pytest.fail("accepted a gap over 200 ms")


def test_phase_c_trace_paces_followups_in_one_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = TraceClock()
    trace, query = synthetic_trace(tmp_path, monkeypatch, clock=clock)
    started = clock.monotonic()

    def snapshot(_timeout: float) -> str:
        elapsed_us = round((clock.monotonic() - started) * 1_000_000)
        backend = (
            trace_backend(42)
            if elapsed_us < 600_000
            else trace_backend(42, SINK_ROLE, "dur053-k6-sink")
        )
        return json.dumps(trace_snapshot(elapsed_us, backend))

    query.side_effect = snapshot
    try:
        sample_synthetic(trace)
    except SmokeFailure as error:
        pytest.fail(f"persistent trace missed the server cadence: {error}")
    observations = trace.startups[0]["observations"]
    assert len(observations) >= 4
    assert trace.startups[0]["outcome"] == "resolved"


def test_phase_c_credential_scan(tmp_path: Path) -> None:
    evidence = phase_c.PhaseCEvidence(tmp_path)
    empty_trace = json.loads((tmp_path / phase_c.TRACE_FILE).read_bytes())
    assert empty_trace["rejections"] == []
    evidence.secret("phase-c-private-password")
    for index, prohibited in enumerate(
        (
            b"phase-c-private-password",
            b"SCRAM-SHA-256$4096:verifier",
            b"-----BEGIN PRIVATE KEY-----",
            b"eyJhbGciOiJIUzI1NiJ9.someLongJWTComposition.signatureNotReal",
        )
    ):
        with pytest.raises(SmokeFailure, match="CREDENTIAL_SCAN_FAILED"):
            evidence.write_bytes("blocked.jsonl", prohibited)
        (tmp_path / f"unsafe-{index}.json").write_bytes(prohibited)
    with pytest.raises(SmokeFailure, match="CREDENTIAL_SCAN_FAILED"):
        evidence.scan()
    assert {p.name for p in tmp_path.iterdir()} == {"smoke.json", phase_c.TRACE_FILE}


def test_phase_c_handoff_is_explicit_after_b_and_fail_isolated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    order = []
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.setenv("DUR053_K6_C_OUTPUT", str(tmp_path / "from-environment"))
    monkeypatch.setattr(dur053_k3, "matched_arms", lambda *_: None)
    monkeypatch.setattr(dur053_k3, "scratch_ownership", lambda *_: None)
    monkeypatch.setattr(dur053_k3, "artifact_hashes", lambda *_: {})
    monkeypatch.setattr(dur053_k3, "LIVE_RESERVATION", 0)

    def stub(output: Path, scratch: Path) -> StubK3Driver:
        driver = StubK3Driver(output, scratch)
        order.append(driver)
        return driver

    def run_b(output: Path, *_args: object) -> int:
        order.append(output)
        return 0

    def run_c(output: Path, *_args: object) -> int:
        order.append(output)
        raise RuntimeError("synthetic Phase C failure")

    monkeypatch.setattr(dur053_k3, "Driver", stub)
    monkeypatch.setattr(phase_b, "run", run_b)
    monkeypatch.setattr(phase_c, "run", run_c)
    k3, scratch, b, c = (tmp_path / part for part in ("k3", "scratch", "b", "c"))
    assert dur053_k3.run("smoke", k3, scratch, b, c) == 1
    driver, *phases = order
    assert phases == [b, c]
    assert "DUR053_K6_C_OUTPUT" not in driver.unittest_env
    assert not (tmp_path / "from-environment").exists()
    assert json.loads((k3 / "manifest.json").read_bytes())["status"] == "PASS"
    report = json.loads((c / "smoke.json").read_bytes())
    assert (report["status"], report["reason"]) == ("FAIL", "UNEXPECTED_RUNTIMEERROR")
    assert (report["measurement_claim"], report["charged_seconds"]) == (False, 0)
