"""Offline corruption, budget, credential scan and explicit handoff checks."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dur053_k3  # noqa: E402
import dur053_k6_b_smoke as phase_b  # noqa: E402
import dur053_k6_c_smoke as phase_c  # noqa: E402
from dur053_k6_b_evidence import SmokeFailure  # noqa: E402
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
    sample = {"limit": 30, "reserved": 3, "clients": [[phase_c.SINK_ROLE, "dur053-k6-sink", 8]]}
    assert phase_c.trace_count("sink", sample) == 8
    sample["clients"][0][2] = 9
    with pytest.raises(SmokeFailure, match="DB_CLIENT_BUDGET_SINK"):
        phase_c.trace_count("sink", sample)
    sample["clients"][0][1] = "unregistered"
    with pytest.raises(SmokeFailure, match="DB_UNACCOUNTED_CLIENT_SINK"):
        phase_c.trace_count("sink", sample)


def test_phase_c_credential_scan(tmp_path: Path) -> None:
    evidence = phase_c.PhaseCEvidence(tmp_path)
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
    assert [p.name for p in tmp_path.iterdir()] == ["smoke.json"]


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
