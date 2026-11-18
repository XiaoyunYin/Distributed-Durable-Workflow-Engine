"""Offline behavioral checks for the campaign-only D2b boundaries."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from functools import partial
from pathlib import Path
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import dur053_k6_d2b_smoke as smoke  # noqa: E402
import dur053_k6_hook as hook  # noqa: E402
from dur053_k6_d2b_join import exchanges  # noqa: E402
from dur053_k6_join import identity, verify  # noqa: E402
from test_k6_phase_c import fixture  # noqa: E402
from test_k6_worker import Delivery  # noqa: E402
from workers.kafka_worker import handle_delivery  # noqa: E402

TOKENS = {"relay": "a" * 64, "worker": "b" * 64}


@pytest.fixture
def receiver(tmp_path):
    instance = hook.Receiver(tmp_path / "host.jsonl", TOKENS)
    try:
        yield instance
    finally:
        instance.file.close()


def registered(receiver):
    target = dict(actor="relay", uid="pod-uid", process_start_id="1:123", session=uuid4().hex)
    receiver.event(
        hook.encode({"event": "register", "identity": target}), "Bearer " + TOKENS["relay"]
    )
    return target


def message(target):
    return dict(
        event="boundary_reached",
        identity=target,
        boundary="relay-post-commit",
        fields=dict(key="key", invocation="invocation", request="request", application="app"),
        hook_id=uuid4().hex,
        nonce=uuid4().hex,
    )


def receive(receiver, row):
    return receiver.event(hook.encode(row), "Bearer " + TOKENS["relay"])


def arm_and_reach(receiver):
    target = registered(receiver)
    reach = message(target)
    arm = receiver.arm("episode", reach["boundary"], target, dict(key="key"), 10)
    return arm, reach


def test_forged_relay_actor_is_rejected(receiver):
    victim = registered(receiver)
    reach = message(victim)
    receiver.arm("episode", reach["boundary"], victim, dict(key="key"), 10)
    with pytest.raises(ValueError, match="HOOK_AUTH"):
        receiver.event(hook.encode(reach), "Bearer " + TOKENS["worker"])
    assert receiver.observe("episode")["state"] == "armed"


def test_repeated_arm_is_rejected(receiver):
    _, reach = arm_and_reach(receiver)
    with pytest.raises(ValueError, match="HOOK_REPEATED_ARM"):
        receiver.arm("episode", reach["boundary"], reach["identity"], dict(key="another"), 10)
    with pytest.raises(ValueError, match="HOOK_REPLAYED_ARM"):
        receiver.arm("different-episode", reach["boundary"], reach["identity"], dict(key="key"), 10)
    with pytest.raises(ValueError, match="HOOK_COMPANION_BOUNDARY"):
        receiver.arm("episode", "successor", reach["identity"], dict(key="key"), 10, companion=True)


def test_explicit_companion_hold_keeps_both_acks_distinct(tmp_path):
    receiver = hook.Receiver(tmp_path / "companion.jsonl", TOKENS, companion_boundary="successor")
    try:
        primary, reach = arm_and_reach(receiver)
        target = reach["identity"]
        second = receiver.arm("episode", "successor", target, dict(key="key"), 10, companion=True)
        primary_ack = receive(receiver, reach)
        second_ack = receive(receiver, {**message(target), "boundary": "successor"})
        assert primary_ack["arm"] == primary["arm"] != second["arm"] == second_ack["arm"]
        assert receiver.observe("episode")["state"] == "reached"
        assert receiver.observe("episode", companion=True)["state"] == "reached"
    finally:
        receiver.file.close()


def test_no_ack_or_action_if_fsync_fails(receiver, monkeypatch):
    _, reach = arm_and_reach(receiver)

    def failed_sync(_fd):
        raise OSError("simulated fsync failure")

    monkeypatch.setattr(hook.os, "fsync", failed_sync)
    with pytest.raises(OSError, match="simulated fsync failure"):
        receive(receiver, reach)
    assert receiver.observe("episode")["state"] == "armed"
    assert [row["event"] for row in receiver.rows] == ["register", "arm"]


def test_arm_ack_precedes_gate_and_command_is_one_shot(receiver, tmp_path):
    arm, reach = arm_and_reach(receiver)
    rows = (tmp_path / "host.jsonl").read_bytes().splitlines()
    assert arm["arm_ack"] == {"sequence": 2, "sha256": identity(rows[1].decode())}
    ack = receive(receiver, reach)
    assert ack["sequence"] == 3 and ack["sha256"] == identity(hook.encode(reach).decode())
    assert ack["journal_sha256"] == identity((tmp_path / "host.jsonl").read_text().splitlines()[2])
    proof = {key: ack[key] for key in ("sequence", "sha256", "journal_sha256")}
    with pytest.raises(ValueError, match="HOOK_NOT_ACKNOWLEDGED_HELD"):
        receiver.authorize("episode", proof, "drop", evidence={})
    with pytest.raises(ValueError, match="HOOK_REPLAYED_NONCE"):
        receive(receiver, reach)
    held = {**reach, **proof, "event": "held", "arm": ack["arm"]}
    with pytest.raises(ValueError, match="HOOK_ACK_MISMATCH"):
        receive(receiver, {**held, "sequence": ack["sequence"] + 1})
    assert receive(receiver, held)["command"] == "wait"
    receiver.authorize("episode", proof, "drop", evidence={})
    assert receive(receiver, held)["command"] == "drop"
    with pytest.raises(ValueError, match="HOOK_COMMAND_ALREADY_ISSUED"):
        receive(receiver, held)
    assert receive(receiver, {**held, "event": "released"})["command"] == "done"
    with pytest.raises(FileExistsError):
        hook.Receiver(tmp_path / "host.jsonl", TOKENS)


def test_stale_registration_and_hold_expiry(receiver, monkeypatch):
    arm, reach = arm_and_reach(receiver)
    ack = receive(receiver, reach)
    monkeypatch.setattr(hook, "now_ms", lambda: arm["until_ms"] + 1)
    assert receiver.observe("episode")["state"] == "expired"
    with pytest.raises(ValueError, match="HOOK_NO_LIVE_ARM"):
        receive(receiver, {**reach, **ack, "event": "held"})
    registered(receiver)
    with pytest.raises(ValueError, match="HOOK_TARGET_STALE"):
        receive(receiver, {**reach, "nonce": uuid4().hex})


def duplicate_rows():
    journal, sink, options = fixture()
    first, second = journal[:5], journal[5:10]
    events = ("copy_open", "sink_send", "sink_response", "extra_reply", "close")
    second = [
        {
            **row,
            "event": event,
            "copy": 2,
            "ordinal": 1,
            "worker_binding": first[0]["worker_binding"],
        }
        for row, event in zip(second, events, strict=True)
    ]
    for call in sink["calls"]:
        call["message_id"] = "invocation"
    options["worker_observations"] = options["worker_observations"][:1]
    options["exchange_reader"] = partial(exchanges, faults={identity("request"): "medium-14"})
    rows = first[:3] + second + first[3:] + journal[-1:]
    for sequence, row in enumerate(rows, 1):
        row["sequence"] = sequence
    return rows, sink, options


def test_duplicate_joins_two_distinct_sink_calls_to_one_application():
    records, snapshot, options = duplicate_rows()
    verdict = verify(records, snapshot, **options)
    assert verdict["applications"] == 1
    assert [row["call_id"] for row in verdict["joined"]] == [1, 2]
    assert len({row["connection_hash"] for row in verdict["joined"]}) == 2
    assert len({row["response_hash"] for row in verdict["joined"]}) == 1


def test_missing_second_delivery_record_is_rejected():
    records, _, _ = duplicate_rows()
    rows = [r for r in records[:-1] if not (r.get("copy") == 2 and r["event"] == "sink_send")]
    with pytest.raises(ValueError, match="D2B_SECOND_DELIVERY_INCOMPLETE"):
        list(exchanges({("session", 1): rows}, {identity("request"): "medium-14"}))


def test_campaign_join_rejects_sink_bypass():
    records, snapshot, options = duplicate_rows()
    snapshot["calls"].append(
        {**snapshot["calls"][1], "call_id": 3, "connection_hash": identity("bypass")}
    )
    with pytest.raises(ValueError, match="SINK_UNWITNESSED_CALL"):
        verify(records, snapshot, **options)


@pytest.mark.parametrize("cell", ["medium-13", "medium-15"])
def test_withheld_reply_has_independent_caller_close_and_zero_bytes(cell):
    records, _, _ = duplicate_rows()
    primary = [r for r in records[:-1] if not r.get("copy")]
    inbound, sent, response, delivery, close = primary
    caller_closed = {**close, "event": "caller_closed"}
    drop = {**close, "event": "worker_drop"}
    if cell == "medium-13":
        rows = [inbound, sent, caller_closed, response, drop, close]
    else:
        rows = [
            inbound,
            sent,
            response,
            caller_closed,
            {**response, "event": "fault_drop"},
            drop,
            close,
        ]
    assert list(exchanges({("session", 1): rows}, {identity("request"): cell})) == [
        (inbound, response, False)
    ]
    with pytest.raises(ValueError, match="D2B_WITHHELD_EXCHANGE_INCOMPLETE"):
        list(
            exchanges({("session", 1): [*rows[:-2], delivery, close]}, {identity("request"): cell})
        )


def test_ordinary_python_worker_has_no_campaign_entry_point_or_hold(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    script = """
import importlib.util
from workers.k6_effect import EffectAdapter
assert importlib.util.find_spec('dur053_k6_campaign') is None
adapter = EffectAdapter.configured()
adapter.transfer('pre-send', 'apply', {}, {}, 'ordinary', b'')
"""
    monkeypatch.setenv("PYTHONPATH", str(root / "python"))
    env = {
        **os.environ,
        "DUR053_K6_ACTIVITY": "1",
        "POD_UID": "ordinary",
        "DUR053_K6_HOOK_URL": "https://127.0.0.1:1",
        "DUR053_K6_HOOK_CA_FILE": "/missing",
        "DUR053_K6_HOOK_TOKEN": "a" * 64,
        "DUR053_K6_HOOK_WORKLOAD": "{}",
        "DUR053_K6_RELAY_URL": "https://127.0.0.1:2",
        "DUR053_K6_RELAY_CA_FILE": "/missing",
        "DUR053_K6_SINK_TOKEN_FILE": "/missing",
    }
    run = subprocess.run(
        [sys.executable, "-c", script], env=env, cwd=root, capture_output=True, timeout=5
    )
    assert run.returncode == 0, f"ordinary worker activated the campaign: {run.stderr!r}"
    assert run.stdout == b""


def test_campaign_worker_holds_after_delay_and_before_wire_send(receiver, monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "campaign/k6"))
    from dur053_k6_campaign.worker import Adapter, Hook

    delivery = Delivery(monkeypatch, tmp_path)
    client = object.__new__(Hook)
    target = {
        "actor": "worker",
        "uid": "worker-uid",
        "session": uuid4().hex,
        "process_start_id": "1:321",
    }
    auth = "Bearer " + TOKENS["worker"]
    receiver.event(hook.encode(dict(event="register", identity=target)), auth)
    fields = {
        "key": delivery.claim["logical_key"],
        "invocation": str(uuid4()),
        "request": str(uuid4()),
    }
    receiver.arm("episode", "worker-pre-send", target, fields, 10)
    client.identity = target

    def send(row, _timeout=2):
        raw = hook.encode({**row, "identity": target})
        reply = receiver.event(raw, auth)
        if reply["command"] == "ack":
            assert delivery.events[-1] == ("connect",)
            assert ("delay", 5) in delivery.events
            assert not any(event[0] == "request" for event in delivery.events)
        if reply["command"] == "wait":
            held = receiver.observe("episode")
            receiver.authorize("episode", held["ack"], "release", evidence={})
        return reply, identity(raw.decode())

    client.send = send
    adapter = Adapter(
        client, {fields["key"]: {"invocation": fields["invocation"], "apply": fields["request"]}}
    )
    handle_delivery(
        delivery.consumer,
        delivery.message,
        delivery.control,
        "worker",
        effect_factory=lambda: adapter,
    )
    assert receiver.observe("episode")["state"] == "done"
    sent = next(event[2] for event in delivery.events if event[0] == "request")
    assert (sent["message_id"], sent["request_id"]) == (fields["invocation"], fields["request"])


def test_chaos_selector_and_ack_use_exact_container():
    target = {"pod": "worker-exact", "container": "worker"}
    spec = smoke.kill_object(target, "kill")["spec"]
    assert spec == {
        "action": "container-kill",
        "mode": "one",
        "containerNames": ["worker"],
        "selector": {"namespaces": [smoke.NS], "pods": {smoke.NS: ["worker-exact"]}},
    }
    row = {"id": f"{smoke.NS}/worker-exact/worker", "phase": "Injected"}
    backend = {
        "status": {
            "conditions": [{"type": "AllInjected", "status": "True"}],
            "experiment": {"containerRecords": [row]},
        }
    }
    assert smoke.exact_chaos_ack(backend, target)
    row["id"] += "-wrong"
    assert not smoke.exact_chaos_ack(backend, target)


def test_d2b_missing_host_records_ordered_failure(tmp_path):
    output = tmp_path / "d2b"
    smoke.inherited_failure(output)
    evidence = json.loads((output / "smoke.json").read_text())
    assert evidence["reason"] == "SHARED_KIND_HARNESS_FAILED_BEFORE_PHASE_D2B"
    assert evidence["status"] == "FAIL"
    assert evidence["charged_seconds"] == 0 and evidence["measurement_claim"] is False
