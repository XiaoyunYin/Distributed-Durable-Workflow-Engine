"""Offline tests for deployed hook gates and independent evidence joins."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import dur053_k6_hook as hook  # noqa: E402
from dur053_k6_b_evidence import SmokeFailure  # noqa: E402
from dur053_k6_d2b_smoke import exact_chaos_ack, kill_object  # noqa: E402
from dur053_k6_e1_join import (  # noqa: E402
    EVENTS,
    GROUP,
    SCHEDULERS,
    TASKS,
    claim_at_hold,
    journal,
    offset_after,
    offset_at_hold,
    receipt_after,
    receipt_at_hold,
    relay_after,
    relay_at_hold,
)
from dur053_k6_e1_smoke import (  # noqa: E402
    ACTORS,
    CELLS,
    NS,
    E1Evidence,
    deployment_objects,
    e1_addresses,
    e1_receiver_address,
    e1_receiver_san,
    e1_receiver_url,
    ensure_no_e1_placeholders,
    pushed_digest,
    workload,
)
from workers.control import Claim  # noqa: E402
from workers.kafka_worker import handle_delivery  # noqa: E402
from workers.registry import ActivityRegistry  # noqa: E402
from workers.runner import ActivityRunner, ActivityTask  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
TOKEN = "a" * 64
WORKER = {"actor": "worker", "uid": "pod-uid", "process_start_id": "1:50", "session": "b" * 32}
WORKFLOW, EVENT = "e1-hook-3", "event"


def test_e1_pushed_digest_selects_its_repository_from_multiple_entries():
    selected = "localhost:5001/dur053-k6-e1-worker@sha256:" + "a" * 64
    base = "localhost:5001/dur053-worker@sha256:" + "b" * 64
    assert pushed_digest([base, selected], "localhost:5001/dur053-k6-e1-worker") == selected


@pytest.mark.parametrize(
    "digests",
    [
        ["localhost:5001/other@sha256:" + "a" * 64],
        [
            "localhost:5001/dur053-k6-e1-worker@sha256:" + "a" * 64,
            "localhost:5001/dur053-k6-e1-worker@sha256:" + "b" * 64,
        ],
    ],
)
def test_e1_pushed_digest_requires_exactly_one_repository_match(digests):
    with pytest.raises(SmokeFailure, match="E1_IMAGE_DIGEST_AMBIGUOUS"):
        pushed_digest(digests, "localhost:5001/dur053-k6-e1-worker")


def test_e1_addresses_require_all_dependency_addresses():
    addresses = {
        "192.0.2.1": "192.0.2.10",
        "192.0.2.2": "192.0.2.20",
        "192.0.2.3": "10.96.0.1",
        "192.0.2.4": "172.18.0.2",
        "unused": "not-an-ip",
    }
    assert e1_addresses(addresses) == {key: addresses[key] for key in addresses if key != "unused"}
    for key in ("192.0.2.3", "192.0.2.4"):
        missing = addresses.copy()
        del missing[key]
        with pytest.raises(SmokeFailure, match="E1_ADDRESS_INVALID"):
            e1_addresses(missing)
        invalid = addresses.copy()
        invalid[key] = "not-an-ip"
        with pytest.raises(SmokeFailure, match="E1_ADDRESS_INVALID"):
            e1_addresses(invalid)


def test_e1_receiver_bind_and_certificate_use_kind_gateway_not_engine_db():
    calls = []

    def inspect(*args):
        calls.append(args)
        return b"172.18.0.1\n"

    gateway = e1_receiver_address(inspect, "172.18.0.4")

    assert gateway == "172.18.0.1"
    assert gateway != "172.18.0.4"
    assert calls == [
        (
            "E1_KIND_BRIDGE_GATEWAY",
            "docker",
            "inspect",
            "--format",
            '{{(index .NetworkSettings.Networks "kind").Gateway}}',
            "dur053-k3a-control-plane",
        )
    ]
    assert e1_receiver_san(gateway) == "subjectAltName=IP:172.18.0.1"
    assert e1_receiver_url(gateway, 44321) == "https://172.18.0.1:44321"


def test_e1_unexpected_error_is_diagnosable_and_fails_closed_on_credentials(tmp_path):
    evidence = E1Evidence(tmp_path)
    evidence.current_step = "a-build-tagged-images"
    evidence.current_command = "E1_IMAGE_DIGEST"
    assert evidence.safe_reason(ValueError("bad digest")) == (
        "UNEXPECTED_VALUEERROR step=a-build-tagged-images "
        "command=E1_IMAGE_DIGEST message=bad digest"
    )
    evidence.secret("private-token")
    assert evidence.safe_reason(ValueError("private-token")) == "CREDENTIAL_SCAN_FAILED"


def delivered(commit):
    message = SimpleNamespace(
        headers=[("event-id", EVENT.encode()), ("workflow-id", WORKFLOW.encode())],
        key=EVENT.encode(),
        value=b"{}",
        topic=TASKS,
        partition=0,
        offset=0,
    )
    control = SimpleNamespace(
        _post=lambda *_: {"commit_offset": True, "next_offset": 1, "disposition": "ACCEPTED"}
    )
    return SimpleNamespace(commit=commit), message, control, "worker"


def register(receiver, target=WORKER):
    return receiver.event(hook.encode({"event": "register", "identity": target}), f"Bearer {TOKEN}")


def test_deployed_worker_cannot_commit_before_reach_fsync(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "campaign/k6"))
    from dur053_k6_campaign.worker import Deployment, Hook

    receiver = hook.Receiver(tmp_path / "journal", {"worker": TOKEN})
    try:
        register(receiver)
        receiver.arm("hook-3", "before_offset_ack", WORKER, {"key": WORKFLOW, "cell": "hook-3"}, 10)
        implementation = Deployment.__new__(Deployment)
        implementation.hook = Hook.__new__(Hook)
        implementation.hook.identity = WORKER

        def send(message, _timeout=2):
            raw = hook.encode({**message, "identity": WORKER})
            result = receiver.event(raw, f"Bearer {TOKEN}")
            if message["event"] == "held" and receiver.observe("hook-3")["state"] == "held":
                arm = receiver.observe("hook-3")
                receiver.authorize("hook-3", arm["ack"], "release", evidence={})
            return result, hashlib.sha256(raw).hexdigest()

        monkeypatch.setattr(implementation.hook, "send", send)
        committed = []

        def unavailable(_):
            raise OSError("test journal fsync failure")

        monkeypatch.setattr(hook.os, "fsync", unavailable)
        with pytest.raises(OSError, match="test journal fsync failure"):
            handle_delivery(*delivered(committed.append), deployment_hook=implementation.observe)
        assert committed == []
        assert receiver.observe("hook-3")["state"] == "armed"
    finally:
        receiver.file.close()


def test_stale_deployed_session_cannot_take_over_arm(tmp_path):
    receiver = hook.Receiver(tmp_path / "journal", {"worker": TOKEN})
    try:
        register(receiver)
        receiver.arm("hook-3", "before_offset_ack", WORKER, {"key": WORKFLOW}, 10)
        replacement = {**WORKER, "session": "c" * 32, "process_start_id": "1:51"}
        register(receiver, replacement)
        with pytest.raises(ValueError, match="HOOK_REPLAYED_REGISTRATION"):
            register(receiver)
        raw = hook.encode(
            {
                "event": "boundary_reached",
                "boundary": "before_offset_ack",
                "identity": WORKER,
                "fields": {"key": WORKFLOW},
                "hook_id": "d" * 32,
                "nonce": "e" * 32,
            }
        )
        with pytest.raises(ValueError, match="HOOK_TARGET_STALE"):
            receiver.event(raw, f"Bearer {TOKEN}")
    finally:
        receiver.file.close()


def test_durable_ack_requires_retained_journal_and_fresh_nonce(tmp_path, monkeypatch):
    path = tmp_path / "journal"
    receiver = hook.Receiver(path, {"worker": TOKEN})
    try:
        register(receiver)
        receiver.arm("hook-3", "before_offset_ack", WORKER, {"key": WORKFLOW}, 10)
        message = {
            "event": "boundary_reached",
            "boundary": "before_offset_ack",
            "identity": WORKER,
            "fields": {"key": WORKFLOW},
            "hook_id": "d" * 32,
            "nonce": "e" * 32,
        }
        auth = f"Bearer {TOKEN}"
        ack = receiver.event(hook.encode(message), auth)
        with pytest.raises(ValueError, match="HOOK_REPLAYED_NONCE"):
            receiver.event(hook.encode({**message, "hook_id": "f" * 32}), auth)
        held = {**message, **ack, "event": "held"}
        with pytest.raises(ValueError, match="HOOK_ACK_MISMATCH"):
            receiver.event(hook.encode({**held, "sha256": "f" * 64}), auth)
        assert receiver.event(hook.encode(held), auth)["command"] == "wait"
        arm = receiver.observe("hook-3")
        assert journal(path.read_bytes(), arm)["reach"]["hook_id"] == message["hook_id"]
        with pytest.raises(SmokeFailure, match="E1_JOURNAL_FRAMED"):
            journal(b"", arm)
        with pytest.raises(SmokeFailure, match="E1_JOURNAL_BOUND_HASH"):
            journal(path.read_bytes().replace(b"d" * 32, b"f" * 32), arm)
        monkeypatch.setattr(hook, "now_ms", lambda: arm["until_ms"])
        with pytest.raises(ValueError, match="HOOK_NO_LIVE_ARM"):
            receiver.event(hook.encode(held), auth)
    finally:
        receiver.file.close()


def test_ordinary_worker_never_loads_deployed_hooks(monkeypatch):
    for key, value in {
        "DUR053_K6_DEPLOYED_HOOKS": "1",
        "DUR053_K6_HOOK_URL": "https://127.0.0.1:1",
        "DUR053_K6_HOOK_CA_FILE": "unreachable",
        "DUR053_K6_HOOK_TOKEN": TOKEN,
        "DUR053_K6_HOOK_WORKLOAD": "{}",
        "POD_UID": "ordinary",
    }.items():
        monkeypatch.setenv(key, value)
    commits = []
    try:
        handle_delivery(*delivered(commits.append))
    except (ImportError, OSError, ValueError) as error:
        pytest.fail(f"ordinary worker activated deployed hook: {type(error).__name__}")
    assert len(commits) == 1


def test_claim_hook_runs_before_activity_entry_and_heartbeat(monkeypatch):
    trace = []
    registry = ActivityRegistry()
    registry.register("pure.echo", "v1", lambda payload: trace.append(payload))
    control = SimpleNamespace(
        claim=lambda *_: Claim(1, "protected-token", "PURE_ACTIVITY", ""),
        result=lambda *_: {"Disposition": "ACCEPTED"},
    )

    def hit(boundary, fields):
        assert boundary == "attempt_claimed"
        assert fields["token_hash"] == hashlib.sha256(b"protected-token").hexdigest()
        assert "protected-token" not in fields.values()
        raise RuntimeError("held before activity entry")

    monkeypatch.setattr("workers.runner.configured_witness", lambda: pytest.fail("entered witness"))
    runner = ActivityRunner(control, registry, deployment_hook=hit)
    task = ActivityTask(WORKFLOW, "pure.echo", 0, "worker", "request", "pure.echo", "v1", {})
    with pytest.raises(RuntimeError, match="held before activity entry"):
        runner.run_task(task)
    assert trace == []


def broker(*records, partitions=8):
    return [
        {
            "partition": p,
            "first": 0,
            "next": len(records) if p == 0 else 0,
            "records": list(records) if p == 0 else [],
        }
        for p in range(partitions)
    ]


def relay_hold():
    return (
        {"event": EVENT, "owner": "old", "attempt": "1", "topic": EVENTS},
        {
            "outbox": [
                {
                    "event_id": EVENT,
                    "event_type": "workflow.created",
                    "topic": EVENTS,
                    "publish_state": "CLAIMED",
                    "claim_owner": "old",
                    "relay_attempts": 1,
                    "published_at": None,
                    "traceparent": "trace",
                }
            ],
            "publications": [],
        },
        {"key": EVENT, "event": EVENT, "workflow": WORKFLOW, "traceparent": "trace", "offset": 0},
    )


def test_hook0_rejects_broker_copy_before_publish():
    objects, db, row = relay_hold()
    assert relay_at_hold("hook-0", WORKFLOW, objects, db, broker()) == []
    with pytest.raises(SmokeFailure, match="E1_BEFORE_PUBLISH_BROKER_COPIES"):
        relay_at_hold("hook-0", WORKFLOW, objects, db, broker(row))


def test_hook1_requires_independent_broker_copy_at_ack():
    objects, db, row = relay_hold()
    assert relay_at_hold("hook-1", WORKFLOW, objects, db, broker(row))[0]["offset"] == 0
    with pytest.raises(SmokeFailure, match="E1_AFTER_ACK_BROKER_COPIES"):
        relay_at_hold("hook-1", WORKFLOW, objects, db, broker())


def test_hook1_replay_joins_replacement_relay_and_original_disposition():
    objects, held, first = relay_hold()
    before = relay_at_hold("hook-1", WORKFLOW, objects, held, broker(first))
    db = {
        "outbox": [
            {
                **held["outbox"][0],
                "publish_state": "PUBLISHED",
                "relay_attempts": 2,
                "workflow_id": WORKFLOW,
            }
        ],
        "publications": [
            {
                "event_id": EVENT,
                "broker_acknowledged": True,
                "relay_attempt": 2,
                "relay_owner": "replacement",
            }
        ],
        "inbox": [
            {
                "event_id": EVENT,
                "consumer_id": SCHEDULERS,
                "disposition": "ACCEPTED",
                "kafka_partition": 0,
                "kafka_offset": 0,
            }
        ],
    }
    broker_log = broker(first, {**first, "offset": 1})
    proof = relay_after(objects, before, db, broker_log, {"session_uuid": "replacement"})
    assert [copy["offset"] for copy in proof["copies"]] == [0, 1]
    with pytest.raises(SmokeFailure, match="E1_REPLACEMENT_RELAY"):
        relay_after(objects, before, db, broker_log, {"session_uuid": "unrelated"})
    with pytest.raises(SmokeFailure, match="E1_REPLACEMENT_BROKER_COPIES"):
        relay_after(objects, before, db, broker(first), {"session_uuid": "replacement"})


def test_hook2_requires_worker_control_commit_audit():
    target = {"uid": WORKER["uid"], "pod": "pod"}
    objects = {
        "node": "pure.echo",
        "iteration": "0",
        "worker": "pod-0",
        "request": "request",
        "token_hash": "hash",
    }
    row = {
        "node_id": "pure.echo",
        "iteration": 0,
        "attempt_number": 1,
        "worker_request_id": "request",
        "worker_id": f"system:serviceaccount:{NS}:worker:pod:pod:uid:{WORKER['uid']}",
        "token_hash": "hash",
        "state": "CLAIMED",
        "is_current": True,
        "result": None,
    }
    db = {"attempts": [row], "claims": [row], "history": [], "result_evidence": []}
    assert claim_at_hold(db, objects, target, NS, []) == row
    with pytest.raises(SmokeFailure, match="E1_WORKER_CONTROL_COMMIT_AUDIT"):
        claim_at_hold({**db, "claims": []}, objects, target, NS, [])


def offset_hold():
    objects = {
        "partition": "0",
        "offset": "1",
        "next_offset": "2",
        "disposition": "ACCEPTED",
        "event": EVENT,
    }
    rows = [
        {"offset": 0, "event_id": "older-event", "disposition": "ACCEPTED", "recorded_at": "first"},
        {"offset": 1, "event_id": EVENT, "disposition": "ACCEPTED", "recorded_at": "second"},
    ]
    log = broker(
        *(
            {"offset": row["offset"], "event": row["event_id"], "key": row["event_id"]}
            for row in rows
        ),
        partitions=1,
    )
    return (
        objects,
        {"next_offset": 2, "rows": rows},
        {
            "group": GROUP,
            "topic": TASKS,
            "partition": 0,
            "next": 1,
        },
        log,
    )


def test_hook3_requires_sampled_broker_group_offset():
    objects, db, group, log = offset_hold()
    assert offset_at_hold(objects, db, group, log) == db["rows"]
    with pytest.raises(SmokeFailure, match="E1_I7_HELD_KAFKA_COMMIT"):
        offset_at_hold(objects, db, {**group, "next": 2}, log)


def test_hook3_checks_every_lower_offset_and_replacement_redelivery():
    objects, db, group, log = offset_hold()
    with pytest.raises(SmokeFailure, match="E1_I7_LOWER_DISPOSITION"):
        offset_at_hold(objects, {**db, "rows": db["rows"][1:]}, group, log)
    before = offset_at_hold(objects, db, group, log)
    callback = {
        **objects,
        "boundary": "before_offset_ack",
        "session": "replacement",
        "disposition": "ALREADY_HANDLED",
    }
    replacement = {"session": "replacement"}
    assert offset_after(objects, before, db, {**group, "next": 2}, [callback], replacement)
    with pytest.raises(SmokeFailure, match="E1_I7_REPLACEMENT_BROKER_COMMIT"):
        offset_after(objects, before, db, group, [callback], replacement)
    with pytest.raises(SmokeFailure, match="E1_I7_REDELIVERY"):
        offset_after(objects, before, db, {**group, "next": 2}, [], replacement)


def test_hook4_requires_durable_kafka_receipt_before_consumption():
    objects = {
        "attempt": "1",
        "node": "pure.echo",
        "iteration": "0",
        "token_hash": "token-hash",
        "result_hash": "result-hash",
        "revision": "4",
        "partition": "1",
        "owner": "old",
        "epoch": "2",
    }
    result = {
        "attempt_number": 1,
        "state": "SUCCEEDED",
        "node_id": "pure.echo",
        "iteration": 0,
        "token_hash": "token-hash",
        "result_hash": "result-hash",
        "is_current": False,
    }
    lease = {"partition_id": 1, "owner_id": "old", "epoch": 2, "live": True}
    target = {"session_uuid": "session", "uid": "uid"}
    db = {
        "attempts": [result],
        "workflow": {"revision": 4},
        "history": [{"reason": "ATTEMPT_RESULT_RECORDED", "revision": 4}],
        "leases": [lease],
        "audit": [{**lease, "kind": "acquire", "session_id": "session", "pod_uid": "uid"}],
        "outbox": [{"event_type": "activity.result", "aggregate_revision": 4, "event_id": EVENT}],
        "inbox": [{"consumer_id": SCHEDULERS, "event_id": EVENT, "disposition": "ACCEPTED"}],
    }
    assert receipt_at_hold(objects, db, target)["receipt"] == result
    with pytest.raises(SmokeFailure, match="E1_DEPLOYED_KAFKA_RECEIPT"):
        receipt_at_hold(objects, {**db, "inbox": []}, target)


def test_hook4_requires_one_fenced_consumption_from_replacement_session():
    receipt = {"attempt_number": 1, "token_hash": "hash"}
    before = {
        "receipt": receipt,
        "record": {"revision": 3},
        "lease": {"epoch": 1, "owner_id": "old"},
    }
    consumed = {
        "reason": "RESULT_CONSUMED",
        "revision": 4,
        "attempt_number": 1,
        "transition_id": "transition",
        "scheduler_epoch": 2,
    }
    audit = {"owner_id": "successor", "epoch": 2, "session_id": "new", "pod_uid": "pod"}
    db = {
        "attempts": [receipt],
        "history": [consumed],
        "audit": [
            {**audit, "kind": "history", "transition_id": "transition"},
            {**audit, "kind": "acquire"},
        ],
    }
    assert receipt_after(before, db, {"session_uuid": "new", "uid": "pod"})["consumed"] == consumed
    with pytest.raises(SmokeFailure, match="E1_REPLACEMENT_OWNER_COMMIT"):
        receipt_after(before, db, {"session_uuid": "old", "uid": "pod"})
    with pytest.raises(SmokeFailure, match="E1_OWNER_CONSUMPTION_MATCHED_LINES"):
        receipt_after(
            before, {**db, "history": [consumed, consumed]}, {"session_uuid": "new", "uid": "pod"}
        )


def test_e1_render_binds_only_the_five_hooks_to_tagged_roles():
    base = json.loads((ROOT / "deploy/k8s/base/resources.yaml").read_bytes())["items"]
    manifests = deployment_objects(
        base,
        {"runtime": "tagged-go", "worker": "tagged-python"},
        "192.0.2.1",
        443,
    )
    hook_roles = set()
    for item in manifests:
        if item["kind"] != "Deployment":
            continue
        name = item["metadata"]["name"]
        (container,) = item["spec"]["template"]["spec"]["containers"]
        env = {row["name"]: row.get("value") for row in container["env"]}
        if "DUR053_K6_HOOK_URL" in env:
            hook_roles.add(name)
            assert env["DUR053_K6_HOOK_URL"] == "https://192.0.2.1:443"
            assert container["image"] == ("tagged-python" if name == "worker" else "tagged-go")
        else:
            assert container["image"] not in {"tagged-go", "tagged-python"}
    assert hook_roles == set(ACTORS)
    hook_policies = [
        item
        for item in manifests
        if item["kind"] == "NetworkPolicy" and item["metadata"]["name"].startswith("e1-hook-")
    ]
    assert len(hook_policies) == len(ACTORS)
    for item in hook_policies:
        assert item["spec"]["egress"] == [
            {
                "to": [{"ipBlock": {"cidr": "192.0.2.1/32"}}],
                "ports": [{"protocol": "TCP", "port": 443}],
            }
        ]
    assert set(CELLS) == {f"hook-{i}" for i in range(5)}
    assert all(workload(cell)[0]["initial_node_id"] == "pure.echo" for cell in CELLS)


def test_e1_render_substitutes_api_addresses_and_rejects_placeholders():
    addresses = {
        "192.0.2.1": "172.18.0.10",
        "192.0.2.2": "172.18.0.20",
        "192.0.2.3": "10.96.0.1",
        "192.0.2.4": "172.18.0.2",
    }
    base = json.loads((ROOT / "deploy/k8s/base/resources.yaml").read_bytes())["items"]
    rendered = json.dumps(base)
    for placeholder, address in addresses.items():
        rendered = rendered.replace(placeholder + "/32", address + "/32")
    substituted = json.loads(rendered)
    manifests = deployment_objects(
        substituted,
        {"runtime": "tagged-go", "worker": "tagged-python"},
        "172.18.0.1",
        443,
    )
    for name in ("api-egress", "worker-control-egress"):
        policy = next(
            item
            for item in manifests
            if item["kind"] == "NetworkPolicy" and item["metadata"]["name"] == name
        )
        cidrs = {
            peer["ipBlock"]["cidr"]
            for rule in policy["spec"]["egress"]
            for peer in rule.get("to", [])
            if "ipBlock" in peer
        }
        assert {"10.96.0.1/32", "172.18.0.2/32"} <= cidrs
        assert not any(cidr.startswith("192.0.2.") for cidr in cidrs)
    ensure_no_e1_placeholders(manifests)

    unresolved = [
        {
            "kind": "NetworkPolicy",
            "metadata": {"name": "api-egress"},
            "spec": {"egress": [{"to": [{"ipBlock": {"cidr": "192.0.2.3/32"}}]}]},
        }
    ]
    with pytest.raises(SmokeFailure, match="E1_PLACEHOLDER_UNRESOLVED NetworkPolicy/api-egress"):
        ensure_no_e1_placeholders(unresolved)


def test_e1_chaos_scope_requires_exact_container_ack():
    target = {"pod": "deployed-relay", "container": "relay"}
    spec = kill_object(target, "e1-hook-0", NS)["spec"]
    assert spec == {
        "action": "container-kill",
        "mode": "one",
        "selector": {"namespaces": [NS], "pods": {NS: [target["pod"]]}},
        "containerNames": ["relay"],
    }
    status = {
        "status": {
            "conditions": [{"type": "AllInjected", "status": "True"}],
            "experiment": {
                "containerRecords": [{"id": f"{NS}/deployed-relay/relay", "phase": "Injected"}]
            },
        },
    }
    assert exact_chaos_ack(status, target, NS)
    assert not exact_chaos_ack(status, {**target, "pod": "other"}, NS)
