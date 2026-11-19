"""Exercise the deployed lease takeover evidence against adverse fixtures."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import dur053_k6_e1_smoke as observers  # noqa: E402
import dur053_k6_e2_smoke as smoke  # noqa: E402
from dur053_k6_b_evidence import SmokeFailure  # noqa: E402
from dur053_k6_e2_join import (  # noqa: E402
    CELL,
    COMPANION,
    PRIMARY,
    WATCHED,
    confirm_rejection,
    lease_at_hold,
    reject_stale_writes,
    takeover,
    timeline,
)
from dur053_k6_hook import Receiver, encode  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]


def test_compile_every_deployed_observer_script():
    for name in ("OBSERVER_SCRIPT", "SUBMIT_TRACED", "READ_WITNESS", "READ_WITNESS_INVENTORY"):
        compile(getattr(observers, name), name, "exec")
    assert smoke.SUBMIT_TRACED is observers.SUBMIT_TRACED


def test_render_two_schedulers_with_distinct_hook_identities_and_no_placeholders():
    base = json.loads((ROOT / "deploy/k8s/base/resources.yaml").read_bytes())["items"]
    addresses = ("172.18.0.10", "172.18.0.20", "10.96.0.1", "172.18.0.2")
    raw = json.dumps(base)
    for index, address in enumerate(addresses, 1):
        raw = raw.replace(f"192.0.2.{index}/32", f"{address}/32")
    tags = {"runtime": "tagged-go", "worker": "tagged-python"}
    objects = smoke.deployment_objects(json.loads(raw), tags, "172.18.0.1", 443)
    schedulers = {
        item["metadata"]["name"]: item["spec"]
        for item in objects
        if item["kind"] == "Deployment"
        and item["spec"]["selector"]["matchLabels"].get("app") == "scheduler"
    }
    assert {name: spec["replicas"] for name, spec in schedulers.items()} == {
        "scheduler": 1,
        "scheduler-peer": 0,
    }
    for name, role in (("scheduler", "predecessor"), ("scheduler-peer", "successor")):
        spec = schedulers[name]
        assert spec["selector"]["matchLabels"][smoke.ROLE_LABEL] == role
        assert spec["template"]["metadata"]["labels"][smoke.ROLE_LABEL] == role
        (container,) = spec["template"]["spec"]["containers"]
        assert container["image"] == tags["runtime"]
        env = {item["name"]: item for item in container["env"]}
        assert env["DUR053_K6_LEASE_TAKEOVER_ROLE"]["value"] == role
        assert env["DUR053_K6_LEASE_TAKEOVER_WORKFLOW"]["value"] == smoke.WORKFLOW
        assert env["DUR053_K6_HOOK_URL"]["value"] == "https://172.18.0.1:443"
        assert env["DUR053_K6_HOOK_TOKEN"]["valueFrom"]["secretKeyRef"]["name"] == f"hook-{role}"
    with pytest.raises(SmokeFailure, match="E1_PLACEHOLDER_UNRESOLVED"):
        smoke.deployment_objects(base, tags, "172.18.0.1", 443)


@pytest.fixture
def challenge():
    target = {
        "actor": "predecessor",
        "uid": "predecessor-pod",
        "session": "b" * 32,
        "session_uuid": str(UUID("b" * 32)),
        "process_start_id": "1:100",
        "container_id": "containerd://predecessor",
        "host_pid": 500,
    }
    fields = {
        "key": smoke.WORKFLOW,
        "invocation": "old-call-1",
        "objects": {
            "cell": CELL,
            "owner": "old-owner",
            "epoch": "1",
            "revision": "2",
            "partition": str(smoke.PARTITION),
            "node": "pure.echo",
            "iteration": "0",
            "backend_pid": "410",
            "backend_start_us": "12345",
        },
    }
    reach = {
        "fields": fields,
        "target": {k: target[k] for k in ("actor", "uid", "session", "process_start_id")},
    }
    call = {
        "operation": "CreateAttempt",
        "outcome": "ErrLeaseNotOwned",
        "workflow": fields["key"],
        "invocation": fields["invocation"],
        "objects": fields["objects"].copy(),
        "pid": 1,
        "process_start_id": target["process_start_id"],
        "session": target["session_uuid"],
        "pod_uid": target["uid"],
    }
    backend = {
        "pid": 410,
        "start_us": 12345,
        "application": "dur053-e2-" + target["session_uuid"],
        "state": "idle",
        "in_transaction": False,
    }
    return call, reach, target, {"backends": [backend]}


def refuses(code, check, *args, **kwargs):
    try:
        check(*args, **kwargs)
    except SmokeFailure as error:
        assert str(error).startswith(code), str(error)
    else:
        pytest.fail(f"{code}_ACCEPTED")


def test_requires_released_deployed_call_over_identical_harness_write(challenge):
    call, reach, target, cut = challenge
    confirm_rejection(call, reach, target, target.copy(), cut)
    # Preserve inputs and the correct typed outcome, but swap in the source
    # process of a harness call. Even reported DB fields can be copied.
    harness = {
        **call,
        "pid": 620,
        "process_start_id": "620:25",
        "session": "harness",
        "pod_uid": "observer",
    }
    refuses("E2_DEPLOYED_CALL_ORIGIN", confirm_rejection, harness, reach, target, target, cut)


@pytest.mark.parametrize("outcome", ["OTHER_ERROR", "scheduler lease is not owned or has expired"])
def test_only_typed_lease_rejection_counts(challenge, outcome):
    call, reach, target, cut = challenge
    confirm_rejection(call, reach, target, target, cut)
    refuses(
        "E2_TYPED_REJECTION",
        confirm_rejection,
        {**call, "outcome": outcome},
        reach,
        target,
        target,
        cut,
    )


def test_commit_is_observed_i3_violation(challenge):
    call, reach, target, cut = challenge
    refuses(
        "E2_I3_STALE_CALL_COMMITTED",
        confirm_rejection,
        {**call, "outcome": "COMMITTED"},
        reach,
        target,
        target,
        cut,
    )


@pytest.mark.parametrize(
    ("key", "bad_value"), [("backend_pid", "411"), ("backend_start_us", "12346")]
)
def test_original_backend_lifetime_joins_resumed_pid(challenge, key, bad_value):
    call, reach, target, cut = challenge
    confirm_rejection(call, reach, target, target, cut)
    call["objects"][key] = bad_value
    refuses("E2_RESUMED_DB_BACKEND", confirm_rejection, call, reach, target, target, cut)


def test_stale_commit_rejected_for_each_watched_table():
    owner = {"session_id": "old", "epoch": 1}
    acquisition = {"session_id": "peer", "commit_order": 2, "epoch": 2}
    for table in WATCHED:
        old = {"commit_order": 1, "session_id": "old", "epoch": 1, "relation": table}
        cut = {"writes": [old, acquisition], "acquisitions": []}
        reject_stale_writes(cut, acquisition, owner, successor_held=True)
        cut["writes"].append({**old, "commit_order": 3})
        refuses(
            "E2_I3_STALE_MUTATION",
            reject_stale_writes,
            cut,
            acquisition,
            owner,
            successor_held=True,
        )


def test_later_predecessor_commit_needs_new_owner_epoch():
    owner = {"session_id": "old", "epoch": 1}
    acquisition = {"session_id": "peer", "commit_order": 2, "epoch": 2}
    reacquired = {"session_id": "old", "owner_id": "old-owner", "epoch": 3, "transaction_id": "43"}
    new_owner_write = {
        **reacquired,
        "commit_order": 3,
        "relation": "partition_leases",
        "xid": "43",
    }
    later = {**reacquired, "commit_order": 4, "relation": "transition_history"}
    cut = {"writes": [acquisition, new_owner_write, later], "acquisitions": [reacquired]}
    reject_stale_writes(cut, acquisition, owner, successor_held=False)
    later["epoch"] = 1
    refuses(
        "E2_I3_STALE_MUTATION", reject_stale_writes, cut, acquisition, owner, successor_held=False
    )


def test_expiry_and_both_commits_join_to_independent_database_cut(challenge):
    _, reach, target, cut = challenge
    old = {
        "owner_id": "old-owner",
        "epoch": 1,
        "partition_id": smoke.PARTITION,
        "live": True,
        "lease_expires_at": "2026-01-01T00:00:15+00:00",
    }
    owner = {
        "owner_id": "old-owner",
        "epoch": 1,
        "event_order": 1,
        "session_id": target["session_uuid"],
        "pod_uid": target["uid"],
        "container_id": target["uid"],
    }
    before = {
        **cut,
        "lease": old,
        "db_at": "2026-01-01T00:00:01+00:00",
        "workflow": {"id": smoke.WORKFLOW, "revision": 2},
        "acquisitions": [owner],
        "writes": [
            {
                "relation": "partition_leases",
                "owner_id": "old-owner",
                "epoch": 1,
                "commit_order": 1,
                "row_identity": {"expires_at": old["lease_expires_at"]},
            }
        ],
    }
    arm = {"target": reach["target"], "selector": {"key": smoke.WORKFLOW}}
    assert lease_at_hold(arm, reach, target, before, smoke.PARTITION) == owner
    successor = {
        "event_order": 3,
        "session_id": "peer",
        "transaction_id": "42",
        "owner_id": "peer-owner",
        "epoch": 2,
    }
    write = {
        "relation": "partition_leases",
        "xid": "42",
        "pid": 520,
        "session_id": "peer",
        "owner_id": "peer-owner",
        "epoch": 2,
        "commit_order": 2,
    }
    lease = {**old, "owner_id": "peer-owner", "epoch": 2, "updated_at": "2026-01-01T00:00:16+00:00"}
    after = {"lease": lease, "db_at": "2026-01-01T00:00:17+00:00", "writes": [write]}
    peer_reach = {"fields": {"objects": {"backend_pid": "520", "transaction": "42"}}}
    assert takeover(owner, before, successor, after, peer_reach) == write
    lease["updated_at"] = "2026-01-01T00:00:14+00:00"
    refuses("E2_DB_EXPIRY_ORDER", takeover, owner, before, successor, after, peer_reach)


@pytest.fixture
def ordered_journal(tmp_path):
    path = tmp_path / "hook.jsonl"
    tokens = {"predecessor": "a" * 64, "successor": "b" * 64}
    receiver = Receiver(path, tokens, companion_boundary=COMPANION)
    with receiver.file:

        def hold(actor, boundary, companion, nonce):
            target = {
                "actor": actor,
                "uid": actor + "-pod",
                "process_start_id": "1:10",
                "session": nonce * 32,
            }
            auth = f"Bearer {tokens[actor]}"
            receiver.event(encode({"event": "register", "identity": target}), auth)
            receiver.arm(
                CELL,
                boundary,
                target,
                {"key": smoke.WORKFLOW},
                10 if companion else 60,
                companion=companion,
                share_primary_deadline=companion,
            )
            reach = {
                "boundary": boundary,
                "identity": target,
                "hook_id": nonce * 32,
                "nonce": nonce * 31 + "0",
                "fields": {"key": smoke.WORKFLOW, "invocation": nonce * 32},
            }
            ack = receiver.event(encode({**reach, "event": "boundary_reached"}), auth)
            proof = {k: ack[k] for k in ("arm", "sequence", "sha256", "journal_sha256")}
            receiver.event(encode({**reach, **proof, "event": "held"}), auth)
            return receiver.observe(CELL, companion=companion), {**reach, **proof}, auth

        primary, primary_proof, primary_auth = hold("predecessor", PRIMARY, False, "c")
        receiver.note(CELL, primary["ack"], "prolonged")
        successor, successor_proof, successor_auth = hold("successor", COMPANION, True, "d")
        receiver.note(CELL, successor["ack"], "takeover_verified", companion=True)

        def end(arm, proof, auth):
            receiver.authorize(CELL, arm["ack"], "release", companion=arm["companion"], evidence={})
            receiver.event(encode({**proof, "event": "held"}), auth)
            receiver.event(encode({**proof, "event": "released"}), auth)

        end(primary, primary_proof, primary_auth)
        receiver.note(CELL, successor["ack"], "rejection_observed", companion=True)
        end(successor, successor_proof, successor_auth)
        yield path.read_bytes(), primary, successor


def test_successor_release_must_follow_predecessor_rejection(ordered_journal):
    raw, primary, successor = ordered_journal
    assert timeline(raw, primary, successor)
    rows = [json.loads(line) for line in raw.splitlines()]
    observed = next(i for i, row in enumerate(rows) if row["event"] == "rejection_observed")
    issued_action = rows[observed + 1]
    rows[observed + 1] = rows[observed]
    rows[observed] = issued_action
    # Keep the original observer times and sequence values at each position.
    rows[observed]["at_ns"], rows[observed + 1]["at_ns"] = (
        rows[observed + 1]["at_ns"],
        rows[observed]["at_ns"],
    )
    rows[observed]["sequence"], rows[observed + 1]["sequence"] = observed + 1, observed + 2
    changed = b"".join(encode(row) + b"\n" for row in rows)
    refuses("E2_RELEASE_ORDER", timeline, changed, primary, successor)


def test_waits_for_successor_ack():
    receiver = SimpleNamespace(
        observe=lambda *_args, **_kw: {"state": "reached", "boundary": COMPANION}
    )
    driver = SimpleNamespace(receiver=receiver)
    assert smoke.LeaseTakeover.held(driver, companion=True) is None, (
        "E2_MISSING_SUCCESSOR_ACK_ACCEPTED"
    )
    receiver.observe = lambda *_args, **_kw: {"state": "held"}
    assert smoke.LeaseTakeover.held(driver, companion=True) == {"state": "held"}


def test_companion_cannot_share_deadline_before_primary_is_held(tmp_path):
    tokens = {"predecessor": "a" * 64, "successor": "b" * 64}
    receiver = Receiver(tmp_path / "hook.jsonl", tokens, companion_boundary=COMPANION)
    targets = {
        role: {"actor": role, "uid": role, "session": token[:32], "process_start_id": "1:10"}
        for role, token in tokens.items()
    }
    with receiver.file:
        for role, token in tokens.items():
            receiver.event(
                encode({"event": "register", "identity": targets[role]}), f"Bearer {token}"
            )
        receiver.arm(CELL, PRIMARY, targets["predecessor"], {"key": smoke.WORKFLOW}, 60)
        with pytest.raises(ValueError, match="HOOK_SHARED_DEADLINE_PRIMARY_NOT_HELD"):
            receiver.arm(
                CELL,
                COMPANION,
                targets["successor"],
                {"key": smoke.WORKFLOW},
                60,
                companion=True,
                share_primary_deadline=True,
            )
