"""Offline controller and checker fixtures. Hosted evidence is required for qualification."""

import base64
import json
from copy import deepcopy

import pytest

from deploy.j4 import campaign, cells
from deploy.j4.checker import VerdictError, check, check_observation, negative_controls
from deploy.j4.evidence import ARMS, SECOND, admit, schedule, seal, verify_index, write_json

PROVIDER = "j4.recorded-llm"
MATCH = "92b3bc136a6617117cfac28e4324b272b6bf6b72ab5d137a948cb9fc0d25e751"
WORKFLOW = "test-workflow"
MS = 1_000_000
TOPIC = "durable-agent.tasks.v1"
GROUP = "runtime-workers-v1"


def encode(value):
    return base64.b64encode(value.encode()).decode()


def hook(data, name, stamp, step=PROVIDER, **fields):
    identity = {
        "workflow": WORKFLOW,
        "node": step,
        "attempt": "1",
        "iteration": "0",
        "worker": f"test-worker-{step}",
        "request": f"test-request-{step}",
        "token_hash": f"test-token-hash-{step}",
    }
    data["worker"].append(
        {
            "hook": name,
            "process_session": "test-session",
            "boot_id": "test-boot",
            "monotonic_ns": stamp,
            "fields": {**identity, **{k: str(v) for k, v in fields.items()}},
        }
    )


def add_attempt(data, node, event, starting, payload):
    db, stamp = data["db"], f"committed-{node}"
    identity = {"workflow_id": WORKFLOW, "node_id": node, "iteration": 0, "attempt_number": 1}
    attempt = {
        **identity,
        "state": "SUCCEEDED",
        "is_current": False,
        "result": payload,
        "worker_id": f"test-worker-{node}",
        "worker_request_id": f"test-request-{node}",
        "claim_token_hash": f"test-token-hash-{node}",
        "committed_at": stamp,
        "transaction_id": stamp,
    }
    db["attempts"].append(attempt)
    offset = len(db["inbox"])
    task_payload = {"workflow_id": WORKFLOW, "node_id": node, "iteration": 0, "attempt_number": 1}
    key = encode(event)
    data["broker"].append(
        {
            "kind": "notice",
            "event_id": event,
            "key_b64": key,
            "value_b64": encode(json.dumps(task_payload)),
            "offset": offset,
            "partition": 0,
            "event_type": "attempt.dispatch",
            "schema_version": 1,
            "headers": [
                ["event-id", key],
                ["workflow-id", encode(WORKFLOW)],
                ["event-type", encode("attempt.dispatch")],
                ["schema-version", encode("1")],
            ],
            "at_ns": starting - MS,
            "boot_id": "test-boot",
        }
    )
    hook(
        data,
        "j4_offset_committed",
        starting,
        node,
        topic=TOPIC,
        partition=0,
        offset=offset,
        next_offset=offset + 1,
        disposition="ACCEPTED",
        event=event,
        consumer=GROUP,
    )
    hook(data, "attempt_claimed", starting + MS, node)
    db["inbox"].append(
        {
            "consumer_id": GROUP,
            "event_id": event,
            "kafka_offset": offset,
            "kafka_partition": 0,
            "disposition": "ACCEPTED",
        }
    )
    db["outbox"].append(
        {
            "workflow_id": WORKFLOW,
            "event_id": event,
            "topic": TOPIC,
            "schema_version": 1,
            "event_type": "attempt.dispatch",
            "payload": task_payload,
            "publish_state": "PUBLISHED",
            "aggregate_revision": len(db["history"]) + 1,
        }
    )
    first = len(db["history"]) + 1
    for rev, reason, kind, body in (
        (first, "ATTEMPT_RESULT_RECORDED", "activity.result", payload),
        (
            first + 1,
            "RESULT_CONSUMED",
            "workflow.result_consumed",
            {**identity, "attempt_state": "SUCCEEDED"},
        ),
    ):
        db["history"].append(
            {
                **identity,
                "revision": rev,
                "reason": reason,
                "new_state": "SUCCEEDED" if node == "pure.echo" else "RUNNABLE",
                "actor_id": attempt["worker_id"],
                "committed_at": stamp,
                "transaction_id": stamp,
            }
        )
        db["outbox"].append(
            {
                "workflow_id": WORKFLOW,
                "event_id": f"event-{rev}",
                "event_type": kind,
                "aggregate_revision": rev,
                "payload": body,
                "topic": "durable-agent.events.v1",
            }
        )


def sample(arm="B0"):
    call_base = 100 * SECOND
    cell = f"j4-{arm.lower()}-1"
    fixture = {"model": "recorded-llm-v1", "prompt": "Return fixture alpha."}
    workflow = {
        "workflow_id": WORKFLOW,
        "namespace": cell,
        "submission_key": WORKFLOW,
        "definition_id": "j4.recorded-llm-recovery",
        "definition_version": 1,
    }
    clocks = {}
    for role in ("runtime", "worker", "stub"):
        clocks[role] = {
            "before_ns": call_base - SECOND,
            "remote_ns": call_base - SECOND + MS,
            "after_ns": call_base - SECOND + 2 * MS,
        }
    data = {
        "schema": "j4.cell.v1",
        "arm": arm,
        "cell": cell,
        "workflow_id": WORKFLOW,
        "namespace": cell,
        "clock_start": {"boot_id": "test-boot", "samples": clocks},
        "submit": {
            "request_ns": call_base - 20 * MS,
            "accepted_ns": call_base - 10 * MS,
            "request": {"submission_key": WORKFLOW, "payload": fixture, "initial_input": fixture},
            "status": 201,
            "response": {"created": True, "workflow": {**workflow, "revision": 1}},
        },
        "stub": [],
        "scheduler": [],
        "broker": [],
        "worker": [],
        "cut": None,
        "redelivery": [],
        "db": {
            k: []
            for k in (
                "workflows",
                "nodes",
                "attempts",
                "outbox",
                "inbox",
                "offsets",
                "history",
                "reservations",
            )
        },
    }
    db = data["db"]
    db["unexpected_results"] = 0
    db["history"].append(
        {"workflow_id": WORKFLOW, "revision": 1, "reason": "SUBMITTED", "new_state": "RUNNABLE"}
    )
    success = {"completion": "fixture alpha", "request_sha256": MATCH, "provider_call_id": "call-4"}
    add_attempt(data, PROVIDER, "provider-dispatch", call_base, success)
    issued = call_base + 35 * MS
    for ordinal, (status, retry_after) in enumerate(((429, 1), (0, 0), (429, 12), (200, 0)), 1):
        call_id = f"call-{ordinal}"
        origin = issued + (2000 if not status else 45) * MS
        before = issued + 20 * MS
        received = issued + 30 * MS
        details = {
            "call_id": call_id,
            "ordinal": ordinal,
            "workflow_id": WORKFLOW,
            "node_id": PROVIDER,
            "cell": cell,
        }
        hook(data, "j4_provider", issued + MS, event="reserved", at_ns=issued + MS, **details)
        hook(
            data,
            "j4_provider",
            before + MS,
            event="send",
            at_ns=before + MS,
            before_ns=before,
            **details,
        )
        reply = origin + (10 * MS if not status else 0)
        hook(
            data,
            "j4_provider",
            reply,
            event="reply",
            at_ns=reply,
            status=status,
            retry_after_seconds=retry_after,
            **details,
        )
        delay = (2302, 4484, 8486, 16039)[ordinal - 1] if arm == "B0" else 3400
        eligible = origin + max(delay * MS, retry_after * SECOND) if status != 200 else None
        hook(
            data,
            "j4_provider",
            reply + MS,
            event="outcome_committed",
            at_ns=reply + MS,
            outcome_ns=origin,
            eligible_ns=eligible or 0,
            **details,
        )
        db["reservations"].append(
            {
                "workflow_id": WORKFLOW,
                "node_id": PROVIDER,
                "iteration": 0,
                "attempt_number": 1,
                "call_id": call_id,
                "boot_id": "test-boot",
                "sequence": ordinal,
                "issued_ns": issued,
                "deadline_ns": issued + 2 * SECOND,
                "observed_ns": origin,
                "outcome": {0: "timeout", 429: "rate_limited", 200: "success"}[status],
                "retry_after_seconds": retry_after,
                "eligible_ns": eligible,
                "terminal_state": "SUCCEEDED" if status == 200 else None,
                "terminal_payload": success if status == 200 else None,
                "committed_at": "test-commit",
            }
        )
        n = len(data["stub"]) + 1
        common = {"boot_id": "test-boot"}
        data["stub"].extend(
            [
                {
                    **common,
                    **details,
                    "event": "receipt",
                    "sequence": n,
                    "at_ns": received,
                    "attempt": 1,
                    "iteration": 0,
                    "request_sha256": MATCH,
                    "matched": True,
                    "branch": retry_after if status == 429 else "success" if status else "withhold",
                    "header_names": [
                        "host",
                        "content-type",
                        "content-length",
                        "connection",
                        "x-j4-cell",
                        "x-j4-workflow",
                        "x-j4-node",
                        "x-j4-iteration",
                        "x-j4-attempt",
                        "x-j4-ordinal",
                        "x-j4-call",
                    ],
                },
                {
                    **common,
                    "event": "response" if status else "withhold",
                    "sequence": n + 1,
                    "at_ns": received + MS,
                    "receipt_sequence": n,
                    "status": status,
                    "retry_after": retry_after,
                    "payload": success if status == 200 else {},
                },
                {
                    **common,
                    "event": "close",
                    "at_ns": reply + MS,
                    "sequence": n + 2,
                    "receipt_sequence": n,
                    "disposition": "response_complete" if status else "peer_closed",
                },
            ]
        )
        issued = (eligible or origin) + 10 * MS
    hook(data, "j4_result_receipt", issued + MS)
    add_attempt(data, "pure.echo", "echo-dispatch", issued + SECOND, success)
    db["nodes"].extend(
        [
            {
                "workflow_id": WORKFLOW,
                "node_id": PROVIDER,
                "input": fixture,
                "iteration": 0,
                "accepted_result": success,
            },
            {
                "workflow_id": WORKFLOW,
                "node_id": "pure.echo",
                "input": success,
                "iteration": 0,
                "accepted_result": success,
            },
        ]
    )
    db["workflows"].append({**workflow, "revision": len(db["history"]), "state": "SUCCEEDED"})
    db["offsets"].append(
        {"consumer_id": GROUP, "next_offset": 2, "kafka_partition": 0, "topic": TOPIC}
    )
    final = issued + 2 * SECOND
    data["broker"].append({"kind": "offset", "offset": 2, "boot_id": "test-boot", "at_ns": final})
    data["terminal"] = {"lower_ns": final - MS, "observed_ns": final}
    data["observation_end_ns"] = final + MS
    data["clock_end"] = {
        "boot_id": "test-boot",
        "samples": {
            role: {
                "before_ns": final + 2 * MS,
                "remote_ns": final + 3 * MS,
                "after_ns": final + 4 * MS,
            }
            for role in clocks
        },
    }
    return data


def test_frozen_cell_order_and_admission():
    qualification, measurement = schedule("qualification"), schedule("measurement")
    assert [cell["arm"] for cell in qualification] == list(ARMS)
    assert [cell["arm"] for cell in measurement] == list(ARMS) * 3
    assert len({cell["cell"] for cell in measurement}) == 33
    assert admit(qualification, {}, 0, bootstrap=True) == 125 * 60
    assert admit(measurement, {}, 0, bootstrap=True) == 279 * 60
    assert admit(measurement[1:], {"reset": 180}, 0, bootstrap=False) == (32 * 5 + 31 * 3 + 25) * 60
    with pytest.raises(ValueError, match="forecast"):
        admit(measurement, {"B4": 150 * 60}, 0, bootstrap=True)


@pytest.mark.parametrize("arm,control", [("B0", None), ("B5", "FIXED_DELAY_REJECTED")])
def test_independent_replay_timing(arm, control):
    row = sample(arm)
    verdict = check(row)
    assert verdict["status"] == "pass", verdict
    assert verdict["control"] == control
    assert [r["verdict"] for r in verdict["timing"]] == (
        ["inside"] * 3 if arm == "B0" else ["late", "early", "inside"]
    )
    row["worker"][-1]["process_session"] = "foreign"
    assert check(row)["reason"] == "OFFSET_BEFORE_CLAIM"


def test_negative_controls_copy_successful_evidence(tmp_path):
    trace = sample()
    pristine = deepcopy(trace)
    assert negative_controls(trace, tmp_path) == {
        "missing-call": {"status": "invalid", "reason": "MISSING_CALL"},
        "missing-history": {"status": "invalid", "reason": "MISSING_HISTORY"},
        "missing-reservation": {"status": "invalid", "reason": "MISSING_RESERVATION"},
    }
    assert trace == pristine
    missing = json.loads((tmp_path / "missing-reservation.json").read_bytes())
    assert len(missing["db"]["reservations"]) == 3
    assert check(pristine)["status"] == "pass"


def test_evidence_index_detects_changed_missing_and_unindexed_files(tmp_path):
    file = tmp_path / "provider-resilience/j4-b0-1/cell.json"
    write_json(file, sample())
    seal(tmp_path)
    verify_index(tmp_path)
    for operation in (lambda: write_json(file, {}), file.unlink, lambda: file.touch()):
        operation()
        with pytest.raises(ValueError, match="index"):
            verify_index(tmp_path)


def test_kill_boundary_requires_durable_ack_and_script_specific_proof():
    row = sample()
    traces = {name: row[name] for name in ("worker", "broker", "stub", "scheduler")}
    db = row["db"]
    db["attempts"] = [db["attempts"][0] | {"state": "CLAIMED", "is_current": True}]
    db["history"] = db["history"][:1]
    db["outbox"] = [event for event in db["outbox"] if event["event_type"] == "attempt.dispatch"]
    db["reservations"] = []
    traces["broker"].append({"kind": "offset", "offset": 1})
    receipt = traces["stub"][0]
    traces["stub"] = [receipt, {"event": "withhold", "receipt_sequence": receipt["sequence"]}]
    assert cells.boundary_ready("C1", traces, db, "test-session", WORKFLOW)
    assert not cells.boundary_ready("B2", traces, db, "test-session", WORKFLOW)
    db["reservations"] = [{"call_id": receipt["call_id"], "outcome": None}]
    assert cells.boundary_ready("B2", traces, db, "test-session", WORKFLOW)
    assert not cells.boundary_ready("B2", traces, db, "other-session", WORKFLOW)
    traces["stub"].append({"event": "close", "receipt_sequence": receipt["sequence"]})
    assert not cells.boundary_ready("B2", traces, db, "test-session", WORKFLOW)
    assert not cells.boundary_ready("C4", traces, db, "test-session", WORKFLOW)
    assert cells.boundary_ready(
        "C4",
        {"worker": [], "stub": []},
        {"workflows": [], "attempts": [], "outbox": []},
        "test-session",
        WORKFLOW,
    )


def test_c2_b1_and_b3_gates_cannot_substitute_for_one_another():
    row = sample()
    traces = {name: row[name] for name in ("worker", "broker", "stub", "scheduler")}
    db = row["db"]
    db["attempts"] = db["attempts"][:1]
    db["nodes"] = db["nodes"][:1]
    db["history"] = db["history"][:2]
    db["outbox"] = db["outbox"][:2]
    traces["broker"].append({"kind": "offset", "offset": 1})
    gate = {
        "hook": "cut_held",
        "process_session": "test-session",
        "fields": {"arm": "C2", "workflow": WORKFLOW},
    }
    traces["worker"].append(gate)
    traces["scheduler"].append({"event": "held"})
    assert cells.boundary_ready("C2", traces, db, "test-session", WORKFLOW)
    traces["scheduler"].clear()
    assert not cells.boundary_ready("C2", traces, db, "test-session", WORKFLOW)
    db["outbox"].pop()
    db["history"].pop()
    db["attempts"][0].update(state="CLAIMED", is_current=True)
    db["reservations"] = db["reservations"][:1]
    traces["stub"].clear()
    db["reservations"][0]["outcome"] = None
    assert not cells.boundary_ready("B1", traces, db, "test-session", WORKFLOW)
    traces["worker"].append(gate | {"fields": {"arm": "B1", "workflow": WORKFLOW}})
    assert cells.boundary_ready("B1", traces, db, "test-session", WORKFLOW)
    db["reservations"][0]["outcome"] = "rate_limited"
    assert not cells.boundary_ready("B1", traces, db, "test-session", WORKFLOW)
    assert cells.boundary_ready("B3", traces, db, "test-session", WORKFLOW)
    db["reservations"][0]["observed_ns"] += 1
    assert not cells.boundary_ready("B3", traces, db, "test-session", WORKFLOW)


def test_qualification_refuses_before_construction_without_j9a(monkeypatch, tmp_path):
    monkeypatch.setenv("GITHUB_SHA", "a" * 40)
    monkeypatch.setenv("GITHUB_RUN_ID", "101")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setenv("RUNNER_ENVIRONMENT", "github-hosted")
    monkeypatch.setattr(campaign, "github", lambda _: {"private": False})
    monkeypatch.setattr(
        campaign.subprocess,
        "check_output",
        lambda args, **_: "a" * 40 if args[1] == "rev-parse" else b"",
    )
    monkeypatch.setattr(campaign, "read_json", lambda _: {"schema_version": 1, "switches": {}})
    monkeypatch.setattr(campaign, "Host", lambda *args: pytest.fail("qualification armed"))
    from argparse import Namespace

    output = tmp_path / "evidence"
    assert (
        campaign.campaign(
            Namespace(
                output=output,
                scratch=tmp_path / "private",
                job_started=0,
                mode="qualification",
                relay_switch="J9A_TEST_MODE",
                relay_value="1",
            )
        )
        == 1
    )
    verify_index(output)
    manifest = json.loads((output / "campaign.json").read_bytes())
    assert manifest["counts"]["unperformed"] == 11
    assert manifest["cleanup"] == "not-started"
    assert "requires a J9a switch entry" in manifest["error"]


def test_exhaustion_observes_both_real_dispositions_past_latest_eligible():
    deadline = 120 * SECOND
    latest = deadline + 34_789 * MS
    terminal = deadline + SECOND
    dispatch = {
        "kind": "notice",
        "offset": 0,
        "value_b64": encode(json.dumps({"node_id": PROVIDER, "iteration": 0, "attempt_number": 1})),
    }
    trace = {
        "arm": "B4",
        "submit": {"accepted_ns": 95 * SECOND},
        "broker": [dispatch],
        "worker": [],
        "terminal": {"lower_ns": terminal - MS, "observed_ns": terminal},
        "redelivery": [
            {
                "original_offset": 0,
                "offset": 1,
                "request_ns": terminal + MS,
                "ack_ns": terminal + SECOND,
            },
            {
                "original_offset": 0,
                "offset": 2,
                "request_ns": latest + SECOND,
                "ack_ns": latest + 2 * SECOND,
            },
        ],
        "observation_end_ns": latest + 10 * SECOND,
    }
    for delivery in trace["redelivery"]:
        hook(trace, "j4_offset_committed", delivery["ack_ns"], offset=delivery["offset"])
    reservations = {4: {"deadline_ns": deadline}}
    check_observation(trace, reservations)
    trace["observation_end_ns"] -= 1
    with pytest.raises(VerdictError, match="EXHAUSTION_OBSERVATION"):
        check_observation(trace, reservations)
