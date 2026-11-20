"""Run the hosted F3 closeout on the Phase F artifact shapes."""

from __future__ import annotations

import json
import sys
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from dur053_k6_b_evidence import SmokeFailure  # noqa: E402
from dur053_k6_e1_join import one  # noqa: E402
from dur053_k6_f1a_inputs import worker_principal  # noqa: E402
from dur053_k6_f2_inputs import quiet_digest  # noqa: E402
from dur053_k6_f3_inputs import CELLS, SECOND, join  # noqa: E402
from dur053_k6_registry import read_registry  # noqa: E402

HOSTED = ROOT / "tests/fixtures/k6_f3_hosted.json"
ORIGIN = datetime(2026, 11, 20, 10, 18, 20, tzinfo=UTC)
BOOT = "176f9bfa-ca7b-49ea-b3a1-11e6fe9659ea"


def timestamp(seconds):
    return (ORIGIN + timedelta(seconds=seconds)).isoformat()


def mark(seconds):
    now = round(seconds * SECOND)
    return {"monotonic_ns": now, "wall_ns": int(ORIGIN.timestamp()) * SECOND + now}


def cr(target, seconds, observed_cr):
    result = deepcopy(observed_cr)
    result["status"]["experiment"]["containerRecords"][0]["id"] = (
        f"{target['namespace']}/{target['pod']}/{target['container']}"
    )
    result["metadata"].update(namespace=target["namespace"], uid="cr-" + target["uid"])
    result["spec"]["selector"] = {
        "namespaces": [target["namespace"]],
        "pods": {target["namespace"]: [target["pod"]]},
    }
    result["spec"].update(timeOffset=f"{seconds}s", containerNames=[target["container"]])
    return result


def clock_evidence(hosted, target, seconds, shift):
    identity = {
        "pid": 1,
        "pod_uid": target["uid"],
        "process_start_id": target["process"],
        "boot_id": BOOT,
        "role": target["role"],
    }
    if target["role"] == "scheduler":
        identity["session"] = target["session"]
    samples = []
    for i in range(0, 35):
        s = i * 0.5
        send, receive = mark(s), mark(s + 0.004)
        offset = shift if 3 <= s < 12 else 0
        samples.append(
            {
                "send": send,
                "receive": receive,
                "answer": {
                    **identity,
                    "wall_ns": mark(s + 0.002)["wall_ns"] + round(offset * SECOND),
                },
            }
        )
    injected = cr(target, seconds, hosted["injected_cr"])
    return {
        "target": target,
        "bindings": [deepcopy(target) for _ in range(3)],
        "node_boot_id": BOOT,
        "identity": identity,
        "injected": injected,
        "last_status": deepcopy(injected),
        "deleted": cr(target, seconds, hosted["deleted_cr"]),
        "remaining": [],
        "samples": samples,
        "marks": {
            "apply": mark(2),
            "ack": mark(3.1),
            "remove": mark(11.9),
            "absent": mark(13),
        },
    }


def fixture(cell, *, flat=False):
    data = json.loads(HOSTED.read_bytes())
    role, seconds = CELLS[cell]
    wf = data["offer"]["workflow"]
    cut = data["terminal"]["workflows"][wf]
    attempt = cut["attempts"][0]
    worker = one(
        "FIXTURE_TARGET",
        [p for p in data["workers"] if worker_principal(p) == attempt["worker_id"]],
    )
    worker_result = one(
        "FIXTURE_RESULT",
        [r for r in data["witness"] if r["event"] == "f1a_http" and r["operation"] == "result"],
    )
    target = {
        "namespace": cut["workflow"]["namespace"],
        "pod": worker["metadata"]["name"] if role == "worker" else "scheduler-76cdd944dc-vzxq5",
        "uid": worker["metadata"]["uid"]
        if role == "worker"
        else "aa98d8a1-9573-40e6-a291-f42b72803cad",
        "role": role,
        "container": role,
        "container_id": "containerd://" + "1" * 64,
        "process": worker_result["process"] if role == "worker" else "1:49382",
        "session": worker_result["session"]
        if role == "worker"
        else "c8e12aa6-5977-4bf7-bc27-42dcb06276af",
        "host_pid": 5331,
        "restart_count": 0,
        "node": "dur053-kind-control-plane",
    }
    fault = clock_evidence(data, target, seconds, 0 if flat else seconds)
    owner = {
        "partition_id": cut["workflow"]["partition_id"],
        "owner_id": "scheduler-owner",
        "epoch": 31,
    }
    fault["held_owner"] = {**owner, "pod_uid": target["uid"], "session_id": target["session"]}
    cut_keys = ("workflow_id", "node_id", "iteration", "attempt_number", "token_hash", "worker_id")
    current_claim = {key: attempt[key] for key in cut_keys}
    current_claim.update(is_current=True, state="CLAIMED", deadline=timestamp(23))
    fault["db"] = {
        name: {
            "send": mark(s),
            "receive": mark(s + 0.008),
            "value": {
                "db_at": timestamp(s + 0.004),
                "read_only": "on",
                "claim": [current_claim.copy()],
                "lease": [{**owner, "lease_expires_at": timestamp(s + 14)}],
            },
        }
        for name, s in (("baseline", 1.8), ("during_start", 4), ("during_end", 11), ("after", 14))
    }
    attempt_fields = {key: attempt[key] for key in cut_keys}
    row = dict.fromkeys(
        (
            "partition_id",
            "owner_id",
            "epoch",
            "workflow_id",
            "node_id",
            "iteration",
            "attempt_number",
            "worker_id",
            "token_hash",
        )
    )
    fault["writes"] = [
        {
            **row,
            **attempt_fields,
            "id": 1,
            "branch": "dispatch",
            "application": "durable-scheduler",
            "session": target["session"],
            "written_at": timestamp(0),
            "deadline": timestamp(30),
            "old_deadline": None,
            "row_id": f"{wf}/pure.echo/0/1",
        },
        {
            **row,
            **attempt_fields,
            "id": 2,
            "branch": "claim",
            "application": "durable-worker-control",
            "session": "control-session",
            "written_at": timestamp(0.25),
            "deadline": timestamp(16.25),
            "old_deadline": timestamp(30),
            "row_id": f"{wf}/pure.echo/0/1",
        },
        {
            **row,
            **attempt_fields,
            "id": 3,
            "branch": "heartbeat",
            "application": "durable-worker-control",
            "session": "control-session",
            "written_at": timestamp(5.25),
            "deadline": timestamp(21.25),
            "old_deadline": timestamp(16.25),
            "row_id": f"{wf}/pure.echo/0/1",
        },
        {
            **row,
            **owner,
            "id": 4,
            "branch": "renewal",
            "application": "durable-scheduler",
            "session": target["session"],
            "written_at": timestamp(7.25),
            "deadline": timestamp(22.25),
            "old_deadline": timestamp(20.25),
            "row_id": str(owner["partition_id"]),
        },
    ]
    control = None
    if flat:
        independent = {
            **target,
            "pod": "clock-control",
            "uid": "control-uid",
            "role": "clock-control",
            "container": "worker",
            "container_id": "containerd://" + "2" * 64,
            "host_pid": 5432,
            "process": "1:50825",
            "session": "",
        }
        control = {
            "purpose": "harness validation only",
            **clock_evidence(data, independent, seconds, seconds),
        }
    digest = quiet_digest(data["terminal"], data["witness"])
    data["quiet"] = [
        {
            "started": mark(s),
            "ended": mark(s + 0.1),
            "digest": digest,
            "db_at": data["terminal"]["db_at"],
        }
        for s in (20, 35, 51)
    ]
    data["marker"].update(before=mark(18), after=mark(18.1))
    data.update(
        cell=cell,
        fault=fault,
        control=control,
        attempt=attempt,
        static={
            "sha256": data["build_sha256"],
            "build_sha256": data["build_sha256"],
            "go_version_m": (
                "go1.27.4\nbuild\tCGO_ENABLED=0\nbuild\t-tags=dur053_k6\nbuild\tGOOS=linux"
            ),
            "file": "ELF 64-bit LSB executable, x86-64, statically linked",
            "program_headers": "  LOAD 0x001000",
            "containerd": "[plugins]",
            "cni": '{"name":"k8s-pod-network"}',
            "calico": {"containers": ["calico-node"]},
        },
        registered=one(
            "FIXTURE_REGISTRY",
            [r for r in read_registry()["coverage"] if r["arm"] == "chaos-8-9-10-11"],
        ),
    )
    return data


def vary(data, predicate, seconds):
    for sample in data["fault"]["samples"]:
        if predicate(sample["send"]["monotonic_ns"] / SECOND):
            sample["answer"]["wall_ns"] += round(seconds * SECOND)


def rejected(data, expected, marker):
    try:
        join(data)
    except SmokeFailure as error:
        assert expected in str(error), f"{marker}: {error}"
    else:
        pytest.fail(marker)


@pytest.mark.parametrize("cell", CELLS)
def test_whole_tail_for_each_cell(cell):
    try:
        output = join(fixture(cell))
    except SmokeFailure as error:
        pytest.fail(f"F3_MUT_COVERAGE: {error}")
    assert output["outcome"] == "CONFIRMED-PASS"
    assert output["passing_replicate"] is True
    assert output["invariants_covered"] == ["I1", "I2", "I4", "I6", "I11", "I12"], "F3_MUT_COVERAGE"
    assert output["outcomes"]["I12"]["witnesses"] in ([3], [4])


def test_whole_tail_not_exercised():
    try:
        output = join(fixture("medium-8", flat=True))
    except SmokeFailure as error:
        pytest.fail(f"F3_MUT_UNEXERCISED_I12: {error}")
    assert output["outcome"] == "NOT EXERCISED"
    assert output["passing_replicate"] is False, "F3_MUT_UNEXERCISED_REPLICATE"
    assert output["outcomes"]["I12"]["status"] == "NOT COVERED", "F3_MUT_UNEXERCISED_I12"
    assert output["control"]["confirmation"] == "CONFIRMED"


def test_whole_tail_harness_failure():
    data = fixture("medium-8", flat=True)
    for sample in data["control"]["samples"]:
        if 3 <= sample["send"]["monotonic_ns"] / SECOND < 12:
            sample["answer"]["wall_ns"] -= 30 * SECOND
    rejected(data, "F3_HARNESS_SENSITIVITY", "F3_MUT_CONTROL_REQUIRED")


def test_wrong_sign_fails():
    data = fixture("medium-9")
    vary(data, lambda s: 3 <= s < 12, 60)
    rejected(data, "F3_CLOCK_DURING_AMBIGUOUS", "F3_MUT_OFFSET_SIGN")


def test_small_shift_is_distinguishable_from_zero():
    data = fixture("medium-8", flat=True)
    vary(data, lambda s: 3 <= s < 12, 0.2)
    rejected(data, "F3_CLOCK_DURING_AMBIGUOUS", "F3_MUT_DISTINGUISHABLE_ZERO")


def test_offset_outside_tolerance_fails():
    data = fixture("medium-8")
    vary(data, lambda s: 3 <= s < 12, 1.3)
    rejected(data, "F3_CLOCK_DURING_AMBIGUOUS", "F3_MUT_OFFSET_TOLERANCE")


def test_after_removal_must_restore_baseline():
    data = fixture("medium-10")
    vary(data, lambda s: s >= 13, 30)
    rejected(data, "F3_CLOCK_BASELINE_NOT_RESTORED", "F3_MUT_RESTORATION")


@pytest.mark.parametrize("key, other", [("pid", 2), ("pod_uid", "changed-uid")])
def test_in_process_binding_rejects_drift(key, other):
    data = fixture("medium-11")
    data["fault"]["samples"][13]["answer"][key] = other
    rejected(data, "F3_CLOCK_PROCESS_DRIFT", "F3_MUT_PROCESS_BINDING")


@pytest.mark.parametrize("selector_field", ["pods", "namespaces"])
def test_cr_rejects_broadened_pod_selector(selector_field):
    data = fixture("medium-8")
    fault = data["fault"]
    selector = fault["injected"]["spec"]["selector"]
    if selector_field == "pods":
        selector["pods"][fault["target"]["namespace"]].append("peer")
    else:
        selector["namespaces"].append("peer")
    rejected(data, "F3_CHAOS_SPEC", "F3_MUT_EXACT_POD")


def test_deadline_uses_database_time_plus_configured_ttl():
    data = fixture("medium-10")
    data["fault"]["writes"][2]["deadline"] = timestamp(51.25)
    rejected(data, "F3_DB_DEADLINE_TTL", "F3_MUT_DEADLINE_TTL")


def test_owner_and_epoch_cannot_change_between_cuts():
    data = fixture("medium-8")
    data["fault"]["db"]["during_end"]["value"]["lease"][0]["epoch"] += 1
    rejected(data, "F3_NO_SPURIOUS_TAKEOVER", "F3_MUT_NO_TAKEOVER")
