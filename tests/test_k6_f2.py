"""F2's whole offline tail with hosted row, witness and broker samples."""

from __future__ import annotations

import json
import subprocess
import sys
import time
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock
from uuid import UUID

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from dur053_k6_b_evidence import SmokeFailure  # noqa: E402
from dur053_k6_e1_join import EVENTS, TASKS, by, one  # noqa: E402
from dur053_k6_f2_inputs import (  # noqa: E402
    BASE,
    CELLS,
    PG_ROLES,
    QUIET_NS,
    UNCOVERED,
    docker_time,
    join,
    marks,
    quiet_digest,
    runtime_start,
)
from dur053_k6_f2_smoke import F2Evidence, F2Smoke  # noqa: E402
from dur053_k6_hook import Receiver, encode  # noqa: E402
from dur053_k6_registry import read_registry  # noqa: E402

HOSTED = json.loads((ROOT / "tests/fixtures/k6_f2_hosted.json").read_bytes())
SECOND = 10**9
T0 = docker_time("2026-11-20T07:40:00Z")


def cut(workflows, at):
    return {"workflows": workflows, "read_only": "on", "started_ns": at - 1, "at_ns": at}


def topic_samples(at):
    return {
        topic: {
            "started_ns": at - 1,
            "observed_ns": at,
            "value": [{"partition": p, "first": 0, "next": 0, "records": []} for p in range(8)],
        }
        for topic in (EVENTS, TASKS)
    }


def terminal_inputs(cell):
    namespace = f"dur053-k6-f2-{cell}"
    at = T0 + 900 * SECOND
    workflows, witness, workers, offers = {}, [], [], []
    topics = topic_samples(at)
    marker = {"marker": "/f1a-control/f1a-release", "started_ns": at}
    for fixture in HOSTED["terminal"].values():
        old = fixture["cut"]["workflow"]["namespace"]
        value = json.loads(json.dumps(fixture).replace(old, namespace))
        evidence = value["cut"]
        workflow = evidence["workflow"]["workflow_id"]
        workflows[workflow] = evidence
        witness.extend(value["witness"])
        workers.append(value["worker"])
        marker["started_ns"] = min(marker["started_ns"], value["marker"]["started_ns"])
        offers.append(
            {
                "workflow": workflow,
                "fingerprint": evidence["workflow"]["submission_payload_hash"],
                "samples": [
                    {
                        "workflow_id": workflow,
                        "status": 201,
                        "created": True,
                        "started_ns": T0 + 4 * SECOND,
                        "at_ns": T0 + 4 * SECOND + 1,
                    }
                ],
            }
        )
        for topic, sample in value["broker"].items():
            for part in sample["value"]:
                target = topics[topic]["value"][part["partition"]]
                old_offset = target["next"]
                for r in part["records"]:
                    target["records"].append({**r, "offset": r["offset"] + old_offset})
                target["next"] += part["next"]
                for row in by(evidence["inbox"], topic=topic, kafka_partition=part["partition"]):
                    row["kafka_offset"] += old_offset
    end = cut(workflows, at)
    digest = quiet_digest(end, witness)
    registry = read_registry()
    episode = min(by(registry["episodes"], cell=cell), key=lambda item: item["ordinal"])
    return {
        "cell": cell,
        "namespace": namespace,
        "terminal": end,
        "witness": witness,
        "workers": workers,
        "offers": offers,
        "marker": marker,
        "topics": topics,
        "quiet": [
            {
                "started_ns": at - QUIET_NS - 10,
                "at_ns": at - QUIET_NS - 5,
                "cut_at_ns": at - QUIET_NS - 9,
                "digest": digest,
            },
            {"started_ns": at - 1, "at_ns": at + 1, "cut_at_ns": at, "digest": digest},
        ],
        "registered": one("FIXTURE_ARM", by(registry["coverage"], arm=episode["arm"])),
    }


def held_event(inputs, workflow):
    row = inputs["terminal"]["workflows"][workflow]
    outbox = deepcopy(one("FIXTURE_CREATED", by(row["outbox"], event_type="workflow.created")))
    return {
        "workflow": {**row["workflow"], "state": "RUNNABLE"},
        "attempts": [],
        "outbox": [{**outbox, "published_at": None, "publish_state": "PENDING"}],
        "publications": [],
    }


def probe(topic, success, start, *, duration=1):
    return {
        "start_ticks": 1,
        "started_ns": start,
        "at_ns": start + duration,
        "ok": success,
        "exit_code": 0 if success else 1,
        "nonce": "1" * 32,
        "value": {
            "topic": topic,
            "nonce": "1" * 32,
            "metadata": success,
            "produced": success,
            "read_back": success,
            "partition": 0,
            "offset": 1 if success else None,
        },
    }


def relay_fault(inputs, tmp_path, monkeypatch):
    import dur053_k6_hook

    seed, *arrivals = [r["workflow"] for r in inputs["offers"]]
    target = HOSTED["relay_target"]
    session = str(UUID(target["session"]))
    clock = [T0]
    monkeypatch.setattr(dur053_k6_hook.time, "time_ns", lambda: clock[0])
    path = tmp_path / "relay.jsonl"
    receiver = Receiver(path, {"relay": "1" * 64})
    with receiver.file:
        token = "Bearer " + "1" * 64
        receiver.event(encode({"event": "register", "identity": target}), token)
        receiver.arm(
            "medium-5",
            "before_publish",
            target,
            {
                "key": seed,
                "type": "workflow.created",
                "cell": "medium-5",
            },
            45,
        )
        clock[0] = T0 + SECOND
        held = held_event(inputs, seed)
        event = held["outbox"][0]
        event.update(publish_state="CLAIMED", relay_attempts=1, claim_owner=session)
        fields = {
            "event": event["event_id"],
            "cell": "medium-5",
            "topic": EVENTS,
            "type": "workflow.created",
            "attempt": "1",
            "owner": session,
        }
        wire = {
            "event": "boundary_reached",
            "identity": target,
            "boundary": "before_publish",
            "fields": {"key": seed, "objects": fields},
            "hook_id": "a" * 32,
            "nonce": "b" * 32,
        }
        reply = receiver.event(encode(wire), token)
        binding = {k: reply[k] for k in ("arm", "sequence", "sha256", "journal_sha256")}
        receiver.event(encode({**wire, **binding, "event": "held"}), token)
        arm = receiver.observe("medium-5")
        clock[0] = T0 + 7 * SECOND
        receiver.authorize("medium-5", arm["ack"], "release", evidence={})
        receiver.event(encode({**wire, **binding, "event": "held"}), token)
        clock[0] += 1
        receiver.event(encode({**wire, **binding, "event": "released"}), token)
    process = {**target, "session_uuid": session}
    topic = "dur053-k6-f2-probe-medium-5-test"
    return {
        "seed": seed,
        "arm": arm,
        "journal": path.read_text(),
        "relay": [process] * 3,
        "cuts": [
            cut({seed: held}, T0 + 2 * SECOND),
            cut(
                {seed: deepcopy(held), **{w: held_event(inputs, w) for w in arrivals}},
                T0 + 5 * SECOND,
            ),
        ],
        "probe": probe(topic, True, T0 + 6 * SECOND),
        "probe_topic": topic,
        "marks_before": topic_samples(T0 + 3 * SECOND),
        "marks_during": topic_samples(T0 + 6 * SECOND),
    }


def restart_fault(inputs):
    pg = inputs["cell"] == "medium-6"
    before = deepcopy(HOSTED["inspect"])
    target = "dur053-k3a-" + ("postgres" if pg else "kafka")
    before[0]["Name"] = "/" + target
    before[0]["State"]["StartedAt"] = "2026-11-20T07:39:50.076032655Z"
    after = deepcopy(before)
    after[0]["State"]["StartedAt"] = "2026-11-20T07:40:04.076032655Z"
    cid = before[0]["Id"]
    issued = T0 + 3 * SECOND
    events = ""
    for name, moment, extra in (
        ("die", issued + 100, {"exitCode": "0"}),
        ("start", issued + SECOND, {}),
    ):
        events += (
            json.dumps(
                {
                    "status": name,
                    "id": cid,
                    "from": "locked-image",
                    "Type": "container",
                    "Action": name,
                    "Actor": {"ID": cid, "Attributes": {"name": target, **extra}},
                    "scope": "local",
                    "time": moment // SECOND,
                    "timeNano": moment,
                }
            )
            + "\n"
        )
    seed, *arrivals = [r["workflow"] for r in inputs["offers"]]
    prior = held_event(inputs, seed)
    fault = {
        "target": target,
        "seed": seed,
        "before_cut": cut({seed: prior}, issued - 100),
        "inspect": [before, after],
        "issued_ns": issued,
        "completed_ns": issued + SECOND + 200,
        "events": events,
        "exit_code": 0,
        "stop_timeout_seconds": 10,
        "image_stop_signal": '"SIGINT"',
    }
    topic = "dur053-k6-f2-probe-medium-7-test"
    samples = [
        probe(topic, True, issued - SECOND),
        probe(topic, False, issued + 200),
        probe(topic, False, issued + 2 * SECOND + 200),
        probe(topic, True, issued + 3 * SECOND),
    ]
    fault["samples"] = samples
    if pg:
        new_time, old_time = "2026-11-20T07:40:04.1+00:00", "2026-11-20T07:39:50.1+00:00"

        def postgres(at, pid):
            return {
                "postmaster_start": at,
                "postmaster_pid": 1,
                "read_only": "on",
                "backends": [
                    {"application": f"durable-{role}", "pid": pid + i, "backend_start": at}
                    for i, role in enumerate(PG_ROLES)
                ],
                "retained": {
                    "workflows": {"count": 1, "hash": "c" * 64},
                    "history": {"count": 3, "hash": "d" * 64},
                },
            }

        fault["postgres"] = [postgres(old_time, 20), postgres(new_time, 25)]
        for row, at in zip((samples[0], samples[-1]), (old_time, new_time), strict=True):
            row["value"] = {"postmaster_start": at, "read_only": "on", "backend_pid": 42}
        proc = {
            "uid": "pod",
            "pod": "runtime",
            "process_start": "1:123",
            "runtime_log": runtime_start(HOSTED["runtime_log"]),
        }
        fault["engines"] = {role: [[proc], [proc]] for role in PG_ROLES}
    else:
        retained = topic_samples(issued - 2)
        saved = inputs["topics"][EVENTS]["value"][0]["records"][0]
        retained[EVENTS]["value"][0].update(next=1, records=[saved])
        fault.update(
            probe_topic=topic,
            marks=[retained, deepcopy(retained)],
            broker_process=[{"pid": 1, "start_ticks": 110}, {"pid": 1, "start_ticks": 120}],
            pending_cut=cut({w: held_event(inputs, w) for w in arrivals}, issued + 2 * SECOND),
        )
    return fault


def make_cell(cell, tmp_path, monkeypatch):
    inputs = terminal_inputs(cell)
    inputs["fault"] = (
        relay_fault(inputs, tmp_path, monkeypatch) if cell == "medium-5" else restart_fault(inputs)
    )
    return inputs


@pytest.mark.parametrize("cell", CELLS)
def test_whole_post_fault_tail(cell, tmp_path, monkeypatch):
    inputs = make_cell(cell, tmp_path, monkeypatch)
    smoke = F2Smoke.__new__(F2Smoke)
    smoke.cell_id, smoke.namespace = cell, inputs["namespace"]
    samples = iter(inputs["quiet"])
    clock = iter(s["at_ns"] for s in inputs["quiet"])
    retained = {}

    def monitor_cut():
        sample = next(samples)
        return {
            **inputs["terminal"],
            "started_ns": sample["started_ns"],
            "at_ns": sample["cut_at_ns"],
        }

    def read_until(stage, sample, seconds):
        assert stage == "F2_TERMINAL_QUIET"
        assert sample()[0] is None
        confirmed, _ = sample()
        assert confirmed is not None
        return confirmed

    monkeypatch.setattr(time, "time_ns", lambda: next(clock))
    monkeypatch.setattr(time, "sleep", lambda _: None)
    monkeypatch.setattr(smoke, "monitor_cut", monitor_cut)
    monkeypatch.setattr(smoke, "read_until", read_until)
    monkeypatch.setattr(smoke, "release_activity", lambda: inputs["marker"])
    monkeypatch.setattr(smoke, "witness", lambda: (inputs["witness"], []))
    monkeypatch.setattr(smoke, "pods", lambda role: inputs["workers"])
    monkeypatch.setattr(smoke, "engine_topics", lambda: inputs["topics"])
    monkeypatch.setattr(smoke, "retain", lambda key, value: retained.update({key: value}))
    result = smoke.closeout(inputs["fault"], inputs["offers"], inputs["registered"])
    assert join(json.loads(json.dumps(retained["checker-inputs"]))) == result
    assert result["status"] == "PASS"
    assert result["confirmation"] == "CONFIRMED"
    assert result["invariants_covered"] == list(BASE)
    assert set(result["invariants_not_covered"]) == set(UNCOVERED)
    assert result["pending_final"] == 0
    assert len(result["accepted"]) == 3
    assert result["dispositions"]
    if cell == "medium-5":
        assert result["fault"]["high_water_before_and_during"] == marks(topic_samples(T0))
    else:
        assert result["fault"]["lifecycle"]["die"]["Action"] == "die"


def reject(inputs, label, survived):
    try:
        join(inputs)
    except SmokeFailure as error:
        assert str(error).startswith(label), str(error)
    else:
        pytest.fail(survived)


def test_relay_publish_progress(tmp_path, monkeypatch):
    inputs = make_cell("medium-5", tmp_path, monkeypatch)
    outbox = inputs["fault"]["cuts"][1]["workflows"][inputs["fault"]["seed"]]["outbox"]
    outbox.append(
        {
            **outbox[0],
            "event_id": "unexpected-published-mark",
            "publish_state": "PUBLISHED",
            "published_at": "2026-11-20T07:40:04+00:00",
        }
    )
    reject(inputs, "F2_RELAY_PUBLISHED_ADVANCED", "F2_RELAY_PUBLISH_PROGRESS_ACCEPTED")


def test_probe_topic_requires_read_back(tmp_path, monkeypatch):
    inputs = make_cell("medium-5", tmp_path, monkeypatch)
    inputs["fault"]["probe"]["value"]["read_back"] = False
    reject(inputs, "F2_PROBE_HEALTHY_BROKER", "F2_UNHEALTHY_PROBE_ACCEPTED")


def test_pg_requires_new_postmaster(tmp_path, monkeypatch):
    inputs = make_cell("medium-6", tmp_path, monkeypatch)
    before, after = inputs["fault"]["postgres"]
    after["postmaster_start"] = before["postmaster_start"]
    inputs["fault"]["samples"][-1]["value"]["postmaster_start"] = before["postmaster_start"]
    reject(inputs, "F2_PG_POSTMASTER_START_NOT_NEW", "F2_OLD_POSTMASTER_ACCEPTED")


def test_pg_requires_retained_hash(tmp_path, monkeypatch):
    inputs = make_cell("medium-6", tmp_path, monkeypatch)
    inputs["fault"]["postgres"][1]["retained"]["workflows"]["hash"] = "e" * 64
    reject(inputs, "F2_PG_RETAINED_HASH", "F2_CHANGED_RETAINED_HASH_ACCEPTED")


@pytest.mark.parametrize(("cell", "label"), (("medium-6", "PG"), ("medium-7", "KAFKA")))
def test_pinned_address(cell, label, tmp_path, monkeypatch):
    inputs = make_cell(cell, tmp_path, monkeypatch)
    inputs["fault"]["inspect"][1][0]["NetworkSettings"]["Networks"]["kind"]["IPAddress"] = (
        "172.19.0.9"
    )
    reject(inputs, f"F2_{label}_ADDRESS_CHANGED", "F2_CHANGED_PINNED_ADDRESS_ACCEPTED")


def test_kafka_low_high_water(tmp_path, monkeypatch):
    inputs = make_cell("medium-7", tmp_path, monkeypatch)
    inputs["fault"]["marks"][1] = topic_samples(T0 + 7 * SECOND)
    reject(inputs, "F2_KAFKA_HIGH_WATER_REGRESSED", "F2_KAFKA_LOST_HIGH_WATER_ACCEPTED")


def test_kafka_restart_verdict_read_is_single_shot_and_retains_failure(tmp_path, monkeypatch):
    smoke = F2Smoke.__new__(F2Smoke)
    smoke.evidence = F2Evidence(tmp_path)
    smoke.env = {}
    smoke.deadline = datetime.now(UTC) + timedelta(seconds=30)
    smoke.broker = "192.0.2.2"
    smoke.kafka_probe = tmp_path / "kafka-read"
    ranges = json.dumps(topic_samples(T0)[EVENTS]["value"]).encode()
    stderr = b"K6E1_KAFKA_READ: Leader Not Available\n"
    run = Mock(
        side_effect=[
            subprocess.CompletedProcess([], 0, ranges, b""),
            subprocess.CompletedProcess([], 1, b"", stderr),
        ]
    )
    monkeypatch.setattr(subprocess, "run", run)

    with (
        pytest.raises(SmokeFailure, match="^E1_KAFKA_DIRECT_READ_EXIT_1$"),
        smoke.evidence.step("medium-7"),
    ):
        smoke.engine_topics()

    assert run.call_count == 2
    rows = [
        json.loads(line)
        for line in (tmp_path / "kafka-direct-read-attempts.jsonl").read_bytes().splitlines()
    ]
    assert [(row["topic"], row["exit_status"], row["stderr"]) for row in rows] == [
        (EVENTS, 0, ""),
        (TASKS, 1, stderr.decode()),
    ]
    assert all(
        row["step"] == "medium-7" and row["attempt"] == 1 and not row["setup"] for row in rows
    )


def test_coverage_derived_from_witnesses():
    from dur053_k6_f2_inputs import coverage

    predicates = {name: {"witness": "id"} for name in BASE if name != "I11"}
    try:
        coverage(predicates)
    except SmokeFailure as error:
        assert str(error).startswith("F2_B_COVERAGE_INCOMPLETE")
    else:
        pytest.fail("F2_MISSING_B_WITNESS_ACCEPTED")


def test_timestamped_log():
    result = runtime_start(HOSTED["runtime_log"])
    assert result["timestamp"] == "2026-11-20T07:41:48.085579042Z"
    assert result["at_ns"] % SECOND == 85_579_042
    with pytest.raises(SmokeFailure, match="F2_TIMESTAMPED_RUNTIME_START"):
        runtime_start("2026/11/20 07:41:48 INFO other log line")
