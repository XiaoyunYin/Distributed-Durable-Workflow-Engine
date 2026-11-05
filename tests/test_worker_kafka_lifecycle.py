"""Real eager-group membership witness; activity/control are barrier stubs."""

from __future__ import annotations

import os
import threading
import time
import uuid
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from workers.control import ControlClient
from workers.lifecycle import WorkerLifecycle
from workers.runner import ActivityRunner


def _measure_unblocked_assignment(brokers: list[str], topic: str, admin: KafkaAdminClient) -> float:
    """Measure a fresh consumer group's assignment latency on the local broker."""
    control_topic = topic + "-control"
    admin.create_topics([NewTopic(control_topic, num_partitions=2, replication_factor=1)])
    assigned = threading.Event()

    def observe() -> None:
        consumer = KafkaConsumer(
            control_topic,
            bootstrap_servers=brokers,
            group_id=control_topic,
            enable_auto_commit=False,
            auto_offset_reset="latest",
        )
        try:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline and not assigned.is_set():
                consumer.poll(timeout_ms=100, max_records=1)
                if consumer.assignment():
                    assigned.set()
        finally:
            consumer.close(autocommit=False, timeout_ms=1000)

    started = time.monotonic()
    thread = threading.Thread(target=observe, daemon=True)
    thread.start()
    try:
        assert assigned.wait(60), "unblocked control peer was not assigned within 60 s"
        return time.monotonic() - started
    finally:
        thread.join(3)
        admin.delete_topics([control_topic])
        assert not thread.is_alive(), "control peer observer did not stop"


def test_real_kafka_assignment_during_long_activity(monkeypatch: pytest.MonkeyPatch) -> None:
    brokers = os.getenv("DURABLE_KAFKA_BROKERS")
    if not brokers:
        pytest.skip("real Kafka witness requires DURABLE_KAFKA_BROKERS")
    import workers.kafka_worker as current

    reference = ModuleType("pre_k1_membership")
    source = Path(__file__).with_name("fixtures") / "k1-pre-kafka-worker.txt"
    exec(compile(source.read_bytes(), str(source), "exec"), reference.__dict__)
    admin = KafkaAdminClient(bootstrap_servers=brokers.split(","), request_timeout_ms=10000)
    broker_list = brokers.split(",")
    witnesses: list[dict[str, Any]] = []
    try:
        for legacy, module in [(True, reference), (False, current)]:
            topic = "dur053-k1-" + uuid.uuid4().hex
            admin.create_topics([NewTopic(topic, num_partitions=2, replication_factor=1)])
            lifecycle = WorkerLifecycle(legacy=legacy)
            entered, release, activity_timed_out = (
                threading.Event(),
                threading.Event(),
                threading.Event(),
            )
            observation_window = 60.0 if not legacy else 0.0
            barrier_seconds = [0.0]
            activity_deadline = [0.0]
            monkeypatch.setattr(module, "TASK_TOPIC", topic)
            monkeypatch.setattr(module, "GROUP", topic)

            def run_task(
                _runner: ActivityRunner,
                _task: Any,
                activity_entered: threading.Event = entered,
                activity_release: threading.Event = release,
                timed_out: threading.Event = activity_timed_out,
                activity_barrier_seconds: list[float] = barrier_seconds,
                deadline: list[float] = activity_deadline,
            ) -> dict[str, Any]:
                deadline[0] = time.monotonic() + activity_barrier_seconds[0]
                activity_entered.set()
                if not activity_release.wait(activity_barrier_seconds[0]):
                    timed_out.set()
                    raise AssertionError("activity barrier timed out")
                return {}

            def post(
                _client: ControlClient, _path: str, _body: Any, workflow_id: str = topic
            ) -> dict[str, Any]:
                return {
                    "commit_offset": True,
                    "next_offset": 1,
                    "task": {
                        "workflow_id": workflow_id,
                        "node_id": "pure.echo",
                        "iteration": 0,
                        "attempt_number": 1,
                        "activity_name": "pure.echo",
                        "activity_version": "v1",
                        "input": {},
                    },
                }

            monkeypatch.setattr(ActivityRunner, "run_task", run_task)
            monkeypatch.setattr(ControlClient, "_post", post)
            if legacy:
                threads = module.start_pool(brokers, "http://unused", topic, lifecycle.drain, 1)
            else:
                threads = module.start_pool(
                    brokers, "http://unused", topic, lifecycle.drain, 1, lifecycle
                )
            producer = KafkaProducer(bootstrap_servers=brokers.split(","))
            peer_stop, peer_assigned = threading.Event(), threading.Event()
            peer_thread = None
            try:
                control_join_seconds = _measure_unblocked_assignment(broker_list, topic, admin)
                if legacy:
                    observation_window = max(5.0, 3.0 * control_join_seconds)
                barrier_seconds[0] = observation_window + 30.0
                producer.send(topic, b"barrier", partition=0).get(timeout=10)
                assert entered.wait(60), "first worker did not enter activity within 60 s"

                def observe_peer(
                    peer_topic: str = topic,
                    stop: threading.Event = peer_stop,
                    assigned: threading.Event = peer_assigned,
                ) -> None:
                    peer = KafkaConsumer(
                        peer_topic,
                        bootstrap_servers=brokers.split(","),
                        group_id=peer_topic,
                        enable_auto_commit=False,
                        auto_offset_reset="latest",
                    )
                    try:
                        while not stop.is_set():
                            peer.poll(timeout_ms=100, max_records=1)
                            if peer.assignment():
                                assigned.set()
                    finally:
                        peer.close(autocommit=False, timeout_ms=1000)

                peer_thread = threading.Thread(target=observe_peer, daemon=True)
                started = time.monotonic()
                peer_thread.start()
                assigned_before_release = peer_assigned.wait(observation_window)
                elapsed = time.monotonic() - started
                activity_held = (
                    entered.is_set()
                    and not release.is_set()
                    and not activity_timed_out.is_set()
                    and time.monotonic() < activity_deadline[0]
                )
                witnesses.append(
                    {
                        "legacy": legacy,
                        "control_join_seconds": round(control_join_seconds, 3),
                        "observation_window_seconds": round(observation_window, 3),
                        "activity_barrier_seconds": round(barrier_seconds[0], 3),
                        "peer_assigned_before_activity_release": assigned_before_release,
                        "activity_held_when_observation_ended": activity_held,
                        "activity_timed_out": activity_timed_out.is_set(),
                        "observation_seconds": round(elapsed, 3),
                    }
                )
                assert activity_held, f"activity was not held through observation: {witnesses}"
                assert assigned_before_release != legacy, (
                    f"unexpected eager assignment witness: {witnesses}"
                )
                # The new consumer leaves its group before the activity ends.
                lifecycle.begin_drain()
                if not legacy:
                    threads[0].join(2)
                    assert not threads[0].is_alive(), "draining consumer waited for activity"
            finally:
                lifecycle.begin_drain()
                release.set()
                for thread in threads:
                    thread.join(3)
                peer_stop.set()
                if peer_thread is not None:
                    peer_thread.join(8)
                    assert not peer_thread.is_alive(), "peer membership observer did not stop"
                producer.close(timeout=1)
                admin.delete_topics([topic])
        print("K1 real Kafka membership witness:", witnesses)
    finally:
        admin.close()
