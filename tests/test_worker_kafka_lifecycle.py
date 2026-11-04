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


def test_real_kafka_assignment_during_long_activity(monkeypatch: pytest.MonkeyPatch) -> None:
    brokers = os.getenv("DURABLE_KAFKA_BROKERS")
    if not brokers:
        pytest.skip("real Kafka witness requires DURABLE_KAFKA_BROKERS")
    import workers.kafka_worker as current

    reference = ModuleType("pre_k1_membership")
    source = Path(__file__).with_name("fixtures") / "k1-pre-kafka-worker.txt"
    exec(compile(source.read_bytes(), str(source), "exec"), reference.__dict__)
    admin = KafkaAdminClient(bootstrap_servers=brokers.split(","), request_timeout_ms=10000)
    witnesses: list[dict[str, Any]] = []
    try:
        for legacy, module in [(True, reference), (False, current)]:
            topic = "dur053-k1-" + uuid.uuid4().hex
            admin.create_topics([NewTopic(topic, num_partitions=2, replication_factor=1)])
            lifecycle = WorkerLifecycle(legacy=legacy)
            entered, release = threading.Event(), threading.Event()
            monkeypatch.setattr(module, "TASK_TOPIC", topic)
            monkeypatch.setattr(module, "GROUP", topic)

            def run_task(
                _runner: ActivityRunner,
                _task: Any,
                activity_entered: threading.Event = entered,
                activity_release: threading.Event = release,
            ) -> dict[str, Any]:
                activity_entered.set()
                assert activity_release.wait(12), "activity barrier timed out"
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
                producer.send(topic, b"barrier", partition=0).get(timeout=10)
                assert entered.wait(10), "first worker did not enter activity"

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
                assigned_before_release = peer_assigned.wait(4)
                elapsed = time.monotonic() - started
                witnesses.append(
                    {
                        "legacy": legacy,
                        "peer_assigned_before_activity_release": assigned_before_release,
                        "observation_seconds": round(elapsed, 3),
                    }
                )
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
