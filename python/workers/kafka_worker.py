"""Kafka deliveries -> durable inbox -> authoritative claim -> Python activity.

Each slot owns its consumer on one thread. No auto commit; a message is
acknowledged only after the API commits its inbox/quarantine disposition.
Crashes between acknowledgment and claim/result are repaired by the scheduler.
"""

from __future__ import annotations

import base64
import logging
import threading
import uuid
from typing import Any

from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition

from workers.control import Claim, ControlClient, ControlError
from workers.registry import default_registry
from workers.runner import ActivityRunner, ActivityTask

LOG = logging.getLogger(__name__)
TASK_TOPIC = "durable-agent.tasks.v1"
GROUP = "runtime-workers-v1"


class RetryingControl(ControlClient):
    """Retry uncertain HTTP outcomes using identical bodies, never rerun work."""

    def __init__(self, url: str, stop: threading.Event) -> None:
        super().__init__(url)
        self.stop = stop
        self.expected_attempt = 0
        self.worker_id = ""
        self.last_result_retry_count = 0

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        delay = 0.1
        operation = path.rsplit("/", 1)[-1]
        retry_count = 0
        if operation == "result":
            self.last_result_retry_count = 0
        while not self.stop.is_set():
            try:
                response = super()._post(path, body)
                if operation == "result":
                    self.last_result_retry_count = retry_count
                return response
            except ControlError as error:
                error.operation = operation
                error.retry_count = retry_count
                if error.status < 500:
                    raise
                retry_count += 1
                error.retry_count = retry_count
                if operation == "result":
                    LOG.info(
                        "control result retry scheduled "
                        "workflow_id=%s node_id=%s attempt_number=%s worker_id=%s "
                        "retry_number=%s status=%s code=%s",
                        path.split("/")[3] if len(path.split("/")) > 3 else "",
                        path.split("/")[5] if len(path.split("/")) > 5 else "",
                        body.get("attempt_number", ""),
                        self.worker_id,
                        retry_count,
                        error.status,
                        error.code,
                    )
                else:
                    LOG.warning("control request unavailable: %s", error.code)
                self.stop.wait(delay)
                delay = min(5.0, delay * 2)
        raise ControlError(503, "SHUTTING_DOWN", "worker is stopping")

    def claim(
        self,
        workflow_id: str,
        node_id: str,
        iteration: int,
        worker_id: str,
        request_id: str,
        attempt_lease_ms: int = 60_000,
        expected_attempt: int = 0,
    ) -> Claim:
        return super().claim(
            workflow_id,
            node_id,
            iteration,
            worker_id,
            request_id,
            attempt_lease_ms,
            expected_attempt or self.expected_attempt,
        )


def delivery_body(message: Any) -> dict[str, Any]:
    headers = dict(message.headers or [])

    def header(name: str) -> str:
        return (headers.get(name) or b"").decode("utf-8", errors="replace")

    try:
        version = int(header("schema-version"))
        if version <= 0:
            version = -1
    except ValueError:
        version = -1
    return {
        "event_id": header("event-id") or (message.key or b"").decode("utf-8", errors="replace"),
        "event_type": header("event-type"),
        "schema_version": version,
        "topic": message.topic,
        "partition": message.partition,
        "offset": message.offset,
        "payload": base64.b64encode(message.value or b"").decode("ascii"),
    }


def handle_delivery(consumer: Any, message: Any, control: RetryingControl, worker_id: str) -> None:
    traceparent = (
        dict(message.headers or []).get("traceparent", b"").decode("utf-8", errors="replace")
    )
    control.traceparent = traceparent
    response = control._post("/v1/worker-deliveries", delivery_body(message))
    if response["commit_offset"]:
        consumer.commit(
            {
                TopicPartition(message.topic, message.partition): OffsetAndMetadata(
                    int(response["next_offset"]), "", -1
                )
            }
        )
    task = response.get("task")
    if task is None:
        return
    control.worker_id = worker_id
    control.expected_attempt = int(task["attempt_number"])
    work = ActivityTask(
        workflow_id=task["workflow_id"],
        node_id=task["node_id"],
        iteration=task["iteration"],
        worker_id=worker_id,
        request_id=f"{worker_id}:{uuid.uuid4()}",
        activity_name=task["activity_name"],
        activity_version=task["activity_version"],
        input=task["input"],
    )
    try:
        ActivityRunner(control, default_registry()).run_task(work)
    except ControlError as error:
        if error.code not in {"STALE_CLAIM", "STALE_ATTEMPT"}:
            raise
        # Keep the rejected delivery identity and operation in the log. The
        # same stale code can come from claim, heartbeat, or result; only the
        # result operation proves a late result reached the API and was refused.
        LOG.info(
            "delivery control rejected "
            "workflow_id=%s node_id=%s attempt_number=%s worker_id=%s operation=%s code=%s",
            task["workflow_id"],
            task["node_id"],
            task["attempt_number"],
            worker_id,
            error.operation or "unknown",
            error.code,
        )


def consume_slot(brokers: str, url: str, worker_id: str, stop: threading.Event) -> None:
    while not stop.is_set():
        consumer = None
        try:
            consumer = KafkaConsumer(
                TASK_TOPIC,
                bootstrap_servers=brokers.split(","),
                group_id=GROUP,
                client_id=worker_id,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                max_poll_records=1,
            )
            control = RetryingControl(url, stop)
            while not stop.is_set():
                for records in consumer.poll(timeout_ms=500, max_records=1).values():
                    for message in records:
                        handle_delivery(consumer, message, control, worker_id)
        except Exception:
            if not stop.is_set():
                LOG.exception("Kafka worker slot failed; restarting from committed offset")
                stop.wait(1.0)
        finally:
            if consumer is not None:
                consumer.close(autocommit=False)


def start_pool(
    brokers: str, url: str, worker_id: str, stop: threading.Event, slots: int = 4
) -> list[threading.Thread]:
    if slots < 1 or slots > 16:
        raise ValueError("WORKER_SLOTS must be between 1 and 16")
    threads = [
        threading.Thread(
            target=consume_slot,
            args=(brokers, url, f"{worker_id}-{slot}", stop),
            name=f"kafka-slot-{slot}",
            daemon=True,
        )
        for slot in range(slots)
    ]
    for thread in threads:
        thread.start()
    return threads
