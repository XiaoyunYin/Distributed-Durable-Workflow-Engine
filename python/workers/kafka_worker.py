"""Kafka deliveries -> durable inbox -> authoritative claim -> Python activity.

Each slot owns its consumer on one thread. No auto commit; a message is
acknowledged only after the API commits its inbox/quarantine disposition.
Crashes between acknowledgment and claim/result are repaired by the scheduler.
"""

from __future__ import annotations

import base64
import json
import logging
import threading
import time
import uuid
from typing import Any

from kafka import ConsumerRebalanceListener, KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition

from workers.control import Claim, ControlClient, ControlError
from workers.k4_control import CURRENT, new_request
from workers.k4_witness import campaign_enabled
from workers.lifecycle import BoundedExecutor, WorkerLifecycle
from workers.registry import default_registry
from workers.runner import ActivityRunner, ActivityTask
from workers.witness import campaign_witness

LOG = logging.getLogger(__name__)
TASK_TOPIC = "durable-agent.tasks.v1"
GROUP = "runtime-workers-v1"


class RetryingControl(ControlClient):
    """Retry uncertain HTTP outcomes using identical bodies, never rerun work."""

    def __init__(
        self, url: str, stop: threading.Event, lifecycle: WorkerLifecycle | None = None
    ) -> None:
        super().__init__(url)
        self.stop = stop
        self.lifecycle = lifecycle
        self.request_deadlines = threading.local()
        self.expected_attempt = 0
        self.worker_id = ""
        self.last_result_retry_count = 0

    def aborted(self) -> bool:
        return self.stop.is_set() or (self.lifecycle is not None and self.lifecycle.aborted())

    def request_timeout(self) -> float:
        if self.lifecycle is not None:
            budget = self.lifecycle.remaining()
            deadline = getattr(self.request_deadlines, "deadline", time.monotonic() + 30.0)
            return min(self.timeout, max(0.001, min(budget, deadline - time.monotonic())))
        return self.timeout

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        if not campaign_enabled():
            return self._retry_post(path, body)
        # Freeze the serialized body once for all attempts. Neither a retry nor
        # another thread may replace this logical request's identity or body.
        body = json.loads(json.dumps(body, separators=(",", ":")))
        token = CURRENT.set(new_request(body))
        try:
            return self._retry_post(path, body)
        finally:
            CURRENT.reset(token)

    def _retry_post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        deadline = time.monotonic() + 30.0 if self.lifecycle is not None else float("inf")
        self.request_deadlines.deadline = deadline
        delay = 0.1
        operation = path.rsplit("/", 1)[-1]
        retry_count = 0
        if operation == "result":
            self.last_result_retry_count = 0
        while not self.aborted() and time.monotonic() < deadline:
            if (
                operation in {"worker-deliveries", "claim"}
                and self.lifecycle is not None
                and not self.lifecycle.can_start()
            ):
                break
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
                self.stop.wait(
                    min(delay, self.lifecycle.remaining(), max(0.0, deadline - time.monotonic()))
                    if self.lifecycle
                    else delay
                )
                delay = min(5.0, delay * 2)
        if self.lifecycle is not None and time.monotonic() >= deadline and not self.aborted():
            raise ControlError(503, "CONTROL_UNAVAILABLE", "control retry deadline exceeded")
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


def handle_delivery(
    consumer: Any,
    message: Any,
    control: RetryingControl,
    worker_id: str,
    lifecycle: WorkerLifecycle | None = None,
    executor: BoundedExecutor | None = None,
    assignment: Assignment | None = None,
) -> threading.Event | None:
    if lifecycle is not None and not lifecycle.can_start():
        return None
    generation = assignment.generation if assignment is not None else 0
    traceparent = (
        dict(message.headers or []).get("traceparent", b"").decode("utf-8", errors="replace")
    )
    control.traceparent = traceparent
    response = control._post("/v1/worker-deliveries", delivery_body(message))
    if witness := campaign_witness():
        task_identity = response.get("task") or {}
        witness.trace(
            "disposed",
            {
                "consumer": worker_id,
                "consumer_session": getattr(consumer, "_dur053_session", ""),
                "generation": group_generation(consumer),
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "event_id": delivery_body(message)["event_id"],
                "disposition": response.get("disposition", ""),
                "commit_offset": response["commit_offset"],
                "next_offset": response.get("next_offset"),
                "workflow": task_identity.get("workflow_id", ""),
                "node": task_identity.get("node_id", ""),
                "iteration": task_identity.get("iteration"),
                "attempt": task_identity.get("attempt_number"),
            },
        )
    if assignment is not None and not assignment.current(message, generation):
        return None
    if response["commit_offset"]:
        consumer.commit(
            {
                TopicPartition(message.topic, message.partition): OffsetAndMetadata(
                    int(response["next_offset"]), "", -1
                )
            }
        )
    if lifecycle is not None and not lifecycle.can_start():
        return None
    task = response.get("task")
    if task is None:
        return None
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

    def execute() -> None:
        try:
            receipt = ActivityRunner(control, default_registry()).run_task(work)
            if witness := campaign_witness():
                witness.trace(
                    "result_disposition",
                    {
                        "consumer": worker_id,
                        "consumer_session": getattr(consumer, "_dur053_session", ""),
                        "generation": group_generation(consumer),
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "event_id": delivery_body(message)["event_id"],
                        "workflow": work.workflow_id,
                        "node": work.node_id,
                        "iteration": work.iteration,
                        "attempt": control.expected_attempt,
                        "disposition": receipt.get("Disposition", ""),
                    },
                )
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

    if executor is not None:
        return executor.submit(execute)
    execute()
    return None


def legacy_consume_slot(brokers: str, url: str, worker_id: str, stop: threading.Event) -> None:
    while not stop.is_set():
        consumer = None
        try:
            consumer = KafkaConsumer(
                *([] if campaign_enabled() else [TASK_TOPIC]),
                bootstrap_servers=brokers.split(","),
                group_id=GROUP,
                client_id=worker_id,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                max_poll_records=1,
            )
            if campaign_enabled():
                callbacks = CallbackTrace(worker_id)
                callbacks.attach(consumer)
                consumer.subscribe([TASK_TOPIC], listener=callbacks)
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
    brokers: str,
    url: str,
    worker_id: str,
    stop: threading.Event,
    slots: int = 4,
    lifecycle: WorkerLifecycle | None = None,
) -> list[threading.Thread]:
    if slots < 1 or slots > 16:
        raise ValueError("WORKER_SLOTS must be between 1 and 16")
    if lifecycle is not None and not lifecycle.legacy:
        executor = BoundedExecutor(lifecycle, slots)
        threads = [
            threading.Thread(
                target=consume_slot,
                args=(brokers, url, f"{worker_id}-{slot}", lifecycle, executor),
                name=f"kafka-slot-{slot}",
                daemon=True,
            )
            for slot in range(slots)
        ]
        for thread in threads:
            thread.start()
        return threads
    threads = [
        threading.Thread(
            target=legacy_consume_slot,
            args=(brokers, url, f"{worker_id}-{slot}", stop),
            name=f"kafka-slot-{slot}",
            daemon=True,
        )
        for slot in range(slots)
    ]
    for thread in threads:
        thread.start()
    return threads


class Assignment(ConsumerRebalanceListener):  # type: ignore[misc]
    """Revoke invalidates transport work, never an already durable activity claim."""

    def __init__(self, lifecycle: WorkerLifecycle) -> None:
        self.lifecycle = lifecycle
        self.generation = 0
        self.partitions: set[Any] = set()
        self.trace: CallbackTrace | None = None

    def on_partitions_revoked(self, partitions: Any) -> None:
        if self.trace is not None:
            self.trace.on_partitions_revoked(partitions)
        self.generation += 1
        self.partitions.difference_update(partitions)
        with self.lifecycle.lock:
            self.lifecycle.rebalances += 1

    def on_partitions_assigned(self, partitions: Any) -> None:
        if self.trace is not None:
            self.trace.on_partitions_assigned(partitions)
        self.generation += 1
        self.partitions = set(partitions)

    def current(self, message: Any, generation: int) -> bool:
        return (
            self.generation == generation
            and TopicPartition(message.topic, message.partition) in self.partitions
        )


def group_generation(consumer: Any) -> int:
    coordinator = getattr(consumer, "_coordinator", None)
    generation = getattr(coordinator, "_generation", None)
    return int(getattr(generation, "generation_id", -1))


class CallbackTrace(ConsumerRebalanceListener):  # type: ignore[misc]
    """Observation only. Assignment's existing transport validity is unchanged."""

    def __init__(self, consumer_id: str) -> None:
        self.consumer_id = consumer_id
        self.consumer_session = str(uuid.uuid4())
        self.consumer: Any = None
        self.partitions: set[Any] = set()

    def attach(self, consumer: Any) -> None:
        self.consumer = consumer
        consumer._dur053_session = self.consumer_session
        coordinator = consumer._coordinator
        reset = coordinator.reset_generation

        def observed_reset(*args: Any, **kwargs: Any) -> Any:
            generation = group_generation(consumer)
            member = str(getattr(coordinator._generation, "member_id", ""))
            partitions = set(self.partitions)
            result = reset(*args, **kwargs)
            # kafka-python 2.2.16 has no lost-listener dispatch. Observe the
            # original membership reset, after it runs, without a poll/rejoin.
            if generation >= 0 and partitions:
                self.emit("lost", partitions, generation, member)
            return result

        coordinator.reset_generation = observed_reset

    def emit(
        self, event: str, partitions: Any, generation: int | None = None, member: str | None = None
    ) -> None:
        if witness := campaign_witness():
            group = getattr(getattr(self.consumer, "_coordinator", None), "_generation", None)
            witness.trace(
                event,
                {
                    "consumer": self.consumer_id,
                    "consumer_session": self.consumer_session,
                    "member_id": str(getattr(group, "member_id", "")) if member is None else member,
                    "generation": group_generation(self.consumer)
                    if generation is None
                    else generation,
                    "partitions": [
                        {"topic": p.topic, "partition": p.partition} for p in sorted(partitions)
                    ],
                },
            )

    def on_partitions_revoked(self, partitions: Any) -> None:
        self.emit("revoke", partitions)
        self.partitions.difference_update(partitions)

    def on_partitions_assigned(self, partitions: Any) -> None:
        self.emit("assigned", partitions)
        self.partitions = set(partitions)

    def on_partitions_lost(self, partitions: Any) -> None:
        self.emit("lost", partitions)


def consume_slot(
    brokers: str, url: str, worker_id: str, lifecycle: WorkerLifecycle, executor: BoundedExecutor
) -> None:
    active: threading.Event | None = None
    while lifecycle.can_start():
        consumer = None
        try:
            assignment = Assignment(lifecycle)
            consumer = KafkaConsumer(
                bootstrap_servers=brokers.split(","),
                group_id=GROUP,
                client_id=worker_id,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                max_poll_records=1,
                request_timeout_ms=11000,
                session_timeout_ms=10000,
                heartbeat_interval_ms=3000,
                api_version_auto_timeout_ms=3000,
            )
            if campaign_enabled():
                assignment.trace = CallbackTrace(worker_id)
                assignment.trace.attach(consumer)
            consumer.subscribe([TASK_TOPIC], listener=assignment)
            control = RetryingControl(url, lifecycle.abort, lifecycle)
            while lifecycle.can_start():
                busy = active is not None and not active.is_set()
                partitions = consumer.assignment()
                if partitions:
                    if busy:
                        consumer.pause(*partitions)
                    else:
                        consumer.resume(*partitions)
                # Poll/rejoin even while a long activity holds a durable claim.
                records_by_partition = consumer.poll(timeout_ms=100, max_records=1)
                if busy:
                    # A rebalance in poll can assign fresh, unpaused partitions.
                    # Do not lose fetched positions while the executor is full.
                    for partition, records in records_by_partition.items():
                        if records and partition in consumer.assignment():
                            consumer.seek(partition, records[0].offset)
                    continue
                for records in records_by_partition.values():
                    for message in records:
                        active = handle_delivery(
                            consumer, message, control, worker_id, lifecycle, executor, assignment
                        )
        except Exception:
            if lifecycle.can_start():
                LOG.exception("Kafka worker slot failed; restarting from committed offset")
                lifecycle.drain.wait(1.0)
        finally:
            if consumer is not None:
                # Same thread as poll/commit/pause/subscribe, and no activity join.
                consumer.close(autocommit=False, timeout_ms=1000)
