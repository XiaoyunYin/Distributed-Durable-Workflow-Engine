"""Bounded, heartbeat-aware activity runner for direct M2 dispatch tests."""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Protocol

from workers.control import Claim, ControlClient, ControlError
from workers.registry import ActivityRegistry

LOG = logging.getLogger(__name__)


class WorkerControl(Protocol):
    def claim(
        self,
        workflow_id: str,
        node_id: str,
        iteration: int,
        worker_id: str,
        request_id: str,
        attempt_lease_ms: int = 60_000,
    ) -> Claim: ...

    def heartbeat(
        self,
        workflow_id: str,
        node_id: str,
        iteration: int,
        attempt_number: int,
        claim_token: str,
        extension_ms: int = 60_000,
    ) -> str: ...

    def result(
        self,
        workflow_id: str,
        node_id: str,
        iteration: int,
        attempt_number: int,
        claim_token: str,
        attempt_state: str,
        payload: Any,
        event_type: str = "",
        reconciliation_ref: str = "",
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class ActivityTask:
    workflow_id: str
    node_id: str
    iteration: int
    worker_id: str
    request_id: str
    activity_name: str
    activity_version: str
    input: Any
    attempt_lease_ms: int = 60_000


class ActivityRunner:
    def __init__(
        self,
        control: WorkerControl,
        registry: ActivityRegistry,
        max_workers: int = 4,
        heartbeat_interval: float = 5.0,
    ) -> None:
        if max_workers <= 0 or heartbeat_interval <= 0:
            raise ValueError("max_workers and heartbeat_interval must be positive")
        self.control = control
        self.registry = registry
        self.max_workers = max_workers
        self.heartbeat_interval = heartbeat_interval

    def run_task(self, task: ActivityTask) -> dict[str, Any]:
        # Resolve before claiming. A configuration error must not create a
        # claimed attempt that can only ever be retried.
        activity = self.registry.resolve(task.activity_name, task.activity_version)
        try:
            claim = self.control.claim(
                task.workflow_id,
                task.node_id,
                task.iteration,
                task.worker_id,
                task.request_id,
                task.attempt_lease_ms,
            )
        except ControlError as error:
            error.operation = "claim"
            raise
        stop = threading.Event()
        definitive_heartbeat_errors: list[ControlError] = []

        def heartbeat_loop() -> None:
            delay = self.heartbeat_interval
            while not stop.wait(delay):
                try:
                    self.control.heartbeat(
                        task.workflow_id,
                        task.node_id,
                        task.iteration,
                        claim.attempt_number,
                        claim.claim_token,
                        task.attempt_lease_ms,
                    )
                    delay = self.heartbeat_interval
                except Exception as error:
                    if isinstance(error, ControlError):
                        error.operation = "heartbeat"
                    if isinstance(error, ControlError) and error.code in {
                        "STALE_CLAIM",
                        "STALE_ATTEMPT",
                    }:
                        definitive_heartbeat_errors.append(error)
                        return
                    # A transient control outage must not discard work that
                    # has already run. Retry with bounded backoff and still
                    # submit the terminal result after the activity returns.
                    delay = min(max(self.heartbeat_interval, delay * 2), 60.0)

        heartbeat_thread = threading.Thread(
            target=heartbeat_loop, name="activity-heartbeat", daemon=True
        )
        heartbeat_thread.start()
        try:
            payload = activity(task.input)
        except Exception as error:
            payload = {"error": str(error), "type": type(error).__name__}
            attempt_state = "FAILED_RETRYABLE"
        else:
            attempt_state = "SUCCEEDED"
        finally:
            stop.set()
            heartbeat_thread.join(timeout=max(1.0, self.heartbeat_interval))
        payload_fingerprint = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
                "utf-8"
            )
        ).hexdigest()
        try:
            receipt = self.control.result(
                task.workflow_id,
                task.node_id,
                task.iteration,
                claim.attempt_number,
                claim.claim_token,
                attempt_state,
                payload,
            )
        except ControlError as error:
            error.operation = "result"
            LOG.info(
                "activity result submission "
                "workflow_id=%s node_id=%s iteration=%s attempt_number=%s worker_id=%s "
                "attempt_state=%s payload_sha256=%s outcome=rejected status=%s code=%s retries=%s",
                task.workflow_id,
                task.node_id,
                task.iteration,
                claim.attempt_number,
                task.worker_id,
                attempt_state,
                payload_fingerprint,
                error.status,
                error.code,
                error.retry_count,
            )
            raise
        retry_count = int(getattr(self.control, "last_result_retry_count", 0))
        LOG.info(
            "activity result submission "
            "workflow_id=%s node_id=%s iteration=%s attempt_number=%s worker_id=%s "
            "attempt_state=%s payload_sha256=%s outcome=accepted retries=%s",
            task.workflow_id,
            task.node_id,
            task.iteration,
            claim.attempt_number,
            task.worker_id,
            attempt_state,
            payload_fingerprint,
            retry_count,
        )
        if definitive_heartbeat_errors:
            # The result/evidence call above is always attempted first. The
            # stale claim remains visible to the caller after that durable
            # handoff, while successful evidence is never hidden.
            raise definitive_heartbeat_errors[0]
        return receipt

    def run_many(self, tasks: list[ActivityTask]) -> list[dict[str, Any]]:
        with ThreadPoolExecutor(
            max_workers=self.max_workers, thread_name_prefix="activity"
        ) as pool:
            futures: list[Future[dict[str, Any]]] = [
                pool.submit(self.run_task, task) for task in tasks
            ]
            return [future.result() for future in futures]


def runner_for_url(
    control_url: str, registry: ActivityRegistry, max_workers: int = 4
) -> ActivityRunner:
    return ActivityRunner(ControlClient(control_url), registry, max_workers=max_workers)
