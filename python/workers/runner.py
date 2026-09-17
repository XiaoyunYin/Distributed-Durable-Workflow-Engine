"""Bounded, heartbeat-aware activity runner for direct M2 dispatch tests."""

from __future__ import annotations

import threading
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Protocol

from workers.control import Claim, ControlClient
from workers.registry import ActivityRegistry


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
        claim = self.control.claim(
            task.workflow_id,
            task.node_id,
            task.iteration,
            task.worker_id,
            task.request_id,
            task.attempt_lease_ms,
        )
        stop = threading.Event()
        heartbeat_errors: list[BaseException] = []

        def heartbeat_loop() -> None:
            while not stop.wait(self.heartbeat_interval):
                try:
                    self.control.heartbeat(
                        task.workflow_id,
                        task.node_id,
                        task.iteration,
                        claim.attempt_number,
                        claim.claim_token,
                        task.attempt_lease_ms,
                    )
                except BaseException as error:  # preserve stale-claim evidence for the caller
                    heartbeat_errors.append(error)
                    return

        heartbeat_thread = threading.Thread(
            target=heartbeat_loop, name="activity-heartbeat", daemon=True
        )
        heartbeat_thread.start()
        try:
            activity = self.registry.resolve(task.activity_name, task.activity_version)
            payload = activity(task.input)
        except BaseException as error:
            payload = {"error": str(error), "type": type(error).__name__}
            attempt_state = "FAILED_RETRYABLE"
        else:
            attempt_state = "SUCCEEDED"
        finally:
            stop.set()
            heartbeat_thread.join(timeout=max(1.0, self.heartbeat_interval))
        if heartbeat_errors:
            raise heartbeat_errors[0]
        return self.control.result(
            task.workflow_id,
            task.node_id,
            task.iteration,
            claim.attempt_number,
            claim.claim_token,
            attempt_state,
            payload,
        )

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
