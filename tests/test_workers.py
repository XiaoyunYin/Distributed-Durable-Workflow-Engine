from __future__ import annotations

import threading
import time
from typing import Any

import pytest
from workers.control import Claim, ControlError
from workers.registry import ActivityRegistry
from workers.runner import ActivityRunner, ActivityTask


class FakeControl:
    def __init__(self) -> None:
        self.heartbeats = 0
        self.results: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def claim(
        self,
        workflow_id: str,
        node_id: str,
        iteration: int,
        worker_id: str,
        request_id: str,
        attempt_lease_ms: int = 60_000,
    ) -> Claim:
        return Claim(1, f"token-{request_id}", "PURE_ACTIVITY", "deadline")

    def heartbeat(
        self,
        workflow_id: str,
        node_id: str,
        iteration: int,
        attempt_number: int,
        claim_token: str,
        extension_ms: int = 60_000,
    ) -> str:
        with self._lock:
            self.heartbeats += 1
        return "deadline"

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
    ) -> dict[str, Any]:
        result = {"attempt_state": attempt_state, "payload": payload}
        with self._lock:
            self.results.append(result)
        return result


def task(request_id: str, value: Any = 1) -> ActivityTask:
    return ActivityTask(
        "workflow", f"node-{request_id}", 0, "worker", request_id, "fixture", "v1", value
    )


def test_registry_requires_versioned_unique_activity() -> None:
    registry = ActivityRegistry()
    registry.register("fixture", "v1", lambda value: value + 1)
    assert registry.resolve("fixture", "v1")(2) == 3
    with pytest.raises(ValueError):
        registry.register("fixture", "v1", lambda value: value)
    with pytest.raises(LookupError):
        registry.resolve("fixture", "v2")


def test_runner_heartbeats_and_returns_durable_result() -> None:
    control = FakeControl()
    registry = ActivityRegistry()

    def slow(value: Any) -> Any:
        time.sleep(0.03)
        return {"value": value}

    registry.register("fixture", "v1", slow)
    result = ActivityRunner(control, registry, heartbeat_interval=0.005).run_task(task("one", 7))
    assert result == {"attempt_state": "SUCCEEDED", "payload": {"value": 7}}
    assert control.heartbeats > 0
    assert len(control.results) == 1


def test_runner_bounds_concurrency() -> None:
    control = FakeControl()
    registry = ActivityRegistry()
    active = 0
    maximum = 0
    lock = threading.Lock()

    def bounded(value: Any) -> Any:
        nonlocal active, maximum
        with lock:
            active += 1
            maximum = max(maximum, active)
        time.sleep(0.02)
        with lock:
            active -= 1
        return value

    registry.register("fixture", "v1", bounded)
    tasks = [task(str(index), index) for index in range(6)]
    results = ActivityRunner(control, registry, max_workers=2, heartbeat_interval=0.1).run_many(
        tasks
    )
    assert [item["payload"] for item in results] == list(range(6))
    assert maximum == 2


def test_runner_does_not_hide_lost_claim() -> None:
    control = FakeControl()
    registry = ActivityRegistry()
    registry.register("fixture", "v1", lambda value: time.sleep(0.02) or value)

    def fail_heartbeat(*args: Any, **kwargs: Any) -> str:
        raise ControlError(409, "STALE_CLAIM", "claim is stale")

    control.heartbeat = fail_heartbeat  # type: ignore[method-assign]
    with pytest.raises(ControlError, match="STALE_CLAIM"):
        ActivityRunner(control, registry, heartbeat_interval=0.001).run_task(task("lost"))
