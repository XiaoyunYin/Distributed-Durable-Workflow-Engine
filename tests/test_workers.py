from __future__ import annotations

import logging
import threading
import time
from typing import Any

import pytest
from workers.control import Claim, ControlError
from workers.registry import ActivityRegistry, default_registry
from workers.runner import ActivityRunner, ActivityTask


class FakeControl:
    def __init__(self) -> None:
        self.heartbeats = 0
        self.claims = 0
        self.heartbeat_failures: list[ControlError] = []
        self.result_failures: list[ControlError] = []
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
        with self._lock:
            self.claims += 1
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
        if self.heartbeat_failures:
            raise self.heartbeat_failures.pop(0)
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
        if self.result_failures:
            raise self.result_failures.pop(0)
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


def test_dur050_sha256_activity_is_absent_without_campaign_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DUR050_ENABLE_SHA256_ACTIVITY", raising=False)
    monkeypatch.delenv("DUR050_SHA256_WORK_UNITS", raising=False)
    registry = default_registry()
    with pytest.raises(LookupError, match="not registered"):
        registry.resolve("dur050.sha256.seq8.0", "v1")


def test_dur050_sha256_activity_is_versioned_deterministic_and_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DUR050_ENABLE_SHA256_ACTIVITY", "1")
    monkeypatch.setenv("DUR050_SHA256_WORK_UNITS", "17")
    registry = default_registry()
    sequential = registry.resolve("dur050.sha256.seq8.0", "v1")
    branch = registry.resolve("dur050.sha256.fanout8.7", "v1")

    first = sequential({"ignored": True})
    assert first == sequential(None)
    assert first == branch({"another": "input"})
    assert first["work_units"] == 17
    assert len(first["digest"]) == 64


def test_dur050_sha256_activity_rejects_invalid_workload_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DUR050_ENABLE_SHA256_ACTIVITY", "1")
    monkeypatch.setenv("DUR050_SHA256_WORK_UNITS", "zero")
    with pytest.raises(ValueError, match="positive integer"):
        default_registry()


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

    control.heartbeat_failures = [ControlError(409, "STALE_CLAIM", "claim is stale")]
    with pytest.raises(ControlError, match="STALE_CLAIM") as raised:
        ActivityRunner(control, registry, heartbeat_interval=0.001).run_task(task("lost"))
    assert raised.value.operation == "heartbeat"
    assert len(control.results) == 1


def test_runner_attributes_stale_result_rejection_to_result_operation() -> None:
    control = FakeControl()
    control.result_failures = [ControlError(409, "STALE_ATTEMPT", "attempt is no longer current")]
    registry = ActivityRegistry()
    registry.register("fixture", "v1", lambda value: value)

    with pytest.raises(ControlError, match="STALE_ATTEMPT") as raised:
        ActivityRunner(control, registry).run_task(task("late-result"))

    assert raised.value.operation == "result"


def test_runner_logs_rejected_result_metadata_without_payload(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="workers.runner")
    control = FakeControl()
    control.result_failures = [ControlError(409, "STALE_ATTEMPT", "attempt is no longer current")]
    registry = ActivityRegistry()
    registry.register("fixture", "v1", lambda _value: {"sensitive": "do-not-log"})

    with pytest.raises(ControlError):
        ActivityRunner(control, registry).run_task(task("late-result"))

    assert "activity result submission workflow_id=workflow" in caplog.text
    assert "attempt_state=SUCCEEDED" in caplog.text
    assert "payload_sha256=" in caplog.text
    assert "outcome=rejected status=409 code=STALE_ATTEMPT retries=0" in caplog.text
    assert "do-not-log" not in caplog.text


def test_runner_submits_after_transient_heartbeat_failure() -> None:
    control = FakeControl()
    control.heartbeat_failures = [ControlError(503, "CONTROL_UNAVAILABLE", "temporary outage")]
    registry = ActivityRegistry()
    registry.register("fixture", "v1", lambda value: time.sleep(0.1) or value)
    result = ActivityRunner(control, registry, heartbeat_interval=0.005).run_task(task("transient"))
    assert result["attempt_state"] == "SUCCEEDED"
    assert control.heartbeats > 0
    assert len(control.results) == 1


def test_runner_resolves_version_before_claim() -> None:
    control = FakeControl()
    with pytest.raises(LookupError, match="not registered"):
        ActivityRunner(control, ActivityRegistry()).run_task(task("unknown"))
    assert control.claims == 0


def test_runner_does_not_convert_shutdown_into_activity_failure() -> None:
    control = FakeControl()
    registry = ActivityRegistry()

    def shutdown(_value: Any) -> Any:
        raise KeyboardInterrupt()

    registry.register("fixture", "v1", shutdown)
    with pytest.raises(KeyboardInterrupt):
        ActivityRunner(control, registry).run_task(task("shutdown"))
    assert control.results == []
