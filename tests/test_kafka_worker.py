from __future__ import annotations

import base64
import threading
from types import SimpleNamespace
from typing import Any

import pytest
from workers.control import ControlClient, ControlError
from workers.kafka_worker import RetryingControl, delivery_body, handle_delivery


def message() -> SimpleNamespace:
    return SimpleNamespace(
        topic="durable-agent.tasks.v1",
        partition=2,
        offset=4,
        key=b"event",
        value=b"not-json",
        headers=[],
    )


def test_malformed_delivery_preserves_raw_evidence() -> None:
    body = delivery_body(message())
    assert body["schema_version"] == -1
    assert base64.b64decode(body["payload"]) == b"not-json"


def test_inbox_precedes_offset_and_claim(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    bodies: list[dict[str, Any]] = []

    def post(_self: ControlClient, path: str, body: dict[str, Any]) -> dict[str, Any]:
        calls.append(path.rsplit("/", 1)[-1])
        bodies.append(body)
        if path.endswith("worker-deliveries"):
            return {
                "commit_offset": True,
                "next_offset": 5,
                "task": {
                    "workflow_id": "wf",
                    "node_id": "pure.echo",
                    "iteration": 0,
                    "attempt_number": 3,
                    "activity_name": "pure.echo",
                    "activity_version": "v1",
                    "input": {"value": 8},
                },
            }
        if path.endswith("claim"):
            return {
                "attempt_number": 3,
                "claim_token": "token",
                "effect_class": "PURE_ACTIVITY",
                "heartbeat_deadline": "later",
            }
        return {"accepted": True}

    monkeypatch.setattr(ControlClient, "_post", post)
    consumer = SimpleNamespace(commit=lambda offsets: calls.append("commit"))
    handle_delivery(
        consumer, message(), RetryingControl("http://unused", threading.Event()), "worker"
    )
    assert calls == ["worker-deliveries", "commit", "claim", "result"]
    assert bodies[1]["expected_attempt"] == 3
    assert bodies[2]["payload"] == {"value": 8}


def test_no_offset_on_failed_inbox(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(_self: ControlClient, path: str, body: dict[str, Any]) -> dict[str, Any]:
        raise ControlError(400, "INVALID_REQUEST", "bad identity")

    monkeypatch.setattr(ControlClient, "_post", fail)
    calls: list[Any] = []
    with pytest.raises(ControlError):
        handle_delivery(
            SimpleNamespace(commit=calls.append),
            message(),
            RetryingControl("http://unused", threading.Event()),
            "worker",
        )
    assert calls == []


def test_uncertain_result_retries_identical_body(monkeypatch: pytest.MonkeyPatch) -> None:
    bodies: list[dict[str, Any]] = []

    def post(_self: ControlClient, path: str, body: dict[str, Any]) -> dict[str, Any]:
        bodies.append(body.copy())
        if len(bodies) == 1:
            raise ControlError(503, "CONTROL_UNAVAILABLE", "response lost")
        return {"accepted": True}

    monkeypatch.setattr(ControlClient, "_post", post)
    control = RetryingControl("http://unused", threading.Event())
    assert control.result("wf", "node", 0, 3, "token", "SUCCEEDED", {"answer": 8})
    assert len(bodies) == 2 and bodies[0] == bodies[1]
