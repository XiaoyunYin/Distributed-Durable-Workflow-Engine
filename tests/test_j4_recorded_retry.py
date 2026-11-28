from __future__ import annotations

from ipaddress import IPv4Address
from pathlib import Path
from typing import Any

import pytest
from workers import __main__ as worker
from workers import recorded_retry
from workers.recorded_activity import RESILIENCE_MODE, ProviderConfig
from workers.recorded_wire import ACTIVITY, EXPECTED_HASH, MODE, ProviderReply

FIXTURE = {"model": "recorded-llm-v1", "prompt": "Return fixture alpha."}


@pytest.mark.parametrize(
    ("activity", "resilience", "match"),
    [("true", "0", MODE), ("1", "true", RESILIENCE_MODE), ("0", "1", "requires")],
)
def test_worker_rejects_invalid_j4_modes_before_listener_start(
    monkeypatch: pytest.MonkeyPatch, activity: str, resilience: str, match: str
) -> None:
    monkeypatch.setenv(MODE, activity)
    monkeypatch.setenv(RESILIENCE_MODE, resilience)

    def listener(*_args):
        pytest.fail("worker opened a listener with an invalid J4 mode")

    monkeypatch.setattr(worker, "ThreadingHTTPServer", listener)
    with pytest.raises(ValueError, match=match):
        worker.run()


@pytest.fixture
def b2_worker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    boot_file = tmp_path / "boot"
    boot_file.write_text("boot", encoding="ascii")
    monkeypatch.setattr(recorded_retry, "BOOT_ID", boot_file)
    monkeypatch.setattr(
        ProviderConfig, "load", lambda: ProviderConfig(IPv4Address("127.0.0.1"), "B1-run1")
    )


def complete(exchange, trace):
    return recorded_retry.complete_resilient(
        FIXTURE,
        workflow_id="workflow",
        node_id=ACTIVITY,
        attempt=2,
        iteration=0,
        claim_token="private-claim",
        exchange=exchange,
        trace=trace,
    )


def test_recovered_worker_reads_schedule_and_uses_only_new_permits(
    b2_worker: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    exchanges = []
    sends = []
    waited = []
    trace = []
    scope = {"boot_id": "boot", "outcome_ns": 2_000_000_000, "eligible_ns": 4_302_000_000}
    result = {
        "completion": "fixture alpha",
        "request_sha256": EXPECTED_HASH,
        "provider_call_id": "c3",
    }

    def exchange(action: str, body: dict[str, Any]):
        assert body["boot_id"] == "boot" and body["attempt_number"] == 2
        assert body["claim_token"] == "private-claim"
        exchanges.append((action, body))
        if len(exchanges) == 1:
            assert action == "reserve" and "request_id" not in body
            return scope
        if action == "reserve":
            sequence = 2 if len(exchanges) == 2 else 3
            issued = 4_302_000_000 if sequence == 2 else 8_900_000_000
            return {
                **scope,
                "permit": {
                    "call_id": f"c{sequence}",
                    "sequence": sequence,
                    "attempt": 2,
                    "issued_ns": issued,
                    "deadline_ns": issued + 2_000_000_000,
                },
            }
        if body["call_id"] == "c2":
            assert body["kind"] == "rate_limited" and body["retry_after_seconds"] == 1
            return {**scope, "eligible_ns": 8_900_000_000}
        assert body["kind"] == "success"
        return {**scope, "terminal_state": "SUCCEEDED", "payload": result}

    def send(_config, call, _value, deadline_ns, observe):
        sends.append(call.call_id)
        observe({"event": "send", "at_ns": deadline_ns - 1_999_000_000})
        if call.ordinal == 2:
            return ProviderReply(429, deadline_ns - 1_800_000_000, retry_after="1")
        return ProviderReply(200, deadline_ns - 1_700_000_000, result)

    monkeypatch.setattr(recorded_retry, "call_provider", send)
    monkeypatch.setattr(recorded_retry, "wait_until", waited.append)
    assert complete(exchange, trace.append) == ("SUCCEEDED", result)
    assert sends == ["c2", "c3"]
    assert waited == [4_302_000_000, 8_900_000_000]
    assert [event["event"] for event in trace] == [
        "schedule_read",
        "reserved",
        "send",
        "reply",
        "outcome_committed",
        "reserved",
        "send",
        "reply",
        "outcome_committed",
    ]
    assert "private-claim" not in str(trace)


def test_terminal_schedule_replays_without_starting_another_send(
    b2_worker: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    def no_send(*_args):
        pytest.fail("durable exhaustion started another send")

    monkeypatch.setattr(recorded_retry, "call_provider", no_send)
    calls = []
    payload = {"reason": "PROVIDER_BUDGET_EXHAUSTED"}

    def exchange(action, body):
        calls.append((action, body))
        return {
            "boot_id": "boot",
            "outcome_ns": 90,
            "eligible_ns": 900,
            "terminal_state": "FAILED_FINAL",
            "payload": payload,
        }

    assert complete(exchange, lambda _: None) == ("FAILED_FINAL", payload)
    assert len(calls) == 1 and calls[0][0] == "reserve"


def test_go_worker_boot_mismatch_fails_before_send(b2_worker: None) -> None:
    with pytest.raises(RuntimeError, match="boot identity mismatch"):
        complete(lambda *_: {"boot_id": "wrong-host"}, lambda _: None)
