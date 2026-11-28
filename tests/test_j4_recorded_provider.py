from __future__ import annotations

import hashlib
import json
import os
import subprocess
import threading
import time
from collections.abc import Iterator
from ipaddress import IPv4Address
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from workers import recorded_activity, recorded_wire
from workers.control import Claim
from workers.recorded_activity import ProviderConfig
from workers.recorded_stub import ReceiptJournal, StubHandler, StubServer
from workers.recorded_wire import ACTIVITY, MODE, CallIdentity, canonical_request, request_once
from workers.registry import default_registry
from workers.runner import ActivityRunner, ActivityTask

from deploy.j4 import install_egress

MATCH = {"prompt": "Return fixture alpha.", "model": "recorded-llm-v1"}
WRONG = {**MATCH, "prompt": "Return fixture beta."}
HOST = IPv4Address("127.0.0.1")


def call(cell: str, ordinal: int = 1, call_id: str = "call-1") -> CallIdentity:
    return CallIdentity(cell, "workflow", ACTIVITY, 0, 1, ordinal, call_id)


def deadline_ms(milliseconds: int = 2000) -> int:
    return time.monotonic_ns() + milliseconds * 1_000_000


@pytest.fixture
def stub(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    path = tmp_path / "receipts.jsonl"
    journal = ReceiptJournal(path, "test-boot")
    with StubServer(
        ("127.0.0.1", 0), journal, {f"{s}-test": s for s in ("C0", "C3", "B0")}
    ) as server:
        monkeypatch.setattr(recorded_wire, "STUB_PORT", server.server_port)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            yield path
        finally:
            server.shutdown()
            thread.join(5)
    journal.close()


def rows(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_bytes().splitlines()]


def test_stub_matches_canonical_hash_and_fsyncs_before_response(
    stub: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = StubHandler.respond
    real_fsync = os.fsync
    synced: list[str] = []

    def mark_sync(fd: int) -> None:
        real_fsync(fd)
        synced.append(rows(stub)[-1]["event"])

    def respond(handler, status, value, retry_after=None):
        assert synced == ["receipt"]
        receipt = rows(stub)[0]
        assert receipt["request_sha256"] == hashlib.sha256(canonical_request(MATCH)).hexdigest()
        assert receipt["header_names"] == sorted(
            ["connection", "content-length", "content-type", "host"]
            + [header.lower() for header in call("C0-test").headers()]
        )
        original(handler, status, value, retry_after)

    monkeypatch.setattr(os, "fsync", mark_sync)
    monkeypatch.setattr(StubHandler, "respond", respond)
    trace: list[dict[str, Any]] = []
    reply = request_once(
        HOST, call("C0-test"), json.dumps(MATCH).encode(), deadline_ms(), trace.append
    )
    assert reply.status == 200
    assert reply.payload == {
        "completion": "fixture alpha",
        "request_sha256": "92b3bc136a6617117cfac28e4324b272b6bf6b72ab5d137a948cb9fc0d25e751",
        "provider_call_id": "call-1",
    }
    assert len(trace) == 1 and trace[0]["before_ns"] <= trace[0]["at_ns"] <= reply.observed_ns


def test_wrong_hash_cannot_return_or_advance_fixture(stub: Path) -> None:
    wrong = request_once(
        HOST, call("C0-test"), canonical_request(WRONG), deadline_ms(), lambda _: None
    )
    assert wrong.status == 422
    assert wrong.payload == {"reason": "RECORDED_REQUEST_MISMATCH"}
    assert rows(stub)[0]["request_sha256"] == (
        "7fd339f77e3935f75c39a7436f7c71dd74f4644f87c3f5968120ce9095e06522"
    )
    match = request_once(
        HOST,
        call("C0-test", call_id="call-2"),
        canonical_request(MATCH),
        deadline_ms(),
        lambda _: None,
    )
    assert match.status == 200


def test_stub_timeout_and_429_keep_independent_receipts(stub: Path) -> None:
    limited = request_once(
        HOST, call("B0-test"), canonical_request(MATCH), deadline_ms(), lambda _: None
    )
    assert (limited.status, limited.retry_after) == (429, "1")
    deadline = deadline_ms(150)
    timeout = request_once(
        HOST,
        call("B0-test", 2, "call-2"),
        canonical_request(MATCH),
        deadline,
        lambda _: None,
    )
    assert timeout.status == 0 and timeout.observed_ns >= deadline
    records = rows(stub)
    second = next(row for row in records if row.get("call_id") == "call-2")
    assert any(
        row["event"] == "withhold" and row["receipt_sequence"] == second["sequence"]
        for row in records
    )
    assert not any(
        row["event"] == "response" and row["receipt_sequence"] == second["sequence"]
        for row in records
    )


def test_early_socket_timer_keeps_reading_until_absolute_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    times = iter([0, 1_000_000, 1_500_000])
    sizes = iter([0, 1])
    waits = []

    def recv_into(buffer):
        if not next(sizes):
            raise TimeoutError("early socket timer")
        buffer[0] = 1
        return 1

    clock = SimpleNamespace(monotonic_ns=lambda: next(times))
    transport = SimpleNamespace(settimeout=waits.append, recv_into=recv_into)
    monkeypatch.setattr(recorded_wire, "time", clock)
    reader = recorded_wire.DeadlineReader(transport, 2_000_000)
    buffer = bytearray(1)
    assert reader.readinto(buffer) == 1
    assert buffer == b"\x01"
    assert waits == [0.002, 0.0005]


def test_invalid_body_with_duplicate_keys_does_not_return_fixture(stub: Path) -> None:
    with pytest.raises(ValueError, match="status 400"):
        request_once(
            HOST,
            call("C0-test"),
            b'{"model":"x","model":"recorded-llm-v1","prompt":"Return fixture alpha."}',
            deadline_ms(),
            lambda _: None,
        )
    assert not rows(stub)[0]["matched"]


def test_journal_reopen_retains_receipts_and_rejects_reused_ids(tmp_path: Path) -> None:
    path = tmp_path / "journal.jsonl"
    journal = ReceiptJournal(path, "test-boot")
    identity = call("C0-test")
    digest = hashlib.sha256(canonical_request(MATCH)).hexdigest()
    assert journal.receipt(identity, digest, "success", 1, []) == (1, "success")
    journal.close()
    previous = path.read_bytes()
    journal = ReceiptJournal(path, "test-boot")
    try:
        assert path.read_bytes() == previous
        assert journal.receipt(identity, digest, "success", 2, []) == (2, "duplicate_id")
        assert journal.receipt(call("C0-test", call_id="other"), digest, "success", 3, []) == (
            3,
            "duplicate_ordinal",
        )
    finally:
        journal.close()
    assert path.read_bytes().startswith(previous)
    with pytest.raises(ValueError, match="boot identity changed"):
        ReceiptJournal(path, "different-boot")


class Control:
    lease: int

    def claim(self, *args):
        self.lease = args[-1]
        return Claim(1, "control-only-token", "PURE_ACTIVITY", "deadline")

    def result(self, *args):
        return {"state": args[-2], "payload": args[-1]}


@pytest.mark.parametrize(
    ("arm", "value", "expected"), [("C0", MATCH, "SUCCEEDED"), ("C3", WRONG, "FAILED_FINAL")]
)
def test_core_runner_reports_one_result_through_claim_path(
    stub: Path, monkeypatch: pytest.MonkeyPatch, arm: str, value: dict, expected: str
) -> None:
    monkeypatch.setenv(MODE, "1")
    monkeypatch.setattr(ProviderConfig, "load", lambda: ProviderConfig(HOST, f"{arm}-test"))
    sent: list[CallIdentity] = []

    def portable_launch(config, call_id, data, deadline, trace):
        sent.append(call_id)
        return request_once(config.stub_ipv4, call_id, canonical_request(data), deadline, trace)

    monkeypatch.setattr(recorded_activity, "call_provider", portable_launch)
    hooks: list[tuple[str, dict[str, str]]] = []
    control = Control()
    runner = ActivityRunner(
        control, default_registry(), deployment_hook=lambda name, data: hooks.append((name, data))
    )
    receipt = runner.run_task(
        ActivityTask("workflow", ACTIVITY, 0, "w", "r", ACTIVITY, "v1", value)
    )
    assert control.lease == 12_000
    assert receipt["state"] == expected
    assert len(sent) == 1
    assert sum(row["event"] == "receipt" for row in rows(stub)) == 1
    if expected == "SUCCEEDED":
        assert receipt["payload"]["provider_call_id"] == sent[0].call_id
    else:
        assert receipt["payload"] == {"reason": "RECORDED_REQUEST_MISMATCH"}
    assert hooks[-1][0] == "j4_result_receipt"
    assert "control-only-token" not in str(hooks)


def test_unset_registry_and_existing_workflow_equal_legacy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(MODE, raising=False)
    registry = default_registry()
    assert registry.manifest() == [
        {"name": "pure.add", "version": "v1"},
        {"name": "pure.echo", "version": "v1"},
    ]
    with pytest.raises(LookupError):
        registry.resolve(ACTIVITY, "v1")
    control = Control()
    runner = ActivityRunner(control, registry)
    result = runner.run_task(
        ActivityTask("workflow", "pure.echo", 0, "w", "r", "pure.echo", "v1", 2)
    )
    assert result == {"state": "SUCCEEDED", "payload": 2}
    assert control.lease == 60_000
    monkeypatch.setenv(MODE, "0")
    assert default_registry().manifest() == registry.manifest()
    for invalid in ("yes", ""):
        monkeypatch.setenv(MODE, invalid)
        with pytest.raises(ValueError, match=MODE):
            default_registry()


def test_policy_init_publishes_only_after_successful_install(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    address = IPv4Address("192.168.3.10")
    proof = tmp_path / "installed.json"
    calls = []

    def run(command, **kwargs):
        assert not proof.exists()
        assert kwargs["check"]
        calls.append(command)
        if command[-1] == "-":
            rules = kwargs["input"]
            assert "table inet j4_provider" in rules
            assert [
                line.strip() for line in rules.splitlines() if line.strip().startswith("meta")
            ] == [
                "meta skuid 65533 ip daddr 192.168.3.10 tcp dport 8082 counter accept",
                "meta skuid 65533 counter drop",
            ]
            assert "ip daddr 192.168.3.10 tcp dport 8082 counter drop" in rules
        return subprocess.CompletedProcess(command, 0, '{"nftables":[{"verified":"test"}]}')

    monkeypatch.setattr(subprocess, "run", run)
    install_egress.install(address, proof)
    assert calls == [
        ["nft", "--check", "-f", "-"],
        ["nft", "-f", "-"],
        ["nft", "--json", "list", "table", "inet", "j4_provider"],
    ]
    assert json.loads(proof.read_bytes())["stub_ipv4"] == str(address)
    with pytest.raises(RuntimeError, match="already exists"):
        install_egress.install(address, proof)


def test_policy_install_failure_never_admits_worker(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    proof = tmp_path / "installed.json"

    def reject(command, **_):
        raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(subprocess, "run", reject)
    with pytest.raises(subprocess.CalledProcessError):
        install_egress.install(IPv4Address("192.168.3.10"), proof)
    assert not proof.exists()
    with pytest.raises(ValueError):
        install_egress.rules(HOST)
