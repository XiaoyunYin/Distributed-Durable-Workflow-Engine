"""R314: handlers return under lock contention; persistence precedes drain."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest
import workers.__main__ as process
import workers.kafka_worker as kafka
import workers.witness as witnesses
from workers.k4_witness import CampaignWitness, read_sealed_session
from workers.lifecycle import WorkerLifecycle


@pytest.mark.parametrize("point", ["condition", "nested"])
def test_real_signal_at_lock_boundary(tmp_path: Path, point: str) -> None:
    try:
        result = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).with_name("fixtures") / "r314_signal_process.py"),
                point,
            ],
            env={
                **os.environ,
                "PYTHONPATH": os.pathsep.join(sys.path),
                "DUR053_WITNESS_DIR": str(tmp_path),
            },
            capture_output=True,
            text=True,
            timeout=5,
        )
    except subprocess.TimeoutExpired as error:
        pytest.fail(f"signal handler deadlocked on worker lock: {error.stderr!r}")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "SIGNAL_DRAIN_COMPLETE" in result.stdout


@pytest.mark.parametrize("legacy", [False, True])
def test_handler_defers_trace_and_orders_drain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, legacy: bool
) -> None:
    monkeypatch.setenv("WORKER_ADDR", "127.0.0.1:0")
    monkeypatch.setenv("WORKER_DRAIN_MODE", "legacy" if legacy else "drain")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "stub")
    monkeypatch.setenv("DUR053_K4_CAMPAIGN", "1")
    monkeypatch.setenv("DUR053_WITNESS_DIR", str(tmp_path))
    monkeypatch.setenv("RUNTIME_NAMESPACE", "test")
    monkeypatch.setenv("POD_UID", "pod")
    monkeypatch.setenv("CONTAINER_ID", "pod")
    monkeypatch.setattr(witnesses, "_GLOBAL", None)
    monkeypatch.setattr(process.HealthHandler, "lifecycle", None)
    handlers: dict[int, Any] = {}
    monkeypatch.setattr(signal, "signal", lambda n, handler: handlers.update({n: handler}))
    witness = witnesses.campaign_witness()
    assert witness is not None
    original = CampaignWitness.trace
    calls = []
    receipt_bounds = []

    def trace(self: CampaignWitness, event: str, fields: dict[str, Any], **kw: Any) -> None:
        if event == "signal":
            lifecycle = process.HealthHandler.lifecycle
            assert lifecycle is not None
            assert not lifecycle.drain.is_set(), "drain preceded durable signal trace"
            calls.append((fields["signal"], kw["at"]))
        original(self, event, fields, **kw)

    monkeypatch.setattr(CampaignWitness, "trace", trace)
    begin_drain = WorkerLifecycle.begin_drain

    def draining(self: WorkerLifecycle) -> None:
        sealed = read_sealed_session(witness.seal_path)
        signals = [
            json.loads(row)
            for row in sealed["streams"]["trace"]["data"].splitlines()
            if json.loads(row)["event"] == "signal"
        ]
        assert len(signals) == 1, "drain preceded durable signal trace"
        assert signals[0]["at"] == calls[0][1]
        begin_drain(self)

    monkeypatch.setattr(WorkerLifecycle, "begin_drain", draining)

    def send() -> None:
        # Interrupt a main-thread critical section with all handler-visible locks
        # held. A handler must return without touching even an Event condition.
        lifecycle = process.HealthHandler.lifecycle
        assert lifecycle is not None
        with witnesses._GLOBAL_LOCK, witness.lock, lifecycle.lock, lifecycle.drain._cond:
            before = time.monotonic()
            handlers[signal.SIGTERM](signal.SIGTERM, None)
            after = time.monotonic()
            receipt_bounds.extend((before, after))
            assert not lifecycle.drain.is_set()
        assert lifecycle.drain.wait(1)
        assert lifecycle.abort.is_set() == legacy
        handlers[signal.SIGINT](signal.SIGINT, None)

    if legacy:
        serve = process.ThreadingHTTPServer.serve_forever

        def serving(self: Any) -> None:
            send()
            serve(self, poll_interval=0.01)

        monkeypatch.setattr(process.ThreadingHTTPServer, "serve_forever", serving)
    else:
        monkeypatch.setattr(kafka, "start_pool", lambda *args: send() or [])
    if legacy:
        monkeypatch.setattr(kafka, "start_pool", lambda *args: [])
    process.run()
    assert len(calls) == 1
    assert calls[0][0] == signal.SIGTERM
    assert receipt_bounds[0] <= calls[0][1] <= receipt_bounds[1], "signal receipt time lost"


@pytest.mark.parametrize("legacy", [False, True])
def test_main_wait_coordinates_two_sigterms(tmp_path: Path, legacy: bool) -> None:
    _wait_process(tmp_path, "published", legacy)


@pytest.mark.skipif(sys.platform != "linux", reason="pthread-directed SIGTERM requires Linux")
@pytest.mark.parametrize("legacy", [False, True])
def test_pthread_sigterm_wakes_main_wait(tmp_path: Path, legacy: bool) -> None:
    _wait_process(tmp_path, "pthread", legacy)


def _wait_process(tmp_path: Path, mode: str, legacy: bool) -> None:
    try:
        result = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).with_name("fixtures") / "r314_wait_process.py"),
                mode,
                "legacy" if legacy else "drain",
            ],
            env={
                **os.environ,
                "PYTHONPATH": os.pathsep.join(sys.path),
                "DUR053_WITNESS_DIR": str(tmp_path),
            },
            capture_output=True,
            text=True,
            timeout=5,
        )
    except subprocess.TimeoutExpired as error:
        pytest.fail(
            f"main wait did not seal signal and begin drain within one second: {error.stderr!r}"
        )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "BOUNDED_SIGNAL_DRAIN_COMPLETE" in result.stdout


@pytest.mark.parametrize("legacy", [False, True])
def test_two_real_sigterms_one_signal_row(tmp_path: Path, legacy: bool) -> None:
    _wait_process(tmp_path, "repeated", legacy)
