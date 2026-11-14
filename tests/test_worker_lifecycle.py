from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest
from kafka.structs import TopicPartition
from workers.__main__ import HealthHandler
from workers.control import Claim, ControlClient, ControlError
from workers.kafka_worker import (
    Assignment,
    RetryingControl,
    consume_slot,
    handle_delivery,
    start_pool,
)
from workers.lifecycle import BoundedExecutor, WorkerLifecycle, worker_identity
from workers.registry import ActivityRegistry
from workers.runner import ActivityRunner, ActivityTask


def delivery() -> SimpleNamespace:
    return SimpleNamespace(
        topic="tasks", partition=0, offset=4, key=b"event", value=b"raw", headers=[]
    )


def disposition() -> dict[str, Any]:
    return {
        "commit_offset": True,
        "next_offset": 5,
        "task": {
            "workflow_id": "wf",
            "node_id": "echo",
            "iteration": 0,
            "attempt_number": 1,
            "activity_name": "pure.echo",
            "activity_version": "v1",
            "input": {"value": 8},
        },
    }


def post(calls: list[str]) -> Any:
    def respond(_self: ControlClient, path: str, body: dict[str, Any]) -> dict[str, Any]:
        action = path.rsplit("/", 1)[-1]
        calls.append(action)
        if action == "worker-deliveries":
            return disposition()
        if action == "claim":
            return {
                "attempt_number": 1,
                "claim_token": "token",
                "effect_class": "PURE_ACTIVITY",
                "heartbeat_deadline": "later",
            }
        return {"accepted": True}

    return respond


def test_drain_before_delivery_leaves_offset_uncommitted(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(ControlClient, "_post", post(calls))
    lifecycle = WorkerLifecycle()
    lifecycle.begin_drain()
    handle_delivery(
        SimpleNamespace(commit=lambda _: calls.append("commit")),
        delivery(),
        RetryingControl("http://unused", lifecycle.abort),
        "worker",
        lifecycle,
    )
    assert calls == [], "drain posted a new delivery"


@pytest.mark.parametrize("boundary", ["disposition", "commit"])
def test_shutdown_between_disposition_commit_claim(
    monkeypatch: pytest.MonkeyPatch, boundary: str
) -> None:
    calls: list[str] = []
    lifecycle = WorkerLifecycle()
    respond = post(calls)

    def intercept(client: ControlClient, path: str, body: dict[str, Any]) -> dict[str, Any]:
        result = respond(client, path, body)
        if boundary == "disposition" and path.endswith("worker-deliveries"):
            lifecycle.begin_drain()
        return result

    def commit(_: Any) -> None:
        calls.append("commit")
        if boundary == "commit":
            lifecycle.begin_drain()

    monkeypatch.setattr(ControlClient, "_post", intercept)
    handle_delivery(
        SimpleNamespace(commit=commit),
        delivery(),
        RetryingControl("http://unused", lifecycle.abort),
        "worker",
        lifecycle,
    )
    assert calls == ["worker-deliveries", "commit"], "drain admitted a claim after disposition"


def test_delivery_response_loss_retries_same_disposition(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    bodies: list[dict[str, Any]] = []
    respond = post(calls)

    def intercept(client: ControlClient, path: str, body: dict[str, Any]) -> dict[str, Any]:
        if path.endswith("worker-deliveries"):
            bodies.append(body.copy())
            if len(bodies) == 1:
                calls.append("disposition-committed-response-lost")
                raise ControlError(503, "CONTROL_UNAVAILABLE", "lost response after commit")
        return respond(client, path, body)

    monkeypatch.setattr(ControlClient, "_post", intercept)
    handle_delivery(
        SimpleNamespace(commit=lambda _: calls.append("commit")),
        delivery(),
        RetryingControl("http://unused", threading.Event()),
        "worker",
        WorkerLifecycle(),
    )
    assert len(bodies) == 2 and bodies[0] == bodies[1]
    assert calls == [
        "disposition-committed-response-lost",
        "worker-deliveries",
        "commit",
        "claim",
        "result",
    ]


def test_commit_failure_does_not_claim(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(ControlClient, "_post", post(calls))

    def commit(_: Any) -> None:
        raise RuntimeError("commit response lost")

    with pytest.raises(RuntimeError, match="commit response lost"):
        handle_delivery(
            SimpleNamespace(commit=commit),
            delivery(),
            RetryingControl("http://unused", threading.Event()),
            "worker",
            WorkerLifecycle(),
        )
    assert calls == ["worker-deliveries"]


def test_revoke_during_disposition_fences_offset_and_claim(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    lifecycle = WorkerLifecycle()
    assignment = Assignment(lifecycle)
    partition = TopicPartition("tasks", 0)
    assignment.on_partitions_assigned([partition])
    respond = post(calls)

    def intercept(client: ControlClient, path: str, body: dict[str, Any]) -> dict[str, Any]:
        result = respond(client, path, body)
        assignment.on_partitions_revoked([partition])
        assignment.on_partitions_assigned([partition])
        return result

    monkeypatch.setattr(ControlClient, "_post", intercept)
    handle_delivery(
        SimpleNamespace(commit=lambda _: calls.append("commit")),
        delivery(),
        RetryingControl("http://unused", threading.Event()),
        "worker",
        lifecycle,
        assignment=assignment,
    )
    assert calls == ["worker-deliveries"], "revoked generation committed or claimed"
    assert lifecycle.rebalances == 1


def task() -> ActivityTask:
    return ActivityTask("wf", "echo", 0, "worker", "request", "test", "v1", {})


class Control:
    def __init__(self, lifecycle: WorkerLifecycle) -> None:
        self.lifecycle = lifecycle
        self.calls: list[str] = []

    def aborted(self) -> bool:
        return self.lifecycle.aborted()

    def claim(self, *args: Any) -> Claim:
        self.calls.append("claim")
        return Claim(1, "token", "PURE_ACTIVITY", "later")

    def heartbeat(self, *args: Any) -> str:
        self.calls.append("heartbeat")
        return "later"

    def result(self, *args: Any) -> dict[str, Any]:
        self.calls.append("result")
        return {"accepted": True}


def test_result_accepted_during_drain_before_deadline() -> None:
    lifecycle = WorkerLifecycle()
    control = Control(lifecycle)
    registry = ActivityRegistry()

    def activity(_: Any) -> dict[str, bool]:
        lifecycle.begin_drain()
        time.sleep(0.025)
        return {"finished": True}

    registry.register("test", "v1", activity)
    assert ActivityRunner(control, registry, heartbeat_interval=0.005).run_task(task()) == {
        "accepted": True
    }
    assert "heartbeat" in control.calls and control.calls[-1] == "result"
    assert not lifecycle.aborted()


def test_abort_reports_nothing_and_stops_heartbeats() -> None:
    lifecycle = WorkerLifecycle()
    control = Control(lifecycle)
    registry = ActivityRegistry()

    def activity(_: Any) -> dict[str, bool]:
        lifecycle.abort.set()
        time.sleep(0.03)
        return {"finished": True}

    registry.register("test", "v1", activity)
    with pytest.raises(ControlError, match="SHUTTING_DOWN"):
        ActivityRunner(control, registry, heartbeat_interval=0.005).run_task(task())
    assert control.calls == ["claim"], "abort emitted heartbeat or result"


def test_retry_control_abort_and_admission_guards(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(ControlClient, "_post", post(calls))
    lifecycle = WorkerLifecycle()
    control = RetryingControl("http://unused", lifecycle.abort, lifecycle)
    lifecycle.begin_drain()
    for action in ["worker-deliveries", "claim"]:
        with pytest.raises(ControlError, match="SHUTTING_DOWN"):
            control._post("/v1/" + action, {})
    lifecycle.abort.set()
    with pytest.raises(ControlError, match="SHUTTING_DOWN"):
        control._post("/v1/result", {})
    assert calls == [], "abort/drain control guard admitted a request"


def test_monotonic_deadline_never_extended(monkeypatch: pytest.MonkeyPatch) -> None:
    now = [100.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    lifecycle = WorkerLifecycle(0.1)
    lifecycle.begin_drain()
    deadline = lifecycle.deadline
    now[0] = 100.09
    lifecycle.begin_drain()
    assert lifecycle.deadline == deadline, "repeated drain extended deadline"
    control = RetryingControl("http://unused", lifecycle.abort, lifecycle)
    assert control.request_timeout() < 0.011, "HTTP timeout exceeds drain deadline"
    now[0] = 100.11
    assert lifecycle.aborted(), "monotonic deadline did not abort"


def test_bounded_executor_has_no_queue() -> None:
    lifecycle = WorkerLifecycle()
    executor = BoundedExecutor(lifecycle, 1)
    release = threading.Event()
    done = executor.submit(lambda: release.wait(2))
    try:
        with pytest.raises(RuntimeError, match="executor is full"):
            executor.submit(lambda: None)
        assert lifecycle.in_flight == 1
    finally:
        release.set()
        assert done.wait(1)
    assert lifecycle.in_flight == 0


def test_abort_exits_process_without_joining_activity() -> None:
    code = """
import threading
from workers.lifecycle import WorkerLifecycle, BoundedExecutor
from workers.__main__ import finish_drain
lifecycle = WorkerLifecycle(0.05)
BoundedExecutor(lifecycle, 1).submit(lambda: threading.Event().wait())
lifecycle.begin_drain()
finish_drain(lifecycle, [])
raise RuntimeError('abort returned instead of exiting')
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=3,
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
    )
    assert result.returncode == 0, f"abort failed process exit: {result.stderr}"


def test_worker_identity_metrics_and_readiness(monkeypatch: pytest.MonkeyPatch) -> None:
    from http.server import ThreadingHTTPServer
    from urllib.error import HTTPError
    from urllib.request import urlopen

    monkeypatch.setenv("POD_NAME", "pod-unique")
    monkeypatch.setenv("WORKER_ID", "compose-worker")
    assert worker_identity() == "pod-unique"
    lifecycle = WorkerLifecycle()
    monkeypatch.setattr(HealthHandler, "lifecycle", lifecycle)
    server = ThreadingHTTPServer(("127.0.0.1", 0), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        with urlopen(base + "/readyz", timeout=1) as response:
            assert response.status == 200
        lifecycle.begin_drain()
        with pytest.raises(HTTPError) as error:
            urlopen(base + "/readyz", timeout=1)
        assert error.value.code == 503, "draining worker remained ready"
        with urlopen(base + "/healthz", timeout=1) as response:
            assert response.status == 200
        with urlopen(base + "/metrics", timeout=1) as response:
            metrics = response.read().decode()
        assert "durable_worker_draining 1" in metrics
        assert "durable_worker_in_flight 0" in metrics
        assert "durable_worker_rebalances_total 0" in metrics
    finally:
        server.shutdown()
        thread.join(1)
        server.server_close()


def test_consumer_keeps_polling_rebalances_and_closes_before_activity_finishes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import workers.kafka_worker as worker

    lifecycle = WorkerLifecycle()
    calls: list[tuple[str, int]] = []
    activity_entered = threading.Event()
    activity_release = threading.Event()
    rebalanced = threading.Event()
    partition = TopicPartition("tasks", 0)
    polls = [0]

    class Consumer:
        def record(self, name: str) -> None:
            calls.append((name, threading.get_ident()))

        def subscribe(self, _: Any, listener: Any) -> None:
            self.record("subscribe")
            self.listener = listener
            listener.on_partitions_assigned([partition])

        def assignment(self) -> set[Any]:
            self.record("assignment")
            return {partition}

        def pause(self, *args: Any) -> None:
            self.record("pause")

        def resume(self, *args: Any) -> None:
            self.record("resume")

        def seek(self, *args: Any) -> None:
            self.record("seek")

        def commit(self, _: Any) -> None:
            self.record("commit")

        def poll(self, **kwargs: Any) -> dict[Any, Any]:
            self.record("poll")
            polls[0] += 1
            if polls[0] == 1:
                return {partition: [delivery()]}
            assert activity_entered.wait(1)
            self.listener.on_partitions_revoked([partition])
            self.listener.on_partitions_assigned([partition])
            rebalanced.set()
            lifecycle.begin_drain()
            return {partition: [delivery()]}

        def close(self, **kwargs: Any) -> None:
            self.record("close")
            assert not activity_release.is_set(), "consumer close waited for long activity"

    monkeypatch.setattr(worker, "KafkaConsumer", lambda **kwargs: Consumer())
    monkeypatch.setattr(ControlClient, "_post", post([]))

    def run_task(_self: ActivityRunner, _task: ActivityTask) -> dict[str, Any]:
        activity_entered.set()
        assert activity_release.wait(2)
        return {}

    monkeypatch.setattr(ActivityRunner, "run_task", run_task)
    executor = BoundedExecutor(lifecycle, 1)
    thread = threading.Thread(
        target=consume_slot, args=("unused", "http://unused", "worker", lifecycle, executor)
    )
    thread.start()
    try:
        assert rebalanced.wait(1), "long activity blocked rebalance poll"
        thread.join(1)
        assert not thread.is_alive()
        assert len({identity for _, identity in calls}) == 1, "consumer calls crossed threads"
        assert [name for name, _ in calls].count("poll") == 2
        assert "seek" in [name for name, _ in calls], "busy rebalance lost fetched position"
        assert lifecycle.in_flight == 1
    finally:
        activity_release.set()
        thread.join(2)


def test_legacy_flag_uses_pre_k1_consumer_and_no_result_after_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import workers.kafka_worker as worker

    lifecycle = WorkerLifecycle(legacy=True)
    selected: list[str] = []
    monkeypatch.setattr(worker, "legacy_consume_slot", lambda *args: selected.append("pre-k1"))
    for thread in start_pool("unused", "http://unused", "worker", lifecycle.drain, 1, lifecycle):
        thread.join(1)
    assert selected == ["pre-k1"], "legacy flag selected new executor lifecycle"
    calls: list[str] = []
    respond = post(calls)

    def intercept(client: ControlClient, path: str, body: dict[str, Any]) -> dict[str, Any]:
        response = respond(client, path, body)
        if path.endswith("claim"):
            lifecycle.begin_drain()
        return response

    monkeypatch.setattr(ControlClient, "_post", intercept)
    with pytest.raises(ControlError, match="SHUTTING_DOWN"):
        handle_delivery(
            SimpleNamespace(commit=lambda _: calls.append("commit")),
            delivery(),
            RetryingControl("http://unused", lifecycle.drain),
            "worker",
        )
    # cf10bef: activity returns after stop.set(); _post's while-not-stop skips result.
    assert calls == ["worker-deliveries", "commit", "claim"]
    assert lifecycle.abort.is_set()


def test_legacy_trace_matches_frozen_pre_k1_source(monkeypatch: pytest.MonkeyPatch) -> None:
    source = Path(__file__).with_name("fixtures") / "k1-pre-kafka-worker.txt"
    reference = ModuleType("pre_k1_worker")
    exec(compile(source.read_bytes(), str(source), "exec"), reference.__dict__)

    def trace(module: Any) -> list[str]:
        stop = threading.Event()
        calls: list[str] = []
        respond = post(calls)

        def intercept(client: ControlClient, path: str, body: dict[str, Any]) -> dict[str, Any]:
            response = respond(client, path, body)
            if path.endswith("claim"):
                stop.set()
            return response

        monkeypatch.setattr(ControlClient, "_post", intercept)
        with pytest.raises(ControlError, match="SHUTTING_DOWN"):
            module.handle_delivery(
                SimpleNamespace(commit=lambda _: calls.append("commit")),
                delivery(),
                module.RetryingControl("http://unused", stop),
                "worker",
            )
        return calls

    traces = [
        trace(reference),
        trace(__import__("workers.kafka_worker", fromlist=["handle_delivery"])),
    ]
    assert traces[0] == traces[1] == ["worker-deliveries", "commit", "claim"]


def test_retries_bounded_during_control_outage(monkeypatch: pytest.MonkeyPatch) -> None:
    lifecycle = WorkerLifecycle()
    control = RetryingControl("http://unused", lifecycle.abort, lifecycle)
    now = [100.0]
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(time, "monotonic", lambda: now[0])

    def unavailable(_client: ControlClient, path: str, body: dict[str, Any]) -> dict[str, Any]:
        calls.append(body.copy())
        now[0] += 31.0
        if len(calls) > 1:
            return {"accepted": True}
        raise ControlError(503, "CONTROL_UNAVAILABLE", "database down")

    monkeypatch.setattr(ControlClient, "_post", unavailable)
    monkeypatch.setattr(control.stop, "wait", lambda _: False)
    with pytest.raises(ControlError, match="CONTROL_UNAVAILABLE"):
        control.result("wf", "node", 0, 1, "token", "SUCCEEDED", {})
    assert len(calls) == 1, "retry exceeded operation deadline"


@pytest.mark.parametrize("seconds", [0, -1, 31, float("nan")])
def test_drain_budget_rejects_invalid_values(seconds: float) -> None:
    with pytest.raises(ValueError, match="drain deadline"):
        WorkerLifecycle(seconds)


def test_signal_handler_registered_before_consumers(monkeypatch: pytest.MonkeyPatch) -> None:
    import signal
    from http.server import ThreadingHTTPServer

    import workers.__main__ as process
    import workers.kafka_worker as worker

    monkeypatch.setattr(HealthHandler, "lifecycle", None)
    handlers: dict[Any, Any] = {}
    servers: list[ThreadingHTTPServer] = []
    monkeypatch.setenv("WORKER_ADDR", "127.0.0.1:0")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "unused")
    monkeypatch.setenv("WORKER_DRAIN_MODE", "drain")
    monkeypatch.setattr(
        signal, "signal", lambda signum, handler: handlers.setdefault(signum, handler)
    )

    def create_server(*args: Any) -> ThreadingHTTPServer:
        server = ThreadingHTTPServer(*args)
        servers.append(server)
        return server

    def start(*args: Any) -> list[threading.Thread]:
        assert signal.SIGTERM in handlers, "SIGTERM handler missing before consumer startup"
        handlers[signal.SIGTERM](signal.SIGTERM, None)
        assert args[-1].drain.wait(1), "signal coordinator did not begin drain"
        return []

    monkeypatch.setattr(process, "ThreadingHTTPServer", create_server)
    monkeypatch.setattr(worker, "start_pool", start)
    try:
        process.run()
    finally:
        for server in servers:
            server.server_close()


def test_abort_watchdog_exits_while_startup_hook_blocks() -> None:
    code = """
import os, signal, threading
from faults.client import FailpointClient
from workers.__main__ import run
class HeldHook:
    def hit(self, *args):
        signal.raise_signal(signal.SIGTERM)
        threading.Event().wait()
    def emit(self, *args):
        pass
    def close(self):
        pass
FailpointClient.from_environment = classmethod(lambda cls: HeldHook())
os.environ['WORKER_ADDR'] = '127.0.0.1:0'
os.environ['WORKER_DRAIN_SECONDS'] = '0.05'
os.environ['WORKER_DRAIN_MODE'] = 'drain'
os.environ.pop('KAFKA_BOOTSTRAP_SERVERS', None)
run()
raise RuntimeError('held startup hook returned')
"""
    try:
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            timeout=3,
            env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
        )
    except subprocess.TimeoutExpired:
        pytest.fail("startup hook hid worker abort deadline")
    assert result.returncode == 0, f"startup abort failed: {result.stderr}"
