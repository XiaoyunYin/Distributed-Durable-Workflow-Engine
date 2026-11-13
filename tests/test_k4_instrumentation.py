from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from kafka.structs import TopicPartition
from test_workers import FakeControl, task
from workers import kafka_worker, witness
from workers.control import ControlClient, ControlError
from workers.k4_witness import CampaignWitness, read_sealed_session
from workers.lifecycle import BoundedExecutor, WorkerLifecycle
from workers.registry import ActivityRegistry
from workers.runner import ActivityRunner


@pytest.fixture
def campaign(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> CampaignWitness:
    monkeypatch.setenv("DUR053_K4_CAMPAIGN", "1")
    monkeypatch.setenv("DUR053_WITNESS_DIR", str(tmp_path))
    monkeypatch.setenv("POD_UID", "pod")
    monkeypatch.setenv("CONTAINER_ID", "container")
    monkeypatch.setenv("RUNTIME_NAMESPACE", "campaign")
    monkeypatch.setattr(witness, "_GLOBAL", None)
    result = witness.campaign_witness()
    assert result is not None
    return result


def traces(campaign: CampaignWitness) -> list[dict[str, Any]]:
    return [json.loads(line) for line in campaign.trace_path.read_text().splitlines()]


def test_k4_worker_records_global_hold_expiry(campaign, tmp_path, monkeypatch):
    hold = tmp_path / "worker.hold"
    hold.touch()
    os.utime(hold, (1, 1))
    monkeypatch.setenv("DUR053_K4_HOLD_FILE", str(hold))

    def unexpected_wait(_):
        raise AssertionError("worker waited after hold bound")

    monkeypatch.setattr("workers.k4_witness.time.sleep", unexpected_wait)
    registry = ActivityRegistry()
    registry.register("fixture", "v1", lambda x: x + 1)
    assert ActivityRunner(FakeControl(), registry).run_task(task("expiry"))["payload"] == 2
    assert any(row["event"] == "hold_bound_expired" for row in traces(campaign)), (
        "worker hold expiry was silent"
    )


def test_k4_hashes_and_dead_session_seals(
    campaign: CampaignWitness, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = ActivityRegistry()
    registry.register("fixture", "v1", lambda x: x + 1)
    assert ActivityRunner(FakeControl(), registry).run_task(task("one"))["payload"] == 2
    rows = [json.loads(line) for line in campaign.path.read_text().splitlines()]
    assert [row["event"] for row in rows] == ["start", "finish"]
    identity = rows[0]["identity"]
    assert identity["token_hash"] == hashlib.sha256(b"token-one").hexdigest(), (
        "K4 token hash missing"
    )
    assert set(identity) == {"namespace", "workflow", "node", "iteration", "attempt", "token_hash"}
    for path in campaign.directory.iterdir():
        assert b"token-one" not in path.read_bytes(), "K4 raw claim token persisted"
    assert campaign.seal_path.exists(), "K4 durable session inventory missing"
    sealed = read_sealed_session(campaign.seal_path)
    assert sealed["inventory"]["acknowledged"] == [rows[0]["id"]], (
        "K4 acknowledged start not sealed"
    )
    # New container under the same UID cannot overwrite the previous session.
    new = CampaignWitness(str(campaign.directory), "pod", "pod", "replacement-container")
    assert new.session != campaign.session
    assert len(list(campaign.directory.glob("*.inventory.json"))) == 2
    monkeypatch.setattr(witness, "_GLOBAL", None)
    assert read_sealed_session(campaign.seal_path) == sealed, "K4 dead session needs live worker"
    # An interrupted next append is retained separately from acknowledged data.
    with campaign.path.open("ab") as handle:
        handle.write(b"unacknowledged partial row")
    recovered = read_sealed_session(campaign.seal_path)
    assert recovered["streams"]["invocations"]["data"] == sealed["streams"]["invocations"]["data"]
    assert recovered["streams"]["invocations"]["unacknowledged_tail"]
    campaign.path.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="differs"):
        read_sealed_session(campaign.seal_path)


@pytest.mark.parametrize("failure", ["session", "start", "finish", "seal", "trace"])
def test_k4_persistence_failure_exits_before_further_work(tmp_path: Path, failure: str) -> None:
    # Use a subprocess: an exception on a daemon thread would not stop workers.
    code = r"""
import os, threading
from pathlib import Path
from workers.k4_witness import CampaignWitness
from workers import witness
from workers.runner import ActivityRunner
from workers.registry import ActivityRegistry
from workers.control import Claim
root = Path(os.environ["DUR053_WITNESS_DIR"])
failure = os.environ["FAILURE"]
class Control:
    def claim(self, *args): return Claim(1, "secret", "PURE_ACTIVITY", "deadline")
    def heartbeat(self, *args): return "deadline"
    def result(self, *args):
        (root / "result-ran").write_text("bad")
        return {}
original = Path.open
def broken(path, *args, **kwargs):
    if failure == "seal" and path.suffix == ".pending": raise OSError("seal failure")
    if path.suffix == ".jsonl" and not path.name.endswith("trace.jsonl"):
        if failure == "start" or (failure == "finish" and (root / "work-ran").exists()):
            raise OSError("ledger failure")
    return original(path, *args, **kwargs)
if failure == "session":
    CampaignWitness(str(root / "absent"), "pod", "uid", "cid")
    raise SystemExit(9)
external = CampaignWitness(str(root), "pod", "uid", "cid")
witness._GLOBAL = external
Path.open = broken
if failure == "trace":
    external.trace_path.unlink()
    external.trace_path.mkdir()
    thread = threading.Thread(target=lambda: external.trace("http", {}))
    thread.start(); thread.join()
    raise SystemExit(9)
def activity(value):
    (root / "work-ran").write_text("yes")
    return value
registry = ActivityRegistry(); registry.register("fixture", "v1", activity)
from workers.runner import ActivityTask
task = ActivityTask("wf", "node", 0, "worker", "request", "fixture", "v1", {})
ActivityRunner(Control(), registry).run_task(task)
raise SystemExit(9)
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=10,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(sys.path),
            "DUR053_K4_CAMPAIGN": "1",
            "DUR053_WITNESS_DIR": str(tmp_path),
            "RUNTIME_NAMESPACE": "campaign",
            "FAILURE": failure,
        },
    )
    assert result.returncode == 70, f"K4 witness failed open: {result.returncode} {result.stderr}"
    assert not (tmp_path / "result-ran").exists(), "K4 failed witness sent result"
    assert (tmp_path / "work-ran").exists() == (failure == "finish"), (
        "K4 unpersisted start ran workload"
    )


def test_k4_retry_trace_identity_body_status(
    campaign: CampaignWitness, monkeypatch: pytest.MonkeyPatch
) -> None:
    requests: list[Any] = []

    def urlopen(request: Any, **_kwargs: Any) -> Any:
        requests.append(request)
        if len(requests) == 1:
            raise TimeoutError("uncertain")
        return SimpleNamespace()  # replaced below with context manager

    class Response:
        status = 200

        def __enter__(self) -> Response:
            return self

        def __exit__(self, *_args: Any) -> None:
            pass

        def read(self) -> bytes:
            return b'{"accepted":true}'

    def respond(request: Any, **kwargs: Any) -> Any:
        urlopen(request, **kwargs)
        return Response()

    monkeypatch.setattr("urllib.request.urlopen", respond)
    control = kafka_worker.RetryingControl("http://unused", threading.Event())
    assert control._post("/v1/result", {"claim_token": "never-persist-this", "payload": 1})[
        "accepted"
    ]
    rows = [row for row in traces(campaign) if row["event"] == "http"]
    assert len(rows) == 2, "K4 control attempt trace missing"
    starts = [row for row in traces(campaign) if row["event"] == "http_start"]
    assert len(starts) == 2 and all(row["error"] == "pending" for row in starts), (
        "K4 in-flight HTTP request missing"
    )
    assert rows[0]["request"] == rows[1]["request"], "K4 retry request identity changed"
    assert [row["attempt"] for row in rows] == [1, 2]
    assert [row["status"] for row in rows] == [503, 200]
    assert (
        rows[0]["body_hash"] == rows[1]["body_hash"] == hashlib.sha256(requests[0].data).hexdigest()
    ), "K4 retry body hash changed"
    assert requests[0].data == requests[1].data
    assert requests[0].get_header("X-dur053-request-id") == requests[1].get_header(
        "X-dur053-request-id"
    )
    assert "never-persist-this" not in campaign.trace_path.read_text(), (
        "K4 control trace leaked token"
    )

    # Non-retrying direct clients and terminal rejection use the same trace path.
    def reject(*_args: Any, **_kwargs: Any) -> Any:
        raise ControlError(409, "STALE_CLAIM", "secret")

    monkeypatch.setattr(ControlClient, "_post_once", reject)
    with pytest.raises(ControlError):
        ControlClient("http://unused")._post("/v1/claim", {})
    assert traces(campaign)[-1]["status"] == 409
    assert traces(campaign)[-1]["error"] == "STALE_CLAIM"


@pytest.mark.parametrize("legacy", [True, False])
def test_k4_callbacks_both_paths_and_disposition(
    campaign: CampaignWitness, monkeypatch: pytest.MonkeyPatch, legacy: bool
) -> None:
    lifecycle = WorkerLifecycle(legacy=legacy)
    stop = lifecycle.drain
    calls: list[str] = []
    partition = TopicPartition("durable-agent.tasks.v1", 2)

    class Consumer:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            self._coordinator = SimpleNamespace(
                _generation=SimpleNamespace(generation_id=17), reset_generation=self.reset
            )

        def reset(self, *_args: Any, **_kwargs: Any) -> str:
            calls.append("reset")
            self._coordinator._generation = SimpleNamespace(generation_id=-1)
            return "original-return"

        def subscribe(self, _topics: Any, listener: Any) -> None:
            calls.append("subscribe")
            listener.on_partitions_assigned([partition])
            assert self._coordinator.reset_generation() == "original-return"
            listener.on_partitions_revoked([partition])
            self._coordinator._generation = SimpleNamespace(generation_id=18)
            listener.on_partitions_assigned([partition])

        def assignment(self) -> set[Any]:
            return {partition}

        def resume(self, *_args: Any) -> None:
            calls.append("resume")

        def poll(self, **_kwargs: Any) -> dict[Any, Any]:
            calls.append("poll")
            stop.set()
            return {}

        def close(self, **_kwargs: Any) -> None:
            calls.append("close")

    monkeypatch.setattr(kafka_worker, "KafkaConsumer", Consumer)
    if legacy:
        kafka_worker.legacy_consume_slot("broker", "http://unused", "consumer", stop)
    else:
        kafka_worker.consume_slot(
            "broker", "http://unused", "consumer", lifecycle, BoundedExecutor(lifecycle, 1)
        )
    rows = [row for row in traces(campaign) if row["event"] in {"assigned", "revoke", "lost"}]
    assert [row["event"] for row in rows] == ["assigned", "lost", "revoke", "assigned"], (
        "K4 legacy callback trace missing" if legacy else "K4 executor callback trace missing"
    )
    assert [row["generation"] for row in rows] == [17, 17, -1, 18]
    assert all(row["consumer"] == "consumer" and row["session"] == campaign.session for row in rows)
    assert all(row["partitions"] == [{"topic": partition.topic, "partition": 2}] for row in rows)
    assert calls.count("poll") == 1 and calls[-1] == "close", "K4 callback changed polling"


def test_k4_durable_delivery_identity(
    campaign: CampaignWitness, monkeypatch: pytest.MonkeyPatch
) -> None:
    response = {"commit_offset": True, "next_offset": 5, "disposition": "RECORDED", "task": None}
    monkeypatch.setattr(ControlClient, "_post_once", lambda *_args: response)
    message = SimpleNamespace(
        topic="tasks", partition=2, offset=4, key=b"registered-event", value=b"raw", headers=[]
    )
    committed = []
    kafka_worker.handle_delivery(
        SimpleNamespace(commit=committed.append),
        message,
        kafka_worker.RetryingControl("http://unused", threading.Event()),
        "consumer",
    )
    row = traces(campaign)[-1]
    assert row["event"] == "disposed" and row["event_id"] == "registered-event", (
        "K4 durable delivery identity missing"
    )
    assert row["offset"] == 4 and row["next_offset"] == 5 and row["disposition"] == "RECORDED"
    assert len(committed) == 1


@pytest.mark.parametrize("executor_path", [False, True])
def test_k4_result_disposition_identity(
    campaign: CampaignWitness, monkeypatch: pytest.MonkeyPatch, executor_path: bool
) -> None:
    response = {
        "commit_offset": True,
        "next_offset": 5,
        "disposition": "RECORDED",
        "task": {
            "workflow_id": "registered-workflow",
            "node_id": "pure.echo",
            "iteration": 0,
            "attempt_number": 3,
            "activity_name": "pure.echo",
            "activity_version": "v1",
            "input": {},
        },
    }
    monkeypatch.setattr(ControlClient, "_post_once", lambda *_args: response)
    monkeypatch.setattr(
        kafka_worker.ActivityRunner, "run_task", lambda *_args: {"Disposition": "ACCEPTED"}
    )
    consumer = SimpleNamespace(
        commit=lambda _offsets: None,
        _coordinator=SimpleNamespace(_generation=SimpleNamespace(generation_id=19)),
        _dur053_session="consumer-session",
    )
    message = SimpleNamespace(
        topic="tasks", partition=2, offset=4, key=b"registered-event", value=b"raw", headers=[]
    )
    lifecycle = WorkerLifecycle()
    done = kafka_worker.handle_delivery(
        consumer,
        message,
        kafka_worker.RetryingControl("http://unused", lifecycle.abort),
        "consumer",
        executor=BoundedExecutor(lifecycle, 1) if executor_path else None,
    )
    if done is not None:
        assert done.wait(2)
    row = traces(campaign)[-1]
    assert row["event"] == "result_disposition", "K4 durable result disposition missing"
    assert (
        row["workflow"] == "registered-workflow"
        and row["attempt"] == 3
        and row["disposition"] == "ACCEPTED"
    )
    assert (
        row["event_id"] == "registered-event"
        and row["generation"] == 19
        and row["consumer_session"] == "consumer-session"
    )


def test_k4_off_requires_no_storage(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DUR053_K4_CAMPAIGN", "0")
    monkeypatch.delenv("DUR053_WITNESS_DIR", raising=False)
    assert witness.configured_witness() is None
    assert witness.campaign_witness() is None
    monkeypatch.setenv("DUR053_K4_CAMPAIGN", "1")
    with pytest.raises(ValueError, match="requires"):
        witness.configured_witness()


def test_k4_short_write_and_fsync(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from workers import k4_witness

    synced: list[int] = []
    original = os.fsync

    def sync(fd: int) -> None:
        synced.append(fd)
        original(fd)

    monkeypatch.setattr(k4_witness.os, "fsync", sync)
    ledger = CampaignWitness(str(tmp_path), "pod", "uid", "cid")
    ledger.trace("test", {})
    assert len(synced) == (4 if os.name == "nt" else 6), "K4 witness fsync omitted"

    class Exit70(BaseException):
        pass

    def exit70(code: int) -> Any:
        assert code == 70
        raise Exit70()

    original_open = Path.open

    class ShortWriter:
        def __init__(self, handle: Any) -> None:
            self.handle = handle

        def __enter__(self) -> ShortWriter:
            return self

        def __exit__(self, *_args: Any) -> None:
            self.handle.close()

        def write(self, _data: bytes) -> int:
            return 0

        def fileno(self) -> int:
            return self.handle.fileno()

    def short(path: Path, *args: Any, **kwargs: Any) -> Any:
        handle = original_open(path, *args, **kwargs)
        return ShortWriter(handle) if path.name.endswith("trace.jsonl") else handle

    monkeypatch.setattr(Path, "open", short)
    monkeypatch.setattr(k4_witness.os, "_exit", exit70)
    try:
        CampaignWitness(str(tmp_path), "pod", "uid", "cid")
    except Exit70:
        return
    pytest.fail("K4 short write failed open")


def test_k4_directory_fsync(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from workers import k4_witness

    calls: list[str] = []
    monkeypatch.setattr(k4_witness.os, "name", "posix")
    monkeypatch.setattr(k4_witness.os, "open", lambda *_args: calls.append("open") or 23)
    monkeypatch.setattr(k4_witness.os, "fsync", lambda _fd: calls.append("sync"))
    monkeypatch.setattr(k4_witness.os, "close", lambda _fd: calls.append("close"))
    k4_witness.sync_directory(tmp_path)
    assert calls == ["open", "sync", "close"], "K4 inventory directory sync omitted"
