from __future__ import annotations

import hashlib
import importlib.util
import json
import re
import sys
from pathlib import Path

import pytest
from test_workers import FakeControl, task
from workers import witness
from workers.registry import ActivityRegistry, default_registry
from workers.runner import ActivityRunner


def test_cpu_fixture_bounded_and_deterministic(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DUR053_ALLOW_FIXTURE_ACTIVITY", "0")
    with pytest.raises(LookupError):
        default_registry().resolve("dur053.cpu", "v1")
    monkeypatch.setenv("DUR053_ALLOW_FIXTURE_ACTIVITY", "1")
    monkeypatch.setenv("DUR053_CPU_WORK_UNITS", "10")
    digest = bytes(32)
    for _ in range(10):
        digest = hashlib.sha256(digest).digest()
    activity = default_registry().resolve("dur053.cpu", "v1")
    assert activity({}) == activity("ignored") == {"digest": digest.hex(), "work_units": 10}
    for bad in ("0", "-1", "1000001", "invalid"):
        monkeypatch.setenv("DUR053_CPU_WORK_UNITS", bad)
        with pytest.raises(ValueError):
            default_registry()
    monkeypatch.setenv("DUR053_ALLOW_FIXTURE_ACTIVITY", "invalid")
    with pytest.raises(ValueError):
        default_registry()


def test_witness_is_external_unique_and_semantics_neutral(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    external = witness.InvocationWitness(str(tmp_path), "pod", "uid")
    monkeypatch.setenv("DUR053_WITNESS_DIR", str(tmp_path))
    monkeypatch.setattr(witness, "_GLOBAL", external)
    registry = ActivityRegistry()
    registry.register("fixture", "v1", lambda x: x + 1)
    control = FakeControl()
    runner = ActivityRunner(control, registry)
    assert runner.run_task(task("one"))["payload"] == 2
    assert runner.run_task(task("two"))["payload"] == 2
    rows = [json.loads(line) for line in external.path.read_text().splitlines()]
    assert [r["event"] for r in rows] == ["session", "start", "end", "start", "end"]
    assert rows[1]["invocation_id"] != rows[3]["invocation_id"]
    for start, end in ((rows[1], rows[2]), (rows[3], rows[4])):
        for field in (
            "workflow_id",
            "node_id",
            "iteration",
            "attempt_number",
            "claim_token",
            "pod_uid",
            "invocation_id",
        ):
            assert start[field] == end[field]
        assert start["monotonic_ns"] <= end["monotonic_ns"]
        assert start["timestamp"] <= end["timestamp"]
    inventory = external.inventory()
    assert inventory["complete"] and inventory["written"] == 5
    assert inventory["sha256"] == hashlib.sha256(external.path.read_bytes()).hexdigest()
    # No directory/fallback is created when external storage is unavailable.
    broken = witness.InvocationWitness(str(tmp_path / "missing"), "pod", "uid")
    monkeypatch.setattr(witness, "_GLOBAL", broken)
    assert runner.run_task(task("three"))["payload"] == 2
    assert len(control.results) == 3
    assert not broken.inventory()["complete"] and broken.inventory()["errors"] == 3
    assert not (tmp_path / "missing").exists()


def load_driver():  # type: ignore[no-untyped-def]
    path = Path(__file__).resolve().parents[1] / "scripts/dur053_prerequisites.py"
    spec = importlib.util.spec_from_file_location("dur053_driver", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_incomplete_witness_export_fails(tmp_path: Path) -> None:
    driver = load_driver()
    pod = tmp_path / "pod"
    pod.mkdir()
    external = witness.InvocationWitness(str(pod), "pod", "uid")
    inventory = external.inventory()
    assert driver.export_witness(tmp_path, [inventory], ["uid"])["ledgers"]
    external.path.write_bytes(external.path.read_bytes()[:-1])
    with pytest.raises(RuntimeError, match="differs"):
        driver.export_witness(tmp_path, [inventory], ["uid"])


def test_prerequisites_driver_failure_is_retained_and_cleaned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver = load_driver()
    output, scratch = tmp_path / "evidence", tmp_path / "scratch"

    def fail(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError("injected tool failure")

    monkeypatch.setattr(driver, "execute", fail)
    assert driver.run(output, scratch) == 1
    report = json.loads((output / "manifest.json").read_text())
    assert report["status"] == "FAIL" and report["scratch_removed"]
    assert "injected tool failure" in report["error"]
    assert list(scratch.iterdir()) == []
    with pytest.raises(FileExistsError):
        driver.run(output, scratch)


def test_existing_prerequisites_output_is_refused_before_docker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver = load_driver()
    output, scratch = tmp_path / "existing-evidence", tmp_path / "scratch"
    output.mkdir()
    sentinel = output / "sentinel"
    sentinel.write_text("preserve")
    calls: list[list[str]] = []
    monkeypatch.setattr(driver, "execute", lambda command, _env: calls.append(command))

    with pytest.raises(FileExistsError):
        driver.run(output, scratch)

    assert calls == []
    assert sentinel.read_text() == "preserve"
    assert not scratch.exists()


def test_prerequisites_output_root_forwarded_and_ci_artifact_matches() -> None:
    root = Path(__file__).resolve().parents[1]
    entrypoint = (root / "scripts/dur053-kubernetes.ps1").read_text(encoding="utf-8")
    workflow = (root / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    job = re.split(
        r"\n  [a-z][a-z0-9-]*:\n",
        workflow.split("\n  k2-prerequisites:\n", 1)[1],
        maxsplit=1,
    )[0]

    assert "[string]$OutputRoot" in entrypoint
    assert '@("--output", $OutputRoot)' in entrypoint
    assert "-OutputRoot $evidence" in job
    assert '$env:RUNNER_TEMP "dur053-k2-prerequisites"' in job
    assert "if: always()" in job
    assert "name: dur053-k2-prerequisites" in job
    assert "path: ${{ runner.temp }}/dur053-k2-prerequisites/" in job
    assert "if-no-files-found: error" in job
    assert "k2-prerequisites-v1/compose-prerequisites" not in job


def test_witness_short_write_and_fsync(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import os
    from typing import Any

    synced: list[int] = []
    original_sync = os.fsync

    def sync(fd: int) -> None:
        synced.append(fd)
        original_sync(fd)

    monkeypatch.setattr(witness.os, "fsync", sync)
    external = witness.InvocationWitness(str(tmp_path), "pod", "uid")
    invocation = external.begin({"workflow_id": "wf"})
    external.end(invocation, "SUCCEEDED")
    assert len(synced) == 3, "witness durability guard bypassed"
    monkeypatch.setattr(witness.os, "fsync", lambda _fd: None)
    original_open = Path.open

    class ShortWriter:
        def __enter__(self) -> ShortWriter:
            return self

        def __exit__(self, *_args: Any) -> None:
            pass

        def write(self, _data: bytes) -> int:
            return 0

        def fileno(self) -> int:
            return -1

    def open_file(path: Path, *args: Any, **kwargs: Any) -> Any:
        if path.parent == tmp_path:
            return ShortWriter()
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", open_file)
    external = witness.InvocationWitness(str(tmp_path), "pod", "uid")
    assert external.inventory()["errors"] == 1
    assert not external.inventory()["complete"], "witness short-write guard bypassed"


@pytest.mark.parametrize("fault", ["none", "registry", "load", "checker", "negative", "cleanup"])
def test_prerequisites_episode_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    import subprocess
    from typing import Any

    driver = load_driver()
    output, scratch = tmp_path / "evidence", tmp_path / "scratch"
    scratch.mkdir()
    trace = {"History": [], "Submissions": []}
    report = {"trace": trace, "valid": True, "status": "PASS", "campaign": {}}

    def execute(args: list[str], env: dict[str, str], **_kwargs: Any) -> Any:
        code, stdout = 0, ""
        if args[0] == "loadgen":
            Path(args[args.index("-output") + 1]).write_text('{"rows":[]}')
            code = int(fault == "load")
        elif args[0] == "checker":
            is_negative = any("negative-incomplete-witness.json" in arg for arg in args)
            if is_negative:
                code = 0 if fault == "negative" else 1
                stdout = json.dumps({"valid": False, "status": "FAIL"})
            else:
                code = int(fault == "checker")
                stdout = json.dumps({**report, "valid": not code})
        elif args[:2] == ["docker", "inspect"]:
            stdout = json.dumps(
                [{"Id": "cid", "Image": "sha256:image", "Created": "now", "State": {}}]
            )
        elif "activity-registry" in args:
            names = ["dur048.sleep", "dur053.cpu", "pure.add", "pure.echo"]
            if fault == "registry" and "python" in args:
                names.remove("dur053.cpu")
            stdout = json.dumps([{"name": name, "version": "v1"} for name in names])
        elif any("SELECT count(*)" in arg for arg in args):
            stdout = "0\n"
        elif any("DROP DATABASE" in arg for arg in args) and fault == "cleanup":
            code = 1
        return subprocess.CompletedProcess(args, code, stdout, "")

    monkeypatch.setattr(driver, "execute", execute)
    monkeypatch.setattr(driver, "reconcile", lambda *_args: [])
    monkeypatch.setattr(
        driver,
        "export_witness",
        lambda *_args: {"ledgers": [{"data": "YQo="}], "expected_pods": []},
    )
    result = driver.episode(
        "short", 0, output, scratch, {}, {"loadgen": "loadgen", "checker": "checker"}
    )
    assert result["status"] == ("PASS" if fault == "none" else "FAIL"), (
        "episode fail-closed guard bypassed"
    )
    assert json.loads((output / "manifest.json").read_text())["status"] == result["status"]
    if fault == "none":
        assert (output / "offline-checker.json").read_text() == json.dumps(report), (
            "checker report bytes changed during export"
        )


def test_prerequisites_quiet_interval_and_terminal_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver = load_driver()
    now = [0.0]
    monkeypatch.setattr(driver.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(driver.time, "sleep", lambda seconds: now.__setitem__(0, now[0] + seconds))

    def get(url: str):  # type: ignore[no-untyped-def]
        if url.endswith("/witness"):
            return {"written": 1 if now[0] < 20 else 2}
        return {"workflow": {"state": "SUCCEEDED"}}

    monkeypatch.setattr(driver, "get", get)
    driver.reconcile({"rows": [{"workflow_id": "wf"}]}, ["http://worker"], 60)
    assert 50 <= now[0] < 51, "witness quiet interval guard bypassed"
    now[0] = 0
    monkeypatch.setattr(
        driver,
        "get",
        lambda url: (
            {"written": 1} if url.endswith("/witness") else {"workflow": {"state": "RUNNABLE"}}
        ),
    )
    with pytest.raises(RuntimeError, match="timed out"):
        driver.reconcile({"rows": [{"workflow_id": "wf"}]}, ["http://worker"], 60)


def test_checker_snapshot_export_preserves_raw_trace() -> None:
    driver = load_driver()
    trace = (
        '{"Snapshot":{"table":{"rows":[{"long_key":9007199254740993,'
        '"a":"\\u4e2d","tiny":1.000000e-8}],"sha256":"opaque"}}}'
    )
    report = '{"earlier":{"trace":{"wrong":true}},"trace":' + trace + ',"status":"PASS"}'
    # Validate the full report as the episode driver does, but retain its raw
    # trace bytes. Nested trace keys cannot redirect the top-level extraction.
    assert json.loads(report)["status"] == "PASS"
    assert driver.trace_document(report) == trace + "\n", "snapshot bytes changed during export"
    assert json.dumps(json.loads(trace), sort_keys=True) != trace
    with pytest.raises(RuntimeError, match="missing top-level trace"):
        driver.trace_document('{"earlier":{"trace":{"wrong":true}}}')
