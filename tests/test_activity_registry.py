from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest
from workers.registry import default_registry

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "registry_check", ROOT / "scripts/check-activity-registry.py"
)
assert SPEC is not None and SPEC.loader is not None
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


def test_registry_check_rejects_every_mismatch() -> None:
    good = [{"name": "pure.echo", "version": "v1"}, {"name": "pure.add", "version": "v1"}]
    CHECK.check(good, list(reversed(good)))
    for invalid in (
        None,
        [],
        {},
        [{"name": "", "version": "v1"}],
        [{"name": "pure.echo", "version": 1}],
        [{"name": "pure.echo", "version": "v1", "extra": 1}],
        [good[0], good[0]],
    ):
        with pytest.raises(ValueError):
            CHECK.registry_set(invalid)
    for bad in (
        None,
        [],
        {},
        [good[0]],
        [*good, good[0]],
        [{"name": "pure.echo", "version": "v2"}, good[1]],
        [*good, {"name": "extra", "version": "v1"}],
        [{"name": "", "version": "v1"}],
        [{"name": "pure.echo", "version": 1}],
        [{"name": "pure.echo", "version": "v1", "extra": 1}],
    ):
        with pytest.raises(ValueError):
            CHECK.check(good, bad)
        with pytest.raises(ValueError):
            CHECK.check(bad, good)


def test_worker_manifest_is_effective_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DUR048_ACTIVITY_DELAY_MS", "0")
    monkeypatch.setenv("DUR050_ENABLE_SHA256_ACTIVITY", "0")
    registry = default_registry()
    assert registry.manifest() == [
        {"name": "pure.add", "version": "v1"},
        {"name": "pure.echo", "version": "v1"},
    ]
    registry.register("pure.echo", "v2", lambda value: value)
    assert {"name": "pure.echo", "version": "v2"} in registry.manifest()
    monkeypatch.setenv("DUR048_ACTIVITY_DELAY_MS", "1")
    assert {"name": "dur048.sleep", "version": "v1"} in default_registry().manifest()
    # R223 remains deferred: this existing legacy opt-in must fail compatibility.
    monkeypatch.setenv("DUR050_ENABLE_SHA256_ACTIVITY", "1")
    with pytest.raises(ValueError, match="registry mismatch"):
        CHECK.check(registry.manifest(), default_registry().manifest())


def test_registry_cli_rejects_mismatched_images(tmp_path: Path) -> None:
    runtime, worker = tmp_path / "runtime.json", tmp_path / "worker.json"
    runtime.write_text('[{"name":"pure.echo","version":"v1"}]')
    worker.write_text('[{"name":"pure.echo","version":"v2"}]')
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/check-activity-registry.py"),
            "--runtime",
            str(runtime),
            "--worker",
            str(worker),
        ],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    assert result.returncode != 0, "registry CLI mismatch guard bypassed"
    assert "registry mismatch" in result.stderr
