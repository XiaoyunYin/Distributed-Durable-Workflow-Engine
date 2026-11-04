from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest


def load_runner() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "scripts" / "mutation-runner.py"
    spec = importlib.util.spec_from_file_location("durable_mutation_runner", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_baseline_command_adds_verbose_selection_evidence() -> None:
    runner = load_runner()
    command = ["go", "test", "./cmd/dur049-checker", "-run", "^TestFenceCheck$"]
    assert runner.baseline_test_command(command) == [
        "go",
        "test",
        "-v",
        "./cmd/dur049-checker",
        "-run",
        "^TestFenceCheck$",
    ]


@pytest.mark.parametrize(
    "output",
    [
        "testing: warning: no tests to run\nok\tdurable/pkg\t0.003s\t[no tests to run]\n",
        "--- SKIP: TestFenceCheck (0.00s)\nPASS\nok\tdurable/pkg\t0.003s\n",
    ],
)
def test_case_baseline_rejects_no_test_and_skip_output(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, output: str
) -> None:
    runner = load_runner()
    monkeypatch.setattr(runner, "run", lambda *_args: (0, output))
    valid, evidence = runner.validate_case_baseline(
        "dur049-checker-test",
        ["go", "test", "./cmd/dur049-checker", "-run", "^Missing$"],
        tmp_path,
        {},
    )
    assert not valid
    assert "selected no tests" in evidence


def test_case_baseline_accepts_a_real_passing_test(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runner = load_runner()
    output = (
        "=== RUN   TestFenceCheck\n--- PASS: TestFenceCheck (0.00s)\nPASS\n"
        "ok\tdurable/pkg\t0.003s\n"
    )
    monkeypatch.setattr(runner, "run", lambda *_args: (0, output))
    valid, evidence = runner.validate_case_baseline(
        "dur049-checker-test",
        ["go", "test", "./cmd/dur049-checker", "-run", "^TestFenceCheck$"],
        tmp_path,
        {},
    )
    assert valid
    assert evidence == output


def test_case_command_keeps_python_config_tripwire_arguments(tmp_path: Path) -> None:
    runner = load_runner()
    assert runner.case_command(["python", "-c", "assert True"], "project-python", tmp_path) == [
        "project-python",
        "-c",
        "assert True",
    ]


def test_case_command_normalizes_legacy_pytest_without_nested_sync(tmp_path: Path) -> None:
    runner = load_runner()
    assert runner.case_command(
        ["uv", "run", "python", "-m", "pytest", "tests/test_incident_agent.py", "-q"],
        "project-python",
        tmp_path,
    ) == [
        "project-python",
        "-m",
        "pytest",
        "tests/test_incident_agent.py",
        "-q",
        "--basetemp",
        str(tmp_path),
        "-p",
        "no:cacheprovider",
    ]


def test_source_fingerprint_detects_restoration_drift(tmp_path: Path) -> None:
    runner = load_runner()
    source = tmp_path / "source.py"
    source.write_bytes(b"guard=True\n")
    original = runner.source_fingerprint(tmp_path)
    source.write_bytes(b"guard=False\n")
    assert runner.source_fingerprint(tmp_path) != original
    source.write_bytes(b"guard=True\n")
    assert runner.source_fingerprint(tmp_path) == original
