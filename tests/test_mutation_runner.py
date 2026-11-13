from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
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


def test_pytest_case_uses_a_nested_basetemp(tmp_path: Path) -> None:
    runner = load_runner()
    test_file = tmp_path / "test_nested.py"
    test_file.write_text(
        "def test_temp(tmp_path):\n    assert tmp_path.is_dir()\n",
        encoding="utf-8",
    )
    basetemp = tmp_path / "not-created" / "case"
    command = runner.case_command(
        ["python", "-m", "pytest", str(test_file), "-q"], sys.executable, basetemp
    )
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "1 passed" in result.stdout


def test_working_tree_export_removes_deleted_platform_test(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import io
    import tarfile

    runner = load_runner()
    repository, destination = tmp_path / "repo", tmp_path / "export"
    repository.mkdir()
    destination.mkdir()
    old = "internal/invariants/k3b_windows_test.go"
    new = "internal/invariants/k3b_pilot_window_union_test.go"
    (repository / new).parent.mkdir(parents=True)
    (repository / new).write_bytes(b"package invariants\n")
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w") as handle:
        entry = tarfile.TarInfo(old)
        entry.size = 3
        handle.addfile(entry, io.BytesIO(b"old"))

    def output(command: list[str], **_kwargs: object) -> bytes:
        if "archive" in command:
            return archive.getvalue()
        if "--diff-filter=D" in command:
            return old.encode() + b"\0"
        if "--others" in command:
            return new.encode() + b"\0"
        return b""

    monkeypatch.setattr(runner.subprocess, "check_output", output)
    runner.export_head(repository, destination, working_tree=True)
    assert not (destination / old).exists(), "deleted platform test retained in mutation export"
    assert (destination / new).read_bytes() == b"package invariants\n"


@pytest.mark.parametrize("output_name", ["results-powershell.json", "results-bash.json"])
def test_report_records_wall_clock_phases_without_changing_verdicts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, output_name: str
) -> None:
    runner = load_runner()
    repository = tmp_path / "repository"
    (repository / "scripts").mkdir(parents=True)
    cases = [
        {
            "id": kind,
            "kind": kind,
            "file": "guard.txt",
            "old": "enabled",
            "new": "disabled",
            "command": ["go", "test", "./guard", "-run", "^TestGuard$"],
            "expected": "guard removed",
            "requires_database": False,
        }
        for kind in ("behavioral", "configuration_tripwire")
    ]
    (repository / "manifest.json").write_text(json.dumps({"cases": cases}), encoding="utf-8")
    elapsed = 0.0

    def advance(seconds: float) -> None:
        nonlocal elapsed
        elapsed += seconds

    def check_output(command: list[str], **_kwargs: object) -> str:
        if command[0] == "uv":
            advance(1.25)
            return sys.executable
        return "a" * 40

    def export(_repo: Path, destination: Path, working_tree: bool) -> None:
        assert working_tree
        advance(0.75)
        (destination / "guard.txt").write_bytes(b"enabled\n")

    fingerprint = runner.source_fingerprint

    def timed_fingerprint(root: Path) -> dict[str, str]:
        advance(0.25)
        return fingerprint(root)

    baselines = iter((2.0, 3.0))
    case_baselines = iter((4.0, 5.0))
    mutations = iter((6.0, 7.0))

    def run(command: list[str], cwd: Path, _env: dict[str, str]) -> tuple[int, str]:
        if cwd == repository:
            advance(next(baselines))
            return 0, "ok global baseline"
        if "-v" in command:
            assert (cwd / "guard.txt").read_bytes() == b"enabled\n"
            advance(next(case_baselines))
            return 0, "--- PASS: TestGuard (0.00s)\nPASS\n"
        assert (cwd / "guard.txt").read_bytes() == b"disabled\n"
        advance(next(mutations))
        return 1, "guard removed"

    monkeypatch.setattr(runner, "__file__", str(repository / "scripts/mutation-runner.py"))
    monkeypatch.setattr(runner.time, "perf_counter", lambda: elapsed)
    monkeypatch.setattr(runner.subprocess, "check_output", check_output)
    monkeypatch.setattr(runner, "export_head", export)
    monkeypatch.setattr(runner, "source_fingerprint", timed_fingerprint)
    monkeypatch.setattr(runner, "run", run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mutation-runner",
            "--manifest",
            "manifest.json",
            "--working-tree",
            "--output",
            output_name,
        ],
    )
    assert runner.main() == 0
    report = json.loads((repository / output_name).read_text(encoding="utf-8"))
    assert report["schema"] == "dur-046-mutation-results.v1"
    assert report["status"] == "PASS"
    assert report["source_basis"] == "working-tree overlay"
    assert report["git_commit"] == "a" * 40
    assert report["duration_seconds"] == 29.75
    assert report["setup_duration_seconds"] == 2.25
    assert report["baseline_duration_seconds"] == 2.0
    assert report["restored_baseline_duration_seconds"] == 3.0
    for index, result in enumerate(report["cases"]):
        assert result["id"] == cases[index]["id"]
        assert result["kind"] == cases[index]["kind"]
        assert result["status"] == result["baseline_status"] == "PASS"
        assert result["exit_code"] == 1
        assert result["expected_failure"] == result["output_tail"] == "guard removed"
        assert result["source_sha256"]
        assert result["baseline_output_tail"]
        assert result["duration_seconds"] == 10.25 + index * 2
        assert result["setup_duration_seconds"] == 0.25
        assert result["baseline_duration_seconds"] == 4.0 + index
        assert result["mutation_duration_seconds"] == 6.0 + index
