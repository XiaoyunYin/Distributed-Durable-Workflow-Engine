import json
import sys
from fractions import Fraction
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import j3_runner_gate as gate  # noqa: E402


def test_registered_lock_loads_half_percent():
    assert gate.lock_threshold(gate.LOCK) == Fraction(1, 2)


def test_io_full_share_uses_counter_deltas_and_elapsed_time():
    samples = [
        {
            "monotonic_ns": (10 + i) * gate.SECOND,
            "io": f"some avg10=99.0 total=99\nfull avg10=42.0 total={555_555 + 10000 * i}",
        }
        for i in range(121)
    ]
    assert gate.io_full_share(samples) == 1
    samples[1]["monotonic_ns"] += gate.SECOND
    with pytest.raises(ValueError, match="invalid idle PSI window"):
        gate.io_full_share(samples)
    samples[1]["monotonic_ns"] -= gate.SECOND
    samples[-1]["io"] = "full avg10=0 total=0"
    with pytest.raises(ValueError, match="invalid idle PSI window"):
        gate.io_full_share(samples)


@pytest.mark.parametrize(
    ("admissions", "expected_verdict", "exit_code"),
    [(12, "PASS", 0), (11, "FAIL", 1)],
)
def test_validation_report_checks_locked_gate_without_writing_x(
    tmp_path, monkeypatch, admissions, expected_verdict, exit_code
):
    identity = {"run": "42", "head": "head", "workflow_attempt": 1}
    monkeypatch.setattr(gate, "identity", lambda: identity)
    source = tmp_path / "baseline"
    for index in range(1, 21):
        directory = source / str(index)
        directory.mkdir(parents=True)
        share = Fraction(499, 1000) if index <= admissions else Fraction(1, 2)
        gate.save(
            directory / "baseline.json",
            {
                **identity,
                "stage": "baseline",
                "baseline_index": index,
                "window_seconds": 120,
                "io_full_share_ratio": str(share),
                "runner": {"cpu_models": [f"Model {index}"], "image_version": "version"},
            },
        )
    output = tmp_path / "report"
    monkeypatch.setattr(sys, "argv", ["gate", "report", str(output), "--source", str(source)])
    original_lock = gate.LOCK.read_bytes()
    assert gate.main() == exit_code
    assert gate.LOCK.read_bytes() == original_lock
    assert {file.name for file in output.iterdir()} == {
        "baseline-report.json",
        "prior-run-comparison.json",
    }
    report = json.loads((output / "baseline-report.json").read_bytes())
    assert report["admitted_count"] == admissions
    assert report["admission_rate"] == admissions / 20
    assert report["verdict"] == expected_verdict
    assert [row["admitted"] for row in report["runners"]] == [True] * admissions + [False] * (
        20 - admissions
    )
    assert report["runners"][-1]["io_full_share_pct"] == 0.5
    assert report["runners"][-1]["runner"] == {
        "cpu_models": ["Model 20"],
        "image_version": "version",
    }
    comparison = json.loads((output / "prior-run-comparison.json").read_bytes())
    assert comparison["pre_origin_io_full_share_pct"] == 4.82
    assert comparison["numerically_below_x"] is False
    assert {"x_pct", "x_tenths_percentage_point"}.isdisjoint(report.keys() | comparison.keys())


@pytest.mark.parametrize(
    ("share", "decision"),
    [
        (Fraction(499, 1000), "admitted"),
        (Fraction(1, 2), "not-armed"),
        (Fraction(501, 1000), "not-armed"),
    ],
)
def test_exact_threshold_rejects(share, decision):
    record = gate.evaluate_gate({}, share, gate.LOCK, {})
    assert record["decision"] == decision and record["stage_attempt"] == 1


def test_stage_history_caps_attempts_and_ignores_other_jobs():
    names = [
        "j3-runner-gate-calibration-42-1-not-armed",
        "j3-runner-gate-calibration-42-2-not-armed",
        "j3-runner-gate-calibration-42-3-not-armed",
        "j3-runner-gate-confirmation-43-1-admitted",
    ]
    prior = gate.previous_attempts(names, "calibration", "42", 4)
    result = gate.evaluate_gate({}, 0, gate.LOCK, prior)
    assert (result["decision"], result["reason"], result["stage_attempt"]) == (
        "not-armed",
        "attempt_cap",
        4,
    )
    third = gate.evaluate_gate({}, 0, gate.LOCK, {1: "not-armed", 2: "not-armed"})
    assert third["decision"] == "admitted" and third["stage_attempt"] == 3
    prior = gate.previous_attempts(names, "confirmation", "42", 4)
    assert gate.evaluate_gate({}, 0, gate.LOCK, prior)["stage_attempt"] == 1
    result = gate.evaluate_gate({}, 0, gate.LOCK, {3: "admitted"})
    assert result["reason"] == "stage_already_admitted"


@pytest.mark.parametrize("missing_lock", [False, True])
def test_rejection_publishes_not_armed_trace_and_verdict(tmp_path, monkeypatch, missing_lock):
    manifest = tmp_path / "missing-lock" if missing_lock else gate.LOCK
    monkeypatch.setattr(gate, "LOCK", manifest)
    for name, value in {
        "GITHUB_RUN_ID": "42",
        "GITHUB_RUN_ATTEMPT": "2",
        "GITHUB_SHA": "head",
        "GITHUB_REPOSITORY": "owner/repo",
        "ImageVersion": "version",
        "GITHUB_OUTPUT": str(tmp_path / "outputs"),
    }.items():
        monkeypatch.setenv(name, value)

    def capture(path):
        path.write_text("idle trace\n", encoding="utf-8")
        return Fraction(1, 2)

    def history(*_args, **_kwargs):
        assert (tmp_path / "runner-gate" / "idle-pressure.jsonl").is_file()
        return "j3-runner-gate-calibration-42-1-not-armed\n"

    monkeypatch.setattr(gate, "idle_trace", capture)
    monkeypatch.setattr(gate, "runner_cpu", lambda: {"cpu_models": ["model"], "online_cores": 4})
    monkeypatch.setattr(gate.subprocess, "check_output", history)
    gate.collect("calibration", tmp_path / "runner-gate")
    report = json.loads((tmp_path / "verdict.json").read_bytes())
    assert report["status"] == "NOT_ARMED" and report["stage_attempt"] == 2
    assert report["reason"] == ("missing_lock" if missing_lock else "io_full_at_or_above_threshold")
    assert (tmp_path / "outputs").read_text(encoding="utf-8") == "decision=not-armed\n"
    with pytest.raises(ValueError, match="missing current runner gate admission"):
        gate.verify_admission(tmp_path, "calibration")
