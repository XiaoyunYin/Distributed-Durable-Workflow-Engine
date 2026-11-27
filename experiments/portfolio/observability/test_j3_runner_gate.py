import json
import sys
from fractions import Fraction
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import j3_runner_gate as gate  # noqa: E402


def lock(path, x=10):
    path.write_text(
        json.dumps(
            {
                "x_tenths_percentage_point": x,
                "window_seconds": 120,
                "baseline_size": 10,
            }
        ),
        encoding="utf-8",
    )
    return path


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
    ("max_share", "x"),
    [
        (Fraction(0), 5),
        (Fraction(1, 6), 5),
        (Fraction(17, 100), 6),
        (Fraction(7, 10), 21),
        (Fraction(701, 1000), 22),
    ],
)
def test_threshold_uses_the_maximum_floor_and_upward_rounding(max_share, x):
    assert gate.candidate_threshold([Fraction(0)] * 9 + [max_share]) == x
    with pytest.raises(ValueError, match="ten complete"):
        gate.candidate_threshold([max_share] * 9)


def test_report_keeps_prior_run_out_of_candidate_calculation(tmp_path, monkeypatch):
    identity = {"run": "42", "head": "head", "workflow_attempt": 1}
    monkeypatch.setattr(gate, "identity", lambda: identity)
    source = tmp_path / "baseline"
    for index in range(1, 11):
        directory = source / str(index)
        directory.mkdir(parents=True)
        gate.save(
            directory / "baseline.json",
            {
                **identity,
                "stage": "baseline",
                "baseline_index": index,
                "window_seconds": 120,
                "io_full_share_ratio": str(Fraction(index, 50)),
            },
        )
    report = tmp_path / "report"
    gate.baseline_report(source, report)
    candidate = report / "runner-gate.candidate.json"
    assert gate.lock_threshold(candidate) == Fraction(6, 10)
    comparison = json.loads((report / "prior-run-comparison.json").read_bytes())
    assert comparison["pre_origin_io_full_share_pct"] == 4.82
    assert comparison["numerically_below_x"] is False


@pytest.mark.parametrize(
    ("share", "decision"),
    [
        (Fraction(999, 1000), "admitted"),
        (Fraction(1), "not-armed"),
        (Fraction(1001, 1000), "not-armed"),
    ],
)
def test_exact_threshold_rejects(tmp_path, share, decision):
    record = gate.evaluate_gate({}, share, lock(tmp_path / "lock"), {})
    assert record["decision"] == decision and record["stage_attempt"] == 1


def test_stage_history_caps_attempts_and_ignores_other_jobs(tmp_path):
    names = [
        "j3-runner-gate-calibration-42-1-not-armed",
        "j3-runner-gate-calibration-42-2-not-armed",
        "j3-runner-gate-calibration-42-3-not-armed",
        "j3-runner-gate-confirmation-43-1-admitted",
    ]
    prior = gate.previous_attempts(names, "calibration", "42", 4)
    result = gate.evaluate_gate({}, 0, lock(tmp_path / "lock"), prior)
    assert (result["decision"], result["reason"], result["stage_attempt"]) == (
        "not-armed",
        "attempt_cap",
        4,
    )
    third = gate.evaluate_gate({}, 0, tmp_path / "lock", {1: "not-armed", 2: "not-armed"})
    assert third["decision"] == "admitted" and third["stage_attempt"] == 3
    prior = gate.previous_attempts(names, "confirmation", "42", 4)
    assert gate.evaluate_gate({}, 0, tmp_path / "lock", prior)["stage_attempt"] == 1
    result = gate.evaluate_gate({}, 0, tmp_path / "lock", {3: "admitted"})
    assert result["reason"] == "stage_already_admitted"


@pytest.mark.parametrize("missing_lock", [False, True])
def test_rejection_publishes_not_armed_trace_and_verdict(tmp_path, monkeypatch, missing_lock):
    manifest = tmp_path / "lock"
    if not missing_lock:
        lock(manifest)
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
        return Fraction(1)

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
