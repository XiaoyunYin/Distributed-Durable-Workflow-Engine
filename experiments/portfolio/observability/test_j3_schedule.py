import hashlib
import json
import sys
from fractions import Fraction
from pathlib import Path
from zipfile import ZipFile

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

from j3_schedule import (  # noqa: E402
    ARRIVAL_RATE,
    CALIBRATION_MINUTES,
    SECOND,
    derive,
    recovery_evidence,
    restored_rate,
    verify_seal,
)


def rates_for(t):
    mu = ARRIVAL_RATE * (1 + Fraction(300, t))
    return [mu + 1, mu - 1, mu]


@pytest.mark.parametrize("mu", [ARRIVAL_RATE - 1, ARRIVAL_RATE])
def test_rate_at_or_below_offered_halts(mu):
    result = derive([mu + 1, mu - 1, mu], [])
    assert result["status"] == "HALT"
    assert result["reason"] == "mu_at_or_below_arrival_rate"
    assert "delta_s" not in result


@pytest.mark.parametrize(
    ("t", "r", "delta", "forecast", "status"),
    [
        (539, 200, 0, 226, "SEALED"),
        (540, 200, 0, 226, "SEALED"),
        (600, 200, 60, 228, "SEALED"),
        (Fraction(600000001, 1000000), 200, 120, 230, "SEALED"),
        (540, Fraction(267_499_999_999, SECOND), 0, 226, "SEALED"),
        (540, Fraction(535, 2), 60, 228, "SEALED"),
        (540, Fraction(595, 2), 120, 230, "SEALED"),
        (220, 361, 240, 234, "SEALED"),
        (362, 649, 780, 252, "SEALED"),
        (4260, 200, 3720, 350, "SEALED"),
        (4261, 200, 3780, 352, "HALT"),
    ],
)
def test_seal_uses_exact_boundaries_and_shifts_both_arms(t, r, delta, forecast, status):
    result = derive(rates_for(t), [10 * SECOND, int(r * SECOND), 20 * SECOND])
    assert result["status"] == status
    assert Fraction(result["T_ratio"]) == t
    assert result["delta_s"] == delta
    assert result["forecast_minutes"] == forecast
    assert result["qualification_forecast"]["calibration"]["forecast_minutes"] == 293
    assert (
        result["qualification_forecast"]["confirmation"]["forecast_minutes"] == 164 + 2 * delta / 60
    )
    primary = result["primary"]
    assert [primary[k] for k in ("catchup_before_s", "oldest_clear_by_s", "burn_clear_by_s")] == [
        1440 + delta,
        1450 + delta,
        1570 + delta,
    ]
    assert primary["restore_s"] == 900
    assert primary["oldest_fire_by_s"] == 700
    assert primary["burn_fire_by_s"] == 765
    assert primary["ready_and_first_progress_by_s"] == 960
    for arm in (primary, result["matched_healthy"]):
        assert arm["final_healthy_window_s"] == [1620 + delta, 2220 + delta]
        assert arm["stop_s"] == 2220 + delta
        assert arm["drain_and_observation_end_s"] == 2820 + delta
    count = result["arrival_volume"]
    assert count - 1 < (2220 + delta) * ARRIVAL_RATE <= count
    assert result["baseline_arrivals"] == 15843


def test_measured_forecast_halts_when_the_base_forecast_passes():
    measurements = {name: 60 * minutes for name, minutes in CALIBRATION_MINUTES.items()}
    measurements["foreign"] += 7 * 60
    result = derive(rates_for(540), [200 * SECOND] * 3, measurements)
    assert result["status"] == "SEALED"
    assert result["qualification_forecast"]["calibration"]["forecast_seconds"] == 18_000
    measurements["foreign"] += 1
    result = derive(rates_for(540), [200 * SECOND] * 3, measurements)
    assert result["status"] == "HALT"
    assert result["reason"] == "forecast_over_cap"
    assert result["over_cap"] == ["calibration"]
    del measurements["final_evidence"]
    with pytest.raises(ValueError, match="missing forecast allowances"):
        derive(rates_for(540), [200 * SECOND] * 3, measurements)


def outage(last):
    def row(identity, admitted, terminal):
        return {
            "workflow_id": identity,
            "admitted_ns": admitted * SECOND,
            "terminal_ns": terminal * SECOND,
            "state": "SUCCEEDED",
        }

    return {
        "origin_ns": 100 * SECOND,
        "scale_down_ns": 700 * SECOND,
        "restore_ns": 1000 * SECOND,
        "observed_through_ns": 1600 * SECOND,
        "backlog": {"sample_ns": 998 * SECOND, "workflow_ids": ["backlog"]},
        "admissions": [
            row("already-finished", 680, 681),
            row("backlog", 970, 1010),
            row("admitted-before-restore", 999, last),
            row("new-arrival", 1001, 1020),
            row("late-new-arrival", 1030, 1550),
        ],
    }


@pytest.mark.parametrize(
    ("last", "stop", "count", "rule"),
    [(1030, 1030, 3, "all_pre_restore_finished"), (1560, 1540, 2, "540_second_cap")],
)
def test_rate_uses_restore_command_and_includes_new_successes(last, stop, count, rule):
    rate, evidence = restored_rate(outage(last))
    assert evidence["stop_ns"] == stop * SECOND
    assert evidence["stop_rule"] == rule
    assert "admitted-before-restore" in evidence["pre_restore_workflow_ids"]
    assert "new-arrival" in evidence["succeeded_workflow_ids"]
    assert rate == Fraction(count, stop - 1000)


def test_missing_backlog_and_invalid_episode_fail():
    evidence = outage(1030)
    evidence["backlog"]["workflow_ids"] = []
    with pytest.raises(ValueError, match="empty or duplicate"):
        restored_rate(evidence)
    evidence["backlog"]["workflow_ids"] = ["backlog"]
    evidence["admissions"][-1]["state"] = "FAILED"
    with pytest.raises(ValueError, match="unsuccessful"):
        restored_rate(evidence)


@pytest.mark.parametrize(
    ("case", "status", "r"),
    [
        ("later_terminal", "PASS", 365),
        ("later_age", "PASS", 360),
        ("zero_claimable", "PASS", 360),
        ("missing_age", "FAIL", None),
        ("not_terminal", "FAIL", None),
        ("refire", "FAIL", 360),
        ("missing_tick", "INVALID", None),
        ("missing_terminal_timestamp", "INVALID", None),
    ],
)
def test_recovery_endpoints_require_replay_coverage_and_continuing_offers(case, status, r):
    evidence = outage(1365 if case == "later_terminal" else 1355)
    stop = 2320 * SECOND
    evidence["observed_through_ns"] = stop + SECOND
    evaluations = [
        {
            "at_ns": at * SECOND,
            "valid": True,
            "rule_ok": True,
            "claimable_attempts": int(case != "zero_claimable" or at < 1360),
            "oldest_claimable_age_seconds": 28.540
            if at >= 1360 and case != "zero_claimable"
            else 90,
        }
        for at in range(1000, 2321, 5)
        if case != "missing_tick" or at != 1350
    ]
    if case == "missing_age":
        for row in evaluations[:-1]:
            row["oldest_claimable_age_seconds"] = 90
    if case in {"not_terminal", "missing_terminal_timestamp"}:
        evidence["admissions"][2]["terminal_ns"] = None
    if case == "not_terminal":
        evidence["admissions"][2]["state"] = "WAITING"
    transitions = (
        [{"at_ns": 1370 * SECOND, "firing": True, "alert": "OldestClaimable"}]
        if case == "refire"
        else []
    )
    if status == "INVALID":
        with pytest.raises(ValueError, match="missing"):
            recovery_evidence(evidence, evaluations, transitions, stop)
        return
    result = recovery_evidence(evidence, evaluations, transitions, stop)
    assert result["status"] == status and result["R_s"] == r
    assert result["oldest_refires_before_stop"] == transitions
    assert (result["age_endpoint"] is None) == (case == "missing_age")
    assert (result["terminal_endpoint"] is None) == (case == "not_terminal")


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("valid", None),
        ("outer_digest", "schedule artifact digest mismatch"),
        ("inner_digest", "sealed schedule digest mismatch"),
        ("over_cap", "confirmation forecast over cap"),
    ],
)
def test_confirmation_verifies_artifact_and_inner_seal_before_running(tmp_path, case, message):
    schedule = derive(rates_for(540), [649 * SECOND] * 3)
    if case == "over_cap":
        schedule["qualification_forecast"]["confirmation"]["forecast_seconds"] = 18_001
    payload = json.dumps(schedule).encode()
    digest = hashlib.sha256(payload).hexdigest()
    archive = tmp_path / "seal.zip"
    with ZipFile(archive, "w") as bundle:
        bundle.writestr("sealed-schedule.json", payload)
        bundle.writestr(
            "schedule-seal.json",
            json.dumps(
                {
                    "sha256": "0" * 64 if case == "inner_digest" else digest,
                    "campaign": "campaign",
                    "head": "head",
                }
            ),
        )
    artifact_digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    if case == "outer_digest":
        artifact_digest = "0" * 64
    if message:
        with pytest.raises(ValueError, match=message):
            verify_seal(archive, artifact_digest, "campaign", "head")
    else:
        verified, seal = verify_seal(archive, "sha256:" + artifact_digest, "campaign", "head")
        assert verified == schedule and seal["sha256"] == digest
