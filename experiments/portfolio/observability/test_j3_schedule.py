import sys
from fractions import Fraction
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

from j3_schedule import ARRIVAL_RATE, RESERVED_MINUTES, SECOND, derive, restored_rate  # noqa: E402


def rates_for(t):
    mu = ARRIVAL_RATE * (1 + Fraction(300, t))
    return [mu + 1, mu - 1, mu]


@pytest.mark.parametrize("mu", [ARRIVAL_RATE - 1, ARRIVAL_RATE])
def test_rate_at_or_below_offered_halts(mu):
    result = derive([mu + 1, mu - 1, mu])
    assert result["status"] == "HALT"
    assert result["reason"] == "mu_at_or_below_arrival_rate"
    assert "delta_s" not in result


@pytest.mark.parametrize(
    ("t", "delta", "forecast", "status"),
    [
        (539, 0, 226, "SEALED"),
        (540, 0, 226, "SEALED"),
        (600, 60, 228, "SEALED"),
        (Fraction(600000001, 1000000), 120, 230, "SEALED"),
        (4260, 3720, 350, "SEALED"),
        (4261, 3780, 352, "HALT"),
    ],
)
def test_seal_uses_exact_boundaries_and_shifts_both_arms(t, delta, forecast, status):
    result = derive(rates_for(t))
    assert result["status"] == status
    assert Fraction(result["T_ratio"]) == t
    assert result["delta_s"] == delta
    assert result["forecast_minutes"] == forecast
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
    measurements = {name: 60 * minutes + 1 for name, minutes in RESERVED_MINUTES.items()}
    measurements["bootstrap"] = 10  # Keep the reservation for a faster allowance.
    result = derive(rates_for(4260), measurements)
    assert result["forecast_base_minutes"] == 350
    assert result["forecast_seconds"] == 350 * 60 + len(measurements) - 1
    assert result["status"] == "HALT"
    assert result["reason"] == "forecast_over_cap"
    assert result["forecast_allowance_seconds"]["bootstrap"] == 25 * 60
    assert result["forecast_extended_arm_seconds"] == {
        "primary": 47 * 60 + 3720 + 1,
        "matched_healthy": 47 * 60 + 3720 + 1,
    }
    del measurements["final_evidence"]
    with pytest.raises(ValueError, match="missing forecast allowances"):
        derive(rates_for(4260), measurements)


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
