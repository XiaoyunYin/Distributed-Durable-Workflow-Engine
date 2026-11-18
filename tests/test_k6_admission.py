"""Exercise budget decisions with fixed, synthetic ledger and episode timings."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from dur053_k6_admission import (  # noqa: E402
    ActiveJob,
    EpisodeProgress,
    admit,
    schedule_sensitivities,
)
from dur053_k6_registry import build_registry, registered_schedule  # noqa: E402

LEDGER_ACTUAL = 139553


def test_report_only_projections() -> None:
    projections = schedule_sensitivities()
    assert projections["frozen_seconds"] == pytest.approx(101520.701112, abs=0.000001, rel=0)
    assert projections["k4_inflation_seconds"] == pytest.approx(140057.094327, abs=0.000001, rel=0)
    assert projections["volume_matched_seconds"] == pytest.approx(
        111743.599282, abs=0.000001, rel=0
    )


def test_admit_funded_schedule_and_account_once() -> None:
    registry = build_registry()
    dispatched = admit(
        registry, ledger_actual=LEDGER_ACTUAL, progress={}, effect_overhead_seconds=10
    )
    assert dispatched["admit"] and dispatched["episode"] == "hook-0-0"
    assert dispatched["setup_jobs"] == 6
    # Frozen projection includes six 300 s setups. Switch to measured setup and charge 22 effects.
    assert dispatched["expected_total_seconds"] == pytest.approx(
        LEDGER_ACTUAL + 101520.701112 + 6 * (334.94 - 300) + 22 * 10
    )
    active = admit(
        registry,
        ledger_actual=LEDGER_ACTUAL,
        progress={},
        effect_overhead_seconds=10,
        active_job=ActiveJob("k6-1", 500),
        workflow_elapsed=700,
    )
    assert active["admit"] and active["setup_jobs"] == 5
    assert active["actual_seconds"] == LEDGER_ACTUAL + 500
    assert active["expected_total_seconds"] == pytest.approx(
        dispatched["expected_total_seconds"] - 334.94 + 500
    )


def test_refuse_unfunded_full_schedule() -> None:
    result = admit(build_registry(), ledger_actual=175000, progress={}, effect_overhead_seconds=1)
    assert not result["admit"], "admitted an unfunded K6 and K5 schedule"
    assert result["reason"] == "unfunded-full-schedule"
    assert len(result["unperformed"]) == 241
    assert list(result["unperformed"])[114:116] == ["medium-16-4", "step-medium-0-0"]
    assert set(result["unperformed"].values()) == {result["reason"]}


def test_refuse_near_workflow_ceiling() -> None:
    # The live clock still fits. Non-live workflow time uses the remaining room.
    arguments = dict(
        ledger_actual=LEDGER_ACTUAL,
        progress={},
        effect_overhead_seconds=1,
        active_job=ActiveJob("k6-1", 16000),
    )
    last_start = 21000 - (120 + 5.708 + 180 + 600 + 60 + 300 + 600)
    registry = build_registry()
    assert admit(registry, workflow_elapsed=last_start, **arguments)["admit"]
    result = admit(registry, workflow_elapsed=last_start + 0.001, **arguments)
    assert not result["admit"], "admitted an episode beyond the workflow ceiling"
    assert result["reason"] == "workflow-episode-fit"
    assert list(result["unperformed"]) == registry["groups"][0]["episodes"]


def test_refuse_near_live_ceiling() -> None:
    result = admit(
        build_registry(),
        ledger_actual=LEDGER_ACTUAL,
        progress={},
        effect_overhead_seconds=1,
        active_job=ActiveJob("k6-1", 17035),
        workflow_elapsed=17035,
    )
    assert not result["admit"] and result["reason"] == "live-job-episode-fit"


def test_refuse_effect_without_smoke_overhead() -> None:
    registry = build_registry()
    progress = {row["episode"]: EpisodeProgress("completed") for row in registry["episodes"][:90]}
    result = admit(registry, ledger_actual=LEDGER_ACTUAL, progress=progress)
    assert not result["admit"], "admitted an effect episode without measured overhead"
    assert result["reason"] == "missing-effect-overhead"
    assert result["episode"] == "medium-12-0"
    assert result["expected_total_seconds"] is None
    assert len(result["unperformed"]) == 151
    assert "medium-16-4" in result["unperformed"]


def test_round351_cost_substitution_uses_maximum_in_exact_cell() -> None:
    registry = build_registry()
    progress = {row["episode"]: EpisodeProgress("completed") for row in registry["episodes"][:30]}
    progress.update(
        {
            "medium-0-0": EpisodeProgress("completed", True, 100, 850),
            "medium-0-1": EpisodeProgress("failed", True, 1000, 1400),
            "medium-0-2": EpisodeProgress("invalid", True, 1500, 2500),
            "medium-1-0": EpisodeProgress("completed", False, 2600, 3800),
            "medium-1-1": EpisodeProgress("censored", True, 3900, 4900),
            "medium-1-2": EpisodeProgress("failed-to-start", True, 4901, 5901),
        }
    )
    result = admit(
        registry, ledger_actual=LEDGER_ACTUAL, progress=progress, effect_overhead_seconds=1
    )
    costs = result["expected_episode_seconds"]
    assert costs["medium-0-3"] == costs["medium-0-4"] == 750
    assert costs["medium-1-3"] == costs["medium-1-4"] == 395, "pooled costs across K6 cells"
    assert result["dispositions"]["medium-0-2"] == "invalid"
    assert result["dispositions"]["medium-1-1"] == "censored"


def test_effect_cost_includes_overhead_and_controls_have_distinct_units() -> None:
    registry = build_registry()
    progress = {
        "medium-12-0": EpisodeProgress("completed", True, 100, 850),
        "medium-16-0": EpisodeProgress("completed", True, 900, 1400),
    }
    result = admit(
        registry, ledger_actual=LEDGER_ACTUAL, progress=progress, effect_overhead_seconds=13
    )
    costs = result["expected_episode_seconds"]
    assert costs["medium-12-1"] == 750
    assert costs["medium-13-0"] == 395 + 13
    assert costs["medium-16-1"] == 500
    assert costs["medium-16-3"] == costs["medium-16-4"] == 395 + 13


def test_admission_rejects_altered_cost_unit() -> None:
    registry = build_registry()
    registry["episodes"][30]["cost_unit"] = "medium-1"
    with pytest.raises(ValueError, match="k6-episode-cost-unit"):
        admit(registry, ledger_actual=LEDGER_ACTUAL, progress={}, effect_overhead_seconds=1)


def test_k5_recheck_preserves_reservation_and_full_schedule_stop() -> None:
    registry = build_registry()
    _, groups, _ = registered_schedule()
    progress = {key: EpisodeProgress("completed") for g in groups[:-1] for key in g["episodes"]}
    result = admit(registry, ledger_actual=241000, progress=progress)
    assert result["reason"] == "unfunded-full-schedule"
    assert list(result["unperformed"]) == groups[-1]["episodes"]

    job_ids = groups[-1]["episodes"]
    progress.update({key: EpisodeProgress("completed") for key in job_ids[:-1]})
    result = admit(registry, ledger_actual=240301, progress=progress)
    assert result["reason"] == "dispatch-reservation"
    assert result["unperformed"] == {job_ids[-1]: "dispatch-reservation"}
    assert admit(registry, ledger_actual=240300, progress=progress)["admit"]
