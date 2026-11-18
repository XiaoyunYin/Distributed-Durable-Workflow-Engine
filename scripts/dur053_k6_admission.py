"""Offline arithmetic for B.8 item 2 and the exact-cell Round 351 ruling."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from dur053_k6_overhead import read_effect_overhead
from dur053_k6_registry import registered_schedule, require, validate_registry
from dur053_schedule import CEILING, SETUP

CAP = 259200
WORKFLOW_LIMIT = 350 * 60
DIAGNOSTICS = 300
CLOSEOUT = 600
ALLOWANCE = 1710
MEASURED_SETUP = 334.94

Disposition = Literal[
    "completed", "failed", "invalid", "failed-to-start", "censored", "unsupported"
]


@dataclass(frozen=True)
class EpisodeProgress:
    disposition: Disposition
    verified: bool = False
    warmup_start: float | None = None
    reset_export_end: float | None = None

    def measured_seconds(self) -> float | None:
        if (
            not self.verified
            or self.disposition not in ("completed", "failed")
            or self.warmup_start is None
            or self.reset_export_end is None
        ):
            return None
        start = seconds(self.warmup_start, "k6-warmup-start")
        end = seconds(self.reset_export_end, "k6-reset-export-end")
        require(end > start, "k6-measured-cost-interval")
        return end - start


@dataclass(frozen=True)
class ActiveJob:
    group: str
    # Live age from the first owned resource, including setup and any ongoing overhead.
    live_elapsed: float


def seconds(value: float, reason: str) -> float:
    require(type(value) in (float, int) and math.isfinite(value) and value >= 0, reason)
    return value


def schedule_sensitivities() -> dict[str, float]:
    """Reproduce the three pre-data projections, excluding new effect overhead."""
    _, groups, rows = registered_schedule()
    frozen = math.fsum(row["expected_seconds"] for row in rows.values())
    volume_matched = math.fsum(
        row["expected_seconds"] * (1.391 if row["arm"] == "short" else 1.063)
        for row in rows.values()
    )
    return {
        "frozen_seconds": frozen + len(groups) * SETUP + ALLOWANCE,
        "k4_inflation_seconds": frozen * 1.391046343 + len(groups) * 334.944495 + ALLOWANCE,
        "volume_matched_seconds": volume_matched + len(groups) * MEASURED_SETUP + ALLOWANCE,
    }


def admit(
    registry: dict,
    *,
    ledger_actual: int,
    progress: dict[str, EpisodeProgress],
    setup_seconds: float = MEASURED_SETUP,
    effect_overhead_seconds: float | None = None,
    effect_overhead_record: Path | None = None,
    active_job: ActiveJob | None = None,
    workflow_elapsed: float = 0,
) -> dict:
    """Decide budget eligibility for the next unstarted ID at either recheck.

    Inputs are reconciled ledger charges and episode dispositions from the caller.
    Verified timeline boundaries are on the same observer clock. The active job's
    charge is absent from the committed ledger. A fresh job for an unstarted suffix
    incurs setup at its next dispatch recheck. Continuation eligibility and evidence
    verification belong to the Phase H caller.
    """
    validate_registry(registry)
    require(type(ledger_actual) is int and ledger_actual >= 0, "k6-ledger-actual")
    setup = seconds(setup_seconds, "k6-setup-seconds")
    workflow = seconds(workflow_elapsed, "k6-workflow-elapsed")
    if effect_overhead_record is not None:
        require(effect_overhead_seconds is None, "k6-ambiguous-effect-overhead")
        effect_overhead_seconds = read_effect_overhead(effect_overhead_record)
    if effect_overhead_seconds is not None:
        seconds(effect_overhead_seconds, "k6-effect-overhead-seconds")

    _, groups, frozen = registered_schedule()
    require(progress.keys() <= frozen.keys(), "k6-progress-identity")
    pending = [
        (group, [key for key in group["episodes"] if key not in progress]) for group in groups
    ]
    pending = [(group, keys) for group, keys in pending if keys]
    k6_rows = {row["episode"]: row for row in registry["episodes"]}
    measured: dict[str, float] = {}
    dispositions = {}
    for key in frozen:
        if key not in progress:
            continue
        entry = progress[key]
        require(
            entry.disposition
            in ("completed", "failed", "invalid", "failed-to-start", "censored", "unsupported")
            and type(entry.verified) is bool,
            "k6-progress-disposition",
        )
        dispositions[key] = entry.disposition
        duration = entry.measured_seconds()
        if key in k6_rows and duration is not None:
            unit = k6_rows[key]["cost_unit"]
            measured[unit] = max(measured.get(unit, 0), duration)

    live_elapsed = 0.0
    if active_job is not None:
        live_elapsed = seconds(active_job.live_elapsed, "k6-live-elapsed")
        require(live_elapsed <= workflow, "k6-active-job-clocks")

    remaining = [key for _, keys in pending for key in keys]
    effect_ids = {key for key, row in k6_rows.items() if row["workload"] == "COOPERATING_EFFECT"}
    expected_by_episode = {}
    for key in remaining:
        unit = k6_rows[key]["cost_unit"] if key in k6_rows else None
        if unit is not None and unit in measured:
            expected_by_episode[key] = measured[unit]
        elif key not in effect_ids:
            expected_by_episode[key] = frozen[key]["expected_seconds"]
        elif effect_overhead_seconds is not None:
            expected_by_episode[key] = frozen[key]["expected_seconds"] + effect_overhead_seconds

    missing_costs = len(expected_by_episode) != len(remaining)
    missing_effect = missing_costs or (
        effect_overhead_seconds is None and bool(remaining) and remaining[0] in effect_ids
    )
    active_has_pending = (
        any(group["job"] == active_job.group for group, _ in pending) if active_job else False
    )
    setup_jobs = len(pending) - int(active_has_pending)
    actual = ledger_actual + live_elapsed
    total = (
        None
        if missing_costs
        else (actual + math.fsum(expected_by_episode.values()) + setup_jobs * setup + ALLOWANCE)
    )
    result = {
        "admit": False,
        "episode": remaining[0] if remaining else None,
        "group": pending[0][0]["job"] if pending else None,
        "actual_seconds": actual,
        "expected_total_seconds": total,
        "setup_jobs": setup_jobs,
        "expected_episode_seconds": expected_by_episode,
        "dispositions": dispositions,
        "unperformed": {},
        "reason": None,
    }
    if not pending:
        return dict(result, reason="no-unstarted-episodes")
    group, group_ids = pending[0]
    starting_job = active_job is None

    reason = None
    affected = remaining
    if missing_effect:
        reason = "missing-effect-overhead"
    elif total is not None and total > CAP:
        reason = "unfunded-full-schedule"
    elif starting_job and ledger_actual + CEILING > CAP:
        reason = "dispatch-reservation"
    elif active_job is not None and active_job.group != group["job"]:
        reason = "next-episode-outside-active-group"
    else:
        episode = group_ids[0]
        bound = frozen[episode]["full_window_reservation_seconds"] + DIAGNOSTICS + CLOSEOUT
        need = bound + (setup if starting_job else 0)
        if workflow + need > WORKFLOW_LIMIT:
            reason, affected = "workflow-episode-fit", group_ids
        elif live_elapsed + need > CEILING:
            reason, affected = "live-job-episode-fit", group_ids

    if reason is not None:
        return dict(result, reason=reason, unperformed=dict.fromkeys(affected, reason))
    return dict(result, admit=True)
