#!/usr/bin/env python3
"""Reproduce Amendment B's planning arithmetic, never a pilot result."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

CEILING = 18900
SETUP = 300


def plan() -> dict:
    families: dict[str, list[tuple[str, float, int]]] = {
        "K3": [
            ("short-pilot", 824.4, 4),
            ("medium-pilot", 2820, 4),
            ("CPU-pilot", 1104, 4),
            ("long-initial", 9195, 1),
            ("long-repeats", 11385, 3),
        ],
        "K4": [(f"short-core-{c}-pair-{p}", 660.2, 2) for c in range(7) for p in range(5)]
        + [(f"long-core-{c}-pair-{p}", 1110, 2) for c in range(7) for p in range(5)]
        + [(f"short-coverage-{c}-{p}", 330.1, 1) for c in range(7) for p in range(3)],
        "K6": [(f"hook-{c}-{p}", 330.1, 1) for c in range(6) for p in range(5)]
        + [(f"medium-{c}-{p}", 335, 1) for c in range(17) for p in range(5)],
        "K5": [(f"step-medium-{c}-{p}", 515, 1) for c in range(3) for p in range(3)]
        + [(f"step-cpu-{c}-{p}", 511, 1) for c in range(3) for p in range(3)]
        + [(f"other-medium-{c}-{p}", 335, 1) for c in range(18) for p in range(3)]
        + [(f"other-cpu-{c}-{p}", 331, 1) for c in range(18) for p in range(3)],
        "K7": [(f"long-pair-{p}", 1110, 2) for p in range(10)],
    }
    jobs = []
    for family, units in families.items():
        current = {"family": family, "expected_seconds": SETUP, "episodes": 0, "units": []}
        for name, seconds, count in units:
            amount = seconds + 60 * count
            if current["units"] and (
                current["expected_seconds"] + amount > CEILING - 600
                or (
                    family == "K3"
                    and (name == "long-initial" or current["units"] == ["long-initial"])
                )
            ):
                jobs.append(current)
                current = {"family": family, "expected_seconds": SETUP, "episodes": 0, "units": []}
            current["expected_seconds"] += amount
            current["episodes"] += count
            current["units"].append(name)
        jobs.append(current)
    for job in jobs:
        job["expected_seconds"] = round(job["expected_seconds"], 1)
    return {
        "ceiling_seconds": CEILING,
        "jobs": jobs,
        "campaign_episodes": sum(j["episodes"] for j in jobs),
        "expected_family_seconds": round(sum(j["expected_seconds"] for j in jobs), 1),
        "validation_allowance_seconds": 1710,
        "one_k3a_smoke_reservation_seconds": 3600,
        "expected_total_seconds": round(sum(j["expected_seconds"] for j in jobs) + 1710 + 3600, 1),
    }


def plan_b2() -> dict:
    """B.2 adds a setup, without rewriting the Amendment B registration."""
    result = plan()
    result["jobs"][:3] = [
        {
            "family": "K3",
            "job": "k3-pilot-1a",
            "expected_seconds": 4424.4,
            "episodes": 8,
            "units": ["short-pilot", "medium-pilot"],
        },
        {
            "family": "K3",
            "job": "k3-pilot-1b",
            "expected_seconds": 1644,
            "episodes": 4,
            "units": ["CPU-pilot"],
        },
        {
            "family": "K3",
            "job": "k3-pilot-2",
            "expected_seconds": 9555,
            "episodes": 1,
            "units": ["long-initial"],
        },
        {
            "family": "K3",
            "job": "k3-pilot-3",
            "expected_seconds": 11865,
            "episodes": 3,
            "units": ["long-repeats"],
        },
    ]
    result["expected_family_seconds"] = round(sum(j["expected_seconds"] for j in result["jobs"]), 1)
    result["expected_total_seconds"] = round(result["expected_family_seconds"] + 1710 + 3600, 1)
    result["calibration_note"] = (
        "Expected schedule preserves the registered validation allowance; calibration has no "
        "measured expectation. Admission separately reserves its full 7920 s worst case. "
        "Actual calibration time spends the ledger; expected durations never authorize admission."
    )
    return result


def observed_plan(arms: dict, cpu_duration: float) -> dict:
    """Recheck every registered family using frozen b/r/L, without authorizing it."""
    durations = {"short": 0.1, "medium": 5, "long": 45, "cpu": cpu_duration}
    regular, steps, timeout = {}, {}, {}
    for arm, duration in durations.items():
        r, L = arms[arm]["r"], arms[arm]["L"]
        window = max(180, (20 if arm == "long" else 60) / (0.5 * r))
        regular[arm] = 120 + window + duration + 30
        v = max(120, 4 * duration)
        steps[arm] = 120 + 3 * v + max(0, 60 - 1.75 * r * v) / (0.25 * r) + duration + 30
        timeout[arm] = max(600, 10 * L, 4 * duration)
    # Preserve Amendment B.2's cells, minimum counts, pair boundaries and family order.
    source = plan_b2()
    units = []
    for job in source["jobs"]:
        for name in job["units"]:
            family = job["family"]
            if family == "K3":
                arm = {"CPU": "cpu"}.get(name.split("-")[0], name.split("-")[0])
                indices = (
                    [0]
                    if name == "long-initial"
                    else ([1, 2, 3] if name == "long-repeats" else range(4))
                )
                d, r = durations[arm], arms[arm]["r"]
                seconds = sum(
                    120
                    + 200 / (1 / max(1, d) if i == 0 and arm != "cpu" else 1 if i == 0 else 0.5 * r)
                    + d
                    + 30
                    for i in indices
                )
                count = len(indices)
                reserved = sum(
                    120
                    + 200 / (1 / max(1, d) if i == 0 and arm != "cpu" else 1 if i == 0 else 0.5 * r)
                    + 1200
                    + 60
                    for i in indices
                )
            else:
                arm = (
                    "cpu"
                    if "cpu" in name
                    else (
                        "long"
                        if "long" in name
                        else "short"
                        if "short" in name or "hook" in name
                        else "medium"
                    )
                )
                count = 2 if "pair" in name else 1
                seconds = count * (steps[arm] if "step" in name else regular[arm])
                # Confirmation extensions and complete reconciliation timeout are reserved.
                reserved = (
                    seconds
                    - count * (durations[arm] + 30)
                    + count * (2 * arms[arm]["L"] + timeout[arm] + 60)
                )
            units.append(
                {
                    "family": family,
                    "unit": name,
                    "episodes": count,
                    "expected_seconds": seconds + count * 60,
                    "full_window_reservation_seconds": reserved,
                    "fits_fresh_job": reserved + 300 + 600 <= CEILING,
                }
            )
    return {
        "schema": "dur053.observed-schedule.v1",
        "busy_time_sensitivity": {
            arm: {"initial_b": values["b"], "max_repeat_b": values["max_repeat_b"]}
            for arm, values in arms.items()
            if "max_repeat_b" in values
        },
        "units": units,
        "campaign_episodes": sum(x["episodes"] for x in units),
        "reconciliation_timeout_seconds": timeout,
        "note": "Admission still requires full job packing, ledger and history gates. "
        "Calibration has no invented expected duration; actual ledger time includes it.",
    }


def plan_b6(repository: Path, frozen: dict | None = None, *, fallback: bool = False) -> dict:
    """Budget disclosure, not admission. All registered family cells are retained."""
    from dur053_k3_policy import committed_ledger
    from dur053_pilot import EVIDENCE, PLANNING_RATES, committed_bytes, initial_dependency
    from dur053_sustained import (
        ROOTS,
        duration,
        harness_capacity_failure,
        operating_record,
        registration,
        reservation,
    )

    ledger = committed_ledger(
        repository / "experiments/portfolio/k8s-recovery/live-time-ledger.json", repository
    )
    charged = sum(row["charged_seconds"] for row in ledger["entries"])
    arms = {a: {"b": 4 / r, "r": r, "L": 0} for a, r in PLANNING_RATES.items()}
    assumptions = []
    # Initial references are disclosed; CPU duration is not a new measured prediction.
    cpu_duration = 1.0
    for path in (repository / EVIDENCE).glob("*/*/manifest.json"):
        m = json.loads(committed_bytes(repository, path))
        for a in arms:
            if f"{a}-0" in m["completed"]:
                initial = initial_dependency(path.parent, a, repository)
                arms[a].update(b=initial["b"], r=initial["r"])
                if a == "cpu":
                    cpu_duration = max(0.514, initial["b"])
    from dur053_payload import materialize
    from dur053_sustained import stationarity

    long_gates = []
    completed_v1 = set()
    for path in (repository / EVIDENCE).glob("*/*/manifest.json"):
        header = json.loads(committed_bytes(repository, path))
        completed_v1.update(header["completed"])
        if any(f"long-{i}" in header["completed"] for i in (1, 2, 3)):
            with materialize(path.parent, repository) as root:
                long_gates.extend(
                    stationarity(root / f"long-{i}", offline=True)
                    for i in (1, 2, 3)
                    if f"long-{i}" in header["completed"]
                )
    # B.6.1: offline 4(a)/(b) disclosures do not trigger additional jobs.
    long_triggered = len(long_gates) == 3 and any(not g["pass"] for g in long_gates)
    pilots = []
    for arm in ("short", "cpu", *(["long"] if long_triggered else [])):
        reg = registration(repository, arm)
        selected = (frozen or {}).get(arm, {}).get("passing_version") or (3 if fallback else 2)
        arms[arm]["r"] = operating_record(reg, selected)["r"]
        for v in range(2, selected + 1):
            rate = operating_record(reg, v)["q"]
            # No measured expectation for calibration: its entire bound is retained.
            expected = (
                300
                + (2640 if arm == "cpu" else 0)
                + 3 * (120 + duration(rate) + (cpu_duration if arm == "cpu" else 0.1) + 30 + 60)
            )
            if arm == "long":
                for i in (1, 2, 3):
                    pilots.append(
                        {
                            "family": "K3",
                            "job": f"k3-pilot-{'4' if v == 2 else '5'}c{i}",
                            "expected_seconds": 300 + 120 + duration(rate) + 45 + 30 + 60,
                            "episodes": 1,
                            "full_admission_seconds": reservation(rate, repeats=1),
                            "rate_precision": operating_record(reg, v),
                        }
                    )
            else:
                pilots.append(
                    {
                        "family": "K3",
                        "job": f"k3-pilot-{'4' if v == 2 else '5'}{'b' if arm == 'cpu' else 'a'}",
                        "expected_seconds": expected,
                        "episodes": 3,
                        "full_admission_seconds": reservation(rate, cpu=arm == "cpu"),
                        "rate_precision": operating_record(reg, v),
                    }
                )
    for arm, values in (frozen or {}).items():
        if values.get("r") is not None and values.get("L") is not None:
            arms[arm].update({k: values[k] for k in ("r", "L")})
    schedule = observed_plan(arms, cpu_duration)
    units = []
    for row in schedule["units"]:
        if row["family"] == "K3" and row["unit"] not in {"long-initial", "long-repeats"}:
            continue  # These completed v1 jobs are in charged, never charged again.
        if row["unit"] == "long-initial" and "long-0" in completed_v1:
            continue
        if row["unit"] == "long-repeats" and all(f"long-{i}" in completed_v1 for i in (1, 2, 3)):
            continue
        row["arm"] = (
            "cpu"
            if "cpu" in row["unit"].lower()
            else "long"
            if "long" in row["unit"]
            else "short"
            if "short" in row["unit"] or "hook" in row["unit"]
            else "medium"
        )
        if row["family"] != "K3" and (not frozen or frozen.get(row["arm"], {}).get("L") is None):
            row.update(full_window_reservation_seconds=None, fits_fresh_job=None)
        units.append(row)
    jobs = list(pilots)
    current = None
    for row in units:
        if (
            current is None
            or current["family"] != row["family"]
            or current["expected_seconds"] + row["expected_seconds"] > CEILING - 600
            or row["unit"] == "long-repeats"
        ):
            current = {"family": row["family"], "expected_seconds": 300, "episodes": 0, "units": []}
            jobs.append(current)
        current["expected_seconds"] += row["expected_seconds"]
        current["episodes"] += row["episodes"]
        current["units"].append(row["unit"])
    # Remove jobs already recorded after B.6 merges, retaining actual charges.
    completed_jobs = {row["category"] for row in ledger["entries"] if row["status"] == "PASS"}
    jobs = [j for j in jobs if j.get("job") not in completed_jobs]
    capacity_jobs = []
    for evidence in ROOTS.values():
        for path in (repository / evidence).glob("*/*/manifest.json"):
            with materialize(path.parent, repository) as root:
                manifest = json.loads((root / "manifest.json").read_bytes())
                if harness_capacity_failure(root, manifest):
                    capacity_jobs.append(manifest["job"])
    for job in jobs:
        if capacity_jobs.count(job.get("job")) == 1:
            # The failed run is already charged. Reserve the entire same-name
            # replay instead of the ordinary expected-duration estimate.
            job.update(capacity_replay=True, expected_seconds=job["full_admission_seconds"])
    expected = charged + sum(j["expected_seconds"] for j in jobs) + 1710
    assumptions.append(
        "Long rate remains its planning reference until its v1 initial exists; "
        "long offline gate/fallback is conditional."
    )
    assumptions.append(
        "Unfrozen downstream L is unknown: full reservations are unavailable until freeze. "
        "No admission is authorized by this estimate."
    )
    assumptions.append(
        "CPU expected duration uses max(0.514 s, v1 initial b); drift has no measured "
        "expectation, so its full 2640 s bound is included per CPU version."
    )
    return {
        "schema": "dur053.b6-schedule.v1",
        "charged_seconds": charged,
        "harness_capacity_failures": capacity_jobs,
        "cap_seconds": 259200,
        "expected_total_seconds": expected,
        "fits_expected_cap": expected <= 259200,
        "expected_margin_seconds": 259200 - expected,
        "registered_remaining_comparison_episodes": sum(
            r["episodes"] for r in units if r["family"] != "K3"
        ),
        "validation_allowance_seconds": 1710,
        "jobs": jobs,
        "units": units,
        "rates": {a: v["r"] for a, v in arms.items()},
        "assumptions": assumptions,
        "fallback_scenario": fallback,
        "long_offline_gate": long_gates,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--amendment", choices=["b", "b2", "b6"], default="b")
    parser.add_argument("--freeze", type=Path)
    parser.add_argument("--fallback", action="store_true")
    args = parser.parse_args()
    print(
        json.dumps(
            plan_b6(
                Path(__file__).resolve().parents[1],
                json.loads(args.freeze.read_bytes())["arms"] if args.freeze else None,
                fallback=args.fallback,
            )
            if args.amendment == "b6"
            else plan_b2()
            if args.amendment == "b2"
            else plan(),
            indent=2,
        )
    )
