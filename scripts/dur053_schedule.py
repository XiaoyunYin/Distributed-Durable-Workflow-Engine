#!/usr/bin/env python3
"""Reproduce Amendment B's planning arithmetic, never a pilot result."""

from __future__ import annotations

import argparse
import json

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
        "units": units,
        "campaign_episodes": sum(x["episodes"] for x in units),
        "reconciliation_timeout_seconds": timeout,
        "note": "Admission still requires full job packing, ledger and history gates. "
        "Calibration has no invented expected duration; actual ledger time includes it.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--amendment", choices=["b", "b2"], default="b")
    print(json.dumps(plan_b2() if parser.parse_args().amendment == "b2" else plan(), indent=2))
