#!/usr/bin/env python3
"""Reproduce Amendment B's planning arithmetic, never a pilot result."""

from __future__ import annotations

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


if __name__ == "__main__":
    print(json.dumps(plan(), indent=2))
