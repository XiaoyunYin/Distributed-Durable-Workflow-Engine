"""Generate and validate the pre-data DUR-053 K6 registration and job manifests."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
from typing import Any

from dur053_schedule import CEILING

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "experiments/portfolio/k8s-recovery"
FREEZE = EVIDENCE / "k3-pilot-v1/freeze.json"
SCHEDULE = EVIDENCE / "amendment-b-schedule.json"
REGISTRY = EVIDENCE / "k6-registry-v1.json"
REGISTRY_SCHEMA = "dur053.k6-registry.v1"
MANIFEST_SCHEMA = "dur053.k6-manifest.v1"
WARMUP = 120
RESET = 60

TOPOLOGY = {
    "kind_nodes": 1,
    "scheduler_pods": 2,
    "worker_pods": 2,
    "worker_slots_per_pod": 2,
    "task_partitions": 8,
    "event_partitions": 8,
    "shared_worker_groups": 1,
    "autoscaling": False,
    "drain_seconds": 30,
    "termination_grace_seconds": 40,
    "operator_deployed": False,
    "chaos_mesh_version": "2.7.2",
}

# Each row preserves a distinct arm in the design table, including rows with the same labels.
BASE = (1, 2, 4, 6, 11)
COVERAGE = (
    ("hooks-0-1-2-4", ("hook-0", "hook-1", "hook-2", "hook-4"), ()),
    ("hook-3", ("hook-3",), (7,)),
    ("hook-5", ("hook-5",), (3,)),
    ("chaos-0-1", ("medium-0", "medium-1"), ()),
    ("chaos-2-3", ("medium-2", "medium-3"), (3,)),
    ("chaos-4-5-6-7", ("medium-4", "medium-5", "medium-6", "medium-7"), ()),
    ("chaos-8-9-10-11", ("medium-8", "medium-9", "medium-10", "medium-11"), (12,)),
    ("effects-12-13-14-15", ("medium-12", "medium-13", "medium-14", "medium-15"), (8, 10)),
    ("control-16-pure", ("medium-16-pure",), ()),
    ("control-16-effect", ("medium-16-effect",), (8, 10)),
)

CONTAINER_KILL = "chaos-mesh/PodChaos/container-kill"
PARTITION = "chaos-mesh/NetworkChaos/partition"
TIME_CHAOS = "chaos-mesh/TimeChaos"
INTERVENTIONS = {
    "hook-0": ("before_publish", CONTAINER_KILL, "relay"),
    "hook-1": ("after_broker_ack", CONTAINER_KILL, "relay"),
    "hook-2": ("attempt_claimed", CONTAINER_KILL, "worker"),
    "hook-3": ("before_offset_ack", CONTAINER_KILL, "worker"),
    "hook-4": ("result_recorded", CONTAINER_KILL, "scheduler"),
    "hook-5": ("lease_takeover", "deployed-hook/prolong-predecessor-hold", "scheduler"),
    "medium-0": ("pod-kill", "chaos-mesh/PodChaos/pod-kill", "worker"),
    "medium-1": ("container-kill", CONTAINER_KILL, "scheduler"),
    "medium-2": ("partition-to-postgresql-lock-held", PARTITION, "scheduler"),
    "medium-3": ("partition-to-postgresql-no-lock", PARTITION, "scheduler"),
    "medium-4": ("partition-to-worker-control", PARTITION, "worker"),
    "medium-5": ("relay-stall", "deployed-hook/before_publish", "relay"),
    "medium-6": ("restart-engine-postgresql", "docker/restart", "engine-postgresql"),
    "medium-7": ("restart-kafka", "docker/restart", "kafka"),
    "medium-8": ("clock-plus-30s", TIME_CHAOS, "scheduler"),
    "medium-9": ("clock-minus-30s", TIME_CHAOS, "scheduler"),
    "medium-10": ("clock-plus-30s", TIME_CHAOS, "worker"),
    "medium-11": ("clock-minus-30s", TIME_CHAOS, "worker"),
    "medium-12": ("kill-before-request", CONTAINER_KILL, "worker"),
    "medium-13": ("kill-after-commit-before-response", CONTAINER_KILL, "worker"),
    "medium-14": ("duplicate-request", "effect-ingress-relay/pre-forward", "effect-ingress-relay"),
    "medium-15": (
        "lost-response-same-key-lookup",
        "effect-ingress-relay/post-commit",
        "effect-ingress-relay",
    ),
    "medium-16": ("none", "none", "none"),
}


def json_bytes(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode("utf-8")


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


def registered_schedule() -> tuple[dict, list[dict], dict[str, dict]]:
    """Join the registered K6/K5 order to its full-precision frozen costs."""
    freeze_bytes = FREEZE.read_bytes()
    freeze = json.loads(freeze_bytes)
    freeze["sha256"] = hashlib.sha256(freeze_bytes).hexdigest()
    observed = freeze["observed_schedule"]
    schedule = json.loads(SCHEDULE.read_bytes())
    groups = [job for job in schedule["jobs"] if job["family"] in ("K6", "K5")]
    frozen_groups = [job for job in observed["jobs"] if job["family"] in ("K6", "K5")]
    require(schedule["ceiling_seconds"] == CEILING, "k6-schedule-ceiling")
    require(
        [(g["family"], g["units"], g["episodes"]) for g in groups]
        == [(g["family"], g["units"], g["episodes"]) for g in frozen_groups],
        "k6-frozen-groups",
    )
    rows = [row for row in observed["units"] if row["family"] in ("K6", "K5")]
    require(
        [row["unit"] for row in rows] == [key for g in groups for key in g["units"]]
        and len({row["unit"] for row in rows}) == len(rows)
        and all(row["episodes"] == 1 for row in rows),
        "k6-frozen-schedule-order",
    )
    units = {row["unit"]: row for row in rows}
    return (
        freeze,
        [
            {"job": f"{family.lower()}-{i}", "episodes": g["units"]}
            for family in ("K6", "K5")
            for i, g in enumerate((g for g in groups if g["family"] == family), 1)
        ],
        units,
    )


def build_registry() -> dict:
    freeze, groups, units = registered_schedule()
    coverage = []
    for arm, cost_units, extra in COVERAGE:
        labels = sorted((*BASE, *extra))
        coverage.append(
            {
                "arm": arm,
                "cost_units": list(cost_units),
                "invariants_covered": [f"I{i}" for i in labels],
                "invariants_not_covered": [f"I{i}" for i in range(1, 14) if i not in labels],
                "requires_confirmed_clock_skew": extra == (12,),
            }
        )
    arms = {unit: row["arm"] for row in coverage for unit in row["cost_units"]}
    episodes = []
    for group in groups[:3]:
        for episode in group["episodes"]:
            cell, repeat = episode.rsplit("-", 1)
            effect = cell in ("medium-12", "medium-13", "medium-14", "medium-15") or (
                cell == "medium-16" and int(repeat) >= 3
            )
            cost_unit = f"{cell}-{'effect' if effect else 'pure'}" if cell == "medium-16" else cell
            duration = units[episode]["arm"]
            frozen = freeze["arms"][duration]
            rate = 0.5 * frozen["r"]
            nominal = 0.1 if duration == "short" else 5
            window = {
                "warmup": WARMUP,
                "W": max(180, 60 / rate),
                "confirmation": 2 * frozen["L"],
                "T": max(600, 10 * frozen["L"], 4 * nominal),
                "reset": RESET,
            }
            require(
                abs(sum(window.values()) - units[episode]["full_window_reservation_seconds"])
                < 1e-9,
                "k6-frozen-window",
            )
            intervention, backend, target = INTERVENTIONS[cell]
            ordinal = len(episodes)
            episodes.append(
                {
                    "episode": episode,
                    "group": group["job"],
                    "cell": cell,
                    "repeat": int(repeat),
                    "ordinal": ordinal,
                    "seed": 530001 + ordinal,
                    "arm": arms[cost_unit],
                    "cost_unit": cost_unit,
                    "duration": duration,
                    "r": frozen["r"],
                    "L": frozen["L"],
                    "arrival_rate": rate,
                    "window_seconds": window,
                    "workload": "COOPERATING_EFFECT" if effect else "PURE",
                    "intervention": intervention,
                    "backend": backend,
                    "target_role": target,
                }
            )
    return {
        "schema": REGISTRY_SCHEMA,
        "freeze_sha256": freeze["sha256"],
        "topology": dict(TOPOLOGY),
        "workload_mixes": {
            "PURE": {"positions": ["sleep"] * 8 + ["echo", "add"], "add_arguments": [2, 3]},
            "COOPERATING_EFFECT": {
                "positions": ["dur053.cooperating/v1"] * 8 + ["echo", "add"],
                "pre_call_delay_seconds": 5,
                "add_arguments": [2, 3],
            },
        },
        "groups": groups[:3],
        "coverage": coverage,
        "episodes": episodes,
    }


def same(value: Any, expected: Any) -> bool:
    """Compare JSON values strictly, including Boolean versus integer fields."""
    return json_bytes(value) == json_bytes(expected)


def validate_registry(value: dict) -> None:
    expected = build_registry()
    require(
        type(value) is dict
        and value.keys() == expected.keys()
        and value["schema"] == REGISTRY_SCHEMA,
        "k6-registry-schema",
    )
    require(same(value["groups"], expected["groups"]), "k6-group-order-membership")
    require(same(value["coverage"], expected["coverage"]), "k6-coverage-registration")
    validate_episodes(value["episodes"], expected["episodes"])
    for key in ("freeze_sha256", "topology", "workload_mixes"):
        require(same(value[key], expected[key]), f"k6-registry-{key}")


def validate_episodes(rows: list, expected: list[dict]) -> None:
    require(type(rows) is list and len(rows) == len(expected), "k6-episode-membership")
    for row, registered in zip(rows, expected, strict=True):
        require(type(row) is dict and row.keys() == registered.keys(), "k6-episode-schema")
        require(row["episode"] == registered["episode"], "k6-episode-order")
        require(same(row["ordinal"], registered["ordinal"]), "k6-episode-ordinal")
        require(same(row["seed"], registered["seed"]), "k6-episode-seed")
        require(row["arm"] == registered["arm"], "k6-episode-arm")
        require(row["cost_unit"] == registered["cost_unit"], "k6-episode-cost-unit")
        for key in registered.keys() - {"episode", "ordinal", "seed", "arm", "cost_unit"}:
            require(same(row[key], registered[key]), f"k6-episode-{key}")


def read_registry() -> dict:
    raw = REGISTRY.read_bytes()
    registry = json.loads(raw)
    validate_registry(registry)
    require(raw == json_bytes(registry), "k6-registry-canonical-bytes")
    return registry


def synthetic_manifest(registry: dict, job: str) -> dict:
    validate_registry(registry)
    require(job in {group["job"] for group in registry["groups"]}, "k6-manifest-job")
    return {
        "schema": MANIFEST_SCHEMA,
        "job": job,
        "registry_sha256": hashlib.sha256(json_bytes(registry)).hexdigest(),
        "measurement_claim": False,
        "charged_seconds": 0,
        "episodes": copy.deepcopy([row for row in registry["episodes"] if row["group"] == job]),
    }


def validate_manifest(value: dict, registry: dict) -> None:
    require(type(value) is dict and type(value.get("job")) is str, "k6-manifest-schema")
    expected = synthetic_manifest(registry, value["job"])
    require(value.keys() == expected.keys(), "k6-manifest-schema")
    for key in expected.keys() - {"episodes"}:
        require(same(value[key], expected[key]), f"k6-manifest-{key}")
    validate_episodes(value["episodes"], expected["episodes"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("generate", "check"))
    args = parser.parse_args()
    if args.command == "generate":
        REGISTRY.write_bytes(json_bytes(build_registry()))
    else:
        read_registry()
