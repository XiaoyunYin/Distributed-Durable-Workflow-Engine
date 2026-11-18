"""Pin the pre-data K6 registry and its synthetic manifest contract."""

from __future__ import annotations

import copy
import hashlib
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from dur053_k6_registry import (  # noqa: E402
    REGISTRY,
    build_registry,
    json_bytes,
    read_registry,
    synthetic_manifest,
    validate_manifest,
    validate_registry,
)


def test_registered_ids_groups_and_frozen_windows() -> None:
    registry = read_registry()
    assert REGISTRY.read_bytes() == json_bytes(build_registry())
    hooks = [f"hook-{cell}-{repeat}" for cell in range(6) for repeat in range(5)]
    medium = [f"medium-{cell}-{repeat}" for cell in range(17) for repeat in range(5)]
    expected = [hooks + medium[:15], medium[15:60], medium[60:]]
    assert registry["groups"] == [
        {"job": f"k6-{i}", "episodes": episodes} for i, episodes in enumerate(expected, 1)
    ]
    episodes = registry["episodes"]
    assert [row["episode"] for row in episodes] == hooks + medium
    assert len({row["cost_unit"] for row in episodes}) == 24
    for i, row in enumerate(episodes):
        assert row["ordinal"] == i and row["seed"] == 530001 + i
        short = i < 30
        r, L = (14.272201140132513, 2.854) if short else (0.9994137335861809, 52.041)
        assert (row["duration"], row["r"], row["L"], row["arrival_rate"]) == (
            "short" if short else "medium",
            r,
            L,
            r / 2,
        )
        assert row["window_seconds"] == {
            "warmup": 120,
            "W": 180,
            "confirmation": 2 * L,
            "T": 600,
            "reset": 60,
        }
        cell = row["episode"].rsplit("-", 1)[0]
        if cell == "medium-16":
            cell += "-effect" if i >= 113 else "-pure"
        assert row["cost_unit"] == cell
        effect = 90 <= i < 110 or i >= 113
        assert row["workload"] == ("COOPERATING_EFFECT" if effect else "PURE")
    assert [row["intervention"] for row in episodes[-5:]] == ["none"] * 5
    mixes = registry["workload_mixes"]
    assert mixes["PURE"]["positions"] == ["sleep"] * 8 + ["echo", "add"]
    assert mixes["COOPERATING_EFFECT"]["positions"] == ["dur053.cooperating/v1"] * 8 + [
        "echo",
        "add",
    ]
    assert mixes["COOPERATING_EFFECT"]["pre_call_delay_seconds"] == 5


def test_per_arm_label_expectations() -> None:
    registry = read_registry()
    base = {1, 2, 4, 6, 11}
    additions = [set(), {7}, {3}, set(), {3}, set(), {12}, {8, 10}, set(), {8, 10}]
    cell_groups = [
        ["hook-0", "hook-1", "hook-2", "hook-4"],
        ["hook-3"],
        ["hook-5"],
        ["medium-0", "medium-1"],
        ["medium-2", "medium-3"],
        [f"medium-{i}" for i in range(4, 8)],
        [f"medium-{i}" for i in range(8, 12)],
        [f"medium-{i}" for i in range(12, 16)],
        ["medium-16-pure"],
        ["medium-16-effect"],
    ]
    for row, cells, extra in zip(registry["coverage"], cell_groups, additions, strict=True):
        assert row["cost_units"] == cells
        covered = base | extra
        assert row["invariants_covered"] == [f"I{i}" for i in sorted(covered)]
        assert row["invariants_not_covered"] == [f"I{i}" for i in range(1, 14) if i not in covered]
        assert row["requires_confirmed_clock_skew"] is (extra == {12})
        assert {r["cost_unit"] for r in registry["episodes"] if r["arm"] == row["arm"]} == set(
            cells
        )


def test_fixed_topology_and_intervention_backends() -> None:
    registry = read_registry()
    assert registry["topology"] == {
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
    cells = {row["cell"]: row for row in registry["episodes"]}
    container_kill = "chaos-mesh/PodChaos/container-kill"
    assert [cells[f"hook-{i}"]["intervention"] for i in range(6)] == [
        "before_publish",
        "after_broker_ack",
        "attempt_claimed",
        "before_offset_ack",
        "result_recorded",
        "lease_takeover",
    ]
    assert [cells[f"hook-{i}"]["backend"] for i in range(5)] == [container_kill] * 5
    assert cells["hook-5"]["backend"] == "deployed-hook/prolong-predecessor-hold"
    assert [cells[f"medium-{i}"]["backend"] for i in range(17)] == [
        "chaos-mesh/PodChaos/pod-kill",
        container_kill,
        *["chaos-mesh/NetworkChaos/partition"] * 3,
        "deployed-hook/before_publish",
        "docker/restart",
        "docker/restart",
        *["chaos-mesh/TimeChaos"] * 4,
        container_kill,
        container_kill,
        "effect-ingress-relay/pre-forward",
        "effect-ingress-relay/post-commit",
        "none",
    ]
    assert [
        (cells[f"medium-{i}"]["target_role"], cells[f"medium-{i}"]["intervention"])
        for i in range(8, 12)
    ] == [
        ("scheduler", "clock-plus-30s"),
        ("scheduler", "clock-minus-30s"),
        ("worker", "clock-plus-30s"),
        ("worker", "clock-minus-30s"),
    ]


def test_registry_refuses_group_order() -> None:
    registry = build_registry()
    registry["groups"].reverse()
    with pytest.raises(ValueError, match="k6-group-order-membership"):
        validate_registry(registry)


def test_registry_refuses_group_membership() -> None:
    registry = build_registry()
    registry["groups"][0]["episodes"].pop()
    with pytest.raises(ValueError, match="k6-group-order-membership"):
        validate_registry(registry)


@pytest.mark.parametrize("extra", [False, True])
def test_registry_refuses_coverage_row_changes(extra: bool) -> None:
    registry = build_registry()
    if extra:
        registry["coverage"].append(copy.deepcopy(registry["coverage"][0]))
    else:
        registry["coverage"].pop()
    with pytest.raises(ValueError, match="k6-coverage-registration"):
        validate_registry(registry)


@pytest.mark.parametrize("job", ["k6-1", "k6-2", "k6-3"])
def test_manifest_round_trip(job: str) -> None:
    registry = read_registry()
    manifest = synthetic_manifest(registry, job)
    decoded = json.loads(json_bytes(manifest))
    validate_manifest(decoded, registry)
    assert manifest == decoded
    assert manifest["registry_sha256"] == hashlib.sha256(REGISTRY.read_bytes()).hexdigest()
    assert manifest["measurement_claim"] is False and manifest["charged_seconds"] == 0


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [("seed", 530002, "seed"), ("ordinal", False, "ordinal"), ("arm", "unknown", "arm")],
)
def test_manifest_refuses_wrong_episode_identity(field: str, value: object, reason: str) -> None:
    registry = build_registry()
    manifest = synthetic_manifest(registry, "k6-1")
    manifest["episodes"][0][field] = value
    with pytest.raises(ValueError, match=f"k6-episode-{reason}"):
        validate_manifest(manifest, registry)


def test_manifest_refuses_reordered_episodes() -> None:
    registry = build_registry()
    manifest = synthetic_manifest(registry, "k6-1")
    manifest["episodes"].reverse()
    with pytest.raises(ValueError, match="k6-episode-order"):
        validate_manifest(manifest, registry)
