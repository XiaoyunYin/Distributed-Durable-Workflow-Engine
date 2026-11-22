"""Assemble smoke inputs and independently check the two public bundle replays."""

from __future__ import annotations

import hashlib
import json
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

from dur053_k6_b_smoke import require
from dur053_k6_g_registration import utc

Json = dict[str, Any]
LABELS = {"predicate_partial:G2b", "slice_not_implemented:G2b"}
FENCE_ROLES = {"worker", "relay", "decision", "allocator"}


def canonical(value: Any) -> bytes:
    return compact(value) + b"\n"


def compact(value: Any, *, sort_keys: bool = True) -> bytes:
    return json.dumps(
        value, sort_keys=sort_keys, separators=(",", ":"), ensure_ascii=False
    ).encode()


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def wire(raw: bytes) -> Json:
    return {"length": len(raw), "sha256": sha(raw)}


def select_cells(registry: Json) -> list[Json]:
    return [
        min(
            (row for row in registry["episodes"] if row["workload"] == workload),
            key=lambda row: (sum(row["window_seconds"].values()), row["ordinal"]),
        )
        for workload in ("PURE", "COOPERATING_EFFECT")
    ]


def make_plan(binding: Json, seed: int, effect: bool) -> tuple[Json, dict[str, bytes], Json]:
    sealed = utc()
    windows = []
    requests = {}
    allocator_plan: Json = {"namespace": binding["namespace"], "operations": []}
    for window in ("warmup", "measured"):
        run_id = f"g-{window}"
        key = f"dur053-{run_id}-{seed}-000000"
        node = "dur053.cooperating" if effect else "pure.echo"
        body = {
            "definition_id": "d2a-" + node,
            "definition_version": 1,
            "initial_node_id": node,
            "initial_input": {} if effect else 7,
            "payload": {},
        }
        request = canonical(
            {**body, "namespace": binding["namespace"], "submission_key": key, "workflow_id": key}
        )
        requests[key] = request
        resource = key + "-resource"
        if effect:
            allocator_plan["operations"].append(
                {
                    "submission_key": key,
                    "definition_id": body["definition_id"],
                    "definition_version": 1,
                    "node_id": node,
                    "iteration": 0,
                    "expected_revision": "0",
                    "resource": resource,
                    "logical_key": key + "-effect",
                    "arguments": {"cell": "medium-12"},
                }
            )
        windows.append(
            {
                "name": window,
                "load_run_id": run_id,
                "submissions": [
                    {
                        "sequence": 0,
                        "scheduled_utc": sealed,
                        "submission_key": key,
                        "body_sha256": sha(compact(body, sort_keys=False)),
                        "request": wire(request),
                        "body": body,
                        "target": resource if effect else node,
                        "expected": {} if effect else 7,
                    }
                ],
            }
        )
    plan = {
        "schema": "dur053.k6-plan.v1",
        "episode": binding["episode"],
        "namespace": binding["namespace"],
        "reset_id": binding["reset_id"],
        "sealed_utc": sealed,
        "fault_mode": "control",
        "hold_deadline_utc": None,
        "windows": windows,
    }
    return plan, requests, allocator_plan


def write_fence(path: Path, binding: Json, admission_stopped: dict[str, str]) -> None:
    require(set(admission_stopped) == FENCE_ROLES, "G_FENCE_BEFORE_DRAIN")
    temporary = path.with_suffix(".pending")
    temporary.write_bytes(canonical(binding))
    temporary.replace(path)


def replay(
    directory: Path, live: bytes, source: str, offline: Callable[[Path, str], tuple[int, bytes]]
) -> None:
    good = directory / "bundle"
    (directory / "checker-live.json").write_bytes(live)
    code, result = offline(good, "offline")
    (directory / "checker-offline.json").write_bytes(result)
    require(code == 3, f"G_OFFLINE_EXIT_{code}")
    require(live == result, "G_CANONICAL_PARITY")
    positive = json.loads(result)
    require(positive["findings"] == [], "G_LIVE_CHECKER_FINDINGS")
    require(
        len(positive["labels"]) == 13
        and all(label["reason"] in LABELS for label in positive["labels"]),
        "G_CHECKER_LABELS",
    )

    negative = directory / "missing-source"
    shutil.copytree(good, negative)
    producer_result = negative / "producer-result.json"
    shutil.copyfile(directory / "producer-result.json", producer_result)
    (negative / source).unlink()
    # Drop the corresponding index entry so the test reaches semantic completeness.
    index_path = negative / "index.json"
    index = json.loads(index_path.read_bytes())
    index["files"] = [entry for entry in index["files"] if entry["path"] != source]
    index_path.write_bytes(canonical(index))
    code, output = offline(negative, "missing")
    (directory / "checker-missing-source.json").write_bytes(output)
    require(code == 1, f"G_MISSING_EXIT_{code}")
    verdict = json.loads(output)
    require(
        verdict["findings"]
        == [
            {
                "predicate": "RelationIntegrity",
                "code": "required_proof_missing:" + source,
                "source": source,
                "witness": None,
            }
        ],
        "G_EXACT_MISSING_SOURCE_REASON",
    )
    require(verdict["completeness"] == "FAIL", "G_MISSING_SOURCE_COMPLETENESS")
    require(
        json.loads(producer_result.read_bytes())["status"] == "PASS", "G_PRODUCER_PASS_RETAINED"
    )
