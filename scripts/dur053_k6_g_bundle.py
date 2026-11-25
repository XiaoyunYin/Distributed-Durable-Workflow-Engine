"""Assemble smoke inputs and independently check the two public bundle replays."""

from __future__ import annotations

import hashlib
import json
import shutil
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from dur053_k6_b_smoke import require
from dur053_k6_g_registration import utc

Json = dict[str, Any]
FENCE_ROLES = {"worker", "relay", "decision", "allocator"}
BROKER_PATH = "broker/positions.jsonl"
CONTROL_PATH = "fault/control.json"
STALE_PATH = "fault/stale-challenge.json"
IDLE_RELATIONS = (
    "activity_attempts",
    "attempt_result_evidence",
    "event_inbox",
    "outbox",
    "outbox_publications",
    "transition_history",
    "workflow_executions",
)


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
            (
                row
                for row in registry["episodes"]
                if row["cell"] == "medium-16" and row["workload"] == workload
            ),
            key=lambda row: row["ordinal"],
        )
        for workload in ("PURE", "COOPERATING_EFFECT")
    ]


def make_plan(
    binding: Json, seed: int, effect: bool, *, idle: bool = False
) -> tuple[Json, dict[str, bytes], Json]:
    sealed = utc()
    windows: list[Json] = []
    requests = {}
    allocator_plan: Json = {"namespace": binding["namespace"], "operations": []}
    for window in ("warmup", "measured"):
        run_id = f"g-{window}"
        if idle:
            windows.append({"name": window, "load_run_id": run_id, "submissions": []})
            continue
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
        proposal = None
        if effect:
            proposal = {
                "node_id": node,
                "iteration": 0,
                "expected_revision": "0",
                "logical_key": key + "-effect",
                "arguments": {"cell": "medium-16"},
            }
            allocator_plan["operations"].append(
                {
                    "submission_key": key,
                    "definition_id": body["definition_id"],
                    "definition_version": 1,
                    "resource": resource,
                    **proposal,
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
                        **({"effect": proposal} if proposal is not None else {}),
                    }
                ],
            }
        )
    stale = binding["episode"].startswith("hook-5-")
    plan = {
        "schema": "dur053.k6-plan.v1",
        "episode": binding["episode"],
        "namespace": binding["namespace"],
        "reset_id": binding["reset_id"],
        "sealed_utc": sealed,
        "fault_mode": "injected" if stale else "control",
        "hold_deadline_utc": utc(datetime.now(UTC) + timedelta(minutes=10)) if stale else None,
        "windows": windows,
    }
    return plan, requests, allocator_plan


def write_fence(path: Path, binding: Json, admission_stopped: dict[str, str]) -> None:
    require(set(admission_stopped) == FENCE_ROLES, "G_FENCE_BEFORE_DRAIN")
    temporary = path.with_suffix(".pending")
    temporary.write_bytes(canonical(binding))
    temporary.replace(path)


def read_json(path: Path) -> Json:
    return json.loads(path.read_bytes())  # type: ignore[no-any-return]


def read_lines(path: Path) -> list[Json]:
    return [json.loads(line) for line in path.read_bytes().splitlines()]


def idle_sources(bundle: Path, sessions: list[str]) -> Json:
    plan, arrivals = (read_json(bundle / name) for name in ("plan.json", "arrivals.json"))
    require([w["submissions"] for w in plan["windows"]] == [[], []], "G_IDLE_PLAN")
    require(arrivals["rows"] == [], "G_IDLE_ARRIVALS")
    inventory = {
        row["relation"]: row for row in read_json(bundle / "engine/snapshot.json")["relations"]
    }
    indexed = {row["path"]: row for row in read_json(bundle / "index.json")["files"]}
    relations = {}
    for name in IDLE_RELATIONS:
        relation, path = f"engine.{name}", f"engine/engine.{name}.jsonl"
        require(
            path in indexed and relation in inventory and (bundle / path).is_file(),
            "G_IDLE_SOURCE_ABSENT",
        )
        descriptor = inventory[relation]
        require(
            (bundle / path).read_bytes() == b""
            and descriptor["count"] == 0
            and descriptor["length"] == 0
            and descriptor["sha256"] == sha(b"")
            and all(descriptor[key] == indexed[path][key] for key in ("length", "sha256")),
            "G_IDLE_SOURCE_NOT_EMPTY",
        )
        relations[path] = descriptor
    _, *positions, seal = read_lines(bundle / BROKER_PATH)
    require(
        bool(positions)
        and seal["mode"] == "terminal"
        and all(
            p["low"] == p["end"] == 0 and p["committed"] in (0, None)
            for row in positions
            for p in row["data"]["partitions"]
        ),
        "G_IDLE_BROKER_TRAFFIC_OR_UNSEALED",
    )
    ledgers = {}
    for session in sessions:
        path = f"witness/{session}.jsonl"
        require(path in indexed and (bundle / path).is_file(), "G_IDLE_LEDGER_ABSENT")
        _, *rows, ledger_seal = read_lines(bundle / path)
        require(
            bool(rows)
            and ledger_seal["mode"] in ("terminal", "receiver_final")
            and all(
                row["data"]["kind"] in ("session", "idle_probe", "lease_renewal") for row in rows
            ),
            "G_IDLE_LEDGER_TRAFFIC_OR_UNSEALED",
        )
        ledgers[path] = {"traffic_rows": 0, "seal": ledger_seal}
    return {
        "offers": 0,
        "arrivals": 0,
        "empty_relations": relations,
        "worker_ledgers": ledgers,
        "broker_seal": seal,
    }


def utc_ns(value: str) -> int:
    seconds, dot, fraction = value.removesuffix("Z").partition(".")
    return int(datetime.fromisoformat(seconds + "+00:00").timestamp()) * 10**9 + (
        int(fraction.ljust(9, "0")) if dot else 0
    )


def broker_mark(sample: Json) -> Json:
    return next(mark for mark in sample["marks"] if mark["path"] == BROKER_PATH)


def quiet_summary(bundle: Path) -> Json:
    barrier = read_json(bundle / "barrier.json")
    samples = read_json(bundle / "samples/external.json")["rows"]
    generations = []
    for attempt in barrier["attempts"]:
        generation = attempt["generation"]
        group = [row for row in samples if row["generation"] == generation]
        pre, post = (
            next(row for row in group if row["phase"] == phase) for phase in ("pre", "post")
        )
        window = [row for row in group if row["sequence"] <= pre["sequence"]]
        quiet_start = window[0]
        for previous, row in zip(window, window[1:], strict=False):
            if (
                not previous["terminal"]
                or [m["activity"] for m in previous["marks"]]
                != [m["activity"] for m in row["marks"]]
                or previous["databases"] != row["databases"]
            ):
                quiet_start = row
        elapsed = min(
            utc_ns(pre["at"]["utc"]) - utc_ns(quiet_start["at"]["utc"]),
            pre["at"]["monotonic_ns"] - quiet_start["at"]["monotonic_ns"],
        )
        largest_gap = max(
            max(
                utc_ns(b["at"]["utc"]) - utc_ns(a["at"]["utc"]),
                b["at"]["monotonic_ns"] - a["at"]["monotonic_ns"],
            )
            for a, b in zip(group, group[1:], strict=False)
        )
        require(elapsed >= 30 * 10**9 and largest_gap <= 5 * 10**9, "G_QUIET_TIMING")
        generations.append(
            {
                "generation": generation,
                "reasons": attempt["reasons"],
                "finished": attempt["finished"],
                "quiet_start": quiet_start["at"],
                "quiet_ns": elapsed,
                "largest_sample_gap_ns": largest_gap,
                "pre": {"broker": broker_mark(pre), "databases": pre["databases"], "at": pre["at"]},
                "post": {
                    "broker": broker_mark(post),
                    "databases": post["databases"],
                    "at": post["at"],
                },
                "snapshots": {db["db"]: db["snapshot_sha256"] for db in attempt["databases"]},
            }
        )
    return {
        "deadline_utc": barrier["deadline_utc"],
        "selected_generation": barrier["selected_generation"],
        "generations": generations,
    }


def verify_retake(quiet: Json, receipts: list[Json], fenced_utc: str) -> None:
    require(
        quiet["selected_generation"] == 2 and len(quiet["generations"]) == 2, "G_RETAKE_SELECTION"
    )
    raced, retake = quiet["generations"]
    before_pre, mid_cut = receipts
    require(
        raced["reasons"] == ["CUT_RACED:external"] and retake["reasons"] == [], "G_RETAKE_REASON"
    )
    require(
        utc_ns(fenced_utc) <= utc_ns(before_pre["started_utc"])
        and utc_ns(before_pre["observer"]["at"]["utc"])
        <= utc_ns(before_pre["started_utc"])
        <= utc_ns(before_pre["acked_utc"])
        <= utc_ns(raced["quiet_start"]["utc"])
        and utc_ns(raced["pre"]["at"]["utc"])
        <= utc_ns(mid_cut["started_utc"])
        <= utc_ns(mid_cut["acked_utc"])
        <= utc_ns(raced["post"]["at"]["utc"])
        and utc_ns(raced["post"]["at"]["utc"]) <= utc_ns(retake["quiet_start"]["utc"])
        and utc_ns(retake["finished"]["utc"]) <= utc_ns(quiet["deadline_utc"]),
        "G_RETAKE_WRITE_TIMING",
    )
    require(
        broker_mark(before_pre["observer"])["sequence"] + 1
        == before_pre["frame_sequence"]
        == raced["pre"]["broker"]["sequence"]
        and before_pre["end"] == mid_cut["offset"]
        and raced["pre"]["broker"]["sequence"] + 1
        == mid_cut["frame_sequence"]
        == raced["post"]["broker"]["sequence"]
        and raced["post"]["broker"]["digest"]
        == retake["pre"]["broker"]["digest"]
        == retake["post"]["broker"]["digest"],
        "G_RETAKE_BROKER_BINDING",
    )


def replay(
    directory: Path,
    live: bytes,
    source: str,
    offline: Callable[[Path, str], tuple[int, bytes]],
    *,
    idle: bool,
) -> None:
    good = directory / "bundle"
    (directory / "checker-live.json").write_bytes(live)
    code, result = offline(good, "offline")
    (directory / "checker-offline.json").write_bytes(result)
    require(code == (3 if idle else 0), f"G_OFFLINE_EXIT_{code}")
    require(live == result, "G_CANONICAL_PARITY")
    positive = json.loads(result)
    require(
        positive["confirmation"]
        == (
            {"status": "pending", "reason": "no_positive_population"}
            if idle
            else {"status": "confirmed", "reason": "cell_predicate_satisfied"}
        ),
        "G_CHECKER_CONFIRMATION",
    )
    manifest = json.loads((good / "manifest.json").read_bytes())
    require(manifest["cell"] in {"medium-16", "hook-5"}, "G_CHECKER_CELL")
    require(
        positive["findings"] == []
        and positive["relation_integrity"] == "covered/pass"
        and positive["status"] == ("incomplete" if idle else "passed")
        and positive["completeness"] == positive["safety"] == ("incomplete" if idle else "PASS"),
        "G_LIVE_CHECKER_FINDINGS",
    )
    ids = [f"I{i}" for i in range(1, 14)]
    labels = positive["labels"]
    registered = {"I1", "I2", "I4", "I6", "I11"}
    if manifest["cell"] == "hook-5":
        registered.add("I3")
    if source == "witness/effect-ingress.jsonl":
        registered.update(("I8", "I10"))
    covered = [] if idle else [identity for identity in ids if identity in registered]
    require(
        [label["id"] for label in labels] == ids
        and {label["id"] for label in labels if label["registered"]} == registered
        and positive["invariants_covered"] == covered
        and positive["invariants_not_covered"] == [label for label in ids if label not in covered],
        "G_CHECKER_LABELS",
    )
    for label in labels:
        if label["id"] in registered:
            require(label["registered"], "G_CHECKER_LABELS")
            outcome = "NOT COVERED" if idle else "covered/pass"
            reason = "no_positive_population" if idle else "predicate_satisfied"
        else:
            outcome = "NOT COVERED"
            reason = "outside_registry_ceiling"
        require((label["outcome"], label["reason"]) == (outcome, reason), "G_CHECKER_LABELS")

    replay_missing(directory, good, source, offline, "source")
    if source == "witness/topology.json" and not idle:
        proof = STALE_PATH if manifest["cell"] == "hook-5" else CONTROL_PATH
        replay_missing(directory, good, proof, offline, "confirmation")


def replay_missing(
    directory: Path,
    good: Path,
    source: str,
    offline: Callable[[Path, str], tuple[int, bytes]],
    name: str,
) -> None:
    negative = directory / f"missing-{name}"
    shutil.copytree(good, negative)
    producer_result = negative / "producer-result.json"
    shutil.copyfile(directory / "producer-result.json", producer_result)
    (negative / source).unlink()
    # Drop the corresponding index entry so the test reaches semantic completeness.
    index_path = negative / "index.json"
    index = json.loads(index_path.read_bytes())
    index["files"] = [entry for entry in index["files"] if entry["path"] != source]
    index_path.write_bytes(canonical(index))
    code, output = offline(negative, "missing" if name == "source" else "missing-confirmation")
    (directory / f"checker-missing-{name}.json").write_bytes(output)
    require(code == 1, f"G_MISSING_EXIT_{code}")
    verdict = json.loads(output)
    require(
        verdict["findings"]
        == [
            {
                "predicate": "RelationIntegrity",
                "code": "required_proof_missing:" + source,
                "kind": "missing",
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
