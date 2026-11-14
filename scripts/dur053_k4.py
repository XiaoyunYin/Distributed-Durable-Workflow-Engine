"""Registered K4 arithmetic. Offline only; expected packing never authorizes dispatch."""

from __future__ import annotations

import hashlib
import json
import math
import re
import statistics
from pathlib import Path

from dur053_k3_policy import require
from dur053_pilot import ARMS, EVIDENCE, committed_bytes, finite
from dur053_schedule import CEILING, SETUP, observed_plan
from dur053_sustained import REGISTERED

ROOT = Path(__file__).resolve().parents[1]
FREEZE = EVIDENCE / "freeze.json"
EVIDENCE_ROOT = EVIDENCE.with_name("k4-v1")
LEDGER = EVIDENCE.parent / "live-time-ledger.json"
CORE = (
    "worker rollout",
    "worker scale-down 4 to 2",
    "scheduler rollout",
    "API rollout",
    "worker SIGKILL",
    "scheduler SIGKILL",
    "node drain/PDB eviction",
)
COVERAGE = (
    "relay rollout",
    "relay SIGKILL",
    "API SIGKILL",
    "worker rollback",
    "scheduler rollback",
    "API rollback",
    "relay rollback",
)
RESET = 60
WARMUP = 120
QUIET = 30
DIAGNOSTICS = SETUP  # Existing 300 s diagnostic reserve.
TEARDOWN = 2 * SETUP  # Existing 600 s teardown reserve.
CAP = 259200


def registry() -> list[dict]:
    result = []
    ordinal = 0
    for duration in ("short", "long"):
        for index, event in enumerate(CORE):
            for pair in range(5):
                unit = f"{duration}-core-{index}-pair-{pair}"
                order = ("baseline", "treatment") if ordinal % 2 == 0 else ("treatment", "baseline")
                for position, arm in enumerate(order):
                    result.append(
                        dict(
                            episode=f"{unit}-{arm}",
                            unit=unit,
                            event=event,
                            index=index,
                            kind="core",
                            duration=duration,
                            pair=pair,
                            ordinal=ordinal,
                            seed=530001 + ordinal,
                            arm=arm,
                            position=position,
                        )
                    )
                ordinal += 1
    for index, event in enumerate(COVERAGE):
        for repeat in range(3):
            unit = f"short-coverage-{index}-{repeat}"
            result.append(
                dict(
                    episode=f"{unit}-treatment",
                    unit=unit,
                    event=event,
                    index=index,
                    kind="coverage",
                    duration="short",
                    pair=repeat,
                    ordinal=ordinal,
                    seed=530001 + ordinal,
                    arm="treatment",
                    position=0,
                )
            )
            ordinal += 1
    return result


def frozen(repository: Path = ROOT) -> dict:
    raw = committed_bytes(repository, repository / FREEZE)
    f = json.loads(raw)
    for arm, values in f["arms"].items():
        for key in ("r", "L", "b"):
            finite(values[key], "k4-freeze-finite")
        if arm in REGISTERED:
            r, q = REGISTERED[arm]
            require(
                format(values["r"], ".6f") == r and format(0.5 * values["r"], ".6f") == q,
                "k4-registered-rate",
            )
        for key, check in values.get("registered_check_values", {}).items():
            require(format(values[key], ".6f") == check, "k4-registered-check")
    f["sha256"] = hashlib.sha256(raw).hexdigest()
    return f


def schedule(f: dict) -> dict:
    # One source of formulas, including full reconciliation/confirmation/reset.
    source = observed_plan(f["arms"], max(0.514, f["arms"]["cpu"]["b"]))
    units = [r for r in source["units"] if r["family"] == "K4"]
    registered = [r for r in f["observed_schedule"]["units"] if r["family"] == "K4"]
    require(
        [r["unit"] for r in units] == list(dict.fromkeys(r["unit"] for r in registry())),
        "k4-unit-registry",
    )
    for row, old in zip(units, registered, strict=True):
        require(all(row[k] == old[k] for k in row), "k4-frozen-reservations")
    groups = []
    for row in units:
        if (
            not groups
            or groups[-1]["expected_seconds"] + row["expected_seconds"] > CEILING - TEARDOWN
        ):
            groups.append(
                dict(
                    job=f"k4-{len(groups) + 1}",
                    family="K4",
                    expected_seconds=SETUP,
                    episodes=0,
                    units=[],
                )
            )
        g = groups[-1]
        g["units"].append(row["unit"])
        g["episodes"] += row["episodes"]
        g["expected_seconds"] += row["expected_seconds"]
    old = [j for j in f["observed_schedule"]["jobs"] if j["family"] == "K4"]
    require(
        len(groups) == 5
        and [{k: g[k] for k in j} for g, j in zip(groups, old, strict=True)] == old,
        "k4-frozen-groups",
    )
    return dict(units=units, groups=groups)


def parameters(f: dict, duration: str) -> dict:
    require(duration in ("short", "long"), "k4-duration")
    row = next(r for r in schedule(f)["units"] if r["unit"].startswith(duration))
    a = f["arms"][duration]
    # Extract W and T from the shared observed_plan, rather than clone formulas.
    shared = observed_plan(f["arms"], max(0.514, f["arms"]["cpu"]["b"]))
    return dict(
        rate=0.5 * a["r"],
        L=a["L"],
        warmup=WARMUP,
        W=row["expected_seconds"] / row["episodes"] - WARMUP - ARMS[duration] - QUIET - RESET,
        confirmation=2 * a["L"],
        T=shared["reconciliation_timeout_seconds"][duration],
        reset=RESET,
    )


def job_name(name: str) -> tuple[str, int]:
    m = re.fullmatch(r"(k4-[1-5])(?:-c([1-9][0-9]*))?", name)
    require(m is not None, "k4-job-name")
    return m[1], int(m[2] or 0)


def episode_identity(name: str) -> dict:
    original = name.removesuffix("-replacement-1")
    rows = {r["episode"]: r for r in registry()}
    require(original in rows, "k4-episode-registry")
    return rows[original]


def replacement_ids(prior: dict) -> list[str]:
    return [
        d["episode"] + "-replacement-1"
        for d in prior["dispositions"]
        if d["classification"] == "infrastructure-invalid"
    ]


def assignment(
    f: dict, name: str, history: dict[str, dict], *, replacement_for: str = ""
) -> list[str]:
    base, number = job_name(name)
    rows = list({r["job_id"]: r for r in history.values() if "job_id" in r}.values())
    require(
        all(not r.get("intake_stopped") or r.get("claude_acknowledgement") is True for r in rows),
        "k4-stopped-intake-claude-acknowledgement-required",
    )
    if replacement_for:
        prior = next((r for r in rows if r["job_id"] == replacement_for), None)
        require(
            prior is not None
            and prior["job"] == name
            and bool(replacement_ids(prior))
            and not any(r["replacement_for"] == replacement_for for r in rows)
            and not prior["replacement_for"],
            "k4-infrastructure-replacement",
        )
        return replacement_ids(prior)
    history = {r.get("job", key): r for key, r in history.items() if not r.get("replacement_for")}
    if number == 0:
        require(name not in history, "k4-job-replay")
        return next(g["units"] for g in schedule(f)["groups"] if g["job"] == base)
    predecessor = base if number == 1 else f"{base}-c{number - 1}"
    require(predecessor in history and name not in history, "k4-predecessor-committed")
    prior = history[predecessor]
    require(
        prior["status"] in {"CARRY_OVER", "interrupted", "FAIL", "invalid", "unexercised"}
        and bool(prior["pending"]),
        "k4-continuation-suffix",
    )
    return list(prior["pending"])


def admit(
    f: dict,
    unit: str,
    *,
    elapsed: float,
    ledger_remaining: float,
    reservation: int = CEILING,
    setup: bool = False,
) -> float:
    elapsed = finite(elapsed, "k4-elapsed", zero=True)
    finite(ledger_remaining, "k4-ledger-remaining", zero=True)
    require(type(reservation) is int and 0 < reservation <= CEILING, "k4-reservation")
    rows = {r["unit"]: r for r in schedule(f)["units"]}
    fraction = 1
    if unit.endswith("-replacement-1"):
        cell = episode_identity(unit)
        unit = cell["unit"]
        fraction = 1 / rows[unit]["episodes"]
    require(unit in rows, "k4-admit-unit")
    needed = (
        rows[unit]["full_window_reservation_seconds"] * fraction
        + DIAGNOSTICS
        + TEARDOWN
        + (SETUP if setup else 0)
    )
    require(elapsed + needed <= reservation, "k4-whole-pair-fit")
    require(elapsed + needed <= ledger_remaining, "k4-ledger-fit")
    return needed


def remaining(f: dict, verified: list[str], charges: list[dict]) -> dict:
    expected = {r["episode"]: r for r in registry()}
    canonical = [episode_identity(e)["episode"] for e in verified]
    require(
        len(canonical) == len(set(canonical)),
        "k4-progress-identity",
    )
    require(len({r["job_id"] for r in charges}) == len(charges), "k4-charge-duplicate")
    charged = math.fsum(finite(r["charged_seconds"], "k4-charge") for r in charges)
    ledger_episodes = [e for r in charges for e in r.get("k4_episodes", [])]
    require(
        len(ledger_episodes) == len(set(ledger_episodes)) and set(verified) == set(ledger_episodes),
        "k4-progress-ledger-binding",
    )
    done = set(canonical)
    units = []
    for row in schedule(f)["units"]:
        ids = {r["episode"] for r in expected.values() if r["unit"] == row["unit"]}
        if not ids <= done:
            # A valid partner is retained; reserve only the unresolved episode.
            fraction = len(ids - done) / len(ids)
            units.append(dict(row, expected_seconds=row["expected_seconds"] * fraction))
    # Expected next-fit packing; actual charges replace, never supplement, done units.
    jobs = sum(any(r["unit"] in group["units"] for r in units) for group in schedule(f)["groups"])
    future = math.fsum(r["expected_seconds"] for r in units) + jobs * SETUP
    downstream = math.fsum(
        j["expected_seconds"] for j in f["observed_schedule"]["jobs"] if j["family"] != "K4"
    )
    allowance = f["observed_schedule"]["validation_allowance_seconds"]
    return dict(
        charged_seconds=charged,
        remaining_units=[r["unit"] for r in units],
        expected_k4_remaining_seconds=future,
        expected_remaining_seconds=future + downstream + allowance,
        expected_total_seconds=charged + future + downstream + allowance,
        remaining_cap_seconds=CAP - charged,
    )


def k7(rows: list[dict], *, complete_pairs: bool, safety: bool) -> dict:
    require(len(rows) <= 5, "k4-k7-population")
    excess = []
    for r in rows:
        low = finite(r["wasted_lower"], "k4-k7-cost", zero=True) + finite(
            r["reexecuted_lower"], "k4-k7-cost", zero=True
        )
        threshold = max(5, 0.10 * finite(r["busy_upper"], "k4-k7-cost", zero=True))
        excess.append(low - threshold)
    eligible = sum(r["busy_removed"] and r["idle_survivor"] and not r["ambiguous"] for r in rows)
    median = statistics.median(excess) if excess else None
    go = (
        len(rows) == 5
        and complete_pairs
        and safety
        and eligible >= 2
        and not any(r["ambiguous"] for r in rows)
        and median > 0
    )
    return dict(
        decision="GO" if go else "NO-GO",
        excess=excess,
        median_excess=median,
        eligible=eligible,
        complete_pairs=complete_pairs,
        safety=safety,
    )
