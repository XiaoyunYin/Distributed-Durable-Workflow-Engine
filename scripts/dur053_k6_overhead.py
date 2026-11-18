"""Observer brackets and conservative episode-equivalent K6 effect overhead."""

from __future__ import annotations

import json
import math
from pathlib import Path

from dur053_k6_registry import require

SCHEMA = "dur053.k6-effect-overhead.v1"
EPISODE_BLOCKS = 15  # 300 s at 0.5 * 0.9994137335861809 arrivals/s, rounded up to 150.
METHOD = {
    "clock": "monotonic nanoseconds on each local observer; no cross-process subtraction",
    "baseline": "same-position PURE submission and terminal observation brackets",
    "difference": "effect upper bound minus PURE lower bound, floored at zero",
    "normalization": "block elapsed delta times 15, assuming consecutive blocks without overlap",
    "details": (
        "approval: submission through first read of grant history; "
        "claim: worker-control round trip including allocation; "
        "pre_call_delay: worker's monotonic wait after receiving its claim; "
        "grant_read: sink authorization and in-transaction recheck brackets; "
        "call/lookup: worker TLS exchange; witness: relay entry through matching durable host ACK"
    ),
    "overlap": "stage diagnostics can overlap and are not added to the governing elapsed delta",
    "lookup": "clean forward-only workload; zero samples if no recovery is observed",
    "p95": "nearest rank",
    "admission": "max",
}
STAGES = ("approval", "claim", "pre_call_delay", "grant_read", "call", "lookup", "witness")


def bracket(value: object) -> bool:
    return (
        isinstance(value, list)
        and len(value) == 2
        and all(type(ns) is int for ns in value)
        and 0 <= value[0] <= value[1]
    )


def interval(sample: dict) -> tuple[int, int]:
    sent, terminal = sample["submitted_ns"], sample["terminal_ns"]
    require(bracket(sent) and bracket(terminal) and sent[0] <= terminal[1], "k6-bracket")
    return max(0, terminal[0] - sent[1]), terminal[1] - sent[0]


def block_interval(samples: list[dict]) -> tuple[int, int]:
    return interval(
        {
            "submitted_ns": [min(row["submitted_ns"][i] for row in samples) for i in (0, 1)],
            "terminal_ns": [max(row["terminal_ns"][i] for row in samples) for i in (0, 1)],
        }
    )


def measurement(baseline: list[dict], blocks: dict[str, list[dict]]) -> dict:
    require(set(blocks) == {"effect", "forward-control"}, "k6-overhead-blocks")
    require(
        len(baseline) == 10 and all(row["stages_ns"] == {} for row in baseline),
        "k6-overhead-baseline",
    )
    output = []
    counts = dict.fromkeys(STAGES, 0)
    baseline_block_lower, _ = block_interval(baseline)
    for name, block in blocks.items():
        require(len(block) == 10, "k6-overhead-block-size")
        deltas = []
        for position, (effect, pure) in enumerate(zip(block, baseline, strict=True)):
            lower, _ = interval(pure)
            _, upper = interval(effect)
            stages = effect["stages_ns"]
            require(set(stages) == (set(STAGES) if position < 8 else set()), "k6-overhead-stages")
            if position < 8:
                grant = effect["grant_ns"]
                require(
                    bracket(grant)
                    and effect["submitted_ns"][0] <= grant[0]
                    and grant[1] <= effect["terminal_ns"][1]
                    and stages["approval"] == [grant[1] - effect["submitted_ns"][0]],
                    "k6-overhead-approval-bracket",
                )
                deltas.append(max(0, upper - lower))
            for stage, values in stages.items():
                require(
                    isinstance(values, list)
                    and (stage == "lookup" or len(values) > 0)
                    and all(type(ns) is int and ns > 0 for ns in values),
                    "k6-overhead-stage-sample",
                )
                counts[stage] += len(values)
        block_upper = block_interval(block)[1]
        block_delta = max(0, block_upper - baseline_block_lower)
        block_stages = {
            stage: [ns for sample in block[:8] for ns in sample["stages_ns"][stage]]
            for stage in STAGES
        }
        output.append(
            {
                "block": name,
                "samples": block,
                "additional_activity_ns": deltas,
                "additional_block_elapsed_ns": block_delta,
                "stage_totals_ns": {stage: sum(values) for stage, values in block_stages.items()},
                "stage_sample_counts": {
                    stage: len(values) for stage, values in block_stages.items()
                },
                "episode_equivalent_seconds": block_delta * EPISODE_BLOCKS / 1e9,
            }
        )
    estimates = sorted(row["episode_equivalent_seconds"] for row in output)
    return {
        "schema": SCHEMA,
        "status": "PASS",
        "measurement_claim": False,
        "charged_seconds": 0,
        "method": METHOD,
        "episode_blocks": EPISODE_BLOCKS,
        "baseline": baseline,
        "blocks": output,
        "sample_counts": {
            "baseline": len(baseline),
            "blocks": len(output),
            "cooperating": sum(len(row["additional_activity_ns"]) for row in output),
            **counts,
        },
        "max_episode_equivalent_seconds": estimates[-1],
        "p95_episode_equivalent_seconds": estimates[math.ceil(0.95 * len(estimates)) - 1],
    }


def object_without_duplicates(items: list[tuple[str, object]]) -> dict:
    result = dict(items)
    require(len(result) == len(items), "k6-duplicate-overhead-field")
    return result


def read_effect_overhead(path: Path) -> float:
    """Use the maximum verified engineering estimate for each unmeasured effect episode."""
    try:
        raw = json.loads(path.read_bytes(), object_pairs_hook=object_without_duplicates)
        blocks = {row["block"]: row["samples"] for row in raw["blocks"]}
        verified = measurement(raw["baseline"], blocks)
    except (AttributeError, KeyError, OSError, OverflowError, TypeError, ValueError) as error:
        raise ValueError("k6-invalid-effect-overhead-record") from error
    require(
        len(blocks) == len(raw["blocks"])
        and json.dumps(raw, sort_keys=True) == json.dumps(verified, sort_keys=True)
        and math.isfinite(verified["max_episode_equivalent_seconds"])
        and verified["max_episode_equivalent_seconds"] > 0,
        "k6-invalid-effect-overhead-record",
    )
    return verified["max_episode_equivalent_seconds"]
