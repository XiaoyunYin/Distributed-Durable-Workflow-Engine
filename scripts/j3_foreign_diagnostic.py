"""Reconstruct the foreign alert-isolation evidence without changing the verdict."""

import argparse
import json
from pathlib import Path

from j2_offline import utc_ns
from j3_evidence import (
    foreign_isolation,
    foreign_positive,
    jsonl,
    slot_occupancy,
    target_only_attainment,
)
from j3_overlay import FOREIGN
from j3_replay import DEADLINE, SECOND, replay, replay_matches, require


def diagnostic(episode):
    data = json.loads((episode / "replay-input.json").read_bytes())
    stored = json.loads((episode / "replay.json").read_bytes())
    result = replay(data, jsonl(episode / "visibility.jsonl"))
    require(replay_matches(result, stored, legacy=True), "retained alert replay differs")
    run = json.loads((episode / "run.json").read_bytes())
    original = json.loads((episode.parent / "verdict.json").read_bytes())
    seeds = list(jsonl(episode / "foreign-seed.jsonl"))
    ledger = json.loads((episode / "slot-ledger.json").read_bytes())
    data["slot_events"] = ledger["events"]
    origin, target = data["start_ns"], data["namespace"]
    evaluations = data["evaluations"]
    quiet = [
        row
        for row in evaluations
        if row["valid"]
        and not row["firing"]
        and origin + 840 * SECOND <= row["sample_ns"] < origin + 900 * SECOND
    ]
    require(quiet, "no simultaneous quiet target observations")
    at = quiet[0]["sample_ns"]
    require(
        all(
            row["ready_slots"] == 4
            and row["sql_oldest_claimable_seconds"] <= 28.540
            and row["oldest_claimable_age_seconds"] <= 28.540
            for row in quiet
        ),
        "target age or worker slots exceeded a foreign-control bound",
    )
    isolation = foreign_isolation(data, seeds, FOREIGN)
    ready_rows = [row for row in jsonl(episode / "ready-slots.jsonl") if row["sample_ns"] >= origin]
    require(ready_rows, "missing target worker witnesses")
    slots = ready_rows[-1]["slots"]
    workers = ready_rows[-1]["witness"].keys()
    require(
        len(workers) == 2
        and all(row["slots"] == slots and row["witness"].keys() == workers for row in ready_rows),
        "foreign worker slots changed or a witness is missing",
    )
    occupancy = slot_occupancy(data, slots, target, exclude_foreign=False)
    require(
        occupancy == slot_occupancy(data, slots, target, exclude_foreign=True),
        "target slot occupancy differs",
    )
    widths = result["published_attainment"]
    attainment = target_only_attainment(
        data, target, max(widths["setup_width_ns"], widths["closing_width_ns"]), data["end_ns"]
    )
    require(
        attainment == {key: widths[key] for key in attainment}, "target-only attainment differs"
    )

    # The fixture connection issued autocommit statements serially. The next
    # seed's database timestamp is an upper bound for the previous commit.
    times = [utc_ns(row["seeded_at"]) for row in seeds]
    require(all(a < b for a, b in zip(times, times[1:], strict=False)), "unordered seed receipts")
    confirmed = [
        {**row, "commit_seen_ns": upper}
        for row, upper in zip(seeds, times[1:], strict=False)
        if upper <= at
    ]
    ids = {row["workflow_id"] for row in confirmed}
    keys = {row["submission_key"] for row in data["workflows"] if row["workflow_id"] in ids}
    deadlines = [
        row["scheduled_ns"] + DEADLINE
        for row in data["plan"]
        if row["namespace"] == FOREIGN and row["submission_key"] in keys
    ]
    require(len(deadlines) == len(ids) and confirmed, "missing witnessed foreign plan")
    counts = {
        window: sum(at - seconds * SECOND < deadline <= at for deadline in deadlines)
        for window, seconds in (("2m", 120), ("10m", 600))
    }
    positive = {
        "sample_ns": at,
        "namespace": FOREIGN,
        "admission_ids": sorted(ids),
        "claimable_attempts": len(ids),
        "oldest_claimable_age_ns": at - times[0],
        "windows": {window: {"eligible": count, "bad": count} for window, count in counts.items()},
    }
    foreign_positive(positive, quiet, confirmed, FOREIGN)
    del positive["admission_ids"]
    transitions = result["alert_inputs"]["transitions"]
    return {
        "classification": "diagnostic_only",
        "run": run["run"],
        "original_verdict": original["status"],
        "target_evaluations": len(evaluations),
        "all_target_evaluations_valid": all(row["valid"] for row in evaluations),
        "target_alert_transitions": transitions,
        "retained_notification_pairs": json.loads(
            (episode / "notification-pairs.json").read_bytes()
        ),
        "quiet_foreign_window_evaluations": len(quiet),
        "foreign_durable_isolation": isolation,
        "target_slot_occupancy_equal": True,
        "target_only_attainment_equal": True,
        "target_only_attainment": attainment,
        "conservative_foreign_positive": positive,
        "foreign_positive_method": "unchanged seed rows; each commit bounded by the next receipt",
        "independent_live_foreign_query_retained": False,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("foreign_episode", type=Path)
    print(json.dumps(diagnostic(parser.parse_args().foreign_episode), indent=2))
