"""Derive and seal the J3 qualification schedule from durable outage evidence."""

import argparse
import json
from fractions import Fraction
from math import ceil
from pathlib import Path
from statistics import median

from j3_replay import SECOND, require

ARRIVAL_RATE = Fraction("7.136100570066256")
OUTAGE = 300
RESTORED_LIMIT = 540
FORECAST_CAP = 350
BASELINE_ARRIVALS = ceil(2220 * ARRIVAL_RATE)
RESERVED_MINUTES = {
    "bootstrap": 25,
    "primary": 47,
    "matched_healthy": 47,
    "foreign": 30,
    "scrape_loss": 22,
    "primary_export_reset": 10,
    "matched_healthy_export_reset": 10,
    "foreign_export_reset": 10,
    "final_evidence": 25,
}


def restored_rate(evidence):
    """Count commits in (restore, stop], using independently captured DB commit times.

    The restore snapshot provides the witnessed backlog. The final inventory also
    includes admissions in the gap between that snapshot and the restore command.
    A collector must preserve commit timestamps before PostgreSQL can discard them.
    """
    stopped = evidence["scale_down_ns"]
    restored = evidence["restore_ns"]
    origin = evidence["origin_ns"]
    backlog = evidence["backlog"]
    require(
        abs(stopped - (origin + 600 * SECOND)) <= 5 * SECOND
        and abs(restored - (origin + (600 + OUTAGE) * SECOND)) <= 5 * SECOND,
        "scale commands outside the 300-second outage schedule",
    )
    require(stopped < backlog["sample_ns"] <= restored, "missing restore backlog snapshot")
    observed = backlog["workflow_ids"]
    require(observed and len(observed) == len(set(observed)), "empty or duplicate restore backlog")

    rows = evidence["admissions"]
    by_id = {row["workflow_id"]: row for row in rows}
    require(len(by_id) == len(rows), "duplicate durable admissions in rate evidence")
    upper = restored + RESTORED_LIMIT * SECOND
    observed_through = evidence["observed_through_ns"]
    for row in rows:
        admitted = row["admitted_ns"]
        committed = row["terminal_ns"]
        require(type(admitted) is int and admitted > 0, "missing admission commit timestamp")
        if committed is not None:
            require(
                type(committed) is int and admitted <= committed <= observed_through,
                "invalid terminal commit",
            )
        require(row["state"] == "SUCCEEDED" and committed is not None, "unsuccessful durable work")

    for identity in observed:
        row = by_id.get(identity)
        require(
            row is not None and row["admitted_ns"] <= backlog["sample_ns"] < row["terminal_ns"],
            f"unverifiable restore backlog: {identity}",
        )
    pre_restore = sorted(
        identity for identity, row in by_id.items() if row["admitted_ns"] <= restored
    )
    caught_up = max(by_id[identity]["terminal_ns"] for identity in pre_restore)
    require(caught_up > restored, "no unfinished admissions at the restore command")
    stop = min(upper, caught_up)
    require(observed_through >= stop, "missing evidence through the rate stop boundary")
    successes = sorted(
        identity for identity, row in by_id.items() if restored < row["terminal_ns"] <= stop
    )
    rate = Fraction(len(successes) * SECOND, stop - restored)
    return rate, {
        "restore_ns": restored,
        "scale_down_ns": stopped,
        "stop_ns": stop,
        "stop_rule": "all_pre_restore_finished" if caught_up <= upper else "540_second_cap",
        "pre_restore_workflow_ids": pre_restore,
        "succeeded_workflow_ids": successes,
        "elapsed_ns": stop - restored,
        "restored_rate_per_second": float(rate),
        "restored_rate_ratio": str(rate),
    }


def derive(rates, measured_seconds=None):
    """Use rational arithmetic through the ceil and cap comparisons."""
    rates = [Fraction(str(rate)) for rate in rates]
    require(len(rates) == 3 and all(rate >= 0 for rate in rates), "expected three restored rates")
    mu = median(rates)
    result = {
        "arrival_rate_per_second": float(ARRIVAL_RATE),
        "restored_rates_per_second": [float(rate) for rate in rates],
        "mu_per_second": float(mu),
        "mu_ratio": str(mu),
        "forecast_cap_minutes": FORECAST_CAP,
        "baseline_arrivals": BASELINE_ARRIVALS,
    }
    if mu <= ARRIVAL_RATE:
        return {**result, "status": "HALT", "reason": "mu_at_or_below_arrival_rate"}

    recovery = ARRIVAL_RATE * OUTAGE / (mu - ARRIVAL_RATE)
    delta = 60 * ceil(max(0, recovery - RESTORED_LIMIT) / 60)
    if measured_seconds is not None:
        require(measured_seconds.keys() == RESERVED_MINUTES.keys(), "missing forecast allowances")
        require(
            all(type(value) is int and value > 0 for value in measured_seconds.values()),
            "invalid qualification duration",
        )
    measurements = measured_seconds or dict.fromkeys(RESERVED_MINUTES, 0)
    allowances = {
        name: max(minutes * 60, measurements[name]) for name, minutes in RESERVED_MINUTES.items()
    }
    # The measurements use baseline arms. Charge the extension to both arms.
    forecast_seconds = sum(allowances.values()) + 2 * delta
    final_window = [1620 + delta, 2220 + delta]
    common = {
        "final_healthy_window_s": final_window,
        "stop_s": 2220 + delta,
        "drain_and_observation_end_s": 2820 + delta,
        "quiet_drain_s": 30,
    }
    return {
        **result,
        "status": "HALT" if forecast_seconds > FORECAST_CAP * 60 else "SEALED",
        "reason": "forecast_over_cap" if forecast_seconds > FORECAST_CAP * 60 else None,
        "T_s": float(recovery),
        "T_ratio": str(recovery),
        "delta_s": delta,
        "primary": {
            "scale_down_s": 600,
            "zero_workers_by_s": 660,
            "oldest_fire_by_s": 700,
            "burn_fire_by_s": 765,
            "restore_s": 900,
            "ready_and_first_progress_by_s": 960,
            "catchup_before_s": 1440 + delta,
            "oldest_clear_by_s": 1450 + delta,
            "burn_clear_by_s": 1570 + delta,
            **common,
        },
        "matched_healthy": common,
        "arrival_volume": ceil(common["stop_s"] * ARRIVAL_RATE),
        "forecast_base_minutes": sum(RESERVED_MINUTES.values()) + 2 * delta // 60,
        "forecast_measured_seconds": measured_seconds,
        "forecast_allowance_seconds": allowances,
        "forecast_extended_arm_seconds": {
            name: allowances[name] + delta for name in ("primary", "matched_healthy")
        },
        "forecast_seconds": forecast_seconds,
        "forecast_minutes": forecast_seconds / 60,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("outages", nargs=3, type=Path)
    parser.add_argument("--measurements", required=True, type=Path)
    args = parser.parse_args()
    result = {"status": "INVALID", "outages": []}
    rates = []
    try:
        require(len(set(args.outages)) == 3, "expected evidence from three episodes")
        for path in args.outages:
            rate, evidence = restored_rate(json.loads(path.read_bytes()))
            rates.append(rate)
            result["outages"].append({"evidence": str(path), **evidence})
        result.update(derive(rates, json.loads(args.measurements.read_bytes())))
    except (KeyError, OSError, TypeError, ValueError) as error:
        result["error"] = str(error)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return 0 if result["status"] == "SEALED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
