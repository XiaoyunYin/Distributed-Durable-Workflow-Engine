"""Derive and seal the J3 qualification schedule from durable outage evidence."""

import argparse
import hashlib
import json
from fractions import Fraction
from math import ceil
from pathlib import Path
from statistics import median
from zipfile import ZipFile

from j3_replay import DOMAIN, SECOND, oldest_clear_input, require

ARRIVAL_RATE = Fraction("7.136100570066256")
OUTAGE = 300
RESTORED_LIMIT = 540
FORECAST_CAP = 350
QUALIFICATION_CAP = 300
BASE_STOP = 2220
BASELINE_ARRIVALS = ceil(BASE_STOP * ARRIVAL_RATE)
CALIBRATION_ARMS = {
    "foreign": ("foreign", 1200, 30),
    **{f"outage-{i}": ("outage", BASE_STOP, 47) for i in range(1, 4)},
    "scrape-loss": ("scrape_loss", 720, 22),
}
CONFIRMATION_ARMS = ("matched-healthy", "full-volume")
CALIBRATION_MINUTES = {
    "bootstrap": 25,
    "final_evidence": 25,
    **{name: minutes for name, (_, _, minutes) in CALIBRATION_ARMS.items()},
    **{f"{name}_closeout": 10 for name in CALIBRATION_ARMS},
}
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


def recovery_age_evidence(evaluations, transitions, restored, stop):
    first_clear_input = next(
        (row for row in evaluations if restored < row["at_ns"] < stop and oldest_clear_input(row)),
        None,
    )
    oldest = [row for row in transitions if row["alert"] == DOMAIN[0]]
    clear = next(
        (row for row in oldest if restored < row["at_ns"] < stop and not row["firing"]), None
    )
    age_endpoint = next(
        (row for row in evaluations if clear and row["at_ns"] == clear["at_ns"]), None
    )
    require(
        not clear or (age_endpoint and oldest_clear_input(age_endpoint)), "invalid oldest clear"
    )
    return {
        "first_clear_input": first_clear_input,
        "age_endpoint": age_endpoint,
        "oldest_refires_before_stop": [
            row
            for row in oldest
            if clear and row["firing"] and clear["at_ns"] < row["at_ns"] < stop
        ],
    }


def recovery_evidence(outage, evaluations, transitions, stop):
    """Called only after a successful independent replay of the given evaluations."""
    restored = outage["restore_ns"]
    require(
        stop == outage["origin_ns"] + BASE_STOP * SECOND and outage["observed_through_ns"] >= stop,
        "missing calibration evidence through offer stop",
    )
    ticks = [row["at_ns"] for row in evaluations]
    require(
        ticks
        and ticks[0] <= restored < stop <= ticks[-1]
        and all(right - left == 5 * SECOND for left, right in zip(ticks, ticks[1:], strict=False)),
        "missing calibration evaluations through offer stop",
    )
    require(
        all(row["rule_ok"] is True and row["valid"] is True for row in evaluations),
        "missing valid calibration input",
    )
    require(
        all(
            type(row["admitted_ns"]) is int and row["admitted_ns"] > 0
            for row in outage["admissions"]
        ),
        "missing admission commit timestamp",
    )
    pre_restore = [row for row in outage["admissions"] if row["admitted_ns"] <= restored]
    require(pre_restore, "missing pre-restore admissions")
    terminals = []
    for row in pre_restore:
        terminal = row["terminal_ns"]
        if terminal is None:
            require(
                row["state"] not in {"SUCCEEDED", "FAILED", "CANCELED", "REJECTED", "ABANDONED"},
                "missing terminal commit timestamp",
            )
        else:
            require(
                type(terminal) is int
                and row["admitted_ns"] <= terminal <= outage["observed_through_ns"],
                "invalid terminal commit timestamp",
            )
            terminals.append(row)
    require(outage["backlog"]["workflow_ids"], "missing witnessed backlog")
    terminal_endpoint = (
        max(terminals, key=lambda row: row["terminal_ns"])
        if len(terminals) == len(pre_restore)
        else None
    )
    age = recovery_age_evidence(evaluations, transitions, restored, stop)
    age_endpoint = age["age_endpoint"]
    failed = (
        terminal_endpoint is None or age_endpoint is None or bool(age["oldest_refires_before_stop"])
    )
    elapsed = (
        max(terminal_endpoint["terminal_ns"], age_endpoint["at_ns"]) - restored
        if terminal_endpoint and age_endpoint
        else None
    )
    return {
        "status": "FAIL" if failed else "PASS",
        "restore_ns": restored,
        "offer_stop_ns": stop,
        "terminal_endpoint": terminal_endpoint,
        **age,
        "refire_check_through_ns": stop,
        "R_ns": elapsed,
        "R_s": None if elapsed is None else elapsed / SECOND,
        "evidence": ["outage.json", "replay-input.json", "replay.json"],
    }


def allowance_seconds(reservations, measured):
    return {
        name: max(minutes * 60, measured.get(name, 0)) for name, minutes in reservations.items()
    }


def qualification_forecast(delta, measured=None):
    measured = measured or {}
    calibration = allowance_seconds(CALIBRATION_MINUTES, measured)
    primary = max(measured.get(f"outage-{i}", 0) for i in range(1, 4))
    closeout = max(measured.get(f"outage-{i}_closeout", 0) for i in range(1, 4))
    confirmation = {
        "bootstrap": calibration["bootstrap"],
        "final_evidence": calibration["final_evidence"],
        **{name: max(47 * 60, primary) + delta for name in CONFIRMATION_ARMS},
        **{f"{name}_closeout": max(10 * 60, closeout) for name in CONFIRMATION_ARMS},
    }
    return {
        stage: {
            "allowance_seconds": allowances,
            "forecast_seconds": sum(allowances.values()),
            "forecast_minutes": sum(allowances.values()) / 60,
        }
        for stage, allowances in (("calibration", calibration), ("confirmation", confirmation))
    }


def measurement_forecast(delta, measured=None, confirmation=None):
    measured, confirmation = measured or {}, confirmation or {}
    primary = max(measured.get(f"outage-{i}", 0) for i in range(1, 4))
    closeout = max(measured.get(f"outage-{i}_closeout", 0) for i in range(1, 4))
    timings = {
        "bootstrap": max(measured.get("bootstrap", 0), confirmation.get("bootstrap", 0)),
        "primary": max(primary, confirmation.get("full-volume", 0) - delta),
        "matched_healthy": max(primary, confirmation.get("matched-healthy", 0) - delta),
        "foreign": measured.get("foreign", 0),
        "scrape_loss": measured.get("scrape-loss", 0),
        "primary_export_reset": max(closeout, confirmation.get("full-volume_closeout", 0)),
        "matched_healthy_export_reset": max(
            closeout, confirmation.get("matched-healthy_closeout", 0)
        ),
        "foreign_export_reset": measured.get("foreign_closeout", 0),
        "final_evidence": max(
            measured.get("final_evidence", 0) + measured.get("scrape-loss_closeout", 0),
            confirmation.get("final_evidence", 0),
        ),
    }
    allowances = allowance_seconds(RESERVED_MINUTES, timings)
    seconds = sum(allowances.values()) + 2 * delta
    return {
        "forecast_base_minutes": sum(RESERVED_MINUTES.values()) + 2 * delta // 60,
        "forecast_measured_seconds": timings,
        "forecast_allowance_seconds": allowances,
        "forecast_extended_arm_seconds": {
            name: allowances[name] + delta for name in ("primary", "matched_healthy")
        },
        "forecast_seconds": seconds,
        "forecast_minutes": seconds / 60,
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


def derive(rates, recovery_ns, measured_seconds=None):
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

    require(
        len(recovery_ns) == 3 and all(type(value) is int and value > 0 for value in recovery_ns),
        "missing recovery endpoints",
    )
    if measured_seconds is not None:
        require(
            measured_seconds.keys() == CALIBRATION_MINUTES.keys(), "missing forecast allowances"
        )
        require(
            all(type(value) is int and value >= 0 for value in measured_seconds.values()),
            "invalid qualification duration",
        )
    recovery = ARRIVAL_RATE * OUTAGE / (mu - ARRIVAL_RATE)
    strict_bound = 2 * Fraction(max(recovery_ns), SECOND) + 5
    delta = 60 * max(
        0, ceil((recovery - RESTORED_LIMIT) / 60), (strict_bound - RESTORED_LIMIT) // 60 + 1
    )
    qualification = qualification_forecast(delta, measured_seconds)
    measurement = measurement_forecast(delta, measured_seconds)
    over_cap = [
        stage
        for stage, forecast in qualification.items()
        if forecast["forecast_seconds"] > QUALIFICATION_CAP * 60
    ]
    if measurement["forecast_seconds"] > FORECAST_CAP * 60:
        over_cap.append("measurement")
    final_window = [1620 + delta, 2220 + delta]
    common = {
        "final_healthy_window_s": final_window,
        "stop_s": BASE_STOP + delta,
        "drain_and_observation_end_s": 2820 + delta,
        "quiet_drain_s": 30,
    }
    return {
        **result,
        "status": "HALT" if over_cap else "SEALED",
        "reason": "forecast_over_cap" if over_cap else None,
        "over_cap": over_cap,
        "qualification_cap_minutes": QUALIFICATION_CAP,
        "qualification_forecast": qualification,
        "calibration_measured_seconds": measured_seconds,
        "T_s": float(recovery),
        "T_ratio": str(recovery),
        "R_ns": recovery_ns,
        "R_s": [value / SECOND for value in recovery_ns],
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
        **measurement,
    }


def derive_from_outages(paths, measured):
    require(len(set(paths)) == 3, "expected evidence from three episodes")
    rates, tails, details = [], [], []
    for path in paths:
        rate, evidence = restored_rate(json.loads(path.read_bytes()))
        rates.append(rate)
        recovery = path.with_name("calibration-recovery.json")
        proof = json.loads(recovery.read_bytes())
        require(
            proof["status"] == "PASS" and proof["restore_ns"] == evidence["restore_ns"],
            "unverified calibration recovery",
        )
        tails.append(proof["R_ns"])
        details.append({"evidence": str(path), "recovery_evidence": str(recovery), **evidence})
    return {**derive(rates, tails, measured), "outages": details}


def verify_seal(archive, artifact_digest, campaign, head):
    with archive.open("rb") as stream:
        require(
            hashlib.file_digest(stream, "sha256").hexdigest()
            == artifact_digest.removeprefix("sha256:"),
            "schedule artifact digest mismatch",
        )
        with ZipFile(stream) as bundled:
            raw = bundled.read("sealed-schedule.json")
            seal = json.loads(bundled.read("schedule-seal.json"))
    require(seal["sha256"] == hashlib.sha256(raw).hexdigest(), "sealed schedule digest mismatch")
    require(
        seal["campaign"] == campaign and seal["head"] == head, "schedule seal provenance differs"
    )
    schedule = json.loads(raw)
    require(schedule["status"] == "SEALED", "schedule was not sealed")
    for stage in ("calibration", "confirmation"):
        require(
            schedule["qualification_forecast"][stage]["forecast_seconds"] <= QUALIFICATION_CAP * 60,
            f"{stage} forecast over cap",
        )
    require(schedule["forecast_seconds"] <= FORECAST_CAP * 60, "measurement forecast over cap")
    return schedule, seal


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("outages", nargs=3, type=Path)
    parser.add_argument("--measurements", required=True, type=Path)
    args = parser.parse_args()
    try:
        result = derive_from_outages(args.outages, json.loads(args.measurements.read_bytes()))
    except (KeyError, OSError, TypeError, ValueError) as error:
        result = {"status": "INVALID", "error": str(error)}
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return 0 if result["status"] == "SEALED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
