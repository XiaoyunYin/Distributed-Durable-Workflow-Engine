"""Replay integrated J2 engineering episodes from retained raw evidence."""

from collections import Counter

from j2_checker_report import IntegrityError, indexed_report, require_checker_exit
from j2_control_evidence import gates_samples, quiet_evidence
from j2_drain import checked_cut, cohort_counts, expected_arrivals, replay_episode
from j2_offline import ClockEvidenceError, bins_gate, clock_evidence, summarize
from j2_qualification_evidence import DRAIN_SECONDS, read, require
from j2_qualification_resources import checker_resource_summary, require_checker_resources
from j2_qualify_volume import OBSERVER_LIMIT
from j2_schedule import RATES, k4_report, registered_population
from j2_step_rehearsal import verified_crossings


def summarize_episode(root, *, snapshot_only=False):
    schedule = read(root / "schedule.json")
    result = read(root / "drain-result.json")
    final = read(root / "final.json")
    expired = result.get("classification") == "drain_failure"
    registered = schedule.get("registered_clock") is True
    if registered:
        require_checker_exit(read(root / "live-checker-read.json")["exit"])
    try:
        checked, width = (
            replay_episode(root, snapshot_only=True) if snapshot_only else replay_episode(root)
        )
    except ClockEvidenceError as error:
        if registered:
            raise
        raise IntegrityError(f"checked evidence failed replay: {error}") from error
    except (KeyError, OSError, ValueError) as error:
        raise IntegrityError(f"checked evidence failed replay: {error}") from error
    if checked != read(root / "phase-rows.json") or cohort_counts(schedule, checked) != read(
        root / "cohorts.json"
    ):
        raise IntegrityError("checked phase rows or counts differ from retained replay evidence")
    raw = (root / "sender-journal.jsonl").read_bytes()
    expected, _ = expected_arrivals(raw, schedule)
    committed = {row["id"] for row in final["db"]["submissions"]}
    sender = k4_report(raw, schedule, sorted(committed))
    require(sender == read(root / "sender-report.json"), "retained sender report differs")
    require(
        read(root / "journal-read.json")["exit"] == 0
        and read(root / "sender-exit.json")["exec_exit"] in (0, 1),
        "sender execution evidence missing",
    )
    cut = read(root / "live-checker-read.json")
    require_checker_exit(cut["exit"])
    if not snapshot_only:
        resources = checker_resource_summary(
            root, read(root / "checker-resource-summary.json")["exit"], OBSERVER_LIMIT
        )
        require_checker_resources(resources)
    report, _ = indexed_report(
        root / "live-checker.json", join_trace=True, verify_tables=True, drain_counts=True
    )
    inventory = sorted(
        (x["inventory"] for x in read(root / "witness.json")["ledgers"]), key=lambda x: x["pod_uid"]
    )
    if not expired:
        state = checked_cut(report, cut, inventory)
        if {row["id"]: row for row in state["db"]["submissions"]} != {
            row["id"]: row for row in final["db"]["submissions"]
        } or state["witness"] != final["witness"]:
            raise IntegrityError("checker and final quiet identities differ")
        quiet = quiet_evidence(root, final, expected, sender, cut, state)
    else:
        stop = read(root / "deadline-anchor.json")["stop_mono_ns"]
        require(
            result["deadline_mono_ns"] == stop + DRAIN_SECONDS * 10**9
            and result["drain_seconds"] == DRAIN_SECONDS
            and result["check_status"] == "PASS",
            "checked expiry does not bind the registered deadline",
        )
        quiet = {
            "status": "drain_failure",
            "missing": result["missing"],
            "pending": result["pending"],
            "unpublished": result["unpublished"],
            "claimed": result["claimed"],
            "checked_export_seconds": result["checked_export_seconds"],
        }
    sent = {
        row["key"] for row in sender["sender_samples"] if row["phase"] in {"intent", "finished"}
    }
    sender_valid = (
        sender["closed"]
        and not sender["torn_suffix"]
        and not sender["outside_slot"]
        and not sender["remaining_intents"]
        and sent == committed == expected.keys()
    )
    attempts = Counter(
        row["workflow_id"]
        for row in report["trace"]["Snapshot"]["engine.transition_history"]["rows"]
        if row["reason"] == "ATTEMPT_CREATED"
    )
    rows = {
        row["workflow_id"]: {
            **row,
            "status": "observed",
            "terminal_state": "SUCCEEDED",
            "admitted": True,
            "attempts": attempts[row["workflow_id"]],
            "raw_end_to_end_ns": row["terminal_ns"] - row["scheduled_ns"],
            "end_to_end_ns": row["terminal_ns"] - row["scheduled_ns"] + width,
        }
        for row in checked
    }
    start, warm, stop = (schedule[k] for k in ("origin_ns", "warmup_end_ns", "stop_ns"))
    boundary = schedule.get("hold_end_ns")
    windows = [("warmup", start, warm), ("hold" if boundary else "grid", warm, boundary or stop)]
    crossing = []
    if boundary is not None:
        windows.append(("step", boundary, stop))
        probe = read(root / "step-transition.json")
        if (
            not probe["worker_uids_before"]
            or probe["worker_uids_before"] != probe["worker_uids_after"]
        ):
            raise IntegrityError("workers changed across the continuous step")
        crossing = verified_crossings(probe, checked, boundary, width, pending=result["pending"])
    samples = gates_samples(read(root / "prometheus-samples.json"))
    pending = set(result["pending"]) if registered and expired else set()
    cohorts = {}
    for name, lo, hi in windows:
        offered = {
            key: entry for key, entry in expected.items() if lo <= entry["scheduled_ns"] < hi
        }
        observed = [rows[key] for key in sorted(offered.keys() & rows.keys())]
        unresolved = [
            {
                "workflow_id": key,
                "scheduled_ns": entry["scheduled_ns"],
                "status": "censored" if not registered or key in pending else "unresolved",
                "admitted": key in committed,
                "attempts": attempts[key],
            }
            for key, entry in offered.items()
            if key not in rows
        ]
        phases = summarize([*observed, *unresolved] if registered else observed)
        if unresolved:
            phases["censored_keys"] = sorted(
                row["workflow_id"] for row in unresolved if row["status"] == "censored"
            )
        cohorts[name] = {
            "offered": len(offered),
            "sent": len(sent & offered.keys()),
            "admitted": len(committed & offered.keys()),
            "checked_success": len(observed),
            "in_window_success": sorted(
                r["workflow_id"] for r in observed if lo <= r["terminal_ns"] < hi
            ),
            "carry_in_success": sorted(
                key for key, r in rows.items() if r["scheduled_ns"] < lo <= r["terminal_ns"] < hi
            ),
            "post_window_success": sorted(
                r["workflow_id"] for r in observed if r["terminal_ns"] >= hi
            ),
            "phases": phases,
        }
        if name != "warmup":
            cohorts[name]["gate"] = bins_gate(
                [*observed, *unresolved],
                samples,
                lo,
                hi,
                sender_valid,
                checked_expiry=registered and expired,
            )
        if registered:
            cohorts[name]["first_observed_minus_durable_ns"] = {
                row["workflow_id"]: row["polling_minus_durable_ns"] for row in observed
            }
    summary = {
        "cohorts": cohorts,
        "clock_bracket_ns": width,
        "drain": quiet,
        "sender_valid": sender_valid,
        "stock_verdict": sender["stock_verdict"],
        "verified_crossing_ids": crossing,
        "measured_eligible": False,
    }
    if not snapshot_only:
        summary["checker_peak_rss_kib"] = resources["checker_peak_rss_kib"]
    if registered:
        clock = clock_evidence(read(root / "clock.json"), start, final["upper_unix_ns"])
        require(clock["uncertainty_ns"] == width, "replayed clock uncertainty differs")
        summary.update(
            registered_clock=True,
            clock=clock,
            population=registered_population(schedule, cohorts),
        )
        if boundary is not None:
            summary["soak_evidence_eligible"] = (
                not snapshot_only
                and sender_valid
                and quiet["status"] == "PASS"
                and cohorts["hold"]["gate"]["status"] == "pass"
                and cohorts["step"]["gate"]["status"] in {"pass", "failed_gate"}
            )
        else:
            summary["grid_eligible"] = (
                sender_valid
                and quiet["status"] == "PASS"
                and cohorts["grid"]["gate"]["status"] == "pass"
            )
    return summary


def registered_boundary(episodes):
    stable, drains = [], {}
    saturation = None
    lower_passed = True
    for cell in RATES:
        episode = episodes.get(cell)
        if episode is None:
            lower_passed = False
            continue
        drains[cell] = episode["drain"]["status"]
        gate = episode["cohorts"]["grid"]["gate"]
        if episode["sender_valid"] and gate["status"] == "pass":
            stable.append(cell)
        else:
            if (
                lower_passed
                and episode["sender_valid"]
                and gate["status"] == "failed_gate"
                and gate["sustained_growth"]
            ):
                saturation = cell
            lower_passed = False
    return {"stable_rates": stable, "drains": drains, "saturation": saturation}


def engineering_boundary(episodes, order):
    for cell in order:
        episode = episodes.get(cell)
        if not episode or episode["drain"]["status"] != "PASS" or not episode["sender_valid"]:
            break
        gate = episode["cohorts"]["grid"]["gate"]
        if gate["status"] != "pass":
            if gate["status"] == "failed_gate" and gate["sustained_growth"]:
                return {"diagnostic_first_failure": cell, "attribution": "unresolved"}
            break
    return {"diagnostic_first_failure": None, "attribution": "unresolved"}
