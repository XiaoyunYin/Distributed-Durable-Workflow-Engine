"""Replay integrated J2 engineering episodes from retained raw evidence."""

from collections import Counter

from j2_checker_report import IntegrityError, indexed_report, require_checker_exit
from j2_control_evidence import gates_samples, quiet_evidence
from j2_drain import checked_cut, cohort_counts, expected_arrivals, replay_episode
from j2_offline import bins_gate, summarize
from j2_qualification_evidence import DRAIN_SECONDS, read, require
from j2_qualification_resources import checker_resource_summary, require_checker_resources
from j2_qualify_volume import OBSERVER_LIMIT
from j2_schedule import k4_report
from j2_step_rehearsal import verified_crossings


def summarize_episode(root):
    schedule = read(root / "schedule.json")
    result = read(root / "drain-result.json")
    final = read(root / "final.json")
    expired = result.get("classification") == "drain_failure"
    try:
        checked, width = replay_episode(root)
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
    cohorts = {}
    for name, lo, hi in windows:
        offered = {
            key: entry for key, entry in expected.items() if lo <= entry["scheduled_ns"] < hi
        }
        observed = [rows[key] for key in sorted(offered.keys() & rows.keys())]
        unresolved = [
            {"workflow_id": key, "scheduled_ns": entry["scheduled_ns"], "status": "censored"}
            for key, entry in offered.items()
            if key not in rows
        ]
        phases = summarize(observed)
        if unresolved:
            phases["censored_keys"] = sorted(row["workflow_id"] for row in unresolved)
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
                [*observed, *unresolved], samples, lo, hi, sender_valid
            )
    return {
        "cohorts": cohorts,
        "clock_bracket_ns": width,
        "drain": quiet,
        "sender_valid": sender_valid,
        "stock_verdict": sender["stock_verdict"],
        "verified_crossing_ids": crossing,
        "checker_peak_rss_kib": resources["checker_peak_rss_kib"],
        "measured_eligible": False,
    }


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
