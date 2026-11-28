"""Run and independently replay the three-host R422 full-clock engineering lane."""

import argparse
import os
import re
import subprocess
import time
from pathlib import Path

import dur053_k3 as kind
from j2_barrier import JOBS, Campaign, RetryLater, canonical
from j2_barrier_rehearsal import client
from j2_drain import DrainExpired, with_state_read_schema
from j2_harness import APP, DB, hosted_dispatch, run
from j2_integrated_diagnostics import attribution_samples
from j2_integrated_evidence import summarize_episode
from j2_integrated_rehearsal import install_episode
from j2_integrated_report import checker_replay
from j2_measure_replay import ReplayBudget, ReplayInputs
from j2_measure_telemetry import cell_samples, coverage_gaps, fresh_telemetry, runner_inventory
from j2_offline import SECOND
from j2_prequalify_checker import WORKER_BASE
from j2_qualification_evidence import file_record, read, require, write
from j2_qualify_volume import OBSERVER_LIMIT
from j2_schedule import DISPATCH_JOURNAL, HOLD, WARMUP, registered_schedule, schedule_slots
from j2_schedule_rehearsal import PRECOMMIT_LEAD_SECONDS, run_precommitted, verify_precommit_install
from j2_step_rehearsal import DrainHarness, exercise_drain
from j2_verify_overlay import start_profile

SCOPE = "R422 full-clock engineering only"
MODE = "r422-full-clock"
MS = 1_000_000
P999_NS = 12 * MS
MAX_NS = 20 * MS
LIVE_WORK_MINUTES = 70


def gate(identifier, hold, authorization_ref, workflow_ref):
    require(hold in {"A", "AB"}, "R422 lane hold must be A or AB")
    require(
        isinstance(identifier, str)
        and re.fullmatch(rf"eng-r422-{hold.lower()}-[a-z0-9][a-z0-9-]{{0,40}}", identifier),
        "R422 lane requires an unregistered engineering campaign ID paired with its hold",
    )
    require(
        not authorization_ref
        and not workflow_ref.startswith(("refs/heads/j2-authorization/", "j2-authorization/")),
        "R422 engineering lane forbids authorization refs",
    )


def schedule_for(identifier, hold, job, origin):
    schedule = registered_schedule(
        identifier,
        f"job-{job}",
        "soak",
        f"j2-{identifier}-job-{job}-soak",
        APP,
        origin,
        hold,
    )
    schedule.update(
        scope=SCOPE,
        campaign_clock=True,
        worker_sleep_ms=DrainHarness.worker_sleep_ms,
        dispatch_journal=DISPATCH_JOURNAL,
    )
    return schedule


def latency(values):
    if not values:
        return {"samples": 0, "p99_9_ns": None, "max_ns": None}
    ordered = sorted(values)
    rank = (999 * len(ordered) + 999) // 1000
    return {"samples": len(ordered), "p99_9_ns": ordered[rank - 1], "max_ns": ordered[-1]}


def acceptance(schedule, sender, summary):
    origin = schedule["origin_ns"]
    prefix = f"dur053-{schedule['run_id']}-{schedule['seed']}-"
    slots = schedule_slots(schedule)
    rows = {row["key"]: row for row in sender["sender_samples"]}
    categories = {
        label: []
        for label in (
            "refused",
            "cutoff_after_intent",
            "missing_write",
            "outside_slot",
            "slot_margin",
        )
    }
    windows = {name: ([], [], []) for name in ("warmup", "hold", "step")}
    totals, before, transport = [], [], []
    for i, (at, cutoff) in enumerate(slots):
        key = prefix + f"{i:06d}"
        row = rows.get(key, {})
        intent, entry, written = (
            row.get(name)
            for name in ("dispatch_intent_ns", "transport_entry_ns", "request_written_ns")
        )
        if row.get("outcome") == "unsent":
            categories["refused"].append(key)
        if row.get("error") in {"missed-send-deadline", "cutoff-after-intent"} or (
            row.get("error") == "write-cutoff" and (written is None or origin + written >= cutoff)
        ):
            categories["cutoff_after_intent"].append(key)
        if written is None or entry is None or intent is None:
            categories["missing_write"].append(key)
            continue
        lateness = origin + written - at
        if not 0 <= lateness < cutoff - at:
            categories["outside_slot"].append(key)
        if lateness > min(MAX_NS, 3 * (cutoff - at) // 4):
            categories["slot_margin"].append(key)
        window = (
            "warmup"
            if at < schedule["warmup_end_ns"]
            else "hold"
            if at < schedule["hold_end_ns"]
            else "step"
        )
        for value, all_rows, per_window in zip(
            (written - intent, entry - intent, written - entry),
            (totals, before, transport),
            windows[window],
            strict=True,
        ):
            all_rows.append(value)
            per_window.append(value)
    overall = latency(totals)
    breakdown = {
        name: dict(
            zip(
                ("intent_to_write", "intent_to_transport_entry", "transport_entry_to_write"),
                map(latency, parts),
                strict=True,
            )
        )
        for name, parts in windows.items()
    }
    initial = next((row for row in sender["flushes"] if row["batch"] == 0), {})
    start, end = initial.get("precommit_start_ns"), initial.get("directory_sync_return_ns")
    size, rss = initial.get("precommit_bytes"), initial.get("peak_rss_kib")
    elapsed = end - start if type(start) is int and type(end) is int and start <= end else None
    resources_ok = (
        elapsed is not None
        and end <= -2 * SECOND
        and type(size) is int
        and size > 0
        and type(rss) is int
        and 0 < rss <= OBSERVER_LIMIT // 1024
    )
    pass_ = (
        resources_ok
        and not any(categories.values())
        and not sender["timing_exceptions"]
        and summary["sender_valid"]
        and len(totals) == schedule["count"]
        and overall["p99_9_ns"] <= P999_NS
        and overall["max_ns"] <= MAX_NS
        and all(
            row["intent_to_write"]["samples"] > 0
            and row["intent_to_write"]["p99_9_ns"] <= P999_NS
            and row["intent_to_write"]["max_ns"] <= MAX_NS
            for row in breakdown.values()
        )
    )
    return {
        "status": "PASS" if pass_ else "FAIL",
        "scope": SCOPE,
        "measured_eligible": False,
        "planned": schedule["count"],
        "thresholds_ns": {
            "p99_9": P999_NS,
            "max": MAX_NS,
            "slot": "min(20000000, floor(3*width/4))",
        },
        "precommit": {
            "time_ns": elapsed,
            "bytes": size,
            "peak_rss_kib": rss,
            "start_ns": start,
            "directory_sync_return_ns": end,
            "resource_evidence_valid": resources_ok,
        },
        "intent_to_write": overall,
        "intent_to_transport_entry": latency(before),
        "transport_entry_to_write": latency(transport),
        "timing_failures": categories,
        "windows": breakdown,
        "all_accepted_verdict": summary["all_accepted_verdict"],
        "timing_exception_count": len(sender["timing_exceptions"]),
        "sender_reconciled": summary["sender_valid"],
        "drain": summary["drain"],
    }


def episode_report(root, schedule, summary):
    report = acceptance(schedule, read(root / "sender-report.json"), summary)
    telemetry = cell_samples(read(root / "prometheus-samples.json"), schedule)
    require(
        telemetry == read(root / "cell-scrape-evidence.json"),
        "engineering telemetry reduction differs",
    )
    gaps = sorted(coverage_gaps(telemetry))
    report["telemetry_gaps"] = gaps
    if gaps:
        report["status"] = "FAIL"
    return report


def run_lane(args):
    args.evidence.mkdir(parents=True, exist_ok=True)
    write(args.evidence / "runner-inventory.json", runner_inventory())
    args.scratch.mkdir()
    kind.CLUSTER, kind.LABEL = "j2-r422-lane", "j2.harness=owned"
    driver = DrainHarness(args.evidence, args.scratch, args.checker)
    driver.env.pop("GITHUB_TOKEN", None)
    driver.deadline = time.monotonic() + LIVE_WORK_MINUTES * 60
    driver.live_reservation = 110 * 60
    build = args.evidence / "dispatch-build.json"

    def scenario(driver, images, exporter_images):
        with driver.stage("bootstrap"):
            addresses, exporter = start_profile(driver, images, exporter_images)
        with driver.stage("fresh-soak"):
            driver.fresh_episode("soak", addresses, exporter, None)
            fresh_telemetry(driver, exporter, DB)
        with driver.stage("install-soak"):
            install_episode(driver, "soak", None, {True: (args.sender, build)}, precommit=True)

        def schedule():
            return schedule_for(
                args.campaign, args.hold, args.job, time.time_ns() + PRECOMMIT_LEAD_SECONDS * SECOND
            )

        def release(driver, schedule, *, partial):
            write(
                driver.output / "sender-release.json",
                {
                    "run_id": schedule["run_id"],
                    "telemetry": file_record(driver.output / "pre-traffic-telemetry.json"),
                    "inventory": file_record(args.evidence / "runner-inventory.json"),
                    "unix_ns": time.time_ns(),
                },
            )
            return run_precommitted(driver, schedule, partial=partial)

        with driver.stage("soak"):
            try:
                with attribution_samples(driver, campaign=True):
                    exercise_drain(driver, schedule, stepped=True, execute=release)
            except DrainExpired as error:
                require(
                    error.result["check_status"] == "PASS", "unchecked engineering drain expiry"
                )
            root = driver.output
            summary = summarize_episode(root)
            report = episode_report(root, read(root / "schedule.json"), summary)
            write(root / "episode-replay.json", summary)
            write(root / "engineering-acceptance.json", report)
            require(
                report["status"] == "PASS", "engineering sender margins or reconciliation failed"
            )

    return run(driver, args.layouts, args.exporter_layout, scenario=scenario, scope=SCOPE)


def artifact_name(run, job):
    return f"j2-{MODE}-{run}-1-job-{job}"


def verify_lane_episode(evidence, campaign, job, hold, checker, budget, api):
    root = evidence / "soak"
    expected = read(root / "schedule.json")
    require(
        expected
        == with_state_read_schema(
            schedule_for(campaign.campaign, hold, job, expected["origin_ns"]),
            expected,
            required=True,
        ),
        "archived engineering schedule differs",
    )
    verify_precommit_install(root, evidence / "dispatch-build.json", campaign.head)
    release = read(root / "sender-release.json")
    telemetry = root / "pre-traffic-telemetry.json"
    require(
        not any(read(root / "fresh-db.json")["counts"].values())
        and read(root / "fresh-db.json")["settings"] == ["on", "on", "on", "100"]
        and read(root / "fresh-db.json")["engine_partitions"] == list(range(16))
        and read(telemetry)["freshness"]["distinct_samples"] >= 5
        and release["run_id"] == expected["run_id"]
        and release["telemetry"] == file_record(telemetry)
        and release["inventory"] == file_record(evidence / "runner-inventory.json")
        and release["unix_ns"] < expected["origin_ns"] - 2 * SECOND
        and all(
            not any(partitions.values())
            for partitions in read(root / "before-workloads-transport.json")["latest"].values()
        ),
        "engineering episode did not start from fresh state",
    )
    with budget.stage("checker", api) as remaining:
        checked = checker_replay(root, checker, min(240, remaining()))
    with budget.stage("reduce"):
        summary = summarize_episode(root)
        report = episode_report(root, expected, summary)
        require(
            canonical(summary) == canonical(read(root / "episode-replay.json"))
            and report == read(root / "engineering-acceptance.json")
            and checked["identities"] == expected["count"],
            "independent engineering evidence differs from the live reduction",
        )
        cleanup = read(evidence / "cleanup.json")
        status = read(evidence / "status.json")
        require(
            status["scope"] == SCOPE
            and status["head"] == campaign.head
            and status["run_id"] == str(campaign.run)
            and status["run_attempt"] == "1"
            and status["cleanup_verified"] == (cleanup["status"] == "PASS"),
            "engineering closeout identity differs",
        )
        return {
            **report,
            "checker": checked,
            "cleanup": cleanup["status"],
            "origin_ns": expected["origin_ns"],
            "stop_ns": expected["stop_ns"],
        }


def replay_lane(args):
    args.evidence.mkdir(parents=True, exist_ok=True)
    elapsed = max(0, time.time() - int(os.environ["J2_REPLAY_START"]))
    budget = ReplayBudget(args.evidence, elapsed)
    campaign = Campaign(
        os.environ["GITHUB_REPOSITORY"],
        int(os.environ["GITHUB_RUN_ID"]),
        1,
        os.environ["GITHUB_SHA"],
        args.campaign,
        {},
        26678,
        SCOPE,
    )
    barrier = client(campaign, args.evidence, budget.cutoff)
    names = {artifact_name(campaign.run, job): job for job in JOBS}
    rows, errors = {}, {}
    try:
        with budget.stage("bootstrap") as remaining:
            subprocess.run(
                ["docker", "pull", WORKER_BASE], timeout=min(120, remaining()), check=True
            )

        def discover():
            metadata = barrier.api.artifacts(campaign, set(names))
            if set(metadata) != set(names):
                raise RetryLater("waiting for the three engineering artifacts")
            return metadata

        with budget.stage("download", barrier.api):
            metadata = barrier.retry(discover)
        for name, item in metadata.items():
            item.update(
                source_run=campaign.run, source_attempt=1, job=f"job-{names[name]}", kind=MODE
            )
        inputs = ReplayInputs(barrier, metadata, budget)
        for name, job in names.items():
            try:
                with inputs.archive(name) as (_, root, freeze):
                    rows[str(job)] = {
                        **verify_lane_episode(
                            root, campaign, job, args.hold, args.checker, budget, barrier.api
                        ),
                        "freeze_sha256": freeze,
                        "artifact": metadata[name],
                    }
            except Exception as error:
                errors[str(job)] = str(error)
        write(args.evidence / "archive-index.json", inputs.index)
    except Exception as error:
        errors["discovery"] = str(error)
    overlap = (
        max(
            0,
            min(row["stop_ns"] for row in rows.values())
            - max(row["origin_ns"] for row in rows.values()),
        )
        if len(rows) == 3
        else 0
    )
    common_step = (
        min(row["stop_ns"] for row in rows.values())
        - max(row["origin_ns"] + (WARMUP + HOLD) * SECOND for row in rows.values())
        if len(rows) == 3
        else 0
    )
    passed = (
        not errors
        and overlap > 0
        and all(row["status"] == "PASS" and row["cleanup"] == "PASS" for row in rows.values())
    )
    write(
        args.evidence / "engineering-replay.json",
        {
            "status": "PASS" if passed else "FAIL",
            "scope": SCOPE,
            "measured_eligible": False,
            "campaign": args.campaign,
            "head": campaign.head,
            "run": campaign.run,
            "attempt": 1,
            "hold": args.hold,
            "jobs": rows,
            "errors": errors,
            "common_offering_ns": overlap,
            "common_step_ns": max(0, common_step),
            "budget": budget.record(),
        },
    )
    return 0 if passed else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("gate")
    live = commands.add_parser("hosted")
    live.add_argument("job", type=int, choices=JOBS)
    for name in ("evidence", "scratch", "layouts", "exporter_layout", "sender", "checker"):
        live.add_argument(name, type=Path)
    replay = commands.add_parser("replay")
    replay.add_argument("evidence", type=Path)
    replay.add_argument("checker", type=Path)
    args = parser.parse_args()
    hosted_dispatch()
    require(os.environ.get("GITHUB_RUN_ATTEMPT") == "1", "R422 requires attempt one")
    args.campaign, args.hold = os.environ["J2_CAMPAIGN"], os.environ["J2_R422_HOLD"]
    gate(
        args.campaign,
        args.hold,
        os.environ.get("J2_AUTHORIZATION_REF", ""),
        os.environ["GITHUB_REF"],
    )
    if args.command == "gate":
        return 0
    return run_lane(args) if args.command == "hosted" else replay_lane(args)


if __name__ == "__main__":
    raise SystemExit(main())
