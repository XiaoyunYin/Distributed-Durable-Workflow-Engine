"""Hosted full-clock J2 engineering rehearsal for step continuity and quiet drain."""

from __future__ import annotations

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

import dur053_k3 as kind
from dur053_k3_policy import stable_worker_sessions
from j2_checker_report import indexed_report
from j2_drain import (
    DrainExpired,
    IntegrityError,
    checked_results,
    cohort_counts,
    expected_arrivals,
    fingerprint,
    quiet_drain,
    replay_episode,
)
from j2_harness import APP, DB, Harness, hosted_dispatch, run
from j2_offline import SECOND, clock_bounds, utc_ns
from j2_qualification_evidence import DRAIN_SECONDS, file_record, read, require, write
from j2_qualification_resources import checker_samples, sample_checker
from j2_qualify_volume import OBSERVER_LIMIT, ObserverEvidence, prometheus, retain_scrapes, watch
from j2_schedule import (
    ENGINEERING,
    MEASURED,
    RATES,
    WARMUP,
    continuous_schedule,
    k4_report,
    stock_schedule,
)
from j2_schedule_rehearsal import (
    JOURNAL,
    LEAD_SECONDS,
    SCHEDULE_POD_IMPORTS,
    STEP_JOURNAL,
    install_sender,
    observer_resources,
    run_continuous,
    run_stock,
)
from j2_verify_overlay import start_profile

HOLD = 1800
STEP = 300
ENGINEERING_SLEEP_MS = 150
POPULATION_LIMIT = 26678
WORK_MINUTES = 75
TERMINAL_SQL = (
    "SELECT workflow_id FROM engine.workflow_executions "
    "WHERE state IN ('SUCCEEDED','FAILED','CANCELED','REJECTED','ABANDONED') ORDER BY 1;"
)
PENDING_SQL = (
    "SELECT workflow_id FROM engine.workflow_executions "
    "WHERE state NOT IN ('SUCCEEDED','FAILED','CANCELED','REJECTED','ABANDONED') ORDER BY 1;"
)
# All identity and fingerprint columns come from one PostgreSQL snapshot.
# Exclude continuously renewed ownership leases from the quiet fingerprint.
STATE_SQL = """
SELECT json_build_object(
    'submissions', (SELECT coalesce(json_agg(json_build_object(
        'id', workflow_id, 'key', submission_key, 'namespace', namespace,
        'hash', submission_payload_hash, 'revision', revision, 'state', state
    ) ORDER BY workflow_id), '[]') FROM engine.workflow_executions),
    'succeeded', (SELECT coalesce(json_agg(w.workflow_id ORDER BY w.workflow_id), '[]')
        FROM engine.workflow_executions w JOIN engine.transition_history h
          ON h.workflow_id=w.workflow_id AND h.revision=w.revision
        WHERE w.state='SUCCEEDED' AND h.new_state='SUCCEEDED'),
    'terminal_failures', (SELECT count(*) FROM engine.workflow_executions
        WHERE state IN ('FAILED','CANCELED','REJECTED','ABANDONED')),
    'unpublished', (SELECT count(*) FROM engine.outbox WHERE published_at IS NULL),
    'claimed', (SELECT count(*) FROM engine.activity_attempts WHERE state='CLAIMED'),
    'terminal_revisions', (SELECT md5(string_agg(workflow_id || ':' || revision || ':' || xmin,
        ',' ORDER BY workflow_id, revision)) FROM engine.transition_history
        WHERE new_state IN ('SUCCEEDED','FAILED','CANCELED','REJECTED','ABANDONED')),
    'attempts', (SELECT md5(string_agg(workflow_id || ':' || node_id || ':' || iteration || ':'
        || attempt_number || ':' || xmin, ','
        ORDER BY workflow_id,node_id,iteration,attempt_number))
        FROM engine.activity_attempts),
    'outbox', (SELECT md5(string_agg(event_id || ':' || xmin, ',' ORDER BY event_id))
        FROM engine.outbox),
    'receipts', (SELECT md5(string_agg(evidence_id || ':' || xmin, ',' ORDER BY evidence_id))
        FROM engine.attempt_result_evidence));
"""


class DrainHarness(ObserverEvidence, Harness):
    app = APP

    def __init__(self, output, scratch, checker):
        super().__init__(output, scratch, checker)
        self.deadline = time.monotonic() + 60 * WORK_MINUTES
        self.live_reservation = 100 * 60
        self.pod_imports = (*SCHEDULE_POD_IMPORTS, "resource", "shutil")

    def read_database(self, sql):
        result = self.command(
            [
                "docker",
                "exec",
                "-i",
                kind.CLUSTER + "-postgres",
                "psql",
                "-XAtq",
                "-v",
                "ON_ERROR_STOP=1",
                "-U",
                "durable",
                "-d",
                DB,
            ],
            data="BEGIN READ ONLY;\nSET LOCAL statement_timeout = '2s';\n" + sql + "\nCOMMIT;",
            timeout=8,
            record=False,
        )
        return result.stdout.strip()

    def worker_uids(self):
        pods = self.kube("pods", "-n", APP, "-l", "app=worker")["items"]
        stable_worker_sessions({p["metadata"]["uid"] for p in self.workers}, pods)
        return sorted(p["metadata"]["uid"] for p in pods)


class Polls:
    def __init__(self, output):
        self.path = output / "terminal-polls.jsonl"
        self.first = {}

    def retain(self, ids, bracket):
        new = sorted(set(ids) - self.first.keys())
        self.first.update(dict.fromkeys(new, bracket["upper_unix_ns"]))
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({**bracket, "terminal_ids": new}) + "\n")


def bracketed(read):
    before_mono, before_wall = time.monotonic_ns(), time.time_ns()
    value = read()
    return value, {
        "lower_mono_ns": before_mono,
        "lower_unix_ns": before_wall,
        "upper_unix_ns": time.time_ns(),
        "upper_mono_ns": time.monotonic_ns(),
    }


def observation(driver, polls):
    def snapshot():
        db = json.loads(driver.read_database(STATE_SQL))
        witness = sorted(driver.witness_inventories(), key=lambda row: row["pod_uid"])
        return db, witness

    try:
        (db, witness), bracket = bracketed(snapshot)
    except ValueError as error:
        raise OSError(f"bracketed state unavailable: {error}") from error
    polls.retain(db["succeeded"], bracket)
    return {"db": db, "witness": witness, **bracket}


def wait_until(event, target_ns):
    return not event.wait(max(0, (target_ns - time.monotonic_ns()) / SECOND))


def carry_probe(driver, schedule, step_mono, cancelled):
    """Hold dispatch at the boundary without replacing workers or stopping the sender."""
    if not wait_until(cancelled, step_mono - 4 * SECOND):
        return
    output = driver.output
    broker = kind.CLUSTER + "-kafka"
    before_uids = driver.worker_uids()
    record = {
        "scope": ENGINEERING,
        "intervention": "brief Kafka pause",
        "worker_sleep_ms": driver.worker_sleep_ms,
        "worker_uids_before": before_uids,
    }
    try:
        _, record["pause"] = bracketed(
            lambda: driver.command(["docker", "pause", broker], timeout=8)
        )
        if not wait_until(cancelled, step_mono - SECOND // 2):
            return
        pending, before = bracketed(lambda: driver.read_database(PENDING_SQL).splitlines())
        prefix = f"dur053-{schedule['run_id']}-{schedule['seed']}-"
        hold_count = schedule["periods"][0]["count"]
        hold_keys = {f"{prefix}{sequence:06d}" for sequence in range(hold_count)}
        record["pending_before_step"] = sorted(hold_keys.intersection(pending))
        record["before_step_read"] = before
        if not wait_until(cancelled, step_mono + SECOND // 4):
            return
    finally:
        result, resumed = bracketed(
            lambda: driver.command(["docker", "unpause", broker], timeout=8, allowed=True)
        )
        record["resume"] = resumed
        record["resume_exit"] = result.returncode
        write(output / "step-transition.json", record)
        if "pause" in record:
            require(result.returncode == 0, "engineering broker resume failed")
    record["worker_uids_after"] = driver.worker_uids()
    write(output / "step-transition.json", record)
    require(
        record["pending_before_step"]
        and before["upper_mono_ns"] < step_mono
        and resumed["lower_mono_ns"] >= step_mono,
        "engineering transition lacked a bracketed pending hold identity",
    )


def retain_sender(driver, schedule, stop_mono, polls, errors, *, stepped):
    execute = run_continuous if stepped else run_stock
    cancelled = Event()
    with ThreadPoolExecutor(max_workers=2) as pool:
        sender = pool.submit(execute, driver, schedule, partial=False)
        if stepped:
            step_mono = stop_mono - STEP * SECOND
            carry = pool.submit(carry_probe, driver, schedule, step_mono, cancelled)
        try:
            while time.monotonic_ns() < stop_mono:
                if errors:
                    raise IntegrityError(str(errors[0])) from errors[0]
                if sender.done():
                    outcome = sender.result()
                    if (
                        outcome.returncode not in (0, 1)
                        or not (driver.output / driver.journal_name).stat().st_size
                    ):
                        raise IntegrityError(
                            f"sender failed before scheduled end: {outcome.returncode}"
                        )
                if stepped and carry.done():
                    carry.result()
                ids, bracket = bracketed(lambda: driver.read_database(TERMINAL_SQL).splitlines())
                polls.retain(ids, bracket)
                time.sleep(max(0, min(4, (stop_mono - time.monotonic_ns()) / SECOND)))
            # The first drain read happens at the scheduled stop, even while
            # the HTTP client is returning its last responses.
            initial = observation(driver, polls)
            write(driver.output / "scheduled-stop-read.json", initial)
            while not sender.done():
                time.sleep(1)
                observation(driver, polls)
            result = sender.result()
            if stepped:
                carry.result()
        finally:
            cancelled.set()
    write(
        driver.output / "sender-exit.json",
        {
            "exec_exit": result.returncode,
            "returned_with_journal_unix_ns": time.time_ns(),
        },
    )
    (driver.output / "sender-events.jsonl").write_text(result.stdout, encoding="utf-8")
    require(
        result.returncode in (0, 1) and read(driver.output / "journal-read.json")["exit"] == 0,
        "sender or journal copy failed",
    )


def capture_checker(driver, expected_pods, errors, state, arrivals, sender, polls, uncertainty):
    driver.witness(state["witness"], upload=False)
    pod = f"{APP}/{driver.observer}"
    uid = expected_pods[pod]["uid"]
    leaf = driver.container_groups[pod, uid]
    with sample_checker(driver.output, driver.node_exec[-1], leaf, driver.env) as sampler:
        status = driver.run_stage(
            "live-checker",
            driver.checker_args("-namespace", APP, "-require-terminal"),
            DRAIN_SECONDS,
            stdout="live-checker.json",
            monitor_errors=errors,
        )
    sampling = {"exit": sampler.returncode, **checker_samples(driver.output)}
    sampling["limit_bytes"] = OBSERVER_LIMIT
    write(driver.output / "checker-resource-summary.json", sampling)
    require(
        status == sampling["exit"] == 0
        and 0 < sampling["observer_peak_bytes"] <= OBSERVER_LIMIT
        and sampling["checker_vmhwm_peak_kib"],
        "locked checker or observer resource evidence failed",
    )
    report, tables = indexed_report(
        driver.output / "live-checker.json",
        snapshot=driver.output / "snapshot.json",
        join_trace=True,
        verify_tables=True,
    )
    write(driver.output / "live-checker-tables.json", tables)
    checked_results(
        report,
        arrivals,
        sender,
        read(driver.output / "witness.json"),
        polls.first,
        uncertainty,
        state["upper_unix_ns"],
    )
    rows = report["trace"]["Snapshot"]["engine.workflow_executions"]["rows"]
    revisions = {row["workflow_id"]: row["revision"] for row in rows}
    require(
        revisions == {row["id"]: row["revision"] for row in state["db"]["submissions"]},
        "checker snapshot revisions differ from bracketed DB state",
    )


def episode_schedule(name, *, stepped):
    campaign = f"eng-{os.environ['GITHUB_RUN_ID']}-{os.environ['GITHUB_RUN_ATTEMPT']}"
    common = (
        campaign,
        "job-1",
        name,
        f"j2-{campaign}-{name}",
        APP,
        time.time_ns() + LEAD_SECONDS * SECOND,
    )
    if stepped:
        schedule = continuous_schedule(
            *common, ("1", WARMUP + HOLD), (RATES["C"], STEP), warmup=WARMUP
        )
    else:
        schedule = stock_schedule(*common, "1", WARMUP, MEASURED)
    schedule["worker_sleep_ms"] = ENGINEERING_SLEEP_MS if stepped else 100
    schedule["intervention"] = "brief Kafka pause at step" if stepped else None
    require(schedule["count"] <= POPULATION_LIMIT, "episode exceeds the registered observer volume")
    return schedule


def exercise(driver, name, *, stepped):
    output = driver.output
    polls = Polls(output)
    work_deadline = driver.deadline
    with prometheus(driver) as query, observer_resources(driver), watch(driver) as (pods, errors):
        setup = driver.clock()
        uncertainty = utc_ns(setup["observer_after"]) - utc_ns(setup["observer_before"])
        require(
            utc_ns(setup["observer_before"])
            <= utc_ns(setup["database"])
            <= utc_ns(setup["observer_after"]),
            "setup clock probe did not bracket database time",
        )
        schedule = episode_schedule(name, stepped=stepped)
        anchor_wall, anchor_mono = time.time_ns(), time.monotonic_ns()
        stop_mono = anchor_mono + schedule["stop_ns"] - anchor_wall
        write(
            output / "deadline-anchor.json",
            {
                "anchor_unix_ns": anchor_wall,
                "anchor_mono_ns": anchor_mono,
                "stop_mono_ns": stop_mono,
            },
        )
        deadline = stop_mono + DRAIN_SECONDS * SECOND
        require(deadline / SECOND < work_deadline, "episode would exceed harness work budget")
        driver.deadline = deadline / SECOND
        try:
            retain_sender(driver, schedule, stop_mono, polls, errors, stepped=stepped)
            binding = file_record(output / "schedule.json")
            require(read(output / "schedule.json") == schedule, "schedule changed after launch")
            write(output / "schedule-binding.json", binding)
            raw = (output / "sender-journal.jsonl").read_bytes()
            arrivals, sender = expected_arrivals(raw, schedule)

            def observe():
                if errors:
                    raise IntegrityError(str(errors[0])) from errors[0]
                return observation(driver, polls)

            def check(state):
                capture_checker(driver, pods, errors, state, arrivals, sender, polls, uncertainty)

            def retain(label, data):
                if label in {"poll", "read-error", "check-retry", "quiet-start"}:
                    with (output / "drain-polls.jsonl").open("a", encoding="utf-8") as log:
                        log.write(json.dumps({"event": label, **data}) + "\n")
                else:
                    write(output / f"{label}.json", data)

            result = quiet_drain(
                stop_ns=stop_mono,
                observe=observe,
                check=check,
                expected=arrivals,
                sender=sender,
                retain=retain,
            )
            # Final evidence must also close within the registered deadline.
            final = read(output / "final.json")
            probes = {"setup": setup, "measured": driver.clock()}
            clock_bounds(probes, schedule["origin_ns"], final["upper_unix_ns"])
            require(time.monotonic_ns() <= deadline, "final clock bracket exceeded drain allowance")
            write(output / "clock.json", probes)
            write(output / "drain-result.json", result)
        except (OSError, ValueError) as error:
            if not isinstance(error, DrainExpired) and time.monotonic_ns() >= deadline:
                raise DrainExpired(
                    "600 s drain allowance expired during sender/export closeout"
                ) from error
            raise
        finally:
            driver.deadline = work_deadline
        durable = [row["id"] for row in final["db"]["submissions"]]
        report = k4_report(raw, schedule, durable)
        write(output / "sender-report.json", report)
        joined, _ = replay_episode(output)
        if stepped:
            carry = read(output / "step-transition.json")
            pending_at_step = set(carry["pending_before_step"])
            crossed = [
                row["workflow_id"]
                for row in joined
                if row["workflow_id"] in pending_at_step
                and row["scheduled_ns"] < schedule["hold_end_ns"] <= row["terminal_ns"]
            ]
            require(crossed, "no checked hold identity completed at or after the step")
            carry["verified_crossing_ids"] = crossed
            write(output / "step-transition.json", carry)
        write(output / "phase-rows.json", joined)
        write(output / "cohorts.json", cohort_counts(schedule, joined))
        memory = driver.health(pods)["observer"]
        write(
            output / "observer-budget.json",
            {
                "offered": schedule["count"],
                "admitted": len(durable),
                "qualified_identity_limit": POPULATION_LIMIT,
                "memory": memory,
                "drain_allowance_seconds": DRAIN_SECONDS,
                "checked_export_seconds": result["checked_export_seconds"],
                "drain_seconds": result["drain_seconds"],
            },
        )
        if errors:
            raise IntegrityError(str(errors[0])) from errors[0]
        retain_scrapes(
            driver,
            query,
            schedule["origin_ns"] / SECOND,
            final["upper_unix_ns"] / SECOND,
            len(durable),
        )
    require(fingerprint(final) == result["final_fingerprint"], "final read identity drift")


def rehearse(driver, layouts, exporter_layout, binaries):
    with driver.stage("bootstrap"):
        addresses, exporter = start_profile(driver, layouts, exporter_layout)
    previous = None
    for name, stepped in (("grid-drain", False), ("step-drain", True)):
        driver.worker_sleep_ms = ENGINEERING_SLEEP_MS if stepped else 100
        with driver.stage(f"fresh-{name}"):
            previous = driver.fresh_episode(name, addresses, exporter, previous)
        with driver.stage(f"install-{name}"):
            driver.observer = driver.pod(APP, "observer")["metadata"]["name"]
            driver.workers = driver.kube("pods", "-n", APP, "-l", "app=worker")["items"]
            driver.pod_journal = STEP_JOURNAL if stepped else JOURNAL
            driver.journal_name = "sender-journal.jsonl"
            driver.install_checker()
            binary, manifest = binaries[stepped]
            install_sender(driver, binary, driver.evidence / manifest, continuous=stepped)
        with driver.stage(name):
            exercise(driver, name, stepped=stepped)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in (
        "evidence",
        "scratch",
        "layouts",
        "exporter_layout",
        "stock",
        "continuous",
        "checker",
    ):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    hosted_dispatch()
    args.evidence.mkdir(parents=True, exist_ok=True)
    args.scratch.mkdir()
    kind.CLUSTER, kind.LABEL = "j2-harness", "j2.harness=owned"
    binaries = {
        False: (args.stock, "stock-build.json"),
        True: (args.continuous, "continuous-build.json"),
    }
    return run(
        DrainHarness(args.evidence, args.scratch, args.checker),
        args.layouts,
        args.exporter_layout,
        scenario=lambda d, images, exporter: rehearse(d, images, exporter, binaries),
        scope=f"unmeasured J2 schedule 2b2 ({ENGINEERING})",
    )


if __name__ == "__main__":
    raise SystemExit(main())
