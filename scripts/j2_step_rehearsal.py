"""Hosted full-clock J2 engineering rehearsal for step continuity and quiet drain."""

from __future__ import annotations

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar, copy_context
from pathlib import Path
from threading import Event

import dur053_k3 as kind
from dur053_k3_policy import stable_worker_sessions
from j2_checker_report import (
    ResourceEvidenceError,
    indexed_report,
    require_checker_exit,
    require_checker_pass,
)
from j2_drain import (
    DRAIN_POLL_LOG,
    POLL_SECONDS,
    STATE_READ_LOG,
    STATE_READ_SCHEMA,
    STOP_READ,
    DrainExpired,
    IntegrityError,
    StateReadError,
    checked_cut,
    checked_results,
    cohort_counts,
    expected_arrivals,
    expire_drain,
    fingerprint,
    quiet_drain,
    readiness,
    replay_episode,
    retain_read_error,
    state_read_binding,
)
from j2_harness import APP, DB, Harness, hosted_dispatch, run
from j2_offline import SECOND, clock_bounds, utc_ns
from j2_qualification_evidence import DRAIN_SECONDS, file_record, read, require, write
from j2_qualification_resources import (
    checker_resource_summary,
    require_checker_resources,
    sample_checker,
)
from j2_qualify_volume import OBSERVER_LIMIT, ObserverEvidence, prometheus, retain_scrapes, watch
from j2_relay_diagnostics import bracketed, relay_attribution
from j2_schedule import (
    DISPATCH_JOURNAL,
    ENGINEERING,
    MEASURED,
    RATES,
    WARMUP,
    continuous_schedule,
    sender_report,
    stock_schedule,
)
from j2_schedule_rehearsal import (
    JOURNAL,
    LEAD_SECONDS,
    SCHEDULE_POD_IMPORTS,
    STEP_JOURNAL,
    container_target,
    install_sender,
    observer_resources,
    run_continuous,
    run_stock,
)
from j2_verify_overlay import start_profile

HOLD = 1800
STEP = 300
HOLD_RATES = ("1", "4", "8")
POPULATION_LIMIT = 26678
WORK_MINUTES = 75
# The longest completed full-clock expiry took 80.994 s after the drain cut.
EXPIRY_HANDOFF_SECONDS = 10
EXPIRY_EXPORT_SECONDS = 120
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
# Read only server metadata after a timed-out snapshot. Omit session query text.
STATE_CONTENTION_SQL = """
WITH activity AS MATERIALIZED (
    SELECT pid, backend_type, state, wait_event_type, wait_event, query_start, xact_start
    FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()
)
SELECT json_build_object(
    'pg_stat_activity', (SELECT coalesce(json_agg(a ORDER BY pid), '[]') FROM activity a),
    'pg_locks', (SELECT coalesce(json_agg(l ORDER BY pid, locktype, mode), '[]')
        FROM pg_locks l
        WHERE database = (SELECT oid FROM pg_database WHERE datname = current_database())
            OR pid IN (SELECT pid FROM activity)));
"""
STATE_DIAGNOSTIC_TIMEOUT = 1
STATE_DIAGNOSTIC_STATEMENT_TIMEOUT = "250ms"


class StateDatabaseError(OSError):
    def __init__(self, returncode, stderr):
        super().__init__(f"psql exited {returncode}: {stderr.strip()}")
        self.timed_out = "canceling statement due to statement timeout" in stderr.lower()


class DrainHarness(ObserverEvidence, Harness):
    app = APP

    def __init__(self, output, scratch, checker):
        self._command_deadline = ContextVar("j2_command_deadline", default=None)
        super().__init__(output, scratch, checker)
        self.deadline = time.monotonic() + 60 * WORK_MINUTES
        self.live_reservation = 100 * 60
        self.pod_imports = (*SCHEDULE_POD_IMPORTS, "resource", "shutil")

    @property
    def deadline(self):
        current = self._command_deadline.get()
        return self._work_deadline if current is None else current

    @deadline.setter
    def deadline(self, value):
        self._work_deadline = value

    @contextmanager
    def command_deadline(self, value):
        token = self._command_deadline.set(value)
        try:
            yield
        finally:
            self._command_deadline.reset(token)

    def read_database(self, sql, *, timeout=8, statement_timeout="2s", inspect_error=False):
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
            data=f"BEGIN READ ONLY;\nSET LOCAL statement_timeout = '{statement_timeout}';\n"
            + sql
            + "\nCOMMIT;",
            timeout=timeout,
            allowed=inspect_error,
            record=False,
        )
        if result.returncode != 0:
            raise StateDatabaseError(result.returncode, result.stderr)
        return result.stdout.strip()

    def worker_uids(self):
        pods = self.kube("pods", "-n", APP, "-l", "app=worker")["items"]
        stable_worker_sessions({p["metadata"]["uid"] for p in self.workers}, pods)
        return sorted(p["metadata"]["uid"] for p in pods)


class CarryInUnobserved(ValueError):
    classification = "carry_in_unobserved"


class Polls:
    def __init__(self, output):
        self.path = output / "terminal-polls.jsonl"
        self.first = {}

    def retain(self, ids, bracket):
        new = sorted(set(ids) - self.first.keys())
        self.first.update(dict.fromkeys(new, bracket["upper_unix_ns"]))
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({**bracket, "terminal_ids": new}) + "\n")


def read_bracket(probe):
    lower, wall = time.monotonic_ns(), time.time_ns()
    value = error = None
    try:
        value = probe()
    except (OSError, ValueError) as failure:
        error = failure
    upper_wall, upper = time.time_ns(), time.monotonic_ns()
    return (
        value,
        error,
        {
            "lower_mono_ns": lower,
            "upper_mono_ns": upper,
            "lower_unix_ns": wall,
            "upper_unix_ns": upper_wall,
            "duration_ns": upper - lower,
        },
    )


def state_contention(driver):
    def query():
        data = json.loads(
            driver.read_database(
                STATE_CONTENTION_SQL,
                timeout=STATE_DIAGNOSTIC_TIMEOUT,
                statement_timeout=STATE_DIAGNOSTIC_STATEMENT_TIMEOUT,
                inspect_error=True,
            )
        )
        require(
            isinstance(data, dict)
            and data.keys() == {"pg_stat_activity", "pg_locks"}
            and all(
                isinstance(rows, list) and all(isinstance(row, dict) for row in rows)
                for rows in data.values()
            ),
            "invalid state-read contention snapshot",
        )
        return data

    evidence, error, bracket = read_bracket(query)
    return (
        {**bracket, "status": "error", "error": str(error)}
        if error is not None
        else {
            **bracket,
            "status": "ok",
            **evidence,
        }
    )


class StateReads:
    def __init__(self, output, stop):
        self.output, self.stop = output, stop
        self.deadline = stop + DRAIN_SECONDS * SECOND
        self.initial = None
        self.count = 0
        for path in (STATE_READ_LOG, DRAIN_POLL_LOG):
            (output / path).touch()

    def append(self, record):
        with (self.output / STATE_READ_LOG).open("a", encoding="utf-8") as log:
            log.write(json.dumps(record) + "\n")

    def retain(self, label, data):
        if label in {"poll", "read-error", "check-retry", "check-invalid", "quiet-start"}:
            with (self.output / DRAIN_POLL_LOG).open("a", encoding="utf-8") as log:
                log.write(json.dumps({"event": label, **data}) + "\n")
        else:
            write(self.output / f"{label}.json", data)


def observation(driver, polls, reads, phase):
    db_duration = None
    source = "database"

    def snapshot():
        nonlocal db_duration, source
        started = time.monotonic_ns()
        try:
            db = json.loads(driver.read_database(STATE_SQL, inspect_error=True))
        finally:
            db_duration = time.monotonic_ns() - started
        source = "witness"
        witness = sorted(driver.witness_inventories(), key=lambda row: row["pod_uid"])
        return db, witness

    values, error, bracket = read_bracket(snapshot)
    reads.count += 1
    record = {**bracket, "read_id": reads.count, "phase": phase, "db_duration_ns": db_duration}
    if error is not None:
        timed_out = isinstance(error, TimeoutError) or getattr(error, "timed_out", False)
        record.update(
            status="error",
            error=f"bracketed state unavailable: {error}",
            timed_out=timed_out,
            source=source,
        )
        if timed_out:
            record["contention"] = state_contention(driver)
        reads.append(record)
        raise StateReadError(record) from error
    db, witness = values
    state = {"db": db, "witness": witness, **bracket, "read_id": reads.count}
    reads.append({**record, "status": "ok", "fingerprint": fingerprint(state)})
    polls.retain(db["succeeded"], bracket)
    if (
        reads.initial is None
        and reads.stop <= state["lower_mono_ns"] <= state["upper_mono_ns"] <= reads.deadline
    ):
        reads.initial = state
        write(reads.output / STOP_READ, state)
    return state


def wait_until(event, target_ns):
    return not event.wait(max(0, (target_ns - time.monotonic_ns()) / SECOND))


def check_monitor(errors):
    if errors:
        error = errors[0]
        if isinstance(error, ResourceEvidenceError):
            raise error
        raise IntegrityError(str(error)) from error


def observed_carry(reads, step_mono):
    before, after = set(), set()
    before_reads = after_reads = 0
    for row in reads:
        lower, upper = row["lower_mono_ns"], row["upper_mono_ns"]
        if step_mono - SECOND <= lower <= upper < step_mono:
            before.update(row["pending_hold_ids"])
            before_reads += 1
        elif step_mono <= lower <= upper <= step_mono + SECOND:
            after.update(row["pending_hold_ids"])
            after_reads += 1
    return {
        "valid_before_reads": before_reads,
        "valid_after_reads": after_reads,
        "pending_before_ids": sorted(before),
        "pending_after_ids": sorted(after),
        "observed_carry_ids": sorted(before & after),
    }


def verified_crossings(carry, checked, step_ns, clock_width, *, pending=()):
    if carry["step_unix_ns"] != step_ns:
        raise IntegrityError("carry-in probe step differs from the schedule")
    reads = observed_carry(carry["reads"], carry["step_mono_ns"])
    before, after = (set(reads[key]) for key in ("pending_before_ids", "pending_after_ids"))
    terminals = {
        row["workflow_id"]: row["terminal_ns"] for row in checked if row["scheduled_ns"] < step_ns
    }
    # A checked expiry may still contain pending submissions. A checked
    # terminal, when present, must agree with every valid after-step read.
    unreconciled = after - terminals.keys() - set(pending)
    unreconciled.update(key for key in after & terminals.keys() if terminals[key] < step_ns)
    if unreconciled:
        raise IntegrityError(
            f"after-step pending IDs lack consistent checked terminals: {sorted(unreconciled)}"
        )
    return sorted(
        key
        for key in before & terminals.keys()
        if terminals[key] >= step_ns + clock_width or key in after
    )


def replay_crossings(root):
    """Recompute the crossing IDs from the retained harness reads and checked phase rows."""
    schedule = read(root / "schedule.json")
    carry = read(root / "step-transition.json")
    checked = read(root / "phase-rows.json")
    step_ns = schedule["hold_end_ns"]
    last_observation = max((row["first_observed_ns"] for row in checked), default=step_ns)
    width = clock_bounds(
        read(root / "clock.json"), schedule["origin_ns"], max(step_ns, last_observation)
    )
    result_path = root / "drain-result.json"
    pending = read(result_path)["pending"] if result_path.is_file() else ()
    return verified_crossings(carry, checked, step_ns, width, pending=pending)


def carry_probe(driver, schedule, step_mono, cancelled):
    """Observe admitted hold identities on both sides of the live rate change."""
    if not wait_until(cancelled, step_mono - 4 * SECOND):
        return
    prefix = f"dur053-{schedule['run_id']}-{schedule['seed']}-"
    hold_keys = {f"{prefix}{sequence:06d}" for sequence in range(schedule["periods"][0]["count"])}
    record = {
        "scope": ENGINEERING,
        "hold_rate_per_second": schedule["rate"],
        "worker_sleep_ms": driver.worker_sleep_ms,
        "step_mono_ns": step_mono,
        "step_unix_ns": schedule["hold_end_ns"],
        "reads": [],
    }
    try:
        record["worker_uids_before"] = driver.worker_uids()
        for offset_ms in (-750, -400, -150, -60, 0, 200, 400):
            if not wait_until(cancelled, step_mono + offset_ms * 1_000_000):
                return
            pending, bracket = bracketed(lambda: driver.read_database(PENDING_SQL).splitlines())
            lower, upper = bracket["lower_mono_ns"], bracket["upper_mono_ns"]
            record["reads"].append(
                {
                    **bracket,
                    "pending_hold_ids": sorted(hold_keys & set(pending)),
                    "duration_ns": upper - lower,
                    "straddles_step": lower < step_mono <= upper,
                }
            )
        record["worker_uids_after"] = driver.worker_uids()
        require(
            record["worker_uids_before"] == record["worker_uids_after"],
            "worker identities changed across the step",
        )
        record.update(observed_carry(record["reads"], step_mono))
        require(
            record["valid_before_reads"] and record["valid_after_reads"],
            "carry-in reads did not bracket the scheduled step",
        )
        record["status"] = "observed" if record["observed_carry_ids"] else "unobserved"
    except (OSError, ValueError) as error:
        record.update(status="invalid", error=str(error))
        raise
    finally:
        write(driver.output / "step-transition.json", record)


def retain_stop_reads(stop, done, observe, retain, *, now=time.monotonic_ns, sleep=time.sleep):
    deadline = stop + DRAIN_SECONDS * SECOND
    while now() < deadline:
        try:
            state = observe()
        except OSError as error:
            retain_read_error(error, retain, now)
        else:
            if state["upper_mono_ns"] > deadline or done():
                break
        delay = min(POLL_SECONDS, (deadline - now()) / SECOND)
        if delay > 0:
            sleep(delay)


def retain_sender(
    driver, schedule, stop_mono, polls, errors, reads, retain, *, stepped, execute=None
):
    if execute is None:
        execute = run_continuous if stepped else run_stock
    cancelled = Event()
    with ThreadPoolExecutor(max_workers=2) as pool:
        sender = pool.submit(copy_context().run, execute, driver, schedule, partial=False)
        if stepped:
            step_mono = stop_mono - (schedule["stop_ns"] - schedule["hold_end_ns"])
            carry = pool.submit(
                copy_context().run, carry_probe, driver, schedule, step_mono, cancelled
            )
        try:
            while time.monotonic_ns() < stop_mono:
                check_monitor(errors)
                if sender.done():
                    outcome = sender.result()
                    if (
                        schedule.get("dispatch_journal") == DISPATCH_JOURNAL
                        and outcome.returncode == 2
                    ):
                        write(driver.output / "sender-exit.json", {"exec_exit": outcome.returncode})
                        (driver.output / "sender-events.jsonl").write_text(
                            outcome.stdout, encoding="utf-8"
                        )
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

            def stop_observe():
                check_monitor(errors)
                return observation(driver, polls, reads, "sender")

            retain_stop_reads(stop_mono, sender.done, stop_observe, retain)
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


def capture_checker(driver, expected_pods, errors, *, terminal):
    pod = f"{APP}/{driver.observer}"
    uid = expected_pods[pod]["uid"]
    leaf = driver.container_groups.get((pod, uid))
    if leaf is None:
        raise ResourceEvidenceError("observer cgroup is missing from checker resource evidence")
    args = ["-namespace", APP]
    if terminal:
        args.append("-require-terminal")
    for name in ("snapshot.json", "live-checker-read.json", "live-checker-tables.json"):
        (driver.output / name).unlink(missing_ok=True)
    sampler = status = None
    try:
        with sample_checker(driver.output, driver.node_exec[-1], leaf, driver.env) as sampler:
            status, bracket = bracketed(
                lambda: driver.run_stage(
                    "live-checker",
                    driver.checker_args(*args),
                    DRAIN_SECONDS,
                    stdout="live-checker.json",
                    monitor_errors=errors,
                )
            )
    except OSError as error:
        if sampler is not None:
            if status is None:
                raise
            require_checker_exit(status)
        raise ResourceEvidenceError(f"checker resource sampling failed: {error}") from error
    write(driver.output / "live-checker-read.json", {**bracket, "exit": status})
    sampling = checker_resource_summary(driver.output, sampler.returncode, OBSERVER_LIMIT)
    write(driver.output / "checker-resource-summary.json", {**sampling, "exit": sampler.returncode})
    try:
        report, tables = indexed_report(
            driver.output / "live-checker.json",
            snapshot=driver.output / "snapshot.json",
            join_trace=True,
            verify_tables=True,
            drain_counts=not terminal,
        )
    except (OSError, ValueError, KeyError) as error:
        require_checker_exit(status)
        raise IntegrityError(f"locked checker report is incomplete: {error}") from error
    write(driver.output / "live-checker-tables.json", tables)
    require_checker_exit(status)
    require_checker_pass(report)
    return report, bracket, sampling


def episode_schedule(name, *, stepped, hold_rate="1"):
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
        require(hold_rate in HOLD_RATES, "unknown engineering hold rate")
        schedule = continuous_schedule(
            *common, (hold_rate, WARMUP + HOLD), (RATES["C"], STEP), warmup=WARMUP
        )
        schedule["hold_rate_scope"] = "non-grid engineering hold"
    else:
        schedule = stock_schedule(*common, "1", WARMUP, MEASURED)
    schedule["worker_sleep_ms"] = Harness.worker_sleep_ms
    require(schedule["count"] <= POPULATION_LIMIT, "episode exceeds the registered observer volume")
    return schedule


def exercise(driver, name, *, stepped, hold_rate):
    exercise_drain(
        driver,
        lambda: episode_schedule(name, stepped=stepped, hold_rate=hold_rate),
        stepped=stepped,
    )
    if stepped and not read(driver.output / "step-transition.json")["verified_crossing_ids"]:
        raise CarryInUnobserved(f"no natural carry-in observed at engineering hold {hold_rate}/s")


def checked_expiry_export(driver, deadline, export, *, now=time.monotonic_ns):
    started = now()
    limit = started + EXPIRY_EXPORT_SECONDS * SECOND
    record = {"start_mono_ns": started, "deadline_mono_ns": limit, "status": "failed"}
    try:
        require(
            deadline <= started <= deadline + EXPIRY_HANDOFF_SECONDS * SECOND,
            f"checked expiry export missed its {EXPIRY_HANDOFF_SECONDS} s drain handoff",
        )
        reason = f"checked expiry export exceeded its {EXPIRY_EXPORT_SECONDS} s reserve"
        # Give the export its full budget after any clamped read has unwound.
        with driver.command_deadline(limit / SECOND):
            try:
                state = export()
            except ResourceEvidenceError:
                raise
            except (OSError, ValueError, RuntimeError) as error:
                if now() >= limit:
                    raise IntegrityError(reason) from error
                raise
            finished = now()
            if finished > limit:
                raise IntegrityError(reason)
        record.update(status="PASS", end_mono_ns=finished)
        return state
    except Exception as error:
        record["error"] = str(error)
        raise
    finally:
        if "end_mono_ns" not in record:
            record["end_mono_ns"] = now()
        write(driver.output / "expiry-export-budget.json", record)


def exercise_drain(driver, build_schedule, *, stepped=False, execute=None, visibility_delay=0):
    output = driver.output
    polls = Polls(output)
    work_deadline = driver.deadline
    with (
        prometheus(driver) as query,
        observer_resources(driver),
        watch(driver) as (pods, errors),
        ExitStack() as diagnostics,
        ExitStack() as polling,
    ):
        setup = driver.clock()
        write(output / "setup-clock.json", setup)
        uncertainty = utc_ns(setup["observer_after"]) - utc_ns(setup["observer_before"])
        require(
            utc_ns(setup["observer_before"])
            <= utc_ns(setup["database"])
            <= utc_ns(setup["observer_after"]),
            "setup clock probe did not bracket database time",
        )
        relay = container_target(driver, driver.pod(APP, "relay")["metadata"]["name"])
        schedule = {**build_schedule(), "state_read_evidence": STATE_READ_SCHEMA}
        anchor_wall, anchor_mono = time.time_ns(), time.monotonic_ns()
        stop_mono = anchor_mono + schedule["stop_ns"] - anchor_wall
        deadline = stop_mono + DRAIN_SECONDS * SECOND
        expiry_start_by = deadline + EXPIRY_HANDOFF_SECONDS * SECOND
        expiry_end_by = expiry_start_by + EXPIRY_EXPORT_SECONDS * SECOND
        write(
            output / "deadline-anchor.json",
            {
                "anchor_unix_ns": anchor_wall,
                "anchor_mono_ns": anchor_mono,
                "stop_mono_ns": stop_mono,
                "work_deadline_mono_ns": int(work_deadline * SECOND),
                "expiry_start_by_mono_ns": expiry_start_by,
                "expiry_end_by_mono_ns": expiry_end_by,
            },
        )
        require(
            expiry_end_by / SECOND <= work_deadline,
            "episode work budget cannot reserve the checked expiry export before traffic",
        )
        diagnostics.enter_context(
            relay_attribution(driver, schedule, stop_mono, work_deadline, relay)
        )
        # Keep the health thread on the work deadline so a cut-edge poll cannot
        # leave a stale drain-deadline error for the post-expiry checker.
        polling.enter_context(driver.command_deadline(deadline / SECOND))
        reads = StateReads(output, stop_mono)
        retain = reads.retain

        expired = None
        try:
            retain_sender(
                driver,
                schedule,
                stop_mono,
                polls,
                errors,
                reads,
                retain,
                stepped=stepped,
                execute=execute,
            )
            binding = file_record(output / "schedule.json")
            require(read(output / "schedule.json") == schedule, "schedule changed after launch")
            write(output / "schedule-binding.json", binding)
            raw = (output / "sender-journal.jsonl").read_bytes()
            arrivals, sender = expected_arrivals(raw, schedule, schedule_sha256=binding["sha256"])

            def observe(phase="drain"):
                check_monitor(errors)
                if visibility_delay and time.monotonic_ns() < stop_mono + visibility_delay * SECOND:
                    raise TimeoutError("engineering fixture delays the qualifying quiet read")
                return observation(driver, polls, reads, phase)

            def check(state):
                driver.witness(state["witness"], upload=False)
                report, _, sampling = capture_checker(driver, pods, errors, terminal=True)
                checked_results(
                    report,
                    arrivals,
                    sender,
                    read(output / "witness.json"),
                    polls.first,
                    uncertainty,
                    state["upper_unix_ns"],
                )
                rows = report["trace"]["Snapshot"]["engine.workflow_executions"]["rows"]
                require(
                    {row["workflow_id"]: row["revision"] for row in rows}
                    == {row["id"]: row["revision"] for row in state["db"]["submissions"]},
                    "checker snapshot revisions differ from bracketed DB state",
                )
                require_checker_resources(sampling)

            def export_expiry():
                # The locked checker selects and checks the same repeatable-read cut.
                # This export uses closeout time and cannot turn expiry into a pass.
                report, bracket, sampling = capture_checker(driver, pods, errors, terminal=False)
                inventories = sorted(driver.witness_inventories(), key=lambda row: row["pod_uid"])
                state = checked_cut(report, bracket, inventories)
                write(output / "final.json", state)
                polls.retain(state["db"]["succeeded"], bracket)
                driver.witness(inventories, upload=False, pinned_prefix=True)
                probes = {"setup": setup, "measured": driver.clock()}
                write(output / "clock.json", probes)
                width = clock_bounds(probes, schedule["origin_ns"], state["upper_unix_ns"])
                checked_results(
                    report,
                    arrivals,
                    sender,
                    read(output / "witness.json"),
                    polls.first,
                    width,
                    state["upper_unix_ns"],
                    expiry=state,
                )
                require_checker_resources(sampling)
                return state

            def check_expiry():
                return checked_expiry_export(driver, deadline, export_expiry)

            result = quiet_drain(
                stop_ns=stop_mono,
                observe=observe,
                observe_after=lambda: observe("post-check"),
                initial=reads.initial,
                check=check,
                check_expiry=check_expiry,
                expected=arrivals,
                sender=sender,
                retain=retain,
            )
            # Final evidence must also close within the registered deadline.
            final = read(output / "final.json")
            probes = None
            try:
                probes = {"setup": setup, "measured": driver.clock()}
            except ResourceEvidenceError:
                raise
            except (OSError, ValueError, RuntimeError):
                if time.monotonic_ns() < deadline:
                    raise
            if probes is not None:
                try:
                    clock_bounds(probes, schedule["origin_ns"], final["upper_unix_ns"])
                except ValueError as error:
                    raise IntegrityError(str(error)) from error
            if probes is None or time.monotonic_ns() > deadline:
                expire_drain(
                    stop_mono,
                    deadline,
                    (final, readiness(final, arrivals, sender)),
                    check_expiry,
                    arrivals,
                    sender,
                    retain,
                    now=time.monotonic_ns,
                )
            write(output / "clock.json", probes)
        except DrainExpired as error:
            expired = error
            result = error.result
            final = read(output / "final.json")
        finally:
            polling.close()
        result["state_read_binding"] = state_read_binding(output)
        write(output / "drain-result.json", result)
        diagnostics.close()
        durable = [row["id"] for row in final["db"]["submissions"]]
        report = sender_report(raw, schedule, durable, schedule_sha256=binding["sha256"])
        write(output / "sender-report.json", report)
        try:
            joined, clock_width = replay_episode(output)
        except (KeyError, OSError, ValueError) as error:
            raise IntegrityError(f"checked evidence failed replay: {error}") from error
        if stepped:
            carry = read(output / "step-transition.json")
            carry["clock_bracket_width_ns"] = clock_width
            carry["verified_crossing_ids"] = verified_crossings(
                carry, joined, schedule["hold_end_ns"], clock_width, pending=result["pending"]
            )
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
        check_monitor(errors)
        try:
            args = (
                driver,
                query,
                schedule["origin_ns"] / SECOND,
                final["upper_unix_ns"] / SECOND,
                len(durable),
            )
            if schedule.get("campaign_clock"):
                from j2_measure_telemetry import cell_samples

                series = retain_scrapes(*args, require_quiet=False, allow_gaps=True)
                write(output / "cell-scrape-evidence.json", cell_samples(series, schedule))
            else:
                retain_scrapes(*args, require_quiet=expired is None)
        except (OSError, ValueError, KeyError, TypeError) as error:
            raise ResourceEvidenceError(f"incomplete scrape evidence: {error}") from error
    if fingerprint(final) != result["final_fingerprint"]:
        raise IntegrityError("final read identity drift")
    if expired is not None:
        raise expired


def rehearse(driver, layouts, exporter_layout, binaries, hold_rate):
    with driver.stage("bootstrap"):
        addresses, exporter = start_profile(driver, layouts, exporter_layout)
    previous = None
    grid_expiry = None
    for name, stepped in (("grid-drain", False), ("step-drain", True)):
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
        try:
            with driver.stage(name):
                exercise(driver, name, stepped=stepped, hold_rate=hold_rate)
        except DrainExpired as error:
            if stepped:
                raise
            grid_expiry = error
    if grid_expiry is not None:
        raise grid_expiry


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
    parser.add_argument("--hold-rate", choices=HOLD_RATES, default="1")
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
        scenario=lambda d, images, exporter: rehearse(
            d, images, exporter, binaries, args.hold_rate
        ),
        scope=f"unmeasured J2 schedule 2b2 ({ENGINEERING})",
    )


if __name__ == "__main__":
    raise SystemExit(main())
