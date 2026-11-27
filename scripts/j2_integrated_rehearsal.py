"""Drive the attempt-scoped J2 engineering rehearsal on one owned runner."""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

import dur053_k3 as kind
from j2_barrier import BARRIER_SECONDS, JOBS, RetryLater, barrier_deadline
from j2_barrier_rehearsal import client, context, emit, receipt
from j2_drain import DrainExpired
from j2_harness import APP, NS, OTHER, hosted_dispatch, run
from j2_harness_evidence import seal
from j2_integrated_diagnostics import attribution_samples
from j2_integrated_evidence import summarize_episode
from j2_offline import SECOND
from j2_qualification_evidence import DRAIN_SECONDS, file_record, read, require, write
from j2_schedule import ORDERS, RATES, continuous_schedule, registered_schedule, stock_schedule
from j2_schedule_rehearsal import JOURNAL, LEAD_SECONDS, STEP_JOURNAL, install_sender
from j2_step_rehearsal import DrainHarness, exercise_drain
from j2_verify_overlay import check_network, start_profile

SCOPE = "unmeasured J2 integrated rehearsal"
MATRIX_SECONDS = 180 * 60
REPLAY_SECONDS = 90 * 60
BOOTSTRAP_SECONDS = 25 * 60
CELL_RESERVE = 4 * 60
SOAK_SETUP = 3 * 60
FINAL_RESERVE = 13 * 60
WARMUP = 15
GRID_MEASURED = 60
HOLD_MEASURED = 115
STEP = 60
CELL_SECONDS = LEAD_SECONDS + WARMUP + GRID_MEASURED + DRAIN_SECONDS + CELL_RESERVE
EPISODE_SECONDS = LEAD_SECONDS + WARMUP + HOLD_MEASURED + STEP + DRAIN_SECONDS
EXPIRY_FIXTURE = (1, "B")
ROUNDS = ("integrity", "no-eligible", "eligible")


def forecast(job, *, matrix_limit=MATRIX_SECONDS, replay_limit=REPLAY_SECONDS):
    order = ORDERS[job - 1]
    matrix = {
        "bootstrap": BOOTSTRAP_SECONDS,
        "grid_sender": len(order) * (LEAD_SECONDS + WARMUP + GRID_MEASURED),
        "grid_drain": len(order) * DRAIN_SECONDS,
        "grid_reset_export_upload": len(order) * CELL_RESERVE,
        "barrier": BARRIER_SECONDS,
        "continuous_setup": SOAK_SETUP,
        "continuous_sender": EPISODE_SECONDS - DRAIN_SECONDS,
        "continuous_drain": DRAIN_SECONDS,
        "episode_teardown_upload": FINAL_RESERVE,
    }
    replay = {
        "bootstrap": 600,
        "barrier_replay": 300,
        "matrix_evidence": 18 * 150 + 3 * 30,
        "volume_fixture": 1200,
        "qualification_archive": 300,
        "volume_teardown": 60,
        "report_upload": 120,
    }
    totals = sum(matrix.values()), sum(replay.values())
    require(totals[0] <= matrix_limit, f"matrix forecast {totals[0]} s exceeds {matrix_limit} s")
    require(totals[1] <= replay_limit, f"replay forecast {totals[1]} s exceeds {replay_limit} s")
    return {
        "scope": SCOPE,
        "job": job,
        "order": order,
        "matrix": matrix,
        "replay": replay,
        "matrix_seconds": totals[0],
        "matrix_cap_seconds": matrix_limit,
        "replay_seconds": totals[1],
        "replay_cap_seconds": replay_limit,
    }


def label_name(job, label):
    run_id, attempt = (os.environ[key] for key in ("GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT"))
    return f"j2-rehearsal-{run_id}-{attempt}-job-{job}-{label}"


def episode_schedule(job, cell, rate, origin, *, campaign=None):
    require(rate in RATES and (cell == "soak" or cell == rate), "invalid episode selection")
    if campaign is not None:
        return registered_schedule(
            campaign, f"job-{job}", cell, f"j2-{campaign}-job-{job}-{cell}", APP, origin, rate
        )
    campaign = f"eng-integrated-{os.environ['GITHUB_RUN_ID']}-{os.environ['GITHUB_RUN_ATTEMPT']}"
    run_id = f"j2-{campaign}-job-{job}-{cell}"
    common = (campaign, f"job-{job}", cell, run_id, APP, origin)
    if cell == "soak":
        schedule = continuous_schedule(
            *common, (RATES[rate], WARMUP + HOLD_MEASURED), (RATES["C"], STEP), warmup=WARMUP
        )
        require(schedule["count"] <= 26678, "engineering episode exceeds qualified volume")
    else:
        schedule = stock_schedule(*common, RATES[rate], WARMUP, GRID_MEASURED)
    schedule.update(
        scope=SCOPE, measured_eligible=False, worker_sleep_ms=DrainHarness.worker_sleep_ms
    )
    return schedule


def wait_file(path, deadline, *, stopped=None):
    while not path.is_file():
        require(stopped is None or not stopped.is_file(), f"driver stopped waiting for {path.name}")
        remaining = deadline - time.monotonic()
        require(remaining > 0, f"deadline while waiting for {path.name}")
        time.sleep(min(remaining, 0.5))
    return read(path)


def publish(path, value):
    incoming = path.with_suffix(".incoming")
    write(incoming, value)
    incoming.replace(path)


def start_time(root, job):
    campaign = context("eligible")
    barrier = client(campaign, root, time.monotonic() + 60)

    def find():
        row = barrier.api.jobs(campaign, allow_failed=True).get(job)
        if row is None or row["status"] != "in_progress":
            raise RetryLater("waiting for own Actions job")
        return row

    row = barrier.retry(find)
    elapsed = max(0, time.time() - datetime.fromisoformat(row["started_at"]).timestamp())
    return time.monotonic() - elapsed, row["id"]


def begin(root, job):
    hosted_dispatch()
    ledger = forecast(job)
    root.mkdir(parents=True, exist_ok=True)
    require(shutil.disk_usage(root).free >= 5 << 30, "matrix disk headroom below forecast")
    (root / "commands").mkdir()
    (root / "driver").mkdir()
    (root / "final").mkdir()
    (root / "receipts").mkdir()
    job_start, job_id = start_time(root, job)
    write(root / "final" / "forecast.json", ledger)
    write(
        root / "job.json",
        {
            "started": job_start,
            "deadline": job_start + MATRIX_SECONDS,
            "work_end": job_start + ledger["matrix_seconds"] - FINAL_RESERVE,
            "job_id": job_id,
            "job": job,
            "head": os.environ["GITHUB_SHA"],
        },
    )
    require(time.monotonic() < job_start + BOOTSTRAP_SECONDS, "bootstrap budget exhausted")
    emit(root=str(root), final_name=label_name(job, "final"))


def checkpoint(driver, job, label, identity, rate):
    root = driver.output
    result = read(root / "drain-result.json")
    require(
        result["status"] == "PASS"
        or (
            result.get("classification") == "drain_failure" and result.get("check_status") == "PASS"
        ),
        "incomplete checked drain returned to the rehearsal",
    )
    manifest = {
        "scope": SCOPE,
        "job": job,
        "cell": label,
        "offered_rate": rate,
        "identity": identity,
        "drain": result.get("classification", "pass"),
        "measured_eligible": False,
        "quiet_visibility_fixture": (job, label) == EXPIRY_FIXTURE,
    }
    write(root / "checkpoint.json", manifest)
    summary = summarize_episode(root)
    if summary.get("registered_clock"):
        manifest["population"] = summary["population"]
        write(root / "checkpoint.json", manifest)
    write(root / "episode-replay.json", summary)
    seal(root)
    return file_record(root / "freeze.json")["sha256"]


def exercise(driver, job, cell, rate, addresses, exporter, previous, binaries):
    with driver.stage(f"fresh-{cell}"):
        current = driver.fresh_episode(cell, addresses, exporter, previous)
    with driver.stage(f"install-{cell}"):
        if previous is None:
            check_network(driver, APP)
            for namespace in (NS, OTHER):
                driver.k("-n", namespace, "delete", "pod", "unrelated", "--wait=true")
        stepped = cell == "soak"
        driver.observer = driver.pod(APP, "observer")["metadata"]["name"]
        driver.workers = driver.kube("pods", "-n", APP, "-l", "app=worker")["items"]
        driver.pod_journal = STEP_JOURNAL if stepped else JOURNAL
        driver.journal_name = "sender-journal.jsonl"
        driver.install_checker()
        binary, manifest = binaries[stepped]
        install_sender(driver, binary, manifest, continuous=stepped)
    force_expiry = (job, cell) == EXPIRY_FIXTURE
    try:
        with driver.stage(cell), attribution_samples(driver):
            exercise_drain(
                driver,
                lambda: episode_schedule(job, cell, rate, time.time_ns() + LEAD_SECONDS * SECOND),
                stepped=stepped,
                visibility_delay=DRAIN_SECONDS - 15 if force_expiry else 0,
            )
    except DrainExpired as error:
        require(error.result.get("check_status") == "PASS", "unverified drain expiry")
    else:
        require(not force_expiry, "engineering quiet fixture failed to expire")
    return current, checkpoint(driver, job, cell, current, rate)


def serve(root, layouts, exporter_layout, stock, continuous, checker):
    hosted_dispatch()
    state = read(root / "job.json")
    scratch = root / "scratch"
    scratch.mkdir()
    kind.CLUSTER, kind.LABEL = "j2-integrated", "j2.harness=owned"
    driver = DrainHarness(root / "driver", scratch, checker)
    # The existing driver reserves 900 s against its relative live ceiling.
    # The attempt-anchored stage deadlines below govern this rehearsal.
    driver.live_reservation = MATRIX_SECONDS + 900
    driver.deadline = state["started"] + BOOTSTRAP_SECONDS
    binaries = {
        False: (stock, root / "final" / "stock-build.json"),
        True: (continuous, root / "final" / "continuous-build.json"),
    }

    def scenario(driver, images, exporter_images):
        with driver.stage("bootstrap"):
            addresses, exporter = start_profile(driver, images, exporter_images)
        publish(root / "ready.json", {"status": "ready"})
        job = state["job"]
        previous = None
        order = ORDERS[job - 1]
        try:
            for i, cell in enumerate((*order, "soak"), 1):
                request = wait_file(root / "commands" / f"{cell}.json", state["work_end"])
                if request["action"] == "finish":
                    return
                rate = request["rate"]
                require(cell == "soak" or rate == cell, "out-of-order cell")
                deadline = (
                    state["started"] + BOOTSTRAP_SECONDS + CELL_SECONDS * i
                    if cell != "soak"
                    else state["work_end"]
                )
                driver.deadline = min(deadline, time.monotonic() + request["budget"])
                previous, index = exercise(
                    driver, job, cell, rate, addresses, exporter, previous, binaries
                )
                publish(root / "commands" / f"{cell}-done.json", {"index_sha256": index})
        finally:
            if (driver.output / "freeze.json").is_file():
                driver.output = root / "final" / "teardown"
                driver.output.mkdir()

    status = run(driver, layouts, exporter_layout, scenario=scenario, scope=SCOPE)
    for name in ("cleanup", "runner", "stages"):
        suffix = ".jsonl" if name == "stages" else ".json"
        path = driver.evidence / (name + suffix)
        if path.is_file():
            (root / "final" / path.name).write_bytes(path.read_bytes())
    publish(root / "final" / "status.json", read(driver.evidence / "status.json"))
    return status


def request_cell(root, job, slot):
    state = read(root / "job.json")
    cell = ORDERS[job - 1][slot - 1]
    require(job == state["job"], "matrix job drift")
    return request_episode(root, job, cell, cell, CELL_SECONDS)


def request_episode(root, job, cell, rate, budget):
    emit(checkpoint=label_name(job, cell), cell=cell)
    command = root / "commands" / f"{cell}.json"
    require(not command.exists(), "episode requested twice")
    publish(command, {"action": "run", "rate": rate, "budget": budget})
    end = time.monotonic() + budget
    wait_file(root / "commands" / f"{cell}-done.json", end, stopped=root / "driver" / "status.json")
    emit(sealed="true")


def bind_upload(root, job, label):
    saved = {
        **receipt("CHECKPOINT"),
        "name": label_name(job, label),
        **read(root / "commands" / f"{label}-done.json"),
    }
    write(root / "receipts" / f"{label}.json", saved)
    write(root / "final" / f"{label}-upload-time.json", {"uploaded_mono_ns": time.monotonic_ns()})


def prepare_barrier(root, job):
    state = read(root / "job.json")
    for cell in ORDERS[job - 1]:
        require((root / "receipts" / f"{cell}.json").is_file(), f"missing checkpoint {cell}")
    ready = time.monotonic()
    end = barrier_deadline(ready, state["work_end"] - EPISODE_SECONDS - SOAK_SETUP)
    write(root / "barrier.json", {"ready": ready, "deadline": end})


def start_soak(root, job):
    barrier_root = Path(os.environ["RUNNER_TEMP"]) / "j2-barrier"
    expected = {
        "integrity": "integrity_stop",
        "no-eligible": "no_eligible_rate",
        "eligible": "would_start",
    }
    outcomes = {
        round_name: read(barrier_root / round_name / "outcome" / "outcome.json")
        for round_name in ROUNDS
    }
    for name, result in outcomes.items():
        require(result["outcome"] == expected[name], f"unexpected barrier outcome for {name}")
    decision = outcomes["eligible"]["decision"]
    require(decision["selected_rate"] == "AB", "unexpected engineering soak selection")
    write(root / "final" / "barrier-outcomes.json", outcomes)
    return request_episode(
        root, job, "soak", decision["selected_rate"], EPISODE_SECONDS + SOAK_SETUP
    )


def finish(root):
    state = read(root / "job.json")
    status = root / "final" / "status.json"
    if not (root / "driver.pid").exists():
        write(status, {"status": "FAIL", "reason": "runner profile never started"})
        raise ValueError("runner profile never started")
    for label in (*ORDERS[state["job"] - 1], "soak"):
        path = root / "commands" / f"{label}.json"
        if not path.exists():
            publish(path, {"action": "finish"})
    wait_file(status, min(state["deadline"] - 180, time.monotonic() + 540))
    labels = (*ORDERS[state["job"] - 1], "soak")
    receipts = {
        label: read(root / "receipts" / f"{label}.json")
        for label in labels
        if (root / "receipts" / f"{label}.json").is_file()
    }
    write(root / "final" / "checkpoints.json", receipts)
    write(
        root / "final" / "coverage.json",
        {
            label: (
                "checkpointed"
                if label in receipts
                else "evidence_loss"
                if (root / "commands" / f"{label}-done.json").is_file()
                else "partial"
                if (root / "driver" / label).exists()
                else "unperformed"
            )
            for label in labels
        },
    )
    seal(root / "final")
    emit(sealed="true")
    require(read(status)["cleanup_verified"], "rehearsal cleanup was not verified")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for action in ("begin", "serve", "ready", "cell", "receipt", "barrier", "soak", "finish"):
        command = commands.add_parser(action)
        command.add_argument("root", type=Path)
        if action in {"begin", "cell", "receipt", "barrier", "soak"}:
            command.add_argument("job", type=int, choices=JOBS)
        if action == "serve":
            for arg in ("layouts", "exporter_layout", "stock", "continuous", "checker"):
                command.add_argument(arg, type=Path)
        if action == "cell":
            command.add_argument("slot", type=int, choices=range(1, 6))
        if action == "receipt":
            command.add_argument("label", choices=(*RATES, "soak"))
    args = parser.parse_args()
    root = args.root
    if args.command == "begin":
        begin(root, args.job)
    elif args.command == "serve":
        return serve(
            root, args.layouts, args.exporter_layout, args.stock, args.continuous, args.checker
        )
    elif args.command == "ready":
        wait_file(
            root / "ready.json",
            read(root / "job.json")["started"] + BOOTSTRAP_SECONDS,
            stopped=root / "driver" / "status.json",
        )
    elif args.command == "cell":
        request_cell(root, args.job, args.slot)
    elif args.command == "receipt":
        bind_upload(root, args.job, args.label)
    elif args.command == "barrier":
        prepare_barrier(root, args.job)
    elif args.command == "soak":
        start_soak(root, args.job)
    else:
        finish(root)
    return 0


if __name__ == "__main__":
    sys.exit(main())
