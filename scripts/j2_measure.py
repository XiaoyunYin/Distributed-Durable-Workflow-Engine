"""Run the reviewed J2 grid and select a continuous episode from peer evidence."""

import argparse
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import dur053_k3 as kind
from j2_barrier import (
    ENGINEERING_GRID_SCOPE,
    NO_RATE,
    PIN_FILES,
    REGISTERED_GRID_SCOPE,
    BarrierStop,
    Campaign,
    RetryLater,
    barrier_deadline,
    canonical,
    digest,
    validate_grid,
)
from j2_barrier_rehearsal import client, emit, receipt, write_bundle
from j2_checker import ROOT
from j2_checker_report import IntegrityError, ResourceEvidenceError
from j2_dispatch_manifest import FINAL_REHEARSAL, confirm_prerequisites, verify_package
from j2_drain import DrainExpired
from j2_harness import APP, DB, hosted_dispatch, run
from j2_harness_evidence import seal
from j2_integrated_diagnostics import attribution_samples
from j2_integrated_evidence import summarize_episode
from j2_integrated_rehearsal import install_episode, publish, wait_file
from j2_measure_authorization import (
    authorization,
    authorization_branch,
    cell_traffic,
    engineering,
    r422_enabled,
    recheck,
    traffic_disposition,
)
from j2_measure_telemetry import coverage_gaps, fresh_telemetry, runner_inventory
from j2_offline import SECOND
from j2_qualification_evidence import DRAIN_SECONDS, file_record, read, require, write
from j2_schedule import (
    DISPATCH_JOURNAL,
    ENGINEERING_GRID,
    ENGINEERING_HOLD,
    ENGINEERING_STEP,
    ENGINEERING_WARMUP,
    HOLD,
    MEASURED,
    MEASURED_CAMPAIGN,
    ORDERS,
    RATES,
    STEP,
    WARMUP,
    continuous_schedule,
    registered_schedule,
    stock_schedule,
)
from j2_schedule_rehearsal import PRECOMMIT_LEAD_SECONDS, run_precommitted
from j2_step_rehearsal import POPULATION_LIMIT, DrainHarness, exercise_drain
from j2_verify_overlay import start_profile


def context(campaign):
    scope = ENGINEERING_GRID_SCOPE if engineering(campaign) else REGISTERED_GRID_SCOPE
    pins = {name: file_record(ROOT / path)["sha256"] for name, path in PIN_FILES.items()}
    return Campaign(
        os.environ["GITHUB_REPOSITORY"],
        int(os.environ["GITHUB_RUN_ID"]),
        int(os.environ["GITHUB_RUN_ATTEMPT"]),
        os.environ["GITHUB_SHA"],
        campaign,
        pins,
        POPULATION_LIMIT,
        scope,
    )


def allowances(campaign):
    if engineering(campaign):
        arrival, continuous, extras, final, cap = (
            ENGINEERING_WARMUP + ENGINEERING_GRID,
            ENGINEERING_WARMUP + ENGINEERING_HOLD + ENGINEERING_STEP,
            240,
            780,
            180,
        )
    else:
        arrival, continuous, extras, final, cap = (
            WARMUP + MEASURED,
            WARMUP + HOLD + STEP,
            480,
            1500,
            350,
        )
    return {
        "bootstrap": 1500,
        "cell": arrival + DRAIN_SECONDS + extras,
        "episode": continuous + PRECOMMIT_LEAD_SECONDS + DRAIN_SECONDS + extras + 180,
        "closeout": final,
        "cap": cap * 60,
    }


def schedule_for(campaign, job, cell, rate, origin, *, precommit=False):
    common = (
        campaign.campaign,
        f"job-{job}",
        cell,
        f"j2-{campaign.campaign}-job-{job}-{cell}",
        APP,
        origin,
    )
    if campaign.scope == REGISTERED_GRID_SCOPE:
        schedule = registered_schedule(*common, rate)
    elif cell == "soak":
        schedule = continuous_schedule(
            *common,
            (RATES[rate], ENGINEERING_WARMUP + ENGINEERING_HOLD),
            (RATES["C"], ENGINEERING_STEP),
            warmup=ENGINEERING_WARMUP,
        )
    else:
        schedule = stock_schedule(*common, RATES[rate], ENGINEERING_WARMUP, ENGINEERING_GRID)
    schedule.update(
        campaign_clock=True, scope=campaign.scope, worker_sleep_ms=DrainHarness.worker_sleep_ms
    )
    if precommit:
        schedule["dispatch_journal"] = DISPATCH_JOURNAL
    return schedule


def begin(root, job, campaign_id):
    hosted_dispatch()
    root.mkdir(parents=True)
    for name in ("commands", "driver", "final", "receipts"):
        (root / name).mkdir()
    final = root / "final"
    try:
        require(job in (1, 2, 3), "invalid logical measure job")
        authorization_branch(campaign_id)
        require(os.environ["GITHUB_RUN_ATTEMPT"] == "1", "campaign dispatch must be attempt 1")
        campaign = context(campaign_id)
        require(
            subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
            == campaign.head,
            "checkout differs from dispatch head",
        )
        final_candidate = (
            f"eng-r422-final-{campaign.head[:8]}" if r422_enabled(ROOT) else FINAL_REHEARSAL
        )
        require(
            not campaign_id.startswith("eng-r422-final-") or campaign_id == final_candidate,
            "R422 final rehearsal ID must name the checkout head",
        )
        package = (
            verify_package(ROOT, campaign.pins)
            if campaign_id in {MEASURED_CAMPAIGN, final_candidate}
            else None
        )
        limits = allowances(campaign_id)
        api = client(campaign, final, time.monotonic() + 60)

        def own_job():
            row = api.api.jobs(campaign, allow_failed=True).get(job)
            if row is None or row["status"] != "in_progress":
                raise RetryLater("waiting for the actual Actions job")
            return row

        row = api.retry(own_job)
        elapsed = max(0, time.time() - datetime.fromisoformat(row["started_at"]).timestamp())
        started = time.monotonic() - elapsed
        binding = authorization(
            campaign_id,
            check_history=True,
            wait_seconds=max(0, min(360, limits["bootstrap"] - elapsed)),
        )
        if package is not None:
            gate = client(campaign, final, time.monotonic() + 40)
            write(
                final / "pre-dispatch-package.json",
                {
                    "binding": campaign.binding(job, row["id"]),
                    **package,
                    "live_prerequisites": confirm_prerequisites(
                        gate.api, package.get("supplemental_controls")
                    ),
                },
            )
        require(time.monotonic() < started + limits["bootstrap"], "bootstrap allowance expired")
        require(shutil.disk_usage(root).free >= 5 << 30, "matrix disk headroom below forecast")
        write(final / "authorization.json", binding)
        write(
            final / "runner-inventory.json",
            {"binding": campaign.binding(job, row["id"]), **runner_inventory()},
        )
        write(
            root / "job.json",
            {
                "campaign": campaign_id,
                "job": job,
                "job_id": row["id"],
                "started": started,
                "bootstrap_end": started + limits["bootstrap"],
                "hard_end": started + limits["cap"],
                "work_end": started + limits["cap"] - limits["closeout"],
                "limits": limits,
            },
        )
    except Exception as error:
        write(
            final / "authorization-stop.json",
            {
                "campaign": campaign_id,
                "run": os.environ.get("GITHUB_RUN_ID"),
                "job": job,
                "bootstrap": "unperformed",
                "reason": str(error),
                "disposition": "no_requests",
            },
        )
        raise
    emit(final_name=campaign.name("final", job))


def verify_recovery(seen, current):
    repeated = sorted(seen & current)
    if repeated:
        raise ResourceEvidenceError(
            f"required series breached again after verified reset: {repeated}"
        )


def cell_record(summary, scrape):
    cohorts = summary["cohorts"]
    gate = cohorts.get("grid", cohorts.get("hold"))["gate"]
    gaps = coverage_gaps(scrape)
    if gaps:
        outcome, reason = "invalid", "resource_evidence"
    elif not summary["sender_valid"]:
        outcome, reason = "invalid", "sender_invalid"
    elif gate["status"] == "invalid_gate_inputs":
        raise ResourceEvidenceError("gate inputs cannot be verified from the required telemetry")
    elif summary["drain"]["status"] != "PASS":
        outcome, reason = "drain_failure", "checked drain expired"
    elif gate["status"] != "pass":
        outcome, reason = "gate_failure", gate["status"]
    else:
        outcome, reason = "pass", None
    checked = sum(window["checked_success"] for window in cohorts.values())
    counts = {
        name: sum(window[name] for window in cohorts.values())
        for name in ("offered", "sent", "admitted")
    }
    return {
        "outcome": outcome,
        "reason": reason,
        "counts": {
            **counts,
            "terminal": checked,
            "succeeded": checked,
        },
        "checked": True,
        "population": summary["population"],
        "recovery": None,
    }, gaps


def serve(root, layouts, exporter_layout, sender, checker):
    hosted_dispatch()
    state = read(root / "job.json")
    campaign = context(state["campaign"])
    job, limits = state["job"], state["limits"]
    final = root / "final"
    binding = read(final / "authorization.json")
    controls = read(final / "controls-applicability.json")
    require(
        controls["head"] == campaign.head and controls["status"] == "PASS",
        "missing head-specific control applicability",
    )
    scratch = root / "scratch"
    scratch.mkdir()
    kind.CLUSTER, kind.LABEL = "j2-measure", "j2.harness=owned"
    driver = DrainHarness(root / "driver", scratch, checker)
    driver.env.pop("GITHUB_TOKEN", None)
    driver.live_reservation = limits["cap"] + 900
    driver.deadline = state["bootstrap_end"]
    binaries = {True: (sender, final / "dispatch-build.json")}
    labels = (*ORDERS[job - 1], "soak")

    def scenario(driver, images, exporter_images):
        with driver.stage("bootstrap"):
            recheck(binding, campaign.campaign)
            addresses, exporter = start_profile(driver, images, exporter_images)
        publish(root / "ready.json", {"status": "ready"})
        previous, prepared, seen = None, None, set()

        def fresh(label, predecessor):
            with driver.stage(f"fresh-{label}"):
                identity = driver.fresh_episode(label, addresses, exporter, predecessor)
                proof = fresh_telemetry(driver, exporter, DB)
            return identity, proof

        try:
            for i, cell in enumerate(labels):
                request = wait_file(root / "commands" / f"{cell}.json", state["work_end"])
                if request["action"] == "finish":
                    return
                rate = request["rate"]
                require(rate in RATES and (cell == "soak" or rate == cell), "invalid campaign cell")
                driver.deadline = min(state["work_end"], time.monotonic() + request["budget"])
                if prepared is None:
                    current, _ = fresh(cell, previous)
                else:
                    current, _ = prepared
                    prepared = None
                    driver.output = driver.evidence / cell
                output = driver.output
                with driver.stage(f"install-{cell}"):
                    install_episode(driver, cell, previous, binaries, precommit=True)

                def build_schedule(cell=cell, rate=rate):
                    recheck(binding, campaign.campaign)
                    return schedule_for(
                        campaign,
                        job,
                        cell,
                        rate,
                        time.time_ns() + PRECOMMIT_LEAD_SECONDS * SECOND,
                        precommit=True,
                    )

                def release(driver, schedule, *, partial):
                    output = driver.output
                    peer_stop = final / "peer-failure.json"
                    if schedule["cell"] == "soak" and peer_stop.is_file():
                        raise BarrierStop("peer_failure", "peer failed before the sender release")
                    write(
                        output / "sender-release.json",
                        {
                            "run_id": schedule["run_id"],
                            "authorized_ref": binding["revision"],
                            "controls": file_record(final / "controls-applicability.json"),
                            "inventory": file_record(final / "runner-inventory.json"),
                            "telemetry": file_record(output / "pre-traffic-telemetry.json"),
                            "unix_ns": time.time_ns(),
                        },
                    )
                    return run_precommitted(
                        driver,
                        schedule,
                        partial=partial,
                        peer_stop=peer_stop if schedule["cell"] == "soak" else None,
                    )

                with driver.stage(cell):
                    try:
                        with attribution_samples(driver, campaign=True):
                            exercise_drain(
                                driver, build_schedule, stepped=cell == "soak", execute=release
                            )
                    except DrainExpired as error:
                        if error.result.get("check_status") != "PASS":
                            raise IntegrityError("unverified drain expiry") from error
                    summary = summarize_episode(output)
                    row, gaps = cell_record(summary, read(output / "cell-scrape-evidence.json"))
                    verify_recovery(seen, gaps)
                    if row["outcome"] == "invalid" and cell != "soak":
                        next_cell = labels[i + 1]
                        prepared = fresh(next_cell, current)
                        next_identity, proof = prepared
                        row["recovery"] = {
                            "replaced_with": next_cell,
                            "telemetry_sha256": proof["sha256"],
                            "identity_sha256": file_record(driver.output / "identity.json")[
                                "sha256"
                            ],
                            "series": sorted(gaps),
                        }
                        write(
                            output / "recovery.json",
                            {
                                **row["recovery"],
                                "identity": next_identity,
                                "telemetry": read(driver.output / "pre-traffic-telemetry.json"),
                            },
                        )
                        seen.update(gaps)
                    row.update(rate=rate, offered_rate=RATES[rate], reset_verified=True)
                    write(
                        output / "checkpoint.json",
                        {
                            "scope": campaign.scope,
                            "job": job,
                            "cell": cell,
                            "offered_rate": rate,
                            "identity": current,
                            "population": summary["population"],
                            "row": row,
                        },
                    )
                    write(output / "episode-replay.json", summary)
                    write(output / "traffic.json", cell_traffic(output))
                    seal(output)
                    publish(
                        root / "commands" / f"{cell}-done.json",
                        {
                            "index_sha256": file_record(output / "freeze.json")["sha256"],
                        },
                    )
                    previous = current
        finally:
            if (driver.output / "freeze.json").is_file():
                driver.output = final / "teardown"
                driver.output.mkdir()

    code = run(driver, layouts, exporter_layout, scenario=scenario, scope=campaign.scope)
    for name in ("cleanup.json", "stages.jsonl", "runner.json"):
        evidence = driver.evidence / name
        if evidence.is_file():
            shutil.copyfile(evidence, final / name)
    publish(final / "status.json", read(driver.evidence / "status.json"))
    return code


def request_cell(root, state, campaign, cell, rate, budget):
    command = root / "commands" / f"{cell}.json"
    require(not command.exists(), "campaign episode already requested")
    emit(checkpoint=campaign.checkpoint(state["job"], cell), cell=cell)
    end = min(state["work_end"], time.monotonic() + budget)
    publish(command, {"action": "run", "rate": rate, "budget": budget})
    if cell == "soak":
        wait_for_soak(root, campaign, end)
    else:
        wait_file(
            root / "commands" / f"{cell}-done.json", end, stopped=root / "driver" / "status.json"
        )
    emit(sealed="true")


def wait_for_soak(root, campaign, end, *, now=time.monotonic, sleep=time.sleep):
    done = root / "commands" / "soak-done.json"
    stopped = root / "driver" / "status.json"
    released = root / "driver" / "soak" / "sender-release.json"
    report = root / "final" / "peer-failure.json"
    gate = client(campaign, root / "final", end)
    next_poll = now()
    while not done.is_file():
        remaining = end - now()
        require(
            not stopped.is_file() and remaining > 0,
            "continuous driver stopped or reached its deadline",
        )
        if not report.is_file() and now() >= next_poll:
            try:
                gate.api.jobs(campaign)
            except BarrierStop as error:
                # Atomically stop further sends while keeping the journal and checked evidence.
                publish(
                    report,
                    {
                        "reason": error.reason,
                        "detail": str(error),
                        "sender_released": released.is_file(),
                        "observed_unix_ns": time.time_ns(),
                    },
                )
            except RetryLater:
                pass
            next_poll = now() + 15
        sleep(min(0.5, remaining))


def bind_upload(root, state, campaign, cell):
    saved = {
        **receipt("CHECKPOINT"),
        "name": campaign.checkpoint(state["job"], cell),
        **read(root / "commands" / f"{cell}-done.json"),
    }
    write(root / "receipts" / f"{cell}.json", saved)


def prepare_grid(root, state, campaign):
    require(
        not (root / "driver" / "status.json").is_file(), "stopped driver has an incomplete grid"
    )
    job = state["job"]
    index = {"binding": campaign.binding(job, state["job_id"]), "cells": {}, "checkpoints": {}}
    for cell in ORDERS[job - 1]:
        path = root / "driver" / cell
        checkpoint = read(path / "checkpoint.json")
        saved = read(root / "receipts" / f"{cell}.json")
        require(
            saved["index_sha256"] == file_record(path / "freeze.json")["sha256"]
            and checkpoint["cell"] == cell
            and checkpoint["job"] == job,
            "local checkpoint binding differs",
        )
        index["cells"][cell] = checkpoint["row"]
        index["checkpoints"][cell] = saved
    write_bundle(root / "grid-evidence", "evidence", index)
    emit(
        evidence_name=campaign.name("grid-evidence", job),
        grid_name=campaign.name("grid", job),
        decision_name=campaign.name("decision", job),
    )


def publish_grid(root, state, campaign):
    index = read(root / "grid-evidence" / "evidence.json")
    job = state["job"]
    grid = {
        "binding": index["binding"],
        "pins": campaign.pins,
        "qualified_volume": campaign.volume,
        "evidence": {
            **receipt("GRID_EVIDENCE"),
            "name": campaign.name("grid-evidence", job),
            "index_sha256": digest(canonical(index)),
        },
        "cells": [
            dict(row, evidence_sha256=digest(canonical(row)))
            for row in (index["cells"][cell] for cell in ORDERS[job - 1])
        ],
        "stop": None,
    }
    validate_grid(grid, campaign, job, state["job_id"])
    write_bundle(root / "grid", "grid", grid)


def barrier(root, state, campaign):
    final = root / "final"
    ready = time.monotonic()
    upload = receipt("GRID")
    try:
        cutoff = barrier_deadline(
            ready, state["work_end"] - state["limits"]["episode"], allow_early=True
        )
        write(final / "barrier.json", {"published_mono": ready, "deadline": cutoff})
        gate = client(campaign, final, cutoff)
        name = campaign.name("grid", state["job"])

        def local_publication():
            metadata = gate.api.artifacts(campaign, {name})
            if name not in metadata:
                raise RetryLater("waiting for local grid publication")
            require(
                metadata[name]["id"] == upload["id"]
                and metadata[name]["digest"] == "sha256:" + upload["sha256"],
                "local grid publication receipt differs",
            )

        gate.retry(local_publication)
        write(final / "grid-publication.json", {**upload, "published_mono": ready})
        decision, jobs = gate.retry(gate.grids)
        job = state["job"]
        require(jobs[job]["id"] == state["job_id"], "measured barrier job drift")
        write_bundle(
            root / "decision",
            "decision",
            {"binding": campaign.binding(job, state["job_id"]), "decision": decision},
        )
        emit(publish_decision="true")
    except BarrierStop as error:
        write(
            final / "soak-decision.json",
            {"outcome": "unperformed", "reason": error.reason, "detail": str(error)},
        )
        if error.reason != "insufficient_budget":
            raise


def start_soak(root, state, campaign):
    final = root / "final"
    decision = read(root / "decision" / "decision.json")["decision"]
    gate = client(campaign, final, read(final / "barrier.json")["deadline"])
    job = state["job"]
    own_receipt = receipt("DECISION")
    try:

        def confirmed():
            peers = gate.api.jobs(campaign)
            require(
                job in peers and peers[job]["id"] == state["job_id"], "soak peer binding differs"
            )
            gate.decisions(decision, peers, only=(job,), receipt=own_receipt)

        gate.retry(confirmed)
    except BarrierStop as error:
        write(
            final / "soak-decision.json",
            {"outcome": "unperformed", "reason": error.reason, "detail": str(error)},
        )
        raise
    selected = decision["selected_rate"]
    write(
        final / "soak-decision.json",
        {
            "outcome": "unperformed" if selected == NO_RATE else "selected",
            "decision": decision,
            "receipt": own_receipt,
        },
    )
    if selected != NO_RATE:
        require(
            time.monotonic() + state["limits"]["episode"] <= state["work_end"],
            "continuous episode would exceed reserved budget",
        )
        request_cell(root, state, campaign, "soak", selected, state["limits"]["episode"])


def finish(root, state, campaign):
    final = root / "final"
    labels = (*ORDERS[state["job"] - 1], "soak")
    started = (root / "driver.pid").exists()
    for label in labels:
        path = root / "commands" / f"{label}.json"
        if not path.exists():
            publish(path, {"action": "finish"})
    status_path = final / "status.json"
    if started:
        wait_file(status_path, min(state["hard_end"] - 180, time.monotonic() + 540))
    else:
        write(status_path, {"status": "FAIL", "reason": "runner profile never started"})
    status = read(status_path)
    traffic = {label: cell_traffic(root / "driver" / label) for label in labels}
    disposition = traffic_disposition(traffic.values())
    write(
        final / "campaign-dispatch.json",
        {
            "binding": campaign.binding(state["job"], state["job_id"]),
            "authorization": read(final / "authorization.json"),
            "disposition": disposition,
            "cells": traffic,
            "reason": status.get("stop", status.get("reason")),
        },
    )
    receipts = {
        label: read(root / "receipts" / f"{label}.json")
        for label in labels
        if (root / "receipts" / f"{label}.json").is_file()
    }
    write(final / "checkpoints.json", receipts)
    write(
        final / "grid-status.json",
        {
            "complete": (final / "grid-publication.json").is_file() and not status.get("stop"),
            "grid_cells_checkpointed": [label for label in labels[:-1] if label in receipts],
            "stop": status.get("stop"),
        },
    )
    seal(final)
    emit(sealed="true")
    require(status["status"] == "PASS" and status["cleanup_verified"], "measurement driver stopped")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=(
            "begin",
            "controls",
            "serve",
            "ready",
            "cell",
            "receipt",
            "prepare-grid",
            "publish-grid",
            "barrier",
            "soak",
            "finish",
            "report",
        ),
    )
    parser.add_argument("root", type=Path)
    parser.add_argument("arguments", nargs="*")
    args = parser.parse_args()
    root, arguments = args.root, args.arguments
    if args.action == "begin":
        begin(root, int(arguments[0]), arguments[1])
        return 0
    if args.action == "report":
        from j2_integrated_report import measurement_receipts

        authorization_branch(arguments[0])
        measurement_receipts(root, context(arguments[0]))
        return 0
    state = read(root / "job.json")
    campaign = context(state["campaign"])
    if args.action == "serve":
        return serve(root, *map(Path, arguments))
    elif args.action == "ready":
        wait_file(
            root / "ready.json", state["bootstrap_end"], stopped=root / "driver" / "status.json"
        )
    elif args.action == "controls":
        from j2_integrated_report import control_applicability

        control_applicability(root / "final", campaign)
    elif args.action == "cell":
        require(int(arguments[0]) == state["job"], "matrix job drift")
        cell = ORDERS[state["job"] - 1][int(arguments[1]) - 1]
        request_cell(root, state, campaign, cell, cell, state["limits"]["cell"])
    elif args.action == "receipt":
        require(int(arguments[0]) == state["job"], "matrix job drift")
        bind_upload(root, state, campaign, arguments[1])
    else:
        {
            "prepare-grid": prepare_grid,
            "publish-grid": publish_grid,
            "barrier": barrier,
            "soak": start_soak,
            "finish": finish,
        }[args.action](root, state, campaign)
    return 0


if __name__ == "__main__":
    sys.exit(main())
