"""Run and report the synthetic J2 barrier fixtures on GitHub-hosted runners."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from datetime import datetime
from pathlib import Path

from j2_barrier import (
    BARRIER_SECONDS,
    COUNTS,
    INTEGRATED_SCOPE,
    JOB_MINUTES,
    JOBS,
    NO_RATE,
    PIN_FILES,
    ActionsAPI,
    Barrier,
    BarrierStop,
    Budget,
    Campaign,
    RetryLater,
    barrier_deadline,
    canonical,
    decode,
    digest,
    population,
    require,
    schema,
    sha256,
    validate_grid,
)
from j2_schedule import MEASURED, ORDERS, RATES, WARMUP

ROOT = Path(__file__).resolve().parents[1]
ROUNDS = {
    "barrier-success": ("eligible", "no-eligible", "integrity", "corrupted", "mixed"),
    "barrier-peer-failure": ("peer-failure",),
    "barrier-missing-peer": ("missing-peer",),
}
EXPECTED = {
    "eligible": "would_start",
    "no-eligible": "no_eligible_rate",
    "integrity": "integrity_stop",
    "corrupted": "unverifiable_evidence",
    "mixed": "unverifiable_evidence",
    "peer-failure": "peer_failure",
    "missing-peer": "barrier_timeout",
}


def context(round_name):
    require(
        os.environ.get("GITHUB_ACTIONS") == "true"
        and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted"
        and os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch",
        "barrier fixtures require a GitHub-hosted dispatch",
    )
    head = os.environ["GITHUB_SHA"]
    actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    require(head == actual, "barrier checkout differs from dispatch head")
    integrated = "J2_INTEGRATED_ROOT" in os.environ
    return Campaign(
        repository=os.environ["GITHUB_REPOSITORY"],
        run=int(os.environ["GITHUB_RUN_ID"]),
        attempt=int(os.environ["GITHUB_RUN_ATTEMPT"]),
        head=head,
        campaign=f"eng-integrated-{round_name}" if integrated else f"eng-{round_name}",
        pins={name: digest((ROOT / path).read_bytes()) for name, path in PIN_FILES.items()},
        volume=26678,
        scope=INTEGRATED_SCOPE if integrated else "synthetic engineering grid",
    )


def emit(**values):
    with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as stream:
        for key, value in values.items():
            stream.write(f"{key}={value}\n")


def read(path):
    return decode(path.read_bytes())


def write(path, value):
    path.write_bytes(canonical(value))


def write_bundle(root, kind, body, *, corrupt=False):
    root.mkdir(exist_ok=True)
    raw = canonical(body)
    (root / f"{kind}.json").write_bytes(raw)
    sha = digest(raw)
    (root / f"{kind}.sha256").write_text(
        ("0" * 64 if corrupt else sha) + "\n", encoding="ascii", newline="\n"
    )
    return sha


def fixture_cells(round_name, job):
    cells = {}
    for rate in ORDERS[job - 1]:
        outcome, reason = "pass", None
        if rate == "C" or (rate == "AB" and job == 3 and round_name == "no-eligible"):
            outcome, reason = "drain_failure", "checked expiry with unpublished outbox rows"
        elif rate == "BC" and job == 2:
            outcome, reason = "invalid", "gate_input_gap"
        elif rate == "A" and job == 1 and round_name == "no-eligible":
            outcome, reason = "invalid", "sender_invalid"
        offered = population(rate, WARMUP + MEASURED)
        sent = offered - int(reason == "sender_invalid")
        cells[rate] = {
            "rate": rate,
            "offered_rate": RATES[rate],
            "outcome": outcome,
            "reason": reason,
            "counts": {name: offered if name == "offered" else sent for name in COUNTS},
            "checked": True,
            "reset_verified": True,
        }
    return cells


def client(campaign, root, end):
    def record(event, **details):
        with (root / "events.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({"event": event, "unix_ns": time.time_ns(), **details}) + "\n")

    return Barrier(
        campaign, ActionsAPI(campaign.repository, os.environ["GITHUB_TOKEN"], Budget(end), record)
    )


def prepare(root, round_name, job, campaign):
    root.mkdir(parents=True, exist_ok=False)
    cells = fixture_cells(round_name, job)
    if campaign.scope == INTEGRATED_SCOPE:
        attempt = Path(os.environ["J2_INTEGRATED_ROOT"])
        local = read(attempt / "job.json")
        require(local["job"] == job and local["head"] == campaign.head, "matrix binding drift")
        timing = read(attempt / "barrier.json")
        binding = campaign.binding(job, local["job_id"])
        job_deadline, started, deadline = local["deadline"], timing["ready"], timing["deadline"]
        index = {
            "binding": binding,
            "cells": cells,
            "checkpoints": {
                rate: read(attempt / "receipts" / f"{rate}.json") for rate in ORDERS[job - 1]
            },
        }
    else:
        job_deadline = time.monotonic() + JOB_MINUTES * 60
        barrier = client(campaign, root, job_deadline)

        def own_job():
            row = barrier.api.jobs(campaign, allow_failed=True).get(job)
            if row is None or row["status"] != "in_progress":
                raise RetryLater("waiting for own live job ID")
            return row

        job_row = barrier.retry(own_job)
        binding = campaign.binding(job, job_row["id"])
        started_at = datetime.fromisoformat(job_row["started_at"]).timestamp()
        job_deadline = time.monotonic() + JOB_MINUTES * 60 - max(0, time.time() - started_at)
        index = {"binding": binding, "cells": cells}
    index_hash = write_bundle(root / "grid-evidence", "evidence", index)
    if campaign.scope != INTEGRATED_SCOPE:
        started = time.monotonic()
        deadline = started + BARRIER_SECONDS
    state = {
        "binding": binding,
        "index_hash": index_hash,
        "started": started,
        "deadline": deadline,
        "job_deadline": job_deadline,
    }
    emit(
        **{
            f"{kind.replace('-', '_')}_name": campaign.name(kind, job)
            for kind in ("grid-evidence", "grid", "decision", "outcome")
        }
    )
    try:
        state["deadline"] = min(deadline, barrier_deadline(started, job_deadline))
    except BarrierStop as error:
        outcome(root, state, error.reason, str(error))
        return
    write(root / "state.json", state)
    emit(ready="true")


def receipt(prefix):
    artifact_id = os.environ.get(prefix + "_ID", "")
    checksum = os.environ.get(prefix + "_DIGEST", "")
    require(
        artifact_id.isdecimal() and int(artifact_id) > 0 and sha256(checksum),
        "missing upload receipt",
    )
    return {"id": int(artifact_id), "sha256": checksum}


def publish_grid(root, round_name, job, campaign, state):
    Budget(state["deadline"]).remaining()
    evidence = {
        **receipt("GRID_EVIDENCE"),
        "name": campaign.name("grid-evidence", job),
        "index_sha256": state["index_hash"],
    }
    cells = read(root / "grid-evidence" / "evidence.json")["cells"]
    binding = state["binding"]
    if round_name == "mixed" and job == 3:
        binding = {**binding, "attempt": binding["attempt"] + 1}
    stop = (
        {"classification": "integrity_stop", "reason": "fixture checker violation"}
        if round_name == "integrity" and job == 3
        else None
    )
    write_bundle(
        root / "grid",
        "grid",
        {
            "binding": binding,
            "pins": campaign.pins,
            "qualified_volume": campaign.volume,
            "cells": [
                {**cells[rate], "evidence_sha256": digest(canonical(cells[rate]))}
                for rate in ORDERS[job - 1]
            ],
            "evidence": evidence,
            "stop": stop,
        },
        corrupt=round_name == "corrupted" and job == 3,
    )


def outcome(root, state, result, detail, **evidence):
    path = root / "events.jsonl"
    events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    write_bundle(
        root / "outcome",
        "outcome",
        {
            "binding": state["binding"],
            "outcome": result,
            "detail": detail,
            "elapsed_seconds": time.monotonic() - state["started"],
            "events": events,
            "soak": "unperformed",
            **evidence,
        },
    )


def attempt(root, state, operation):
    try:
        operation()
    except BarrierStop as error:
        outcome(root, state, error.reason, str(error))


def rendezvous(root, job, campaign, state):
    barrier = client(campaign, root, state["deadline"])

    def decide():
        decision, jobs = barrier.retry(barrier.grids)
        require(
            state["binding"] == campaign.binding(job, jobs[job]["id"]), "local job identity drift"
        )
        write(root / "peer-ids.json", {str(n): jobs[n]["id"] for n in JOBS})
        write_bundle(
            root / "decision", "decision", {"binding": state["binding"], "decision": decision}
        )
        emit(publish_decision="true")

    attempt(root, state, decide)


def confirm(root, job, campaign, state):
    barrier = client(campaign, root, state["deadline"])

    def finalize():
        decision = read(root / "decision" / "decision.json")["decision"]
        upload = receipt("DECISION")
        peer_ids = read(root / "peer-ids.json")

        def confirmed():
            jobs = barrier.api.jobs(campaign)
            if jobs.keys() != set(JOBS):
                raise RetryLater("waiting for all peer statuses")
            require(peer_ids == {str(n): jobs[n]["id"] for n in JOBS}, "peer job identity drift")
            saved = barrier.decisions(decision, jobs, only=(job,), receipt=upload)
            # Check again after downloading the immutable decision. Peers make
            # their own local decisions and the reporter has no release role.
            current = barrier.api.jobs(campaign)
            if current.keys() != jobs.keys():
                raise RetryLater("waiting for the post-publication peer status")
            require(all(jobs[n]["id"] == current[n]["id"] for n in JOBS), "peer job identity drift")
            return saved

        saved = barrier.retry(confirmed)
        rate = decision["selected_rate"]
        outcome(
            root,
            state,
            "no_eligible_rate" if rate == NO_RATE else "would_start",
            "synthetic barrier decision",
            decision=decision,
            decision_upload=saved[job],
        )

    attempt(root, state, finalize)


def wait_for_fixture(root, campaign, state, *, withheld):
    barrier = client(campaign, root, state["job_deadline"] if withheld else state["deadline"])

    def finished():
        jobs = barrier.api.jobs(campaign, allow_failed=True)
        wanted = (1, 2) if withheld else (3,)
        if any(jobs.get(job, {}).get("status") != "completed" for job in wanted):
            raise RetryLater("waiting for the fixture peer jobs")
        if not withheld:
            require(jobs[3]["conclusion"] == "failure", "injected peer did not fail")

    def wait():
        barrier.retry(finished)
        if withheld:
            outcome(root, state, "withheld", "fixture withheld the manifest while peers timed out")

    attempt(root, state, wait)


def report_round(root, round_name, end):
    campaign = context(round_name)
    barrier = client(campaign, root, end)

    def terminal_jobs():
        jobs = barrier.api.jobs(campaign, allow_failed=True)
        if any(job["status"] != "completed" for job in jobs.values()):
            raise RetryLater("waiting for terminal matrix status visibility")
        return jobs

    jobs = barrier.retry(terminal_jobs)
    names = {job: campaign.name("outcome", job) for job in JOBS}
    grids = {job: campaign.name("grid", job) for job in JOBS}
    decisions = {job: campaign.name("decision", job) for job in JOBS}
    outcomes, receipts = {}, {}
    artifacts = {}
    visibility_error = None

    def retain_visible():
        nonlocal artifacts
        artifacts = barrier.api.artifacts(
            campaign, set(names.values()) | set(grids.values()) | set(decisions.values())
        )
        for job, name in names.items():
            if name not in artifacts or job not in jobs:
                continue
            artifact = artifacts[name]
            body, _ = barrier.bundle("outcome", artifact)
            require(
                isinstance(body, dict)
                and campaign.matches(body.get("binding"), job, jobs[job]["id"]),
                "outcome identity drift",
            )
            require(body.get("soak") == "unperformed", "unexpected engineering soak")
            outcomes[job] = body
            receipts[job] = {"id": artifact["id"], "sha256": artifact["digest"][7:]}
        waiting = [
            job
            for job in jobs
            if job not in outcomes
            and (
                jobs[job]["conclusion"] == "success" or (round_name == "peer-failure" and job == 3)
            )
        ]
        if waiting:
            raise RetryLater(f"waiting for local outcome archives from jobs {waiting}")

    try:
        barrier.retry(retain_visible)
    except BarrierStop as error:
        visibility_error = error
    for job in JOBS:
        outcomes.setdefault(
            job, {"outcome": "evidence_lost", "soak": "unperformed", "job": jobs.get(job)}
        )
    summary = {
        "outcomes": outcomes,
        "receipts": receipts,
        "scope": campaign.scope,
        "soak": "unperformed",
    }
    try:
        if visibility_error is not None:
            raise visibility_error
        require(jobs.keys() == set(JOBS), "three job records required")
        expected = {job: EXPECTED[round_name] for job in JOBS}
        conclusions = dict.fromkeys(JOBS, "success")
        if round_name == "peer-failure":
            expected[3], conclusions[3] = "injected_failure", "failure"
            require(grids[3] in artifacts, "missing failed peer's published manifest")
            metadata = artifacts[grids[3]]
            peer, _ = barrier.retry(lambda: barrier.bundle("grid", metadata))
            validate_grid(peer, campaign, 3, jobs[3]["id"])
            require(
                outcomes[3].get("grid_upload")
                == {"id": metadata["id"], "sha256": metadata["digest"][7:]},
                "failed peer grid upload receipt drift",
            )
        if round_name == "missing-peer":
            expected[3] = "withheld"
            require(grids[3] not in artifacts, "withheld manifest was published")
            for job in (1, 2):
                events = outcomes[job].get("events", [])
                require(
                    outcomes[job].get("elapsed_seconds", 0) >= BARRIER_SECONDS,
                    "missing-peer fixture did not wait for the real barrier deadline",
                )
                require(
                    any(
                        row.get("job") == 3 and row.get("status") == "in_progress" for row in events
                    ),
                    "withholding peer was not observed live",
                )
                waits = [row["detail"] for row in events if row.get("event") == "retry"]
                require(
                    waits and waits[-1] == "waiting for grids and evidence from jobs [3]",
                    "last successful poll did not identify the withheld grid",
                )
        require(
            all(jobs[job]["conclusion"] == conclusions[job] for job in JOBS),
            "unexpected matrix job conclusion",
        )
        require(
            all(outcomes[job]["outcome"] == expected[job] for job in JOBS),
            "unexpected local barrier outcome",
        )
        if round_name in {"eligible", "no-eligible"}:
            computed, verified_jobs = barrier.retry(barrier.grids)
            rate = "AB" if round_name == "eligible" else NO_RATE
            require(computed["selected_rate"] == rate, "unexpected synthetic eligibility result")
            uploads = barrier.retry(lambda: barrier.decisions(computed, verified_jobs))
            require(
                all(
                    outcomes[job]["decision"] == computed
                    and outcomes[job]["decision_upload"] == uploads[job]
                    for job in JOBS
                ),
                "reported local decision differs from the verified archive",
            )
            summary.update(agreement=computed, decision_uploads=uploads)
        else:
            require(
                not any(name in artifacts for name in decisions.values()),
                "blocked round published a decision",
            )
            causes = {
                "integrity": "fixture checker violation",
                "corrupted": "grid.json digest mismatch",
                "mixed": "grid campaign/run/attempt/head/job drift",
            }
            if round_name in causes:
                require(
                    all(causes[round_name] in outcomes[job]["detail"] for job in JOBS),
                    "blocked round did not observe the injected fault",
                )
        summary["status"] = "PASS"
    except BarrierStop as error:
        summary.update(status="FAIL", reason=error.reason, detail=str(error))
    return summary


def report(root, mode):
    root.mkdir(parents=True, exist_ok=True)
    end = time.monotonic() + BARRIER_SECONDS
    results = {}
    for round_name in ROUNDS[mode]:
        try:
            results[round_name] = report_round(root, round_name, end)
        except BarrierStop as error:
            results[round_name] = {
                "status": "FAIL",
                "reason": error.reason,
                "detail": str(error),
                "soak": "unperformed",
            }
    write(root / "report.json", results)
    for round_name, result in results.items():
        print(round_name, result["status"], result.get("detail", ""), flush=True)
    return int(any(result["status"] != "PASS" for result in results.values()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=(
            "prepare",
            "grid",
            "rendezvous",
            "confirm",
            "fail",
            "withhold",
            "wait-for-failure",
            "report",
        ),
    )
    parser.add_argument("root", type=Path)
    parser.add_argument("scenario")
    parser.add_argument("job", type=int, choices=JOBS, nargs="?")
    args = parser.parse_args()
    root, name, job = args.root, args.scenario, args.job
    if args.command == "report":
        return report(root, name)
    require(name in EXPECTED and job in JOBS, "unknown fixture or job")
    campaign = context(name)
    if args.command == "prepare":
        prepare(root, name, job, campaign)
        return 0
    state = read(root / "state.json")
    schema(state, "binding index_hash started deadline job_deadline", "round state")
    if args.command == "grid":
        publish_grid(root, name, job, campaign, state)
    elif args.command == "rendezvous":
        rendezvous(root, job, campaign, state)
    elif args.command == "confirm":
        confirm(root, job, campaign, state)
    elif args.command == "fail":
        outcome(
            root,
            state,
            "injected_failure",
            "fixture failed after publishing the grid",
            grid_upload=receipt("GRID"),
        )
        return 1
    else:
        wait_for_fixture(root, campaign, state, withheld=args.command == "withhold")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
