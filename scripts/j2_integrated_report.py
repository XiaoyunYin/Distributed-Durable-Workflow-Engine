"""Verify integrated rehearsal archives on a fresh GitHub-hosted runner."""

from __future__ import annotations

import argparse
import hashlib
import math
import os
import shutil
import stat
import subprocess
import tempfile
import time
import zipfile
from contextlib import contextmanager
from pathlib import Path
from urllib.request import Request

from dur053_pilot import witness_events
from j2_barrier import JOBS, RetryLater, canonical, decode
from j2_barrier_rehearsal import client, context, report_round
from j2_checker import CONTAINER_CHECKER, MOUNTED_CHECKER, verify_binary
from j2_checker_report import indexed_report, require_checker_pass
from j2_harness import hosted_dispatch, verify_new_identity
from j2_harness_evidence import safe_name
from j2_integrated_evidence import engineering_boundary, summarize_episode
from j2_integrated_rehearsal import (
    EXPIRY_FIXTURE,
    ROUNDS,
    SCOPE,
    episode_schedule,
    forecast,
    label_name,
)
from j2_offline import SECOND, utc_ns
from j2_prequalify_checker import WORKER_BASE
from j2_qualification_evidence import file_record, read, require, verify, write
from j2_schedule import ORDERS, RATES

QUALIFICATION_ARTIFACT = 15158465873
QUALIFICATION_HASH = "7e8e5d09f3ca5499b36dd9ddb88afaccb550f6ebaed552d62dd731191542202f"
QUALIFICATION_HEAD = "03f3c767a8143e47e0cd725fd33f1ce16a72faba"
ZIP_BOUND = 512 << 20
UNCOMPRESSED_BOUND = 3 << 30


def download(api, metadata, path):
    artifact = metadata["id"]
    location = api.request(
        f"{api.root}/artifacts/{artifact}/zip",
        f"artifact {artifact} redirect",
        authenticated=True,
        redirect=True,
    )
    remaining = metadata["size_in_bytes"]
    require(
        type(remaining) is int and 0 < remaining <= ZIP_BOUND, "artifact exceeds disk allowance"
    )
    digest = hashlib.sha256()
    try:
        with (
            api.opener.open(Request(location), timeout=min(15, api.budget.remaining())) as response,
            path.open("wb") as stream,
        ):
            require(response.status == 200, "artifact storage request failed")
            while chunk := response.read(1 << 20):
                api.budget.remaining()
                remaining -= len(chunk)
                require(remaining >= 0, "artifact body exceeds recorded size")
                digest.update(chunk)
                stream.write(chunk)
    except OSError:
        raise RetryLater(f"artifact {artifact} storage unavailable") from None
    require(
        remaining == 0 and metadata["digest"] == f"sha256:{digest.hexdigest()}",
        "artifact ZIP size or digest differs",
    )


def extract(archive, directory, *, integrated=True):
    directory.mkdir()
    with zipfile.ZipFile(archive) as bundle:
        entries = [entry for entry in bundle.infolist() if not entry.is_dir()]
        names = {entry.filename for entry in entries}
        require(
            len(names) == len(entries)
            and "freeze.json" in names
            and sum(e.file_size for e in entries) <= UNCOMPRESSED_BOUND
            and all(
                safe_name(e.filename) and not stat.S_ISLNK(e.external_attr >> 16) for e in entries
            ),
            "unsafe members, duplicate input or disk bound exceeded",
        )
        raw = bundle.read("freeze.json")
        index = decode(raw)
        require(names == set(index["files"]) | {"freeze.json"}, "archive membership differs")
        if integrated:
            require(
                index["schema"] == "j2.harness.freeze.v1"
                and index["head"] == os.environ["GITHUB_SHA"]
                and index["run_id"] == os.environ["GITHUB_RUN_ID"]
                and index["run_attempt"] == os.environ["GITHUB_RUN_ATTEMPT"],
                "archive run, attempt or head drift",
            )
        for name, expected in index["files"].items():
            require(
                bundle.getinfo(name).file_size == expected["bytes"], f"archive size drift: {name}"
            )
            target = directory / name
            target.parent.mkdir(parents=True, exist_ok=True)
            with bundle.open(name) as source, target.open("wb") as stream:
                shutil.copyfileobj(source, stream)
            require(file_record(target) == expected, f"archive digest drift: {name}")
    return hashlib.sha256(raw).hexdigest()


def checker_replay(root, checker, timeout, *, qualification=False):
    lock = verify_binary(checker)
    args = ["-snapshot", "/evidence/snapshot.json"]
    if qualification:
        args.extend(
            ["-arrivals", "/evidence/arrivals.json", "-witness-export", "/evidence/witness.json"]
        )
    if qualification or read(root / "drain-result.json")["status"] == "PASS":
        args.append("-require-terminal")
    command = [
        "docker",
        "run",
        "--rm",
        "--network",
        "none",
        "--cpus=1",
        "--memory=2g",
        "--memory-swap=2g",
        "--read-only",
        "--cap-drop=ALL",
        "--security-opt=no-new-privileges",
        "--user=65532:65532",
        "--mount",
        f"type=bind,src={checker.resolve()},dst={MOUNTED_CHECKER},readonly",
        "--mount",
        f"type=bind,src={root.resolve()},dst=/evidence,readonly",
        WORKER_BASE,
        "python",
        "-c",
        CONTAINER_CHECKER,
        MOUNTED_CHECKER,
        lock["sha256"],
        lock["gomemlimit"],
        *args,
    ]
    output = root / "recomputed-checker.json"
    with output.open("wb") as stdout, (root / "recomputed-checker.stderr").open("wb") as stderr:
        subprocess.run(command, stdout=stdout, stderr=stderr, check=True, timeout=timeout)
    snapshot = root / "recomputed-snapshot.json"
    reproduced, tables = indexed_report(
        output, snapshot=snapshot, verify_tables=True, join_trace=not qualification
    )
    live, live_tables = indexed_report(root / "live-checker.json", verify_tables=True)
    require(
        file_record(snapshot) == file_record(root / "snapshot.json") and live_tables == tables,
        "offline checker snapshot differs from the live export",
    )
    if qualification:
        durations = check_witness_durations(
            read(root / "witness.json"),
            live,
            reproduced,
            indexed_report(root / "offline-checker.json")[0],
        )
        (root / "offline-checker.json").replace(root / "archived-offline-checker.json")
        output.replace(root / "offline-checker.json")
        reconciled = verify(root)
        result = {name: value for name, value in reconciled.items() if name != "tables"}
        require(
            result["identities"] == 26678 and result["identical_verdict"] == "PASS",
            "qualification replay did not reconcile the registered population",
        )
        return {**result, "witness_durations": durations}
    require_checker_pass(reproduced)
    offline_ids = [
        row["workflow_id"]
        for row in reproduced["trace"]["Snapshot"]["engine.workflow_executions"]["rows"]
    ]
    require(
        sorted(live["workflow_ids"]) == sorted(offline_ids), "offline checker identity set differs"
    )
    for report in (live, reproduced):
        report.pop("trace")
        # Snapshot mode does not receive separate workflow ID flags.
        report.pop("workflow_ids", None)
        report["non_terminal"] = sorted(report["non_terminal"] or [])
    require(live == reproduced, "locked checker verdict differs")
    return {
        "identities": len(offline_ids),
        "verdict": "PASS",
        "snapshot": file_record(snapshot),
    }


def check_witness_durations(witness, *reports):
    events = witness_events(witness)
    starts = sorted(
        (event for event in events if event["event"] == "start"),
        key=lambda event: (utc_ns(event["timestamp"]), event["invocation_id"]),
    )
    ends = {event["invocation_id"]: event for event in events if event["event"] == "end"}
    busy = reexecuted = 0
    seen = set()
    for event in starts:
        key = tuple(event[name] for name in ("workflow_id", "node_id", "iteration"))
        end = ends[event["invocation_id"]]
        duration = end["monotonic_ns"] - event["monotonic_ns"]
        require(duration >= 0, "reversed qualification witness duration")
        busy += duration
        if key in seen:
            reexecuted += duration
        seen.add(key)
    totals = {"busy_seconds": busy / SECOND, "reexecuted_seconds": reexecuted / SECOND}
    for report in reports:
        for name, seconds in totals.items():
            # Each positive addend is rounded once before the Go map-ordered sum.
            tolerance = math.ulp(seconds) * (len(starts) + 2)
            require(
                abs(report["campaign"][name] - seconds) <= tolerance,
                f"{name} differs from witness duration",
            )
    return totals


@contextmanager
def artifact_evidence(barrier, metadata, output, max_seconds):
    start = time.monotonic()
    with tempfile.TemporaryDirectory(dir=output) as temp:
        root = Path(temp)
        archive = root / "checkpoint.zip"
        old = barrier.api.budget.end
        barrier.api.budget.end = min(old, start + max_seconds)
        try:
            barrier.retry(lambda: download(barrier.api, metadata, archive))
            extract_root = root / "evidence"
            index_hash = extract(archive, extract_root)
        finally:
            barrier.api.budget.end = old
        yield_seconds = start + max_seconds - time.monotonic()
        require(yield_seconds > 0, "archive verification exceeded its forecast allowance")
        yield extract_root, index_hash, min(yield_seconds, barrier.api.budget.remaining())


def qualification_archive(barrier, checker, output):
    api = barrier.api
    meta = decode(
        api.request(
            f"{api.root}/artifacts/{QUALIFICATION_ARTIFACT}",
            "qualification metadata",
            authenticated=True,
        )
    )
    if meta["expired"]:
        return {"status": "expired", "id": QUALIFICATION_ARTIFACT, "expires_at": meta["expires_at"]}
    require(
        meta["id"] == QUALIFICATION_ARTIFACT
        and meta["digest"] == f"sha256:{QUALIFICATION_HASH}"
        and meta["workflow_run"]["head_sha"] == QUALIFICATION_HEAD
        and meta["workflow_run"]["id"] == 45893066798,
        "registered qualification archive metadata differs",
    )
    with tempfile.TemporaryDirectory(dir=output) as temp:
        root = Path(temp)
        archive = root / "qualification.zip"
        barrier.retry(lambda: download(api, meta, archive))
        index = extract(archive, root / "evidence", integrated=False)
        result = checker_replay(
            root / "evidence", checker, min(150, api.budget.remaining()), qualification=True
        )
    return {"status": "PASS", "metadata": meta, "index_sha256": index, **result}


def verify_episode(root, job, cell, previous, checker, timeout):
    checkpoint = read(root / "checkpoint.json")
    rate = checkpoint["offered_rate"]
    require(
        checkpoint["scope"] == SCOPE
        and checkpoint["job"] == job
        and checkpoint["cell"] == cell
        and checkpoint["measured_eligible"] is False
        and (cell != "soak" or rate == "AB"),
        "episode checkpoint binding differs",
    )
    schedule = read(root / "schedule.json")
    require(
        schedule == episode_schedule(job, cell, rate, schedule["origin_ns"]),
        "engineering clock drift",
    )
    identity = read(root / "identity.json")
    require(identity == checkpoint["identity"], "fresh-state identity drift")
    verify_new_identity(previous, identity)
    require(
        previous is None or (root / "previous-cleanup.json").is_file(),
        "prior reset evidence missing",
    )
    fresh = read(root / "fresh-db.json")
    initial = read(root / "before-workloads-transport.json")
    require(
        not any(fresh["counts"].values())
        and fresh["settings"] == ["on", "on", "on", "100"]
        and fresh["engine_partitions"] == list(range(16))
        and all(not any(partitions.values()) for partitions in initial["latest"].values()),
        "replayed fresh-state predicates differ",
    )
    checked = checker_replay(root, checker, timeout)
    summary = summarize_episode(root)
    if cell == "soak":
        require(
            summary["verified_crossing_ids"]
            == read(root / "step-transition.json")["verified_crossing_ids"],
            "retained crossings differ",
        )
    require(
        canonical(summary) == canonical(read(root / "episode-replay.json")),
        "episode replay differs from the checkpoint",
    )
    if (job, cell) == EXPIRY_FIXTURE:
        require(
            checkpoint["quiet_visibility_fixture"]
            and summary["drain"]["status"] == "drain_failure"
            and "engineering fixture delays the qualifying quiet read"
            in (root / "drain-polls.jsonl").read_text(),
            "checked grid expiry fixture missing",
        )
    return identity, {"episode": summary, "checker": checked}


def replay_job(barrier, output, checker, job, artifacts, checkpoint_refs):
    order = ORDERS[job - 1]
    final_name = label_name(job, "final")
    cells, errors, final, coverage = {}, {}, {}, {}
    receipts = dict(checkpoint_refs or {})
    if checkpoint_refs is None:
        errors["barrier"] = "pre-barrier checkpoint index unavailable"
    try:
        require(final_name in artifacts, f"job {job} final evidence lost")
        with artifact_evidence(barrier, artifacts[final_name], output, 30) as (root, index, _):
            cleanup = read(root / "cleanup.json")
            retained = read(root / "checkpoints.json")
            state = read(root / "status.json")
            coverage = read(root / "coverage.json")
            if checkpoint_refs is not None:
                require(
                    all(retained.get(cell) == checkpoint_refs[cell] for cell in order),
                    "barrier receipt drift",
                )
            require(
                read(root / "forecast.json") == decode_from_forecast(job), "matrix forecast differs"
            )
            receipts.update(retained)
            final = {
                "cleanup": cleanup,
                "status": state,
                "metadata": artifacts[final_name],
                "index_sha256": index,
                "coverage": coverage,
            }
            if (
                cleanup["status"] != "PASS"
                or not state["cleanup_verified"]
                or state["status"] != "PASS"
            ):
                errors["cleanup"] = "attempt did not finish with verified cleanup"
    except Exception as error:
        errors["final"] = str(error)
    previous = None
    for cell in (*order, "soak"):
        name = label_name(job, cell)
        try:
            require(name in artifacts, f"job {job} {cell} {coverage.get(cell, 'evidence lost')}")
            meta, receipt = artifacts[name], receipts.get(cell)
            if receipt is None:
                errors[cell] = "checkpoint upload receipt unavailable"
            else:
                require(
                    name == receipt["name"]
                    and meta["id"] == receipt["id"]
                    and meta["digest"] == f"sha256:{receipt['sha256']}",
                    "checkpoint upload receipt differs",
                )
            with artifact_evidence(barrier, meta, output, 150) as (root, index, budget):
                require(
                    receipt is None or index == receipt["index_sha256"],
                    "checkpoint index differs from receipt",
                )
                previous, result = verify_episode(root, job, cell, previous, checker, budget)
                cells[cell] = {"metadata": meta, "index_sha256": index, **result}
        except Exception as error:
            errors[cell] = str(error)
            previous = None
    return {
        "cells": cells,
        "errors": errors,
        "final": final,
        "diagnostic": engineering_boundary(
            {k: v["episode"] for k, v in cells.items()}, tuple(RATES)
        ),
    }


def decode_from_forecast(job):
    ledger = forecast(job)
    return {**ledger, "order": list(ledger["order"])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("checker", type=Path)
    args = parser.parse_args()
    hosted_dispatch()
    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    ledger = forecast(1)
    reserves = ledger["replay"]
    elapsed = max(0, time.time() - int(os.environ["J2_REPLAY_START"]))
    require(elapsed <= reserves["bootstrap"], "replay bootstrap exceeded forecast")
    end = time.monotonic() + min(
        reserves["barrier_replay"]
        + reserves["matrix_evidence"]
        + reserves["qualification_archive"],
        ledger["replay_cap_seconds"]
        - elapsed
        - reserves["volume_fixture"]
        - reserves["volume_teardown"]
        - reserves["report_upload"],
    )
    require(shutil.disk_usage(output).free >= 5 << 30, "replay disk headroom below forecast")
    subprocess.run(["docker", "pull", WORKER_BASE], timeout=180, check=True)
    results = {"scope": SCOPE, "measured_eligible": False, "rounds": {}, "jobs": {}, "errors": {}}
    for round_name in ROUNDS:
        try:
            results["rounds"][round_name] = report_round(
                output, round_name, min(end, time.monotonic() + 100)
            )
        except Exception as error:
            results["errors"][round_name] = str(error)
    campaign = context("eligible")
    barrier = client(campaign, output, end)
    names = {label_name(job, cell) for job in JOBS for cell in (*RATES, "soak", "final")}
    grid_evidence_names = {campaign.name("grid-evidence", job) for job in JOBS}
    try:
        artifacts = barrier.retry(
            lambda: barrier.api.artifacts(campaign, names | grid_evidence_names)
        )
        for job in JOBS:
            refs = None
            try:
                name = campaign.name("grid-evidence", job)
                if name in artifacts:
                    refs = barrier.retry(
                        lambda name=name: barrier.bundle("evidence", artifacts[name])
                    )[0]["checkpoints"]
            except Exception as error:
                results["errors"][f"job-{job}-manifest"] = str(error)
            try:
                results["jobs"][job] = replay_job(
                    barrier, output, args.checker, job, artifacts, refs
                )
            except Exception as error:
                results["errors"][str(job)] = str(error)
        try:
            barrier.api.budget.end = min(
                end, time.monotonic() + ledger["replay"]["qualification_archive"]
            )
            results["qualification_archive"] = qualification_archive(barrier, args.checker, output)
        except Exception as error:
            results["errors"]["qualification_archive"] = str(error)
    except Exception as error:
        results["errors"]["artifact_listing"] = str(error)
    write(output / "integrated-replay.json", results)
    return int(
        bool(
            results["errors"]
            or any(r["status"] != "PASS" for r in results["rounds"].values())
            or any(r["errors"] for r in results["jobs"].values())
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
