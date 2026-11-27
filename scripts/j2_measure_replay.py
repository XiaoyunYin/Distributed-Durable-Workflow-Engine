"""Independently replay measured J2 archives and authorize dependent observations."""

import argparse
import json
import os
import shutil
import subprocess
import tempfile
import time
import zipfile
from contextlib import contextmanager
from pathlib import Path
from urllib.error import HTTPError, URLError

from j2_attribution import diagnostic_pair, observe_bins
from j2_barrier import (
    JOBS,
    NO_RATE,
    Barrier,
    Campaign,
    RetryLater,
    canonical,
    decide,
    decode,
    digest,
    read_bundle,
    validate_grid,
)
from j2_barrier_rehearsal import client
from j2_checker_report import IntegrityError
from j2_harness import APP, DB, NS, OTHER, hosted_dispatch, verify_new_identity
from j2_harness_evidence import seal
from j2_integrated_evidence import registered_boundary
from j2_integrated_report import (
    CONTROL_ARTIFACTS,
    checker_replay,
    extract,
    verify_episode_reduction,
)
from j2_measure import cell_record, schedule_for
from j2_measure_authorization import control_identity, engineering, validate_ledger
from j2_measure_telemetry import cell_samples
from j2_prequalify_checker import WORKER_BASE
from j2_qualification_evidence import file_record, read, require, write
from j2_replay_storage import (
    QUALIFICATION_NAME,
    load_release_index,
    metadata_source,
    prerequisite_archives,
    pull_archive,
    release_member_record,
    run_metadata,
    source_campaign,
    source_names,
    verify_prerequisite,
    verify_report_source,
)
from j2_schedule import ORDERS, RATES

CAP_MINUTES = 90
MINUTES = {
    "bootstrap": 8,
    "download": 18,
    "extract": 8,
    "checker": 22,
    "reduce": 6,
    "retry": 14,
    "upload": 9,
}
DISK_RESERVE = 256 << 20


class ReplayBudget:
    def __init__(
        self, output, elapsed, *, now=time.monotonic, disk=shutil.disk_usage, sleep=time.sleep
    ):
        self.output, self.now, self.disk, self.sleep = output, now, disk, sleep
        self.started = now() - elapsed
        self.cutoff = self.started + (CAP_MINUTES - MINUTES["upload"]) * 60
        self.seconds = dict.fromkeys(MINUTES, 0.0)
        self.seconds["bootstrap"] = elapsed
        self.peak = {
            "zip_bytes": 0,
            "extracted_bytes": 0,
            "checker_scratch_bytes": 0,
            "retained_report_bytes": 0,
        }
        self.retry_events = []
        self.archives = []
        self.free_min = disk(output).free

    def remaining(self, stage=None, since=None):
        left = self.cutoff - self.now()
        if stage is not None:
            left = min(left, MINUTES[stage] * 60 - self.seconds[stage] - (self.now() - since))
        require(left > 0, f"replay {stage or 'work'} budget exhausted with upload time reserved")
        return left

    @contextmanager
    def stage(self, name, api=None):
        started = self.now()
        old = api.budget.end if api is not None else None

        def seconds():
            return self.remaining(name, started)

        if api is not None:
            api.budget.end = min(old, self.now() + seconds())
        try:
            seconds()
            yield seconds
            seconds()
        finally:
            self.seconds[name] += max(0, self.now() - started)
            if api is not None:
                api.budget.end = old

    def retry(self, operation):
        while True:
            self.remaining()
            try:
                return operation()
            except (RetryLater, URLError) as error:
                if isinstance(error, HTTPError) and error.code not in {
                    408,
                    429,
                    500,
                    502,
                    503,
                    504,
                }:
                    raise
                with self.stage("retry") as left:
                    delay = error.delay if isinstance(error, RetryLater) else 5
                    self.retry_events.append({"reason": str(error), "delay_seconds": delay})
                    require(delay < left(), "retry reserve exhausted")
                    self.sleep(delay)

    def disk_for(self, required, *, zip_bytes=0, extracted=0, scratch=0):
        free = self.disk(self.output).free
        self.free_min = min(self.free_min, free)
        self.peak["zip_bytes"] = max(self.peak["zip_bytes"], zip_bytes)
        self.peak["extracted_bytes"] = max(self.peak["extracted_bytes"], extracted)
        self.peak["checker_scratch_bytes"] = max(self.peak["checker_scratch_bytes"], scratch)
        require(free >= required + DISK_RESERVE, "replay disk headroom below next archive bound")

    def record(self):
        self.peak["retained_report_bytes"] = sum(
            item.stat().st_size for item in self.output.rglob("*") if item.is_file()
        )
        return {
            "cap_minutes": CAP_MINUTES,
            "reservations_minutes": MINUTES,
            "reserved_minutes": sum(MINUTES.values()),
            "elapsed_seconds": self.now() - self.started,
            "work_seconds_by_stage": self.seconds,
            "disk_high_water": self.peak,
            "minimum_free_bytes": self.free_min,
            "retry_events": self.retry_events,
            "archives": self.archives,
        }


class ReplayInputs:
    def __init__(self, gate, metadata, budget, *, release=None):
        self.gate, self.metadata, self.budget, self.release = gate, metadata, budget, release
        self.index = {}

    @contextmanager
    def archive(self, name):
        require(name in self.metadata, f"missing checkpoint or receipt {name}")
        meta = self.metadata[name]
        before = self.budget.seconds.copy()
        record = {"name": name, "compressed_bytes": meta["size_in_bytes"], "status": "partial"}
        self.budget.archives.append(record)
        try:
            with self._stage_archive(name, meta, record) as source:
                yield source
                record["status"] = "verified"
        finally:
            record["seconds_by_stage"] = {
                stage: elapsed - before[stage]
                for stage, elapsed in self.budget.seconds.items()
                if elapsed > before[stage]
            }

    @contextmanager
    def _stage_archive(self, name, meta, record):
        self.budget.disk_for(meta["size_in_bytes"])
        with tempfile.TemporaryDirectory(dir=self.budget.output.parent) as temp:
            root = Path(temp)
            path = root / "source.zip"

            def download():
                with self.budget.stage("download", self.gate.api) as left:
                    pull_archive(self.gate, meta, path, release=self.release, remaining=left)

            self.budget.retry(download)
            with self.budget.stage("extract") as left, zipfile.ZipFile(path) as archive:
                sizes = {
                    item.filename: item.file_size
                    for item in archive.infolist()
                    if not item.is_dir()
                }
                expanded = sum(sizes.values())
                scratch = sizes.get("snapshot.json", 0) + sizes.get("live-checker.json", 0)
                record["expanded_bytes"] = expanded
                self.budget.disk_for(expanded + scratch, zip_bytes=path.stat().st_size)
                inventory = release_member_record(path, meta, left)
                self.index[name] = {
                    "actions": {key: value for key, value in meta.items() if key != "release"},
                    "inventory": inventory,
                    **({"release": meta["release"]} if "release" in meta else {}),
                }
                evidence = root / "evidence"
                if inventory["freeze"] is not None:
                    binding = (
                        self.gate.campaign if meta["source_run"] == self.gate.campaign.run else None
                    )
                    freeze = extract(
                        path, evidence, integrated=False, binding=binding, remaining=left
                    )
                    self.budget.disk_for(scratch, extracted=expanded)
                else:
                    evidence, freeze = None, None
            try:
                yield path, evidence, freeze
            finally:
                if evidence is not None:
                    scratch = sum(
                        p.stat().st_size for p in evidence.glob("recomputed-*") if p.is_file()
                    )
                    if (evidence / "archived-offline-checker.json").is_file():
                        scratch += (evidence / "offline-checker.json").stat().st_size
                    record["checker_scratch_bytes"] = scratch
                    self.budget.disk_for(0, scratch=scratch)

    def bundle(self, name, kind):
        with self.archive(name) as (archive, _, _):
            return read_bundle(archive.read_bytes(), kind)


def check_ref(meta, receipt, name):
    require(
        meta["id"] == receipt["id"]
        and meta["digest"] == "sha256:" + receipt["sha256"]
        and meta["name"] == receipt.get("name", name) == name,
        "archived upload receipt differs",
    )


def verify_checkpoint(
    root, campaign, job, cell, previous, checker, budget, api, *, release_proof=None
):
    checkpoint = read(root / "checkpoint.json")
    rate = checkpoint["offered_rate"]
    require(
        checkpoint["scope"] == campaign.scope
        and checkpoint["job"] == job
        and checkpoint["cell"] == cell
        and (rate == cell if cell != "soak" else rate in {"A", "AB"}),
        "source checkpoint binding differs",
    )
    schedule = read(root / "schedule.json")
    require(
        schedule == schedule_for(campaign, job, cell, rate, schedule["origin_ns"]),
        "verified schedule differs from the source clock",
    )
    released = read(root / "sender-release.json")
    require(
        released["run_id"] == schedule["run_id"]
        and released["telemetry"] == file_record(root / "pre-traffic-telemetry.json")
        and (
            release_proof is None
            or all(released[key] == value for key, value in release_proof.items())
        ),
        "sender release does not bind its pre-traffic evidence",
    )
    identity = read(root / "identity.json")
    require(identity == checkpoint["identity"], "source checkpoint reset identity differs")
    verify_new_identity(previous, identity)
    if previous is not None:
        cleanup = read(root / "previous-cleanup.json")
        require(
            set(cleanup["namespaces_absent"]) == {APP, NS, OTHER}
            and cleanup["database_absent"] == DB
            and bool(cleanup["witness_pv_absent"]),
            "reset cleanup evidence differs",
        )
    fresh = read(root / "fresh-db.json")
    initial = read(root / "before-workloads-transport.json")
    require(
        not any(fresh["counts"].values())
        and fresh["settings"] == ["on", "on", "on", "100"]
        and fresh["engine_partitions"] == list(range(16))
        and all(not any(partitions.values()) for partitions in initial["latest"].values()),
        "source reset or fresh state differs",
    )
    with budget.stage("checker", api) as left:
        checked = checker_replay(root, checker, min(240, left()))
        summary = verify_episode_reduction(root)
    with budget.stage("reduce"):
        telemetry = cell_samples(read(root / "prometheus-samples.json"), schedule)
        require(
            telemetry == read(root / "cell-scrape-evidence.json"),
            "source telemetry reduction differs",
        )
        row, gaps = cell_record(summary, telemetry)
        retained = checkpoint["row"]
        if retained["recovery"] is not None:
            recovery = read(root / "recovery.json")
            require(
                {key: recovery[key] for key in retained["recovery"]} == retained["recovery"]
                and sorted(gaps) == recovery["series"],
                "invalid cell recovery proof differs",
            )
            require(
                _original_digest(recovery["identity"]) == recovery["identity_sha256"]
                and _original_digest(recovery["telemetry"]) == recovery["telemetry_sha256"],
                "fresh recovery proof hash differs",
            )
            row["recovery"] = retained["recovery"]
        row.update(rate=rate, offered_rate=RATES[rate], reset_verified=True)
        require(
            row == retained and summary["population"] == checkpoint["population"],
            "independent grid or episode outcome differs",
        )
        if checked["identities"] != row["counts"]["admitted"]:
            raise IntegrityError("checker admission count differs")
        bins, attribution_error = None, None
        if cell != "soak":
            try:
                bins = observe_bins(root, summary)
            except (OSError, ValueError, KeyError, TypeError, IndexError) as error:
                bins, attribution_error = [], str(error)
    return {
        "identity": identity,
        "row": row,
        "episode": summary,
        "checker": checked,
        "bins": bins,
        "attribution_evidence_error": attribution_error,
        "telemetry_sha256": file_record(root / "pre-traffic-telemetry.json")["sha256"],
        "identity_sha256": file_record(root / "identity.json")["sha256"],
    }


def _original_digest(value):
    # j2_qualification_evidence.write uses this file encoding for the recovery receipts.
    return digest((json.dumps(value, sort_keys=True, indent=2) + "\n").encode("utf-8"))


def replay_job(inputs, campaign, jobs, job, checker, *, control_hashes, omit=None):
    result = {"cells": {}, "errors": {}, "final": {}, "integrity_stop": False, "decision": None}
    order = ORDERS[job - 1]
    grid = refs = None
    receipts = {}
    release_proof = None
    try:
        final_name = campaign.name("final", job)
        with inputs.archive(final_name) as (_, root, _):
            final = read(root / "campaign-dispatch.json")
            require(
                campaign.matches(final["binding"], job, jobs[job]["id"]),
                "final dispatch binding differs",
            )
            status, cleanup = read(root / "status.json"), read(root / "cleanup.json")
            authorization = read(root / "authorization.json")
            controls = read(root / "controls-applicability.json")
            require(
                authorization == final["authorization"]
                and controls["head"] == campaign.head
                and controls["status"] == "PASS"
                and controls["reviewer_approved_component_hashes"] == control_hashes
                and digest(canonical(controls["reviewer_approved_component_hashes"]))
                == controls["controls_sha256"],
                "pre-traffic control or authorization binding differs",
            )
            validate_ledger(
                authorization["ledger"],
                repository=campaign.repository,
                campaign=campaign.campaign,
                run=campaign.run,
                attempt=campaign.attempt,
                head=campaign.head,
                controls=controls["controls_sha256"],
            )
            release_proof = {
                "authorized_ref": authorization["revision"],
                "controls": file_record(root / "controls-applicability.json"),
                "inventory": file_record(root / "runner-inventory.json"),
            }
            peer = root / "peer-failure.json"
            result["final"] = {
                "status": status,
                "cleanup": cleanup,
                "runner": read(root / "runner-inventory.json"),
                "peer_failure": read(peer) if peer.is_file() else None,
            }
            result["integrity_stop"] = (status.get("stop") or {}).get(
                "classification"
            ) == "integrity_stop"
            receipts = read(root / "checkpoints.json")
            result["live_decision"] = read(root / "soak-decision.json")
            require(
                status["cleanup_verified"] and cleanup["status"] == "PASS", "cleanup is unverified"
            )
    except Exception as error:
        result["errors"]["final"] = str(error)
    try:
        grid_name, index_name = (campaign.name(name, job) for name in ("grid", "grid-evidence"))
        grid, grid_hash = inputs.bundle(grid_name, "grid")
        validate_grid(grid, campaign, job, jobs[job]["id"])
        check_ref(inputs.metadata[index_name], grid["evidence"], index_name)
        index, index_hash = inputs.bundle(index_name, "evidence")
        require(
            index_hash == grid["evidence"]["index_sha256"]
            and campaign.matches(index["binding"], job, jobs[job]["id"])
            and set(index["cells"]) == set(index["checkpoints"]) == set(RATES),
            "source grid index binding differs",
        )
        refs = index["checkpoints"]
        require(
            all(
                index["cells"][cell["rate"]]
                == {k: v for k, v in cell.items() if k != "evidence_sha256"}
                and digest(canonical(index["cells"][cell["rate"]])) == cell["evidence_sha256"]
                for cell in grid["cells"]
            ),
            "grid cell hash index differs",
        )
        require(
            all(receipts.get(cell) == refs[cell] for cell in order),
            "grid and teardown receipts differ",
        )
        result["grid"] = {"body": grid, "sha256": grid_hash}
        decision_name = campaign.name("decision", job)
        decision, _ = inputs.bundle(decision_name, "decision")
        require(
            campaign.matches(decision["binding"], job, jobs[job]["id"]), "decision binding differs"
        )
        check_ref(inputs.metadata[decision_name], result["live_decision"]["receipt"], decision_name)
        require(
            decision["decision"] == result["live_decision"]["decision"],
            "live decision receipt disagrees",
        )
        result["decision"] = decision["decision"]
    except Exception as error:
        result["errors"]["barrier"] = str(error)
        result["integrity_stop"] |= getattr(error, "reason", None) == "integrity_stop"
    previous = None
    for cell in (*order, "soak"):
        name = campaign.checkpoint(job, cell)
        if (
            cell == "soak"
            and name not in inputs.metadata
            and result["decision"]
            and result["decision"]["selected_rate"] == NO_RATE
        ):
            continue
        try:
            require((job, cell) != omit, "engineering fixture withheld this checkpoint")
            meta = inputs.metadata[name]
            ref = refs.get(cell) if refs is not None and cell != "soak" else receipts.get(cell)
            if ref is None:
                result["errors"][cell] = "checkpoint upload receipt missing"
            else:
                check_ref(meta, ref, name)
            with inputs.archive(name) as (_, root, freeze):
                require(
                    ref is None or freeze == ref["index_sha256"], "checkpoint freeze hash differs"
                )
                replayed = verify_checkpoint(
                    root,
                    campaign,
                    job,
                    cell,
                    previous,
                    checker,
                    inputs.budget,
                    inputs.gate.api,
                    release_proof=release_proof,
                )
                previous = replayed["identity"]
                result["cells"][cell] = replayed
            if grid and cell != "soak":
                archived = next(item for item in grid["cells"] if item["rate"] == cell)
                require(
                    digest(canonical(replayed["row"])) == archived["evidence_sha256"],
                    "replayed cell differs from common grid",
                )
        except (IntegrityError, subprocess.CalledProcessError) as error:
            result["errors"][cell] = str(error)
            result["integrity_stop"] = True
            previous = None
        except Exception as error:
            result["errors"][cell] = str(error)
            previous = None
    for cell, evidence in result["cells"].items():
        receipt = evidence["row"]["recovery"]
        if receipt:
            next_cell = result["cells"].get(receipt["replaced_with"])
            if next_cell is None and receipt["replaced_with"] == "soak":
                # The unused pre-traffic reset itself is archived with the invalid cell.
                continue
            if (
                next_cell is None
                or next_cell["identity_sha256"] != receipt["identity_sha256"]
                or next_cell["telemetry_sha256"] != receipt["telemetry_sha256"]
            ):
                result["errors"][cell] = "recovered cell does not bind its next verified reset"
    episodes = {
        k: v["episode"]
        for k, v in result["cells"].items()
        if k in RATES and k not in result["errors"] and v["row"]["outcome"] != "invalid"
    }
    result["boundary"] = registered_boundary(episodes)
    result["attribution"] = diagnostic_pair(result["boundary"], result["cells"])
    return result


def replay_authorization(jobs, states):
    reasons = []
    for job in JOBS:
        result = jobs.get(job, {})
        if (
            not result
            or not result.get("grid")
            or any(cell not in result.get("cells", {}) for cell in RATES)
        ):
            reasons.append(f"job-{job} is missing a verified complete grid")
        if result.get("integrity_stop"):
            reasons.append(f"job-{job} integrity stop")
        if any(key in {"barrier", "final", *RATES} for key in result.get("errors", {})):
            reasons.append(f"job-{job} grid or decision evidence failed replay")
        if states.get(job, {}).get("conclusion") != "success" or result.get("final", {}).get(
            "peer_failure"
        ):
            reasons.append(f"job-{job} peer failure or unverified peer state")
        if result.get("decision") is None:
            reasons.append(f"job-{job} decision missing")
    if reasons:
        return {"status": "unverifiable", "reasons": reasons}
    expected = decide(
        {j: jobs[j]["grid"]["body"] for j in JOBS},
        {j: jobs[j]["grid"]["sha256"] for j in JOBS},
        26678,
    )
    if any(jobs[j]["decision"] != expected for j in JOBS):
        return {"status": "unverifiable", "reasons": ["independent grid decisions disagree"]}
    return {
        "status": "unperformed" if expected["selected_rate"] == NO_RATE else "verified",
        "decision": expected,
    }


def claim_candidates(campaign, jobs, authorization, prerequisites):
    if engineering(campaign.campaign) or authorization["status"] != "verified" or not prerequisites:
        return []
    reports = []
    selected = authorization["decision"]["selected_rate"]
    for job in JOBS:
        outcome = jobs[job]
        soak = outcome["cells"].get("soak")
        if (
            not soak
            or "soak" in outcome["errors"]
            or soak["row"]["rate"] != selected
            or soak["row"].get("outcome") == "invalid"
            or not soak["episode"]["soak_evidence_eligible"]
        ):
            continue
        hold = soak["episode"]["cohorts"]["hold"]
        quantiles = hold["phases"]["end_to_end_quantiles"]
        quantile = "p99" if "p99_ns" in quantiles else "p95"
        latency = quantiles.get(f"{quantile}_ns")
        if latency is None:
            continue
        done = len(hold["in_window_success"])
        cpus = len(outcome["final"]["runner"]["available_cpu_ids"])
        reports.append(
            {
                "job": job,
                "hold_successes": done,
                "hold_seconds": 1800,
                "latency_ns": latency,
                "quantile": quantile,
                "sentence": (
                    f"Rebuilt registered profile. Observed {done / 1800:.3f} completed workflows/s "
                    f"at {quantile} {latency / 1e9:.3f} s for 30 min "
                    "for the registered short workload on single-node kind with PostgreSQL, Kafka "
                    "and workers colocated on one shared "
                    f"GitHub-hosted runner ({cpus} vCPU), admission disabled."
                ),
            }
        )
    for report in reports:
        report["qualification"] = f"{len(reports)} of 3 jobs qualified"
    return reports


def qualified_summary(reports):
    if len(reports) != len(JOBS):
        return None
    throughput = [row["hold_successes"] / row["hold_seconds"] for row in reports]
    latencies = {}
    for row in reports:
        latencies.setdefault(row["quantile"], []).append(row["latency_ns"])
    return {
        "pairs": [
            {
                "job": row["job"],
                "success_per_second": rate,
                "latency_ns": row["latency_ns"],
                "quantile": row["quantile"],
            }
            for row, rate in zip(reports, throughput, strict=True)
        ],
        "success_per_second_range": [min(throughput), max(throughput)],
        "latency_ns_ranges": {
            quantile: [min(values), max(values)]
            for quantile, values in sorted(latencies.items())
            if len(values) > 1
        },
    }


def archived_prerequisites(inputs, checker, repeats):
    from j2_controls import replay_archive

    verified = []
    for repeat in range(1, repeats + 1):
        with inputs.archive(QUALIFICATION_NAME) as (_, root, freeze):
            require(root is not None, "qualification freeze missing")
            with inputs.budget.stage("checker") as left:
                result = checker_replay(root, checker, min(240, left()), qualification=True)
        verified.append({"fixture_repeat": repeat, "freeze_sha256": freeze, **result})
    original, receipt = CONTROL_ARTIFACTS
    with inputs.archive(receipt) as (path, _, _), zipfile.ZipFile(path) as bundle:
        expected = bundle.read("offline-replay.json")
        control_source = decode(bundle.read("artifact.json"))
        require(
            all(
                control_source[key] == inputs.metadata[original][key]
                for key in ("id", "name", "digest", "workflow_run")
            ),
            "control verification receipt does not bind its source",
        )
    with (
        inputs.archive(original) as (path, _, _),
        inputs.budget.stage("reduce"),
        tempfile.TemporaryDirectory(dir=inputs.budget.output.parent) as temp,
    ):
        report = Path(temp) / "controls.json"
        replay_archive(path, report)
        require(
            canonical(read(report)) == canonical(decode(expected)),
            "controls replay differs from archived receipt",
        )
    return {"qualification": verified, "controls": "PASS"}


def verify_dispatch(inputs, campaign, jobs, authorization):
    name = f"j2-measure-dispatch-{campaign.run}-{campaign.attempt}"
    with inputs.archive(name) as (path, _, _), zipfile.ZipFile(path) as bundle:
        dispatch = decode(bundle.read("campaign-dispatch-manifest.json"))
    require(
        (
            dispatch["campaign"],
            dispatch["head"],
            dispatch["run_id"],
            dispatch["attempt"],
            dispatch["scope"],
        )
        == (campaign.campaign, campaign.head, campaign.run, campaign.attempt, campaign.scope),
        "dispatch manifest belongs to a different campaign or attempt",
    )
    for job, row in dispatch["jobs"].items():
        final = inputs.metadata.get(campaign.name("final", int(job)))
        if final is not None:
            require(
                row["artifact_id"] == final["id"] and row["digest"] == final["digest"],
                "dispatch manifest refers to a different final archive",
            )
    if authorization["status"] in {"verified", "unperformed"}:
        require(
            not dispatch["errors"]
            and set(map(int, dispatch["jobs"])) == set(jobs)
            and dispatch["live_grid_agreement"]["status"] == "verified"
            and dispatch["live_grid_agreement"]["decision"] == authorization["decision"],
            "dispatch manifest disagrees with the independent grid decision",
        )
    return dispatch["disposition"]


def source_inputs(args, budget):
    run = args.source_run or int(os.environ["GITHUB_RUN_ID"])
    repository = os.environ["GITHUB_REPOSITORY"]
    current = run == int(os.environ["GITHUB_RUN_ID"])
    require(
        current or engineering(args.campaign_id) or args.case == "release",
        "an Actions replay of a different run must use an engineering campaign",
    )
    require(
        args.case != "partial" or engineering(args.campaign_id),
        "partial replay fixture is engineering only",
    )
    if args.case == "release":
        index = load_release_index(
            repository,
            args.campaign_id,
            run,
            args.release_tag,
            args.release_index,
            args.release_index_sha256,
        )
        campaign = source_campaign(repository, run, args.campaign_id, index["head"])
        gate = client(campaign, args.output, budget.cutoff)
        jobs = {int(job): row for job, row in index["jobs"].items()}
        metadata = index["artifacts"]
        for name, (job, kind) in source_names(campaign).items():
            if name in metadata:
                meta = metadata[name]
                require(
                    meta["source_run"] == run
                    and meta["source_attempt"] == 1
                    and meta["workflow_run"]["id"] == run
                    and meta["workflow_run"]["head_sha"] == campaign.head
                    and meta["job"] == job
                    and meta["kind"] == kind
                    and meta["name"] == name,
                    "release index has a foreign source archive",
                )
        for name in prerequisite_archives():
            meta = metadata[name]
            prereq_run, kind = verify_prerequisite(name, meta)
            require(
                (meta["source_run"], meta["source_attempt"], meta["job"], meta["kind"])
                == (prereq_run, 1, "shared", kind),
                "release index has a foreign prerequisite archive",
            )
        return ReplayInputs(gate, metadata, budget, release=index), campaign, jobs
    provisional = Campaign(repository, run, 1, "", args.campaign_id, {}, 26678, "")
    gate = client(provisional, args.output, budget.cutoff)
    info = budget.retry(lambda: run_metadata(gate.api, run))
    require(current or info["status"] == "completed", "source Actions attempt still active")
    campaign = source_campaign(repository, run, args.campaign_id, info["head_sha"])
    gate = Barrier(campaign, gate.api)
    jobs = budget.retry(lambda: gate.api.jobs(campaign, allow_failed=True))
    metadata = metadata_source(gate)
    return ReplayInputs(gate, metadata, budget), campaign, jobs


def run_report(args, budget):
    result = {
        "status": "partial",
        "case": args.case,
        "jobs": {},
        "errors": {},
        "authorization": {"status": "unverifiable"},
        "draft_sentences": [],
    }
    inputs = None
    try:
        with budget.stage("bootstrap"):
            inputs, campaign, states = source_inputs(args, budget)
            control_hashes = control_identity(head=campaign.head)
            subprocess.run(["docker", "pull", WORKER_BASE], timeout=120, check=True)
        result.update(
            campaign=campaign.campaign,
            source_run=campaign.run,
            source_head=campaign.head,
            attempt=campaign.attempt,
            scope=campaign.scope,
            repository=campaign.repository,
            report_head=os.environ["GITHUB_SHA"],
            pins=campaign.pins,
            transport="release" if inputs.release else "actions",
        )
        omitted = (1, "C") if args.case == "partial" else None
        for job in JOBS:
            result["jobs"][job] = replay_job(
                inputs,
                campaign,
                states,
                job,
                args.checker,
                control_hashes=control_hashes,
                omit=omitted,
            )
            write(args.output / f"job-{job}.json", result["jobs"][job])
        result["authorization"] = replay_authorization(result["jobs"], states)
        try:
            with budget.stage("reduce"):
                result["source_disposition"] = verify_dispatch(
                    inputs, campaign, states, result["authorization"]
                )
        except (OSError, ValueError, KeyError, zipfile.BadZipFile) as error:
            result["errors"]["dispatch"] = str(error)
            result["authorization"] = {
                "status": "unverifiable",
                "reasons": ["dispatch manifest missing or inconsistent"],
            }
        repeats = 3 if engineering(campaign.campaign) and args.case in {"full", "release"} else 1
        if args.case == "partial":
            result["intentional_omission"] = {"job": 1, "cell": "C"}
        prerequisite = archived_prerequisites(inputs, args.checker, repeats)
        result["prerequisites"] = prerequisite
        if inputs.release:
            for name, meta in inputs.metadata.items():
                if meta["kind"] in {"replay", "replay-receipt"}:
                    with inputs.archive(name) as (path, _, _):
                        verify_report_source(path, meta, campaign, inputs.metadata)
        from j2_attribution_fixture import fixture_report

        if engineering(campaign.campaign):
            result["attribution_fixture"] = fixture_report(args.output / "attribution-fixture")
        boundaries = [job["boundary"]["saturation"] for job in result["jobs"].values()]
        components = [job["attribution"]["component"] for job in result["jobs"].values()]
        agreed = len(set(boundaries)) == 1 and boundaries[0] is not None
        result["shared_boundary"] = boundaries[0] if agreed else "unresolved"
        result["shared_attribution"] = (
            components[0] if agreed and len(set(components)) == 1 else "unresolved"
        )
        result["draft_sentences"] = claim_candidates(
            campaign, result["jobs"], result["authorization"], True
        )
        result["three_repeat_summary"] = qualified_summary(result["draft_sentences"])
        if result["authorization"]["status"] != "unverifiable":
            result["status"] = (
                "partial" if any(job["errors"] for job in result["jobs"].values()) else "complete"
            )
        result["release_verified"] = inputs.release is not None and result["status"] == "complete"
        result["archive_coverage"] = {
            "grid": sum(len(set(job["cells"]) & set(RATES)) for job in result["jobs"].values()),
            "continuous": sum("soak" in job["cells"] for job in result["jobs"].values()),
            "full_volume_qualification_replays": repeats,
        }
    except Exception as error:
        result["errors"]["report"] = str(error)
        result["draft_sentences"] = []
    finally:
        if inputs is not None:
            write(args.output / "archive-index.json", inputs.index)
        slim = {key: value for key, value in result.items() if key != "jobs"}
        slim["jobs"] = {
            job: {
                "cells": sorted(row["cells"]),
                "errors": row["errors"],
                "boundary": row["boundary"],
                "attribution": row["attribution"],
                "integrity_stop": row["integrity_stop"],
            }
            for job, row in result["jobs"].items()
        }
        write(args.output / "campaign-replay.json", slim)
        write(args.output / "reporter-budget.json", budget.record())
        seal(args.output)
    if args.case == "partial":
        fixture_errors = {
            (job, cell): error
            for job, report in result["jobs"].items()
            for cell, error in report["errors"].items()
        }
        return int(
            bool(result["errors"])
            or fixture_errors != {(1, "C"): "engineering fixture withheld this checkpoint"}
            or result["authorization"]["status"] != "unverifiable"
            or result.get("archive_coverage", {}).get("grid") != 14
            or result.get("archive_coverage", {}).get("continuous") != 3
        )
    return int(result["status"] != "complete")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("checker", type=Path)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--source-run", type=int)
    parser.add_argument("--case", choices=("full", "partial", "release"), default="full")
    parser.add_argument("--release-tag", default="")
    parser.add_argument("--release-index", default="")
    parser.add_argument("--release-index-sha256", default="")
    args = parser.parse_args()
    hosted_dispatch()
    args.output.mkdir(parents=True, exist_ok=True)
    elapsed = max(0, time.time() - int(os.environ["J2_REPLAY_START"]))
    return run_report(args, ReplayBudget(args.output, elapsed))


if __name__ == "__main__":
    raise SystemExit(main())
