#!/usr/bin/env python3
"""K3b dispatch: fail-closed before resources, retain every failed attempt."""

from __future__ import annotations

import argparse
import os
import secrets
import shutil
import time
from pathlib import Path
from typing import Any

from dur053_k3 import ROOT, github, job_identity_and_history, save
from dur053_k3_policy import (
    artifact_hashes,
    committed_ledger,
    fresh_output,
    ledger_check,
    require,
    scratch_ownership,
)
from dur053_pilot import CEILING, JOBS, admit, carry_over, committed_bytes, job_base
from dur053_pilot_driver import PilotDriver
from dur053_pilot_evidence import ci_proof, dependency_plan, same_job_initial, verify_calibration

LEDGER = (
    Path(__file__).resolve().parents[1] / "experiments/portfolio/k8s-recovery/live-time-ledger.json"
)


def campaign_gate() -> int:
    require(os.getenv("GITHUB_EVENT_NAME") == "workflow_dispatch", "campaign dispatch required")
    _, prior, _ = job_identity_and_history()
    return ledger_check(committed_ledger(LEDGER), 18900, prior)


def live_charge(cleanup: dict[str, Any], started: bool, reservation: int) -> int:
    if not started:
        return 1
    elapsed = cleanup.get("elapsed_live_seconds")
    if cleanup.get("status") == "PASS" and type(elapsed) is int and 0 < elapsed <= reservation:
        return elapsed
    return reservation


def run(output: Path, scratch_root: Path, name: str, *, engineering: bool = False) -> int:
    from dur053_sustained import JOBS as B6_JOBS

    if name.split("-c")[0] in B6_JOBS:
        from dur053_sustained_campaign import run as run_sustained

        return run_sustained(output, scratch_root, name)
    fresh_output(output)
    scratch_root.mkdir(parents=True, exist_ok=True)
    scratch = scratch_root / ("kind-" + secrets.token_hex(8))
    scratch.mkdir()
    if engineering:
        from dur053_sustained_driver import SustainedDriver

        d = SustainedDriver(output, scratch, engineering=True)
    else:
        d = PilotDriver(output, scratch, engineering=False)
    manifest: dict[str, Any] = {
        "schema": "dur053.k3b.v1",
        "job": name,
        "status": "FAIL",
        "completed": [],
        "dependencies": {},
        "run_id": os.getenv("GITHUB_RUN_ID"),
        "job_id": None,
        "registered_live_seconds": 0 if engineering else CEILING,
        "live_ceiling_seconds": 3600 if engineering else CEILING,
        "budget_scope": "engineering-validation" if engineering else "registered-campaign",
        "measurement_claim": not engineering,
        "charged_seconds": 0 if engineering else CEILING,
        "limitations": ["single GitHub-hosted shared runner per job", "no capacity or HA claim"],
        "runner_identity": {
            k: os.getenv(k)
            for k in ("RUNNER_NAME", "RUNNER_OS", "RUNNER_ARCH", "GITHUB_REPOSITORY")
        },
    }
    save(output / "manifest.json", manifest)
    started = False
    pending: list[tuple[str, int]] = []
    try:
        require(bool(os.getenv("GITHUB_ACTIONS")), "pilot-hosted-only")
        manifest["merge_ref_sha"] = d.command(
            ["git", "rev-parse", "HEAD"], record=False
        ).stdout.strip()
        manifest["tree_sha"] = d.command(
            ["git", "rev-parse", "HEAD^{tree}"], record=False
        ).stdout.strip()
        if engineering:
            require(
                name == "pilot-smoke"
                and (
                    os.getenv("GITHUB_EVENT_NAME") == "pull_request"
                    or (
                        os.getenv("GITHUB_EVENT_NAME") == "push"
                        and os.getenv("GITHUB_REF") == "refs/heads/main"
                    )
                ),
                "pilot-engineering-ci-only",
            )
            jobs = github(
                f"repos/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{manifest['run_id']}/jobs?per_page=100"
            )
            mine = [j for j in jobs["jobs"] if j["name"] == "DUR-053 pilot smoke"]
            require(len(mine) == 1, "pilot-engineering-identity")
            manifest.update(job_id=str(mine[0]["id"]), head_sha=os.environ["DUR053_HEAD_SHA"])
            pending = [(a, i) for a in ("short", "cpu") for i in range(2)]
            initials: dict[str, Any] = {}
        else:
            pending = list(JOBS[job_base(name)])
            job, _, head = job_identity_and_history()
            manifest.update(job_id=job, head_sha=head)
            manifest["remaining_after_max_job_reservation"] = campaign_gate()
            committed_bytes(ROOT, LEDGER)
            for entry in committed_ledger(LEDGER)["entries"]:
                committed_bytes(ROOT, ROOT / entry["artifact"])
            manifest["ci_proof"] = ci_proof(ROOT)
            pending, initials, predecessor = dependency_plan(ROOT, name)
            manifest["dependencies"] = initials.copy()
            manifest["predecessor"] = predecessor["job_id"] if predecessor else None
            need_calibration = job_base(name) == "k3-pilot-1b" and predecessor is None
            manifest["full_admission_seconds"] = admit(
                pending, initials, calibration=need_calibration
            )
        manifest["planned"] = [list(x) for x in pending]
        save(output / "manifest.json", manifest)
        d.install()
        # The build registry is an owned live resource, so start accounting here.
        d.live_start, started = time.monotonic(), True
        d.build()
        from dur053_k3 import LOCK

        for addon, lock in LOCK["addons"].items():
            if lock["installed_in_k3a"]:
                d.prepare_addon(addon)
        d.preload_addons()
        addresses = d.cluster()
        units = 10000
        if engineering or (job_base(name) == "k3-pilot-1b" and "-c" not in name):
            if not engineering:
                admit(
                    pending,
                    initials,
                    calibration=True,
                    setup=False,
                    elapsed=time.monotonic() - d.live_start,
                )
            calibration = d.calibration(addresses)
            units = calibration["units"]
            if not engineering:
                verify_calibration(output)
        elif any(a == "cpu" for a, _ in pending):
            cpu_root = Path(manifest["dependencies"]["cpu"]["artifact"])
            units = verify_calibration(cpu_root)["units"]
        for position, (arm, index) in enumerate(pending):
            if index:
                if (output / f"{arm}-0").is_dir():
                    initials[arm] = same_job_initial(output, arm)
                else:
                    from dur053_pilot import initial_dependency

                    initials[arm] = initial_dependency(Path(initials[arm]["artifact"]), arm, ROOT)
            if not engineering:
                try:
                    admit(
                        [(arm, index)],
                        initials,
                        setup=False,
                        elapsed=time.monotonic() - d.live_start,
                    )
                except ValueError as error:
                    if str(error) == "K3 guard: pilot-job-fit" and position:
                        manifest["carry_over"] = carry_over(
                            name, pending[position:], "pilot-job-fit", False
                        )
                        manifest["status"] = "CARRY_OVER"
                        break
                    raise
            if arm == "cpu" and not engineering and index == 0:
                verify_calibration(output)
            d.pilot(arm, index, addresses, initials.get(arm), units)
            manifest["completed"].append(f"{arm}-{index}")
            save(output / "manifest.json", manifest)
        else:
            if engineering:
                from dur053_sustained import precision, require_stationary
                from dur053_sustained_driver import SustainedDriver

                d.root_output = output / "v2-smoke"
                d.root_output.mkdir()
                try:
                    result = SustainedDriver.sustained(
                        d, "short", 1, addresses, precision(2.0, {"engineering": True}), 2
                    )
                    manifest["v2_engineering_gate"] = result["stationarity_gate"]
                    require_stationary(result["stationarity_gate"])
                    manifest["completed"].append("v2-smoke/short-1")
                finally:
                    d.root_output, d.output = output, output
            manifest["status"] = "PASS"
    except Exception as error:
        manifest["error"] = (
            str(error).replace(d.password, "[REDACTED]").replace(d.demand_token, "[REDACTED]")
        )
        d.output = output
        if d.live_start is not None:
            try:
                d.diagnostics()
                d.failure_export("dur053-kind")
            except Exception as diagnostics_error:
                manifest["diagnostic_error"] = str(diagnostics_error)
    finally:
        manifest["planned"] = [list(cell) for cell in pending]
        manifest["unperformed"] = [
            {
                "arm": arm,
                "index": index,
                "started": False,
                "reason": manifest.get("error", manifest.get("carry_over", {}).get("reason")),
            }
            for arm, index in pending
            if not (output / f"{arm}-{index}/record.json").exists()
        ]
        d.output = output
        try:
            manifest["cleanup"] = d.cleanup()
        except Exception as error:
            manifest.update(status="FAIL", cleanup={"status": "FAIL", "error": str(error)})
        manifest["elapsed_live_seconds"] = manifest["cleanup"].get("elapsed_live_seconds")
        manifest["charged_seconds"] = (
            0 if engineering else live_charge(manifest["cleanup"], started, CEILING)
        )
        manifest["artifact_sha256"] = artifact_hashes(output)
        save(output / "manifest.json", manifest)
        scratch_ownership(scratch, scratch_root)
        shutil.rmtree(scratch)
    return 0 if manifest["status"] in {"PASS", "CARRY_OVER"} else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scratch-root", type=Path, required=True)
    parser.add_argument("--job", required=True)
    args = parser.parse_args()
    return run(
        args.output.resolve(),
        args.scratch_root.resolve(),
        args.job,
        engineering=args.job == "pilot-smoke",
    )


if __name__ == "__main__":
    raise SystemExit(main())
