"""B.6 dispatch admission; all history/proof gates precede owned live resources."""

from __future__ import annotations

import json
import os
import secrets
import shutil
import time
from pathlib import Path
from typing import Any

from dur053_k3 import LOCK, ROOT, job_identity_and_history, save
from dur053_k3_policy import (
    artifact_hashes,
    committed_ledger,
    fresh_output,
    require,
    scratch_ownership,
)
from dur053_payload import materialize
from dur053_pilot import CEILING, carry_over, committed_bytes, read_artifact
from dur053_pilot_evidence import ci_proof, pre_start_refusal
from dur053_sustained import (
    evidence_root,
    fallback_required,
    harness_capacity_failure,
    job_spec,
    registration,
    reservation,
    stationarity,
    version_rate,
)
from dur053_sustained_driver import SustainedDriver


def dependency_plan(repository: Path, name: str) -> tuple[list[int], dict[str, Any]]:
    arm, version, indices = job_spec(name)
    reg = registration(repository, arm)  # String equality guard before admission.
    if version == 3:
        fallback_required(repository, arm)
    if arm == "long":
        from dur053_payload import materialize as materialize_long
        from dur053_pilot import EVIDENCE

        # B.6.1: offline pass uses throughput/backlog only; 4(a)/(b) disclose.
        gates = []
        for path in (repository / EVIDENCE).glob("*/*/manifest.json"):
            with materialize_long(path.parent, repository) as root:
                manifest = json.loads((root / "manifest.json").read_bytes())
                gates.extend(
                    stationarity(root / f"long-{i}", offline=True)
                    for i in (1, 2, 3)
                    if f"long-{i}" in manifest["completed"]
                )
        require(len(gates) == 3 and any(not g["pass"] for g in gates), "b6-long-trigger")
    available = [
        (p.parent, read_artifact(p.parent, repository))
        for p in (repository / evidence_root(name)).glob("*/*/manifest.json")
    ]
    if "-c" in name:
        prior = [m for _, m in available if m.get("carry_over", {}).get("job") == name]
        require(
            len(prior) == 1
            and prior[0]["status"] == "CARRY_OVER"
            and prior[0]["cleanup"]["status"] == "PASS"
            and prior[0]["carry_over"]["reason"] == "pilot-job-fit"
            and prior[0]["carry_over"]["started"] is False,
            "b6-carry-predecessor",
        )
        cells = prior[0]["carry_over"]["cells"]
        pending = [i for a, i in cells if a == arm]
        require(
            bool(pending)
            and pending == indices[-len(pending) :]
            and cells == [[arm, i] for i in pending],
            "b6-carry-order",
        )
        indices = pending
    previous = [(p, m) for p, m in available if m["job"] == name]
    capacity, refusals = 0, 0
    for path, _ in previous:
        with materialize(path, repository) as root:
            manifest = json.loads((root / "manifest.json").read_bytes())
            if pre_start_refusal(root, manifest):
                refusals += 1
            else:
                require(harness_capacity_failure(root, manifest), "pilot-no-replay-job")
                capacity += 1
    require(capacity <= 1, "b62-capacity-replay-limit")
    require(refusals < 3, "pilot-pre-start-replay-limit")
    return indices, reg


def admit(
    q: float, repeats: int, *, cpu: bool = False, elapsed: float = 0, setup: bool = True
) -> int:
    needed = reservation(q, cpu=cpu, repeats=repeats) - (0 if setup else 300)
    require(elapsed + needed <= CEILING, "pilot-job-fit")
    return needed


def run(output: Path, scratch_root: Path, name: str) -> int:
    from dur053_campaign import LEDGER, campaign_gate, live_charge
    from dur053_schedule import plan_b6

    fresh_output(output)
    scratch_root.mkdir(parents=True, exist_ok=True)
    scratch = scratch_root / ("kind-" + secrets.token_hex(8))
    scratch.mkdir()
    d = SustainedDriver(output, scratch)
    arm, version, indices = job_spec(name)
    manifest: dict[str, Any] = {
        "schema": "dur053.k3b.v1",
        "job": name,
        "version": version,
        "status": "FAIL",
        "completed": [],
        "planned": [[arm, i] for i in indices],
        "dependencies": {},
        "run_id": os.getenv("GITHUB_RUN_ID"),
        "job_id": None,
        "registered_live_seconds": CEILING,
        "live_ceiling_seconds": CEILING,
        "budget_scope": "registered-campaign",
        "measurement_claim": True,
        "charged_seconds": CEILING,
        "limitations": ["single GitHub-hosted shared runner per job", "no capacity or HA claim"],
        "runner_identity": {
            k: os.getenv(k)
            for k in ("RUNNER_NAME", "RUNNER_OS", "RUNNER_ARCH", "GITHUB_REPOSITORY")
        },
    }
    save(output / "manifest.json", manifest)
    started = False
    try:
        require(bool(os.getenv("GITHUB_ACTIONS")), "pilot-hosted-only")
        for key, expression in (("merge_ref_sha", "HEAD"), ("tree_sha", "HEAD^{tree}")):
            manifest[key] = d.command(["git", "rev-parse", expression], record=False).stdout.strip()
        job, _, head = job_identity_and_history()
        manifest.update(job_id=job, head_sha=head)
        indices, reg = dependency_plan(ROOT, name)
        manifest.update(planned=[[arm, i] for i in indices], registration=reg)
        manifest["remaining_after_max_job_reservation"] = campaign_gate()
        committed_bytes(ROOT, LEDGER)
        for entry in committed_ledger(LEDGER)["entries"]:
            committed_bytes(ROOT, ROOT / entry["artifact"])
        manifest["ci_proof"] = ci_proof(ROOT)
        q = version_rate(reg, version)
        manifest["full_admission_seconds"] = admit(q, len(indices), cpu=arm == "cpu")
        manifest["schedule_recheck"] = plan_b6(ROOT)
        save(output / "manifest.json", manifest)
        d.install()
        d.live_start, started = time.monotonic(), True
        d.build()
        for addon, lock in LOCK["addons"].items():
            if lock["installed_in_k3a"]:
                d.prepare_addon(addon)
        d.preload_addons()
        addresses = d.cluster()
        if arm == "cpu":
            admit(q, len(indices), cpu=True, setup=False, elapsed=time.monotonic() - d.live_start)
            d.drift_check(addresses, version)
        for position, index in enumerate(indices):
            try:
                admit(q, 1, setup=False, elapsed=time.monotonic() - d.live_start)
            except ValueError as error:
                if str(error) == "K3 guard: pilot-job-fit" and position:
                    manifest["carry_over"] = carry_over(
                        name, [(arm, i) for i in indices[position:]], "pilot-job-fit", False
                    )
                    manifest["status"] = "CARRY_OVER"
                    break
                raise
            d.sustained(arm, index, addresses, reg, version)
            manifest["completed"].append(f"{arm}-{index}")
            save(output / "manifest.json", manifest)
        else:
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
            except Exception as diagnostic:
                manifest["diagnostic_error"] = str(diagnostic)
    finally:
        manifest["unperformed"] = [
            {
                "arm": arm,
                "index": i,
                "started": (output / f"{arm}-{i}/record.json").exists(),
                "reason": manifest.get("error", manifest.get("carry_over", {}).get("reason")),
            }
            for i in indices
            if f"{arm}-{i}" not in manifest["completed"]
        ]
        d.output = output
        try:
            manifest["cleanup"] = d.cleanup()
        except Exception as error:
            manifest.update(status="FAIL", cleanup={"status": "FAIL", "error": str(error)})
        manifest["elapsed_live_seconds"] = manifest["cleanup"].get("elapsed_live_seconds")
        manifest["charged_seconds"] = live_charge(manifest["cleanup"], started, CEILING)
        manifest["artifact_sha256"] = artifact_hashes(output)
        save(output / "manifest.json", manifest)
        scratch_ownership(scratch, scratch_root)
        shutil.rmtree(scratch)
    return 0 if manifest["status"] in {"PASS", "CARRY_OVER"} else 1
