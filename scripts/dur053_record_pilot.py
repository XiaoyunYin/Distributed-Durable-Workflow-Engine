#!/usr/bin/env python3
"""Claude's offline, byte-preserving campaign intake; never dispatches a job."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

from dur053_campaign import live_charge
from dur053_k3 import save
from dur053_k3_policy import ROOT, artifact_hashes, ledger_check, require
from dur053_payload import pack, scratch
from dur053_pilot import CEILING, EVIDENCE, job_base, read_artifact


def record(source: Path, repository: Path = ROOT) -> Path:
    manifest = read_artifact(source)
    raw = (source / "manifest.json").read_bytes()
    require(manifest["status"] in {"PASS", "FAIL", "CARRY_OVER"}, "pilot-intake-status")
    require(
        manifest["registered_live_seconds"] == CEILING
        and manifest["charged_seconds"]
        == live_charge(
            manifest["cleanup"], not manifest["cleanup"].get("no_resources_started", False), CEILING
        ),
        "pilot-intake-charge",
    )
    require(
        all(
            len(manifest[k]) == 40 and all(c in "0123456789abcdef" for c in manifest[k])
            for k in ("head_sha", "merge_ref_sha", "tree_sha")
        ),
        "pilot-intake-provenance",
    )
    ledger_path = repository / "experiments/portfolio/k8s-recovery/live-time-ledger.json"
    ledger = json.loads(ledger_path.read_bytes())
    ledger_check(ledger, manifest["charged_seconds"], set())
    require(
        manifest["job_id"] not in {str(x["job_id"]) for x in ledger["entries"]},
        "pilot-intake-duplicate-job",
    )
    target = repository / EVIDENCE / manifest["job"] / manifest["run_id"]
    require(not target.exists(), "pilot-intake-existing")
    row = {
        k: manifest[k]
        for k in (
            "run_id",
            "job_id",
            "head_sha",
            "merge_ref_sha",
            "tree_sha",
            "status",
            "charged_seconds",
        )
    }
    row.update(
        category=manifest["job"],
        artifact=(target / "manifest.json").relative_to(repository).as_posix(),
        artifact_sha256=hashlib.sha256(raw).hexdigest(),
    )
    with scratch(repository) as staging:
        pack(source, staging)
        row["payload_index_sha256"] = hashlib.sha256(
            (staging / "payload-index.json").read_bytes()
        ).hexdigest()
        shutil.copytree(staging, target)
    ledger["entries"].append(row)
    save(ledger_path, ledger)
    return target


def interrupted(source: Path, repository: Path, name: str) -> Path:
    """Retain raw Actions provenance and partial bytes; charge full reservation.

    source contains actions-run.json and actions-job.json downloaded by Claude.
    No partial manifest can supply a refund or satisfy pilot dependencies.
    """
    with scratch(repository) as staging:
        return _interrupted(source, repository, name, staging)


def _interrupted(source: Path, repository: Path, name: str, staging: Path) -> Path:
    job_base(name)
    require(
        not source.is_symlink() and all(not p.is_symlink() for p in source.rglob("*")),
        "pilot-interrupted-symlinks",
    )
    run = json.loads((source / "actions-run.json").read_bytes())
    job = json.loads((source / "actions-job.json").read_bytes())
    require(
        job["run_id"] == run["id"]
        and job["name"] == "DUR-053 campaign"
        and run["event"] == "workflow_dispatch"
        and run["run_attempt"] == 1
        and job["conclusion"] in {"cancelled", "timed_out", "failure"},
        "pilot-interrupted-actions",
    )
    target = repository / EVIDENCE / name / str(run["id"])
    require(not target.exists(), "pilot-interrupted-existing")
    # The original partial artifact (including any manifest) remains byte-identical.
    raw = staging / "raw"
    shutil.copytree(source, raw / "downloaded")
    manifest: dict[str, Any] = {
        "schema": "dur053.k3b.v1",
        "job": name,
        "status": "FAIL",
        "run_id": str(run["id"]),
        "job_id": str(job["id"]),
        "head_sha": run["head_sha"],
        "merge_ref_sha": run["head_sha"],
        "tree_sha": json.loads((source / "actions-commit.json").read_bytes())["commit"]["tree"][
            "sha"
        ],
        "budget_scope": "registered-campaign",
        "measurement_claim": True,
        "registered_live_seconds": CEILING,
        "charged_seconds": CEILING,
        "completed": [],
        "cleanup": {"status": "UNTRUSTWORTHY"},
        "error": "termination: full reservation charged",
        "artifact_sha256": artifact_hashes(raw),
    }
    commit = json.loads((source / "actions-commit.json").read_bytes())
    require(commit["sha"] == run["head_sha"], "pilot-interrupted-commit")
    save(raw / "manifest.json", manifest)
    ledger_path = repository / "experiments/portfolio/k8s-recovery/live-time-ledger.json"
    ledger = json.loads(ledger_path.read_bytes())
    ledger_check(ledger, CEILING, set())
    require(
        str(job["id"]) not in {str(e["job_id"]) for e in ledger["entries"]},
        "pilot-intake-duplicate-job",
    )
    row = {
        k: manifest[k]
        for k in (
            "run_id",
            "job_id",
            "head_sha",
            "merge_ref_sha",
            "tree_sha",
            "status",
            "charged_seconds",
        )
    }
    row.update(
        category=name,
        artifact=(target / "manifest.json").relative_to(repository).as_posix(),
        artifact_sha256=hashlib.sha256((raw / "manifest.json").read_bytes()).hexdigest(),
    )
    archives = staging / "archives"
    archives.mkdir()
    pack(raw, archives)
    row["payload_index_sha256"] = hashlib.sha256(
        (archives / "payload-index.json").read_bytes()
    ).hexdigest()
    shutil.copytree(archives, target)
    ledger["entries"].append(row)
    save(ledger_path, ledger)
    return target


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--downloaded", type=Path, required=True)
    parser.add_argument(
        "--interrupted-job", help="retain untrustworthy termination at full reservation"
    )
    args = parser.parse_args()
    print(
        interrupted(args.downloaded, ROOT, args.interrupted_job)
        if args.interrupted_job
        else record(args.downloaded)
    )


if __name__ == "__main__":
    main()
