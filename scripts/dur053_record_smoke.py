#!/usr/bin/env python3
"""Verify downloaded smoke bytes, copy immutable evidence and append its charge."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path

from dur053_k3_policy import ROOT, artifact_hashes, ledger_check, require


def record(source: Path, repository: Path = ROOT) -> Path:
    manifest_path = source / "manifest.json"
    raw = manifest_path.read_bytes()
    manifest = json.loads(raw)
    require(
        manifest["schema"] == "dur053.k3a.v1" and manifest["scenario"] == "smoke",
        "intake-smoke-schema",
    )
    require(manifest["artifact_sha256"] == artifact_hashes(source), "intake-artifact-hashes")
    require(all(not p.is_symlink() for p in source.rglob("*")), "intake-no-symlinks")
    run, job = str(manifest["run_id"]), str(manifest["job_id"])
    require(run.isdigit() and job.isdigit(), "intake-run-identity")
    ledger_path = repository / "experiments/portfolio/k8s-recovery/live-time-ledger.json"
    ledger = json.loads(ledger_path.read_text())
    ledger_check(ledger, 1, set())
    require(job not in {e["job_id"] for e in ledger["entries"]}, "intake-duplicate-job")
    target = repository / "experiments/portfolio/k8s-recovery/k3-smoke-v1" / run
    charge = manifest.get("charged_seconds", manifest["registered_live_seconds"])
    require(type(charge) is int and 0 < charge <= 18900, "intake-charge")
    require(
        sum(e["charged_seconds"] for e in ledger["entries"]) + charge <= 259200,
        "intake-total-budget",
    )
    row = {
        key: manifest[key]
        for key in ("run_id", "job_id", "head_sha", "merge_ref_sha", "tree_sha", "status")
    }
    row.update(
        charged_seconds=charge,
        artifact_sha256=hashlib.sha256(raw).hexdigest(),
        artifact=(target / "manifest.json").relative_to(repository).as_posix(),
    )
    # Both intended outputs are committed together by Claude. Interrupted writes
    # remain visible and must be reviewed; no automatic overwrite/replacement.
    shutil.copytree(source, target)
    ledger["entries"].append(row)
    ledger_path.write_text(json.dumps(ledger, indent=2) + "\n", encoding="utf-8", newline="\n")
    return target


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--downloaded", type=Path, required=True)
    args = parser.parse_args()
    print(record(args.downloaded.resolve()))


if __name__ == "__main__":
    main()
