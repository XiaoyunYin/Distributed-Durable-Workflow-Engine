#!/usr/bin/env python3
"""Dispatch contract only. K3a cannot start any pilot or measurement."""

import argparse
import json
import os
from pathlib import Path

from dur053_k3_policy import committed_ledger, fresh_output, ledger_check

parser = argparse.ArgumentParser()
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
fresh_output(args.output)
manifest = {
    "status": "FAIL",
    "scenario": os.getenv("DUR053_SCENARIO"),
    "arm": os.getenv("DUR053_ARM"),
    "run_id": os.getenv("GITHUB_RUN_ID"),
    "live_seconds": 0,
}
try:
    ledger = committed_ledger(
        Path(__file__).resolve().parents[1]
        / "experiments/portfolio/k8s-recovery/live-time-ledger.json"
    )
    manifest["remaining_after_max_job_reservation"] = ledger_check(ledger, 18900, set())
    raise ValueError("K3b: campaign scenario not implemented; no live resources started")
except ValueError as error:
    manifest["error"] = str(error)
finally:
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
raise SystemExit(1)
