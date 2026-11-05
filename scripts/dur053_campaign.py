#!/usr/bin/env python3
"""Dispatch contract only. K3a cannot start any pilot or measurement."""

import argparse
import json
import os
from pathlib import Path

from dur053_k3 import job_identity_and_history
from dur053_k3_policy import committed_ledger, fresh_output, ledger_check

LEDGER = (
    Path(__file__).resolve().parents[1] / "experiments/portfolio/k8s-recovery/live-time-ledger.json"
)


def campaign_gate() -> int:
    if os.getenv("GITHUB_EVENT_NAME") != "workflow_dispatch":
        raise ValueError("campaign dispatch required")
    _, prior, _ = job_identity_and_history()
    return ledger_check(committed_ledger(LEDGER), 18900, prior)


def run(output: Path) -> int:
    fresh_output(output)
    manifest = {
        "status": "FAIL",
        "scenario": os.getenv("DUR053_SCENARIO"),
        "arm": os.getenv("DUR053_ARM"),
        "run_id": os.getenv("GITHUB_RUN_ID"),
        "live_seconds": 0,
    }
    try:
        manifest["remaining_after_max_job_reservation"] = campaign_gate()
        raise ValueError("K3b: campaign scenario not implemented; no live resources started")
    except (ValueError, OSError, KeyError) as error:
        manifest["error"] = str(error)
    finally:
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    return run(parser.parse_args().output)


if __name__ == "__main__":
    raise SystemExit(main())
