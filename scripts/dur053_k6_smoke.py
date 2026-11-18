"""Exercise the registered Phase A contracts in an offline engineering smoke."""

from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path

from dur053_k3_policy import ledger_check
from dur053_k6_admission import admit, schedule_sensitivities
from dur053_k6_registry import (
    EVIDENCE,
    ROOT,
    json_bytes,
    read_registry,
    require,
    synthetic_manifest,
    validate_manifest,
    validate_registry,
)
from dur053_schedule import CEILING


def smoke(output_root: Path) -> None:
    output_root = output_root.resolve()
    require(not output_root.is_relative_to(ROOT), "k6-smoke-output-outside-repository")
    output_root.mkdir()
    report = {
        "schema": "dur053.k6-phase-a-smoke.v1",
        "status": "FAIL",
        "measurement_claim": False,
        "charged_seconds": 0,
    }
    try:
        registry = read_registry()
        (output_root / "registry.json").write_bytes(json_bytes(registry))
        manifest = synthetic_manifest(registry, "k6-1")
        (output_root / "manifest.json").write_bytes(json_bytes(manifest))
        validate_manifest(json.loads(json_bytes(manifest)), registry)

        bad_manifest = copy.deepcopy(manifest)
        bad_manifest["episodes"][0]["seed"] += 1
        try:
            validate_manifest(bad_manifest, registry)
        except ValueError as error:
            require(str(error) == "k6-episode-seed", "k6-smoke-manifest-rejection")
        else:
            raise ValueError("k6-smoke-manifest-accepted-changed-seed")

        bad_registry = copy.deepcopy(registry)
        bad_registry["topology"]["worker_pods"] += 1
        try:
            validate_registry(bad_registry)
        except ValueError as error:
            require(str(error) == "k6-registry-topology", "k6-smoke-topology-rejection")
        else:
            raise ValueError("k6-smoke-registry-accepted-changed-topology")

        ledger = json.loads((EVIDENCE / "live-time-ledger.json").read_bytes())
        ledger_check(ledger, CEILING, set())
        actual = sum(row["charged_seconds"] for row in ledger["entries"])
        decision = admit(registry, ledger_actual=actual, progress={})
        require(
            not decision["admit"] and decision["reason"] == "missing-effect-overhead",
            "k6-smoke-missing-effect-overhead",
        )
        report.update(
            status="PASS",
            topology=registry["topology"],
            rejected_manifest="k6-episode-seed",
            rejected_topology="k6-registry-topology",
            admission_reason=decision["reason"],
            ledger_actual_seconds=actual,
            report_only_schedule=schedule_sensitivities(),
        )
    except Exception as error:
        report["error"] = str(error)
        raise
    finally:
        (output_root / "smoke.json").write_bytes(json_bytes(report))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, required=True)
    smoke(parser.parse_args().output_root)
