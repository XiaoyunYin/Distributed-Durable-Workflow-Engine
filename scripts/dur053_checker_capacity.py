"""B.6.2 Linux capacity evidence from committed v1 short-1; no live workload."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from pathlib import Path

from dur053_k3_policy import ROOT, require
from dur053_payload import materialize
from dur053_pilot import EVIDENCE

LIMIT = 2 * 1024**3
LIVE_ESTIMATE_FLOOR = 512 * 1024**2
LEDGER = Path("experiments/portfolio/k8s-recovery/live-time-ledger.json")


def max_rss(text: str) -> int:
    values = re.findall(r"^\s*Maximum resident set size \(kbytes\):\s*(\d+)\s*$", text, re.M)
    require(len(values) == 1 and int(values[0]) > 0, "b62-capacity-rss")
    return int(values[0]) * 1024  # Linux time reports KiB, despite its label.


def projection(rss: int) -> dict:
    require(type(rss) is int and rss > 0, "b62-capacity-rss")
    estimate = max(rss, LIVE_ESTIMATE_FLOOR)
    projected = estimate * 1.50 * 2
    return {
        "offline_max_rss_bytes": rss,
        "live_estimate_floor_bytes": LIVE_ESTIMATE_FLOOR,
        "selected_v1_estimate_bytes": estimate,
        "combined_arrivals_scale": 1.50,
        "margin": 2,
        "projected_bytes": projected,
        "limit_bytes": LIMIT,
        "pass": projected <= LIMIT,
        "method": (
            "max(offline RSS, former 512Mi allocation) x 1.50 x 2; live estimate, not a bound"
        ),
    }


def select_source(repository: Path = ROOT) -> Path:
    candidates = []
    for manifest_path in sorted((repository / EVIDENCE / "k3-pilot-1a").glob("*/manifest.json")):
        try:
            manifest = json.loads(manifest_path.read_bytes())
        except (OSError, json.JSONDecodeError):
            continue
        if (
            manifest.get("status") == "PASS"
            and (manifest_path.parent / "payload-index.json").is_file()
        ):
            candidates.append((manifest_path, manifest))
    require(len(candidates) == 1, "b62-capacity-source")

    manifest_path, manifest = candidates[0]
    ledger_path = repository / LEDGER
    try:
        ledger = json.loads(ledger_path.read_bytes())
    except (OSError, json.JSONDecodeError):
        require(False, "b62-capacity-source")
    rows = [
        row
        for row in ledger.get("entries", [])
        if row.get("category") == "k3-pilot-1a"
        and str(row.get("run_id")) == str(manifest.get("run_id"))
    ]
    raw = manifest_path.read_bytes()
    relative = manifest_path.relative_to(repository).as_posix()
    require(
        len(rows) == 1
        and rows[0].get("artifact") == relative
        and hashlib.sha256(raw).hexdigest() == rows[0].get("artifact_sha256"),
        "b62-capacity-source",
    )
    return manifest_path.parent


def measure(checker: Path, output: Path, repository: Path = ROOT) -> dict:
    source = select_source(repository)
    output.parent.mkdir(parents=True, exist_ok=True)
    time_path = output.with_suffix(".time.txt")
    verdict_path = output.with_suffix(".checker.json")
    with materialize(source, repository) as root:
        cell = root / "short-1"
        command = [
            "/usr/bin/time",
            "-v",
            "-o",
            str(time_path),
            str(checker.resolve()),
            "-snapshot",
            str(cell / "snapshot.json"),
            "-arrivals",
            str(cell / "measured-combined-arrivals.json"),
            "-witness-export",
            str(cell / "witness.json"),
            "-require-terminal",
        ]
        with verdict_path.open("wb") as verdict:
            result = subprocess.run(command, stdout=verdict, env={**os.environ, "LC_ALL": "C"})
        numbers = projection(max_rss(time_path.read_text(encoding="utf-8")))
        numbers.update(
            schema="dur053.b62-checker-capacity.v1",
            source=source.relative_to(repository).as_posix(),
            cell="short-1",
            checker_exit=result.returncode,
            checker_sha256=hashlib.sha256(checker.read_bytes()).hexdigest(),
        )
        output.write_bytes((json.dumps(numbers, indent=2) + "\n").encode())
        require(
            result.returncode == 0 and json.loads(verdict_path.read_bytes()).get("valid") is True,
            "b62-capacity-checker",
        )
        require(numbers["pass"], "b62-capacity-limit")
    return numbers


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--checker", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    measure(args.checker, args.output)
