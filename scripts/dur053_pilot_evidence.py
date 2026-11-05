#!/usr/bin/env python3
"""Committed dependency, CI proof and reproducible freeze verification."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any

from dur053_k3 import github
from dur053_k3_policy import artifact_hashes, committed_ledger, require
from dur053_pilot import (
    EVIDENCE,
    JOBS,
    committed_bytes,
    initial_dependency,
    job_base,
    read_artifact,
    select_cpu,
    slo,
    summarize,
)

TOOLING = [
    "scripts",
    "migrations",
    "requirements-worker.txt",
    "deploy/k8s",
    "cmd",
    "internal",
    "python",
    "tests",
    ".github/workflows",
    "mutations/manifest.json",
    "go.mod",
    "go.sum",
    "pyproject.toml",
    "uv.lock",
    "experiments/portfolio/k8s-recovery/protocol-v2.md",
]


def ci_proof(repository: Path) -> dict[str, Any]:
    """Green CI proves this committed tooling, even after ledger-only commits."""
    listing = subprocess.run(
        ["git", "ls-files", "-z", "--", *TOOLING], cwd=repository, capture_output=True, check=True
    ).stdout
    paths = [p.decode() for p in listing.split(b"\0") if p]
    require(bool(paths), "pilot-ci-source-inventory")
    current = {p: committed_bytes(repository, repository / p) for p in paths}
    untracked = subprocess.run(
        ["git", "ls-files", "--others", "--exclude-standard", "--", *TOOLING],
        cwd=repository,
        capture_output=True,
        check=True,
    ).stdout
    require(not untracked.strip(), "pilot-ci-uncommitted-tooling")
    repo = os.environ["GITHUB_REPOSITORY"]
    for page in range(1, 101):
        runs = github(
            f"repos/{repo}/actions/workflows/ci.yml/runs?status=success&per_page=100&page={page}"
        )["workflow_runs"]
        for run in runs:
            # Push CI's event SHA is the tested checkout; a PR may test a merge tree.
            if run.get("event") != "push" or run.get("head_branch") != "main":
                continue
            jobs = github(f"repos/{repo}/actions/runs/{run['id']}/jobs?per_page=100")
            require(jobs["total_count"] <= 100, "pilot-ci-job-pagination")
            if not any(
                j["name"] == "DUR-053 pilot smoke" and j["conclusion"] == "success"
                for j in jobs["jobs"]
            ):
                continue
            same = True
            for relative, data in current.items():
                result = subprocess.run(
                    ["git", "show", run["head_sha"] + ":" + relative],
                    cwd=repository,
                    capture_output=True,
                    check=False,
                )
                if result.returncode or result.stdout != data:
                    same = False
                    break
            if same:
                return {
                    "run_id": str(run["id"]),
                    "head_sha": run["head_sha"],
                    "tooling_sha256": {
                        p: hashlib.sha256(b).hexdigest() for p, b in current.items()
                    },
                }
        if len(runs) < 100:
            break
    raise ValueError("K3 guard: pilot-ci-proof-missing")


def artifacts(repository: Path) -> list[tuple[Path, dict[str, Any]]]:
    root = repository / EVIDENCE
    return [
        (p.parent, read_artifact(p.parent, repository))
        for p in sorted(root.glob("*/*/manifest.json"))
    ]


def dependency_plan(
    repository: Path, name: str
) -> tuple[list[tuple[str, int]], dict[str, Any], dict[str, Any] | None]:
    base = job_base(name)
    available = artifacts(repository)
    pending = JOBS[base]
    predecessor = None
    if "-c" in name:
        prior = [(p, m) for p, m in available if m.get("carry_over", {}).get("job") == name]
        require(len(prior) == 1, "pilot-carry-predecessor")
        _, predecessor = prior[0]
        require(
            predecessor["status"] == "CARRY_OVER"
            and predecessor["cleanup"]["status"] == "PASS"
            and predecessor["carry_over"]["reason"] == "pilot-job-fit"
            and predecessor["carry_over"]["started"] is False,
            "pilot-carry-predecessor-safe",
        )
        pending = [tuple(x) for x in predecessor["carry_over"]["cells"]]
        require(bool(pending) and pending == JOBS[base][-len(pending) :], "pilot-carry-order")
    require(not any(m["job"] == name for _, m in available), "pilot-no-replay-job")
    initials = {}
    for arm in {a for a, i in pending if i > 0 and (a, 0) not in pending}:
        matches = [(p, m) for p, m in available if f"{arm}-0" in m["completed"]]
        require(len(matches) == 1, "pilot-cross-job-initial-required")
        initials[arm] = initial_dependency(matches[0][0], arm, repository)
    return list(pending), initials, predecessor


def same_job_initial(root: Path, arm: str) -> dict[str, Any]:
    directory = root / f"{arm}-0"
    seal = json.loads((directory / "seal.json").read_bytes())
    require(
        seal == {k: v for k, v in artifact_hashes(directory).items() if k != "seal.json"},
        "pilot-same-job-initial-hashes",
    )
    result = summarize(
        directory, engineering=json.loads((directory / "record.json").read_bytes())["engineering"]
    )
    record = json.loads((directory / "record.json").read_bytes())
    require(result["b"] == record["b"] and result["r"] == record["r"], "pilot-same-job-br")
    return result


def verify_calibration(root: Path) -> dict[str, Any]:
    path = root / "calibration.json"
    calibration = json.loads(path.read_bytes())
    require(
        calibration["status"] == "PASS" and calibration["engineering"] is False,
        "pilot-calibration-accepted",
    )
    require(
        json.loads((root / "calibration-seal.json").read_bytes())["sha256"]
        == hashlib.sha256(path.read_bytes()).hexdigest(),
        "pilot-calibration-seal",
    )
    selection = select_cpu(calibration["candidates"])
    require(all(calibration[k] == v for k, v in selection.items()), "pilot-calibration-selection")
    from dur053_pilot import busy_times

    for candidate in calibration["candidates"]:
        directory = root / f"calibration-{candidate['units']}"
        observed = []
        require(
            len(candidate["invocation_elapsed_seconds"]) == 22
            and all(0 < x <= 120 for x in candidate["invocation_elapsed_seconds"]),
            "pilot-calibration-time-bound",
        )
        for i in range(22):
            arrivals = json.loads((directory / f"invocation-{i}-arrivals.json").read_bytes())
            require(
                arrivals["offered"] == 1
                and arrivals["status"] == "PASS"
                and arrivals["concurrency"] == 1
                and arrivals["cpu_units"] == candidate["units"],
                "pilot-calibration-arrival",
            )
            report = json.loads((directory / f"invocation-{i}-checker.json").read_bytes())
            require(report["valid"] is True, "pilot-calibration-checker")
            witness = json.loads((directory / f"invocation-{i}-witness.json").read_bytes())
            values = busy_times(witness, {arrivals["rows"][0]["workflow_id"]})
            require(len(values) == 1 and values[0] < 120, "pilot-calibration-invocation")
            observed.append(values[0])
        require(
            candidate["discarded_busy_seconds"] == observed[0]
            and candidate["measured_busy_seconds"] == observed[1:],
            "pilot-calibration-witness-source",
        )
    return calibration


def recheck(directory: Path, checker: Path) -> None:
    """Run the independent Go oracle again on committed snapshots, offline."""
    for snapshot in sorted(directory.rglob("*-snapshot.json")) + sorted(
        directory.glob("*/snapshot.json")
    ):
        parent, stem = snapshot.parent, snapshot.name.removesuffix("-snapshot.json")
        if snapshot.name == "snapshot.json":
            arrivals, witness = "measured-combined-arrivals.json", "witness.json"
        else:
            arrivals, witness = stem + "-combined-arrivals.json", stem + "-witness.json"
        result = subprocess.run(
            [
                str(checker),
                "-snapshot",
                str(snapshot),
                "-arrivals",
                str(parent / arrivals),
                "-witness-export",
                str(parent / witness),
                "-require-terminal",
            ],
            capture_output=True,
            check=False,
            timeout=180,
        )
        require(
            result.returncode == 0 and json.loads(result.stdout)["valid"] is True,
            "pilot-freeze-independent-checker",
        )


def freeze(repository: Path, checker: Path) -> dict[str, Any]:
    available = artifacts(repository)
    require(bool(available), "pilot-freeze-artifacts-missing")
    for _root, manifest in available:
        require(
            manifest["status"] in {"PASS", "CARRY_OVER"}
            and manifest["cleanup"]["status"] == "PASS",
            "pilot-freeze-blocking-job",
        )
    ledger_path = repository / "experiments/portfolio/k8s-recovery/live-time-ledger.json"
    committed_bytes(repository, ledger_path)
    ledger = committed_ledger(ledger_path, repository)
    cells: dict[tuple[str, int], tuple[Path, dict[str, Any]]] = {}
    hashes = {}
    calibration_roots = []
    for root, manifest in available:
        require(
            any(
                row.get("category") == manifest["job"]
                and row["job_id"] == manifest["job_id"]
                and row["artifact"] == (root / "manifest.json").relative_to(repository).as_posix()
                for row in ledger["entries"]
            ),
            "pilot-freeze-ledger-entry",
        )
        require(
            not manifest.get("error") and manifest["ci_proof"]["run_id"].isdigit(),
            "pilot-freeze-tooling-proof",
        )
        require(
            manifest["artifact_sha256"].get("versions.json")
            and manifest["artifact_sha256"].get("images.json"),
            "pilot-freeze-environment",
        )
        recheck(root, checker)
        require(
            set(manifest["completed"]) == {p.parent.name for p in root.glob("*/record.json")},
            "pilot-freeze-run-inventory",
        )
        hashes[(root / "manifest.json").relative_to(repository).as_posix()] = hashlib.sha256(
            (root / "manifest.json").read_bytes()
        ).hexdigest()
        if (root / "calibration.json").exists():
            verify_calibration(root)
            calibration_roots.append(root)
        for relative in manifest["completed"]:
            summary = summarize(root / relative)
            cell = (summary["arm"], summary["index"])
            require(cell not in cells, "pilot-freeze-duplicate-cell")
            cells[cell] = (root, summary)
    require(
        set(cells) == {(a, i) for a in ("short", "medium", "long", "cpu") for i in range(4)},
        "pilot-freeze-all-cells",
    )
    require(
        len(calibration_roots) == 1 and calibration_roots[0] == cells[("cpu", 0)][0],
        "pilot-freeze-cpu-calibration",
    )
    calibration = verify_calibration(calibration_roots[0])
    arms = {}
    from dur053_pilot import rates

    for arm in ("short", "medium", "long", "cpu"):
        root, initial = cells[(arm, 0)]
        for i in range(4):
            current, _ = cells[(arm, i)]
            record = json.loads((current / f"{arm}-{i}/record.json").read_bytes())
            expected = rates(arm, i, initial)
            require(
                (record["warmup_rate"], record["measured_rate"]) == expected, "pilot-freeze-rates"
            )
            if arm == "cpu":
                require(
                    record["cpu_units"] == calibration["units"]
                    and json.loads((current / "images.json").read_bytes())["worker"]
                    == calibration["image"],
                    "pilot-freeze-cpu-units-image",
                )
                if i == 0:
                    require(
                        record["calibration_sha256"]
                        == hashlib.sha256((current / "calibration.json").read_bytes()).hexdigest()
                        and calibration["persisted_ns"]
                        == record["calibration_persisted_ns"]
                        <= record["warmup_start_ns"],
                        "pilot-calibration-before-initial",
                    )
            if i and current != root:
                dep = json.loads((current / "manifest.json").read_bytes())["dependencies"][arm]
                require(
                    dep["manifest_sha256"]
                    == hashlib.sha256((root / "manifest.json").read_bytes()).hexdigest()
                    and dep["b"] == initial["b"]
                    and dep["r"] == initial["r"],
                    "pilot-freeze-dependency",
                )
            elif i:
                same_job_initial(root, arm)
        arms[arm] = {
            "b": initial["b"],
            "r": initial["r"],
            "L": slo([cells[(arm, i)][1]["latencies"] for i in range(1, 4)]),
            "arrival_concurrency_cap": 16,
        }
    from dur053_schedule import observed_plan

    return {
        "schema": "dur053.k3-freeze.v1",
        "source_artifacts": hashes,
        "arms": arms,
        "observed_schedule": observed_plan(
            arms, max(calibration["median_busy_seconds"], arms["cpu"]["b"])
        ),
        "cpu_calibration": {
            k: calibration[k] for k in ("units", "median_busy_seconds", "limitation")
        },
        "environments": {
            root.relative_to(repository).as_posix(): {
                "versions_sha256": m["artifact_sha256"]["versions.json"],
                "images_sha256": m["artifact_sha256"]["images.json"],
                "resource_samples_sha256": {
                    p: h for p, h in m["artifact_sha256"].items() if p.endswith("samples.jsonl")
                },
            }
            for root, m in available
        },
        "limitations": [
            "single shared host per job; cross-job host variance",
            "terminal latency uses first-observed terminal upper bounds; sampling error retained",
            "not a capacity or sustainable-throughput claim",
        ],
    }
