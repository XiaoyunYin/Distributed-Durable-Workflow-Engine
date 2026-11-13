from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_k3b_pilot import fixture_refusal

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import dur053_pilot as pilot  # noqa: E402
import dur053_pilot_evidence as evidence  # noqa: E402
import dur053_record_pilot as intake  # noqa: E402
from dur053_k3 import save  # noqa: E402
from dur053_k3_policy import artifact_hashes  # noqa: E402


def git(root: Path, *args: str) -> bytes:
    return subprocess.check_output(
        ["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", *args],
        cwd=root,
        stderr=subprocess.PIPE,
    )


def initialize(root: Path) -> None:
    git(root, "init")
    git(root, "config", "core.autocrlf", "false")


def commit(root: Path) -> None:
    git(root, "add", ".")
    git(root, "commit", "-m", "fixture")


class AdmissionTests(unittest.TestCase):
    def test_ci_proof_requires_every_main_push_gate(self) -> None:
        runs = {
            "workflow_runs": [
                {"id": 99, "head_sha": "a" * 40, "event": "push", "head_branch": "main"}
            ]
        }
        for name in sorted(evidence.REQUIRED_CI_JOBS):
            for conclusion in (None, "failure", "cancelled", "skipped", "timed_out", "duplicate"):
                with self.subTest(job=name, conclusion=conclusion):
                    jobs = [
                        {"name": job, "conclusion": "success"}
                        for job in sorted(evidence.REQUIRED_CI_JOBS)
                        if job != name
                    ]
                    if conclusion == "duplicate":
                        jobs.extend([{"name": name, "conclusion": "success"}] * 2)
                    elif conclusion is not None:
                        jobs.append({"name": name, "conclusion": conclusion})
                    with (
                        patch.object(evidence, "tooling_inventory", return_value={"go.mod": b"x"}),
                        patch.dict(os.environ, {"GITHUB_REPOSITORY": "fixture/repo"}),
                        patch.object(
                            evidence,
                            "github",
                            side_effect=[runs, {"total_count": len(jobs), "jobs": jobs}],
                        ),
                        self.assertRaisesRegex(ValueError, "pilot-ci-proof-missing"),
                    ):
                        evidence.ci_proof(ROOT)

    def test_normalized_committed_content(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            initialize(root)
            (root / ".gitattributes").write_bytes(b"*.ps1 text eol=crlf\n*.txt text=auto\n")
            scripts = root / "scripts"
            scripts.mkdir()
            path = scripts / "probe.ps1"
            path.write_bytes(b"Write-Output 'one'\r\n")
            (scripts / "probe.txt").write_bytes(b"one\r\n")
            commit(root)
            self.assertIn(b"\r\n", path.read_bytes())
            self.assertEqual(
                pilot.committed_bytes(root, path),
                b"Write-Output 'one'\n",
                "normalized git content rejected CRLF checkout",
            )
            self.assertEqual(
                evidence.tooling_inventory(root)["scripts/probe.ps1"],
                git(root, "show", "HEAD:scripts/probe.ps1"),
            )
            self.assertEqual(pilot.committed_bytes(root, scripts / "probe.txt"), b"one\n")
            tested_sha = git(root, "rev-parse", "HEAD").decode().strip()
            api_runs = {
                "workflow_runs": [
                    {"id": 99, "head_sha": tested_sha, "event": "push", "head_branch": "main"}
                ]
            }
            api_jobs = {
                "total_count": len(evidence.REQUIRED_CI_JOBS),
                "jobs": [
                    {"name": name, "conclusion": "success"}
                    for name in sorted(evidence.REQUIRED_CI_JOBS)
                ],
            }
            with (
                patch.dict(os.environ, {"GITHUB_REPOSITORY": "fixture/repo"}),
                patch.object(evidence, "github", side_effect=[api_runs, api_jobs]),
            ):
                self.assertEqual(evidence.ci_proof(root)["head_sha"], tested_sha)
            path.write_bytes(b"Write-Output 'two'\r\n")
            with self.assertRaisesRegex(ValueError, "pilot-committed-bytes"):
                pilot.committed_bytes(root, path)
            with self.assertRaisesRegex(ValueError, "pilot-ci-uncommitted-tooling"):
                evidence.tooling_inventory(root)
            git(root, "add", "scripts/probe.ps1")
            with self.assertRaisesRegex(ValueError, "pilot-ci-uncommitted-tooling"):
                evidence.tooling_inventory(root)
            commit(root)
            with (
                patch.dict(os.environ, {"GITHUB_REPOSITORY": "fixture/repo"}),
                patch.object(evidence, "github", side_effect=[api_runs, api_jobs]),
                self.assertRaisesRegex(ValueError, "pilot-ci-proof-missing"),
            ):
                evidence.ci_proof(root)
            git(root, "reset", "--hard", "HEAD")
            path.unlink()
            with self.assertRaisesRegex(ValueError, "pilot-ci-uncommitted-tooling"):
                evidence.tooling_inventory(root)
            git(root, "reset", "--hard", "HEAD")
            (scripts / "untracked.py").write_bytes(b"change")
            with self.assertRaisesRegex(ValueError, "pilot-ci-uncommitted-tooling"):
                evidence.tooling_inventory(root)

    def test_real_checkout_tooling_inventory(self) -> None:
        # CI mandates a clean real checkout. Local uncommitted implementations must
        # fail admission; checking that refusal lets the full local suite run.
        status = git(
            ROOT, "status", "--porcelain=v1", "-z", "--untracked-files=all", "--", *evidence.TOOLING
        )
        if os.getenv("DUR053_REQUIRE_CLEAN_TOOLING") == "1":
            self.assertFalse(status, "hosted tooling checkout must be clean")
        if status:
            with self.assertRaisesRegex(ValueError, "pilot-ci-uncommitted-tooling"):
                evidence.tooling_inventory(ROOT)
        else:
            inventory = evidence.tooling_inventory(ROOT)
            self.assertIn("scripts/ci.ps1", inventory)
            self.assertIn(b"\r\n", (ROOT / "scripts/ci.ps1").read_bytes())
            self.assertEqual(inventory["scripts/ci.ps1"], git(ROOT, "show", "HEAD:scripts/ci.ps1"))

    def test_evidence_crlf_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            initialize(root)
            git(root, "config", "core.autocrlf", "true")
            (root / ".gitattributes").write_bytes((ROOT / ".gitattributes").read_bytes())
            raw = b"literal artifact\r\nsecond line\r\n"
            for evidence_root in (
                pilot.EVIDENCE,
                Path("experiments/portfolio/k8s-recovery/k3b-tooling-v1"),
            ):
                for name in ("output.txt", "objects.yaml", "data.jsonl", "unknown.data"):
                    path = root / evidence_root / "job" / "123" / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(raw)
            commit(root)
            for path in root.glob("experiments/**/*"):
                if path.is_file():
                    relative = path.relative_to(root).as_posix()
                    self.assertEqual(git(root, "show", "HEAD:" + relative), raw)
                    path.unlink()
                    git(root, "checkout", "HEAD", "--", relative)
                    self.assertEqual(path.read_bytes(), raw, "artifact CRLF bytes changed")
                    self.assertEqual(pilot.committed_bytes(root, path), raw)

    def test_pre_start_definition(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = fixture_refusal(root)
            self.assertTrue(evidence.pre_start_refusal(root, pilot.read_artifact(root)))
            edits = [
                lambda m: m.update(status="PASS"),
                lambda m: m["cleanup"].update(status="FAIL"),
                lambda m: m["cleanup"].update(no_resources_started=False),
                lambda m: m["cleanup"].update(elapsed_live_seconds=1),
                lambda m: m["cleanup"].update(extra=True),
                lambda m: m.update(elapsed_live_seconds=1),
                lambda m: m.update(charged_seconds=2),
                lambda m: m.update(completed=["short-0"]),
                lambda m: m["unperformed"][0].update(started=True),
                lambda m: m["unperformed"][0].pop("started"),
                lambda m: m["unperformed"].pop(),
                lambda m: m["unperformed"].append(m["unperformed"][0]),
                lambda m: m["planned"].pop(),
                lambda m: m.update(planned=[]),
                lambda m: m["unperformed"][0].update(index=3),
                lambda m: m.update(artifact_sha256={}),
                lambda m: m.pop("cleanup"),
                lambda m: m.update(elapsed_live_seconds=False),
            ]
            for edit in edits:
                bad = copy.deepcopy(manifest)
                edit(bad)
                self.assertFalse(
                    evidence.pre_start_refusal(root, bad), "pre-start defining field bypassed"
                )
            for name in ("short-0", "calibration-10000", "seal", "unknown"):
                directory = root / name
                directory.mkdir()
                self.assertFalse(evidence.pre_start_refusal(root, manifest))
                directory.rmdir()
            for name in ("calibration.json", "calibration-seal.json", "seal.json"):
                path = root / name
                path.write_bytes(b"{}")
                bad = {**manifest, "artifact_sha256": artifact_hashes(root)}
                self.assertFalse(evidence.pre_start_refusal(root, bad))
                path.unlink()
            for raw in (
                b'{"args": ["docker", "ps"]}\n',
                b'{"args": ["/usr/bin/git"]}\n',
                b'{"args": []}\n',
                b"{}\n",
                b"not json\n",
                b"",
            ):
                (root / "commands.jsonl").write_bytes(raw)
                bad = {**manifest, "artifact_sha256": artifact_hashes(root)}
                self.assertFalse(
                    evidence.pre_start_refusal(root, bad), "pre-start command allowlist bypassed"
                )
            (root / "commands.jsonl").unlink()
            self.assertFalse(evidence.pre_start_refusal(root, manifest))

    def test_pre_start_replay_bound(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ("k3-pilot-1a", "k3-pilot-1a-c1"):
                previous = []
                predecessor = None
                if "-c" in name:
                    predecessor = {
                        "job": "k3-pilot-1a",
                        "status": "CARRY_OVER",
                        "cleanup": {"status": "PASS"},
                        "completed": [],
                        "carry_over": {
                            "job": name,
                            "reason": "pilot-job-fit",
                            "started": False,
                            "cells": [["medium", 0], ["medium", 1], ["medium", 2], ["medium", 3]],
                        },
                    }
                for n in range(1, 5):
                    path = root / name / str(n)
                    manifest = fixture_refusal(path, job=name, run=str(n))
                    previous.append((path, manifest))
                    available = ([(root, predecessor)] if predecessor else []) + previous
                    with patch.object(evidence, "artifacts", return_value=available):
                        if n < 3:
                            expected = (
                                [("medium", i) for i in range(4)]
                                if predecessor
                                else pilot.JOBS[name]
                            )
                            self.assertEqual(evidence.dependency_plan(root, name)[0], expected)
                        else:
                            with self.assertRaisesRegex(
                                ValueError,
                                "pilot-pre-start-replay-limit",
                                msg="pre-start replay bound bypassed",
                            ):
                                evidence.dependency_plan(root, name)
                ordinary = copy.deepcopy(previous[0][1])
                ordinary["charged_seconds"] = 2
                with patch.object(evidence, "artifacts", return_value=[(previous[0][0], ordinary)]):
                    # A carry name also still needs its original valid predecessor.
                    if predecessor:
                        evidence.artifacts.return_value.insert(0, (root, predecessor))
                    with self.assertRaisesRegex(ValueError, "pilot-no-replay-job"):
                        evidence.dependency_plan(root, name)

    def test_refusal_intake_retains_charge_and_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = fixture_refusal(root / "download")
            repository = root / "repository"
            ledger = repository / "experiments/portfolio/k8s-recovery/live-time-ledger.json"
            ledger.parent.mkdir(parents=True)
            save(
                ledger, {"schema": "dur053.live-ledger.v1", "limit_seconds": 259200, "entries": []}
            )
            target = intake.record(root / "download", repository)
            self.assertEqual(
                (target / "manifest.json").read_bytes(),
                (root / "download/manifest.json").read_bytes(),
            )
            row = json.loads(ledger.read_bytes())["entries"][0]
            self.assertEqual(row["charged_seconds"], 1)
            self.assertEqual(row["job_id"], manifest["job_id"])
            self.assertTrue(evidence.pre_start_refusal(target, pilot.read_artifact(target)))


if __name__ == "__main__":
    unittest.main()


def test_k4_smoke_is_a_required_main_push_ci_job() -> None:
    assert "DUR-053 K4 lifecycle smoke" in evidence.REQUIRED_CI_JOBS
