from __future__ import annotations

import base64
import copy
import hashlib
import json
import math
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import dur053_campaign as campaign  # noqa: E402
import dur053_freeze as freeze_cli  # noqa: E402
import dur053_pilot as pilot  # noqa: E402
import dur053_pilot_evidence as evidence  # noqa: E402
import dur053_record_pilot as intake  # noqa: E402
from dur053_k3 import campaign_job_name, save  # noqa: E402
from dur053_k3_policy import artifact_hashes  # noqa: E402
from dur053_pilot_driver import PilotDriver, poll_pause  # noqa: E402
from dur053_schedule import plan, plan_b2  # noqa: E402


def fixture_run(
    root: Path,
    arm: str = "short",
    index: int = 0,
    *,
    engineering: bool = False,
    initial: dict | None = None,
) -> Path:
    directory = root / f"{arm}-{index}"
    directory.mkdir(parents=True)
    warm, measured = pilot.rates(arm, index, initial)
    duration, count = (20, 10) if engineering else (120, 200)
    now = datetime(2026, 11, 5, tzinfo=UTC)

    def window(label: str, n: int, rate: float) -> dict:
        return {
            "schema": "dur053.arrivals.v1",
            "offered": n,
            "status": "PASS",
            "rate": rate,
            "concurrency": 16,
            "cpu_units": 10000,
            "rows": [
                {
                    "workflow_id": f"{label}-{i}",
                    "outcome": "ACCEPTED",
                    "scheduled_at": (now + timedelta(seconds=i / rate)).isoformat(),
                }
                for i in range(n)
            ],
        }

    warmup = window("warmup", math.ceil(duration * warm), warm)
    measured_rows = window("measured", count, measured)
    save(directory / "warmup-arrivals.json", warmup)
    save(directory / "measured-arrivals.json", measured_rows)
    reports = {"valid": True, "non_terminal": [], "campaign": {"valid": True, "non_terminal": []}}
    for name in ("warmup-checker.json", "checker.json", "offline-checker.json"):
        save(directory / name, reports)
    rows = warmup["rows"] + measured_rows["rows"]
    events = [{"event": "session", "sequence": 1, "session": "one", "pod_uid": "pod-1"}]
    for row in rows:
        start = {
            "event": "start",
            "invocation_id": row["workflow_id"],
            "sequence": len(events) + 1,
            "session": "one",
            "pod_uid": "pod-1",
            "workflow_id": row["workflow_id"],
            "node_id": "activity",
            "iteration": 0,
            "attempt_number": 1,
            "claim_token": "token",
            "worker_id": "worker",
            "monotonic_ns": 1,
        }
        end = {
            **start,
            "event": "end",
            "sequence": len(events) + 2,
            "monotonic_ns": 100000001,
            "outcome": "SUCCEEDED",
        }
        events.extend([start, end])
    second = [{"event": "session", "sequence": 1, "session": "two", "pod_uid": "pod-2"}]
    export = {"expected_pods": ["pod-1", "pod-2"], "ledgers": []}
    for source in (events, second):
        raw = ("\n".join(json.dumps(e) for e in source) + "\n").encode()
        export["ledgers"].append(
            {
                "inventory": {
                    "pod_uid": source[0]["pod_uid"],
                    "session": source[0]["session"],
                    "complete": True,
                    "errors": 0,
                    "written": len(source),
                    "attempted": len(source),
                    "sha256": hashlib.sha256(raw).hexdigest(),
                },
                "data": base64.b64encode(raw).decode(),
            }
        )
    save(directory / "witness.json", export)
    save(
        directory / "snapshot.json",
        {
            "Snapshot": {
                "engine.workflow_executions": {
                    "rows": [
                        {"workflow_id": row["workflow_id"], "state": "SUCCEEDED", "revision": 3}
                        for row in rows
                    ]
                },
                "engine.transition_history": {
                    "rows": [
                        {
                            "workflow_id": row["workflow_id"],
                            "new_state": "SUCCEEDED",
                            "revision": 3,
                            "created_at": (
                                datetime.fromisoformat(row["scheduled_at"]) + timedelta(seconds=1)
                            ).isoformat(),
                        }
                        for row in rows
                    ]
                },
            }
        },
    )
    record = {
        "arm": arm,
        "index": index,
        "status": "PASS",
        "timed_out": False,
        "engineering": engineering,
        "faults": [],
        "slots": 4,
        "autoscaling": "paused-at-two-replicas",
        "warmup_rate": warm,
        "measured_rate": measured,
        "warmup_start_ns": 1,
        "warmup_end_ns": duration * 10**9 + 1,
        "warmup_reconciled_ns": (duration + 31) * 10**9,
        "measured_start_ns": (duration + 32) * 10**9,
        "warmup_quiet_seconds": 30,
        "measured_quiet_seconds": 30,
        "reconciliation_timeout_seconds": 600,
        "terminal_observed_at": {
            row["workflow_id"]: (
                datetime.fromisoformat(row["scheduled_at"]) + timedelta(seconds=2)
            ).isoformat()
            for row in rows
        },
        "cpu_units": 10000,
    }
    save(
        directory / "clock.json",
        {
            phase: {
                "observer_before": moment.isoformat(),
                "database": (moment + timedelta(milliseconds=1)).isoformat(),
                "observer_after": (moment + timedelta(milliseconds=2)).isoformat(),
            }
            for phase, moment in (
                ("setup", now - timedelta(seconds=1)),
                ("measured", now + timedelta(seconds=20000)),
            )
        },
    )
    observed_at = (now + timedelta(seconds=10000)).isoformat()
    record["terminal_observed_at"] = {r["workflow_id"]: observed_at for r in rows}
    resources = {"limits": {"cpu": "1", "memory": "1Gi"}, "requests": {"cpu": "1", "memory": "1Gi"}}
    sample = {
        "monotonic_ns": 1,
        "observed_at": observed_at,
        "connections": 20,
        "worker_deployment": {"spec": {"replicas": 2}},
        "scaledobject": {"metadata": {"annotations": {"autoscaling.keda.sh/paused-replicas": "2"}}},
        "terminal_ids": list(record["terminal_observed_at"]),
        "pods": [
            {
                "metadata": {"uid": p},
                "spec": {
                    "containers": [
                        {"resources": resources, "env": [{"name": "WORKER_SLOTS", "value": "2"}]}
                    ]
                },
                "status": {"containerStatuses": [{"ready": True, "restartCount": 0}]},
            }
            for p in ("pod-1", "pod-2")
        ],
    }
    (directory / "samples.jsonl").write_text(json.dumps(sample) + "\n")
    save(directory / "record.json", record)
    summary = pilot.summarize(directory, engineering=engineering)
    save(directory / "record.json", {**record, **summary})
    save(directory / "seal.json", artifact_hashes(directory))
    return directory


def fixture_artifact(
    root: Path,
    *,
    cells: list[tuple[str, int]] | None = None,
    job: str = "k3-pilot-1a",
    run: str = "10",
    job_id: str = "20",
) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    for arm, index in cells or []:
        initial = {"r": 40} if index else None
        fixture_run(root, arm, index, initial=initial)
    save(root / "versions.json", {"ImageVersion": "fixture"})
    save(root / "images.json", {"worker": "worker@sha256:" + "a" * 64})
    m = {
        "schema": "dur053.k3b.v1",
        "job": job,
        "run_id": run,
        "job_id": job_id,
        "head_sha": "a" * 40,
        "merge_ref_sha": "b" * 40,
        "tree_sha": "c" * 40,
        "registered_live_seconds": 18900,
        "charged_seconds": 100,
        "elapsed_live_seconds": 100,
        "cleanup": {"status": "PASS", "elapsed_live_seconds": 100},
        "status": "PASS",
        "budget_scope": "registered-campaign",
        "measurement_claim": True,
        "completed": [f"{a}-{i}" for a, i in cells or []],
        "ci_proof": {"run_id": "99"},
        "dependencies": {},
        "artifact_sha256": artifact_hashes(root),
    }
    save(root / "manifest.json", m)
    return m


def fixture_calibration(root: Path) -> None:
    directory = root / "calibration-10000"
    directory.mkdir()
    records = [{"event": "session", "sequence": 1, "session": "one", "pod_uid": "pod-1"}]
    for i in range(22):
        key = f"cal-{i}"
        start = {
            "event": "start",
            "invocation_id": key,
            "sequence": len(records) + 1,
            "session": "one",
            "pod_uid": "pod-1",
            "workflow_id": key,
            "node_id": "dur053.cpu",
            "iteration": 0,
            "attempt_number": 1,
            "claim_token": "token",
            "worker_id": "worker",
            "monotonic_ns": i * 10**10 + 1,
        }
        records.extend(
            [
                start,
                {
                    **start,
                    "event": "end",
                    "sequence": len(records) + 2,
                    "monotonic_ns": start["monotonic_ns"] + 10**9,
                    "outcome": "SUCCEEDED",
                },
            ]
        )
        export = {"expected_pods": ["pod-1", "pod-2"], "ledgers": []}
        for events in (
            records,
            [{"event": "session", "sequence": 1, "session": "two", "pod_uid": "pod-2"}],
        ):
            data = ("\n".join(json.dumps(e) for e in events) + "\n").encode()
            export["ledgers"].append(
                {
                    "inventory": {
                        "pod_uid": events[0]["pod_uid"],
                        "session": events[0]["session"],
                        "complete": True,
                        "errors": 0,
                        "attempted": len(events),
                        "written": len(events),
                        "sha256": hashlib.sha256(data).hexdigest(),
                    },
                    "data": base64.b64encode(data).decode(),
                }
            )
        save(
            directory / f"invocation-{i}-arrivals.json",
            {
                "offered": 1,
                "status": "PASS",
                "concurrency": 1,
                "cpu_units": 10000,
                "rows": [{"workflow_id": key}],
            },
        )
        save(directory / f"invocation-{i}-checker.json", {"valid": True})
        save(directory / f"invocation-{i}-witness.json", export)
    selection = {"units": 10000, "median_busy_seconds": 1, "limitation": None}
    save(
        root / "calibration.json",
        {
            "schema": "dur053.cpu-calibration.v1",
            "status": "PASS",
            "engineering": False,
            "persisted_ns": 1,
            "image": "worker@sha256:" + "a" * 64,
            **selection,
            "candidates": [
                {
                    "units": 10000,
                    "status": "PASS",
                    "discarded": 1,
                    "concurrency": 1,
                    "timeout_seconds": 120,
                    "measured_busy_seconds": [1] * 21,
                    "discarded_busy_seconds": 1,
                    "invocation_elapsed_seconds": [40] * 22,
                }
            ],
        },
    )
    if (root / "cpu-0/record.json").exists():
        path = root / "cpu-0/record.json"
        value = json.loads(path.read_bytes())
        value.update(
            calibration_sha256=hashlib.sha256((root / "calibration.json").read_bytes()).hexdigest(),
            calibration_persisted_ns=1,
        )
        save(path, value)
        save(
            root / "cpu-0/seal.json",
            {k: v for k, v in artifact_hashes(root / "cpu-0").items() if k != "seal.json"},
        )
    save(
        root / "calibration-seal.json",
        {"sha256": hashlib.sha256((root / "calibration.json").read_bytes()).hexdigest()},
    )


class K3bPilotTests(unittest.TestCase):
    def test_freeze_cli_refuses_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / pilot.EVIDENCE / "freeze.json"
            target.parent.mkdir(parents=True)
            target.write_bytes(b"preserve existing freeze")
            with (
                patch.object(freeze_cli, "ROOT", root),
                patch.object(sys, "argv", ["freeze", "--checker", "checker"]),
                patch.object(
                    freeze_cli, "freeze", return_value={"unexpected": "replacement"}
                ) as compute,
                self.assertRaisesRegex(
                    ValueError,
                    "pilot-freeze-no-overwrite",
                    msg="pilot guard bypassed freeze-cli-overwrite",
                ),
            ):
                freeze_cli.main()
            compute.assert_not_called()
            self.assertEqual(target.read_bytes(), b"preserve existing freeze")

    def test_shell_pilot_job_input_gates(self) -> None:
        ps = (ROOT / "scripts/dur053-kubernetes.ps1").read_text()
        bash = (ROOT / "scripts/dur053-k3.sh").read_text()
        self.assertIn(
            'if ($Scenario -eq "pilot" -and -not $PilotJob) { throw',
            ps,
            "pilot guard bypassed PowerShell pilot-job input gate",
        )
        self.assertIn(
            ': "${job:?registered pilot job required}"',
            bash,
            "pilot guard bypassed Bash pilot-job input gate",
        )

    def test_ci_proof_uses_tested_push_commit(self) -> None:
        for event, branch in (
            ("pull_request", "feature"),
            ("pull_request", "main"),
            ("push", "feature"),
            ("push", "main"),
        ):
            run = {"id": 99, "head_sha": "a" * 40, "event": event, "head_branch": branch}
            with (
                patch.object(
                    evidence.subprocess,
                    "run",
                    side_effect=[
                        subprocess.CompletedProcess([], 0, b"go.mod\0"),
                        subprocess.CompletedProcess([], 0, b""),
                        subprocess.CompletedProcess([], 0, b"source"),
                    ],
                ),
                patch.object(evidence, "committed_bytes", return_value=b"source"),
                patch.object(
                    evidence,
                    "github",
                    side_effect=[
                        {"workflow_runs": [run]},
                        {
                            "total_count": 1,
                            "jobs": [{"name": "DUR-053 pilot smoke", "conclusion": "success"}],
                        },
                    ],
                ),
                patch.dict(os.environ, {"GITHUB_REPOSITORY": "fixture/repo"}),
            ):
                if event == "push" and branch == "main":
                    self.assertEqual(evidence.ci_proof(ROOT)["run_id"], "99")
                else:
                    with self.assertRaisesRegex(
                        ValueError,
                        "pilot-ci-proof-missing",
                        msg="pilot guard bypassed tested-push-tree",
                    ):
                        evidence.ci_proof(ROOT)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with (
                patch.dict(
                    os.environ,
                    {
                        "GITHUB_ACTIONS": "true",
                        "GITHUB_EVENT_NAME": "push",
                        "GITHUB_REF": "refs/heads/main",
                        "GITHUB_REPOSITORY": "fixture/repo",
                        "GITHUB_RUN_ID": "10",
                        "DUR053_HEAD_SHA": "a" * 40,
                    },
                ),
                patch.object(
                    campaign,
                    "campaign_gate",
                    side_effect=AssertionError("engineering read history"),
                ),
                patch.object(
                    campaign,
                    "github",
                    return_value={"jobs": [{"name": "DUR-053 pilot smoke", "id": 20}]},
                ),
                patch.object(
                    PilotDriver,
                    "command",
                    return_value=subprocess.CompletedProcess([], 0, "a" * 40),
                ),
                patch.object(PilotDriver, "install", side_effect=ValueError("stop before live")),
                patch.object(
                    PilotDriver,
                    "cleanup",
                    return_value={"status": "PASS", "no_resources_started": True},
                ),
            ):
                campaign.run(root / "out", root / "scratch", "pilot-smoke", engineering=True)
            manifest = json.loads((root / "out/manifest.json").read_bytes())
            self.assertEqual(manifest["error"], "stop before live")
            self.assertEqual(manifest["charged_seconds"], 0)
            self.assertFalse(manifest["measurement_claim"])

    def test_poll_cadence_and_unperformed_refusal(self) -> None:
        with patch("dur053_pilot_driver.time.monotonic", return_value=2.8):
            self.assertAlmostEqual(poll_pause(2, 1), 0.2, msg="pilot guard bypassed poll-cadence")
        with patch("dur053_pilot_driver.time.monotonic", return_value=4):
            self.assertEqual(poll_pause(2, 1), 0, "pilot guard bypassed poll-cadence")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with (
                patch.dict(os.environ, {"GITHUB_ACTIONS": "true"}),
                patch.object(
                    PilotDriver,
                    "command",
                    return_value=subprocess.CompletedProcess([], 0, "a" * 40),
                ),
                patch.object(
                    campaign, "job_identity_and_history", return_value=("20", set(), "a" * 40)
                ),
                patch.object(campaign, "campaign_gate", return_value=200000),
                patch.object(campaign, "committed_bytes"),
                patch.object(campaign, "committed_ledger", return_value={"entries": []}),
                patch.object(campaign, "ci_proof", return_value={"run_id": "10"}),
                patch.object(
                    campaign, "dependency_plan", return_value=(pilot.JOBS["k3-pilot-1a"], {}, None)
                ),
                patch.object(campaign, "admit", side_effect=ValueError("K3 guard: pilot-job-fit")),
                patch.object(PilotDriver, "install") as install,
                patch.object(
                    PilotDriver,
                    "cleanup",
                    return_value={"status": "PASS", "no_resources_started": True},
                ),
            ):
                self.assertEqual(campaign.run(root / "out", root / "scratch", "k3-pilot-1a"), 1)
                install.assert_not_called()
            manifest = json.loads((root / "out/manifest.json").read_bytes())
            self.assertEqual(len(manifest["unperformed"]), 8)
            self.assertTrue(
                all(
                    r["started"] is False and r["reason"] == "K3 guard: pilot-job-fit"
                    for r in manifest["unperformed"]
                )
            )

    def test_campaign_history_names(self) -> None:
        for name in ("campaign", "DUR-053 campaign", *pilot.JOBS, "k3-pilot-1b-c18"):
            self.assertTrue(campaign_job_name(name), "pilot guard bypassed history-job-names")
        for name in ("DUR-053 kind smoke", "DUR-053 pilot smoke", "k3-pilot-1b-c0"):
            self.assertFalse(campaign_job_name(name))

    def test_teardown_bound_and_workflow_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = PilotDriver(Path(tmp), Path(tmp))
            d.accounting_start = 1
            d.teardown_deadline = 1
            with (
                patch("dur053_pilot_driver.time.monotonic", return_value=2),
                patch(
                    "dur053_k3.subprocess.run",
                    return_value=subprocess.CompletedProcess([], 0, "", ""),
                ),
                self.assertRaisesRegex(
                    ValueError,
                    "pilot-teardown-deadline",
                    msg="pilot guard bypassed teardown-deadline",
                ),
            ):
                d.command(["docker", "ps"])
        ci = (ROOT / ".github/workflows/ci.yml").read_text()
        dispatch = (ROOT / ".github/workflows/dur053-campaign.yml").read_text()
        self.assertIn("group: dur053-live-ledger\n  cancel-in-progress: false", dispatch)
        self.assertIn("timeout-minutes: 350", dispatch)
        self.assertIn("name: DUR-053 pilot smoke", ci)
        self.assertIn('--job pilot-smoke --output "$RUNNER_TEMP/', ci)
        self.assertIn('--job "$DUR053_PILOT_JOB"', dispatch)

    def test_dispatch_and_ci_proof_guards(self) -> None:
        with (
            patch.dict(os.environ, {"GITHUB_EVENT_NAME": "pull_request"}),
            self.assertRaisesRegex(
                ValueError, "campaign dispatch required", msg="pilot guard bypassed dispatch-only"
            ),
        ):
            campaign.campaign_gate()
        for label, env, jobs in (
            ("pilot-hosted-only", {}, {"jobs": []}),
            (
                "pilot-engineering-ci-only",
                {"GITHUB_ACTIONS": "true", "GITHUB_EVENT_NAME": "push"},
                {"jobs": []},
            ),
            (
                "pilot-engineering-identity",
                {
                    "GITHUB_ACTIONS": "true",
                    "GITHUB_EVENT_NAME": "pull_request",
                    "GITHUB_REPOSITORY": "fixture/repo",
                    "GITHUB_RUN_ID": "10",
                },
                {"jobs": []},
            ),
        ):
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                with (
                    patch.dict(os.environ, {"PATH": os.environ["PATH"], **env}, clear=True),
                    patch.object(campaign, "github", return_value=jobs),
                    patch.object(
                        PilotDriver,
                        "command",
                        return_value=subprocess.CompletedProcess([], 0, "a" * 40),
                    ),
                    patch.object(
                        PilotDriver,
                        "cleanup",
                        return_value={"status": "PASS", "no_resources_started": True},
                    ),
                ):
                    self.assertEqual(
                        campaign.run(
                            root / "out", root / "scratch", "pilot-smoke", engineering=True
                        ),
                        1,
                    )
                self.assertEqual(
                    json.loads((root / "out/manifest.json").read_bytes())["error"],
                    "K3 guard: " + label,
                    "pilot guard bypassed dispatch-scope",
                )
        with (
            patch.object(
                evidence.subprocess,
                "run",
                side_effect=[
                    subprocess.CompletedProcess([], 0, b"go.mod\0"),
                    subprocess.CompletedProcess([], 0, b""),
                    subprocess.CompletedProcess([], 0, b"source"),
                ],
            ),
            patch.object(evidence, "committed_bytes", return_value=b"source"),
            patch.object(
                evidence,
                "github",
                side_effect=[
                    {
                        "workflow_runs": [
                            {"id": 99, "head_sha": "a" * 40, "event": "push", "head_branch": "main"}
                        ]
                    },
                    {
                        "total_count": 101,
                        "jobs": [{"name": "DUR-053 pilot smoke", "conclusion": "success"}],
                    },
                ],
            ),
            patch.dict(os.environ, {"GITHUB_REPOSITORY": "fixture/repo"}),
            self.assertRaisesRegex(
                ValueError, "pilot-ci-job-pagination", msg="pilot guard bypassed ci-pagination"
            ),
        ):
            evidence.ci_proof(ROOT)

    def test_driver_and_calibration_guard_details(self) -> None:
        from itertools import chain, repeat

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture_calibration(root)
            for label, file, edit in (
                (
                    "pilot-calibration-arrival",
                    "invocation-0-arrivals.json",
                    lambda x: x.update(concurrency=2),
                ),
                (
                    "pilot-calibration-checker",
                    "invocation-0-checker.json",
                    lambda x: x.update(valid=False),
                ),
            ):
                path = root / "calibration-10000" / file
                original = path.read_bytes()
                data = json.loads(original)
                edit(data)
                save(path, data)
                with self.assertRaisesRegex(
                    ValueError, label, msg="pilot guard bypassed calibration-detail"
                ):
                    evidence.verify_calibration(root)
                path.write_bytes(original)
            with (
                patch.object(pilot, "busy_times", return_value=[1, 2]),
                self.assertRaisesRegex(
                    ValueError,
                    "pilot-calibration-invocation",
                    msg="pilot guard bypassed calibration-invocation",
                ),
            ):
                evidence.verify_calibration(root)
            for label, clock, durations in (
                ("pilot-calibration-timeout", chain([0], repeat(121)), [1]),
                ("pilot-calibration-single-invocation", repeat(0), [1, 2]),
            ):
                d = PilotDriver(root, root)
                d.images = {"worker": "pinned-image"}
                with (
                    patch.object(d, "setup", return_value=("ns", "db")),
                    patch.object(d, "arrivals", return_value={"rows": [{"workflow_id": "cal-0"}]}),
                    patch.object(d, "reconcile"),
                    patch.object(d, "reset_episode"),
                    patch.object(d, "failure_export"),
                    patch(
                        "dur053_pilot_driver.time.monotonic",
                        side_effect=lambda clock=clock: next(clock),
                    ),
                    patch("dur053_pilot_driver.busy_times", return_value=durations),
                    self.assertRaisesRegex(
                        ValueError, label, msg="pilot guard bypassed calibration-live-detail"
                    ),
                ):
                    d.calibration({})
            d = PilotDriver(root, root)
            arrivals = {
                "status": "PASS",
                "offered": 1,
                "rows": [{"workflow_id": "id", "outcome": "ACCEPTED"}],
            }
            with (
                patch.object(d, "pod", return_value={"metadata": {"name": "observer"}}),
                patch.object(d, "k", return_value=subprocess.CompletedProcess([], 1, "")),
                patch.object(d, "python", return_value=json.dumps(arrivals)),
                patch.object(d, "sample"),
                self.assertRaisesRegex(
                    ValueError, "pilot-loadgen-exit", msg="pilot guard bypassed loadgen-exit"
                ),
            ):
                d.arrivals("ns", "db", "measured", 1, 1)

    def test_ci_and_carry_refusal_details(self) -> None:
        for label, outputs in (
            ("pilot-ci-source-inventory", [b"", b""]),
            ("pilot-ci-uncommitted-tooling", [b"go.mod\0", b"new-code.py"]),
        ):
            with (
                patch.object(
                    evidence.subprocess,
                    "run",
                    side_effect=[subprocess.CompletedProcess([], 0, x) for x in outputs],
                ),
                patch.object(evidence, "committed_bytes", return_value=b"source"),
                patch.object(evidence, "github", return_value={"workflow_runs": []}),
                patch.dict(os.environ, {"GITHUB_REPOSITORY": "fixture/repo"}),
                self.assertRaisesRegex(ValueError, label, msg="pilot guard bypassed ci-input"),
            ):
                evidence.ci_proof(ROOT)
        previous = {
            "job": "k3-pilot-1a",
            "status": "CARRY_OVER",
            "cleanup": {"status": "PASS"},
            "completed": [],
            "carry_over": {
                "job": "k3-pilot-1a-c1",
                "reason": "pilot-job-fit",
                "started": False,
                "cells": [["medium", 3]],
            },
        }
        with (
            patch.object(evidence, "artifacts", return_value=[(ROOT, previous)]),
            self.assertRaisesRegex(
                ValueError, "pilot-no-replay-job", msg="pilot guard bypassed no-replay"
            ),
        ):
            evidence.dependency_plan(ROOT, "k3-pilot-1a")
        for label, edit in (
            ("pilot-carry-predecessor-safe", lambda m: m.update(status="FAIL")),
            ("pilot-carry-order", lambda m: m["carry_over"].update(cells=[["medium", 2]])),
        ):
            bad = copy.deepcopy(previous)
            edit(bad)
            with (
                patch.object(evidence, "artifacts", return_value=[(ROOT, bad)]),
                self.assertRaisesRegex(
                    ValueError, label, msg="pilot guard bypassed carry-predecessor"
                ),
            ):
                evidence.dependency_plan(ROOT, "k3-pilot-1a-c1")
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            (directory / "warmup-snapshot.json").write_text("{}")
            with (
                patch.object(
                    evidence.subprocess,
                    "run",
                    return_value=subprocess.CompletedProcess([], 1, b'{"valid":false}'),
                ),
                self.assertRaisesRegex(
                    ValueError,
                    "pilot-freeze-independent-checker",
                    msg="pilot guard bypassed independent-checker",
                ),
            ):
                evidence.recheck(directory, Path("checker"))

    def test_precise_dependency_and_file_guards(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "bytes"
            path.write_bytes(b"expected")
            with (
                patch.object(Path, "is_symlink", return_value=True),
                patch.object(
                    pilot.subprocess,
                    "run",
                    return_value=subprocess.CompletedProcess([], 0, b"expected"),
                ),
                self.assertRaisesRegex(
                    ValueError,
                    "pilot-committed-symlink",
                    msg="pilot guard bypassed committed-symlink",
                ),
            ):
                pilot.committed_bytes(root, path)
            artifact = root / "artifact"
            manifest = fixture_artifact(artifact, cells=[("short", 0)])
            with (
                patch.object(Path, "is_symlink", return_value=True),
                self.assertRaisesRegex(
                    ValueError,
                    "pilot-artifact-symlinks",
                    msg="pilot guard bypassed artifact-symlinks",
                ),
            ):
                pilot.read_artifact(artifact)
            for label, edit in (
                ("pilot-artifact-identity", lambda m: m.update(run_id="non-numeric")),
                ("pilot-dependency-initial-missing", lambda m: m.update(completed=[])),
            ):
                bad = copy.deepcopy(manifest)
                edit(bad)
                save(artifact / "manifest.json", bad)
                with self.assertRaisesRegex(
                    ValueError, label, msg="pilot guard bypassed dependency-metadata"
                ):
                    pilot.initial_dependency(artifact, "short")
            save(artifact / "manifest.json", manifest)
            record = artifact / "short-0/record.json"
            original = record.read_bytes()
            for label, edit in (
                ("pilot-dependency-cell", lambda r: r.update(arm="medium")),
                ("pilot-dependency-br", lambda r: r.update(b=100)),
            ):
                value = json.loads(original)
                edit(value)
                save(record, value)
                manifest["artifact_sha256"] = artifact_hashes(artifact)
                save(artifact / "manifest.json", manifest)
                with self.assertRaisesRegex(
                    ValueError, label, msg="pilot guard bypassed dependency-record"
                ):
                    pilot.initial_dependency(artifact, "short")
            record.write_bytes(original)
            (artifact / "short-0/samples.jsonl").write_text("")
            with self.assertRaisesRegex(
                ValueError, "pilot-samples-required", msg="pilot guard bypassed samples-required"
            ):
                pilot.summarize(artifact / "short-0")
        for outcome in pilot.SATURATION:
            with self.assertRaisesRegex(
                ValueError, "pilot-generator-saturation", msg="pilot guard bypassed saturation"
            ):
                pilot.window_ok(
                    {
                        "status": "PASS",
                        "offered": 1,
                        "rows": [{"workflow_id": "id", "outcome": outcome}],
                    },
                    1,
                )
        with self.assertRaisesRegex(
            ValueError, "pilot-arrival-identities", msg="pilot guard bypassed identities"
        ):
            pilot.window_ok(
                {
                    "status": "PASS",
                    "offered": 2,
                    "rows": [{"workflow_id": "id", "outcome": "ACCEPTED"}] * 2,
                },
                2,
            )
        with tempfile.TemporaryDirectory() as tmp:
            directory = fixture_run(Path(tmp))
            witness = json.loads((directory / "witness.json").read_bytes())
            witness["expected_pods"] = ["pod-1"]
            witness["ledgers"] = witness["ledgers"][:1]
            with self.assertRaisesRegex(
                ValueError, "pilot-four-slot-witness", msg="pilot guard bypassed four-slot-witness"
            ):
                pilot.witness_events(witness)
            clock = json.loads((directory / "clock.json").read_bytes())
            clock["setup"]["observer_after"] = "2000-01-01T00:00:00+00:00"
            save(directory / "clock.json", clock)
            with self.assertRaisesRegex(
                ValueError, "pilot-observer-clock-order", msg="pilot guard bypassed observer-clock"
            ):
                pilot.summarize(directory)

    def test_driver_checker_and_calibration_failures(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            directory = fixture_run(root)
            sample = json.loads((directory / "samples.jsonl").read_text())
            for pod in sample["pods"]:
                pod["status"]["podIP"] = "127.0.0.1"
            warmup = json.loads((directory / "warmup-arrivals.json").read_bytes())
            witness = json.loads((directory / "witness.json").read_bytes())
            for offline_failure in (False, True):
                d = PilotDriver(root, root)
                d.terminal_observed = {r["workflow_id"]: "observed" for r in warmup["rows"]}
                clock = iter(range(0, 2000, 10))

                def k(
                    *args: str, offline_failure=offline_failure, **kwargs: object
                ) -> subprocess.CompletedProcess:
                    if "pods" in args:
                        return subprocess.CompletedProcess(
                            [], 0, json.dumps({"items": sample["pods"]})
                        )
                    if "exec" in args and "/checker" in args:
                        ok = offline_failure and "-snapshot" not in args
                        return subprocess.CompletedProcess(
                            [],
                            0 if ok else 1,
                            json.dumps(
                                {"valid": True, "trace": {}}
                                if ok
                                else {"fatal": "database unavailable"}
                            ),
                        )
                    return subprocess.CompletedProcess([], 0, "")

                with (
                    patch.object(d, "sample"),
                    patch.object(d, "k", side_effect=k),
                    patch.object(d, "python", return_value="stable witness inventory"),
                    patch.object(d, "collect_witness", return_value=witness),
                    patch.object(d, "pod", return_value={"metadata": {"name": "observer"}}),
                    patch.object(d, "remote_json"),
                    patch(
                        "dur053_pilot_driver.time.monotonic",
                        side_effect=lambda clock=clock: next(clock),
                    ),
                    patch("dur053_pilot_driver.time.sleep"),
                    self.assertRaisesRegex(
                        ValueError,
                        "pilot-offline-checker" if offline_failure else "pilot-live-checker",
                        msg="pilot guard bypassed driver-checker",
                    ),
                ):
                    d.reconcile("ns", "db", [warmup], "checker-fail-" + str(offline_failure))
            d = PilotDriver(root, root)
            d.images = {"worker": "pinned-image"}
            arrival = {"rows": [{"workflow_id": "cal"}]}
            with (
                patch.object(d, "setup", return_value=("ns", "db")),
                patch.object(d, "arrivals", return_value=arrival),
                patch.object(d, "reconcile", side_effect=TimeoutError("timeout")),
                patch.object(d, "failure_export"),
                self.assertRaises(TimeoutError),
            ):
                d.calibration({})
            self.assertEqual(json.loads((root / "calibration.json").read_bytes())["status"], "FAIL")

    def test_freeze_reproducible_and_calibration_guards(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repository = Path(tmp)
            available = []
            for n, (job, cells) in enumerate(pilot.JOBS.items()):
                root = repository / pilot.EVIDENCE / job / str(n + 10)
                manifest = fixture_artifact(
                    root, cells=cells, job=job, run=str(n + 10), job_id=str(n + 20)
                )
                if job == "k3-pilot-1b":
                    fixture_calibration(root)
                if job == "k3-pilot-3":
                    initial = available[2][0]
                    manifest["dependencies"]["long"] = {
                        "manifest_sha256": hashlib.sha256(
                            (initial / "manifest.json").read_bytes()
                        ).hexdigest(),
                        "b": 0.1,
                        "r": 40,
                    }
                manifest["artifact_sha256"] = artifact_hashes(root)
                save(root / "manifest.json", manifest)
                available.append((root, manifest))
            with (
                patch.object(evidence, "artifacts", return_value=available),
                patch.object(evidence, "recheck"),
            ):
                ledger_path = (
                    repository / "experiments/portfolio/k8s-recovery/live-time-ledger.json"
                )
                save(
                    ledger_path,
                    {
                        "schema": "dur053.live-ledger.v1",
                        "limit_seconds": 259200,
                        "entries": [
                            {
                                **{
                                    k: m[k]
                                    for k in (
                                        "run_id",
                                        "job_id",
                                        "head_sha",
                                        "merge_ref_sha",
                                        "tree_sha",
                                        "status",
                                        "charged_seconds",
                                    )
                                },
                                "category": m["job"],
                                "artifact": (p / "manifest.json")
                                .relative_to(repository)
                                .as_posix(),
                                "artifact_sha256": hashlib.sha256(
                                    (p / "manifest.json").read_bytes()
                                ).hexdigest(),
                            }
                            for p, m in available
                        ],
                    },
                )
                with patch.object(
                    evidence, "committed_bytes", side_effect=lambda _repo, path: path.read_bytes()
                ):
                    result = evidence.freeze(repository, Path("checker-fixture"))
                with patch.object(
                    evidence, "committed_bytes", side_effect=lambda _repo, path: path.read_bytes()
                ):
                    self.assertEqual(result, evidence.freeze(repository, Path("checker-fixture")))
                self.assertEqual(result["observed_schedule"]["campaign_episodes"], 438)
                self.assertEqual(set(result["arms"]), set(pilot.ARMS))
                self.assertEqual(
                    {a: result["arms"][a]["L"] for a in pilot.ARMS},
                    dict.fromkeys(pilot.ARMS, 2.004),
                    "freeze L must reproduce durable timestamp latencies",
                )
                self.assertFalse(any("under overload" in x for x in result["limitations"]))
                for arm in pilot.ARMS:
                    self.assertEqual(
                        result["arms"][arm]["repeats"],
                        [
                            {
                                "index": i,
                                "b": 0.1,
                                "measured_rate": 20.0,
                                "implied_utilisation": 0.5,
                            }
                            for i in range(1, 4)
                        ],
                        "repeat load disclosure missing",
                    )
                    self.assertEqual(
                        result["observed_schedule"]["busy_time_sensitivity"][arm],
                        {"initial_b": 0.1, "max_repeat_b": 0.1},
                    )
                original_summarize = evidence.summarize
                original_rates = pilot.rates
                # Keep all fixture validation; vary only the measured summary's busy time.
                # Exercise each OR branch and the strict equality boundaries independently.
                for busy, rate, limited in (
                    (0.15, 20, False),
                    (0.151, 20, True),
                    (0.1, 40, False),
                    (0.1, 40.01, True),
                    (0.3, 20, True),
                ):

                    def measured_summary(
                        directory: Path, *, busy: float = busy, **kwargs: object
                    ) -> dict:
                        summary = original_summarize(directory, **kwargs)
                        if directory.name == "cpu-1":
                            summary["b"] = busy
                        return summary

                    # Registered rates remain valid: for utilisation-only cases, exercise
                    # disclosure arithmetic with an initial reference yielding that rate.
                    def fixture_rates(
                        arm: str, index: int, initial: dict, rate: float = rate
                    ) -> tuple:
                        warm, measured = original_rates(arm, index, initial)
                        return (warm, rate) if arm == "cpu" and index == 1 else (warm, measured)

                    cpu_root = available[1][0]
                    record_path = cpu_root / "cpu-1/record.json"
                    arrival_path = cpu_root / "cpu-1/measured-arrivals.json"
                    before_arrival = arrival_path.read_bytes()
                    arrival = json.loads(before_arrival)
                    arrival["rate"] = rate
                    save(arrival_path, arrival)
                    before_record = record_path.read_bytes()
                    record = json.loads(before_record)
                    record["measured_rate"] = rate
                    save(record_path, record)
                    try:
                        with (
                            patch.object(evidence, "summarize", side_effect=measured_summary),
                            patch.object(pilot, "rates", side_effect=fixture_rates),
                            patch.object(
                                evidence,
                                "committed_bytes",
                                side_effect=lambda _repo, path: path.read_bytes(),
                            ),
                        ):
                            observed = evidence.freeze(repository, Path("checker-fixture"))
                    finally:
                        record_path.write_bytes(before_record)
                        arrival_path.write_bytes(before_arrival)
                    self.assertEqual(
                        observed["arms"]["cpu"]["repeats"][0],
                        {
                            "index": 1,
                            "b": busy,
                            "measured_rate": rate,
                            "implied_utilisation": rate * busy / 4,
                        },
                        "repeat load disclosure missing",
                    )
                    limitations = [x for x in observed["limitations"] if "under overload" in x]
                    self.assertEqual(
                        bool(limitations), limited, "repeat overload disclosure missing"
                    )
                    if limited:
                        self.assertIn(
                            "scheduling reference does not hold under load", limitations[0]
                        )
                    self.assertEqual(
                        observed["observed_schedule"]["busy_time_sensitivity"]["cpu"],
                        {"initial_b": 0.1, "max_repeat_b": max(0.1, busy)},
                    )
                    # Sensitivity must never alter any existing scheduling calculation or L/r/b.
                    for key in result["observed_schedule"]:
                        if key != "busy_time_sensitivity":
                            self.assertEqual(
                                observed["observed_schedule"][key], result["observed_schedule"][key]
                            )
                    for key in ("b", "r", "L", "arrival_concurrency_cap"):
                        self.assertEqual(observed["arms"]["cpu"][key], result["arms"]["cpu"][key])
                for label, edit in (
                    ("all-cells", lambda: available.pop()),
                    ("duplicate-cell", lambda: available[0][1]["completed"].append("short-0")),
                    ("tooling-proof", lambda: available[0][1].update(error="failed")),
                    (
                        "environment",
                        lambda: available[0][1]["artifact_sha256"].pop("versions.json"),
                    ),
                ):
                    before = copy.deepcopy(available)
                    edit()
                    with (
                        self.assertRaises(ValueError, msg=f"pilot guard bypassed freeze-{label}"),
                        patch.object(
                            evidence,
                            "committed_bytes",
                            side_effect=lambda _repo, path: path.read_bytes(),
                        ),
                        patch.object(
                            evidence,
                            "committed_ledger",
                            return_value={
                                "entries": [
                                    row
                                    for row in json.loads(ledger_path.read_bytes())["entries"]
                                    if label != "all-cells"
                                    or row["artifact"]
                                    in {
                                        (p / "manifest.json").relative_to(repository).as_posix()
                                        for p, _ in available
                                    }
                                ]
                            },
                        ),
                    ):
                        evidence.freeze(repository, Path("checker-fixture"))
                    available[:] = before
                for label, relative, edit in (
                    (
                        "pilot-freeze-rates",
                        "short-1/record.json",
                        lambda x: x.update(measured_rate=0.5),
                    ),
                    (
                        "pilot-freeze-cpu-units-image",
                        "cpu-0/record.json",
                        lambda x: x.update(cpu_units=100000),
                    ),
                    (
                        "pilot-calibration-before-initial",
                        "cpu-0/record.json",
                        lambda x: x.update(calibration_persisted_ns=999),
                    ),
                ):
                    target_root = available[1][0] if relative.startswith("cpu") else available[0][0]
                    path = target_root / relative
                    original = path.read_bytes()
                    value = json.loads(original)
                    edit(value)
                    save(path, value)
                    arrivals_path = target_root / "short-1/measured-arrivals.json"
                    if "rates" in label:
                        original_arrivals = arrivals_path.read_bytes()
                        arrivals = json.loads(original_arrivals)
                        arrivals["rate"] = 0.5
                        save(arrivals_path, arrivals)
                    with (
                        patch.object(
                            evidence,
                            "committed_bytes",
                            side_effect=lambda _repo, path: path.read_bytes(),
                        ),
                        self.assertRaisesRegex(
                            ValueError, label, msg="pilot guard bypassed freeze-derived-record"
                        ),
                    ):
                        evidence.freeze(repository, Path("checker-fixture"))
                    path.write_bytes(original)
                    if "rates" in label:
                        arrivals_path.write_bytes(original_arrivals)
                current_manifest = available[-1][0] / "manifest.json"
                original = current_manifest.read_bytes()
                value = json.loads(original)
                value["dependencies"]["long"]["b"] = 999
                save(current_manifest, value)
                ledger_original = ledger_path.read_bytes()
                ledger_value = json.loads(ledger_original)
                ledger_value["entries"][-1]["artifact_sha256"] = hashlib.sha256(
                    current_manifest.read_bytes()
                ).hexdigest()
                save(ledger_path, ledger_value)
                with (
                    patch.object(
                        evidence,
                        "committed_bytes",
                        side_effect=lambda _repo, path: path.read_bytes(),
                    ),
                    self.assertRaisesRegex(
                        ValueError,
                        "pilot-freeze-dependency",
                        msg="pilot guard bypassed freeze-dependency",
                    ),
                ):
                    evidence.freeze(repository, Path("checker-fixture"))
                current_manifest.write_bytes(original)
                ledger_path.write_bytes(ledger_original)
                available[0][1]["completed"].pop()
                with (
                    patch.object(
                        evidence,
                        "committed_bytes",
                        side_effect=lambda _repo, path: path.read_bytes(),
                    ),
                    self.assertRaisesRegex(
                        ValueError,
                        "pilot-freeze-run-inventory",
                        msg="pilot guard bypassed freeze-run-inventory",
                    ),
                ):
                    evidence.freeze(repository, Path("checker-fixture"))
                available[0][1]["completed"].append("medium-3")
                ledger_value = json.loads(ledger_original)
                ledger_value["entries"].pop(0)
                save(ledger_path, ledger_value)
                with (
                    patch.object(
                        evidence,
                        "committed_bytes",
                        side_effect=lambda _repo, path: path.read_bytes(),
                    ),
                    self.assertRaisesRegex(
                        ValueError,
                        "pilot-freeze-ledger-entry",
                        msg="pilot guard bypassed freeze-ledger-entry",
                    ),
                ):
                    evidence.freeze(repository, Path("checker-fixture"))
                ledger_path.write_bytes(ledger_original)
                fixture_calibration(available[0][0])
                with (
                    patch.object(
                        evidence,
                        "committed_bytes",
                        side_effect=lambda _repo, path: path.read_bytes(),
                    ),
                    self.assertRaisesRegex(
                        ValueError,
                        "pilot-freeze-cpu-calibration",
                        msg="pilot guard bypassed freeze-cpu-calibration",
                    ),
                ):
                    evidence.freeze(repository, Path("checker-fixture"))
            cpu = available[1][0]
            for edit in (
                lambda x: x.update(status="FAIL"),
                lambda x: x.update(units=100000),
                lambda x: x["candidates"][0].update(invocation_elapsed_seconds=[121] * 22),
                lambda x: (
                    x.update(median_busy_seconds=2),
                    x["candidates"][0].update(measured_busy_seconds=[2] * 21),
                ),
            ):
                path = cpu / "calibration.json"
                raw = path.read_bytes()
                bad = json.loads(raw)
                edit(bad)
                save(path, bad)
                save(
                    cpu / "calibration-seal.json",
                    {"sha256": hashlib.sha256(path.read_bytes()).hexdigest()},
                )
                with self.assertRaises(ValueError, msg="pilot guard bypassed calibration-evidence"):
                    evidence.verify_calibration(cpu)
                path.write_bytes(raw)
            save(cpu / "calibration-seal.json", {"sha256": "bad"})
            with self.assertRaises(ValueError, msg="pilot guard bypassed calibration-seal"):
                evidence.verify_calibration(cpu)

    def test_more_evidence_guards(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            directory = fixture_run(root)
            record_path = directory / "record.json"
            record = json.loads(record_path.read_bytes())
            record["b"] = 99
            save(record_path, record)
            save(
                directory / "seal.json",
                {k: v for k, v in artifact_hashes(directory).items() if k != "seal.json"},
            )
            with self.assertRaises(ValueError, msg="pilot guard bypassed same-job-br"):
                evidence.same_job_initial(root, "short")
            sample_path = directory / "samples.jsonl"
            sample = json.loads(sample_path.read_text())
            for edit in (
                lambda x: x.update(connections=93),
                lambda x: x.update(monotonic_ns=-1),
                lambda x: x["worker_deployment"]["spec"].update(replicas=3),
                lambda x: x["pods"][0]["spec"]["containers"][0]["resources"].update(limits={}),
                lambda x: x.update(terminal_ids=[]),
            ):
                bad = copy.deepcopy(sample)
                edit(bad)
                sample_path.write_text(json.dumps(bad) + "\n")
                with self.assertRaises(ValueError, msg="pilot guard bypassed samples"):
                    pilot.summarize(directory)
            sample_path.write_text("")
            with self.assertRaises(ValueError, msg="pilot guard bypassed samples-required"):
                pilot.summarize(directory)
            export = json.loads((directory / "witness.json").read_bytes())
            original = json.loads(base64.b64decode(export["ledgers"][0]["data"]).splitlines()[1])
            for edit in (
                lambda rows: rows[1].update(sequence=999),
                lambda rows: rows.pop(),
                lambda rows: rows[2].update(outcome="FAILED"),
            ):
                bad = copy.deepcopy(export)
                ledger = bad["ledgers"][0]
                rows = [json.loads(line) for line in base64.b64decode(ledger["data"]).splitlines()]
                edit(rows)
                # For orphan ends, preserve contiguous record sequence to isolate pairing.
                for i, row in enumerate(rows):
                    if row["sequence"] != 999:
                        row["sequence"] = i + 1
                raw = ("\n".join(json.dumps(row) for row in rows) + "\n").encode()
                ledger["data"] = base64.b64encode(raw).decode()
                ledger["inventory"].update(
                    sha256=hashlib.sha256(raw).hexdigest(), attempted=len(rows), written=len(rows)
                )
                with self.assertRaises(ValueError, msg="pilot guard bypassed witness-events"):
                    pilot.busy_times(bad, {original["workflow_id"]})
            with self.assertRaises(ValueError, msg="pilot guard bypassed empty-windows"):
                pilot.combine([])

    def test_intake_provenance_and_termination(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source, repository = root / "download", root / "repository"
            manifest = fixture_artifact(source)
            ledger = repository / "experiments/portfolio/k8s-recovery/live-time-ledger.json"
            ledger.parent.mkdir(parents=True)
            save(
                ledger, {"schema": "dur053.live-ledger.v1", "limit_seconds": 259200, "entries": []}
            )
            for edit in (
                lambda x: x.update(status="UNKNOWN"),
                lambda x: x.update(charged_seconds=1),
                lambda x: x.update(head_sha="invalid"),
                lambda x: x.update(run_id="../../"),
            ):
                bad = copy.deepcopy(manifest)
                edit(bad)
                save(source / "manifest.json", bad)
                with self.assertRaises(ValueError, msg="pilot guard bypassed intake-metadata"):
                    intake.record(source, repository)
            save(source / "manifest.json", manifest)
            target = repository / pilot.EVIDENCE / manifest["job"] / manifest["run_id"]
            target.mkdir(parents=True)
            with self.assertRaises(ValueError, msg="pilot guard bypassed intake-existing"):
                intake.record(source, repository)
            failed = root / "termination"
            failed.mkdir()
            save(
                failed / "actions-run.json",
                {"id": 50, "head_sha": "a" * 40, "event": "workflow_dispatch", "run_attempt": 1},
            )
            job = {"id": 60, "run_id": 50, "name": "DUR-053 campaign", "conclusion": "timed_out"}
            save(failed / "actions-job.json", job)
            save(
                failed / "actions-commit.json",
                {"sha": "a" * 40, "commit": {"tree": {"sha": "c" * 40}}},
            )
            with (
                patch.object(Path, "is_symlink", return_value=True),
                self.assertRaisesRegex(
                    ValueError,
                    "pilot-interrupted-symlinks",
                    msg="pilot guard bypassed interrupted-symlinks",
                ),
            ):
                intake.interrupted(failed, repository, "k3-pilot-2")
            target = intake.interrupted(failed, repository, "k3-pilot-2")
            recorded = json.loads((target / "manifest.json").read_bytes())
            self.assertEqual(
                recorded["charged_seconds"], 18900, "pilot guard bypassed interrupted-charge"
            )
            self.assertEqual(
                (target / "downloaded/actions-run.json").read_bytes(),
                (failed / "actions-run.json").read_bytes(),
            )
            with self.assertRaises(ValueError, msg="pilot guard bypassed interrupted-existing"):
                intake.interrupted(failed, repository, "k3-pilot-2")
            job["conclusion"] = "success"
            save(failed / "actions-job.json", job)
            with self.assertRaisesRegex(
                ValueError,
                "pilot-interrupted-actions",
                msg="pilot guard bypassed interrupted-actions",
            ):
                intake.interrupted(failed, repository, "k3-pilot-3")
            job.update(conclusion="timed_out", run_id=51)
            save(failed / "actions-job.json", job)
            save(
                failed / "actions-run.json",
                {"id": 51, "head_sha": "a" * 40, "event": "workflow_dispatch", "run_attempt": 1},
            )
            save(
                failed / "actions-commit.json",
                {"sha": "b" * 40, "commit": {"tree": {"sha": "c" * 40}}},
            )
            with self.assertRaisesRegex(
                ValueError,
                "pilot-interrupted-commit",
                msg="pilot guard bypassed interrupted-commit",
            ):
                intake.interrupted(failed, repository, "k3-pilot-1b")
            save(
                failed / "actions-commit.json",
                {"sha": "a" * 40, "commit": {"tree": {"sha": "c" * 40}}},
            )
            with self.assertRaisesRegex(
                ValueError,
                "pilot-intake-duplicate-job",
                msg="pilot guard bypassed interrupted-duplicate",
            ):
                intake.interrupted(failed, repository, "k3-pilot-3")

    def test_driver_slot_and_pool_observers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            directory = fixture_run(root)
            sample = json.loads((directory / "samples.jsonl").read_text())
            current = {"value": sample}
            d = PilotDriver(root, root)
            d.expected_workers = {"pod-1", "pod-2"}
            for pod in sample["pods"]:
                pod["status"]["phase"] = "Running"

            def k(*args: str, **kwargs: object) -> subprocess.CompletedProcess:
                if "pods" in args:
                    value = {"items": current["value"]["pods"]}
                elif "deployment/worker" in args:
                    value = current["value"]["worker_deployment"]
                else:
                    value = current["value"]["scaledobject"]
                return subprocess.CompletedProcess([], 0, json.dumps(value))

            with (
                patch.object(d, "k", side_effect=k),
                patch.object(d, "sql", side_effect=["20", ""]),
            ):
                d.sample("namespace", "database")
            for change, sql_value in (
                (lambda x: x["pods"][0]["status"].update(phase="Pending"), "20"),
                (lambda x: x["worker_deployment"]["spec"].update(replicas=3), "20"),
                (lambda x: None, "93"),
            ):
                before = copy.deepcopy(sample)
                change(sample)
                with (
                    patch.object(d, "k", side_effect=k),
                    patch.object(d, "sql", return_value=sql_value),
                    self.assertRaises(ValueError, msg="pilot guard bypassed live-observer"),
                ):
                    d.sample("namespace", "database")
                sample = before
                current["value"] = sample

    def test_arithmetic_and_admission(self) -> None:
        self.assertEqual(pilot.reference([0.1] * 160 + [0] * 40, 200), {"b": 0.08, "r": 50})
        self.assertEqual(pilot.reference([0], 1)["r"], 400)
        self.assertEqual(pilot.nearest_rank(list(range(1, 201)), 0.99), 198)
        self.assertEqual(pilot.nearest_rank(list(range(1, 22)), 0.5), 11)
        self.assertEqual(pilot.slo([[1.0001] * 200, [2.0001] * 200, [1.5] * 200]), 4.001)
        original = pilot.JOBS["k3-pilot-1a"] + pilot.JOBS["k3-pilot-1b"]
        self.assertEqual(pilot.full_reservation(original), 20384)
        with self.assertRaisesRegex(
            ValueError, "pilot-job-fit", msg="pilot guard bypassed job-fit"
        ):
            pilot.admit(original)
        for name, expected in zip(pilot.JOBS, (14364, 14840, 11280, 15840), strict=True):
            self.assertEqual(
                pilot.admit(pilot.JOBS[name], calibration=name.endswith("1b")), expected
            )
        self.assertEqual(pilot.CALIBRATION_RESERVATION, 7920)
        with self.assertRaisesRegex(
            ValueError, "pilot-ledger-fit", msg="pilot guard bypassed ledger-fit"
        ):
            pilot.admit([("long", 0)], ledger_remaining=1000)
        with self.assertRaisesRegex(
            ValueError, "pilot-job-fit", msg="pilot guard bypassed elapsed-fit"
        ):
            pilot.admit([("short", 1)], elapsed=18000, setup=False)
        self.assertEqual(pilot.rates("cpu", 0), (1, 1))
        self.assertEqual(pilot.rates("short", 1, {"r": 3}), (0.75, 1.5))
        for f, args in (
            (pilot.nearest_rank, ([], 0.99)),
            (pilot.reference, ([], 200)),
            (pilot.slo, ([[1] * 199] * 3,)),
            (pilot.rates, ("bad", 1)),
        ):
            with self.assertRaises(ValueError, msg="pilot guard bypassed numeric-input"):
                f(*args)
        for value in (float("nan"), float("inf"), -1, True):
            with self.assertRaises(ValueError, msg="pilot guard bypassed finite"):
                pilot.finite(value, "invalid")

    def test_schedule(self) -> None:
        old = json.loads(
            (ROOT / "experiments/portfolio/k8s-recovery/amendment-b-schedule.json").read_bytes()
        )
        new = json.loads(
            (ROOT / "experiments/portfolio/k8s-recovery/amendment-b2-schedule.json").read_bytes()
        )
        self.assertEqual(plan(), old)
        self.assertEqual(plan_b2(), new)
        self.assertEqual(
            (len(new["jobs"]), new["campaign_episodes"], new["expected_total_seconds"]),
            (16, 438, 225283.5),
        )
        self.assertEqual(new["jobs"][4:], old["jobs"][3:])

    def test_carry_over(self) -> None:
        pending = [("short", 3), ("medium", 0)]
        self.assertEqual(
            pilot.carry_over("k3-pilot-1a", pending, "pilot-job-fit", False)["cells"],
            [list(x) for x in pending],
        )
        self.assertEqual(
            pilot.carry_over("k3-pilot-1a-c1", pending, "pilot-job-fit", False)["job"],
            "k3-pilot-1a-c2",
        )
        for reason, started in (
            ("unsafe", False),
            ("timeout", False),
            ("saturated", False),
            ("incomplete", False),
            ("pilot-job-fit", True),
        ):
            with self.assertRaisesRegex(
                ValueError, "pilot-carry-time-only", msg="pilot guard bypassed carry-time-only"
            ):
                pilot.carry_over("k3-pilot-1a", pending, reason, started)
        for name in ("pilot", "k3-pilot-1", "k3-pilot-1b-c0", "k3-pilot-1b-c1/../../"):
            with self.assertRaises(ValueError, msg="pilot guard bypassed job-name"):
                pilot.job_base(name)

    def test_calibration_selection(self) -> None:
        def candidate(units: int, value: float) -> dict:
            return {
                "units": units,
                "status": "PASS",
                "discarded": 1,
                "concurrency": 1,
                "timeout_seconds": 120,
                "measured_busy_seconds": [value] * 21,
            }

        rows = [candidate(10000, 0.1), candidate(100000, 1)]
        self.assertEqual(pilot.select_cpu(rows)["units"], 100000)
        self.assertIsNone(pilot.select_cpu(rows)["limitation"])
        rows.append(candidate(1000000, 0.8))
        rows[1]["measured_busy_seconds"] = [0.3] * 21
        self.assertIsNotNone(pilot.select_cpu(rows)["limitation"])
        for edit in (
            lambda x: x[0].update(units=100000),
            lambda x: x[0].update(status="TIMEOUT"),
            lambda x: x[0].update(discarded=0),
            lambda x: x[0].update(concurrency=2),
            lambda x: x[0].update(timeout_seconds=121),
            lambda x: x[0].update(measured_busy_seconds=[1] * 20),
        ):
            bad = copy.deepcopy(rows)
            edit(bad)
            with self.assertRaises(ValueError, msg="pilot guard bypassed calibration"):
                pilot.select_cpu(bad)
        with self.assertRaises(ValueError, msg="pilot guard bypassed calibration-first"):
            pilot.select_cpu([candidate(10000, 1), candidate(100000, 2)])
        with self.assertRaises(ValueError, msg="pilot guard bypassed calibration-incomplete"):
            pilot.select_cpu([candidate(10000, 0.1)])

    def test_summary_blocks_bad_windows_and_safety(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            directory = fixture_run(Path(tmp))
            self.assertEqual(pilot.summarize(directory)["count"], 200)

            def change(file: str, edit: object) -> None:
                path = directory / file
                before = path.read_bytes()
                value = json.loads(before)
                edit(value)
                save(path, value)
                try:
                    with self.assertRaises(ValueError, msg="pilot guard bypassed summary"):
                        pilot.summarize(directory)
                finally:
                    path.write_bytes(before)

            for edit in (
                lambda x: x.update(status="FAIL"),
                lambda x: x.update(timed_out=True),
                lambda x: x.update(engineering=True),
                lambda x: x.update(slots=8),
                lambda x: x.update(faults=["kill"]),
                lambda x: x.update(autoscaling="enabled"),
                lambda x: x.update(measured_start_ns=1),
                lambda x: x.update(warmup_end_ns=1),
                lambda x: x.update(measured_rate=2),
                lambda x: x.update(warmup_quiet_seconds=29),
                lambda x: x.update(reconciliation_timeout_seconds=599),
            ):
                change("record.json", edit)
            for outcome in (*pilot.SATURATION, "UNCERTAIN", "REJECTED"):
                change(
                    "measured-arrivals.json", lambda x, o=outcome: x["rows"][0].update(outcome=o)
                )
            change("measured-arrivals.json", lambda x: x.update(rows=x["rows"][:-1]))
            change(
                "measured-arrivals.json", lambda x: x["rows"][1].update(workflow_id="measured-0")
            )
            change("measured-arrivals.json", lambda x: x["rows"][0].update(workflow_id="warmup-0"))
            change("checker.json", lambda x: x.update(valid=False))
            change("checker.json", lambda x: x["campaign"].update(non_terminal=["missing"]))
            change(
                "snapshot.json", lambda x: x["Snapshot"]["engine.workflow_executions"]["rows"].pop()
            )
            change(
                "snapshot.json",
                lambda x: x["Snapshot"]["engine.workflow_executions"]["rows"][0].update(
                    state="FAILED"
                ),
            )

    def test_witness_and_same_job_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            directory = fixture_run(root)
            self.assertEqual(evidence.same_job_initial(root, "short")["r"], 40)
            (directory / "checker.json").write_text("changed")
            with self.assertRaisesRegex(
                ValueError,
                "pilot-same-job-initial-hashes",
                msg="pilot guard bypassed same-job-hashes",
            ):
                evidence.same_job_initial(root, "short")
        with tempfile.TemporaryDirectory() as tmp:
            directory = fixture_run(Path(tmp))
            export = json.loads((directory / "witness.json").read_bytes())
            for edit in (
                lambda x: x.update(expected_pods=["pod-1"]),
                lambda x: x["ledgers"].pop(),
                lambda x: x["ledgers"][0]["inventory"].update(errors=1),
                lambda x: x["ledgers"][0]["inventory"].update(sha256="bad"),
            ):
                bad = copy.deepcopy(export)
                edit(bad)
                with self.assertRaises(ValueError, msg="pilot guard bypassed witness"):
                    pilot.busy_times(bad, {"measured-0"})
            with self.assertRaises(ValueError, msg="pilot guard bypassed witness-coverage"):
                pilot.busy_times(export, {"not-observed"})

    def test_dependency_job3_and_carry_over(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch.object(evidence, "artifacts", return_value=[]):
                self.assertEqual(
                    evidence.dependency_plan(root, "k3-pilot-1a")[0], pilot.JOBS["k3-pilot-1a"]
                )
                for job in ("k3-pilot-3", "k3-pilot-1a-c1"):
                    with self.assertRaises(
                        ValueError, msg="pilot guard bypassed dependency-required"
                    ):
                        evidence.dependency_plan(root, job)
            directory = root / "long"
            manifest = fixture_artifact(directory, cells=[("long", 0)], job="k3-pilot-2")
            with (
                patch.object(evidence, "artifacts", return_value=[(directory, manifest)]),
                patch.object(
                    pilot, "committed_bytes", side_effect=lambda _repo, path: path.read_bytes()
                ),
            ):
                cells, deps, _ = evidence.dependency_plan(root, "k3-pilot-3")
                self.assertEqual(cells, [("long", 1), ("long", 2), ("long", 3)])
                self.assertEqual(deps["long"]["r"], 40)
                manifest["status"] = "FAIL"
                save(directory / "manifest.json", manifest)
                with self.assertRaises(ValueError, msg="pilot guard bypassed failed-initial"):
                    evidence.dependency_plan(root, "k3-pilot-3")

    def test_artifact_intake_and_engineering_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source, repository = root / "download", root / "repository"
            manifest = fixture_artifact(source)
            ledger = repository / "experiments/portfolio/k8s-recovery/live-time-ledger.json"
            ledger.parent.mkdir(parents=True)
            save(
                ledger, {"schema": "dur053.live-ledger.v1", "limit_seconds": 259200, "entries": []}
            )
            target = intake.record(source, repository)
            self.assertEqual(artifact_hashes(source), artifact_hashes(target))
            self.assertEqual(
                json.loads(ledger.read_bytes())["entries"][0]["category"], "k3-pilot-1a"
            )
            manifest["run_id"] = "11"
            save(source / "manifest.json", manifest)
            with self.assertRaisesRegex(
                ValueError,
                "pilot-intake-duplicate-job",
                msg="pilot guard bypassed intake-duplicate",
            ):
                intake.record(source, repository)
            manifest.update(
                budget_scope="engineering-validation", charged_seconds=0, measurement_claim=False
            )
            save(source / "manifest.json", manifest)
            with self.assertRaises(ValueError, msg="pilot guard bypassed engineering-intake"):
                intake.record(source, repository)
            with self.assertRaises(ValueError, msg="pilot guard bypassed engineering-freeze"):
                pilot.read_artifact(source)
            manifest.update(
                budget_scope="registered-campaign", charged_seconds=100, measurement_claim=True
            )
            save(source / "manifest.json", manifest)
            (source / "images.json").write_text("tampered")
            with self.assertRaises(ValueError, msg="pilot guard bypassed artifact-hashes"):
                pilot.read_artifact(source)

    def test_committed_dependency_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "record.json"
            path.write_bytes(b"expected")
            for result in (
                subprocess.CompletedProcess([], 1, b"", b""),
                subprocess.CompletedProcess([], 0, b"different", b""),
            ):
                with (
                    patch.object(pilot.subprocess, "run", return_value=result),
                    self.assertRaises(ValueError, msg="pilot guard bypassed committed-bytes"),
                ):
                    pilot.committed_bytes(root, path)

    def test_charge_and_freeze_refusals(self) -> None:
        self.assertEqual(campaign.live_charge({}, False, 18900), 1)
        self.assertEqual(
            campaign.live_charge({"status": "PASS", "elapsed_live_seconds": 100}, True, 18900), 100
        )
        for cleanup in (
            {},
            {"status": "FAIL", "elapsed_live_seconds": 2},
            {"status": "PASS", "elapsed_live_seconds": None},
            {"status": "PASS", "elapsed_live_seconds": -1},
        ):
            self.assertEqual(
                campaign.live_charge(cleanup, True, 18900),
                18900,
                "pilot guard bypassed full-termination-charge",
            )
        with (
            patch.object(evidence, "artifacts", return_value=[]),
            self.assertRaisesRegex(
                ValueError,
                "pilot-freeze-artifacts-missing",
                msg="pilot guard bypassed freeze-missing",
            ),
        ):
            evidence.freeze(ROOT, Path("unused"))
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            m = fixture_artifact(root)
            m["status"] = "FAIL"
            with (
                patch.object(evidence, "artifacts", return_value=[(root, m)]),
                self.assertRaisesRegex(
                    ValueError,
                    "pilot-freeze-blocking-job",
                    msg="pilot guard bypassed freeze-failure",
                ),
            ):
                evidence.freeze(ROOT, Path("unused"))

    def test_ci_proof_and_engineering_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output = root / "out"
            with (
                patch.dict(
                    os.environ,
                    {
                        "GITHUB_ACTIONS": "true",
                        "GITHUB_EVENT_NAME": "pull_request",
                        "GITHUB_REPOSITORY": "fixture/repo",
                        "GITHUB_RUN_ID": "10",
                        "DUR053_HEAD_SHA": "a" * 40,
                    },
                ),
                patch.object(
                    campaign,
                    "campaign_gate",
                    side_effect=AssertionError("engineering read campaign ledger"),
                ),
                patch.object(
                    campaign,
                    "github",
                    return_value={"jobs": [{"id": 20, "name": "DUR-053 pilot smoke"}]},
                ),
                patch.object(
                    PilotDriver,
                    "command",
                    return_value=subprocess.CompletedProcess([], 0, "a" * 40, ""),
                ),
                patch.object(PilotDriver, "install", side_effect=ValueError("stop before live")),
                patch.object(
                    PilotDriver,
                    "cleanup",
                    return_value={"status": "PASS", "no_resources_started": True},
                ),
            ):
                self.assertEqual(
                    campaign.run(output, root / "scratch", "pilot-smoke", engineering=True), 1
                )
            manifest = json.loads((output / "manifest.json").read_bytes())
            self.assertEqual(
                manifest["charged_seconds"], 0, "pilot guard bypassed engineering-charge"
            )
            self.assertFalse(manifest["measurement_claim"])
            self.assertEqual(manifest["error"], "stop before live")
        with (
            patch.object(evidence, "github", return_value={"workflow_runs": []}),
            patch.object(
                evidence, "committed_bytes", side_effect=lambda _repo, path: path.read_bytes()
            ),
            patch.object(
                evidence.subprocess,
                "run",
                side_effect=[
                    subprocess.CompletedProcess([], 0, b"go.mod\0", b""),
                    subprocess.CompletedProcess([], 0, b"", b""),
                ],
            ),
            patch.dict(os.environ, {"GITHUB_REPOSITORY": "fixture/repo"}),
            self.assertRaisesRegex(
                ValueError, "pilot-ci-proof-missing", msg="pilot guard bypassed missing-ci-proof"
            ),
        ):
            evidence.ci_proof(ROOT)


if __name__ == "__main__":
    unittest.main()
