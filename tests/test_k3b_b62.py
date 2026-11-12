from __future__ import annotations

import copy
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import dur053_checker_capacity as capacity  # noqa: E402
import dur053_sustained as b6  # noqa: E402
import dur053_sustained_campaign as dispatch  # noqa: E402
import dur053_sustained_freeze as freezing  # noqa: E402
from dur053_k3 import Driver, save  # noqa: E402
from dur053_payload import materialize  # noqa: E402
from dur053_pilot_driver import PilotDriver  # noqa: E402
from dur053_schedule import plan_b6  # noqa: E402
from dur053_sustained_driver import SustainedDriver  # noqa: E402


def pods(reason: str = "OOMKilled", restarts: int = 1) -> dict:
    return {
        "items": [
            {
                "metadata": {
                    "uid": "observer-uid",
                    "namespace": "dur053-kind",
                    "labels": {"app": "observer"},
                },
                "status": {
                    "containerStatuses": [
                        {
                            "name": "observer",
                            "restartCount": restarts,
                            "lastState": {"terminated": {"reason": reason}},
                        }
                    ]
                },
            }
        ],
    }


def failure(root: Path, run: str = "10") -> dict:
    (root / "short-2").mkdir(parents=True)
    (root / "short-2/checker.json").write_bytes(b"")
    (root / "diagnostics").mkdir()
    save(root / "diagnostics/pods-json.txt", pods())
    manifest = {
        "job": "k3-pilot-4a",
        "run_id": run,
        "job_id": run,
        "version": 2,
        "status": "FAIL",
        "cleanup": {"status": "PASS"},
        "error": "K3 guard: pilot-checker-capacity",
        "completed": ["short-1"],
        "planned": [["short", i] for i in (1, 2, 3)],
        "artifact_sha256": {},
        "unperformed": [
            {"arm": "short", "index": 2, "started": True},
            {"arm": "short", "index": 3, "started": False},
        ],
    }
    save(root / "manifest.json", manifest)
    return manifest


class B62Tests(unittest.TestCase):
    def test_observer_only_memory(self) -> None:
        items = json.loads((ROOT / "deploy/k8s/base/resources.yaml").read_bytes())["items"]
        observer = next(
            i for i in items if i["kind"] == "Deployment" and i["metadata"]["name"] == "observer"
        )
        container = next(
            c for c in observer["spec"]["template"]["spec"]["containers"] if c["name"] == "observer"
        )
        self.assertEqual(
            container["resources"],
            {
                "requests": {"cpu": "100m", "memory": "512Mi"},
                "limits": {"cpu": "1000m", "memory": "2Gi"},
            },
        )

    def test_rss_and_threshold(self) -> None:
        self.assertEqual(capacity.max_rss("\tMaximum resident set size (kbytes): 123\n"), 125952)
        for bad in (
            "",
            "Maximum resident set size (kbytes): 0",
            "Maximum resident set size (kbytes): -1",
            "Maximum resident set size (kbytes): 1\n" * 2,
        ):
            with self.subTest(bad=bad), self.assertRaisesRegex(ValueError, "b62-capacity-rss"):
                capacity.max_rss(bad)
        self.assertEqual(capacity.projection(1)["projected_bytes"], 1536 * 1024**2)
        boundary = capacity.LIMIT // 3
        self.assertTrue(capacity.projection(boundary)["pass"], "b62 threshold too strict")
        self.assertFalse(capacity.projection(boundary + 1)["pass"], "b62 threshold bypassed")
        self.assertEqual(capacity.projection(boundary)["selected_v1_estimate_bytes"], boundary)

    def test_capacity_measure_command_and_retained_numbers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            source = repo / b6.EVIDENCE / "k3-pilot-1a/42181882847"
            source.mkdir(parents=True)
            save(source / "manifest.json", {})
            checker = repo / "checker"
            checker.write_bytes(b"test binary")
            output = repo / "runner-temp/b62-capacity.json"

            def timed(command: list, *, stdout: object, env: dict) -> subprocess.CompletedProcess:
                self.assertEqual(
                    command[:4], ["/usr/bin/time", "-v", "-o", str(output.with_suffix(".time.txt"))]
                )
                for flag in ("-snapshot", "-arrivals", "-witness-export", "-require-terminal"):
                    self.assertIn(flag, command)
                self.assertEqual(env["LC_ALL"], "C")
                Path(command[3]).write_text("Maximum resident set size (kbytes): 500000\n")
                stdout.write(b'{"valid":true}')
                return subprocess.CompletedProcess(command, 0)

            with (
                patch.object(capacity, "materialize", side_effect=lambda p, _: nullcontext(p)),
                patch.object(capacity.subprocess, "run", side_effect=timed),
            ):
                numbers = capacity.measure(checker, output, repo)
            self.assertEqual(json.loads(output.read_bytes()), numbers)
            self.assertEqual(numbers["offline_max_rss_bytes"], 512000000)
            self.assertTrue(numbers["pass"])

    def test_observer_baseline_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            driver = SustainedDriver(root, root / "scratch")
            driver.engine_sampling = True
            with (
                patch.object(PilotDriver, "setup", return_value=("ns", "db")),
                patch.object(driver, "observer_status", return_value=pods("", 3)) as read,
            ):
                self.assertEqual(driver.setup(root, {}), ("ns", "db"))
                read.assert_called_once_with("ns")
            self.assertEqual(driver.observer_before, pods("", 3))
            self.assertEqual(json.loads((root / "observer-before.json").read_bytes()), pods("", 3))

    def test_classification(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            m = failure(root)
            self.assertTrue(b6.harness_capacity_failure(root, m), "b62 capacity not classified")
            for key, value in (
                ("status", "PASS"),
                ("cleanup", {"status": "FAIL"}),
                ("error", "K3 guard: pilot-live-checker"),
                ("version", 1),
                ("job", "k3-pilot-1a"),
            ):
                bad = {**m, key: value}
                self.assertFalse(
                    b6.harness_capacity_failure(root, bad), "b62 classification bypassed"
                )
            for verdict in (b'{"valid":false}', b'{"valid":true}', b" ", b"{}"):
                (root / "short-2/checker.json").write_bytes(verdict)
                self.assertFalse(b6.harness_capacity_failure(root, m), "b62 verdict excluded")
            (root / "short-2/checker.json").write_bytes(b"")
            save(root / "diagnostics/pods-json.txt", pods("Error"))
            self.assertFalse(b6.harness_capacity_failure(root, m), "b62 OOM requirement bypassed")
            wrong = pods()
            wrong["items"][0]["status"]["containerStatuses"][0]["name"] = "worker"
            save(root / "diagnostics/pods-json.txt", wrong)
            self.assertFalse(b6.harness_capacity_failure(root, m))
            save(root / "diagnostics/pods-json.txt", pods())
            legacy = {**m, "run_id": "43159046254", "error": "K3 guard: pilot-live-checker"}
            self.assertTrue(b6.harness_capacity_failure(root, legacy))
            self.assertFalse(b6.harness_capacity_failure(root, {**legacy, "run_id": "43159046255"}))
            self.assertFalse(b6.harness_capacity_failure(root, {**legacy, "job": "k3-pilot-4b"}))

    def test_real_committed_failure_and_retained_exclusion(self) -> None:
        path = ROOT / b6.ROOTS[2] / "k3-pilot-4a/43159046254"
        with materialize(path, ROOT) as root:
            m = json.loads((root / "manifest.json").read_bytes())
            self.assertTrue(b6.harness_capacity_failure(root, m), "b62 real failure not classified")
            with tempfile.TemporaryDirectory() as tmp:
                repo = Path(tmp)
                isolated = repo / b6.ROOTS[2] / "k3-pilot-4a/43159046254"
                shutil.copytree(root, isolated)
                with (
                    patch.object(b6, "materialize", side_effect=lambda p, _: nullcontext(p)),
                    patch.object(
                        b6, "summarize", side_effect=AssertionError("failed population read")
                    ),
                ):
                    self.assertEqual(b6.retained(repo, "short", 2), {})

    def test_replay_once_all_indices_and_prestart_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            parent = repo / b6.ROOTS[2] / "k3-pilot-4a"
            first = parent / "10"
            m = failure(first)
            reg = b6.precision(2.0, {})
            with (
                patch.object(dispatch, "registration", return_value=reg),
                patch.object(
                    dispatch,
                    "read_artifact",
                    side_effect=lambda p, _: json.loads((p / "manifest.json").read_bytes()),
                ),
                patch.object(dispatch, "materialize", side_effect=lambda p, _: nullcontext(p)),
            ):
                self.assertEqual(dispatch.dependency_plan(repo, m["job"]), ([1, 2, 3], reg))
                second = parent / "11"
                failure(second, "11")
                with self.assertRaisesRegex(
                    ValueError, "b62-capacity-replay-limit", msg="b62 second replay admitted"
                ):
                    dispatch.dependency_plan(repo, m["job"])
                # B.3 refusal count remains separate from the one capacity replay.
                with patch.object(dispatch, "pre_start_refusal", return_value=True):
                    self.assertEqual(dispatch.dependency_plan(repo, m["job"])[0], [1, 2, 3])
                    failure(parent / "12", "12")
                    with self.assertRaisesRegex(ValueError, "pilot-pre-start-replay-limit"):
                        dispatch.dependency_plan(repo, m["job"])

    def test_retained_excludes_completed_capacity_cells_and_stops_second(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            parent = repo / b6.ROOTS[2] / "k3-pilot-4a"
            failure(parent / "10")
            with (
                patch.object(b6, "materialize", side_effect=lambda p, _: nullcontext(p)),
                patch.object(
                    b6, "summarize", side_effect=AssertionError("b62 failed cell included")
                ),
            ):
                self.assertEqual(b6.retained(repo, "short", 2), {}, "b62 failure not excluded")
                failure(parent / "11", "11")
                with self.assertRaisesRegex(ValueError, "b62-capacity-replay-limit"):
                    b6.retained(repo, "short", 2)

    def test_guard_selection(self) -> None:
        args = (
            "-n",
            "dur053-kind",
            "exec",
            "observer",
            "--",
            "/checker",
            "-arrivals",
            "/evidence/measured-combined-arrivals.json",
            "-namespace",
            "dur053-kind",
        )
        cases = [
            (1, "", "OOMKilled", 1, "pilot-checker-capacity"),
            (0, "", "OOMKilled", 1, "pilot-checker-capacity"),
            (1, "", "Error", 1, "pilot-checker-capacity"),
            (1, "", "Error", 0, "pilot-live-checker"),
            (1, '{"valid":false}', "OOMKilled", 1, "pilot-live-checker"),
            (0, '{"valid":false}', "OOMKilled", 1, "pilot-live-checker"),
            (0, '{"valid":true}', "", 0, None),
            (1, '{"valid":true}', "OOMKilled", 1, "pilot-checker-capacity"),
            (1, "{}", "OOMKilled", 1, "pilot-checker-capacity"),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            driver = SustainedDriver(Path(tmp), Path(tmp) / "scratch")
            driver.observer_before = pods("", 0)
            for code, stdout, reason, restarts, guard in cases:
                with (
                    self.subTest(case=(code, stdout, reason, restarts)),
                    patch.object(
                        Driver, "k", return_value=subprocess.CompletedProcess([], code, stdout)
                    ),
                    patch.object(driver, "observer_status", return_value=pods(reason, restarts)),
                ):
                    if guard:
                        with self.assertRaisesRegex(
                            ValueError, guard, msg="b62 guard selection bypassed"
                        ):
                            driver.k(*args, allowed=True)
                    else:
                        # Normal exits return to the unchanged parent's valid-is-True guard.
                        self.assertEqual(driver.k(*args).stdout, stdout)
            driver.observer_before = pods("", 3)
            with (
                patch.object(Driver, "k", return_value=subprocess.CompletedProcess([], 1, "")),
                patch.object(driver, "observer_status", return_value=pods("Error", 3)),
                self.assertRaisesRegex(ValueError, "pilot-live-checker"),
            ):
                driver.k(*args, allowed=True)

    def test_schedule_replay_reservation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            failure(repo / b6.ROOTS[2] / "k3-pilot-4a/10")
            ledger = {
                "entries": [
                    {"category": "prior", "charged_seconds": 31558, "status": "PASS"},
                    {"category": "k3-pilot-4a", "charged_seconds": 1021, "status": "FAIL"},
                ]
            }
            with (
                patch("dur053_payload.materialize", side_effect=lambda p, _: nullcontext(p)),
                patch("dur053_k3_policy.committed_ledger", return_value=ledger),
                patch.object(b6, "registration", return_value=b6.precision(2.0, {})),
            ):
                plan = plan_b6(repo)
                replay = next(j for j in plan["jobs"] if j.get("job") == "k3-pilot-4a")
                self.assertTrue(replay["capacity_replay"], "b62 replay reservation missing")
                self.assertEqual(replay["expected_seconds"], 5040)
                self.assertEqual(replay["full_admission_seconds"], 5040)
                self.assertEqual(plan["harness_capacity_failures"], ["k3-pilot-4a"])
                self.assertEqual(plan["charged_seconds"], 32579)
                self.assertEqual(
                    plan["expected_total_seconds"],
                    plan["charged_seconds"]
                    + sum(j["expected_seconds"] for j in plan["jobs"])
                    + 1710,
                )
                ledger["entries"].append(
                    {"category": "k3-pilot-4a", "charged_seconds": 3000, "status": "PASS"}
                )
                after = plan_b6(repo)
                self.assertFalse(any(j.get("job") == "k3-pilot-4a" for j in after["jobs"]))
                self.assertEqual(after["charged_seconds"], 35579)

    def test_freeze_failure_retained_outside_populations(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            v1 = repo / b6.EVIDENCE / "k3-pilot-3/9"
            v1.mkdir(parents=True)
            save(
                v1 / "manifest.json",
                {"completed": [f"{a}-{i}" for a in ("medium", "long") for i in (1, 2, 3)]},
            )
            failed = repo / b6.ROOTS[2] / "k3-pilot-4a/10"
            failure(failed)
            relative = (failed / "manifest.json").relative_to(repo).as_posix()
            ledger = {
                "entries": [{"artifact": relative, "job_id": "10", "category": "k3-pilot-4a"}]
            }
            result = {
                "arms": {
                    a: {"b": 1, "r": 4, "L": 2, "repeats": []}
                    for a in ("short", "cpu", "medium", "long")
                },
                "source_artifacts": {},
                "limitations": [],
            }
            rows = {
                i: {"b": 0.1, "latencies": [1.0] * 600, "gate": {"pass": True}} for i in (1, 2, 3)
            }
            with (
                patch.object(freezing, "materialize", side_effect=lambda p, _: nullcontext(p)),
                patch.object(freezing, "committed_ledger", return_value=ledger),
                patch.object(freezing, "stationarity", return_value={"pass": True}),
                patch.object(freezing, "registration", return_value=b6.precision(2.0, {})),
                patch.object(
                    freezing, "retained", side_effect=lambda r, a, v, c: rows if v == 2 else {}
                ),
                patch("dur053_schedule.plan_b6", return_value={"units": []}),
            ):
                frozen = freezing.finish(repo, Path("checker"), copy.deepcopy(result))
                self.assertEqual(frozen["excluded_harness_capacity_failures"], [relative])
                self.assertIn(relative, frozen["source_artifacts"])
                self.assertEqual(frozen["arms"]["short"]["versions"], {2: rows, 3: {}})
                self.assertEqual(frozen["arms"]["short"]["L"], 2)
                ledger["entries"] = []
                with self.assertRaisesRegex(ValueError, "b6-freeze-ledger-entry"):
                    freezing.finish(repo, Path("checker"), copy.deepcopy(result))
