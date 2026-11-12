from __future__ import annotations

import copy
import hashlib
import json
import math
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


def capacity_run(repo: Path, run: str, status: str, *, payload: bool = True) -> Path:
    source = repo / b6.EVIDENCE / f"k3-pilot-1a/{run}"
    source.mkdir(parents=True)
    manifest = {"run_id": run, "status": status}
    raw = json.dumps(manifest, indent=2).encode() + b"\n"
    (source / "manifest.json").write_bytes(raw)
    if payload:
        (source / "payload-index.json").write_text("{}\n")
    ledger_path = repo / capacity.LEDGER
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ledger = json.loads(ledger_path.read_bytes()) if ledger_path.exists() else {"entries": []}
    ledger["entries"].append(
        {
            "category": "k3-pilot-1a",
            "run_id": run,
            "artifact": (source / "manifest.json").relative_to(repo).as_posix(),
            "artifact_sha256": hashlib.sha256(raw).hexdigest(),
        }
    )
    ledger_path.write_text(json.dumps(ledger) + "\n")
    return source


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
            capacity_run(repo, "42181882847", "PASS")
            capacity_run(repo, "42166833080", "FAIL", payload=False)
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
            self.assertEqual(
                numbers["source"],
                "experiments/portfolio/k8s-recovery/k3-pilot-v1/k3-pilot-1a/42181882847",
            )

    def test_capacity_source_selects_committed_pass_run(self) -> None:
        self.assertEqual(
            capacity.select_source(ROOT).relative_to(ROOT).as_posix(),
            "experiments/portfolio/k8s-recovery/k3-pilot-v1/k3-pilot-1a/42181882847",
        )

    def test_capacity_source_ignores_fail_with_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            selected = capacity_run(repo, "pass", "PASS")
            capacity_run(repo, "fail", "FAIL")
            self.assertEqual(capacity.select_source(repo), selected, "b62 FAIL run selected")

    def test_capacity_source_rejects_two_pass_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            capacity_run(repo, "pass-one", "PASS")
            capacity_run(repo, "pass-two", "PASS")
            with self.assertRaisesRegex(
                ValueError, "b62-capacity-source", msg="b62 ambiguous PASS runs accepted"
            ):
                capacity.select_source(repo)

    def test_capacity_source_rejects_ledger_hash_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            capacity_run(repo, "pass", "PASS")
            ledger_path = repo / capacity.LEDGER
            ledger = json.loads(ledger_path.read_bytes())
            ledger["entries"][0]["artifact_sha256"] = "0" * 64
            ledger_path.write_text(json.dumps(ledger) + "\n")
            with self.assertRaisesRegex(
                ValueError, "b62-capacity-source", msg="b62 ledger hash accepted"
            ):
                capacity.select_source(repo)

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


# B.6.3 keeps the capacity class unchanged and shares its single replay budget.
def deadline_failure(root: Path) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    manifest = {
        "job": "k3-pilot-4b",
        "run_id": "43173232814",
        "job_id": "131031261405",
        "version": 2,
        "status": "FAIL",
        "cleanup": {"status": "PASS"},
        "error": "command-179-kubectl-timeout",
        "completed": [],
        "artifact_sha256": {},
        "unperformed": [
            {"arm": "cpu", "index": 1, "started": True},
            {"arm": "cpu", "index": 2, "started": False},
        ],
    }
    save(root / "manifest.json", manifest)
    (root / "commands.jsonl").write_text(
        json.dumps(
            {
                "error": "command-179-kubectl-timeout",
                "args": [
                    "kubectl",
                    "-n",
                    "dur053-kind",
                    "exec",
                    "observer",
                    "--",
                    "/loadgen",
                    "-arm",
                    "cpu",
                    "-run-id",
                    "cpu-1-continuous",
                ],
            }
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return manifest


def cpu_capacity(root: Path) -> dict:
    manifest = failure(root)
    (root / "short-2").rename(root / "cpu-2")
    manifest.update(job="k3-pilot-4b", completed=["cpu-1"])
    for row in manifest["unperformed"]:
        row["arm"] = "cpu"
    save(root / "manifest.json", manifest)
    return manifest


class B63Tests(unittest.TestCase):
    def test_continuous_timeout(self) -> None:
        class Captured(Exception):
            pass

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            driver = SustainedDriver(root, root / "scratch")
            driver.engineering = False
            save(root / "cpu-drift.json", {"persisted_ns": 1})

            def setup(directory: Path, _: dict) -> tuple[str, str]:
                directory.mkdir(exist_ok=True)
                driver.output = directory
                return "ns", "db"

            for arm, x in (("short", 14.272201), ("cpu", 1.738646731039214)):
                for version in (2, 3):
                    reg = b6.precision(x, {})
                    with (
                        self.subTest(arm=arm, version=version),
                        patch.object(driver, "setup", side_effect=setup),
                        patch.object(driver, "failure_export"),
                        patch.object(driver, "arrivals", side_effect=Captured) as arrivals,
                        self.assertRaises(Captured),
                    ):
                        driver.sustained(arm, 1, {}, reg, version)
                    q = x / (2 if version == 2 else 4)
                    count = math.ceil((120 + b6.duration(q)) * q)
                    args, kwargs = arrivals.call_args
                    self.assertEqual(args, ("ns", "db", "continuous", count, q))
                    deadline = count / q + 2 * 10 + 30
                    self.assertGreater(
                        kwargs.get("timeout", count / q + 60),
                        deadline,
                        "b63 explicit timeout missing",
                    )
                    self.assertEqual(kwargs["timeout"], deadline + 120)

    def test_deadline_classification(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = deadline_failure(root)
            self.assertTrue(b6.harness_deadline_failure(root, manifest), "b63 failure missed")
            for field, value in (
                ("job", "k3-pilot-5b"),
                ("run_id", "43173232815"),
                ("version", 3),
                ("status", "PASS"),
                ("cleanup", {"status": "FAIL"}),
                ("error", "command-180-kubectl-timeout"),
                ("unperformed", []),
                ("unperformed", [{"arm": "cpu", "index": 2, "started": True}]),
                ("unperformed", [{"arm": "short", "index": 1, "started": True}]),
                ("unperformed", [{"arm": "cpu", "index": 1, "started": False}]),
                ("unperformed", [{"arm": "cpu", "index": 1, "started": True}] * 2),
            ):
                with self.subTest(field=field, value=value):
                    self.assertFalse(
                        b6.harness_deadline_failure(root, {**manifest, field: value}),
                        "b63 classification bypassed",
                    )
            path = root / "commands.jsonl"
            original = path.read_text()
            row = json.loads(original)
            args = row["args"]
            for bad in (
                {**row, "error": "other-timeout"},
                {**row, "args": [v.replace("/loadgen", "/checker") for v in args]},
                {**row, "args": [v.replace("cpu-1-continuous", "cpu-2-continuous") for v in args]},
                {**row, "args": ["short" if v == "cpu" else v for v in args]},
                {**row, "args": [v.replace("exec", "logs") for v in args]},
                {**row, "args": [v.replace("kubectl", "echo") for v in args]},
                {**row, "args": args + ["-arm", "cpu"]},
                {**row, "args": args[:-1]},
                {**row, "args": ["/loadgen", "-arm", "cpu", "-run-id", "cpu-1-continuous"]},
            ):
                path.write_text(json.dumps(bad) + "\n")
                self.assertFalse(
                    b6.harness_deadline_failure(root, manifest),
                    "b63 command classification bypassed",
                )
            for raw in ("", "invalid", "null\n", original * 2):
                path.write_text(raw)
                self.assertFalse(
                    b6.harness_deadline_failure(root, manifest),
                    "b63 command classification bypassed",
                )
            path.unlink()
            self.assertFalse(b6.harness_deadline_failure(root, manifest))

    def test_real_deadline_failure(self) -> None:
        relative = b6.ROOTS[2] / "k3-pilot-4b/43173232814"
        if (ROOT / relative).exists():
            with materialize(ROOT / relative, ROOT) as root:
                manifest = json.loads((root / "manifest.json").read_bytes())
                self.assertTrue(b6.harness_deadline_failure(root, manifest))
            return
        # Before PR #22 merges, read its fixed committed bytes without merging it.
        commit = "afb8a95"
        probe = subprocess.run(["git", "cat-file", "-e", commit], cwd=ROOT, capture_output=True)
        if probe.returncode:
            self.skipTest("PR #22 evidence absent from HEAD and afb8a95 unavailable")
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            names = subprocess.check_output(
                ["git", "ls-tree", "-r", "--name-only", commit, "--", relative.as_posix()],
                cwd=ROOT,
                text=True,
            ).splitlines()
            names.append("experiments/portfolio/k8s-recovery/live-time-ledger.json")
            for name in names:
                target = repo / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(
                    subprocess.check_output(
                        ["git", "show", f"{commit}:{name}"],
                        cwd=ROOT,
                    )
                )

            def committed(repository: Path, path: Path) -> bytes:
                raw = subprocess.check_output(
                    ["git", "show", f"{commit}:{path.relative_to(repository).as_posix()}"],
                    cwd=ROOT,
                )
                self.assertEqual(path.read_bytes(), raw)
                return raw

            with (
                patch("dur053_pilot.committed_bytes", side_effect=committed),
                materialize(repo / relative, repo) as root,
            ):
                manifest = json.loads((root / "manifest.json").read_bytes())
                self.assertTrue(
                    b6.harness_deadline_failure(root, manifest), "b63 committed failure missed"
                )
                isolated = repo / "isolated" / relative
                shutil.copytree(root, isolated)
            with (
                patch.object(b6, "materialize", side_effect=lambda p, _: nullcontext(p)),
                patch.object(b6, "summarize", side_effect=AssertionError("b63 population read")),
            ):
                self.assertEqual(b6.retained(repo / "isolated", "cpu", 2), {})

    def test_shared_replay_limit_and_exclusion(self) -> None:
        for order in (("deadline", "capacity"), ("capacity", "deadline"), ("deadline", "deadline")):
            with self.subTest(order=order), tempfile.TemporaryDirectory() as tmp:
                repo = Path(tmp)
                parent = repo / b6.ROOTS[2] / "k3-pilot-4b"
                constructors = {"deadline": deadline_failure, "capacity": cpu_capacity}
                first = parent / "10"
                m = constructors[order[0]](first)
                # Even completed cells of an excluded run must never reach summarize().
                m["completed"] = ["cpu-3"]
                save(first / "manifest.json", m)
                reg = b6.precision(2.0, {})
                with (
                    patch.object(dispatch, "registration", return_value=reg),
                    patch.object(
                        dispatch,
                        "read_artifact",
                        side_effect=lambda p, _: json.loads((p / "manifest.json").read_bytes()),
                    ),
                    patch.object(dispatch, "materialize", side_effect=lambda p, _: nullcontext(p)),
                    patch.object(b6, "materialize", side_effect=lambda p, _: nullcontext(p)),
                    patch.object(
                        b6, "summarize", side_effect=AssertionError("b63 population read")
                    ),
                ):
                    self.assertEqual(
                        dispatch.dependency_plan(repo, "k3-pilot-4b"), ([1, 2, 3], reg)
                    )
                    self.assertEqual(b6.retained(repo, "cpu", 2), {}, "b63 failure not excluded")
                    constructors[order[1]](parent / "11")
                    with self.assertRaisesRegex(
                        ValueError,
                        "b63-harness-replay-limit",
                        msg="b63 second harness replay admitted",
                    ):
                        dispatch.dependency_plan(repo, "k3-pilot-4b")
                    with self.assertRaisesRegex(
                        ValueError,
                        "b63-harness-replay-limit",
                        msg="b63 retained second replay admitted",
                    ):
                        b6.retained(repo, "cpu", 2)

    def test_later_timeout_is_ordinary_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            root = repo / b6.ROOTS[2] / "k3-pilot-4b/next"
            m = deadline_failure(root)
            m["run_id"] = "next"
            save(root / "manifest.json", m)
            with (
                patch.object(dispatch, "registration", return_value=b6.precision(2.0, {})),
                patch.object(dispatch, "read_artifact", return_value=m),
                patch.object(dispatch, "materialize", side_effect=lambda p, _: nullcontext(p)),
                self.assertRaisesRegex(ValueError, "pilot-no-replay-job"),
            ):
                dispatch.dependency_plan(repo, "k3-pilot-4b")
            with (
                patch.object(b6, "materialize", side_effect=lambda p, _: nullcontext(p)),
                self.assertRaisesRegex(ValueError, "b6-retained-job"),
            ):
                b6.retained(repo, "cpu", 2)

    def test_deadline_schedule_reservation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            parent = repo / b6.ROOTS[2] / "k3-pilot-4b"
            deadline_failure(parent / "10")
            ledger = {
                "entries": [{"category": "k3-pilot-4b", "charged_seconds": 1917, "status": "FAIL"}]
            }
            with (
                patch("dur053_payload.materialize", side_effect=lambda p, _: nullcontext(p)),
                patch("dur053_k3_policy.committed_ledger", return_value=ledger),
                patch.object(b6, "registration", return_value=b6.precision(2.0, {})),
            ):
                plan = plan_b6(repo)
                replay = next(j for j in plan["jobs"] if j.get("job") == "k3-pilot-4b")
                self.assertTrue(replay["deadline_replay"], "b63 replay reservation missing")
                self.assertEqual(replay["expected_seconds"], 7680, "b63 full reservation missing")
                self.assertEqual(replay["full_admission_seconds"], 7680)
                self.assertEqual(plan["harness_deadline_failures"], ["k3-pilot-4b"])
                self.assertEqual(plan["charged_seconds"], 1917)
                self.assertEqual(
                    plan["expected_total_seconds"],
                    1917 + sum(j["expected_seconds"] for j in plan["jobs"]) + 1710,
                )
                ledger["entries"].append(
                    {"category": "k3-pilot-4b", "charged_seconds": 3000, "status": "PASS"}
                )
                after = plan_b6(repo)
                self.assertFalse(any(j.get("job") == "k3-pilot-4b" for j in after["jobs"]))
                cpu_capacity(parent / "11")
                with self.assertRaisesRegex(
                    ValueError,
                    "b63-harness-replay-limit",
                    msg="b63 schedule second replay admitted",
                ):
                    plan_b6(repo)

    def test_deadline_freeze_provenance_and_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            v1 = repo / b6.EVIDENCE / "k3-pilot-3/9"
            v1.mkdir(parents=True)
            save(
                v1 / "manifest.json",
                {
                    "completed": [f"{a}-{i}" for a in ("medium", "long") for i in (1, 2, 3)],
                },
            )
            parent = repo / b6.ROOTS[2] / "k3-pilot-4b"
            failed = parent / "10"
            deadline_failure(failed)
            relative = (failed / "manifest.json").relative_to(repo).as_posix()
            ledger = {
                "entries": [
                    {"artifact": relative, "job_id": "131031261405", "category": "k3-pilot-4b"}
                ]
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
                self.assertEqual(
                    frozen["excluded_harness_deadline_failures"],
                    [relative],
                    "b63 freeze exclusion missing",
                )
                self.assertIn(relative, frozen["source_artifacts"])
                self.assertEqual(frozen["excluded_harness_capacity_failures"], [])
                self.assertEqual(frozen["arms"]["cpu"]["versions"], {2: rows, 3: {}})
                ledger["entries"] = []
                with self.assertRaisesRegex(ValueError, "b6-freeze-ledger-entry"):
                    freezing.finish(repo, Path("checker"), copy.deepcopy(result))
                ledger["entries"] = [
                    {"artifact": relative, "job_id": "131031261405", "category": "k3-pilot-4b"}
                ]
                cpu_capacity(parent / "11")
                ledger["entries"].append(
                    {
                        "artifact": (parent / "11/manifest.json").relative_to(repo).as_posix(),
                        "job_id": "10",
                        "category": "k3-pilot-4b",
                    }
                )
                with self.assertRaisesRegex(
                    ValueError,
                    "b63-harness-replay-limit",
                    msg="b63 freeze second replay admitted",
                ):
                    freezing.finish(repo, Path("checker"), copy.deepcopy(result))
