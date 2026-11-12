from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import dur053_sustained as b6  # noqa: E402
import dur053_sustained_campaign as dispatch  # noqa: E402
import dur053_sustained_freeze as freezing  # noqa: E402
from dur053_k3 import save  # noqa: E402
from dur053_pilot_driver import PilotDriver  # noqa: E402
from dur053_schedule import plan_b6  # noqa: E402
from dur053_sustained_driver import SustainedDriver  # noqa: E402


def series(root: Path) -> Path:
    root.mkdir(parents=True)
    origin = datetime(2026, 11, 11, tzinfo=UTC)
    rows = [
        {"workflow_id": str(i), "scheduled_at": (origin + timedelta(seconds=i)).isoformat()}
        for i in range(600)
    ]
    save(root / "measured-arrivals.json", {"rows": rows})
    save(
        root / "record.json",
        {
            "measurement_start_at": origin.isoformat(),
            "measurement_seconds": 600,
            "measured_rate": 1,
        },
    )
    save(
        root / "snapshot.json",
        {
            "Snapshot": {
                "engine.workflow_executions": {
                    "rows": [{"workflow_id": r["workflow_id"], "revision": 1} for r in rows]
                },
                "engine.transition_history": {
                    "rows": [
                        {
                            "workflow_id": r["workflow_id"],
                            "revision": 1,
                            "new_state": "SUCCEEDED",
                            "created_at": (origin + timedelta(seconds=i + 1)).isoformat(),
                        }
                        for i, r in enumerate(rows)
                    ]
                },
            }
        },
    )
    samples = [
        {
            "observed_at": (origin + timedelta(seconds=t)).isoformat(),
            "outstanding": 1,
            "unpublished": 2,
        }
        for t in (1, 51, 501, 551)
    ]
    (root / "engine-samples.jsonl").write_text(
        "".join(json.dumps(r) + "\n" for r in samples), encoding="utf-8"
    )
    return root


def change_latency(directory: Path, indices: range, latency: float) -> None:
    rows = json.loads((directory / "measured-arrivals.json").read_bytes())["rows"]
    snapshot = json.loads((directory / "snapshot.json").read_bytes())
    for i in indices:
        snapshot["Snapshot"]["engine.transition_history"]["rows"][i]["created_at"] = (
            datetime.fromisoformat(rows[i]["scheduled_at"]) + timedelta(seconds=latency)
        ).isoformat()
    save(directory / "snapshot.json", snapshot)


class SustainedTests(unittest.TestCase):
    def test_registration_refusal_precedes_admission_and_resources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with (
                patch.dict(os.environ, {"GITHUB_ACTIONS": "true", "GITHUB_RUN_ID": "10"}),
                patch.object(
                    SustainedDriver,
                    "command",
                    return_value=subprocess.CompletedProcess([], 0, "a" * 40),
                ),
                patch.object(
                    dispatch, "job_identity_and_history", return_value=("20", set(), "b" * 40)
                ),
                patch.object(
                    dispatch,
                    "dependency_plan",
                    side_effect=ValueError("K3 guard: b6-rate-registration"),
                ),
                patch(
                    "dur053_campaign.campaign_gate",
                    side_effect=AssertionError("admission before registration"),
                ),
                patch.object(SustainedDriver, "install") as install,
                patch.object(
                    SustainedDriver,
                    "cleanup",
                    return_value={
                        "status": "PASS",
                        "no_resources_started": True,
                        "elapsed_live_seconds": 0,
                    },
                ),
            ):
                self.assertEqual(dispatch.run(root / "out", root / "scratch", "k3-pilot-4a"), 1)
            install.assert_not_called()
            manifest = json.loads((root / "out/manifest.json").read_bytes())
            self.assertEqual(manifest["error"], "K3 guard: b6-rate-registration")
            self.assertEqual(manifest["charged_seconds"], 1)

    def test_uncommitted_v2_cannot_authorize_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            root = repo / b6.ROOTS[2] / "k3-pilot-4a/10"
            root.mkdir(parents=True)
            save(root / "manifest.json", {"job": "k3-pilot-4a", "artifact_sha256": {}})
            with self.assertRaisesRegex(ValueError, "pilot-committed-bytes"):
                b6.fallback_required(repo, "short")

    def test_budget_over_cap_keeps_all_minima(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with (
                patch(
                    "dur053_k3_policy.committed_ledger",
                    return_value={
                        "entries": [
                            {"charged_seconds": 10108, "category": "prior", "status": "PASS"}
                        ]
                    },
                ),
                patch.object(b6, "registration", return_value=b6.precision(2.0, {})),
            ):
                result = plan_b6(
                    Path(tmp),
                    {a: {"r": 0.00001, "L": 2} for a in ("short", "cpu", "medium", "long")},
                )
            self.assertFalse(result["fits_expected_cap"])
            self.assertGreater(result["expected_total_seconds"], 259200)
            self.assertEqual(result["registered_remaining_comparison_episodes"], 422)

    def test_fixed_unit_drift_never_selects(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out = root / "out"
            out.mkdir()
            driver = SustainedDriver(out, root / "scratch")
            driver.images = {"worker": "worker@sha256:" + "a" * 64}
            calls = []

            def setup(directory: Path, _addresses: dict) -> tuple[str, str]:
                directory.mkdir()
                driver.output = directory
                return "ns", "db"

            def arrivals(
                _ns: str, _db: str, label: str, count: int, rate: float, **kw: object
            ) -> dict:
                calls.append((driver.units, count, rate, kw))
                save(driver.output / (label + "-witness.json"), {})
                return {"rows": [{"workflow_id": label}]}

            with (
                patch.object(driver, "setup", side_effect=setup),
                patch.object(driver, "arrivals", side_effect=arrivals),
                patch.object(driver, "reconcile"),
                patch.object(driver, "reset_episode"),
                patch("dur053_sustained_driver.busy_times", return_value=[0.6]),
                patch(
                    "dur053_pilot_driver.select_cpu", side_effect=AssertionError("units reselected")
                ),
            ):
                record = driver.drift_check({}, 2)
            self.assertEqual(len(calls), 22)
            self.assertTrue(all(c[0] == 1000000 for c in calls), "b6 fixed units bypassed")
            self.assertEqual(record["units"], 1000000)
            self.assertEqual(len(record["measured_busy_seconds"]), 21)
            self.assertEqual(record["reference_median_seconds"], 0.514)
            self.assertEqual(record["image"], driver.images["worker"])

    def test_continuous_v2_driver_and_scope(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out = root / "out"
            out.mkdir()
            driver = SustainedDriver(out, root / "scratch", engineering=True)
            calls = []
            origin = datetime.now(UTC) - timedelta(seconds=120)

            def setup(directory: Path, _addresses: dict) -> tuple[str, str]:
                directory.mkdir()
                driver.output = directory
                return "ns", "db"

            def arrivals(
                _ns: str, _db: str, label: str, count: int, rate: float, *, timeout: float
            ) -> dict:
                self.assertEqual(timeout, count / rate + 2 * 10 + 30 + 120)
                calls.append((label, count, rate))
                driver.sample(_ns, _db)
                rows = [
                    {
                        "workflow_id": str(i),
                        "outcome": "ACCEPTED",
                        "scheduled_at": (origin + timedelta(seconds=i / rate)).isoformat(),
                    }
                    for i in range(count)
                ]
                data = {"rows": rows, "rate": rate, "offered": count, "status": "PASS"}
                save(driver.output / (label + "-arrivals.json"), data)
                return data

            def reconcile(_ns: str, _db: str, windows: list, label: str) -> float:
                self.assertEqual(label, "measured")
                self.assertEqual(len(windows), 1)
                return 30

            gate = {"pass": True, "checks": {"b6-throughput": True}}
            with (
                patch.object(driver, "setup", side_effect=setup),
                patch.object(driver, "arrivals", side_effect=arrivals),
                patch.object(driver, "reconcile", side_effect=reconcile),
                patch.object(PilotDriver, "sample"),
                patch.object(driver, "sql", return_value='{"outstanding": 1, "unpublished": 0}'),
                patch.object(driver, "reset_episode"),
                patch("dur053_sustained_driver.summarize", return_value={"p99": 0.2}),
                patch("dur053_sustained_driver.stationarity", return_value=gate),
            ):
                record = driver.sustained("short", 1, {}, b6.precision(2.0, {}), 2)
            self.assertEqual(calls, [("continuous", 66, 1.0)])
            self.assertTrue(
                (out / "short-1/engine-samples.jsonl").is_file(), "b6 v2 samples missing"
            )
            self.assertFalse(driver.engine_sampling, "b6 sample scope leaked")
            self.assertEqual(record["warmup_count"], 6)
            self.assertEqual(record["measured_count"], 60)
            self.assertEqual(record["stationarity_gate"], gate)
            self.assertTrue(record["engineering"])
            self.assertNotEqual(record["seed"], 530002)
            saved = json.loads((out / "short-1/record.json").read_bytes())
            self.assertEqual(saved["stationarity_gate"], gate)
            source = (ROOT / "scripts/dur053_campaign.py").read_text(encoding="utf-8")
            self.assertIn("SustainedDriver.sustained", source)
            self.assertIn('"engineering-validation" if engineering', source)
            self.assertIn('"measurement_claim": not engineering', source)

    def test_v1_engineering_cell_has_no_engine_samples(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out = root / "out"
            out.mkdir()
            driver = SustainedDriver(out, root / "scratch", engineering=True)
            origin = datetime.now(UTC) - timedelta(seconds=120)

            def setup(directory: Path, _addresses: dict) -> tuple[str, str]:
                directory.mkdir()
                driver.output = directory
                return "ns", "db"

            def sample(_ns: str, _db: str) -> None:
                with (driver.output / "samples.jsonl").open("a", encoding="utf-8") as handle:
                    handle.write("{}\n")

            def arrivals(ns: str, db: str, _label: str, _count: int, _rate: float) -> dict:
                driver.sample(ns, db)
                return {"rows": [{"scheduled_at": origin.isoformat()}]}

            with (
                patch.object(driver, "setup", side_effect=setup),
                patch.object(driver, "arrivals", side_effect=arrivals),
                patch.object(driver, "reconcile", return_value=30),
                patch.object(driver, "reset_episode"),
                patch.object(PilotDriver, "sample", side_effect=sample) as parent_sample,
                patch.object(
                    driver, "sql", return_value='{"outstanding": 1, "unpublished": 0}'
                ) as sql,
                patch("dur053_pilot_driver.summarize", return_value={}),
            ):
                driver.pilot("short", 0, {}, None)
            self.assertTrue((out / "short-0/samples.jsonl").is_file())
            self.assertFalse(
                (out / "short-0/engine-samples.jsonl").exists(), "b6 v1 samples polluted"
            )
            self.assertEqual(parent_sample.call_count, 2)
            sql.assert_not_called()

    def test_sampling_flag_restored_after_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            driver = SustainedDriver(root, root / "scratch", engineering=True)
            for previous in (False, True):
                driver.engine_sampling = previous
                with (
                    patch.object(
                        driver, "setup", side_effect=ValueError("K3 guard: fixture-setup")
                    ),
                    patch.object(driver, "failure_export"),
                    self.assertRaisesRegex(ValueError, "fixture-setup"),
                ):
                    driver.sustained("short", 1, {}, b6.precision(2.0, {}), 2)
                self.assertIs(driver.engine_sampling, previous, "b6 sample flag not restored")

    def test_registration_guard_fixture(self) -> None:
        from contextlib import nullcontext

        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            root = repo / b6.EVIDENCE / "k3-pilot-1a/11"
            root.mkdir(parents=True)
            save(
                root / "manifest.json",
                {
                    "completed": [f"short-{i}" for i in (1, 2, 3)],
                    "run_id": "11",
                    "status": "PASS",
                    "cleanup": {"status": "PASS"},
                },
            )

            def metrics(directory: Path) -> dict:
                return {
                    "completion_throughput_first_to_last": {
                        "short-1": 18.0,
                        "short-2": 14.272201140132513,
                        "short-3": 15.0,
                    }[directory.name]
                }

            with (
                patch.object(b6, "committed_bytes", side_effect=lambda _, p: p.read_bytes()),
                patch.object(b6, "materialize", side_effect=lambda p, _: nullcontext(p)),
                patch.object(b6, "engine_path_metrics", side_effect=metrics),
            ):
                reg = b6.registration(repo, "short")
                self.assertEqual(reg["source"]["cell"], "short-2")
                for values in (("bad", "7.136101"), ("14.272201", "bad")):
                    with (
                        patch.dict(b6.REGISTERED, {"short": values}),
                        self.assertRaisesRegex(
                            ValueError, "b6-rate-registration", msg="b6 registration bypassed"
                        ),
                    ):
                        b6.registration(repo, "short")

    def test_stationary_and_bin_one_exclusion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = series(Path(tmp) / "repeat")
            self.assertTrue(b6.stationarity(d)["pass"])
            change_latency(d, range(100), 1000)
            gate = b6.stationarity(d)
            self.assertEqual(gate["bins"][0]["completions"], 0)
            self.assertTrue(gate["pass"], "b6 bin one is pipeline fill")

    def test_throughput_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = series(Path(tmp) / "repeat")
            change_latency(d, range(100, 125), 1000)
            boundary = b6.stationarity(d)
            self.assertEqual(
                boundary["pooled_bins_2_to_6"],
                {"arrivals": 500, "completions": 475},
                "b6-throughput pooled sum",
            )
            self.assertTrue(boundary["pass"])
            change_latency(d, range(125, 126), 1000)
            gate = b6.stationarity(d)
            self.assertEqual(gate["pooled_bins_2_to_6"]["completions"], 474)
            self.assertEqual(
                gate["checks"],
                {"b6-throughput": False, "b6-latency-drift": True, "b6-engine-path": True},
            )
            with self.assertRaisesRegex(ValueError, "b6-throughput", msg="b6 throughput bypassed"):
                b6.require_stationary(gate)

    def test_latency_absolute_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = series(Path(tmp) / "repeat")
            change_latency(d, range(400, 600), 1.5)
            self.assertTrue(b6.stationarity(d)["pass"], "absolute .5 allowance must apply")
            change_latency(d, range(400, 600), 1.500001)
            gate = b6.stationarity(d)
            self.assertFalse(gate["checks"]["b6-latency-drift"])
            self.assertTrue(gate["checks"]["b6-throughput"])
            with self.assertRaisesRegex(ValueError, "b6-latency-drift", msg="b6 drift bypassed"):
                b6.require_stationary(gate)

    def test_latency_p90_and_relative_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = series(Path(tmp) / "repeat")
            change_latency(d, range(600), 4)
            change_latency(d, range(400, 600), 5)
            self.assertTrue(b6.stationarity(d)["pass"], "relative 1.25 allowance must apply")
            change_latency(d, range(560, 600), 5.000001)
            gate = b6.stationarity(d)
            self.assertFalse(gate["checks"]["b6-latency-drift"])
            self.assertEqual(gate["last_third_median_p90"][0], 5)
            with self.assertRaisesRegex(ValueError, "b6-latency-drift"):
                b6.require_stationary(gate)

    def test_engine_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = series(Path(tmp) / "repeat")
            path = d / "engine-samples.jsonl"
            rows = [json.loads(r) for r in path.read_text().splitlines()]
            for row in rows[2:]:
                row.update(outstanding=5, unpublished=22)
            baseline = copy.deepcopy(rows)
            for key, guard in (
                ("outstanding", "b6-engine-path"),
                ("unpublished", "b6-engine-path"),
            ):
                path.write_text("".join(json.dumps(r) + "\n" for r in baseline))
                self.assertTrue(b6.stationarity(d)["pass"])
                rows = copy.deepcopy(baseline)
                rows[-1][key] += 0.000002
                path.write_text("".join(json.dumps(r) + "\n" for r in rows))
                gate = b6.stationarity(d)
                self.assertFalse(gate["checks"][guard])
                self.assertTrue(gate["checks"]["b6-throughput"])
                self.assertTrue(gate["checks"]["b6-latency-drift"])
                with self.assertRaisesRegex(ValueError, guard, msg="b6 engine path bypassed"):
                    b6.require_stationary(gate)

    def test_jittered_stationary_pools_completions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = series(Path(tmp) / "repeat")
            change_latency(d, range(100, 106), 101)
            gate = b6.stationarity(d)
            self.assertFalse(
                all(b["completions"] >= 0.95 * b["arrivals"] for b in gate["bins"][1:])
            )
            self.assertEqual(gate["pooled_bins_2_to_6"], {"arrivals": 500, "completions": 500})
            self.assertTrue(gate["pass"], "b6 pooled jitter rejected")

    def test_offline_throughput_boundary_ignores_disclosures(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = series(Path(tmp) / "repeat")
            # Both offline disclosure checks fail; only engine-path measures decide.
            change_latency(d, range(400, 600), 1000)
            metrics = {
                "completion_throughput_first_to_last": 0.95,
                "offered_rate": 1.0,
                "warmup_outbox_unpublished_at_start": 0,
            }
            with patch.object(b6, "engine_path_metrics", return_value=metrics):
                gate = b6.stationarity(d, offline=True)
                self.assertTrue(gate["pass"], "b6 offline disclosures decided admission")
                self.assertFalse(any(gate["disclosure_checks"].values()))
                self.assertEqual(gate["measured_rate"], 1.0)
                metrics["completion_throughput_first_to_last"] = 0.949999999
                gate = b6.stationarity(d, offline=True)
                with self.assertRaisesRegex(ValueError, "b6-offline-throughput"):
                    b6.require_stationary(gate)

    def test_offline_backlog_and_disclosure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = series(Path(tmp) / "repeat")
            metrics = {
                "completion_throughput_first_to_last": 1.0,
                "offered_rate": 1.0,
                "warmup_outbox_unpublished_at_start": 0,
            }
            with patch.object(b6, "engine_path_metrics", return_value=metrics):
                self.assertTrue(b6.stationarity(d, offline=True)["pass"])
                metrics["warmup_outbox_unpublished_at_start"] = 1
                gate = b6.stationarity(d, offline=True)
                with self.assertRaisesRegex(ValueError, "b6-engine-path"):
                    b6.require_stationary(gate)

    def test_committed_medium_offline_criterion(self) -> None:
        gates = {}
        for path in (ROOT / b6.EVIDENCE).glob("*/*/manifest.json"):
            header = json.loads(b6.committed_bytes(ROOT, path))
            if not any(f"medium-{i}" in header["completed"] for i in (1, 2, 3)):
                continue
            with b6.materialize(path.parent, ROOT) as root:
                for i in (1, 2, 3):
                    if f"medium-{i}" in header["completed"]:
                        gates[i] = b6.stationarity(root / f"medium-{i}", offline=True)
        self.assertEqual(set(gates), {1, 2, 3})
        for gate in gates.values():
            self.assertTrue(gate["pass"])
            self.assertEqual(gate["measured_rate"], 0.49970686679309045)
            self.assertGreaterEqual(
                gate["completion_throughput_first_to_last"], 0.95 * gate["measured_rate"]
            )
            self.assertEqual(gate["warmup_outbox_unpublished_at_start"], 0)
            self.assertEqual(gate["pooled_bins_2_to_6"], {"arrivals": 166, "completions": 168})
            self.assertTrue(gate["disclosure_checks"]["b6-throughput"])
        self.assertFalse(gates[3]["disclosure_checks"]["b6-latency-drift"])

    def test_long_offline_failure_controls_admission_and_schedule(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            root = repo / b6.EVIDENCE / "k3-pilot-3/10"
            root.mkdir(parents=True)
            save(root / "manifest.json", {"completed": [f"long-{i}" for i in (1, 2, 3)]})
            for i in (1, 2, 3):
                series(root / f"long-{i}")
                # Jitter/drift disclosures cannot cause a long trigger.
                change_latency(root / f"long-{i}", range(400, 600), 1000)
            metrics = {
                "completion_throughput_first_to_last": 1.0,
                "offered_rate": 1.0,
                "warmup_outbox_unpublished_at_start": 0,
            }
            reg = b6.precision(2.0, {})
            with (
                patch.object(dispatch, "registration", return_value=reg),
                patch.object(b6, "registration", return_value=reg),
                patch("dur053_payload.materialize", side_effect=lambda p, _: nullcontext(p)),
                patch.object(
                    b6,
                    "engine_path_metrics",
                    side_effect=lambda d: (
                        metrics
                        if d.name == "long-2"
                        else {
                            "completion_throughput_first_to_last": 1.0,
                            "offered_rate": 1.0,
                            "warmup_outbox_unpublished_at_start": 0,
                        }
                    ),
                ),
                patch("dur053_pilot.committed_bytes", side_effect=lambda _, p: p.read_bytes()),
                patch("dur053_k3_policy.committed_ledger", return_value={"entries": []}),
            ):
                with self.assertRaisesRegex(ValueError, "b6-long-trigger"):
                    dispatch.dependency_plan(repo, "k3-pilot-4c1")
                plan = plan_b6(repo)
                self.assertFalse(
                    any(j.get("job", "").startswith("k3-pilot-4c") for j in plan["jobs"]),
                    "b6 long schedule trigger bypassed",
                )
                for key, value in (
                    ("completion_throughput_first_to_last", 0.94),
                    ("warmup_outbox_unpublished_at_start", 1),
                ):
                    before = metrics[key]
                    metrics[key] = value
                    self.assertEqual(dispatch.dependency_plan(repo, "k3-pilot-4c1"), ([1], reg))
                    plan = plan_b6(repo)
                    self.assertEqual(
                        [
                            j["job"]
                            for j in plan["jobs"]
                            if j.get("job", "").startswith("k3-pilot-4c")
                        ],
                        [f"k3-pilot-4c{i}" for i in (1, 2, 3)],
                    )
                    metrics[key] = before

    def test_committed_rate_registration(self) -> None:
        for arm, expected in (("short", 14.272201140132513), ("cpu", 1.738646731039214)):
            reg = b6.registration(ROOT, arm)
            self.assertEqual(reg["X_min"], expected)
            self.assertEqual(reg["q"], expected / 2)
            self.assertEqual(reg["repr"]["r"], repr(expected))
            self.assertEqual(reg["source"]["cell"], "short-2" if arm == "short" else "cpu-3")
            self.assertEqual(b6.duration(reg["q"]), 600)
            with (
                patch.dict(b6.REGISTERED, {arm: (b6.REGISTERED[arm][0], "bad")}),
                self.assertRaisesRegex(
                    ValueError, "b6-rate-registration", msg="b6 registration bypassed"
                ),
            ):
                b6.registration(ROOT, arm)

    def test_freeze_rate_and_version_limit(self) -> None:
        reg = b6.precision(14.272201140132513, {"cell": "short-2", "run_id": "42181882847"})
        for version in (2, 3):
            rate = b6.operating_record(reg, version)
            self.assertEqual(rate["r"], 2 * rate["q"], "b6 r = 2q bypassed")
            self.assertEqual(rate["q"], reg["q"] / (2 if version == 3 else 1))
        with self.assertRaisesRegex(
            ValueError, "b6-version-limit", msg="b6 version bound bypassed"
        ):
            b6.version_rate(reg, 4)
        with self.assertRaisesRegex(ValueError, "b6-v1-L-excluded", msg="b6 v1 L bypassed"):
            b6.choose({})
        passing = [{"pass": True}] * 3
        failed = [{"pass": False}, *passing[:2]]
        self.assertEqual(b6.choose({2: passing}), 2)
        self.assertEqual(b6.choose({2: failed, 3: passing}), 3)
        self.assertIsNone(b6.choose({2: failed, 3: failed}))
        with self.assertRaisesRegex(ValueError, "b6-first-passing-version"):
            b6.choose({2: passing, 3: passing})
        with self.assertRaisesRegex(ValueError, "b6-version-limit"):
            b6.choose({2: failed, 4: passing})

    def test_complete_v2_required_for_v3(self) -> None:
        for data in ({1: {"gate": {"pass": False}}}, {}, {1: {}, 2: {}, 4: {}}):
            with (
                patch.object(b6, "retained", return_value=data),
                patch.object(dispatch, "registration", return_value=b6.precision(2.0, {})),
                self.assertRaisesRegex(ValueError, "b6-v3-complete-v2"),
            ):
                dispatch.dependency_plan(ROOT, "k3-pilot-5a")

    def test_retained_failure_required_for_v3(self) -> None:
        data = {i: {"gate": {"pass": True}} for i in (1, 2, 3)}
        with (
            patch.object(b6, "retained", return_value=data),
            self.assertRaisesRegex(
                ValueError, "b6-v3-retained-v2-failure", msg="b6 fallback bypassed"
            ),
        ):
            b6.fallback_required(ROOT, "short")
        data[1]["gate"]["pass"] = False
        with (
            tempfile.TemporaryDirectory() as tmp,
            patch.object(dispatch, "registration", return_value=b6.precision(2.0, {})),
            patch.object(b6, "retained", return_value=data),
        ):
            self.assertEqual(dispatch.dependency_plan(Path(tmp), "k3-pilot-5a")[0], [1, 2, 3])
            self.assertEqual(b6.fallback_required(ROOT, "short"), data)

    def test_reservations_and_job_split(self) -> None:
        reg = b6.precision(1.738646731039214, {})
        for v in (2, 3):
            self.assertEqual(dispatch.admit(b6.version_rate(reg, v), 3, cpu=True), 7680)
        with self.assertRaisesRegex(ValueError, "pilot-job-fit"):
            dispatch.admit(0.01, 3)
        for v in (4, 5):
            for i in (1, 2, 3):
                self.assertEqual(b6.job_spec(f"k3-pilot-{v}c{i}")[2], [i])
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            directory = repo / b6.ROOTS[2] / "k3-pilot-4a/10"
            directory.mkdir(parents=True)
            save(directory / "manifest.json", {})
            prior = {
                "job": "k3-pilot-4a",
                "status": "CARRY_OVER",
                "cleanup": {"status": "PASS"},
                "carry_over": {
                    "job": "k3-pilot-4a-c1",
                    "reason": "pilot-job-fit",
                    "started": False,
                    "cells": [["short", 3]],
                },
            }
            for key, value in (("reason", "other"), ("started", True)):
                bad = copy.deepcopy(prior)
                bad["carry_over"][key] = value
                with (
                    patch.object(dispatch, "registration", return_value=reg),
                    patch.object(dispatch, "read_artifact", return_value=bad),
                    self.assertRaisesRegex(ValueError, "b6-carry-predecessor"),
                ):
                    dispatch.dependency_plan(repo, "k3-pilot-4a-c1")
            with (
                patch.object(dispatch, "registration", return_value=reg),
                patch.object(dispatch, "read_artifact", return_value=prior),
            ):
                self.assertEqual(dispatch.dependency_plan(repo, "k3-pilot-4a-c1"), ([3], reg))

    def test_medium_long_execution_unchanged(self) -> None:
        # SHA-256 of exact base 9d192b1 file bytes, verified once at registration.
        # Needs neither git nor the base commit in CI's depth-1 checkout.
        expected = "02745503a400821d39f9659ecbf44571e23733905fcd66278ed13680093e9d65"
        self.assertEqual(
            hashlib.sha256((ROOT / "scripts/dur053_pilot_driver.py").read_bytes()).hexdigest(),
            expected,
        )
        import dur053_pilot as pilot

        self.assertEqual(pilot.JOBS["k3-pilot-2"], [("long", 0)])
        self.assertEqual(pilot.JOBS["k3-pilot-3"], [("long", i) for i in (1, 2, 3)])
        self.assertIs(PilotDriver.pilot, SustainedDriver.pilot)
        self.assertIs(PilotDriver.calibration, SustainedDriver.calibration)
        for arm in ("medium", "long"):
            self.assertEqual(pilot.rates(arm, 1, {"r": 0.08}), (0.02, 0.04))

    def test_schedule_recheck(self) -> None:
        result = plan_b6(ROOT)
        ledger = subprocess.run(
            ["git", "show", "HEAD:experiments/portfolio/k8s-recovery/live-time-ledger.json"],
            cwd=ROOT,
            check=True,
            capture_output=True,
        )
        committed_charged_seconds = sum(
            entry["charged_seconds"] for entry in json.loads(ledger.stdout)["entries"]
        )
        self.assertEqual(result["charged_seconds"], committed_charged_seconds)
        self.assertGreaterEqual(committed_charged_seconds, 10108)
        self.assertEqual(result["cap_seconds"], 259200)
        self.assertEqual(result["registered_remaining_comparison_episodes"], 422)
        self.assertEqual(result["rates"]["short"], 14.272201140132513)
        self.assertEqual(result["rates"]["cpu"], 1.738646731039214)
        self.assertEqual(result["fits_expected_cap"], result["expected_total_seconds"] <= 259200)

    def test_limited_freeze_preserves_medium(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            v1 = repo / b6.EVIDENCE / "k3-pilot-1a/10"
            v1.mkdir(parents=True)
            save(
                v1 / "manifest.json",
                {"completed": [f"{a}-{i}" for a in ("medium", "long") for i in (1, 2, 3)]},
            )
            original = {
                "arms": {
                    a: {"b": 1, "r": 4, "L": 2, "repeats": []}
                    for a in ("short", "cpu", "medium", "long")
                },
                "source_artifacts": {},
                "limitations": [
                    "host limitation: retained verbatim",
                    "short: L for this arm is derived under engine-path overload. "
                    "Completion throughput, carried warmup outbox backlog or latency drift "
                    "triggered engine_path_saturation; L depends on the 200-arrival window "
                    "length and is not a steady-state latency target. K4-K7 reporting must "
                    "surface this flag. Registered rates, L and reservations are unchanged.",
                    "cpu: L for this arm is derived under engine-path overload. "
                    "Completion throughput, carried warmup outbox backlog or latency drift "
                    "triggered engine_path_saturation; L depends on the 200-arrival window "
                    "length and is not a steady-state latency target. K4-K7 reporting must "
                    "surface this flag. Registered rates, L and reservations are unchanged.",
                    "cpu: L was measured under overload; the scheduling reference does not "
                    "hold under load (repeat utilisation > 1 or b > 1.5 times initial b). "
                    "Repeat busy times are a disclosed sensitivity only; registered "
                    "reservations are unchanged.",
                    "medium: retained v1 limitation",
                    "R265 medium queueing limitation: retained verbatim",
                ],
            }
            rows = {
                i: {"b": 0.1, "latencies": [1.0] * 600, "gate": {"pass": True}} for i in (1, 2, 3)
            }

            def versions(_repo: Path, _arm: str, version: int, _checker: Path) -> dict:
                return rows if version == 2 else {}

            from contextlib import nullcontext

            with (
                patch.object(freezing, "materialize", side_effect=lambda p, _: nullcontext(p)),
                patch.object(freezing, "committed_ledger", return_value={"entries": []}),
                patch.object(
                    freezing,
                    "stationarity",
                    return_value={
                        "pass": True,
                        "disclosure_checks": {"b6-throughput": False, "b6-latency-drift": False},
                    },
                ),
                patch.object(freezing, "registration", return_value=b6.precision(10.0, {})),
                patch.object(freezing, "retained", side_effect=versions),
                patch("dur053_schedule.plan_b6", return_value={"units": []}),
            ):
                result = freezing.finish(repo, Path("checker"), copy.deepcopy(original))
            self.assertEqual(result["arms"]["short"]["r"], 10)
            self.assertEqual(result["arms"]["short"]["L"], 2)
            self.assertEqual(result["arms"]["medium"]["r"], original["arms"]["medium"]["r"])
            self.assertEqual(result["arms"]["long"]["L"], original["arms"]["long"]["L"])
            self.assertEqual(
                result["arms"]["long"]["r"],
                original["arms"]["long"]["r"],
                "b6 long freeze criterion bypassed",
            )
            self.assertIn(b6.PARTITION_LIMITATION, result["limitations"])
            self.assertIn("host limitation: retained verbatim", result["limitations"])
            self.assertIn("medium: retained v1 limitation", result["limitations"])
            self.assertIn(
                "R265 medium queueing limitation: retained verbatim", result["limitations"]
            )
            expected_disclosures = {
                "short": (
                    "short: v1 repeats showed engine-path saturation and are excluded from L "
                    "under Amendment B.6; frozen r and L come from the v2 repeats at q, which "
                    "passed the B.6 stationarity gate."
                ),
                "cpu": (
                    "cpu: v1 repeats showed engine-path saturation and are excluded from L "
                    "under Amendment B.6; the R251 busy-time sensitivity remains disclosed in "
                    "the v1 busy times in overload_calibration_v1_repeats_excluded_from_L; "
                    "frozen r and L come from the v2 repeats at q, which passed the B.6 "
                    "stationarity gate."
                ),
            }
            for arm in ("short", "cpu"):
                self.assertFalse(
                    any(
                        limitation.startswith(
                            (
                                f"{arm}: L for this arm is derived under engine-path overload",
                                f"{arm}: L was measured under overload",
                            )
                        )
                        for limitation in result["limitations"]
                    )
                )
                arm_disclosures = [
                    limitation
                    for limitation in result["limitations"]
                    if limitation.startswith(f"{arm}: v1 repeats")
                ]
                self.assertEqual(arm_disclosures, [expected_disclosures[arm]])

            def unperformed_versions(_repo: Path, _arm: str, _version: int, _checker: Path) -> dict:
                return rows

            with (
                patch.object(freezing, "materialize", side_effect=lambda p, _: nullcontext(p)),
                patch.object(freezing, "committed_ledger", return_value={"entries": []}),
                patch.object(
                    freezing,
                    "stationarity",
                    return_value={
                        "pass": True,
                        "disclosure_checks": {"b6-throughput": False, "b6-latency-drift": False},
                    },
                ),
                patch.object(freezing, "registration", return_value=b6.precision(10.0, {})),
                patch.object(freezing, "retained", side_effect=unperformed_versions),
                patch.object(freezing, "choose", return_value=None),
                patch("dur053_schedule.plan_b6", return_value={"units": []}),
            ):
                unperformed = freezing.finish(repo, Path("checker"), copy.deepcopy(original))
            self.assertIn(
                "short: v1 repeats showed engine-path saturation and are excluded from L "
                "under Amendment B.6; no stationary L was frozen.",
                unperformed["limitations"],
            )

            def offline_failure(directory: Path, *, offline: bool) -> dict:
                self.assertTrue(offline)
                return {
                    "pass": directory.name != "long-2",
                    "disclosure_checks": {"b6-throughput": True, "b6-latency-drift": True},
                }

            with (
                patch.object(freezing, "materialize", side_effect=lambda p, _: nullcontext(p)),
                patch.object(freezing, "committed_ledger", return_value={"entries": []}),
                patch.object(freezing, "stationarity", side_effect=offline_failure),
                patch.object(freezing, "registration", return_value=b6.precision(10.0, {})),
                patch.object(freezing, "retained", side_effect=versions),
                patch("dur053_schedule.plan_b6", return_value={"units": []}),
            ):
                retried = freezing.finish(repo, Path("checker"), copy.deepcopy(original))
            self.assertEqual(retried["arms"]["long"]["passing_version"], 2)
            self.assertEqual(retried["arms"]["long"]["r"], 10)

            budget_root = repo / b6.ROOTS[3] / "k3-pilot-5a/11"
            budget_root.mkdir(parents=True)
            budget_manifest = {
                "version": 3,
                "planned": [["short", i] for i in (1, 2, 3)],
                "error": "K3 guard: ledger-budget",
                "cleanup": {"no_resources_started": True},
                "job_id": "20",
                "job": "k3-pilot-5a",
                "artifact_sha256": {},
            }
            save(budget_root / "manifest.json", budget_manifest)
            relative = (budget_root / "manifest.json").relative_to(repo).as_posix()
            failed = copy.deepcopy(rows)
            failed[1]["gate"]["pass"] = False

            def stopped_versions(_repo: Path, arm: str, version: int, _checker: Path) -> dict:
                if arm == "short":
                    return failed if version == 2 else {1: rows[1]}
                return rows if version == 2 else {}

            units = [{"arm": "short", "unit": "short-core-0-pair-0", "episodes": 2}]
            with (
                patch.object(freezing, "materialize", side_effect=lambda p, _: nullcontext(p)),
                patch.object(
                    freezing,
                    "committed_ledger",
                    return_value={
                        "entries": [
                            {"artifact": relative, "job_id": "20", "category": "k3-pilot-5a"}
                        ]
                    },
                ),
                patch.object(
                    freezing,
                    "stationarity",
                    return_value={
                        "pass": True,
                        "disclosure_checks": {"b6-throughput": False, "b6-latency-drift": False},
                    },
                ),
                patch.object(freezing, "registration", return_value=b6.precision(10.0, {})),
                patch.object(freezing, "retained", side_effect=stopped_versions),
                patch("dur053_schedule.plan_b6", return_value={"units": units}),
            ):
                stopped = freezing.finish(repo, Path("checker"), copy.deepcopy(original))
            self.assertEqual(stopped["unperformed_stationary_arms"], ["short"])
            self.assertIsNone(stopped["arms"]["short"]["L"])
            self.assertEqual(stopped["unperformed_cells"], units)

            budget_manifest["error"] = "K3 guard: pilot-job-fit"
            save(budget_root / "manifest.json", budget_manifest)
            with (
                patch.object(freezing, "materialize", side_effect=lambda p, _: nullcontext(p)),
                patch.object(
                    freezing,
                    "committed_ledger",
                    return_value={
                        "entries": [
                            {"artifact": relative, "job_id": "20", "category": "k3-pilot-5a"}
                        ]
                    },
                ),
                patch.object(
                    freezing,
                    "stationarity",
                    return_value={
                        "pass": True,
                        "disclosure_checks": {"b6-throughput": False, "b6-latency-drift": False},
                    },
                ),
                patch.object(freezing, "registration", return_value=b6.precision(10.0, {})),
                patch.object(freezing, "retained", side_effect=stopped_versions),
                patch("dur053_schedule.plan_b6", return_value={"units": units}),
            ):
                no_job_fit = freezing.finish(repo, Path("checker"), copy.deepcopy(original))
            self.assertEqual(no_job_fit["unperformed_stationary_arms"], ["short"])
            self.assertEqual(no_job_fit["unperformed_cells"], units)
