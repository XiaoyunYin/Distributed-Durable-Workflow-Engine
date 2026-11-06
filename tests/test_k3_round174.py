from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from test_k3b_pilot import fixture_artifact, fixture_calibration, fixture_run

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import dur053_k3 as k3  # noqa: E402
import dur053_k3_policy as policy  # noqa: E402
import dur053_pilot as pilot  # noqa: E402
import dur053_pilot_evidence as evidence  # noqa: E402
from dur053_pilot_driver import PilotDriver  # noqa: E402


class Round174Tests(unittest.TestCase):
    def test_namespace_reuse_preserves_cached_service_answers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            d = PilotDriver(root, root)
            d.images = {"worker": "worker@sha256:test"}
            base = json.loads((ROOT / "deploy/k8s/base/resources.yaml").read_bytes())["items"]
            active, epoch, corrupt = {}, [0], [False]
            pods = [
                {
                    "kind": "Pod",
                    "metadata": {"uid": p, "name": p, "labels": {"app": "worker"}},
                    "status": {
                        "containerStatuses": [{"restartCount": 0, "imageID": "worker@sha256:test"}]
                    },
                }
                for p in ("p1", "p2")
            ]

            def apply(obj: dict) -> None:
                if obj["kind"] == "Namespace":
                    active.clear()
                    epoch[0] += 1
                for item in obj["items"] if obj["kind"] == "List" else [obj]:
                    if item["kind"] == "Service":
                        name = item["metadata"]["name"]
                        requested = item["spec"].get("clusterIP")
                        if name in active:
                            self.assertIn(
                                requested,
                                (None, active[name]),
                                "Service IP must remain immutable within an episode",
                            )
                        else:
                            active[name] = requested or f"10.96.{epoch[0]}.{len(active) + 1}"

            def k(*args: str, **kwargs: object) -> subprocess.CompletedProcess:
                if "deployments,pods,services,networkpolicies,pdb,jobs" in args:
                    rows = [
                        {
                            "kind": "Service",
                            "metadata": {"name": name},
                            "spec": {"clusterIP": "10.96.250.1" if corrupt[0] else ip},
                        }
                        for name, ip in active.items()
                    ]
                else:
                    rows = pods
                return subprocess.CompletedProcess([], 0, json.dumps({"items": rows}))

            with (
                patch.object(d, "render", side_effect=lambda *args: (root, copy.deepcopy(base))),
                patch.object(d, "sql"),
                patch.object(d, "apply", side_effect=apply),
                patch.object(d, "k", side_effect=k),
                patch.object(d, "pod", return_value={"metadata": {"name": "observer"}}),
            ):
                for cell in ("first", "second"):
                    d.output = root / cell
                    d.output.mkdir()
                    try:
                        d.setup_episode("kind", {"192.0.2.1": "postgres", "192.0.2.2": "kafka"})
                    except Exception as error:
                        self.fail(
                            "cached Service DNS continuity must allow the next episode: "
                            + str(error)
                        )
                    if cell == "first":
                        cached_dns = dict(active)
                    else:
                        self.assertEqual(
                            cached_dns,
                            active,
                            "cached DNS must still name every recreated frontend",
                        )
                        self.assertEqual(
                            json.loads((d.output / "kind-service-ips.json").read_bytes()),
                            cached_dns,
                        )
                corrupt[0] = True
                d.output = root / "bad-observation"
                d.output.mkdir()
                with self.assertRaisesRegex(
                    ValueError,
                    "episode-service-ip-continuity",
                    msg="changed observed Service IP must fail closed",
                ):
                    d.setup_episode("kind", {"192.0.2.1": "postgres", "192.0.2.2": "kafka"})

    def test_bounded_wait_rejects_predicate_success_after_deadline(self) -> None:
        now = [0.0]

        def late() -> bool:
            now[0] = 11.0
            return True

        with (
            patch("dur053_k3.time.monotonic", side_effect=lambda: now[0]),
            patch("dur053_k3.time.sleep"),
            self.assertRaisesRegex(
                TimeoutError, "bounded condition", msg="late predicate must not pass"
            ),
        ):
            k3.Driver.until(late, 10)
        now[0] = 0.0
        with patch("dur053_k3.time.monotonic", side_effect=lambda: now[0]):
            k3.Driver.until(lambda: True, 10)

    def test_reconciliation_rejects_late_poll_and_late_verification(self) -> None:
        now, samples, exports = [0.0], [0], []
        for phase in ("observation", "verification"):
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                d = PilotDriver(root, root)
                d.terminal_observed = {"wanted": "seen"}
                now[0], samples[0] = 0.0, 0
                exports.clear()

                def sample(ns: str, db: str, phase=phase) -> None:
                    samples[0] += 1
                    if phase == "observation" and samples[0] == 2:
                        now[0] = 100.0

                def command(
                    *args: str, phase=phase, **kwargs: object
                ) -> subprocess.CompletedProcess:
                    if "pods" in args:
                        return subprocess.CompletedProcess(
                            [], 0, json.dumps({"items": [{"status": {"podIP": "127.0.0.1"}}]})
                        )
                    if "-snapshot" in args:
                        now[0] = 100.0
                    return subprocess.CompletedProcess([], 0, '{"valid":true,"trace":{}}')

                def pause(seconds: float) -> None:
                    now[0] += 31.0

                with (
                    patch.object(d, "sample", side_effect=sample),
                    patch.object(d, "k", side_effect=command),
                    patch.object(d, "python", return_value="stable"),
                    patch.object(
                        d,
                        "collect_witness",
                        side_effect=lambda ns, label: exports.append(label) or {},
                    ),
                    patch.object(d, "remote_json"),
                    patch.object(d, "pod", return_value={"metadata": {"name": "observer"}}),
                    patch("dur053_pilot_driver.time.monotonic", side_effect=lambda: now[0]),
                    patch("dur053_pilot_driver.time.sleep", side_effect=pause),
                    self.assertRaisesRegex(
                        TimeoutError,
                        f"pilot reconciliation {phase} timed out",
                        msg="late reconciliation must fail " + phase,
                    ),
                ):
                    d.reconcile(
                        "ns",
                        "db",
                        [{"rows": [{"workflow_id": "wanted"}]}],
                        "deadline-test",
                        seconds=50,
                    )
                self.assertEqual(bool(exports), phase == "verification")

    def test_reset_accepts_absent_groups_and_refuses_active_group(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = PilotDriver(Path(tmp), Path(tmp))
            for active in (False, True):

                def command(
                    args: list[str], active=active, **kwargs: object
                ) -> subprocess.CompletedProcess:
                    if "--group" in args:
                        return subprocess.CompletedProcess(args, 1, "", "group delete refused")
                    listing = "runtime-schedulers-v1\n" if active else ""
                    return subprocess.CompletedProcess(args, 0, listing, "")

                with (
                    patch.object(d, "k"),
                    patch.object(d, "drop"),
                    patch.object(d, "topics") as topics,
                    patch.object(d, "command", side_effect=command),
                    patch.object(
                        d,
                        "until",
                        side_effect=lambda predicate, seconds: self.assertTrue(predicate()),
                    ),
                ):
                    if active:
                        with self.assertRaisesRegex(
                            ValueError,
                            "kafka-group-delete-verified",
                            msg="active group deletion must fail closed",
                        ):
                            d.reset_episode("ns", "db")
                        self.assertNotIn(unittest.mock.call("--create"), topics.call_args_list)
                    else:
                        d.reset_episode("ns", "db")
                        self.assertEqual(topics.call_args_list[-1], unittest.mock.call("--create"))

    def test_reset_clears_both_kafka_groups_before_next_episode(self) -> None:
        self.assertFalse(
            policy.application_groups_absent("runtime-schedulers-v1\n"),
            "scheduler group must be absent",
        )
        self.assertFalse(policy.application_groups_absent("runtime-workers-v1\n"))
        self.assertTrue(policy.application_groups_absent("unrelated\n"))
        with tempfile.TemporaryDirectory() as tmp:
            d = PilotDriver(Path(tmp), Path(tmp))
            events = []
            for _episode in range(2):
                events.clear()
                listings = iter(("runtime-schedulers-v1\n", ""))

                def command(
                    args: list[str], listings=listings, **kwargs: object
                ) -> subprocess.CompletedProcess:
                    if "--group" in args:
                        events.append(args[-1])
                    if "--list" in args and args[3].endswith("kafka-consumer-groups.sh"):
                        listing = next(listings)
                        events.append("group-list:" + listing.strip())
                    else:
                        listing = ""
                    return subprocess.CompletedProcess(args, 0, listing, "")

                def until(predicate: object, seconds: float) -> None:
                    for _ in range(3):
                        if predicate():
                            return
                    self.fail("reset absence never verified")

                with (
                    patch.object(
                        d, "k", side_effect=lambda *args, **kw: events.append("namespace")
                    ) as deletion,
                    patch.object(d, "drop", side_effect=lambda db: events.append("database")),
                    patch.object(d, "topics", side_effect=lambda action: events.append(action)),
                    patch.object(d, "command", side_effect=command),
                    patch.object(d, "until", side_effect=until),
                ):
                    d.reset_episode("dur053-kind", "dur053_kind")
                self.assertIn("--wait=true", deletion.call_args.args)
                self.assertEqual(
                    events,
                    [
                        "namespace",
                        "database",
                        "--delete",
                        "runtime-workers-v1",
                        "runtime-schedulers-v1",
                        "group-list:runtime-schedulers-v1",
                        "group-list:",
                        "--create",
                    ],
                    "both groups must be deleted and verified absent before topic recreation",
                )

    def test_jobs_are_transformed_before_creation_and_remain_immutable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            d = PilotDriver(root, root)
            d.units = 100000
            objects = json.loads((ROOT / "deploy/k8s/base/resources.yaml").read_bytes())["items"]
            first = {}
            events = []
            applied = []
            transform = d.episode_objects

            def episode_objects(objects: list[dict]) -> None:
                events.append("transform")
                transform(objects)

            def apply(obj: dict) -> None:
                applied.append(copy.deepcopy(obj))
                for item in obj["items"] if obj["kind"] == "List" else [obj]:
                    if item["kind"] != "Job":
                        continue
                    events.append("job")
                    self.assertIn("transform", events, "episode transform must precede first Job")
                    name = item["metadata"]["name"]
                    template = copy.deepcopy(item["spec"]["template"])
                    self.assertEqual(
                        first.setdefault(name, template),
                        template,
                        "Job template must remain immutable",
                    )
                    for c in template["spec"]["containers"] + template["spec"].get(
                        "initContainers", []
                    ):
                        self.assertIn(
                            {"name": "DUR053_CPU_WORK_UNITS", "value": "100000"}, c["env"]
                        )

            d.images = {"worker": "worker@sha256:test"}
            pods = [
                {
                    "metadata": {"uid": p, "name": p, "labels": {"app": "worker"}},
                    "status": {
                        "containerStatuses": [{"restartCount": 0, "imageID": "worker@sha256:test"}]
                    },
                }
                for p in ("p1", "p2")
            ]
            service_rows = [
                {
                    "kind": "Service",
                    "metadata": o["metadata"],
                    "spec": {"clusterIP": f"10.96.1.{i + 1}"},
                }
                for i, o in enumerate(o for o in objects if o["kind"] == "Service")
            ]

            def k(*args: str, **kwargs: object) -> subprocess.CompletedProcess:
                rows = (
                    service_rows
                    if "deployments,pods,services,networkpolicies,pdb,jobs" in args
                    else pods
                )
                return subprocess.CompletedProcess([], 0, json.dumps({"items": rows}))

            with (
                patch.object(d, "render", return_value=(root, objects)),
                patch.object(d, "sql"),
                patch.object(d, "apply", side_effect=apply),
                patch.object(d, "episode_objects", side_effect=episode_objects),
                patch.object(d, "pod", return_value={"metadata": {"name": "observer"}}),
                patch.object(d, "k", side_effect=k),
            ):
                d.setup_episode("kind", {"192.0.2.1": "postgres", "192.0.2.2": "kafka"})
            self.assertEqual(set(first), {"migrate", "compatibility"})
            self.assertEqual(
                json.loads((root / "applied-objects.json").read_bytes()),
                applied[-1]["items"],
                "saved episode objects must equal the applied List",
            )

    def test_unrecorded_failure_has_its_own_redacted_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            d = k3.Driver(root, root)
            with patch.object(
                k3.subprocess,
                "run",
                return_value=subprocess.CompletedProcess([], 0, "previous", ""),
            ):
                d.command(["kubectl", "logs"])
            for allowed in (False, True):
                number = d.sequence + 1
                with patch.object(
                    k3.subprocess,
                    "run",
                    return_value=subprocess.CompletedProcess(
                        [], 1, d.password, "immutable " + d.demand_token
                    ),
                ):
                    if allowed:
                        d.command(["kubectl", "apply"], record=False, allowed=True)
                    else:
                        with self.assertRaisesRegex(
                            ValueError,
                            f"command-{number:03d}-kubectl-exit-1",
                            msg="failed unrecorded command must name its output",
                        ):
                            d.apply({"kind": "List", "items": []})
                path = root / f"command-{number:03d}.txt"
                self.assertTrue(path.exists(), "failed unrecorded command must retain output")
                self.assertEqual(path.read_text(), "[REDACTED]immutable [REDACTED]")
            self.assertEqual((root / "command-001.txt").read_text(), "previous")

    def test_timeout_output_and_concurrent_sequence_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            d = k3.Driver(root, root)
            with (
                patch.object(
                    k3.subprocess,
                    "run",
                    side_effect=subprocess.TimeoutExpired(
                        "kubectl", 1, b"partial stdout", d.password.encode()
                    ),
                ),
                self.assertRaisesRegex(TimeoutError, "command-001-kubectl-timeout"),
            ):
                d.command(["kubectl"], record=False)
            self.assertEqual((root / "command-001.txt").read_text(), "partial stdout[REDACTED]")
            with ThreadPoolExecutor(max_workers=8) as pool:
                numbers = list(pool.map(lambda i: d.command_output(str(i), ""), range(64)))
            self.assertEqual(len(set(numbers)), 64)
            self.assertEqual(
                {(root / f"command-{i:03d}.txt").read_text() for i in numbers},
                {str(i) for i in range(64)},
            )
            # Finish a second command after the first has written, but before
            # its guard runs. Each guard must retain its own local file number.
            original_output = d.command_output
            first_written, second_written = threading.Event(), threading.Event()
            first_number = d.sequence + 1

            def output(stdout: str, stderr: str) -> int:
                number = original_output(stdout, stderr)
                if stdout == "first":
                    first_written.set()
                    self.assertTrue(second_written.wait(5))
                else:
                    second_written.set()
                return number

            def fail(name: str) -> str:
                try:
                    d.command(["kubectl", name], record=False)
                except ValueError as error:
                    return str(error)
                self.fail("failed command must raise")

            with (
                patch.object(d, "command_output", side_effect=output),
                patch.object(
                    k3.subprocess,
                    "run",
                    side_effect=lambda args, **kw: subprocess.CompletedProcess(
                        args, 1, args[-1], ""
                    ),
                ),
                ThreadPoolExecutor(max_workers=2) as pool,
            ):
                first = pool.submit(fail, "first")
                self.assertTrue(first_written.wait(5))
                second = pool.submit(fail, "second")
                self.assertIn(f"command-{first_number + 1:03d}", second.result())
                self.assertIn(f"command-{first_number:03d}", first.result())

    def test_durable_latency_probe_and_polling_cross_checks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            directory = fixture_run(Path(tmp))
            result = pilot.summarize(directory)
            self.assertEqual(
                result["latencies"],
                [1.002] * 200,
                "latency must use durable history plus probe width",
            )
            self.assertEqual(
                result["polling_minus_durable_seconds"], {"max": 9999.0, "p99": 9997.0}
            )
            snapshot = directory / "snapshot.json"
            original = snapshot.read_bytes()
            for label, edit in (
                ("duplicate", lambda rows: rows.append(copy.deepcopy(rows[-1]))),
                ("missing", lambda rows: rows.pop()),
                ("revision", lambda rows: rows[-1].update(revision=2)),
                (
                    "later-than-poll",
                    lambda rows: rows[-1].update(created_at="2026-11-05T03:00:00+00:00"),
                ),
                (
                    "before-scheduled",
                    lambda rows: rows[-1].update(created_at="2026-11-04T00:00:00+00:00"),
                ),
            ):
                value = json.loads(original)
                edit(value["Snapshot"]["engine.transition_history"]["rows"])
                k3.save(snapshot, value)
                with self.assertRaisesRegex(
                    ValueError, "pilot-terminal-", msg="durable latency guard bypassed " + label
                ):
                    pilot.summarize(directory)
            snapshot.write_bytes(original)
            clock = directory / "clock.json"
            value = json.loads(clock.read_bytes())
            value["measured"]["observer_after"] = "2026-11-05T05:33:20.020000+00:00"
            k3.save(clock, value)
            self.assertEqual(
                pilot.summarize(directory)["latencies"],
                [1.02] * 200,
                "larger final probe bracket must bound every latency",
            )
            for label, edit in (
                ("db-bracket", lambda x: x["setup"].update(database="2000-01-01T00:00:00+00:00")),
                ("probe-window", lambda x: x.update(setup=copy.deepcopy(x["measured"]))),
            ):
                bad = copy.deepcopy(value)
                edit(bad)
                k3.save(clock, bad)
                with self.assertRaisesRegex(
                    ValueError,
                    "pilot-(db-clock-bracket|clock-probe-window)",
                    msg="clock guard bypassed " + label,
                ):
                    pilot.summarize(directory)
            value.pop("measured")
            k3.save(clock, value)
            try:
                pilot.summarize(directory)
            except Exception as error:
                self.assertIsInstance(error, ValueError, "missing probe guard bypassed")
                self.assertIn("pilot-clock-probes-required", str(error))
            else:
                self.fail("missing probe guard bypassed")

    def test_observer_probe_and_measured_reconciliation_persist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            directory = fixture_run(root)
            clock = json.loads((directory / "clock.json").read_bytes())
            d = PilotDriver(root, root)
            with (
                patch.object(d, "setup_episode", return_value=("ns", "db", "observer", set())),
                patch.object(d, "sample"),
                patch.object(d, "pod", return_value={"metadata": {"name": "observer"}}),
                patch.object(
                    d,
                    "k",
                    return_value=subprocess.CompletedProcess([], 0, json.dumps(clock["setup"])),
                ),
            ):
                d.setup(root / "setup-episode", {})
            self.assertTrue(
                (root / "setup-episode/clock.json").exists(),
                "setup must persist observer database probe",
            )
            self.assertEqual(
                json.loads((root / "setup-episode/clock.json").read_bytes()),
                {"setup": clock["setup"]},
                "setup must persist observer database probe",
            )
            d.output = root
            with (
                patch.object(d, "pod", return_value={"metadata": {"name": "observer"}}),
                patch.object(
                    d,
                    "k",
                    return_value=subprocess.CompletedProcess([], 0, json.dumps(clock["setup"])),
                ) as command,
            ):
                d.clock_probe("ns", "setup")
                self.assertIn("-clock-probe", command.call_args.args)
                self.assertEqual(
                    json.loads((root / "clock.json").read_bytes()), {"setup": clock["setup"]}
                )
                with self.assertRaisesRegex(ValueError, "pilot-clock-probe-no-overwrite"):
                    d.clock_probe("ns", "setup")
            # Exercise successful live + offline checker path, including exact
            # RawMessage snapshot bytes and post-reconciliation probe ordering.
            d.output = directory
            d.terminal_observed = {
                r["workflow_id"]: "seen"
                for r in json.loads((directory / "warmup-arrivals.json").read_bytes())["rows"]
            }
            windows = [
                json.loads((directory / f"{w}-arrivals.json").read_bytes())
                for w in ("warmup", "measured")
            ]
            d.terminal_observed.update({r["workflow_id"]: "seen" for r in windows[1]["rows"]})
            calls = []

            def command(*args: str, **kwargs: object) -> subprocess.CompletedProcess:
                calls.append(args)
                if "pods" in args:
                    return subprocess.CompletedProcess(
                        [], 0, json.dumps({"items": [{"status": {"podIP": "127.0.0.1"}}]})
                    )
                if "/checker" in args:
                    return subprocess.CompletedProcess(
                        [], 0, '{"valid":true,"trace":{ "Snapshot":{} }}'
                    )
                return subprocess.CompletedProcess([], 0, "")

            tick = iter(range(0, 1000, 10))
            with (
                patch.object(d, "sample"),
                patch.object(d, "k", side_effect=command),
                patch.object(d, "python", return_value="stable"),
                patch.object(d, "collect_witness", return_value={}),
                patch.object(d, "remote_json"),
                patch.object(d, "pod", return_value={"metadata": {"name": "observer"}}),
                patch.object(
                    d, "clock_probe", side_effect=lambda ns, phase: calls.append((phase,))
                ) as probe,
                patch("dur053_pilot_driver.time.monotonic", side_effect=lambda: next(tick)),
                patch("dur053_pilot_driver.time.sleep"),
                patch("dur053_pilot_driver.datetime") as date,
            ):
                from datetime import datetime

                date.fromisoformat.side_effect = datetime.fromisoformat
                date.now.return_value = datetime.fromisoformat(
                    windows[-1]["rows"][-1]["scheduled_at"]
                )
                d.reconcile("ns", "db", windows, "measured")
                probe.assert_called_once_with("ns", "measured")
            self.assertEqual(
                calls[-1], ("measured",), "final probe must follow offline reconciliation"
            )
            self.assertEqual((directory / "snapshot.json").read_bytes(), b'{ "Snapshot":{} }\n')

    def test_freeze_rejects_ledger_artifacts_outside_inventory(self) -> None:
        for relative in (Path("outside/failed"), pilot.EVIDENCE / "k3-pilot-1b-c1/99"):
            with tempfile.TemporaryDirectory() as tmp:
                repository = Path(tmp)
                available = []
                for n, (job, cells) in enumerate(pilot.JOBS.items()):
                    inside = repository / pilot.EVIDENCE / job / str(n + 10)
                    good = fixture_artifact(
                        inside, cells=cells, job=job, run=str(n + 10), job_id=str(n + 20)
                    )
                    if job == "k3-pilot-1b":
                        fixture_calibration(inside)
                    if job == "k3-pilot-3":
                        good["dependencies"]["long"] = {
                            "manifest_sha256": hashlib.sha256(
                                (available[2][0] / "manifest.json").read_bytes()
                            ).hexdigest(),
                            "b": 0.1,
                            "r": 40,
                        }
                    good["artifact_sha256"] = k3.artifact_hashes(inside)
                    k3.save(inside / "manifest.json", good)
                    available.append((inside, good))
                outside = repository / relative
                bad = fixture_artifact(outside, job="k3-pilot-1b-c1", job_id="29", run="99")
                bad["status"] = "FAIL"
                k3.save(outside / "manifest.json", bad)
                entries = []
                for path, manifest in [*available, (outside, bad)]:
                    entries.append(
                        {
                            **{
                                k: manifest[k]
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
                            "category": manifest["job"],
                            "artifact": (path / "manifest.json").relative_to(repository).as_posix(),
                            "artifact_sha256": hashlib.sha256(
                                (path / "manifest.json").read_bytes()
                            ).hexdigest(),
                        }
                    )
                k3.save(
                    repository / "experiments/portfolio/k8s-recovery/live-time-ledger.json",
                    {"entries": entries},
                )
                with (
                    patch.object(evidence, "artifacts", return_value=available),
                    patch.object(evidence, "recheck"),
                    patch.object(
                        evidence,
                        "committed_bytes",
                        side_effect=lambda repo, path: path.read_bytes(),
                    ),
                    self.assertRaisesRegex(
                        ValueError,
                        "pilot-freeze-ledger-artifact-inventory",
                        msg="ledger inventory guard bypassed",
                    ),
                ):
                    evidence.freeze(repository, Path("unused"))
