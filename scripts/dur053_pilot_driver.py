#!/usr/bin/env python3
"""Same pilot path for registered dispatch and reduced engineering CI."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import shutil
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dur053_k3 import Driver, save
from dur053_k3_policy import require, single_witness_session, stable_worker_sessions
from dur053_pilot import (
    ARMS,
    CANDIDATES,
    CONCURRENCY,
    busy_times,
    combine,
    rates,
    select_cpu,
    summarize,
    window_ok,
)
from dur053_prerequisites import export_witness, trace_document


def poll_pause(started: float, tick: int) -> float:
    """Absolute one-second cadence: RPC time consumes the interval."""
    return max(0.0, started + tick - time.monotonic())


class PilotDriver(Driver):
    def __init__(self, output: Path, scratch: Path, *, engineering: bool = False) -> None:
        super().__init__(output, scratch)
        self.root_output = output
        self.engineering = engineering
        self.live_reservation = 3600 if engineering else 18900
        self.arm, self.units = "short", 10000
        self.seed = 530001
        self.expected_workers: set[str] = set()
        self.terminal_observed: dict[str, str] = {}
        self.command_lock = threading.Lock()
        self.accounting_start: float | None = None
        self.teardown_deadline: float | None = None

    def command(self, args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if self.live_start is not None:
            self.accounting_start = self.live_start
        elif self.accounting_start is not None:
            if self.teardown_deadline is None:
                self.teardown_deadline = min(
                    self.accounting_start + self.live_reservation, time.monotonic() + 600
                )
            remaining = self.teardown_deadline - time.monotonic()
            require(remaining > 0, "pilot-teardown-deadline")
            kwargs["timeout"] = min(kwargs.get("timeout", 180), remaining)
        began = time.monotonic_ns()
        entry: dict[str, Any] = {
            "started_monotonic_ns": began,
            "args": [
                a.replace(self.password, "[REDACTED]").replace(self.demand_token, "[REDACTED]")
                for a in args
            ],
        }
        try:
            result = super().command(args, **kwargs)
            entry["returncode"] = result.returncode
            return result
        except Exception as error:
            entry["error"] = (
                str(error)
                .replace(self.password, "[REDACTED]")
                .replace(self.demand_token, "[REDACTED]")
            )
            raise
        finally:
            entry["ended_monotonic_ns"] = time.monotonic_ns()
            with (
                self.command_lock,
                (self.root_output / "commands.jsonl").open("a", encoding="utf-8") as handle,
            ):
                handle.write(json.dumps(entry, sort_keys=True) + "\n")

    def episode_objects(self, objects: list[dict[str, Any]]) -> None:
        delay = str(round(1000 * ARMS[self.arm])) if self.arm != "cpu" else "100"
        for obj in objects:
            if obj["kind"] == "ScaledObject":
                obj["metadata"].setdefault("annotations", {})[
                    "autoscaling.keda.sh/paused-replicas"
                ] = "2"
            if obj["kind"] in {"Deployment", "Job"}:
                pod = obj["spec"]["template"]["spec"]
                for container in pod["containers"] + pod.get("initContainers", []):
                    env = container.setdefault("env", [])
                    for key, value in (
                        ("DUR048_ACTIVITY_DELAY_MS", delay),
                        ("DUR053_CPU_WORK_UNITS", str(self.units)),
                    ):
                        if key == "DUR048_ACTIVITY_DELAY_MS":
                            for entry in env:
                                if entry["name"] == key:
                                    entry["value"] = value
                        else:
                            env[:] = [e for e in env if e["name"] != key]
                            env.append({"name": key, "value": value})
        save(self.output / "applied-objects.json", objects)

    def setup(self, directory: Path, addresses: dict[str, str]) -> tuple[str, str]:
        directory.mkdir()
        self.output = directory
        package = self.scratch / "package-kind"
        if package.exists():
            shutil.rmtree(package)
        ns, db, _, self.expected_workers = self.setup_episode("kind", addresses)
        save(
            directory / "namespace.json",
            json.loads(self.k("get", "namespace", ns, "-o", "json").stdout),
        )
        self.terminal_observed = {}
        before = datetime.now(UTC).isoformat()
        remote = json.loads(
            self.python(
                ns,
                "observer",
                'import time,json,datetime; print(json.dumps({"utc":datetime.datetime.now('
                'datetime.timezone.utc).isoformat(),"monotonic_ns":time.monotonic_ns()}))',
                record=False,
            )
        )
        save(
            directory / "clock.json",
            {
                "host_before": before,
                "observer": remote,
                "host_after": datetime.now(UTC).isoformat(),
            },
        )
        self.sample(ns, db)
        return ns, db

    def sample(self, ns: str, db: str) -> None:
        pods = json.loads(
            self.k(
                "-n", ns, "get", "pods", "-l", "app=worker", "-o", "json", record=False, timeout=10
            ).stdout
        )["items"]
        stable_worker_sessions(self.expected_workers, pods)
        require(
            all(
                p["status"].get("phase") == "Running"
                and all(c.get("ready") is True for c in p["status"]["containerStatuses"])
                and len(p["spec"]["containers"]) == 1
                and next(
                    e["value"]
                    for e in p["spec"]["containers"][0]["env"]
                    if e["name"] == "WORKER_SLOTS"
                )
                == "2"
                for p in pods
            ),
            "pilot-observed-four-slots",
        )
        scale = json.loads(
            self.k(
                "-n", ns, "get", "deployment/worker", "-o", "json", record=False, timeout=10
            ).stdout
        )
        scaler = json.loads(
            self.k(
                "-n",
                ns,
                "get",
                "scaledobject/worker-demand",
                "-o",
                "json",
                record=False,
                timeout=10,
            ).stdout
        )
        require(
            scale["spec"]["replicas"] == 2
            and scaler["metadata"]["annotations"].get("autoscaling.keda.sh/paused-replicas") == "2",
            "pilot-autoscaler-paused",
        )
        connections = int(
            self.sql("SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend';")
        )
        require(connections <= 92, "pilot-observed-connection-budget")
        terminal = self.sql(
            "SELECT workflow_id FROM engine.workflow_executions WHERE state='SUCCEEDED';", db
        )
        now = datetime.now(UTC).isoformat()
        for key in terminal.splitlines():
            self.terminal_observed.setdefault(key, now)
        row = {
            "monotonic_ns": time.monotonic_ns(),
            "observed_at": now,
            "pods": pods,
            "worker_deployment": scale,
            "scaledobject": scaler,
            "connections": connections,
            "terminal_ids": terminal.splitlines(),
        }
        with (self.output / "samples.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, sort_keys=True) + "\n")

    def collect_witness(self, ns: str, label: str) -> dict[str, Any]:
        pods = json.loads(
            self.k(
                "-n", ns, "get", "pods", "-l", "app=worker", "-o", "json", record=False, timeout=10
            ).stdout
        )["items"]
        stable_worker_sessions(self.expected_workers, pods)
        ips = [p["status"]["podIP"] for p in pods]
        inventories = json.loads(
            self.python(
                ns,
                "observer",
                "import json,urllib.request; print(json.dumps([json.load(urllib.request.urlopen("
                '"http://"+ip+":8081/witness",timeout=3)) for ip in ' + repr(ips) + "]))",
                record=False,
            )
        )
        root = self.output / (label + "-witness-raw")
        root.mkdir()
        for inventory in inventories:
            name = inventory["pod_name"]
            files = json.loads(
                self.k(
                    "-n",
                    ns,
                    "exec",
                    name,
                    "--",
                    "python",
                    "-c",
                    "import pathlib,json; print(json.dumps(sorted(p.name for p in pathlib.Path("
                    + repr("/witness/" + name)
                    + ').glob("*.jsonl"))))',
                    record=False,
                ).stdout
            )
            single_witness_session(files, inventory)
            directory = root / name
            directory.mkdir()
            raw = self.k(
                "-n",
                ns,
                "exec",
                name,
                "--",
                "python",
                "-c",
                "import base64; print(base64.b64encode(open("
                + repr("/witness/" + name + "/" + inventory["file"])
                + ',"rb").read()).decode())',
                record=False,
            ).stdout.strip()
            (directory / inventory["file"]).write_bytes(base64.b64decode(raw))
        return export_witness(root, inventories, sorted(self.expected_workers))

    def remote_json(self, ns: str, filename: str, value: Any) -> None:
        self.k(
            "-n",
            ns,
            "exec",
            "-i",
            self.pod(ns, "observer")["metadata"]["name"],
            "--",
            "python",
            "-c",
            "import sys; open(" + repr("/evidence/" + filename) + ',"w").write(sys.stdin.read())',
            data=json.dumps(value),
            record=False,
        )

    def arrivals(
        self,
        ns: str,
        db: str,
        name: str,
        count: int,
        rate: float,
        *,
        concurrency: int = CONCURRENCY,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        run_id = self.output.name + "-" + name
        args = [
            "-n",
            ns,
            "exec",
            self.pod(ns, "observer")["metadata"]["name"],
            "--",
            "/loadgen",
            "-url",
            "http://api:8080",
            "-token-file",
            "/identity/token",
            "-namespace",
            ns,
            "-run-id",
            run_id,
            "-arm",
            self.arm,
            "-count",
            str(count),
            "-rate",
            str(rate),
            "-concurrency",
            str(concurrency),
            "-cpu-units",
            str(self.units),
            "-seed",
            str(self.seed),
            "-output",
            "/evidence/" + name + "-arrivals.json",
        ]
        try:
            poll_started, tick = time.monotonic(), 0
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(
                    self.k, *args, allowed=True, timeout=timeout or count / rate + 60
                )
                while not future.done():
                    try:
                        self.sample(ns, db)
                    except Exception:
                        # Stop further arrivals immediately on observer/safety failure.
                        self.python(
                            ns,
                            "observer",
                            "import pathlib,os,signal; [(os.kill(int(p.name),signal.SIGTERM)) "
                            'for p in pathlib.Path("/proc").iterdir() if p.name.isdigit() '
                            'and (p/"comm").exists() and '
                            '(p/"comm").read_text().strip()=="loadgen"]',
                            record=False,
                        )
                        raise
                    tick += 1
                    time.sleep(poll_pause(poll_started, tick))
                result = future.result()
        finally:
            raw = self.python(
                ns,
                "observer",
                "print(open(" + repr("/evidence/" + name + "-arrivals.json") + ").read())",
                record=False,
            )
            (self.output / (name + "-arrivals.json")).write_text(
                raw, encoding="utf-8", newline="\n"
            )
        arrivals = json.loads(raw)
        window_ok(arrivals, count)
        require(result.returncode == 0, "pilot-loadgen-exit")
        return arrivals

    def reconcile(
        self, ns: str, db: str, windows: list[dict[str, Any]], label: str, *, seconds: float = 600
    ) -> float:
        started, quiet_since, last = time.monotonic(), time.monotonic(), ""
        tick = 0
        if label == "measured":
            final = max(
                datetime.fromisoformat(r["scheduled_at"].replace("Z", "+00:00"))
                for r in windows[-1]["rows"]
            )
            seconds = min(seconds, 600 - (datetime.now(UTC) - final).total_seconds())
        wanted = {r["workflow_id"] for w in windows for r in w["rows"]}
        while time.monotonic() - started < seconds:
            self.sample(ns, db)
            # Inventory both independent worker sessions, including idle workers.
            items = json.loads(
                self.k(
                    "-n", ns, "get", "pods", "-l", "app=worker", "-o", "json", record=False
                ).stdout
            )["items"]
            ips = [p["status"]["podIP"] for p in items]
            inventories = self.python(
                ns,
                "observer",
                "import json,urllib.request; print(json.dumps([json.load(urllib.request.urlopen("
                '"http://"+ip+":8081/witness",timeout=3)) for ip in ' + repr(ips) + "]))",
                record=False,
            )
            if inventories != last:
                quiet_since, last = time.monotonic(), inventories
            if wanted <= self.terminal_observed.keys() and time.monotonic() - quiet_since >= 30:
                quiet = time.monotonic() - quiet_since
                break
            tick += 1
            time.sleep(poll_pause(started, tick))
        else:
            raise TimeoutError("pilot reconciliation timed out")
        witness = self.collect_witness(ns, label)
        save(
            self.output / ("witness.json" if label == "measured" else label + "-witness.json"),
            witness,
        )
        arrivals = combine(windows) if len(windows) > 1 else windows[0]
        save(self.output / (label + "-combined-arrivals.json"), arrivals)
        self.remote_json(ns, label + "-combined-arrivals.json", arrivals)
        self.remote_json(ns, label + "-witness.json", witness)
        checker = [
            "-n",
            ns,
            "exec",
            self.pod(ns, "observer")["metadata"]["name"],
            "--",
            "/checker",
            "-arrivals",
            "/evidence/" + label + "-combined-arrivals.json",
            "-witness-export",
            "/evidence/" + label + "-witness.json",
            "-require-terminal",
        ]
        result = self.k(*checker, "-namespace", ns, allowed=True)
        report = json.loads(result.stdout)
        filename = "checker.json" if label == "measured" else label + "-checker.json"
        (self.output / filename).write_text(result.stdout, encoding="utf-8", newline="\n")
        raw = trace_document(result.stdout)
        (
            self.output / ("snapshot.json" if label == "measured" else label + "-snapshot.json")
        ).write_text(raw, encoding="utf-8", newline="\n")
        require(result.returncode == 0 and report["valid"] is True, "pilot-live-checker")
        # Preserve RawMessage snapshot byte spelling; remote_json would normalize it.
        self.k(
            "-n",
            ns,
            "exec",
            "-i",
            self.pod(ns, "observer")["metadata"]["name"],
            "--",
            "python",
            "-c",
            "import sys; open("
            + repr("/evidence/" + label + "-snapshot.json")
            + ',"w").write(sys.stdin.read())',
            data=raw,
            record=False,
        )
        offline = self.k(
            *checker, "-snapshot", "/evidence/" + label + "-snapshot.json", allowed=True
        )
        (
            self.output
            / ("offline-checker.json" if label == "measured" else label + "-offline-checker.json")
        ).write_text(offline.stdout, encoding="utf-8", newline="\n")
        require(
            offline.returncode == 0 and json.loads(offline.stdout)["valid"] is True,
            "pilot-offline-checker",
        )
        return quiet

    def pilot(
        self,
        arm: str,
        index: int,
        addresses: dict[str, str],
        initial: dict[str, Any] | None,
        units: int = 10000,
    ) -> dict[str, Any]:
        self.arm, self.units = arm, units
        self.seed = 530001 + {"short": 0, "medium": 4, "cpu": 8, "long": 12}[arm] + index
        directory = self.root_output / f"{arm}-{index}"
        record: dict[str, Any] = {
            "arm": arm,
            "index": index,
            "status": "FAIL",
            "timed_out": False,
            "engineering": self.engineering,
            "slots": 4,
            "faults": [],
            "autoscaling": "paused-at-two-replicas",
            "reconciliation_timeout_seconds": 600,
            "cpu_units": units,
            "seed": self.seed,
        }
        try:
            ns, db = self.setup(directory, addresses)
            if arm == "cpu" and index == 0:
                calibration_path = self.root_output / "calibration.json"
                calibration = json.loads(calibration_path.read_bytes())
                record["calibration_sha256"] = hashlib.sha256(
                    calibration_path.read_bytes()
                ).hexdigest()
                record["calibration_persisted_ns"] = calibration["persisted_ns"]
            warm, measured = rates(arm, index, initial)
            record.update(
                warmup_rate=warm, measured_rate=measured, warmup_start_ns=time.monotonic_ns()
            )
            save(directory / "record.json", record)
            duration, count = (20, 10) if self.engineering else (120, 200)
            warmup = self.arrivals(ns, db, "warmup", math.ceil(duration * warm), warm)
            scheduled = datetime.fromisoformat(
                warmup["rows"][0]["scheduled_at"].replace("Z", "+00:00")
            )
            # Definition preparation precedes loadgen t0; it is not warmup.
            end = time.monotonic() + max(
                0, duration - (datetime.now(UTC) - scheduled).total_seconds()
            )
            while time.monotonic() < end:
                self.sample(ns, db)
                time.sleep(min(1, max(0, end - time.monotonic())))
            record["warmup_end_ns"] = time.monotonic_ns()
            record["warmup_quiet_seconds"] = self.reconcile(ns, db, [warmup], "warmup")
            record["warmup_reconciled_ns"] = time.monotonic_ns()
            record["measured_start_ns"] = time.monotonic_ns()
            arrivals = self.arrivals(ns, db, "measured", count, measured)
            record["measured_quiet_seconds"] = self.reconcile(
                ns, db, [warmup, arrivals], "measured"
            )
            record["terminal_observed_at"] = self.terminal_observed
            record["status"] = "PASS"
            save(directory / "record.json", record)
            result = summarize(directory, engineering=self.engineering)
            record.update(result)
            save(directory / "record.json", record)
            self.reset_episode(ns, db)
            # Seal after reset/export commands, before any same-job repeat.
            from dur053_k3_policy import artifact_hashes

            save(directory / "seal.json", artifact_hashes(directory))
            return result
        except Exception as error:
            directory.mkdir(exist_ok=True)
            record.update(
                status="FAIL", timed_out=isinstance(error, TimeoutError), error=str(error)
            )
            save(directory / "record.json", record)
            self.failure_export("dur053-kind")
            raise
        finally:
            self.output = self.root_output

    def calibration(self, addresses: dict[str, str]) -> dict[str, Any]:
        record: dict[str, Any] = {
            "schema": "dur053.cpu-calibration.v1",
            "status": "FAIL",
            "engineering": self.engineering,
            "candidates": [],
            "image": self.images["worker"],
        }
        path = self.root_output / "calibration.json"
        save(path, record)
        try:
            for units in CANDIDATES:
                self.arm, self.units = "cpu", units
                directory = self.root_output / f"calibration-{units}"
                ns, db = self.setup(directory, addresses)
                candidate: dict[str, Any] = {
                    "units": units,
                    "status": "IN_PROGRESS",
                    "discarded": 1,
                    "concurrency": 1,
                    "timeout_seconds": 120,
                    "measured_busy_seconds": [],
                    "invocation_elapsed_seconds": [],
                }
                record["candidates"].append(candidate)
                windows = []
                for i in range(4 if self.engineering else 22):
                    self.seed = 530100 + CANDIDATES.index(units) * 22 + i
                    began = time.monotonic()
                    arrivals = self.arrivals(
                        ns, db, f"invocation-{i}", 1, 1, concurrency=1, timeout=120
                    )
                    windows.append(arrivals)
                    self.reconcile(
                        ns,
                        db,
                        windows,
                        f"invocation-{i}",
                        seconds=max(0, 120 - (time.monotonic() - began)),
                    )
                    require(time.monotonic() - began <= 120, "pilot-calibration-timeout")
                    exported = json.loads((directory / f"invocation-{i}-witness.json").read_bytes())
                    durations = busy_times(exported, {arrivals["rows"][0]["workflow_id"]})
                    require(len(durations) == 1, "pilot-calibration-single-invocation")
                    if i:
                        candidate["measured_busy_seconds"].append(durations[0])
                    else:
                        candidate["discarded_busy_seconds"] = durations[0]
                    candidate["invocation_elapsed_seconds"].append(time.monotonic() - began)
                    save(path, record)
                candidate["status"] = "PASS"
                self.reset_episode(ns, db)
                self.output = self.root_output
                if (
                    sorted(candidate["measured_busy_seconds"])[1 if self.engineering else 10] >= 1
                    or units == 1000000
                ):
                    break
            record.update(select_cpu(record["candidates"], engineering=self.engineering))
            record["status"] = "PASS"
            record["persisted_ns"] = time.monotonic_ns()
            save(path, record)
            # The same-job calibration record is persisted and sealed before CPU initial.
            save(
                self.root_output / "calibration-seal.json",
                {"sha256": hashlib.sha256(path.read_bytes()).hexdigest()},
            )
            return record
        except Exception as error:
            record["error"] = str(error)
            save(path, record)
            self.failure_export("dur053-kind")
            raise
        finally:
            self.output = self.root_output
