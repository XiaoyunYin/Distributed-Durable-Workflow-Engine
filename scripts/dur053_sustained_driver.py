"""Versioned B.6 runner; v1 PilotDriver execution is deliberately untouched."""

from __future__ import annotations

import json
import math
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from dur053_k3 import save
from dur053_k3_policy import artifact_hashes, require
from dur053_pilot import busy_times, nearest_rank
from dur053_pilot_driver import PilotDriver
from dur053_sustained import duration, operating_record, stationarity, summarize, version_rate


class SustainedDriver(PilotDriver):
    def sample(self, ns: str, db: str) -> None:
        super().sample(ns, db)
        if not getattr(self, "engine_sampling", False):
            return
        counts = self.sql(
            "SELECT json_build_object('outstanding', (SELECT count(*) FROM "
            "engine.workflow_executions WHERE state NOT IN "
            "('SUCCEEDED','FAILED','REJECTED','CANCELED','ABANDONED')), "
            "'unpublished', (SELECT count(*) FROM engine.outbox WHERE published_at IS NULL));",
            db,
        )
        row = {**json.loads(counts), "observed_at": datetime.now(UTC).isoformat()}
        with (self.output / "engine-samples.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, sort_keys=True) + "\n")

    def drift_check(self, addresses: dict[str, str], version: int) -> dict[str, Any]:
        """A single fixed candidate; this method never calls select_cpu."""
        self.arm, self.units = "cpu", 1000000
        directory = self.root_output / "cpu-drift"
        record: dict[str, Any] = {
            "status": "FAIL",
            "units": self.units,
            "reference_median_seconds": 0.514,
            "image": self.images["worker"],
            "discarded": 1,
            "concurrency": 1,
            "timeout_seconds": 120,
            "measured_busy_seconds": [],
            "invocation_elapsed_seconds": [],
        }
        path = self.root_output / "cpu-drift.json"
        save(path, record)
        try:
            ns, db = self.setup(directory, addresses)
            windows = []
            for i in range(4 if self.engineering else 22):
                self.seed = 536000 + version * 100 + i
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
                require(time.monotonic() - began <= 120, "b6-drift-timeout")
                witness = json.loads((directory / f"invocation-{i}-witness.json").read_bytes())
                values = busy_times(witness, {arrivals["rows"][0]["workflow_id"]})
                require(len(values) == 1, "b6-drift-single")
                if i:
                    record["measured_busy_seconds"].append(values[0])
                else:
                    record["discarded_busy_seconds"] = values[0]
                record["invocation_elapsed_seconds"].append(time.monotonic() - began)
                save(path, record)
            record.update(
                status="PASS",
                median_busy_seconds=nearest_rank(record["measured_busy_seconds"], 0.5),
            )
            record["drift_seconds"] = record["median_busy_seconds"] - 0.514
            record["persisted_ns"] = time.monotonic_ns()
            save(path, record)
            self.reset_episode(ns, db)
            return record
        finally:
            self.output = self.root_output

    def sustained(
        self,
        arm: str,
        index: int,
        addresses: dict[str, str],
        registration: dict[str, Any],
        version: int,
    ) -> dict[str, Any]:
        self.arm, self.units = arm, 1000000 if arm == "cpu" else 10000
        self.seed = 534000 + version * 100 + {"short": 0, "cpu": 10, "long": 20}[arm] + index
        q = version_rate(registration, version)
        warm, T = (6, 60) if self.engineering else (120, duration(q))
        directory = self.root_output / f"{arm}-{index}"
        record = {
            "status": "FAIL",
            "timed_out": False,
            "arm": arm,
            "index": index,
            "version": version,
            "engineering": self.engineering,
            "slots": 4,
            "faults": [],
            "autoscaling": "paused-at-two-replicas",
            "seed": self.seed,
            "reconciliation_timeout_seconds": 600,
            "cpu_units": self.units,
            "warmup_rate": q,
            "measured_rate": q,
            "registration": registration,
            "measurement_seconds": T,
            "rate_precision": operating_record(registration, version),
        }
        previous_sampling = getattr(self, "engine_sampling", False)
        self.engine_sampling = True
        try:
            ns, db = self.setup(directory, addresses)
            if arm == "cpu":
                import hashlib

                drift = self.root_output / "cpu-drift.json"
                record["cpu_drift_sha256"] = hashlib.sha256(drift.read_bytes()).hexdigest()
                record["cpu_drift_persisted_ns"] = json.loads(drift.read_bytes())["persisted_ns"]
            record["warmup_start_ns"] = time.monotonic_ns()
            save(directory / "record.json", record)
            # One absolute loadgen schedule crosses the warmup boundary. No stop,
            # drain, checker, quiet wait or second definition setup at the boundary.
            continuous = self.arrivals(ns, db, "continuous", math.ceil((warm + T) * q), q)
            origin = datetime.fromisoformat(
                continuous["rows"][0]["scheduled_at"].replace("Z", "+00:00")
            )
            boundary = origin + timedelta(seconds=warm)
            record["measurement_start_at"] = boundary.isoformat()
            for label, predicate in (
                ("warmup", lambda t: t < boundary),
                ("measured", lambda t: t >= boundary),
            ):
                rows = [
                    r
                    for r in continuous["rows"]
                    if predicate(datetime.fromisoformat(r["scheduled_at"].replace("Z", "+00:00")))
                ]
                save(
                    directory / (label + "-arrivals.json"),
                    {**continuous, "rows": rows, "offered": len(rows)},
                )
                record[label + "_count"] = len(rows)
            # Observe through the end of the entire T, including its final partial interval.
            end = origin + timedelta(seconds=warm + T)
            while datetime.now(UTC) < end:
                self.sample(ns, db)
                time.sleep(min(1, max(0, (end - datetime.now(UTC)).total_seconds())))
            record["measured_quiet_seconds"] = self.reconcile(ns, db, [continuous], "measured")
            record.update(terminal_observed_at=self.terminal_observed, status="PASS")
            save(directory / "record.json", record)
            record.update(summarize(directory, engineering=self.engineering))
            record["stationarity_gate"] = stationarity(directory)
            # A gate failure is retained valid no-fault evidence, not a replayable error.
            save(directory / "record.json", record)
            self.reset_episode(ns, db)
            save(directory / "seal.json", artifact_hashes(directory))
            return record
        except Exception as error:
            directory.mkdir(exist_ok=True)
            record.update(
                status="FAIL", timed_out=isinstance(error, TimeoutError), error=str(error)
            )
            save(directory / "record.json", record)
            self.failure_export("dur053-kind")
            raise
        finally:
            self.engine_sampling = previous_sampling
            self.output = self.root_output
