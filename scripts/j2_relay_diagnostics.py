"""Passive relay attribution for the J2 engineering drains."""

import json
import time
from contextlib import contextmanager
from threading import Event, Thread

from j2_harness import APP, REPLICAS, partition_end_offsets
from j2_offline import SECOND
from j2_qualification_evidence import DRAIN_SECONDS, read, require, write
from j2_schedule import timestamp
from j2_schedule_rehearsal import container_resources, container_target

OFFSET_INTERVAL = 15
OFFSET_TIMEOUT = 8
CPU_FIELDS = ("usage_usec", "nr_periods", "nr_throttled", "throttled_usec")
READ_ERRORS = (OSError, ValueError, KeyError)


def bracketed(probe):
    before_mono, before_wall = time.monotonic_ns(), time.time_ns()
    value = probe()
    return value, {
        "lower_mono_ns": before_mono,
        "lower_unix_ns": before_wall,
        "upper_unix_ns": time.time_ns(),
        "upper_mono_ns": time.monotonic_ns(),
    }


def publication_timeline(driver, start_ns, end_ns):
    # PostgreSQL timestamps are microsecond precision. Round bounds up so
    # comparisons preserve the half-open interval for every stored timestamp.
    def postgres_timestamp(ns):
        return timestamp(((ns + 999) // 1000) * 1000)[:-4] + "Z"

    start, end = map(postgres_timestamp, (start_ns, end_ns))
    sql = f"""
SELECT floor(extract(epoch FROM published_at - TIMESTAMPTZ '{start}'))::int AS second,
       count(*)
FROM engine.outbox
WHERE published_at >= TIMESTAMPTZ '{start}' AND published_at < TIMESTAMPTZ '{end}'
GROUP BY 1 ORDER BY 1;
"""
    raw, bracket = bracketed(lambda: driver.read_database(sql))
    counts = {}
    seconds = (end_ns - start_ns + SECOND - 1) // SECOND
    for line in raw.splitlines():
        second, count = map(int, line.split("|"))
        require(
            0 <= second < seconds and second not in counts and count > 0,
            "invalid durable outbox publication bucket",
        )
        counts[second] = count
    return {
        **bracket,
        "start_unix_ns": start_ns,
        "end_exclusive_unix_ns": end_ns,
        "published_rows_by_second": [counts.get(second, 0) for second in range(seconds)],
        "total_published_rows": sum(counts.values()),
        "source": "engine.outbox.published_at",
    }


def retain_logs(driver):
    logs = driver.output / "relay-attribution-logs"
    logs.mkdir()
    records = []
    for role in ("relay", "scheduler"):
        try:
            pods = driver.kube("pods", "-n", APP, "-l", f"app={role}", timeout=10)["items"]
        except READ_ERRORS as error:
            records.append({"role": role, "error": str(error)})
            continue
        if len(pods) != REPLICAS[role]:
            records.append({"role": role, "error": f"unexpected {role} count: {len(pods)}"})
        for pod in pods:
            name = pod["metadata"]["name"]
            path = logs / f"{name}.log"
            record = {"role": role, "pod": name, "uid": pod["metadata"]["uid"], "container": role}
            try:
                result = driver.k(
                    "logs",
                    "-n",
                    APP,
                    name,
                    "-c",
                    role,
                    "--timestamps=true",
                    timeout=15,
                    record=False,
                    allowed=True,
                )
                output = result.stdout + result.stderr
                for secret in (driver.password, driver.demand_token):
                    if secret:
                        output = output.replace(secret, "[REDACTED]")
                path.write_text(output, encoding="utf-8")
                record.update(
                    file=path.relative_to(driver.output).as_posix(), exit=result.returncode
                )
                require(result.returncode == 0, f"could not capture {role} logs for {name}")
            except READ_ERRORS as error:
                record["error"] = str(error)
            records.append(record)
    return records


def cpu_evidence(output, start_ns, end_ns):
    summary = read(output / "relay-resource-summary.json")
    require(
        summary["exit"] == 0
        and summary["samples"] >= 2
        and summary["complete"]
        and not summary["errors"]
        and summary["first_sample_unix_ns"] <= start_ns
        and summary["last_sample_unix_ns"] >= end_ns - 2 * SECOND
        and 0 < summary["max_gap_ns"] <= 3 * SECOND,
        "relay CPU samples do not cover the drain",
    )
    first = last = None
    first_at = last_at = None
    for line in (output / "relay-resources.tsv").read_text(encoding="utf-8").splitlines():
        stamp, _, _, body = line.split("\t", 3)
        at = int(stamp)
        counters = dict(counter.split() for counter in body.split(";"))
        row = {field: int(counters[field]) for field in CPU_FIELDS}
        require(min(row.values()) >= 0, "negative relay CPU counters")
        if at <= start_ns:
            first, first_at = row, at
        if last is not None:
            require(
                all(row[field] >= last[field] for field in CPU_FIELDS),
                "relay CPU counters regressed",
            )
        last, last_at = row, at
        if at >= end_ns:
            break
    require(first is not None and last is not None, "missing relay CPU boundary samples")
    return {
        "first_sample_unix_ns": first_at,
        "last_sample_unix_ns": last_at,
        "first": first,
        "last": last,
        "delta": {field: last[field] - first[field] for field in CPU_FIELDS},
    }


class DrainOffsets:
    def __init__(self, driver, stop_mono):
        self.driver = driver
        self.stop_mono = stop_mono
        self.stop = Event()
        self.rows = []
        self.poll_error = None
        self.thread = Thread(target=self.poll)

    def capture(self, phase):
        def offsets():
            try:
                raw = self.driver.kafka(
                    "get-offsets", "--time", "-1", timeout=OFFSET_TIMEOUT, record=False
                ).stdout
                return {"offsets": partition_end_offsets(raw), "raw": raw}
            except READ_ERRORS as error:
                return {"error": str(error)}

        result, bracket = bracketed(offsets)
        row = {"phase": phase, **bracket, **result}
        with (self.driver.output / "drain-kafka-offsets.jsonl").open("a", encoding="utf-8") as log:
            log.write(json.dumps(row) + "\n")
        self.rows.append(row)

    def poll(self):
        target = self.stop_mono
        while not self.stop.wait(max(0, (target - time.monotonic_ns()) / SECOND)):
            if time.monotonic_ns() + OFFSET_TIMEOUT * SECOND >= (
                self.stop_mono + DRAIN_SECONDS * SECOND
            ):
                break
            try:
                self.capture("during-drain")
            except OSError as error:
                self.poll_error = str(error)
                break
            target += OFFSET_INTERVAL * SECOND

    def close(self, end_mono):
        self.stop.set()
        self.thread.join()
        self.capture("closeout")
        require(
            self.poll_error is None, f"could not retain Kafka offset samples: {self.poll_error}"
        )
        successful = [row for row in self.rows if "offsets" in row]
        require(len(successful) == len(self.rows), "Kafka offset read failed during attribution")
        require(
            len(successful) >= 2
            and self.stop_mono
            <= successful[0]["lower_mono_ns"]
            <= min(end_mono, self.stop_mono + OFFSET_INTERVAL * SECOND)
            and successful[-1]["lower_mono_ns"] >= end_mono,
            "Kafka offset snapshots do not bracket the drain closeout",
        )
        in_drain = [row["lower_mono_ns"] for row in successful if row["lower_mono_ns"] < end_mono]
        bounds = [self.stop_mono, *in_drain, end_mono]
        max_gap = max(right - left for left, right in zip(bounds, bounds[1:], strict=False))
        require(
            0 < max_gap <= (2 * OFFSET_INTERVAL + OFFSET_TIMEOUT) * SECOND,
            "Kafka offset sampling left a gap in the drain timeline",
        )
        require(
            all(
                previous["offsets"][topic][partition] <= offset
                for previous, current in zip(successful, successful[1:], strict=False)
                for topic, partitions in current["offsets"].items()
                for partition, offset in partitions.items()
            ),
            "Kafka partition end offset regressed during the drain",
        )
        return {
            "samples": len(successful),
            "interval_seconds": OFFSET_INTERVAL,
            "max_gap_ns": max_gap,
            "closeout_delay_ns": successful[-1]["lower_mono_ns"] - end_mono,
        }


@contextmanager
def relay_attribution(driver, schedule, stop_mono, work_deadline, target):
    output = driver.output
    name = target["pod"]
    offsets = DrainOffsets(driver, stop_mono)
    record = {"errors": {}, "scheduled_stop_mono_ns": stop_mono}

    def retain(label, probe):
        try:
            record[label] = probe()
        except READ_ERRORS as error:
            record["errors"][label] = str(error)

    start = schedule["stop_ns"]
    end = start
    try:
        with container_resources(driver, target, "relay", ("cpu.stat",)):
            offsets.thread.start()
            try:
                yield
            finally:
                driver.deadline = work_deadline
                result_path = output / "drain-result.json"
                if result_path.is_file() and read(result_path)["status"] == "PASS":
                    final = read(output / "final.json")
                    end_mono, end = final["upper_mono_ns"], final["upper_unix_ns"]
                else:
                    elapsed = max(0, min(DRAIN_SECONDS * SECOND, time.monotonic_ns() - stop_mono))
                    end_mono, end = stop_mono + elapsed, start + elapsed
                record["observed_through_mono_ns"] = end_mono
                retain("kafka_end_offsets", lambda: offsets.close(end_mono))

                def publications():
                    timeline = publication_timeline(driver, start, end)
                    write(output / "outbox-publications.json", timeline)
                    return {
                        "file": "outbox-publications.json",
                        "rows": timeline["total_published_rows"],
                    }

                retain("outbox_publications", publications)
                retain("logs", lambda: retain_logs(driver))
                if any("error" in log for log in record.get("logs", [])):
                    record["errors"]["logs"] = "one or more container logs could not be collected"
                retain("final_relay_target", lambda: container_target(driver, name))
                if record.get("final_relay_target") != target:
                    record["errors"]["relay_identity"] = "relay container identity changed"
    finally:
        retain("relay_cpu", lambda: cpu_evidence(output, start, end))
        record["status"] = "FAIL" if record["errors"] else "PASS"
        write(output / "relay-attribution.json", record)
    require(not record["errors"], f"incomplete relay attribution: {record['errors']}")
