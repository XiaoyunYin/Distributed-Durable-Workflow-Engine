"""Bounded, passive attribution samples outside the locked J2 exporter."""

import json
import time
from contextlib import contextmanager
from pathlib import Path
from threading import Event, Thread

from j2_qualification_evidence import write
from j2_relay_diagnostics import bracketed

INTERVAL_SECONDS = 15

# Projection from J3 R405's pg_stat_activity/pg_locks diagnostic (94f5373).
# DrainHarness.read_database supplies the read-only transaction and 2 s timeout.
PG_SNAPSHOT = """
SELECT json_build_object(
    'sampled_at', clock_timestamp(),
    'pg_stat_activity', (SELECT coalesce(json_agg(a), '[]') FROM (
        SELECT pid, leader_pid, datname, usename, application_name, backend_type, state,
               wait_event_type, wait_event, xact_start, query_start, state_change,
               backend_xid, backend_xmin, query_id
        FROM pg_catalog.pg_stat_activity WHERE pid <> pg_backend_pid()) AS a),
    'pg_locks', (SELECT coalesce(json_agg(l), '[]') FROM pg_catalog.pg_locks AS l)
);
"""
PG_DATABASE_COUNTERS = """
    'database_counters', (SELECT row_to_json(s) FROM (
        SELECT xact_commit, xact_rollback, blks_read, blks_hit, blk_read_time, blk_write_time
        FROM pg_catalog.pg_stat_database WHERE datname = current_database()) AS s),
"""


@contextmanager
def attribution_samples(driver, *, campaign=False):
    output = driver.output
    done = Event()
    query = PG_SNAPSHOT
    if campaign:
        query = query.replace("    'sampled_at'", PG_DATABASE_COUNTERS + "    'sampled_at'")
    write(
        output / "attribution-sampler.json",
        {
            "interval_seconds": INTERVAL_SECONDS,
            "max_additional_database_sessions": 1,
            "connection_budget": "second existing future-monitor reservation",
            "node_samples": ["observer-resources.tsv", "relay-resources.tsv"],
        },
    )

    def sample():
        target = time.monotonic()
        with (output / "attribution-samples.jsonl").open("a", encoding="utf-8") as stream:
            while not done.wait(max(0, target - time.monotonic())):
                started = time.monotonic_ns()
                row = {"unix_ns": time.time_ns(), "errors": {}}
                sources = {
                    "postgres": lambda: json.loads(driver.read_database(query)),
                    "kafka_groups": lambda: (
                        driver.kafka(
                            "consumer-groups",
                            "--describe",
                            "--offsets",
                            "--all-groups",
                            record=False,
                            timeout=5,
                        ).stdout
                    ),
                    "runner": lambda: {
                        name: (Path("/proc") / name).read_text()
                        for name in (
                            "stat",
                            "loadavg",
                            "pressure/cpu",
                            "pressure/io",
                            "pressure/memory",
                        )
                    },
                }
                for name, probe in sources.items():
                    if done.is_set():
                        break
                    try:
                        value, bounds = bracketed(probe)
                        row[name] = {**bounds, "value": value}
                    except (OSError, ValueError, KeyError) as error:
                        row["errors"][name] = str(error)
                row["wall_ns"] = time.monotonic_ns() - started
                stream.write(json.dumps(row) + "\n")
                stream.flush()
                target = max(time.monotonic(), target + INTERVAL_SECONDS)

    thread = Thread(target=sample, daemon=True)
    thread.start()
    try:
        yield
    finally:
        done.set()
        thread.join(15)
        if thread.is_alive():
            raise TimeoutError("bounded attribution sampler did not exit")
