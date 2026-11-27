"""Pre-traffic and per-cell evidence for the registered J2 path."""

import hashlib
import json
import os
import subprocess
from decimal import Decimal
from pathlib import Path

from j2_checker import ROOT
from j2_checker_report import ResourceEvidenceError
from j2_offline import SECOND
from j2_overlay import EXPORTER, PROM, PROMETHEUS, budget, registered_budget
from j2_qualification_evidence import file_record, read, require, write
from j2_qualify_volume import METRICS, runner_cgroup
from j2_verify_overlay import NS

PROFILE_SQL = """
SELECT json_build_object(
  'role_limit', rolconnlimit, 'settings', rolconfig,
  'max_connections', current_setting('max_connections')::int,
  'client_backends', (SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend'),
  'exporter_sessions', (SELECT coalesce(json_agg(application_name), '[]')
                        FROM pg_stat_activity WHERE usename='j2_exporter')
) FROM pg_roles WHERE rolname='j2_exporter'
"""


def runner_inventory(proc=Path("/proc")):
    model = next(
        (
            line.partition(":")[2].strip()
            for line in (proc / "cpuinfo").read_text().splitlines()
            if line.partition(":")[0].strip() == "model name"
        ),
        "",
    )
    meminfo = {
        line.partition(":")[0]: line.partition(":")[2].strip()
        for line in (proc / "meminfo").read_text().splitlines()
    }
    online = os.sysconf("SC_NPROCESSORS_ONLN")
    available = sorted(os.sched_getaffinity(0))
    memory = {key: int(meminfo[key].split()[0]) * 1024 for key in ("MemTotal", "MemAvailable")}
    cgroup = runner_cgroup()
    image = {key: os.environ.get(key) for key in ("ImageOS", "ImageVersion")}
    require(
        model
        and available
        and online >= len(available)
        and 0 < memory["MemAvailable"] <= memory["MemTotal"]
        and all(image.values())
        and all(row["present"] and row["value"] for key, row in cgroup.items() if "." in key),
        "runner inventory is incomplete",
    )
    return {
        "cpu_model": model,
        "online_cores": online,
        "available_cpu_ids": available,
        "memory_bytes": memory,
        **image,
        "kernel": os.uname().release,
        "cgroup": cgroup,
        "host_pressure": {
            name: (proc / "pressure" / name).read_text().strip() for name in ("cpu", "memory", "io")
        },
    }


def checked_configuration(rendered, running_config, sessions, exporter_source):
    require(
        running_config == "global:\n  scrape_interval: 1s\n  scrape_timeout: 1s\n"
        "scrape_configs:\n  - job_name: j2\n    static_configs:\n"
        "      - targets: [j2-exporter:9187]\n",
        "running Prometheus scrape configuration differs",
    )
    deployments = {
        row["metadata"]["name"]: row["spec"]
        for row in rendered["items"]
        if row["kind"] == "Deployment"
    }
    prom = deployments[PROM]["template"]["spec"]["containers"][0]
    require(
        prom["image"] == PROMETHEUS and not prom.get("env") and not prom.get("envFrom"),
        "Prometheus image or DB environment differs",
    )
    connection_budget = budget(rendered, registered_budget())
    require(
        sessions["role_limit"] == 1
        and set(sessions["settings"])
        == {"statement_timeout=2s", "default_transaction_read_only=on"}
        and sessions["exporter_sessions"] == ["j2-db-exporter"]
        and sessions["max_connections"] == 100
        and 1 <= sessions["client_backends"] <= connection_budget["with_admin"],
        "observed exporter timeout or connection budget differs",
    )
    require(
        all(
            fragment in exporter_source
            for fragment in (
                b"const interval = time.Second",
                b'const statementTimeout = "2000"',
                b"db.SetMaxOpenConns(1)",
                b"for range tick.C {",
                b"s.collect(context.Background()",
                b"QueryRowContext(ctx, query)",
            )
        ),
        "pinned exporter cadence, statement timeout or serial query mechanism differs",
    )
    return {
        "sample_interval_seconds": 1,
        "scrape_interval_seconds": 1,
        "scrape_timeout_seconds": 1,
        "statement_timeout_ms": 2000,
        "max_concurrent_queries": 1,
        "budget": connection_budget,
        "live": sessions,
    }


def fresh_telemetry(driver, exporter, database):
    lock = read(ROOT / "experiments/portfolio/load-envelope/exporter.lock.json")
    require(lock["image"] == exporter, "exporter lock drift")
    source_path = "experiments/portfolio/load-envelope/exporter/main.go"
    # Compare against the source tree from which the retained image was built.
    source = subprocess.check_output(
        ["git", "show", f"{lock['build_head']}:{source_path}"],
        cwd=ROOT,
        timeout=20,
    )
    require(
        source == (ROOT / source_path).read_bytes().replace(b"\r\n", b"\n"),
        "exporter source differs from the pinned build",
    )
    rendered = read(driver.output / "overlay-rendered.json")
    config = driver.kube("configmap", PROM, "-n", NS)["data"]["prometheus.yml"]
    session = json.loads(driver.sql(PROFILE_SQL, database))
    record = checked_configuration(rendered, config, session, source)
    require(
        any(
            item["kind"] == "Deployment"
            and item["metadata"]["name"] == EXPORTER
            and item["spec"]["template"]["spec"]["containers"][0]["image"] == exporter
            for item in rendered["items"]
        ),
        "rendered exporter image differs",
    )
    record.update(
        exporter=exporter,
        exporter_source_sha256=hashlib.sha256(source).hexdigest(),
        freshness=read(driver.output / "freshness.json"),
    )
    require(record["freshness"]["distinct_samples"] >= 5, "fresh telemetry lost")
    # The preflight series is distinct from the following cell's raw samples.
    old = driver.output / "prometheus-samples.json"
    old.replace(driver.output / "pre-traffic-prometheus.json")
    write(driver.output / "pre-traffic-telemetry.json", record)
    return file_record(driver.output / "pre-traffic-telemetry.json")


def cell_samples(series, schedule):
    windows = [("grid", schedule["warmup_end_ns"], schedule["stop_ns"])]
    if "hold_end_ns" in schedule:
        step = schedule["hold_end_ns"]
        windows = [("hold", schedule["warmup_end_ns"], step), ("step", step, schedule["stop_ns"])]
    try:
        points = {}
        for name in METRICS:
            (item,) = series[name]
            values = [(Decimal(str(t)), Decimal(str(v))) for t, v in item["values"]]
            require(
                values
                and all(t.is_finite() and v.is_finite() for t, v in values)
                and len({t for t, _ in values}) == len(values),
                f"unverifiable series {name}",
            )
            points[name] = dict(values)
        up, ok, stamp = (
            points[name] for name in ("up", "j2_query_ok", "j2_sample_timestamp_seconds")
        )
        require(all(v in (0, 1) for v in (*up.values(), *ok.values())), "invalid scrape health")
        source = {
            name: {
                int(epoch * SECOND)
                for t, epoch in stamp.items()
                if t in rows and up.get(t) == ok.get(t) == 1
            }
            for name, rows in points.items()
        }
        coverage = {}
        for label, start, end in windows:
            edges = [start + (end - start) * i // 6 for i in range(7)]
            coverage[label] = {}
            for name, timestamps in source.items():
                actual = sorted(ns for ns in timestamps if start <= ns < end)
                ordered = [start, *actual, end]
                gap = max(b - a for a, b in zip(ordered, ordered[1:], strict=False))
                empty = [
                    i + 1
                    for i, (lo, hi) in enumerate(zip(edges, edges[1:], strict=False))
                    if not any(lo <= ns < hi for ns in actual)
                ]
                coverage[label][name] = {"maximum_gap_ns": gap, "empty_bins": empty}
        start, stop = (Decimal(schedule[k]) / SECOND for k in ("origin_ns", "stop_ns"))
        return {
            "failed_scrapes": sum(v == 0 for t, v in up.items() if start <= t < stop),
            "failed_queries": sum(v == 0 for t, v in ok.items() if start <= t < stop),
            "coverage": coverage,
        }
    except (KeyError, TypeError, ValueError, ArithmeticError) as error:
        raise ResourceEvidenceError(f"unverifiable required telemetry: {error}") from error


def coverage_gaps(samples):
    return {
        name
        for window in samples["coverage"].values()
        for name, info in window.items()
        if info["maximum_gap_ns"] > 5 * SECOND or info["empty_bins"]
    }
