"""Compare the deployed and candidate J3 projection on hosted PostgreSQL."""

import argparse
import json
import os
import subprocess
import time
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from math import ceil
from pathlib import Path
from threading import Thread
from types import SimpleNamespace
from urllib.parse import parse_qs, quote, urlsplit

from dur053_k3 import download
from j3_diagnostics import enable_exporter_diagnostics

ROOT = Path(__file__).resolve().parents[1]
J3 = ROOT / "experiments/portfolio/observability"
POSTGRES = "j3-bench-postgres"
EXPORTER = "j3-bench-exporter"
TARGET_COUNT = 20_000
FOREIGN_COUNT = 2141
CYCLES = 120
JIT_SAMPLES = 12
PHASE_SECONDS = CYCLES + 5
ARRIVAL_RATE = 7.136100570066256
SAMPLE = "SELECT j2_monitor.j3_sample()"


def run(*args, data=None, timeout=60):
    result = subprocess.run(
        args,
        input=data,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=timeout,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(f"{args[:2]} exited {result.returncode}: {result.stderr}")
    return result


def save(path, data):
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def psql_args(database, role="postgres", application="j3-bench-admin"):
    return [
        "docker",
        "exec",
        "-i",
        "-e",
        "LC_ALL=C",
        "-e",
        f"PGAPPNAME={application}",
        POSTGRES,
        "psql",
        "-XqAt",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        role,
        "-d",
        database,
    ]


def sql(database, statement, *, role="postgres"):
    return run(*psql_args(database, role), data=statement, timeout=120).stdout.strip()


def cpu_usec(container):
    pid = int(run("docker", "inspect", "--format", "{{.State.Pid}}", container).stdout)
    membership = Path(f"/proc/{pid}/cgroup").read_text(encoding="utf-8")
    (group,) = [entry[4:] for entry in membership.splitlines() if entry.startswith("0::/")]
    counters = (Path("/sys/fs/cgroup") / group / "cpu.stat").read_text(encoding="utf-8")
    return int(dict(entry.split() for entry in counters.splitlines())["usage_usec"])


def wait_postgres():
    for _ in range(60):
        try:
            run("docker", "exec", POSTGRES, "pg_isready", "-h", "127.0.0.1", "-U", "postgres")
        except RuntimeError:
            time.sleep(1)
        else:
            return
    raise RuntimeError("benchmark PostgreSQL did not become ready")


def role_sql(source):
    return (
        source.replace(":'namespace'", "'target'")
        .replace(":'foreign'", "'foreign'")
        .replace(":'password'", "'bench-fixture'")
        .replace(":'u_ns'", "'60000000'")
    )


def engine_indexes(database):
    return json.loads(
        sql(
            database,
            """
SELECT coalesce(json_agg(indexdef ORDER BY indexname), '[]')
FROM pg_indexes WHERE schemaname = 'engine';
""",
        )
    )


def create_fixture(database, projection, output):
    sql("postgres", f'CREATE DATABASE "{database}";')
    for path in sorted((ROOT / "migrations").glob("*.up.sql")):
        if int(path.name.split("_", 1)[0]) in {1, 2, 3, 6, 15, 17, 19}:
            sql(database, path.read_text(encoding="utf-8"))
    original = engine_indexes(database)
    sql(database, role_sql(projection))
    sql(database, role_sql((J3 / "foreign.sql").read_text(encoding="utf-8")))
    sql(database, (J3 / "exporter/benchmark.sql").read_text(encoding="utf-8"))
    installed = engine_indexes(database)
    save(
        output / f"{database}-engine-indexes.json",
        {
            "original": original,
            "installed": installed,
            "equal": installed == original,
        },
    )
    if installed != original:
        raise RuntimeError(f"{database} fixture changed the engine indexes")


def populate_fixture(database):
    sql(database, f"SELECT j3_bench.add_targets(1, {TARGET_COUNT});")
    for first in range(1, FOREIGN_COUNT + 1, 200):
        sql(
            database,
            f"SELECT count(j2_monitor.j3_seed('foreign-' || i)) "
            f"FROM generate_series({first}, {min(first + 199, FOREIGN_COUNT)}) i;",
            role="j3_fixture",
        )
    sql(database, "ANALYZE;")


def population(database):
    return json.loads(
        sql(
            database,
            """
SELECT coalesce(json_object_agg(namespace, count), '{}'::json)
FROM (SELECT namespace, count(*) FROM engine.workflow_executions GROUP BY namespace) n;
""",
        )
    )


def explain(database, projection, output, *, jit="off"):
    body = projection.split("AS $$\n", 1)[1].split("\n$$;", 1)[0]
    options = "EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON) "
    errors = {}
    for name, query, role in (("function", SAMPLE, "j3_exporter"), ("body", body, "postgres")):
        try:
            result = sql(
                database,
                f"SET jit = {jit}; SET statement_timeout = '2s'; {options}{query};",
                role=role,
            )
        except (RuntimeError, subprocess.TimeoutExpired) as error:
            errors[name] = str(error)
        else:
            (output / f"explain-{name}.json").write_text(result + "\n", encoding="utf-8")
    if errors:
        save(output / "explain-errors.json", errors)
    return not errors


def benchmark_role(database, output):
    output.mkdir()

    def postgres(statement, *, role="postgres"):
        return run(*psql_args(database, role), data=statement)

    enable_exporter_diagnostics(SimpleNamespace(output=output, postgres=postgres))


def measure_jit_floor(database, projection, output):
    """Compare the deployed projection on empty tables before fixture ANALYZE."""
    output.mkdir()
    report = {"population": population(database), "variants": {}}
    if report["population"]:
        raise RuntimeError("JIT floor comparison requires empty workflow tables")
    settings = """
        SELECT json_object_agg(name, setting ORDER BY name) FROM pg_settings
        WHERE name IN ('jit', 'jit_provider', 'jit_above_cost', 'jit_inline_above_cost',
                       'jit_optimize_above_cost', 'statement_timeout', 'track_commit_timestamp',
                       'auto_explain.log_analyze', 'auto_explain.log_timing');
    """
    report["server_settings"] = json.loads(sql(database, settings))
    for jit in ("on", "off"):
        variant = output / jit
        variant.mkdir()
        evidence = report["variants"][jit] = {}
        try:
            setup = f"SET jit = {jit};\n"
            evidence["session_settings"] = json.loads(
                sql(database, setup + settings, role="j3_exporter")
            )
            evidence["explain_complete"] = explain(database, projection, variant, jit=jit)
            body_plan = variant / "explain-body.json"
            if body_plan.is_file():
                evidence["body_plan_used_jit"] = "JIT" in json.loads(body_plan.read_bytes())[0]
            # Two fresh role sessions use the same warmup and native PostgreSQL JIT thresholds.
            commands = setup + "\\o /dev/null\n" + SAMPLE + ";\n\\timing on\n"
            result = run(
                *psql_args(database, "j3_exporter", f"j3-bench-jit-{jit}"),
                data=commands + (SAMPLE + ";\n") * JIT_SAMPLES,
            )
            (variant / "psql-timing.log").write_text(
                result.stdout + result.stderr, encoding="utf-8"
            )
            rows = [
                {"sql_ns": round(float(line.split()[1]) * 1_000_000)}
                for line in result.stdout.splitlines()
                if line.startswith("Time: ")
            ]
            evidence["samples"] = len(rows)
            evidence["sql_ns"] = latency(rows, "sql_ns")
        except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
            evidence["error"] = str(error)
    on = report["variants"]["on"]
    off = report["variants"]["off"]
    valid = all(
        evidence.get("samples") == JIT_SAMPLES
        and evidence.get("explain_complete")
        and evidence["session_settings"].get("jit") == jit
        for jit, evidence in report["variants"].items()
    )
    report["comparison_valid"] = valid
    report["body_plans_show_jit_contrast"] = (
        on.get("body_plan_used_jit") is True and off.get("body_plan_used_jit") is False
    )
    save(output / "summary.json", report)
    return report


@contextmanager
def writer(database, path, *, foreign):
    name = "foreign" if foreign else "target"
    role = "j3_fixture" if foreign else "postgres"
    start = (FOREIGN_COUNT if foreign else TARGET_COUNT) + 1
    application = f"j3-bench-{name}"
    fixture = path / f"{name}-writes.sql"
    with fixture.open("w", encoding="utf-8") as stream:
        for i in range(start, start + 2000):
            query = (
                f"j2_monitor.j3_seed('foreign-{i}')"
                if foreign
                else f"j3_bench.add_targets({i}, {i})"
            )
            stream.write(f"SELECT {query} \\g /dev/null\nSELECT pg_sleep(0.12) \\g /dev/null\n")
    # Each statement is an autocommitted write on an independent connection.
    with (
        fixture.open("rb") as commands,
        (path / f"{name}-writes.log").open("wb") as log,
        subprocess.Popen(
            psql_args(database, role, application), stdin=commands, stdout=log, stderr=log
        ) as process,
    ):
        try:
            yield process
        finally:
            if process.poll() is None:
                try:
                    sql(
                        database,
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        f"WHERE application_name='{application}';",
                    )
                    process.wait(timeout=5)
                finally:
                    if process.poll() is None:
                        process.kill()


class Demand(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        if self.path != "/v1/demand" or self.headers.get("Authorization") != "Bearer bench-fixture":
            self.send_error(401)
            return
        body = json.dumps(
            {
                "namespace": "target",
                "sampled_at": datetime.now(UTC).isoformat(),
                "oldest_claimable_age_seconds": 0,
                "claimable_attempts": 0,
                "active_claims": 0,
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


def measure_inventory(output, tool):
    """Compare the changed host-side pod inventories with a fixed API response."""
    from j3_episode import Sampler
    from j3_overlay import APP, EXPORTER, NS, PINS, RECEIVER

    kubectl = output / "kubectl"
    kubectl.write_bytes(download(tool["url"], tool["sha256"]))
    kubectl.chmod(0o700)
    sampler = Sampler(None, {*PINS, EXPORTER, RECEIVER}, "foreign")
    selector = "app in (" + ",".join(sorted(sampler.roles | sampler.pins | {"worker"})) + ")"
    pods = {
        name: [
            {"metadata": {"namespace": name, "name": f"{role}-fixture", "labels": {"app": role}}}
            for role in sorted(roles)
        ]
        for name, roles in ((APP, sampler.roles | {"worker"}), (NS, sampler.pins))
    }
    receiver = [p for p in pods[NS] if p["metadata"]["labels"]["app"] == RECEIVER]
    app_url = f"/api/v1/namespaces/{APP}/pods"
    telemetry_url = f"/api/v1/namespaces/{NS}/pods"
    receiver_url = telemetry_url + "?labelSelector=" + quote(f"app={RECEIVER}")
    all_url = "/api/v1/pods?labelSelector=" + quote(selector)
    fixtures = {
        (app_url, ""): pods[APP],
        (telemetry_url, ""): pods[NS],
        (telemetry_url, f"app={RECEIVER}"): receiver,
        ("/api/v1/pods", selector): pods[APP] + pods[NS],
    }

    class API(Demand):
        def do_GET(self):  # noqa: N802
            request = urlsplit(self.path)
            path = request.path, parse_qs(request.query).get("labelSelector", [""])[0]
            if path not in fixtures:
                self.send_error(404)
                return
            body = json.dumps({"apiVersion": "v1", "kind": "PodList", "items": fixtures[path]})
            raw = body.encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

    try:
        with ThreadingHTTPServer(("127.0.0.1", 0), API) as server:
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                results = {}
                for name, urls in (
                    ("baseline", [app_url, telemetry_url, receiver_url]),
                    ("candidate", [all_url]),
                ):
                    start, cpu = time.monotonic(), os.times()
                    for _ in range(30):
                        inventory = [
                            json.loads(
                                run(
                                    str(kubectl),
                                    f"--kubeconfig={os.devnull}",
                                    f"--server=http://127.0.0.1:{server.server_port}",
                                    "get",
                                    "--raw",
                                    url,
                                ).stdout
                            )["items"]
                            for url in urls
                        ]
                        if name == "baseline":
                            valid = inventory == [pods[APP], pods[NS], receiver]
                        else:
                            valid = [
                                [p for p in inventory[0] if p["metadata"]["namespace"] == scope]
                                for scope in (APP, NS)
                            ] == [pods[APP], pods[NS]]
                        if not valid:
                            raise RuntimeError("inventory fixture differed")
                    final = os.times()
                    results[name] = {
                        "cycles": 30,
                        "get_calls_per_cycle": len(urls),
                        "wall_s": time.monotonic() - start,
                        "parent_cpu_s": final.user + final.system - cpu.user - cpu.system,
                        "kubectl_cpu_s": (
                            final.children_user
                            + final.children_system
                            - cpu.children_user
                            - cpu.children_system
                        ),
                    }
            finally:
                server.shutdown()
                thread.join(timeout=5)
    finally:
        kubectl.unlink()
    return {"scope": "pod inventory subprocesses and synthetic API only", **results}


def latency(rows, field):
    values = sorted(row[field] for row in rows)
    if not values:
        return None
    return {
        "p50_ns": values[ceil(len(values) * 0.5) - 1],
        "p99_ns": values[ceil(len(values) * 0.99) - 1],
        "max_ns": values[-1],
    }


def summarize(rows):
    failures = [
        row
        for row in rows
        if row.get("sql_failure") or row.get("demand_failure") or row.get("evidence_failed")
    ]
    cycle = latency(rows, "cycle_ns")
    return {
        "samples": len(rows),
        "failures": len(failures),
        **{
            field: latency(rows, field)
            for field in (
                "cycle_ns",
                "sql_ns",
                "demand_ns",
                "sql_decode_ns",
            )
        },
        "max_sql_bytes": max((row["sql_bytes"] for row in rows), default=0),
        "meets_target": (
            len(rows) >= CYCLES
            and not failures
            and cycle["p99_ns"] <= 1_000_000_000
            and cycle["max_ns"] <= 1_500_000_000
        ),
    }


def measure(database, projection, output, port, demand_port, image, *, concurrent):
    output.mkdir()
    storage = output / "volume"
    storage.mkdir(mode=0o777)
    storage.chmod(0o777)  # The pinned exporter runs as uid 65532.
    before = population(database)
    started = time.monotonic()
    rows = []
    cpu = {}
    with ExitStack() as stack:
        writers = (
            [
                stack.enter_context(writer(database, output, foreign=foreign))
                for foreign in (False, True)
            ]
            if concurrent
            else []
        )
        run(
            "docker",
            "run",
            "-d",
            "--name",
            EXPORTER,
            "--network",
            "host",
            "--cpus",
            "0.5",
            "--memory",
            "256m",
            "-v",
            f"{storage}:/evidence",
            "-e",
            f"J3_MONITOR_DSN=postgresql://j3_exporter:bench-fixture@127.0.0.1:{port}/{database}?sslmode=disable",
            "-e",
            "J3_NAMESPACE=target",
            "-e",
            "J3_DEMAND_TOKEN=bench-fixture",
            "-e",
            f"J3_DEMAND_URL=http://127.0.0.1:{demand_port}/v1/demand",
            image,
        )
        try:
            base_cpu = {name: cpu_usec(name) for name in (POSTGRES, EXPORTER)}
            time.sleep(PHASE_SECONDS)
            cpu = {name: cpu_usec(name) - baseline for name, baseline in base_cpu.items()}
        finally:
            # Stop first so the log ends on a definite boundary.
            try:
                run("docker", "stop", "--time", "2", EXPORTER)
                logs = run("docker", "logs", "--timestamps", EXPORTER)
                text = logs.stdout + logs.stderr
                (output / "exporter-container.log").write_text(text, encoding="utf-8")
                rows = [
                    json.loads(line.split(" J3_CYCLE ", 1)[1])
                    for line in text.splitlines()
                    if " J3_CYCLE " in line
                ]
                with (output / "cycle-timing.jsonl").open("w", encoding="utf-8") as stream:
                    stream.writelines(json.dumps(row) + "\n" for row in rows)
                save(output / "summary.json", {**summarize(rows), "container_cpu_usec": cpu})
            finally:
                run("docker", "rm", "-fv", EXPORTER)
                (storage / "visibility.jsonl").unlink(missing_ok=True)
                storage.rmdir()
        after = population(database)
        elapsed = time.monotonic() - started
        rates = {key: (after[key] - before[key]) / elapsed for key in before}
        result = {
            **summarize(rows),
            "container_cpu_usec": cpu,
            "population_before": before,
            "population_after": after,
            "elapsed_s": elapsed,
            "writes_per_second": rates,
        }
        result["load_valid"] = (
            before["target"] >= TARGET_COUNT
            and before["foreign"] == FOREIGN_COUNT
            and (
                all(rate >= ARRIVAL_RATE for rate in rates.values())
                if concurrent
                else after == before
            )
            and all(process.poll() is None for process in writers)
        )
        result["explain_complete"] = explain(database, projection, output)
        save(output / "summary.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("image")
    args = parser.parse_args()
    if os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
        raise RuntimeError("disposable GitHub-hosted runner required")
    lock = json.loads((ROOT / "deploy/k8s/tools.lock.json").read_bytes())
    pin = lock["postgres"]
    identity = run("docker", "image", "inspect", "--format", "{{.Id}}", args.image).stdout.strip()
    report = {
        "status": "INVALID",
        "postgres": pin,
        "image": args.image,
        "image_id": identity,
        "run": os.environ["GITHUB_RUN_ID"],
        "head": os.environ["GITHUB_SHA"],
        "attempt": os.environ["GITHUB_RUN_ATTEMPT"],
        "phases": {},
    }
    baseline = args.baseline.read_text(encoding="utf-8")
    candidate = (J3 / "exporter/monitor.sql").read_text(encoding="utf-8")
    try:
        run(
            "docker",
            "run",
            "-d",
            "--name",
            POSTGRES,
            "-e",
            "POSTGRES_PASSWORD=bench-fixture",
            "-p",
            "127.0.0.1::5432",
            pin,
            "-c",
            "max_connections=100",
            "-c",
            "fsync=on",
            "-c",
            "synchronous_commit=on",
            "-c",
            "full_page_writes=on",
            "-c",
            "track_commit_timestamp=on",
        )
        try:
            wait_postgres()
            port = int(run("docker", "port", POSTGRES, "5432/tcp").stdout.rsplit(":", 1)[1])
            with ThreadingHTTPServer(("127.0.0.1", 0), Demand) as demand:
                thread = Thread(target=demand.serve_forever, daemon=True)
                thread.start()
                try:
                    for name, source in (("baseline", baseline), ("candidate", candidate)):
                        create_fixture(name, source, args.output)
                        if name == "baseline":
                            # The older installer lacks the deployed observer role default.
                            sql(name, "ALTER ROLE j3_exporter SET jit = off;")
                        benchmark_role(name, args.output / f"{name}-role")
                        if name == "candidate":
                            report["jit_floor"] = measure_jit_floor(
                                name, source, args.output / "jit-floor"
                            )
                            save(args.output / "benchmark.json", report)
                        populate_fixture(name)
                        if name == "candidate":
                            # Equality at one statement timestamp and one MVCC snapshot.
                            original = baseline.split("CREATE FUNCTION ", 1)[1].split("$$;", 1)[0]
                            sql(
                                name,
                                "CREATE FUNCTION "
                                + original.replace("j3_sample()", "j3_sample_baseline()", 1)
                                + "$$;",
                            )
                            parity = sql(
                                name, SAMPLE + "::jsonb = j2_monitor.j3_sample_baseline()::jsonb;"
                            )
                            report["same_snapshot_projection_equal"] = parity == "t"
                            if parity != "t":
                                raise RuntimeError("baseline and candidate projections differ")
                        for phase in ("idle", "writes"):
                            key = f"{name}-{phase}"
                            report["phases"][key] = measure(
                                name,
                                source,
                                args.output / key,
                                port,
                                demand.server_port,
                                args.image,
                                concurrent=phase == "writes",
                            )
                            save(args.output / "benchmark.json", report)
                        sql(
                            "postgres",
                            f'DROP DATABASE "{name}"; DROP ROLE j3_exporter, j3_fixture;',
                        )
                finally:
                    demand.shutdown()
                    thread.join(timeout=5)
        finally:
            try:
                logs = run("docker", "logs", POSTGRES)
                (args.output / "postgres.log").write_text(
                    logs.stdout + logs.stderr, encoding="utf-8"
                )
            finally:
                run("docker", "rm", "-fv", POSTGRES)
        phases = report["phases"]
        valid = all(row["load_valid"] for row in phases.values())
        candidates = [phases[f"candidate-{phase}"] for phase in ("idle", "writes")]
        report["projection_status"] = "INVALID"
        if valid and not all(row["meets_target"] for row in candidates):
            report["status"] = report["projection_status"] = "TARGET_MISSED"
            report["finding"] = {
                "id": "R398",
                "reason": "The candidate missed the cycle gate on the original engine schema.",
                "failed_phases": [
                    phase
                    for phase in ("candidate-idle", "candidate-writes")
                    if not phases[phase]["meets_target"]
                ],
                "next_action": "STOP_BEFORE_DEPLOYMENT",
            }
        elif valid and all(row["explain_complete"] for row in candidates):
            report["projection_status"] = "PASS"
        try:
            save(
                args.output / "harness-inventory.json",
                measure_inventory(args.output, lock["tools"]["kubectl"]),
            )
        except (OSError, RuntimeError, subprocess.TimeoutExpired, ValueError) as error:
            report["harness_inventory_error"] = str(error)
        else:
            report["status"] = report["projection_status"]
    except Exception as error:
        report["error"] = str(error)
        raise
    finally:
        save(args.output / "benchmark.json", report)
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
