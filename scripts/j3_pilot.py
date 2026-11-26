"""Run the unmeasured hosted overlay and foreign dispatch proof."""

import argparse
import base64
import hashlib
import json
import os
import secrets
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime
from http.client import HTTPException
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

import dur053_k3 as kind
from j2_oci import restore
from j2_offline import attempt_key, utc_ns
from j2_overlay import J2, obj
from j3_diagnostics import enable_exporter_diagnostics
from j3_evidence import jsonl, notifications
from j3_overlay import (
    APP,
    EXPORTER,
    FOREIGN,
    GRAFANA,
    J3,
    NS,
    PINS,
    PROM,
    RECEIVER,
    TARGET,
    budget,
    render,
)
from j3_prometheus import evaluation_rows
from j3_replay import SECOND, replay, require
from j3_slots import sample_ready_slots

DB = "dur053_kind"
COUNT, RATE, RUN, SEED = 70, 7.136100570066256, "j3-pilot", 530001


class Pilot(kind.Driver):
    target_namespace = TARGET

    def topics(self, action, *, kafka=None):
        super().topics(action, kafka=kafka or kind.CLUSTER + "-kafka")

    def episode_objects(self, objects):
        for item in objects:
            if item["kind"] == "ScaledObject":
                item["metadata"]["annotations"]["autoscaling.keda.sh/paused-replicas"] = "2"
            if item["kind"] == "Deployment":
                for container in item["spec"]["template"]["spec"]["containers"]:
                    for var in container.get("env", []):
                        if var["name"] == "RUNTIME_NAMESPACE":
                            var.clear()
                            var.update(name="RUNTIME_NAMESPACE", value=self.target_namespace)
        kind.save(self.output / "application-rendered.json", objects)

    def command_output(self, stdout, stderr):
        clean = [
            part.replace(self.monitor_password, "[REDACTED]").replace(
                self.fixture_password, "[REDACTED]"
            )
            if isinstance(part, str)
            else part
            for part in (stdout, stderr)
        ]
        return super().command_output(*clean)

    def secret(self, namespace, name, **data):
        self.apply(obj("v1", "Secret", name, namespace, stringData=data))

    def postgres(self, query, *, role="durable", allowed=False):
        password = {
            "durable": self.password,
            "j3_exporter": self.monitor_password,
            "j3_fixture": self.fixture_password,
        }[role]
        psql = ["psql", "-h", "127.0.0.1", "-U", role, "-XqAt", "-d", DB]
        psql.extend(["-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=verbose"])
        docker = [
            "docker",
            "exec",
            "-i",
            "-e",
            f"PGPASSWORD={password}",
            kind.CLUSTER + "-postgres",
        ]
        return self.command(docker + psql, data=query, allowed=allowed, record=False)

    def query(self, sql):
        return self.postgres(sql).stdout.strip()

    def inspect(self, namespace, resource):
        return json.loads(
            self.k("-n", namespace, "get", resource, "-o", "json", record=False).stdout
        )

    def rollout(self, namespace, role):
        target = f"deployment/{role}"
        self.k("-n", namespace, "rollout", "status", target, "--timeout=180s", timeout=190)

    @contextmanager
    def forward(self, name, ports):
        with (self.output / f"{name}-forward.log").open("w") as log:
            command = ["kubectl", "--context", "kind-" + kind.CLUSTER, "-n", NS, "port-forward"]
            process = subprocess.Popen(
                command + [f"deployment/{name}", *ports], env=self.env, stdout=log, stderr=log
            )
            try:
                yield
            finally:
                process.terminate()
                process.wait(timeout=10)


def clock(driver):
    before = time.time_ns()
    value = int(driver.query("SELECT (extract(epoch FROM clock_timestamp())*1e9)::bigint;"))
    after = time.time_ns()
    require(before <= value <= after, "database clock escaped observer bracket")
    return {"before_ns": before, "db_ns": value, "after_ns": after}


def db_denials(driver):
    positive = driver.postgres(
        "SHOW transaction_read_only; SHOW statement_timeout; "
        "SELECT count(*) FROM j2_monitor.j3_sample();",
        role="j3_exporter",
    )
    require(
        positive.stdout.splitlines() == ["on", "2s", "1"],
        "projection or session restrictions failed",
    )
    checks = (
        ("j3_exporter", "SELECT * FROM j2_monitor.j3_plan;", "42501"),
        ("j3_exporter", "CREATE TABLE j2_monitor.j3_write_probe (x int);", "25006"),
        (
            "j3_exporter",
            "SET default_transaction_read_only=off; "
            "UPDATE engine.workflow_executions SET state=state WHERE false;",
            "42501",
        ),
        ("j3_exporter", "SELECT j2_monitor.j3_seed('foreign-1');", "42501"),
        ("j3_fixture", "INSERT INTO engine.outbox DEFAULT VALUES;", "42501"),
        ("j3_fixture", "UPDATE engine.workflow_executions SET state=state WHERE false;", "42501"),
        ("j3_fixture", "SELECT * FROM j2_monitor.j3_plan;", "42501"),
    )
    for role, sql, code in checks:
        denied = driver.postgres(sql, role=role, allowed=True)
        require(denied.returncode and code in denied.stderr, f"SQL denial differs: {role} {sql}")
    kind.save(
        driver.output / "db-denials.json",
        {"role_sqlstate": checks, "session": positive.stdout.splitlines()},
    )


def network_denials(driver):
    pod = driver.pod(NS, EXPORTER)["metadata"]["name"]
    worker = driver.images["worker"]
    driver.k(
        "-n",
        NS,
        "debug",
        pod,
        "--attach=false",
        "--profile=general",
        f"--image={worker}",
        "--container=probe",
        "--",
        "python",
        "-c",
        "import time; time.sleep(120)",
    )
    driver.until(
        lambda: any(
            "running" in s["state"]
            for s in driver.pod(NS, EXPORTER)["status"].get("ephemeralContainerStatuses", [])
        ),
        45,
    )
    api = driver.inspect(APP, "service/api")["spec"]["clusterIP"]
    code = f"""
import http.client,json
results=[]
token={driver.demand_token!r}
for auth,path in [('wrong','/v1/demand'),(token, '/v1/demand?namespace={FOREIGN}'),
                  (token, '/v1/workflows')]:
    c=http.client.HTTPConnection('demand.{APP}',8091,timeout=4)
    method='POST' if path.endswith('workflows') else 'GET'
    c.request(method,path,headers={{'Authorization':'Bearer '+auth}})
    r=c.getresponse()
    results.append(json.loads(r.read())['namespace'] if r.status==200 else r.status)
    c.close()
try:
    c=http.client.HTTPConnection({api!r},8080,timeout=3)
    c.request('POST','/v1/workflows','{{}}'); results.append(c.getresponse().status)
except TimeoutError:
    results.append('TCP timeout')
print(json.dumps(results))
"""
    result = driver.k(
        "-n", NS, "exec", pod, "-c", "probe", "--", "python", "-c", code, record=False
    )
    actual = json.loads(result.stdout)
    require(
        actual[:2] == [401, driver.target_namespace]
        and actual[2] in {404, 405}
        and actual[3] == "TCP timeout",
        f"unexpected private API probe: {actual}",
    )
    ip = driver.pod(NS, EXPORTER)["status"]["podIP"]
    denied = f"""
import socket
try:
    socket.create_connection(({ip!r},9187),3).close()
    print('connected')
except TimeoutError:
    print('TCP timeout')
"""
    for namespace in (NS, "j3-unrelated"):
        driver.k(
            "-n",
            namespace,
            "run",
            "scrape-deny",
            f"--image={worker}",
            "--restart=Never",
            "--command",
            "--",
            "python",
            "-c",
            "import time; time.sleep(30)",
        )
        driver.k(
            "-n", namespace, "wait", "pod/scrape-deny", "--for=condition=Ready", "--timeout=60s"
        )
        require(
            driver.k(
                "-n", namespace, "exec", "scrape-deny", "--", "python", "-c", denied
            ).stdout.strip()
            == "TCP timeout",
            f"unrelated scrape allowed: {namespace}",
        )
    kind.save(
        driver.output / "network-denials.json",
        {
            "demand_auth_scope_write_and_app_write": actual,
            "wrong_namespace_demand_read": {
                "requested": FOREIGN,
                "returned": actual[1],
                "expected": "Returns the server-bound target scope by registered design.",
            },
            "unrelated_scrape": {"same_namespace": "TCP timeout", "other_namespace": "TCP timeout"},
        },
    )


def request(port, path, **params):
    url = f"http://127.0.0.1:{port}{path}"
    if params:
        url += "?" + urlencode(params)
    with urlopen(url, timeout=8) as response:
        return response.read()


def json_request(port, path, **params):
    return json.loads(request(port, path, **params))


def prom(expr, *, at=None):
    params = {"query": expr}
    if at is not None:
        params["time"] = at
    return json_request(19090, "/api/v1/query", **params)["data"]["result"]


def endpoint_ready(port, path):
    try:
        return bool(request(port, path))
    except (OSError, HTTPException):
        return False


def image_evidence(driver, pins):
    roles = driver.inspect(NS, "pods")["items"]
    refs = {}
    for pod in roles:
        name = pod["metadata"]["labels"].get("app")
        if name not in pins:
            continue
        (status,) = pod["status"]["containerStatuses"]
        require(
            status["restartCount"] == 0 and status["imageID"].endswith(pins[name].split("@")[1]),
            f"running digest differs: {name}: {status['imageID']}",
        )
        refs[name] = status["imageID"]
    require(refs.keys() == pins.keys(), "missing telemetry images")
    # setup_episode has already verified and retained the application IDs.
    kind.save(driver.output / "running-images.json", refs)


def database_rows(driver):
    sql = """
SELECT jsonb_build_object(
 'workflows', (SELECT jsonb_agg(to_jsonb(w)) FROM engine.workflow_executions w),
 'attempts', (SELECT jsonb_agg(to_jsonb(a) - 'claim_token' ||
               jsonb_build_object('token_hash',
                   encode(sha256(convert_to(claim_token::text,'UTF8')), 'hex')))
              FROM engine.activity_attempts a),
 'history', (SELECT jsonb_agg(to_jsonb(h)) FROM engine.transition_history h),
 'outbox', (SELECT jsonb_agg(to_jsonb(o)) FROM engine.outbox o));
"""
    return {key: rows or [] for key, rows in json.loads(driver.query(sql)).items()}


def worker_pods(driver):
    return [
        p
        for p in driver.inspect(APP, "pods")["items"]
        if p["metadata"]["labels"].get("app") == "worker"
    ]


def install_sender(driver, binary):
    raw = binary.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    expected = (driver.output / "sender.sha256").read_text().split()[0]
    require(digest == expected, "sender hash differs")
    code = (
        "import base64,hashlib,pathlib,sys; "
        "body=base64.b64decode(sys.stdin.buffer.read(),validate=True); "
        "assert hashlib.sha256(body).hexdigest()==sys.argv[2]; "
        "path=pathlib.Path(sys.argv[1]); path.write_bytes(body); path.chmod(0o500)"
    )
    pod, path = driver.pod(APP, "observer")["metadata"]["name"], "/evidence/j3-loadgen"
    command = ["-n", APP, "exec", "-i", pod, "--", "python", "-c", code, path, digest]
    driver.k(*command, data=base64.b64encode(raw).decode(), record=False)
    return pod, path


def pilot_dispatch_proof(driver, origin, db_ip, observer, sender):
    """Unmeasured proof that takes a real reconciler lease in the serving DB."""
    pods = worker_pods(driver)
    uids = {p["metadata"]["uid"] for p in pods}
    kind.stable_worker_sessions(uids, pods)
    slots = sample_ready_slots(driver, APP, NS, RECEIVER, pods, observer_pod=observer)["slots"]
    require(len(slots) == 4, "expected four witnessed Ready slots before the pilot")
    start = datetime.fromtimestamp(origin // SECOND, UTC).isoformat()
    seed = driver.postgres("SELECT j2_monitor.j3_seed('foreign-1');", role="j3_fixture")
    kind.save(driver.output / "foreign-seed.json", json.loads(seed.stdout))
    driver.postgres("ALTER SYSTEM SET log_statement = 'all'; SELECT pg_reload_conf();")
    options = {
        "k4-start": start,
        "k4-stop": "/evidence/j3-stop",
        "url": "http://api:8080",
        "token-file": "/identity/token",
        "namespace": TARGET,
        "run-id": RUN,
        "seed": SEED,
        "arm": "short",
        "rate": RATE,
        "count": COUNT,
        "concurrency": 16,
        "output": "/evidence/j3-arrivals.jsonl",
    }
    args = ["-n", APP, "exec", observer, "--", sender, "-k4"]
    args.extend(f"-{key}={value}" for key, value in options.items())
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            load = executor.submit(driver.k, *args, timeout=95)
            where = "WHERE namespace=" + repr(TARGET)
            any_target = "SELECT EXISTS (SELECT FROM engine.workflow_executions " + where + ");"

            def target_started():
                if load.done():
                    load.result()
                return driver.query(any_target) == "t"

            driver.until(target_started, 30)
            dsn = f"postgresql://durable:{driver.password}@{db_ip}:5432/{DB}?sslmode=disable"
            probe = ["go", "test", "-p", "1", "-v", "-count=1"]
            probe.append("./experiments/portfolio/observability/probe")
            driver.command(probe, timeout=45, env={**driver.env, "J3_PROBE_DSN": dsn})
            load.result()
    finally:
        result = driver.k(
            "-n",
            APP,
            "exec",
            observer,
            "--",
            "cat",
            "/evidence/j3-arrivals.jsonl",
            allowed=True,
            record=False,
        )
        journal = result.stdout
        (driver.output / "target-journal.jsonl").write_text(journal, encoding="utf-8")
    entries = [json.loads(line) for line in journal.splitlines()]
    accepted = {row["id"] for row in entries if row.get("outcome") == "accepted"}
    require(len(accepted) == COUNT, "target loadgen acceptance missing")
    succeeded = "SELECT count(*) FROM engine.workflow_executions WHERE namespace=" + repr(TARGET)
    succeeded += " AND state='SUCCEEDED';"
    driver.until(lambda: int(driver.query(succeeded)) == COUNT, 55)
    drained = "SELECT NOT EXISTS (SELECT FROM engine.outbox WHERE published_at IS NULL);"
    driver.until(lambda: driver.query(drained) == "t", 30)
    driver.postgres("ALTER SYSTEM RESET log_statement; SELECT pg_reload_conf();")
    db_log = driver.command(["docker", "logs", kind.CLUSTER + "-postgres"], record=False)
    statements = [
        line
        for line in (db_log.stdout + db_log.stderr).splitlines()
        if "WHERE namespace = $1 AND workflow_id > $2" in line or f"$1 = '{TARGET}'" in line
    ]
    (driver.output / "scheduler-scan.log").write_text(
        "\n".join(statements) + "\n", encoding="utf-8"
    )
    schedulers = [
        p
        for p in driver.inspect(APP, "pods")["items"]
        if p["metadata"]["labels"].get("app") == "scheduler"
    ]
    scopes = {
        p["metadata"]["name"]: next(
            v["value"]
            for v in p["spec"]["containers"][0]["env"]
            if v["name"] == "RUNTIME_NAMESPACE"
        )
        for p in schedulers
    }
    require(
        scopes
        and set(scopes.values()) == {TARGET}
        and any("workflow_id > $2" in line for line in statements),
        "missing scoped repair scan",
    )
    kind.save(driver.output / "scheduler-scopes.json", scopes)
    data = database_rows(driver)
    kind.save(driver.output / "durable-rows.json", data)
    foreign = "j3-fixture-foreign-1"
    (wf,) = [row for row in data["workflows"] if row["namespace"] == FOREIGN]
    (a,) = [row for row in data["attempts"] if row["workflow_id"] == foreign]
    (h,) = [row for row in data["history"] if row["workflow_id"] == foreign]
    require(
        wf["workflow_id"] == foreign
        and wf["revision"] == h["revision"] == 1
        and h["reason"] == "J3_FOREIGN_SEED"
        and a["state"] == "DISPATCHABLE"
        and all(a[k] is None for k in ("token_hash", "heartbeat_deadline", "worker_id"))
        and all(row["workflow_id"] != foreign for row in data["outbox"]),
        "foreign durable state changed",
    )
    require(
        {
            r["workflow_id"]
            for r in data["outbox"]
            if r["event_type"] == "attempt.dispatch" and r["published_at"]
        }
        == accepted,
        "relay did not publish the target tasks",
    )
    claims = {
        (attempt_key(r), r["token_hash"], r["worker_id"], r["worker_request_id"].split(":", 1)[0])
        for r in data["attempts"]
        if r["token_hash"]
    }
    kind.stable_worker_sessions(uids, worker_pods(driver))
    inventories = sample_ready_slots(driver, APP, NS, RECEIVER, pods, observer_pod=observer)[
        "witness"
    ]
    witnessed, ledgers = [], []
    for pod in pods:
        name = pod["metadata"]["name"]
        inv = inventories[pod["metadata"]["uid"]]
        code = """import json,pathlib,sys
root=pathlib.Path('/witness') / sys.argv[1]
assert [p.name for p in root.glob('*.jsonl')]==[sys.argv[2]]
print(json.dumps((root / sys.argv[2]).read_text()))
"""
        result = driver.k(
            "-n", APP, "exec", name, "--", "python", "-c", code, name, inv["file"], record=False
        )
        raw = json.loads(result.stdout)
        rows = [json.loads(line) for line in raw.splitlines()]
        require(
            inv["complete"]
            and inv["written"] == len(rows)
            and inv["pod_uid"] == pod["metadata"]["uid"]
            and inv["pod_name"] == name
            and inv["sha256"] == hashlib.sha256(raw.encode()).hexdigest(),
            "incomplete worker witness",
        )
        for row in rows:
            if "claim_token" in row:
                row["token_hash"] = hashlib.sha256(row.pop("claim_token").encode()).hexdigest()
            if row["event"] == "start":
                actor = f"system:serviceaccount:{APP}:worker:pod:{name}:uid:{inv['pod_uid']}"
                witnessed.append((attempt_key(row), row["token_hash"], actor, row["worker_id"]))
        ledgers.append({"inventory": inv, "events": rows})
    kind.save(driver.output / "slot-ledger.json", {"ready_slots": slots, "witness": ledgers})
    require(
        set(witnessed) == claims
        and len(witnessed) == len(claims)
        and {row[0][0] for row in claims} == accepted,
        "worker slot ledger differs from durable target claims",
    )
    require(
        all(slot in slots for _, _, _, slot in claims),
        "claim outside configured slots",
    )
    return accepted


def dashboard_evidence(driver, target, accepted):
    board = json_request(13000, "/api/dashboards/uid/j3-observability")["dashboard"]
    datasource = json_request(13000, "/api/datasources/uid/j3-prometheus")
    require(
        datasource["access"] == "proxy" and datasource["url"] == f"http://{PROM}:9090",
        "Grafana datasource differs",
    )
    require(
        board["uid"] == "j3-observability" and len(board["panels"]) == 8,
        "Grafana dashboard differs",
    )
    kind.save(driver.output / "dashboard-api.json", {"dashboard": board, "datasource": datasource})
    end = int(time.time())
    proxy = "/api/datasources/proxy/uid/j3-prometheus/api/v1/query_range"

    def series(expr):
        return json_request(13000, proxy, query=expr, start=end - 45, end=end, step=1)["data"][
            "result"
        ]

    results = {}
    for panel in board["panels"]:
        for query in panel["targets"]:
            expr = query["expr"]
            for key, value in target.items():
                expr = expr.replace(f"${key}", value)
            results[f"{panel['id']}{query['refId']}"] = series(expr)
    kind.save(driver.output / "dashboard-ranges.json", results)
    required_panels = set(results) - {"4A", "4B"}  # No ownership fault in this pilot.
    require(all(results[key] for key in required_panels), "missing dashboard panel samples")
    require(
        all(
            labels["metric"].get("namespace") == target["namespace"]
            for series in results.values()
            for labels in series
        ),
        "unexpected dashboard series scope",
    )
    raw = request(19188, "/visibility.jsonl")
    visibility = [json.loads(line) for line in raw.splitlines()]
    (driver.output / "visibility.jsonl").write_bytes(raw)
    by_source = {sample["sample_ns"] / SECOND: sample for sample in visibility}
    (source,) = series("j3_sample_timestamp_seconds")
    by_tick = {tick: by_source[float(value)] for tick, value in source["values"]}
    for panel, field in (("5A", "eligible"), ("5B", "bad")):
        for result in results[panel]:
            window = result["metric"]["window"]
            require(
                all(
                    int(value) == by_tick[tick]["windows"][window][field]
                    for tick, value in result["values"]
                ),
                "dashboard differs from snapshot",
            )
    kind.save(driver.output / "dashboard-source-times.json", source)
    observed = {key for sample in visibility for key in sample["admissions"]["set"]}
    require(
        observed == accepted
        and any(sample["windows"]["all"]["eligible"] == len(accepted) for sample in visibility),
        "missing or foreign exporter admission identities",
    )
    require(not prom("ALERTS"), "pilot ended with active alerts")


def collect_replay_inputs(driver, target, origin, end):
    output = driver.output
    span = (end - origin) // SECOND + 12
    labels = ",".join(f"{key}={json.dumps(value)}" for key, value in target.items())
    observed_at = f"{end // SECOND}.{end % SECOND:09d}"
    matrices = []
    for qualifier in (
        'job="j3"',
        '__name__="j3_ready_slots"',
        '__name__=~"j3:.*"',
        '__name__="ALERTS",alertstate="firing"',
    ):
        response = json_request(
            19090, "/api/v1/query", query=f"{{{labels},{qualifier}}}[{span}s]", time=observed_at
        )
        matrices.extend(response["data"]["result"])
    (group,) = json_request(19090, "/api/v1/rules")["data"]["groups"]
    kind.save(output / "prometheus-raw.json", {"matrices": matrices, "rule_group": group})
    path = output / "visibility.jsonl"
    path.write_bytes(request(19188, "/visibility.jsonl"))
    return evaluation_rows(matrices, jsonl(path), target, origin, end, group["rules"])


def pilot_replay(driver, target, keys, origin, setup):
    output = driver.output
    end = time.time_ns()
    closing = clock(driver)
    evaluations = collect_replay_inputs(driver, target, origin, end)
    path = output / "visibility.jsonl"
    raw = json.loads((output / "durable-rows.json").read_bytes())
    data = {
        "namespace": TARGET,
        "start_ns": origin,
        "end_ns": end,
        "clock": {"setup": setup, "closing": closing},
        "plan": [
            {
                "namespace": TARGET,
                "submission_key": key,
                "scheduled_ns": origin + int(i * SECOND / RATE),
            }
            for i, key in enumerate(keys)
        ],
        "journal": [
            {
                "namespace": TARGET,
                "workflow_id": row["workflow"],
                "submission_key": row["key"],
                "outcome": row["outcome"],
            }
            for row in jsonl(output / "target-journal.jsonl")
            if row.get("outcome") == "accepted"
        ],
        "workflows": raw["workflows"],
        "history": [{**row, "created_ns": utc_ns(row["created_at"])} for row in raw["history"]],
        "first_eval_ns": evaluations[0]["at_ns"],
        "last_eval_ns": evaluations[-1]["at_ns"],
        "evaluations": evaluations,
    }
    kind.save(output / "replay-input.json", data)
    result = replay(data, jsonl(path))
    kind.save(output / "replay.json", result)
    require(all(row["valid"] for row in evaluations), "missing fresh pilot evaluation inputs")
    require(
        result["published_attainment"]["eligible"] == COUNT
        and result["published_attainment"]["incomplete"] == 0,
        "pilot published replay failed target reconciliation",
    )
    return data, result


def receiver_log(driver):
    log = driver.python(
        NS,
        RECEIVER,
        "import time; from pathlib import Path; "
        "p=Path('/evidence/notifications.jsonl'); print(p.read_text(),end=''); "
        'print(\'{"heartbeat":"end","received_ns":\'+str(time.time_ns())+\'}\')',
    )
    path = driver.output / "notifications.jsonl"
    path.write_text(log + "\n", encoding="utf-8")
    return path


def retain_monitor_logs(driver, *, required):
    status = {}
    for role in (EXPORTER, PROM):
        for previous in (False, True):
            name = f"{role}{'-previous' if previous else ''}"
            try:
                result = driver.k(
                    "-n",
                    NS,
                    "logs",
                    f"deployment/{role}",
                    "--timestamps=true",
                    *(["--previous"] if previous else []),
                    allowed=True,
                    record=False,
                    timeout=15,
                )
            except (OSError, RuntimeError, ValueError) as error:
                status[name] = {"error": str(error)}
                continue
            status[name] = {"exit": result.returncode, "stderr": result.stderr}
            if not previous or result.returncode == 0:
                (driver.output / f"{name}-container.log").write_text(
                    result.stdout,
                    encoding="utf-8",
                )
            if role == EXPORTER and not previous:
                status[name]["cycle_timings"] = result.stdout.count(" J3_CYCLE ")
    kind.save(driver.output / "monitor-logs.json", status)
    if required:
        require(
            all(status[role].get("exit") == 0 for role in (EXPORTER, PROM))
            and status[EXPORTER]["cycle_timings"] > 0,
            "missing monitor logs or exporter cycle diagnostics",
        )


def prepare_overlay(driver, db_ip, pins, target, *, diagnostics=False):
    require(
        driver.query("SELECT count(*) FROM engine.workflow_executions;") == "0",
        "episode DB is not empty",
    )
    setup = clock(driver)
    width = setup["after_ns"] - setup["before_ns"]
    sql = (
        (J3 / "exporter/monitor.sql").read_text().replace(":'namespace'", repr(target["namespace"]))
    )
    driver.postgres(
        sql.replace(":'password'", repr(driver.monitor_password)).replace(
            ":'u_ns'", repr(str(width))
        )
    )
    if diagnostics:
        enable_exporter_diagnostics(driver)
    sql = (J3 / "foreign.sql").read_text().replace(":'foreign'", repr(FOREIGN))
    driver.postgres(sql.replace(":'password'", repr(driver.fixture_password)))
    db_denials(driver)
    driver.secret(
        NS,
        "j3-monitor",
        J3_MONITOR_DSN=f"postgresql://j3_exporter:{driver.monitor_password}@{db_ip}:5432/{DB}?sslmode=disable",
        J3_DEMAND_TOKEN=driver.demand_token,
    )
    overlay = render(db_ip, target, pins)
    kind.save(driver.output / "overlay.json", overlay)
    kind.save(driver.output / "db-budget.json", budget())
    # Pick the monotonic sender's absolute UTC anchor after the overlay has rolled out.
    driver.apply(overlay)
    for name in pins:
        driver.rollout(NS, name)
    image_evidence(driver, pins)
    network_denials(driver)
    return setup


def run_overlay(driver, db_ip, pins, target, binary):
    setup = prepare_overlay(driver, db_ip, pins, target)
    observer, sender = install_sender(driver, binary)
    with (
        driver.forward(PROM, ["19090:9090"]),
        driver.forward(GRAFANA, ["13000:3000"]),
        driver.forward(EXPORTER, ["19188:9188"]),
    ):
        driver.until(lambda: endpoint_ready(19090, "/-/ready"), 30)
        driver.until(lambda: bool(prom("j3:valid")), 30)
        driver.until(lambda: endpoint_ready(13000, "/api/dashboards/uid/j3-observability"), 45)
        origin = (time.time_ns() // SECOND + 12) * SECOND
        keys = [f"dur053-{RUN}-{SEED}-{i:06d}" for i in range(COUNT)]
        values = [
            f"('{TARGET}','{key}',{origin + int(i * SECOND / RATE)})" for i, key in enumerate(keys)
        ]
        values.append(f"('{FOREIGN}','foreign-1',{origin - 30 * SECOND})")
        driver.postgres("INSERT INTO j2_monitor.j3_plan VALUES " + ",".join(values) + ";")
        kind.save(
            driver.output / "plan.json",
            {"origin_ns": origin, "target_keys": keys, "foreign_key": "foreign-1"},
        )
        accepted = pilot_dispatch_proof(driver, origin, db_ip, observer, sender)
        driver.until(lambda: bool(prom(f'j3_eligible{{window="all"}} == {COUNT}')), 30)
        sample_ready_slots(driver, APP, NS, RECEIVER, worker_pods(driver), observer_pod=observer)
        driver.until(lambda: bool(prom("j3_ready_slots == 4")), 5)
        try:
            dashboard_evidence(driver, target, accepted)
            data, result = pilot_replay(driver, target, keys, origin, setup)
        except Exception:
            try:
                receiver_log(driver)
            except Exception as error:
                kind.save(driver.output / "receiver-export-error.json", {"error": str(error)})
            raise
        path = receiver_log(driver)
    sessions = driver.query(
        "SELECT application_name || '|' || count(*) FROM pg_stat_activity "
        "WHERE usename='j3_exporter' GROUP BY application_name;"
    )
    maximum = int(driver.query("SHOW max_connections;"))
    kind.save(driver.output / "db-sessions.json", {"monitor": sessions, "max_connections": maximum})
    require(
        sessions == "j3-db-exporter|1" and maximum == 100,
        "live monitor budget differs",
    )
    kind.save(driver.output / "clock.json", data["clock"])
    delivery_evidence = notifications(
        list(jsonl(path)), result["alert_inputs"]["transitions"], target, origin, data["end_ns"]
    )
    kind.save(
        driver.output / "setup-incidents.json",
        {"setup_incidents": delivery_evidence["setup_incidents"]},
    )
    require(not delivery_evidence["matches"], "unexpected receiver notifications during pilot")


def hosted_arguments(description):
    parser = argparse.ArgumentParser(description=description)
    for name in ("evidence", "scratch", "runtime_layout", "exporter_layout", "sender"):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    require(
        os.environ.get("GITHUB_ACTIONS") == "true"
        and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted",
        "disposable GitHub-hosted runner required",
    )
    args.evidence.mkdir(exist_ok=True)
    args.scratch.mkdir()
    return args


def locked_images(driver):
    driver.monitor_password, driver.fixture_password = (secrets.token_hex(32) for _ in range(2))
    runtime = json.loads((J2 / "images.lock.json").read_bytes())
    exporter = json.loads((J3 / "exporter.lock.json").read_bytes())["image"]
    driver.images = {name: runtime[name] for name in ("runtime", "worker")}
    pins = {**PINS, EXPORTER: exporter, RECEIVER: driver.images["worker"]}
    return runtime, pins


def bootstrap(driver, args, pins):
    driver.install()
    for addon in ("calico", "keda"):
        driver.prepare_addon(addon)
    registry = kind.CLUSTER + "-registry"
    driver.created.add(registry)
    command = ["docker", "run", "-d", "--name", registry, "--label", kind.LABEL]
    driver.command(command + ["-p", "127.0.0.1:5001:5000", kind.LOCK["registry"]])
    driver.until(lambda: endpoint_ready(5001, "/v2/"), 30)
    for name, ref in driver.images.items():
        restore(args.runtime_layout / name, ref)
    restore(args.exporter_layout / "exporter", pins[EXPORTER])
    addresses = driver.cluster()
    node = ["docker", "exec", kind.CLUSTER + "-control-plane"]
    pull = ["ctr", "--namespace", "k8s.io", "images", "pull", "--hosts-dir"]
    driver.command(node + pull + ["/etc/containerd/certs.d", pins[EXPORTER]])
    return addresses


def main():
    args = hosted_arguments(__doc__)
    kind.CLUSTER, kind.LABEL = "j3-overlay", "j3.overlay=owned"
    driver = Pilot(args.evidence, args.scratch)
    runtime, pins = locked_images(driver)
    target = {"campaign": f"j3-pilot-{os.environ['GITHUB_RUN_ID']}", "namespace": TARGET}
    kind.save(
        args.evidence / "run.json",
        {
            "scope": "unmeasured overlay pilot",
            "target": target,
            "kubernetes_namespace": APP,
            "run": os.environ["GITHUB_RUN_ID"],
            "head": os.environ["GITHUB_SHA"],
            "attempt": os.environ["GITHUB_RUN_ATTEMPT"],
            "runtime_profile": runtime["profile"],
            "pins": pins,
        },
    )
    try:
        addresses = bootstrap(driver, args, pins)
        driver.setup_episode("kind", addresses)
        for namespace in (NS, "j3-unrelated"):
            driver.k("create", "namespace", namespace)
        run_overlay(driver, addresses["192.0.2.1"], pins, target, args.sender)
        retain_monitor_logs(driver, required=True)
        kind.save(
            args.evidence / "verdict.json", {"status": "PASS", "scope": "unmeasured overlay pilot"}
        )
    except Exception as error:
        kind.save(args.evidence / "verdict.json", {"status": "FAIL", "error": str(error)})
        if driver.cluster_started:
            retain_monitor_logs(driver, required=False)
            for ns in (NS, APP):
                driver.k("-n", ns, "get", "pods,events", "-o", "json", allowed=True)
                for name in pins if ns == NS else ("scheduler", "relay", "worker", "demand"):
                    driver.k("-n", ns, "logs", f"deployment/{name}", "--tail=100", allowed=True)
        raise
    finally:
        kind.save(args.evidence / "cleanup.json", driver.cleanup())
        kind.save(args.evidence / "sha256.json", kind.artifact_hashes(args.evidence))


if __name__ == "__main__":
    main()
