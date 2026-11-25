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
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import dur053_k3 as kind
from j2_oci import restore
from j2_overlay import J2, obj
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
from j3_replay import SECOND, require

DB = "dur053_kind"
COUNT, RATE, RUN, SEED = 70, 7.136100570066256, "j3-pilot", 530001


class Pilot(kind.Driver):
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
                            var.update(name="RUNTIME_NAMESPACE", value=TARGET)
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
        actual[:2] == [401, TARGET] and actual[2] in {404, 405} and actual[3] == "TCP timeout",
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


def prom(expr):
    return json_request(19090, "/api/v1/query", query=expr)["data"]["result"]


def endpoint_ready(port, path):
    try:
        return bool(request(port, path))
    except URLError:
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


def dispatch_proof(driver, origin, db_ip, observer, sender):
    pods = worker_pods(driver)
    uids = {p["metadata"]["uid"] for p in pods}
    kind.stable_worker_sessions(uids, pods)
    slots = []
    for p in pods:
        require(
            next(c for c in p["status"]["conditions"] if c["type"] == "Ready")["status"] == "True",
            "worker not Ready",
        )
        slot_count = next(
            v["value"] for v in p["spec"]["containers"][0]["env"] if v["name"] == "WORKER_SLOTS"
        )
        require(slot_count == "2", "worker slot count changed")
        slots.extend(f"{p['metadata']['name']}-{i}" for i in range(int(slot_count)))
    driver.python(
        NS,
        RECEIVER,
        "import os,pathlib; p=pathlib.Path('/evidence/metrics.tmp'); "
        "p.write_text('{\"ready_slots\":4}'); os.replace(p,'/evidence/metrics.json')",
    )
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
        for line in db_log.stderr.splitlines()
        if "WHERE namespace = $1 AND workflow_id > $2" in line
        or "parameters: $1 = 'j3-target'" in line
    ]
    (driver.output / "scheduler-scan.log").write_text(
        "\n".join(statements) + "\n", encoding="utf-8"
    )
    require(
        any("workflow_id > $2" in line for line in statements)
        and any("'j3-target'" in line for line in statements),
        "no target scheduler repair scan evidence",
    )
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
        (r["workflow_id"], r["worker_id"], r["token_hash"])
        for r in data["attempts"]
        if r["token_hash"]
    }
    kind.stable_worker_sessions(uids, worker_pods(driver))
    witnessed, ledgers = [], []
    for pod in pods:
        name = pod["metadata"]["name"]
        code = """import json,pathlib,urllib.request
i=json.load(urllib.request.urlopen('http://127.0.0.1:8081/witness',timeout=3))
root=pathlib.Path('/witness') / i['pod_name']
assert [p.name for p in root.glob('*.jsonl')]==[i['file']]
print(json.dumps({'inventory':i,'data':(root / i['file']).read_text()}))
"""
        result = driver.k("-n", APP, "exec", name, "--", "python", "-c", code, record=False)
        exported = json.loads(result.stdout)
        inv, raw = exported["inventory"], exported["data"]
        rows = [json.loads(line) for line in raw.splitlines()]
        require(
            inv["complete"]
            and inv["written"] == len(rows)
            and inv["pod_uid"] in uids
            and inv["sha256"] == hashlib.sha256(raw.encode()).hexdigest(),
            "incomplete worker witness",
        )
        for row in rows:
            if "claim_token" in row:
                row["token_hash"] = hashlib.sha256(row.pop("claim_token").encode()).hexdigest()
            if row["event"] == "start":
                witnessed.append((row["workflow_id"], row["worker_id"], row["token_hash"]))
        ledgers.append({"inventory": inv, "events": rows})
    require(
        set(witnessed) == claims
        and len(witnessed) == len(claims)
        and {row[0] for row in claims} == accepted,
        "worker slot ledger differs from durable target claims",
    )
    require(
        all(any(row[1].endswith(slot) for slot in slots) for row in claims),
        "claim outside configured slots",
    )
    kind.save(driver.output / "slot-ledger.json", {"ready_slots": slots, "witness": ledgers})
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
        and any(sample["windows"]["all"]["eligible"] == COUNT for sample in visibility),
        "missing or foreign exporter admission identities",
    )
    require(not prom("ALERTS"), "pilot ended with active alerts")


def run_overlay(driver, db_ip, pins, target, binary):
    require(
        driver.query("SELECT count(*) FROM engine.workflow_executions;") == "0",
        "pilot DB is not empty",
    )
    setup = clock(driver)
    width = setup["after_ns"] - setup["before_ns"]
    sql = (J3 / "exporter/monitor.sql").read_text().replace(":'namespace'", repr(TARGET))
    driver.postgres(
        sql.replace(":'password'", repr(driver.monitor_password)).replace(
            ":'u_ns'", repr(str(width))
        )
    )
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
        accepted = dispatch_proof(driver, origin, db_ip, observer, sender)
        driver.until(lambda: bool(prom(f'j3_eligible{{window="all"}} == {COUNT}')), 30)
        dashboard_evidence(driver, target, accepted)
    sessions = driver.query(
        "SELECT application_name || '|' || count(*) FROM pg_stat_activity "
        "WHERE usename='j3_exporter' GROUP BY 1;"
    )
    require(
        sessions == "j3-db-exporter|1" and driver.query("SHOW max_connections;") == "100",
        "live monitor budget differs",
    )
    kind.save(driver.output / "clock.json", {"setup": setup, "closing": clock(driver)})
    log = driver.python(
        NS,
        RECEIVER,
        "import time; from pathlib import Path; "
        "p=Path('/evidence/notifications.jsonl'); print(p.read_text(),end=''); "
        'print(\'{"heartbeat":"end","received_ns":\'+str(time.time_ns())+\'}\')',
    )
    (driver.output / "notifications.jsonl").write_text(log + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
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
    kind.CLUSTER, kind.LABEL = "j3-overlay", "j3.overlay=owned"
    driver = Pilot(args.evidence, args.scratch)
    driver.monitor_password, driver.fixture_password = (secrets.token_hex(32) for _ in range(2))
    runtime = json.loads((J2 / "images.lock.json").read_bytes())
    exporter = json.loads((J3 / "exporter.lock.json").read_bytes())["image"]
    driver.images = {name: runtime[name] for name in ("runtime", "worker")}
    pins = {**PINS, EXPORTER: exporter, RECEIVER: driver.images["worker"]}
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
        restore(args.exporter_layout / "exporter", exporter)
        addresses = driver.cluster()
        node = ["docker", "exec", kind.CLUSTER + "-control-plane"]
        pull = ["ctr", "--namespace", "k8s.io", "images", "pull", "--hosts-dir"]
        driver.command(node + pull + ["/etc/containerd/certs.d", exporter])
        driver.setup_episode("kind", addresses)
        for namespace in (NS, "j3-unrelated"):
            driver.k("create", "namespace", namespace)
        run_overlay(driver, addresses["192.0.2.1"], pins, target, args.sender)
        kind.save(
            args.evidence / "verdict.json", {"status": "PASS", "scope": "unmeasured overlay pilot"}
        )
    except Exception as error:
        kind.save(args.evidence / "verdict.json", {"status": "FAIL", "error": str(error)})
        if driver.cluster_started:
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
