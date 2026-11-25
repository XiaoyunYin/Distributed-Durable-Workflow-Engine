"""Hosted J2 telemetry pilot on an empty application namespace."""

from __future__ import annotations

import argparse
import json
import os
import secrets
import subprocess
import time
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import dur053_k3 as kind_driver
from dur053_k3 import Driver, save
from j2_oci import restore, retain
from j2_overlay import EXPORTER, J2, PROM, PROMETHEUS, budget, registered_budget, render

NS = "j2-overlay-pilot"
DB = "j2_overlay_pilot"
OTHER = "j2-unrelated"


class J2Driver(Driver):
    def topics(self, action, *, kafka=None):
        super().topics(action, kafka=kafka or kind_driver.CLUSTER + "-kafka")

    def command_output(self, stdout, stderr):
        # PostgreSQL may include SQL input in a failed command's diagnostics.
        def redact(value):
            return (
                value.replace(self.monitor_password, "[REDACTED]")
                if isinstance(value, str)
                else value
            )

        return super().command_output(redact(stdout), redact(stderr))


def require(condition, message):
    if not condition:
        raise ValueError(message)


def apply_app(driver, endpoints):
    _, objects = driver.render("kind", endpoints)
    objects = json.loads(json.dumps(objects).replace("dur053-kind", NS))
    for o in objects:
        if o["kind"] == "ScaledObject":
            o["metadata"].setdefault("annotations", {}).update(
                {
                    "autoscaling.keda.sh/paused-replicas": "2",
                    "autoscaling.keda.sh/paused-scale-in": "true",
                    "autoscaling.keda.sh/paused-scale-out": "true",
                }
            )
    save(driver.output / "application-rendered.json", objects)
    driver.databases.add(DB)
    driver.sql(f'CREATE DATABASE "{DB}"')
    driver.k("create", "namespace", NS)
    driver.k("create", "namespace", OTHER)
    for name, values in (
        (
            "campaign-db",
            {"url": f"postgresql://durable:{driver.password}@postgres:5432/{DB}?sslmode=disable"},
        ),
        ("campaign-demand", {"token": driver.demand_token}),
    ):
        secret(driver, name, values)
    driver.apply(
        {
            "apiVersion": "v1",
            "kind": "List",
            "items": [
                o
                for o in objects
                if o["kind"]
                not in {
                    "Job",
                    "Deployment",
                    "ScaledObject",
                    "PodDisruptionBudget",
                }
            ],
        }
    )
    for name, ip, port in (
        ("postgres", endpoints["192.0.2.1"], 5432),
        ("kafka", endpoints["192.0.2.2"], 19092),
    ):
        driver.apply(
            {
                "apiVersion": "v1",
                "kind": "Endpoints",
                "metadata": {"name": name, "namespace": NS},
                "subsets": [{"addresses": [{"ip": ip}], "ports": [{"name": "tcp", "port": port}]}],
            }
        )
    for name in ("migrate", "compatibility"):
        driver.apply(
            next(o for o in objects if o["kind"] == "Job" and o["metadata"]["name"] == name)
        )
        driver.k(
            "-n",
            NS,
            "wait",
            "--for=condition=complete",
            f"job/{name}",
            "--timeout=180s",
            timeout=190,
        )
    driver.apply({"apiVersion": "v1", "kind": "List", "items": objects})
    for o in objects:
        if o["kind"] == "Deployment":
            rollout(driver, o["metadata"]["name"])


def secret(driver, name, fields):
    driver.apply(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": name, "namespace": NS},
            "stringData": fields,
        }
    )


def rollout(driver, name):
    driver.k("-n", NS, "rollout", "status", f"deployment/{name}", "--timeout=180s", timeout=190)


def postgres_probe(driver, password, sql, superuser=False):
    options = ["-U", "durable"] if superuser else ["-h", "127.0.0.1", "-U", "j2_exporter"]
    return driver.command(
        [
            "docker",
            "exec",
            "-i",
            "-e",
            f"PGPASSWORD={password}",
            kind_driver.CLUSTER + "-postgres",
            "psql",
            "-XAt",
            "-v",
            "ON_ERROR_STOP=1",
            "-v",
            "VERBOSITY=verbose",
            "-d",
            DB,
            *options,
        ],
        data=sql,
        allowed=True,
        record=False,
    )


def check_db_denial(driver, password):
    readonly = postgres_probe(
        driver,
        password,
        "SHOW transaction_read_only; SHOW statement_timeout; SELECT count(*) FROM j2_monitor.sample();",
    )
    require(
        readonly.returncode == 0 and readonly.stdout.splitlines() == ["on", "2s", "1"],
        "monitor SQL projection failed",
    )
    sql = "UPDATE engine.workflow_executions SET state=state WHERE false;"
    read_only = postgres_probe(driver, password, "CREATE TABLE j2_monitor.j2_write_probe (x int);")
    denied = postgres_probe(driver, password, "SET default_transaction_read_only=off;\n" + sql)
    require(
        read_only.returncode != 0 and "25006" in read_only.stderr,
        "monitor write did not return read-only SQLSTATE",
    )
    require(
        denied.returncode != 0 and "42501" in denied.stderr,
        "monitor role has unexpected write privileges",
    )
    save(
        driver.output / "db-policy.json",
        {
            "query": readonly.stdout.splitlines(),
            "read_only": read_only.stderr,
            "privileges": denied.stderr,
        },
    )


def check_network(driver):
    ip = driver.pod(NS, EXPORTER)["status"]["podIP"]
    api = json.loads(driver.k("-n", NS, "get", "svc", "api", "-o", "json").stdout)["spec"][
        "clusterIP"
    ]
    worker = driver.images["worker"]
    for ns in (NS, OTHER):
        driver.k(
            "-n",
            ns,
            "run",
            "unrelated",
            f"--image={worker}",
            "--restart=Never",
            "--command",
            "--",
            "python",
            "-c",
            "import time; time.sleep(180)",
        )
        driver.k("-n", ns, "wait", "pod/unrelated", "--for=condition=Ready", "--timeout=100s")
    exporter_pod = driver.pod(NS, EXPORTER)["metadata"]["name"]
    driver.k(
        "-n",
        NS,
        "debug",
        exporter_pod,
        "--attach=false",
        f"--image={worker}",
        "--container=network-probe",
        "--profile=general",
        "--",
        "python",
        "-c",
        "import time; time.sleep(180)",
    )
    driver.until(
        lambda: any(
            "running" in status["state"]
            for status in driver.pod(NS, EXPORTER)["status"].get("ephemeralContainerStatuses", [])
        ),
        60,
    )
    write_probe = """import http.client,sys
try:
    c = http.client.HTTPConnection(sys.argv[1], 8080, timeout=3)
    c.request('POST', '/v1/workflows', '{}', {'Content-Type':'application/json'})
    print(c.getresponse().status)
except TimeoutError:
    print('TCP timeout')
"""
    allowed = driver.python(NS, "observer", write_probe.replace("sys.argv[1]", repr(api)))
    require(allowed in {"400", "401", "403", "422"}, "application route positive control failed")
    denied = driver.k(
        "-n",
        NS,
        "exec",
        exporter_pod,
        "-c",
        "network-probe",
        "--",
        "python",
        "-c",
        write_probe,
        api,
    ).stdout.strip()
    require(denied == "TCP timeout", "exporter can reach application write route")
    scrape_probe = """import socket,sys
try:
    with socket.create_connection((sys.argv[1], 9187), 3):
        print('connected')
except TimeoutError:
    print('TCP timeout')
"""
    unrelated = {
        ns: driver.k(
            "-n", ns, "exec", "unrelated", "--", "python", "-c", scrape_probe, ip
        ).stdout.strip()
        for ns in (NS, OTHER)
    }
    require(set(unrelated.values()) == {"TCP timeout"}, "unrelated pod can reach exporter")
    save(
        driver.output / "network-policy.json",
        {
            "unrelated_scrapes": unrelated,
            "app_post_from_exporter": denied,
            "app_post_from_observer": allowed,
        },
    )


def check_scrapes(driver):
    # Port-forward is for evidence extraction. Scraping itself originates in Prometheus.
    kubectl = driver.scratch / "bin/kubectl"
    with (
        (driver.output / "port-forward.log").open("w") as log,
        subprocess.Popen(
            [
                str(kubectl),
                "--context",
                "kind-" + kind_driver.CLUSTER,
                "-n",
                NS,
                "port-forward",
                f"deployment/{PROM}",
                "19090:9090",
            ],
            env=driver.env,
            stdout=log,
            stderr=log,
        ) as forwarding,
    ):

        def query(expression):
            params = urlencode({"query": expression})
            try:
                with urlopen(
                    f"http://127.0.0.1:19090/api/v1/query?{params}", timeout=2
                ) as response:
                    return json.load(response)["data"]["result"]
            except URLError:
                return []

        try:
            driver.until(lambda: bool(query("up{job='j2'} == 1") and query("j2_query_ok == 1")), 45)
            time.sleep(8)
            series = {
                name: query(f"{name}[8s]")
                for name in (
                    "up",
                    "j2_query_ok",
                    "j2_sample_timestamp_seconds",
                    "j2_outstanding",
                    "j2_unpublished",
                    "j2_admissions",
                    "j2_terminals",
                    "j2_oldest_claimable_seconds",
                    "j2_db_wait_lock",
                )
            }
            save(driver.output / "prometheus-samples.json", series)
            for name, result in series.items():
                require(
                    len(result) == 1 and len(result[0]["values"]) >= 6,
                    f"missing scrape samples: {name}",
                )
                if name in {"up", "j2_query_ok"}:
                    require(
                        all(v == "1" for _, v in result[0]["values"]),
                        f"failed scrape or query: {name}",
                    )
            actual = sorted(
                {float(value) for _, value in series["j2_sample_timestamp_seconds"][0]["values"]}
            )
            gaps = [b - a for a, b in zip(actual, actual[1:])]
            require(len(actual) >= 5 and max(gaps) <= 5, "exporter samples are missing or stale")
            save(
                driver.output / "freshness.json",
                {"distinct_samples": len(actual), "max_gap_seconds": max(gaps)},
            )
        finally:
            forwarding.terminate()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    parser.add_argument("scratch", type=Path)
    parser.add_argument("layouts", type=Path)
    args = parser.parse_args()
    require(
        os.environ.get("GITHUB_ACTIONS") == "true"
        and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted",
        "J2 overlay pilot requires GitHub-hosted Actions",
    )
    args.evidence.mkdir()
    args.scratch.mkdir()
    kind_driver.CLUSTER, kind_driver.LABEL = "j2-overlay", "j2.overlay=owned"
    driver = J2Driver(args.evidence, args.scratch)
    driver.monitor_password = secrets.token_hex(32)
    lock = json.loads((J2 / "images.lock.json").read_bytes())
    driver.images = {name: lock[name] for name in ("runtime", "worker")}
    try:
        driver.install()
        for addon in ("calico", "keda"):
            driver.prepare_addon(addon)
        registry = kind_driver.CLUSTER + "-registry"
        driver.created.add(registry)
        driver.command(
            [
                "docker",
                "run",
                "-d",
                "--name",
                registry,
                "--label",
                kind_driver.LABEL,
                "-p",
                "127.0.0.1:5001:5000",
                kind_driver.LOCK["registry"],
            ]
        )
        driver.until(
            lambda: (
                driver.command(
                    ["curl", "-fsS", "http://localhost:5001/v2/"], allowed=True, record=False
                ).returncode
                == 0
            ),
            30,
        )
        for name, image in driver.images.items():
            restore(args.layouts / name, image)
        tag = "localhost:5001/j2-exporter:pilot"
        driver.command(
            [
                "docker",
                "build",
                "--provenance=false",
                "--platform=linux/amd64",
                "-t",
                tag,
                "-f",
                str(J2 / "exporter/Dockerfile"),
                ".",
            ],
            timeout=900,
        )
        driver.command(["docker", "push", tag])
        (exporter,) = json.loads(
            driver.command(["docker", "inspect", "--format", "{{json .RepoDigests}}", tag]).stdout
        )
        retain(driver.output / "exporter", exporter)
        save(driver.output / "exporter-image.json", {"image": exporter})
        addresses = driver.cluster()
        apply_app(driver, addresses)
        password = driver.monitor_password
        db_ip = addresses["192.0.2.1"]
        result = postgres_probe(
            driver,
            driver.password,
            (J2 / "exporter/monitor.sql")
            .read_text()
            .replace(":'namespace'", f"'{NS}'")
            .replace(":'password'", f"'{password}'"),
            superuser=True,
        )
        require(result.returncode == 0, "monitor SQL installation failed")
        check_db_denial(driver, password)
        secret(
            driver,
            "j2-monitor",
            {"dsn": f"postgresql://j2_exporter:{password}@{db_ip}:5432/{DB}?sslmode=disable"},
        )
        rendered = render(NS, db_ip, exporter)
        save(driver.output / "overlay-rendered.json", rendered)
        save(driver.output / "connection-budget.json", budget(rendered, registered_budget()))
        driver.apply(rendered)
        rollout(driver, EXPORTER)
        rollout(driver, PROM)
        check_scrapes(driver)
        sessions = driver.sql(
            "SELECT application_name, count(*) FROM pg_stat_activity WHERE usename='j2_exporter' GROUP BY 1",
            DB,
        )
        require(sessions == "j2-db-exporter|1", "live monitor connection budget differs")
        save(driver.output / "monitor-connections.json", {"pg_stat_activity": sessions})
        pods = json.loads(driver.k("-n", NS, "get", "pods", "-o", "json").stdout)["items"]
        for pod in pods:
            app = pod["metadata"].get("labels", {}).get("app")
            pin = {PROM: PROMETHEUS, EXPORTER: exporter}.get(app)
            for c in pod["status"].get("containerStatuses", []):
                require(c["restartCount"] == 0, "pod restarted during overlay pilot")
                if not pin:
                    pin = next((v for v in driver.images.values() if v == c["image"]), None)
                if pin:
                    require(
                        c["imageID"].endswith(pin.split("@")[1]), f"running image differs for {app}"
                    )
        check_network(driver)
        save(
            driver.output / "verdict.json", {"status": "PASS", "scope": "unmeasured overlay pilot"}
        )
    except BaseException as error:
        save(driver.output / "verdict.json", {"status": "FAIL", "error": str(error)})
        if driver.cluster_started:
            driver.k("-n", NS, "get", "pods,events", "-o", "json", allowed=True)
        raise
    finally:
        save(driver.output / "cleanup.json", driver.cleanup())


if __name__ == "__main__":
    main()
