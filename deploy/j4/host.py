"""Hosted J4 infrastructure. Job-owned kind, forwards, pods and read-only DB exports."""

from __future__ import annotations

import gzip
import http.client
import json
import os
import re
import subprocess
import time
from contextlib import ExitStack
from pathlib import Path

from deploy.j4.evidence import digest, read_jsonl, write_json
from deploy.j4.overlay import ROOT, Overlay, kind_config
from deploy.j4.qualification import (
    require_calico_policies,
    require_calico_ready,
    require_probe_evidence,
)

LOCK = json.loads((ROOT / "deploy/k8s/tools.lock.json").read_bytes())
TASK = "durable-agent.tasks.v1"
EVENT = "durable-agent.events.v1"
DEFINITION = "j4.recorded-llm-recovery"
PROVIDER = "j4.recorded-llm"
GRAPH = {
    "entry": PROVIDER,
    "nodes": [
        {"id": PROVIDER, "kind": "activity", "next": "pure.echo"},
        {"id": "pure.echo", "kind": "activity"},
    ],
}
MODES = {"runtime": 8080, "worker": 8081, "stub": 8082, "private": 8083, "postgres": 5432}
ROLES = ("runtime", "worker", "stub", "wrong-pod", "outside", "dns-decoy")
MIGRATIONS = sorted((ROOT / "migrations").glob("*.up.sql"))
B2_MIGRATION = ROOT / "migrations/j4_b2/000025_j4_provider_resilience.up.sql"

# One read-only statement gives all tables the same committed snapshot. The
# claim UUIDs are hashed in PostgreSQL, before data can reach the exporter.
SNAPSHOT = """
SELECT jsonb_build_object(
 'workflows', COALESCE((SELECT jsonb_agg(to_jsonb(w)) FROM engine.workflow_executions w), '[]'),
 'nodes', COALESCE((SELECT jsonb_agg(to_jsonb(n) ORDER BY node_id, iteration)
                    FROM engine.node_instances n), '[]'),
 'attempts', COALESCE((SELECT jsonb_agg((to_jsonb(a) - 'claim_token') ||
                    jsonb_build_object('claim_token_hash',
                      encode(sha256(convert_to(a.claim_token::text, 'UTF8')), 'hex'),
                      'transaction_id', a.xmin::text,
                      'committed_at', pg_xact_commit_timestamp(a.xmin))
                    ORDER BY node_id, iteration, attempt_number) FROM engine.activity_attempts a),
                    '[]'),
 'history', COALESCE((SELECT jsonb_agg(to_jsonb(h) || jsonb_build_object(
                    'transaction_id', h.xmin::text,
                    'committed_at', pg_xact_commit_timestamp(h.xmin))
                    ORDER BY revision) FROM engine.transition_history h), '[]'),
 'outbox', COALESCE((SELECT jsonb_agg(to_jsonb(o) - 'claim_owner'
                     ORDER BY aggregate_revision, event_id)
                     FROM engine.outbox o), '[]'),
 'inbox', COALESCE((SELECT jsonb_agg(to_jsonb(i) ORDER BY consumer_id, event_id)
                    FROM engine.event_inbox i), '[]'),
 'offsets', COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY consumer_id, topic, kafka_partition)
                      FROM engine.consumer_offsets p), '[]'),
 'publications', COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY event_id, relay_attempt)
                           FROM engine.outbox_publications p), '[]'),
 'unexpected_results', (SELECT count(*) FROM engine.attempt_result_evidence),
 'reservations', %s)
"""
B2_ROWS = """COALESCE((SELECT jsonb_agg(to_jsonb(r) || jsonb_build_object(
    'transaction_id', r.xmin::text, 'committed_at', pg_xact_commit_timestamp(r.xmin))
    ORDER BY sequence) FROM engine.j4_provider_attempts r), '[]')"""


def wait_for(probe, until_ns: int, label: str, interval: float = 0.025):
    while time.monotonic_ns() < until_ns:
        if result := probe():
            if time.monotonic_ns() < until_ns:
                return result
            break
        time.sleep(interval)
    raise TimeoutError(label)


def request(port: int, path: str, body: dict | None = None, timeout: float = 1) -> tuple[int, dict]:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        connection.request(
            "GET" if body is None else "POST",
            path,
            body=None if body is None else json.dumps(body),
            headers={"Content-Type": "application/json", "Connection": "close"},
        )
        response = connection.getresponse()
        raw = response.read(65537)
        if len(raw) > 65536:
            raise ValueError("unexpected J4 HTTP response length")
        return response.status, json.loads(raw) if raw.startswith(b"{") else {}
    finally:
        connection.close()


class Host:
    def __init__(self, scratch: Path, raw: Path, campaign: str, images: dict, deadline_ns: int):
        self.scratch, self.raw = scratch, raw
        self.name = campaign
        self.images = images
        self.deadline_ns = deadline_ns
        self.env = {
            **os.environ,
            "KUBECONFIG": str(scratch / "kubeconfig"),
            "PATH": str(scratch / "bin") + os.pathsep + os.environ["PATH"],
        }
        self.started = False
        self.loaded = set()

    def seconds(self, ceiling: float) -> float:
        budget = min(ceiling, (self.deadline_ns - time.monotonic_ns()) / 1e9)
        if budget <= 0:
            raise TimeoutError("J4 stage deadline")
        return budget

    def cmd(self, args, *, data=None, timeout=30, check=True, env=None):
        result = subprocess.run(
            args,
            cwd=ROOT,
            env=self.env if env is None else env,
            input=data,
            text=True,
            capture_output=True,
            timeout=self.seconds(timeout),
            check=False,
        )
        if check and result.returncode:
            # Avoid including request content or a command environment in retained errors.
            raise RuntimeError(f"J4 command failed {args[0]} {args[1]} ({result.returncode})")
        return result.stdout if check else result

    def kube(self, *args, ns=None, **options):
        return self.cmd(["kubectl", *([] if ns is None else ["-n", ns]), *args], **options)

    def apply(self, manifests, ns=None):
        self.kube(
            "apply",
            "-f",
            "-",
            data=json.dumps({"apiVersion": "v1", "kind": "List", "items": manifests}),
            ns=ns,
            timeout=60,
        )

    def bootstrap(self, output: Path) -> None:
        import yaml
        from dur053_k3 import configure_calico, download, lock_addon_images

        tools = self.scratch / "bin"
        tools.mkdir()
        for name in ("kind", "kubectl"):
            pin = LOCK["tools"][name]
            path = tools / name
            path.write_bytes(download(pin["url"], pin["sha256"]))
            path.chmod(0o755)
        cfg = kind_config(self.raw)
        config = self.scratch / "kind.json"
        write_json(config, cfg)
        addon = LOCK["addons"]["calico"]
        manifests = list(filter(None, yaml.safe_load_all(download(addon["url"], addon["sha256"]))))
        configure_calico(manifests, cfg)
        pinned = lock_addon_images(manifests, addon["images"])
        self.started = True
        self.cmd(
            [
                "kind",
                "create",
                "cluster",
                "--name",
                self.name,
                "--image",
                LOCK["node"],
                "--config",
                str(config),
                "--kubeconfig",
                self.env["KUBECONFIG"],
            ],
            timeout=420,
        )
        all_images = sorted(pinned | {LOCK["postgres"], LOCK["kafka"]})
        for image in all_images:
            self.cmd(["docker", "pull", image], timeout=240)
        all_images.extend(spec["tag"] for spec in self.images.values())
        self.cmd(["kind", "load", "docker-image", "--name", self.name, *all_images], timeout=420)
        self.apply(manifests)
        self.kube(
            "-n",
            "kube-system",
            "rollout",
            "status",
            "ds/calico-node",
            "--timeout=240s",
            timeout=245,
        )
        self.kube("wait", "--for=condition=Ready", "node", "--all", "--timeout=120s", timeout=125)
        require_calico_ready(
            json.loads(self.kube("-n", "kube-system", "get", "ds/calico-node", "-o", "json")),
            LOCK["images"],
        )
        nodes = json.loads(self.kube("get", "nodes", "-o", "json"))["items"]
        if len(nodes) != 1:
            raise ValueError("J4 requires exactly one live kind node")
        write_json(
            output / "infrastructure.json",
            {
                "node": LOCK["node"],
                "node_versions": nodes[0]["status"]["nodeInfo"],
                "calico": manifests,
                "images": sorted(pinned),
                "lock_sha256": digest(ROOT / "deploy/k8s/tools.lock.json"),
            },
        )

    def remove_namespace(self, namespace: str) -> None:
        self.kube(
            "delete",
            "ns",
            namespace,
            "--ignore-not-found=true",
            "--wait=true",
            "--timeout=100s",
            timeout=105,
        )
        if self.kube("get", "ns", namespace, "--ignore-not-found=true", "-o", "name").strip():
            raise ValueError("J4 namespace still exists after reset")

    def destroy(self) -> None:
        if self.started:
            self.cmd(["kind", "delete", "cluster", "--name", self.name], timeout=150)
            nodes = self.cmd(
                ["docker", "ps", "-aq", "--filter", f"label=io.x-k8s.kind.cluster={self.name}"]
            )
            if nodes.strip() or self.name in self.cmd(["kind", "get", "clusters"]).splitlines():
                raise ValueError("J4 job-owned kind resources survived teardown")

    def start(self, args, output: Path, stack: ExitStack, stdin=None):
        stream = stack.enter_context(output.open("wb"))
        proc = subprocess.Popen(
            args, cwd=ROOT, env=self.env, stdin=stdin, stdout=stream, stderr=subprocess.DEVNULL
        )
        stack.callback(stop_process, proc)
        return proc


def stop_process(proc) -> None:
    if proc.stdin:
        proc.stdin.close()
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(2)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(2)


def seal_images(host: Host, images: dict, output: Path, prior: Path | None) -> dict:
    if prior:
        for role, spec in images.items():
            archive = prior / "images" / f"{role}.tar.gz"
            if digest(archive) != spec["archive_sha256"]:
                raise ValueError("J4 candidate archive differs from qualification")
            host.cmd(["docker", "load", "--input", str(archive)], timeout=240)
            host.loaded.add(spec["tag"])
    else:
        runtime, worker = (images[key]["tag"] for key in ("runtime", "worker"))
        base = f"j4-base:{runtime.split(':')[1]}"
        for role, tag in (("runtime", runtime), ("worker", base)):
            host.cmd(
                [
                    "docker",
                    "build",
                    "-f",
                    "deploy/k8s/Dockerfile",
                    "--target",
                    role,
                    "-t",
                    tag,
                    ".",
                ],
                timeout=720,
            )
            host.loaded.add(tag)
        host.cmd(
            [
                "docker",
                "build",
                "-f",
                "deploy/j4/Dockerfile",
                "--build-arg",
                f"WORKER_IMAGE={base}",
                "-t",
                worker,
                ".",
            ],
            timeout=360,
        )
        host.loaded.add(worker)
        (output / "images").mkdir()
        for role, spec in images.items():
            archive = output / "images" / f"{role}.tar.gz"
            pending = host.scratch / f"{role}.tar"
            host.cmd(["docker", "save", "-o", str(pending), spec["tag"]], timeout=180)
            with pending.open("rb") as source, gzip.open(archive, "wb", compresslevel=1) as zipped:
                while batch := source.read(1024 * 1024):
                    host.seconds(180)
                    zipped.write(batch)
            pending.unlink()
            spec["archive_sha256"] = digest(archive)
    for spec in images.values():
        actual = host.cmd(
            ["docker", "image", "inspect", "--format", "{{.Id}}", spec["tag"]]
        ).strip()
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", actual) or spec.get("id", actual) != actual:
            raise ValueError("J4 loaded a different candidate image")
        spec["id"] = actual
        host.loaded.add(spec["tag"])
    return images


class Topology:
    def __init__(self, host: Host, overlay: Overlay, out: Path, stack: ExitStack):
        self.host, self.overlay, self.out, self.stack = host, overlay, out, stack
        self.raw = host.raw / overlay.cell
        self.raw.mkdir()
        self.ports = {}
        self.manifests = []
        self.forwards = {}
        for role in ROLES:
            path = self.raw / role
            path.mkdir()
            if role != "worker":
                # Each pod mounts only its own segment. The controller owns
                # the directory and grants write access to the pod's group.
                host.cmd(["sudo", "chgrp", "65532", str(path)])
                path.chmod(0o775)

    def kube(self, *args, **options):
        return self.host.kube(*args, ns=self.overlay.namespace, **options)

    def apply(self, manifests):
        self.manifests.extend(manifests)
        write_json(self.out / "overlay.json", self.manifests)
        self.host.apply(manifests)

    def object(self, kind, name):
        return json.loads(self.kube("get", kind, name, "-o", "json"))

    def ip(self, role):
        return self.object("pod", role)["status"]["podIP"]

    def service(self, role):
        return self.object("service", role)["spec"]["clusterIP"]

    def ready(self, *roles):
        self.kube(
            "wait",
            "--for=condition=Ready",
            *(f"pod/{r}" for r in roles),
            "--timeout=65s",
            timeout=70,
        )

    def exec(self, role, *args, **options):
        return self.kube("exec", "-i", role, "--", *args, **options)

    def forward(self, role):
        if role in self.forwards:
            stop_process(self.forwards.pop(role))
        path = self.out / f"forward-{role}.log"
        pod = "runtime" if role == "private" else role
        proc = self.host.start(
            [
                "kubectl",
                "-n",
                self.overlay.namespace,
                "port-forward",
                "--address=127.0.0.1",
                f"pod/{pod}",
                f":{MODES[role]}",
            ],
            path,
            self.stack,
        )

        def selected_port():
            return re.search(r"Forwarding from 127\.0\.0\.1:(\d+)", path.read_text())

        match = wait_for(selected_port, time.monotonic_ns() + 8_000_000_000, f"J4 {role} forward")
        self.forwards[role] = proc
        self.ports[role] = int(match[1])
        return self.ports[role]

    def health(self, role):
        try:
            status, body = request(
                self.ports[role], "/readyz" if role == "stub" else "/healthz", timeout=0.2
            )
            return body if status == 200 else None
        except (OSError, http.client.HTTPException, ValueError):
            return None

    def http_ready(self, role):
        return wait_for(
            lambda: self.health(role),
            min(self.host.deadline_ns, time.monotonic_ns() + 25_000_000_000),
            f"J4 {role} health",
        )

    def setup_infra(self):
        self.apply(self.overlay.bootstrap())
        self.broker = self.service("kafka")
        self.apply(self.overlay.service_pods(self.broker, LOCK["postgres"], LOCK["kafka"]))
        self.ready("postgres", "kafka", "stub", "wrong-pod", "outside", "dns-decoy", "probe")
        self.forward("postgres")
        self.forward("stub")
        self.http_ready("stub")
        self.wait_service("postgres", "pg_isready", "-U", "postgres")
        kafka = "/opt/kafka/bin/kafka-topics.sh"
        self.wait_service("kafka", kafka, "--bootstrap-server", "localhost:9092", "--list")
        for topic in (TASK, EVENT):
            self.exec(
                "kafka",
                kafka,
                "--bootstrap-server",
                "localhost:9092",
                "--create",
                "--topic",
                topic,
                "--partitions",
                "1",
                "--replication-factor",
                "1",
            )

    def wait_service(self, role, *args):
        wait_for(
            lambda: self.exec(role, *args, check=False, timeout=6).returncode == 0,
            min(self.host.deadline_ns, time.monotonic_ns() + 30_000_000_000),
            f"J4 {role} startup",
            0.1,
        )

    def postgres(self, *, readonly: bool):
        import psycopg

        conn = psycopg.connect(
            host="127.0.0.1",
            port=self.ports["postgres"],
            dbname="postgres",
            user="postgres",
            connect_timeout=3,
            autocommit=True,
            options="-c statement_timeout=2000",
        )
        conn.read_only = readonly
        if readonly:
            conn.execute("SET default_transaction_read_only=on")
        return conn

    def seed(self):
        migrations = MIGRATIONS + ([B2_MIGRATION] if self.overlay.b2 else [])
        # The fresh DB cannot have a persisted engine result yet.
        self.exec(
            "postgres",
            "psql",
            "-X",
            "-q",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "postgres",
            data="\n".join(path.read_text() for path in migrations),
            timeout=45,
        )
        with self.postgres(readonly=False) as conn:
            actual = [
                n
                for (n,) in conn.execute(
                    "SELECT version FROM engine.schema_migrations ORDER BY version"
                ).fetchall()
            ]
            expected = [int(path.name.split("_")[0]) for path in migrations]
            if actual != expected:
                raise ValueError(
                    "J4 deployed migration inventory differs from the selected profile"
                )
            conn.execute(
                "INSERT INTO engine.workflow_definitions "
                "(definition_id, version, definition_hash, graph, "
                "activity_versions, effect_classes) "
                "VALUES (%s, 1, %s, %s::jsonb, %s::jsonb, %s::jsonb)",
                (
                    DEFINITION,
                    "j4.fixture.graph.v1",
                    json.dumps(GRAPH),
                    json.dumps({PROVIDER: "v1", "pure.echo": "v1"}),
                    json.dumps({PROVIDER: "PURE_ACTIVITY", "pure.echo": "PURE_ACTIVITY"}),
                ),
            )
        write_json(
            self.out / "migrations.json",
            {
                "ledger": actual,
                "files": {str(p.relative_to(ROOT)): digest(p) for p in migrations},
            },
        )
        self.snapshot_connection = self.stack.enter_context(self.postgres(readonly=True))

    def snapshot(self) -> dict:
        requested = time.monotonic_ns()
        rows = self.snapshot_connection.execute(
            SNAPSHOT % (B2_ROWS if self.overlay.b2 else "'[]'::jsonb")
        ).fetchone()[0]
        return {"request_ns": requested, "observed_ns": time.monotonic_ns(), "db": rows}

    def start_runtime(self, selection, revision):
        self.apply([self.overlay.runtime(self.service("postgres"), self.broker)])
        self.ready("runtime")
        self.forward("runtime")
        self.forward("private")
        report = self.http_ready("runtime")
        # These fields must be reported by the running J9a relay. The
        # desired PodSpec environment cannot prove which mode was selected.
        relay = {
            "source_revision": revision,
            "runtime_image_id": self.image_id("runtime"),
            "switch": report.get("relay_switch"),
            "value": report.get("relay_value"),
            "effective_relay_mode": report.get("effective_relay_mode"),
        }
        selection.confirm(relay, revision, self.host.images["runtime"]["id"])
        if (report.get("recorded_activity"), report.get("provider_resilience")) != (
            True,
            self.overlay.b2,
        ):
            raise ValueError("J4 runtime switch did not take effect")
        write_json(self.out / "relay.json", relay)
        base = (
            f"/v1/workflows/{self.overlay.workflow_id}/nodes/{PROVIDER}"
            "/iterations/0/provider-attempts"
        )
        statuses = {
            role: {
                route: request(self.ports[role], f"{base}/{route}", {})[0]
                for route in ("reserve", "outcome")
            }
            for role in ("runtime", "private")
        }
        expected_private = 400 if self.overlay.b2 else 404
        if list(statuses["runtime"].values()) != [404, 404] or (
            list(statuses["private"].values()) != [expected_private, expected_private]
        ):
            raise ValueError("J4 private provider route inventory mismatch")
        write_json(self.out / "routes.json", statuses)

    def start_worker(self):
        manifests = self.overlay.worker(
            self.ip("stub"),
            self.broker,
            self.service("worker-control"),
            self.ip("wrong-pod"),
            self.ip("outside"),
            self.ip("dns-decoy"),
        )
        self.plan = manifests[0]["data"]["probes.json"]
        live = json.loads(self.kube("get", "networkpolicy", "-o", "json"))
        require_calico_policies(self.overlay, live["items"])
        write_json(self.out / "running-policies.json", live["items"])
        self.apply(manifests)
        self.ready("worker")
        self.forward("worker")
        return self.http_ready("worker")

    def image_id(self, role):
        image = self.object("pod", role)["status"]["containerStatuses"][0]["imageID"]
        match = re.search(r"sha256:[0-9a-f]{64}$", image)
        if match is None:
            raise ValueError(f"missing live J4 {role} image ID")
        if "@sha256:" in image and role not in {"postgres", "kafka"}:
            # CRI can return a manifest reference. Compare its actual
            # configuration digest to the one from docker image inspect.
            node = self.host.name + "-control-plane"
            report = json.loads(
                self.host.cmd(
                    ["docker", "exec", node, "crictl", "inspecti", image.split("://")[-1]]
                )
            )
            config = report["status"]["id"]
            if not re.fullmatch(r"sha256:[0-9a-f]{64}", config):
                raise ValueError(f"missing live J4 {role} image config ID")
            return config
        return match[0]

    def verify_worker(self, health):
        if (health.get("recorded_activity"), health.get("provider_resilience")) != (
            True,
            self.overlay.b2,
        ):
            raise ValueError("J4 worker switches differ from runtime switches")
        witness = next(
            row["fields"]
            for row in self.traces()["worker"]
            if row["process_session"] == health["process_session"]
            and row["hook"] == "isolation_verified"
        )
        reach = json.loads(
            self.exec(
                "probe",
                "python",
                "-m",
                "deploy.j4.probes",
                "reachable",
                self.ip("stub"),
                data=self.plan,
            )
        )
        decoys = {
            "wrong_port": "stub",
            "wrong_pod": "wrong-pod",
            "outside": "outside",
            "dns": "dns-decoy",
        }
        rows = {key: read_jsonl(self.raw / role / "decoy.jsonl") for key, role in decoys.items()}
        probe_ip, worker_ip = self.ip("probe"), self.ip("worker")
        require_probe_evidence(witness, reach, rows, probe_ip, worker_ip)
        inventory = read_jsonl(self.raw / "stub/inventory.jsonl")
        if not inventory or any(
            r["provider_credentials"] or r["engine_credentials"] for r in inventory
        ):
            raise ValueError("J4 stub credential inventory is absent or nonempty")
        caller = witness["caller"]["inventory"]
        if (
            witness["worker"]["provider_credentials"]
            or caller["provider_credentials"]
            or caller["engine_credentials"]
            or caller["uid"] != 65533
        ):
            raise ValueError("J4 provider credentials present")
        write_json(
            self.out / f"probes-{health['process_session']}.json",
            {
                "inventory": inventory,
                "worker": witness,
                "reachability": reach,
                "decoys": rows,
                "probe_ipv4": probe_ip,
                "worker_ipv4": worker_ip,
            },
        )

    def parity(self):
        runtime = json.loads(self.exec("runtime", "/runtime", "activity-registry"))
        worker = json.loads(self.exec("worker", "python", "-m", "workers", "activity-registry"))
        if not any(r["name"] == PROVIDER and r["version"] == "v1" for r in worker):
            raise ValueError("J4 worker did not register the fixture")
        worker_file = self.raw / "runtime/worker-registry.json"
        write_json(worker_file, worker)
        check = self.exec(
            "runtime",
            "/runtime",
            "compatibility-check",
            "--namespace",
            self.overlay.namespace,
            "--no-surviving-workers",
            "--worker-registry",
            "/evidence/worker-registry.json",
        )
        write_json(self.out / "parity.json", {"runtime": runtime, "worker": worker, "check": check})

    def start_broker(self):
        self.broker_file = self.out / "broker.jsonl"
        self.broker_proc = self.host.start(
            [
                "kubectl",
                "-n",
                self.overlay.namespace,
                "exec",
                "-i",
                "probe",
                "--",
                "python",
                "-m",
                "deploy.j4.kafka",
                f"{self.broker}:9092",
            ],
            self.broker_file,
            self.stack,
            stdin=subprocess.PIPE,
        )
        wait_for(
            lambda: any(r["kind"] == "ready" for r in read_jsonl(self.broker_file)),
            min(self.host.deadline_ns, time.monotonic_ns() + 12_000_000_000),
            "J4 independent Kafka witness",
        )

    def traces(self):
        return {
            "worker": read_jsonl(self.raw / "worker/worker.jsonl"),
            "stub": read_jsonl(self.raw / "stub/stub.jsonl"),
            "scheduler": read_jsonl(self.raw / "runtime/scheduler.jsonl"),
            "broker": read_jsonl(self.out / "broker.jsonl"),
        }
