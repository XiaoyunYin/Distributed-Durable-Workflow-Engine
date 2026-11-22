"""Collect Phase G engineering evidence on the hosted kind harness."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import tempfile
from contextlib import contextmanager, nullcontext
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from dur053_k3 import CLUSTER, K8S, LABEL, LOCK, ROOT, Driver
from dur053_k6_b_evidence import Evidence
from dur053_k6_b_smoke import ENGINE_ROLE, Smoke, json_data, private_file, require, subject
from dur053_k6_c_deploy import (
    ENGINE_CONTAINER,
    EXPORTER,
    RELAY_PEER,
    SINK_DB,
    SINK_PEER,
    encoded,
)
from dur053_k6_c_render import PORT, RELAY, SINK
from dur053_k6_c_smoke import make_output
from dur053_k6_d_render import CONTROL, OBSERVER, ROLES, base, mount, render, set_env
from dur053_k6_d_smoke import NS as D_NAMESPACE
from dur053_k6_d_smoke import SQL, Matrix, definition_sql
from dur053_k6_e1_join import EVENTS, GROUP, TASKS
from dur053_k6_g_bundle import canonical, make_plan, replay, select_cells, sha, wire, write_fence
from dur053_k6_g_registration import AUDIENCE, INTAKE, bind_registration, observe_runtime, utc
from dur053_k6_identity import REVIEWER, WORKER, obj
from dur053_k6_registry import REGISTRY, read_registry

NODE = CLUSTER + "-control-plane"
KAFKA = CLUSTER + "-kafka"
PUBLISHER = "outbox-publisher"
SUBMIT_WIRE = """
import http.client, sys
from pathlib import Path

token = Path('/identity/token').read_text().strip()
connection = http.client.HTTPConnection('api', 8080, timeout=5)
try:
    connection.request('POST', '/v1/workflows', sys.stdin.buffer.read(), {
        'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json',
    })
    response = connection.getresponse()
    print(response.status, response.read().hex())
finally:
    connection.close()
"""


class PhaseGEvidence(Evidence):
    schema = "dur053.k6-phase-g-smoke.v1"


def atomic(path: Path, data: bytes) -> None:
    temp = path.with_suffix(".pending")
    temp.write_bytes(data)
    temp.replace(path)


def obs_lines(logs: bytes, event: str) -> list[dict]:
    prefix = f"DUR053_K6_OBSERVATION {event} ".encode()
    return [
        json.loads(line[len(prefix) :]) for line in logs.splitlines() if line.startswith(prefix)
    ]


def components(objects: list[dict], database: str) -> list[dict]:
    out = [{"kind": "database", "name": database.replace("_", "-"), "role": "engine"}]
    for item in objects:
        kind, meta = item["kind"].lower(), item["metadata"]
        if kind in {"deployment", "service"}:
            out.append(
                {"kind": kind, "name": meta["name"], "role": meta["labels"]["dur053.io/component"]}
            )
    return sorted(out, key=lambda row: (row["kind"], row["name"]))


class Episode(Matrix):
    inputs_schema = "dur053.k6-phase-g-inputs.v1"

    def __init__(self, evidence, scratch, harness, shared, cell):
        self.effect = cell["workload"] == "COOPERATING_EFFECT"
        self.arm = "effect" if self.effect else "pure"
        self.namespace = "dur053-g-" + self.arm
        self.database = f"dur053_g_{self.arm}"
        self.sink_container = CLUSTER + "-g-sink-db"
        self.sink_volume = self.sink_container + "-data"
        super().__init__(evidence, scratch, harness)
        self.shared = shared
        self.cell = cell
        self.binding = dict(
            episode=cell["episode"],
            namespace=self.namespace,
            reset_id=str(uuid4()),
            barrier_id=str(uuid4()),
        )
        self.witness_dir = scratch / "witness"
        self.bootstrap_dir = scratch / "bootstrap"
        self.allocator_dir = scratch / "allocator"
        self.frames = scratch / "frames"
        for path in (self.witness_dir, self.bootstrap_dir, self.allocator_dir, self.frames):
            path.mkdir()
        self.command(
            "G_WRITER_VOLUME_OWNERSHIP",
            "docker",
            "exec",
            NODE,
            "chown",
            "65532:65532",
            str(self.witness_dir),
            str(self.allocator_dir),
        )
        self.registrations = scratch / "registrations.json"
        self.registrations.write_bytes(b"[]\n")
        self.fence = scratch / "fence.json"
        self.members = scratch / "members.json"
        self.decisions = scratch / "decisions.jsonl"
        self.allocator = self.allocator_dir / "allocator.jsonl"
        self.intake: list[dict] = []
        self.coordinator: list[dict] = []
        self.boot_nonces: dict[str, str] = {}

    def plan(self, namespace: str) -> dict:
        require(namespace == self.namespace, "G_NAMESPACE")
        return self.allocator_plan

    def pods_for(self, role: str) -> list[dict]:
        return json.loads(
            self.k("G_PODS", "-n", self.namespace, "get", "pods", "-l", f"app={role}", "-o", "json")
        )["items"]

    def pod_document(self, name: str) -> dict:
        return json.loads(
            self.k("G_POD_RUNTIME", "-n", self.namespace, "get", "pod", name, "-o", "json")
        )

    def exec_worker(self, name: str, *command: str) -> bytes:
        return self.k(
            "G_WORKER_EXEC", "-n", self.namespace, "exec", name, "-c", "worker", "--", *command
        )

    def logs_for(self, name: str) -> bytes:
        return self.k("G_WORKER_LOGS", "-n", self.namespace, "logs", name, "-c", "worker")

    def register(self, observation: dict, account_uid: str) -> dict:
        name = observation["pod_name"]
        identity = observation["identity"]

        def boot_nonce():
            raw = self.exec_worker(
                name, "sh", "-c", "test ! -f /tmp/g-boot || cat /tmp/g-boot"
            ).decode()
            return (
                raw
                if re.fullmatch(r"[a-f0-9]{32}", raw) and self.boot_nonces.get(name) != raw
                else None
            )

        nonce = self.wait_for(boot_nonce, "G_BOOT_NONCE", seconds=10)
        self.boot_nonces[name] = nonce
        token = self.exec_worker(name, "cat", "/identity/token").decode().strip()
        self.evidence.secret(token)
        review = json.loads(
            self.k(
                "G_TOKEN_REVIEW",
                "create",
                "--raw",
                "/apis/authentication.k8s.io/v1/tokenreviews",
                "-f",
                "-",
                data=json_data(
                    {
                        "apiVersion": "authentication.k8s.io/v1",
                        "kind": "TokenReview",
                        "spec": {"token": token, "audiences": [AUDIENCE]},
                    }
                ),
            )
        )
        claimant = f"{subject(self.namespace, WORKER)}:pod:{name}:uid:{identity['pod_uid']}"
        started = int(datetime.fromisoformat(observation["startedAt"]).timestamp()) * 10**9
        atomic(
            self.bootstrap_dir / nonce,
            canonical(
                {
                    "CONTAINER_ID": identity["container_id"],
                    "CONTAINER_STARTED_NS": str(started),
                    "CONTAINER_RESTART_COUNT": str(identity["restart"]),
                    "DUR053_K6_G_CLAIMANT": claimant,
                }
            ),
        )

        def startup():
            sessions = obs_lines(self.logs_for(name), "k6_g_session")
            return sessions[0] if len(sessions) == 1 else None

        session = self.wait_for(startup, "G_WORKER_STARTUP_NOT_REPORTED", seconds=30)
        after = observe_runtime(self.pod_document(name))
        registration = bind_registration(observation, after, review, account_uid, session)
        status = review["status"]
        self.intake.append(
            {
                "received_utc": utc(),
                "intake": INTAKE,
                "runtime": observation,
                "verified_after": after,
                "tokenreview": {
                    "authenticated": status["authenticated"],
                    "audiences": status["audiences"],
                    "username": status["user"]["username"],
                    "service_account_uid": status["user"]["uid"],
                    "pod_uid": status["user"]["extra"]["authentication.kubernetes.io/pod-uid"][0],
                },
                "registration": registration,
            }
        )
        atomic(self.registrations, canonical([entry["registration"] for entry in self.intake]))
        self.evidence.write("registration-intake.json", self.intake)
        return session

    def running(self, count: int, *, name: str = "", restart: int = 0):
        pods = [p for p in self.pods_for("worker") if not name or p["metadata"]["name"] == name]
        try:
            runtimes = [observe_runtime(pod) for pod in pods]
        except KeyError:
            return None
        if len(runtimes) == count and all(
            row["identity"]["restart"] == restart for row in runtimes
        ):
            return sorted(runtimes, key=lambda row: row["pod_name"])
        return None

    def worker_bootstrap(self, worker: dict) -> None:
        body = worker["spec"]
        body["replicas"] = 2
        pod = body["template"]["spec"]
        container = pod["containers"][0]
        container["command"] = ["python", "/g-entry/worker.py"]
        # The identity handoff is read-only for the pod. Only the ledger mount is writable.
        mount(pod, "g-entry", "/g-entry", {"configMap": {"name": "g-entry"}})
        mount(
            pod,
            "g-bootstrap",
            "/g-bootstrap",
            {"hostPath": {"path": str(self.bootstrap_dir), "type": "Directory"}},
        )
        mount(
            pod,
            "g-worker-ledgers",
            "/g-worker-ledgers",
            {"hostPath": {"path": str(self.witness_dir), "type": "Directory"}},
        )
        container["volumeMounts"][-1]["readOnly"] = False
        set_env(
            container,
            RUNTIME_NAMESPACE=self.namespace,
            DUR053_K6_G_WORKER_DIR="/g-worker-ledgers",
            **self.g_env(),
        )

    def g_env(self) -> dict[str, str]:
        return {
            f"DUR053_K6_G_{name}": self.binding[key]
            for name, key in (
                ("EPISODE", "episode"),
                ("RESET", "reset_id"),
                ("BARRIER", "barrier_id"),
            )
        }

    def engine_manifests(self, addresses: dict, key: str, plan_hash: str) -> list[dict]:
        objects = render(self.harness.images, addresses, key, plan_hash, namespace=self.namespace)
        stock = base()
        for item in objects:
            kind, name = item["kind"], item["metadata"]["name"]
            if kind in {"Service", "Deployment"}:
                item["metadata"].setdefault("labels", {})["dur053.io/component"] = (
                    PUBLISHER if name == "relay" else name
                )
            if kind != "Deployment":
                continue
            if name == "relay":
                item["metadata"]["name"] = PUBLISHER
            pod = item["spec"]["template"]["spec"]
            container = pod["containers"][0]
            if not self.effect and name in {CONTROL, "worker", "scheduler"}:
                orig = stock["Deployment", name]["spec"]["template"]["spec"]
                container["env"] = [
                    row
                    for row in orig["containers"][0]["env"]
                    if row["name"] != "DUR053_WITNESS_DIR"
                ]
                if name == CONTROL:
                    pod.pop("initContainers")
                    pod["volumes"] = orig["volumes"]
                    container["volumeMounts"] = orig["containers"][0]["volumeMounts"]
                    for probe in ("startupProbe", "readinessProbe", "livenessProbe"):
                        if probe in container:
                            container[probe] = orig["containers"][0][probe]
                elif name == "worker":
                    pod["volumes"] = [row for row in orig["volumes"] if row["name"] != "witness"]
                    container["volumeMounts"] = [
                        row
                        for row in orig["containers"][0]["volumeMounts"]
                        if row["name"] != "witness"
                    ]
                else:
                    pod["volumes"] = orig["volumes"]
                    container["volumeMounts"] = orig["containers"][0]["volumeMounts"]
            if name in {"api", CONTROL, "scheduler", "relay"}:
                set_env(container, DUR053_K4_CAMPAIGN="1")
            if name == CONTROL:
                set_env(container, RUNTIME_TOKEN_SERVICE_ACCOUNT=WORKER)
            if name == "scheduler":
                item["spec"]["replicas"] = 2
            if name == CONTROL and self.effect:
                mount(
                    pod,
                    "g-allocator",
                    "/g-allocator",
                    {"hostPath": {"path": str(self.allocator_dir), "type": "Directory"}},
                )
                container["volumeMounts"][-1]["readOnly"] = False
                set_env(
                    container,
                    DUR053_K6_G_ALLOCATOR_FILE="/g-allocator/allocator.jsonl",
                    **self.g_env(),
                )
            if name == "worker":
                self.worker_bootstrap(item)
        return objects

    def setup(self, addresses: dict) -> tuple[list[dict], dict]:
        self.address = addresses["192.0.2.1"]
        self.harness.databases.add(self.database)
        if self.effect:
            self.evidence.write("engine-tls.json", self.bootstrap(self.address))
            instance_rows = self.create_databases()
            self.evidence.write("database-instances.json", instance_rows)
            self.evidence.write("effect-tls.json", self.tls())
            engine_instance = instance_rows["engine"]["instance"]
        else:
            self.sql("G_ENGINE_DATABASE", f'CREATE DATABASE "{self.database}";')
            self.engine_created = True
            engine_instance = self.sql(
                "G_ENGINE_INSTANCE", "SELECT system_identifier FROM pg_control_system();"
            )
        plan, self.requests, self.allocator_plan = make_plan(
            self.binding, self.cell["seed"], self.effect
        )
        self.public_plan = plan
        plan_raw = canonical(plan)
        self.plan_file = self.scratch / "plan.json"
        self.plan_file.write_bytes(plan_raw)
        allocator_raw = json_data(self.allocator_plan)
        self.identity_created = True
        if self.effect:
            migration, episode = self.install_topology()
        else:
            identities, episode = self.identities(self.namespace)
            migration = next(item for item in identities if item["kind"] == "Job")
            self.create(
                "G_ENGINE_DB_SECRET",
                obj(
                    "Secret",
                    "campaign-db",
                    self.namespace,
                    stringData={"url": self.admin_dsn},
                    type="Opaque",
                ),
            )
        self.migrate(self.namespace, migration, "G_ENGINE_MIGRATE")
        self.db(definition_sql())
        reset_id = self.binding["reset_id"]
        self.sql(
            "G_ENGINE_BINDING",
            f"COMMENT ON DATABASE {self.database} IS 'dur053.k6.v1|{engine_instance}|{reset_id}';",
        )
        sink = None
        key = ""
        if self.effect:
            self.migrate_sink()
            sink = {"database": SINK_DB, "instance": instance_rows["sink"]["instance"]}
            self.psql(
                "sink",
                "admin",
                f"COMMENT ON DATABASE {SINK_DB} IS 'dur053.k6.v1|{sink['instance']}|{reset_id}';",
            )
            self.seed, key = self.signing_key()
            for name, data in (
                ("k6-load-plan", {"plan.json": allocator_raw.decode()}),
                ("k6-effect-ca", {"ca.crt": self.effect_ca.read_text()}),
            ):
                self.create(
                    "G_PLAN_CONFIGMAP",
                    obj("ConfigMap", name, self.namespace, immutable=True, data=data),
                )
        self.create(
            "G_WORKER_BOOTSTRAP",
            obj(
                "ConfigMap",
                "g-entry",
                self.namespace,
                immutable=True,
                data={"worker.py": (ROOT / "scripts/dur053_k6_g_worker.py").read_text()},
            ),
        )
        objects = self.engine_manifests(addresses, key, sha(allocator_raw))
        self.evidence.write("deployed-topology.json", objects)
        image_set = {**self.harness.images, **({"effect": self.image} if self.effect else {})}
        component_list = components(objects, self.database)
        self.manifest = {
            **self.binding,
            "schema": "dur053.k6-evidence.v1",
            "campaign": "g2c-engineering",
            "job": "k6-phase-g-smoke",
            "run_id": os.environ["GITHUB_RUN_ID"],
            **{field: self.cell[field] for field in ("ordinal", "cell", "workload", "seed")},
            "database": self.database,
            "engine_instance": str(engine_instance),
            "sink": sink,
            "plan_sha256": sha(plan_raw),
            "registry_sha256": sha(REGISTRY.read_bytes()),
            "source_tree_sha256": sha(
                self.command("G_SOURCE_TREE", "git", "ls-tree", "-rz", "HEAD")
            ),
            "image_set_sha256": sha(canonical(image_set)),
            "freeze_sha256": read_registry()["freeze_sha256"],
            "topology_sha256": sha(
                canonical(component_list if not self.effect else [*component_list, *self.rendered])
            ),
            "instrumentation": [],
            "required_sources": json.loads(
                self.command(
                    "G_SOURCE_LIST", str(self.shared / "collector"), "--required-sources", self.arm
                )
            ),
        }
        self.evidence.write(
            "reset-provisioning.json",
            {
                "cell": self.cell,
                "binding": self.binding,
                "engine_instance": str(engine_instance),
                "sink": sink,
                "image_set": image_set,
                "measurement_claim": False,
                "charged_seconds": 0,
            },
        )
        for item in objects:
            if item["kind"] != "Deployment":
                self.create("G_ENGINE_NETWORK", item)
        if not self.effect:
            self.capture_pure_inventory(component_list)
        return objects, episode

    def capture_pure_inventory(self, rendered: list[dict]) -> None:
        actual = sorted(
            self.command(
                "G_PLATFORM_CONTAINERS",
                "docker",
                "ps",
                "--filter",
                f"label={LABEL}",
                "--format",
                "{{.Names}}",
            )
            .decode()
            .splitlines()
        )
        expected = sorted((ENGINE_CONTAINER, KAFKA, CLUSTER + "-registry"))
        require(actual == expected, "G_PURE_PLATFORM_CONTAINERS")
        dbs = self.sql(
            "G_PLATFORM_DATABASES",
            "SELECT datname FROM pg_database WHERE datallowconn ORDER BY datname;",
        ).splitlines()
        require(set(dbs) == {"postgres", "template1", self.database}, "G_PURE_DATABASE_INVENTORY")
        require(not self.journal.exists(), "G_PURE_INGRESS_JOURNAL")
        platform = {
            "revision": sha(canonical({"docker": actual, "databases": dbs})),
            "scanned": ["database", "journal", "relay"],
            "objects": [
                {"kind": "database", "name": self.database.replace("_", "-"), "role": "engine"}
            ],
        }
        self.evidence.write(
            "platform-census.json",
            {"containers": actual, "databases": dbs, "ingress_journals": [], "inventory": platform},
        )
        (self.scratch / "platform.json").write_bytes(canonical(platform))
        (self.scratch / "rendered.json").write_bytes(
            canonical(
                {
                    "kind": "rendered",
                    "observer": "arm-renderer",
                    "revision": self.binding["reset_id"],
                    "components": rendered,
                    "scanned": ["database", "deployment", "journal", "relay", "service"],
                }
            )
        )

    def topic_ids(self) -> dict[str, str]:
        raw = self.command(
            "G_KAFKA_TOPIC_IDS",
            "docker",
            "exec",
            KAFKA,
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "localhost:19092",
            "--describe",
        )
        ids = dict(re.findall(rb"Topic:\s+(\S+)\s+TopicId:\s+(\S+)", raw))
        topics = {name: ids[name.encode()].decode() for name in (TASKS, EVENTS)}
        self.evidence.write("broker-topics.json", topics)
        return topics

    @contextmanager
    def collector(self):
        ready = self.scratch / "watch-ready"
        context = (
            self.command(
                "G_NODE_KUBECONFIG",
                "docker",
                "exec",
                "-e",
                "KUBECONFIG=/etc/kubernetes/admin.conf",
                NODE,
                str(self.harness.scratch / "bin/kubectl"),
                "config",
                "current-context",
            )
            .decode()
            .strip()
        )
        config = {
            "manifest": self.manifest,
            "output": str(self.frames),
            "context": context,
            "worker_selector": "app=worker",
            "worker_container": "worker",
            "receiver": str(self.scratch),
            "registrations": str(self.registrations),
            "brokers": ["kafka:19092"],
            "group": GROUP,
            "topics": self.topic_ids(),
            "members": str(self.members),
            "rendered": str(self.scratch / "rendered.json") if not self.effect else "",
            "platform": str(self.scratch / "platform.json") if not self.effect else "",
            "relay_journal": str(self.journal) if self.effect else "",
            "relay_peer": RELAY_PEER if self.effect else "",
            "sink_peer": SINK_PEER if self.effect else "",
            "allocator": str(self.allocator) if self.effect else "",
            "decisions": str(self.decisions) if self.effect else "",
            "fence": str(self.fence),
            "watch_ready": str(ready),
        }
        path = self.scratch / "collector.json"
        private_file(path, canonical(config))
        logs = self.scratch / "collector.log"
        with logs.open("wb") as diagnostics:
            process = subprocess.Popen(
                (
                    "docker",
                    "exec",
                    "-e",
                    "KUBECONFIG=/etc/kubernetes/admin.conf",
                    "-e",
                    "GOMAXPROCS=2",
                    NODE,
                    "env",
                    "PATH=" + str(self.harness.scratch / "bin") + ":/usr/bin:/bin",
                    str(self.shared / "collector"),
                    str(path),
                ),
                env=self.env,
                stdout=diagnostics,
                stderr=diagnostics,
            )
            try:
                self.collector_process = process
                cursor = self.wait_for(
                    lambda: self.root_read(ready).decode().strip() if ready.is_file() else None,
                    "G_WATCH_NOT_READY",
                    seconds=18,
                )
                self.evidence.write(
                    "watch-start.json", {"cursor": cursor, "observed_before_worker_creation": utc()}
                )
                yield
            finally:
                if process.poll() is None:
                    self.command(
                        "G_COLLECTOR_STOP",
                        "docker",
                        "exec",
                        NODE,
                        "pkill",
                        "-TERM",
                        "-f",
                        "^" + str(self.shared / "collector") + " " + str(path),
                        codes=(0, 1),
                    )
                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                self.evidence.write_bytes("collector.log", logs.read_bytes())

    def coordinator_state(self, worker_names: list[str]) -> dict | None:
        rows = [
            row
            for name in worker_names
            for row in obs_lines(self.logs_for(name), "k6_g_coordinator")
        ]
        assigned = [row for row in rows if row["event"] == "assigned" and row["generation"] >= 0]
        if not assigned:
            return None
        generation = max(row["generation"] for row in assigned)
        latest = {row["consumer_session"]: row for row in rows if row["generation"] == generation}
        members = {
            row["member_id"]: generation for row in latest.values() if row["event"] == "assigned"
        }
        if len(members) != 4:
            return None
        self.coordinator = rows
        self.evidence.write(
            "worker-coordinator.json", {"provenance": "producer-side", "rows": rows}
        )
        state = {"generation": generation, "members": members}
        atomic(self.members, canonical(state))
        return state

    def frame(self, predicate):
        require(self.collector_process.poll() is None, "G_COLLECTOR_EXITED")
        current = self.frames / "current"
        if not current.is_file():
            return None
        folder = self.frames / self.root_read(current).decode().strip()
        return folder if predicate(folder) else None

    def root_read(self, path: Path) -> bytes:
        return self.command("G_READ_ROOT_ARTIFACT", "sudo", "cat", str(path))

    def launch_workers(self, worker: dict) -> tuple[list[str], int]:
        self.create("G_WORKER_DEPLOYMENT", worker)
        initial = self.wait_for(lambda: self.running(2), "G_WORKER_RUNTIME_NOT_READY")
        sa_uid = self.uid(self.namespace, "serviceaccount", WORKER)
        sessions = [self.register(runtime, sa_uid) for runtime in initial]
        if not self.effect:
            # Leave the killed container's checkpoint for the receiver.
            target = initial[0]["pod_name"]
            self.ready("worker")
            self.command(
                "G_RESTART_CONTAINER",
                "docker",
                "exec",
                NODE,
                "crictl",
                "stop",
                "--timeout",
                "0",
                sessions[0]["container_id"],
            )
            after = self.wait_for(
                lambda: self.running(1, name=target, restart=1), "G_WORKER_RESTART_NOT_OBSERVED"
            )
            restarted = self.register(after[0], sa_uid)
            require(
                restarted["pod_uid"] == sessions[0]["pod_uid"]
                and restarted["container_id"] != sessions[0]["container_id"]
                and restarted["container_started_utc"] > sessions[0]["container_started_utc"]
                and restarted["id"] != sessions[0]["id"],
                "G_WORKER_RESTART_IDENTITY_REUSED",
            )
            self.evidence.write("worker-restart.json", {"before": sessions[0], "after": restarted})
        self.ready("worker")
        names = [runtime["pod_name"] for runtime in initial]
        state = self.wait_for(
            lambda: self.coordinator_state(names), "G_COORDINATOR_ASSIGNMENT_MISSING", seconds=70
        )
        self.wait_for(lambda: self.frame(lambda _: True), "G_COLLECTOR_FRAME_MISSING", seconds=20)
        return names, state["generation"]

    def effect_admissions(self, episode: dict):
        reviewer = self.reviewer(self.namespace, REVIEWER)
        token, cutoff, uncertainty = self.warmup(episode, self.admin_dsn)
        credentials = self.go(
            "provision",
            DSN=self.admin_dsn,
            Episode=episode,
            Cutoff=cutoff,
            Uncertainty=uncertainty,
            EngineRoles=[ENGINE_ROLE],
        )
        self.provisioned = True
        reader, decision = credentials["ReaderPassword"], credentials["DecisionPassword"]
        for password in (reader, decision):
            self.evidence.secret(password)
        self.start_witness()
        self.deploy_effect(reviewer, reader, uncertainty, signing_seed=self.seed)
        allocator_config = {
            "SinkURL": f"https://{SINK}:{PORT}",
            "SinkPeer": SINK_PEER,
            "AllocatorTLS": self.engine_tls,
            "WorkerControlTLS": self.cert(CONTROL, "DNS:" + CONTROL, "serverAuth"),
        }
        self.create(
            "G_ALLOCATOR_SECRET",
            obj(
                "Secret",
                "k6-allocator",
                self.namespace,
                data={"config.json": encoded(json_data(allocator_config))},
                immutable=True,
            ),
        )
        jail = self.scratch / "jail"
        jail.mkdir(mode=0o700)
        shutil.copy2(self.approver, jail / "approver")
        dsn = self.dsn("k6_decision_writer", decision, "verify-full", str(self.ca))
        adapter = self.adapter_socket(
            self.scratch,
            jail / "live",
            episode,
            reviewer,
            dsn,
            evidence_config={"Path": str(self.decisions), "Binding": self.binding},
        )
        return jail, token, adapter

    def offers(self, jail: Path, token: str) -> None:
        arrivals = []
        operations = {row["submission_key"]: row for row in self.allocator_plan["operations"]}
        for window in self.public_plan["windows"]:
            [entry] = window["submissions"]
            key = entry["submission_key"]
            sent = utc()
            status, body = (
                self.k(
                    "G_WORKFLOW_SUBMIT",
                    "-n",
                    self.namespace,
                    "exec",
                    "-i",
                    "deployment/" + OBSERVER,
                    "--",
                    "python",
                    "-c",
                    SUBMIT_WIRE,
                    data=self.requests[key],
                )
                .decode()
                .split()
            )
            require(status == "201", "G_SUBMISSION_STATUS_" + status)
            raw = bytes.fromhex(body)
            response = json.loads(raw)
            require(
                response["created"] and response["workflow"]["workflow_id"] == key,
                "G_SUBMISSION_RESPONSE",
            )
            arrivals.append(
                {
                    "window": window["name"],
                    "sequence": 0,
                    "submission_key": key,
                    "disposition": "sent",
                    "unsent_reason": None,
                    "attempts": [
                        {
                            "attempt": 1,
                            "sent_utc": sent,
                            "observed_utc": utc(),
                            "body_sha256": entry["body_sha256"],
                            "request": entry["request"],
                            "response": wire(raw),
                            "outcome": "accepted",
                            "http_status": 201,
                            "workflow_id": key,
                            "payload_hash": response["workflow"]["payload_hash"],
                        }
                    ],
                }
            )
            if self.effect:
                operation = operations[key]

                def pending(key=key):
                    row = self.outcomes().get(key, {})
                    approvals = row.get("approvals", [])
                    return (
                        approvals[0]
                        if len(approvals) == 1 and approvals[0]["decision"] == "PENDING"
                        else None
                    )

                intent = self.wait_for(pending, "G_APPROVAL_INTENT_NOT_PENDING")["intent_id"]
                proposal = {
                    "Namespace": self.namespace,
                    "WorkflowID": key,
                    "NodeID": operation["node_id"],
                    "Target": operation["resource"],
                    "ResourceRevision": "0",
                    "Key": operation["logical_key"],
                    "Arguments": operation["arguments"],
                }
                answer = self.approve(jail, "live", proposal, token)
                require(
                    answer.get("identity", {}).get("intent_id") == intent, "G_APPROVAL_PENDING_READ"
                )
                approved = self.approve(
                    jail, "live", proposal, token, IntentID=intent, Choice="APPROVED"
                )
                require(
                    approved.get("identity", {}).get("decision") == "APPROVED", "G_APPROVAL_WRITE"
                )
        self.arrivals_file = self.scratch / "arrivals.json"
        self.arrivals_file.write_bytes(
            canonical(
                {
                    "schema": "dur053.k6-arrivals.v1",
                    "episode": self.binding["episode"],
                    "reset_id": self.binding["reset_id"],
                    "plan_sha256": self.manifest["plan_sha256"],
                    "removal_utc": None,
                    "sealed_utc": utc(),
                    "rows": arrivals,
                }
            )
        )

        def terminal():
            outcomes = self.outcomes()
            return (
                outcomes
                if len(outcomes) == len(arrivals)
                and all(row["state"] == "SUCCEEDED" for row in outcomes.values())
                else None
            )

        self.evidence.write(
            "producer-result.json",
            {"status": "PASS", "outcomes": self.wait_for(terminal, "G_PRODUCER_TERMINAL")},
        )

    def outcomes(self) -> dict:
        return {
            row["workflow_id"]: row
            for row in json.loads(self.db(SQL.replace(D_NAMESPACE, self.namespace)))
        }

    def stop_roles(self, names: dict[str, str]) -> None:
        if not names:
            return
        self.k(
            "G_STOP_ADMISSION",
            "-n",
            self.namespace,
            "scale",
            *("deployment/" + name for name in names),
            "--replicas=0",
        )
        self.wait_for(
            lambda: (
                {"drained": True}
                if all(not self.pods_for(label) for label in names.values())
                else None
            ),
            "G_ADMISSION_DRAIN_TIMEOUT",
            seconds=50,
        )

    def checker(self) -> None:
        output = self.evidence.root
        config = {
            "schema": "dur053.k6-live-config.v1",
            "database_url": self.admin_dsn,
            "sink_database_url": self.sink_dsn(EXPORTER, self.export_password, self.effect_ca)
            if self.effect
            else "",
            "episode_dir": str(output / "bundle"),
            "manifest": self.manifest,
            "plan_file": str(self.plan_file),
            "arrivals_file": str(self.arrivals_file),
            "external_dir": str(self.frames),
        }
        path = self.scratch / "checker.json"
        private_file(path, canonical(config))
        checker = str(self.shared / "checker")

        def invoke(args: tuple[str, ...], name: str, timeout: int) -> bytes:
            run = subprocess.run(
                args, env=self.env, timeout=timeout, check=False, capture_output=True
            )
            self.evidence.write_bytes(name + ".stdout.json", run.stdout)
            self.evidence.write_bytes(name + ".stderr.txt", run.stderr)
            require(run.returncode == 3, f"G_{name.upper()}_EXIT_{run.returncode}")
            return run.stdout

        live = invoke(("sudo", checker, "-k6-live-v1", str(path)), "live", 140)
        calls = iter(("offline", "missing"))

        def offline(directory: Path) -> bytes:
            return invoke(
                ("sudo", "unshare", "--net", "--", checker, "-k6-offline-v1", str(directory)),
                next(calls),
                20,
            )

        replay(
            output,
            live,
            "witness/effect-ingress.jsonl" if self.effect else "witness/topology.json",
            offline,
        )

    def execute(self, addresses: dict) -> None:
        with self.evidence.step("reset-and-deployment"):
            objects, episode = self.setup(addresses)
            self.worker = next(
                item
                for item in objects
                if item["kind"] == "Deployment" and item["metadata"]["name"] == "worker"
            )
            jail, token, adapter = (
                self.effect_admissions(episode)
                if self.effect
                else (self.scratch, "", nullcontext())
            )
            for item in objects:
                if item["kind"] == "Deployment" and item != self.worker:
                    self.create("G_ENGINE_ROLLOUT", item)
            for name in ROLES:
                if name != "worker":
                    self.ready(PUBLISHER if name == "relay" else name)
        with self.collector():
            with adapter:
                with self.evidence.step("list-watch-runtime-and-registration"):
                    names, generation = self.launch_workers(self.worker)
                with self.evidence.step("deployed-workloads"):
                    self.offers(jail, token)
                    last = self.wait_for(
                        lambda: self.coordinator_state(names),
                        "G_FINAL_COORDINATOR_STATE",
                        seconds=20,
                    )
                    generation = last["generation"]
                with self.evidence.step("stop-worker-relay-and-allocator-admission"):
                    self.stop_roles({"worker": "worker"})
                    stops = {"worker": utc()}
                    self.stop_roles({PUBLISHER: "relay", **({RELAY: RELAY} if self.effect else {})})
                    if self.effect:
                        self.stop_witness()
                    stops["relay"] = utc()
                    self.stop_roles({CONTROL: CONTROL, "scheduler": "scheduler", "api": "api"})
                    stops["allocator"] = utc()
            with self.evidence.step("observe-drained-sources"):
                stops["decision"] = utc()
                atomic(self.members, canonical({"generation": generation, "members": {}}))
                if self.effect:
                    self.evidence.write_bytes(
                        "host-ingress-journal.jsonl", self.journal.read_bytes()
                    )
                    self.evidence.write_bytes(
                        "allocator-journal.jsonl",
                        self.command(
                            "G_ALLOCATOR_RAW", "docker", "exec", NODE, "cat", str(self.allocator)
                        ),
                    )
                    self.evidence.write_bytes("decision-journal.jsonl", self.decisions.read_bytes())
                write_fence(self.fence, self.binding, stops)
                self.evidence.write(
                    "writer-fence.json",
                    {"fence": self.binding, "admission_stopped_utc": stops, "fenced_utc": utc()},
                )

                def fenced(folder):
                    rows = self.root_read(folder / "broker/positions.jsonl").splitlines()
                    return json.loads(rows[-1])["mode"] == "terminal"

                self.wait_for(lambda: self.frame(fenced), "G_TERMINAL_FRAME_MISSING", seconds=30)
            with self.evidence.step("bundle-parity-and-incomplete-export"):
                self.checker()


def run(output: Path) -> int:
    evidence = PhaseGEvidence(make_output(output))
    harness = None
    scratch = None
    try:
        with evidence.step("hosted-engineering-setup"):
            require(
                os.name == "posix"
                and bool(os.getenv("RUNNER_TEMP"))
                and bool(os.getenv("GITHUB_ACTIONS")),
                "HOSTED_LINUX_RUNNER_REQUIRED",
            )
            head = os.environ["DUR053_HEAD_SHA"]
            fields = {}
            for label, revision in (("checkout", "HEAD"), ("source_head", head)):
                commit, tree = (
                    subprocess.check_output(["git", "rev-parse", item], cwd=ROOT, text=True).strip()
                    for item in (revision, revision + "^{tree}")
                )
                fields[label] = {"commit": commit, "tree": tree}
            evidence.write("source.json", fields)
            require(
                fields["checkout"]["tree"] == fields["source_head"]["tree"], "G_SOURCE_TREE_DRIFT"
            )
            scratch = Path(tempfile.mkdtemp(prefix="dur053-k6-g-", dir=os.environ["RUNNER_TEMP"]))
            kind, shared = scratch / "kind", scratch / "bin"
            kind.mkdir()
            shared.mkdir()
            nested = scratch / "kind-evidence"
            nested.mkdir()
            harness = Driver(nested, kind)
            harness.install()
            harness.build()
            for name, addon in LOCK["addons"].items():
                if addon["installed_in_k3a"]:
                    harness.prepare_addon(name)
            config = json.loads((K8S / "kind-cluster.yaml").read_bytes())
            config["nodes"][0]["extraMounts"] = [
                {"hostPath": str(scratch), "containerPath": str(scratch)}
            ]
            kind_config = kind / "phase-g-cluster.json"
            kind_config.write_bytes(canonical(config))
            addresses = harness.cluster(config=kind_config)
            for name, package in (
                ("collector", "dur053-k6-collect"),
                ("checker", "dur049-checker"),
            ):
                harness.command(
                    [
                        "go",
                        "build",
                        "-p",
                        "1",
                        "-trimpath",
                        "-o",
                        str(shared / name),
                        "./cmd/" + package,
                    ],
                    env=dict(harness.env, CGO_ENABLED="0"),
                    timeout=300,
                )
        for cell in select_cells(read_registry()):
            name = cell["episode"]
            with evidence.step("episode-" + name):
                episode_path = output / "episodes" / name
                episode_path.mkdir(parents=True)
                arm_evidence = PhaseGEvidence(episode_path)
                private = scratch / name
                private.mkdir()
                arm = Episode(arm_evidence, private, harness, shared, cell)
                try:
                    if arm.effect:
                        Smoke.build(arm)
                        arm.build_effect()
                    arm.execute(addresses)
                finally:
                    try:
                        arm.teardown()
                    finally:
                        evidence.secrets.update(arm_evidence.secrets)
                        arm_evidence.scan()
                arm_evidence.report.update(status="PASS", reason="DEPLOYED_PARITY_VERIFIED")
                arm_evidence.checkpoint()
                if not arm.effect:
                    harness.command(["docker", "rm", "-f", KAFKA])
                    harness.start_kafka(KAFKA)
                    harness.topics("--create")
                    addresses["192.0.2.2"] = harness.kind_address(KAFKA)
        evidence.report.update(status="PASS", reason="DEPLOYED_PARITY_VERIFIED")
    except (Exception, KeyboardInterrupt) as error:
        evidence.report.update(status="FAIL", reason=evidence.safe_reason(error))
    finally:
        if harness is not None:
            try:
                evidence.write("cleanup.json", harness.cleanup())
            except Exception as error:
                evidence.report.update(status="FAIL", reason=evidence.safe_reason(error))
        if scratch is not None:
            subprocess.run(
                ["sudo", "chown", "-R", f"{os.getuid()}:{os.getgid()}", str(scratch), str(output)],
                check=True,
            )
            shutil.rmtree(scratch)
        evidence.scan()
        evidence.checkpoint()
    print(f"DUR-053 K6 Phase G {evidence.report['status']} {evidence.report['reason']}", flush=True)
    return 0 if evidence.report["status"] == "PASS" else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, required=True)
    raise SystemExit(run(parser.parse_args().output_root))
