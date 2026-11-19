"""Hosted kind engineering smoke for K6 medium-0, medium-1 and medium-4."""

from __future__ import annotations

import copy
import json
import os
import tempfile
import time
from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from dur053_k3 import CLUSTER, Driver
from dur053_k3_policy import DIGEST
from dur053_k4_collectors import Streams, structured_log
from dur053_k4_interventions import ready as k4_ready
from dur053_k6_b_evidence import SmokeFailure
from dur053_k6_b_smoke import json_data, require
from dur053_k6_c_smoke import make_output
from dur053_k6_chaos import (
    Chaos,
    Target,
    backend_selected,
    bind,
    prove_death,
    prove_restoration,
    select_target,
)
from dur053_k6_d2b_smoke import Matrix as D2bMatrix
from dur053_k6_d_render import mount, set_env
from dur053_k6_e1_join import EVENTS, TASKS, broker_records, by, expect, one
from dur053_k6_e1_smoke import (
    DEFINITION,
    OBSERVER_SCRIPT,
    ROLES,
    SUBMIT_TRACED,
    E1Evidence,
    Matrix,
    ensure_no_e1_placeholders,
    workload,
)
from dur053_k6_e2_smoke import pod_state, rendered_strings
from dur053_k6_f1a_inputs import (
    CONTROL_URL,
    chain_counters,
    first_fenced_transition,
    require_drop_rise,
    require_stale,
    route_rows,
)
from dur053_k6_identity import obj
from dur053_k6_registry import read_registry

JsonObject = dict[str, Any]

CELLS = ("medium-0", "medium-1", "medium-4")
SOURCE_NAMESPACE = "dur053-kind"
SCHEDULER_HOLD_MS = "4000"  # Below the scheduler's five-second iteration deadline.

READ_SEALS = (
    OBSERVER_SCRIPT
    + r"""
from pathlib import Path
from workers.k4_witness import read_sealed_session

def read():
    try:
        paths = sorted(Path("/witness").glob("*/*.inventory.json"))
        result = [read_sealed_session(path) for path in paths]
    except (OSError, ValueError, KeyError):
        fail("F1A_WITNESS_SEAL_INVALID")
    for session in result:
        for stream in session["streams"].values():
            stream.pop("unacknowledged_tail", None)
    print(json.dumps(result))

run(read, "F1A_WITNESS_UNEXPECTED")
"""
)

CONTROL_PROBE = (
    OBSERVER_SCRIPT
    + r"""
import http.client
import os
import socket
from urllib.parse import urlsplit

def probe():
    url = os.environ.get("CONTROL_API_URL")
    if url != sys.argv[1]:
        fail("F1A_PROBE_CONTROL_ROUTE")
    endpoint = urlsplit(url)
    try:
        addresses = sorted({r[4][0] for r in
                            socket.getaddrinfo(endpoint.hostname, endpoint.port,
                                               socket.AF_INET, socket.SOCK_STREAM)})
    except OSError:
        fail("F1A_PROBE_SERVICE_DNS")
    connection = http.client.HTTPConnection(endpoint.hostname, endpoint.port, timeout=2)
    result = {"url": url, "dns": addresses, "status": 0, "peer": None}
    try:
        connection.connect()
        result["peer"] = connection.sock.getpeername()[0]
        connection.request("GET", "/healthz")
        result["status"] = connection.getresponse().status
    except (OSError, http.client.HTTPException):
        result["failure"] = "F1A_PROBE_HTTP_UNAVAILABLE"
    finally:
        connection.close()
    print(json.dumps(result))

run(probe, "F1A_PROBE_UNEXPECTED")
"""
)

RELEASE_ACTIVITY = (
    OBSERVER_SCRIPT
    + r"""
import re
from pathlib import Path

def release():
    invocation, = sys.argv[1:]
    if invocation and not re.fullmatch(r"[0-9a-f-]{36}", invocation):
        fail("F1A_RELEASE_INVOCATION_INVALID")
    suffix = "-" + invocation if invocation else ""
    try:
        Path("/witness/f1a-release" + suffix).touch(exist_ok=False)
    except OSError:
        fail("F1A_RELEASE_MARKER_WRITE")
    print(json.dumps({"released": True}))

run(release, "F1A_RELEASE_UNEXPECTED")
"""
)


class F1aEvidence(E1Evidence):
    schema = "dur053.k6-phase-f1a-smoke.v1"


def rewrite_namespace(item: JsonObject, namespace: str) -> None:
    metadata = item["metadata"]
    if item["kind"] == "Namespace" and metadata["name"] == SOURCE_NAMESPACE:
        metadata["name"] = namespace
    if metadata.get("namespace") == SOURCE_NAMESPACE:
        metadata["namespace"] = namespace
    for subject in item.get("subjects", []):
        if subject.get("namespace") == SOURCE_NAMESPACE:
            subject["namespace"] = namespace
    if item["kind"] in {"Deployment", "Job"}:
        pod = item["spec"]["template"]["spec"]
        for container in pod.get("containers", []) + pod.get("initContainers", []):
            for env in container.get("env", []):
                if (
                    env["name"] in {"RUNTIME_NAMESPACE", "RUNTIME_TOKEN_NAMESPACE"}
                    and env.get("value") == SOURCE_NAMESPACE
                ):
                    env["value"] = namespace


def audit_topology(objects: list[JsonObject], pushed: set[str]) -> None:
    ensure_no_e1_placeholders(objects)
    for item in objects:
        location = f"{item['kind']}/{item['metadata']['name']}"
        for path, value in rendered_strings(item):  # type: ignore[no-untyped-call]
            if path[-1] == "image":
                require(
                    value in pushed and bool(DIGEST.fullmatch(value)),
                    f"F1A_IMAGE_NOT_PUSHED {location}/{path}",
                )
            else:
                require(
                    SOURCE_NAMESPACE not in value and "UNBOUND" not in value,
                    f"F1A_NAMESPACE_OR_PLACEHOLDER_UNREWRITTEN {location}/{path}",
                )


def deployment_objects(
    source: list[JsonObject],
    namespace: str,
    tags: dict[str, str],
    ordinary: dict[str, str],
    *,
    scheduler_hold: bool = False,
) -> list[JsonObject]:
    result = []
    for item in copy.deepcopy(source):
        kind, name = item["kind"], item["metadata"]["name"]
        if kind in {"ScaledObject", "TriggerAuthentication", "PodDisruptionBudget"}:
            continue
        if kind == "Deployment" and name not in ROLES:
            continue
        rewrite_namespace(item, namespace)
        if kind in {"ClusterRole", "ClusterRoleBinding"}:
            item["metadata"]["name"] = namespace + "-tokenreview"
        if kind == "ClusterRoleBinding":
            item["roleRef"]["name"] = namespace + "-tokenreview"
            item["subjects"] = [row for row in item["subjects"] if row["name"] in ROLES]
        if kind == "Service" and name == "worker-control":
            # Chaos Mesh matches the backend Pod IPs in the worker's namespace.
            item["spec"]["clusterIP"] = "None"
        if kind == "Deployment":
            deployment = item["spec"]
            pod = deployment["template"]["spec"]
            container = pod["containers"][0]
            if name in {"worker", "scheduler", "worker-control"}:
                container["image"] = tags["worker" if name == "worker" else "runtime"]
                set_env(container, DUR053_K4_CAMPAIGN="1")
                for variable in ("POD_UID", "CONTAINER_ID"):
                    if not by(container["env"], name=variable):
                        container["env"].append(
                            {
                                "name": variable,
                                "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}},
                            }
                        )
            if name == "worker":
                deployment["replicas"] = 2
                set_env(
                    container,
                    RUNTIME_NAMESPACE=namespace,
                    DUR053_K6_CHAOS_SMOKE="1",
                    WORKER_SLOTS="2",
                )
            if name == "scheduler":
                deployment["replicas"] = 2
                if scheduler_hold:
                    set_env(container, RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS=SCHEDULER_HOLD_MS)
            if name == "observer":
                mount(
                    pod, "witness", "/witness", {"persistentVolumeClaim": {"claimName": "witness"}}
                )
        result.append(item)
    audit_topology(result, {*tags.values(), *ordinary.values()})
    return result


def poll[T](check: Callable[[], tuple[T | None, Any]], stage: str, deadline: float) -> T:
    observed = None
    while time.monotonic() < deadline:
        result, observed = check()
        if result is not None and time.monotonic() < deadline:
            return result
        time.sleep(0.3)
    raise SmokeFailure(f"{stage} observed={observed!r} expected=confirmed_before_deadline")


def ready(pod: JsonObject) -> bool:
    return bool(k4_ready(pod))  # type: ignore[no-untyped-call]


def ns(timestamp: str) -> int:
    return int(datetime.fromisoformat(timestamp).timestamp() * 1e9)


class F1aSmoke(Matrix):
    inputs_schema = "dur053.k6-phase-f1a-inputs.v1"
    image_repository = "dur053-k6-f1a"

    def __init__(
        self, evidence: F1aEvidence, scratch: Path, harness: Driver, addresses: dict[str, str]
    ) -> None:
        super().__init__(evidence, scratch, harness, addresses)  # type: ignore[no-untyped-call]
        self.deadline = datetime.now(UTC) + timedelta(minutes=15)
        self.cell_id = "setup"
        self.namespace = ""
        self.database = ""
        self.database_started = self.namespace_started = False
        self.schema_dir = scratch / "schemas"
        self.schema_dir.mkdir()
        self.crds: list[JsonObject] = []
        self.streams: Any = None
        self.chaos_run: Chaos | None = None

    def retain(self, label: str, data: Any) -> None:
        self.evidence.write(f"{self.cell_id}-{label}.json", data)

    def backend(self) -> None:
        import yaml  # type: ignore[import-untyped]

        try:
            D2bMatrix.chaos(self)  # type: ignore[arg-type]
        except Exception:
            self.collect_role_logs("chaos-mesh", ("chaos-controller-manager", "chaos-daemon"))
            raise
        self.crds = list(yaml.safe_load_all((self.scratch / "chaos/chaos-crds.yaml").read_bytes()))
        self.crds = [row for row in self.crds if row]

    def pg(self, query: str) -> str:
        return (
            self.command(
                "F1A_DB",
                "docker",
                "exec",
                "-i",
                CLUSTER + "-postgres",
                "psql",
                "-X",
                "-q",
                "-v",
                "ON_ERROR_STOP=1",
                "-At",
                "-U",
                "durable",
                "-d",
                self.database,
                data=query.encode(),
                timeout=6,
            )
            .decode()
            .strip()
        )

    def pods(self, role: str) -> list[JsonObject]:
        return cast(
            list[JsonObject], self.objects("F1A_PODS", "pods", "-l", f"app={role}")["items"]
        )

    def objects(self, stage: str, *args: str) -> JsonObject:
        result: JsonObject = json.loads(
            self.k(stage, "-n", self.namespace, "get", *args, "-o", "json", timeout=6)
        )
        return result

    def exec_python(
        self, stage: str, target: str, script: str, *args: str, data: bytes | None = None
    ) -> Any:
        raw = self.k(
            f"F1A_{stage}",
            "-n",
            self.namespace,
            "exec",
            "--quiet",
            "-i",
            target,
            "--",
            "python",
            "-c",
            script,
            *args,
            data=data,
            timeout=9,
            kubectl_exec=True,
        )
        try:
            return json.loads(raw)
        except ValueError:
            raise SmokeFailure(f"F1A_{stage}_OUTPUT_INVALID") from None

    def rollout(self, role: str) -> None:
        try:
            self.k(
                "F1A_ROLLOUT_" + role.upper().replace("-", "_"),
                "-n",
                self.namespace,
                "rollout",
                "status",
                f"deployment/{role}",
                "--timeout=120s",
                timeout=125,
            )
        except SmokeFailure as error:
            diagnostic: JsonObject = {"role": role, "reason": self.evidence.safe_reason(error)}
            try:
                diagnostic["pods"] = [pod_state(p) for p in self.pods(role)]
            except (SmokeFailure, KeyError, TypeError, ValueError) as listing_error:
                diagnostic["pod_list_error"] = self.evidence.safe_reason(listing_error)
            self.retain("rollout-failure", diagnostic)
            raise

    def setup_episode(self) -> None:
        renderer = copy.copy(self.harness)
        renderer.scratch = renderer.output = self.scratch / self.cell_id
        renderer.scratch.mkdir()
        _, source = renderer.render("kind", self.addresses)
        objects = deployment_objects(
            source,
            self.namespace,
            self.tags,
            self.harness.images,
            scheduler_hold=self.cell_id == "medium-1",
        )
        self.retain("topology", objects)
        renderer.reset_transport()
        for topic in (EVENTS, TASKS):
            sample = self.kafka(topic)
            self.retain("kafka-initial-" + topic, sample)
            expect("F1A_FRESH_KAFKA_TOPIC", broker_records(sample["value"], 8), [])
        self.create("F1A_NAMESPACE", obj("Namespace", self.namespace, None))
        self.namespace_started = True
        self.sql("F1A_DB_CREATE", f'CREATE DATABASE "{self.database}";')
        self.database_started = True
        self.harness.databases.add(self.database)
        dsn = (
            f"postgresql://durable:{self.harness.password}@postgres:5432/"
            f"{self.database}?sslmode=disable"
        )
        self.create(
            "F1A_DB_SECRET", obj("Secret", "campaign-db", self.namespace, stringData={"url": dsn})
        )
        self.create(
            "F1A_INFRASTRUCTURE",
            {
                "apiVersion": "v1",
                "kind": "List",
                "items": [
                    row for row in objects if row["kind"] not in {"Namespace", "Deployment", "Job"}
                ],
            },
        )
        for name, ip, port in (
            ("postgres", self.engine_address, 5432),
            ("kafka", self.broker, 19092),
        ):
            self.create(
                "F1A_ENDPOINT",
                obj(
                    "Endpoints",
                    name,
                    self.namespace,
                    subsets=[{"addresses": [{"ip": ip}], "ports": [{"name": "tcp", "port": port}]}],
                ),
            )
        for name in ("migrate", "compatibility"):
            self.create(
                "F1A_JOB_" + name.upper(),
                one(
                    "F1A_RENDERED_JOB",
                    [o for o in objects if o["kind"] == "Job" and o["metadata"]["name"] == name],
                ),
            )
            try:
                self.k(
                    "F1A_JOB_" + name.upper(),
                    "-n",
                    self.namespace,
                    "wait",
                    "--for=condition=complete",
                    f"job/{name}",
                    "--timeout=120s",
                    timeout=125,
                )
            except SmokeFailure:
                self.retain(
                    "job-failure",
                    {
                        "job": name,
                        "pods": [
                            pod_state(p)
                            for p in self.objects("F1A_JOB_PODS", "pods", "-l", f"job-name={name}")[
                                "items"
                            ]
                        ],
                    },
                )
                raise
        self.pg(f"""
            INSERT INTO engine.workflow_definitions
              (definition_id, version, definition_hash, graph, activity_versions, effect_classes)
            VALUES ('{DEFINITION}', 1, 'f1a-pure-echo-v1',
              '{{"entry":"pure.echo","nodes":[{{"id":"pure.echo","kind":"activity","next":"done"}},
                {{"id":"done","kind":"success"}}]}}',
              '{{"pure.echo":"v1"}}', '{{"pure.echo":"PURE_ACTIVITY"}}');
        """)
        counts = json.loads(
            self.pg("""
            BEGIN READ ONLY;
            SELECT jsonb_build_object(
              'workflows', (SELECT count(*) FROM engine.workflow_executions),
              'attempts', (SELECT count(*) FROM engine.activity_attempts),
              'outbox', (SELECT count(*) FROM engine.outbox),
              'offsets', (SELECT count(*) FROM engine.consumer_offsets));
            COMMIT;
        """)
        )
        self.retain("initial-engine", counts)
        expect(
            "F1A_FRESH_ENGINE",
            counts,
            dict.fromkeys(("workflows", "attempts", "outbox", "offsets"), 0),
        )
        self.create(
            "F1A_DEPLOYMENTS",
            {
                "apiVersion": "v1",
                "kind": "List",
                "items": [row for row in objects if row["kind"] == "Deployment"],
            },
        )
        for role in ROLES:
            self.rollout(role)
        for role in ("scheduler", "worker"):
            pods = self.pods(role)
            expect("F1A_TOPOLOGY_" + role.upper(), [ready(p) for p in pods], [True, True])
        for kind in ("podchaos", "networkchaos"):
            expect("F1A_FRESH_CHAOS", self.objects("F1A_FRESH_CHAOS", kind)["items"], [])

    def offer_episode(self) -> tuple[str, str]:
        payload, fingerprint, traceparent = workload(self.cell_id)
        payload["namespace"] = self.namespace
        self.retain(
            "offered", {"request": payload, "fingerprint": fingerprint, "at_ns": time.time_ns()}
        )
        receipt = self.exec_python(
            "SUBMIT",
            "deployment/observer",
            SUBMIT_TRACED,
            traceparent,
            "/identity/token",
            "api:8080",
            data=json_data(payload),
        )
        self.retain("accepted", {"response": receipt, "at_ns": time.time_ns()})
        workflow = payload["workflow_id"]
        expect(
            "F1A_SUBMISSION_RECEIPT",
            receipt,
            {"status": 201, "workflow_id": workflow, "created": True},
        )
        return workflow, fingerprint

    def cut(self, workflow: str) -> JsonObject:
        result = self.snapshot(workflow)
        for entry in result["audit"]:
            entry["observed_ns"] = ns(entry["finalized_at"])
        return result

    def witness(self) -> tuple[list[JsonObject], list[JsonObject]]:
        seals = self.exec_python("WITNESS", "deployment/observer", READ_SEALS)
        observed: list[JsonObject] = []
        for bundle in seals:
            seal = bundle["inventory"]
            expect("F1A_WITNESS_COMPLETE", (seal["complete"], seal["errors"]), (True, 0))
            observed.extend(
                {
                    **json.loads(line),
                    "stream": key,
                    "session": seal["session"],
                    "pod_uid": seal["pod_uid"],
                }
                for key, rows in bundle["streams"].items()
                for line in rows["data"].splitlines()
            )
        return observed, seals

    def proc_start(self, node: str, pid: int) -> str | None:
        try:
            raw = (
                self.command(
                    "F1A_PROCESS_STAT",
                    "docker",
                    "exec",
                    node,
                    "cat",
                    f"/proc/{pid}/stat",
                    timeout=4,
                )
                .decode()
                .strip()
            )
        except SmokeFailure:
            listing = (
                self.command(
                    "F1A_PROCESS_INVENTORY", "docker", "exec", node, "ls", "-1", "/proc", timeout=4
                )
                .decode()
                .splitlines()
            )
            if str(pid) not in listing:
                return None
            raise
        fields = raw.rsplit(")", 1)[-1].split()
        expect("F1A_PROCESS_STAT_FORMAT", len(fields) > 19, True)
        return "1:" + fields[19]

    def identity(self, role: str, pod: JsonObject, session: str) -> Target:
        name = pod["metadata"]["name"]
        uid = pod["metadata"]["uid"]
        node = pod["spec"]["nodeName"]
        row = one("F1A_WORKLOAD_CONTAINER", by(pod["status"]["containerStatuses"], name=role))
        container_id = row["containerID"]
        expect(
            "F1A_TARGET_READY", ready(pod) and not pod["metadata"].get("deletionTimestamp"), True
        )
        expect("F1A_TARGET_CONTAINERD", container_id.startswith("containerd://"), True)
        image = self.tags["worker" if role == "worker" else "runtime"]
        expect("F1A_TARGET_IMAGE", row["imageID"].rsplit("@", 1)[-1], image.rsplit("@", 1)[-1])
        inspected = json.loads(
            self.command(
                "F1A_CRI_INSPECT",
                "docker",
                "exec",
                node,
                "crictl",
                "inspect",
                container_id.split("://")[1],
                timeout=4,
            )
        )
        expect("F1A_CRI_CONTAINER", inspected["status"]["id"], container_id.split("://")[1])
        expect("F1A_CRI_POD_UID", inspected["status"]["labels"].get("io.kubernetes.pod.uid"), uid)
        pid = inspected["info"]["pid"]
        expect("F1A_CRI_PID", type(pid) is int and pid > 0, True)
        process = self.proc_start(node, pid)
        require(process is not None, "F1A_TARGET_NO_PROCESS observed=absent expected=alive")
        assert process is not None
        return Target(
            self.namespace,
            name,
            uid,
            role,
            row["name"],
            container_id,
            row["restartCount"],
            node,
            pid,
            process,
            session,
        )

    def reobserve(self, target: Target) -> Target:
        return self.identity(
            target.role, self.objects("F1A_RECHECK_TARGET", "pod", target.pod), target.session
        )

    def active_worker(self, workflow: str, *, heartbeat: bool) -> tuple[list[Target], JsonObject]:
        def active() -> tuple[tuple[list[Target], JsonObject] | None, Any]:
            cut = self.cut(workflow)
            rows, seals = self.witness()
            attempts = by(cut["attempts"], is_current=True, state="CLAIMED", attempt_number=1)
            pods = {p["metadata"]["uid"]: p for p in self.pods("worker") if ready(p)}
            matches, witnesses = [], {}
            for attempt in attempts:
                for entry in by(
                    rows,
                    event="f1a_activity_wait",
                    workflow=workflow,
                    token_hash=attempt["token_hash"],
                ):
                    if entry["pod_uid"] not in pods:
                        continue
                    if not any(
                        row["event"] == "start"
                        and row.get("id") == entry["invocation"]
                        and row.get("session") == entry["session"]
                        for row in rows
                    ):
                        continue
                    if by(rows, event="f1a_activity_release", invocation=entry["invocation"]):
                        continue
                    pod = pods[entry["pod_uid"]]
                    if entry["worker"] != attempt["worker_id"] or not entry["worker"].startswith(
                        pod["metadata"]["name"] + "-"
                    ):
                        continue
                    if entry["pod_uid"] in witnesses:
                        continue
                    target = self.identity("worker", pod, entry["session"])
                    routes = route_rows(rows, target, workflow, attempt=1)
                    claim = by(
                        cut["claims"], token_hash=entry["token_hash"], worker_id=entry["worker"]
                    )
                    if not claim or (
                        heartbeat and not by(routes, operation="heartbeat", status=200)
                    ):
                        continue
                    expect(
                        "F1A_ACTIVE_CLAIM_ROUTE",
                        bool(by(routes, operation="claim", status=200)),
                        True,
                    )
                    matches.append(target)
                    witnesses[target.uid] = {
                        "invocation": entry,
                        "claim": claim[0],
                        "attempt": attempt,
                        "routes": routes,
                    }
            if not matches:
                return None, {
                    "attempts": attempts,
                    "worker_uids": list(pods),
                    "witness_sessions": len(seals),
                }
            self.retain(
                "active-worker",
                {"cut": cut, "seals": seals, "candidates": list(witnesses.values())},
            )
            return (matches, witnesses), None

        return poll(active, "F1A_WARMUP_ACTIVE_WORKER", time.monotonic() + 32)

    def active_scheduler(self, workflow: str) -> tuple[list[Target], JsonObject]:
        def active() -> tuple[tuple[list[Target], JsonObject] | None, Any]:
            cut = self.cut(workflow)
            leases = [row for row in cut["leases"] if row["live"] and row["owner_id"]]
            leases = [row for row in leases if ns(row["lease_expires_at"]) > time.time_ns()]
            if not leases:
                return None, cut["leases"]
            lease = one("F1A_CURRENT_LEASE", leases)
            acquiring = by(
                cut["audit"], kind="acquire", owner_id=lease["owner_id"], epoch=lease["epoch"]
            )
            if not acquiring:
                return None, lease
            entry = one("F1A_ACTIVE_SCHEDULER_ACQUIRE", acquiring)
            pods = [
                p
                for p in self.pods("scheduler")
                if ready(p) and p["metadata"]["uid"] == entry["pod_uid"]
            ]
            if not pods:
                return None, entry["pod_uid"]
            return ([self.identity("scheduler", pods[0], entry["session_id"])], cut), None

        return poll(active, "F1A_ACTIVE_SCHEDULER", time.monotonic() + 16)

    def ensure_scheduler_lease(self, target: Target, workflow: str) -> JsonObject:
        def active() -> tuple[JsonObject | None, Any]:
            cut = self.cut(workflow)
            active_leases = [
                lease
                for lease in cut["leases"]
                if lease["live"]
                and by(
                    cut["audit"],
                    kind="acquire",
                    owner_id=lease["owner_id"],
                    epoch=lease["epoch"],
                    pod_uid=target.uid,
                    session_id=target.session,
                )
            ]
            if not active_leases:
                return None, cut["leases"]
            lease = active_leases[0]
            acquired = self.scheduler_acquisition(target, lease)
            if not acquired:
                return None, lease
            self.retain("scheduler-acquisition", acquired)
            return cut, None

        return poll(active, "F1A_BOUND_SCHEDULER_LEASE", time.monotonic() + 16)

    def role_log(self, pod: str, role: str, *, previous: bool = False) -> bytes:
        raw = self.k(
            "F1A_ROLE_LOG",
            "-n",
            self.namespace,
            "logs",
            pod,
            "-c",
            role,
            "--timestamps",
            *(("--previous",) if previous else ()),
            timeout=6,
        )
        self.evidence.check(raw)
        return raw

    def scheduler_acquisition(self, target: Target, lease: JsonObject) -> JsonObject | None:
        raw = self.role_log(target.pod, "scheduler")
        for line in raw.decode().splitlines():
            entry = structured_log(line)  # type: ignore[no-untyped-call]
            if (
                entry
                and entry.get("event") == "scheduler_acquisition"
                and entry["boundary"] == "after_lease_acquired_hold_enter"
                and entry["pod_uid"] == target.uid
                and entry["session"] == target.session
                and entry["owner"] == lease["owner_id"]
                and entry["epoch"] == lease["epoch"]
                and entry["partition"] == lease["partition_id"]
            ):
                expect(
                    "F1A_SCHEDULER_ACQUISITION_PID", str(entry["pid"]), target.process.split(":")[0]
                )
                return cast(JsonObject, entry)
        return None

    def collect_role_logs(self, namespace: str, roles: tuple[str, ...]) -> None:
        failures = {}
        pods = json.loads(
            self.k("F1A_DIAGNOSTIC_PODS", "-n", namespace, "get", "pods", "-o", "json", timeout=6)
        )["items"]
        for pod in pods:
            name = pod["metadata"]["name"]
            for status in pod.get("status", {}).get("containerStatuses", []):
                container = status["name"]
                if container not in roles and namespace != "chaos-mesh":
                    continue
                for previous in (False, True) if status["restartCount"] else (False,):
                    label = f"{self.cell_id}-{name}-{container}" + ("-previous" if previous else "")
                    try:
                        logs = self.k(
                            "F1A_DIAGNOSTIC_LOG",
                            "-n",
                            namespace,
                            "logs",
                            name,
                            "-c",
                            container,
                            *(("--previous",) if previous else ()),
                            timeout=6,
                        )
                        self.evidence.write_bytes(label + ".log", logs)
                    except SmokeFailure as error:
                        failures[label] = {
                            "reason": self.evidence.safe_reason(error),
                            "pod": pod_state(pod),
                        }
        self.retain(
            f"{namespace}-log-diagnostics",
            {
                "failures": failures,
                "pods": [pod_state(p) for p in pods],
            },
        )
        expect("F1A_ROLE_LOG_FAILURES", list(failures), [])

    def clear_episode(self) -> None:
        with ExitStack() as cleanup:
            if self.database_started:
                cleanup.callback(self.harness.drop, self.database)
            if self.namespace_started:
                cleanup.callback(
                    self.k,
                    "F1A_RBAC_CLEANUP",
                    "delete",
                    f"clusterrole/{self.namespace}-tokenreview",
                    f"clusterrolebinding/{self.namespace}-tokenreview",
                    "--ignore-not-found",
                )
                cleanup.callback(
                    self.k,
                    "F1A_NAMESPACE_CLEANUP",
                    "delete",
                    "namespace",
                    self.namespace,
                    "--ignore-not-found",
                    "--timeout=90s",
                    timeout=95,
                )
            if self.chaos_run is not None and self.chaos_run.possibly_created:
                cleanup.callback(self.chaos_run.remove)
        self.namespace_started = self.database_started = False

    def cell_setup(self, cell: str) -> None:
        self.cell_id = cell
        self.namespace = "dur053-k6-f1a-" + cell
        self.database = self.namespace.replace("-", "_")
        self.chaos_run = None
        self.setup_episode()

    def targets_behind_service(self) -> list[dict[str, str]]:
        pods = {pod["metadata"]["uid"]: pod for pod in self.pods("worker-control") if ready(pod)}
        service = self.objects("F1A_CONTROL_SERVICE", "service", "worker-control")
        expect("F1A_CONTROL_HEADLESS", service["spec"]["clusterIP"], "None")
        expect("F1A_CONTROL_PORT", [p["port"] for p in service["spec"]["ports"]], [8080])
        slices = self.objects(
            "F1A_CONTROL_ENDPOINTS",
            "endpointslices",
            "-l",
            "kubernetes.io/service-name=worker-control",
        )["items"]
        endpoints = []
        for entry in slices:
            expect(
                "F1A_CONTROL_ENDPOINT_OWNER",
                service["metadata"]["uid"]
                in [row["uid"] for row in entry["metadata"]["ownerReferences"]],
                True,
            )
            if entry["addressType"] != "IPv4":
                continue
            for endpoint in entry["endpoints"]:
                ref = endpoint["targetRef"]
                expect("F1A_CONTROL_ENDPOINT_READY", endpoint["conditions"].get("ready"), True)
                expect("F1A_CONTROL_ENDPOINT_UID", ref["uid"] in pods, True)
                pod = pods[ref["uid"]]
                expect(
                    "F1A_CONTROL_ENDPOINT_REF",
                    (ref["name"], ref["namespace"]),
                    (pod["metadata"]["name"], self.namespace),
                )
                expect("F1A_CONTROL_ENDPOINT_IP", endpoint["addresses"], [pod["status"]["podIP"]])
                endpoints.append(
                    {"pod": ref["name"], "uid": ref["uid"], "ip": pod["status"]["podIP"]}
                )
        expect(
            "F1A_CONTROL_ENDPOINT_INVENTORY", sorted(row["uid"] for row in endpoints), sorted(pods)
        )
        expect("F1A_CONTROL_ENDPOINT_COUNT", len(endpoints), 2)
        return sorted(endpoints, key=lambda row: row["uid"])

    def connectivity(self, target: Target, endpoints: list[dict[str, str]]) -> JsonObject:
        probe = self.exec_python("CONNECTIVITY", target.pod, CONTROL_PROBE, CONTROL_URL)
        ips = sorted(row["ip"] for row in endpoints)
        expect("F1A_CONTROL_DNS_ENDPOINTS", probe["dns"], ips)
        if probe["status"]:
            expect("F1A_PROBE_PEER", probe["peer"] in ips, True)
        observed = self.reobserve(target)
        bind(target, observed)
        return {**probe, "at_ns": time.time_ns(), "target": asdict(observed)}

    def watch_kill(self, target: Target, chaos: Chaos) -> JsonObject:
        issued_ns = chaos.issued_ns
        assert issued_ns is not None

        def dead() -> tuple[JsonObject | None, Any]:
            self.streams.check()
            with self.streams.lock:
                exits = list(self.streams.cri_exits)
            process = self.proc_start(target.node, target.host_pid)
            old_pods = [p for p in self.pods(target.role) if p["metadata"]["uid"] == target.uid]
            if process == target.process or (target.role == "worker" and old_pods):
                return None, {"old_process": process, "old_uid_present": bool(old_pods)}
            cid = target.container_id
            matches = [row for row in exits if row["container_id"] == cid.split("://")[1]]
            if not matches and time.time_ns() - issued_ns < 8e9:
                return None, {"process": process, "cri_exit": "waiting"}
            proof = prove_death(target, issued_ns, process, exits)
            self.retain(
                "process-death",
                {
                    **proof,
                    "at_ns": time.time_ns(),
                    "old_uid_present": bool(old_pods),
                    "old_pod": pod_state(old_pods[0]) if old_pods else None,
                },
            )
            return proof, None

        return poll(dead, "F1A_KILL_DEATH", time.monotonic() + 18)

    def ready_replacement(self, target: Target, before_uids: set[str], absent_ns: int) -> Target:
        def healthy() -> tuple[Target | None, Any]:
            pods = self.pods(target.role)
            for pod in pods:
                if not ready(pod) or pod["metadata"].get("deletionTimestamp"):
                    continue
                uid = pod["metadata"]["uid"]
                if uid in before_uids and uid != target.uid:
                    continue
                row = one(
                    "F1A_REPLACEMENT_CONTAINER",
                    by(pod["status"]["containerStatuses"], name=target.container),
                )
                if row["containerID"] == target.container_id:
                    continue
                if uid == target.uid:
                    expect("F1A_RESTART_COUNT", row["restartCount"], target.restart_count + 1)
                    terminated = row.get("lastState", {}).get("terminated", {})
                    if terminated:
                        expect(
                            "F1A_TERMINATED_CONTAINER",
                            terminated.get("containerID"),
                            target.container_id,
                        )
                        expect(
                            "F1A_TERMINATION_SIGNAL",
                            terminated.get("exitCode") == 137 or terminated.get("signal") == 9,
                            True,
                        )
                    self.retain("container-termination", terminated)
                replacement = self.identity(target.role, pod, "")
                expect(
                    "F1A_REPLACEMENT_PROCESS_CHANGED", replacement.process != target.process, True
                )
                expect(
                    "F1A_REPLACEMENT_UID_RULE", target.role != "worker" or uid != target.uid, True
                )
                proof = {"target": asdict(replacement), "at_ns": time.time_ns(), "ready": True}
                prove_restoration(absent_ns, proof, {"ready": True})
                if target.role == "worker":
                    expect(
                        "F1A_OLD_UID_ABSENT_AFTER_REMOVE",
                        target.uid in [p["metadata"]["uid"] for p in pods],
                        False,
                    )
                self.retain("ready-replacement", proof)
                return replacement, None
            return None, [pod_state(p) for p in pods]

        return poll(healthy, "F1A_READY_REPLACEMENT", time.monotonic() + 65)

    def scheduler_reconnect(self, replacement: Target, absent_ns: int) -> None:
        service = self.objects("F1A_POSTGRES_SERVICE", "service", "postgres")
        service_peer = f"{service['spec']['clusterIP']}:5432"
        endpoints = self.objects("F1A_POSTGRES_ENDPOINT", "endpoints", "postgres")
        expect(
            "F1A_SCHEDULER_ENGINE_ENDPOINT",
            [a["ip"] for subset in endpoints["subsets"] for a in subset["addresses"]],
            [self.engine_address],
        )

        def connections() -> tuple[JsonObject | None, Any]:
            raw = self.command(
                "F1A_SCHEDULER_CONNECTIONS",
                "docker",
                "exec",
                replacement.node,
                "nsenter",
                "-t",
                str(replacement.host_pid),
                "-n",
                "ss",
                "-Htnp",
                "state",
                "established",
                timeout=5,
            ).decode()
            matches = [
                line
                for line in raw.splitlines()
                if service_peer in line.split() and f"pid={replacement.host_pid}," in line
            ]
            proof = {
                "at_ns": time.time_ns(),
                "postgres_connections": matches,
                "connected": bool(matches),
                "service_peer": service_peer,
                "engine_address": self.engine_address,
            }
            if not matches:
                return None, {"established": len(raw.splitlines()), "matching": 0}
            bind(replacement, self.reobserve(replacement))
            prove_restoration(absent_ns, proof, {"connected": True})
            self.retain("scheduler-restoration", proof)
            return proof, None

        poll(connections, "F1A_SCHEDULER_RECONNECT", time.monotonic() + 20)

    def partition_chain(self, chaos: Chaos, target: Target) -> str:
        sources = self.objects("F1A_POD_NETWORK", "podnetworkchaos", target.pod)
        found = [
            row
            for row in sources["spec"]["iptables"]
            if row["source"] == f"{self.namespace}/{chaos.name}" and row["direction"] == "output"
        ]
        chain: str = one("F1A_CHAOS_DROP_CHAIN", found)["name"]
        self.retain("pod-network-chaos", sources)
        return chain

    def counters(self, target: Target, chain: str) -> dict[str, int] | None:
        daemon = json.loads(
            self.k(
                "F1A_CHAOS_DAEMON",
                "-n",
                "chaos-mesh",
                "get",
                "pods",
                "--field-selector",
                f"spec.nodeName={target.node}",
                "-o",
                "json",
                timeout=5,
            )
        )["items"]
        pod = one(
            "F1A_CHAOS_DAEMON_ON_NODE",
            [p for p in daemon if p["metadata"]["name"].startswith("chaos-daemon-") and ready(p)],
        )
        raw = self.k(
            "F1A_DROP_COUNTERS",
            "-n",
            "chaos-mesh",
            "exec",
            "--quiet",
            pod["metadata"]["name"],
            "-c",
            "chaos-daemon",
            "--",
            "nsenter",
            "-t",
            str(target.host_pid),
            "-n",
            "iptables-save",
            "-c",
            timeout=6,
            kubectl_exec=True,
        )
        return chain_counters(raw.decode(), chain)

    def check_partition(
        self,
        chaos: Chaos,
        target: Target,
        active: JsonObject,
        workflow: str,
        endpoints: list[dict[str, str]],
    ) -> tuple[JsonObject, int, JsonObject]:
        chain = self.partition_chain(chaos, target)
        baseline = self.counters(target, chain)
        expect("F1A_DROP_COUNTER_BASELINE", baseline is not None, True)
        assert baseline is not None and chaos.issued_ns is not None
        self.retain("drop-baseline", baseline)
        ack_ns = time.time_ns()
        samples: list[JsonObject] = []

        def partitioned() -> tuple[tuple[JsonObject, int] | None, Any]:
            bind(target, self.reobserve(target))
            backend = chaos.sample("partition-active")
            expect(
                "F1A_PARTITION_CR_ACTIVE",
                bool(backend and backend_selected(backend, target, [r["pod"] for r in endpoints])),
                True,
            )
            cut = self.cut(workflow)
            rows, seals = self.witness()
            routes = route_rows(rows, target, workflow, attempt=1)
            token = active["claim"]["token_hash"]
            failed = [
                row
                for row in routes
                if row["at_ns"] > ack_ns
                and row["token_hash"] == token
                and row["code"] == "CONTROL_UNAVAILABLE"
            ]
            witnesses = [
                row
                for row in by(
                    rows, event="f1a_activity_wait", invocation=active["invocation"]["invocation"]
                )
                if row["at_ns"] > ack_ns
                and row["session"] == target.session
                and row["pod_uid"] == target.uid
            ]
            live = self.counters(target, chain)
            replacements = [
                row for row in by(cut["attempts"], is_current=True) if row["attempt_number"] > 1
            ]
            superseded = by(
                cut["attempts"],
                attempt_number=1,
                token_hash=token,
                is_current=False,
                state="TIMED_OUT",
            )
            diagnostics = {
                "failure_count": len(failed),
                "witness_ticks": len(witnesses),
                "baseline": baseline,
                "drops": live,
                "attempts": cut["attempts"],
            }
            samples.append({"at_ns": time.time_ns(), **diagnostics})
            self.retain("active-partition-samples", samples)
            if (
                not failed
                or len(witnesses) < 2
                or not live
                or not all(live[key] > baseline[key] for key in baseline)
                or not replacements
                or not superseded
            ):
                return None, diagnostics
            require_drop_rise(baseline, live)
            expect("F1A_STABLE_WORKER_CONTROL", self.targets_behind_service(), endpoints)
            replacement = one("F1A_PARTITION_REPLACEMENT", replacements)
            old = superseded[0]
            expect(
                "F1A_REPLACEMENT_SAME_INVOCATION_KEY",
                [
                    replacement[key]
                    for key in ("workflow_id", "node_id", "iteration", "attempt_number")
                ],
                [old[key] for key in ("workflow_id", "node_id", "iteration")]
                + [old["attempt_number"] + 1],
            )
            committed = one(
                "F1A_REPLACEMENT_COMMITTED_HISTORY",
                by(cut["history"], reason="TIMEOUT_REPLACEMENT"),
            )
            proof = {
                "cut": cut,
                "seals": seals,
                "routes": routes,
                "replacement": replacement,
                "old": old,
                "replacement_history": committed,
                "witness": witnesses,
                "counters": {"before": baseline, "during": live},
                "failure": failed[0],
            }
            self.retain("partition-confirmed", proof)
            return (proof, cut["sample_observed_ns"]), diagnostics

        proof, replacement_ns = poll(partitioned, "F1A_PARTITION_UNCONFIRMED", chaos.deadline - 8)
        absent_ns = chaos.remove()
        expect(
            "F1A_PARTITION_REMOVAL_BOUND",
            chaos.absent_monotonic is not None and chaos.absent_monotonic < chaos.deadline,
            True,
        )
        restored = self.connectivity(target, endpoints)
        prove_restoration(absent_ns, restored, {"status": 200})
        expect("F1A_PARTITION_RULE_GONE", self.counters(target, chain), None)
        self.retain("partition-restored", restored)
        # Let only the original real ActivityRunner finish. Its own result
        # call carries the old token. The replacement remains held.
        self.release_activity(active["invocation"]["invocation"])

        def stale() -> tuple[JsonObject | None, Any]:
            rows, seals = self.witness()
            routes = by(route_rows(rows, target, workflow, attempt=1), operation="result")
            try:
                proof = require_stale(
                    routes,
                    token_hash=active["claim"]["token_hash"],
                    replacement_observed_ns=replacement_ns,
                    restored_ns=restored["at_ns"],
                )
            except SmokeFailure as error:
                return None, str(error)
            self.retain("stale-rejection", {"response": proof, "seals": seals, "routes": routes})
            return proof, None

        rejected = poll(stale, "F1A_TYPED_STALE_REJECTION", time.monotonic() + 16)
        return proof, absent_ns, rejected

    def release_activity(self, invocation: str = "") -> None:
        expect(
            "F1A_RELEASE",
            self.exec_python("RELEASE", "deployment/observer", RELEASE_ACTIVITY, invocation),
            {"released": True},
        )

    def finish(self, workflow: str) -> JsonObject:
        self.release_activity()

        def terminal() -> tuple[JsonObject | None, Any]:
            cut = self.cut(workflow)
            state = cut["workflow"]["state"]
            return (cut if state == "SUCCEEDED" else None), state

        return poll(terminal, "F1A_TERMINAL", time.monotonic() + 60)

    def exercise(self, row: JsonObject, coverage: JsonObject) -> JsonObject:
        self.retain(
            "registration",
            {
                **row,
                "coverage": coverage,
                "engineering_fixture": {
                    "submissions": 1,
                    "workload": "pure.echo/v1",
                    "gate": "sealed activity entry with heartbeat",
                    "claim_and_heartbeat_lease_ms": 16000,
                    "gate_bound_seconds": 60,
                    "scheduler_acquire_hold_ms": int(SCHEDULER_HOLD_MS)
                    if self.cell_id == "medium-1"
                    else 0,
                },
            },
        )
        workflow, fingerprint = self.offer_episode()
        partition = self.cell_id == "medium-4"
        workers, active_workers = self.active_worker(workflow, heartbeat=partition)
        active_worker = select_target(workers, row["seed"])
        active = active_workers[active_worker.uid]
        candidates, cut = (
            self.active_scheduler(workflow)
            if self.cell_id == "medium-1"
            else (workers, self.cut(workflow))
        )
        target = select_target(candidates, row["seed"])
        self.retain(
            "selection",
            {
                "target": asdict(target),
                "eligible": sorted(t.uid for t in candidates),
                "seed": row["seed"],
                "cut": cut,
            },
        )
        before_uids = {pod["metadata"]["uid"] for pod in self.pods(target.role)}
        endpoints = self.targets_behind_service() if partition else []
        peer_names = [entry["pod"] for entry in endpoints] if partition else None
        if partition:
            baseline = self.connectivity(target, endpoints)
            expect("F1A_BASELINE_HTTP", baseline["status"], 200)
            self.retain("partition-baseline", {"connectivity": baseline, "endpoints": endpoints})
        self.chaos_run = chaos = Chaos(self, self.schema_dir, row, target, peer_names)
        chaos.preflight(self.crds)
        self.evidence.write_bytes(
            f"{self.cell_id}-target-before.log", self.role_log(target.pod, target.role)
        )
        if partition:
            expect("F1A_STABLE_WORKER_CONTROL", self.targets_behind_service(), endpoints)
        current = self.cut(workflow)
        expect(
            "F1A_CLAIM_ALIVE_AT_APPLY",
            bool(
                by(
                    current["attempts"],
                    state="CLAIMED",
                    is_current=True,
                    token_hash=active["claim"]["token_hash"],
                )
            ),
            True,
        )
        rows, _ = self.witness()
        expect(
            "F1A_INVOCATION_ACTIVE_AT_APPLY",
            not by(rows, event="finish", id=active["invocation"]["invocation"]),
            True,
        )
        if target.role == "scheduler":
            cut = self.ensure_scheduler_lease(target, workflow)
            self.retain("scheduler-at-apply", cut)
        chaos.apply(lambda: self.reobserve(target))
        assert chaos.issued_ns is not None

        def acknowledge() -> tuple[JsonObject | None, Any]:
            obj = chaos.sample("injection")
            return (obj if obj and backend_selected(obj, target, peer_names) else None), (
                obj.get("status", {}) if obj else "absent"
            )

        poll(acknowledge, "F1A_BACKEND_ACK", time.monotonic() + 18)
        chaos.events()
        rejected = None
        replacement = None
        if partition:
            _, absent_ns, rejected = self.check_partition(
                chaos, target, active, workflow, endpoints
            )
        else:
            self.watch_kill(target, chaos)
            absent_ns = chaos.remove()
            replacement = self.ready_replacement(target, before_uids, absent_ns)
            if replacement.role == "scheduler":
                self.scheduler_reconnect(replacement, absent_ns)
        final = self.finish(workflow)
        worker_rows, seals = self.witness()
        self.retain("terminal-cut", final)
        self.retain("terminal-witness", seals)
        expect("F1A_ACTIVITY_GATE_NEVER_EXPIRED", by(worker_rows, event="f1a_activity_expired"), [])
        winner = one(
            "F1A_WINNING_ATTEMPT", by(final["attempts"], is_current=False, state="SUCCEEDED")
        )
        winning_claim = one(
            "F1A_WINNING_CLAIM",
            by(
                final["claims"],
                token_hash=winner["token_hash"],
                attempt_number=winner["attempt_number"],
            ),
        )
        if target.role == "worker":
            expect(
                "F1A_WINNER_REPLACED",
                winner["attempt_number"] > 1
                and winner["token_hash"] != active["claim"]["token_hash"],
                True,
            )
        started = one(
            "F1A_WINNING_INVOCATION",
            [
                event
                for event in by(worker_rows, event="start")
                if event["identity"]["token_hash"] == winner["token_hash"]
            ],
        )
        expect(
            "F1A_WINNING_ACTIVITY_FINISHED",
            len(by(worker_rows, event="finish", id=started["id"])),
            1,
        )
        results = by(
            worker_rows,
            event="f1a_http",
            operation="result",
            workflow=workflow,
            token_hash=winner["token_hash"],
            status=200,
            session=started["session"],
            pod_uid=started["pod_uid"],
        )
        expect(
            "F1A_WINNING_RESULT_ROUTE",
            bool(results) and all(result["base_url"] == CONTROL_URL for result in results),
            True,
        )
        if rejected is not None:
            expect(
                "F1A_RESULT_AFTER_TYPED_REJECTION", results[0]["at_ns"] > rejected["at_ns"], True
            )
        fenced = None
        if replacement is not None and replacement.role == "scheduler":
            live = {p["metadata"]["uid"] for p in self.pods("scheduler") if ready(p)}
            sessions = {
                uid: {
                    entry["session_id"] for entry in by(final["audit"], pod_uid=uid, kind="acquire")
                }
                for uid in live
            }
            fenced = first_fenced_transition(cut, final, chaos.issued_ns, target, sessions)
            acquire = fenced["acquire"]
            pod = one(
                "F1A_FENCED_SCHEDULER_POD",
                [
                    p
                    for p in self.pods("scheduler")
                    if ready(p) and p["metadata"]["uid"] == acquire["pod_uid"]
                ],
            )
            author = self.identity("scheduler", pod, acquire["session_id"])
            author_log = self.scheduler_acquisition(author, acquire)
            expect("F1A_FENCED_SCHEDULER_PROCESS", author_log is not None, True)
            fenced.update(process=asdict(author), acquisition_log=author_log)
        checker = {
            "workflow": workflow,
            "body_hash": fingerprint,
            "registered": coverage["invariants_covered"],
            "not_covered": coverage["invariants_not_covered"],
            "registry_arm": coverage["arm"],
            "arm": row,
            "target": asdict(target),
            "removal_absent_ns": absent_ns,
            "winning_claim": winning_claim,
            "winning_invocation": started,
            "winning_result": results[0],
            "stale_rejection": rejected,
            "fenced_transition": fenced,
            "engine_cut": final,
            "witness": seals,
        }
        for topic in (EVENTS, TASKS):
            self.retain("kafka-final-" + topic, self.kafka(topic))
        self.retain("checker-inputs", checker)
        return {
            "workflow": workflow,
            "registered": checker["registered"],
            "not_covered": checker["not_covered"],
            "confirmation": "CONFIRMED",
        }


def inherited_failure(
    output: Path, code: str = "SHARED_KIND_HARNESS_FAILED_BEFORE_PHASE_F1A"
) -> None:
    if not output.exists():
        evidence = F1aEvidence(make_output(output))
        evidence.report.update(reason=code)
        evidence.checkpoint()


def record_handoff_failure(output: Path, error: BaseException) -> None:
    inherited_failure(output, "F1A_HANDOFF_" + type(error).__name__.upper())


def run(output: Path, harness: Driver, addresses: dict[str, str]) -> int:
    started = time.monotonic()
    evidence = F1aEvidence(make_output(output))
    failure = None
    smoke = None
    with ExitStack() as stack:
        try:
            with evidence.step("a-private-scratch"):
                require(
                    os.name == "posix" and bool(os.getenv("RUNNER_TEMP")),
                    "HOSTED_LINUX_RUNNER_REQUIRED",
                )
                scratch = Path(
                    stack.enter_context(
                        tempfile.TemporaryDirectory(
                            prefix="dur053-k6-f1a-",
                            dir=os.environ["RUNNER_TEMP"],
                        )
                    )
                )
                registry = read_registry()
            with evidence.step("a-build-tagged-images"):
                smoke = F1aSmoke(evidence, scratch, harness, addresses)
                smoke.build()
            with evidence.step("a-pinned-chaos-backend"):
                smoke.backend()
            streams: Any = Streams(harness, "", scratch / "cri")  # type: ignore[no-untyped-call]
            stack.callback(streams.close)
            smoke.streams = streams
            with evidence.step("a-cri-monitor"):
                streams.start_cri()
                # ctr emits no readiness line when idle. A running subscription
                # is corroborated later by the actual exits and /proc probes.
                streams.check()
            for cell in CELLS:
                cell_start = time.monotonic()
                clean = True
                with evidence.step(cell + "-registration"):
                    rows = [row for row in registry["episodes"] if row["cell"] == cell]
                    row = min(rows, key=lambda r: r["ordinal"])
                    coverage = one("F1A_ARM_COVERAGE", by(registry["coverage"], arm=row["arm"]))
                    # Record coverage even if this cell cannot reach injection.
                    smoke.cell_id = cell
                    smoke.retain("registered-coverage", coverage)
                try:
                    with evidence.step(cell + "-topology"):
                        smoke.cell_setup(cell)
                    with evidence.step(cell) as step:
                        step.update(smoke.exercise(row, coverage))
                except Exception as error:
                    failure = failure or evidence.safe_reason(error)
                    if evidence.report["outcomes"][-1]["step"] == cell:
                        evidence.report["outcomes"][-1]["confirmation"] = "UNCONFIRMED"
                    else:
                        evidence.report["outcomes"].append(
                            {"step": cell, "status": "SKIPPED", "reason": "F1A_TOPOLOGY_INCOMPLETE"}
                        )
                    evidence.checkpoint()
                finally:
                    try:
                        with evidence.step(cell + "-retain-role-logs"):
                            if smoke.namespace_started:
                                smoke.collect_role_logs(smoke.namespace, ROLES)
                    except Exception as error:
                        failure = failure or evidence.safe_reason(error)
                    try:
                        with evidence.step(cell + "-cleanup") as step:
                            smoke.clear_episode()
                            step["elapsed_seconds"] = round(time.monotonic() - cell_start, 3)
                    except Exception as error:
                        failure = failure or evidence.safe_reason(error)
                        clean = False
                if not clean:
                    break
        except (Exception, KeyboardInterrupt) as error:
            failure = failure or evidence.safe_reason(error)
        finally:
            for cell in CELLS:
                if not by(evidence.report["outcomes"], step=cell):
                    evidence.report["outcomes"].append(
                        {"step": cell, "status": "SKIPPED", "reason": "F1A_SETUP_INCOMPLETE"}
                    )
            if smoke is not None and smoke.chaos_started:
                smoke.cell_id = "setup"
                for stage, operation in (
                    ("d-chaos-role-logs", lambda: smoke.collect_role_logs("chaos-mesh", ())),
                    (
                        "d-chaos-cleanup",
                        lambda: smoke.k(
                            "F1A_CHAOS_NAMESPACE_CLEANUP",
                            "delete",
                            "namespace",
                            "chaos-mesh",
                            "--ignore-not-found",
                            "--timeout=50s",
                            timeout=55,
                        ),
                    ),
                ):
                    try:
                        with evidence.step(stage):
                            operation()
                    except Exception as error:
                        failure = failure or evidence.safe_reason(error)
            try:
                with evidence.step("d-credential-scan"):
                    evidence.scan()
            except Exception as error:
                failure = failure or evidence.safe_reason(error)
            evidence.report.update(
                status="FAIL" if failure else "PASS",
                reason=failure or "MATRIX_COMPLETE",
                elapsed_seconds=round(time.monotonic() - started, 3),
            )
            evidence.checkpoint()
    print(
        f"DUR-053 K6 Phase F1a {evidence.report['status']} {evidence.report['reason']}", flush=True
    )
    return int(failure is not None)
