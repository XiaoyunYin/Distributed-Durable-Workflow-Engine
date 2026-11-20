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
from dur053_k6_d_render import set_env
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
    GATE_SECONDS,
    LEASE_KEY,
    LEASE_MS,
    RELEASE_MARKER,
    attempt_fields,
    chain_counters,
    claim_rows,
    first_fenced_transition,
    invocation_identity,
    ns,
    partition_replacement,
    require_drop_rise,
    require_stale,
    result_after_activity,
    route_rows,
    stale_candidate_failure,
    terminal_winner,
    worker_principal,
)
from dur053_k6_identity import obj
from dur053_k6_registry import read_registry

JsonObject = dict[str, Any]

CELLS = ("medium-0", "medium-1", "medium-4")
SOURCE_NAMESPACE = "dur053-kind"
SCHEDULER_HOLD_MS = "4000"  # Below the scheduler's five-second iteration deadline.
CONTROL_VOLUME = "f1a-control"
CONTROL_DIRECTORY = str(Path(RELEASE_MARKER).parent.as_posix())
RELEASE_ENV = "DUR053_F1A_RELEASE_MARKER"

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
    marker_path, invocation = sys.argv[1:]
    if invocation and not re.fullmatch(r"[0-9a-f-]{36}", invocation):
        fail("F1A_RELEASE_INVOCATION_INVALID")
    marker = Path(marker_path)
    if invocation:
        marker = marker.with_name(marker.name + "-" + invocation)
    try:
        marker.touch(exist_ok=False)
    except OSError as error:
        fail("F1A_RELEASE_MARKER_WRITE_ERRNO_" + str(error.errno))
    print(json.dumps({"released": True, "marker": str(marker)}))

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


def audit_control_mounts(objects: list[JsonObject], namespace: str) -> None:
    expected = {
        ("worker", "containers", "worker", "witness", "/witness", False),
        ("worker", "initContainers", "witness-directory", "witness", "/witness", False),
        ("observer", "containers", "observer", "witness", "/witness", True),
        ("worker", "containers", "worker", CONTROL_VOLUME, CONTROL_DIRECTORY, True),
        ("observer", "containers", "observer", CONTROL_VOLUME, CONTROL_DIRECTORY, False),
    }
    claims = {}
    observed = []
    for item in objects:
        kind, name = item["kind"], item["metadata"]["name"]
        if kind == "PersistentVolumeClaim" and name in {"witness", CONTROL_VOLUME}:
            expect("F1A_CONTROL_PVC_NAMESPACE", item["metadata"].get("namespace"), namespace)
            require(name not in claims, f"F1A_CONTROL_PVC_DUPLICATE {name}")
            claims[name] = item["spec"]
        if kind == "Pod":
            pod = item["spec"]
        elif kind in {"Deployment", "Job", "ReplicaSet", "DaemonSet", "StatefulSet"}:
            pod = item["spec"]["template"]["spec"]
        elif kind == "CronJob":
            pod = item["spec"]["jobTemplate"]["spec"]["template"]["spec"]
        else:
            continue
        volumes = {volume["name"]: volume for volume in pod.get("volumes", [])}
        for group in ("containers", "initContainers", "ephemeralContainers"):
            for container in pod.get(group, []):
                owner = (name, group, container["name"])
                location = f"{kind}/{name}/{group}/{container['name']}"
                mounted = container.get("volumeMounts", []) + container.get("volumeDevices", [])
                for mount in mounted:
                    path = mount.get("mountPath", mount.get("devicePath"))
                    volume = volumes.get(mount["name"], {}).get("persistentVolumeClaim", {})
                    claim = volume.get("claimName")
                    restricted = ("/witness", CONTROL_DIRECTORY)
                    if claim not in {"witness", CONTROL_VOLUME} and not (
                        path
                        and any(path == root or path.startswith(root + "/") for root in restricted)
                    ):
                        continue
                    readonly = mount.get("readOnly", False)
                    require(type(readonly) is bool, f"F1A_CONTROL_MOUNT_READONLY_FLAG {location}")
                    if claim == "witness" and readonly is not True:
                        require(
                            kind == "Deployment" and name == "worker",
                            f"F1A_WITNESS_WRITABLE_OUTSIDE_WORKER {location}",
                        )
                    require(
                        claim != CONTROL_VOLUME
                        or readonly is True
                        or (
                            kind == "Deployment" and owner == ("observer", "containers", "observer")
                        ),
                        f"F1A_CONTROL_WRITABLE_OUTSIDE_OBSERVER {location}",
                    )
                    require(
                        kind == "Deployment"
                        and "mountPath" in mount
                        and not {"subPath", "subPathExpr", "mountPropagation"} & mount.keys()
                        and not (volume.get("readOnly") and not readonly),
                        f"F1A_CONTROL_MOUNT_SOURCE {location}/{path}",
                    )
                    observed.append((*owner, claim, path, readonly))
                if kind == "Deployment" and owner == ("worker", "containers", "worker"):
                    expect(
                        "F1A_GATE_RELEASE_PATH_MISMATCH",
                        by(container.get("env", []), name=RELEASE_ENV),
                        [{"name": RELEASE_ENV, "value": RELEASE_MARKER}],
                    )
    expect("F1A_CONTROL_PVC_INVENTORY", sorted(claims), [CONTROL_VOLUME, "witness"])
    require(
        not any(spec.get("volumeName") for spec in claims.values()),
        "F1A_CONTROL_PVC_MUST_BE_INDEPENDENT",
    )
    expect("F1A_CONTROL_MOUNT_INVENTORY", sorted(observed, key=str), sorted(expected, key=str))


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
        if kind == "PersistentVolumeClaim" and name == "witness":
            control = copy.deepcopy(item)
            control["metadata"]["name"] = CONTROL_VOLUME
            result.append(control)
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
                    **{RELEASE_ENV: RELEASE_MARKER},
                )
            if name == "scheduler":
                deployment["replicas"] = 2
                if scheduler_hold:
                    set_env(container, RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS=SCHEDULER_HOLD_MS)
            if name == "observer":
                pod["volumes"].append(
                    {"name": "witness", "persistentVolumeClaim": {"claimName": "witness"}}
                )
                container["volumeMounts"].append(
                    {"name": "witness", "mountPath": "/witness", "readOnly": True}
                )
            if name in {"observer", "worker"}:
                pod["volumes"].append(
                    {"name": CONTROL_VOLUME, "persistentVolumeClaim": {"claimName": CONTROL_VOLUME}}
                )
                container["volumeMounts"].append(
                    {
                        "name": CONTROL_VOLUME,
                        "mountPath": CONTROL_DIRECTORY,
                        "readOnly": name == "worker",
                    }
                )
        result.append(item)
    audit_topology(result, {*tags.values(), *ordinary.values()})
    audit_control_mounts(result, namespace)
    return result


def candidate(attempt: int | None, pod_uid: str | None, label: str) -> JsonObject:
    return {"attempt_number": attempt, "pod_uid": pod_uid, "label": label}


def failure_label(error: SmokeFailure) -> str:
    return str(error).split()[0]


def ready(pod: JsonObject) -> bool:
    return bool(k4_ready(pod))  # type: ignore[no-untyped-call]


class F1aSmoke(Matrix):
    inputs_schema = "dur053.k6-phase-f1a-inputs.v1"
    image_repository = "dur053-k6-f1a"
    scheduler_replicas = 2

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

    def poll[T](
        self, check: Callable[[], tuple[T | None, JsonObject]], stage: str, deadline: float
    ) -> T:
        observed: JsonObject = {
            "candidates": [candidate(None, None, "F1A_CANDIDATE_NO_SAMPLE_BEFORE_DEADLINE")]
        }
        witness_rows = []
        while time.monotonic() < deadline:
            result, observed = check()
            witness_rows = observed.get("witness_rows") or witness_rows
            if result is not None:
                if time.monotonic() < deadline:
                    return result
                observed["candidates"].append(
                    candidate(None, None, "F1A_CANDIDATE_SAMPLE_COMPLETED_AFTER_DEADLINE")
                )
                break
            time.sleep(0.3)
        label = stage.removeprefix("F1A_").lower().replace("_", "-") + "-timeout-witness"
        self.retain(label, {**observed, "witness_rows": witness_rows})
        raise SmokeFailure(
            f"{stage} observed={observed['candidates']!r} "
            f"expected=confirmed_before_deadline evidence={self.cell_id}-{label}.json"
        )

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

    def render_episode(self, source: list[JsonObject]) -> list[JsonObject]:
        return deployment_objects(
            source,
            self.namespace,
            self.tags,
            self.harness.images,
            scheduler_hold=self.cell_id == "medium-1",
        )

    def prepare_episode_dependencies(self) -> None:
        pass

    def setup_episode(self) -> None:
        renderer = copy.copy(self.harness)
        renderer.scratch = renderer.output = self.scratch / self.cell_id
        renderer.scratch.mkdir()
        _, source = renderer.render("kind", self.addresses)
        try:
            objects = self.render_episode(source)
        except SmokeFailure as error:
            self.retain("render-failure", {"reason": self.evidence.safe_reason(error)})
            raise
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
        self.prepare_episode_dependencies()
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
        for role, replicas in (("scheduler", self.scheduler_replicas), ("worker", 2)):
            pods = self.pods(role)
            expect("F1A_TOPOLOGY_" + role.upper(), [ready(p) for p in pods], [True] * replicas)
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
        try:
            result = self.snapshot(workflow)
        except (TypeError, ValueError):
            raise SmokeFailure("F1A_ENGINE_CUT_INVALID_JSON") from None
        try:
            for entry in result["audit"]:
                entry["observed_ns"] = ns(entry["finalized_at"])
        except (KeyError, TypeError, ValueError):
            self.retain("engine-audit-invalid", result)
            raise SmokeFailure("F1A_ENGINE_AUDIT_FINALIZATION_INVALID") from None
        return result

    def witness(self) -> tuple[list[JsonObject], list[JsonObject]]:
        seals = self.exec_python("WITNESS", "deployment/observer", READ_SEALS)
        observed: list[JsonObject] = []
        try:
            for bundle in seals:
                seal = bundle["inventory"]
                expect("F1A_WITNESS_COMPLETE", (seal["complete"], seal["errors"]), (True, 0))
                for key, rows in bundle["streams"].items():
                    for line in rows["data"].splitlines():
                        row = json.loads(line)
                        scope = {field: seal[field] for field in ("pod_uid", "session")}
                        if key == "trace":
                            for field, value in scope.items():
                                expect(f"F1A_WITNESS_ROW_{field.upper()}", row.get(field), value)
                        observed.append({**row, **scope, "stream": key})
        except (SmokeFailure, AttributeError, KeyError, TypeError, ValueError) as error:
            self.retain("witness-invalid", seals)
            if isinstance(error, SmokeFailure):
                raise
            raise SmokeFailure("F1A_WITNESS_ROW_SHAPE") from None
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

    def identity(
        self, role: str, pod: JsonObject, session: str, *, require_ready: bool = True
    ) -> Target:
        name = pod["metadata"]["name"]
        uid = pod["metadata"]["uid"]
        node = pod["spec"]["nodeName"]
        row = one("F1A_WORKLOAD_CONTAINER", by(pod["status"]["containerStatuses"], name=role))
        container_id = row["containerID"]
        expect(
            "F1A_TARGET_READY",
            (ready(pod) or not require_ready) and not pod["metadata"].get("deletionTimestamp"),
            True,
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

    def worker_candidate(
        self,
        attempt: JsonObject,
        entry: JsonObject,
        pod: JsonObject | None,
        cut: JsonObject,
        rows: list[JsonObject],
        *,
        heartbeat: bool,
    ) -> tuple[Target, JsonObject]:
        require(pod is not None, "F1A_CANDIDATE_WORKER_POD_UID_MISSING")
        assert pod is not None
        require(
            ready(pod) and not pod["metadata"].get("deletionTimestamp"),
            "F1A_CANDIDATE_WORKER_POD_NOT_READY",
        )
        principal = worker_principal(pod)
        require(principal == attempt["worker_id"], "F1A_CANDIDATE_WORKER_PRINCIPAL_MISMATCH")
        require(
            entry["worker"].startswith(pod["metadata"]["name"] + "-"),
            "F1A_CANDIDATE_WORKER_SLOT_MISMATCH",
        )
        require(
            entry["request"] == attempt["worker_request_id"],
            "F1A_CANDIDATE_WORKER_REQUEST_MISMATCH",
        )
        identity = invocation_identity(attempt, self.namespace)
        expect(
            "F1A_CANDIDATE_WORKER_INVOCATION_KEY",
            (entry["node"], entry["iteration"], entry["attempt"]),
            (identity["node"], str(identity["iteration"]), str(identity["attempt"])),
        )
        scope = {key: entry[key] for key in ("session", "pod_uid")}
        event = {"id": entry["invocation"], "identity": identity, **scope}
        one("F1A_CANDIDATE_WORKER_SEALED_START_MISSING", by(rows, event="start", **event))
        require(
            not by(rows, event="finish", **event)
            and not any(
                by(rows, event=boundary, invocation=entry["invocation"], **scope)
                for boundary in ("f1a_activity_release", "f1a_activity_expired")
            ),
            "F1A_CANDIDATE_WORKER_INVOCATION_INACTIVE",
        )
        claim = one(
            "F1A_CANDIDATE_WORKER_COMMITTED_CLAIM_MISSING", claim_rows(cut, attempt, principal)
        )
        target = self.identity("worker", pod, entry["session"])
        routes = by(
            route_rows(rows, target, attempt["workflow_id"], attempt=attempt["attempt_number"]),
            node=attempt["node_id"],
            iteration=attempt["iteration"],
            token_hash=attempt["token_hash"],
        )
        require(
            bool(by(routes, operation="claim", status=200)),
            "F1A_CANDIDATE_WORKER_CLAIM_ROUTE_MISSING",
        )
        require(
            not heartbeat or bool(by(routes, operation="heartbeat", status=200)),
            "F1A_CANDIDATE_WORKER_HEARTBEAT_ROUTE_MISSING",
        )
        return target, {"invocation": entry, "claim": claim, "attempt": attempt, "routes": routes}

    def active_worker(self, workflow: str, *, heartbeat: bool) -> tuple[list[Target], JsonObject]:
        def active() -> tuple[tuple[list[Target], JsonObject] | None, JsonObject]:
            cut = self.cut(workflow)
            rows, seals = self.witness()
            attempts = by(cut["attempts"], is_current=True, state="CLAIMED", attempt_number=1)
            pods = {p["metadata"]["uid"]: p for p in self.pods("worker")}
            failures = [
                candidate(1, uid, "F1A_CANDIDATE_WORKER_CURRENT_CLAIM_MISSING")
                for uid in pods or [None]
                if not attempts
            ]
            matches, witnesses = [], {}
            for attempt in attempts:
                number = attempt["attempt_number"]
                waiting = {
                    (entry["pod_uid"], entry["session"], entry["invocation"]): entry
                    for entry in by(
                        rows,
                        event="f1a_activity_wait",
                        workflow=workflow,
                        token_hash=attempt["token_hash"],
                    )
                }
                waiting_uids = {uid for uid, _, _ in waiting}
                for uid in pods or [None]:
                    if uid not in waiting_uids:
                        failures.append(
                            candidate(number, uid, "F1A_CANDIDATE_WORKER_WAIT_FOR_TOKEN_MISSING")
                        )
                for (uid, _, _), entry in waiting.items():
                    try:
                        target, proof = self.worker_candidate(
                            attempt, entry, pods.get(uid), cut, rows, heartbeat=heartbeat
                        )
                    except SmokeFailure as error:
                        failures.append(candidate(number, uid, failure_label(error)))
                        continue
                    if uid not in witnesses:
                        matches.append(target)
                        witnesses[uid] = proof
            observed = {"cut": cut, "seals": seals, "witness_rows": rows, "candidates": failures}
            if not matches:
                return None, observed
            self.retain(
                "active-worker",
                {"cut": cut, "seals": seals, "candidates": list(witnesses.values())},
            )
            return (matches, witnesses), observed

        return self.poll(active, "F1A_WARMUP_ACTIVE_WORKER", time.monotonic() + 32)

    def scheduler_lease(
        self, workflow: str, stage: str, *, bound: Target | None = None
    ) -> tuple[Target, JsonObject, JsonObject]:
        def active() -> tuple[tuple[Target, JsonObject, JsonObject] | None, JsonObject]:
            cut = self.cut(workflow)
            pods = {p["metadata"]["uid"]: p for p in self.pods("scheduler")}
            attempt = next((r["attempt_number"] for r in cut["attempts"] if r["is_current"]), None)
            leases = [
                r
                for r in cut["leases"]
                if r["live"] and r["owner_id"] and ns(r["lease_expires_at"]) > time.time_ns()
            ]
            failures, logs = [], []
            observed = {"cut": cut, "candidates": failures, "witness_rows": logs}
            if not leases:
                uids = [bound.uid] if bound else (pods or [None])
                failures.extend(
                    candidate(attempt, uid, "F1A_CANDIDATE_SCHEDULER_LIVE_LEASE_MISSING")
                    for uid in uids
                )
                return None, observed
            lease = one("F1A_CURRENT_LEASE", leases)
            acquisitions = by(
                cut["audit"], kind="acquire", **{key: lease[key] for key in LEASE_KEY}
            )
            if not acquisitions:
                uids = [bound.uid] if bound else (pods or [None])
                failures.extend(
                    candidate(attempt, uid, "F1A_CANDIDATE_SCHEDULER_COMMITTED_ACQUIRE_MISSING")
                    for uid in uids
                )
                return None, observed
            entry = max(acquisitions, key=lambda r: r["event_order"])
            uid = bound.uid if bound else entry["pod_uid"]
            pod = pods.get(uid)
            try:
                expect("F1A_CANDIDATE_SCHEDULER_POD_UID_MISMATCH", entry["pod_uid"], uid)
                if bound:
                    expect(
                        "F1A_CANDIDATE_SCHEDULER_SESSION_MISMATCH",
                        entry["session_id"],
                        bound.session,
                    )
                require(
                    pod is not None and ready(pod) and not pod["metadata"].get("deletionTimestamp"),
                    "F1A_CANDIDATE_SCHEDULER_POD_NOT_READY",
                )
                assert pod is not None
                target = self.identity("scheduler", pod, entry["session_id"])
                if bound:
                    bind(bound, target)
                logs.extend(self.scheduler_acquisitions(target))
                acquired = self.scheduler_acquisition(target, entry, logs)
                require(acquired is not None, "F1A_CANDIDATE_SCHEDULER_ACQUISITION_LOG_MISSING")
                assert acquired is not None
            except SmokeFailure as error:
                failures.append(candidate(attempt, uid, failure_label(error)))
                return None, observed
            # Re-check expiry after Kubernetes, CRI and log reads.
            if ns(lease["lease_expires_at"]) <= time.time_ns():
                failures.append(
                    candidate(attempt, uid, "F1A_CANDIDATE_SCHEDULER_LEASE_EXPIRED_DURING_READ")
                )
                return None, observed
            return (target, cut, acquired), observed

        return self.poll(active, stage, time.monotonic() + 16)

    def active_scheduler(self, workflow: str) -> tuple[list[Target], JsonObject]:
        target, cut, acquired = self.scheduler_lease(workflow, "F1A_ACTIVE_SCHEDULER")
        self.retain("scheduler-selected-acquisition", acquired)
        return [target], cut

    def ensure_scheduler_lease(self, target: Target, workflow: str) -> JsonObject:
        _, cut, acquired = self.scheduler_lease(workflow, "F1A_BOUND_SCHEDULER_LEASE", bound=target)
        self.retain("scheduler-acquisition", acquired)
        return cut

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

    def scheduler_acquisitions(self, target: Target) -> list[JsonObject]:
        rows = []
        for line in self.role_log(target.pod, "scheduler").decode().splitlines():
            entry = structured_log(line)  # type: ignore[no-untyped-call]
            if entry and entry.get("event") == "scheduler_acquisition":
                rows.append(entry)
        return rows

    def scheduler_acquisition(
        self, target: Target, acquire: JsonObject, rows: list[JsonObject]
    ) -> JsonObject | None:
        # F1a injects metadata.uid as CONTAINER_ID. CRI identity is in Target.
        expect("F1A_SCHEDULER_AUDIT_POD_HINT", acquire["container_id"], target.uid)
        found = by(
            rows,
            boundary="after_lease_acquired_hold_enter",
            pod_uid=target.uid,
            session=target.session,
            owner=acquire["owner_id"],
            epoch=acquire["epoch"],
            partition=acquire["partition_id"],
        )
        after_commit = [row for row in found if ns(row["timestamp"]) >= acquire["observed_ns"]]
        if not after_commit:
            return None
        entry = min(after_commit, key=lambda r: ns(r["timestamp"]))
        expect("F1A_SCHEDULER_LOG_POD_HINT", entry["container_id"], target.uid)
        expect("F1A_SCHEDULER_ACQUISITION_PID", str(entry["pid"]), target.process.split(":")[0])
        return entry

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

    def clear_namespace(self, namespace: str, timeout: int) -> None:
        self.k(
            "F1A_NAMESPACE_CLEANUP",
            "delete",
            "namespace",
            namespace,
            "--ignore-not-found",
            f"--timeout={timeout}s",
            timeout=timeout + 5,
        )
        expect(
            "F1A_CLEANUP_NAMESPACE_STILL_PRESENT",
            self.k(
                "F1A_CLEANUP_NAMESPACE_READ",
                "get",
                "namespace",
                namespace,
                "--ignore-not-found",
                "-o",
                "name",
                timeout=6,
            )
            .decode()
            .split(),
            [],
        )

    def clear_episode(self) -> None:
        journal: JsonObject = {"namespace": self.namespace, "database": self.database, "steps": []}
        failures = []

        def run_step(label: str, action: Callable[[], Any]) -> None:
            entry: JsonObject = {"label": label, "started_ns": time.time_ns()}
            try:
                action()
            except Exception as error:
                failures.append(label)
                entry["reason"] = self.evidence.safe_reason(error)
            journal["steps"].append({**entry, "observed_ns": time.time_ns()})

        def volumes() -> None:
            def reclaimed() -> tuple[bool | None, JsonObject]:
                entries = json.loads(
                    self.k("F1A_CLEANUP_PV_READ", "get", "pv", "-o", "json", timeout=6)
                )["items"]
                remaining = [
                    {
                        "name": item["metadata"]["name"],
                        "claim": claim,
                        "phase": item["status"]["phase"],
                    }
                    for item in entries
                    if (claim := item["spec"].get("claimRef", {})).get("namespace")
                    == self.namespace
                ]
                journal["remaining_volumes"] = remaining
                return (None if remaining else True), {
                    "volumes": remaining,
                    "candidates": [candidate(None, None, "F1A_CLEANUP_PV_STILL_BOUND")],
                }

            self.poll(reclaimed, "F1A_CLEANUP_PV_RECLAIM", time.monotonic() + 25)

        def rbac() -> None:
            refs = (
                f"clusterrole/{self.namespace}-tokenreview",
                f"clusterrolebinding/{self.namespace}-tokenreview",
            )
            self.k("F1A_RBAC_CLEANUP", "delete", *refs, "--ignore-not-found", timeout=10)
            expect(
                "F1A_CLEANUP_RBAC_STILL_PRESENT",
                self.k(
                    "F1A_CLEANUP_RBAC_READ",
                    "get",
                    *refs,
                    "--ignore-not-found",
                    "-o",
                    "name",
                    timeout=6,
                )
                .decode()
                .split(),
                [],
            )

        deadline = self.deadline
        self.deadline = datetime.now(UTC) + timedelta(seconds=180)
        try:
            if self.chaos_run is not None and self.chaos_run.possibly_created:
                run_step("F1A_CLEANUP_CHAOS", self.chaos_run.remove)
            if self.namespace_started:
                run_step("F1A_CLEANUP_NAMESPACE", lambda: self.clear_namespace(self.namespace, 90))
                if "F1A_CLEANUP_NAMESPACE" not in failures:
                    self.namespace_started = False
                    run_step("F1A_CLEANUP_PV_RECLAIM", volumes)
                run_step("F1A_CLEANUP_RBAC", rbac)
            if self.database_started:
                run_step("F1A_CLEANUP_DATABASE", lambda: self.harness.drop(self.database))
                if "F1A_CLEANUP_DATABASE" not in failures:
                    self.database_started = False
        finally:
            self.deadline = deadline
            self.retain("cleanup", journal)
        require(
            not failures, f"F1A_CLEANUP_FAILED {failures!r} evidence={self.cell_id}-cleanup.json"
        )

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

        def dead() -> tuple[JsonObject | None, JsonObject]:
            self.streams.check()
            with self.streams.lock:
                exits = list(self.streams.cri_exits)
            process = self.proc_start(target.node, target.host_pid)
            old_pods = [p for p in self.pods(target.role) if p["metadata"]["uid"] == target.uid]
            observed = {
                "old_process": process,
                "old_uid_present": bool(old_pods),
                "cri_exits": exits,
            }
            if process == target.process:
                label = "F1A_CANDIDATE_KILLED_PROCESS_STILL_RUNNING"
            elif target.role == "worker" and old_pods:
                label = "F1A_CANDIDATE_KILLED_WORKER_UID_STILL_PRESENT"
            else:
                label = None
            cid = target.container_id
            matches = [row for row in exits if row["container_id"] == cid.split("://")[1]]
            if label is None and not matches and time.time_ns() - issued_ns < 8e9:
                label = "F1A_CANDIDATE_KILLED_CONTAINER_EXIT_NOT_YET_OBSERVED"
            if label:
                return None, {**observed, "candidates": [candidate(1, target.uid, label)]}
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
            return proof, {**observed, "candidates": []}

        return self.poll(dead, "F1A_KILL_DEATH", time.monotonic() + 18)

    def ready_replacement(self, target: Target, before_uids: set[str], absent_ns: int) -> Target:
        def healthy() -> tuple[Target | None, JsonObject]:
            pods = self.pods(target.role)
            failures = []
            observed = {"pods": [pod_state(p) for p in pods], "candidates": failures}
            for pod in pods:
                uid = pod["metadata"]["uid"]
                if uid in before_uids and uid != target.uid:
                    continue
                if not ready(pod) or pod["metadata"].get("deletionTimestamp"):
                    failures.append(candidate(1, uid, "F1A_CANDIDATE_REPLACEMENT_POD_NOT_READY"))
                    continue
                row = one(
                    "F1A_REPLACEMENT_CONTAINER",
                    by(pod["status"]["containerStatuses"], name=target.container),
                )
                if row["containerID"] == target.container_id:
                    failures.append(
                        candidate(1, uid, "F1A_CANDIDATE_REPLACEMENT_CONTAINER_UNCHANGED")
                    )
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
                return replacement, observed
            if not failures:
                failures.append(candidate(1, None, "F1A_CANDIDATE_REPLACEMENT_POD_MISSING"))
            return None, observed

        return self.poll(healthy, "F1A_READY_REPLACEMENT", time.monotonic() + 65)

    def scheduler_reconnect(self, replacement: Target, absent_ns: int) -> None:
        service = self.objects("F1A_POSTGRES_SERVICE", "service", "postgres")
        service_peer = f"{service['spec']['clusterIP']}:5432"
        endpoints = self.objects("F1A_POSTGRES_ENDPOINT", "endpoints", "postgres")
        expect(
            "F1A_SCHEDULER_ENGINE_ENDPOINT",
            [a["ip"] for subset in endpoints["subsets"] for a in subset["addresses"]],
            [self.engine_address],
        )

        def connections() -> tuple[JsonObject | None, JsonObject]:
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
            # ss -Htnp with a TCP state filter omits the state column.
            owned = [line for line in raw.splitlines() if f"pid={replacement.host_pid}," in line]
            matches = [line for line in owned if service_peer in line.split()]
            proof = {
                "at_ns": time.time_ns(),
                "postgres_connections": matches,
                "connected": bool(matches),
                "service_peer": service_peer,
                "engine_address": self.engine_address,
            }
            observed = {**proof, "established": raw.splitlines(), "candidates": []}
            if not matches:
                label = (
                    "F1A_CANDIDATE_SCHEDULER_POSTGRES_PEER_MISSING"
                    if owned
                    else "F1A_CANDIDATE_SCHEDULER_OWNED_SOCKET_MISSING"
                )
                return None, {**observed, "candidates": [candidate(1, replacement.uid, label)]}
            bind(replacement, self.reobserve(replacement))
            prove_restoration(absent_ns, proof, {"connected": True})
            self.retain("scheduler-restoration", proof)
            return proof, observed

        self.poll(connections, "F1A_SCHEDULER_RECONNECT", time.monotonic() + 20)

    def partition_chain(self, chaos: Chaos, target: Target) -> str:
        sources = self.objects("F1A_POD_NETWORK", "podnetworkchaos", target.pod)
        self.retain("pod-network-chaos", sources)
        found = [
            row
            for row in sources.get("spec", {}).get("iptables", [])
            if row["source"] == f"{self.namespace}/{chaos.name}" and row["direction"] == "output"
        ]
        chain: str = one("F1A_CHAOS_DROP_CHAIN", found)["name"]
        return chain

    def daemon_network(self, target: Target, *args: str) -> str:
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
            *args,
            timeout=6,
            kubectl_exec=True,
        )
        return raw.decode()

    def counters(self, target: Target, chain: str) -> dict[str, int] | None:
        return chain_counters(self.daemon_network(target, "iptables-save", "-c"), chain)

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

        def partitioned() -> tuple[tuple[JsonObject, int] | None, JsonObject]:
            bind(target, self.reobserve(target))
            backend = chaos.sample("partition-active")
            expect(
                "F1A_PARTITION_CR_ACTIVE",
                bool(backend and backend_selected(backend, target, [r["pod"] for r in endpoints])),
                True,
            )
            cut = self.cut(workflow)
            rows, seals = self.witness()
            attempt = active["attempt"]
            routes = by(
                route_rows(rows, target, workflow, attempt=attempt["attempt_number"]),
                node=attempt["node_id"],
                iteration=attempt["iteration"],
                token_hash=attempt["token_hash"],
            )
            failed = [
                row
                for row in routes
                if row["at_ns"] > ack_ns and row["code"] == "CONTROL_UNAVAILABLE"
            ]
            witnesses = [
                row
                for row in by(
                    rows,
                    event="f1a_activity_wait",
                    invocation=active["invocation"]["invocation"],
                    session=target.session,
                    pod_uid=target.uid,
                    token_hash=attempt["token_hash"],
                )
                if row["at_ns"] > ack_ns
            ]
            live = self.counters(target, chain)
            diagnostics = {
                "cut": cut,
                "seals": seals,
                "witness_rows": rows,
                "failure_count": len(failed),
                "witness_ticks": len(witnesses),
                "baseline": baseline,
                "drops": live,
                "candidates": [],
            }
            try:
                require(bool(failed), "F1A_CANDIDATE_PARTITION_CONTROL_FAILURE_MISSING")
                require(len(witnesses) >= 2, "F1A_CANDIDATE_PARTITION_INVOCATION_TICKS_MISSING")
                require(live is not None, "F1A_CANDIDATE_PARTITION_DROP_JUMP_MISSING")
                assert live is not None
                require_drop_rise(baseline, live)
                replacement = partition_replacement(cut, attempt)
            except SmokeFailure as error:
                diagnostics["candidates"].append(
                    candidate(attempt["attempt_number"], target.uid, failure_label(error))
                )
                result = None
            else:
                expect("F1A_STABLE_WORKER_CONTROL", self.targets_behind_service(), endpoints)
                proof = {
                    "cut": cut,
                    "seals": seals,
                    "routes": routes,
                    **replacement,
                    "witness": witnesses,
                    "counters": {"before": baseline, "during": live},
                    "failure": failed[0],
                }
                self.retain("partition-confirmed", proof)
                result = (proof, cut["sample_observed_ns"])
            samples.append(
                {
                    "at_ns": time.time_ns(),
                    "attempts": cut["attempts"],
                    **{
                        key: diagnostics[key]
                        for key in (
                            "failure_count",
                            "witness_ticks",
                            "baseline",
                            "drops",
                            "candidates",
                        )
                    },
                }
            )
            self.retain("active-partition-samples", samples)
            return result, diagnostics

        proof, replacement_ns = self.poll(
            partitioned, "F1A_PARTITION_UNCONFIRMED", chaos.deadline - 8
        )
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
        waiting = active["invocation"]
        marker = self.release_activity(waiting)
        rejected = self.wait_stale(
            target,
            active["attempt"],
            waiting["invocation"],
            marker,
            replacement_ns,
            restored["at_ns"],
        )
        return proof, absent_ns, rejected

    def wait_stale(
        self,
        target: Target,
        attempt: JsonObject,
        invocation: str,
        marker: JsonObject,
        replacement_ns: int,
        restored_ns: int,
    ) -> JsonObject:
        order = {
            "token_hash": attempt["token_hash"],
            "replacement_observed_ns": replacement_ns,
            "restored_ns": restored_ns,
        }

        def stale() -> tuple[JsonObject | None, JsonObject]:
            failures = []
            routes = []
            diagnostics = {
                "marker": marker,
                "target": asdict(target),
                "candidates": failures,
                **order,
            }
            try:
                rows, seals = self.witness()
                diagnostics.update(witness_rows=rows, seals=seals)
                self.require_healthy_gates(rows)
                routes = by(
                    route_rows(
                        rows, target, attempt["workflow_id"], attempt=attempt["attempt_number"]
                    ),
                    operation="result",
                    node=attempt["node_id"],
                    iteration=attempt["iteration"],
                )
                proof = require_stale(routes, **order)
                started = one(
                    "F1A_STALE_INVOCATION",
                    by(
                        rows,
                        event="start",
                        id=invocation,
                        pod_uid=target.uid,
                        session=target.session,
                        identity=invocation_identity(attempt, self.namespace),
                    ),
                )
                completion = result_after_activity(rows, started, attempt, proof, marker)
            except SmokeFailure as error:
                label = failure_label(error)
                if label == "F1A_TYPED_STALE_REJECTION":
                    failures.extend(
                        candidate(row["attempt_number"], row["pod_uid"], reason)
                        for row in routes
                        if (reason := stale_candidate_failure(row, **order))
                    )
                    label = "F1A_CANDIDATE_STALE_RESULT_ROUTE_MISSING"
                if not failures:
                    failures.append(candidate(attempt["attempt_number"], target.uid, label))
                return None, diagnostics
            except (KeyError, TypeError, ValueError):
                failures.append(
                    candidate(attempt["attempt_number"], target.uid, "F1A_STALE_SAMPLE_SHAPE")
                )
                return None, diagnostics
            self.retain(
                "stale-rejection",
                {
                    "response": proof,
                    "seals": seals,
                    "routes": routes,
                    "marker": marker,
                    **completion,
                },
            )
            return proof, diagnostics

        return self.poll(stale, "F1A_TYPED_STALE_REJECTION", time.monotonic() + LEASE_MS / 1000)

    def release_activity(
        self, waiting: JsonObject | None = None, *, all_workers: bool = False
    ) -> JsonObject:
        invocation = waiting["invocation"] if waiting and not all_workers else ""
        marker = RELEASE_MARKER + (f"-{invocation}" if invocation else "")
        observation = {
            "started_ns": time.time_ns(),
            "marker": marker,
            "waiting": waiting,
            "claim_lease_ms": LEASE_MS,
            "gate_seconds": GATE_SECONDS,
        }
        try:
            if waiting is not None:
                gate_start = waiting.get("gate_started_ns")
                require(type(gate_start) is int, "F1A_RELEASE_GATE_START_MISSING")
                observation["gate_remaining_ns"] = (
                    gate_start + GATE_SECONDS * 10**9 - observation["started_ns"]
                )
                require(observation["gate_remaining_ns"] > 0, "F1A_RELEASE_AFTER_GATE_BOUND")
            response = self.exec_python(
                "RELEASE", "deployment/observer", RELEASE_ACTIVITY, RELEASE_MARKER, invocation
            )
            observation.update(response=response, observed_ns=time.time_ns())
            expect("F1A_RELEASE_RECEIPT", response, {"released": True, "marker": marker})
        except SmokeFailure as error:
            observation["reason"] = self.evidence.safe_reason(error)
            raise
        finally:
            self.retain("release-" + (invocation if invocation else "all"), observation)
        return observation

    def require_healthy_gates(self, rows: list[JsonObject]) -> None:
        for event, label in (
            ("f1a_activity_control_missing", "F1A_ACTIVITY_CONTROL_MISSING"),
            ("f1a_activity_control_unreadable", "F1A_ACTIVITY_CONTROL_UNREADABLE"),
            ("f1a_activity_expired", "F1A_ACTIVITY_GATE_NEVER_EXPIRED"),
        ):
            failed = by(rows, event=event)
            if failed:
                self.retain("gate-failure", {"label": label, "rows": failed})
                raise SmokeFailure(label)

    def finish(
        self, workflow: str, waiting: JsonObject | None = None
    ) -> tuple[JsonObject, JsonObject, list[JsonObject]]:
        marker = self.release_activity(waiting, all_workers=True)

        def terminal() -> tuple[tuple[JsonObject, JsonObject, list[JsonObject]] | None, JsonObject]:
            diagnostics: JsonObject = {"marker": marker, "candidates": []}
            try:
                cut = self.cut(workflow)
                diagnostics["cut"] = cut
                self.retain("terminal-cut", cut)
                rows, seals = self.witness()
                diagnostics.update(witness_rows=rows, seals=seals)
                self.retain("terminal-witness", seals)
                self.require_healthy_gates(rows)
                expect("F1A_TERMINAL_WORKFLOW_ID", cut["workflow"].get("workflow_id"), workflow)
                expect("F1A_TERMINAL_WORKFLOW_STATE", cut["workflow"]["state"], "SUCCEEDED")
                pods = self.pods("worker")
                diagnostics["pods"] = [
                    {
                        **pod_state(p),
                        "uid": p["metadata"]["uid"],
                        "service_account": p["spec"].get("serviceAccountName"),
                    }
                    for p in pods
                ]
                winner = terminal_winner(cut, rows, pods, self.namespace, marker)
            except (SmokeFailure, KeyError, TypeError, ValueError) as error:
                label = (
                    failure_label(error)
                    if isinstance(error, SmokeFailure)
                    else "F1A_TERMINAL_SAMPLE_SHAPE"
                )
                attempts = diagnostics.get("cut", {}).get("attempts", [])
                for attempt in attempts if isinstance(attempts, list) else []:
                    if not isinstance(attempt, dict) or not (
                        attempt.get("is_current") or attempt.get("state") == "SUCCEEDED"
                    ):
                        continue
                    uids = {
                        r["pod_uid"]
                        for r in diagnostics.get("witness_rows", [])
                        if r.get("event") == "start"
                        and r.get("identity", {}).get("token_hash") == attempt.get("token_hash")
                    }
                    diagnostics["candidates"].extend(
                        candidate(attempt.get("attempt_number"), uid, label)
                        for uid in sorted(uids) or [None]
                    )
                if not diagnostics["candidates"]:
                    diagnostics["candidates"].append(candidate(None, None, label))
                return None, diagnostics
            return (cut, winner, seals), diagnostics

        return self.poll(terminal, "F1A_TERMINAL", time.monotonic() + GATE_SECONDS)

    def reconcile_scheduler(
        self, before: JsonObject, final: JsonObject, issued_ns: int, target: Target
    ) -> JsonObject:
        report: JsonObject = {
            "before": before,
            "final": final,
            "issued_ns": issued_ns,
            "interrupted_target": asdict(target),
        }
        try:
            pods = {p["metadata"]["uid"]: p for p in self.pods("scheduler") if ready(p)}
            report["ready_pods"] = {uid: pod_state(p) for uid, p in pods.items()}
            sessions = {
                uid: {
                    entry["session_id"] for entry in by(final["audit"], pod_uid=uid, kind="acquire")
                }
                for uid in pods
            }
            fenced = first_fenced_transition(before, final, issued_ns, target, sessions)
            report["fenced"] = fenced
            acquire = fenced["acquire"]
            author = self.identity("scheduler", pods[acquire["pod_uid"]], acquire["session_id"])
            report["author"] = asdict(author)
            if author.uid == target.uid:
                expect(
                    "F1A_FENCED_SCHEDULER_RESTARTED_PROCESS",
                    author.container_id != target.container_id
                    and author.process != target.process
                    and author.restart_count > target.restart_count,
                    True,
                )
            author_logs = self.scheduler_acquisitions(author)
            self.retain("fenced-scheduler-witness", author_logs)
            author_log = self.scheduler_acquisition(author, acquire, author_logs)
            expect("F1A_FENCED_SCHEDULER_PROCESS", author_log is not None, True)
            bind(author, self.reobserve(author))
            return {**fenced, "process": asdict(author), "acquisition_log": author_log}
        except (SmokeFailure, KeyError, TypeError, ValueError) as error:
            report["reason"] = self.evidence.safe_reason(error)
            if isinstance(error, SmokeFailure):
                raise
            raise SmokeFailure("F1A_FENCED_RECONCILIATION_SHAPE") from None
        finally:
            self.retain("fenced-reconciliation", report)

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
                    "claim_and_heartbeat_lease_ms": LEASE_MS,
                    "gate_bound_seconds": GATE_SECONDS,
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
        rows, seals = self.witness()
        invocation = active["invocation"]
        same_process = {key: invocation[key] for key in ("pod_uid", "session")}
        inactive = by(rows, event="finish", id=invocation["invocation"], **same_process) + [
            event
            for boundary in ("f1a_activity_release", "f1a_activity_expired")
            for event in by(
                rows, event=boundary, invocation=invocation["invocation"], **same_process
            )
        ]
        if inactive:
            self.retain("invocation-inactive-at-apply", {"rows": inactive, "seals": seals})
        expect(
            "F1A_INVOCATION_ACTIVE_AT_APPLY",
            not inactive,
            True,
        )
        if target.role == "scheduler":
            cut = self.ensure_scheduler_lease(target, workflow)
            self.retain("scheduler-at-apply", cut)
        chaos.apply(lambda: self.reobserve(target))
        assert chaos.issued_ns is not None

        def acknowledge() -> tuple[JsonObject | None, JsonObject]:
            observed = chaos.sample("injection")
            proof = (
                observed if observed and backend_selected(observed, target, peer_names) else None
            )
            label = (
                "F1A_CANDIDATE_BACKEND_ACK_MISSING"
                if observed
                else "F1A_CANDIDATE_BACKEND_CR_ABSENT"
            )
            return proof, {
                "object": observed,
                "candidates": [candidate(1, target.uid, label)] if not proof else [],
            }

        self.poll(acknowledge, "F1A_BACKEND_ACK", time.monotonic() + 18)
        chaos.events()
        rejected = replacement = partition_proof = None
        if partition:
            partition_proof, absent_ns, rejected = self.check_partition(
                chaos, target, active, workflow, endpoints
            )
        else:
            self.watch_kill(target, chaos)
            absent_ns = chaos.remove()
            replacement = self.ready_replacement(target, before_uids, absent_ns)
            if replacement.role == "scheduler":
                self.scheduler_reconnect(replacement, absent_ns)
        final, winning, seals = self.finish(
            workflow, active["invocation"] if target.role == "scheduler" else None
        )
        self.retain("terminal-winner", winning)
        winner = winning["attempt"]
        if target.role == "worker":
            expect(
                "F1A_WINNER_REPLACED",
                winner["attempt_number"] > 1
                and winner["token_hash"] != active["claim"]["token_hash"],
                True,
            )
        if rejected is not None:
            assert partition_proof is not None
            expect(
                "F1A_PARTITION_REPLACEMENT_WON",
                attempt_fields(winner),
                attempt_fields(partition_proof["replacement"]),
            )
            if partition_proof["replacement"]["token_hash"] is not None:
                expect(
                    "F1A_PARTITION_REPLACEMENT_WINNING_TOKEN",
                    winner["token_hash"],
                    partition_proof["replacement"]["token_hash"],
                )
            expect(
                "F1A_RESULT_AFTER_TYPED_REJECTION",
                winning["result"]["sent_ns"] > rejected["at_ns"],
                True,
            )
        fenced = None
        if replacement is not None and replacement.role == "scheduler":
            fenced = self.reconcile_scheduler(cut, final, chaos.issued_ns, target)
        checker = {
            "workflow": workflow,
            "body_hash": fingerprint,
            "registered": coverage["invariants_covered"],
            "not_covered": coverage["invariants_not_covered"],
            "registry_arm": coverage["arm"],
            "arm": row,
            "target": asdict(target),
            "removal_absent_ns": absent_ns,
            "winning_claim": winning["claim"],
            "winning_invocation": winning["invocation"],
            "winning_result": winning["result"],
            "stale_rejection": rejected,
            "fenced_transition": fenced,
            "engine_cut": final,
            "witness": seals,
        }
        self.retain("checker-inputs", checker)
        for topic in (EVENTS, TASKS):
            try:
                self.retain("kafka-final-" + topic, self.kafka(topic))
            except (SmokeFailure, TypeError, ValueError) as error:
                self.retain(
                    "kafka-final-failure",
                    {"topic": topic, "reason": self.evidence.safe_reason(error)},
                )
                raise SmokeFailure(f"F1A_FINAL_KAFKA_SAMPLE {topic}") from None
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
                smoke.deadline = datetime.now(UTC) + timedelta(seconds=120)
                for stage, operation in (
                    ("d-chaos-role-logs", lambda: smoke.collect_role_logs("chaos-mesh", ())),
                    (
                        "d-chaos-cleanup",
                        lambda: smoke.clear_namespace("chaos-mesh", 50),
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
