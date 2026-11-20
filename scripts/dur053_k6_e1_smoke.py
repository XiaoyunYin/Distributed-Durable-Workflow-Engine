"""Run hosted-only engineering kills at the five deployed PURE boundaries."""

from __future__ import annotations

import copy
import ipaddress
import json
import os
import re
import secrets
import tempfile
import time
from contextlib import ExitStack, contextmanager
from hashlib import sha256
from pathlib import Path
from uuid import UUID

from dur053_k3 import CLUSTER, ROOT
from dur053_k3_policy import DIGEST
from dur053_k4_collectors import Streams
from dur053_k4_interventions import kill_proof, ready
from dur053_k6_b_evidence import Evidence, SmokeFailure
from dur053_k6_b_smoke import Smoke, json_data, private_file, require
from dur053_k6_c_render import ip_peer, policy, traffic
from dur053_k6_c_smoke import make_output
from dur053_k6_d2b_smoke import Matrix as D2bMatrix
from dur053_k6_d2b_smoke import hook_pod
from dur053_k6_d_render import mount, set_env
from dur053_k6_e1_join import (
    EVENTS,
    GROUP,
    SCHEDULERS,
    TASKS,
    base_inputs,
    broker_records,
    by,
    claim_after,
    claim_at_hold,
    expect,
    journal,
    offset_after,
    offset_at_hold,
    one,
    receipt_after,
    receipt_at_hold,
    relay_after,
    relay_at_hold,
)
from dur053_k6_hook import Receiver, serve
from dur053_k6_identity import obj

NS = "dur053-k6-e1"
DB = "dur053_k6_e1"
TAG = "dur053_k6"
ACTORS = ("relay", "worker", "scheduler")
CELLS = {
    "hook-0": ("relay", "before_publish", {"type": "workflow.created"}),
    "hook-1": ("relay", "after_broker_ack", {"type": "workflow.created"}),
    "hook-2": ("worker", "attempt_claimed", {"attempt": "1"}),
    "hook-3": ("worker", "before_offset_ack", {}),
    "hook-4": ("scheduler", "result_recorded", {}),
}
CONTROL = "unarmed-control"
ROLES = ("api", "worker-control", "scheduler", "relay", "worker", "observer")
DEFINITION = "e1-pure-echo"
WORKER_LINE = re.compile(r"^DUR053_K6_OBSERVATION k6_deployed_worker (\{[^\n]+\})$", re.M)
OBSERVER_SCRIPT = """
import json
import sys

def fail(code):
    print(code, file=sys.stderr)
    raise SystemExit(1)

def run(main, unexpected):
    try:
        main()
    except Exception:
        fail(unexpected)
"""
SUBMIT_TRACED = (
    OBSERVER_SCRIPT
    + """
import http.client
import re

def submit():
    traceparent, token_path, address = sys.argv[1:]
    try:
        with open(token_path) as source:
            token = source.read().strip()
    except (OSError, UnicodeError):
        fail("E1_SUBMIT_TOKEN_READ_FAILED")
    if not token:
        fail("E1_SUBMIT_TOKEN_EMPTY")

    connection = http.client.HTTPConnection(address, timeout=5)
    try:
        connection.request(
            "POST", "/v1/workflows", body=sys.stdin.buffer.read(),
            headers={"Authorization": "Bearer " + token, "Content-Type": "application/json",
                     "traceparent": traceparent},
        )
        response = connection.getresponse()
        if response.status != 201:
            fail(f"E1_SUBMIT_HTTP_{response.status}")
        try:
            body = json.load(response)
            workflow = body["workflow"]["workflow_id"]
            created = body["created"]
        except (KeyError, TypeError, ValueError):
            fail("E1_SUBMIT_RESPONSE_INVALID")
        if (not isinstance(workflow, str) or not re.fullmatch(r"[a-z0-9-]{1,64}", workflow)
                or type(created) is not bool):
            fail("E1_SUBMIT_RESPONSE_INVALID")
        print(json.dumps({"status": response.status, "workflow_id": workflow, "created": created}))
    except TimeoutError:
        fail("E1_SUBMIT_HTTP_TIMEOUT")
    except (OSError, http.client.HTTPException):
        fail("E1_SUBMIT_HTTP_UNREACHABLE")
    finally:
        connection.close()

run(submit, "E1_SUBMIT_UNEXPECTED")
"""
)
READ_WITNESS = (
    OBSERVER_SCRIPT
    + r"""
import hashlib
import pathlib

def read_witness():
    workflow, root = sys.argv[1:]
    rows = []
    sessions = {}
    try:
        paths = sorted(pathlib.Path(root).glob("*/*.jsonl"))
    except OSError:
        fail("E1_WITNESS_DIR_UNREADABLE")
    if not paths:
        fail("E1_WITNESS_DIR_EMPTY")
    for path in paths:
        try:
            raw = path.read_bytes()
        except OSError:
            fail("E1_WITNESS_FILE_UNREADABLE")
        lines = raw.splitlines()
        if not lines:
            fail("E1_WITNESS_HEAD_INVALID")
        if not raw.endswith(b"\n"):
            fail("E1_WITNESS_FILE_TRUNCATED")
        try:
            head = json.loads(lines[0])
            if (head["event"] != "session" or not isinstance(head["pod_uid"], str)
                    or not head["pod_uid"] or not isinstance(head["session"], str)
                    or not head["session"]):
                fail("E1_WITNESS_HEAD_INVALID")
        except (KeyError, TypeError, ValueError):
            fail("E1_WITNESS_HEAD_INVALID")
        if path.name in sessions:
            fail("E1_WITNESS_SESSION_FILE_DUPLICATE")
        sessions[path.name] = {"pod_uid": head["pod_uid"], "rows": len(lines),
                               "sha256": hashlib.sha256(raw).hexdigest()}
        for sequence, line in enumerate(lines, 1):
            try:
                row = head if sequence == 1 else json.loads(line)
            except ValueError:
                fail("E1_WITNESS_ROW_INVALID")
            if not isinstance(row, dict):
                fail("E1_WITNESS_ROW_INVALID")
            if (type(row.get("sequence")) is not int or row["sequence"] != sequence
                    or row.get("session") != head["session"]):
                fail("E1_WITNESS_SEQUENCE")
            if row.get("workflow_id") == workflow:
                token = row.pop("claim_token", None)
                if not isinstance(token, str) or not token:
                    fail("E1_WITNESS_ROW_CLAIM_TOKEN")
                try:
                    row["token_hash"] = hashlib.sha256(token.encode()).hexdigest()
                except UnicodeError:
                    fail("E1_WITNESS_ROW_CLAIM_TOKEN_ENCODING")
                rows.append(row)
    print(json.dumps({"rows": rows, "sessions": sessions}))

run(read_witness, "E1_WITNESS_UNEXPECTED")
"""
)
READ_WITNESS_INVENTORY = (
    OBSERVER_SCRIPT
    + """
import http.client

def read_inventory():
    connection = http.client.HTTPConnection(sys.argv[1], timeout=4)
    try:
        connection.request("GET", "/witness")
        response = connection.getresponse()
        if response.status != 200:
            fail(f"E1_WITNESS_HTTP_{response.status}")
        try:
            inventory = json.load(response)
        except ValueError:
            fail("E1_WITNESS_INVENTORY_INVALID")
        fields = {"session": str, "pod_name": str, "pod_uid": str, "file": str,
                  "attempted": int, "written": int, "errors": int, "sha256": str, "complete": bool}
        if not isinstance(inventory, dict) or any(
            type(inventory.get(field)) is not kind for field, kind in fields.items()
        ):
            fail("E1_WITNESS_INVENTORY_INVALID")
        print(json.dumps({field: inventory[field] for field in fields}))
    except TimeoutError:
        fail("E1_WITNESS_HTTP_TIMEOUT")
    except (OSError, http.client.HTTPException):
        fail("E1_WITNESS_HTTP_UNREACHABLE")
    finally:
        connection.close()

run(read_inventory, "E1_WITNESS_INVENTORY_UNEXPECTED")
"""
)


class E1Evidence(Evidence):
    schema = "dur053.k6-phase-e1-smoke.v1"

    def __init__(self, root: Path) -> None:
        self.current_step = "setup"
        self.current_command = ""
        super().__init__(root)

    @contextmanager
    def step(self, name: str):
        previous_step, previous_command = self.current_step, self.current_command
        self.current_step, self.current_command = name, ""
        try:
            with super().step(name) as row:
                yield row
        except BaseException:
            raise
        else:
            self.current_step, self.current_command = previous_step, previous_command

    def safe_reason(self, error: BaseException) -> str:
        if isinstance(error, SmokeFailure):
            return super().safe_reason(error)
        detail = (
            f"UNEXPECTED_{type(error).__name__.upper()}"
            f" step={self.current_step}"
            f" command={self.current_command or 'none'}"
            f" message={str(error)[:300]}"
        )
        try:
            self.check(detail.encode())
        except SmokeFailure:
            return "CREDENTIAL_SCAN_FAILED"
        return detail


def pushed_digest(repo_digests: object, repository: str) -> str:
    require(isinstance(repo_digests, list), "E1_IMAGE_DIGEST_INVALID")
    matches = [
        value
        for value in repo_digests
        if isinstance(value, str) and value.partition("@")[0] == repository
    ]
    require(len(matches) == 1, "E1_IMAGE_DIGEST_AMBIGUOUS")
    digest = matches[0]
    require(bool(DIGEST.fullmatch(digest)), "E1_TAGGED_IMAGE_DIGEST")
    return digest


def e1_addresses(addresses: dict[str, str]) -> dict[str, str]:
    result = {}
    for key in ("192.0.2.1", "192.0.2.2", "192.0.2.3", "192.0.2.4"):
        try:
            result[key] = str(ipaddress.IPv4Address(addresses[key]))
        except (KeyError, ValueError, TypeError):
            raise SmokeFailure("E1_ADDRESS_INVALID") from None
    return result


def ensure_no_e1_placeholders(objects: list[dict]) -> None:
    placeholder_range = ipaddress.IPv4Network("192.0.2.0/24")
    address_pattern = re.compile(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?:/\d{1,2})?(?![\d.])")

    def contains_placeholder(value: object) -> bool:
        if isinstance(value, dict):
            return any(contains_placeholder(child) for child in value.values())
        if isinstance(value, list):
            return any(contains_placeholder(child) for child in value)
        if not isinstance(value, str):
            return False
        for match in address_pattern.finditer(value):
            candidate = match.group()
            try:
                network = ipaddress.ip_network(candidate, strict=False)
            except ValueError:
                continue
            if network.version == 4 and network.overlaps(placeholder_range):
                return True
        return False

    for item in objects:
        if contains_placeholder(item):
            kind = item.get("kind", "unknown")
            name = item.get("metadata", {}).get("name", "unknown")
            raise SmokeFailure(f"E1_PLACEHOLDER_UNRESOLVED {kind}/{name}")


def e1_receiver_address(command, engine_address: str) -> str:
    gateway = (
        command(
            "E1_KIND_BRIDGE_GATEWAY",
            "docker",
            "inspect",
            "--format",
            '{{(index .NetworkSettings.Networks "kind").Gateway}}',
            CLUSTER + "-control-plane",
        )
        .decode()
        .strip()
    )
    try:
        gateway = str(ipaddress.IPv4Address(gateway))
    except ipaddress.AddressValueError:
        raise SmokeFailure("E1_RECEIVER_GATEWAY_INVALID") from None
    require(gateway != engine_address, "E1_RECEIVER_GATEWAY_DB_COLLISION")
    return gateway


def e1_receiver_san(address: str) -> str:
    return f"subjectAltName=IP:{address}"


def e1_receiver_url(address: str, port: int) -> str:
    return f"https://{address}:{port}"


def compact(data) -> str:
    return json.dumps(data, separators=(",", ":"))


def workload(cell: str) -> tuple[dict, str, str]:
    workflow = f"e1-{cell}"
    submission = {
        "definition_id": DEFINITION,
        "definition_version": 1,
        "initial_node_id": "pure.echo",
        "initial_input": {"cell": cell},
        "payload": {},
    }
    digest = sha256(compact(submission).encode()).hexdigest()
    return (
        {**submission, "namespace": NS, "submission_key": workflow, "workflow_id": workflow},
        "sub-v1:" + digest,
        f"00-{digest[:32]}-{digest[32:48]}-01",
    )


def deployment_objects(objects: list[dict], tags: dict, receiver_address: str, port: int) -> list:
    result = []
    for item in json.loads(json.dumps(objects).replace("dur053-kind", NS)):
        kind, name = item["kind"], item["metadata"]["name"]
        if kind in {"ScaledObject", "TriggerAuthentication", "PodDisruptionBudget"}:
            continue
        if kind == "Deployment":
            if name not in ROLES:
                continue
            spec = item["spec"]
            pod = spec["template"]["spec"]
            container = pod["containers"][0]
            if name in ACTORS:
                # The engineering fixture selects exactly one deployed process per
                # role before offering work. The registered matrix is untouched.
                spec["replicas"] = 1
                container["image"] = tags["worker" if name == "worker" else "runtime"]
                hook_pod(item, name, e1_receiver_url(receiver_address, port), {})
            if name == "worker":
                set_env(container, DUR053_K6_DEPLOYED_HOOKS="1")
            if name in {"scheduler", "worker-control"}:
                set_env(container, DUR053_K4_CAMPAIGN="1")
                for variable in ("POD_UID", "CONTAINER_ID"):
                    if not by(container["env"], name=variable):
                        container["env"].append(
                            {
                                "name": variable,
                                "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}},
                            }
                        )
            if name == "observer":
                mount(
                    pod, "witness", "/witness", {"persistentVolumeClaim": {"claimName": "witness"}}
                )
        if kind in {"ClusterRole", "ClusterRoleBinding"}:
            item["metadata"]["name"] += "-e1"
        if kind == "ClusterRoleBinding":
            item["roleRef"]["name"] += "-e1"
            item["subjects"] = [r for r in item["subjects"] if r["name"] != "operator"]
        result.append(item)
    result += [
        policy(
            NS,
            f"e1-hook-{actor}",
            actor,
            "Egress",
            [traffic("to", port, ip_peer(receiver_address))],
        )
        for actor in ACTORS
    ]
    return result


class Matrix(Smoke):
    namespace = NS
    inputs_schema = "dur053.k6-phase-e1-inputs.v1"
    image_repository = "dur053-k6-e1"

    def __init__(self, evidence, scratch, harness, addresses):
        super().__init__(evidence, scratch, harness)
        self.addresses = e1_addresses(addresses)
        self.engine_address = self.addresses["192.0.2.1"]
        self.broker = self.addresses["192.0.2.2"]
        self.receiver_address = ""
        self.chaos_started = self.namespace_started = self.database_started = False
        self.live_chaos: set[str] = set()
        self.previous: dict[str, bytes] = {}
        self.hook_log = scratch / "hook.jsonl"
        self.kafka_probe = scratch / "kafka-read"
        self.tags: dict[str, str] = {}

    def command(self, stage: str, *args: str, **kwargs) -> bytes:
        self.evidence.current_command = stage
        return super().command(stage, *args, **kwargs)

    def build(self) -> None:
        runtime = self.scratch / "runtime"
        for name, path, tags in (
            ("runtime", runtime, ("-tags", TAG)),
            ("dur053-k6-hook-kafka", self.kafka_probe, ()),
        ):
            self.command(
                "E1_GO_BUILD",
                "go",
                "build",
                "-p",
                "1",
                "-buildvcs=false",
                "-trimpath",
                *tags,
                "-o",
                str(path),
                f"./cmd/{name}",
                timeout=300,
            )
        dockerfile = self.scratch / "Dockerfile"
        dockerfile.write_text(
            "ARG BASE\nFROM ${BASE}\nCOPY --chmod=755 runtime /runtime\n", encoding="ascii"
        )
        for role in ("runtime", "worker"):
            base = self.harness.images[role]
            require(bool(DIGEST.fullmatch(base)), "E1_ORDINARY_IMAGE_DIGEST")
            repository = f"localhost:5001/{self.image_repository}-{role}"
            ref = repository + ":candidate"
            if role == "runtime":
                source, arg, context = dockerfile, "BASE", self.scratch
            else:
                source, arg, context = (
                    ROOT / "deploy/k8s/Dockerfile.k6-worker",
                    "WORKER_IMAGE",
                    ROOT,
                )
            self.command(
                "E1_IMAGE_BUILD",
                "docker",
                "build",
                "--provenance=false",
                "--platform=linux/amd64",
                "--build-arg",
                f"{arg}={base}",
                "-f",
                str(source),
                "-t",
                ref,
                str(context),
                timeout=300,
            )
            self.command("E1_IMAGE_PUSH", "docker", "push", ref, timeout=120)
            raw_digests = json.loads(
                self.command(
                    "E1_IMAGE_DIGEST",
                    "docker",
                    "inspect",
                    "--format",
                    "{{json .RepoDigests}}",
                    ref,
                )
            )
            digest = pushed_digest(raw_digests, repository)
            self.command(
                "E1_IMAGE_KIND_LOAD",
                "docker",
                "exec",
                CLUSTER + "-control-plane",
                "ctr",
                "--namespace",
                "k8s.io",
                "images",
                "pull",
                "--hosts-dir",
                "/etc/containerd/certs.d",
                digest,
                timeout=120,
            )
            self.tags[role] = digest
        self.evidence.write(
            "campaign-build.json",
            {
                "tag": TAG,
                "images": self.tags,
                "ordinary_images": self.harness.images,
                "runtime_sha256": sha256(runtime.read_bytes()).hexdigest(),
                "kafka_probe_sha256": sha256(self.kafka_probe.read_bytes()).hexdigest(),
                "registry_sha256": sha256(
                    (ROOT / "experiments/portfolio/k8s-recovery/k6-registry-v1.json").read_bytes()
                ).hexdigest(),
            },
        )

    def tls(self) -> tuple[Path, Path]:
        key, crt = (self.scratch / f"receiver.{suffix}" for suffix in ("key", "crt"))
        private_file(key, b"")
        self.command(
            "E1_TLS",
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=dur053-hook",
            "-addext",
            e1_receiver_san(self.receiver_address),
            "-keyout",
            str(key),
            "-out",
            str(crt),
        )
        self.evidence.secret(key.read_text())
        require(key.stat().st_mode & 0o777 == 0o600, "E1_TLS_KEY_MODE")
        return crt, key

    def pg(self, query: str) -> str:
        return (
            self.command(
                "E1_DB_READ",
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
                DB,
                data=query.encode(),
                timeout=5,
            )
            .decode()
            .strip()
        )

    def setup(self, port: int, cert: Path, tokens: dict) -> None:
        renderer = copy.copy(self.harness)
        renderer.scratch = renderer.output = self.scratch / "render"
        renderer.scratch.mkdir()
        _, source = renderer.render("kind", self.addresses)
        renderer.reset_transport()
        for topic in (EVENTS, TASKS):
            sample = self.kafka(topic)
            self.evidence.write(f"kafka-initial-{topic}.json", sample)
            expect("E1_FRESH_BROKER_TOPIC", broker_records(sample["value"], 8), [])
        objects = deployment_objects(
            source,
            self.tags,
            self.receiver_address,
            port,
        )
        ensure_no_e1_placeholders(objects)
        self.evidence.write("topology.json", objects)
        self.create("E1_NAMESPACE", obj("Namespace", NS, None))
        self.namespace_started = True
        self.sql("E1_DB_CREATE", f'CREATE DATABASE "{DB}";')
        self.database_started = True
        self.harness.databases.add(DB)
        dsn = f"postgresql://durable:{self.harness.password}@postgres:5432/{DB}?sslmode=disable"
        self.create("E1_DB_SECRET", obj("Secret", "campaign-db", NS, stringData={"url": dsn}))
        self.create(
            "E1_HOOK_CA",
            obj("ConfigMap", "k6-hook-ca", NS, immutable=True, data={"ca.crt": cert.read_text()}),
        )
        for actor, token in tokens.items():
            self.create(
                "E1_HOOK_SECRET",
                obj("Secret", f"hook-{actor}", NS, immutable=True, stringData={"token": token}),
            )
        infrastructure = [o for o in objects if o["kind"] not in {"Namespace", "Deployment", "Job"}]
        self.create(
            "E1_INFRASTRUCTURE", {"apiVersion": "v1", "kind": "List", "items": infrastructure}
        )
        for name, ip, service_port in (
            ("postgres", self.engine_address, 5432),
            ("kafka", self.broker, 19092),
        ):
            self.create(
                "E1_ENDPOINT",
                obj(
                    "Endpoints",
                    name,
                    NS,
                    subsets=[
                        {
                            "addresses": [{"ip": ip}],
                            "ports": [{"name": "tcp", "port": service_port, "protocol": "TCP"}],
                        }
                    ],
                ),
            )
        for name in ("migrate", "compatibility"):
            self.create(
                "E1_JOB",
                next(o for o in objects if o["kind"] == "Job" and o["metadata"]["name"] == name),
            )
            self.k(
                "E1_JOB_WAIT",
                "-n",
                NS,
                "wait",
                "--for=condition=complete",
                f"job/{name}",
                "--timeout=180s",
                timeout=190,
            )
        self.pg(f"""
            INSERT INTO engine.workflow_definitions
              (definition_id, version, definition_hash, graph, activity_versions, effect_classes)
            VALUES ('{DEFINITION}', 1, 'e1-pure-echo-v1',
              '{{"entry":"pure.echo","nodes":[{{"id":"pure.echo","kind":"activity","next":"done"}},
                {{"id":"done","kind":"success"}}]}}',
              '{{"pure.echo":"v1"}}', '{{"pure.echo":"PURE_ACTIVITY"}}');
        """)
        self.create(
            "E1_DEPLOYMENTS",
            {
                "apiVersion": "v1",
                "kind": "List",
                "items": [o for o in objects if o["kind"] == "Deployment"],
            },
        )
        for role in ROLES:
            self.k(
                "E1_ROLLOUT",
                "-n",
                NS,
                "rollout",
                "status",
                f"deployment/{role}",
                "--timeout=180s",
                timeout=190,
            )

    def get(self, kind: str, name: str) -> dict:
        return json.loads(self.k("E1_OBJECT", "-n", NS, "get", kind, name, "-o", "json", timeout=4))

    def pods(self, role: str) -> list[dict]:
        return json.loads(
            self.k("E1_PODS", "-n", NS, "get", "pods", "-l", f"app={role}", "-o", "json", timeout=4)
        )["items"]

    def wait_for(self, predicate, code: str, *, seconds: float = 30):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            result = predicate()
            if result and time.monotonic() < deadline:
                return result
            time.sleep(0.25)
        raise SmokeFailure(code)

    def process(self, actor: str) -> tuple[dict, dict]:
        pods = self.pods(actor)
        require(len(pods) == 1, "E1_TARGET_POD_CARDINALITY")
        pod = pods[0]
        containers = pod.get("status", {}).get("containerStatuses", [])
        require(len(containers) == 1, "E1_TARGET_CONTAINER_CARDINALITY")
        container = containers[0]
        require(
            ready(pod) and container["containerID"].startswith("containerd://"),
            "E1_TARGET_NOT_RUNNING",
        )
        digest = container["imageID"].rsplit("@", 1)[-1]
        reference = self.tags["worker" if actor == "worker" else "runtime"].rsplit("@", 1)[-1]
        expect("E1_TARGET_IMAGE_DIGEST", digest, reference)
        cid = container["containerID"].split("://", 1)[1]
        node = pod["spec"]["nodeName"]
        inspected = json.loads(
            self.command(
                "E1_TARGET_CRI", "docker", "exec", node, "crictl", "inspect", cid, timeout=4
            )
        )
        expect("E1_CRI_CONTAINER", inspected["status"]["id"], cid)
        pid = inspected["info"]["pid"]
        require(isinstance(pid, int) and pid > 0, "E1_CRI_PID")
        stat = self.command(
            "E1_PROCESS_START", "docker", "exec", node, "cat", f"/proc/{pid}/stat", timeout=4
        ).decode()
        with self.receiver.lock:
            target = dict(self.receiver.targets.get(actor, {}))
        stat_fields = stat.rsplit(")", 1)
        require(len(stat_fields) == 2, "E1_PROCESS_STAT_FORMAT")
        stat_fields = stat_fields[1].split()
        require(len(stat_fields) > 19, "E1_PROCESS_STAT_FORMAT")
        try:
            session_uuid = str(UUID(target["session"]))
        except (KeyError, ValueError, TypeError):
            raise SmokeFailure("E1_TARGET_SESSION_UUID") from None
        expect(
            "E1_ACK_PROCESS_BINDING",
            (target.get("uid"), target.get("process_start_id")),
            (pod["metadata"]["uid"], "1:" + stat_fields[19]),
        )
        return pod, {
            **target,
            "session_uuid": session_uuid,
            "pod": pod["metadata"]["name"],
            "container": container["name"],
            "container_id": container["containerID"],
            "host_pid": pid,
            "restart_count": container["restartCount"],
        }

    def wait_replacement(self, actor: str, before: dict, target: dict) -> dict:
        def restarted():
            pod = self.get("pod", target["pod"])
            containers = pod.get("status", {}).get("containerStatuses", [])
            if not containers:
                return None
            require(len(containers) == 1, "E1_REPLACEMENT_CONTAINER_CARDINALITY")
            container = containers[0]
            with self.receiver.lock:
                registered = self.receiver.targets.get(actor, {})
            return (
                ready(pod)
                and container.get("restartCount", 0) > target["restart_count"]
                and registered.get("session") != target["session"]
            )

        self.wait_for(restarted, "E1_REPLACEMENT_NOT_READY", seconds=90)
        after, new = self.process(actor)
        term = kill_proof(before, after, True)
        expect("E1_RESTART_COUNT", new["restart_count"], target["restart_count"] + 1)
        expect(
            "E1_REPLACEMENT_PROCESS_CHANGED",
            new["process_start_id"] != target["process_start_id"],
            True,
        )
        self.previous[target["container_id"]] = self.role_log(target["pod"], actor, previous=True)
        return {
            "identity": new,
            "termination": {k: term[k] for k in ("exitCode", "signal", "finishedAt") if k in term},
            "at_ns": time.time_ns(),
        }

    def role_log(self, pod: str, role: str, *, previous: bool = False) -> bytes:
        result = self.k(
            "E1_ROLE_LOG",
            "-n",
            NS,
            "logs",
            pod,
            "-c",
            role,
            *(("--previous",) if previous else ()),
            timeout=8,
        )
        self.evidence.check(result)
        return result

    def observe_worker(self, script: str, code: str, *args: str, data: bytes | None = None):
        stage = f"E1_OBSERVER_{script}"
        output = self.k(
            stage,
            "-n",
            NS,
            "exec",
            "--quiet",
            "-i",
            "deployment/observer",
            "--",
            "python",
            "-c",
            code,
            *args,
            data=data,
            timeout=8,
            kubectl_exec=True,
        )
        try:
            return json.loads(output)
        except ValueError:
            raise SmokeFailure(f"{stage}_OUTPUT_INVALID") from None

    def kafka(self, topic: str, partition: int = -1, group: str = "") -> dict:
        started = time.time_ns()
        result = self.command(
            "E1_KAFKA_DIRECT_READ",
            str(self.kafka_probe),
            "-broker",
            self.broker + ":19092",
            "-topic",
            topic,
            "-partition",
            str(partition),
            "-group",
            group,
            timeout=6,
        )
        return {"started_ns": started, "observed_ns": time.time_ns(), "value": json.loads(result)}

    def snapshot(self, workflow: str, *, lease_writes: bool = False) -> dict:
        expect("E1_SAFE_WORKFLOW", bool(re.fullmatch(r"[a-z0-9-]{1,64}", workflow)), True)
        start = time.time_ns()
        writes = (
            """
            , 'writes', ARRAY(SELECT to_jsonb(writes) FROM k6_e2.writes ORDER BY commit_order),
            'read_only', current_setting('transaction_read_only')
        """
            if lease_writes
            else ""
        )
        rows = json.loads(
            self.pg(f"""
          BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
          WITH w AS (SELECT * FROM engine.workflow_executions WHERE workflow_id = '{workflow}'),
          h AS (SELECT * FROM engine.transition_history WHERE workflow_id = '{workflow}'),
          o AS (SELECT event_id,workflow_id,aggregate_revision,event_type,topic,publish_state,
              relay_attempts,claim_owner,claim_expires_at,published_at,traceparent
            FROM engine.outbox WHERE workflow_id = '{workflow}'),
          a AS (SELECT workflow_id,node_id,iteration,attempt_number,state,is_current,worker_id,
              worker_request_id,heartbeat_deadline,result,effect_class,outcome_disposition,
              encode(sha256(convert_to(claim_token::text,'UTF8')),'hex') token_hash,
              encode(sha256(convert_to(result::text,'UTF8')),'hex') result_hash
            FROM engine.activity_attempts WHERE workflow_id = '{workflow}')
          SELECT jsonb_build_object(
            'db_at', clock_timestamp(),
            'workflow', (SELECT to_jsonb(w) FROM w),
            'history', ARRAY(SELECT to_jsonb(h) FROM h ORDER BY revision),
            'outbox', ARRAY(SELECT to_jsonb(o) FROM o ORDER BY aggregate_revision,event_id),
            'attempts', ARRAY(SELECT to_jsonb(a) FROM a ORDER BY node_id,iteration,attempt_number),
            'inbox', ARRAY(SELECT to_jsonb(i) FROM engine.event_inbox i JOIN o USING (event_id)
              ORDER BY i.consumer_id,i.topic,i.kafka_partition,i.kafka_offset),
            'publications', ARRAY(SELECT to_jsonb(p) FROM engine.outbox_publications p
              JOIN o USING (event_id) ORDER BY p.event_id,p.relay_attempt),
            'claims', ARRAY(SELECT to_jsonb(c) FROM engine.k4_claim_audit c
              WHERE c.workflow_id = '{workflow}' ORDER BY c."order"),
            'result_evidence', ARRAY(SELECT to_jsonb(e) - 'claim_token'
              FROM engine.attempt_result_evidence e WHERE e.workflow_id = '{workflow}'
              ORDER BY e.recorded_at,e.evidence_id),
            'leases', ARRAY(SELECT to_jsonb(l) ||
              jsonb_build_object('live',l.lease_expires_at > clock_timestamp())
              FROM engine.partition_leases l JOIN w USING (partition_id)),
            'audit', ARRAY(SELECT to_jsonb(audit) - 'claim_identity' FROM engine.k4_audit audit
              JOIN w USING (partition_id) WHERE audit.kind = 'acquire'
                OR audit.transition_id IN (SELECT transition_id FROM h) ORDER BY audit.event_order)
            {writes}
          );
          COMMIT;
        """)
        )
        return {**rows, "sample_started_ns": start, "sample_observed_ns": time.time_ns()}

    def partition_db(self, partition: int) -> dict:
        expect("E1_I7_PARTITION", partition in range(8), True)
        start = time.time_ns()
        sample = json.loads(
            self.pg(f"""
          BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
          SELECT jsonb_build_object('db_at',clock_timestamp(),'next_offset',(
            SELECT next_offset FROM engine.consumer_offsets WHERE consumer_id='{GROUP}'
              AND topic='{TASKS}' AND kafka_partition={partition}),
            'rows', ARRAY(
              SELECT to_jsonb(r) FROM (
                SELECT event_id::text,kafka_offset AS offset,disposition,recorded_at
                  FROM engine.event_inbox
                  WHERE consumer_id='{GROUP}' AND topic='{TASKS}' AND kafka_partition={partition}
                    AND disposition<>'QUARANTINED'
                UNION ALL
                SELECT event_id,kafka_offset,'QUARANTINED',recorded_at
                  FROM engine.transport_quarantine
                  WHERE consumer_id='{GROUP}' AND topic='{TASKS}' AND kafka_partition={partition}
              ) r ORDER BY r.offset));
          COMMIT;
        """)
        )
        return {**sample, "started_ns": start, "observed_ns": time.time_ns()}

    def read_journal(self, arm: dict, *, complete: bool = False) -> dict:
        with self.receiver.lock:
            require(self.hook_log.is_file(), "E1_HOST_JOURNAL_MISSING")
            return journal(self.hook_log.read_bytes(), arm, complete=complete)

    def offer(self, cell: str) -> tuple[str, str]:
        payload, body_hash, traceparent = workload(cell)
        started = time.time_ns()
        response = self.observe_worker(
            "SUBMIT",
            SUBMIT_TRACED,
            traceparent,
            "/identity/token",
            "api:8080",
            data=json_data(payload),
        )
        workflow = payload["workflow_id"]
        self.evidence.write(
            f"{cell}-submission.json",
            {
                "offer_ns": started,
                "accepted_ns": time.time_ns(),
                "request": payload,
                "response": response,
                "body_hash": body_hash,
                "traceparent": traceparent,
            },
        )
        expect(
            "E1_OFFER_ACCEPTED", response, {"status": 201, "workflow_id": workflow, "created": True}
        )
        return workflow, body_hash

    def invocations(self, workflow: str) -> dict:
        inventory = self.observe_worker("WITNESS_INVENTORY", READ_WITNESS_INVENTORY, "worker:8081")
        value = self.observe_worker("WITNESS", READ_WITNESS, workflow, "/witness")
        expect("E1_WITNESS_COMPLETE", inventory["complete"], True)
        expect(
            "E1_WITNESS_INVENTORY_JOIN",
            value["sessions"].get(inventory["file"]),
            {
                "pod_uid": inventory["pod_uid"],
                "rows": inventory["written"],
                "sha256": inventory["sha256"],
            },
        )
        return {**value, "current": inventory}

    def terminal(self, workflow: str) -> dict:
        last = {}

        def complete():
            nonlocal last
            last = self.snapshot(workflow)
            return (
                last
                if last["workflow"]
                and last["workflow"]["state"] == "SUCCEEDED"
                and all(
                    item["publish_state"] == "PUBLISHED"
                    and by(last["inbox"], event_id=item["event_id"])
                    for item in last["outbox"]
                )
                else None
            )

        try:
            return self.wait_for(complete, "E1_TERMINAL_OR_TRANSPORT_PENDING", seconds=145)
        finally:
            self.evidence.write(f"{workflow}-final-db.json", last)

    def worker_lines(self, target: dict) -> list[dict]:
        raw = self.role_log(target["pod"], "worker").decode()
        return [json.loads(line) for line in WORKER_LINE.findall(raw)]

    def cell(self, cell: str, streams: Streams) -> dict:
        actor, boundary, selector = CELLS[cell]
        initial_pod, target = self.process(actor)
        identity = {key: target[key] for key in ("actor", "uid", "process_start_id", "session")}
        planned = workload(cell)[0]["workflow_id"]
        arm = self.receiver.arm(
            cell, boundary, identity, {"key": planned, "cell": cell, **selector}, 60
        )
        workflow, payload_hash = self.offer(cell)

        def held():
            live = self.receiver.observe(cell)
            if live["state"] == "expired":
                raise SmokeFailure("E1_HOLD_EXPIRED_BEFORE_ACK")
            return live if live["state"] == "held" else None

        arm = self.wait_for(held, "E1_MATCHING_HELD_ACK_MISSING", seconds=35)
        timeline = self.read_journal(arm)
        objects = timeline["reach"]["fields"]["objects"]
        expect("E1_ACK_LOGICAL_IDENTITY", timeline["reach"]["fields"]["key"], workflow)
        snapshot = self.snapshot(workflow)
        samples: dict = {"db": snapshot, "target": target}
        if cell in ("hook-0", "hook-1"):
            expect("E1_RELAY_SESSION_OWNER", objects["owner"], target["session_uuid"])
            samples["broker"] = self.kafka(EVENTS)
            self.evidence.write(f"{cell}-hold.json", samples)
            before = relay_at_hold(cell, workflow, objects, snapshot, samples["broker"]["value"])
        elif cell == "hook-2":
            samples["witness"] = self.invocations(workflow)
            samples["worker_control_pods"] = [
                {"pod": pod["metadata"]["name"], "uid": pod["metadata"]["uid"]}
                for pod in self.pods("worker-control")
                if ready(pod)
            ]
            self.evidence.write(f"{cell}-hold.json", samples)
            expect("E1_HELD_WITNESS_UID", samples["witness"]["current"]["pod_uid"], target["uid"])
            before = claim_at_hold(snapshot, objects, target, NS, samples["witness"]["rows"])
            one(
                "E1_WORKER_CONTROL_AUDIT_PRODUCER",
                by(samples["worker_control_pods"], uid=snapshot["claims"][0]["pod_uid"]),
            )
        elif cell == "hook-3":
            try:
                part = int(objects["partition"])
            except (KeyError, ValueError, TypeError):
                raise SmokeFailure("E1_KAFKA_PARTITION_INVALID") from None
            samples["offset_db"] = self.partition_db(part)
            samples["broker_log"] = self.kafka(TASKS, part)
            samples["broker_group"] = self.kafka(TASKS, part, GROUP)
            self.evidence.write(f"{cell}-hold.json", samples)
            before = offset_at_hold(
                objects,
                samples["offset_db"],
                samples["broker_group"]["value"],
                samples["broker_log"]["value"],
            )
        else:

            def ingested():
                current = self.snapshot(workflow)
                events = by(current["outbox"], event_type="activity.result")
                return (
                    current
                    if events
                    and by(current["inbox"], event_id=events[0]["event_id"], consumer_id=SCHEDULERS)
                    else None
                )

            snapshot = self.wait_for(ingested, "E1_SCHEDULER_RESULT_INBOX_NOT_VISIBLE", seconds=6)
            samples["db"] = snapshot
            samples["activity_witness"] = self.observe_worker(
                "WITNESS", READ_WITNESS, workflow, "/witness"
            )
            self.evidence.write(f"{cell}-hold.json", samples)
            before = receipt_at_hold(objects, snapshot, target)
            rows = samples["activity_witness"]["rows"]
            start = one(
                "E1_RECEIPT_ACTIVITY_INVOCATION",
                by(rows, event="start", token_hash=objects["token_hash"]),
            )
            one(
                "E1_RECEIPT_ACTIVITY_FINISH",
                by(rows, event="end", invocation_id=start["invocation_id"], outcome="SUCCEEDED"),
            )
            before["activity_invocation_id"] = start["invocation_id"]

        def verify_target():
            _, current = self.process(actor)
            expect("E1_EXACT_KILL_TARGET_RECHECK", current, target)
            expect("E1_STILL_HELD_BEFORE_KILL", self.receiver.observe(cell)["state"], "held")
            streams.check()

        death = D2bMatrix.kill_container(
            self, cell, arm["ack"], target, streams, "e1", before_inject=verify_target
        )
        self.receiver.note(
            cell,
            arm["ack"],
            "process_exit",
            observed_ns=death["death_observed_ns"],
            backend_ns=death["death_ns"],
            target=target,
        )
        self.receiver.authorize(
            cell,
            arm["ack"],
            "killed",
            evidence={"hold_ended_by": "process_exit", "container_id": target["container_id"]},
        )
        replacement = self.wait_replacement(actor, initial_pod, target)
        new = replacement["identity"]
        self.evidence.write(f"{cell}-replacement.json", {**death, **replacement})
        after = self.terminal(workflow)
        witness = self.invocations(workflow)
        self.evidence.write(f"{cell}-invocations.json", witness)
        predicates = base_inputs(cell, workflow, payload_hash, after, witness["rows"])
        if cell in ("hook-0", "hook-1"):
            broker = self.kafka(EVENTS)
            self.evidence.write(f"{cell}-recovery-broker.json", broker)
            proof = relay_after(objects, before, after, broker["value"], new)
            last = proof["copies"][-1]

            def replay_committed():
                sample = self.kafka(EVENTS, last["partition"], SCHEDULERS)
                self.evidence.write(f"{cell}-recovery-consumer.json", sample)
                return sample if sample["value"]["next"] > last["offset"] else None

            proof["broker_consumer"] = self.wait_for(
                replay_committed, "E1_REPLAY_BROKER_COMMIT_MISSING", seconds=12
            )
        elif cell == "hook-2":
            old_witness = samples["witness"]["current"]
            expect(
                "E1_DEAD_WITNESS_PREFIX",
                witness["sessions"].get(old_witness["file"]),
                samples["witness"]["sessions"][old_witness["file"]],
            )
            expect(
                "E1_FRESH_WORKER_WITNESS",
                witness["current"]["session"] != old_witness["session"],
                True,
            )
            proof = claim_after(before, after, witness["rows"], new)
            proof["replacement_entry"] = one(
                "E1_REPLACEMENT_ACTIVITY_ENTRY",
                by(
                    self.worker_lines(new),
                    boundary="activity_start",
                    session=new["session"],
                    invocation=proof["start"]["invocation_id"],
                    token_hash=proof["receipt"]["token_hash"],
                ),
            )
        elif cell == "hook-3":
            sample = self.kafka(TASKS, part, GROUP)
            lower = self.partition_db(part)
            lines = self.worker_lines(new)
            self.evidence.write(
                f"{cell}-recovery-offset.json",
                {"broker": sample, "db": lower, "worker_lines": lines},
            )
            proof = offset_after(objects, before, lower, sample["value"], lines, new)
            predicates["predicates"]["I7"] = proof
        else:
            proof = receipt_after(before, after, new)
            proof["activity_invocation_id"] = predicates["predicates"]["I4"]["invocation"]
            expect(
                "E1_RECEIPT_SAME_ACTIVITY_INVOCATION",
                proof["activity_invocation_id"],
                before["activity_invocation_id"],
            )
        with self.receiver.lock:
            self.receiver.append("recovery", arm=arm["arm"], replacement=new, workflow=workflow)
        completed = self.read_journal(arm, complete=True)
        self.evidence.write(
            f"{cell}-joined.json",
            {"timeline": completed, "hold": before, "recovery": proof, **predicates},
        )
        return {"workflow": workflow, "boundary": boundary, "registered": predicates["registered"]}

    def control(self) -> dict:
        before = {actor: self.process(actor)[1] for actor in ACTORS}
        with self.receiver.lock:
            slots = copy.deepcopy(self.receiver.arms)
            sequence = len(self.receiver.rows)
        workflow, payload_hash = self.offer(CONTROL)
        after = self.terminal(workflow)
        witness = self.invocations(workflow)
        expect(
            "E1_UNARMED_PROCESS_CONTINUITY",
            {actor: self.process(actor)[1] for actor in ACTORS},
            before,
        )
        with self.receiver.lock:
            expect("E1_UNARMED_ARM_INVENTORY", self.receiver.arms, slots)
            expect("E1_UNARMED_HOST_JOURNAL", self.receiver.rows[sequence:], [])
        backend = json.loads(
            self.k("E1_UNARMED_BACKEND", "-n", NS, "get", "podchaos", "-o", "json")
        )
        expect("E1_UNARMED_BACKEND_EMPTY", backend["items"], [])
        result = base_inputs(CONTROL, workflow, payload_hash, after, witness["rows"])
        self.evidence.write(
            "unarmed-control-joined.json", {"processes": before, "invocations": witness, **result}
        )
        return {"workflow": workflow, "registered": result["registered"]}

    def diagnostics(self) -> None:
        if self.hook_log.exists():
            with self.receiver.lock:
                self.evidence.write_bytes("host-hook-journal.jsonl", self.hook_log.read_bytes())
        counts = {}
        for old, data in self.previous.items():
            name = old.split("://", 1)[1]
            self.evidence.write_bytes(f"previous-{name}.log", data)
            counts[f"previous-{name}"] = len(WORKER_LINE.findall(data.decode(errors="replace")))
        if self.namespace_started:
            for role in ROLES:
                for pod in self.pods(role):
                    name = pod["metadata"]["name"]
                    try:
                        data = self.role_log(name, role)
                    except SmokeFailure as error:
                        self.evidence.write(
                            f"{name}-log-error.json", {"reason": self.evidence.safe_reason(error)}
                        )
                        continue
                    self.evidence.write_bytes(f"{name}.log", data)
                    counts[name] = len(WORKER_LINE.findall(data.decode(errors="replace")))
        self.evidence.write("matched-lines.json", counts)

    def teardown(self, stack: ExitStack) -> None:
        with ExitStack() as cleanup:
            if self.chaos_started:
                cleanup.callback(
                    self.k,
                    "E1_CHAOS_CLEANUP",
                    "delete",
                    "namespace",
                    "chaos-mesh",
                    "--ignore-not-found",
                    "--timeout=45s",
                    timeout=50,
                )
            if self.database_started:
                cleanup.callback(self.harness.drop, DB)
            if self.namespace_started:
                cleanup.callback(
                    self.k,
                    "E1_REVIEWER_CLEANUP",
                    "delete",
                    "clusterrolebinding",
                    "dur053-tokenreview-e1",
                    "--ignore-not-found",
                )
                cleanup.callback(
                    self.k,
                    "E1_REVIEWER_ROLE_CLEANUP",
                    "delete",
                    "clusterrole",
                    "dur053-tokenreview-e1",
                    "--ignore-not-found",
                )
                cleanup.callback(
                    self.k,
                    "E1_NAMESPACE_CLEANUP",
                    "delete",
                    "namespace",
                    NS,
                    "--ignore-not-found",
                    "--timeout=90s",
                    timeout=95,
                )
            cleanup.callback(stack.close)
            for name in sorted(self.live_chaos):
                self.k(
                    "E1_ABORT_REMOVE",
                    "-n",
                    NS,
                    "delete",
                    "podchaos",
                    name,
                    "--ignore-not-found",
                    "--timeout=12s",
                    timeout=15,
                )


def inherited_failure(
    output: Path, code: str = "SHARED_KIND_HARNESS_FAILED_BEFORE_PHASE_E1"
) -> None:
    if not output.exists():
        evidence = E1Evidence(make_output(output))
        evidence.report.update(reason=code)
        evidence.checkpoint()


def record_handoff_failure(output: Path, error: BaseException) -> None:
    if not output.exists():
        evidence = E1Evidence(make_output(output))
        evidence.report.update(reason=evidence.safe_reason(error))
        evidence.checkpoint()


def run(output: Path, harness, addresses: dict[str, str]) -> int:
    evidence = E1Evidence(make_output(output))
    stack = ExitStack()
    smoke = None
    failure = None
    try:
        with evidence.step("a-private-scratch"):
            require(
                os.name == "posix" and bool(os.getenv("RUNNER_TEMP")),
                "HOSTED_LINUX_RUNNER_REQUIRED",
            )
            scratch = Path(
                stack.enter_context(
                    tempfile.TemporaryDirectory(
                        prefix="dur053-k6-e1-", dir=os.environ["RUNNER_TEMP"]
                    )
                )
            )
        with evidence.step("a-build-tagged-images"):
            smoke = Matrix(evidence, scratch, harness, addresses)
            smoke.build()
        with evidence.step("a-locked-chaos-backend"):
            D2bMatrix.chaos(smoke)
        with evidence.step("b-private-engine-topology"):
            smoke.receiver_address = e1_receiver_address(smoke.command, smoke.engine_address)
            tokens = {actor: secrets.token_hex(32) for actor in ACTORS}
            for token in tokens.values():
                evidence.secret(token)
            cert, key = smoke.tls()
            smoke.receiver = Receiver(smoke.hook_log, tokens)
            stack.callback(smoke.receiver.file.close)
            port = stack.enter_context(serve(smoke.receiver, smoke.receiver_address, cert, key))
            smoke.setup(port, cert, tokens)
            streams = Streams(harness, NS, scratch / "streams")
            stack.callback(streams.close)
            streams.start_cri()
            for actor in ACTORS:
                smoke.process(actor)
        for cell in (*CELLS, CONTROL):
            if failure:
                evidence.report["outcomes"].append(
                    {"step": cell, "status": "SKIPPED", "reason": "EARLIER_E1_CELL_FAILED"}
                )
                evidence.checkpoint()
                continue
            try:
                with evidence.step(cell) as row:
                    row.update(smoke.control() if cell == CONTROL else smoke.cell(cell, streams))
            except Exception as error:
                failure = evidence.safe_reason(error)
    except (Exception, KeyboardInterrupt) as error:
        failure = evidence.safe_reason(error)
    finally:
        attempted = {row["step"] for row in evidence.report["outcomes"]}
        for cell in (*CELLS, CONTROL):
            if cell not in attempted:
                evidence.report["outcomes"].append(
                    {"step": cell, "status": "SKIPPED", "reason": "E1_SETUP_INCOMPLETE"}
                )
        operations = []
        if smoke is not None:
            operations += [
                ("d-retain-role-logs", smoke.diagnostics),
                ("d-cleanup", lambda: smoke.teardown(stack)),
            ]
        else:
            operations.append(("d-cleanup", stack.close))
        operations.append(("d-credential-scan", evidence.scan))
        for name, operation in operations:
            try:
                with evidence.step(name):
                    operation()
            except Exception as error:
                failure = failure or evidence.safe_reason(error)
        evidence.report.update(
            status="FAIL" if failure else "PASS", reason=failure or "MATRIX_COMPLETE"
        )
        evidence.checkpoint()
    print(
        f"DUR-053 K6 Phase E1 {evidence.report['status']} {evidence.report['reason']}", flush=True
    )
    return int(failure is not None)
