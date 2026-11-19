"""Hosted engineering smoke for the tagged K6 effect boundaries."""

from __future__ import annotations

import argparse
import copy
import json
import os
import re
import shutil
import time
from collections import Counter
from contextlib import suppress
from functools import partial
from pathlib import Path
from uuid import uuid4

from dur053_k3 import CLUSTER, LOCK, ROOT
from dur053_k3_policy import DIGEST
from dur053_k4_collectors import Streams
from dur053_k4_driver import install_chaos_backend, render_chaos_backend
from dur053_k4_interventions import chaos_ack, kill_proof, ready
from dur053_k6_b_evidence import SmokeFailure
from dur053_k6_b_smoke import ENGINE_ROLE, Smoke, json_data, require, subject
from dur053_k6_c_deploy import ENGINE_PEER, RELAY_PEER, SINK_PEER, encoded
from dur053_k6_c_render import PORT, RELAY, SINK, ip_peer, policy, traffic
from dur053_k6_c_smoke import make_output
from dur053_k6_c_smoke import run as run_hosted
from dur053_k6_d2b_join import exchanges, validate_cell
from dur053_k6_d_render import ACTIVITY, CONTROL, OBSERVER, ROLES, mount, render, set_env
from dur053_k6_d_smoke import NS as D2A_NS
from dur053_k6_d_smoke import SQL, SUBMIT, PhaseDEvidence, collect, definition_sql
from dur053_k6_d_smoke import Matrix as D2aMatrix
from dur053_k6_hook import Receiver, serve
from dur053_k6_identity import APPROVER, REVIEWER, obj
from dur053_k6_join import digest, identity, load_records, receipt_bytes, unique, verify

NS = "dur053-k6-d2b"
TAG = "dur053_k6"
CELLS = {
    "medium-12": ("worker", "worker-pre-send", "killed"),
    "medium-13": ("sink", "sink-post-commit", "release"),
    "medium-14": ("relay", "relay-pre-forward", "duplicate"),
    "medium-15": ("relay", "relay-post-commit", "drop"),
}
CONTROL_CELL = "unarmed-control"
WORKER_ROLES = {"worker": "worker", "sink": SINK, "relay": RELAY}
HOOK_CA = "/k6-hook-ca/ca.crt"
OVERHEAD = "effect-recovery-overhead.json"
TRANSFER = re.compile(
    r"^(?:\[pod/[^\]\r\n]+\] )?DUR053_K6_OBSERVATION k6_transfer (\{[^\n]+\})$", re.M
)
IMAGE_DIGEST = re.compile(r"sha256:[0-9a-f]{64}$")


def chaos_inventory(objects: list[dict], node_count: int) -> tuple[int, int]:
    """Return the expected Chaos Mesh pod and container counts from rendered resources."""
    pods = containers = 0
    for resource in objects:
        if resource.get("kind") not in {"Deployment", "DaemonSet"}:
            continue
        spec = resource["spec"]
        replicas = spec.get("replicas", 1) if resource["kind"] == "Deployment" else node_count
        pod_spec = spec["template"]["spec"]
        pods += replicas
        containers += replicas * len(pod_spec.get("containers", []))
    return pods, containers


def chaos_image_diagnostic(
    objects: list[dict], pods: list[dict], node_count: int, locked_images: set[str]
) -> str | None:
    """Return a credential-free mismatch diagnostic, or None for the exact locked inventory."""
    expected_pods, expected_containers = chaos_inventory(objects, node_count)
    expected_digests = {image.rsplit("@", 1)[1] for image in locked_images if "@" in image}
    running_digests = set()
    ready_count = observed_count = 0
    all_pods_ready = True
    observed = []
    for pod in pods:
        statuses = pod.get("status", {}).get("containerStatuses", [])
        conditions = pod.get("status", {}).get("conditions", [])
        pod_ready = any(
            row.get("type") == "Ready" and row.get("status") == "True" for row in conditions
        )
        all_pods_ready = all_pods_ready and pod_ready
        observed.append(
            {
                "name": pod.get("metadata", {}).get("name"),
                "phase": pod.get("status", {}).get("phase"),
                "ready": pod_ready,
                "containers": [
                    {
                        "image": row.get("image"),
                        "imageID": row.get("imageID"),
                        "ready": row.get("ready", False),
                    }
                    for row in statuses
                ],
            }
        )
        observed_count += len(statuses)
        for row in statuses:
            if row.get("ready"):
                ready_count += 1
                image_id = row.get("imageID", "")
                digest = image_id.rsplit("@", 1)[-1]
                if IMAGE_DIGEST.fullmatch(digest):
                    running_digests.add(digest)
    if (
        len(pods) == expected_pods
        and all_pods_ready
        and observed_count == expected_containers
        and ready_count == expected_containers
        and running_digests == expected_digests
    ):
        return None
    return "D2B_CHAOS_IMAGE_MISMATCH " + json.dumps(
        {
            "expected_pods": expected_pods,
            "expected_containers": expected_containers,
            "expected_digests": sorted(expected_digests),
            "observed_pods": observed,
            "ready_containers": ready_count,
            "observed_containers": observed_count,
            "running_digests": sorted(running_digests),
        },
        sort_keys=True,
        separators=(",", ":"),
    )


class D2bEvidence(PhaseDEvidence):
    schema = "dur053.k6-phase-d2b-smoke.v1"


def plan() -> dict:
    return {
        "namespace": NS,
        "operations": [
            {
                "submission_key": f"d2b-{cell}",
                "definition_id": "d2a-" + ACTIVITY,
                "definition_version": 1,
                "node_id": ACTIVITY,
                "iteration": 0,
                "expected_revision": "0",
                "resource": f"d2b-{cell}-resource",
                "logical_key": f"d2b-{cell}-key",
                "arguments": {"cell": cell},
            }
            for cell in (*CELLS, CONTROL_CELL)
        ],
    }


def hook_pod(item: dict, actor: str, address: str, workload: dict) -> None:
    pod = item["spec"]["template"]["spec"]
    mount(pod, "hook-ca", "/k6-hook-ca", {"configMap": {"name": "k6-hook-ca"}})
    container = pod["containers"][0]
    set_env(container, DUR053_K6_HOOK_URL=address, DUR053_K6_HOOK_CA_FILE=HOOK_CA)
    env = container["env"]
    env[:] = [row for row in env if row["name"] != "POD_UID"]
    env.extend(
        [
            {"name": "POD_UID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}}},
            {
                "name": "DUR053_K6_HOOK_TOKEN",
                "valueFrom": {"secretKeyRef": {"name": f"hook-{actor}", "key": "token"}},
            },
        ]
    )
    if actor == "worker":
        set_env(container, DUR053_K6_HOOK_WORKLOAD=json_data(workload).decode())


def kill_object(worker: dict, name: str, namespace: str = NS) -> dict:
    return {
        "apiVersion": "chaos-mesh.org/v1alpha1",
        "kind": "PodChaos",
        "metadata": {"namespace": namespace, "name": name},
        "spec": {
            "action": "container-kill",
            "mode": "one",
            "selector": {"namespaces": [namespace], "pods": {namespace: [worker["pod"]]}},
            "containerNames": [worker["container"]],
        },
    }


def exact_chaos_ack(status: dict, target: dict, namespace: str = NS) -> bool:
    if not chaos_ack(status):
        return False
    selection = status["status"]["experiment"]["containerRecords"]
    expected = f"{namespace}/{target['pod']}/{target['container']}"
    return [row.get("id") for row in selection] == [expected]


def worker_observations(records: list[dict], raw: list[dict]) -> list[dict]:
    inbound = unique(
        [r for r in records if r["event"] == "inbound"],
        lambda r: r["request_id_hash"],
        "D2B_RELAY_REQUEST_REUSED",
    )
    result = []
    for worker in raw:
        request = inbound.get(worker["request_id_hash"])
        require(
            request is not None
            and request["invocation_hash"] == worker["invocation_hash"]
            and request["body_hash"] == worker["request_hash"],
            "D2B_WORKER_EXCHANGE_IDENTITY",
        )
        result.append({"connection_hash": request["worker_binding"], **worker})
    return result


class Matrix(D2aMatrix):
    namespace = NS
    database = "dur053_k6_d2b"
    sink_container = CLUSTER + "-k6-d2b-sink-db"
    sink_volume = sink_container + "-data"
    inputs_schema = "dur053.k6-phase-d2b-inputs.v1"

    def __init__(self, evidence, scratch, harness):
        super().__init__(evidence, scratch, harness)
        self.hook_log = scratch / "hook.jsonl"
        self.receiver = None
        self.chaos_started = False
        self.worker_logs: dict[str, str] = {}
        self.live_chaos: set[str] = set()
        self.samples: dict[str, dict] = {}
        self.identities_by_cell = {
            op["logical_key"]: {key: str(uuid4()) for key in ("invocation", "apply")}
            for op in self.plan(NS)["operations"]
            if not op["submission_key"].endswith(CONTROL_CELL)
        }

    @staticmethod
    def plan(namespace: str) -> dict:
        require(namespace == NS, "D2B_NAMESPACE")
        return plan()

    def build(self) -> None:
        Smoke.build(self)
        self.build_effect(TAG)
        ordinary = self.harness.images["worker"]
        require(bool(DIGEST.fullmatch(ordinary)), "D2B_WORKER_BASE_DIGEST")
        campaign = "localhost:5001/dur053-k6-worker-campaign:candidate"
        # The ordinary image sees valid-looking campaign settings; its Python
        # search path contains only the normal worker package.
        self.command(
            "D2B_ORDINARY_WORKER_SURFACE",
            "docker",
            "run",
            "--rm",
            "--network=none",
            "-e",
            "DUR053_K6_HOOK_URL=https://127.0.0.1:1",
            "-e",
            "DUR053_K6_HOOK_CA_FILE=/unused",
            "-e",
            "DUR053_K6_HOOK_TOKEN=" + "0" * 64,
            "-e",
            "POD_UID=ordinary",
            "-e",
            "DUR053_K6_HOOK_WORKLOAD={}",
            "--entrypoint",
            "python",
            ordinary,
            "-c",
            "import importlib.util; assert importlib.util.find_spec('dur053_k6_campaign') is None",
        )
        self.command(
            "D2B_CAMPAIGN_WORKER_BUILD",
            "docker",
            "build",
            "--provenance=false",
            "--platform=linux/amd64",
            "--build-arg",
            "WORKER_IMAGE=" + ordinary,
            "-f",
            "deploy/k8s/Dockerfile.k6-worker",
            "-t",
            campaign,
            str(ROOT),
            timeout=300,
        )
        self.command("D2B_CAMPAIGN_WORKER_PUSH", "docker", "push", campaign, timeout=120)
        (self.worker_image,) = json.loads(
            self.command(
                "D2B_CAMPAIGN_WORKER_DIGEST",
                "docker",
                "inspect",
                "--format",
                "{{json .RepoDigests}}",
                campaign,
            )
        )
        require(bool(DIGEST.fullmatch(self.worker_image)), "D2B_CAMPAIGN_WORKER_DIGEST")
        self.command(
            "D2B_CAMPAIGN_WORKER_LOAD",
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
            self.worker_image,
            timeout=120,
        )
        self.evidence.write(
            "campaign-build.json",
            {
                "tag": TAG,
                "images": {
                    "effect": self.image,
                    "worker": self.worker_image,
                    "worker_base": ordinary,
                    "runtime": self.harness.images["runtime"],
                },
                "registry_sha256": digest(
                    (ROOT / "experiments/portfolio/k8s-recovery/k6-registry-v1.json").read_bytes()
                ),
                "binary_sha256": {
                    p.name: digest(p.read_bytes())
                    for p in (self.sink, self.relay, self.witness, self.probe_bin)
                },
            },
        )

    def chaos(self) -> None:
        import yaml

        backend = copy.copy(self.harness)
        backend.scratch = backend.output = self.scratch / "chaos"
        (backend.scratch / "bin").mkdir(parents=True)
        backend.addon_images = render_chaos_backend(backend, backend.scratch, backend.output)
        backend.preload_addons()
        backend.load_addons()
        self.chaos_started = True
        install_chaos_backend(backend, backend.output)
        objects = [
            obj
            for obj in yaml.safe_load_all((backend.output / "chaos-mesh.yaml").read_text())
            if obj
        ]
        nodes = json.loads(self.k("D2B_CHAOS_NODES", "get", "nodes", "-o", "json"))["items"]
        expected_pods, _ = chaos_inventory(objects, len(nodes))
        pods = []
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            pods = json.loads(
                self.k("D2B_CHAOS_IMAGES", "-n", "chaos-mesh", "get", "pods", "-o", "json")
            )["items"]
            if len(pods) == expected_pods and all(
                any(
                    row.get("type") == "Ready" and row.get("status") == "True"
                    for row in pod.get("status", {}).get("conditions", [])
                )
                for pod in pods
            ):
                break
            time.sleep(2)
        diagnostic = chaos_image_diagnostic(objects, pods, len(nodes), backend.addon_images)
        if diagnostic is not None:
            raise SmokeFailure(diagnostic)
        running = {
            row["imageID"]: row["image"]
            for pod in pods
            for row in pod["status"].get("containerStatuses", [])
            if row.get("ready")
        }
        for name in ("chaos-values.json", "chaos-mesh.yaml", "chaos-crds.yaml"):
            self.evidence.write_bytes(name, (backend.output / name).read_bytes())
        self.evidence.write(
            "chaos-backend.json",
            {"version": LOCK["addons"]["chaos"]["version"], "running": running},
        )

    def topology(self, objects: list[dict]) -> list[dict]:
        adjusted = super().topology(objects)
        for item in adjusted:
            if item["kind"] == "Deployment":
                actor = next(
                    k for k, role in WORKER_ROLES.items() if role == item["metadata"]["name"]
                )
                hook_pod(item, actor, self.hook_address, {})
        return adjusted + [
            policy(
                NS,
                "hook-" + actor,
                role,
                "Egress",
                [traffic("to", self.hook_port, ip_peer(self.endpoint_addresses["witness"]))],
            )
            for actor, role in WORKER_ROLES.items()
        ]

    def get(self, kind: str, name: str) -> dict:
        return json.loads(
            self.k("D2B_OBJECT", "-n", NS, "get", kind, name, "-o", "json", timeout=4)
        )

    def process(self, actor: str) -> tuple[dict, dict]:
        role = WORKER_ROLES[actor]
        (pod,) = json.loads(
            self.k(
                "D2B_TARGET_POD",
                "-n",
                NS,
                "get",
                "pod",
                "-l",
                f"app={role}",
                "-o",
                "json",
                timeout=4,
            )
        )["items"]
        (container,) = pod["status"]["containerStatuses"]
        require(
            ready(pod) and container["containerID"].startswith("containerd://"),
            "D2B_TARGET_NOT_RUNNING",
        )
        container_id = container["containerID"].split("://", 1)[1]
        node = CLUSTER + "-control-plane"
        inspected = json.loads(
            self.command(
                "D2B_TARGET_CRI",
                "docker",
                "exec",
                node,
                "crictl",
                "inspect",
                container_id,
                timeout=4,
            )
        )
        pid = inspected["info"]["pid"]
        require(isinstance(pid, int) and pid > 0, "D2B_TARGET_HOST_PID")
        stat = self.command(
            "D2B_PROCESS_START", "docker", "exec", node, "cat", f"/proc/{pid}/stat", timeout=4
        ).decode()
        process = "1:" + stat.rsplit(")", 1)[1].split()[19]
        with self.receiver.lock:
            target = dict(self.receiver.targets.get(actor, {}))
        require(
            target.get("uid") == pod["metadata"]["uid"]
            and target.get("process_start_id") == process,
            "D2B_TARGET_SESSION_UNJOINED",
        )
        return pod, {
            **target,
            "pod": pod["metadata"]["name"],
            "container": container["name"],
            "container_id": container["containerID"],
            "restart_count": container["restartCount"],
            "host_pid": pid,
        }

    def pod_worker_logs(self, target: dict, *, previous: bool = False) -> str:
        return self.k(
            "D2B_WORKER_LOGS",
            "-n",
            NS,
            "logs",
            target["pod"],
            "-c",
            target["container"],
            "--prefix",
            "--tail=-1",
            *(["--previous"] if previous else []),
            timeout=5,
        ).decode()

    def kill(self, cell: str, ack: dict, before: dict, worker: dict, streams: Streams) -> dict:
        return self.kill_container(cell, ack, worker, streams, "d2b")

    def kill_container(
        self,
        cell: str,
        ack: dict,
        worker: dict,
        streams: Streams,
        prefix: str,
        *,
        before_inject=None,
    ) -> dict:
        chaos = kill_object(worker, prefix + "-" + cell, self.namespace)
        name = chaos["metadata"]["name"]
        self.evidence.write(name + "-manifest.json", chaos)
        self.k(
            "D2B_KILL_DRY_RUN",
            "create",
            "--dry-run=server",
            "-f",
            "-",
            data=json_data(chaos),
            timeout=4,
        )
        if before_inject is not None:
            before_inject()
        issued = time.time_ns()
        self.receiver.note(cell, ack, "inject", target=worker, issued_ns=issued)
        self.live_chaos.add(name)
        self.create("D2B_KILL_CREATE", chaos)

        def backend_ack():
            current = self.get("podchaos", name)
            self.evidence.write(name + "-backend-status.json", current)
            return current if exact_chaos_ack(current, worker, self.namespace) else None

        backend = self.wait_for(backend_ack, "D2B_CHAOS_TARGET_ACK_MISSING", seconds=18)
        acknowledged = time.time_ns()

        def process_exit():
            streams.check()
            with streams.lock:
                candidates = [
                    e
                    for e in streams.cri_exits
                    if e["container_id"] == worker["container_id"].split("://", 1)[1]
                ]
            return candidates[0] if len(candidates) == 1 else None

        death = self.wait_for(process_exit, "D2B_CRI_PROCESS_DEATH_MISSING", seconds=10)
        death_ns = int(death["at"] * 10**9)
        require(
            death["exit_status"] == 137
            and death["exit_status_source"] == "explicit"
            and death_ns >= issued,
            "D2B_WRONG_PROCESS_EXIT",
        )
        self.receiver.note(
            cell,
            ack,
            "backend_confirmed",
            backend_uid=backend["metadata"]["uid"],
            acknowledged_ns=acknowledged,
            death_ns=death_ns,
        )
        self.evidence.write(name + "-cri-exit.json", death)
        self.k(
            "D2B_KILL_REMOVE",
            "-n",
            self.namespace,
            "delete",
            "podchaos",
            name,
            "--wait=true",
            "--timeout=12s",
            timeout=15,
        )
        self.live_chaos.remove(name)
        require(
            not self.k(
                "D2B_KILL_ABSENT",
                "-n",
                self.namespace,
                "get",
                "podchaos",
                name,
                "--ignore-not-found",
                "-o",
                "name",
            ),
            "D2B_KILL_STILL_PRESENT",
        )
        result = {"death_ns": death_ns, "acknowledged_ns": acknowledged}
        if prefix != "d2b":
            result.update(issued_ns=issued, death_observed_ns=int(death["received"] * 10**9))
        return result

    def wait_replacement(self, before: dict, target: dict, death: dict) -> dict:
        def replacement():
            after = self.get("pod", target["pod"])
            status = after.get("status", {}).get("containerStatuses", [{}])[0]
            return (
                after
                if status.get("restartCount", 0) > target["restart_count"] and ready(after)
                else None
            )

        after = self.wait_for(replacement, "D2B_REPLACEMENT_NOT_READY", seconds=75)
        term = kill_proof(before, after, True)
        _, current = self.process("worker")
        require(
            current["session"] != target["session"]
            and current["process_start_id"] != target["process_start_id"]
            and current["restart_count"] == target["restart_count"] + 1,
            "D2B_WRONG_REPLACEMENT",
        )
        self.worker_logs[target["container_id"]] = self.pod_worker_logs(target, previous=True)
        return {
            **death,
            "replacement_ns": time.time_ns(),
            "old": target,
            "new": current,
            "termination": {k: term[k] for k in ("exitCode", "signal", "finishedAt") if k in term},
        }

    def ingress(self) -> list[dict]:
        # Partial last rows are ignored while the writer is open. The final
        # sealed reader requires a complete JSONL file and continuous sequence.
        with self.journal.open("rb") as journal:
            data = journal.read()
            os.fsync(journal.fileno())
        return [json.loads(line) for line in data.split(b"\n")[:-1]]

    def intervention(
        self, cell: str, worker_pod: dict, worker: dict, selector: dict, streams: Streams
    ) -> dict:
        actor, boundary, action = CELLS[cell]
        held = self.wait_for(
            lambda: a if (a := self.receiver.observe(cell))["state"] == "held" else None,
            "D2B_DURABLE_HOLD_MISSING",
            seconds=28,
        )
        fields, ack = held["reach"]["fields"], held["ack"]
        _, current = self.process(actor)
        require(
            all(current[k] == v for k, v in held["target"].items())
            and held["boundary"] == boundary,
            "D2B_ACK_WRONG_PROCESS",
        )
        related = [
            r
            for r in self.ingress()
            if r.get("invocation_hash") == identity(selector["invocation"])
        ]
        events = [r["event"] for r in related]
        expected = {
            "medium-12": [],
            "medium-13": ["inbound", "sink_send"],
            "medium-14": ["inbound"],
            "medium-15": ["inbound", "sink_send", "sink_response"],
        }
        require(events == expected[cell], "D2B_PRE_INJECT_INGRESS")
        detail = {"ingress_sequences": [r["sequence"] for r in related]}
        if cell in {"medium-13", "medium-15"}:
            committed = self.export_sink()
            receipt = next(
                (r for r in committed["receipts"] if r["application_id"] == fields["application"]),
                None,
            )
            require(
                receipt is not None
                and receipt["logical_key"] == fields["key"]
                and receipt["scope_hash"] == fields["scope"]
                and digest(receipt_bytes(receipt)) == fields["reply"]
                and any(
                    r["resource_id"] == fields["resource"]
                    and r["logical_key"] == fields["key"]
                    and r["revision"] == receipt["applied_revision"]
                    for r in committed["journal"]
                )
                and any(
                    c["application_id"] == fields["application"]
                    and c["request_id"] == fields["request"]
                    and c["connection_hash"] == fields["binding"]
                    and c["outcome"] == "APPLIED"
                    and c["response_hash"] == fields["reply"]
                    for c in committed["calls"]
                ),
                "D2B_PRE_INJECT_COMMIT_UNOBSERVED",
            )
            detail["commit"] = {
                "application_id": fields["application"],
                "reply_hash": fields["reply"],
                "revision": receipt["applied_revision"],
            }
        death = None
        if cell in {"medium-12", "medium-13"}:
            _, live_worker = self.process("worker")
            require(live_worker == worker, "D2B_CALLER_CHANGED_BEFORE_KILL")
            if cell == "medium-13":
                attempts = [json.loads(m) for m in TRANSFER.findall(self.pod_worker_logs(worker))]
                require(
                    any(
                        r["stage"] == "sent"
                        and r["request"] == identity(fields["request"])
                        and r["session"] == worker["session"]
                        for r in attempts
                    ),
                    "D2B_HELD_CALLER_UNJOINED",
                )
            death = self.kill(cell, ack, worker_pod, worker, streams)
            if cell == "medium-13":

                def closed():
                    return next(
                        (
                            r
                            for r in self.ingress()
                            if r["event"] == "caller_closed"
                            and r["request_id_hash"] == identity(fields["request"])
                        ),
                        None,
                    )

                row = self.wait_for(closed, "D2B_CALLER_CLOSE_MISSING", seconds=5)
                detail["caller_close_sequence"] = row["sequence"]
            else:
                require(
                    not any(
                        r.get("invocation_hash") == identity(fields["invocation"])
                        for r in self.ingress()
                    ),
                    "D2B_KILLED_INVOCATION_SENT",
                )
        self.receiver.authorize(cell, ack, action, evidence=detail)
        return self.wait_replacement(worker_pod, worker, death) if death else {}

    def workflow(self, cell: str, operation: dict, approve, streams: Streams) -> dict:
        key = operation["submission_key"]
        body = dict(
            workflow_id=key,
            namespace=NS,
            submission_key=key,
            payload={},
            definition_id=operation["definition_id"],
            definition_version=1,
            initial_node_id=ACTIVITY,
            initial_input={},
        )
        offered = time.time_ns()
        response = json.loads(
            self.k(
                "D2B_SUBMIT",
                "-n",
                NS,
                "exec",
                "-i",
                "deployment/" + OBSERVER,
                "--",
                "python",
                "-c",
                SUBMIT,
                data=json_data(body),
            )
        )
        require(
            response == {"status": 201, "workflow_id": key, "created": True}, "D2B_SUBMIT_REFUSED"
        )

        def row():
            return self.observer_rows()[key]

        def pending():
            approvals = row()["approvals"]
            return (
                approvals[0]
                if len(approvals) == 1 and approvals[0]["decision"] == "PENDING"
                else None
            )

        pending_approval = self.wait_for(pending, "D2B_PENDING_INTENT_MISSING")
        worker_pod, worker = self.process("worker")
        _, relay = self.process("relay")
        sample = {
            "workflow_id": key,
            "offered_ns": offered,
            "first_worker": worker,
            "relay_session": relay["session"],
        }
        self.samples[cell] = sample
        if cell in CELLS:
            actor, boundary, _ = CELLS[cell]
            _, process = self.process(actor)
            target = {k: process[k] for k in ("actor", "uid", "process_start_id", "session")}
            ids = self.identities_by_cell[operation["logical_key"]]
            selector = {
                "key": operation["logical_key"],
                "invocation": ids["invocation"],
                "request": ids["apply"],
            }
            sample["arm"] = self.receiver.arm(cell, boundary, target, selector, 45)
        approve(operation, pending_approval["intent_id"])
        if cell in CELLS:
            sample.update(self.intervention(cell, worker_pod, worker, selector, streams))

        def terminal():
            current = row()
            if cell in {"medium-12", "medium-13"} and any(
                a["number"] == 2 and a["state"] == "CLAIMED" for a in current["attempts"]
            ):
                sample.setdefault("replacement_claim_observed_ns", time.time_ns())
            require(
                current["state"]
                in {
                    "RUNNABLE",
                    "WAITING_APPROVAL",
                    "WAITING_ACTIVITY",
                    "WAITING_TIMER",
                    "SUCCEEDED",
                },
                "D2B_WORKFLOW_FAILED_OR_PAUSED",
            )
            return current if current["state"] == "SUCCEEDED" else None

        self.wait_for(terminal, "D2B_WORKFLOW_NOT_TERMINAL", seconds=105)
        sample["terminal_ns"] = time.time_ns()
        if cell in CELLS:
            self.wait_for(
                lambda: a if (a := self.receiver.observe(cell))["state"] == "done" else None,
                "D2B_HOOK_RELEASE_MISSING",
                seconds=8,
            )
        _, final = self.process("worker")
        sample["last_worker"] = final
        require(final == sample.get("new", worker), "D2B_UNEXPECTED_WORKER_RESTART")
        self.evidence.write(cell + "-workload.json", sample)
        return sample

    def observer_rows(self) -> dict:
        return {row["workflow_id"]: row for row in json.loads(self.db(SQL.replace(D2A_NS, NS)))}

    def run_matrix(self) -> None:
        with self.evidence.step("a-locked-chaos-backend"):
            self.chaos()
        host = self.endpoint_addresses["witness"]
        self.cert("k6-hook", f"IP:{host}", "serverAuth")
        tokens = {actor: self.password() for actor in WORKER_ROLES}
        self.receiver = Receiver(self.hook_log, tokens)
        try:
            with serve(
                self.receiver, host, self.scratch / "k6-hook.crt", self.scratch / "k6-hook.key"
            ) as port:
                self.hook_port, self.hook_address = port, f"https://{host}:{port}"
                self.workload_d2b(tokens)
        except Exception as error:
            with suppress(Exception):
                self.diagnostics()
            with suppress(Exception):
                logs = self.retain_logs(partial_logs=True)
                if self.samples and not (self.evidence.root / OVERHEAD).exists():
                    observations = collect(
                        *(logs.get(k, "") for k in ("worker", "allocator", "sink", "relay"))
                    )
                    self.write_overhead(
                        "FAIL",
                        {
                            cell: {
                                "status": (
                                    "TERMINAL_UNVERIFIED" if "terminal_ns" in sample else "CENSORED"
                                ),
                                "reason": self.evidence.safe_reason(error),
                                "offered_ns": sample["offered_ns"],
                                "terminal_ns": sample.get("terminal_ns"),
                                "observed_through_ns": time.time_ns(),
                            }
                            for cell, sample in self.samples.items()
                        },
                        observations,
                    )
            raise
        finally:
            for cell in self.receiver.arms:
                with suppress(Exception):
                    self.receiver.observe(cell)
            self.receiver.file.close()
            self.evidence.write_bytes("host-hook-journal.jsonl", self.hook_log.read_bytes())

    def workload_d2b(self, tokens: dict[str, str]) -> None:
        evidence = self.evidence
        with evidence.step("b-private-effect-topology"):
            self.identity_created = True
            migration, episode = self.install_topology()
            self.migrate(NS, migration, "D2B_ENGINE_MIGRATE")
            self.migrate_sink()
            self.export_config()
            initial = self.export_sink()
            require(all(r["count"] == 0 for r in initial["relations"]), "D2B_SINK_NOT_FRESH")
            evidence.write("sink-initial.json", initial)
            seed, public = self.signing_key()
            workload = self.plan(NS)
            plan_bytes = json_data(workload)
            evidence.write(
                "sealed-workload.json",
                {
                    "plan": workload,
                    "sha256": digest(plan_bytes),
                    "identity": self.identities_by_cell,
                },
            )
            for name, data in (
                ("k6-load-plan", {"plan.json": plan_bytes.decode()}),
                ("k6-effect-ca", {"ca.crt": self.effect_ca.read_text()}),
                ("k6-hook-ca", {"ca.crt": self.effect_ca.read_text()}),
            ):
                self.create("D2B_CONFIGMAP", obj("ConfigMap", name, NS, immutable=True, data=data))
            for actor, token in tokens.items():
                self.create(
                    "D2B_HOOK_SECRET",
                    obj(
                        "Secret",
                        "hook-" + actor,
                        NS,
                        immutable=True,
                        type="Opaque",
                        data={"token": encoded(token.encode())},
                    ),
                )
            endpoints = {
                "192.0.2.1": self.address,
                "192.0.2.2": self.instance_address(CLUSTER + "-kafka"),
                "192.0.2.3": self.endpoint_addresses["api_service"],
                "192.0.2.4": self.endpoint_addresses["api_endpoint"],
            }
            manifests = render(
                self.harness.images, endpoints, public, digest(plan_bytes), namespace=NS
            )
            for item in manifests:
                if item["kind"] == "Deployment" and item["metadata"]["name"] == "worker":
                    item["spec"]["template"]["spec"]["containers"][0]["image"] = self.worker_image
                    hook_pod(item, "worker", self.hook_address, self.identities_by_cell)
                if item["kind"] != "Deployment":
                    self.create("D2B_ENGINE_NETWORK", item)
            evidence.write("engine-topology.json", manifests)
            self.db(definition_sql())
        with evidence.step("b-identity-and-tagged-rollout"):
            reviewer = self.reviewer(NS, REVIEWER)
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
            reader, decision = (credentials["ReaderPassword"], credentials["DecisionPassword"])
            for password in (reader, decision):
                evidence.secret(password)
            self.start_witness()
            self.deploy_effect(reviewer, reader, uncertainty, signing_seed=seed)
            allocator = dict(
                SinkURL=f"https://{SINK}:{PORT}",
                SinkPeer=SINK_PEER,
                AllocatorTLS=self.engine_tls,
                WorkerControlTLS=self.cert(CONTROL, "DNS:" + CONTROL, "serverAuth"),
            )
            self.create(
                "D2B_ALLOCATOR",
                obj(
                    "Secret",
                    "k6-allocator",
                    NS,
                    immutable=True,
                    type="Opaque",
                    data={"config.json": encoded(json_data(allocator))},
                ),
            )
        streams = Streams(self.harness, NS, self.scratch / "cri")
        try:
            streams.start_cri()
            with evidence.step("b-campaign-worker-rollout"):
                for item in manifests:
                    if item["kind"] == "Deployment":
                        self.create("D2B_RUNTIME_ROLES", item)
                for role in ROLES:
                    self.ready(role)
                self.wait_for(
                    lambda: {} if "cri-exits" in streams.first_events else None,
                    "D2B_CRI_STREAM_NOT_READY",
                    seconds=5,
                )
                streams.check()
            jail = self.scratch / "jail"
            jail.mkdir(mode=0o700)
            shutil.copy2(self.approver, jail / "approver")
            dsn = self.dsn("k6_decision_writer", decision, "verify-full", str(self.ca))
            decisions = []
            with self.adapter_socket(self.scratch, jail / "live", episode, reviewer, dsn):

                def approve(operation, intent):
                    proposal = dict(
                        Namespace=NS,
                        WorkflowID=operation["submission_key"],
                        NodeID=ACTIVITY,
                        Target=operation["resource"],
                        ResourceRevision="0",
                        Key=operation["logical_key"],
                        Arguments=operation["arguments"],
                    )
                    pending = self.approve(jail, "live", proposal, token)
                    result = self.approve(
                        jail, "live", proposal, token, IntentID=intent, Choice="APPROVED"
                    )
                    answer = result.get("identity", {})
                    require(
                        pending.get("status") == result.get("status") == 200
                        and pending.get("identity", {}).get("intent_id")
                        == answer.get("intent_id")
                        == intent
                        and pending["identity"]["decision"] == "PENDING"
                        and answer.get("decision") == "APPROVED"
                        and answer.get("subject") == subject(NS, APPROVER)
                        and answer.get("proposal_hash") == pending["identity"].get("proposal_hash")
                        and bool(answer.get("decided_at")),
                        "D2B_APPROVAL_IDENTITY",
                    )
                    decisions.append(answer)
                    evidence.write("approvals.json", decisions)

                for cell, op in zip((*CELLS, CONTROL_CELL), workload["operations"], strict=True):
                    with evidence.step("c-" + cell) as row:
                        self.workflow(cell, op, approve, streams)
                        row.update(workflow=op["submission_key"], terminal=True)
            streams.check()
            with evidence.step("d-seal-export-and-join") as row:
                row.update(self.seal_and_verify(workload))
            with evidence.step("e-revoke"):
                self.delete_binding(NS)
        finally:
            streams.close()

    def retain_logs(self, *, partial_logs: bool = False) -> dict:
        logs = {}
        for name, role in (("allocator", CONTROL), ("relay", RELAY), ("sink", SINK)):
            try:
                logs[name] = self.logs(role)
            except Exception:
                if not partial_logs:
                    raise
        try:
            _, worker = self.process("worker")
            self.worker_logs[worker["container_id"]] = self.pod_worker_logs(worker)
        except Exception:
            if not partial_logs:
                raise
        logs["worker"] = "\n".join(self.worker_logs.values())
        self.evidence.retain_role_logs(logs)
        return logs

    def seal_and_verify(self, workload: dict) -> dict:
        def closed_exchanges():
            closes = [r for r in self.ingress() if r["event"] == "close"]
            return {} if len(closes) >= 9 else None  # Four two-call cells and one control.

        self.wait_for(closed_exchanges, "D2B_WITNESS_CLOSE_MISSING", seconds=10)
        logs = self.retain_logs()
        observations = collect(logs["worker"], logs["allocator"], logs["sink"], logs["relay"])
        transfers = [json.loads(body) for body in TRANSFER.findall(logs["worker"])]
        observations["matched_lines"]["worker_transfer"] = len(transfers)
        self.evidence.retain_matched_lines(observations["matched_lines"])
        require(
            not any(r["stage"] == "hook_failed" for r in transfers)
            and "K6_RELAY_HOOK" not in logs["relay"]
            and "K6_SINK_HOOK" not in logs["sink"],
            "D2B_TARGET_HOOK_FAILED",
        )
        self.seal()
        self.stop_witness()
        self.evidence.write_bytes("host-ingress-journal.jsonl", self.journal.read_bytes())
        records = load_records(self.journal)
        snapshot, engine = self.export_sink(), self.observer_rows()
        self.evidence.write("sink-export.json", snapshot)
        self.evidence.write("workflow-outcomes.json", engine)
        self.evidence.write(
            "effect-observations.json", {**observations, "worker_transfers": transfers}
        )
        by_key = {op["logical_key"]: op for op in workload["operations"]}
        faults = {
            identity(self.identities_by_cell[op["logical_key"]]["apply"]): cell
            for cell, op in zip(CELLS, workload["operations"][:-1], strict=True)
        }
        try:
            verdict = verify(
                records,
                snapshot,
                engine_peer=ENGINE_PEER,
                relay_peer=RELAY_PEER,
                sink_peer=SINK_PEER,
                expected_applications=len(by_key),
                allocations=observations["allocations"],
                denied_requests=[],
                worker_observations=worker_observations(records, observations["worker"]),
                exchange_reader=partial(exchanges, faults=faults),
            )
            counts = Counter(row["logical_key"] for row in snapshot["journal"])
            duplicate_effects = sum(max(0, n - 1) for n in counts.values())
            require(
                counts == Counter({key: 1 for key in by_key}) and duplicate_effects == 0,
                "D2B_DUPLICATE_EFFECTS",
            )
            require(
                set(engine) == {s["workflow_id"] for s in self.samples.values()}
                and {r["resource_id"] for r in snapshot["resources"]}
                == {op["resource"] for op in by_key.values()}
                and set(self.receiver.arms) == set(CELLS)
                and len(verdict["session_high_water"]) == 1
                and set(verdict["session_high_water"])
                == {s["relay_session"] for s in self.samples.values()}
                and all(
                    r["sequence"] == sequence for sequence, r in enumerate(self.receiver.rows, 1)
                )
                and not self.live_chaos,
                "D2B_WORKLOAD_INVENTORY",
            )
            metrics = {}
            for cell, operation in zip((*CELLS, CONTROL_CELL), workload["operations"], strict=True):
                canonical = json.dumps(
                    operation["arguments"], sort_keys=True, separators=(",", ":")
                )
                operation = {**operation, "canonical_hash": identity(canonical)}
                metrics[cell] = validate_cell(
                    cell,
                    self.samples[cell],
                    operation,
                    engine[operation["submission_key"]],
                    snapshot,
                    transfers,
                    records,
                    self.receiver.rows,
                    subject(NS, APPROVER),
                )
                key_hash = identity(operation["logical_key"])
                intervals = [
                    (stage, ns)
                    for key, stage, ns in observations["worker_timings"]
                    if key == key_hash
                ]
                delays = [ns for stage, ns in intervals if stage == "delay"]
                lookup_timings = [ns for stage, ns in intervals if stage == "lookup"]
                require(
                    len(delays) == (2 if cell in {"medium-12", "medium-13"} else 1)
                    and all(ns >= 5_000_000_000 for ns in delays)
                    and len(lookup_timings)
                    == (1 if cell in {"medium-12", "medium-13", "medium-15"} else 0),
                    "D2B_WORKER_TIMING_INVENTORY",
                )
                metrics[cell]["lookup_latency_ns"] = lookup_timings
        except ValueError as error:
            raise SmokeFailure(str(error)) from None
        self.write_overhead("PASS", metrics, observations)
        self.evidence.write(
            "join-verdict.json",
            {"status": "PASS", **verdict, "duplicate_effects": duplicate_effects, "cells": metrics},
        )
        return {
            "applications": len(counts),
            "duplicate_effects": duplicate_effects,
            "joined": len(verdict["joined"]),
            "matched_lines": observations["matched_lines"],
        }

    def write_overhead(self, status: str, metrics: dict, observations: dict) -> None:
        timings = [
            dict(logical_key_hash=key, stage=stage, elapsed_ns=ns)
            for key, stage, ns in observations["worker_timings"]
            if stage in {"lookup", "apply"}
        ]
        self.evidence.write(
            OVERHEAD,
            {
                "schema": "dur053.k6-effect-recovery-overhead.v1",
                "status": status,
                "measurement_claim": False,
                "charged_seconds": 0,
                "boundaries": metrics,
                "worker_timings": timings,
            },
        )

    def teardown(self) -> None:
        try:
            for name in sorted(self.live_chaos):
                self.k(
                    "D2B_ABORT_KILL_REMOVE",
                    "-n",
                    NS,
                    "delete",
                    "podchaos",
                    name,
                    "--ignore-not-found",
                    "--timeout=15s",
                )
        finally:
            try:
                super().teardown()
            finally:
                if self.chaos_started:
                    self.k(
                        "D2B_CHAOS_CLEANUP",
                        "delete",
                        "namespace",
                        "chaos-mesh",
                        "--ignore-not-found",
                        "--timeout=45s",
                        timeout=50,
                    )


def inherited_failure(
    output: Path, code: str = "SHARED_KIND_HARNESS_FAILED_BEFORE_PHASE_D2B"
) -> None:
    if not output.exists():
        evidence = D2bEvidence(make_output(output))
        evidence.report.update(reason=code)
        evidence.checkpoint()


def record_handoff_failure(output: Path, error: BaseException) -> None:
    if not output.exists():
        evidence = D2bEvidence(make_output(output))
        with suppress(Exception), evidence.step("phase-d2b-handoff"):
            raise error


def run(output: Path, harness=None, address: str | None = None) -> int:
    code = run_hosted(
        output, harness, address, phase="d2b", evidence_type=D2bEvidence, matrix_type=Matrix
    )
    if code and (output / OVERHEAD).exists():
        path = output / OVERHEAD
        record = json.loads(path.read_bytes())
        record["status"] = "FAIL"
        path.write_bytes(json_data(record))
    report = json.loads((output / "smoke.json").read_bytes())
    print(f"DUR-053 K6 Phase D2b {report['status']} {report['reason']}", flush=True)
    return code


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, required=True)
    raise SystemExit(run(parser.parse_args().output_root))
