"""Hosted-only K6 Phase D2a cooperating Kafka workflows and effect overhead."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import tarfile
import time
from collections import Counter
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path

from dur053_k3 import CLUSTER
from dur053_k6_b_evidence import Evidence, SmokeFailure
from dur053_k6_b_smoke import ENGINE_ROLE, json_data, require, subject
from dur053_k6_c_deploy import (
    ENGINE_PEER,
    RELAY_PEER,
    SINK_PEER,
    Deployment,
    encoded,
)
from dur053_k6_c_render import PORT, RELAY, SINK
from dur053_k6_c_smoke import make_output
from dur053_k6_c_smoke import run as run_hosted
from dur053_k6_d_render import (
    ACTIVITY,
    CASES,
    CONTROL,
    NS,
    OBSERVER,
    ROLES,
    compatibility_job,
    effect_topology,
    render,
)
from dur053_k6_identity import APPROVER, REVIEWER, obj
from dur053_k6_join import EXCHANGE, digest, identity, load_records, unique, verify
from dur053_k6_overhead import measurement

OVERHEAD = "effect-overhead.json"
OLD = "bc6ecea57e19c2168dbe3edd6143c55cbddb0ad3"  # Phase C merge, before D1 registration.
PURE, EFFECT, FORWARD = "pure", "effect", "forward-control"
SQL = f"""
SELECT coalesce(json_agg(row_to_json(t) ORDER BY t.submission_key), '[]'::json)
FROM (
  SELECT w.workflow_id, w.submission_key, w.state, n.accepted_result AS result,
    (SELECT count(*) FROM engine.transition_history h
       WHERE h.workflow_id=w.workflow_id AND h.reason='APPROVAL_GRANTED') AS grant_transitions,
    (SELECT coalesce(json_agg(json_build_object('intent_id', i.intent_id, 'decision', i.decision,
       'status', i.dispatch_status, 'approver', i.approver_id, 'key', i.k6_logical_key,
       'target', i.target, 'scope_hash', i.grant_scope_hash)), '[]'::json)
       FROM engine.approval_action_intents i WHERE i.workflow_id=w.workflow_id) AS approvals,
    (SELECT coalesce(json_agg(json_build_object('number', a.attempt_number, 'state', a.state,
       'class', a.effect_class, 'fence', a.k6_fence, 'proof_operation', a.k6_proof->>'operation',
       'signed', coalesce(length(a.k6_proof->>'signature')=86,false))), '[]'::json)
       FROM engine.activity_attempts a WHERE a.workflow_id=w.workflow_id) AS attempts
  FROM engine.workflow_executions w JOIN engine.node_instances n USING (workflow_id)
  WHERE w.namespace='{NS}' AND n.node_id <> 'done'
) t;
"""
SUBMIT = """
import http.client,json,re,sys
c=http.client.HTTPConnection("api",8080,timeout=5)
try:
    with open("/identity/token") as f:
        token=f.read().strip()
    c.request("POST","/v1/workflows",body=sys.stdin.buffer.read(),
              headers={"Authorization":"Bearer "+token,"Content-Type":"application/json"})
    r=c.getresponse()
    if r.status == 201:
        body=json.load(r)
        print(json.dumps({"status":r.status,"workflow_id":body["workflow"]["workflow_id"],
                          "created":body["created"]}))
    else:
        code=json.load(r).get("error",{}).get("code","")
        safe=code if re.fullmatch("[A-Z_]{1,50}",code) else ""
        print(json.dumps({"status":r.status,"code":safe}))
finally:
    c.close()
"""


class PhaseDEvidence(Evidence):
    schema = "dur053.k6-phase-d-smoke.v1"

    def retain_role_logs(self, logs: dict[str, str]) -> None:
        self.role_logs = logs

    def retain_matched_lines(self, counts: dict[str, int]) -> None:
        self.matched_lines = counts

    def scan(self) -> None:
        for _role, logs in getattr(self, "role_logs", {}).items():
            self.check(logs.encode())
        super().scan()
        counts = getattr(self, "matched_lines", None)
        if counts is not None:
            row = next(
                (
                    outcome
                    for outcome in self.report["outcomes"]
                    if outcome.get("step") == "d-seal-export-and-join"
                ),
                None,
            )
            if row is not None:
                row["matched_lines"] = counts
                self.checkpoint()
        for role, logs in getattr(self, "role_logs", {}).items():
            self.write_bytes(f"role-logs-{role}.log", logs.encode())


def plan() -> tuple[dict, dict[str, list[dict]]]:
    blocks = {}
    operations = []
    for name in (PURE, EFFECT, FORWARD):
        block = []
        for position in range(10):
            key = f"d2a-{name}-{position}"
            effect = name != PURE and position < 8
            node = ACTIVITY if effect else "pure.add" if position == 9 else "pure.echo"
            argument = [2, 3] if position == 9 else {"position": position, "value": "k6"}
            definition = "d2a-" + node
            submit = {
                "workflow_id": key,
                "namespace": NS,
                "submission_key": key,
                "payload": {},
                "definition_id": definition,
                "definition_version": 1,
                "initial_node_id": node,
                "initial_input": {} if effect else argument,
            }
            if effect:
                operations.append(
                    {
                        "submission_key": key,
                        "definition_id": definition,
                        "definition_version": 1,
                        "node_id": node,
                        "iteration": 0,
                        "expected_revision": "0",
                        "resource": key + "-resource",
                        "logical_key": key + "-key",
                        "arguments": argument,
                    }
                )
            block.append(submit)
        blocks[name] = block
    return {"namespace": NS, "operations": operations}, blocks


def definition_sql() -> str:
    values = []
    for name, effect_class in (
        (ACTIVITY, "COOPERATING_EFFECT"),
        ("pure.echo", "PURE_ACTIVITY"),
        ("pure.add", "PURE_ACTIVITY"),
    ):
        graph = {
            "entry": name,
            "nodes": [{"id": name, "next": "done"}, {"id": "done", "kind": "success"}],
        }
        documents = [json_data(doc).decode() for doc in (graph, {name: "v1"}, {name: effect_class})]
        checksum = identity("".join(documents))
        values.append(
            "("
            + ",".join(f"'{value}'" for value in ("d2a-" + name, "1", checksum, *documents))
            + ")"
        )
    return (
        "INSERT INTO engine.workflow_definitions "
        "(definition_id,version,definition_hash,graph,activity_versions,effect_classes) VALUES "
        + ",".join(values)
    )


def worker_observations(records: list[dict], raw: list[dict]) -> list[dict]:
    """Bridge Python's independent bytes/identity observation to the relay's TLS binding."""
    inbound = unique(
        [row for row in records if row["event"] == "inbound"],
        lambda row: row["request_id_hash"],
        "D_RELAY_REQUEST_ID_REUSED",
    )
    observations = []
    for worker in raw:
        row = inbound.pop(worker["request_id_hash"], None)
        require(
            row is not None
            and row["invocation_hash"] == worker["invocation_hash"]
            and row["body_hash"] == worker["request_hash"],
            "D_WORKER_EXCHANGE_IDENTITY",
        )
        observations.append({"connection_hash": row["worker_binding"], **worker})
    require(not inbound, "D_WORKER_EXCHANGE_MISSING")
    return observations


def verify_work(operations: list[dict], engine: list[dict], sink: dict) -> None:
    rows = unique(engine, lambda row: row["submission_key"], "D_DUPLICATE_SUBMISSION")
    receipts = unique(sink["receipts"], lambda row: row["workflow_id"], "D_DUPLICATE_RECEIPT")
    require(len(operations) == len(receipts), "D_RECEIPT_COUNT")
    for operation in operations:
        key = operation["submission_key"]
        row, receipt = rows[key], receipts[key]
        require(
            row["grant_transitions"] == 1
            and row["approvals"]
            == [
                {
                    "intent_id": receipt["intent_id"],
                    "decision": "APPROVED",
                    "status": "DISPATCHED",
                    "approver": subject(NS, APPROVER),
                    "key": operation["logical_key"],
                    "target": operation["resource"],
                    "scope_hash": receipt["scope_hash"],
                }
            ]
            and receipt["logical_key"] == operation["logical_key"]
            and receipt["resource_id"] == operation["resource"]
            and receipt["canonical_hash"]
            == identity(json.dumps(operation["arguments"], sort_keys=True, separators=(",", ":")))
            and row["attempts"]
            == [
                {
                    "number": 1,
                    "state": "SUCCEEDED",
                    "class": "COOPERATING_EFFECT",
                    "fence": 1,
                    "proof_operation": "apply",
                    "signed": True,
                }
            ]
            and row["result"] == receipt,
            "D_EFFECT_RECEIPT_GRANT_OR_FENCE",
        )
    require(
        Counter(call["operation"] for call in sink["calls"])
        == {"allocate": len(operations), "apply": len(operations)}
        and all(
            call["outcome"] == ("ALLOCATED" if call["operation"] == "allocate" else "APPLIED")
            for call in sink["calls"]
        ),
        "D_FORWARD_ONLY_DELIVERY_COUNTS",
    )


class Matrix(Deployment):
    namespace = NS
    database = "dur053_k6_d"
    sink_container = CLUSTER + "-k6-d-sink-db"
    sink_volume = sink_container + "-data"
    inputs_schema = "dur053.k6-phase-d-inputs.v1"
    live_worker = True

    def __init__(self, evidence, scratch, harness):
        super().__init__(evidence, scratch, harness)
        # The historical image build precedes the existing B warmup/cutoff gate.
        self.deadline = datetime.now(UTC) + timedelta(minutes=35)

    @staticmethod
    def plan(namespace: str) -> dict:
        require(namespace == NS, "D_PLAN_NAMESPACE")
        return plan()[0]

    def topology(self, objects: list[dict]) -> list[dict]:
        return effect_topology(objects)

    def build_old_images(self) -> dict:
        archive = self.scratch / "phase-c.tar"
        source = self.scratch / "phase-c"
        self.command(
            "D_OLD_SOURCE",
            "git",
            "archive",
            "--format=tar",
            "-o",
            str(archive),
            OLD,
            "go.mod",
            "go.sum",
            "cmd",
            "internal",
            "migrations",
            "python",
            "requirements-worker.txt",
            "deploy/k8s/Dockerfile",
        )
        with tarfile.open(archive) as tar:
            tar.extractall(source, filter="data")
        archive.unlink()
        images = dict(self.harness.images)
        for target in ("runtime", "worker"):
            tag = f"localhost:5001/dur053-k6-old-{target}:candidate"
            self.command(
                "D_OLD_IMAGE_BUILD",
                "docker",
                "build",
                "--provenance=false",
                "--platform=linux/amd64",
                "--target",
                target,
                "-t",
                tag,
                "-f",
                str(source / "deploy/k8s/Dockerfile"),
                str(source),
                timeout=540,
            )
            self.command("D_OLD_IMAGE_PUSH", "docker", "push", tag, timeout=120)
            (ref,) = json.loads(
                self.command(
                    "D_OLD_IMAGE_DIGEST",
                    "docker",
                    "inspect",
                    "--format",
                    "{{json .RepoDigests}}",
                    tag,
                )
            )
            require(
                bool(re.fullmatch(r"localhost:5001/[\w-]+@sha256:[0-9a-f]{64}", ref)),
                "D_OLD_IMAGE_DIGEST",
            )
            self.command(
                "D_OLD_IMAGE_LOAD",
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
                ref,
                timeout=120,
            )
            images["old_" + target] = ref
        return images

    def db(self, sql: str) -> bytes:
        return self.command(
            "D_ENGINE_OBSERVER",
            "docker",
            "exec",
            "-i",
            CLUSTER + "-postgres",
            "psql",
            "-X",
            "-v",
            "ON_ERROR_STOP=1",
            "-At",
            "-U",
            ENGINE_ROLE,
            "-d",
            self.database,
            data=sql.encode(),
            timeout=8,
        )

    def wait_for(self, condition, code: str, *, seconds: int = 70) -> dict:
        started = time.monotonic_ns()
        while time.monotonic_ns() - started < seconds * 10**9:
            result = condition()
            if result is not None:
                return result
            time.sleep(0.2)
        raise SmokeFailure(code)

    def pod(self, selector: str) -> dict:
        items = json.loads(
            self.k("D_POD_STATUS", "-n", NS, "get", "pod", "-l", selector, "-o", "json")
        )["items"]
        require(len(items) <= 1, "D_UNEXPECTED_REPLICAS")
        return items[0] if items else {}

    def compatibility(self, images: dict) -> None:
        for name in CASES:
            with self.evidence.step("compatibility-" + name) as row:
                job = compatibility_job(images, name)
                self.create("D_COMPATIBILITY_CREATE", job)
                selector = "job-name=" + job["metadata"]["name"]

                def terminated(selector=selector):
                    pod = self.pod(selector)
                    status = pod.get("status", {})
                    return status if status.get("phase") in {"Succeeded", "Failed"} else None

                status = self.wait_for(terminated, "D_COMPATIBILITY_POD_NOT_TERMINAL")
                logs = self.k(
                    "D_COMPATIBILITY_LOGS", "-n", NS, "logs", "job/" + job["metadata"]["name"]
                )
                expected = {
                    "old-runtime": b"json: unknown field",
                    "old-worker": b"runtime/worker registry mismatch",
                    "class-drift": b"invalid activity effect class",
                    "mixed-survivors": (
                        b"surviving consumer registry: runtime/worker registry mismatch"
                    ),
                    "opt-out": b"activity compatibility PASS",
                    "new-new": b"activity compatibility PASS",
                }[name]
                accepted = name in {"opt-out", "new-new"}
                require(
                    expected in logs
                    and (name != "old-runtime" or b"effect_class" in logs)
                    and status["phase"] == ("Succeeded" if accepted else "Failed")
                    and status["containerStatuses"][0]["state"]["terminated"]["exitCode"]
                    == (0 if accepted else 1)
                    and all(
                        item["state"]["terminated"]["exitCode"] == 0
                        for item in status["initContainerStatuses"]
                    ),
                    "D_COMPATIBILITY_DISPOSITION",
                )
                row.update(
                    verdict="ACCEPTED" if accepted else "REFUSED", predicate=expected.decode()
                )
                exports = {}
                for item in job["spec"]["template"]["spec"]["initContainers"]:
                    name_of_export = item["name"]
                    data = self.k(
                        "D_COMPATIBILITY_EXPORT",
                        "-n",
                        NS,
                        "logs",
                        "job/" + job["metadata"]["name"],
                        "-c",
                        name_of_export,
                    )
                    exports[name_of_export] = {"image": item["image"], "entries": json.loads(data)}
                self.evidence.write(
                    f"compatibility-{name}.json",
                    {
                        "runtime_image": job["spec"]["template"]["spec"]["containers"][0]["image"],
                        "worker_exports": exports,
                        "accepted": accepted,
                    },
                )

    def signing_key(self) -> tuple[str, str]:
        private = self.command(
            "D_RECEIPT_SIGNING_KEY",
            "openssl",
            "genpkey",
            "-algorithm",
            "ED25519",
            "-outform",
            "DER",
        )
        prefix = bytes.fromhex("302e020100300506032b657004220420")
        require(
            private.startswith(prefix) and len(private) == len(prefix) + 32, "D_SIGNING_KEY_FORMAT"
        )
        seed = encoded(private[len(prefix) :])
        self.evidence.secret(seed)
        public = self.command(
            "D_RECEIPT_VERIFY_KEY",
            "openssl",
            "pkey",
            "-inform",
            "DER",
            "-pubout",
            "-outform",
            "DER",
            data=private,
        )
        prefix = bytes.fromhex("302a300506032b6570032100")
        require(
            public.startswith(prefix) and len(public) == len(prefix) + 32, "D_VERIFY_KEY_FORMAT"
        )
        return seed, encoded(public[len(prefix) :])

    def observer_rows(self) -> dict:
        return {row["workflow_id"]: row for row in json.loads(self.db(SQL))}

    def block(self, name: str, submissions: list[dict], plans: dict, approve) -> list[dict]:
        samples = []
        last_not_terminal = {}

        def observe() -> tuple[int, int, dict]:
            start = time.monotonic_ns()
            rows = self.observer_rows()
            end = time.monotonic_ns()
            for position, sample in enumerate(samples):
                key = sample["workflow_id"]
                if "terminal_ns" in sample:
                    continue
                row = rows.get(key)
                require(row is not None, "D_SUBMITTED_WORKFLOW_MISSING")
                if row["state"] == "SUCCEEDED":
                    sample["terminal_ns"] = [last_not_terminal[key], end]
                    if name == PURE or position >= 8:
                        expected = 5 if position == 9 else submissions[position]["initial_input"]
                        require(
                            row["result"] == expected
                            and not row["approvals"]
                            and row["grant_transitions"] == 0
                            and row["attempts"]
                            == [
                                {
                                    "number": 1,
                                    "state": "SUCCEEDED",
                                    "class": "PURE_ACTIVITY",
                                    "fence": None,
                                    "proof_operation": None,
                                    "signed": False,
                                }
                            ],
                            "D_PURE_RESULT",
                        )
                else:
                    require(
                        row["state"]
                        in {"RUNNABLE", "WAITING_APPROVAL", "WAITING_ACTIVITY", "WAITING_TIMER"},
                        "D_WORKFLOW_FAILED_OR_PAUSED",
                    )
                    last_not_terminal[key] = start
            return start, end, rows

        for body in submissions:
            key = body["workflow_id"]
            start = time.monotonic_ns()
            result = json.loads(
                self.k(
                    "D_SUBMIT",
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
                result == {"status": 201, "workflow_id": key, "created": True},
                "D_SUBMIT_REFUSED_" + str(result.get("status", "UNKNOWN")),
            )
            samples.append(
                {
                    "workflow_id": key,
                    "submitted_ns": [start, time.monotonic_ns()],
                    "stages_ns": {},
                }
            )
            last_not_terminal[key] = start
            observe()
        if name != PURE:
            effects = samples[:8]
            last_ungranted = {row["workflow_id"]: row["submitted_ns"][0] for row in effects}

            def granted():
                start, end, rows = observe()
                for sample in effects:
                    key = sample["workflow_id"]
                    count = rows[key]["grant_transitions"]
                    require(count <= 1, "D_DUPLICATE_GRANT_TRANSITION")
                    if count == 0:
                        last_ungranted[key] = start
                    elif "grant_ns" not in sample:
                        sample["grant_ns"] = [last_ungranted[key], end]
                        sample["stages_ns"]["approval"] = [end - sample["submitted_ns"][0]]
                return {} if all("grant_ns" in sample for sample in effects) else None

            for sample in effects:
                key = sample["workflow_id"]
                operation = plans[key]

                def pending(key=key):
                    _, _, rows = observe()
                    row = rows[key]
                    if (
                        row
                        and len(row["approvals"]) == 1
                        and row["approvals"][0]["decision"] == "PENDING"
                    ):
                        return row["approvals"][0]
                    return None

                intent = self.wait_for(pending, "D_SCHEDULER_INTENT_MISSING")
                approve(
                    {
                        "Namespace": NS,
                        "WorkflowID": key,
                        "NodeID": ACTIVITY,
                        "Target": operation["resource"],
                        "ResourceRevision": operation["expected_revision"],
                        "Key": operation["logical_key"],
                        "Arguments": operation["arguments"],
                    },
                    intent["intent_id"],
                )
                granted()

            self.wait_for(granted, "D_SCHEDULER_GRANT_MISSING")

        def terminal():
            _, _, rows = observe()
            return rows if all("terminal_ns" in sample for sample in samples) else None

        self.wait_for(terminal, "D_WORKFLOW_NOT_TERMINAL")
        return samples

    def logs(self, role: str) -> str:
        pods = json.loads(
            self.k(
                "D_ROLE_PODS",
                "-n",
                self.namespace,
                "get",
                "pods",
                "-l",
                "app=" + role,
                "-o",
                "json",
            )
        )["items"]
        collected = [
            self.k(
                "D_ROLE_LOGS",
                "-n",
                self.namespace,
                "logs",
                "-l",
                "app=" + role,
                "--all-containers=true",
                "--max-log-requests=100",
                "--prefix",
                "--tail=-1",
            ).decode()
        ]
        for pod in pods:
            name = pod["metadata"]["name"]
            statuses = pod.get("status", {}).get("containerStatuses", [])
            for container in statuses:
                container_name = container["name"]
                args = [
                    "-n",
                    self.namespace,
                    "logs",
                    name,
                    "-c",
                    container_name,
                    "--prefix",
                    "--tail=-1",
                ]
                if container.get("restartCount", 0):
                    collected.append(self.k("D_ROLE_PREVIOUS_LOGS", *args, "--previous").decode())
        return "".join(collected)

    def diagnostics(self) -> None:
        pods = json.loads(
            self.k("D_DIAGNOSTICS", "-n", self.namespace, "get", "pods", "-o", "json")
        )["items"]
        diagnostics = []
        for pod in pods:
            status = pod["status"]
            containers = []
            for item in status.get("initContainerStatuses", []) + status.get(
                "containerStatuses", []
            ):
                state = item.get("state", {})
                containers.append(
                    {
                        "name": item["name"],
                        "restarts": item["restartCount"],
                        "exit": state.get("terminated", {}).get("exitCode"),
                        "reason": state.get("waiting", state.get("terminated", {})).get("reason"),
                    }
                )
            diagnostics.append(
                {"pod": pod["metadata"]["name"], "phase": status["phase"], "containers": containers}
            )
        self.evidence.write("diagnostics.json", diagnostics)
        with suppress(Exception):
            self.evidence.write("engine-observer.partial.json", list(self.observer_rows().values()))

    def run_matrix(self) -> None:
        try:
            self.workload()
        except Exception:
            with suppress(Exception):
                self.diagnostics()
            raise

    def workload(self) -> None:
        evidence = self.evidence
        with evidence.step("a-pre-submission-setup"):
            self.identity_created = True
            migration, episode = self.install_topology()
            self.migrate(NS, migration, "D_ENGINE_MIGRATE")
            self.migrate_sink()
            self.export_config()
            initial = self.export_sink()
            require(
                all(relation["count"] == 0 for relation in initial["relations"]),
                "D_SINK_DB_NOT_FRESH",
            )
            evidence.write("sink-initial.json", initial)
            images = self.build_old_images()
            evidence.write("compatibility-images.json", {"phase_c_source": OLD, "images": images})
            seed, public_key = self.signing_key()
            sealed, workloads = plan()
            plan_bytes = json_data(sealed)
            evidence.write(
                "sealed-workload.json",
                {"plan": sealed, "plan_sha256": digest(plan_bytes), "submissions": workloads},
            )
            self.create(
                "D_LOAD_PLAN",
                obj(
                    "ConfigMap",
                    "k6-load-plan",
                    NS,
                    immutable=True,
                    data={"plan.json": plan_bytes.decode()},
                ),
            )
            self.create(
                "D_EFFECT_CA",
                obj(
                    "ConfigMap",
                    "k6-effect-ca",
                    NS,
                    immutable=True,
                    data={"ca.crt": self.effect_ca.read_text()},
                ),
            )
            kafka_ip = self.instance_address(CLUSTER + "-kafka")
            endpoints = {
                "192.0.2.1": self.address,
                "192.0.2.2": kafka_ip,
                "192.0.2.3": self.endpoint_addresses["api_service"],
                "192.0.2.4": self.endpoint_addresses["api_endpoint"],
            }
            manifests = render(images, endpoints, public_key, digest(plan_bytes))
            evidence.write("engine-topology.json", manifests)
            for item in manifests:
                if item["kind"] != "Deployment":
                    self.create("D_ENGINE_NETWORK", item)
            self.db(definition_sql())
        self.compatibility(images)
        with evidence.step("b-episode-identity-and-sink"):
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
            server_tls = self.cert(CONTROL, "DNS:" + CONTROL, "serverAuth")
            allocator = {
                "SinkURL": f"https://{SINK}:{PORT}",
                "SinkPeer": SINK_PEER,
                "AllocatorTLS": self.engine_tls,
                "WorkerControlTLS": server_tls,
            }
            self.create(
                "D_ALLOCATOR_CONFIG",
                obj(
                    "Secret",
                    "k6-allocator",
                    NS,
                    data={"config.json": encoded(json_data(allocator))},
                    immutable=True,
                    type="Opaque",
                ),
            )
        with evidence.step("b-compatible-lineage-rollout"):
            for item in manifests:
                if item["kind"] == "Deployment":
                    self.create("D_RUNTIME_ROLES", item)
            for name in ROLES:
                self.ready(name)
        with evidence.step("b-pure-baseline-reaches-terminal"):
            baseline = self.block(PURE, workloads[PURE], {}, None)
        plans = {op["submission_key"]: op for op in sealed["operations"]}
        jail = self.scratch / "jail"
        jail.mkdir(mode=0o700)
        shutil.copy2(self.approver, jail / "approver")
        decision_dsn = self.dsn("k6_decision_writer", decision, "verify-full", str(self.ca))
        decisions = []
        with self.adapter_socket(self.scratch, jail / "live", episode, reviewer, decision_dsn):

            def approve(approval_plan, intent):
                pending = self.approve(jail, "live", approval_plan, token)
                require(
                    pending.get("status") == 200
                    and pending.get("identity", {}).get("intent_id") == intent
                    and pending["identity"]["decision"] == "PENDING",
                    "D_PENDING_IDENTITY",
                )
                approved = self.approve(
                    jail, "live", approval_plan, token, IntentID=intent, Choice="APPROVED"
                )
                answer = approved.get("identity", {})
                require(
                    approved.get("status") == 200
                    and answer.get("intent_id") == intent
                    and answer.get("decision") == "APPROVED"
                    and answer.get("subject") == subject(NS, APPROVER)
                    and answer.get("proposal_hash") == pending["identity"]["proposal_hash"]
                    and bool(answer.get("decided_at")),
                    "D_APPROVER_DECISION",
                )
                decisions.append(answer)
                evidence.write("approvals.json", decisions)

            blocks = {}
            for name in (EFFECT, FORWARD):
                with evidence.step("c-" + name + "-reaches-terminal") as row:
                    blocks[name] = self.block(name, workloads[name], plans, approve)
                    row.update(workflows=10, cooperating=8)
        with evidence.step("d-seal-export-and-join") as row:
            role_logs = {
                "worker": self.logs("worker"),
                "allocator": self.logs(CONTROL),
                "sink": self.logs(SINK),
                "relay": self.logs(RELAY),
            }
            worker_log, allocator_log = role_logs["worker"], role_logs["allocator"]
            sink_log = role_logs["sink"]
            evidence.retain_role_logs(role_logs)

            def complete_witness():
                observations = collect(worker_log, allocator_log, sink_log, role_logs["relay"])
                evidence.retain_matched_lines(observations["matched_lines"])
                closes = sum(event == "close" for _, event, _ in observations["witness_timings"])
                return observations if closes >= len(plans) else None

            observations = self.wait_for(complete_witness, "D_WITNESS_CLOSE_MISSING", seconds=10)
            self.seal()
            self.stop_witness()
            evidence.write_bytes("host-ingress-journal.jsonl", self.journal.read_bytes())
            records = load_records(self.journal)
            snapshot = self.export_sink()
            evidence.write("sink-export.json", snapshot)
            engine = list(self.observer_rows().values())
            evidence.write("workflow-outcomes.json", engine)
            require(
                len(engine) == 30 and all(r["state"] == "SUCCEEDED" for r in engine),
                "D_TERMINAL_INVENTORY",
            )
            verify_work(sealed["operations"], engine, snapshot)
            evidence.write("effect-observations.json", observations)
            try:
                verdict = verify(
                    records,
                    snapshot,
                    engine_peer=ENGINE_PEER,
                    relay_peer=RELAY_PEER,
                    sink_peer=SINK_PEER,
                    expected_applications=len(plans),
                    allocations=observations["allocations"],
                    denied_requests=[],
                    worker_observations=worker_observations(records, observations["worker"]),
                )
            except ValueError as error:
                raise SmokeFailure(str(error)) from None
            require(
                len(verdict["joined"]) == len(plans) and len(verdict["session_high_water"]) == 1,
                "D_SINK_DELIVERY_COUNT",
            )
            evidence.write("join-verdict.json", {"status": "PASS", **verdict})
            row.update(
                applications=verdict["applications"],
                joined=len(verdict["joined"]),
                dispatched=len(plans),
                matched_lines=observations["matched_lines"],
            )
        with evidence.step("e-overhead-and-revoke") as row:
            add_timings(blocks, plans, snapshot, observations)
            try:
                record = measurement(baseline, blocks)
            except ValueError as error:
                raise SmokeFailure(str(error)) from None
            self.delete_binding(NS)
            evidence.write(OVERHEAD, record)
            row.update(
                sample_counts=record["sample_counts"],
                max_episode_equivalent_seconds=record["max_episode_equivalent_seconds"],
            )


def collect(worker_log: str, allocator_log: str, sink_log: str, relay_log: str) -> dict:
    h = r"([0-9a-f]{64})"
    prefix = r"^(?:\[pod/[^\]\r\n]+\] )?DUR053_K6_OBSERVATION "
    workers = [
        dict(
            zip(
                (
                    "request_id_hash",
                    "invocation_hash",
                    "request_hash",
                    "response_hash",
                    "response_length",
                    "status",
                ),
                (*match[:4], int(match[4]), int(match[5])),
                strict=True,
            )
        )
        for match in re.findall(
            rf"{prefix}k6_exchange {h} {h} {h} {h} (\d+) (\d+)\s*$", worker_log, re.M
        )
    ]
    return {
        "worker": workers,
        "allocations": [
            json.loads(body)
            for body in re.findall(r" k6_allocation (\{[^\n]+\})$", allocator_log, re.M)
        ],
        "worker_timings": [
            (key, phase, int(ns))
            for key, phase, ns in re.findall(
                rf"{prefix}k6_timing {h} (claim|delay|apply|lookup) (\d+)\s*$",
                worker_log,
                re.M,
            )
        ],
        "grant_timings": [
            (body, operation, phase, int(ns))
            for body, operation, phase, ns in re.findall(
                rf" k6_grant {h} (allocate|apply|lookup) (read|recheck) (\d+)\s*$",
                sink_log,
                re.M,
            )
        ],
        "witness_timings": [
            (request, event, int(ns))
            for request, event, ns in re.findall(
                rf" k6_witness {h} (\w+) (\d+)\s*$", relay_log, re.M
            )
        ],
        "matched_lines": {
            "worker": len(workers)
            + len(
                re.findall(
                    rf"{prefix}k6_timing {h} (claim|delay|apply|lookup) (\d+)\s*$",
                    worker_log,
                    re.M,
                )
            ),
            "allocator": len(re.findall(r" k6_allocation \{[^\n]+\}$", allocator_log, re.M)),
            "sink": len(
                re.findall(
                    rf" k6_grant {h} (allocate|apply|lookup) (read|recheck) (\d+)\s*$",
                    sink_log,
                    re.M,
                )
            ),
            "relay": len(re.findall(rf" k6_witness {h} (\w+) (\d+)\s*$", relay_log, re.M)),
        },
    }


def add_timings(blocks: dict, plans: dict, snapshot: dict, observations: dict) -> None:
    for block in blocks.values():
        for sample in block[:8]:
            operation = plans[sample["workflow_id"]]
            key = operation["logical_key"]
            calls = {
                call["operation"]: call for call in snapshot["calls"] if call["logical_key"] == key
            }
            worker_rows = [
                (phase, ns)
                for value, phase, ns in observations["worker_timings"]
                if value == identity(key)
            ]
            worker = dict(worker_rows)
            body_hashes = {call["body_hash"] for call in calls.values()}
            grants = [
                (phase, stage, ns)
                for body, phase, stage, ns in observations["grant_timings"]
                if body in body_hashes
            ]
            request = identity(calls["apply"]["request_id"])
            witnesses = [
                (event, ns)
                for value, event, ns in observations["witness_timings"]
                if value == request
            ]
            require(
                len(worker_rows) == len(worker) == 3
                and set(worker) == {"claim", "delay", "apply"}
                and worker["delay"] >= 5 * 10**9
                and Counter((phase, stage) for phase, stage, _ in grants)
                == {
                    ("allocate", "read"): 1,
                    ("allocate", "recheck"): 1,
                    ("apply", "read"): 1,
                    ("apply", "recheck"): 1,
                }
                and Counter(event for event, _ in witnesses) == Counter(EXCHANGE),
                "D_TIMING_INVENTORY",
            )
            sample["stages_ns"].update(
                claim=[worker["claim"]],
                pre_call_delay=[worker["delay"]],
                grant_read=[ns for _, _, ns in grants],
                call=[worker["apply"]],
                lookup=[],
                witness=[ns for _, ns in witnesses],
            )


def inherited_failure(
    output: Path, code: str = "SHARED_KIND_HARNESS_FAILED_BEFORE_PHASE_D"
) -> None:
    if not output.exists():
        evidence = PhaseDEvidence(make_output(output))
        evidence.report.update(reason=code)
        evidence.checkpoint()


def record_handoff_failure(output: Path, error: BaseException) -> None:
    if not output.exists():
        evidence = PhaseDEvidence(make_output(output))
        with suppress(Exception), evidence.step("phase-d-handoff"):
            raise error


def run(output: Path, harness=None, address: str | None = None) -> int:
    code = run_hosted(
        output, harness, address, phase="d", evidence_type=PhaseDEvidence, matrix_type=Matrix
    )
    path = output / OVERHEAD
    if code and path.exists():
        record = json.loads(path.read_bytes())
        record["status"] = "FAIL"
        path.write_bytes(json_data(record))
    report = json.loads((output / "smoke.json").read_bytes())
    print(f"DUR-053 K6 Phase D {report['status']} {report['reason']}", flush=True)
    return code


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, required=True)
    raise SystemExit(run(parser.parse_args().output_root))
