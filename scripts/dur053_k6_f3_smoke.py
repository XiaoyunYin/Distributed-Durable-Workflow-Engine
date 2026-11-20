"""K6 TimeChaos smoke on the disposable Phase F kind cluster."""

from __future__ import annotations

import json
import os
import secrets
import tempfile
import time
from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path

from dur053_k3 import CLUSTER, Driver
from dur053_k6_c_render import pod_peer, policy, traffic
from dur053_k6_c_smoke import make_output
from dur053_k6_chaos import Chaos, Target
from dur053_k6_d_render import set_env
from dur053_k6_e1_join import by, expect, one
from dur053_k6_e1_smoke import ROLES, workload
from dur053_k6_f1a_smoke import (
    CONTROL_DIRECTORY,
    CONTROL_VOLUME,
    RELEASE_ACTIVITY,
    F1aEvidence,
    deployment_objects,
    ready,
)
from dur053_k6_f2_inputs import QUIET_NS, pending, quiet_digest
from dur053_k6_f2_smoke import F2Smoke, LineProcess
from dur053_k6_f3_inputs import (
    CELLS,
    NOT_EXERCISED,
    SECOND,
    acknowledge,
    clock_proof,
    join,
    recovery,
    time_spec,
)
from dur053_k6_identity import obj
from dur053_k6_registry import read_registry

CONTROL = "f3-control"
CONTROL_POD = "clock-control"
PORTS = {"scheduler": 8080, "worker": 8083, CONTROL_POD: 8083}

# Triggers capture each committed write, including rows overwritten before the
# next sample. Store only metadata and the hash of the claim token.
INSTALL_WITNESS = """
CREATE SCHEMA k6_f3;
CREATE TABLE k6_f3.deadline_writes (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    written_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    application text NOT NULL DEFAULT current_setting('application_name'),
    session text DEFAULT current_setting('durable.k4_session', true),
    branch text NOT NULL,
    partition_id int,
    owner_id text,
    epoch bigint,
    workflow_id text,
    node_id text,
    iteration int,
    attempt_number bigint,
    worker_id text,
    token_hash text,
    row_id text NOT NULL,
    deadline timestamptz,
    old_deadline timestamptz
);
CREATE FUNCTION k6_f3.witness() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    CASE TG_TABLE_NAME
    WHEN 'partition_leases' THEN
      INSERT INTO k6_f3.deadline_writes
        (branch, partition_id, owner_id, epoch, row_id, deadline, old_deadline)
      VALUES (CASE
                  WHEN NEW.owner_id IS NULL THEN 'lease-release'
                  WHEN NEW.owner_id IS DISTINCT FROM OLD.owner_id OR NEW.epoch <> OLD.epoch
                    THEN 'lease-acquire'
                  ELSE 'renewal'
              END, NEW.partition_id, NEW.owner_id::text, NEW.epoch, NEW.partition_id::text,
              NEW.lease_expires_at, OLD.lease_expires_at);
    WHEN 'activity_attempts' THEN
      INSERT INTO k6_f3.deadline_writes
        (branch, workflow_id, node_id, iteration, attempt_number, worker_id, token_hash,
         row_id, deadline, old_deadline)
      VALUES (CASE
                  WHEN TG_OP = 'INSERT' AND NEW.state = 'DISPATCHABLE' THEN 'dispatch'
                  WHEN OLD.state = 'DISPATCHABLE' AND NEW.state = 'CLAIMED' THEN 'claim'
                  WHEN OLD.state = 'CLAIMED' AND NEW.state = 'CLAIMED'
                      AND OLD.heartbeat_deadline IS DISTINCT FROM NEW.heartbeat_deadline
                    THEN 'heartbeat'
                  ELSE 'attempt-update'
              END, NEW.workflow_id, NEW.node_id, NEW.iteration, NEW.attempt_number, NEW.worker_id,
              encode(sha256(convert_to(NEW.claim_token::text,'UTF8')),'hex'),
              concat_ws('/', NEW.workflow_id, NEW.node_id, NEW.iteration, NEW.attempt_number),
              NEW.heartbeat_deadline, OLD.heartbeat_deadline);
    WHEN 'scheduler_wakeups' THEN
      IF NEW.claim_expires_at IS DISTINCT FROM OLD.claim_expires_at
          AND NEW.claim_expires_at IS NOT NULL THEN
        INSERT INTO k6_f3.deadline_writes
          (branch, row_id, owner_id, partition_id, deadline, old_deadline)
        VALUES ('inbox-claim', NEW.wakeup_id::text, NEW.claim_owner::text, NEW.partition_id,
                NEW.claim_expires_at, OLD.claim_expires_at);
      END IF;
    WHEN 'outbox' THEN
      IF NEW.claim_expires_at IS DISTINCT FROM OLD.claim_expires_at
          AND NEW.claim_expires_at IS NOT NULL THEN
        INSERT INTO k6_f3.deadline_writes
          (branch, row_id, workflow_id, owner_id, deadline, old_deadline)
        VALUES ('outbox-claim', NEW.event_id::text, NEW.workflow_id, NEW.claim_owner::text,
                NEW.claim_expires_at, OLD.claim_expires_at);
      END IF;
    END CASE;
    RETURN NULL;
END $$;
CREATE TRIGGER f3_lease AFTER UPDATE ON engine.partition_leases
FOR EACH ROW EXECUTE FUNCTION k6_f3.witness();
CREATE TRIGGER f3_attempt AFTER INSERT OR UPDATE ON engine.activity_attempts
FOR EACH ROW EXECUTE FUNCTION k6_f3.witness();
CREATE TRIGGER f3_inbox AFTER UPDATE ON engine.scheduler_wakeups
FOR EACH ROW EXECUTE FUNCTION k6_f3.witness();
CREATE TRIGGER f3_outbox AFTER UPDATE ON engine.outbox
FOR EACH ROW EXECUTE FUNCTION k6_f3.witness();
"""

DB_CLOCK = """
SELECT jsonb_build_object(
  'db_at', clock_timestamp(), 'read_only', current_setting('transaction_read_only'),
  'claim', ARRAY(SELECT jsonb_build_object('workflow_id', a.workflow_id, 'node_id', a.node_id,
      'iteration', a.iteration, 'attempt_number', a.attempt_number, 'state', a.state,
      'is_current', a.is_current, 'worker_id', a.worker_id, 'deadline', a.heartbeat_deadline,
      'token_hash', encode(sha256(convert_to(a.claim_token::text,'UTF8')),'hex'))
      FROM engine.activity_attempts a WHERE a.is_current),
  'lease', ARRAY(SELECT to_jsonb(l) FROM engine.partition_leases l
      JOIN engine.workflow_executions w USING (partition_id)));
"""

OBSERVER = """
import http.client
import json
import sys
import threading
import time

host, port = sys.argv[1], int(sys.argv[2])
rows = []
stop = threading.Event()
lock = threading.Lock()

def instant():
    return {"wall_ns": time.time_ns(), "monotonic_ns": time.monotonic_ns()}

def sample():
    while not stop.is_set():
        conn = http.client.HTTPConnection(host, port, timeout=0.4)
        send = instant()
        try:
            conn.request("GET", "/internal/k6/clock")
            response = conn.getresponse()
            raw = response.read(4096)
            receive = instant()
            if response.status != 200:
                answer = {"http_status": response.status}
            else:
                answer = json.loads(raw)
        except (OSError, ValueError, http.client.HTTPException) as error:
            receive = instant()
            answer = {"error": type(error).__name__}
        finally:
            conn.close()
        with lock:
            rows.append({"send": send, "receive": receive, "answer": answer})
        stop.wait(max(0, 0.5 - (time.monotonic_ns() - send["monotonic_ns"]) / 1e9))

thread = threading.Thread(target=sample, daemon=True)
thread.start()
for line in sys.stdin:
    command = line.strip()
    if command == "mark":
        output = instant()
    elif command in ("snapshot", "stop"):
        if command == "stop":
            stop.set()
            thread.join()
        with lock:
            output = list(rows)
    else:
        raise ValueError("F3_OBSERVER_COMMAND")
    print(json.dumps(output), flush=True)
"""


class F3Evidence(F1aEvidence):
    schema = "dur053.k6-phase-f3-smoke.v1"


class TimeChaos(Chaos):
    deleted: dict
    injection: dict | None = None
    on_absent: Callable[[], None] | None = None

    def __init__(self, smoke: F3Smoke, row: dict, target: Target, seconds: int) -> None:
        super().__init__(smoke, smoke.schema_dir, row, target)
        self.manifest["spec"] = time_spec(asdict(target), seconds)

    def recovery_watch(self, version: str) -> list[dict]:
        if self.on_absent:
            self.on_absent()
        rows = super().recovery_watch(version)
        self.deleted = one("F3_DELETED_WATCH", by(rows, type="DELETED"))["object"]
        if self.injection:
            recovery(self.deleted, self.injection)
        return rows


class F3Smoke(F2Smoke):
    inputs_schema = "dur053.k6-phase-f3-inputs.v1"
    broker_name: str | None = None
    control_artifacts = False

    def retain(self, label: str, data: object) -> None:
        if label == "chaos-removal" and isinstance(data, dict):
            # The parent records runner wall times for kubectl diagnostics.
            # Intervention durations come from the in-pod observer.
            data = {key: value for key, value in data.items() if key != "elapsed_seconds"}
        if self.control_artifacts and label.startswith("chaos-"):
            label = "control-" + label
        super().retain(label, data)

    def cell_setup(self, cell: str) -> None:
        self.cell_id = cell
        self.namespace = "dur053-k6-f3-" + cell
        self.database = self.namespace.replace("-", "_")
        self.monitor_role = self.database + "_monitor"
        self.monitor_password = secrets.token_hex(32)
        self.evidence.secret(self.monitor_password)
        self.monitor_created = False
        self.restart_target = self.probe_topic = self.chaos_run = None
        self.workflow = workload(cell)[0]["workflow_id"]
        self.broker_name = CLUSTER + "-f3-" + cell
        # The same advertised host resolves on the Docker bridge and through
        # the episode's Kubernetes Service. A bare "kafka" inside Docker would
        # refer to the broker serving the earlier phases.
        broker_dns = f"kafka.{self.namespace}.svc.cluster.local"
        self.harness.start_kafka(self.broker_name, alias=broker_dns)
        self.broker = self.harness.kind_address(self.broker_name)
        self.addresses["192.0.2.2"] = self.broker
        self.setup_episode()
        self.pg(INSTALL_WITNESS)
        self.sql(
            "F3_MONITOR_CREATE",
            f"""
            CREATE ROLE {self.monitor_role} LOGIN PASSWORD '{self.monitor_password}';
            ALTER ROLE {self.monitor_role} SET default_transaction_read_only = on;
            ALTER ROLE {self.monitor_role} SET statement_timeout = '1500ms';
            """,
        )
        self.monitor_created = True
        self.pg(f"""
            GRANT USAGE ON SCHEMA engine, k6_f3 TO {self.monitor_role};
            GRANT SELECT ON ALL TABLES IN SCHEMA engine, k6_f3 TO {self.monitor_role};
        """)

    def reset_episode_transport(self, _renderer: Driver) -> None:
        assert self.broker_name
        self.harness.topics("--create", kafka=self.broker_name)
        self.retain(
            "dedicated-topics",
            self.command(
                "F3_KAFKA_TOPIC_IDS",
                "docker",
                "exec",
                self.broker_name,
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                "localhost:19092",
                "--describe",
            ).decode(),
        )

    def render_episode(self, source: list[dict]) -> list[dict]:
        rows = deployment_objects(source, self.namespace, self.tags, self.harness.images)
        for item in rows:
            kind, name = item["kind"], item["metadata"]["name"]
            if kind == "PersistentVolumeClaim" and name == CONTROL_VOLUME:
                item["metadata"]["name"] = CONTROL
            if kind != "Deployment":
                continue
            pod = item["spec"]["template"]["spec"]
            container = pod["containers"][0]
            if name in {"scheduler", "worker"}:
                set_env(container, DUR053_K6_CLOCK="1")
            if name == "scheduler" and CELLS[self.cell_id][0] == "scheduler":
                set_env(
                    container,
                    DUR053_K6_CLOCK_WORKFLOW=self.workflow,
                    DUR053_K6_CLOCK_CONTROL=CONTROL_DIRECTORY,
                )
                pod["volumes"].append(
                    {"name": CONTROL_VOLUME, "persistentVolumeClaim": {"claimName": CONTROL_VOLUME}}
                )
                container["volumeMounts"].append(
                    {"name": CONTROL_VOLUME, "mountPath": CONTROL_DIRECTORY, "readOnly": True}
                )
            for volume in pod.get("volumes", []):
                if volume["name"] == CONTROL_VOLUME:
                    volume.update(name=CONTROL, persistentVolumeClaim={"claimName": CONTROL})
            for mount in container.get("volumeMounts", []):
                if mount["name"] == CONTROL_VOLUME:
                    mount["name"] = CONTROL
                elif name == "observer":
                    mount["readOnly"] = True
        for role, port in PORTS.items():
            rows.extend(
                (
                    policy(
                        self.namespace,
                        "f3-clock-from-observer-" + role,
                        role,
                        "Ingress",
                        [traffic("from", port, pod_peer("observer"))],
                    ),
                    policy(
                        self.namespace,
                        "f3-clock-to-" + role,
                        "observer",
                        "Egress",
                        [traffic("to", port, pod_peer(role))],
                    ),
                )
            )
        return rows

    def prepare_episode_dependencies(self) -> None:
        expect("F3_FRESH_TIMECHAOS", self.objects("F3_FRESH_TIMECHAOS", "timechaos")["items"], [])

    def bound(self, role: str, pod: dict, session: str = "") -> Target:
        if role == CONTROL_POD:
            return replace(self.identity("worker", pod, session), role=role)
        return self.identity(role, pod, session)

    def reobserve(self, target: Target) -> Target:
        return self.bound(
            target.role, self.objects("F3_REOBSERVE_TARGET", "pod", target.pod), target.session
        )

    def marker(self, name: str) -> None:
        path = f"{CONTROL_DIRECTORY}/{name}"
        response = self.exec_python(
            "LEASE_MARKER", "deployment/observer", RELEASE_ACTIVITY, path, ""
        )
        expect("F3_LEASE_MARKER", response, {"released": True, "marker": path})

    def held_lease(self) -> tuple[Target, dict]:
        self.marker("lease-arm")

        def held() -> tuple[tuple[Target, dict] | None, dict]:
            cut = self.cut(self.workflow)
            lease = one("F3_LIVE_LEASE_ROW", cut["leases"])
            if lease["live"] and self.monitor(f"""
                SELECT to_jsonb(coalesce(max(written_at)-min(written_at) > interval '600ms', false))
                FROM k6_f3.deadline_writes WHERE branch='renewal'
                  AND partition_id={int(lease["partition_id"])} AND epoch={int(lease["epoch"])};
            """):
                entry = max(
                    by(
                        cut["audit"],
                        kind="acquire",
                        partition_id=lease["partition_id"],
                        owner_id=lease["owner_id"],
                        epoch=lease["epoch"],
                    ),
                    key=lambda row: row["event_order"],
                )
                pod = one(
                    "F3_LEASE_HOLDER",
                    [p for p in self.pods("scheduler") if p["metadata"]["uid"] == entry["pod_uid"]],
                )
                return (self.bound("scheduler", pod, entry["session_id"]), entry), {}
            return None, {"candidates": ["F3_LEASE_NOT_HELD"]}

        return self.poll(held, "F3_HELD_LEASE", time.monotonic() + 10)

    def static(self, target: Target) -> dict:
        def node(stage: str, *args: str) -> bytes:
            return self.command("F3_" + stage, "docker", "exec", target.node, *args, timeout=10)

        executable = self.scratch / f"{self.cell_id}-runtime"
        executable.write_bytes(node("RUNNING_EXECUTABLE", "cat", f"/proc/{target.host_pid}/exe"))
        build = json.loads((self.evidence.root / "campaign-build.json").read_bytes())
        result = {
            "target": asdict(target),
            "sha256": sha256(executable.read_bytes()).hexdigest(),
            "build_sha256": build["runtime_sha256"],
            "go_version_m": self.command(
                "F3_GO_BUILD_SETTINGS", "go", "version", "-m", str(executable)
            ).decode(),
            "file": self.command("F3_ELF_LINKAGE", "file", "-b", str(executable)).decode(),
            "program_headers": self.command(
                "F3_ELF_HEADERS", "readelf", "-lW", str(executable)
            ).decode(),
            "containerd": node("CONTAINERD_CONFIG", "containerd", "config", "dump").decode(),
            "containerd_version": node("CONTAINERD_VERSION", "containerd", "--version").decode(),
            "cni": node("CNI_CONFIG", "cat", "/etc/cni/net.d/10-calico.conflist").decode(),
            "calico": json.loads(
                self.k(
                    "F3_CALICO_NODE", "-n", "kube-system", "get", "ds", "calico-node", "-o", "json"
                )
            )["spec"]["template"]["spec"],
        }
        self.retain("static-go", result)
        return result

    def observer(self, stack: ExitStack, target: Target) -> LineProcess:
        pod = self.objects("F3_TARGET_ADDRESS", "pod", target.pod)
        expect("F3_TARGET_ADDRESS_UID", pod["metadata"]["uid"], target.uid)
        observer = LineProcess(
            [
                "kubectl",
                "--context",
                f"kind-{CLUSTER}",
                "-n",
                self.namespace,
                "exec",
                "--quiet",
                "-i",
                "deployment/observer",
                "--",
                "python",
                "-u",
                "-c",
                OBSERVER,
                pod["status"]["podIP"],
                str(PORTS[target.role]),
            ],
            self.env,
        )
        stack.callback(observer.close)
        return observer

    @staticmethod
    def read(observer: LineProcess, command: str = "mark"):
        observer.send(command)
        return observer.read(5)

    def wait_observer(self, observer: LineProcess, seconds: float) -> None:
        start = self.read(observer)["monotonic_ns"]
        while self.read(observer)["monotonic_ns"] - start < seconds * SECOND:
            time.sleep(0.2)

    def db_sample(self, observer: LineProcess, phase: str, data: dict) -> None:
        send = self.read(observer)
        value = self.monitor(DB_CLOCK)
        data[phase] = {"send": send, "value": value, "receive": self.read(observer)}
        self.retain("db-clock", data)

    def remaining(self) -> list:
        return self.objects("F3_CHAOS_REMAINING", "timechaos")["items"]

    def intervention(
        self,
        row: dict,
        target: Target,
        seconds: int,
        observer: LineProcess,
        *,
        control: bool = False,
    ) -> dict:
        # One name per apply, with server-side dry-run and bound CRI identity.
        subject = asdict(target)
        node_boot = (
            self.command(
                "F3_NODE_BOOT_ID",
                "docker",
                "exec",
                target.node,
                "cat",
                "/proc/sys/kernel/random/boot_id",
            )
            .decode()
            .strip()
        )
        data: dict = {
            "db": {},
            "marks": {},
            "bindings": [subject],
            "target": subject,
            "node_boot_id": node_boot,
        }
        if control:
            data["purpose"] = "harness validation only"
        self.control_artifacts = control
        self.chaos_run = chaos = TimeChaos(self, row, target, seconds)
        try:
            chaos.preflight(self.crds)
            self.wait_observer(observer, 1.8)
            baseline = self.read(observer, "snapshot")
            clock = baseline[0]["answer"]
            data["identity"] = {key: value for key, value in clock.items() if key != "wall_ns"}
            if not control:
                self.db_sample(observer, "baseline", data["db"])

            def bound() -> Target:
                result = self.reobserve(target)
                data["bindings"].append(asdict(result))
                data["marks"]["apply"] = self.read(observer)
                return result

            chaos.apply(bound)

            def ack() -> tuple[dict | None, dict]:
                raw = chaos.sample("injection")
                return (raw if raw and acknowledge(raw, subject, seconds) else None), {
                    "object": raw,
                    "candidates": ["F3_INJECTION_NOT_ACKNOWLEDGED"],
                }

            data["injected"] = chaos.injection = self.poll(
                ack, "F3_INJECTION_ACK", time.monotonic() + 18
            )
            data["marks"]["ack"] = self.read(observer)
            self.wait_observer(observer, 0.8)
            if not control:
                self.db_sample(observer, "during_start", data["db"])
            self.wait_observer(observer, 6 if not control else 1.6)
            if not control:
                self.db_sample(observer, "during_end", data["db"])
            self.wait_observer(observer, 0.8)
            data["last_status"] = chaos.sample("end-during")
            data["marks"]["remove"] = self.read(observer)
            chaos.on_absent = lambda: data["marks"].update(absent=self.read(observer))
            try:
                data["removed_runner_ns"] = chaos.remove()
            finally:
                chaos.on_absent = None
            data["deleted"] = chaos.deleted
            data["remaining"] = self.remaining()
            expect("F3_CHAOS_REMAINING", data["remaining"], [])
            if not control:
                self.db_sample(observer, "after", data["db"])
            self.wait_observer(observer, 1.8)
            data["samples"] = self.read(observer, "stop")
            data["bindings"].append(asdict(self.reobserve(target)))
            if not control:
                data["writes"] = self.monitor(
                    "SELECT coalesce(jsonb_agg(to_jsonb(w) ORDER BY id), '[]'::jsonb) "
                    "FROM k6_f3.deadline_writes w;"
                )
            return data
        finally:
            try:
                if "samples" not in data:
                    data["samples"] = self.read(observer, "stop")
            finally:
                self.retain("clock-control" if control else "clock-target", data)

    def control(self, row: dict, seconds: int) -> dict:
        spec = {
            "automountServiceAccountToken": False,
            "restartPolicy": "Never",
            "securityContext": {"runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"}},
            "containers": [
                {
                    "name": "worker",
                    "image": self.tags["worker"],
                    "command": ["python", "-m", "dur053_k6_campaign.clock"],
                    "ports": [{"containerPort": PORTS[CONTROL_POD]}],
                    "readinessProbe": {
                        "tcpSocket": {"port": PORTS[CONTROL_POD]},
                        "periodSeconds": 1,
                    },
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "readOnlyRootFilesystem": True,
                        "capabilities": {"drop": ["ALL"]},
                    },
                    "env": [
                        {
                            "name": "POD_UID",
                            "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}},
                        }
                    ],
                }
            ],
        }
        manifest = obj("Pod", CONTROL_POD, self.namespace, spec=spec)
        manifest["metadata"]["labels"] = {"app": CONTROL_POD}
        self.create("F3_CREATE_CONTROL", manifest)
        self.k(
            "F3_WAIT_CONTROL",
            "-n",
            self.namespace,
            "wait",
            "--for=condition=Ready",
            f"pod/{CONTROL_POD}",
            "--timeout=25s",
            timeout=28,
        )
        target = self.bound(CONTROL_POD, self.objects("F3_CONTROL_POD", "pod", CONTROL_POD))
        with ExitStack() as stack:
            data = self.intervention(
                {**row, "target_role": CONTROL_POD},
                target,
                seconds,
                self.observer(stack, target),
                control=True,
            )
        return data

    def closeout_f3(self, observer: LineProcess, marker: dict) -> dict:
        quiet: list[dict] = []

        def sample() -> tuple[dict | None, dict]:
            started = self.read(observer)
            cut = self.monitor_cut()
            rows, seals = self.witness()
            self.require_healthy_gates(rows)
            states = {key: value["workflow"]["state"] for key, value in cut["workflows"].items()}
            complete = (
                states == {self.workflow: "SUCCEEDED"}
                and not pending(cut)
                and all(
                    by(data["inbox"], event_id=event["event_id"], disposition="ACCEPTED")
                    for data in cut["workflows"].values()
                    for event in data["outbox"]
                )
            )
            diagnostics = {"states": states, "pending": sorted(pending(cut))}
            if not complete:
                quiet.clear()
                return None, diagnostics
            digest = quiet_digest(cut, rows)
            if quiet and quiet[-1]["digest"] != digest:
                quiet.clear()
            quiet.append(
                {
                    "started": started,
                    "ended": self.read(observer),
                    "digest": digest,
                    "db_at": cut["db_at"],
                }
            )
            if started["monotonic_ns"] - quiet[0]["ended"]["monotonic_ns"] < QUIET_NS:
                time.sleep(0.6)
                return None, diagnostics
            self.retain("terminal-witness", seals)
            return {
                "terminal": cut,
                "witness": rows,
                "quiet": quiet,
                "workers": self.pods("worker"),
                "marker": marker,
                "topics": self.engine_topics(),
            }, diagnostics

        return self.read_until("F3_TERMINAL_QUIET", sample, 110)

    def exercise_f3(self, row: dict, registered: dict) -> dict:
        role, seconds = CELLS[self.cell_id]
        scheduler = self.bound("scheduler", next(p for p in self.pods("scheduler") if ready(p)))
        static = self.static(scheduler)
        workflow, fingerprint = self.offer_episode()
        expect("F3_PLANNED_WORKFLOW", workflow, self.workflow)
        workers, active = self.active_worker(workflow, heartbeat=False)
        worker = one("F3_ACTIVE_WORKER", workers)
        invocation = active[worker.uid]["invocation"]
        attempt = active[worker.uid]["attempt"]
        held_owner = None
        if role == "scheduler":
            scheduler, held_owner = self.held_lease()
            if static["target"]["uid"] != scheduler.uid:
                static = self.static(scheduler)
            else:
                static["target"] = asdict(scheduler)
                self.retain("static-go", static)
        target = scheduler if role == "scheduler" else worker
        scheduler_log = self.role_log(scheduler.pod, "scheduler").decode()
        self.evidence.write_bytes(
            f"{self.cell_id}-scheduler-timestamped.log", scheduler_log.encode()
        )
        with ExitStack() as stack:
            observer = self.observer(stack, target)
            fault = self.intervention(row, target, seconds, observer)
            fault["held_owner"] = held_owner
            proof = clock_proof(fault, seconds)
            if role == "scheduler":
                self.marker("lease-release")
            before = self.read(observer)
            marker = self.release_activity(invocation)
            marker.update(before=before, after=self.read(observer))
            tail = self.closeout_f3(observer, marker)
        control = self.control(row, seconds) if proof["confirmation"] == NOT_EXERCISED else None
        offer = {
            "workflow": workflow,
            "fingerprint": fingerprint,
            "response": json.loads(
                (self.evidence.root / f"{self.cell_id}-accepted.json").read_bytes()
            )["response"],
        }
        inputs = {
            "cell": self.cell_id,
            "fault": fault,
            "control": control,
            "static": static,
            "scheduler_log": scheduler_log,
            "offer": offer,
            "attempt": attempt,
            "registered": registered,
            **tail,
        }
        self.retain("checker-inputs", inputs)
        result = join(inputs)
        self.retain("joined", result)
        return result

    def cleanup_f3(self) -> None:
        errors = []
        if self.namespace_started:
            try:
                if self.chaos_run and self.chaos_run.possibly_created:
                    self.chaos_run.remove()
                remaining = self.remaining()
                self.retain("timechaos-cleanup-inventory", remaining)
                expect("F3_CLEANUP_CHAOS_REMAINING", remaining, [])
            except Exception as error:
                errors.append(self.evidence.safe_reason(error))
        try:
            self.cleanup()
        except Exception as error:
            errors.append(self.evidence.safe_reason(error))
        if self.broker_name:
            self.harness.command(["docker", "rm", "-fv", self.broker_name], allowed=True)
            result = self.harness.command(
                [
                    "docker",
                    "ps",
                    "-a",
                    "--filter",
                    f"name=^/{self.broker_name}$",
                    "--format",
                    "{{.Names}}",
                ]
            )
            expect("F3_CLEANUP_BROKER_REMAINING", result.stdout.strip(), "")
            self.broker_name = None
        expect("F3_CLEANUP_FAILED", errors, [])


def inherited_failure(output: Path, code: str = "F3_SHARED_HARNESS_FAILED") -> None:
    if not output.exists():
        evidence = F3Evidence(make_output(output))
        evidence.report.update(reason=code)
        evidence.checkpoint()


def run(output: Path, harness: Driver, addresses: dict[str, str], f1b_output: Path) -> int:
    started = time.monotonic()
    evidence = F3Evidence(make_output(output))
    failures = []
    results = []
    with ExitStack() as stack:
        try:
            with evidence.step("f3-setup"):
                scratch = Path(
                    stack.enter_context(
                        tempfile.TemporaryDirectory(
                            prefix="dur053-k6-f3-", dir=os.environ["RUNNER_TEMP"]
                        )
                    )
                )
                registry = read_registry()
                smoke = F3Smoke(evidence, scratch, harness, addresses)
                smoke.use_campaign_build(f1b_output)
                smoke.backend()
            try:
                for cell in CELLS:
                    smoke.cell_id = cell
                    smoke.control_artifacts = False
                    smoke.deadline = datetime.now(UTC) + timedelta(minutes=6)
                    cell_start = time.monotonic()
                    row = min(by(registry["episodes"], cell=cell), key=lambda r: r["ordinal"])
                    registered = one("F3_REGISTERED_ARM", by(registry["coverage"], arm=row["arm"]))
                    smoke.retain("registration", {"episode": row, "coverage": registered})
                    try:
                        with evidence.step(cell + "-topology"):
                            smoke.cell_setup(cell)
                        with evidence.step(cell) as step:
                            result = smoke.exercise_f3(row, registered)
                            step.update(
                                outcome=result["outcome"],
                                passing_replicate=result["passing_replicate"],
                            )
                            results.append({"cell": cell, "outcome": result["outcome"]})
                        if result["outcome"] == NOT_EXERCISED:
                            step["status"] = NOT_EXERCISED
                            evidence.checkpoint()
                    except Exception as error:
                        reason = evidence.safe_reason(error)
                        failures.append(reason)
                        results.append({"cell": cell, "outcome": "FAIL", "reason": reason})
                    finally:
                        try:
                            with evidence.step(cell + "-cleanup") as step:
                                smoke.cleanup_f3()
                                step["elapsed_seconds"] = round(time.monotonic() - cell_start, 3)
                        except Exception as error:
                            failures.append(evidence.safe_reason(error))
                    if smoke.namespace_started or smoke.database_started:
                        break
            finally:
                if smoke.chaos_started:
                    smoke.cell_id = "setup"
                    smoke.deadline = datetime.now(UTC) + timedelta(minutes=2)
                    try:
                        expect(
                            "F3_CLUSTER_TIMECHAOS_REMAINING",
                            json.loads(
                                smoke.k(
                                    "F3_CLUSTER_TIMECHAOS", "get", "timechaos", "-A", "-o", "json"
                                )
                            )["items"],
                            [],
                        )
                        smoke.collect_role_logs("chaos-mesh", ROLES)
                    finally:
                        smoke.clear_namespace("chaos-mesh", 50)
        except (Exception, KeyboardInterrupt) as error:
            failures.append(evidence.safe_reason(error))
        try:
            evidence.scan()
        except Exception as error:
            failures.append(evidence.safe_reason(error))
        recorded = {row["cell"] for row in results}
        results.extend(
            {"cell": cell, "outcome": "FAIL", "reason": "F3_SETUP_OR_CLEANUP_INCOMPLETE"}
            for cell in CELLS
            if cell not in recorded
        )
        evidence.report.update(
            status="FAIL" if failures else "PASS",
            reason=failures[0] if failures else "MATRIX_COMPLETE",
            cells=results,
            elapsed_seconds=round(time.monotonic() - started, 3),
        )
        evidence.checkpoint()
    print(
        f"DUR-053 K6 Phase F3 {evidence.report['status']} {evidence.report['reason']}", flush=True
    )
    return int(bool(failures))
