"""Phase F2 engineering smoke on the shared disposable kind cluster."""

from __future__ import annotations

import json
import os
import secrets
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, suppress
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from queue import Empty, Queue
from typing import Any

from dur053_k3 import CLUSTER, Driver
from dur053_k6_b_evidence import SmokeFailure
from dur053_k6_b_smoke import require
from dur053_k6_c_render import ip_peer, policy, traffic
from dur053_k6_c_smoke import make_output
from dur053_k6_d2b_smoke import hook_pod
from dur053_k6_d_render import set_env
from dur053_k6_e1_join import EVENTS, TASKS, by, expect, one
from dur053_k6_e1_smoke import ROLES, TAG, e1_receiver_url, workload
from dur053_k6_f1a_smoke import (
    F1aEvidence,
    F1aSmoke,
    JsonObject,
    audit_topology,
    deployment_objects,
)
from dur053_k6_f2_inputs import (
    CELLS,
    PG_ROLES,
    PROBE_PREFIX,
    QUIET_NS,
    backlog,
    container_row,
    in_flight,
    join,
    pending,
    quiet_digest,
    runtime_start,
)
from dur053_k6_hook import Receiver, serve
from dur053_k6_identity import obj
from dur053_k6_registry import read_registry

STOP_TIMEOUT = "10"
SAMPLE_INTERVAL = 0.25
PG_HEALTH = """SELECT jsonb_build_object('postmaster_start', pg_postmaster_start_time(),
    'backend_pid', pg_backend_pid(), 'read_only', current_setting('transaction_read_only'));"""
PG_BACKENDS = """
SELECT jsonb_build_object('postmaster_start', pg_postmaster_start_time(),
  'read_only', current_setting('transaction_read_only'), 'backends',
  ARRAY(SELECT jsonb_build_object('application', application_name, 'pid', pid,
      'backend_start', backend_start, 'client_addr', client_addr)
    FROM pg_stat_activity WHERE datname = current_database() AND backend_type = 'client backend'
      AND application_name IN
        ('durable-api','durable-worker-control','durable-scheduler','durable-relay')
    ORDER BY application_name, pid));
"""
CUT_SQL = """
SELECT jsonb_build_object(
  'db_at', clock_timestamp(), 'read_only', current_setting('transaction_read_only'),
  'workflows', COALESCE(jsonb_object_agg(w.workflow_id, jsonb_build_object(
    'workflow', to_jsonb(w),
    'history', ARRAY(SELECT to_jsonb(h) FROM engine.transition_history h
      WHERE h.workflow_id = w.workflow_id ORDER BY revision),
    'outbox', ARRAY(SELECT to_jsonb(o) FROM (
      SELECT event_id,workflow_id,aggregate_revision,event_type,topic,publish_state,relay_attempts,
        claim_owner,claim_expires_at,published_at,traceparent
      FROM engine.outbox WHERE workflow_id = w.workflow_id
      ORDER BY aggregate_revision,event_id) o),
    'attempts', ARRAY(SELECT to_jsonb(a) FROM (
      SELECT workflow_id,node_id,iteration,attempt_number,state,is_current,worker_id,
        worker_request_id,heartbeat_deadline,result,effect_class,outcome_disposition,
        encode(sha256(convert_to(claim_token::text,'UTF8')),'hex') token_hash,
        encode(sha256(convert_to(result::text,'UTF8')),'hex') result_hash
      FROM engine.activity_attempts WHERE workflow_id = w.workflow_id
      ORDER BY node_id,iteration,attempt_number) a),
    'inbox', ARRAY(SELECT to_jsonb(i) FROM engine.event_inbox i
      JOIN engine.outbox o USING (event_id) WHERE o.workflow_id = w.workflow_id
      ORDER BY i.consumer_id,i.topic,i.kafka_partition,i.kafka_offset),
    'publications', ARRAY(SELECT to_jsonb(p) FROM engine.outbox_publications p
      JOIN engine.outbox o USING (event_id) WHERE o.workflow_id = w.workflow_id
      ORDER BY p.event_id,p.relay_attempt),
    'claims', ARRAY(SELECT to_jsonb(c) FROM engine.k4_claim_audit c
      WHERE c.workflow_id = w.workflow_id ORDER BY c."order"))), '{}'::jsonb))
FROM engine.workflow_executions w;
"""

# The persistent exec opens before the restart so request timestamps describe
# actual API calls during the short PostgreSQL outage, including retries.
SUBMIT_LOOP = """
import concurrent.futures
import http.client
import json
import sys
import time
from pathlib import Path

def submit(offer):
    samples = []
    deadline = time.monotonic() + 25
    while time.monotonic() < deadline:
        connection = http.client.HTTPConnection("api:8080", timeout=2)
        sample = {"started_ns": time.time_ns()}
        try:
            connection.request(
                "POST", "/v1/workflows", json.dumps(offer["request"], separators=(",", ":")),
                {"Authorization": "Bearer " + Path("/identity/token").read_text().strip(),
                 "Content-Type": "application/json", "traceparent": offer["traceparent"]},
            )
            reply = connection.getresponse()
            sample["status"] = reply.status
            if reply.status in (200, 201):
                body = json.load(reply)
                sample.update(workflow_id=body["workflow"]["workflow_id"], created=body["created"])
        except (OSError, http.client.HTTPException, KeyError, ValueError) as error:
            sample["reason"] = type(error).__name__
        finally:
            connection.close()
            samples.append({**sample, "at_ns": time.time_ns()})
        if "workflow_id" in sample or 400 <= sample.get("status", 0) < 500:
            break
        time.sleep(0.08)
    return {**offer, "samples": samples}

print(json.dumps({"ready": True}), flush=True)
for line in sys.stdin:
    offers = json.loads(line)
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(offers)) as pool:
        print(json.dumps(list(pool.map(submit, offers))), flush=True)
"""

BROKER_PROC = r"""
for p in /proc/[0-9]*; do
  if [ "$(cat "$p/comm" 2>/dev/null)" = java ]; then
    cat "$p/stat"
  fi
done
"""


class F2Evidence(F1aEvidence):
    schema = "dur053.k6-phase-f2-smoke.v1"


class LineProcess:
    """Bounded line exchange with a subprocess whose connection spans a fault."""

    def __init__(self, args: list[str], env: dict) -> None:
        self.lines: Queue[str | None] = Queue()
        self.process = subprocess.Popen(
            args,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
            bufsize=1,
        )
        self.reader = threading.Thread(target=self.read_lines, daemon=True)
        self.reader.start()

    def read_lines(self) -> None:
        assert self.process.stdout is not None
        for line in self.process.stdout:
            self.lines.put(line)
        self.lines.put(None)

    def send(self, line: str) -> None:
        assert self.process.stdin is not None
        self.process.stdin.write(line + "\n")
        self.process.stdin.flush()

    def read(self, timeout: float) -> Any:
        line = self.lines.get(timeout=timeout)
        if line is None:
            raise EOFError("line subprocess exited")
        return json.loads(line)

    def close(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=3)
        self.reader.join(timeout=3)
        assert self.process.stdin is not None and self.process.stdout is not None
        with suppress(BrokenPipeError):
            self.process.stdin.close()
        self.process.stdout.close()


class Watch:
    def __init__(self, sample: Callable[[], JsonObject]) -> None:
        self.sample = sample
        self.rows: list[JsonObject] = []
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self) -> None:
        while not self.stop.is_set():
            started = time.time_ns()
            try:
                result = self.sample()
            except Exception as error:
                result = {"ok": False, "fatal": type(error).__name__}
            self.rows.append({"started_ns": started, "at_ns": time.time_ns(), **result})
            if "fatal" in result:
                break
            self.stop.wait(max(0, SAMPLE_INTERVAL - (time.time_ns() - started) / 1e9))

    def close(self) -> None:
        self.stop.set()
        self.thread.join(timeout=5)
        require(not self.thread.is_alive(), "F2_WATCH_STOP_TIMEOUT")


def stat(raw: str) -> dict:
    pid, _, _ = raw.partition(" ")
    fields = raw.rsplit(")", 1)[-1].split()
    require(pid.isdecimal() and len(fields) >= 20 and fields[19].isdecimal(), "F2_PROC_STAT")
    return {"pid": int(pid), "start_ticks": int(fields[19])}


class F2Smoke(F1aSmoke):
    inputs_schema = "dur053.k6-phase-f2-inputs.v1"

    def use_campaign_build(self, f1b_output: Path) -> None:
        path = f1b_output / "campaign-build.json"
        require(path.is_file(), "F2_F1B_CAMPAIGN_BUILD_MISSING")
        build = json.loads(path.read_bytes())
        expect("F2_CAMPAIGN_BUILD_TAG", build["tag"], TAG)
        expect("F2_CAMPAIGN_BASE_IMAGES", build["ordinary_images"], self.harness.images)
        expect("F2_CAMPAIGN_TAGGED_ROLES", sorted(build["images"]), ["runtime", "worker"])
        self.tags = build["images"]
        self.command(
            "F2_KAFKA_PROBE_BUILD",
            "go",
            "build",
            "-p",
            "1",
            "-buildvcs=false",
            "-trimpath",
            "-o",
            str(self.kafka_probe),
            "./cmd/dur053-k6-hook-kafka",
            timeout=300,
        )
        expect(
            "F2_CAMPAIGN_KAFKA_PROBE_HASH",
            sha256(self.kafka_probe.read_bytes()).hexdigest(),
            build["kafka_probe_sha256"],
        )
        self.evidence.write("campaign-build.json", {**build, "reused_phase": "F1b"})

    def cell_setup(self, cell: str) -> None:
        self.cell_id = cell
        self.namespace = "dur053-k6-f2-" + cell
        self.database = self.namespace.replace("-", "_")
        self.monitor_role = self.database + "_monitor"
        self.monitor_password = secrets.token_hex(32)
        self.evidence.secret(self.monitor_password)
        self.monitor_created = False
        self.restart_target = None
        self.probe_topic = None
        self.setup_episode()
        self.sql(
            "F2_MONITOR_CREATE",
            f"""
          CREATE ROLE {self.monitor_role} LOGIN PASSWORD '{self.monitor_password}';
          ALTER ROLE {self.monitor_role} SET default_transaction_read_only = on;
          ALTER ROLE {self.monitor_role} SET statement_timeout = '1500ms';
          GRANT pg_read_all_stats TO {self.monitor_role};
        """,
        )
        self.monitor_created = True
        self.pg(f"""
          GRANT USAGE ON SCHEMA engine TO {self.monitor_role};
          GRANT SELECT ON ALL TABLES IN SCHEMA engine TO {self.monitor_role};
        """)

    def render_episode(self, source: list[JsonObject]) -> list[JsonObject]:
        items = deployment_objects(source, self.namespace, self.tags, self.harness.images)
        if self.cell_id == "medium-5":
            relay = one(
                "F2_RELAY_DEPLOYMENT",
                [
                    r
                    for r in items
                    if r["kind"] == "Deployment" and r["metadata"]["name"] == "relay"
                ],
            )
            container = relay["spec"]["template"]["spec"]["containers"][0]
            container["image"] = self.tags["runtime"]
            set_env(container, DUR053_K6_RELAY_STALL="1")
            hook_pod(relay, "relay", e1_receiver_url(self.receiver_address, self.port), {})
            items.append(
                policy(
                    self.namespace,
                    "f2-hook-relay",
                    "relay",
                    "Egress",
                    [traffic("to", self.port, ip_peer(self.receiver_address))],
                )
            )
        audit_topology(items, {*self.tags.values(), *self.harness.images.values()})
        return items

    def prepare_episode_dependencies(self) -> None:
        if self.cell_id == "medium-5":
            self.create(
                "F2_RELAY_HOOK_CA",
                obj(
                    "ConfigMap",
                    "k6-hook-ca",
                    self.namespace,
                    immutable=True,
                    data={"ca.crt": self.cert.read_text()},
                ),
            )
            self.create(
                "F2_RELAY_HOOK_TOKEN",
                obj(
                    "Secret",
                    "hook-relay",
                    self.namespace,
                    immutable=True,
                    stringData={"token": self.token},
                ),
            )

    def topic(self, operation: str) -> None:
        assert self.probe_topic and self.probe_topic.startswith(PROBE_PREFIX)
        options = (
            ("--partitions", "1", "--replication-factor", "1")
            if operation == "--create"
            else ("--if-exists",)
        )
        self.command(
            "F2_PROBE_TOPIC",
            "docker",
            "exec",
            CLUSTER + "-kafka",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "localhost:19092",
            operation,
            "--topic",
            self.probe_topic,
            *options,
            timeout=25,
        )

    def read_until[T](
        self,
        stage: str,
        sample: Callable[[], tuple[T | None, dict]],
        seconds: float,
        *,
        interval: float = SAMPLE_INTERVAL,
    ) -> T:
        end = time.monotonic() + seconds
        observations = []
        try:
            while time.monotonic() < end:
                value, details = sample()
                observations.append({"at_ns": time.time_ns(), **details})
                if value is not None and time.monotonic() < end:
                    return value
                time.sleep(interval)
            raise SmokeFailure(f"{stage} evidence={self.cell_id}-{stage.lower()}-samples.json")
        finally:
            self.retain(stage.lower() + "-samples", observations)

    def psql_args(self) -> list[str]:
        return [
            "psql",
            "-XqAt",
            "-w",
            "-v",
            "ON_ERROR_STOP=1",
            "-h",
            self.engine_address,
            "-U",
            self.monitor_role,
            "-d",
            self.database,
        ]

    def psql_env(self) -> dict:
        return {
            **self.env,
            "PGPASSWORD": self.monitor_password,
            "PGCONNECT_TIMEOUT": "2",
            "PGAPPNAME": "dur053-f2-monitor",
            "PGSSLMODE": "disable",
        }

    def monitor_sample(self, sql: str) -> dict:
        try:
            result = subprocess.run(
                self.psql_args(),
                env=self.psql_env(),
                input=sql,
                text=True,
                capture_output=True,
                timeout=2.5,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return {"ok": False, "reason": "F2_PG_CLIENT_TIMEOUT"}
        if result.returncode:
            return {
                "ok": False,
                "exit_code": result.returncode,
                "reason": "F2_PG_CLIENT_FAILED",
                "stderr": result.stderr.strip()[:400],
            }
        return {"ok": True, "value": json.loads(result.stdout)}

    def monitor(self, sql: str) -> dict:
        sample = self.monitor_sample(sql)
        require(sample["ok"], f"F2_MONITOR_QUERY_FAILED {sample!r}")
        return sample["value"]

    def monitor_cut(self) -> dict:
        started = time.time_ns()
        cut = self.monitor(CUT_SQL)
        return {**cut, "started_ns": started, "at_ns": time.time_ns()}

    def kafka_probe_sample(self) -> dict:
        assert self.probe_topic is not None
        nonce = secrets.token_hex(16)
        result = subprocess.run(
            [
                str(self.kafka_probe),
                "-broker",
                self.broker + ":19092",
                "-topic",
                self.probe_topic,
                "-probe",
                nonce,
                "-timeout",
                "800ms",
            ],
            env=self.env,
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        value = json.loads(result.stdout)
        return {
            "ok": result.returncode == 0,
            "value": value,
            "exit_code": result.returncode,
            "nonce": nonce,
            "reason": result.stderr.strip()[:500],
        }

    def engine_topics(self) -> dict:
        return {topic: self.kafka(topic) for topic in (EVENTS, TASKS)}

    def sender(self, stack: ExitStack) -> LineProcess:
        sender = LineProcess(
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
                "-c",
                SUBMIT_LOOP,
            ],
            self.env,
        )
        stack.callback(sender.close)
        try:
            expect("F2_SUBMIT_READY", sender.read(8), {"ready": True})
        except (Empty, EOFError):
            raise SmokeFailure("F2_SUBMIT_OBSERVER_UNAVAILABLE") from None
        return sender

    def planned(self, index: int) -> dict:
        payload, fingerprint, traceparent = workload(f"f2-{self.cell_id}-{index}")
        payload["namespace"] = self.namespace
        return {
            "request": payload,
            "workflow": payload["workflow_id"],
            "fingerprint": fingerprint,
            "traceparent": traceparent,
        }

    def offer(self, sender: LineProcess, *indices: int) -> list[dict]:
        offers = [self.planned(index) for index in indices]
        name = "offer-" + "-".join(str(i) for i in indices)
        self.retain(name + "-requested", offers)
        try:
            sender.send(json.dumps(offers))
            result = sender.read(30)
        except (Empty, EOFError, OSError):
            raise SmokeFailure("F2_SUBMISSION_OBSERVER_LOST") from None
        self.retain(name + "-samples", result)
        expect(
            "F2_SUBMISSION_BATCH", [r["workflow"] for r in result], [r["workflow"] for r in offers]
        )
        for offer in result:
            final = offer["samples"][-1]
            require(
                final.get("workflow_id") == offer["workflow"] and final["status"] in (200, 201),
                f"F2_SUBMISSION_UNRESOLVED {offer['workflow']} evidence={name}-samples",
            )
        return result

    def stalled_relay(self, sender: LineProcess) -> tuple[dict, list[dict]]:
        _, before = self.process("relay")
        workflow = self.planned(0)["workflow"]
        target = {k: before[k] for k in ("actor", "uid", "process_start_id", "session")}
        self.receiver.arm(
            "medium-5",
            "before_publish",
            target,
            {
                "key": workflow,
                "cell": "medium-5",
                "type": "workflow.created",
            },
            45,
        )
        offers = self.offer(sender, 0)

        def held() -> tuple[dict | None, dict]:
            row = self.receiver.observe(self.cell_id)
            return (row if row["state"] == "held" else None), {
                "state": row["state"],
                "target": target,
            }

        arm = self.read_until("F2_RELAY_HOLD_ACK", held, 15)
        initial = self.monitor_cut()
        self.retain("relay-hold-initial", initial)
        marks_before = self.engine_topics()
        arrivals = self.offer(sender, 1, 2)
        during = self.monitor_cut()
        self.retain("relay-hold-during", during)
        marks_during = self.engine_topics()
        self.retain("relay-marks", {"before": marks_before, "during": marks_during})
        probe = {"started_ns": time.time_ns(), **self.kafka_probe_sample(), "at_ns": time.time_ns()}
        self.retain("relay-probe", probe)
        _, same = self.process("relay")
        self.receiver.authorize(
            self.cell_id,
            arm["ack"],
            "release",
            evidence={
                "database_at_ns": during["at_ns"],
                "probe_at_ns": probe["at_ns"],
            },
        )

        def released() -> tuple[bool | None, dict]:
            state = self.receiver.observe(self.cell_id)["state"]
            return (True if state == "done" else None), {"state": state}

        self.read_until("F2_RELAY_AUTHENTICATED_RELEASE", released, 10)
        _, after = self.process("relay")
        return {
            "seed": workflow,
            "arm": arm,
            "journal": self.hook_log.read_text(),
            "cuts": [initial, during],
            "relay": [before, same, after],
            "probe": probe,
            "probe_topic": self.probe_topic,
            "marks_before": marks_before,
            "marks_during": marks_during,
        }, offers + arrivals

    def inspect(self, target: str) -> list[dict]:
        raw = json.loads(self.command("F2_DOCKER_INSPECT", "docker", "inspect", target, timeout=5))
        row = one("F2_DOCKER_INSPECT_ROW", raw)
        # Keep Docker's field names and nesting, excluding environment values.
        return [
            {
                **{key: row[key] for key in ("Id", "Name", "State")},
                "Config": {"StopSignal": row["Config"].get("StopSignal")},
                "NetworkSettings": {"Networks": row["NetworkSettings"]["Networks"]},
            }
        ]

    def broker_process(self) -> dict:
        rows = (
            self.command(
                "F2_BROKER_PROC",
                "docker",
                "exec",
                CLUSTER + "-kafka",
                "sh",
                "-c",
                BROKER_PROC,
                timeout=5,
            )
            .decode()
            .splitlines()
        )
        return stat(one("F2_BROKER_JAVA_PROCESS", rows))

    def postmaster_pid(self) -> int:
        raw = self.command(
            "F2_PG_POSTMASTER_PID",
            "docker",
            "exec",
            CLUSTER + "-postgres",
            "sh",
            "-c",
            'head -n 1 "$PGDATA/postmaster.pid"',
            timeout=5,
        )
        pid = int(raw.strip())
        require(pid > 0, "F2_PG_POSTMASTER_PID_INVALID")
        return pid

    def engine_processes(self, stage: str) -> dict:
        engines: dict[str, list[dict]] = {}
        for role in PG_ROLES:
            engines[role] = []
            for pod in self.pods(role):
                row = one("F2_ENGINE_CONTAINER", by(pod["status"]["containerStatuses"], name=role))
                cid = row["containerID"].removeprefix("containerd://")
                node = pod["spec"]["nodeName"]
                info = json.loads(
                    self.command(
                        "F2_ENGINE_CRI", "docker", "exec", node, "crictl", "inspect", cid, timeout=4
                    )
                )
                uid = pod["metadata"]["uid"]
                expect("F2_ENGINE_CRI_UID", info["status"]["labels"]["io.kubernetes.pod.uid"], uid)
                start = self.proc_start(node, info["info"]["pid"])
                require(start is not None, "F2_ENGINE_PROCESS_GONE")
                name = pod["metadata"]["name"]
                raw = self.role_log(name, role).decode()
                self.evidence.write_bytes(f"{self.cell_id}-{stage}-{name}.log", raw.encode())
                engines[role].append(
                    {
                        "uid": uid,
                        "pod": name,
                        "container_id": cid,
                        "process_start": start,
                        "runtime_log": runtime_start(raw),
                    }
                )
        self.retain("engine-processes-" + stage, engines)
        return engines

    def retained_rows(self, workflow: str, revision: int) -> dict:
        # Select the fixed pre-restart keys. The activity gate keeps the seed
        # execution row stable and history may only append at later revisions.
        expect("F2_RETAINED_WORKFLOW_ID", workflow, self.planned(0)["workflow"])
        return self.monitor(f"""
          WITH w AS (SELECT to_jsonb(w) value FROM engine.workflow_executions w
            WHERE workflow_id='{workflow}'),
          h AS (SELECT to_jsonb(h) value, revision FROM engine.transition_history h
            WHERE workflow_id='{workflow}' AND revision<={int(revision)})
          SELECT jsonb_build_object(
            'workflows', (SELECT jsonb_build_object('count', count(*), 'hash',
              encode(sha256(convert_to(COALESCE(jsonb_agg(value),'[]')::text,'UTF8')),
              'hex')) FROM w),
            'history', (SELECT jsonb_build_object('count', count(*), 'hash',
              encode(sha256(convert_to(
                COALESCE(jsonb_agg(value ORDER BY revision),'[]')::text,'UTF8')),
              'hex')) FROM h));
        """)

    def pg_sampler(self, client: LineProcess) -> Callable[[], dict]:
        live = True

        def sample() -> dict:
            nonlocal live
            if live:
                try:
                    client.send(PG_HEALTH)
                    return {"ok": True, "value": client.read(1), "connection": "pre-restart"}
                except (EOFError, Empty, OSError) as error:
                    live = False
                    return {
                        "ok": False,
                        "reason": "F2_PG_EXISTING_CONNECTION_LOST",
                        "failure": type(error).__name__,
                        "connection": "pre-restart",
                    }
            return {**self.monitor_sample(PG_HEALTH), "connection": "fresh"}

        return sample

    def wait_watch(self, watch: Watch, start: int, *, healthy: bool, seconds: float) -> dict:
        label = "F2_RESTART_RECOVERY_SAMPLE" if healthy else "F2_RESTART_OUTAGE_SAMPLE"

        def check() -> tuple[dict | None, dict]:
            eligible = [r for r in watch.rows if r["at_ns"] >= start]
            require(not any("fatal" in r for r in eligible), "F2_INDEPENDENT_WATCH_FAILED")
            matches = [
                r
                for r in eligible
                if r["ok"] is healthy and (healthy or not r.get("value", {}).get("produced"))
            ]
            return (matches[0] if matches else None), {
                "count": len(eligible),
                "last": eligible[-1:] or watch.rows[-1:],
            }

        return self.read_until(label, check, seconds, interval=0.02)

    def start_events(self, stack: ExitStack, target_id: str) -> tuple[Path, subprocess.Popen]:
        path = self.scratch / f"{self.cell_id}-docker-events.jsonl"
        output = stack.enter_context(path.open("wb"))
        watch = subprocess.Popen(
            [
                "docker",
                "events",
                "--since",
                str(time.time_ns() // 10**9 - 1),
                "--filter",
                "type=container",
                "--filter",
                f"container={target_id}",
                "--format",
                "{{json .}}",
            ],
            env=self.env,
            stdout=output,
            stderr=subprocess.DEVNULL,
        )

        def stop() -> None:
            if watch.poll() is None:
                watch.terminate()
                try:
                    watch.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    watch.kill()
                    watch.wait(timeout=3)
            output.close()

        stack.callback(stop)
        return path, watch

    def seed_active(self, workflow: str) -> dict:
        def active() -> tuple[dict | None, dict]:
            cut = self.monitor_cut()
            rows = cut["workflows"].get(workflow, {}).get("attempts", [])
            return (cut if by(rows, is_current=True, state="CLAIMED") else None), {
                "workflow": workflow,
                "attempts": rows,
            }

        cut = self.read_until("F2_DURABLE_CLAIM", active, 35)
        in_flight(cut, workflow)
        return cut

    def restart(self, sender: LineProcess) -> tuple[dict, list[dict]]:
        pg = self.cell_id == "medium-6"
        target = CLUSTER + ("-postgres" if pg else "-kafka")
        before = self.inspect(target)
        identity = container_row(before, target)
        expected_ip = self.engine_address if pg else self.broker
        expect("F2_RESTART_PINNED_ADDRESS", identity["ip"], expected_ip)
        image_signal = json.loads(
            self.command(
                "F2_DOCKER_IMAGE_STOP_SIGNAL",
                "docker",
                "image",
                "inspect",
                "--format",
                "{{json .Config.StopSignal}}",
                self.command(
                    "F2_DOCKER_IMAGE",
                    "docker",
                    "inspect",
                    "--format",
                    "{{.Image}}",
                    target,
                    timeout=4,
                )
                .decode()
                .strip(),
                timeout=4,
            )
        )
        self.retain("docker-before", before)
        old_engines = self.engine_processes("before") if pg else {}
        old_broker = self.broker_process() if not pg else {}
        old_pid = self.postmaster_pid() if pg else None
        offers = self.offer(sender, 0)
        workflow = offers[0]["workflow"]
        self.seed_active(workflow)
        old_marks = self.engine_topics() if not pg else {}
        with ExitStack() as stack:
            events_path, events_process = self.start_events(stack, identity["id"])
            if pg:
                psql = LineProcess(self.psql_args(), self.psql_env())
                stack.callback(psql.close)
                sample = self.pg_sampler(psql)
            else:
                sample = self.kafka_probe_sample
            watch = Watch(sample)
            stack.callback(watch.close)
            pool = stack.enter_context(ThreadPoolExecutor(max_workers=2))
            try:
                self.wait_watch(watch, 0, healthy=True, seconds=12)
                before_cut = self.monitor_cut()
                in_flight(before_cut, workflow)
                rev = before_cut["workflows"][workflow]["workflow"]["revision"]
                old_pg = (
                    {
                        **self.monitor(PG_BACKENDS),
                        "postmaster_pid": old_pid,
                        "retained": self.retained_rows(workflow, rev),
                    }
                    if pg
                    else {}
                )
                self.retain("restart-before-cut", before_cut)
                self.retain(
                    "restart-dependency-before",
                    old_pg
                    if pg
                    else {
                        "broker": old_broker,
                        "topics": old_marks,
                    },
                )
                require(events_process.poll() is None, "F2_DOCKER_EVENT_STREAM_STOPPED")
                issued = time.time_ns()
                self.restart_target = target

                def restart_command() -> tuple[int, int]:
                    result = self.harness.command(
                        ["docker", "restart", "--time", STOP_TIMEOUT, target],
                        allowed=True,
                        timeout=30,
                    )
                    completed = time.time_ns()
                    self.retain(
                        "restart-command",
                        {
                            "target": target,
                            "issued_ns": issued,
                            "completed_ns": completed,
                            "exit_code": result.returncode,
                            "timeout_seconds": int(STOP_TIMEOUT),
                            "image_stop_signal": image_signal,
                        },
                    )
                    return result.returncode, completed

                command = pool.submit(restart_command)
                first_failure = self.wait_watch(watch, issued, healthy=False, seconds=20)
                offered = pool.submit(self.offer, sender, 1, 2)
                pending_cut = {}
                if not pg:
                    arrivals = offered.result(timeout=32)
                    pending_cut = self.monitor_cut()
                    self.retain("kafka-pending-during", pending_cut)
                exit_code, completed = command.result(timeout=32)
                expect("F2_RESTART_COMMAND_EXIT", exit_code, 0)
                after = self.inspect(target)
                self.retain("docker-after", after)
                address = container_row(after, target)["ip"]
                expect(
                    "F2_PG_ADDRESS_CHANGED" if pg else "F2_KAFKA_ADDRESS_CHANGED",
                    address,
                    expected_ip,
                )
                if not pg:
                    backlog(pending_cut, arrivals)
                self.wait_watch(watch, first_failure["at_ns"], healthy=True, seconds=45)
                arrivals = offered.result(timeout=32)
                detail = {}
                if pg:
                    retained = self.retained_rows(workflow, rev)
                    new_pid = self.postmaster_pid()

                    def reconnected() -> tuple[dict | None, dict]:
                        result = self.monitor(PG_BACKENDS)
                        apps = {row["application"] for row in result["backends"]}
                        return (
                            result if apps == {"durable-" + role for role in PG_ROLES} else None
                        ), result

                    new_pg = self.read_until("F2_PG_ENGINE_RECONNECT", reconnected, 15)
                    new_pg.update(postmaster_pid=new_pid, retained=retained)
                    for role in PG_ROLES:
                        self.rollout(role)
                    new_engines = self.engine_processes("after")
                    detail.update(
                        postgres=[old_pg, new_pg],
                        engines={role: [old_engines[role], new_engines[role]] for role in PG_ROLES},
                    )
                else:
                    detail.update(
                        broker_process=[old_broker, self.broker_process()],
                        marks=[old_marks, self.engine_topics()],
                        pending_cut=pending_cut,
                        probe_topic=self.probe_topic,
                    )
            finally:
                pool.shutdown(wait=True)
                watch.close()
                self.retain("restart-probes", watch.rows)
                if self.restart_target == target:
                    require(events_process.poll() is None, "F2_DOCKER_EVENT_STREAM_STOPPED")
                    events = events_path.read_text()
                    self.evidence.write_bytes(
                        f"{self.cell_id}-docker-events.jsonl", events.encode()
                    )
        return {
            "seed": workflow,
            "before_cut": before_cut,
            "target": target,
            "issued_ns": issued,
            "completed_ns": completed,
            "exit_code": exit_code,
            "inspect": [before, after],
            "samples": watch.rows,
            "events": events,
            "stop_timeout_seconds": int(STOP_TIMEOUT),
            "image_stop_signal": image_signal,
            **detail,
        }, offers + arrivals

    def closeout(self, fault: dict, offers: list[dict], registered: dict) -> dict:
        marker = self.release_activity()
        expected = {offer["workflow"] for offer in offers}
        quiet: list[dict] = []

        def sample() -> tuple[tuple[dict, list[dict]] | None, dict]:
            cut = self.monitor_cut()
            rows, seals = self.witness()
            self.require_healthy_gates(rows)
            states = {w: row["workflow"]["state"] for w, row in cut["workflows"].items()}
            wait = {"states": states, "pending": sorted(pending(cut))}
            complete = (
                set(states) == expected
                and set(states.values()) == {"SUCCEEDED"}
                and not pending(cut)
            )
            if complete:
                complete = all(
                    by(row["inbox"], event_id=event["event_id"], disposition="ACCEPTED")
                    for row in cut["workflows"].values()
                    for event in row["outbox"]
                )
            if not complete:
                quiet.clear()
                return None, wait
            digest = quiet_digest(cut, rows)
            if quiet and quiet[-1]["digest"] != digest:
                quiet.clear()
            quiet.append(
                {
                    "started_ns": cut["started_ns"],
                    "at_ns": time.time_ns(),
                    "cut_at_ns": cut["at_ns"],
                    "digest": digest,
                }
            )
            if quiet[-1]["started_ns"] - quiet[0]["at_ns"] < QUIET_NS:
                time.sleep(0.75)
                return None, wait
            self.retain("terminal-witness", seals)
            return (cut, rows), wait

        cut, rows = self.read_until("F2_TERMINAL_QUIET", sample, 130)
        inputs = {
            "cell": self.cell_id,
            "namespace": self.namespace,
            "fault": fault,
            "offers": offers,
            "terminal": cut,
            "witness": rows,
            "workers": self.pods("worker"),
            "marker": marker,
            "topics": self.engine_topics(),
            "quiet": quiet,
            "registered": registered,
        }
        self.retain("checker-inputs", inputs)
        result = join(inputs)
        self.retain("joined", result)
        return result

    def cleanup(self) -> None:
        if self.restart_target:
            target = self.restart_target
            running = self.command(
                "F2_CLEANUP_INSPECT",
                "docker",
                "inspect",
                "--format",
                "{{.State.Running}}",
                target,
                timeout=5,
            ).strip()
            if running != b"true":
                self.command("F2_CLEANUP_DOCKER_START", "docker", "start", target, timeout=30)
            if self.monitor_created and target.endswith("-postgres"):
                self.read_until("F2_CLEANUP_PG_READY", self.cleanup_pg, 30)
            self.restart_target = None
        try:
            if self.probe_topic:
                self.topic("--delete")
        finally:
            try:
                self.clear_episode()
            finally:
                if self.monitor_created:
                    self.sql("F2_MONITOR_DROP", f"DROP ROLE {self.monitor_role};")
                    self.monitor_created = False

    def cleanup_pg(self) -> tuple[bool | None, dict]:
        result = self.harness.command(
            ["docker", "exec", CLUSTER + "-postgres", "pg_isready", "-U", "durable"],
            allowed=True,
            record=False,
            timeout=4,
        )
        return (True if result.returncode == 0 else None), {"exit_code": result.returncode}


def inherited_failure(output: Path, code: str = "F2_SHARED_HARNESS_FAILED") -> None:
    if not output.exists():
        evidence = F2Evidence(make_output(output))
        evidence.report.update(reason=code)
        evidence.checkpoint()


def record_handoff_failure(output: Path, error: BaseException) -> None:
    inherited_failure(output, "F2_HANDOFF_" + type(error).__name__.upper())


def run(output: Path, harness: Driver, addresses: dict[str, str], f1b_output: Path) -> int:
    started = time.monotonic()
    evidence = F2Evidence(make_output(output))
    failure = None
    with ExitStack() as resources:
        try:
            require(
                os.name == "posix" and bool(os.getenv("RUNNER_TEMP")), "F2_HOSTED_RUNNER_REQUIRED"
            )
            scratch = Path(
                resources.enter_context(
                    tempfile.TemporaryDirectory(
                        prefix="dur053-k6-f2-", dir=os.environ["RUNNER_TEMP"]
                    )
                )
            )
            smoke = F2Smoke(evidence, scratch, harness, addresses)
            registry = read_registry()
            with evidence.step("f2-build"):
                smoke.use_campaign_build(f1b_output)
                smoke.receiver_address = (
                    smoke.command(
                        "F2_KIND_GATEWAY",
                        "docker",
                        "inspect",
                        "--format",
                        '{{(index .NetworkSettings.Networks "kind").Gateway}}',
                        CLUSTER + "-control-plane",
                        timeout=5,
                    )
                    .decode()
                    .strip()
                )
                smoke.cert, key = smoke.tls()
            for cell in CELLS:
                smoke.cell_id = cell
                smoke.deadline = datetime.now(UTC) + timedelta(minutes=7)
                cell_started = time.monotonic()
                clean = True
                with ExitStack() as hooks:
                    try:
                        with evidence.step(cell + "-topology"):
                            episode = min(
                                by(registry["episodes"], cell=cell), key=lambda r: r["ordinal"]
                            )
                            registered = one(
                                "F2_REGISTRY_ARM", by(registry["coverage"], arm=episode["arm"])
                            )
                            smoke.retain("registered", registered)
                            if cell == "medium-5":
                                smoke.hook_log = scratch / f"{cell}-hook.jsonl"
                                smoke.token = secrets.token_hex(32)
                                evidence.secret(smoke.token)
                                smoke.receiver = Receiver(smoke.hook_log, {"relay": smoke.token})
                                hooks.callback(smoke.receiver.file.close)
                                smoke.port = hooks.enter_context(
                                    serve(smoke.receiver, smoke.receiver_address, smoke.cert, key)
                                )
                            smoke.cell_setup(cell)
                            if cell != "medium-6":
                                smoke.probe_topic = PROBE_PREFIX + cell + "-" + secrets.token_hex(6)
                                smoke.topic("--create")
                        with evidence.step(cell) as row, ExitStack() as sender_stack:
                            sender = smoke.sender(sender_stack)
                            fault, offers = (
                                smoke.stalled_relay(sender)
                                if cell == "medium-5"
                                else smoke.restart(sender)
                            )
                            smoke.retain("fault", fault)
                            result = smoke.closeout(fault, offers, registered)
                            row.update(
                                confirmation=result["confirmation"],
                                accepted=result["accepted"],
                                registered=result["invariants_covered"],
                            )
                    except Exception as error:
                        failure = failure or evidence.safe_reason(error)
                    finally:
                        try:
                            with evidence.step(cell + "-diagnostics"):
                                if cell == "medium-5" and smoke.hook_log.exists():
                                    with smoke.receiver.lock:
                                        evidence.write_bytes(
                                            f"{cell}-host-hook-journal.jsonl",
                                            smoke.hook_log.read_bytes(),
                                        )
                                if smoke.namespace_started:
                                    smoke.collect_role_logs(smoke.namespace, ROLES)
                        except Exception as error:
                            failure = failure or evidence.safe_reason(error)
                        try:
                            with evidence.step(cell + "-cleanup"):
                                smoke.cleanup()
                        except Exception as error:
                            failure = failure or evidence.safe_reason(error)
                            clean = False
                evidence.report.setdefault("cell_elapsed_seconds", {})[cell] = round(
                    time.monotonic() - cell_started, 3
                )
                evidence.checkpoint()
                if not clean:
                    break
        except (Exception, KeyboardInterrupt) as error:
            failure = failure or evidence.safe_reason(error)
        finally:
            for cell in CELLS:
                if not by(evidence.report["outcomes"], step=cell):
                    evidence.report["outcomes"].append(
                        {
                            "step": cell,
                            "status": "SKIPPED",
                            "reason": "F2_SETUP_OR_CLEANUP_INCOMPLETE",
                        }
                    )
            try:
                evidence.scan()
            except SmokeFailure as error:
                failure = failure or evidence.safe_reason(error)
            evidence.report.update(
                status="FAIL" if failure else "PASS",
                reason=failure or "MATRIX_COMPLETE",
                elapsed_seconds=round(time.monotonic() - started, 3),
            )
            evidence.checkpoint()
    print(
        f"F2 {evidence.report['status']} {evidence.report['reason']} "
        f"elapsed_seconds={evidence.report['elapsed_seconds']}",
        flush=True,
    )
    return int(failure is not None)
