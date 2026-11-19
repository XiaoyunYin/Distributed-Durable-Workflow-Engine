"""Run the single hosted engineering lease takeover cell in a private database."""

from __future__ import annotations

import copy
import json
import os
import secrets
import tempfile
import time
from contextlib import ExitStack
from datetime import datetime
from hashlib import sha256
from pathlib import Path

from dur053_k3 import CLUSTER
from dur053_k6_b_evidence import SmokeFailure
from dur053_k6_b_smoke import json_data, require
from dur053_k6_c_smoke import make_output
from dur053_k6_d_render import set_env
from dur053_k6_e1_join import EVENTS, TASKS, broker_records, by, expect, one
from dur053_k6_e1_smoke import (
    DEFINITION,
    SUBMIT_TRACED,
    E1Evidence,
    Matrix,
    e1_receiver_address,
    ensure_no_e1_placeholders,
    workload,
)
from dur053_k6_e1_smoke import NS as E1_NAMESPACE
from dur053_k6_e1_smoke import deployment_objects as e1_objects
from dur053_k6_e2_join import (
    CELL,
    COMPANION,
    PRIMARY,
    acknowledged,
    confirm_rejection,
    lease_at_hold,
    lease_commit,
    recovery,
    reject_stale_writes,
    takeover,
    timeline,
)
from dur053_k6_hook import Receiver, now_ms, serve
from dur053_k6_identity import obj

NS = "dur053-k6-e2"
DB = "dur053_k6_e2"
WORKFLOW = "e2-hook-5"
PARTITION = int.from_bytes(sha256(WORKFLOW.encode()).digest()[:8], "big") % 16
PREDECESSOR = "predecessor"
SUCCESSOR = "successor"
ACTORS = (PREDECESSOR, SUCCESSOR, "worker", "relay")
ROLES = ("api", "worker-control", "scheduler", "scheduler-peer", "relay", "worker", "observer")
ROLE_LABEL = "dur053-lease-role"
TOKEN_REVIEW = "dur053-tokenreview-e2"

# Private engineering-only transactional witness. The same lock used by the
# existing commit audit orders the deferred records across concurrent commits.
# Only identities are captured, including for outbox updates and attempt results.
INSTALL_WRITE_WITNESS = r"""
BEGIN;
CREATE SCHEMA k6_e2;
CREATE SEQUENCE k6_e2.commit_order;
CREATE TABLE k6_e2.writes (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    commit_order bigint UNIQUE,
    relation text NOT NULL,
    operation text NOT NULL,
    row_identity jsonb NOT NULL,
    pid int NOT NULL DEFAULT pg_backend_pid(),
    session_id text NOT NULL DEFAULT coalesce(current_setting('durable.k4_session', true), ''),
    xid text NOT NULL DEFAULT pg_current_xact_id()::text,
    owner_id text,
    epoch bigint
);
CREATE FUNCTION k6_e2.finalize_write() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(530004, 1);
    UPDATE k6_e2.writes SET commit_order = nextval('k6_e2.commit_order') WHERE id = NEW.id;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER finalize_write AFTER INSERT ON k6_e2.writes
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION k6_e2.finalize_write();

CREATE FUNCTION k6_e2.record_write() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    row_data jsonb;
    row_identity jsonb;
    row_owner text;
    row_epoch bigint;
BEGIN
    IF TG_OP = 'DELETE' THEN
        row_data := to_jsonb(OLD);
    ELSE
        row_data := to_jsonb(NEW);
    END IF;
    IF TG_TABLE_NAME = 'partition_leases' THEN
        row_owner := coalesce(row_data->>'owner_id', to_jsonb(OLD)->>'owner_id');
        row_epoch := (row_data->>'epoch')::bigint;
        row_identity := jsonb_build_object('partition', row_data->'partition_id',
            'expires_at', row_data->'lease_expires_at', 'updated_at', row_data->'updated_at');
    ELSE
        row_owner := nullif(current_setting('durable.k4_owner', true), '');
        IF row_owner IS NOT NULL THEN
            SELECT epoch INTO row_epoch FROM engine.partition_leases
                WHERE partition_id = {partition} AND owner_id::text = row_owner;
        END IF;
        row_epoch := coalesce((row_data->>'scheduler_epoch')::bigint, row_epoch);
        row_identity := jsonb_build_object('workflow', row_data->'workflow_id',
            'revision', coalesce(row_data->'revision', row_data->'aggregate_revision'),
            'attempt', row_data->'attempt_number', 'node', row_data->'node_id',
            'iteration', row_data->'iteration', 'event', row_data->'event_id',
            'transition', row_data->'transition_id');
    END IF;
    INSERT INTO k6_e2.writes (relation, operation, row_identity, owner_id, epoch)
        VALUES (TG_TABLE_NAME, TG_OP, row_identity, row_owner, row_epoch);
    RETURN NULL;
END $$;
CREATE TRIGGER e2_partition_write AFTER INSERT OR UPDATE OR DELETE ON engine.partition_leases
FOR EACH ROW EXECUTE FUNCTION k6_e2.record_write();
CREATE TRIGGER e2_attempt_write AFTER INSERT OR UPDATE OR DELETE ON engine.activity_attempts
FOR EACH ROW EXECUTE FUNCTION k6_e2.record_write();
CREATE TRIGGER e2_outbox_write AFTER INSERT OR UPDATE OR DELETE ON engine.outbox
FOR EACH ROW EXECUTE FUNCTION k6_e2.record_write();
CREATE TRIGGER e2_history_write AFTER INSERT OR UPDATE OR DELETE ON engine.transition_history
FOR EACH ROW EXECUTE FUNCTION k6_e2.record_write();
COMMIT;
"""


class NotExercised(SmokeFailure):
    pass


class E2Evidence(E1Evidence):
    schema = "dur053.k6-phase-e2-smoke.v1"


def deployment_objects(objects: list[dict], tags: dict, address: str, port: int) -> list[dict]:
    result = json.loads(
        json.dumps(e1_objects(objects, tags, address, port)).replace(E1_NAMESPACE, NS)
    )
    for item in result:
        if item["kind"] in {"ClusterRole", "ClusterRoleBinding"}:
            item["metadata"]["name"] = TOKEN_REVIEW
            if item["kind"] == "ClusterRoleBinding":
                item["roleRef"]["name"] = TOKEN_REVIEW
    scheduler = one(
        "E2_RENDER_SCHEDULER",
        [
            item
            for item in result
            if item["kind"] == "Deployment" and item["metadata"]["name"] == "scheduler"
        ],
    )
    peer = copy.deepcopy(scheduler)
    peer["metadata"]["name"] = "scheduler-peer"
    peer["spec"]["replicas"] = 0
    for deployment, role in ((scheduler, PREDECESSOR), (peer, SUCCESSOR)):
        spec = deployment["spec"]
        spec["selector"]["matchLabels"][ROLE_LABEL] = role
        spec["template"]["metadata"]["labels"][ROLE_LABEL] = role
        container = spec["template"]["spec"]["containers"][0]
        set_env(
            container,
            DUR053_K6_LEASE_TAKEOVER_ROLE=role,
            DUR053_K6_LEASE_TAKEOVER_WORKFLOW=WORKFLOW,
        )
        token = one("E2_RENDER_HOOK_TOKEN", by(container["env"], name="DUR053_K6_HOOK_TOKEN"))
        token["valueFrom"]["secretKeyRef"]["name"] = f"hook-{role}"
        if role == SUCCESSOR:
            set_env(container, RUNTIME_ROLES="scheduler")
    result.append(peer)
    ensure_no_e1_placeholders(result)
    return result


class LeaseTakeover(Matrix):
    namespace = NS
    inputs_schema = "dur053.k6-phase-e2-inputs.v1"
    hold_deadline: float | None = None
    primary_deadline: float | None = None
    committed_takeover: tuple[dict, dict] | None = None

    def command(self, stage: str, *args: str, **kwargs) -> bytes:
        if self.hold_deadline is not None:
            remaining = self.hold_deadline - time.monotonic()
            if remaining <= 0:
                raise NotExercised(f"{stage}_DEADLINE observed=expired expected=live")
            kwargs["timeout"] = min(kwargs.get("timeout", 45), remaining)
        return super().command(stage, *args, **kwargs)

    def pg(self, query: str, stage: str = "E2_DB_READ") -> str:
        return (
            self.command(
                stage,
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
        objects = deployment_objects(source, self.tags, self.receiver_address, port)
        self.evidence.write("topology.json", objects)
        renderer.reset_transport()
        for topic in (EVENTS, TASKS):
            sample = self.kafka(topic)
            self.evidence.write(f"kafka-initial-{topic}.json", sample)
            expect("E2_FRESH_BROKER_TOPIC", broker_records(sample["value"], 8), [])
        self.create("E2_NAMESPACE", obj("Namespace", NS, None))
        self.namespace_started = True
        self.sql("E2_DB_CREATE", f'CREATE DATABASE "{DB}";')
        self.database_started = True
        self.harness.databases.add(DB)
        dsn = f"postgresql://durable:{self.harness.password}@postgres:5432/{DB}?sslmode=disable"
        self.create("E2_DB_SECRET", obj("Secret", "campaign-db", NS, stringData={"url": dsn}))
        self.create(
            "E2_HOOK_CA",
            obj("ConfigMap", "k6-hook-ca", NS, immutable=True, data={"ca.crt": cert.read_text()}),
        )
        for actor, token in tokens.items():
            self.create(
                "E2_HOOK_SECRET",
                obj("Secret", f"hook-{actor}", NS, immutable=True, stringData={"token": token}),
            )
        infrastructure = [o for o in objects if o["kind"] not in {"Namespace", "Deployment", "Job"}]
        self.create(
            "E2_INFRASTRUCTURE", {"apiVersion": "v1", "kind": "List", "items": infrastructure}
        )
        for name, ip, service_port in (
            ("postgres", self.engine_address, 5432),
            ("kafka", self.broker, 19092),
        ):
            self.create(
                "E2_ENDPOINT",
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
                "E2_JOB",
                next(o for o in objects if o["kind"] == "Job" and o["metadata"]["name"] == name),
            )
            self.k(
                "E2_JOB_WAIT",
                "-n",
                NS,
                "wait",
                "--for=condition=complete",
                f"job/{name}",
                "--timeout=180s",
                timeout=190,
            )
        self.pg(
            f"""
            INSERT INTO engine.workflow_definitions
              (definition_id, version, definition_hash, graph, activity_versions, effect_classes)
            VALUES ('{DEFINITION}', 1, 'e2-pure-echo-v1',
              '{{"entry":"pure.echo","nodes":[{{"id":"pure.echo","kind":"activity","next":"done"}},
                {{"id":"done","kind":"success"}}]}}',
              '{{"pure.echo":"v1"}}', '{{"pure.echo":"PURE_ACTIVITY"}}');
        """,
            "E2_PURE_DEFINITION",
        )
        self.pg(INSTALL_WRITE_WITNESS.format(partition=PARTITION), "E2_INSTALL_WRITE_WITNESS")
        self.create(
            "E2_DEPLOYMENTS",
            {
                "apiVersion": "v1",
                "kind": "List",
                "items": [o for o in objects if o["kind"] == "Deployment"],
            },
        )
        for role in ROLES:
            self.k(
                "E2_ROLLOUT",
                "-n",
                NS,
                "rollout",
                "status",
                f"deployment/{role}",
                "--timeout=180s",
                timeout=190,
            )

    def pods(self, role: str) -> list[dict]:
        selector = {
            PREDECESSOR: f"{ROLE_LABEL}={PREDECESSOR}",
            SUCCESSOR: f"{ROLE_LABEL}={SUCCESSOR}",
        }.get(role, f"app={role}")
        return json.loads(
            self.k("E2_PODS", "-n", NS, "get", "pods", "-l", selector, "-o", "json", timeout=4)
        )["items"]

    def role_log(self, pod: str, role: str, *, previous: bool = False) -> bytes:
        raw = self.k(
            "E2_ROLE_LOG",
            "-n",
            NS,
            "logs",
            pod,
            "-c",
            role,
            *(("--previous",) if previous else ()),
            timeout=4,
        )
        self.evidence.check(raw)
        return raw

    def observe_worker(self, script: str, code: str, *args: str, data: bytes | None = None):
        stage = f"E2_OBSERVER_{script}"
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

    def offer(self, cell: str) -> tuple[str, str]:
        payload, body_hash, traceparent = workload(cell)
        payload.update(namespace=NS, workflow_id=WORKFLOW, submission_key=WORKFLOW)
        started = time.time_ns()
        reply = self.observe_worker(
            "SUBMIT",
            SUBMIT_TRACED,
            traceparent,
            "/identity/token",
            "api:8080",
            data=json_data(payload),
        )
        self.evidence.write(
            "submission.json",
            {
                "request": payload,
                "response": reply,
                "offer_ns": started,
                "accepted_ns": time.time_ns(),
                "body_hash": body_hash,
                "traceparent": traceparent,
            },
        )
        expect(
            "E2_SUBMISSION_ACCEPTED",
            reply,
            {"status": 201, "workflow_id": WORKFLOW, "created": True},
        )
        return WORKFLOW, body_hash

    def cut(self, stage: str) -> dict:
        start = time.time_ns()
        value = json.loads(
            self.pg(
                f"""
            BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
            SELECT jsonb_build_object(
                'db_at', clock_timestamp(),
                'lease', (SELECT to_jsonb(l) || jsonb_build_object(
                    'live', l.lease_expires_at > clock_timestamp())
                    FROM engine.partition_leases l WHERE partition_id = {PARTITION}),
                'workflow', (SELECT jsonb_build_object('id', workflow_id, 'revision', revision)
                    FROM engine.workflow_executions WHERE workflow_id = '{WORKFLOW}'),
                'nodes', ARRAY(SELECT jsonb_build_object('id', node_id, 'state', state,
                    'iteration', iteration, 'attempt', current_attempt_number)
                    FROM engine.node_instances WHERE workflow_id = '{WORKFLOW}'
                    ORDER BY node_id, iteration),
                'attempts', ARRAY(SELECT attempt_number FROM engine.activity_attempts
                    WHERE workflow_id = '{WORKFLOW}' ORDER BY attempt_number),
                'acquisitions', ARRAY(SELECT to_jsonb(a) FROM engine.k4_audit a
                    WHERE kind = 'acquire' AND partition_id = {PARTITION} ORDER BY event_order),
                'backends', ARRAY(SELECT jsonb_build_object(
                    'pid', pid, 'application', application_name,
                    'start_us', (extract(epoch FROM backend_start) * 1000000)::bigint,
                    'state', state, 'in_transaction', xact_start IS NOT NULL)
                    FROM pg_stat_activity WHERE datname = current_database()
                    AND application_name LIKE 'dur053-e2-%' ORDER BY pid),
                'writes', ARRAY(SELECT to_jsonb(w) FROM k6_e2.writes w ORDER BY commit_order)
            );
            COMMIT;
        """,
                f"E2_DB_{stage.upper().replace('-', '_')}",
            )
        )
        value.update(sample_started_ns=start, sample_observed_ns=time.time_ns())
        self.evidence.write(f"{stage}.json", value)
        return value

    def wait_hold(self, predicate, stage: str):
        assert self.hold_deadline is not None
        last = None
        while time.monotonic() < self.hold_deadline:
            try:
                last = predicate()
            except SmokeFailure as error:
                if not str(error).endswith("_TIMEOUT"):
                    raise
                last = str(error)
            if last and not isinstance(last, str) and time.monotonic() < self.hold_deadline:
                return last
            time.sleep(0.075)
        raise NotExercised(f"E2_{stage} observed={last!r} expected=before_shared_deadline")

    def journal_bytes(self) -> bytes:
        with self.receiver.lock:
            return self.hook_log.read_bytes()

    def held(self, *, companion: bool = False) -> dict | None:
        arm = self.receiver.observe(CELL, companion=companion)
        if arm["state"] == "expired":
            raise NotExercised(f"E2_HOLD_EXPIRED observed={arm['boundary']} expected=held")
        return arm if arm["state"] == "held" else None

    def pending_call(self, target: dict, invocation: str) -> dict | None:
        raw = self.role_log(target["pod"], target["container"])
        rows = []
        prefix = "DUR053_K6_OBSERVATION k6_lease_takeover_call "
        for line in raw.decode().splitlines():
            if line.startswith(prefix):
                rows.append(json.loads(line.removeprefix(prefix)))
        matches = by(rows, event="k6_lease_takeover_call", invocation=invocation)
        return one("E2_DEPLOYED_COMPLETION", matches) if matches else None

    def resume(self, arm: dict, reason: str) -> None:
        self.receiver.authorize(
            CELL, arm["ack"], "release", evidence={"reason": reason}, companion=arm["companion"]
        )

    def release_pending(self) -> None:
        # A target between reach and held can still acknowledge. Let it either
        # get an authenticated release or expire on the original arm deadline.
        checked = []
        while True:
            checked.clear()
            for slot in (CELL, f"{CELL}/companion"):
                with self.receiver.lock:
                    exists = slot in self.receiver.arms
                if not exists:
                    continue
                arm = self.receiver.observe(CELL, companion=slot != CELL)
                if arm["state"] == "held":
                    self.resume(arm, "episode_exit")
                checked.append(arm)
            if all(arm["state"] in {"done", "expired"} for arm in checked) or (
                self.primary_deadline is None or time.monotonic() >= self.primary_deadline
            ):
                self.evidence.write(
                    "hold-exit.json",
                    {
                        "observed_ns": time.time_ns(),
                        "arms": [
                            {k: arm[k] for k in ("arm", "boundary", "until_ms", "state")}
                            for arm in checked
                        ],
                    },
                )
                return
            time.sleep(0.075)

    def challenge(self, predecessor: dict) -> dict:
        start = time.monotonic()
        self.hold_deadline = self.primary_deadline = start + 60
        arm = self.receiver.arm(
            CELL,
            PRIMARY,
            {key: predecessor[key] for key in ("actor", "uid", "process_start_id", "session")},
            {"key": WORKFLOW},
            60,
        )
        workflow, body_hash = self.offer(CELL)
        primary = self.wait_hold(self.held, "PREDECESSOR_ACK")
        primary_reach = acknowledged(self.journal_bytes(), primary)
        _, held_process = self.process(PREDECESSOR)
        expect("E2_PREDECESSOR_REACH_PROCESS", held_process, predecessor)
        before = self.cut("pre-prolongation")
        if not before["lease"]["live"]:
            raise NotExercised("E2_PRE_CHECK observed=expired expected=live_predecessor")
        owner = lease_at_hold(primary, primary_reach, predecessor, before, PARTITION)
        predecessor_write = lease_commit(before, owner)
        owner["challenge_pid"] = int(primary_reach["fields"]["objects"]["backend_pid"])
        expect("E2_NO_ATTEMPT_AT_BOUNDARY", before["attempts"], [])
        expect(
            "E2_SELECTED_PURE_NODE",
            by(before["nodes"], id="pure.echo"),
            [
                {"id": "pure.echo", "iteration": 0, "state": "WAITING_ACTIVITY", "attempt": None},
            ],
        )
        # Start the pre-rendered peer before W. No scheduler stop, reschedule
        # or additional deployment action occurs after prolongation.
        self.k(
            "E2_START_PEER",
            "-n",
            NS,
            "scale",
            "deployment/scheduler-peer",
            "--replicas=1",
            timeout=5,
        )
        self.receiver.note(CELL, primary["ack"], "prolonged", cut="pre-prolongation.json")
        prolonged = one(
            "E2_PROLONGATION", by(self.receiver.rows, arm=arm["arm"], event="prolonged")
        )
        lease = before["lease"]
        remaining = datetime.fromisoformat(lease["lease_expires_at"]) - datetime.fromisoformat(
            before["db_at"]
        )
        if prolonged["at_ns"] >= before["sample_started_ns"] + int(remaining.total_seconds() * 1e9):
            raise NotExercised(
                "E2_PROLONGATION observed=lease_expiry_uncertain expected=pre_expiry"
            )

        def registered():
            with self.receiver.lock:
                return self.receiver.targets.get(SUCCESSOR)

        peer_target = self.wait_hold(registered, "SUCCESSOR_REGISTRATION")
        if now_ms() >= primary["until_ms"]:
            raise NotExercised("E2_COMPANION_ARM observed=no_time expected=before_shared_deadline")
        companion = self.receiver.arm(
            CELL,
            COMPANION,
            peer_target,
            {"key": WORKFLOW, "epoch": str(lease["epoch"] + 1)},
            60,
            companion=True,
            share_primary_deadline=True,
        )
        expect("E2_COMPANION_DEADLINE", companion["until_ms"], primary["until_ms"])
        successor = self.wait_hold(lambda: self.held(companion=True), "SUCCESSOR_ACK")
        successor_reach = acknowledged(self.journal_bytes(), successor)
        _, peer = self.process(SUCCESSOR)
        expect("E2_TWO_SCHEDULERS", len(self.pods("scheduler")), 2)
        after = self.cut("takeover")
        if not after["lease"]["live"]:
            raise NotExercised("E2_TAKEOVER observed=expired expected=live_successor")
        peer_owner = lease_at_hold(successor, successor_reach, peer, after, PARTITION)
        write = takeover(owner, before, peer_owner, after, successor_reach)
        expect(
            "E2_LEASE_WRITES_COMMIT_ORDER",
            predecessor_write["commit_order"] < write["commit_order"],
            True,
        )
        self.committed_takeover = (write, owner)
        reject_stale_writes(after, write, owner, successor_held=True)
        self.receiver.note(
            CELL,
            successor["ack"],
            "takeover_verified",
            companion=True,
            cut="takeover.json",
            acquire_order=peer_owner["event_order"],
        )
        self.resume(primary, "successor_acquired_and_acknowledged")
        call = self.wait_hold(
            lambda: self.pending_call(predecessor, primary_reach["fields"]["invocation"]),
            "DEPLOYED_REJECTION",
        )
        self.evidence.write("predecessor-call.json", call)
        self.wait_hold(
            lambda: self.receiver.observe(CELL)["state"] == "done",
            "PREDECESSOR_RESUME",
        )
        _, continued = self.process(PREDECESSOR)
        rejected = self.cut("post-rejection")
        confirm_rejection(call, primary_reach, predecessor, continued, rejected)
        reject_stale_writes(rejected, write, owner, successor_held=True)
        expect(
            "E2_NO_SUCCESSOR_WRITES_WHILE_HELD",
            [row for row in rejected["writes"] if row["session_id"] == peer["session_uuid"]],
            [write],
        )
        expect("E2_PEER_STILL_UNEXPIRED", rejected["lease"], after["lease"])
        expect("E2_NO_WORK_BEFORE_PEER_RELEASE", rejected["attempts"], [])
        expect("E2_REVISION_STABLE_WHILE_HELD", rejected["workflow"], before["workflow"])
        expect(
            "E2_SUCCESSOR_HELD_THROUGH_REJECTION",
            self.receiver.observe(CELL, companion=True)["state"],
            "held",
        )
        self.receiver.note(
            CELL,
            successor["ack"],
            "rejection_observed",
            companion=True,
            call="predecessor-call.json",
            cut="post-rejection.json",
        )
        self.resume(successor, "observed_fenced_rejection_and_no_old_writes")
        self.wait_hold(
            lambda: self.receiver.observe(CELL, companion=True)["state"] == "done",
            "SUCCESSOR_RESUME",
        )
        sequence = timeline(self.journal_bytes(), primary, successor)
        self.evidence.write(
            "joined-handoff.json",
            {
                "predecessor": predecessor,
                "successor": peer,
                "predecessor_acquire": owner,
                "predecessor_write": predecessor_write,
                "successor_acquire": peer_owner,
                "successor_write": write,
                "sequences": sequence,
                "primary_deadline_ms": primary["until_ms"],
                "successor_deadline_ms": successor["until_ms"],
                "prolongation_ns": prolonged["at_ns"],
            },
        )
        self.hold_deadline = None
        final = self.terminal(workflow)
        final_cut = self.cut("reconciled")
        reject_stale_writes(final_cut, write, owner, successor_held=False)
        witness = self.invocations(workflow)
        self.evidence.write("worker-invocations.json", witness)
        inputs = recovery(
            final,
            peer,
            peer_owner,
            after["workflow"]["revision"],
            workflow,
            body_hash,
            witness["rows"],
        )
        inputs["predicates"]["I3"].update(
            acquire_orders=[owner["event_order"], peer_owner["event_order"]],
            write_commit_order=write["commit_order"],
            stale_invocation=call["invocation"],
            backend_pid=owner["challenge_pid"],
            journal_sequences=sequence,
        )
        self.evidence.write("checker-inputs.json", inputs)
        return inputs

    def diagnostics(self) -> None:
        if self.hook_log.exists():
            self.evidence.write_bytes("host-hook-journal.jsonl", self.journal_bytes())
        if not self.namespace_started:
            return
        failures = {}
        for role in ROLES:
            if role == "scheduler-peer":
                continue  # Both scheduler pods have app=scheduler.
            for pod in self.pods(role):
                name = pod["metadata"]["name"]
                try:
                    raw = self.role_log(name, role)
                    self.evidence.write_bytes(f"{name}.log", raw)
                    if any(c["restartCount"] for c in pod["status"].get("containerStatuses", [])):
                        prior = self.role_log(name, role, previous=True)
                        self.evidence.write_bytes(f"{name}-previous.log", prior)
                except SmokeFailure as error:
                    failures[name] = self.evidence.safe_reason(error)
        expect("E2_ROLE_LOG_FAILURES", failures, {})

    def teardown(self, stack: ExitStack) -> None:
        with ExitStack() as cleanup:
            if self.database_started:
                cleanup.callback(self.harness.drop, DB)
            if self.namespace_started:
                for kind in ("clusterrole", "clusterrolebinding"):
                    cleanup.callback(
                        self.k,
                        "E2_REVIEWER_CLEANUP",
                        "delete",
                        kind,
                        TOKEN_REVIEW,
                        "--ignore-not-found",
                    )
                cleanup.callback(
                    self.k,
                    "E2_NAMESPACE_CLEANUP",
                    "delete",
                    "namespace",
                    NS,
                    "--ignore-not-found",
                    "--timeout=90s",
                    timeout=95,
                )
            cleanup.callback(stack.close)


def inherited_failure(
    output: Path, code: str = "SHARED_KIND_HARNESS_FAILED_BEFORE_PHASE_E2"
) -> None:
    if not output.exists():
        evidence = E2Evidence(make_output(output))
        evidence.report.update(reason=code)
        evidence.checkpoint()


def record_handoff_failure(output: Path, error: BaseException) -> None:
    inherited_failure(output, "E2_HANDOFF_" + type(error).__name__.upper())


def run(output: Path, harness, addresses: dict[str, str]) -> int:
    evidence = E2Evidence(make_output(output))
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
                        prefix="dur053-k6-e2-",
                        dir=os.environ["RUNNER_TEMP"],
                    )
                )
            )
        with evidence.step("a-build-tagged-images"):
            smoke = LeaseTakeover(evidence, scratch, harness, addresses)
            smoke.build()
        with evidence.step("b-private-engine-topology"):
            smoke.receiver_address = e1_receiver_address(smoke.command, smoke.engine_address)
            tokens = {actor: secrets.token_hex(32) for actor in ACTORS}
            for token in tokens.values():
                evidence.secret(token)
            cert, key = smoke.tls()
            smoke.receiver = Receiver(smoke.hook_log, tokens, companion_boundary=COMPANION)
            stack.callback(smoke.receiver.file.close)
            port = stack.enter_context(serve(smoke.receiver, smoke.receiver_address, cert, key))
            smoke.setup(port, cert, tokens)
            _, predecessor = smoke.process(PREDECESSOR)
            expect("E2_BEFORE_OFFER_PEER", smoke.pods(SUCCESSOR), [])
        with evidence.step(CELL) as row:
            try:
                row.update(smoke.challenge(predecessor))
            except BaseException as primary:
                try:
                    smoke.release_pending()
                except Exception as error:
                    evidence.write("hold-exit-error.json", {"reason": evidence.safe_reason(error)})
                smoke.hold_deadline = None
                if isinstance(primary, NotExercised) and smoke.committed_takeover:
                    try:
                        late = smoke.cut("abort-reconciliation")
                    except Exception as error:
                        evidence.write(
                            "abort-cut-error.json", {"reason": evidence.safe_reason(error)}
                        )
                    else:
                        write, owner = smoke.committed_takeover
                        reject_stale_writes(late, write, owner, successor_held=False)
                raise
            else:
                smoke.release_pending()
    except (Exception, KeyboardInterrupt) as error:
        failure = evidence.safe_reason(error)
        if isinstance(error, NotExercised):
            evidence.report["outcomes"][-1]["status"] = "NOT_EXERCISED"
            evidence.checkpoint()
    finally:
        if not any(row["step"] == CELL for row in evidence.report["outcomes"]):
            evidence.report["outcomes"].append(
                {"step": CELL, "status": "SKIPPED", "reason": "E2_SETUP_INCOMPLETE"}
            )
        operations = (
            [
                ("d-retain-role-logs", smoke.diagnostics),
                ("d-cleanup", lambda: smoke.teardown(stack)),
            ]
            if smoke is not None
            else [("d-cleanup", stack.close)]
        )
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
        f"DUR-053 K6 Phase E2 {evidence.report['status']} {evidence.report['reason']}", flush=True
    )
    return int(failure is not None)
