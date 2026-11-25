"""Acquire the hook-5 sources on the Phase G deployment."""

from __future__ import annotations

import copy
import json
import secrets
import time
from contextlib import ExitStack
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from dur053_k6_b_smoke import private_file, require
from dur053_k6_c_render import policy, traffic
from dur053_k6_chaos import Target
from dur053_k6_d_render import mount, set_env
from dur053_k6_e1_smoke import e1_receiver_address, e1_receiver_san
from dur053_k6_g_bundle import Json
from dur053_k6_g_partition import PartitionDriver
from dur053_k6_g_registration import observe_runtime, utc
from dur053_k6_hook import Receiver, serve
from dur053_k6_identity import obj

if TYPE_CHECKING:
    from dur053_k6_g_smoke import Episode

PREDECESSOR, SUCCESSOR = "predecessor", "successor"
PRIMARY, COMPANION = "lease_takeover", "lease_takeover_acquired"
PEER = "scheduler-peer"
ROLE_LABEL = "dur053-lease-role"


def journal_utc(ns: int) -> str:
    seconds, nanos = divmod(ns, 10**9)
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(seconds)) + f".{nanos:09d}Z"


class StaleDriver:
    def __init__(self, episode: Episode, stack: ExitStack) -> None:
        self.g = episode
        self.stack = stack
        self.workflow = f"dur053-g-measured-{episode.cell['seed']}-000000"
        self.episode = episode.binding["episode"]
        self.path = episode.scratch / "host-hook-journal.jsonl"
        self.partition = (
            PartitionDriver(episode, stack, episode.scratch / "chaos")
            if episode.cell["cell"] == "medium-3"
            else None
        )

    def render(self, objects: list[Json], address: str) -> None:
        gateway = e1_receiver_address(self.g.command, address)
        cert, key = (self.g.scratch / f"hook.{suffix}" for suffix in ("crt", "key"))
        private_file(key, b"")
        self.g.command(
            "G_STALE_TLS",
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
            e1_receiver_san(gateway),
            "-keyout",
            str(key),
            "-out",
            str(cert),
        )
        self.g.evidence.secret(key.read_text())
        tokens = {role: secrets.token_hex(32) for role in (PREDECESSOR, SUCCESSOR)}
        self.receiver = Receiver(self.path, tokens, companion_boundary=COMPANION)
        self.stack.callback(self.receiver.file.close)
        port = self.stack.enter_context(serve(self.receiver, gateway, cert, key))
        ns = self.g.namespace
        self.g.create(
            "G_STALE_CA", obj("ConfigMap", "g-hook-ca", ns, data={"ca.crt": cert.read_text()})
        )
        objects.append(
            policy(
                ns,
                "scheduler-hook-egress",
                "scheduler",
                "Egress",
                [traffic("to", port, {"ipBlock": {"cidr": f"{gateway}/32"}})],
            )
        )
        [scheduler] = [
            item
            for item in objects
            if item["kind"] == "Deployment" and item["metadata"]["name"] == "scheduler"
        ]
        peer = copy.deepcopy(scheduler)
        peer["metadata"]["name"] = PEER
        for role, deployment, replicas in ((PREDECESSOR, scheduler, 1), (SUCCESSOR, peer, 0)):
            token = tokens[role]
            self.g.evidence.secret(token)
            name = "g-hook-" + role
            self.g.create("G_STALE_TOKEN", obj("Secret", name, ns, stringData={"token": token}))
            spec = deployment["spec"]
            spec["replicas"] = replicas
            spec["selector"]["matchLabels"][ROLE_LABEL] = role
            spec["template"]["metadata"]["labels"][ROLE_LABEL] = role
            pod = spec["template"]["spec"]
            mount(pod, "g-hook-ca", "/g-hook", {"configMap": {"name": "g-hook-ca"}})
            container = pod["containers"][0]
            set_env(
                container,
                DUR053_K6_LEASE_TAKEOVER_ROLE=role,
                DUR053_K6_LEASE_TAKEOVER_CELL=self.g.cell["cell"],
                DUR053_K6_LEASE_TAKEOVER_WORKFLOW=self.workflow,
                DUR053_K6_HOOK_URL=f"https://{gateway}:{port}",
                DUR053_K6_HOOK_CA_FILE="/g-hook/ca.crt",
            )
            container["env"].append(
                {
                    "name": "DUR053_K6_HOOK_TOKEN",
                    "valueFrom": {"secretKeyRef": {"name": name, "key": "token"}},
                }
            )
        objects.append(peer)

    def registered(self, role: str) -> Json | None:
        with self.receiver.lock:
            return self.receiver.targets.get(role)

    def process(self, role: str) -> tuple[Target, Json]:
        pods = json.loads(
            self.g.k(
                "G_STALE_PODS",
                "-n",
                self.g.namespace,
                "get",
                "pods",
                "-l",
                f"{ROLE_LABEL}={role}",
                "-o",
                "json",
            )
        )
        [pod] = pods["items"]
        observed = observe_runtime(pod, "scheduler")
        identity = observed["identity"]
        cid, node = identity["container_id"], pod["spec"]["nodeName"]
        cri = json.loads(
            self.g.command(
                "G_STALE_CRI", "docker", "exec", node, "crictl", "inspect", cid, timeout=4
            )
        )
        require(cri["status"]["id"] == cid and cri["info"]["pid"] > 0, "G_STALE_CRI_ID")
        stat = self.g.command(
            "G_STALE_PROCESS",
            "docker",
            "exec",
            node,
            "cat",
            f"/proc/{cri['info']['pid']}/stat",
            timeout=4,
        )
        ticks = stat.rsplit(b")", 1)[1].split()[19].decode()
        status = self.g.command(
            "G_STALE_PROCESS_STATUS",
            "docker",
            "exec",
            node,
            "cat",
            f"/proc/{cri['info']['pid']}/status",
            timeout=4,
        )
        [namespace_pids] = [line for line in status.splitlines() if line.startswith(b"NSpid:")]
        pid = int(namespace_pids.split()[-1])
        target = self.registered(role)
        require(target is not None, "G_STALE_REGISTRATION")
        assert target is not None
        deployment = Target(
            self.g.namespace,
            observed["pod_name"],
            identity["pod_uid"],
            "scheduler",
            "scheduler",
            observed["containerID"],
            identity["restart"],
            node,
            cri["info"]["pid"],
            f"{pid}:{ticks}",
            target["session"],
        )
        return deployment, {
            "pod_uid": identity["pod_uid"],
            "container_id": cid,
            "pid": pid,
            "process_start_id": f"{pid}:{ticks}",
            "session": str(UUID(target["session"])),
        }

    def cut(self, partition: int) -> Json:
        started = time.time_ns()
        raw = self.g.db(f"""
            SELECT jsonb_build_object(
                'db_time', clock_timestamp(), 'owner', l.owner_id, 'epoch', l.epoch,
                'updated', l.updated_at, 'expiry', l.lease_expires_at, 'revision', w.revision,
                'attempts', (SELECT count(*) FROM engine.activity_attempts
                             WHERE workflow_id=w.workflow_id),
                'backends', ARRAY(SELECT jsonb_build_object('pid', pid,
                    'start_us', (extract(epoch FROM backend_start) * 1000000)::bigint,
                    'application', application_name, 'state', state,
                    'in_transaction', xact_start IS NOT NULL)
                    FROM pg_stat_activity WHERE datname=current_database()
                    AND application_name LIKE 'dur053-e2-%'))
            FROM engine.partition_leases l JOIN engine.workflow_executions w USING (partition_id)
            WHERE l.partition_id={partition} AND w.workflow_id='{self.workflow}';
        """)
        cut: Json = json.loads(raw)
        for field in ("db_time", "updated", "expiry"):
            cut[field] = utc(datetime.fromisoformat(cut[field]))
        return {**cut, "start_ns": started, "end_ns": time.time_ns()}

    def arm(self) -> None:
        target = self.g.wait_for(lambda: self.registered(PREDECESSOR), "G_STALE_PREDECESSOR")
        self.receiver.arm(self.episode, PRIMARY, target, {"key": self.workflow}, 60)

    def state(self, companion: bool, state: str) -> Json | None:
        arm: Json = self.receiver.observe(self.episode, companion=companion)
        require(arm["state"] != "expired", "G_STALE_EXPIRED")
        return arm if arm["state"] == state else None

    def call(self, pod: str, invocation: str, container: str) -> Json | None:
        prefix = b"DUR053_K6_OBSERVATION k6_lease_takeover_call "
        logs = self.g.k(
            "G_STALE_CALL_LOG", "-n", self.g.namespace, "logs", pod, "-c", "scheduler", timeout=4
        )
        rows = [
            json.loads(line[len(prefix) :]) for line in logs.splitlines() if line.startswith(prefix)
        ]
        matches = [row for row in rows if row["invocation"] == invocation]
        if not matches:
            return None
        [row] = matches
        del row["event"]
        return {**row, "container_id": container}

    def finish(self) -> tuple[Json, str]:
        old = self.g.wait_for(
            lambda: self.state(False, "held"), "G_STALE_PREDECESSOR_ACK", seconds=12
        )
        target, before = self.process(PREDECESSOR)
        fields = old["reach"]["fields"]["objects"]
        partition = int(fields["partition"])
        hold = self.cut(partition)
        self.receiver.note(self.episode, old["ack"], "prolonged")
        if self.partition:
            self.partition.inject(
                target,
                lambda: self.process(PREDECESSOR)[0],
                partition,
                int(fields["backend_pid"]),
            )
        self.g.k(
            "G_STALE_START_PEER",
            "-n",
            self.g.namespace,
            "scale",
            "deployment/" + PEER,
            "--replicas=1",
            timeout=5,
        )
        peer_registration = self.g.wait_for(
            lambda: self.registered(SUCCESSOR), "G_STALE_PEER_REGISTRATION", seconds=20
        )
        self.receiver.arm(
            self.episode,
            COMPANION,
            peer_registration,
            {"key": self.workflow, "epoch": str(hold["epoch"] + 1)},
            60,
            companion=True,
            share_primary_deadline=True,
        )
        new = self.g.wait_for(lambda: self.state(True, "held"), "G_STALE_PEER_ACK", seconds=25)
        _, peer = self.process(SUCCESSOR)
        takeover = self.cut(partition)

        def release() -> None:
            self.receiver.authorize(self.episode, old["ack"], "release", evidence={})
            if self.partition:
                self.g.wait_for(
                    lambda: self.state(False, "done"),
                    "G_STALE_PRIMARY_RELEASE",
                    seconds=3,
                )

        if self.partition:
            self.partition.restore()
            self.partition.observe_call(release)
        else:
            release()
        invocation = old["reach"]["fields"]["invocation"]
        call = self.g.wait_for(
            lambda: self.call(target.pod, invocation, before["container_id"]),
            "G_STALE_CALL",
            seconds=8,
        )
        resumed, after = self.process(PREDECESSOR)
        require(resumed == target, "G_STALE_PROCESS_CONTINUITY")
        rejected = self.cut(partition)
        self.receiver.note(self.episode, new["ack"], "rejection_observed", companion=True)
        self.receiver.authorize(self.episode, new["ack"], "release", evidence={}, companion=True)
        self.g.wait_for(lambda: self.state(True, "done"), "G_STALE_PEER_RELEASE", seconds=8)
        journal = self.path.read_text()
        return {
            "schema": "dur053.k6-stale-challenge.v1",
            "binding": self.g.binding,
            "journal": journal,
            "before": before,
            "after": after,
            "peer": peer,
            "hold": hold,
            "takeover": takeover,
            "rejected": rejected,
            "call": call,
            **({"partition": self.partition.observation} if self.partition else {}),
        }, journal_utc(json.loads(journal.splitlines()[-1])["at_ns"])

    def complete(self, observation: Json) -> None:
        observation["progress_transaction"] = (
            self.g.db(f"""
            SELECT a.transaction_id FROM engine.k4_audit a
            JOIN engine.transition_history h USING (transition_id)
            WHERE h.workflow_id='{self.workflow}' AND h.reason='ATTEMPT_CREATED'
            ORDER BY a.event_order LIMIT 1;
        """)
            .decode()
            .strip()
        )
