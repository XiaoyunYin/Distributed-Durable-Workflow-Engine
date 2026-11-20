"""Hosted engineering lock-held/no-lock scheduler-to-PostgreSQL partitions."""

from __future__ import annotations

import copy
import json
import os
import re
import secrets
import tempfile
import time
from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

from dur053_k3 import CLUSTER, Driver
from dur053_k6_b_evidence import SmokeFailure
from dur053_k6_b_smoke import require
from dur053_k6_c_render import ip_peer, policy, traffic
from dur053_k6_c_smoke import make_output
from dur053_k6_chaos import Chaos, Target, backend_selected, bind, select_target
from dur053_k6_d2b_smoke import hook_pod
from dur053_k6_d_render import set_env
from dur053_k6_e1_join import EVENTS, TASKS, base_inputs, by, expect, one
from dur053_k6_e1_smoke import ROLES, e1_receiver_url, workload
from dur053_k6_e2_join import reject_stale_writes
from dur053_k6_e2_smoke import INSTALL_WRITE_WITNESS, pod_state
from dur053_k6_f1a_inputs import chain_counters, require_drop_rise
from dur053_k6_f1a_smoke import (
    F1aEvidence,
    F1aSmoke,
    JsonObject,
    audit_control_mounts,
    audit_topology,
    ready,
)
from dur053_k6_f1a_smoke import (
    deployment_objects as f1a_objects,
)
from dur053_k6_f1b_inputs import (
    BOUNDARIES,
    CELLS,
    PEER_APP,
    PEER_ROLE,
    TARGET_ROLE,
    clear_candidate,
    durable_hold,
    fenced_calls,
    partition_options,
    peer_progress,
    peer_takeover,
    process_rows,
    require_hold,
    require_lock,
    restored,
    verify_kernel_rule,
    verify_rule,
)
from dur053_k6_hook import Receiver, serve
from dur053_k6_identity import obj
from dur053_k6_registry import read_registry

ROLE_LABEL = "dur053-f1b-role"
PREFIX = b"DUR053_K6_OBSERVATION f1b "
MONITOR_SQL = r"""
BEGIN READ WRITE;
\set probe_epoch -1
\set ON_ERROR_STOP off
SELECT epoch AS probe_epoch FROM engine.partition_leases
WHERE partition_id = :partition FOR UPDATE NOWAIT \gset
\set probe_state :SQLSTATE
\set ON_ERROR_STOP on
ROLLBACK;
\set rollback_state :SQLSTATE
BEGIN READ ONLY;
SELECT jsonb_build_object(
    'db_at', clock_timestamp(), 'read_only', current_setting('transaction_read_only'),
    'sqlstate', :'probe_state', 'rollback', :'rollback_state', 'probe_epoch', :probe_epoch,
    'backend', (SELECT jsonb_build_object('pid', pid, 'application', application_name,
        'start_us', (extract(epoch FROM backend_start) * 1000000)::bigint,
        'state', state, 'in_transaction', xact_start IS NOT NULL, 'xid', backend_xid::text)
        FROM pg_stat_activity WHERE datname = current_database() AND pid = :backend),
    'peer_waiters', ARRAY(SELECT pid FROM pg_stat_activity WHERE datname = current_database()
        AND application_name = :'peer_app' AND :backend = ANY(pg_blocking_pids(pid))
        AND query LIKE '%engine.partition_leases%' ORDER BY pid));
COMMIT;
"""


class F1bEvidence(F1aEvidence):
    schema = "dur053.k6-phase-f1b-smoke.v1"


def deployment_objects(
    source: list[JsonObject],
    namespace: str,
    tags: JsonObject,
    ordinary: JsonObject,
    cell: str,
    receiver_address: str,
    port: int,
) -> list[JsonObject]:
    items = f1a_objects(source, namespace, tags, ordinary)
    original = one(
        "F1B_SCHEDULER_DEPLOYMENT",
        [o for o in items if o["kind"] == "Deployment" and o["metadata"]["name"] == "scheduler"],
    )
    peer = copy.deepcopy(original)
    peer["metadata"]["name"] = "scheduler-peer"
    for deployment, role, replicas in ((original, "target", 1), (peer, "peer", 0)):
        spec = deployment["spec"]
        spec["replicas"] = replicas
        spec["selector"]["matchLabels"][ROLE_LABEL] = role
        spec["template"]["metadata"]["labels"][ROLE_LABEL] = role
        container = spec["template"]["spec"]["containers"][0]
        database = one("F1B_SCHEDULER_DB_ENV", by(container["env"], name="DATABASE_URL"))
        database["valueFrom"]["secretKeyRef"]["name"] = f"f1b-{role}-db"
        # The store derives application_name from RUNTIME_ROLE, replacing DSN parameters.
        set_env(container, RUNTIME_ROLE=TARGET_ROLE if role == "target" else PEER_ROLE)
    hook_pod(original, "scheduler", e1_receiver_url(receiver_address, port), {})
    set_env(
        original["spec"]["template"]["spec"]["containers"][0],
        DUR053_K6_PARTITION_CELL=cell,
        DUR053_K6_PARTITION_WORKFLOW=workload(cell)[0]["workflow_id"],
    )
    items.extend(
        (
            peer,
            policy(
                namespace,
                "f1b-hook-scheduler",
                "scheduler",
                "Egress",
                [traffic("to", port, ip_peer(receiver_address))],
            ),
        )
    )
    audit_topology(items, {*tags.values(), *ordinary.values()})
    audit_control_mounts(items, namespace)
    return items


class F1bSmoke(F1aSmoke):
    image_repository = "dur053-k6-f1b"
    scheduler_replicas = 1
    receiver: Receiver
    hook_log: Path
    port: int
    cert: Path
    token: str

    def cell_setup(self, cell: str) -> None:
        self.cell_id = cell
        self.namespace = "dur053-k6-f1b-" + cell
        self.database = self.namespace.replace("-", "_")
        self.monitor_role = self.database + "_monitor"
        self.monitor_created = False
        self.chaos_run = None
        self.monitor_samples: list[JsonObject] = []
        self.setup_episode()
        payload, _, _ = workload(cell)
        self.workflow = payload["workflow_id"]
        # The production partition map uses SHA-256 over the durable workflow ID.
        partition = int.from_bytes(sha256(self.workflow.encode()).digest()[:8], "big") % 16
        self.partition = partition
        self.pg(INSTALL_WRITE_WITNESS.format(partition=partition))
        self.sql(
            "F1B_MONITOR_CREATE",
            f"""
            CREATE ROLE {self.monitor_role} LOGIN CONNECTION LIMIT 1;
            ALTER ROLE {self.monitor_role} SET default_transaction_read_only = on;
            ALTER ROLE {self.monitor_role} SET statement_timeout = '2s';
            GRANT pg_read_all_stats TO {self.monitor_role};
        """,
        )
        self.monitor_created = True
        self.pg(f"""
            GRANT USAGE ON SCHEMA engine TO {self.monitor_role};
            GRANT SELECT (partition_id, epoch), UPDATE (epoch)
                ON engine.partition_leases TO {self.monitor_role};
        """)

    def render_episode(self, source: list[JsonObject]) -> list[JsonObject]:
        return deployment_objects(
            source,
            self.namespace,
            self.tags,
            self.harness.images,
            self.cell_id,
            self.receiver_address,
            self.port,
        )

    def prepare_episode_dependencies(self) -> None:
        self.create(
            "F1B_HOOK_CA",
            obj(
                "ConfigMap",
                "k6-hook-ca",
                self.namespace,
                immutable=True,
                data={"ca.crt": self.cert.read_text()},
            ),
        )
        self.create(
            "F1B_HOOK_SECRET",
            obj(
                "Secret",
                "hook-scheduler",
                self.namespace,
                immutable=True,
                stringData={"token": self.token},
            ),
        )
        for role, address in (("target", self.engine_address), ("peer", "postgres")):
            # The rule runs in the pod output namespace, before Service DNAT.
            dsn = (
                f"postgresql://durable:{self.harness.password}@{address}:5432/"
                f"{self.database}?sslmode=disable"
            )
            self.create(
                "F1B_SCHEDULER_DB_SECRET",
                obj("Secret", f"f1b-{role}-db", self.namespace, stringData={"url": dsn}),
            )

    def journal_bytes(self) -> bytes:
        with self.receiver.lock:
            return self.hook_log.read_bytes()

    def retain_journal(self) -> None:
        if self.hook_log.exists():
            self.evidence.write_bytes(
                f"{self.cell_id}-host-hook-journal.jsonl", self.journal_bytes()
            )

    def sample_until[T](
        self,
        check: Callable[[], T | None],
        stage: str,
        deadline: float,
        target: Target | None = None,
    ) -> T:
        def sample() -> tuple[T | None, JsonObject]:
            result = check()
            return result, {
                "candidates": [
                    {"pod_uid": target.uid if target else None, "reason": stage + "_PENDING"}
                ],
                "last_monitor": self.monitor_samples[-1:],
            }

        return self.poll(sample, stage, deadline)

    def scope(self) -> JsonObject:
        services = json.loads(
            self.k("F1B_SERVICE_ADDRESSES", "get", "services", "-A", "-o", "json")
        )["items"]
        addresses = sorted(
            {
                ip
                for service in services
                for ip in service["spec"].get("clusterIPs", [service["spec"].get("clusterIP")])
                if ip and ip != "None"
            }
        )
        exclusions = {
            "broker": [self.broker],
            "receiver_gateway": [self.receiver_address],
            "kubernetes_api": [self.addresses["192.0.2.3"], self.addresses["192.0.2.4"]],
            "service_ips": addresses,
        }
        self.retain("address-isolation", {"engine": self.engine_address, "excluded": exclusions})
        options = partition_options(self.engine_address, exclusions)
        endpoints = self.objects("F1B_ENGINE_ENDPOINT", "endpoints", "postgres")
        expect(
            "F1B_POSTGRES_INSPECTED_ROUTE",
            [a["ip"] for subset in endpoints["subsets"] for a in subset["addresses"]],
            [self.engine_address],
        )
        return options

    def original(self, seed: int) -> Target:
        with self.receiver.lock:
            registered = self.receiver.targets.get("scheduler")
        candidates = []
        rejected = []
        for pod in self.pods("scheduler"):
            uid = pod["metadata"]["uid"]
            if not ready(pod) or not registered or uid != registered["uid"]:
                rejected.append({"pod_uid": uid, "reason": "F1B_SCHEDULER_NOT_READY_OR_REGISTERED"})
                continue
            observed = self.identity("scheduler", pod, registered["session"])
            expect(
                "F1B_HOST_REGISTRATION_PROCESS",
                (observed.uid, observed.process),
                (registered["uid"], registered["process_start_id"]),
            )
            candidates.append(observed)
        self.retain(
            "selection",
            {"seed": seed, "eligible": [asdict(t) for t in candidates], "rejected": rejected},
        )
        target = select_target(candidates, seed)
        self.retain("selected", asdict(target))
        return target

    def reobserve(self, target: Target) -> Target:
        # Database readiness can fail while the bound runtime stays alive.
        return self.identity(
            target.role,
            self.objects("F1B_TARGET_POD", "pod", target.pod),
            target.session,
            require_ready=False,
        )

    def monitor(self, reach: JsonObject, stage: str) -> JsonObject:
        fields = reach["fields"]["objects"]
        started = time.time_ns()
        data = self.command(
            "F1B_MONITOR",
            "docker",
            "exec",
            "-i",
            CLUSTER + "-postgres",
            "psql",
            "-XqAt",
            "-v",
            "ON_ERROR_STOP=1",
            "-v",
            f"partition={self.partition}",
            "-v",
            f"backend={int(fields['backend_pid'])}",
            "-v",
            f"peer_app={PEER_APP}",
            "-U",
            self.monitor_role,
            "-d",
            self.database,
            data=MONITOR_SQL.encode(),
            timeout=4,
        )
        sample: JsonObject = json.loads(data)
        sample.update(stage=stage, started_ns=started, at_ns=time.time_ns())
        self.monitor_samples.append(sample)
        self.retain("monitor", self.monitor_samples)
        expect("F1B_MONITOR_READ_ONLY", sample["read_only"], "on")
        expect("F1B_NOWAIT_ROLLBACK", sample["rollback"], "00000")
        expect("F1B_MONITOR_PROBE_SQLSTATE", sample["sqlstate"] in {"00000", "55P03"}, True)
        return sample

    def fence_cut(self, stage: str) -> JsonObject:
        cut: JsonObject = self.snapshot(self.workflow, lease_writes=True)
        # The snapshot orders audit rows by event_order in the same read-only cut as writes.
        cut["acquisitions"] = by(cut["audit"], kind="acquire", partition_id=self.partition)
        self.retain(stage, cut)
        expect("F1B_CUT_READ_ONLY", cut["read_only"], "on")
        return cut

    def scheduler_rows(self, target: Target, reach: JsonObject) -> list[JsonObject]:
        raw = self.role_log(target.pod, target.container)
        self.evidence.write_bytes(f"{self.cell_id}-scheduler-observations.log", raw)
        rows = []
        for number, line in enumerate(raw.splitlines(), 1):
            _, _, body = line.partition(b" ")  # kubectl's source timestamp
            if not body.startswith(PREFIX):
                continue
            try:
                row = json.loads(body.removeprefix(PREFIX).decode())
            except (UnicodeDecodeError, json.JSONDecodeError):
                raise SmokeFailure(f"F1B_SCHEDULER_OBSERVATION_INVALID line={number}") from None
            if not isinstance(row, dict):
                raise SmokeFailure(f"F1B_SCHEDULER_OBSERVATION_INVALID line={number}")
            rows.append(row)
        self.retain("scheduler-observations", rows)
        return process_rows(rows, target, reach)

    def confirm_partition(
        self,
        target: Target,
        reach: JsonObject,
        arm: JsonObject,
        active_ns: int,
        chain: str,
        baseline: JsonObject,
        deadline: float,
    ) -> JsonObject:
        cell = self.cell_id
        locked = cell == "medium-2"
        hold_ack_ns = reach["hold_ack_ns"]
        idle_timeout_ms = int(reach["fields"]["objects"]["idle_timeout_ms"]) if locked else None
        conditions = {
            "drop_packet_rise": False,
            "drop_byte_rise": False,
            "failed_pool_probe": False,
            "authenticated_hook_poll": False,
        }
        if locked:
            conditions["peer_waiter"] = False
        evidence = {
            "hold_ack_ns": hold_ack_ns,
            "active_ns": active_ns,
            "idle_timeout_ms": idle_timeout_ms,
            "failed_pool_probes": [],
            "authenticated_hook_polls": [],
            "drop_baseline": baseline,
            "drop_current": None,
        }

        def missing_conditions() -> list[str]:
            return [condition for condition, observed in conditions.items() if not observed]

        def check_lock(sample: JsonObject) -> None:
            if locked and sample["backend"] is None:
                missing = [*missing_conditions(), "live_held_backend"]
                self.retain("during-injection", {**evidence, "monitor": sample, "missing": missing})
                raise SmokeFailure(
                    "F1B_IDLE_BOUND_BEFORE_CONFIRMATION "
                    f"hold_ack_ns={hold_ack_ns} active_ns={active_ns} "
                    f"idle_timeout_ms={idle_timeout_ms} observed_ns={sample['at_ns']} "
                    f"missing={','.join(missing)}"
                )
            require_lock(cell, reach, sample)

        def during() -> JsonObject | None:
            monitor = self.monitor(reach, "while-injected")
            if locked:
                conditions["peer_waiter"] = bool(monitor["peer_waiters"])
            # Before reading new signals, a disappeared backend is reported
            # with the most recent observations already in evidence.
            check_lock(monitor)
            require(self.held() is not None, "F1B_HOLD_LOST_DURING_INJECTION")
            drops = self.counters(target, chain)
            rows = self.scheduler_rows(target, reach)
            failed = [
                r
                for r in by(rows, event="pool_probe", outcome="unreachable")
                if r["started_ns"] > active_ns
            ]
            journal = [json.loads(line) for line in self.journal_bytes().splitlines()]
            polls = [r for r in by(journal, arm=arm["arm"], event="wait") if r["at_ns"] > active_ns]
            conditions.update(
                drop_packet_rise=drops is not None and drops["packets"] > baseline["packets"],
                drop_byte_rise=drops is not None and drops["bytes"] > baseline["bytes"],
                failed_pool_probe=bool(failed),
                authenticated_hook_poll=bool(polls),
            )
            missing = missing_conditions()
            evidence.update(
                monitor=monitor,
                failed_pool_probes=failed,
                authenticated_hook_polls=polls,
                drop_current=drops,
                missing=missing,
            )
            self.retain("during-injection", evidence)
            if missing or drops is None:
                return None
            require_drop_rise(baseline, drops)
            bind(target, self.reobserve(target))
            # A second live backend check brackets the network observations.
            confirmed = self.monitor(reach, "after-network-confirmation")
            check_lock(confirmed)
            timing = {
                "confirmed_ns": confirmed["at_ns"],
                "hold_ack_to_confirmation_ns": confirmed["at_ns"] - hold_ack_ns,
            }
            self.retain(
                "during-injection", {**evidence, **timing, "confirmation_monitor": confirmed}
            )
            return {"failed": failed, "polls": polls, "drops": drops, "lock": monitor, **timing}

        return self.sample_until(during, "F1B_ACTIVE_PARTITION", deadline, target)

    def held(self) -> JsonObject | None:
        arm = self.receiver.observe(self.cell_id)
        require(arm["state"] != "expired", "F1B_HOLD_EXPIRED")
        return arm if arm["state"] == "held" else None

    def peer(self, cut: JsonObject, old_epoch: int) -> Target:
        acquire = min(
            (a for a in cut["audit"] if a["kind"] == "acquire" and a["epoch"] > old_epoch),
            key=lambda a: a["event_order"],
        )
        pods = self.objects("F1B_PEER_PODS", "pods", "-l", f"{ROLE_LABEL}=peer")["items"]
        self.retain("peer-candidates", [pod_state(p) for p in pods])
        pod = one(
            "F1B_PEER_COMMIT_POD", [p for p in pods if p["metadata"]["uid"] == acquire["pod_uid"]]
        )
        return self.identity("scheduler", pod, acquire["session_id"].replace("-", ""))

    def server_timeout(self, reach: JsonObject, confirmed_ns: int, clear_ns: int) -> str:
        pid = int(reach["fields"]["objects"]["backend_pid"])
        result = self.harness.command(
            [
                "docker",
                "logs",
                "--timestamps",
                "--since",
                str(confirmed_ns // 10**9),
                CLUSTER + "-postgres",
            ],
            record=False,
            timeout=5,
        )
        lines = (result.stderr + result.stdout).splitlines()
        pattern = (
            rf"\[{pid}\]\s+FATAL:\s+terminating connection"
            r" due to idle-in-transaction timeout"
        )
        matches = [line for line in lines if re.search(pattern, line)]
        expect("F1B_SERVER_IDLE_TERMINATION", len(matches), 1)
        line = matches[0]
        self.retain("server-timeout", {"line": line, "backend_pid": pid})
        observed = int(datetime.fromisoformat(line.split()[0]).timestamp() * 10**9)
        expect("F1B_TIMEOUT_DURING_INJECTION", confirmed_ns < observed < clear_ns, True)
        return line

    def exercise(self, row: JsonObject, coverage: JsonObject) -> JsonObject:
        cell = self.cell_id
        options = self.scope()
        target = self.original(row["seed"])
        chaos = self.chaos_run = Chaos(self, self.schema_dir, row, target, options=options)
        chaos.preflight(self.crds)
        start = time.monotonic()
        self.receiver.arm(
            cell,
            BOUNDARIES[cell],
            {
                "actor": "scheduler",
                "uid": target.uid,
                "process_start_id": target.process,
                "session": target.session,
            },
            {"key": self.workflow},
            60,
        )
        workflow, fingerprint = self.offer_episode()
        expect("F1B_OFFER_IDENTITY", workflow, self.workflow)
        arm = self.sample_until(self.held, "F1B_DURABLE_ACK", start + 12, target)
        reach = durable_hold(self.journal_bytes(), arm, time.time_ns())
        pre = self.fence_cut("before-injection")
        old = require_hold(cell, reach, target, pre)
        expect("F1B_PARTITION_BINDING", old["partition_id"], self.partition)
        require_lock(cell, reach, self.monitor(reach, "before-injection"))
        self.k(
            "F1B_START_PEER",
            "-n",
            self.namespace,
            "scale",
            "deployment/scheduler-peer",
            "--replicas=1",
            timeout=5,
        )
        self.receiver.note(cell, arm["ack"], "injection_authorized", target=asdict(target))
        chaos.apply(lambda: self.reobserve(target))
        assert chaos.issued_ns is not None
        durable_hold(self.journal_bytes(), arm, chaos.issued_ns)

        def activated() -> JsonObject | None:
            status = chaos.sample("await-injection")
            return status if status and backend_selected(status, target) else None

        self.sample_until(activated, "F1B_CHAOS_BACKEND_ACK", start + 13, target)
        active_ns = time.time_ns()
        pod_network = self.objects("F1B_POD_NETWORK_RULE", "podnetworkchaos", target.pod)
        self.retain("pod-network", pod_network)
        chain, ipset, net, ports = verify_rule(
            pod_network["spec"], f"{self.namespace}/{chaos.name}", self.engine_address
        )
        raw_rules = self.daemon_network(target, "iptables-save", "-c")
        raw_sets = self.daemon_network(target, "ipset", "save")
        self.retain(
            "kernel-network", {"iptables": raw_rules.splitlines(), "ipsets": raw_sets.splitlines()}
        )
        verify_kernel_rule(raw_rules, raw_sets, chain, ipset, net, ports, self.engine_address)
        baseline = chain_counters(raw_rules, chain)
        if baseline is None:
            raise SmokeFailure("F1B_DROP_BASELINE_MISSING")

        active = self.confirm_partition(target, reach, arm, active_ns, chain, baseline, start + 14)
        clear = None
        termination = None
        if cell == "medium-2":

            def cleared() -> JsonObject | None:
                sample = self.monitor(reach, "await-lock-clear")
                return sample if clear_candidate(sample, old["epoch"]) else None

            clear = self.sample_until(cleared, "F1B_LOCK_CLEAR_BEFORE_TAKEOVER", start + 17, target)
            termination = self.server_timeout(reach, active["confirmed_ns"], clear["at_ns"])
            self.retain("lock-clear", clear)

        def advanced() -> JsonObject | None:
            require(self.held() is not None, "F1B_HOLD_LOST_BEFORE_TAKEOVER")
            cut = self.fence_cut("takeover-candidate")
            return (
                cut
                if any(a["kind"] == "acquire" and a["epoch"] > old["epoch"] for a in cut["audit"])
                and by(cut["history"], reason="ATTEMPT_CREATED")
                else None
            )

        takeover_cut = self.sample_until(
            advanced, "F1B_PEER_TAKEOVER_AND_PROGRESS", start + 39, target
        )
        peer = self.peer(takeover_cut, old["epoch"])
        acquisition, write = peer_takeover(cell, old, pre, takeover_cut, peer, clear)
        progress = peer_progress(takeover_cut, pre["workflow"]["revision"], peer, acquisition)
        expect(
            "F1B_NOT_TERMINAL_BEFORE_RELEASE",
            takeover_cut["workflow"]["state"] == "SUCCEEDED",
            False,
        )
        status = chaos.sample("after-peer-progress")
        expect(
            "F1B_CHAOS_ACTIVE_THROUGH_TAKEOVER",
            bool(status and backend_selected(status, target)),
            True,
        )
        drop_after_takeover = self.counters(target, chain)
        require(drop_after_takeover is not None, "F1B_DROP_RULE_MISSING_AT_TAKEOVER")
        self.retain(
            "takeover",
            {
                "peer": asdict(peer),
                "acquisition": acquisition,
                "write": write,
                "progress": progress,
                "drop_counters": drop_after_takeover,
            },
        )
        self.receiver.note(
            cell, arm["ack"], "takeover_verified", acquire_order=acquisition["event_order"]
        )

        absent_ns = chaos.remove()
        expect(
            "F1B_REMOVED_BEFORE_FAULT_BOUND",
            chaos.absent_monotonic is not None and chaos.absent_monotonic < chaos.deadline,
            True,
        )
        self.sample_until(
            lambda: True if self.counters(target, chain) is None else None,
            "F1B_DROP_RULE_REMOVED",
            start + 52,
            target,
        )

        def reconnected() -> JsonObject | None:
            require(self.held() is not None, "F1B_HOLD_LOST_BEFORE_RECONNECT")
            rows = self.scheduler_rows(target, reach)
            return next(
                (
                    r
                    for r in by(rows, event="pool_probe", outcome="connected")
                    if r["started_ns"] > absent_ns
                ),
                None,
            )

        reconnect = self.sample_until(
            reconnected, "F1B_ORIGINAL_POOL_RECONNECT", start + 52, target
        )
        restored(absent_ns, reconnect, target)
        bind(target, self.reobserve(target))
        self.retain("restoration", reconnect)
        self.receiver.authorize(
            cell,
            arm["ack"],
            "release",
            evidence={
                "absent_ns": absent_ns,
                "reconnect_ns": reconnect["at_ns"],
                "peer_acquire_order": acquisition["event_order"],
            },
        )
        self.sample_until(
            lambda: True if self.receiver.observe(cell)["state"] == "done" else None,
            "F1B_AUTHENTICATED_RELEASE",
            start + 59,
            target,
        )
        released = one(
            "F1B_RELEASE_JOURNAL",
            by(
                [json.loads(line) for line in self.journal_bytes().splitlines()],
                arm=arm["arm"],
                event="release",
            ),
        )

        def called() -> list[JsonObject] | None:
            rows = self.scheduler_rows(target, reach)
            return (
                rows
                if len(by(rows, event="fenced_call")) >= (2 if cell == "medium-2" else 1)
                else None
            )

        calls = fenced_calls(
            cell,
            self.sample_until(called, "F1B_DEPLOYED_STALE_CALL", start + 68, target),
            reach,
            released["at_ns"],
        )
        bind(target, self.reobserve(target))
        self.reconcile_rejection(
            {
                "workflow": workflow,
                "body_hash": fingerprint,
                "arm": row,
                "registered": coverage["invariants_covered"],
                "not_covered": coverage["invariants_not_covered"],
                "target": asdict(target),
                "hold": reach,
                "injection": active,
                "lock_clear": clear,
                "server_timeout": termination,
                "peer_acquisition": acquisition,
                "peer_progress": progress,
                "absence_ns": absent_ns,
                "restoration": reconnect,
                "release": released,
                "fenced_calls": calls,
            },
            old,
            write,
        )
        return {
            "workflow": workflow,
            "confirmation": "CONFIRMED",
            "registered": coverage["invariants_covered"],
        }

    def reconcile_rejection(self, inputs: JsonObject, old: JsonObject, write: JsonObject) -> None:
        workflow = self.workflow
        rejected = self.fence_cut("after-original-rejection")
        reject_stale_writes(rejected, write, old, successor_held=False)
        _, winner, witness = self.finish(workflow)
        expect(
            "F1B_PEER_ATTEMPT_TERMINAL_WINNER",
            winner["attempt"]["attempt_number"],
            inputs["peer_progress"]["attempt_number"],
        )
        expect(
            "F1B_RESULT_AFTER_TYPED_REJECTION",
            inputs["fenced_calls"][-1]["at_ns"] < winner["result"]["sent_ns"],
            True,
        )
        self.terminal(workflow)  # Reconcile all published outbox events and dispositions.
        reconciled = self.fence_cut("post-rejection-final-writes")
        reject_stale_writes(reconciled, write, old, successor_held=False)
        if self.cell_id == "medium-2":
            expect(
                "F1B_HELD_RENEWAL_NEVER_COMMITTED",
                by(reconciled["writes"], xid=inputs["hold"]["fields"]["objects"]["transaction"]),
                [],
            )
        for topic in (EVENTS, TASKS):
            self.retain("kafka-final-" + topic, self.kafka(topic))
        # Normalize the sealed F1a witnesses for the shared engineering predicates.
        invocations = [
            {
                "event": "start",
                "invocation_id": winner["invocation"]["id"],
                "token_hash": winner["invocation"]["identity"]["token_hash"],
            },
            {
                "event": "end",
                "invocation_id": winner["finish"]["id"],
                "outcome": winner["attempt"]["state"],
            },
        ]
        predicates = base_inputs(
            self.cell_id, workflow, inputs["body_hash"], reconciled, invocations
        )["predicates"]
        predicates["I3"] = {
            "original_acquire_order": old["event_order"],
            "peer_acquire_order": inputs["peer_acquisition"]["event_order"],
            "peer_write_commit_order": write["commit_order"],
            "original_stale_invocation": inputs["hold"]["fields"]["invocation"],
        }
        expect("F1B_COVERAGE_MATCH", sorted(predicates), sorted(inputs["registered"]))
        self.retain(
            "checker-inputs",
            {
                **inputs,
                "engine_cut": reconciled,
                "predicates": predicates,
                "winning_claim": winner["claim"],
                "witness": witness,
            },
        )

    def cleanup_cell(self) -> None:
        with ExitStack() as cleanup:
            if self.monitor_created:
                cleanup.callback(
                    self.sql, "F1B_MONITOR_DROP", f"DROP ROLE IF EXISTS {self.monitor_role};"
                )
            cleanup.callback(self.clear_episode)


def inherited_failure(
    output: Path, code: str = "SHARED_KIND_HARNESS_FAILED_BEFORE_PHASE_F1B"
) -> None:
    if not output.exists():
        evidence = F1bEvidence(make_output(output))
        evidence.report.update(reason=code)
        evidence.checkpoint()
        print(f"F1B FAIL {code}", flush=True)


def record_handoff_failure(output: Path, error: BaseException) -> None:
    inherited_failure(output, "F1B_HANDOFF_" + type(error).__name__.upper())


def run(output: Path, harness: Driver, addresses: dict[str, str]) -> int:
    started = time.monotonic()
    evidence = F1bEvidence(make_output(output))
    smoke = None
    failure = None
    with ExitStack() as stack:
        try:
            with evidence.step("f1b-private-scratch"):
                require(
                    os.name == "posix" and bool(os.getenv("RUNNER_TEMP")),
                    "HOSTED_LINUX_RUNNER_REQUIRED",
                )
                scratch = Path(
                    stack.enter_context(
                        tempfile.TemporaryDirectory(
                            prefix="dur053-k6-f1b-", dir=os.environ["RUNNER_TEMP"]
                        )
                    )
                )
                registry = read_registry()
            with evidence.step("f1b-tagged-build"):
                smoke = F1bSmoke(evidence, scratch, harness, addresses)
                smoke.build()
            with evidence.step("f1b-pinned-chaos-backend"):
                smoke.backend()
            with evidence.step("f1b-receiver-address"):
                smoke.receiver_address = (
                    smoke.command(
                        "F1B_KIND_GATEWAY",
                        "docker",
                        "inspect",
                        "--format",
                        '{{(index .NetworkSettings.Networks "kind").Gateway}}',
                        CLUSTER + "-control-plane",
                    )
                    .decode()
                    .strip()
                )
                partition_options(
                    smoke.engine_address,
                    {"broker": [smoke.broker], "receiver_gateway": [smoke.receiver_address]},
                )
                smoke.cert, key = smoke.tls()
            for cell in CELLS:
                cell_start = time.monotonic()
                smoke.cell_id = cell
                smoke.monitor_created = False
                clean = True
                with ExitStack() as receiver_stack:
                    try:
                        with evidence.step(cell + "-topology"):
                            episode = min(
                                by(registry["episodes"], cell=cell),
                                key=lambda item: item["ordinal"],
                            )
                            coverage = one(
                                "F1B_COVERAGE", by(registry["coverage"], arm=episode["arm"])
                            )
                            smoke.retain("registered-coverage", coverage)
                            smoke.hook_log = scratch / f"{cell}-journal.jsonl"
                            smoke.token = secrets.token_hex(32)
                            evidence.secret(smoke.token)
                            smoke.receiver = Receiver(
                                smoke.hook_log, {"scheduler": smoke.token}, record_waits=True
                            )
                            receiver_stack.callback(smoke.receiver.file.close)
                            smoke.port = receiver_stack.enter_context(
                                serve(smoke.receiver, smoke.receiver_address, smoke.cert, key)
                            )
                            smoke.cell_setup(cell)
                        with evidence.step(cell) as result:
                            result.update(smoke.exercise(episode, coverage))
                    except Exception as error:
                        failure = failure or evidence.safe_reason(error)
                    finally:
                        diagnostics: tuple[tuple[str, Callable[[], Any]], ...] = (
                            (
                                "diagnostics",
                                lambda: (
                                    smoke.collect_role_logs(smoke.namespace, ROLES)
                                    if smoke.namespace_started
                                    else None
                                ),
                            ),
                            ("journal", smoke.retain_journal),
                            ("cleanup", smoke.cleanup_cell),
                        )
                        for stage, operation in diagnostics:
                            try:
                                with evidence.step(f"{cell}-{stage}") as step:
                                    operation()
                                    if stage == "cleanup":
                                        step["elapsed_seconds"] = round(
                                            time.monotonic() - cell_start, 3
                                        )
                            except Exception as error:
                                failure = failure or evidence.safe_reason(error)
                                if stage == "cleanup":
                                    clean = False
                evidence.report.setdefault("cell_elapsed_seconds", {})[cell] = round(
                    time.monotonic() - cell_start, 3
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
                        {"step": cell, "status": "SKIPPED", "reason": "F1B_SETUP_INCOMPLETE"}
                    )
            if smoke is not None and smoke.chaos_started:
                smoke.deadline = datetime.now(UTC) + timedelta(seconds=120)
                for stage, operation in (
                    ("f1b-chaos-logs", lambda: smoke.collect_role_logs("chaos-mesh", ())),
                    ("f1b-chaos-cleanup", lambda: smoke.clear_namespace("chaos-mesh", 50)),
                ):
                    try:
                        with evidence.step(stage):
                            operation()
                    except Exception as error:
                        failure = failure or evidence.safe_reason(error)
            try:
                with evidence.step("f1b-credential-scan"):
                    evidence.scan()
            except Exception as error:
                failure = failure or evidence.safe_reason(error)
            evidence.report.update(
                status="FAIL" if failure else "PASS",
                reason=failure or "MATRIX_COMPLETE",
                elapsed_seconds=round(time.monotonic() - started, 3),
            )
            evidence.checkpoint()
    print(f"F1B {evidence.report['status']} {evidence.report['reason']}", flush=True)
    return int(failure is not None)
