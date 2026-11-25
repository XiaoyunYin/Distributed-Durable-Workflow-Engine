"""Observe the Phase G predecessor's NetworkChaos and fenced SQL execution."""

from __future__ import annotations

import json
import time
from contextlib import ExitStack
from datetime import datetime
from typing import TYPE_CHECKING

from dur053_k3 import CLUSTER
from dur053_k6_b_smoke import ENGINE_ROLE, require
from dur053_k6_chaos import Chaos, Target, backend_selected
from dur053_k6_f1a_inputs import chain_counters
from dur053_k6_f1a_smoke import F1aSmoke
from dur053_k6_f1b_inputs import verify_rule
from dur053_k6_f1b_smoke import MONITOR_SQL
from dur053_k6_f2_smoke import LineProcess
from dur053_k6_g_bundle import Json
from dur053_k6_g_registration import utc

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from dur053_k6_g_smoke import Episode


class PartitionDriver:
    def __init__(self, episode: Episode, stack: ExitStack, schema_dir: Path) -> None:
        self.g = episode
        self.stack = stack
        self.schema_dir = schema_dir
        self.k, self.command = episode.k, episode.command
        self.observation: Json = {}

    def retain(self, name: str, value: object) -> None:
        self.g.evidence.write(name + ".json", value)

    def postgres(self) -> list[str]:
        return [
            "docker",
            "exec",
            "-i",
            CLUSTER + "-postgres",
            "psql",
            "-XqAt",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            ENGINE_ROLE,
            "-d",
            self.g.database,
        ]

    def inject(
        self, target: Target, observe: Callable[[], Target], partition: int, backend: int
    ) -> None:
        import yaml

        self.target = target
        self.partition = partition
        self.backend = backend
        address = self.g.address
        self.chaos = Chaos(
            self, self.schema_dir, self.g.cell, target, options={"externalTargets": [address]}
        )
        self.chaos.preflight(
            list(yaml.safe_load_all((self.schema_dir / "chaos-crds.yaml").read_bytes()))
        )
        start = time.time_ns()
        lock = json.loads(
            self.command(
                "G_STALE_NOWAIT",
                *self.postgres(),
                "-v",
                f"partition={partition}",
                "-v",
                f"backend={backend}",
                "-v",
                "peer_app=unused",
                data=MONITOR_SQL.encode(),
                timeout=4,
            )
        )
        self.observation.update(
            pod=target.pod,
            pod_uid=target.uid,
            container_id=target.container_id.removeprefix("containerd://"),
            host_pid=target.host_pid,
            address=address,
            lock={
                "start_ns": start,
                "end_ns": time.time_ns(),
                "epoch": lock["probe_epoch"],
                "sqlstate": lock["sqlstate"],
                "rollback": lock["rollback"],
            },
        )
        self.retain("before-partition-lock", lock)
        self.retain(
            "joint-intervention",
            {
                "hold": "deployed CreateAttempt before transaction begin",
                "partition": "scheduler egress to PostgreSQL",
            },
        )
        self.stack.callback(self.cleanup)
        self.chaos.apply(observe)

        def active() -> Json | None:
            current = self.chaos.sample("active")
            return current if current is not None and backend_selected(current, target) else None

        controller = self.g.wait_for(active, "G_STALE_PARTITION_ACK", seconds=10)
        daemon = json.loads(
            self.k(
                "G_STALE_POD_NETWORK",
                "-n",
                target.namespace,
                "get",
                "podnetworkchaos",
                target.pod,
                "-o",
                "json",
                timeout=4,
            )
        )
        self.chain, *_ = verify_rule(
            daemon["spec"], f"{target.namespace}/{self.chaos.name}", address
        )
        before = self.network("iptables-save", "-c")
        sets = self.network("ipset", "save")
        failed = self.route()
        during = self.network("iptables-save", "-c")
        self.observation.update(
            issued_ns=self.chaos.issued_ns,
            confirmed_ns=time.time_ns(),
            controller=controller,
            daemon=daemon,
            rules_before=before,
            rules_during=during,
            sets=sets,
            failed=failed,
        )
        self.retain("partition-observed", self.observation)

    def network(self, *args: str) -> str:
        return F1aSmoke.daemon_network(self.g, self.target, *args)  # type: ignore[arg-type]

    def route(self) -> Json:
        target, address = self.target, self.g.address
        started = time.time_ns()
        raw = self.command(
            "G_STALE_ROUTE",
            "docker",
            "exec",
            target.node,
            "nsenter",
            "-t",
            str(target.host_pid),
            "-n",
            "bash",
            "-c",
            """timeout 1 bash -c 'exec 3<>"/dev/tcp/$1/$2"' bash "$1" "$2"; printf '%d\n' "$?" """,
            "bash",
            address,
            "5432",
            timeout=4,
        )
        return {
            "start_ns": started,
            "end_ns": time.time_ns(),
            "address": address,
            "port": 5432,
            "host_pid": target.host_pid,
            "exit": int(raw),
        }

    def restore(self) -> None:
        self.observation["rules_held"] = self.network("iptables-save", "-c")
        absent = self.chaos.remove()

        def cleared() -> str | None:
            rules = self.network("iptables-save", "-c")
            return rules if chain_counters(rules, self.chain) is None else None

        rules = self.g.wait_for(cleared, "G_STALE_KERNEL_RULE_PRESENT", seconds=5)
        gone = time.time_ns()
        self.observation.update(
            absent_ns=absent,
            kernel_gone_ns=gone,
            rules_after=rules,
            restored=self.route(),
        )
        self.retain("partition-restored", self.observation)

    def observe_call(self, release: Callable[[], None]) -> None:
        # A rollback-only observer holds the lease row after restoration so
        # pg_stat_activity can observe the actual CreateAttempt SELECT active.
        # The predecessor's own five-second call deadline remains in force.
        with ExitStack() as stack:
            probe = LineProcess(self.postgres(), self.g.env)  # type: ignore[arg-type]
            stack.callback(probe.close)
            probe.send(rf"""
                BEGIN;
                SET LOCAL idle_in_transaction_session_timeout = '5s';
                SELECT epoch FROM engine.partition_leases WHERE partition_id = {self.partition}
                FOR UPDATE NOWAIT \gset
                SELECT pg_backend_pid();
            """)
            observer = probe.read(3)
            require(isinstance(observer, int) and observer > 0, "G_STALE_OBSERVER_PID")
            release()

            def executing() -> Json | None:
                start = time.time_ns()
                raw = self.g.db(f"""
                    SELECT jsonb_build_object(
                        'pid', pid, 'start_us', (extract(epoch FROM backend_start)*1000000)::bigint,
                        'application', application_name, 'state', state,
                        'in_transaction', xact_start IS NOT NULL,
                        'db_time', clock_timestamp(), 'query_start', query_start, 'query', query,
                        'wait_type', wait_event_type, 'blocked_by', pg_blocking_pids(pid))
                    FROM pg_stat_activity WHERE datname=current_database() AND pid={self.backend}
                    AND state='active' AND {observer}=ANY(pg_blocking_pids(pid));
                """)
                if not raw.strip():
                    return None
                row: Json = json.loads(raw)
                for field in ("db_time", "query_start"):
                    row[field] = utc(datetime.fromisoformat(row[field]))
                return {
                    **row,
                    "start_ns": start,
                    "end_ns": time.time_ns(),
                    "observer_pid": observer,
                }

            observation = self.g.wait_for(executing, "G_STALE_BACKEND_NOT_EXECUTING", seconds=3)
            probe.send("ROLLBACK;\nSELECT to_json(:'SQLSTATE'::text);")
            # psql's SQLSTATE is a text value, so emit it as a JSON string.
            observation["rolled_back"] = probe.read(2)
            self.observation["execution"] = observation
            self.retain("stale-backend-execution", observation)

    def cleanup(self) -> None:
        if self.chaos.possibly_created:
            self.chaos.remove()
