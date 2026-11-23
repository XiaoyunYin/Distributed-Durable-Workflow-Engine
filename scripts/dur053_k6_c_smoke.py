"""Run the engineering-only kind matrix for the Phase C independent sink."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from dur053_k3 import LOCK, ROOT, Driver
from dur053_k6_b_evidence import Evidence, SmokeFailure
from dur053_k6_b_smoke import APPROVER, REVIEWER, require, sha, subject
from dur053_k6_c_deploy import (
    ENGINE_PEER,
    EXPORTER,
    NS,
    RELAY_PEER,
    SINK_ADMIN,
    SINK_PEER,
    SINK_ROLE,
    Deployment,
)
from dur053_k6_c_render import CLIENT, ENGINE, PORT, RELAY, SINK
from dur053_k6_join import digest, identity, load_records, receipt_bytes, verify

NETWORK_DENIAL = {"TCP_DIAL_TIMEOUT", "TCP_CONNECTION_REFUSED", "TCP_UNREACHABLE"}
DENIED = digest(b"K6_AUTH_DENIED\n")
PARALLEL = 6
ENGINE_CLIENTS = {
    ("durable", "dur053-k6-c-admin"): 5,
    ("durable", "dur053-k6-c-monitor"): 2,
    ("durable", "dur053-k6-c-fixture"): 2,
    ("durable", "dur053-migrate"): 2,
    ("k6_sink_grant_reader", "dur053-k6-sink-grant-reader"): 4,
    ("k6_decision_writer", "dur053-k6-c-decision"): 2,
}
SINK_CLIENTS = {
    (SINK_ADMIN, "dur053-k6-c-admin"): 3,
    (SINK_ADMIN, "dur053-k6-c-migrate"): 2,
    (SINK_ADMIN, "dur053-k6-c-checker"): 2,
    (SINK_ROLE, "dur053-k6-sink"): 8,
    (EXPORTER, "dur053-k6-sink-exporter"): 1,
}
STARTUP_SECONDS = 2
STARTUP_GAP_US = 200_000
STARTUP_INTERVAL = 0.1

# Group counts and backend details from one activity snapshot. The address
# stays in memory for pod lookup and is omitted from the retained evidence.
TRACE = """
WITH activity AS MATERIALIZED (
    SELECT pid, backend_start, state, usename, application_name,
           wait_event_type, wait_event, host(client_addr) AS client_addr
    FROM pg_stat_activity WHERE backend_type = 'client backend'
)
SELECT json_build_object(
  'captured_at_us', (extract(epoch FROM statement_timestamp()) * 1000000)::bigint,
  'sampler_pid', pg_backend_pid(),
  'limit', current_setting('max_connections')::int,
  'reserved', current_setting('superuser_reserved_connections')::int
              + current_setting('reserved_connections')::int,
  'clients', COALESCE((
    SELECT json_agg(json_build_array(usename,application_name,n) ORDER BY usename,application_name)
    FROM (SELECT usename, application_name, count(*) AS n FROM activity
          GROUP BY usename,application_name) AS counts
  ), '[]'::json),
  'backends', COALESCE((
    SELECT json_agg(activity ORDER BY pid) FROM activity
  ), '[]'::json)
);
"""

TRACE_FILE = "connection-trace.json"


class PhaseCEvidence(Evidence):
    schema = "dur053.k6-phase-c-smoke.v1"

    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.write(
            TRACE_FILE,
            {"samples": [], "peaks": {"engine": 0, "sink": 0}, "rejections": [], "startups": []},
        )


def is_starting(user: str | None, application: str | None) -> bool:
    return user is None and application == ""


def trace_count(server: str, sample: dict[str, Any]) -> int:
    sink = server == "sink"
    limits = SINK_CLIENTS if sink else ENGINE_CLIENTS
    require(
        sample["limit"] == (30 if sink else 100) and sample["reserved"] == 3,
        "DB_CONNECTION_SETTINGS_" + server.upper(),
    )
    application_count = 0
    for user, application, count in sample["clients"]:
        application_count += count
        if is_starting(user, application):
            continue
        require((user, application) in limits, "DB_UNACCOUNTED_CLIENT_" + server.upper())
        require(0 < count <= limits[user, application], "DB_CLIENT_BUDGET_" + server.upper())
    require(
        application_count + sample["reserved"] <= (19 if sink else 82),
        "DB_TOTAL_BUDGET_" + server.upper(),
    )
    return application_count


class Trace:
    def __init__(self, smoke: Deployment) -> None:
        self.smoke = smoke
        self.rows: list[dict[str, Any]] = []
        self.peaks = {"engine": 0, "sink": 0}
        self.rejections: list[dict[str, Any]] = []
        self.startups: list[dict[str, Any]] = []
        self.stop = threading.Event()
        self.initial = threading.Event()
        self.failure: str | None = None
        self.thread = threading.Thread(target=self.sample, daemon=True)

    def pod_names(self) -> dict[str, str]:
        pods = json.loads(
            self.smoke.k(
                "DB_TRACE_PODS", "-n", self.smoke.namespace, "get", "pods", "-o", "json", timeout=5
            )
        )
        return {
            address["ip"]: pod["metadata"]["name"]
            for pod in pods["items"]
            for address in pod["status"].get("podIPs", [])
        }

    def record_backends(
        self, observation: dict[str, Any], pods: Future[dict[str, str]]
    ) -> dict[str, Any]:
        record = dict(observation)
        try:
            names = pods.result()
        except Exception as error:
            names = {}
            record["pod_lookup_error"] = self.smoke.evidence.safe_reason(error)
        record["backends"] = [
            {
                **{key: value for key, value in backend.items() if key != "client_addr"},
                "pod_name": names.get(backend["client_addr"]),
            }
            for backend in observation["backends"]
        ]
        return record

    def sample_server(
        self, server: str, role: str, index: int, executor: ThreadPoolExecutor
    ) -> None:
        row = self.rows[index]
        suffix = "_" + server.upper()
        unresolved = "DB_STARTUP_UNRESOLVED" + suffix
        pending: dict[tuple[int, str], tuple[int, float]] = {}
        observations = []
        pods = None
        followups: list[dict[str, Any]] = []
        latest: dict[str, Any] = {"captured_at_us": None, "backends": []}
        previous_us = None
        outcome = "resolved"
        try:
            with self.smoke.trace_session(server, role, TRACE) as query:
                while True:
                    started = time.monotonic()
                    # The host deadline bounds transport waits. PostgreSQL timestamps
                    # decide whether samples and resolution meet the gate.
                    remaining = (
                        min((end for _, end in pending.values()), default=started + 8) - started
                    )
                    require(remaining > 0, unresolved)
                    try:
                        sample = json.loads(query(remaining))
                    except SmokeFailure as error:
                        if pending and str(error) == "PSQL" + suffix + "_TIMEOUT":
                            raise SmokeFailure(unresolved) from None
                        raise
                    received = time.monotonic()
                    backends = sample.pop("backends")
                    captured_us = sample["captured_at_us"]
                    latest = {"captured_at_us": captured_us, "backends": backends}
                    if previous_us is None:
                        row[server] = sample
                    else:
                        followups.append(sample)
                        row[server]["followups"] = followups
                    current = {(b["pid"], b["backend_start"]): b for b in backends}
                    starting = {
                        key
                        for key, b in current.items()
                        if is_starting(b["usename"], b["application_name"])
                    }
                    if pending or starting:
                        if pods is None:
                            pods = executor.submit(self.pod_names)
                        observations.append(
                            {
                                "captured_at_us": captured_us,
                                "backends": [
                                    b
                                    for key, b in current.items()
                                    if key in pending or key in starting
                                ],
                            }
                        )

                    total = trace_count(server, sample)
                    sample["total"] = total
                    self.peaks[server] = max(total, self.peaks[server])
                    if previous_us is not None:
                        require(
                            0 < captured_us - previous_us <= STARTUP_GAP_US,
                            "DB_STARTUP_SAMPLE_GAP" + suffix,
                        )
                    for key, (first_us, _) in tuple(pending.items()):
                        require(key in current, "DB_STARTUP_VANISHED" + suffix)
                        elapsed_us = captured_us - first_us
                        require(elapsed_us <= STARTUP_SECONDS * 1_000_000, unresolved)
                        if key not in starting:
                            del pending[key]
                        else:
                            require(elapsed_us < STARTUP_SECONDS * 1_000_000, unresolved)
                    for key in starting - pending.keys():
                        require(key[1] is not None, "DB_STARTUP_IDENTITY" + suffix)
                        pending[key] = (captured_us, received + STARTUP_SECONDS)
                    if not pending:
                        return
                    previous_us = captured_us
                    next_poll = min(
                        started + STARTUP_INTERVAL, min(end for _, end in pending.values())
                    )
                    time.sleep(max(0, next_poll - time.monotonic()))
        except Exception as error:
            outcome = self.smoke.evidence.safe_reason(error)
            self.rejections.append(
                {
                    "sample": index,
                    "server": server,
                    "reason": outcome,
                    **self.record_backends(latest, executor.submit(self.pod_names)),
                }
            )
            raise
        finally:
            if pods is not None:
                self.startups.append(
                    {
                        "sample": index,
                        "server": server,
                        "outcome": outcome,
                        "observations": [self.record_backends(item, pods) for item in observations],
                    }
                )

    def sample(self) -> None:
        try:
            with ThreadPoolExecutor(max_workers=4) as executor:
                previous = None
                while not self.stop.is_set():
                    started = time.monotonic()
                    require(previous is None or started - previous <= 2.5, "DB_TRACE_SAMPLE_GAP")
                    previous = started
                    index = len(self.rows)
                    self.rows.append({"time": datetime.now(UTC).isoformat()})
                    futures = [
                        executor.submit(self.sample_server, name, role, index, executor)
                        for name, role in (("engine", "monitor"), ("sink", "admin"))
                    ]
                    for future in futures:
                        future.result()
                    self.initial.set()
                    self.stop.wait(max(0, 1 - (time.monotonic() - started)))
        except Exception as error:
            self.failure = self.smoke.evidence.safe_reason(error)
            self.initial.set()

    def check(self) -> None:
        if self.failure is not None:
            raise SmokeFailure(self.failure)

    def start(self) -> None:
        self.thread.start()
        require(self.initial.wait(10), "DB_TRACE_INITIAL_TIMEOUT")
        self.check()

    def finish(self) -> None:
        self.stop.set()
        if self.thread.ident is not None:
            self.thread.join(timeout=10)
        self.smoke.evidence.write(
            TRACE_FILE,
            {
                "samples": self.rows,
                "peaks": self.peaks,
                "rejections": self.rejections,
                "startups": self.startups,
            },
        )
        require(not self.thread.is_alive(), "DB_TRACE_STOP_TIMEOUT")
        self.check()


class Matrix(Deployment):
    @staticmethod
    def plan(namespace: str) -> dict:
        return {
            **Deployment.plan(namespace),
            "ResourceRevision": "0",
            "Target": "phase-c-clean-resource",
            "Key": "phase-c-clean-key",
        }

    def plans(self) -> list[dict]:
        clean = self.plan(NS)
        return [
            clean,
            {
                **clean,
                "WorkflowID": NS + "-concurrent",
                "Target": "phase-c-concurrent-resource",
                "Key": "phase-c-concurrent-key",
            },
        ]

    def grants(
        self, plans: list[dict], episode: dict, reviewer: str, migration: dict
    ) -> tuple[list[dict], str, int]:
        self.migrate(NS, migration, "C_ENGINE_MIGRATE")
        token, cutoff, uncertainty = self.warmup(episode, self.admin_dsn)
        credentials = self.go(
            "provision",
            DSN=self.admin_dsn,
            Episode=episode,
            Cutoff=cutoff,
            Uncertainty=uncertainty,
            EngineRoles=["durable"],
        )
        self.provisioned = True
        reader_password, decision_password = (
            credentials["ReaderPassword"],
            credentials["DecisionPassword"],
        )
        for password in (reader_password, decision_password):
            self.evidence.secret(password)
        decision_dsn = (
            self.dsn("k6_decision_writer", decision_password, "verify-full", str(self.ca))
            + "&application_name=dur053-k6-c-decision"
        )
        jail = self.scratch / "jail"
        jail.mkdir(mode=0o700)
        shutil.copy2(self.approver, jail / "approver")
        grants = []
        with self.adapter_socket(self.scratch, jail / "live", episode, reviewer, decision_dsn):
            for plan in plans:
                intent = self.go("intent", DSN=self.admin_dsn, Plan=plan, Cutoff=cutoff)
                pending = self.approve(jail, "live", plan, token)
                identity_input = {
                    "intent_id": intent["intent_id"],
                    "proposal_hash": intent["proposal_hash"],
                }
                require(
                    pending.get("status") == 200
                    and pending.get("identity") == {**identity_input, "decision": "PENDING"},
                    "C_PENDING_APPROVAL_MISMATCH",
                )
                approved = self.approve(
                    jail, "live", plan, token, IntentID=intent["intent_id"], Choice="APPROVED"
                )
                expected = {
                    **identity_input,
                    "decision": "APPROVED",
                    "subject": subject(NS, APPROVER),
                }
                observed = approved.get("identity", {})
                require(
                    approved.get("status") == 200
                    and bool(observed.get("decided_at"))
                    and {key: observed.get(key) for key in expected} == expected
                    and set(observed) == {*expected, "decided_at"},
                    "C_RECORDED_APPROVAL_MISMATCH",
                )
                grant = self.go(
                    "apply",
                    DSN=self.admin_dsn,
                    Plan=plan,
                    Cutoff=cutoff,
                    IntentID=intent["intent_id"],
                    Lease=intent["lease"],
                )
                self.evidence.secret(grant["Token"])
                grants.append(grant)
        self.evidence.write(
            "grant-identities.json",
            [
                {
                    "intent_id": grant["IntentID"],
                    "scope_hash": grant["ScopeHash"],
                    "grant_token_sha256": sha(grant["Token"]),
                    "workflow_id": grant["WorkflowID"],
                }
                for grant in grants
            ],
        )
        return grants, reader_password, uncertainty

    def topology_denials(self) -> dict:
        sink_target = {"Address": f"{self.services[SINK]}:{PORT}"}
        # Control endpoints have already been declared private. Confirm both
        # forbidden paths are blocked from their actual source Pod networks.
        require(
            self.probe_effect(ENGINE, "tcp", sink_target)["code"] == "TCP_CONNECTED",
            "SINK_NETWORK_POSITIVE_CONTROL",
        )
        worker = self.call(CLIENT, SINK, "apply", {}, token=True)["code"]
        relay = self.probe_effect(
            f"deployment/{RELAY}", "tcp", {"Address": self.address + ":5432"}
        )["code"]
        require(worker in NETWORK_DENIAL and relay in NETWORK_DENIAL, "K6_C_NETWORK_DENIAL")
        return {"worker_direct_apply": worker, "relay_to_engine_db": relay}

    def database_denials(self, reader_password: str, grant: dict) -> dict:
        same_engine_user = self.sink_dsn("durable", self.harness.password, self.effect_ca)
        sink_on_engine = self.dsn(SINK_ROLE, self.sink_password, "verify-full", str(self.ca))
        reader = self.dsn("k6_sink_grant_reader", reader_password, "verify-full", str(self.ca))
        before = self.snapshot(self.admin_dsn)
        sink_before = self.export_sink()["relations"]
        roles = {
            "engine_writer_on_sink": self.go(
                "probe", DSN=same_engine_user, Probe="update", Grant=grant
            )["code"],
            "sink_writer_on_engine": self.go(
                "probe", DSN=sink_on_engine, Probe="update", Grant=grant
            )["code"],
            "sink_grant_reader_engine_update": self.go(
                "probe", DSN=reader, Probe="update", Grant=grant
            )["code"],
        }
        require(
            roles
            == {
                "engine_writer_on_sink": "28P01",
                "sink_writer_on_engine": "28P01",
                "sink_grant_reader_engine_update": "42501",
            },
            "K6_C_CROSS_DB_WRITE_DENIAL",
        )
        require(
            self.snapshot(self.admin_dsn) == before
            and self.export_sink()["relations"] == sink_before,
            "K6_C_CROSS_DB_DIGEST_CHANGED",
        )
        return roles

    def allocate(self, plan: dict, grant: dict, name: str) -> tuple[dict, dict]:
        envelope = {
            "intent_id": grant["IntentID"],
            "grant_token": grant["Token"],
            "workflow_id": plan["WorkflowID"],
            "node_id": plan["NodeID"],
            "iteration": 0,
            "logical_key": plan["Key"],
            "scope_hash": grant["ScopeHash"],
            "resource_id": plan["Target"],
            "expected_revision": plan["ResourceRevision"],
            "arguments": plan["Arguments"],
            "attempt": 1,
            "message_id": name + "-message",
            "request_id": name + "-allocate",
        }
        result = self.call(ENGINE, SINK, "allocate", envelope, cert=self.engine_tls)
        require(result.get("status") == 200 and result.get("fence") == 1, "ENGINE_FENCE_ALLOCATION")
        observation = {
            **{
                key: envelope[key]
                for key in ("request_id", "intent_id", "resource_id", "attempt", "logical_key")
            },
            "connection_hash": result["connection_hash"],
            "body_hash": result["request_hash"],
            "fence": result["fence"],
            "engine_peer": ENGINE_PEER,
            "response_hash": result["response_hash"],
            "response_length": result["response_length"],
        }
        return {**envelope, "fence": result["fence"], "request_id": name + "-apply"}, observation

    def confirms(self, responses: list[dict], request: dict, snapshot: dict) -> dict:
        require(all(result.get("status") == 200 for result in responses), "APPLY_HTTP_STATUS")
        require(
            len({result["connection_hash"] for result in responses}) == len(responses),
            "WORKER_DELIVERIES_SHARE_CONNECTION",
        )
        calls = [
            call
            for call in snapshot["calls"]
            if call["request_id"] == request["request_id"] and call["operation"] == "apply"
        ]
        require(
            len(calls) == len(responses)
            and sum(c["outcome"] == "APPLIED" for c in calls) == 1
            and sum(c["outcome"] == "DUPLICATE" for c in calls) == len(calls) - 1,
            "SINK_APPLY_CALL_CARDINALITY",
        )
        writes = [
            row for row in snapshot["journal"] if row["resource_id"] == request["resource_id"]
        ]
        receipts = [
            row for row in snapshot["receipts"] if row["resource_id"] == request["resource_id"]
        ]
        require(
            len(writes) == len(receipts) == 1 and writes[0]["revision"] == 1,
            "SINK_EXPECTED_ONE_APPLICATION",
        )
        reply = digest(receipt_bytes(receipts[0]))
        require(
            all(row["response_hash"] == reply for row in (*responses, *calls)),
            "SINK_RECEIPTS_NOT_BYTE_EQUIVALENT",
        )
        return {
            "calls": len(calls),
            "applications": len(writes),
            "receipt_sha256": reply,
            "worker_observations": responses,
        }

    @contextmanager
    def deny(self, step: str):
        with self.evidence.step(step) as row:
            before = self.export_sink()["relations"]
            yield row
            after = self.export_sink()["relations"]
            require(after == before, "DENIAL_CHANGED_SINK_DIGEST")
            row["sink_state_sha256"] = digest(json.dumps(after, sort_keys=True).encode())

    def relay_threats(self, clean: dict) -> tuple[list[tuple[str, str]], list[dict]]:
        denied = []
        observations = []
        with self.deny("d-wrong-sink-peer") as row:
            # Connectivity is allowed from the relay, which presents its
            # genuine ingress certificate to the allocator's engine-only route.
            request = {k: v for k, v in clean.items() if k != "fence"}
            result = self.call(
                f"deployment/{RELAY}", SINK, "allocate", request, cert=self.relay_tls
            )
            require(
                result.get("status") == 403 and result.get("response_hash") == DENIED,
                "WRONG_PEER_NOT_DENIED",
            )
            row.update(result)
        for label, tamper in (
            ("relay-transport-without-worker", False),
            ("invalid-projected-token", True),
        ):
            with self.deny("d-" + label) as row:
                request = {**clean, "request_id": label}
                result = self.call(
                    CLIENT, RELAY, "apply", request, token=tamper, corrupt_token=tamper
                )
                require(
                    result.get("status") == 403 and result.get("response_hash") == DENIED,
                    "RELAY_WORKER_AUTHORITY_NOT_DENIED",
                )
                denied.append((identity(label), result["request_hash"]))
                observations.append(result)
                row.update(result)
        with self.deny("d-allocate-via-relay") as row:
            result = self.call(
                CLIENT, RELAY, "allocate", {**clean, "request_id": "relay-allocate"}, token=True
            )
            require(result.get("status") == 404, "RELAY_ALLOCATOR_ROUTE_EXPOSED")
            row.update(result)
            observations.append(result)
        with self.deny("d-witness-ack-withheld") as row:
            before = self.journal.read_bytes()
            self.stop_witness()
            try:
                result = self.call(
                    CLIENT, RELAY, "apply", {**clean, "request_id": "no-ack"}, token=True
                )
                require(
                    result.get("status") == 503 and result.get("response_length") == 0,
                    "RELAY_ACK_FAILURE_NOT_CLOSED",
                )
                require(self.journal.read_bytes() == before, "STOPPED_WITNESS_JOURNAL_CHANGED")
                row.update(result)
            finally:
                self.start_witness()
        return denied, observations

    def run_matrix(self) -> None:
        with self.evidence.step("a-install-k6-topology"):
            self.identity_created = True
            migration, episode = self.install_topology()
        trace = Trace(self)
        evidence = self.evidence
        try:
            with evidence.step("a-start-two-database-samplers"):
                trace.start()
            with evidence.step("a-sink-migrations-and-credentials"):
                self.migrate_sink()
                self.export_config()
                empty = self.export_sink()
                require(all(row["count"] == 0 for row in empty["relations"]), "SINK_DB_NOT_FRESH")
            with evidence.step("a-provision-real-phase-b-grants"):
                plans = self.plans()
                evidence.write("fixture-plans.json", plans)
                reviewer = self.reviewer(NS, REVIEWER)
                grants, reader_password, uncertainty = self.grants(
                    plans, episode, reviewer, migration
                )
            with evidence.step("a-deploy-sink-and-relay"):
                self.start_witness()
                self.deploy_effect(reviewer, reader_password, uncertainty)
            with evidence.step("a-network-denials") as row:
                row.update(self.topology_denials())
            with evidence.step("a-database-write-denials") as row:
                row.update(self.database_denials(reader_password, grants[0]))
            trace.check()

            with evidence.step("b-engine-peer-allocate") as row:
                clean, first_allocation = self.allocate(plans[0], grants[0], "clean")
                row.update(first_allocation)
                initial = self.export_sink()
                require(
                    len(initial["resources"]) == 1
                    and initial["resources"][0]["revision"] == 0
                    and not initial["journal"],
                    "CLEAN_INITIAL_STATE",
                )
                evidence.write("sink-initial.json", initial)
            with evidence.step("b-two-clean-relay-deliveries") as row:
                responses = [self.call(CLIENT, RELAY, "apply", clean, token=True) for _ in range(2)]
                row.update(self.confirms(responses, clean, self.export_sink()))
            with evidence.step("c-simultaneous-applies") as row:
                concurrent, other_allocation = self.allocate(plans[1], grants[1], "concurrent")
                initial = self.export_sink()
                fresh = [
                    r for r in initial["resources"] if r["resource_id"] == concurrent["resource_id"]
                ]
                require(len(fresh) == 1 and fresh[0]["revision"] == 0, "CONCURRENT_INITIAL_STATE")
                evidence.write("sink-before-concurrent.json", initial)
                start = (datetime.now(UTC) + timedelta(seconds=4)).isoformat()
                with ThreadPoolExecutor(max_workers=PARALLEL) as workers:
                    pending = [
                        workers.submit(
                            self.call,
                            CLIENT,
                            RELAY,
                            "apply",
                            concurrent,
                            token=True,
                            invocation=f"phase-c-concurrent-{index}",
                            start=start,
                        )
                        for index in range(PARALLEL)
                    ]
                    simultaneous = [future.result() for future in pending]
                observed = self.confirms(simultaneous, concurrent, self.export_sink())
                require(
                    max(r["started_ns"] for r in simultaneous)
                    < min(r["finished_ns"] for r in simultaneous),
                    "PROBE_APPLIES_DID_NOT_OVERLAP",
                )
                row.update(observed, allocation=other_allocation)
            trace.check()

            denied, denied_observations = self.relay_threats(clean)
            with evidence.step("e-same-key-lookup") as row:
                request = {**clean, "request_id": "clean-lookup"}
                result = self.call(CLIENT, RELAY, "lookup", request, token=True)
                require(
                    result.get("status") == 200
                    and result.get("response_hash") == responses[0]["response_hash"],
                    "LOOKUP_COMMITTED_RECEIPT_MISMATCH",
                )
                row.update(result)
            with evidence.step("f-sink-rollout-surge"):
                before = self.export_sink()["relations"]
                self.k("SINK_RESTART", "-n", NS, "rollout", "restart", f"deployment/{SINK}")
                self.ready(SINK)
                require(self.export_sink()["relations"] == before, "SINK_ROLLOUT_CHANGED_STATE")
                trace.check()
            with evidence.step("f-two-database-connection-trace") as row:
                trace.check()
                require(
                    len(trace.rows) >= 2 and all(trace.peaks.values()), "DB_TRACE_MISSING_SAMPLES"
                )
                row.update(samples_so_far=len(trace.rows), peaks_so_far=dict(trace.peaks))
            with evidence.step("g-seal-and-export"):
                self.seal()
                self.stop_witness()
                records = load_records(self.journal)
                evidence.write_bytes("host-ingress-journal.jsonl", self.journal.read_bytes())
                snapshot = self.export_sink()
                evidence.write("sink-export.json", snapshot)
            with evidence.step("g-verify-independent-join") as row:
                try:
                    verdict = verify(
                        records,
                        snapshot,
                        engine_peer=ENGINE_PEER,
                        relay_peer=RELAY_PEER,
                        sink_peer=SINK_PEER,
                        expected_applications=2,
                        allocations=[first_allocation, other_allocation],
                        denied_requests=denied,
                        worker_observations=[
                            *responses,
                            *simultaneous,
                            *denied_observations,
                            result,
                        ],
                    )
                except ValueError as error:
                    raise SmokeFailure(str(error)) from None
                require(len(verdict["joined"]) == 2 + PARALLEL + 1, "K6_JOIN_EXPECTED_DELIVERIES")
                evidence.write("join-verdict.json", {"status": "PASS", **verdict})
                row.update(
                    applications=verdict["applications"],
                    joined=len(verdict["joined"]),
                    session_high_water=verdict["session_high_water"],
                )
        finally:
            try:
                trace.finish()
            except Exception:
                with evidence.step("retain-two-database-connection-trace"):
                    raise


def inherited_failure(
    output: Path, code: str = "SHARED_KIND_HARNESS_FAILED_BEFORE_PHASE_C"
) -> None:
    if not output.exists():
        evidence = PhaseCEvidence(make_output(output))
        evidence.report.update(reason=code)
        evidence.checkpoint()


def record_handoff_failure(output: Path, error: BaseException) -> None:
    if output.exists():
        return
    evidence = PhaseCEvidence(make_output(output))
    code = evidence.safe_reason(error)
    evidence.report.update(reason=code)
    evidence.report["outcomes"].append(
        {"step": "phase-c-handoff", "status": "FAIL", "reason": code}
    )
    evidence.checkpoint()


def make_output(output: Path) -> Path:
    output = output.resolve()
    require(not output.is_relative_to(ROOT), "OUTPUT_INSIDE_REPOSITORY")
    output.mkdir()
    return output


def run(
    output: Path,
    harness: Driver | None = None,
    address: str | None = None,
    *,
    phase: str = "c",
    evidence_type: type[Evidence] = PhaseCEvidence,
    matrix_type: type[Deployment] = Matrix,
) -> int:
    evidence = evidence_type(make_output(output))
    owns_harness = harness is None
    scratch, smoke = None, None
    try:
        with evidence.step("private-scratch"):
            require(
                os.name == "posix" and bool(os.getenv("RUNNER_TEMP")),
                "HOSTED_LINUX_RUNNER_REQUIRED",
            )
            scratch = Path(
                tempfile.mkdtemp(prefix=f"dur053-k6-{phase}-", dir=Path(os.environ["RUNNER_TEMP"]))
            )
        if owns_harness:
            with evidence.step("standalone-kind-harness"):
                kind_scratch, logs = scratch / "kind", scratch / "kind-logs"
                kind_scratch.mkdir(mode=0o700)
                logs.mkdir(mode=0o700)
                harness = Driver(logs, kind_scratch)
                harness.install()
                harness.build()
                for name, addon in LOCK["addons"].items():
                    if addon["installed_in_k3a"]:
                        harness.prepare_addon(name)
                address = harness.cluster()["192.0.2.1"]
        require(harness is not None and address is not None, "KIND_HANDOFF_INCOMPLETE")
        with evidence.step(f"build-phase-{phase}-ordinary-binaries-and-image"):
            smoke = matrix_type(evidence, scratch, harness)
            smoke.build()
        with evidence.step("a-engine-tls-and-kind-issuer") as row:
            row.update(smoke.bootstrap(address))
        with evidence.step("a-distinct-postgres-instances") as row:
            row.update(smoke.create_databases())
        with evidence.step("a-private-episode-tls") as row:
            row.update(smoke.tls())
        smoke.run_matrix()
    except (Exception, KeyboardInterrupt) as error:
        evidence.report["reason"] = next(
            (row["reason"] for row in evidence.report["outcomes"] if row["status"] == "FAIL"),
            evidence.safe_reason(error),
        )
    else:
        evidence.report.update(status="PASS", reason="MATRIX_COMPLETE")
    finally:
        failure = evidence.report["reason"]
        operations = []
        if smoke is not None:
            if failure != "MATRIX_COMPLETE":

                def retain_partial() -> None:
                    if (
                        smoke.journal.exists()
                        and not (evidence.root / "host-ingress-journal.jsonl").exists()
                    ):
                        evidence.write_bytes(
                            "host-ingress-journal.partial.jsonl", smoke.journal.read_bytes()
                        )
                    if (
                        hasattr(smoke, "export_file")
                        and not (evidence.root / "sink-export.json").exists()
                    ):
                        evidence.write("sink-export.partial.json", smoke.export_sink())

                operations.append(("retain-partial-observations", retain_partial))
            operations.append(("episode-cleanup", smoke.teardown))
        if owns_harness and harness is not None:
            operations.append(("kind-cleanup", harness.cleanup))
        if scratch is not None:
            operations.append(("private-scratch-cleanup", lambda: shutil.rmtree(scratch)))
        operations.append(("g-retained-output-credential-scan", evidence.scan))
        for name, operation in operations:
            try:
                with evidence.step(name):
                    operation()
            except Exception as error:
                evidence.report["status"] = "FAIL"
                if (
                    failure == "MATRIX_COMPLETE"
                    or evidence.safe_reason(error) == "CREDENTIAL_SCAN_FAILED"
                ):
                    failure = evidence.safe_reason(error)
        evidence.report["reason"] = failure
        if not (evidence.root / "join-verdict.json").exists():
            evidence.write("join-verdict.json", {"status": "FAIL", "reason": failure})
        evidence.checkpoint()
    return 0 if evidence.report["status"] == "PASS" else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, required=True)
    raise SystemExit(run(parser.parse_args().output_root))
