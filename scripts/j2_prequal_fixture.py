"""Bulk-load a deterministic J2 fixture from retained K3 short row shapes."""

import base64
import csv
import hashlib
import json
import tarfile
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

from j2_checker import ROOT, sha256
from j2_qualification_evidence import POPULATION_RATE, write

K3 = ROOT / "experiments/portfolio/k8s-recovery/k3-pilot-v2/k3-pilot-4a/43220876354"
K3_ARCHIVE = K3 / "payload/0001.tar.xz"
NAMESPACE = "j2-volume"
RUN_ID = "j2-volume-00000000000-1-26678"
SEED = 1_000_534_201
ORIGIN = datetime(2026, 11, 26, tzinfo=UTC)
ACTIVITIES = ["dur048.sleep"] * 8 + ["pure.echo", "pure.add"]
LEASES_PER_WORKFLOW = 26
LINKED = {
    "workflow_executions": ("workflow_id", 1),
    "node_instances": ("workflow_id", 2),
    "activity_attempts": ("workflow_id", 1),
    "retry_policies": ("workflow_id", 2),
    "transition_history": ("workflow_id", 7),
    "outbox": ("workflow_id", 6),
    "event_inbox": ("event_id", 6),
    "outbox_publications": ("event_id", 6),
    "scheduler_wakeups": ("workflow_id", 5),
}


def derive_uuid(value):
    return str(UUID(bytes=hashlib.sha256(value.encode()).digest()[:16]))


def compact(value):
    return json.dumps(value, separators=(",", ":"))


def definition_id(activity):
    return f"dur053-{RUN_ID}-{activity}"


def pod_name(index):
    return f"worker-0000000000-j2f{index:02d}"


def calibration():
    manifest = json.loads((K3 / "manifest.json").read_bytes())["artifact_sha256"]
    with tarfile.open(K3_ARCHIVE) as archive:
        member = archive.getmember("short-1/snapshot.json")
        raw = archive.extractfile(member).read()
    digest = hashlib.sha256(raw).hexdigest()
    if digest != manifest[member.name]:
        raise ValueError("retained K3 snapshot does not match its manifest")
    snapshot = json.loads(raw)["Snapshot"]
    tables = {name: table for name, table in snapshot.items() if name.startswith("engine.")}
    samples = {name.removeprefix("engine."): table["rows"] for name, table in tables.items()}
    if len(samples["workflow_executions"]) != 5138:
        raise ValueError("K3 short population differs from the calibrated 5,138")
    events = {row["event_id"]: row["workflow_id"] for row in samples["outbox"]}
    grouped = defaultdict(dict)
    for table, (key, multiplicity) in LINKED.items():
        for row in samples[table]:
            identity = row[key] if key == "workflow_id" else events[row[key]]
            grouped[identity].setdefault(table, []).append(row)
        if len(samples[table]) != len(grouped) * multiplicity:
            raise ValueError(f"unexpected K3 short count for {table}")
    candidates = defaultdict(list)
    for identity, rows in sorted(grouped.items()):
        if any(len(rows.get(name, [])) != n for name, (_, n) in LINKED.items()):
            raise ValueError(f"incomplete linked K3 workflow {identity}")
        candidates[rows["activity_attempts"][0]["node_id"]].append(rows)
    iterator = {activity: iter(rows) for activity, rows in candidates.items()}
    templates = [next(iterator[activity]) for activity in ACTIVITIES]
    static = {name: samples[name] for name in ("workflow_definitions", "consumer_offsets")}
    return (
        templates,
        static,
        {
            "archive": K3_ARCHIVE.relative_to(ROOT).as_posix(),
            "archive_sha256": sha256(K3_ARCHIVE),
            "member": member.name,
            "member_sha256": digest,
            "member_bytes": len(raw),
            "k3_activity_mix": {activity: len(rows) for activity, rows in candidates.items()},
            "k3_table_counts": {name: t["count"] for name, t in tables.items()},
            "k3_compact_row_bytes": {
                name: len(compact(t["rows"]).encode()) for name, t in tables.items()
            },
            "lease_acquisitions_per_workflow": LEASES_PER_WORKFLOW,
        },
    )


class Workflow:
    def __init__(self, sequence, rows):
        orig = rows["workflow_executions"][0]
        self.sequence = sequence
        self.rows = rows
        self.source_id = orig["workflow_id"]
        self.start = datetime.fromisoformat(orig["created_at"])
        self.scheduled_ns = sequence * 10**9 // POPULATION_RATE
        self.scheduled = ORIGIN + timedelta(microseconds=self.scheduled_ns // 1000)
        self.identity = f"dur053-{RUN_ID}-{SEED}-{sequence:06d}"
        self.partition = sequence % 16
        self.activity = rows["activity_attempts"][0]["node_id"]
        self.worker_id = f"{pod_name(sequence % 2)}-{sequence // 2 % 2}"
        self.payload_hash = "sub-v1:" + hashlib.sha256(self.identity.encode()).hexdigest()
        self.expected = (
            5 if self.activity == "pure.add" else {"run_id": RUN_ID, "sequence": sequence}
        )

    def timestamp(self, source):
        return (
            self.scheduled
            + timedelta(milliseconds=10)
            + (datetime.fromisoformat(source) - self.start)
        ).isoformat()

    def row(self, table, index, original):
        row = dict(original)
        for field, value in original.items():
            if value is None:
                continue
            if field.endswith("_at") or field in {"heartbeat_deadline", "claim_expires_at"}:
                row[field] = self.timestamp(value)
            elif field in {
                "event_id",
                "publication_id",
                "transition_id",
                "wakeup_id",
                "claim_token",
            }:
                row[field] = derive_uuid(f"{self.identity}/{value}")
            elif field in {"workflow_id", "submission_key"}:
                row[field] = self.identity
            elif field in {"partition_id", "kafka_partition"}:
                row[field] = self.partition
            elif field == "kafka_offset":
                row[field] = self.sequence * 6 + index
            elif field == "worker_request_id":
                row[field] = f"{self.worker_id}:{derive_uuid(self.identity + '/request')}"
            elif field == "worker_id":
                row[field] = self.worker_id
            elif field == "logical_effect_key":
                row[field] = value.replace(self.source_id, self.identity)
            elif field == "traceparent" and value:
                digest = hashlib.sha256(f"{self.identity}/{value}".encode()).hexdigest()
                row[field] = f"00-{digest[:32]}-{digest[32:48]}-01"
            elif isinstance(value, dict):
                row[field] = self.payload(value)
        if table == "workflow_executions":
            row.update(
                namespace=NAMESPACE,
                submission_payload_hash=self.payload_hash,
                definition_id=definition_id(self.activity),
            )
        return row

    def payload(self, value):
        if isinstance(value, dict):
            return {
                key: RUN_ID
                if key == "run_id"
                else self.sequence
                if key == "sequence"
                else self.payload(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [self.payload(item) for item in value]
        if isinstance(value, str):
            return value.replace(self.source_id, self.identity)
        return value

    def arrival(self):
        at = self.scheduled.isoformat()
        committed_at = self.timestamp(self.rows["workflow_executions"][0]["created_at"])
        body = [2, 3] if self.activity == "pure.add" else self.expected
        identity = {
            "workflow_id": self.identity,
            "submission_key": self.identity,
            "namespace": NAMESPACE,
            "definition_id": definition_id(self.activity),
            "definition_version": 1,
        }
        return {
            "sequence": self.sequence,
            "key": self.identity,
            "workflow_id": self.identity,
            "activity": self.activity,
            "scheduled_at": at,
            "sent_at": at,
            "scheduled_ns": self.scheduled_ns,
            "send_ns": self.scheduled_ns,
            "finished_ns": self.scheduled_ns + 10**7,
            "lateness_ns": 0,
            "http_status": 201,
            "outcome": "ACCEPTED",
            "request": {
                **identity,
                "actor_id": "dur053-loadgen",
                "initial_input": body,
                "payload": body,
                "initial_node_id": self.activity,
            },
            "response": {
                "created": True,
                "workflow": {
                    **identity,
                    "payload_hash": self.payload_hash,
                    "partition_id": self.partition,
                    "state": "RUNNABLE",
                    "revision": 1,
                    "created_at": committed_at,
                    "updated_at": committed_at,
                },
            },
            "expected": self.expected,
        }

    def witness(self):
        attempt = self.row("activity_attempts", 0, self.rows["activity_attempts"][0])
        history = {row["reason"]: row for row in self.rows["transition_history"]}
        start, end = [
            datetime.fromisoformat(self.timestamp(history[reason]["created_at"]))
            for reason in ("ATTEMPT_CREATED", "ATTEMPT_RESULT_RECORDED")
        ]
        base = {
            k: attempt[k]
            for k in (
                "workflow_id",
                "node_id",
                "iteration",
                "attempt_number",
                "worker_id",
                "claim_token",
            )
        }
        base["invocation_id"] = derive_uuid(self.identity + "/invocation")
        base.update(activity_name=self.activity, activity_version="v1")
        return [
            {
                **base,
                "event": name,
                "timestamp": point.isoformat(),
                "monotonic_ns": (point - ORIGIN) // timedelta(microseconds=1) * 1000 + 1,
                **({"outcome": "SUCCEEDED"} if name == "end" else {}),
            }
            for name, point in (("start", start + (end - start) / 4), ("end", end))
        ]


def copy_rows(sql, table, rows):
    first = next(rows)
    # Columns are the retained template's fields. Later schema additions use
    # the migrated PostgreSQL defaults. jsonb_populate_record preserves types.
    columns = ", ".join(f'"{name}"' for name in first)
    sql.write("COPY j2_input FROM STDIN WITH (FORMAT csv);\n")
    csv_output = csv.writer(sql, lineterminator="\n")
    csv_output.writerow([compact(first)])
    csv_output.writerows([compact(row)] for row in rows)
    sql.write("\\.\n")
    sql.write(
        f"INSERT INTO engine.{table} ({columns}) SELECT {columns} FROM j2_input, "
        f"LATERAL jsonb_populate_record(NULL::engine.{table}, document);\n"
        "TRUNCATE j2_input;\n"
    )


def build_fixture(scratch: Path, evidence: Path, count: int):
    templates, static, source = calibration()
    workflows = [Workflow(i, templates[i % len(templates)]) for i in range(count)]
    with (scratch / "fixture.sql").open("w", encoding="utf-8", newline="\n") as sql:
        sql.write(
            "BEGIN;\nSET LOCAL TIME ZONE 'UTC';\nCREATE TEMP TABLE j2_input(document jsonb);\n"
        )
        copy_rows(
            sql,
            "workflow_definitions",
            (
                {**row, "definition_id": definition_id(row["graph"]["entry"])}
                for row in static["workflow_definitions"]
            ),
        )
        for table in LINKED:
            copy_rows(
                sql,
                table,
                (wf.row(table, i, row) for wf in workflows for i, row in enumerate(wf.rows[table])),
            )
        copy_rows(
            sql,
            "consumer_offsets",
            (
                {**row, "updated_at": ORIGIN.isoformat(), "next_offset": 6 * count}
                for row in static["consumer_offsets"]
            ),
        )
        sql.write(f"""
INSERT INTO engine.lease_acquisitions
SELECT md5('j2-lease-' || n)::uuid, ((n - 1) % 16)::smallint,
       md5('j2-owner-' || ((n - 1) % 16))::uuid, (n - 1) / 16 + 1,
       TIMESTAMPTZ '{ORIGIN.isoformat()}' + (n + 15000) * INTERVAL '1 millisecond',
       TIMESTAMPTZ '{ORIGIN.isoformat()}' + n * INTERVAL '1 millisecond'
FROM generate_series(1, {LEASES_PER_WORKFLOW * count}) AS n;
UPDATE engine.schema_migrations SET applied_at=TIMESTAMPTZ '{ORIGIN.isoformat()}';
UPDATE engine.partition_leases SET updated_at=TIMESTAMPTZ '{ORIGIN.isoformat()}';
COMMIT;
ANALYZE;
""")
    arrivals = [wf.arrival() for wf in workflows]
    write(
        evidence / "arrivals.json",
        {
            "schema": "dur053.arrivals.v1",
            "status": "PASS",
            "namespace": NAMESPACE,
            "run_id": RUN_ID,
            "seed": SEED,
            "offered": count,
            "arm": "short",
            "rate": POPULATION_RATE,
            "rows": arrivals,
        },
    )
    write(
        evidence / "plan.json",
        {
            "run_id": RUN_ID,
            "seed": SEED,
            "namespace": NAMESPACE,
            "periods": [[0, count, POPULATION_RATE]],
        },
    )
    observed = workflows[-1].scheduled + timedelta(seconds=15)
    write(
        evidence / "terminals.json",
        {
            "ids": [wf.identity for wf in workflows],
            "observed_ns": (observed - datetime(1970, 1, 1, tzinfo=UTC))
            // timedelta(microseconds=1)
            * 1000,
        },
    )
    probes = {}
    for phase, at in (("setup", ORIGIN - timedelta(seconds=1)), ("measured", observed)):
        probes[phase] = {
            name: (at + timedelta(milliseconds=i)).isoformat()
            for i, name in enumerate(("observer_before", "database", "observer_after"))
        }
    write(evidence / "clock.json", probes)

    header = {"event": "session", "timestamp": ORIGIN.isoformat(), "monotonic_ns": 1}
    ledgers = []
    for index in range(2):
        pod = {
            "session": derive_uuid(f"j2-session-{index}"),
            "pod_uid": derive_uuid(f"j2-pod-{index}"),
            "pod_name": pod_name(index),
        }
        rows = [header, *(event for wf in workflows[index::2] for event in wf.witness())]
        data = "".join(
            compact({**event, **pod, "sequence": i}) + "\n" for i, event in enumerate(rows, 1)
        ).encode()
        inventory = {
            **pod,
            "file": f"{pod['session']}.jsonl",
            "complete": True,
            "errors": 0,
            "attempted": len(rows),
            "written": len(rows),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
        ledgers.append({"inventory": inventory, "data": base64.b64encode(data).decode("ascii")})
    write(
        evidence / "witness.json",
        {
            "expected_pods": [ledger["inventory"]["pod_uid"] for ledger in ledgers],
            "ledgers": ledgers,
        },
    )
    write(
        evidence / "fixture.json",
        {
            **source,
            "population": count,
            "fixture_activity_mix": Counter(wf.activity for wf in workflows),
            "sql_sha256": sha256(scratch / "fixture.sql"),
            "journal_bytes": (evidence / "arrivals.json").stat().st_size,
            "witness_bytes": (evidence / "witness.json").stat().st_size,
        },
    )
