"""Rehearse J2 setup, fresh-state reset and failure retention on hosted kind."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import shutil
import signal
import time
from collections import Counter
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import dur053_k3 as kind
from dur053_k3_policy import application_groups_absent
from j2_overlay import EXPORTER, J2, PROM
from j2_qualification_evidence import PROFILE, file_record, read, require, write
from j2_qualify_volume import ADMIN_SETTINGS, QUIET_SQL, runner_cgroup
from j2_verify_overlay import NS, OTHER, J2Driver, apply_profile, check_network, start_profile

APP = "j2-harness"
DB = "j2_harness"
TOPICS = {
    "runtime-workers-v1": "durable-agent.tasks.v1",
    "runtime-schedulers-v1": "durable-agent.events.v1",
}
REPLICAS = {
    "api": 2,
    "operator": 1,
    "worker-control": 2,
    "scheduler": 2,
    "relay": 1,
    "demand": 1,
    "worker": 2,
    "observer": 1,
    EXPORTER: 1,
    PROM: 1,
}
PARTITIONS = set(range(8))
DB_TABLES = (
    "workflow_executions",
    "transition_history",
    "activity_attempts",
    "attempt_result_evidence",
    "outbox",
    "event_inbox",
    "consumer_offsets",
)
ARRIVALS = "/evidence/j2-reset-arrivals.json"
SEED_COUNT = 10
INTERRUPTED_COUNT = 30
WORK_SECONDS = 30 * 60
CLOSEOUT_SECONDS = 8 * 60

WITNESS = """import json,pathlib,urllib.request
root=pathlib.Path('/witness')
with urllib.request.urlopen('http://127.0.0.1:8081/witness',timeout=3) as c:
    inventory=json.load(c)
ledger=root / inventory['pod_name'] / inventory['file']
print(json.dumps({'files':sorted(p.relative_to(root).as_posix() for p in
                                 root.rglob('*') if p.is_file()),
                  'inventory':inventory,'ledger':ledger.read_text()}))
"""

# Wait for real committed work before interrupting the stock process. The
# stock binary retains its pre-send plan on interruption. This supervisor
# records a separate partial journal of independently observed admissions.
INTERRUPT = """import json,os,subprocess,sys,time
import psycopg

def event(name, **data):
    print(json.dumps({'event':name,'utc_ns':time.time_ns(),**data}),flush=True)

with psycopg.connect(os.environ['DATABASE_URL'],connect_timeout=3,autocommit=True,
                     options='-c statement_timeout=2000 -c default_transaction_read_only=on') as db:
    proc=subprocess.Popen(sys.argv[1:])
    try:
        event('started')
        limit=time.monotonic()+20
        while time.monotonic()<limit and proc.poll() is None:
            rows=db.execute('SELECT workflow_id FROM engine.workflow_executions '
                            'ORDER BY workflow_id').fetchall()
            if rows:
                event('committed',ids=[r[0] for r in rows])
                break
            time.sleep(.2)
        else:
            raise RuntimeError('no durable partial admission before stop')
    finally:
        if proc.poll() is None:
            proc.terminate()
        try:
            code=proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            code=proc.wait(timeout=3)
        event('stopped',exit_code=code)
"""


class InjectedStop(Exception):
    """The controlled partial submission stop completed with evidence."""


def topic_state(description, offsets):
    headers = re.findall(
        r"Topic:\s+(\S+)\s+TopicId:\s+(\S+)\s+PartitionCount:\s+(\d+)"
        r"\s+ReplicationFactor:\s+(\d+)",
        description,
    )
    topics = {
        topic: identity
        for topic, identity, parts, factor in headers
        if int(parts) == len(PARTITIONS) and factor == "1"
    }
    require(
        len(headers) == len(topics) == len(TOPICS)
        and set(topics) == set(TOPICS.values())
        and len(set(topics.values())) == len(topics),
        "Kafka topic identity, partition count or replication drift",
    )
    latest = {topic: {} for topic in topics}
    for line in offsets.splitlines():
        if line.partition(":")[0] not in latest:
            continue
        topic, partition, offset = line.split(":")
        partition, offset = int(partition), int(offset)
        require(partition not in latest[topic] and offset >= 0, "invalid Kafka offset row")
        latest[topic][partition] = offset
    require(all(set(rows) == PARTITIONS for rows in latest.values()), "missing Kafka offsets")
    return topics, latest


def group_state(raw, group, *, empty):
    parts = set()
    members = set()
    offsets = []
    for line in raw.splitlines():
        cols = line.split()
        if not cols or cols[0] != group:
            continue
        require(len(cols) >= 9, f"incomplete Kafka group row: {group}")
        _, topic, part, committed, end, _, member, *_ = cols
        part = int(part)
        require(
            topic == TOPICS[group] and part not in parts and member != "-",
            f"invalid Kafka assignment: {group}",
        )
        require(
            not empty or (committed in {"-", "0"} and end == "0"),
            f"stale Kafka group offset: {group}",
        )
        parts.add(part)
        members.add(member)
        offsets.append(0 if committed == "-" else int(committed))
    expected_members = 4 if group == "runtime-workers-v1" else 2
    require(
        parts == PARTITIONS and len(members) == expected_members,
        f"incomplete Kafka assignment: {group}",
    )
    return offsets


def verify_witness(records, *, empty):
    files = set()
    starts = 0
    for uid, name, data in records:
        inv = data["inventory"]
        raw = data["ledger"].encode()
        rows = [json.loads(line) for line in raw.splitlines()]
        require(
            inv["pod_uid"] == uid
            and inv["pod_name"] == name
            and inv["complete"] is True
            and inv["errors"] == 0
            and inv["attempted"] == inv["written"] == len(rows)
            and inv["sha256"] == hashlib.sha256(raw).hexdigest()
            and rows
            and rows[0]["event"] == "session"
            and all(r["pod_uid"] == uid and r["session"] == inv["session"] for r in rows),
            f"invalid witness ledger for {name}",
        )
        require(not empty or len(rows) == 1, f"prior invocation evidence for {name}")
        files.add(f"{name}/{inv['file']}")
        starts += sum(row["event"] == "start" for row in rows)
    require(
        len(records) == len(files) == REPLICAS["worker"]
        and all(set(data["files"]) == files for _, _, data in records),
        "witness volume has missing or stale files",
    )
    return starts


def verify_pods(pods, images):
    running = Counter()
    completed = Counter()
    for pod in pods:
        meta, status = pod["metadata"], pod["status"]
        name = meta["name"]
        require(not meta.get("deletionTimestamp"), f"pod is terminating: {name}")
        containers = status["containerStatuses"] + status.get("initContainerStatuses", [])
        require(
            containers
            and all(
                c["restartCount"] == 0 and c["imageID"].split("@")[-1] in images for c in containers
            ),
            f"pod image or restart drift: {name}",
        )
        if status["phase"] == "Succeeded":
            jobs = [
                owner["name"] for owner in meta.get("ownerReferences", []) if owner["kind"] == "Job"
            ]
            require(len(jobs) == 1, f"unexpected completed pod: {name}")
            completed.update(jobs)
        else:
            require(
                status["phase"] == "Running"
                and any(
                    c["type"] == "Ready" and c["status"] == "True"
                    for c in status.get("conditions", [])
                )
                and all(c["ready"] for c in status["containerStatuses"]),
                f"pod not Ready: {name}",
            )
            running[meta["labels"]["app"]] += 1
    require(running == REPLICAS, f"unexpected Ready replicas: {dict(running)}")
    require(completed == {"migrate": 1, "compatibility": 1}, "bootstrap jobs differ")


def verify_new_identity(previous, current):
    if previous is None:
        return
    for field in ("namespaces", "database_oid", "witness_pvc", "witness_pv", "topics", "pods"):
        old, new = previous[field], current[field]
        if isinstance(old, dict):
            require(set(old.values()).isdisjoint(new.values()), f"reused identity: {field}")
        else:
            require(old != new, f"reused identity: {field}")


def resources(spec):
    """Compare API-canonical CPU (1) with the equivalent rendered form (1000m)."""
    return {
        phase: {
            **quantities,
            "cpu": (
                int(quantities["cpu"][:-1])
                if quantities["cpu"].endswith("m")
                else 1000 * int(quantities["cpu"])
            ),
        }
        for phase, quantities in spec["resources"].items()
    }


def verify_container_specs(pods, docs):
    templates = {
        (d["metadata"]["namespace"], d["metadata"]["name"]): d["spec"]["template"]["spec"]
        for d in docs
        if d["kind"] in {"Deployment", "Job"}
    }
    for pod in pods:
        meta = pod["metadata"]
        app = meta.get("labels", {}).get("app")
        jobs = [
            owner["name"] for owner in meta.get("ownerReferences", []) if owner["kind"] == "Job"
        ]
        template = templates[meta["namespace"], jobs[0] if jobs else app]
        statuses = {
            c["name"]: c
            for key in ("containerStatuses", "initContainerStatuses")
            for c in pod["status"].get(key, [])
        }
        for field in ("containers", "initContainers"):
            wanted = {c["name"]: c for c in template.get(field, [])}
            actual = {c["name"]: c for c in pod["spec"].get(field, [])}
            require(wanted.keys() == actual.keys(), f"unexpected containers in {meta['name']}")
            for name, c in actual.items():
                pinned = wanted[name]
                require(
                    c["image"] == pinned["image"]
                    and statuses[name]["imageID"].split("@")[-1] == pinned["image"].split("@")[-1]
                    and resources(c) == resources(pinned),
                    f"image or resources differ for {meta['name']}/{name}",
                )


class Harness(J2Driver):
    def __init__(self, output, scratch):
        super().__init__(output, scratch)
        self.evidence = output
        self.monitor_password = secrets.token_hex(32)
        self.deadline = time.monotonic() + WORK_SECONDS
        self.failed_stage = None
        self.observer = None

    def command(self, args, *, timeout=180, **kwargs):
        left = self.deadline - time.monotonic()
        require(left > 0, "J2 harness stage deadline expired")
        return super().command(args, timeout=min(timeout, left), **kwargs)

    @contextmanager
    def stage(self, name):
        start = time.monotonic_ns()
        record = {"stage": name, "start_ns": start, "status": "completed"}
        try:
            yield
        except BaseException as error:
            self.failed_stage = name
            record.update(status="stopped", error=str(error))
            raise
        finally:
            record["end_ns"] = time.monotonic_ns()
            with (self.evidence / "stages.jsonl").open("a", encoding="utf-8") as log:
                log.write(json.dumps(record) + "\n")

    def kube(self, *args):
        return json.loads(self.k("get", *args, "-o", "json").stdout)

    def kafka(self, tool, *args, **kwargs):
        return self.command(
            [
                "docker",
                "exec",
                kind.CLUSTER + "-kafka",
                f"/opt/kafka/bin/kafka-{tool}.sh",
                "--bootstrap-server",
                "localhost:19092",
                *args,
            ],
            timeout=15,
            **kwargs,
        )

    def transport(self, phase, *, empty):
        raw = "\n".join(
            self.kafka("topics", "--describe", "--topic", topic).stdout for topic in TOPICS.values()
        )
        offsets = self.kafka("get-offsets", "--time", "-1").stdout
        groups = self.kafka("consumer-groups", "--list").stdout
        write(
            self.output / f"{phase}-kafka-raw.json",
            {
                "describe": raw,
                "latest": offsets,
                "groups": groups,
            },
        )
        topics, latest = topic_state(raw, offsets)
        write(
            self.output / f"{phase}-transport.json",
            {"describe": raw, "topic_ids": topics, "latest": latest, "groups": groups},
        )
        require(
            not empty or all(not any(rows.values()) for rows in latest.values()),
            "Kafka contains prior messages",
        )
        return topics

    def groups(self, *, empty):
        observed = {}

        def ready():
            for group in TOPICS:
                result = self.kafka(
                    "consumer-groups",
                    "--describe",
                    "--offsets",
                    "--group",
                    group,
                    allowed=True,
                    record=False,
                )
                observed[group] = {
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                    "exit": result.returncode,
                }
            write(self.output / "group-offsets.json", observed)
            try:
                for group, result in observed.items():
                    if result["exit"] != 0:
                        return False
                    offsets = group_state(result["stdout"], group, empty=empty)
                    if not empty and not any(offsets):
                        return False
                return True
            except ValueError:
                return False

        self.until(ready, 75)

    def pods_and_witness(self, *, empty):
        pods = self.kube("pods", "-n", APP)["items"] + self.kube("pods", "-n", NS)["items"]
        write(self.output / ("fresh-pods.json" if empty else "seeded-pods.json"), pods)
        records = []
        workers = [p for p in pods if p["metadata"].get("labels", {}).get("app") == "worker"]
        for pod in workers:
            meta = pod["metadata"]
            raw = self.k("-n", APP, "exec", meta["name"], "--", "python", "-c", WITNESS)
            records.append((meta["uid"], meta["name"], json.loads(raw.stdout)))
        write(self.output / ("fresh-witness.json" if empty else "seeded-witness.json"), records)
        return pods, verify_witness(records, empty=empty)

    def assert_profile(self, pods, exporter):
        overlay = read(self.output / "overlay-rendered.json")
        app = read(self.output / "application-rendered.json")
        images = {
            *self.images.values(),
            exporter,
            *(
                c["image"]
                for doc in overlay["items"]
                if doc["kind"] == "Deployment"
                for c in doc["spec"]["template"]["spec"]["containers"]
            ),
        }
        verify_pods(pods, {image.split("@")[-1] for image in images})
        verify_container_specs(pods, [*app, *overlay["items"]])
        api = next(o for o in app if o["kind"] == "Deployment" and o["metadata"]["name"] == "api")
        specs = [api["spec"]["template"]["spec"]]
        api_pods = [p for p in pods if p["metadata"].get("labels", {}).get("app") == "api"]
        specs.extend(p["spec"] for p in api_pods)
        require(
            not APP.startswith("dur050-")
            and all(
                not c.get("envFrom")
                and not ADMIN_SETTINGS.intersection(e["name"] for e in c.get("env", []))
                for spec in specs
                for c in spec["containers"]
            ),
            "admission settings present in the API specifications",
        )
        admission = {}
        for pod in api_pods:
            container = pod["status"]["containerStatuses"][0]["containerID"].split("://", 1)[1]
            raw = self.command(
                ["docker", "exec", kind.CLUSTER + "-control-plane", "crictl", "inspect", container],
                record=False,
                timeout=10,
            )
            live_env = json.loads(raw.stdout)["info"]["runtimeSpec"]["process"]["env"]
            require(isinstance(live_env, list) and live_env, "API process environment missing")
            admission[pod["metadata"]["uid"]] = sorted(
                ADMIN_SETTINGS.intersection(e.partition("=")[0] for e in live_env)
            )
        write(self.output / "running-admission-keys.json", admission)
        require(not any(admission.values()), "admission settings present at runtime")
        scaler = self.kube("-n", APP, "scaledobject", "worker-demand")["metadata"]["annotations"]
        require(
            all(
                scaler.get(f"autoscaling.keda.sh/{key}") == value
                for key, value in {
                    "paused-replicas": "2",
                    "paused-scale-in": "true",
                    "paused-scale-out": "true",
                }.items()
            ),
            "KEDA pause drift",
        )
        for worker in (p for p in pods if p["metadata"].get("labels", {}).get("app") == "worker"):
            env = {e["name"]: e.get("value") for e in worker["spec"]["containers"][0]["env"]}
            require(
                env["WORKER_SLOTS"] == "2"
                and env["WORKER_DRAIN_SECONDS"] == "30"
                and env["DUR048_ACTIVITY_DELAY_MS"] == "100"
                and worker["spec"]["terminationGracePeriodSeconds"] == 40,
                "worker profile drift",
            )

    def storage(self):
        storage = self.kube("pvc", "witness", "-n", APP)
        volume = self.kube("pv", storage["spec"]["volumeName"])
        write(self.output / "witness-storage.json", {"claim": storage, "volume": volume})
        require(
            storage["status"]["phase"] == "Bound"
            and volume["spec"]["claimRef"]["uid"] == storage["metadata"]["uid"]
            and volume["spec"]["persistentVolumeReclaimPolicy"] == "Delete",
            "witness volume is not a fresh deletable claim",
        )
        return storage, volume

    def remove_episode(self):
        claim = self.k(
            "get", "pvc", "witness", "-n", APP, "-o", "json", "--ignore-not-found"
        ).stdout
        pv = json.loads(claim)["spec"].get("volumeName") if claim.strip() else None
        namespaces = (APP, NS, OTHER)
        self.k(
            "delete",
            "namespace",
            *namespaces,
            "--ignore-not-found",
            "--wait=true",
            "--timeout=120s",
            timeout=130,
        )
        require(
            not self.k(
                "get", "namespace", *namespaces, "--ignore-not-found", "-o", "name"
            ).stdout.strip(),
            "episode namespace remains after deletion",
        )
        if pv:
            self.until(
                lambda: (
                    not self.k("get", "pv", pv, "--ignore-not-found", "-o", "name").stdout.strip()
                ),
                90,
            )
        database_created = DB in self.databases
        if database_created:
            self.drop(DB)
            self.sql("DROP ROLE IF EXISTS j2_exporter;")
        return {
            "namespaces_absent": list(namespaces),
            "witness_pv_absent": pv,
            "database_absent": DB if database_created else None,
        }

    def wait_for_departed_consumers(self):
        def departed():
            listing = self.kafka("consumer-groups", "--list").stdout.splitlines()
            for group in TOPICS.keys() & set(listing):
                result = self.kafka("consumer-groups", "--describe", "--state", "--group", group)
                states = [
                    line.split()[-2:]
                    for line in result.stdout.splitlines()
                    if line.split()[:1] == [group]
                ]
                if states != [["Empty", "0"]] and states != [["Dead", "0"]]:
                    return False
            return True

        self.until(departed, 45)

    def fresh_episode(self, name, addresses, exporter, previous):
        self.observer = None
        self.output = self.evidence / name
        self.output.mkdir()
        if previous is not None:
            write(self.output / "previous-cleanup.json", self.remove_episode())
            self.wait_for_departed_consumers()
            self.reset_transport()
            shutil.rmtree(self.scratch / "package-kind")
        topic_ids = self.transport("before-workloads", empty=True)
        require(
            application_groups_absent(
                read(self.output / "before-workloads-transport.json")["groups"]
            ),
            "prior Kafka consumer state",
        )
        apply_profile(self, addresses, exporter, APP, DB, OTHER if previous is None else None)
        pods, _ = self.pods_and_witness(empty=True)
        self.assert_profile(pods, exporter)
        self.groups(empty=True)
        require(self.transport("after-readiness", empty=True) == topic_ids, "Kafka topic changed")
        settings = self.sql(
            "SHOW fsync; SHOW synchronous_commit; SHOW full_page_writes; SHOW max_connections;", DB
        ).splitlines()
        selects = ",".join(f"(SELECT count(*) FROM engine.{table})" for table in DB_TABLES)
        counts = dict(
            zip(DB_TABLES, map(int, self.sql(f"SELECT {selects};", DB).split("|")), strict=True)
        )
        partitions = list(
            map(
                int,
                self.sql(
                    "SELECT partition_id FROM engine.partition_leases ORDER BY partition_id;", DB
                ).splitlines(),
            )
        )
        write(
            self.output / "fresh-db.json",
            {
                "settings": settings,
                "counts": counts,
                "engine_partitions": partitions,
            },
        )
        require(
            not any(counts.values())
            and partitions == list(range(16))
            and settings == ["on", "on", "on", "100"],
            "fresh database, ownership partitions or PostgreSQL settings differ",
        )
        storage, volume = self.storage()
        identity = {
            "namespaces": {ns: self.kube("namespace", ns)["metadata"]["uid"] for ns in (APP, NS)},
            "database_oid": int(self.sql(f"SELECT oid FROM pg_database WHERE datname='{DB}';")),
            "witness_pvc": storage["metadata"]["uid"],
            "witness_pv": volume["metadata"]["uid"],
            "pods": {p["metadata"]["name"]: p["metadata"]["uid"] for p in pods},
            "topics": topic_ids,
        }
        write(self.output / "identity.json", identity)
        verify_new_identity(previous, identity)
        return identity

    def load(self, name, *, partial):
        run_id = f"j2-reset-{os.environ['GITHUB_RUN_ID']}-{os.environ['GITHUB_RUN_ATTEMPT']}-{name}"
        seed = int.from_bytes(hashlib.sha256(run_id.encode()).digest()[:4], "big") >> 1
        count = INTERRUPTED_COUNT if partial else SEED_COUNT
        self.observer = self.pod(APP, "observer")["metadata"]["name"]
        args = ["-n", APP, "exec", self.observer, "--"]
        loadgen = [
            "/loadgen",
            "-namespace",
            APP,
            "-url",
            "http://api:8080",
            "-token-file",
            "/identity/token",
            "-run-id",
            run_id,
            "-seed",
            str(seed),
            "-arm",
            "short",
            "-count",
            str(count),
            "-rate",
            "1",
            "-concurrency",
            "16",
            "-output",
            ARRIVALS,
        ]
        launcher = ["python", "-c", INTERRUPT] if partial else []
        result = self.k(*args, *launcher, *loadgen, allowed=True, timeout=60)
        (self.output / "submission-events.jsonl").write_text(result.stdout, encoding="utf-8")
        raw = self.k(*args, "cat", ARRIVALS, allowed=True, timeout=10)
        (self.output / "arrivals.json").write_text(raw.stdout, encoding="utf-8")
        write(
            self.output / "sender-exit.json",
            {
                "exec_exit": result.returncode,
                "journal_read_exit": raw.returncode,
            },
        )
        require(result.returncode == raw.returncode == 0, "reset fixture sender or journal failed")
        plan = read(self.output / "arrivals.json")
        require(plan["run_id"] == run_id and plan["offered"] == count, "fixture journal differs")
        ids = {row["workflow_id"] for row in plan["rows"]}
        require(len(plan["rows"]) == len(ids) == count, "fixture journal has duplicate identities")
        if partial:
            events = [json.loads(line) for line in result.stdout.splitlines()]
            require(
                [e["event"] for e in events] == ["started", "committed", "stopped"]
                and 0 < len(events[1]["ids"]) < count
                and set(events[1]["ids"]) <= ids
                and events[2]["exit_code"] == -signal.SIGTERM
                and plan["status"] == "IN_PROGRESS",
                "controlled stop did not retain a durable partial submission",
            )
            committed = set(
                self.sql("SELECT workflow_id FROM engine.workflow_executions;", DB).splitlines()
            )
            write(self.output / "admissions-after-stop.json", sorted(committed))
            require(
                set(events[1]["ids"]) <= committed <= ids and len(committed) < count,
                "committed identities differ after controlled stop",
            )
            raise InjectedStop(run_id)
        require(
            plan["status"] == "PASS" and all(row["outcome"] == "ACCEPTED" for row in plan["rows"]),
            "fixture load was incomplete",
        )
        self.until(
            lambda: (
                set(
                    self.sql(
                        "SELECT workflow_id FROM engine.workflow_executions "
                        "WHERE state='SUCCEEDED';",
                        DB,
                    ).splitlines()
                )
                == ids
            ),
            60,
        )
        _, starts = self.pods_and_witness(empty=False)
        require(starts >= count, "fixture left no complete invocation evidence")
        self.groups(empty=False)
        self.transport("after-population", empty=False)
        write(self.output / "seeded-db.json", json.loads(self.sql(QUIET_SQL, DB)))

    def retain_stop(self):
        self.deadline = time.monotonic() + CLOSEOUT_SECONDS
        if self.cluster_started:
            journal = self.output / "arrivals.json"
            if self.observer and (not journal.exists() or not journal.stat().st_size):
                raw = self.k(
                    "-n",
                    APP,
                    "exec",
                    self.observer,
                    "--",
                    "cat",
                    ARRIVALS,
                    allowed=True,
                    timeout=10,
                )
                if raw.stdout:
                    journal.write_text(raw.stdout, encoding="utf-8")
            for ns in (APP, NS):
                probe = self.k(
                    "-n", ns, "get", "pods,events", "-o", "json", allowed=True, timeout=15
                )
                (self.output / f"{ns}-at-stop.json").write_text(probe.stdout, encoding="utf-8")
            if DB in self.databases:
                write(
                    self.output / "db-at-stop.json",
                    {
                        "states": self.sql(
                            "SELECT workflow_id,state FROM engine.workflow_executions "
                            "ORDER BY workflow_id;",
                            DB,
                        ).splitlines(),
                        "counts": json.loads(self.sql(QUIET_SQL, DB)),
                    },
                )


def hosted_dispatch():
    require(
        os.environ.get("GITHUB_ACTIONS") == "true"
        and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted"
        and os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
        and read(Path(os.environ["GITHUB_EVENT_PATH"]))["repository"]["private"] is False,
        "J2 harness requires public GitHub-hosted workflow_dispatch",
    )


def rehearse(driver, layouts, exporter_layout):
    with driver.stage("bootstrap"):
        addresses, exporter = start_profile(driver, layouts, exporter_layout)
    previous = None
    for name in ("grid-seed", "grid-reset", "soak-setup", "partial-failure"):
        with driver.stage(f"fresh-{name}"):
            previous = driver.fresh_episode(name, addresses, exporter, previous)
        if name == "grid-seed":
            with driver.stage("calico-probes"):
                check_network(driver, APP)
        if name != "soak-setup":
            with driver.stage(f"populate-{name}"):
                driver.load(name, partial=name == "partial-failure")
    raise ValueError("expected partial submission stop did not occur")


def closeout(driver):
    errors = {}
    try:
        driver.retain_stop()
    except Exception as error:
        errors["diagnostics"] = str(error)
    if driver.cluster_started:
        try:
            write(driver.output / "episode-cleanup.json", driver.remove_episode())
        except Exception as error:
            errors["episode_cleanup"] = str(error)
    driver.output = driver.evidence
    try:
        cleanup = driver.cleanup()
    except Exception as error:
        errors["cluster_cleanup"] = str(error)
        cleanup = {}
    write(
        driver.evidence / "cleanup.json",
        {
            **cleanup,
            "status": "FAIL" if errors else cleanup.get("status"),
            "errors": errors,
        },
    )
    return not errors and cleanup.get("status") == "PASS"


def run(driver, layouts, exporter_layout):
    status = {
        "status": "FAIL",
        "scope": "unmeasured J2 harness reset",
        "profile": PROFILE,
        "run_id": os.environ["GITHUB_RUN_ID"],
        "run_attempt": os.environ["GITHUB_RUN_ATTEMPT"],
        "head": os.environ["GITHUB_SHA"],
    }
    expected_stop = False
    try:
        require(
            driver.command(["git", "rev-parse", "HEAD"]).stdout.strip() == status["head"],
            "checkout differs from dispatch head",
        )
        write(
            driver.evidence / "runner.json",
            {
                "job": os.environ["GITHUB_JOB"],
                "image": os.environ.get("ImageVersion"),  # noqa: SIM112 (runner variable)
                "kernel": os.uname().release,
                "cpu_ids": sorted(os.sched_getaffinity(0)),
                "cgroup": runner_cgroup(),
                "recorded_at": datetime.now(UTC).isoformat(),
                "pins": {
                    p.name: file_record(p)
                    for p in (
                        J2 / "images.lock.json",
                        J2 / "exporter.lock.json",
                        kind.K8S / "tools.lock.json",
                    )
                },
            },
        )
        rehearse(driver, layouts, exporter_layout)
    except InjectedStop as stop:
        expected_stop = True
        status.update(stop={"classification": "engineering_injected_stop", "run_id": str(stop)})
    except Exception as error:
        stage = driver.failed_stage or "setup"
        classification = (
            "integrity_stop" if stage == "bootstrap" or stage.startswith("fresh-") else "invalid"
        )
        status.update(stop={"classification": classification, "stage": stage, "reason": str(error)})
    finally:
        cleaned = closeout(driver)
        status["cleanup_verified"] = cleaned
        if expected_stop and cleaned:
            status["status"] = "PASS"
        write(driver.evidence / "status.json", status)
    return 0 if status["status"] == "PASS" else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("evidence", "scratch", "layouts", "exporter_layout"):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    hosted_dispatch()
    args.evidence.mkdir(parents=True, exist_ok=True)
    args.scratch.mkdir()
    kind.CLUSTER, kind.LABEL = "j2-harness", "j2.harness=owned"
    return run(Harness(args.evidence, args.scratch), args.layouts, args.exporter_layout)


if __name__ == "__main__":
    raise SystemExit(main())
