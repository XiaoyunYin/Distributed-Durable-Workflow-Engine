"""K4 host orchestration, composed with the unchanged K3 cluster Driver."""

from __future__ import annotations

import argparse
import copy
import hashlib
import io
import json
import math
import os
import secrets
import shutil
import tarfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path

from dur053_k3 import LABEL, LOCK, ROOT, Driver, download, github, job_identity_and_history, save
from dur053_k3_policy import artifact_hashes, committed_ledger, fresh_output, ledger_check, require
from dur053_k4 import (
    CEILING,
    LEDGER,
    admit,
    assignment,
    episode_identity,
    frozen,
    parameters,
    registry,
)
from dur053_k4_collectors import (
    Streams,
    apply_process_evidence,
    callback_rows,
    durable_claims,
    export_seals,
    history_kind,
    journal_rows,
    reconciled,
    session_rows,
    structured_log,
    utc,
)
from dur053_k4_evidence import FILES, safety_evidence, summaries
from dur053_k4_evidence import episode as derive_episode
from dur053_k4_interventions import (
    ENGINE_ROLES,
    Interventions,
    empty,
    intervention_outcome,
    ready,
    require_term_evidence,
)
from dur053_k4_schema import CONFIRMATION, EPISODE, JOB, SCHEMAS, load, validate
from dur053_pilot_evidence import ci_proof
from dur053_record_k4 import inventory

SMOKE_CELLS = (
    "short-core-0-pair-0-baseline",
    "short-core-0-pair-0-treatment",
    "short-core-2-pair-0-baseline",
    "short-core-2-pair-0-treatment",
    "short-core-4-pair-0-baseline",
    "short-core-6-pair-0-baseline",
    "short-core-1-pair-0-baseline",
    "short-coverage-3-0-treatment",
    "long-core-0-pair-0-treatment",
)
REGISTERED_SMOKE = "short-core-2-pair-0-treatment"
SMOKE_WARMUP = 5
SMOKE_WINDOWS = {"short": 12, "long": 50}
SMOKE_LIVE_RESERVATION = 4800


def episode_parameters(f, cell, *, engineering):
    p = parameters(f, cell["duration"])
    if engineering and cell["episode"] != REGISTERED_SMOKE:
        p.update(warmup=SMOKE_WARMUP, W=SMOKE_WINDOWS[cell["duration"]])
    return p


def stamp(at):
    return datetime.fromtimestamp(at, UTC).isoformat().replace("+00:00", "Z")


def reconciliation_round(snapshot_read, witness_read, last, quiet):
    """Bracket remote reconciliation reads; t1 bounds everything returned by them."""
    t0 = time.time()
    snapshot = snapshot_read()
    witness = witness_read()
    t1 = time.time()
    fingerprint = hashlib.sha256(json.dumps(witness, sort_keys=True).encode()).hexdigest()
    changed = fingerprint != last
    if changed:
        quiet = t1
        last = fingerprint
    return snapshot, quiet, last, t0, t1, changed


def locked_document(path, value):
    validate(value, SCHEMAS[value["schema"]])
    save(path, value)


def chaos_values():
    values = dict(
        dashboard=dict(create=False),
        dnsServer=dict(create=False),
        prometheus=dict(create=False),
        bpfki=dict(create=False),
        chaosDlv=dict(enable=False),
        enableProfiling=False,
        enableCtrlServer=False,
        controllerManager=dict(
            replicaCount=1,
            imagePullPolicy="Never",
            podChaos=dict(
                podFailure=dict(pauseImage=LOCK["images"]["ghcr.io/chaos-mesh/chaos-mesh:v2.7.2"])
            ),
        ),
        chaosDaemon=dict(
            runtime="containerd",
            socketPath="/run/containerd/containerd.sock",
            imagePullPolicy="Never",
            service=dict(scrape=dict(enabled=False)),
        ),
    )
    for key, image in (("controllerManager", "chaos-mesh"), ("chaosDaemon", "chaos-daemon")):
        digest = LOCK["images"]["ghcr.io/chaos-mesh/" + image + ":v2.7.2"].split("@")[1]
        values[key]["image"] = dict(
            registry="ghcr.io", repository="chaos-mesh/" + image, tag="v2.7.2@" + digest
        )
    return values


def validate_chaos_render(objects):
    found = set()
    allowed = {LOCK["images"][k] for k in LOCK["addons"]["chaos"]["images"] if "dashboard" not in k}

    def walk(value):
        if isinstance(value, dict):
            if isinstance(value.get("image"), str):
                require(
                    value["image"] in allowed and value.get("imagePullPolicy") == "Never",
                    "k4-chaos-unlocked-image",
                )
                found.add(value["image"])
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(objects)
    require(found == allowed, "k4-chaos-image-inventory")
    require(
        {o["kind"] for o in objects if o.get("kind") in {"Deployment", "DaemonSet"}}
        == {"Deployment", "DaemonSet"},
        "k4-chaos-components",
    )
    return found


def lineage_identity(configs):
    for target in ("runtime", "worker"):
        a, prime = (copy.deepcopy(configs[lineage][target]) for lineage in ("a", "a-prime"))
        for value, lineage in ((a, "a"), (prime, "a-prime")):
            require(
                value["Config"]["Labels"].pop("dur053.k4.lineage") == lineage,
                "k4-image-lineage-label",
            )
        require(
            a["Config"] == prime["Config"] and a["RootFS"] == prime["RootFS"],
            "k4-label-only-images",
        )


class K4Driver:
    def __init__(self, output, scratch, engineering=False):
        self.base = Driver(output, scratch)
        self.base.live_reservation = SMOKE_LIVE_RESERVATION if engineering else CEILING
        self.output, self.scratch, self.engineering = output, scratch, engineering
        self.images = {}
        self.proof = {}
        self.packages = {}
        self.namespaces = set()
        self.samples = []
        self.collector = None
        self.observer_stop = threading.Event()
        self.sample_error = []
        self.source = ""
        self.fatal = False
        original_command = self.base.command
        journal_lock = threading.Lock()

        def command(args, **kwargs):
            if "psql" in args and str(kwargs.get("data", "")).startswith("/* K4 observer */"):
                kwargs["timeout"] = min(kwargs.get("timeout", 180), 5)
            ident = secrets.token_hex(8)
            redacted = [
                str(arg)
                .replace(self.base.password, "<redacted>")
                .replace(self.base.demand_token, "<redacted>")
                for arg in args
            ]

            def record(**fields):
                with (
                    journal_lock,
                    (self.output / "commands.jsonl").open(
                        "a", encoding="utf-8", newline="\n"
                    ) as handle,
                ):
                    handle.write(json.dumps(dict(id=ident, at=time.time(), **fields)) + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())

            record(event="start", args=redacted, timeout=kwargs.get("timeout", 180))
            try:
                result = original_command(args, **kwargs)
                record(event="finish", exit_code=result.returncode)
                return result
            except Exception as error:
                record(event="error", error=str(error))
                raise

        self.base.command = command

    def install(self):
        self.base.install()
        tool = LOCK["tools"]["helm"]
        data = download(tool["url"], tool["sha256"])
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
            data = archive.extractfile(tool["member"]).read()
        path = self.scratch / "bin/helm"
        path.write_bytes(data)
        path.chmod(0o755)
        self.base.command(["helm", "version"])
        chart = LOCK["artifacts"]["chaos-chart"]
        path = self.scratch / "chaos-mesh.tgz"
        path.write_bytes(download(chart["url"], chart["sha256"]))
        save(self.output / "chaos-values.json", chaos_values())
        rendered = self.base.command(
            [
                "helm",
                "template",
                "chaos-mesh",
                str(path),
                "--namespace",
                "chaos-mesh",
                "--values",
                str(self.output / "chaos-values.json"),
            ]
        ).stdout
        import yaml

        objects = [o for o in yaml.safe_load_all(rendered) if o]
        self.base.addon_images.update(validate_chaos_render(objects))
        (self.output / "chaos-mesh.yaml").write_text(rendered, encoding="utf-8", newline="\n")
        crd = LOCK["addons"]["chaos"]
        (self.output / "chaos-crds.yaml").write_bytes(download(crd["url"], crd["sha256"]))

    def build(self):
        # One compilation per target. The two lineage variants only add a label.
        self.base.env["GOFLAGS"] = "-buildvcs=false"
        self.base.build()
        original = dict(self.base.images)
        binary = {}
        for lineage in ("a", "a-prime"):
            self.images[lineage] = {}
            for target in ("runtime", "worker"):
                tag = f"localhost:5001/dur053-{target}:{lineage}"
                self.base.command(
                    ["docker", "build", "--provenance=false", "-t", tag, "-"],
                    data="FROM " + original[target] + "\nLABEL dur053.k4.lineage=" + lineage + "\n",
                )
                self.base.command(["docker", "push", tag])
                refs = json.loads(
                    self.base.command(
                        ["docker", "inspect", "--format", "{{json .RepoDigests}}", tag]
                    ).stdout
                )
                require(len(refs) == 1, "k4-image-digest")
                self.images[lineage][target] = refs[0]
                container = self.base.command(
                    ["docker", "create", "--label", LABEL, tag]
                ).stdout.strip()
                self.base.created.add(container)
                try:
                    member = "/runtime" if target == "runtime" else "/app/python"
                    destination = self.scratch / (target + "-" + lineage)
                    self.base.command(["docker", "cp", container + ":" + member, str(destination)])
                    raw = (
                        destination.read_bytes()
                        if destination.is_file()
                        else b"".join(
                            p.relative_to(destination).as_posix().encode() + p.read_bytes()
                            for p in sorted(destination.rglob("*"))
                            if p.is_file()
                        )
                    )
                    binary[target + "_" + lineage.replace("-", "_")] = hashlib.sha256(
                        raw
                    ).hexdigest()
                finally:
                    self.base.command(["docker", "rm", container])
                    self.base.created.discard(container)
        self.proof = {
            k: binary[k] for k in ("runtime_a", "runtime_a_prime", "worker_a", "worker_a_prime")
        }
        require(
            self.proof["runtime_a"] == self.proof["runtime_a_prime"]
            and self.proof["worker_a"] == self.proof["worker_a_prime"],
            "k4-binary-identity",
        )
        # cluster() preloads all variants, with unique keys to avoid losing A.
        self.base.images = {
            lineage + "-" + target: ref
            for lineage, variants in self.images.items()
            for target, ref in variants.items()
        }
        configs = {}
        for lineage, variants in self.images.items():
            configs[lineage] = {}
            for target, ref in variants.items():
                config = json.loads(self.base.command(["docker", "inspect", ref]).stdout)[0]
                configs[lineage][target] = config
        lineage_identity(configs)
        save(self.output / "image-configs.json", configs)
        save(
            self.output / "images.json",
            dict(images=self.images, binary_proof=self.proof, buildvcs=False),
        )

    def cluster(self):
        for name, tool in LOCK["addons"].items():
            if tool["installed_in_k3a"]:
                self.base.prepare_addon(name)
        self.base.preload_addons()
        addresses = self.base.cluster()
        self.base.apply(dict(apiVersion="v1", kind="Namespace", metadata=dict(name="chaos-mesh")))
        self.base.k(
            "apply", "--server-side", "-f", str(self.output / "chaos-crds.yaml"), timeout=180
        )
        self.base.k("wait", "crd", "--all", "--for=condition=Established", "--timeout=120s")
        self.base.k("apply", "-f", str(self.output / "chaos-mesh.yaml"), timeout=180)
        for target in ("deployment/chaos-controller-manager", "daemonset/chaos-daemon"):
            self.base.k(
                "-n", "chaos-mesh", "rollout", "status", target, "--timeout=180s", timeout=190
            )
        self.validate_backend()
        self.base.images = dict(self.images["a"])
        for arm in ("kind", "kind-baseline"):
            _, self.packages[arm] = self.base.render(arm, addresses)
        return addresses

    def validate_backend(self):
        client = json.loads(self.base.k("version", "--client", "-o", "json").stdout)
        require(client["clientVersion"]["gitVersion"] == "v1.32.2", "k4-kubectl-version")
        get_help = self.base.k("get", "--help").stdout
        ctr_version = self.base.command(
            ["docker", "exec", "dur053-k3a-control-plane", "ctr", "version"]
        ).stdout
        ctr_help = self.base.command(
            ["docker", "exec", "dur053-k3a-control-plane", "ctr", "events", "--help"]
        ).stdout
        require("--raw" in get_help and "--filter" not in ctr_help, "k4-stream-cli-contract")
        save(
            self.output / "stream-cli-contract.json",
            dict(
                kubectl=client, get_help=get_help, ctr_version=ctr_version, ctr_events_help=ctr_help
            ),
        )
        ns = "dur053-k4-backend"
        self.base.apply(dict(apiVersion="v1", kind="Namespace", metadata=dict(name=ns)))
        self.namespaces.add(ns)
        self.base.apply(
            dict(
                apiVersion="v1",
                kind="Pod",
                metadata=dict(name="scratch", namespace=ns),
                spec=dict(
                    restartPolicy="Always",
                    containers=[
                        dict(
                            name="scratch",
                            image=self.images["a"]["worker"],
                            imagePullPolicy="Never",
                            command=["python", "-c", "import time; time.sleep(86400)"],
                        )
                    ],
                ),
            )
        )
        self.base.k("-n", ns, "wait", "pod/scratch", "--for=condition=Ready", "--timeout=120s")
        adapter = Interventions(self.base, ns, self.output, self.images, self.proof, self.source)
        before = adapter.get("pod", "scratch")
        streams = Streams(self.base, ns, self.output / "backend-streams")
        try:
            streams.start_cri()
            streams.start_watch("pods")
            streams.start_watch("endpoints")
            # Force a server event for both watches; list rows do not count.
            self.base.k("-n", ns, "label", "pod/scratch", "k4-self-check=true")
            self.base.apply(
                dict(
                    apiVersion="v1",
                    kind="Endpoints",
                    metadata=dict(name="scratch", namespace=ns),
                    subsets=[],
                )
            )
            # Container creation produces a CRI event before the kill. This
            # self-check therefore catches CLI failures before fault injection.
            self.base.k(
                "-n",
                ns,
                "run",
                "stream-check",
                "--restart=Never",
                "--image=" + self.images["a"]["worker"],
                "--image-pull-policy=Never",
                "--command",
                "--",
                "python",
                "-c",
                "print('stream-check')",
            )
            check = streams.self_check()
            after = adapter.kill(before, name="backend-validation")
            cid = before["status"]["containerStatuses"][0]["containerID"].rsplit("/", 1)[-1]
            self.base.until(lambda: any(e["container_id"] == cid for e in streams.cri_exits), 20)
            streams.check()
            exits = [e for e in streams.cri_exits if e["container_id"] == cid]
            require(
                exits
                and any(
                    e["exit_status"] == 137 and e["exit_status_source"] == "explicit" for e in exits
                ),
                "k4-backend-cri-exit-missing",
            )
            save(
                self.output / "chaos-validation.json",
                dict(
                    status="PASS",
                    before=before,
                    after=after,
                    stream_self_check=check,
                    cri_task_exits=exits,
                ),
            )
        finally:
            streams.close()
        self.base.k("delete", "namespace", ns, "--wait=true", "--timeout=60s")
        self.namespaces.discard(ns)

    def setup(self, cell, addresses, *, campaign=True):
        require(not self.fatal, "k4-observer-stop-before-next-episode")
        ns = "dur053-k4-" + secrets.token_hex(6)
        db = ns.replace("-", "_")
        arm = "kind-baseline" if cell["arm"] == "baseline" else "kind"
        original = "dur053-baseline" if arm == "kind-baseline" else "dur053-kind"
        objects = json.loads(json.dumps(copy.deepcopy(self.packages[arm])).replace(original, ns))
        objects = [o for o in objects if o["kind"] not in {"ScaledObject", "TriggerAuthentication"}]
        for o in objects:
            if o["kind"] != "Deployment":
                continue
            role = o["metadata"]["name"]
            pod = o["spec"]["template"]["spec"]
            if role == "worker" and "scale-down" in cell["event"]:
                o["spec"]["replicas"] = 4
            if role == "observer":
                pod["volumes"].append(
                    dict(name="witness", persistentVolumeClaim=dict(claimName="witness"))
                )
                pod["containers"][0]["volumeMounts"].append(
                    dict(name="witness", mountPath="/witness", readOnly=False)
                )
            if role not in ENGINE_ROLES:
                continue
            if role != "worker":
                pod["volumes"].append(
                    dict(name="k4-controls", persistentVolumeClaim=dict(claimName="witness"))
                )
                pod["containers"][0].setdefault("volumeMounts", []).append(
                    dict(name="k4-controls", mountPath="/k4", readOnly=True)
                )
            for c in pod["containers"]:
                env = {e["name"]: e for e in c.get("env", [])}
                if self.episode_output.name.startswith("overhead-"):
                    env["DUR053_K4_COMMIT_TIMING"] = dict(name="DUR053_K4_COMMIT_TIMING", value="1")
                env["RUNTIME_NAMESPACE"] = dict(name="RUNTIME_NAMESPACE", value=ns)
                env["DUR053_K4_HOLD_FILE"] = dict(
                    name="DUR053_K4_HOLD_FILE",
                    value=("/witness" if role == "worker" else "/k4")
                    + "/control/"
                    + role
                    + ".hold",
                )
                env["POD_UID"] = dict(
                    name="POD_UID", valueFrom=dict(fieldRef=dict(fieldPath="metadata.uid"))
                )
                env["CONTAINER_ID"] = dict(
                    name="CONTAINER_ID", valueFrom=dict(fieldRef=dict(fieldPath="metadata.uid"))
                )
                env["DUR053_K4_CAMPAIGN"] = dict(
                    name="DUR053_K4_CAMPAIGN", value="1" if campaign else "0"
                )
                if role == "worker":
                    env["DUR048_ACTIVITY_DELAY_MS"] = dict(
                        name="DUR048_ACTIVITY_DELAY_MS",
                        value="45000" if cell["duration"] == "long" else "100",
                    )
                if role == "scheduler" and cell["event"].startswith("scheduler"):
                    env["RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS"] = dict(
                        name="RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS", value="1"
                    )
                c["env"] = list(env.values())
        self.namespaces.add(ns)
        self.base.databases.add(db)
        self.base.sql('CREATE DATABASE "' + db + '";')
        self.base.apply(dict(apiVersion="v1", kind="Namespace", metadata=dict(name=ns)))
        self.base.apply(
            dict(
                apiVersion="v1",
                kind="Secret",
                metadata=dict(name="campaign-db", namespace=ns),
                stringData=dict(
                    url=f"postgresql://durable:{self.base.password}@postgres:5432/{db}?sslmode=disable"
                ),
            )
        )
        self.base.apply(
            dict(
                apiVersion="v1",
                kind="Secret",
                metadata=dict(name="campaign-demand", namespace=ns),
                stringData=dict(token=self.base.demand_token),
            )
        )
        self.base.apply(
            dict(
                apiVersion="v1",
                kind="List",
                items=[
                    o
                    for o in objects
                    if o["kind"] not in {"Deployment", "Job", "PodDisruptionBudget"}
                ],
            )
        )
        for name, placeholder, port in (
            ("postgres", "192.0.2.1", 5432),
            ("kafka", "192.0.2.2", 19092),
        ):
            self.base.apply(
                dict(
                    apiVersion="v1",
                    kind="Endpoints",
                    metadata=dict(name=name, namespace=ns),
                    subsets=[
                        dict(
                            addresses=[dict(ip=addresses[placeholder])],
                            ports=[dict(name="tcp", port=port, protocol="TCP")],
                        )
                    ],
                )
            )
        for name in ("migrate", "compatibility"):
            self.base.apply(
                next(o for o in objects if o["kind"] == "Job" and o["metadata"]["name"] == name)
            )
            self.base.k(
                "-n",
                ns,
                "wait",
                "--for=condition=complete",
                "job/" + name,
                "--timeout=180s",
                timeout=190,
            )
        self.collector = Streams(self.base, ns, self.episode_output / "streams")
        self.collector.start_cri()
        self.collector.start_watch("pods")
        self.collector.start_watch("endpoints")
        self.base.apply(
            dict(
                apiVersion="v1",
                kind="List",
                items=[o for o in objects if o["kind"] in {"Deployment", "PodDisruptionBudget"}],
            )
        )
        save(self.episode_output / "applied-objects.json", objects)
        for role in sorted(ENGINE_ROLES | {"observer"}):
            self.base.k(
                "-n", ns, "rollout", "status", "deployment/" + role, "--timeout=180s", timeout=190
            )
        return ns, db

    def witness_raw(self, ns):
        text = self.base.python(
            ns,
            "observer",
            "import base64,json,pathlib; files={}\n"
            "for p in pathlib.Path('/witness').rglob('*.inventory.json'):\n"
            " raw=p.read_bytes(); seal=json.loads(raw)\n"
            " files[str(p)]=base64.b64encode(raw).decode()\n"
            " for entry in seal['streams'].values():\n"
            "  child=p.parent/entry['file']\n"
            "  if child.exists(): files[str(child)]=base64.b64encode(child.read_bytes()).decode()\n"
            "print(json.dumps(files))",
            record=False,
        )
        value = json.loads(text)
        save(self.episode_output / "witness-raw.json", value)
        return value

    def snapshot(self, db, *, light=False):
        # One read-only MVCC snapshot; no raw claim tokens leave the DB.
        queries = {
            "workflows": "SELECT * FROM engine.workflow_executions",
            "attempts": "SELECT workflow_id,node_id,iteration,attempt_number,state,"
            "worker_id,worker_request_id,"
            "created_at,updated_at,"
            "encode(sha256(convert_to(COALESCE(claim_token::text,''),'UTF8')),'hex') "
            "AS token_hash FROM engine.activity_attempts",
            "history": 'SELECT h.*, k."order",k.partition,k.owner,k.epoch,k.session '
            "FROM engine.transition_history h JOIN engine.k4_history_order k USING(transition_id)",
            "leases": "SELECT * FROM engine.k4_lease_audit",
            "claim_audit": 'SELECT * FROM engine.k4_claim_audit ORDER BY "order"',
            "sessions": "SELECT DISTINCT session_id AS session,pod_uid,container_id "
            "FROM engine.k4_audit",
            "connections": "SELECT count(*) AS count FROM pg_stat_activity "
            "WHERE backend_type='client backend'",
            "demand": "SELECT COALESCE(GREATEST(0,extract(epoch FROM "
            "statement_timestamp()-min(a.created_at))),0) AS oldest "
            "FROM engine.activity_attempts a JOIN engine.workflow_executions w USING(workflow_id) "
            "JOIN engine.node_instances n USING(workflow_id,node_id,iteration) "
            "WHERE a.is_current AND a.state='DISPATCHABLE' AND w.state='WAITING_ACTIVITY' "
            "AND n.current_attempt_number=a.attempt_number",
        }
        if light:
            # The one-second observer never retains repeated lease/history
            # exports. Those immutable rows are exported once at reconciliation.
            queries = {
                k: v
                for k, v in queries.items()
                if k in {"workflows", "attempts", "connections", "demand"}
            }
            queries["workflows"] = (
                "SELECT workflow_id,state,updated_at FROM engine.workflow_executions"
            )
            queries["attempts"] += (
                " WHERE is_current AND state='CLAIMED' "
                "AND heartbeat_deadline > statement_timestamp()"
            )
        items = ",".join(
            "'" + name + "',COALESCE((SELECT jsonb_agg(t) FROM (" + query + ") t),'[]'::jsonb)"
            for name, query in queries.items()
        )
        t0 = time.time()
        raw = self.base.sql(
            "/* K4 observer */ SET statement_timeout='2s'; SET lock_timeout='500ms'; "
            "BEGIN READ ONLY; SELECT jsonb_build_object("
            + items
            + ",'clock',extract(epoch FROM clock_timestamp())); COMMIT;",
            db,
        )
        t1 = time.time()
        lines = [s for s in raw.splitlines() if s.startswith("{")]
        require(len(lines) == 1, "k4-snapshot-framing")
        doc = json.loads(lines[0])
        require(doc["connections"][0]["count"] <= 92, "k4-observed-connection-budget")
        doc["clock_offset"] = (t0 + t1) / 2 - doc["clock"]
        doc["bracket"] = dict(start=t0, end=t1, width=t1 - t0)
        save(self.episode_output / "database.json", doc)
        return doc

    def start_samples(self, ns, db):
        self.samples = []
        self.sample_error = []
        self.observer_stop.clear()

        def sample():
            tick = time.monotonic()
            last = time.time()
            path = self.episode_output / "observer-raw.jsonl"
            with path.open("a", encoding="utf-8", newline="\n") as handle:
                while not self.observer_stop.is_set():
                    start = time.time()
                    try:
                        data = self.snapshot(db, light=True)
                        pods = json.loads(
                            self.base.k("-n", ns, "get", "pods", "-o", "json", record=False).stdout
                        )["items"]
                        row = dict(
                            # DB statement/history times and this snapshot clock
                            # share a domain. The host bracket stays in data.
                            at=data["clock"],
                            gap=max(0, start - last),
                            width=time.time() - start,
                            data=data,
                            pods=pods,
                            error="",
                        )
                    except Exception as error:
                        row = dict(
                            at=start,
                            gap=max(0, start - last),
                            width=time.time() - start,
                            data=None,
                            pods=[],
                            error=str(error),
                        )
                        self.sample_error.append(str(error))
                    last = start
                    self.samples.append(row)
                    handle.write(json.dumps(row) + "\n")
                    handle.flush()
                    tick += 1
                    self.observer_stop.wait(max(0, tick - time.monotonic()))

        self.sampler = threading.Thread(target=sample, daemon=True)
        self.sampler.start()

    def check_observation(self):
        self.collector.check()
        if self.sample_error:
            self.fatal = True
        require(not self.sample_error, "k4-observer-loss: " + "; ".join(self.sample_error))

    def stop_samples(self):
        self.observer_stop.set()
        self.sampler.join(timeout=30)
        require(not self.sampler.is_alive(), "k4-observer-stop-timeout")
        require(not self.sample_error, "k4-observer-loss: " + "; ".join(self.sample_error))

    def load(self, ns, cell, p, start):
        count = math.ceil((p["warmup"] + p["confirmation"] + p["W"] + 180) * p["rate"])
        return self.base.k(
            "-n",
            ns,
            "exec",
            self.base.pod(ns, "observer")["metadata"]["name"],
            "--",
            "/loadgen",
            "-k4",
            "-k4-start",
            stamp(start),
            "-k4-stop",
            "/evidence/k4-stop",
            "-url",
            "http://api:8080",
            "-token-file",
            "/identity/token",
            "-namespace",
            ns,
            "-run-id",
            "k4-" + secrets.token_hex(6),
            "-arm",
            cell["duration"],
            "-seed",
            str(cell["seed"]),
            "-count",
            str(count),
            "-rate",
            str(p["rate"]),
            "-output",
            "/evidence/k4-arrivals.jsonl",
            timeout=p["warmup"] + p["confirmation"] + p["W"] + 240,
            allowed=True,
        )

    def overlap(self, ns, role, now):
        sessions = self.collector.live.read()
        now = time.time()  # Live selection ends after the evidence read.
        if role == "worker":
            witness = self.witness_raw(ns)
            now = time.time()
            _, invocations, _ = export_seals(witness, sessions)
            active = [v for v in invocations if v["finish"] is None and v["start"] <= now]
            by = {s["session"]: s for s in sessions}
            return [
                dict(
                    role="worker",
                    session=v["session"],
                    pod_uid=by[v["session"]]["pod_uid"],
                    at=v["start"],
                    end=now,
                    acknowledgment="durable-witness:" + v["id"],
                    request="",
                    publication="",
                    workflows=[v["identity"]["workflow"]],
                    claim=v["identity"],
                    partition=None,
                    owner="",
                    epoch=None,
                )
                for v in active
            ]
        entries = {
            key: data
            for (entry_role, key), data in self.collector.live.open.items()
            if entry_role == role
        }
        by = {s["pod_uid"]: s for s in sessions}
        result = []
        for d in entries.values():
            if d["pod_uid"] not in by or d["boundary"] not in {
                "enter",
                "after_lease_acquired_hold_enter",
            }:
                continue
            scheduler = role == "scheduler"
            result.append(
                dict(
                    role=role,
                    session=d["session"],
                    pod_uid=d["pod_uid"],
                    at=utc(d["timestamp"]),
                    end=now,
                    acknowledgment=d["acquisition"] if scheduler else d["identity"],
                    request=d["identity"] if role == "api" else "",
                    publication=d["identity"] if role == "relay" else "",
                    workflows=d.get("workflows") or [],
                    claim=None,
                    partition=d["partition"] if scheduler else None,
                    owner=d["owner"] if scheduler else "",
                    epoch=d["epoch"] if scheduler else None,
                )
            )
        return result

    def confirmed_overlap(self, ns, role, deadline):
        self.observed_exited_overlaps = 0
        while time.time() <= deadline:
            self.check_observation()
            overlap = self.overlap(ns, role, time.time())
            if overlap:
                # Refresh immediately before apply: all selected acknowledgments
                # must still be open, including worker witness invocations.
                current = self.overlap(ns, role, time.time())
                keys = {(o["session"], o["acknowledgment"]) for o in current}
                if all((o["session"], o["acknowledgment"]) in keys for o in overlap):
                    require(time.time() <= deadline, "k4-harness-confirmation-deadline")
                    return overlap
                self.observed_exited_overlaps += sum(
                    (o["session"], o["acknowledgment"]) not in keys for o in overlap
                )
            time.sleep(0.05)
        return []

    def diagnostics(self):
        for ns in sorted(self.namespaces | {"chaos-mesh"}):
            result = self.base.k(
                "-n",
                ns,
                "get",
                "pods,deployments,replicasets,pdb,endpoints,events",
                "-o",
                "json",
                allowed=True,
                timeout=30,
            )
            (self.output / ("diagnostics-" + ns + ".json")).write_text(
                result.stdout, encoding="utf-8", newline="\n"
            )
            pods = self.base.k("-n", ns, "get", "pods", "-o", "json", allowed=True, timeout=15)
            if pods.returncode:
                continue
            for pod in json.loads(pods.stdout)["items"]:
                name = pod["metadata"]["name"]
                self.base.k("-n", ns, "describe", "pod", name, allowed=True, timeout=15)
                for previous in (False, True):
                    args = ["-n", ns, "logs", name, "--all-containers", "--timestamps"]
                    if previous:
                        args.append("--previous")
                    result = self.base.k(*args, allowed=True, timeout=15)
                    (
                        self.output
                        / (
                            "diagnostics-"
                            + ns
                            + "-"
                            + name
                            + ("-previous" if previous else "")
                            + ".log"
                        )
                    ).write_text(result.stdout + result.stderr, encoding="utf-8", newline="\n")
        self.base.k("get", "podchaos", "-A", "-o", "json", allowed=True, timeout=15)
        self.base.diagnostics()

    def cleanup(self):
        if self.collector:
            self.collector.close()
            self.collector = None
        started, self.base.live_start = self.base.live_start, None
        for ns in sorted(self.namespaces):
            self.base.k(
                "delete",
                "namespace",
                ns,
                "--ignore-not-found",
                "--wait=true",
                "--timeout=60s",
                allowed=True,
            )
        self.base.live_start = started
        return self.base.cleanup()

    def arrival_journal(self, ns):
        path = self.episode_output / "arrivals-journal.jsonl"
        if not path.exists():
            raw = self.base.python(
                ns, "observer", 'print(open("/evidence/k4-arrivals.jsonl").read())', record=False
            )
            path.write_text(raw, encoding="utf-8", newline="\n")
        return path.read_text(encoding="utf-8")

    def assemble(self, ns, db, cell, confirmation, targets, start, end, quiet, timeout, manifest):
        confirmation.setdefault(
            "reaction",
            (
                confirmation["intervention"] - min(o["at"] for o in confirmation["overlap"])
                if confirmation["overlap"]
                else None
            ),
        )
        snapshot = self.snapshot(db)
        sessions = apply_process_evidence(
            session_rows(self.collector.pods, self.collector.endpoints),
            self.collector.log_text(),
            self.collector.log_text("cri-exits.jsonl"),
        )
        witness, invocations, traces = export_seals(self.witness_raw(ns), sessions)
        # Map runtime DB session UUIDs to exact observed CRI sessions. The UID
        # downward alias is resolved by enclosing process lifetime, not by name.
        for raw in snapshot["sessions"]:
            rows = [h for h in snapshot["history"] if h["session"] == raw["session"]]
            times = (
                [utc(h["created_at"]) for h in rows]
                + [utc(r["at"]) for r in snapshot["leases"] if r["session"] == raw["session"]]
                + [utc(r["at"]) for r in snapshot["claim_audit"] if r["session"] == raw["session"]]
            )
            require(
                raw["container_id"] == raw["pod_uid"]
                or any(
                    raw["container_id"] == s["container_id"]
                    for s in sessions
                    if s["pod_uid"] == raw["pod_uid"]
                ),
                "k4-db-container-alias",
            )
            candidates = [
                s
                for s in sessions
                if s["pod_uid"] == raw["pod_uid"]
                and all(s["start"] <= at <= (s["death"] or math.inf) for at in times)
            ]
            require(len(candidates) == 1, "k4-db-container-session-ambiguous")
            candidates[0]["session"] = raw["session"]
        timeline = dict(
            schema="dur053.k4-timeline.v1",
            episode=cell["episode"],
            namespace=ns,
            sessions=sessions,
            claims=[],
            invocations=invocations,
            history=[],
            receipts=[],
            workflows=[],
            http=[],
            witness=witness,
        )
        workers = {s["session"]: s for s in sessions if s["role"] == "worker"}
        for trace in traces:
            if trace["event"] == "signal":
                workers[trace["session"]]["signals"].append(
                    dict(at=trace["at"], signal=trace["signal"], evidence="sealed-worker-trace")
                )
        attempts = {
            (a["workflow_id"], a["node_id"], a["iteration"], a["attempt_number"]): a
            for a in snapshot["attempts"]
        }

        def identity(row):
            key = (
                row["workflow_id"],
                row.get("node_id") or "metadata",
                row.get("iteration") or 0,
                row.get("attempt_number") or 1,
            )
            a = attempts.get(key, {})
            return dict(
                namespace=ns,
                workflow=key[0],
                node=key[1],
                iteration=key[2],
                attempt=key[3],
                token_hash=a.get("token_hash", "0" * 64),
            )

        timeline["claims"] = durable_claims(
            snapshot["attempts"], traces, ns, snapshot["claim_audit"]
        )
        for v in invocations:
            matches = [c for c in timeline["claims"] if c["identity"] == v["identity"]]
            require(
                len(matches) == 1
                and matches[0]["session"] == v["session"]
                and matches[0]["at"] <= v["start"],
                "k4-claim-witness-database",
            )
        workflows = {w["workflow_id"]: w for w in snapshot["workflows"]}
        for raw in sorted(snapshot["history"], key=lambda r: r["order"]):
            row = dict(
                identity=identity(raw),
                order=raw["order"],
                at=utc(raw["created_at"]),
                kind=history_kind(raw),
                receipt="",
                partition=raw["partition"],
                owner=raw["owner"],
                epoch=raw["epoch"],
                scheduler_epoch=raw["scheduler_epoch"],
                actor_kind=raw["actor_kind"],
                revision=raw["revision"],
            )
            if (
                raw["owner"]
                and raw["revision"] == workflows[raw["workflow_id"]]["revision"]
                and raw["new_state"] in {"SUCCEEDED", "FAILED", "CANCELED"}
            ):
                row["kind"] = "terminal"
            if raw["reason"] == "ATTEMPT_RESULT_RECORDED" and row["kind"] == "receipt":
                row["receipt"] = raw["transition_id"]
                i = row["identity"]
                vs = [v for v in invocations if v["identity"] == i]
                require(len(vs) == 1, "k4-receipt-invocation-missing")
                a = attempts[i["workflow"], i["node"], i["iteration"], i["attempt"]]
                timeline["receipts"].append(
                    dict(
                        id=row["receipt"],
                        identity=i,
                        session=vs[0]["session"],
                        order=row["order"],
                        at=row["at"],
                        category="committed",
                        outcome="success"
                        if a["state"] == "SUCCEEDED"
                        else "retryable"
                        if a["state"] == "FAILED_RETRYABLE"
                        else "failure",
                        rejection="",
                    )
                )
            timeline["history"].append(row)
        for w in snapshot["workflows"]:
            terminal = [
                h
                for h in timeline["history"]
                if h["identity"]["workflow"] == w["workflow_id"] and h["kind"] == "terminal"
            ]
            observations = [
                r["data"]["clock"]
                for r in self.samples
                if r["data"]
                and any(
                    x["workflow_id"] == w["workflow_id"]
                    and x["state"] in {"SUCCEEDED", "FAILED", "CANCELED"}
                    for x in r["data"]["workflows"]
                )
            ]
            timeline["workflows"].append(
                dict(
                    workflow=w["workflow_id"],
                    key=w["submission_key"],
                    state=w["state"],
                    terminal=terminal[-1]["at"] if terminal else None,
                    clock_width=snapshot["bracket"]["width"],
                    first_observed=min(observations) if observations else None,
                )
            )
        for r in traces:
            if r["event"] == "http" or (
                r["event"] == "http_start"
                and not any(
                    later["event"] == "http"
                    and later["request"] == r["request"]
                    and later["attempt"] == r["attempt"]
                    for later in traces
                )
            ):
                timeline["http"].append(
                    {
                        **{
                            k: r[k]
                            for k in (
                                "request",
                                "body_hash",
                                "attempt",
                                "operation",
                                "at",
                                "status",
                                "error",
                            )
                        },
                        "role": "worker-control",
                    }
                )
        runtime_sessions = [s for s in sessions if s["role"] != "worker"]
        for s in runtime_sessions:
            # Non-worker sessions execute no activities. The collector seals an
            # empty invocation inventory, bound to the persistent process stream.
            timeline["witness"].append(
                dict(
                    session=s["session"],
                    pod_uid=s["pod_uid"],
                    container_id=s["container_id"],
                    sha256=hashlib.sha256(b"").hexdigest(),
                    data="",
                    acknowledged=[],
                    complete=True,
                    errors=0,
                )
            )
        leases = [
            {
                **{
                    k: r[k]
                    for k in (
                        "order",
                        "kind",
                        "partition",
                        "owner",
                        "epoch",
                        "session",
                        "transaction",
                    )
                },
                "at": utc(r["at"]),
            }
            for r in snapshot["leases"]
        ]
        raw = self.arrival_journal(ns)
        plan, updates, torn = journal_rows(raw)
        require(not torn, "k4-arrival-journal-torn")
        arrivals = []
        for r in plan["rows"]:
            at = utc(r["scheduled_at"])
            if at >= confirmation["intervention"] + manifest["W"]:
                continue
            update = updates.get(r["key"])
            require(update is not None, "k4-arrival-journal-gap")
            arrivals.append(
                {
                    k: update[k]
                    for k in (
                        "id",
                        "key",
                        "workflow",
                        "outcome",
                        "status",
                        "error",
                        "request_hash",
                        "attempts",
                    )
                }
                | dict(
                    scheduled=at,
                    sent=utc(update["sent"]) if update["sent"] else None,
                    phase="warmup" if at < confirmation["intervention"] else "measured",
                )
            )
        for a in arrivals:
            timeline["http"].append(
                dict(
                    role="api",
                    request=a["id"],
                    body_hash=a["request_hash"],
                    attempt=a["attempts"],
                    operation="submit",
                    at=a["sent"] or a["scheduled"],
                    status=a["status"],
                    error=a["error"],
                )
            )
        by_uid = {}
        for s in sessions:
            by_uid.setdefault(s["pod_uid"], []).append(s)
        for pod in targets:
            uid = pod["metadata"]["uid"]
            cid = pod["status"]["containerStatuses"][0]["containerID"]
            old = next(s for s in by_uid.get(uid, []) if s["container_id"] == cid)
            replacements = [
                s
                for s in sessions
                if s["role"] == old["role"]
                and s["session"] != old["session"]
                and any(
                    r["present"] and r["at"] >= confirmation["intervention"] for r in s["readiness"]
                )
            ]
            if confirmation["kind"] == "eviction":
                mapped = [
                    e["replaced_by"]
                    for e in confirmation["evictions"]
                    if e["pod_uid"] == uid and e["replaced_by"]
                ]
                require(len(mapped) == 1, "k4-eviction-replacement-missing")
                replacements = [s for s in replacements if s["pod_uid"] == mapped[0]]
            new = min(replacements, key=lambda s: s["start"]) if replacements else None
            confirmation["targets"].append(
                dict(
                    session=old["session"],
                    pod_uid=uid,
                    container_id=cid,
                    deleted=next(
                        (
                            utc(e["object"]["metadata"]["deletionTimestamp"])
                            for e in self.collector.pods
                            if e["object"]["metadata"]["uid"] == uid
                            and e["object"]["metadata"].get("deletionTimestamp")
                        ),
                        None,
                    ),
                    replacement_uid=new["pod_uid"] if new else "",
                    replacement_ready=next(
                        (
                            r["at"]
                            for r in new["readiness"]
                            if r["present"] and r["at"] >= confirmation["intervention"]
                        ),
                        None,
                    )
                    if new
                    else None,
                    restart_before=pod["status"]["containerStatuses"][0]["restartCount"],
                    restart_after=max(
                        (
                            c.get("restartCount", 0)
                            for e in self.collector.pods
                            if e["object"]["metadata"]["uid"] == uid
                            for c in e["object"].get("status", {}).get("containerStatuses", [])
                        ),
                        default=0,
                    ),
                )
            )
        require_term_evidence(confirmation, sessions)
        for o in confirmation["overlap"]:
            s = next(
                (
                    s
                    for s in sessions
                    if s["pod_uid"] == o["pod_uid"]
                    and s["start"] <= o["at"] <= (s["death"] or math.inf)
                ),
                None,
            )
            if s:
                o["session"] = s["session"]
            if o["role"] == "worker":
                v = next(
                    v
                    for v in invocations
                    if v["identity"] == o["claim"] and v["session"] == o["session"]
                )
                o["end"] = v["finish"] if v["finish"] is not None else s["death"] or end
            elif s:
                exits = [
                    d
                    for _, _, text in self.collector.log_text()
                    if (d := structured_log(text))
                    and d.get("boundary") in {"exit", "after_lease_acquired_hold_exit"}
                    and (
                        d.get("identity") == o["acknowledgment"]
                        or d.get("acquisition") == o["acknowledgment"]
                    )
                ]
                o["end"] = min([utc(d["timestamp"]) for d in exits] + [s["death"] or end])
        observer = []

        def claimed_pod(attempt):
            matches = [c for c in timeline["claims"] if c["identity"] == identity(attempt)]
            require(len(matches) == 1, "k4-observer-claim-session-missing")
            return workers[matches[0]["session"]]["pod_uid"]

        for raw_sample in self.samples:
            sample_at = raw_sample["data"]["clock"] if raw_sample["data"] else raw_sample["at"]
            pods = [
                p
                for p in raw_sample["pods"]
                if p["metadata"].get("labels", {}).get("app") == "worker"
            ]
            active = [v for v in invocations if v["start"] <= sample_at < (v["finish"] or end)]
            observer.append(
                dict(
                    at=sample_at,
                    source="host-bracketed-db-and-pod",
                    width=raw_sample["width"],
                    gap=raw_sample["gap"],
                    error=raw_sample["error"],
                    oldest_claimable=float(raw_sample["data"]["demand"][0]["oldest"])
                    if raw_sample["data"]
                    else None,
                    active_claims=[
                        dict(identity=identity(a), pod_uid=claimed_pod(a))
                        for a in (raw_sample["data"]["attempts"] if raw_sample["data"] else [])
                        if a["state"] == "CLAIMED" and a["worker_id"]
                    ],
                    pods=[
                        dict(
                            pod_uid=p["metadata"]["uid"],
                            role="worker",
                            ready=ready(p),
                            eligible=ready(p),
                        )
                        for p in pods
                    ],
                    replicas=len(pods),
                    busy_slots=len(active),
                    total_slots=2 * len(pods),
                )
            )
        docs = dict(
            timeline=timeline,
            leases=dict(schema="dur053.k4-lease-audit.v1", episode=cell["episode"], rows=leases),
            confirmation=confirmation,
            arrivals=dict(schema="dur053.k4-arrivals.v1", episode=cell["episode"], rows=arrivals),
            kafka=dict(
                schema="dur053.k4-kafka.v1",
                episode=cell["episode"],
                rows=callback_rows(traces, arrivals, timeline, confirmation["intervention"]),
            ),
            witness=dict(
                schema="dur053.k4-witness.v1", episode=cell["episode"], rows=timeline["witness"]
            ),
            observers=dict(schema="dur053.k4-observers.v1", episode=cell["episode"], rows=observer),
        )
        save(
            self.episode_output / "container-resolution.json",
            [
                dict(
                    session=s["session"],
                    pod_uid=s["pod_uid"],
                    container_id=s["container_id"],
                    observed_start=s["start"],
                    observed_death=s["death"],
                    death_evidence=s["death_evidence"],
                )
                for s in sessions
            ],
        )
        if manifest["hold"]:
            hold = manifest["hold"]
            require(hold["hold_released"] is not None, "k4-hold-release-missing")
            hold["measured_arrivals"] = sum(
                a["phase"] == "measured"
                and hold["hold_set"]["observer"]
                <= a["scheduled"]
                < min(hold["hold_released"]["observer"], hold["bound_expiry"])
                for a in arrivals
            )
        for key, doc in docs.items():
            locked_document(self.episode_output / FILES[key], doc)
        manifest.update(
            warmup_start=start,
            intervention=confirmation["intervention"],
            window_end=confirmation["intervention"] + manifest["W"],
            reconciliation_end=end,
            timeout=timeout,
            witness_available=True,
            witness_activity=[r["at"] for r in traces],
            quiet_since=quiet,
            clock_brackets=[r["data"]["bracket"] for r in self.samples if r["data"]],
            files={
                k: hashlib.sha256((self.episode_output / v).read_bytes()).hexdigest()
                for k, v in FILES.items()
            },
        )
        locked_document(self.episode_output / "episode.json", manifest)

    def hold_state(self, ns, role, action):
        before = time.time()
        path = "/witness/control/" + role + ".hold"
        code = (
            "import pathlib,time,json; p=pathlib.Path(" + repr(path) + "); "
            "p.parent.mkdir(exist_ok=True); existed=p.exists(); "
            + ("p.touch(); " if action == "set" else "p.unlink(missing_ok=True); ")
            + "print(json.dumps(dict(observer=time.time(), existed=existed, "
            "exists=p.exists(), mtime=p.stat().st_mtime if p.exists() else 0)))"
        )
        state = json.loads(self.base.python(ns, "observer", code))
        return dict(host_start=before, host_end=time.time(), path=path, **state)

    def overhead_probe(self, addresses, f):
        modes = {}
        for mode in ("off", "on"):
            self.episode_output = self.output / ("overhead-" + mode)
            self.episode_output.mkdir()
            cell = episode_identity("short-core-0-pair-0-treatment")
            ns, db = self.setup(cell, addresses, campaign=mode == "on")
            p = dict(parameters(f, "short"), warmup=5, W=60, confirmation=0)
            start = time.time() + 3
            stop = start + p["warmup"] + p["W"]
            self.base.python(
                ns, "observer", 'open("/evidence/k4-stop","w").write(' + repr(stamp(stop)) + ")"
            )
            self.load(ns, cell, p, start)
            deadline = stop + 600
            while True:
                snap = self.snapshot(db, light=True)
                require(time.time() < deadline, "k4-overhead-reconciliation-timeout")
                if snap["workflows"] and all(w["state"] == "SUCCEEDED" for w in snap["workflows"]):
                    break
                time.sleep(1)
            # The off-mode K3 ledger contains tokens. Hash them inside the pod,
            # before transport or any host artifact/command output is produced.
            code = """import json,pathlib,hashlib
rows=[]
for p in pathlib.Path('/witness').rglob('*.jsonl'):
 for line in p.read_text().splitlines():
  row=json.loads(line)
  if 'claim_token' in row:
   row['claim_token_hash']=hashlib.sha256(row.pop('claim_token').encode()).hexdigest()
  rows.append(row)
print(json.dumps(rows))
"""
            rows = json.loads(self.base.python(ns, "observer", code, record=False))
            save(self.episode_output / "sanitized-witness.json", rows)
            starts = {}
            busy = {}
            for row in rows:
                if row.get("event") not in {"start", "finish", "end"}:
                    continue
                identity = row.get("identity", {})
                workflow = identity.get("workflow", row.get("workflow_id"))
                ident = row.get("id", row.get("invocation_id"))
                at = row.get("at", row.get("monotonic_ns", 0) / 1e9)
                if row["event"] == "start":
                    starts[ident] = (workflow, at)
                else:
                    require(ident in starts, "k4-overhead-witness-gap")
                    workflow, began = starts.pop(ident)
                    busy[workflow] = busy.get(workflow, 0) + at - began
            require(not starts, "k4-overhead-unfinished-witness")
            journal = self.base.python(
                ns, "observer", 'print(open("/evidence/k4-arrivals.jsonl").read())', record=False
            )
            (self.episode_output / "arrivals-journal.jsonl").write_text(
                journal, encoding="utf-8", newline="\n"
            )
            plan, updates, torn = journal_rows(journal)
            require(not torn, "k4-overhead-journal-gap")
            measured = {
                r["workflow_id"] for r in plan["rows"] if start + 5 <= utc(r["scheduled_at"]) < stop
            }
            require(measured and measured <= set(busy), "k4-overhead-population")
            complete = {
                w["workflow_id"]: utc(w["updated_at"])
                for w in snap["workflows"]
                if w["state"] == "SUCCEEDED"
            }
            timings = [
                r
                for _, _, line in self.collector.log_text()
                if (r := structured_log(line)) and r.get("event") == "k4_commit_timing"
            ]
            require(
                timings and all(r["success"] for r in timings), "k4-overhead-commit-timing-missing"
            )
            save(self.episode_output / "commit-timings.json", timings)
            value = dict(
                **overhead_sensitivity(plan, snap, measured, timings),
                rate=p["rate"],
                start=start,
                stop=stop,
                W=60,
                warmup=5,
                measured_workflows=len(measured),
                per_workflow_busy={key: busy[key] for key in sorted(measured)},
                b=sum(busy[key] for key in measured) / len(measured),
                completion_throughput=sum(start + 5 <= at < stop for at in complete.values()) / 60,
                arrivals_accepted=sum(r["outcome"] == "accepted" for r in updates.values()),
            )
            save(self.episode_output / "probe.json", value)
            modes[mode] = value
            self.collector.close()
            self.collector = None
            self.base.reset_episode(ns, db)
            self.namespaces.discard(ns)
        result = dict(
            schema="dur053.k4-overhead.v1",
            modes=modes,
            retuned=False,
            same_cluster=True,
            calibration_b=f["arms"]["short"]["b"],
            b_on_over_off=modes["on"]["b"] / modes["off"]["b"],
            b_on_over_calibration=modes["on"]["b"] / f["arms"]["short"]["b"],
            throughput_on_minus_off=modes["on"]["completion_throughput"]
            - modes["off"]["completion_throughput"],
            disclosure="Commit serialization and all K4 instrumentation are enabled together; "
            "Chaos Mesh is present in both probes and both lifecycle arms; "
            "K3 calibration had no Chaos Mesh. Frozen b, r, L remain unchanged.",
        )
        save(self.output / "instrumentation-overhead.json", result)

    def execute_episode(self, cell, addresses, f, job_manifest, checker):
        self.episode_output = self.output / cell["episode"]
        self.episode_output.mkdir()
        p = episode_parameters(f, cell, engineering=self.engineering)
        m = empty(EPISODE)
        m.update(
            schema="dur053.k4-episode.v1",
            episode=cell["episode"],
            replacement_for=cell.get("replacement_for", ""),
            arm=cell["arm"],
            job=job_manifest["job"],
            run_id=job_manifest["run_id"],
            job_id=job_manifest["job_id"],
            head_sha=job_manifest["head_sha"],
            tree_sha=job_manifest["tree_sha"],
            freeze_sha256=f["sha256"],
            status="invalid",
            budget_scope=job_manifest["budget_scope"],
            measurement_claim=job_manifest["measurement_claim"],
            seed=cell["seed"],
            rate=p["rate"],
            L=p["L"],
            W=p["W"],
            images=self.images["a"],
            baseline_flags=["RUNTIME_SHUTDOWN_MODE=legacy", "WORKER_DRAIN_MODE=legacy"]
            if cell["arm"] == "baseline"
            else [],
        )
        if cell["episode"].endswith("-replacement-1"):
            m["replacement_for"] = episode_identity(cell["episode"])["episode"]
        ns, db = self.setup(cell, addresses)
        m["namespace"] = ns
        locked_document(self.episode_output / "episode.json", m)
        adapter = Interventions(
            self.base, ns, self.episode_output, self.images, self.proof, self.source
        )
        stop_value = None

        def stop_load(intervention):
            nonlocal stop_value
            stop = intervention + p["W"]
            value = stamp(stop)
            if stop_value is not None:
                require(stop_value == value, "k4-stop-file-changed")
            stop_value = value
            script = (
                "from pathlib import Path; p=Path('/evidence/k4-stop'); value="
                + repr(value)
                + "; assert not p.exists() or p.read_text() == value; "
                "p.write_text(value) if not p.exists() else None"
            )
            self.base.python(ns, "observer", script)

        adapter.stop_load = stop_load
        if "rollback" in cell["event"]:
            adapter.rollback_setup(cell["event"].split()[0].lower())
        self.start_samples(ns, db)
        adapter.safety = self.check_observation
        start = time.time() + 3
        m["warmup_start"] = start
        locked_document(self.episode_output / "episode.json", m)
        pool = ThreadPoolExecutor(max_workers=1)
        load_future = pool.submit(self.load, ns, cell, p, start)
        try:
            hold_role = "worker" if "PDB" in cell["event"] else cell["event"].split()[0].lower()
            while time.time() < start + max(0, p["warmup"] - 1):
                self.collector.live.read()
                self.check_observation()
                time.sleep(0.05)
            m["hold"] = dict(
                role=hold_role,
                hold_set=self.hold_state(ns, hold_role, "set"),
                hold_released=None,
                bound_expiry=0,
                bound_expired=False,
                measured_arrivals=0,
            )
            m["hold"]["bound_expiry"] = m["hold"]["hold_set"]["mtime"] + 60
            locked_document(self.episode_output / "episode.json", m)

            def release():
                if m["hold"]["hold_released"] is None:
                    m["hold"]["hold_released"] = self.hold_state(ns, hold_role, "release")
                    m["hold"]["bound_expired"] = (
                        m["hold"]["hold_released"]["observer"] >= m["hold"]["bound_expiry"]
                    )
                    locked_document(self.episode_output / "episode.json", m)

            adapter.release = release
            while time.time() < start + p["warmup"]:
                self.check_observation()
                time.sleep(0.05)
            deadline = start + p["warmup"] + p["confirmation"]
            role = "worker" if "PDB" in cell["event"] else cell["event"].split()[0].lower()
            m["confirmation_deadline"] = deadline
            locked_document(self.episode_output / "episode.json", m)
            overlap = self.confirmed_overlap(ns, role, deadline)
            m["confirmation_deadline"] = deadline
            if overlap:
                require(time.time() <= deadline, "k4-harness-confirmation-deadline")
                try:
                    confirmation, targets = adapter.apply(cell, overlap)
                except Exception as error:
                    if adapter.intervention_issued is None or not intervention_outcome(error):
                        raise
                    # A failed intervention is retained as an engine/fault
                    # outcome; it is not eligible for infrastructure replacement.
                    save(self.episode_output / "intervention-error.json", dict(error=str(error)))
                    confirmation = load(self.episode_output / "confirmation.json")
                    targets = []
                    adapter.release()
            else:
                confirmation = empty(CONFIRMATION)
                kind = (
                    "eviction"
                    if "PDB" in cell["event"]
                    else "kill"
                    if "SIGKILL" in cell["event"]
                    else "scale-down"
                    if "scale-down" in cell["event"]
                    else "rollback"
                    if "rollback" in cell["event"]
                    else "rollout"
                )
                confirmation.update(
                    schema="dur053.k4-confirmation.v1",
                    episode=cell["episode"],
                    kind=kind,
                    target="engine-replicated-roles"
                    if kind == "eviction"
                    else "Deployment/" + role,
                    intervention=deadline,
                    removed=time.time(),
                    completed=time.time(),
                )
                targets = []
                save(
                    self.episode_output / "unexercised.json",
                    dict(
                        reason="k4-unexercised-overlap",
                        observed_exited_overlaps=self.observed_exited_overlaps,
                    ),
                )
                adapter.release()
            require(confirmation["intervention"] <= deadline, "k4-harness-confirmation-deadline")
            stop_load(confirmation["intervention"])
            stop = confirmation["intervention"] + p["W"]
            while time.time() < stop:
                self.check_observation()
                time.sleep(0.1)
            require(load_future.result().returncode == 0, "k4-loadgen-failed")
            quiet = time.time()
            last = None
            deadline = max(stop, confirmation["removed"] or stop) + p["T"]
            timeout = False
            poll_path = self.episode_output / "reconciliation-polls.jsonl"
            with poll_path.open("ab") as poll_log:
                while True:
                    snap, quiet, last, t0, t1, changed = reconciliation_round(
                        lambda: self.snapshot(db, light=True),
                        lambda: self.witness_raw(ns),
                        last,
                        quiet,
                    )
                    poll_log.write(
                        (json.dumps(dict(t0=t0, t1=t1, fingerprint_changed=changed)) + "\n").encode(
                            "utf-8"
                        )
                    )
                    poll_log.flush()
                    terminal = all(
                        w["state"] in {"SUCCEEDED", "FAILED", "CANCELED", "ABANDONED", "REJECTED"}
                        for w in snap["workflows"]
                    )
                    state = reconciled(
                        t1,
                        all_terminal=terminal,
                        witness_available=True,
                        last_activity=quiet,
                        deadline=deadline,
                    )
                    if state != "waiting":
                        timeout = state == "timeout"
                        break
                    self.check_observation()
                    time.sleep(1)
            save(
                self.episode_output / "engine-outcome.json",
                dict(snapshot=snap, timeout=timeout, observed_at=time.time(), deadline=deadline),
            )
            self.stop_samples()
            issued = adapter.intervention_issued is not None
            m["status"] = "PASS" if issued else "unexercised"
            # Retain this input before assembly can fail and reset deletes the pod.
            self.arrival_journal(ns)
            self.assemble(
                ns, db, cell, confirmation, targets, start, time.time(), quiet, timeout, m
            )
            metrics = derive_episode(self.episode_output, f, checker)
            if not issued:
                m["status"] = "unexercised"
            else:
                m["status"] = (
                    "PASS" if metrics["valid"] and not metrics["intervention_outcome"] else "FAIL"
                )
            locked_document(self.episode_output / "episode.json", m)
            metrics = derive_episode(self.episode_output, f, checker)
            save(self.episode_output / "metrics.json", metrics)
            return metrics
        finally:
            self.observer_stop.set()
            self.sampler.join(timeout=30)
            if self.collector:
                self.collector.close()
                self.collector = None
            if not load_future.done():
                with suppress(Exception):
                    self.base.python(
                        ns,
                        "observer",
                        'open("/evidence/k4-stop","w").write(' + repr(stamp(time.time())) + ")",
                    )
            pool.shutdown(wait=True)
            try:
                self.base.reset_episode(ns, db)
                self.namespaces.discard(ns)
            except Exception as error:
                self.fatal = True
                save(self.episode_output / "reset-error.json", dict(error=str(error)))


def overhead_sensitivity(plan, snapshot, measured, timings):
    from dur053_pilot import nearest_rank

    scheduled = {r["workflow_id"]: utc(r["scheduled_at"]) for r in plan["rows"]}
    terminal = {
        w["workflow_id"]: utc(w["updated_at"])
        for w in snapshot["workflows"]
        if w["state"] == "SUCCEEDED"
    }
    require(measured <= terminal.keys(), "k4-overhead-terminal-missing")
    latency = [terminal[key] - scheduled[key] for key in sorted(measured)]
    commits = [row["seconds"] for row in timings]
    require(latency and commits and min(latency + commits) >= 0, "k4-overhead-timing-invalid")
    return dict(
        end_to_end_latency=dict(
            n=len(latency), p50=nearest_rank(latency, 0.5), p99=nearest_rank(latency, 0.99)
        ),
        db_commit_path=dict(
            n=len(commits),
            p50=nearest_rank(commits, 0.5),
            p99=nearest_rank(commits, 0.99),
            scope="pgx COMMIT round trip; includes deferred finalization and lock wait",
        ),
    )


def verify_overhead(root, f):
    overhead = json.loads((root / "instrumentation-overhead.json").read_bytes())
    require(
        overhead["retuned"] is False and overhead["same_cluster"] is True, "k4-overhead-disclosure"
    )
    require(set(overhead["modes"]) == {"on", "off"}, "k4-overhead-modes")
    for mode, value in overhead["modes"].items():
        probe = root / ("overhead-" + mode)
        require(
            value == json.loads((probe / "probe.json").read_bytes()), "k4-overhead-probe-binding"
        )
        require(
            value["rate"] == 0.5 * f["arms"]["short"]["r"]
            and value["W"] == 60
            and value["warmup"] == 5
            and value["stop"] == value["start"] + 65,
            "k4-overhead-frozen-rate",
        )
        starts = {}
        busy = {}
        for row in json.loads((probe / "sanitized-witness.json").read_bytes()):
            require("claim_token" not in row, "k4-overhead-raw-token")
            if row.get("event") not in {"start", "finish", "end"}:
                continue
            workflow = row.get("identity", {}).get("workflow", row.get("workflow_id"))
            ident = row.get("id", row.get("invocation_id"))
            at = row.get("at", row.get("monotonic_ns", 0) / 1e9)
            if row["event"] == "start":
                starts[ident] = (workflow, at)
            else:
                require(ident in starts, "k4-overhead-witness-gap")
                workflow, began = starts.pop(ident)
                busy[workflow] = busy.get(workflow, 0) + at - began
        require(not starts, "k4-overhead-unfinished-witness")
        plan, updates, torn = journal_rows((probe / "arrivals-journal.jsonl").read_text())
        measured = {
            r["workflow_id"]
            for r in plan["rows"]
            if value["start"] + 5 <= utc(r["scheduled_at"]) < value["stop"]
        }
        require(not torn and measured and measured <= set(busy), "k4-overhead-population")
        snapshot = json.loads((probe / "database.json").read_bytes())
        timings = json.loads((probe / "commit-timings.json").read_bytes())
        require(timings and all(r["success"] for r in timings), "k4-overhead-commit-timing-missing")
        calculated = dict(
            **overhead_sensitivity(plan, snapshot, measured, timings),
            rate=value["rate"],
            start=value["start"],
            stop=value["stop"],
            W=60,
            warmup=5,
            measured_workflows=len(measured),
            per_workflow_busy={k: busy[k] for k in sorted(measured)},
            b=sum(busy[k] for k in measured) / len(measured),
            completion_throughput=sum(
                value["start"] + 5 <= utc(w["updated_at"]) < value["stop"]
                for w in snapshot["workflows"]
                if w["state"] == "SUCCEEDED"
            )
            / 60,
            arrivals_accepted=sum(r["outcome"] == "accepted" for r in updates.values()),
        )
        require(calculated == value, "k4-overhead-rederived")
    on, off = (overhead["modes"][k] for k in ("on", "off"))
    require(
        overhead["b_on_over_off"] == on["b"] / off["b"]
        and overhead["calibration_b"] == f["arms"]["short"]["b"]
        and overhead["b_on_over_calibration"] == on["b"] / f["arms"]["short"]["b"]
        and overhead["throughput_on_minus_off"]
        == on["completion_throughput"] - off["completion_throughput"],
        "k4-overhead-ratios",
    )
    return overhead


def verify_engineering(root, checker):
    m = load(root / "manifest.json")
    require(
        m["budget_scope"] == "engineering-validation"
        and not m["measurement_claim"]
        and m["charged_seconds"] == 0,
        "k4-engineering-budget",
    )
    require(m["artifact_sha256"] == artifact_hashes(root), "k4-engineering-hashes")
    f = frozen(ROOT)
    metrics = []
    require(
        m["episodes"] == list(SMOKE_CELLS) and m["status"] == "PASS" and m["cleanup"] == "PASS",
        "k4-engineering-coverage",
    )
    for name in m["episodes"]:
        value = derive_episode(root / name, f, checker)
        require(
            value == json.loads((root / name / "metrics.json").read_bytes())
            and value["valid"]
            and value["status"] == "PASS",
            "k4-engineering-metrics",
        )
        metrics.append(value)
    require(
        summaries(metrics) == json.loads((root / "summary.json").read_bytes()),
        "k4-engineering-summary",
    )
    registered = load(root / REGISTERED_SMOKE / "episode.json")
    confirmation = load(root / REGISTERED_SMOKE / "confirmation.json")
    require(
        registered["W"] == parameters(f, "short")["W"]
        and registered["confirmation_deadline"]
        == registered["warmup_start"]
        + parameters(f, "short")["warmup"]
        + parameters(f, "short")["confirmation"]
        and confirmation["reaction"] is not None
        and confirmation["reaction"]
        == confirmation["intervention"] - min(o["at"] for o in confirmation["overlap"]),
        "k4-registered-smoke-confirmation",
    )
    long_timeline = load(root / "long-core-0-pair-0-treatment" / "timeline.json")
    require(
        any(
            s["role"] == "worker"
            and s["death"] is not None
            and any(
                signal["signal"] == 15
                and s["death"] - signal["at"] >= 30
                and any(
                    v["session"] == s["session"]
                    and v["start"] <= signal["at"]
                    and v["finish"] is None
                    for v in long_timeline["invocations"]
                )
                for signal in s["signals"]
            )
            for s in long_timeline["sessions"]
        ),
        "k4-long-drain-deadline-not-exercised",
    )

    verify_overhead(root, f)
    return dict(
        status="PASS",
        budget_scope=m["budget_scope"],
        measurement_claim=False,
        episodes=len(metrics),
        registered_scheduler=dict(
            episode=REGISTERED_SMOKE,
            reaction=confirmation["reaction"],
            intervention=confirmation["intervention"],
            deadline=registered["confirmation_deadline"],
            confirmation="PASS",
        ),
        elapsed_live_seconds=m["elapsed_live_seconds"],
        charged_seconds=m["charged_seconds"],
    )


def run(output, scratch_root, name, engineering=False, replacement_for=""):
    fresh_output(output)
    scratch_root.mkdir(parents=True, exist_ok=True)
    scratch = scratch_root / ("k4-" + secrets.token_hex(8))
    scratch.mkdir()
    d = K4Driver(output, scratch, engineering)
    m = empty(JOB)
    m.update(
        schema="dur053.k4-job.v1",
        job=name,
        run_id=os.getenv("GITHUB_RUN_ID", ""),
        budget_scope="engineering-validation" if engineering else "registered-campaign",
        measurement_claim=not engineering,
        charged_seconds=0 if engineering else CEILING,
        registered_live_seconds=SMOKE_LIVE_RESERVATION if engineering else CEILING,
        status="FAIL",
        cleanup="UNTRUSTWORTHY",
        replacement_for=replacement_for,
    )
    locked_document(output / "manifest.json", m)
    locked_document(
        output / "progress.json", dict(schema="dur053.k4-progress.v1", job=name, started=[])
    )
    metrics = []
    began = None
    started = []
    try:
        require(os.getenv("GITHUB_ACTIONS") == "true", "k4-hosted-only")
        m["head_sha"] = (
            os.getenv("DUR053_HEAD_SHA")
            or d.base.command(["git", "rev-parse", "HEAD"]).stdout.strip()
        )
        m["merge_ref_sha"] = d.base.command(["git", "rev-parse", "HEAD"]).stdout.strip()
        m["tree_sha"] = d.base.command(["git", "rev-parse", "HEAD^{tree}"]).stdout.strip()
        d.source = m["merge_ref_sha"]
        f = frozen(ROOT)
        m["freeze_sha256"] = f["sha256"]
        if engineering:
            require(
                os.getenv("GITHUB_EVENT_NAME") == "pull_request"
                or (
                    os.getenv("GITHUB_EVENT_NAME") == "push"
                    and os.getenv("GITHUB_REF") == "refs/heads/main"
                ),
                "k4-engineering-ci-only",
            )
            m["job_id"] = os.getenv("GITHUB_JOB", "lifecycle-smoke")
            m["assigned"] = list(SMOKE_CELLS)
            units = list(SMOKE_CELLS)
            ledger_remaining = 0
            prior_episodes = set()
        else:
            require(os.getenv("GITHUB_EVENT_NAME") == "workflow_dispatch", "k4-dispatch-only")
            job, prior, head = job_identity_and_history()
            m.update(job_id=job, head_sha=head)
            repo = os.environ["GITHUB_REPOSITORY"]
            run_id = m["run_id"]
            save(output / "actions-run.json", github(f"repos/{repo}/actions/runs/{run_id}"))
            save(output / "actions-job.json", github(f"repos/{repo}/actions/jobs/{job}"))
            save(output / "actions-commit.json", github(f"repos/{repo}/commits/{head}"))
            rows, ledger = inventory(ROOT)
            ledger_check(committed_ledger(ROOT / LEDGER), CEILING, prior)
            ledger_remaining = 259200 - sum(r["charged_seconds"] for r in ledger["entries"])
            save(output / "ci-proof.json", ci_proof(ROOT))
            prior_episodes = {
                eid for _, prior_manifest in rows for eid in prior_manifest["episodes"]
            }
            units = assignment(
                f, name, {x["job_id"]: x for _, x in rows}, replacement_for=replacement_for
            )
            m["assigned"] = units.copy()
            admit(f, units[0], elapsed=0, ledger_remaining=ledger_remaining, setup=True)
        m["pending"] = units.copy()
        locked_document(output / "manifest.json", m)
        d.install()
        began = time.time()
        m["live_start"] = began
        d.base.live_start = time.monotonic()
        d.build()
        checker = scratch / "checker"
        d.base.command(
            ["go", "build", "-buildvcs=false", "-o", str(checker), "./cmd/dur049-checker"]
        )
        addresses = d.cluster()
        if engineering:
            d.overhead_probe(addresses, f)
        for unit in units:
            if not engineering:
                try:
                    admit(f, unit, elapsed=time.time() - began, ledger_remaining=ledger_remaining)
                except ValueError as error:
                    save(output / "carry-over.json", dict(reason=str(error), pending=m["pending"]))
                    m["status"] = "CARRY_OVER"
                    break
            cells = (
                [dict(episode_identity(unit), episode=unit)]
                if engineering or replacement_for
                else [r for r in registry() if r["unit"] == unit]
            )
            cells = [cell for cell in cells if cell["episode"] not in prior_episodes]
            started.append(unit)
            locked_document(
                output / "progress.json",
                dict(schema="dur053.k4-progress.v1", job=name, started=started),
            )
            for cell in cells:
                try:
                    value = d.execute_episode(cell, addresses, f, m, checker)
                    metrics.append(value)
                    m["episodes"].append(cell["episode"])
                    from dur053_record_k4 import derive_disposition, save_disposition_evidence

                    disposition = derive_disposition(output, cell["episode"], value)
                    save_disposition_evidence(output, disposition)
                    m["dispositions"].append(disposition)
                except Exception as error:
                    episode_dir = output / cell["episode"]
                    episode_dir.mkdir(exist_ok=True)
                    ep_path = episode_dir / "episode.json"
                    ep = load(ep_path) if ep_path.exists() else None
                    before = ep is None or ep["warmup_start"] == 0
                    reason = "setup-before-warmup" if before else "collection-failure"
                    if "k4-harness-confirmation-deadline" in str(error):
                        reason = "harness-confirmation-miss"
                    now = time.time()
                    claim_clock_loss = "k4-claim-ack-clock-evidence-missing" in str(error)
                    if ep:
                        ep["status"] = (
                            "unexercised"
                            if claim_clock_loss and (episode_dir / "unexercised.json").exists()
                            else "FAIL"
                            if claim_clock_loss
                            else "invalid"
                        )
                        if before:
                            ep["warmup_start"] = now + 1
                        locked_document(ep_path, ep)
                    incident_source = cell["episode"] + "/harness-source.json"
                    source = dict(
                        status="failure",
                        component="collector",
                        errors=[str(error)],
                        missing=[
                            cell["episode"] + "/" + file
                            for file in FILES.values()
                            if not (episode_dir / file).is_file()
                        ],
                    )
                    if reason == "harness-confirmation-miss":
                        source.update(
                            component="driver",
                            errors=["k4-harness-confirmation-deadline"],
                            observed_overlap=True,
                            deadline=ep["confirmation_deadline"],
                            observed_at=now,
                        )
                    save(output / incident_source, source)
                    incident = cell["episode"] + "/harness-incident.json"
                    locked_document(
                        output / incident,
                        dict(
                            schema="dur053.k4-harness.v1",
                            episode=cell["episode"],
                            reason=reason,
                            component="setup"
                            if before
                            else "driver"
                            if reason == "harness-confirmation-miss"
                            else "collector",
                            at=now,
                            warmup_start=(ep["warmup_start"] if ep else now + 1),
                            source=incident_source,
                        ),
                    )
                    m["episodes"].append(cell["episode"])
                    disposition = dict(
                        episode=cell["episode"],
                        replacement_for=episode_identity(cell["episode"])["episode"]
                        if replacement_for
                        else "",
                        classification="infrastructure-invalid",
                        reason=reason,
                        evidence=[incident],
                    )
                    from dur053_record_k4 import observed_fault_outcome, validate_disposition

                    fault = ep is not None and observed_fault_outcome(output, cell["episode"])
                    if fault and source["missing"]:
                        ep.update(
                            status="FAIL",
                            metrics="not-derivable",
                            missing_exports=[Path(p).name for p in source["missing"]],
                        )
                        locked_document(ep_path, ep)
                        disposition.update(classification="valid", reason="", evidence=[])
                        save(
                            episode_dir / "intake-stopped.json",
                            dict(
                                error="k4-engine-outcome-with-export-loss",
                                missing_exports=ep["missing_exports"],
                                instruction="Charge and retain FAIL; no replacement. "
                                "Continuation requires committed Claude acknowledgement.",
                            ),
                        )
                        m["intake_stopped"].append(cell["episode"])
                        from dur053_record_k4 import stopped_intake

                        value = stopped_intake(output, ep)
                        save(episode_dir / "metrics.json", value)
                        metrics.append(value)
                    validate_disposition(output, disposition)
                    m["dispositions"].append(disposition)
                    save(
                        output / "failure.json",
                        dict(episode=cell["episode"], error=str(error), type=type(error).__name__),
                    )
                    # Export diagnostics before namespace/cluster cleanup.
                    with suppress(Exception):
                        d.diagnostics()
                    if d.collector:
                        d.collector.close()
                        d.collector = None
                    d.fatal = True
                    if m["intake_stopped"]:
                        break

            if not m["intake_stopped"] or m["episodes"][-1] == cells[-1]["episode"]:
                m["pending"].pop(0)
            locked_document(output / "manifest.json", m)
            if d.fatal:
                m["status"] = "FAIL"
                break
        else:
            m["status"] = (
                "PASS"
                if len(metrics) == len(m["episodes"])
                and all(x["valid"] and x["status"] == "PASS" for x in metrics)
                else "FAIL"
            )
    except Exception as error:
        save(output / "driver-error.json", dict(error=str(error), type=type(error).__name__))
        try:
            d.diagnostics()
        except Exception as diagnostic:
            save(output / "diagnostics-error.json", dict(error=str(diagnostic)))
    finally:
        safety_rows = [
            s
            for name in m["episodes"]
            if (s := safety_evidence(output / name, checker)) is not None
        ]
        save(output / "summary.json", summaries(metrics, safety_rows=safety_rows))
        try:
            cleanup = d.cleanup()
            save(output / "cleanup.json", cleanup)
            m["cleanup"] = cleanup["status"]
        except Exception as error:
            save(output / "cleanup-error.json", dict(error=str(error)))
        if began:
            m["live_end"] = time.time()
            m["elapsed_live_seconds"] = math.ceil(m["live_end"] - began)
            m["charged_seconds"] = (
                0
                if engineering
                else (m["elapsed_live_seconds"] if m["cleanup"] == "PASS" else CEILING)
            )
        if m["cleanup"] != "PASS":
            m["status"] = "FAIL"
        m["artifact_sha256"] = artifact_hashes(output)
        locked_document(output / "manifest.json", m)
        # Scratch only. Exported evidence is never erased.
        if m["cleanup"] == "PASS":
            shutil.rmtree(scratch)
    return 0 if m["status"] == "PASS" else 1


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--scenario",
        choices=["lifecycle", "lifecycle-smoke", "verify-evidence", "cleanup"],
        required=True,
    )
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--scratch-root", type=Path, required=True)
    p.add_argument("--job", default="")
    p.add_argument("--replacement-for", default="")
    p.add_argument("--checker", type=Path)
    a = p.parse_args()
    if a.scenario == "verify-evidence":
        require(a.checker is not None, "k4-offline-checker-required")
        manifest = load(a.output / "manifest.json")
        if manifest["budget_scope"] == "engineering-validation":
            result = verify_engineering(a.output, a.checker)
        else:
            from dur053_k4_evidence import verify_evidence

            result = verify_evidence(ROOT, a.checker)
        print(json.dumps(result, indent=2))
        return 0
    if a.scenario == "cleanup":
        # K3 cleanup verifies the same owned cluster/container labels.
        from dur053_k3 import run as cleanup

        return cleanup("cleanup", a.output, a.scratch_root)
    return run(
        a.output,
        a.scratch_root,
        a.job or "lifecycle-smoke",
        a.scenario == "lifecycle-smoke",
        a.replacement_for,
    )


if __name__ == "__main__":
    raise SystemExit(main())
