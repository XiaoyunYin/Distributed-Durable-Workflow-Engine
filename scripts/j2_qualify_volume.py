"""Run one unmeasured J2 qualification on a fresh GitHub-hosted kind cluster."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import subprocess
import threading
import time
from collections import Counter
from contextlib import contextmanager, suppress
from pathlib import Path, PurePosixPath
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import dur053_k3 as kind
from dur053_k3_policy import single_witness_session
from dur053_prerequisites import export_witness
from j2_checker import (
    CHECKER,
    CONTAINER_CHECKER,
    INSTALL,
    MOUNTED_CHECKER,
    POD_CHECKER,
    verify_binary,
)
from j2_checker_report import indexed_report, markers
from j2_offline import SECOND, utc_ns
from j2_overlay import EXPORTER, J2, PROM, ROOT
from j2_qualification_evidence import (
    CAPACITY_EXIT,
    CLOSEOUT_SECONDS,
    DRAIN_SECONDS,
    POPULATION_RATE,
    PROFILE,
    VOLUMES,
    CapacityFailure,
    check_replay_verdict,
    extract,
    file_record,
    read,
    require,
    require_capacity,
    scrape_summary,
    seal,
    write,
)
from j2_qualification_resources import (
    MissingContainerPID,
    checker_samples,
    diagnose_failure,
    oom_kill,
    pod_cgroup,
    sample_checker,
)
from j2_verify_overlay import NS, J2Driver, install_profile

APP = "j2-volume"
DB = "j2_volume"
BOOTSTRAP_SECONDS = 25 * 60
OBSERVER_LIMIT = 2 << 30
ADMIN_SETTINGS = {"DUR050_ADMISSION_MAX_ACTIVE", "DUR050_ADMISSION_MAX_PENDING_OUTBOX"}
METRICS = (
    "up",
    "j2_query_ok",
    "j2_sample_timestamp_seconds",
    "j2_outstanding",
    "j2_unpublished",
    "j2_oldest_claimable_seconds",
    "j2_admissions",
    "j2_terminals",
    *(
        f"j2_db_wait_{kind}"
        for kind in ("lock", "lwlock", "io", "ipc", "client", "timeout", "other")
    ),
)
# Each digest covers the primary key and the PostgreSQL row version that owns
# the persisted bytes. A committed update changes the version even if counts
# stay flat. This DB is dedicated to one application namespace.
QUIET_SQL = """
SELECT json_build_object(
    'workflows', (SELECT count(*) FROM engine.workflow_executions),
    'succeeded', (SELECT count(*) FROM engine.workflow_executions WHERE state='SUCCEEDED'),
    'unpublished', (SELECT count(*) FROM engine.outbox WHERE published_at IS NULL),
    'claimed', (SELECT count(*) FROM engine.activity_attempts WHERE state='CLAIMED'),
    'revisions', (SELECT md5(string_agg(workflow_id || ':' || revision || ':' || xmin, ','
                    ORDER BY workflow_id)) FROM engine.workflow_executions),
    'attempts', (SELECT md5(string_agg(workflow_id || ':' || node_id || ':' || iteration || ':'
                    || attempt_number || ':' || xmin, ',' ORDER BY workflow_id, node_id, iteration,
                    attempt_number)) FROM engine.activity_attempts),
    'outbox', (SELECT md5(string_agg(event_id || ':' || xmin, ',' ORDER BY event_id))
                FROM engine.outbox),
    'evidence', (SELECT md5(string_agg(evidence_id || ':' || xmin, ',' ORDER BY evidence_id))
                  FROM engine.attempt_result_evidence));
"""


class Qualification(J2Driver):
    def __init__(self, output, scratch, checker):
        super().__init__(output, scratch)
        self.live_reservation = 300 * 60
        self.monitor_password = secrets.token_hex(32)
        self.deadline = time.monotonic() + BOOTSTRAP_SECONDS
        self.deadline_criterion = None
        self.population_started = False
        self.observer = None
        self.workers = []
        self.container_groups = {}
        self.checker = checker.resolve()
        self.checker_lock = verify_binary(self.checker)

    def install_checker(self):
        with self.checker.open("rb") as binary:
            status = self.run_stage(
                "install-checker",
                self.exec_args(
                    "python", "-c", INSTALL, POD_CHECKER, self.checker_lock["sha256"], stdin=True
                ),
                60,
                stdin=binary,
            )
        require(status == 0, "could not install verified observer checker")

    def checker_args(self, *args):
        return self.exec_args(
            "python",
            "-c",
            CHECKER,
            POD_CHECKER,
            self.checker_lock["sha256"],
            self.checker_lock["gomemlimit"],
            *args,
        )

    @property
    def node_exec(self):
        return ["docker", "exec", f"{kind.CLUSTER}-control-plane"]

    def time_left(self, maximum):
        remaining = self.deadline - time.monotonic()
        if self.deadline_criterion:
            require_capacity(remaining > 0, self.deadline_criterion, "registered deadline exceeded")
        require(remaining > 0, "qualification stage deadline exceeded")
        return min(remaining, maximum)

    def command(self, args, *, timeout=180, **kwargs):
        return super().command(args, timeout=self.time_left(timeout), **kwargs)

    def exec_args(self, *args, pod=None, stdin=False):
        return [
            "kubectl",
            "--context",
            f"kind-{kind.CLUSTER}",
            "-n",
            APP,
            "exec",
            *(["-i"] if stdin else []),
            pod or self.observer,
            "--",
            *args,
        ]

    def run_stage(self, name, args, timeout, *, stdout=None, stdin=None, monitor_errors=None):
        started = time.monotonic()
        status = None
        error_path = self.output / f"{name}.stderr"
        with (
            (self.output / (stdout or f"{name}.stdout")).open("wb") as out,
            error_path.open("wb") as err,
        ):
            try:
                self.time_left(timeout)
                deadline = min(started + timeout, self.deadline)
                with subprocess.Popen(
                    args, env=self.env, stdin=stdin, stdout=out, stderr=err
                ) as process:
                    while True:
                        try:
                            status = process.poll()
                            if status is not None:
                                break
                            if monitor_errors:
                                raise monitor_errors[0]
                            remaining = deadline - time.monotonic()
                            if remaining <= 0 and deadline == self.deadline:
                                self.time_left(timeout)
                            require(remaining > 0, f"{name} timed out")
                            status = process.wait(timeout=min(5, remaining))
                            break
                        except subprocess.TimeoutExpired:
                            continue
                        except Exception:
                            process.kill()
                            status = process.wait()
                            raise
            finally:
                record = {
                    "stage": name,
                    "exit_status": status,
                    "wall_seconds": time.monotonic() - started,
                }
                with (self.output / "stages.jsonl").open("a", encoding="utf-8") as log:
                    log.write(json.dumps(record) + "\n")
        return status

    def copy_from_pod(self, pod, remote, local):
        destination = self.output / local
        destination.parent.mkdir(parents=True, exist_ok=True)
        status = self.run_stage(
            destination.name + "-copy",
            self.exec_args("cat", remote, pod=pod),
            120,
            stdout=local,
        )
        require(status == 0, f"could not export {remote}")

    def pods(self):
        return [
            pod
            for namespace in (APP, NS)
            for pod in json.loads(
                self.k(
                    "-n", namespace, "get", "pods", "-o", "json", record=False, timeout=10
                ).stdout
            )["items"]
        ]

    def memory(self, pod, uid, container):
        # Read the actual container cgroup in the kind node. The pinned
        # exporter image is scratch and cannot execute a shell.
        runtime_id = container.split("://", 1)[1]
        node = self.node_exec
        result = self.command(
            [*node, "crictl", "inspect", runtime_id], timeout=10, record=False, allowed=True
        )
        pid = None if result.returncode else json.loads(result.stdout).get("info", {}).get("pid")
        if type(pid) is not int or pid <= 0:
            raise MissingContainerPID(pod, uid, container)
        proc = self.command(
            [*node, "cat", f"/proc/{pid}/cgroup"], timeout=10, record=False, allowed=True
        )
        if proc.returncode:
            raise MissingContainerPID(pod, uid, container)
        proc = proc.stdout.strip()
        require(proc.startswith("0::/") and "\n" not in proc, "cgroup v2 required")
        root = "/sys/fs/cgroup" + proc[3:]
        self.container_groups[(pod, uid)] = root
        sample = self.command(
            [*node, "cat", *(f"{root}/memory.{key}" for key in ("max", "peak", "events"))],
            timeout=10,
            record=False,
            allowed=True,
        )
        if sample.returncode:
            raise MissingContainerPID(pod, uid, container)
        maximum, peak, *events = sample.stdout.splitlines()
        return {
            "cgroup": proc,
            "max_bytes": int(maximum),
            "peak_bytes": int(peak),
            "events": event_counts(events),
        }

    def inventory(self):
        rows = {}
        for pod in self.pods():
            metadata, status = pod["metadata"], pod["status"]
            key = f"{metadata['namespace']}/{metadata['name']}"
            rows[key] = {
                "app": metadata.get("labels", {}).get("app"),
                "uid": metadata["uid"],
                "phase": status["phase"],
                "containers": status.get("containerStatuses", [])
                + status.get("initContainerStatuses", []),
            }
        return rows

    def health(self, expected):
        rows = self.inventory()
        record = {"unix_ns": time.time_ns(), "pods": rows, "memory": {}}
        with (self.output / "health.jsonl").open("a", encoding="utf-8") as log:
            try:
                require(
                    {k: r["uid"] for k, r in rows.items()}
                    == {k: r["uid"] for k, r in expected.items()},
                    "pod population changed",
                )
                for name, row in rows.items():
                    for container in row["containers"]:
                        states = [container["state"], container.get("lastState", {})]
                        require_capacity(
                            all(
                                s.get("terminated", {}).get("reason") != "OOMKilled" for s in states
                            ),
                            "oom",
                            "OOMKilled",
                        )
                        require_capacity(
                            container["restartCount"] == 0,
                            "restart",
                            f"container restarted: {container['name']}",
                        )
                    require(row["phase"] == expected[name]["phase"], f"pod phase changed: {name}")
                    if row["app"] in {"observer", EXPORTER}:
                        (container,) = row["containers"]
                        memory = self.memory(name, row["uid"], container["containerID"])
                        record["memory"][row["app"]] = memory
                        limit = OBSERVER_LIMIT if row["app"] == "observer" else (256 << 20)
                        require(memory["max_bytes"] == limit, f"memory limit drift: {row['app']}")
                        require_no_oom(memory["events"])
            finally:
                log.write(json.dumps(record, sort_keys=True) + "\n")
        return record["memory"]

    def diagnose(self, pod, uid, trigger):
        group = self.container_groups.get((pod, uid))
        diagnose_failure(
            self.output,
            self.node_exec,
            f"kind-{kind.CLUSTER}",
            pod,
            uid,
            pod_cgroup(group, uid) if group else None,
            super().command,
            trigger=trigger,
        )

    def check_profile(self, pods):
        replicas = Counter(
            p["metadata"].get("labels", {}).get("app")
            for p in pods
            if p["status"]["phase"] == "Running"
        )
        require(
            replicas
            == {
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
            },
            f"unexpected running pod population: {replicas}",
        )
        objects = read(self.output / "application-rendered.json")
        api = next(
            o for o in objects if o["kind"] == "Deployment" and o["metadata"]["name"] == "api"
        )
        specimens = [api["spec"]["template"]["spec"]]
        specimens.extend(
            p["spec"] for p in pods if p["metadata"].get("labels", {}).get("app") == "api"
        )
        for c in (c for spec in specimens for c in spec["containers"]):
            require(
                not c.get("envFrom")
                and not ADMIN_SETTINGS.intersection(e["name"] for e in c["env"]),
                "admission settings present",
            )
        write(self.output / "application-and-telemetry-pods.json", pods)
        settings = self.sql(
            "SHOW fsync; SHOW synchronous_commit; SHOW full_page_writes; SHOW max_connections;", DB
        ).splitlines()
        write(self.output / "postgres-settings.json", settings)
        require(settings == ["on", "on", "on", "100"], "PostgreSQL durability or budget drift")
        require(
            self.sql("SELECT count(*) FROM engine.workflow_executions", DB) == "0",
            "namespace DB is not fresh",
        )
        self.topics("--describe")
        write(
            self.output / "runner.json",
            {
                "run_id": os.environ["GITHUB_RUN_ID"],
                "attempt": os.environ["GITHUB_RUN_ATTEMPT"],
                "job": os.environ["GITHUB_JOB"],
                "checkout": self.command(["git", "rev-parse", "HEAD"]).stdout.strip(),
                "cpu_model": next(
                    line.split(":", 1)[1].strip()
                    for line in Path("/proc/cpuinfo").read_text().splitlines()
                    if line.startswith("model name")
                ),
                "available_cpus": sorted(os.sched_getaffinity(0)),
                "cgroup": runner_cgroup(),
                "source": {
                    str(path.relative_to(ROOT)): file_record(path)
                    for path in (
                        *sorted((ROOT / "scripts").glob("j2_*")),
                        J2 / "protocol.md",
                        J2 / "images.lock.json",
                        J2 / "exporter.lock.json",
                    )
                },
            },
        )

    def witness_inventories(self):
        ips = [p["status"]["podIP"] for p in self.workers]
        return json.loads(
            self.command(
                self.exec_args(
                    "python",
                    "-c",
                    "import json,sys,urllib.request; "
                    "print(json.dumps([json.load(urllib.request.urlopen("
                    '"http://"+ip+":8081/witness",timeout=3)) for ip in sys.argv[1:]]))',
                    *ips,
                ),
                record=False,
                timeout=10,
            ).stdout
        )

    def witness(self, inventories):
        root = self.output / "witness-raw"
        for inv in inventories:
            require_capacity(
                inv["written"] >= inv["attempted"],
                "truncation",
                f"witness writer dropped events: {inv['pod_name']}",
            )
            pod = inv["pod_name"]
            require(Path(inv["file"]).name == inv["file"], "unexpected witness filename")
            files = json.loads(
                self.command(
                    self.exec_args(
                        "python",
                        "-c",
                        "from pathlib import Path; import json,sys; print(json.dumps(sorted("
                        "p.name for p in (Path('/witness') / sys.argv[1]).glob('*.jsonl'))))",
                        pod,
                        pod=pod,
                    ),
                    record=False,
                    timeout=10,
                ).stdout
            )
            single_witness_session(files, inv)
            self.copy_from_pod(
                pod, f"/witness/{pod}/{inv['file']}", f"witness-raw/{pod}/{inv['file']}"
            )
        path = self.output / "witness.json"
        write(
            path,
            export_witness(root, inventories, sorted(p["metadata"]["uid"] for p in self.workers)),
        )
        with path.open("rb") as stream:
            code = (
                "import shutil,sys; "
                "shutil.copyfileobj(sys.stdin.buffer, open('/evidence/witness.json','wb'))"
            )
            status = self.run_stage(
                "witness-to-observer",
                self.exec_args("python", "-c", code, stdin=True),
                120,
                stdin=stream,
            )
        require(status == 0, "could not transfer witness to the observer")

    def quiet(self, count):
        last = None
        quiet_since = None
        with (self.output / "drain.jsonl").open("w", encoding="utf-8") as log:
            while True:
                before = time.time_ns()
                database = json.loads(self.sql(QUIET_SQL, DB))
                witnesses = self.witness_inventories()
                after = time.time_ns()
                observed = time.monotonic()
                log.write(
                    json.dumps(
                        {
                            "before_ns": before,
                            "after_ns": after,
                            "db": database,
                            "witness": witnesses,
                        }
                    )
                    + "\n"
                )
                log.flush()
                state = (database, witnesses)
                ready = (
                    database["workflows"] == database["succeeded"] == count
                    and database["unpublished"] == database["claimed"] == 0
                )
                if ready and state == last and quiet_since is not None:
                    if observed - quiet_since >= 30:
                        return state
                else:
                    quiet_since = observed if ready else None
                last = state
                time.sleep(self.time_left(5))

    def offline(self, name, args, *, memory=None, stdout=None):
        container = f"{kind.CLUSTER}-{name}"
        self.created.add(container)
        limits = (
            ["--memory", str(memory), "--memory-swap", str(memory), "--cpus=1"] if memory else []
        )
        tooling = (
            ["-v", f"{self.checker}:{MOUNTED_CHECKER}:ro"] if name == "offline-checker" else []
        )
        self.command(
            [
                "docker",
                "create",
                "--name",
                container,
                "--label",
                kind.LABEL,
                "--network=none",
                "--cgroupns=private",
                "--read-only",
                "--cap-drop=ALL",
                "--security-opt=no-new-privileges",
                *limits,
                *tooling,
                "--entrypoint",
                "python",
                "-v",
                f"{self.output}:/qualification:ro",
                "-v",
                f"{ROOT / 'scripts'}:/scripts:ro",
                self.images["worker"],
                *args,
            ],
            timeout=30,
        )
        config = json.loads(
            self.command(
                ["docker", "inspect", "--format", "{{json .HostConfig}}", container],
                timeout=15,
                record=False,
            ).stdout
        )
        write(self.output / f"{name}-isolation.json", config)
        require(
            config["NetworkMode"] == "none" and config["ReadonlyRootfs"], "offline isolation drift"
        )
        status = self.run_stage(
            name, ["docker", "start", "-a", container], CLOSEOUT_SECONDS, stdout=stdout
        )
        state = json.loads(
            self.command(
                ["docker", "inspect", "--format", "{{json .State}}", container],
                timeout=15,
                record=False,
            ).stdout
        )
        write(self.output / f"{name}-container.json", state)
        require_capacity(not state["OOMKilled"], "oom", f"{name} was OOMKilled")
        if name == "offline-checker" and status != 0:
            events = {}
            with suppress(OSError, ValueError, KeyError):
                memory = marker(self.output / f"{name}.stderr", "J2_CGROUP")
                events = event_counts(memory["events"].splitlines())
            require_capacity(
                not oom_kill(events),
                "oom",
                f"{name} recorded OOM events: {events}",
            )
            if state["ExitCode"] == 1:
                check_replay_verdict(self.output)
        if name == "offline-replay" and status != 0 and state["ExitCode"] == CAPACITY_EXIT:
            failure = read(self.output / stdout)
            raise CapacityFailure(failure["capacity_criterion"], failure["error"])
        require(status == state["ExitCode"] == 0 and not state["OOMKilled"], f"{name} failed")

    def clock(self):
        return json.loads(
            self.command(self.checker_args("-clock-probe"), record=False, timeout=20).stdout
        )


def runner_cgroup(proc=Path("/proc/self"), root=Path("/")):
    def absolute(path):
        require(path.startswith("/") and not path.startswith("//"), "invalid cgroup path")
        require(".." not in path.split("/"), "invalid cgroup path")
        return PurePosixPath(path)

    def mount_path(field):
        return absolute(re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), field))

    memberships = [
        line[3:] for line in (proc / "cgroup").read_text().splitlines() if line.startswith("0::")
    ]
    require(len(memberships) == 1, "expected one unified cgroup membership")
    member = absolute(memberships[0])
    mounts = []
    for line in (proc / "mountinfo").read_text().splitlines():
        before, separator, after = line.partition(" - ")
        if not after.startswith("cgroup2 "):
            continue
        fields = before.split()
        require(separator and len(fields) >= 6, "invalid cgroup2 mountinfo entry")
        mount_root, mount = map(mount_path, fields[3:5])
        if member.is_relative_to(mount_root):
            mounts.append((mount_root, mount))
    require(mounts, "no cgroup2 mount covers the unified membership")
    mount_root, mount = max(mounts, key=lambda item: len(item[0].parts))
    path = mount / member.relative_to(mount_root)
    result = {"mount": str(mount), "mount_root": str(mount_root), "path": str(path)}
    for key in ("cpu.max", "memory.max", "cpu.pressure", "memory.pressure"):
        try:
            value = root.joinpath(*path.parts[1:], key).read_text().strip()
        except FileNotFoundError:
            result[key] = {"present": False}
        else:
            result[key] = {"present": True, "value": value}
    return result


def event_counts(lines):
    return {name: int(n) for name, n in map(str.split, lines)}


def require_no_oom(events):
    require("oom_kill" in events and "oom" in events, f"missing OOM counters: {events}")
    require_capacity(not oom_kill(events), "oom", f"OOM kill: {events}")
    require(not any(n for k, n in events.items() if k.startswith("oom")), f"OOM event: {events}")


def marker(path, prefix):
    (value,) = [
        line.removeprefix(prefix + " ")
        for line in path.read_text().splitlines()
        if line.startswith(prefix + " ")
    ]
    return json.loads(value)


@contextmanager
def prometheus(driver):
    args = [
        "kubectl",
        "--context",
        f"kind-{kind.CLUSTER}",
        "-n",
        NS,
        "port-forward",
        f"deployment/{PROM}",
        "19091:9090",
    ]
    with (
        (driver.output / "qualification-port-forward.log").open("w") as log,
        subprocess.Popen(args, env=driver.env, stdout=log, stderr=log) as process,
    ):

        def query(expression, at=None):
            params = {"query": expression}
            if at is not None:
                params["time"] = str(at)
            with urlopen(
                f"http://127.0.0.1:19091/api/v1/query?{urlencode(params)}", timeout=15
            ) as response:
                parsed = json.load(response)
                require(parsed["status"] == "success", "Prometheus query failed")
                return parsed["data"]["result"]

        def ready():
            try:
                return bool(query("j2_query_ok == 1"))
            except URLError:
                return False

        try:
            driver.until(ready, 30)
            yield query
        finally:
            process.terminate()


@contextmanager
def watch(driver):
    expected = driver.inventory()
    driver.health(expected)
    stopped = threading.Event()
    errors = []

    def poll():
        while not stopped.wait(15):
            try:
                driver.health(expected)
            except Exception as error:
                errors.append(error)
                stopped.set()

    thread = threading.Thread(target=poll, daemon=True)
    thread.start()
    try:
        yield expected, errors
    finally:
        stopped.set()
        thread.join(45)
        require(not thread.is_alive(), "resource monitor did not stop")


def populate(driver, count, plan, monitor_errors):
    path = "/evidence/arrivals.json"
    run = driver.exec_args(
        "/loadgen",
        "-url",
        "http://api:8080",
        "-token-file",
        "/identity/token",
        "-namespace",
        APP,
        "-run-id",
        plan["run_id"],
        "-seed",
        str(plan["seed"]),
        "-arm",
        "short",
        "-count",
        str(count),
        "-rate",
        str(POPULATION_RATE),
        "-concurrency",
        "16",
        "-request-timeout",
        "10s",
        "-output",
        path,
    )
    duration = count / POPULATION_RATE
    driver.deadline = time.monotonic() + duration + 90
    try:
        status = driver.run_stage("population", run, duration + 60, monitor_errors=monitor_errors)
    finally:
        with suppress(Exception):
            driver.copy_from_pod(driver.observer, path, "arrivals.json")
            journal = read(driver.output / "arrivals.json")
            if journal["run_id"] == plan["run_id"]:
                driver.population_started = any(
                    row["sent_at"] is not None for row in journal["rows"]
                )
    arrivals = read(driver.output / "arrivals.json")
    write(
        driver.output / "arrival-outcomes.json",
        dict(Counter(r["outcome"] for r in arrivals["rows"])),
    )
    if monitor_errors:
        raise monitor_errors[0]
    require(status == 0 and arrivals["status"] == "PASS", "stock sender failed")
    start = utc_ns(arrivals["rows"][0]["scheduled_at"]) / SECOND
    stop = start + duration
    driver.deadline = time.monotonic() + (stop + DRAIN_SECONDS - time.time())
    driver.deadline_criterion = "drain_deadline"
    return start, stop


def retain_scrapes(driver, query, start, end, count):
    span = int(end - start) + 12
    series = {metric: query(f"{metric}{{job='j2'}}[{span}s]", end) for metric in METRICS}
    write(driver.output / "prometheus-samples.json", series)
    summary = scrape_summary(series, start, end, count)
    write(driver.output / "sample-gaps.json", summary)
    require(
        summary["max_scrape_gap_seconds"] <= 5 and summary["max_query_gap_seconds"] <= 5,
        f"qualification sample gap: {summary}",
    )


def workload(driver, count, plan, result):
    pods = driver.pods()
    driver.check_profile(pods)
    driver.observer = next(
        p["metadata"]["name"]
        for p in pods
        if p["metadata"].get("labels", {}).get("app") == "observer"
    )
    driver.workers = [p for p in pods if p["metadata"].get("labels", {}).get("app") == "worker"]
    driver.install_checker()
    driver.command(["docker", "pull", driver.images["worker"]], timeout=180)
    probes = {"setup": driver.clock()}
    with prometheus(driver) as query, watch(driver) as (expected, errors):
        start = time.time()
        try:
            start, stop = populate(driver, count, plan, errors)
            state, witnesses = driver.quiet(count)
            driver.witness(witnesses)
            checker = [
                "-arrivals",
                "/evidence/arrivals.json",
                "-witness-export",
                "/evidence/witness.json",
                "-require-terminal",
            ]
            pod = f"{APP}/{driver.observer}"
            uid = expected[pod]["uid"]
            cgroup = driver.container_groups[pod, uid]
            sampler = None
            try:
                with sample_checker(
                    driver.output, driver.node_exec[-1], cgroup, driver.env
                ) as sampler:
                    status = driver.run_stage(
                        "live-checker",
                        driver.checker_args(*checker, "-namespace", APP),
                        DRAIN_SECONDS,
                        stdout="live-checker.json",
                        monitor_errors=errors,
                    )
            finally:
                if sampler is not None:
                    sampling = {"sampler_exit": sampler.returncode}
                    result["live_checker_resources"] = sampling
                    try:
                        sampling.update(checker_samples(driver.output))
                    except (OSError, ValueError, IndexError) as error:
                        sampling["error"] = str(error)
            if status != 0:
                driver.diagnose(pod, uid, f"live checker exit {status}")
        except Exception:
            with suppress(Exception):
                retain_scrapes(driver, query, start, time.time(), count)
            raise
        if status == 1:
            report, _ = indexed_report(driver.output / "live-checker.json")
            expected_ids = {r["workflow_id"] for r in read(driver.output / "arrivals.json")["rows"]}
            seen = report.get("workflow_ids", [])
            seen_ids = set(seen)
            require(
                report["status"] == "FAIL"
                and report["valid"] is False
                and isinstance(report["trace"], dict)
                and len(seen_ids) == len(seen)
                and seen_ids <= expected_ids,
                "invalid live report",
            )
            require_capacity(
                seen_ids == expected_ids and not report["non_terminal"],
                "missing_identity",
                "live checker is missing identities or terminal results",
            )
        require(status == 0, "in-pod live checker failed")
        result["resources"] = driver.health(expected)
        if errors:
            raise errors[0]
        sampling = result["live_checker_resources"]
        gap = sampling.get("max_sample_gap_seconds")
        require(
            sampling["sampler_exit"] == 0
            and sampling.get("checker_vmhwm_peak_kib")
            and gap is not None
            and 0 <= gap <= 2,
            f"live-checker resource sampling incomplete: {sampling}",
        )
        retain_scrapes(driver, query, start, time.time(), count)
        require(
            json.loads(driver.sql(QUIET_SQL, DB)) == state
            and driver.witness_inventories() == witnesses,
            "state changed during live export",
        )
        ids = driver.sql(
            "SELECT workflow_id FROM engine.workflow_executions "
            "WHERE state='SUCCEEDED' ORDER BY workflow_id",
            DB,
        ).splitlines()
        write(driver.output / "terminals.json", {"ids": ids, "observed_ns": time.time_ns()})
        probes["measured"] = driver.clock()
        write(driver.output / "clock.json", probes)
        extract(driver.output)
        exported = time.time_ns()
        require_capacity(
            time.monotonic() <= driver.deadline,
            "drain_deadline",
            "live export missed the registered drain deadline",
        )
        result["export_completed_unix_ns"] = exported
        result["scheduled_end_through_export_seconds"] = exported / SECOND - stop
        driver.deadline = time.monotonic() + CLOSEOUT_SECONDS
        driver.deadline_criterion = "closeout_deadline"
    driver.offline(
        "offline-checker",
        [
            "-c",
            CONTAINER_CHECKER,
            MOUNTED_CHECKER,
            driver.checker_lock["sha256"],
            driver.checker_lock["gomemlimit"],
            "-arrivals",
            "/qualification/arrivals.json",
            "-witness-export",
            "/qualification/witness.json",
            "-require-terminal",
            "-snapshot",
            "/qualification/snapshot.json",
        ],
        memory=OBSERVER_LIMIT,
        stdout="offline-checker.json",
    )
    result["checker_processes"] = {
        mode: marker(driver.output / f"{mode}-checker.stderr", "J2_CHECKER")
        for mode in ("live", "offline")
    }
    require(
        all(
            r["exit_status"] == 0
            and r["peak_rss_kib"] > 0
            and r["checker_sha256"] == driver.checker_lock["sha256"]
            and r.get("gomemlimit") == driver.checker_lock["gomemlimit"]
            for r in result["checker_processes"].values()
        ),
        "missing checker process metrics",
    )
    for mode, first in (("live", "load"), ("offline", "offline_decode")):
        phases = markers(driver.output / f"{mode}-checker.stderr", "J2_PHASE")
        require(
            [phase["phase"] for phase in phases]
            == [first, "hash", "check", "campaign_check", "write"],
            f"incomplete checker phase evidence: {mode}",
        )
    memory = marker(driver.output / "offline-checker.stderr", "J2_CGROUP")
    require(
        int(memory["max"]) == OBSERVER_LIMIT and int(memory["peak"]) > 0,
        "offline checker cgroup limit or peak missing",
    )
    result["offline_checker_memory"] = {
        "max_bytes": int(memory["max"]),
        "peak_bytes": int(memory["peak"]),
        "events": event_counts(memory["events"].splitlines()),
    }
    require_no_oom(result["offline_checker_memory"]["events"])
    driver.offline(
        "offline-replay",
        ["/scripts/j2_qualification_evidence.py", "verify", "/qualification"],
        stdout="offline-reconciliation.json",
    )
    result.update(read(driver.output / "offline-reconciliation.json"))


def diagnostics(driver):
    if not driver.cluster_started:
        return
    for namespace in (APP, NS):
        result = driver.k(
            "-n",
            namespace,
            "get",
            "pods,events",
            "-o",
            "json",
            record=False,
            allowed=True,
            timeout=15,
        )
        if result.returncode == 0:
            (driver.output / f"{namespace}-final.json").write_text(result.stdout, encoding="utf-8")
    if driver.observer and not (driver.output / "terminals.json").exists():
        rows = driver.sql(
            "SELECT workflow_id, state FROM engine.workflow_executions ORDER BY workflow_id", DB
        )
        (driver.output / "db-at-stop.txt").write_text(rows, encoding="utf-8")
        if rows.strip():
            driver.population_started = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("count", type=int, choices=VOLUMES)
    for path in ("evidence", "scratch", "layouts", "exporter_layout", "checker"):
        parser.add_argument(path, type=Path)
    args = parser.parse_args()
    require(
        os.environ.get("GITHUB_ACTIONS") == "true"
        and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted"
        and os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch",
        "qualification requires a GitHub-hosted dispatch",
    )
    args.scratch.mkdir()
    kind.CLUSTER, kind.LABEL = "j2-volume", "j2.volume=owned"
    driver = Qualification(args.evidence, args.scratch, args.checker)
    started = time.monotonic()
    run_id = (
        f"j2-volume-{os.environ['GITHUB_RUN_ID']}-{os.environ['GITHUB_RUN_ATTEMPT']}-{args.count}"
    )
    seed = (
        int.from_bytes(
            hashlib.sha256(f"{run_id}/{os.environ['GITHUB_JOB']}/volume".encode()).digest()[:4],
            "big",
        )
        >> 1
    )
    plan = {
        "run_id": run_id,
        "namespace": APP,
        "seed": seed,
        "periods": [[0, args.count, POPULATION_RATE]],
    }
    write(args.evidence / "plan.json", plan)
    result = {
        "status": "FAIL",
        "profile": PROFILE,
        "scope": "unmeasured qualification",
        "planned_identities": args.count,
        "checker": driver.checker_lock,
    }
    try:
        install_profile(driver, args.layouts, args.exporter_layout, APP, DB, unrelated=None)
        workload(driver, args.count, plan, result)
        result["status"] = "PASS"
    except Exception as error:
        if isinstance(error, MissingContainerPID):
            try:
                driver.diagnose(error.pod, error.uid, str(error))
            except CapacityFailure as capacity:
                error = capacity
            except Exception as diagnostic:
                result["diagnostic_error"] = str(diagnostic)
        result["error"] = str(error)
        if isinstance(error, CapacityFailure):
            result["capacity_criterion"] = error.criterion
        print(f"Qualification stopped: {error}", flush=True)
    finally:
        driver.deadline_criterion = None
        # Preserve time to archive and clean up after exhausted stage budgets.
        # The end-to-end closeout still uses the original export timestamp.
        driver.deadline = time.monotonic() + CLOSEOUT_SECONDS
        try:
            diagnostics(driver)
        except Exception as error:
            result["diagnostic_error"] = str(error)
        result["population_started"] = driver.population_started
        result["checker_phases"] = {}
        for mode in ("live", "offline"):
            path = driver.output / f"{mode}-checker.stderr"
            if path.exists():
                with suppress(ValueError, OSError):
                    result["checker_phases"][mode] = markers(path, "J2_PHASE")
        try:
            write(args.evidence / "cleanup.json", driver.cleanup())
        except Exception as error:
            result["status"] = "FAIL"
            result["cleanup_error"] = str(error)
        result["wall_seconds_before_freeze"] = time.monotonic() - started
        seal(args.evidence, result)
        print(json.dumps(result, sort_keys=True), flush=True)
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
