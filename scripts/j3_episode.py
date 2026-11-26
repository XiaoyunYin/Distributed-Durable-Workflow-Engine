"""Collect sender, worker, outage and drain evidence for hosted J3 qualification."""

import hashlib
import json
import os
import subprocess
import time
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from math import ceil
from threading import Event, Thread

import dur053_k3 as kind
from j2_offline import attempt_key, drain_identities, utc_ns
from j3_evidence import jsonl
from j3_overlay import APP, NS, RECEIVER
from j3_pilot import json_request, prom
from j3_prometheus import epoch_ns
from j3_replay import DOMAIN, SECOND, require, unique
from j3_resources import checker_outcome, run_checker
from j3_schedule import ARRIVAL_RATE
from j3_slots import sample_ready_slots

JOURNAL = "/evidence/j3-arrivals.jsonl"
ABORT = "/evidence/j3-abort"
SENDER_EXIT = "/evidence/j3-sender-exit.json"
EXEC_STREAM_GRACE_SECONDS = 30
SEED = 530001
SLOT_INTERVAL = 2
SLOT_MAX_GAP = 5 * SECOND
RULE_SETTLE_SECONDS = 1
TERMINAL = "'SUCCEEDED','FAILED','CANCELED','REJECTED','ABANDONED'"
SCOPE = "namespace=(SELECT namespace FROM j2_monitor.scope)"
COMMIT_NS = "(extract(epoch FROM pg_xact_commit_timestamp(%s.xmin))*1000000000)::bigint"

# The shared sender journals a provisional uncertain row before HTTP. A second
# uncertain row, an unsent row or a rejection stops it at once.
GUARD_SENDER = r"""
import json,os,pathlib,signal,subprocess,sys,time
path,abort,armed,exit_file,*args=sys.argv[1:]
sender=None
closed=False
status=2

def interrupted(signum,frame):
    sys.exit(128+signum)

signals=[getattr(signal,name) for name in ('SIGTERM','SIGINT','SIGHUP') if hasattr(signal,name)]
try:
    for signum in signals:
        signal.signal(signum,interrupted)
    sender=subprocess.Popen(args)
    while not pathlib.Path(path).exists() and sender.poll() is None:
        if pathlib.Path(abort).exists():
            sys.exit(1)
        time.sleep(.02)
    with open(path,'rb') as stream:
        provisional=set()
        while True:
            if pathlib.Path(abort).exists():
                sys.exit(1)
            offset=stream.tell()
            line=stream.readline()
            if not line.endswith(b'\n'):
                if sender.poll() is not None:
                    sys.exit(sender.returncode or 2)
                stream.seek(offset)
                time.sleep(.02)
                continue
            row=json.loads(line)
            if row['event']=='plan':
                pathlib.Path(armed).touch(exist_ok=False)
            if row['event']=='closed':
                closed=True
                sys.exit(sender.wait())
            if row['event']!='arrival' or row['outcome']=='accepted':
                continue
            if (row['outcome']=='uncertain' and row['status']==0 and not row['error']
                and row['key'] not in provisional):
                provisional.add(row['key'])
                continue
            print(json.dumps(row), file=sys.stderr, flush=True)
            sys.exit(1)
except SystemExit as error:
    status=error.code
    raise
except BaseException:
    status=1
    raise
finally:
    try:
        for signum in signals:
            signal.signal(signum,signal.SIG_IGN)
        if sender is not None and sender.poll() is None:
            sender.terminate()
            try:
                sender.wait(timeout=5)
            except subprocess.TimeoutExpired:
                sender.kill()
                sender.wait()
    except BaseException:
        status=1
        raise
    finally:
        temporary=exit_file+'.tmp'
        with open(temporary,'x',encoding='utf-8') as stream:
            json.dump({'returncode':status,'closed':closed,'exited_ns':time.time_ns()},stream)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary,exit_file)
"""

READ_SENDER_EXIT = r"""
import json,pathlib,sys
record,journal=map(pathlib.Path,sys.argv[1:])
raw=record.read_text(encoding='utf-8',errors='replace') if record.exists() else None
last=None
if raw is not None and journal.exists():
    with journal.open('rb') as stream:
        stream.seek(max(0,stream.seek(0,2)-4096))
        tail=stream.read()
    if tail.endswith(b'\n'):
        last=tail.splitlines()[-1].decode('utf-8',errors='replace')
print(json.dumps({'record':raw,'journal_last_line':last}))
"""

OBSERVER_PROCESSES = r"""
import json,pathlib
rows=[]
for proc in pathlib.Path('/proc').iterdir():
    if not proc.name.isdecimal():
        continue
    row={'pid':int(proc.name)}
    try:
        status=(proc/'status').read_text().splitlines()
        state=next(line for line in status if line.startswith('State:'))
        row['state']=state.partition(':')[2].strip()
        row['cmdline']=(proc/'cmdline').read_bytes().decode(errors='replace').rstrip('\0').split('\0')
    except (OSError,StopIteration) as error:
        row['error']=str(error)
    rows.append(row)
print(json.dumps(sorted(rows,key=lambda row:row['pid'])))
"""


def append(path, row):
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(row) + "\n")


class EpisodeFailure(ValueError):
    """A registered behavior failed with observations available."""


def require_behavior(condition, detail):
    if not condition:
        raise EpisodeFailure(detail)


def utc_seconds(ns):
    return datetime.fromtimestamp(ns // SECOND, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def plan(namespace, run, origin, duration):
    rate = float(ARRIVAL_RATE)
    return [
        {
            "namespace": namespace,
            "submission_key": f"dur053-{run}-{SEED}-{i:06d}",
            "scheduled_ns": origin + int(float(i) * SECOND / rate),
        }
        for i in range(ceil(duration * ARRIVAL_RATE))
    ]


def install_plan(driver, rows):
    values = ",".join(
        f"('{row['namespace']}','{row['submission_key']}',{row['scheduled_ns']})" for row in rows
    )
    driver.postgres(
        f"INSERT INTO j2_monitor.j3_plan VALUES {values};"
        "CREATE FUNCTION j2_monitor.j3_plan_sealed() RETURNS trigger LANGUAGE plpgsql "
        "AS $$ BEGIN RAISE EXCEPTION 'J3 plan is sealed'; END; $$;"
        "CREATE TRIGGER j3_plan_sealed BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE "
        "ON j2_monitor.j3_plan FOR EACH STATEMENT EXECUTE FUNCTION j2_monitor.j3_plan_sealed();"
    )
    kind.save(driver.output / "plan.json", rows)


class Sender:
    def __init__(self, driver, observer, binary, rows, run, origin, stop):
        self.driver, self.observer, self.rows = driver, observer, rows
        self.stop = stop
        options = {
            "k4-start": utc_seconds(origin),
            "k4-stop": "/evidence/j3-stop",
            "url": "http://api:8080",
            "token-file": "/identity/token",
            "namespace": rows[0]["namespace"],
            "run-id": run,
            "seed": SEED,
            "arm": "short",
            "rate": float(ARRIVAL_RATE),
            "count": len(rows),
            "concurrency": 16,
            "output": JOURNAL,
        }
        self.args = [binary, "-k4", *(f"-{key}={value}" for key, value in options.items())]
        self.process = None
        self.client_started_ns = None
        self.client_exit_seen_monotonic_ns = None
        self.exit_record = None
        self.exit_seen_ns = None
        self.exit_seen_monotonic_ns = None
        self.next_exit_probe_ns = 0

    def writer(self, path, body):
        self.driver.k(
            "-n",
            APP,
            "exec",
            "-i",
            self.observer,
            "--",
            "python",
            "-c",
            "import pathlib,sys; pathlib.Path(sys.argv[1]).write_bytes(sys.stdin.buffer.read())",
            path,
            data=body,
            record=False,
            timeout=10,
        )

    @contextmanager
    def run(self):
        with (self.driver.output / "sender.log").open("wb") as log:
            try:
                self.writer("/evidence/j3-stop", utc_seconds(self.stop) + "\n")
                self.client_started_ns = time.time_ns()
                self.process = subprocess.Popen(
                    [
                        "kubectl",
                        "-n",
                        APP,
                        "exec",
                        self.observer,
                        "--",
                        "python",
                        "-c",
                        GUARD_SENDER,
                        JOURNAL,
                        ABORT,
                        "/evidence/j3-armed",
                        SENDER_EXIT,
                        *self.args,
                    ],
                    stdout=log,
                    stderr=log,
                    env=self.driver.env,
                )
                yield
                require(self.exit_record is not None, "sender completion was not witnessed")
                self.finish_client()
                self.export()
            except BaseException as error:
                with suppress(Exception):
                    self.retain_failure(error)
                with suppress(Exception):
                    self.abort()
                if self.exit_record is None:
                    with suppress(Exception):
                        self.read_exit(probe_name="sender-exit-probe-after-abort.json")
                with suppress(Exception):
                    self.export()
                raise

    def observer_exec(self, code, *args):
        return self.driver.k(
            "-n",
            APP,
            "exec",
            self.observer,
            "--",
            "python",
            "-c",
            code,
            *args,
            allowed=True,
            record=False,
            timeout=5,
        )

    def client_status(self):
        if self.process is None:
            return {"started_ns": self.client_started_ns, "pid": None, "alive": False}
        status = self.process.poll()
        return {
            "started_ns": self.client_started_ns,
            "pid": self.process.pid,
            "alive": status is None,
            "returncode": status,
        }

    def retain_failure(self, error):
        evidence = {
            "at_ns": time.time_ns(),
            "reason": str(error),
            "observer_pod": self.observer,
            "local_client": self.client_status(),
        }
        try:
            result = self.observer_exec(OBSERVER_PROCESSES)
            evidence["observer_exec_returncode"] = result.returncode
            raw = result.stdout if result.returncode == 0 else result.stderr
            for key in ("password", "demand_token", "monitor_password", "fixture_password"):
                if value := getattr(self.driver, key, None):
                    raw = raw.replace(value, "[REDACTED]")
            if result.returncode == 0:
                evidence["observer_processes"] = json.loads(raw)
            else:
                evidence["observer_exec_error"] = raw
        except (OSError, ValueError) as capture_error:
            evidence["observer_exec_error"] = str(capture_error)
        append(self.driver.output / "sender-process-evidence.jsonl", evidence)

    def read_exit(self, *, probe_name="sender-exit-probe.json"):
        probe = {"at_ns": time.time_ns()}
        record = None
        try:
            result = self.observer_exec(READ_SENDER_EXIT, SENDER_EXIT, JOURNAL)
            probe.update(returncode=result.returncode, stderr=result.stderr)
            if result.returncode == 0:
                reply = json.loads(result.stdout)
                probe.update(reply)
                record = json.loads(reply["record"]) if reply["record"] is not None else None
        except (KeyError, OSError, TypeError, ValueError) as error:
            probe["error"] = str(error)
        kind.save(self.driver.output / probe_name, probe)
        if (
            isinstance(record, dict)
            and type(record.get("returncode")) is int
            and type(record.get("closed")) is bool
            and type(record.get("exited_ns")) is int
        ):
            kind.save(self.driver.output / "sender-exit.json", record)
            return record, probe.get("journal_last_line")
        return None, None

    def terminate_client(self):
        if self.process.poll() is not None:
            return
        with suppress(ProcessLookupError):
            self.process.terminate()
        try:
            self.process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            with suppress(ProcessLookupError):
                self.process.kill()
            self.process.wait(timeout=2)

    def retire_lingering_client(self):
        evidence = {
            "event": "exec_stream_lingering",
            "grace_seconds": EXEC_STREAM_GRACE_SECONDS,
            "sender_exited_ns": self.exit_record["exited_ns"],
            "exit_observed_ns": self.exit_seen_ns,
            "termination_started_ns": time.time_ns(),
            "client_before": self.client_status(),
        }
        path = self.driver.output / "exec-stream-lingering.json"
        kind.save(path, evidence)
        try:
            self.terminate_client()
        finally:
            evidence.update(termination_ended_ns=time.time_ns(), client_after=self.client_status())
            kind.save(path, evidence)

    def finish_client(self):
        if self.process.poll() is None:
            remaining = (
                EXEC_STREAM_GRACE_SECONDS
                - (time.monotonic_ns() - self.exit_seen_monotonic_ns) / SECOND
            )
            try:
                self.process.wait(timeout=max(remaining, 0))
            except subprocess.TimeoutExpired:
                self.retire_lingering_client()

    def abort(self):
        if self.process is None:
            return
        try:
            if self.exit_record is None:
                self.writer(ABORT, "")
                self.process.wait(timeout=10)
        finally:
            self.terminate_client()

    def export(self):
        result = self.driver.k(
            "-n",
            APP,
            "exec",
            self.observer,
            "--",
            "cat",
            JOURNAL,
            allowed=True,
            record=False,
            timeout=30,
        )
        (self.driver.output / "target-journal.jsonl").write_text(result.stdout, encoding="utf-8")
        require(result.returncode == 0, "sender journal unavailable")

    def check_running(self):
        tick = time.monotonic_ns()
        client_status = self.process.poll()
        if self.exit_record is None:
            # A local exit prompts an early probe. It cannot prove the sender's result.
            if client_status is not None and self.client_exit_seen_monotonic_ns is None:
                self.client_exit_seen_monotonic_ns = tick
            if (
                client_status is None and time.time_ns() < self.stop
            ) or tick < self.next_exit_probe_ns:
                return False
            self.next_exit_probe_ns = tick + SECOND
            record, last = self.read_exit()
            if record is None:
                if self.client_exit_seen_monotonic_ns is not None:
                    require(
                        time.monotonic_ns() - self.client_exit_seen_monotonic_ns
                        < EXEC_STREAM_GRACE_SECONDS * SECOND,
                        "sender exited without an exit record",
                    )
                return False
            require(record["returncode"] == 0, f"sender exited with status {record['returncode']}")
            require(
                record["closed"] and json.loads(last or "{}") == {"event": "closed"},
                "sender exit record lacks a closed journal",
            )
            self.exit_record = record
            self.exit_seen_ns = time.time_ns()
            self.exit_seen_monotonic_ns = time.monotonic_ns()
        elif (
            client_status is None
            and tick - self.exit_seen_monotonic_ns >= EXEC_STREAM_GRACE_SECONDS * SECOND
        ):
            self.retire_lingering_client()
        return True

    def completed(self):
        try:
            return self.reconcile_journal()
        except Exception as error:
            with suppress(Exception):
                self.retain_failure(error)
            raise

    def reconcile_journal(self):
        require(self.exit_record is not None, "sender completion was not witnessed")
        entries = list(jsonl(self.driver.output / "target-journal.jsonl"))
        require(
            entries and entries[0]["event"] == "plan" and entries[-1]["event"] == "closed",
            "sender journal lacks a complete plan and close marker",
        )
        sender_plan = entries[0]["plan"]["rows"]
        planned = unique(self.rows, lambda row: row["submission_key"])
        keys = unique(sender_plan, lambda row: row["key"])
        require(keys.keys() == planned.keys(), "sender plan identity differs")
        sent = {}
        accepted = []
        for row in entries[1:-1]:
            key = row.get("key")
            require(
                row["event"] == "arrival"
                and key in planned
                and row["workflow"] == key
                and row["attempts"] == 1
                and row["sent"]
                and utc_ns(row["scheduled"]) == planned[key]["scheduled_ns"]
                and row["request_hash"] == sent_hash(keys[key]),
                "sender entry differs from plan or missed its send",
            )
            if row["outcome"] == "uncertain":
                require(key not in sent and row["status"] == 0 and not row["error"], "sender miss")
                sent[key] = row["sent"]
            else:
                require(
                    row["outcome"] == "accepted"
                    and 200 <= row["status"] < 300
                    and row["sent"] == sent.get(key)
                    and not row["error"],
                    "unexpected sender outcome",
                )
                accepted.append(
                    {
                        "namespace": planned[key]["namespace"],
                        "submission_key": key,
                        "workflow_id": row["workflow"],
                        "outcome": "accepted",
                    }
                )
        by_key = unique(accepted, lambda row: row["submission_key"])
        require(by_key.keys() == sent.keys() == planned.keys(), "missing sender acceptance")
        for i, row in enumerate(sender_plan):
            at = planned[row["key"]]["scheduled_ns"]
            upper = self.rows[i + 1]["scheduled_ns"] if i + 1 < len(self.rows) else self.stop
            require(
                row["sequence"] == i
                and row["workflow_id"] == row["key"]
                and utc_ns(row["scheduled_at"]) == at
                and at <= utc_ns(sent[row["key"]]) < upper,
                "sender missed the absolute schedule",
            )
        return accepted, sender_plan


def sent_hash(row):
    # The K4 request is a compact Go RawMessage. Compare the original bytes
    # against its journal hash using the same JSON encoding used by the sender.
    return hashlib.sha256(
        json.dumps(row["request"], ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def process_signature(pods, roles):
    signature = {}
    for pod in pods:
        metadata, status = pod["metadata"], pod["status"]
        role = metadata["labels"].get("app")
        if role not in roles:
            continue
        require(
            not metadata.get("deletionTimestamp")
            and status.get("containerStatuses")
            and all(
                c["restartCount"] == 0 and "running" in c["state"] and c["ready"]
                for c in status["containerStatuses"]
            ),
            f"process gap in {role}",
        )
        signature[metadata["uid"]] = (
            role,
            tuple(c["containerID"] for c in status["containerStatuses"]),
        )
    require({r for r, _ in signature.values()} == roles, "application or telemetry pod missing")
    return signature


class Sampler:
    def __init__(self, driver, pins, arm):
        self.driver, self.pins = driver, set(pins)
        self.arm = arm
        self.origin = None
        self.clock_offset = time.time_ns() - time.monotonic_ns()
        self.shutdown = Event()
        self.thread = Thread(target=self.loop, daemon=True)
        self.error = None
        self.latest = None
        self.roles = {
            "api",
            "operator",
            "worker-control",
            "scheduler",
            "relay",
            "demand",
            "observer",
        }
        self.inventories = {}
        self.templates = {}

    def pods(self):
        selector = ",".join(sorted(self.roles | self.pins | {"worker"}))
        result = self.driver.k(
            "get",
            "pods",
            "--all-namespaces",
            "-l",
            f"app in ({selector})",
            "-o",
            "json",
            record=False,
            timeout=5,
        )
        pods = json.loads(result.stdout)["items"]
        return [
            [p for p in pods if p["metadata"]["namespace"] == namespace] for namespace in (APP, NS)
        ]

    def sample(self):
        app, telemetry = self.pods()
        processes = process_signature(app, self.roles) | process_signature(telemetry, self.pins)
        append(self.driver.output / "processes.jsonl", {"at_ns": time.time_ns(), "pods": processes})
        require(processes == self.processes, "application or telemetry process was replaced")
        workers = [p for p in app if p["metadata"]["labels"].get("app") == "worker"]
        (observer,) = [
            p["metadata"]["name"] for p in app if p["metadata"]["labels"].get("app") == "observer"
        ]
        (receiver,) = [
            p["metadata"]["name"]
            for p in telemetry
            if p["metadata"]["labels"].get("app") == RECEIVER
        ]
        sample = sample_ready_slots(
            self.driver, APP, NS, RECEIVER, workers, observer_pod=observer, receiver_pod=receiver
        )
        require(
            self.latest is None
            or 0 < sample["sample_ns"] - self.latest["sample_ns"] <= SLOT_MAX_GAP,
            "Ready-slot sampling gap",
        )
        for pod in sample["pods"]:
            uid = pod["uid"]
            for c in pod["containers"]:
                require(
                    c["restartCount"] == 0
                    and (
                        not c.get("imageID")
                        or c["imageID"].endswith(self.driver.images["worker"].split("@")[1])
                    ),
                    f"worker process restart or image change: {uid}",
                )
                if container_id := c.get("containerID"):
                    require(
                        self.templates.setdefault(uid, container_id) == container_id,
                        f"worker process replaced: {uid}",
                    )
        for uid, inv in sample["witness"].items():
            prev = self.inventories.get(uid, inv)
            require(
                prev["session"] == inv["session"] and prev["written"] <= inv["written"],
                f"worker witness process gap: {uid}",
            )
            self.inventories[uid] = inv
        self.latest = sample
        now = time.time_ns()
        require(
            abs(now - time.monotonic_ns() - self.clock_offset) < SECOND,
            "runner clock discontinuity",
        )
        if self.origin is not None and self.origin <= now:
            self.check_live_rules(now)

    def check_live_rules(self, now):
        # Prometheus commits each recording rule separately. Ignore the latest second
        # of samples so a normal in-progress evaluation cannot mix rule timestamps.
        at_ns = now - RULE_SETTLE_SECONDS * SECOND
        at = (at_ns // 1_000_000) / 1_000  # PromQL evaluates at millisecond precision.
        expected = prom("timestamp(j3:expected) and (time() - timestamp(j3:expected) <= 6)", at=at)
        groups = json_request(
            19090,
            "/api/v1/rules",
            **{"rule_group[]": "j3", "file[]": "/etc/j3/rules.yml", "rule_name[]": "j3:expected"},
        )["data"]["groups"]
        require(len(groups) == 1, "missing live J3 rule group status")
        group = groups[0]
        duration = group["evaluationTime"]
        last_evaluation = utc_ns(group["lastEvaluation"])
        overrun = "rule evaluation overran the settle margin"
        require(0 <= duration < RULE_SETTLE_SECONDS, overrun)
        require(expected and last_evaluation > 0, "live rule evaluation gap")
        # A new marker without a completed group is itself evidence of an overrun
        # at this cutoff. evaluationTime describes the last finished group.
        require(all(epoch_ns(row["value"][1]) <= last_evaluation for row in expected), overrun)
        blocked = self.arm == "scrape_loss" and 600 <= (at_ns - self.origin) / SECOND < 670
        if not blocked:
            require(
                bool(
                    prom(
                        "(j3:valid == 1) and (timestamp(j3:valid) == timestamp(j3:expected))", at=at
                    )
                ),
                "live scoped sample gap",
            )

    def loop(self):
        try:
            while not self.shutdown.is_set():
                started = time.monotonic()
                cpu = os.times()
                try:
                    self.sample()
                finally:
                    after = os.times()
                    append(
                        self.driver.output / "sampler-cost.jsonl",
                        {
                            "at_ns": time.time_ns(),
                            "wall_s": time.monotonic() - started,
                            "parent_cpu_s": after.user + after.system - cpu.user - cpu.system,
                            "child_cpu_s": (
                                after.children_user
                                + after.children_system
                                - cpu.children_user
                                - cpu.children_system
                            ),
                        },
                    )
                self.shutdown.wait(max(0, SLOT_INTERVAL - (time.monotonic() - started)))
        except Exception as error:
            self.error = error

    def check(self):
        if self.error:
            raise self.error

    def __enter__(self):
        app, telemetry = self.pods()
        self.processes = process_signature(app, self.roles) | process_signature(
            telemetry, self.pins
        )
        self.thread.start()
        return self

    def __exit__(self, error_type, error, traceback):
        self.shutdown.set()
        self.thread.join(timeout=20)
        if error_type is None:
            require(not self.thread.is_alive(), "slot sampler did not stop")
            self.check()


def duration_witness(driver):
    query = f"""
WITH target AS MATERIALIZED (
    SELECT workflow_id, revision, state FROM engine.workflow_executions WHERE {SCOPE}
), counts AS (
    SELECT count(*) AS admitted, count(*) FILTER (WHERE state='SUCCEEDED') AS succeeded,
        count(*) FILTER (WHERE state IN ({TERMINAL}) AND state <> 'SUCCEEDED') AS failed,
        md5(coalesce(string_agg(workflow_id || ':' || revision, ',' ORDER BY workflow_id), ''))
          AS revision_hash
    FROM target
)
SELECT to_jsonb(counts) || jsonb_build_object(
    'at_ns', (extract(epoch FROM statement_timestamp())*1000000000)::bigint,
    'unpublished', (SELECT count(*) FROM engine.outbox JOIN target USING (workflow_id)
                    WHERE published_at IS NULL),
    'active_claims', (SELECT count(*) FROM engine.activity_attempts JOIN target USING (workflow_id)
                      WHERE activity_attempts.state='CLAIMED' AND is_current))
FROM counts;
"""
    # Qualify counters and revision fingerprint in one read-only snapshot.
    return json.loads(driver.query("BEGIN READ ONLY; " + query + " COMMIT;"))


def backlog(driver):
    row = json.loads(
        driver.query(f"""
SELECT jsonb_build_object(
    'sample_ns', (extract(epoch FROM statement_timestamp())*1000000000)::bigint,
    'workflow_ids', coalesce(jsonb_agg(workflow_id ORDER BY workflow_id), '[]')
) FROM engine.workflow_executions WHERE {SCOPE} AND state NOT IN ({TERMINAL});
""")
    )
    kind.save(driver.output / "restore-backlog.json", row)
    require(row["workflow_ids"], "missing positive restore backlog")
    return row


def outage_commits(driver, origin, down, restore, snapshot):
    rows = json.loads(
        driver.query(f"""
SELECT coalesce(jsonb_agg(jsonb_build_object(
    'workflow_id', w.workflow_id, 'state', w.state,
    'admitted_ns', {COMMIT_NS % "a"}, 'terminal_ns', {COMMIT_NS % "h"}
) ORDER BY w.workflow_id), '[]')
FROM engine.workflow_executions w
LEFT JOIN engine.transition_history a ON a.workflow_id=w.workflow_id AND a.revision=1
LEFT JOIN engine.transition_history h ON h.workflow_id=w.workflow_id AND h.revision=w.revision
    AND h.new_state IN ({TERMINAL})
WHERE w.{SCOPE};
""")
    )
    evidence = {
        "origin_ns": origin,
        "scale_down_ns": down,
        "restore_ns": restore,
        "backlog": snapshot,
        "admissions": rows,
        "observed_through_ns": time.time_ns(),
    }
    kind.save(driver.output / "outage.json", evidence)
    return evidence


def wait_at(at_ns, check):
    remaining = max(0, at_ns - time.time_ns()) / SECOND
    deadline = time.monotonic() + remaining
    while (remaining := deadline - time.monotonic()) > 0:
        check()
        time.sleep(min(remaining, 0.2))
    check()


def scale(driver, replicas, at, check):
    wait_at(at, check)
    started = time.time_ns()
    driver.k("-n", APP, "scale", "deployment/worker", f"--replicas={replicas}", timeout=10)
    append(
        driver.output / "faults.jsonl",
        {
            "replicas": replicas,
            "planned_ns": at,
            "command_ns": started,
            "returned_ns": time.time_ns(),
        },
    )
    require(abs(started - at) <= 5 * SECOND, "worker scale command missed its bound")
    return started


def scrape_access(driver, route, allow, at, check):
    wait_at(at, check)
    source, destination = route
    action = ["-D", "FORWARD"] if allow else ["-I", "FORWARD", "1"]
    # Put the port-specific drop ahead of established-connection accepts.
    command = [
        "docker",
        "exec",
        kind.CLUSTER + "-control-plane",
        "iptables",
        "-w",
        "1",
        *action,
        "-s",
        source,
        "-d",
        destination,
        "-p",
        "tcp",
        "--dport",
        "9187",
        "-j",
        "DROP",
    ]
    started = time.time_ns()
    driver.command(command, timeout=5)
    append(
        driver.output / "faults.jsonl",
        {
            "allow_scrape": allow,
            "planned_ns": at,
            "command_ns": started,
            "returned_ns": time.time_ns(),
            "argv": command,
        },
    )
    require(abs(started - at) <= SECOND, "scrape command missed its bound")
    return started


def drain(driver, sender, samples, deadline, check):
    quiet_start = None
    fingerprint = None
    while time.time_ns() < deadline:
        check()
        sender_complete = sender.check_running()
        observed = duration_witness(driver)
        require(observed["failed"] == 0, "unsuccessful durable work")
        slots = samples.latest
        require(slots is not None, "missing drain worker sample")
        inventories = slots["witness"]
        complete = (
            sender_complete
            and observed["admitted"] == observed["succeeded"] == len(sender.rows)
            and observed["active_claims"] == observed["unpublished"] == 0
            and len(slots["slots"]) == 4
        )
        current = (observed["revision_hash"], inventories)
        if not complete or quiet_start is None or current != fingerprint:
            quiet_start = observed["at_ns"] if complete else None
            fingerprint = current
        append(
            driver.output / "quiet-reads.jsonl",
            {
                **observed,
                "witness": inventories,
                "slots_sample_ns": slots["sample_ns"],
                "quiet_start_ns": quiet_start,
            },
        )
        if (
            quiet_start is not None
            and observed["at_ns"] - quiet_start >= 30 * SECOND
            and observed["at_ns"] <= deadline
        ):
            result = {
                "quiet_start_ns": quiet_start,
                "quiet_end_ns": observed["at_ns"],
                "quiet_seconds": (observed["at_ns"] - quiet_start) / SECOND,
                "bound_ns": deadline,
                "admitted": observed["admitted"],
                "unpublished": observed["unpublished"],
                "active_claims": observed["active_claims"],
                "revision_hash": observed["revision_hash"],
                "witness": inventories,
            }
            kind.save(driver.output / "quiet-drain.json", result)
            return result
        time.sleep(1)
    require(sender.exit_record is not None, "sender exit evidence missing at the drain deadline")
    raise EpisodeFailure("drain did not reach 30 seconds quiet within 600 seconds")


def worker_ledgers(driver, inventories, latest):
    # The observer has read-only access to the existing worker PVC, including
    # retired pods. Their last live inventories authenticate the retained prefix.
    code = """import json,pathlib
root=pathlib.Path('/j3-witness')
print(json.dumps({p.relative_to(root).as_posix():p.read_text()
                  for p in root.glob('*/*.jsonl')}))
"""
    files = json.loads(driver.python(APP, "observer", code, record=False))
    expected = {f"{inv['pod_name']}/{inv['file']}": inv for inv in inventories.values()}
    require(files.keys() == expected.keys(), "missing or unexpected worker witness sessions")
    events = []
    for name, raw in files.items():
        inv = expected[name]
        lines = raw.splitlines(keepends=True)
        require(
            raw.endswith("\n")
            and 0 < inv["written"] <= len(lines)
            and inv["errors"] == 0
            and inv["attempted"] == inv["written"]
            and hashlib.sha256("".join(lines[: inv["written"]]).encode()).hexdigest()
            == inv["sha256"],
            f"invalid retained worker witness: {name}",
        )
        if inv["pod_uid"] in latest:
            require(
                len(lines) == inv["written"], f"live worker witness changed after quiet: {name}"
            )
        for number, line in enumerate(lines, 1):
            row = json.loads(line)
            require(
                row["sequence"] == number
                and row["session"] == inv["session"]
                and row["pod_uid"] == inv["pod_uid"]
                and row["pod_name"] == inv["pod_name"],
                f"worker witness sequence or identity gap: {name}",
            )
            if "claim_token" in row:
                row["token_hash"] = hashlib.sha256(row.pop("claim_token").encode()).hexdigest()
            events.append(row)
    kind.save(driver.output / "slot-ledger.json", {"inventories": inventories, "events": events})
    return events


class CheckerError(ValueError):
    def __init__(self, outcome):
        self.outcome = outcome
        super().__init__(f"checker outcome: {outcome}")


def redact_tokens(value):
    if isinstance(value, list):
        return [redact_tokens(row) for row in value]
    if isinstance(value, dict):
        return {
            key: hashlib.sha256(item.encode()).hexdigest()
            if item and key in {"claim_token", "ClaimToken"}
            else redact_tokens(item)
            for key, item in value.items()
        }
    return value


def check_witness_claims(attempts, events):
    durable = unique((row for row in attempts if row["token_hash"]), attempt_key)
    starts, ends = (
        unique((r for r in events if r["event"] == event), lambda r: r["invocation_id"])
        for event in ("start", "end")
    )
    require(starts.keys() == ends.keys(), "full-volume worker witness has an unpaired invocation")
    seen = set()
    for invocation, start in starts.items():
        end = ends[invocation]
        key = attempt_key(start)
        attempt = durable[key]
        require(
            key not in seen
            and key == attempt_key(end)
            and start["token_hash"] == end["token_hash"] == attempt["token_hash"]
            and start["worker_id"] == end["worker_id"] == attempt["worker_request_id"].split(":")[0]
            and start["session"] == end["session"]
            and start["monotonic_ns"] <= end["monotonic_ns"],
            f"full-volume worker witness differs from durable claim: {invocation}",
        )
        seen.add(key)
    require(seen == durable.keys(), "full-volume durable claim missing its worker witness")
    return len(seen)


def check_full_volume(driver, data, origins, quiet, events):
    namespace = data["namespace"]
    path = driver.output / "checker.json"
    try:
        checked = run_checker(
            driver,
            kind.CLUSTER + "-control-plane",
            APP,
            driver.pod(APP, "observer"),
            "/checker",
            ["-namespace", namespace, "-require-terminal"],
        )
    except Exception as error:
        raise CheckerError(checker_outcome(driver.output)) from error
    finally:
        if path.exists():
            raw = path.read_bytes()
            digest = hashlib.sha256(raw).hexdigest()
            try:
                sanitized = redact_tokens(json.loads(raw))
            except (TypeError, ValueError):
                sanitized = {"incomplete_json": True, "unredacted_bytes": len(raw)}
            kind.save(path, {**sanitized, "unredacted_sha256": digest})
    require(checked["valid"], "invalid full-volume invariant checker verdict")
    trace = checked["trace"]
    ids = checked["workflow_ids"]
    accepted = [r["workflow_id"] for r in data["journal"]]
    admitted = [r["WorkflowID"] for r in trace["Submissions"] if r["Namespace"] == namespace]
    succeeded = [
        r["workflow_id"]
        for r in data["workflows"]
        if r["namespace"] == namespace and r["state"] == "SUCCEEDED"
    ]
    result = drain_identities(admitted, accepted, succeeded, ids)
    checked_successes = [r["WorkflowID"] for r in trace["Workflows"] if r["State"] == "SUCCEEDED"]
    kind.save(
        driver.output / "identity-reconciliation.json",
        {
            **result,
            "admitted_count": len(admitted),
            "checked_count": len(ids),
            "checked_successes": len(checked_successes),
        },
    )
    require(
        result["valid"]
        and len(ids) == quiet["admitted"]
        and sorted(ids) == sorted(checked_successes)
        and all(
            r["Namespace"] == namespace and r["Key"] == r["WorkflowID"]
            for r in trace["Submissions"]
        ),
        "checker and durable drain identities differ",
    )
    expected = {(r["workflow_id"], r["activity"]): r["expected"] for r in origins}
    actual = unique(
        (
            r
            for r in trace["Snapshot"]["engine.node_instances"]["rows"]
            if (r["workflow_id"], r["node_id"]) in expected
        ),
        lambda r: (r["workflow_id"], r["node_id"]),
    )
    require(
        actual.keys() == expected.keys()
        and all(
            json.dumps(actual[key]["accepted_result"], sort_keys=True)
            == json.dumps(wanted, sort_keys=True)
            for key, wanted in expected.items()
        ),
        "checker snapshot differs from expected activity results",
    )
    claims = check_witness_claims(data["attempts"], events)
    kind.save(
        driver.output / "identity-drain.json",
        {
            **result,
            **quiet,
            "expected_results": len(expected),
            "checked": len(ids),
            "witnessed_claims": claims,
        },
    )


def verify_outage(evidence, evaluations, slot_samples, require_catchup=None):
    origin, down, restore = (evidence[k] for k in ("origin_ns", "scale_down_ns", "restore_ns"))
    latest_zero = origin + 660 * SECOND
    zero = next(
        (
            row["sample_ns"]
            for row in slot_samples
            if down <= row["sample_ns"] <= latest_zero and not row["pods"]
        ),
        None,
    )
    require(zero is not None, "zero-worker witness missing by 660 s")
    zero_rows = [row for row in slot_samples if zero <= row["sample_ns"] < restore]
    require(
        zero_rows and all(not row["pods"] and not row["slots"] for row in zero_rows),
        "worker pod or slot remained after confirmed outage",
    )
    old_workers = {p["uid"] for r in slot_samples if r["sample_ns"] < down for p in r["pods"]}
    ready_limit = origin + 960 * SECOND
    ready = next(
        (
            row
            for row in slot_samples
            if restore < row["sample_ns"] <= ready_limit and len(row["slots"]) == 4
        ),
        None,
    )
    require(
        len(old_workers) == 2
        and ready is not None
        and not old_workers.intersection(ready["witness"]),
        "two restored worker processes were not witnessed by 960 s",
    )
    rows = evidence["admissions"]
    require(
        all(type(row["admitted_ns"]) is int and type(row["terminal_ns"]) is int for row in rows),
        "missing durable timestamp in outage evidence",
    )
    require(
        any(zero <= r["admitted_ns"] < restore for r in rows)
        and not any(zero <= r["terminal_ns"] < restore for r in rows)
        and any(restore < r["terminal_ns"] <= ready_limit for r in rows),
        "missing continued commits, stalled progress or restored progress",
    )
    require(
        any(
            zero <= row["at_ns"] < restore and row["claimable_attempts"] > 0 for row in evaluations
        ),
        "positive outage demand witness missing",
    )
    require(
        any(
            zero + 5 * SECOND <= r["at_ns"] < restore and r.get("ready_slots") == 0
            for r in evaluations
        )
        and any(
            ready["sample_ns"] < r["at_ns"] <= ready_limit + 5 * SECOND
            and r.get("ready_slots") == 4
            for r in evaluations
        ),
        "missing scraped zero or restored Ready-slot witness",
    )
    require_behavior(
        all(not r["firing"] for r in evaluations if r["at_ns"] < down)
        and all("ScrapeAbsent" not in r["firing"] for r in evaluations),
        "unexpected alert outside the worker outage",
    )
    for name, limit in zip(DOMAIN, (700, 765), strict=True):
        first = next((r["at_ns"] for r in evaluations if name in r["firing"]), None)
        near_restore = [r for r in evaluations if restore - 5 * SECOND <= r["at_ns"] < restore]
        require_behavior(
            first is not None
            and down <= first <= origin + limit * SECOND
            and len(near_restore) == 1
            and name in near_restore[0]["firing"],
            f"{name} failed its outage bounds",
        )
    if require_catchup is not None:
        end = origin + require_catchup * SECOND
        require_behavior(
            all(r["terminal_ns"] < end for r in rows if r["admitted_ns"] <= restore)
            and any(
                restore < r["at_ns"] < end and r["oldest_claimable_age_seconds"] <= 28.540
                for r in evaluations
            ),
            "sealed pre-restore catch-up bound missed",
        )
    return {
        "zero_workers_ns": zero,
        "restored_ready_ns": ready["sample_ns"],
        "first_restored_progress_ns": min(
            r["terminal_ns"] for r in rows if r["terminal_ns"] > restore
        ),
        "worker_uids_before": sorted(old_workers),
        "restored_worker_uids": sorted(ready["witness"]),
    }
