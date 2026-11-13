"""Durable K4 streams and pure mapping functions. Evidence gaps fail closed."""

from __future__ import annotations

import base64
import hashlib
import json
import shlex
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path

from dur053_k3 import CLUSTER, save
from dur053_k3_policy import require
from dur053_k4_interventions import ENGINE_ROLES, empty
from dur053_k4_schema import SESSION


def utc(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def history_kind(row):
    """Preserve receipt/replacement semantics without inventing an owner."""
    reason = row["reason"].lower()
    if row["reason"] == "ATTEMPT_RESULT_RECORDED":
        return "receipt"
    if "cancel" in reason:
        return "cancel"
    if "replac" in reason or "timeout" in reason:
        return "supersede"
    if not row["owner"]:
        return "claim"
    if row.get("new_state") in {"SUCCEEDED", "FAILED", "CANCELED", "ABANDONED", "REJECTED"}:
        return "terminal"
    return "transition"


def structured_log(text):
    """Accept both Go slog JSON and its default structured text encoding."""
    _, _, body = text.partition(" ")  # kubectl's source timestamp
    try:
        row = json.loads(body)
    except ValueError:
        try:
            row = dict(part.split("=", 1) for part in shlex.split(body) if "=" in part)
        except ValueError:
            return None
    data = row.get("data")
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except ValueError:
            return None
    return data if isinstance(data, dict) else None


def apply_process_evidence(sessions, logs, events):
    for _, _, text in logs:
        row = structured_log(text)
        if not row or row.get("event") != "k4_signal":
            continue
        matches = [
            s
            for s in sessions
            if s["pod_uid"] == row["pod_uid"]
            and s["start"] <= utc(row["timestamp"]) <= (s["death"] or float("inf"))
        ]
        require(len(matches) == 1, "k4-signal-container-ambiguous")
        matches[0]["signals"].append(
            dict(at=utc(row["timestamp"]), signal=row["signal"], evidence="persistent-process-log")
        )
    for _, _, text in events:
        if "{" not in text:
            continue
        raw = json.loads(text[text.index("{") :])
        cid = raw.get("container_id", raw.get("containerID", ""))
        status = raw.get("exit_status", raw.get("exitStatus"))
        exited = raw.get("exited_at", raw.get("exitedAt"))
        if isinstance(exited, dict):
            at = int(exited["seconds"]) + int(exited.get("nanos", 0)) / 1e9
        elif isinstance(exited, str):
            at = utc(exited)
        else:
            at = utc(text.split()[0])
        for s in sessions:
            if s["container_id"].rsplit("/", 1)[-1] == cid and status is not None:
                s.update(
                    death=at,
                    death_evidence="containerd-task-exit:" + cid,
                    exit_code=int(status),
                    exit_signal=9 if status == 137 else None,
                )
    return sessions


def journal_rows(raw):
    lines = raw.splitlines(keepends=True)
    rows, torn = [], False
    for i, line in enumerate(lines):
        try:
            rows.append(json.loads(line))
        except ValueError:
            require(i == len(lines) - 1 and not line.endswith("\n"), "k4-arrival-journal-corrupt")
            torn = True
    require(rows and rows[0].get("event") == "plan", "k4-arrival-plan-missing")
    plan = rows[0]["plan"]
    updates = {}
    for row in rows[1:]:
        if row.get("event") == "arrival":
            require(row["key"] in {p["key"] for p in plan["rows"]}, "k4-arrival-foreign-key")
            updates[row["key"]] = row
    return plan, updates, torn


def reconciled(now, *, all_terminal, witness_available, last_activity, deadline):
    require(witness_available, "k4-witness-unavailable")
    if all_terminal and now - last_activity >= 30:
        return "complete"
    return "timeout" if now >= deadline else "waiting"


class Streams:
    """Each stream owns files and a process. No deleted-pod re-fetch is needed."""

    def __init__(self, driver, ns, output):
        self.d, self.ns, self.output = driver, ns, output
        self.output.mkdir(exist_ok=True)
        self.processes = []
        self.logs = {}
        self.errors = []
        self.stopped = False
        self.lock = threading.RLock()
        self.threads = []
        self.pods = []
        self.endpoints = []

    def start(self, label, args, callback=None):
        path = self.output / (label + ".jsonl")
        err = (self.output / (label + ".stderr")).open("wb")
        proc = subprocess.Popen(
            args,
            cwd=Path(__file__).resolve().parents[1],
            env=self.d.env,
            stdout=subprocess.PIPE,
            stderr=err,
        )
        self.processes.append((proc, err, label))

        def pump():
            try:
                with path.open("a", encoding="utf-8", newline="\n") as handle:
                    pending = ""
                    decoder = json.JSONDecoder()
                    for line in iter(proc.stdout.readline, b""):
                        received = time.time()
                        text = line.decode("utf-8", errors="strict").rstrip("\r\n")
                        if callback:
                            pending += text + "\n"
                            try:
                                obj, consumed = decoder.raw_decode(pending.lstrip())
                            except ValueError:
                                continue
                            require(not pending.lstrip()[consumed:].strip(), "k4-watch-framing")
                            text = json.dumps(obj)
                            pending = ""
                        envelope = dict(received=received, source=text)
                        handle.write(json.dumps(envelope) + "\n")
                        handle.flush()
                        if callback:
                            callback(text, received)
                if pending.strip():
                    self.errors.append(label + ": truncated watch object")
                code = proc.wait()
                if not self.stopped:
                    self.errors.append(f"{label}: unexpected stream EOF exit {code}")
            except Exception as error:
                self.errors.append(label + ": " + str(error))

        t = threading.Thread(target=pump, daemon=True)
        t.start()
        self.threads.append(t)

    def observe_pod(self, text, received):
        event = json.loads(text)
        require(event.get("type") in {"ADDED", "MODIFIED", "DELETED"}, "k4-pod-watch-error")
        p = event["object"]
        role = p["metadata"].get("labels", {}).get("app")
        with self.lock:
            self.pods.append(dict(received=received, type=event["type"], object=p))
        if role in ENGINE_ROLES:
            for c in p.get("status", {}).get("containerStatuses", []):
                cid = c.get("containerID")
                if not cid:
                    continue
                key = (p["metadata"]["uid"], cid)
                with self.lock:
                    if key in self.logs:
                        continue
                    self.logs[key] = dict(pod=p["metadata"]["name"], container=c["name"], role=role)
                self.start(
                    "log-" + key[0] + "-" + cid.rsplit("/", 1)[-1],
                    [
                        "kubectl",
                        "--context",
                        "kind-" + CLUSTER,
                        "-n",
                        self.ns,
                        "logs",
                        "-f",
                        "--timestamps",
                        p["metadata"]["name"],
                        "-c",
                        c["name"],
                    ],
                )

    def observe_endpoint(self, text, received):
        event = json.loads(text)
        require(event.get("type") in {"ADDED", "MODIFIED", "DELETED"}, "k4-endpoint-watch-error")
        with self.lock:
            self.endpoints.append(dict(received=received, **event))

    def start_watch(self, resource):
        # jsonpath emits each watch object as a single JSON line, including
        # source timestamps and resourceVersion. No polling replaces this watch.
        current = json.loads(
            self.d.k("-n", self.ns, "get", resource, "-o", "json", record=False).stdout
        )
        callback = self.observe_pod if resource == "pods" else self.observe_endpoint
        for obj in current["items"]:
            callback(json.dumps({"type": "ADDED", "object": obj}), time.time())
        self.start(
            resource,
            [
                "kubectl",
                "--context",
                "kind-" + CLUSTER,
                "-n",
                self.ns,
                "get",
                resource,
                "--watch-only",
                "--output-watch-events",
                "-o",
                "json",
                "--resource-version=" + current["metadata"]["resourceVersion"],
            ],
            callback,
        )

    def check(self):
        # A natural log EOF is allowed only after independently observed death.
        fatal = []
        with self.lock:
            for error in self.errors:
                if not error.startswith("log-"):
                    fatal.append(error)
                    continue
                uid = error.split(":", 1)[0][4:].rsplit("-", 1)[0]
                dead = any(
                    p["object"]["metadata"]["uid"] == uid
                    and (
                        p["type"] == "DELETED"
                        or any(
                            "terminated" in c.get("state", {})
                            or c.get("lastState", {}).get("terminated")
                            for c in p["object"].get("status", {}).get("containerStatuses", [])
                        )
                    )
                    for p in self.pods
                )
                if not dead or "exit 0" not in error:
                    fatal.append(error)
        require(not fatal, "k4-log-stream-loss: " + "; ".join(fatal))

    def close(self):
        self.stopped = True
        for p, _, _ in self.processes:
            if p.poll() is None:
                p.terminate()
        for p, err, _ in self.processes:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait(timeout=5)
            err.close()
        for t in self.threads:
            t.join(timeout=5)
        save(
            self.output / "stream-status.json",
            dict(errors=self.errors, logs=[list(k) for k in self.logs]),
        )

    def log_text(self, pattern="log-*.jsonl"):
        result = []
        for path in self.output.glob(pattern):
            for line in path.read_text(encoding="utf-8").splitlines():
                row = json.loads(line)
                result.append((path.name, row["received"], row["source"]))
        return result


def session_rows(events, endpoints):
    sessions = {}
    for event in events:
        p = event["object"]
        at = event["received"]
        uid = p["metadata"]["uid"]
        role = p["metadata"].get("labels", {}).get("app")
        if role not in ENGINE_ROLES:
            continue
        for c in p.get("status", {}).get("containerStatuses", []):
            cid = c.get("containerID")
            if not cid:
                continue
            key = uid + "/" + cid
            if key not in sessions:
                s = empty(SESSION)
                started = c.get("state", {}).get("running", {}).get("startedAt")
                s.update(
                    session=key,
                    pod_uid=uid,
                    container_id=cid,
                    role=role,
                    start=utc(started) if started else at,
                    last_alive=utc(started) if started else at,
                )
                sessions[key] = s
            s = sessions[key]
            condition = next(
                (r for r in p.get("status", {}).get("conditions", []) if r["type"] == "Ready"), {}
            )
            source_at = (
                utc(condition["lastTransitionTime"]) if condition.get("lastTransitionTime") else at
            )
            if c.get("state", {}).get("running") and c.get("ready"):
                s["last_alive"] = max(s["last_alive"], source_at)
            s["readiness"].append(
                dict(at=source_at, present=bool(c.get("ready")), width=max(0, at - source_at))
            )
            term = c.get("state", {}).get("terminated")
            if term:
                s.update(
                    death=utc(term["finishedAt"]),
                    death_evidence="pod-watch:" + p["metadata"]["resourceVersion"],
                    exit_code=term["exitCode"],
                    exit_signal=term.get("signal"),
                )
            # After a restart lastState belongs to the preceding container ID.
            last = c.get("lastState", {}).get("terminated")
            if last:
                for prev in sessions.values():
                    if (
                        prev["pod_uid"] == uid
                        and prev["container_id"] != cid
                        and prev["death"] is None
                    ):
                        prev.update(
                            death=utc(last["finishedAt"]),
                            death_evidence="pod-watch-lastState:"
                            + p["metadata"]["resourceVersion"],
                            exit_code=last["exitCode"],
                            exit_signal=last.get("signal"),
                        )
            if event["type"] == "DELETED" and s["death"] is None:
                # A deletion timestamp is not a process exit. Missing CRI/exit
                # evidence remains unknown and fails confirmation.
                s["readiness"].append(dict(at=at, present=False, width=0.0))
    for event in endpoints:
        obj = event["object"]
        uids = {
            a.get("targetRef", {}).get("uid")
            for sub in obj.get("subsets", [])
            for a in sub.get("addresses", [])
        }
        for s in sessions.values():
            if s["role"] == obj["metadata"]["name"]:
                s["endpoints"].append(
                    dict(at=event["received"], present=s["pod_uid"] in uids, width=0.0)
                )
    return list(sessions.values())


def export_seals(raw, sessions):
    """Verify every fsynced prefix, retain tails, map monotonic worker clocks."""
    files = {k: base64.b64decode(v) for k, v in raw.items()}
    witnesses, invocations, traces = [], [], []
    workers = {s["pod_uid"] + "/" + s["container_id"]: s for s in sessions if s["role"] == "worker"}
    seen = set()
    for name, blob in files.items():
        if not name.endswith(".inventory.json"):
            continue
        seal = json.loads(blob)
        streams = {}
        for key, entry in seal["streams"].items():
            path = str(Path(name).parent / entry["file"]).replace("\\", "/")
            require(path in files or entry["bytes"] == 0, "k4-witness-file-missing")
            data = files.get(path, b"")[: entry["bytes"]]
            require(
                len(data) == entry["bytes"]
                and hashlib.sha256(data).hexdigest() == entry["sha256"]
                and len(data.splitlines()) == entry["rows"],
                "k4-witness-seal-gap",
            )
            streams[key] = [json.loads(line) for line in data.splitlines()]
        trace = streams["trace"]
        require(bool(trace), "k4-witness-clock-missing")
        origin = utc(trace[0]["timestamp"]) - trace[0]["at"]
        # CONTAINER_ID is a downward-API UID alias until the collector resolves
        # the actual CRI ID. A restart is resolved by start time, never UID alone.
        candidates = [
            s
            for s in workers.values()
            if s["pod_uid"] == seal["pod_uid"]
            and s["start"] <= utc(trace[0]["timestamp"]) <= (s["death"] or float("inf"))
        ]
        require(len(candidates) == 1, "k4-witness-container-session-ambiguous")
        s = candidates[0]
        require(
            seal.get("container_id") in {seal["pod_uid"], s["container_id"]},
            "k4-witness-container-alias",
        )
        s["session"] = seal["session"]
        seen.add(s["pod_uid"] + "/" + s["container_id"])
        starts = {}
        mapped = []
        for row in streams["invocations"]:
            row["at"] += origin
            mapped.append(row)
            if row["event"] == "start":
                require(row["id"] not in starts, "k4-witness-duplicate-start")
                v = dict(
                    id=row["id"],
                    identity=row["identity"],
                    session=s["session"],
                    start=row["at"],
                    finish=None,
                    last_alive=max(row["at"], s["last_alive"]),
                    abort=None,
                    abort_evidence="",
                    outcome="unknown",
                )
                starts[row["id"]] = v
                invocations.append(v)
            else:
                require(row["id"] in starts, "k4-witness-finish-without-start")
                starts[row["id"]]["finish"] = row["at"]
        require(list(starts) == seal["acknowledged"], "k4-witness-ack-gap")
        data = "".join(
            json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in mapped
        )
        witnesses.append(
            dict(
                session=s["session"],
                pod_uid=s["pod_uid"],
                container_id=s["container_id"],
                sha256=hashlib.sha256(data.encode()).hexdigest(),
                data=data,
                acknowledged=seal["acknowledged"],
                complete=seal["complete"],
                errors=seal["errors"],
            )
        )
        for row in trace:
            if row["event"] == "outcome" and row.get("invocation") in starts:
                starts[row["invocation"]]["outcome"] = row["outcome"]
            row["at"] += origin
            row["session"] = s["session"]
            traces.append(row)
    require(seen == set(workers), "k4-witness-dead-session-missing")
    return witnesses, invocations, traces


def durable_claims(attempts, traces, namespace):
    """A durable claim remains evidence even when no invocation ever started."""
    claims = []
    for row in attempts:
        if not row["worker_id"]:
            continue
        hashes = set()
        for expected in (0, row["attempt_number"]):
            body = dict(
                worker_id=row["worker_id"],
                request_id=row["worker_request_id"],
                attempt_lease_ms=60000,
                expected_attempt=expected,
            )
            hashes.add(hashlib.sha256(json.dumps(body, separators=(",", ":")).encode()).hexdigest())
        acks = [
            r
            for r in traces
            if r["event"] == "http"
            and r.get("operation") == "claim"
            and r.get("body_hash") in hashes
            and r.get("status") == 200
            and not r.get("error")
        ]
        require(
            bool(acks) and len({r["session"] for r in acks}) == 1,
            "k4-claim-ack-clock-evidence-missing",
        )
        first = min(acks, key=lambda r: r["at"])
        ident = dict(
            namespace=namespace,
            workflow=row["workflow_id"],
            node=row["node_id"],
            iteration=row["iteration"],
            attempt=row["attempt_number"],
            token_hash=row["token_hash"],
        )
        claims.append(
            dict(identity=ident, session=first["session"], order=len(claims) + 1, at=first["at"])
        )
    return claims


def callback_rows(traces, arrivals, timeline, intervention):
    """Join callback generations to a durable registered-stream disposition."""
    rows = []
    triggers = [r for r in traces if r["event"] in {"revoke", "lost"}]
    for index, trigger in enumerate(sorted(triggers, key=lambda r: r["at"])):
        later = [
            r
            for r in traces
            if r["event"] == "assigned"
            and r["at"] >= trigger["at"]
            and r["generation"] > trigger["generation"]
        ]
        generation = min((r["generation"] for r in later), default=trigger["generation"])
        assigned = [r for r in later if r["generation"] == generation]
        survivors = sorted({r["consumer_session"] for r in assigned})
        change = "callback-" + str(index)

        def mapped(
            raw,
            event,
            arrival="",
            order=None,
            *,
            change=change,
            survivors=survivors,
            trigger=trigger,
        ):
            return dict(
                change=change,
                event=event,
                at=raw["at"],
                consumer=raw["consumer_session"],
                session=raw["session"],
                generation=raw["generation"],
                partitions=[p["partition"] for p in raw.get("partitions", [])],
                survivors=survivors,
                trigger_uid=trigger["pod_uid"],
                arrival=arrival,
                disposition_order=order,
            )

        rows.append(
            mapped(trigger, "revoke" if trigger["event"] == "revoke" else "rebalance_start")
        )
        for raw in assigned:
            rows.append(mapped(raw, "assigned"))
            rows.append(mapped(raw, "stable"))
        stable_at = max((r["at"] for r in assigned), default=float("inf"))
        for raw in traces:
            if (
                raw["event"] != "disposed"
                or raw["generation"] != generation
                or raw["at"] < stable_at
            ):
                continue
            if raw["consumer_session"] not in survivors or raw.get("disposition") not in {
                "EXECUTE",
                "ACCEPTED",
                "ALREADY_HANDLED",
                "DISPATCH",
            }:
                continue
            arrival = next((a for a in arrivals if a["workflow"] == raw.get("workflow")), None)
            histories = [
                h
                for h in timeline["history"]
                if h["identity"]["workflow"] == raw.get("workflow")
                and h["kind"] in {"receipt", "transition", "terminal"}
                and h["at"] <= raw["at"]
            ]
            if arrival and histories:
                h = max(histories, key=lambda h: h["order"])
                rows.append(mapped(raw, "disposed", arrival["id"], h["order"]))
                break
    return rows
