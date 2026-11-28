"""Controller for the registered hosted J4 cells."""

from __future__ import annotations

import json
import time

from deploy.j4.checker import check
from deploy.j4.evidence import SECOND, read_jsonl, write_json
from deploy.j4.host import DEFINITION, PROVIDER, request, wait_for
from deploy.j4.qualification import live_clock_proof, require_clock_continuity

CUTS = {"C1", "C2", "C4", "B1", "B2", "B3"}
TERMINAL = {"SUCCEEDED", "FAILED", "REJECTED", "CANCELED", "ABANDONED"}
KILL_SCRIPT = """
pid="$1"
start="$2"
stat="/proc/$pid/stat"
same() {
    test -r "$stat" && test "$(cut -d ' ' -f22 "$stat")" = "$start"
}
same || exit 31
kill -KILL "$pid"
while same && test "$(cut -d ' ' -f3 "$stat")" != Z; do sleep 0.01; done
"""


def case_input(arm: str) -> dict:
    return {
        "model": "recorded-llm-v1",
        "prompt": "Return fixture beta." if arm == "C3" else "Return fixture alpha.",
    }


def checkable_snapshot(rows: dict) -> bool:
    return bool(rows["workflows"] and rows["workflows"][0]["state"] in TERMINAL)


def open_calls(rows: list[dict]) -> list[str]:
    closed = {r["receipt_sequence"] for r in rows if r["event"] == "close"}
    return [r["call_id"] for r in rows if r["event"] == "receipt" and r["sequence"] not in closed]


def boundary_ready(arm: str, traces: dict, db: dict, session: str, workflow: str) -> bool:
    worker = [r for r in traces["worker"] if r["process_session"] == session]
    held = any(
        r["hook"] == "cut_held" and r["fields"] == {"arm": arm, "workflow": workflow}
        for r in worker
    )
    if arm == "C4":
        return (
            not any(db[key] for key in ("workflows", "attempts", "outbox")) and not traces["stub"]
        )
    attempts = db["attempts"]
    if len(attempts) != 1:
        return False
    attempt = attempts[0]
    if (attempt["workflow_id"], attempt["node_id"], attempt["attempt_number"]) != (
        workflow,
        PROVIDER,
        1,
    ):
        return False
    original = next(
        (r for r in traces["broker"] if r["kind"] == "notice" and r["offset"] == 0), None
    )
    if (
        not original
        or not any(
            r["hook"] == "j4_offset_committed"
            and r["fields"]["event"] == original["event_id"]
            and r["fields"]["next_offset"] == "1"
            for r in worker
        )
        or not any(r["kind"] == "offset" and r["offset"] == 1 for r in traces["broker"])
    ):
        return False
    receipts = [r for r in db["history"] if r["reason"] == "ATTEMPT_RESULT_RECORDED"]
    events = [r for r in db["outbox"] if r["event_type"] == "activity.result"]
    if arm == "C2":
        return (
            held
            and attempt["state"] == "SUCCEEDED"
            and bool(attempt["committed_at"])
            and len(receipts) == len(events) == len(db["nodes"]) == 1
            and receipts[0]["revision"] == events[0]["aggregate_revision"]
            and not any(r["reason"] == "RESULT_CONSUMED" for r in db["history"])
            and any(r["event"] == "held" for r in traces["scheduler"])
        )
    if attempt["state"] != "CLAIMED" or not attempt["is_current"] or receipts or events:
        return False
    if arm in {"C1", "B2"}:
        journal = traces["stub"]
        active = open_calls(journal)
        if len(active) != 1 or not any(r["event"] == "withhold" for r in journal):
            return False
        if arm == "C1":
            return not db["reservations"]
        return bool(
            len(db["reservations"]) == 1
            and db["reservations"][0]["call_id"] == active[0]
            and db["reservations"][0]["outcome"] is None
        )
    if len(db["reservations"]) != 1 or not db["reservations"][0]["committed_at"]:
        return False
    reservation = db["reservations"][0]
    if arm == "B1":
        return held and reservation["outcome"] is None and not traces["stub"]
    return reservation["outcome"] == "rate_limited" and any(
        r["hook"] == "j4_provider"
        and r["fields"].get("event") == "reply"
        and r["fields"].get("call_id") == reservation["call_id"]
        and r["fields"].get("at_ns") == str(reservation["observed_ns"])
        for r in worker
    )


def sleep_until(target: int) -> None:
    if left := target - time.monotonic_ns():
        time.sleep(max(left / SECOND, 0))


class Cell:
    def __init__(self, topology, case):
        self.t = topology
        self.host = topology.host
        self.arm = case["arm"]
        self.out = topology.out
        self.workflow = topology.overlay.workflow_id
        self.run = {
            "schema": "j4.cell.v1",
            **case,
            "workflow_id": self.workflow,
            "namespace": topology.overlay.namespace,
            "cut": None,
            "redelivery": [],
        }

    def persist(self):
        self.run.update(self.t.traces())
        write_json(self.out / "cell.json", self.run)

    def clocks(self):
        return live_clock_proof({k: self.t.ports[k] for k in ("runtime", "worker", "stub")})

    def submit(self):
        fixture = case_input(self.arm)
        body = {
            "workflow_id": self.workflow,
            "namespace": self.t.overlay.namespace,
            "submission_key": self.workflow,
            "payload": fixture,
            "initial_input": fixture,
            "definition_id": DEFINITION,
            "definition_version": 1,
            "initial_node_id": PROVIDER,
            "actor_id": "j4-controller",
        }
        asked = time.monotonic_ns()
        status, reply = request(self.t.ports["runtime"], "/v1/workflows", body, timeout=3)
        self.run["submit"] = {
            "request": body,
            "response": reply,
            "status": status,
            "request_ns": asked,
            "accepted_ns": time.monotonic_ns(),
        }
        self.persist()
        if (
            status != 201
            or not reply.get("created")
            or reply["workflow"]["workflow_id"] != self.workflow
        ):
            raise ValueError("J4 submission identity or acknowledgment differs")

    def container_status(self):
        return self.t.object("pod", "worker")["status"]["containerStatuses"][0]

    def worker_identity(self):
        status = self.container_status()
        raw = status["containerID"]
        container = raw.removeprefix("containerd://")
        if raw == container or not status["state"].get("running"):
            raise ValueError("J4 kill target is not the running worker container")
        node = self.host.name + "-control-plane"
        inspect = json.loads(
            self.host.cmd(["docker", "exec", node, "crictl", "inspect", container])
        )
        pid = inspect["info"]["pid"]
        stat = self.host.cmd(["docker", "exec", node, "cat", f"/proc/{pid}/stat"])
        if type(pid) is not int or pid <= 1 or stat.split(")", 1)[0] != f"{pid} (python":
            raise ValueError("J4 kill target is not the Python worker PID")
        return container, pid, stat.rsplit(")", 1)[1].split()[19]

    def cut(self, old_health, target):
        old, pid, start = target
        old_session = old_health["process_session"]
        deadline = self.run.get("submit", {}).get("accepted_ns", time.monotonic_ns()) + 30 * SECOND

        def probe():
            sample = self.t.snapshot()
            traces = self.t.traces()
            if not boundary_ready(self.arm, traces, sample["db"], old_session, self.workflow):
                return None
            return sample, traces

        snapshot, traces = wait_for(
            probe, min(deadline, self.host.deadline_ns), f"J4 {self.arm} committed kill boundary"
        )
        boundary = time.monotonic_ns()
        cut = {
            "pre_db": snapshot,
            "boundary_ns": boundary,
            "open_calls": open_calls(traces["stub"]),
            "old_session": old_session,
            "old_container": old,
            "node_pid": pid,
            "pid_start_ticks": start,
        }
        self.run["cut"] = cut
        if self.arm == "B3":
            origin = snapshot["db"]["reservations"][0]["observed_ns"]
            if boundary > origin + SECOND // 2:
                raise TimeoutError("J4 B3 lost its backoff cut")
            sleep_until(origin + SECOND // 2)
            # B3's acknowledgment is the fully received headers timestamp.
            cut["boundary_ns"] = origin
        cut["request_ns"] = time.monotonic_ns()
        self.host.cmd(
            [
                "docker",
                "exec",
                self.host.name + "-control-plane",
                "sh",
                "-ec",
                KILL_SCRIPT,
                "--",
                str(pid),
                start,
            ],
            timeout=0.5,
        )
        cut["death_ns"] = time.monotonic_ns()
        cut["old_process_gone"] = True
        if self.arm == "C2":
            # Stage the exact release contents on the same host filesystem;
            # only the atomic rename opens the gate.
            staged = self.t.raw / "runtime/release.pending"
            with staged.open("w") as release:
                release.write(self.workflow + "\n")
                release.flush()
        if self.arm != "B3" and cut["request_ns"] - boundary > SECOND // 2:
            raise TimeoutError("J4 worker kill missed the acknowledged boundary")

        def replaced():
            status = self.container_status()
            terminated = status.get("lastState", {}).get("terminated", {})
            container = status["containerID"].removeprefix("containerd://")
            if container == old or not status["state"].get("running"):
                return None
            if (
                terminated.get("exitCode") != 137
                or terminated["containerID"] != f"containerd://{old}"
            ):
                raise ValueError("J4 did not witness the registered SIGKILL")
            health = self.t.health("worker")
            if not health or health["process_session"] == old_session:
                return None
            return container, health

        container, health = wait_for(
            replaced, cut["death_ns"] + 18 * SECOND, "J4 replacement readiness", interval=0.1
        )
        cut.update(
            {
                "ready_ns": time.monotonic_ns(),
                "new_container": container,
                "new_session": health["process_session"],
                "exit_code": 137,
            }
        )
        if self.arm == "C2":
            cut["release_request_ns"] = time.monotonic_ns()
            staged.replace(staged.with_name("release"))
        self.persist()
        self.t.verify_worker(health)

    def terminal(self):
        prior = self.run["submit"]["request_ns"]
        limit = (
            self.run["submit"]["accepted_ns"] + (120 if self.arm.startswith("B") else 90) * SECOND
        )

        def probe():
            nonlocal prior
            sample = self.t.snapshot()
            if not checkable_snapshot(sample["db"]):
                prior = sample["request_ns"]
                return None
            return sample

        terminal = wait_for(probe, min(limit, self.host.deadline_ns), "J4 committed terminal state")
        self.run["terminal"] = {"lower_ns": prior, "observed_ns": terminal["observed_ns"]}
        self.run["terminal_db"] = terminal

    def redeliver(self, latest: int) -> dict:
        rows = read_jsonl(self.t.broker_file)
        original = next(r for r in rows if r["kind"] == "notice" and r["offset"] == 0)
        ordinal = len(self.run["redelivery"]) + 1
        asked = time.monotonic_ns()
        if asked > latest:
            raise TimeoutError("J4 missed the registered redelivery window")
        if self.t.broker_proc.stdin is None:
            raise ValueError("J4 Kafka witness cannot republish")
        data = {"delivery": ordinal, "original_offset": original["offset"]}
        self.t.broker_proc.stdin.write(json.dumps(data).encode() + b"\n")
        self.t.broker_proc.stdin.flush()

        def acknowledgment():
            traces = self.t.traces()
            broker = traces["broker"]
            if any(r["kind"] == "publish_error" and r["delivery"] == ordinal for r in broker):
                raise ValueError("J4 Kafka republish failed")
            published = next(
                (r for r in broker if r["kind"] == "republished" and r["delivery"] == ordinal), None
            )
            if not published:
                return None
            offset = published["offset"]
            handled = next(
                (
                    r
                    for r in traces["worker"]
                    if r["hook"] == "j4_offset_committed" and r["fields"]["offset"] == str(offset)
                ),
                None,
            )
            observed = self.t.snapshot()
            durable = any(
                r["consumer_id"] == "runtime-workers-v1" and r["next_offset"] == offset + 1
                for r in observed["db"]["offsets"]
            )
            if (
                handled
                and durable
                and any(r["kind"] == "offset" and r["offset"] == offset + 1 for r in broker)
            ):
                return {
                    "original_offset": original["offset"],
                    "offset": offset,
                    "request_ns": asked,
                    "ack_ns": handled["monotonic_ns"],
                    "durable": observed,
                }
            return None

        result = wait_for(acknowledgment, asked + 5 * SECOND, "J4 duplicate delivery disposition")
        self.run["redelivery"].append(result)
        self.persist()
        return result

    def finish(self):
        self.terminal()
        terminal = self.run["terminal"]
        end = terminal["observed_ns"] + (30 * SECOND if self.arm.startswith("C") else 0)
        if self.arm in {"C2", "B4"}:
            first = self.redeliver(terminal["lower_ns"] + 2 * SECOND)
            if self.arm == "C2":
                end = max(
                    end, self.run["cut"]["request_ns"] + 30 * SECOND, first["ack_ns"] + 30 * SECOND
                )
            else:
                fourth = self.run["terminal_db"]["db"]["reservations"][3]
                latest = fourth["deadline_ns"] + (16_039 + 750 + 18_000) * 1_000_000
                sleep_until(latest + SECOND)
                second = self.redeliver(latest + 3 * SECOND)
                end = max(latest + 10 * SECOND, second["ack_ns"] + 5 * SECOND)
        sleep_until(end)
        # Results and task publications can finish just after the terminal state.
        sample = self.t.snapshot()
        self.run["db"] = sample["db"]
        self.run["observation_end_ns"] = sample["observed_ns"]
        self.run["clock_end"] = self.clocks()
        require_clock_continuity(self.run["clock_start"], self.run["clock_end"])
        self.persist()
        return check(self.run)

    def run_case(self, selection, revision):
        self.t.setup_infra()
        self.t.seed()
        self.t.start_runtime(selection, revision)
        worker = self.t.start_worker()
        self.t.verify_worker(worker)
        self.t.parity()
        self.t.start_broker()
        self.run["clock_start"] = self.clocks()
        images = {
            role: self.t.image_id(role)
            for role in (
                "runtime",
                "worker",
                "postgres",
                "kafka",
                "stub",
                "probe",
                "wrong-pod",
                "outside",
                "dns-decoy",
            )
        }
        for role in {"runtime", "worker", "stub", "probe", "wrong-pod", "outside", "dns-decoy"}:
            sealed = "runtime" if role == "runtime" else "worker"
            if images[role] != self.host.images[sealed]["id"]:
                raise ValueError(f"J4 running {role} image differs from the seal")
        write_json(self.out / "images.json", images)
        target = self.worker_identity() if self.arm in CUTS else None
        if self.arm == "C4":
            self.cut(worker, target)
        self.submit()
        if target and self.arm != "C4":
            self.cut(worker, target)
        return self.finish()
