"""K4 intervention adapters. Commands initiate; observations establish completion."""

from __future__ import annotations

import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from threading import Lock

from dur053_k3 import save
from dur053_k3_policy import require
from dur053_k4_schema import CONFIRMATION, HASH, NUMBER, SHA

ENGINE_ROLES = {"api", "worker-control", "worker", "scheduler", "relay", "demand", "operator"}

# Only these guards describe observed intervention outcomes. Everything else
# (including unknown errors) propagates to the content-checked harness path.
OUTCOME_GUARDS = frozenset(
    {
        "k4-rollout-incomplete",
        "k4-chaos-ack-missing",
        "k4-chaos-wrong-uid",
        "k4-chaos-death-container",
        "k4-chaos-restart-missing",
        "k4-chaos-death-missing",
        "k4-chaos-replacement-not-ready",
        "k4-singleton-denial-missing",
        "k4-eviction-deadline",
        "k4-scale-removed-two",
        "k4-deletion-not-observed",
    }
)
SAFETY_GUARDS = frozenset(
    {
        "k4-eviction-retry-gap",
        "k4-log-stream-loss",
        "k4-observer-loss",
        "k4-observer-stop-timeout",
        "k4-watch-expired",
        "k4-watch-framing",
        "k4-stream-self-check-timeout",
        "k4-observed-connection-budget",
    }
)


def intervention_outcome(error):
    """Classify exact guard IDs centrally; never infer outcomes from substrings."""
    if not isinstance(error, ValueError):
        return False
    prefix, separator, guard = str(error).partition("K3 guard: ")
    return not prefix and bool(separator) and guard.split(":", 1)[0] in OUTCOME_GUARDS


def empty(spec):
    """Construct explicit nullable fields, never a passing observation."""
    if isinstance(spec, dict):
        return {k: empty(v) for k, v in spec.items()}
    if isinstance(spec, list):
        return []
    if isinstance(spec, tuple):
        return None
    if isinstance(spec, frozenset):
        return sorted(spec)[0]
    if spec in (HASH, SHA):
        return "0" * (64 if spec == HASH else 40)
    if spec == NUMBER:
        return 0.0
    if spec == "hashmap":
        return {}
    return {str: "", int: 0, bool: False}[spec]


def ready(pod):
    return not pod["metadata"].get("deletionTimestamp") and any(
        c["type"] == "Ready" and c["status"] == "True"
        for c in pod.get("status", {}).get("conditions", [])
    )


def rollout_complete(dep, image=None):
    spec, status = dep["spec"], dep.get("status", {})
    count = spec["replicas"]
    return (
        status.get("observedGeneration", 0) >= dep["metadata"]["generation"]
        and status.get("updatedReplicas", 0) == count
        and status.get("readyReplicas", 0) == count
        and status.get("availableReplicas", 0) == count
        and status.get("replicas", 0) == count
        and (
            image is None
            or all(c["image"] == image for c in spec["template"]["spec"]["containers"])
        )
    )


def chaos_ack(obj):
    # PodChaos status records acknowledged injection per selected pod; a created
    # object, selected target or AllRecovered condition alone is not injection.
    status = obj.get("status", {})
    records = status.get("experiment", {}).get("containerRecords", [])
    return (
        any(
            c.get("type") == "AllInjected" and c.get("status") == "True"
            for c in status.get("conditions", [])
        )
        and bool(records)
        and all(r.get("phase") in {"Injected", "Wait"} for r in records)
    )


def kill_proof(before, after, acknowledged):
    require(acknowledged, "k4-chaos-ack-missing")
    require(before["metadata"]["uid"] == after["metadata"]["uid"], "k4-chaos-wrong-uid")
    old = before["status"]["containerStatuses"][0]
    new = after["status"]["containerStatuses"][0]
    term = new.get("lastState", {}).get("terminated", {})
    require(
        term.get("containerID", old["containerID"]) == old["containerID"],
        "k4-chaos-death-container",
    )
    require(
        new["restartCount"] > old["restartCount"] and new["containerID"] != old["containerID"],
        "k4-chaos-restart-missing",
    )
    require(term.get("exitCode") == 137 and term.get("signal", 9) == 9, "k4-chaos-death-missing")
    require(ready(after), "k4-chaos-replacement-not-ready")
    return term


def require_term_evidence(record, sessions):
    if record["kind"] == "kill" or record["completed"] is None:
        return
    by = {s["session"]: s for s in sessions}
    for target in record["targets"]:
        require(
            any(signal["signal"] == 15 for signal in by[target["session"]]["signals"]),
            "k4-term-observation-missing",
        )


class Interventions:
    def __init__(self, driver, namespace, output, images, proof, source):
        self.d, self.ns, self.output = driver, namespace, output
        self.images, self.proof, self.source = images, proof, source
        self.setup_completed = None
        self.revisions = ("", "")
        self.release = lambda: None
        self.safety = lambda: None
        self.intervention_issued = None

    def issue(self, targets):
        """Persist the command boundary before the first intervention mutation."""
        if self.intervention_issued is None:
            self.intervention_issued = time.time()
            save(
                self.output / "intervention-issued.json",
                dict(intervention_issued=self.intervention_issued, targets=targets),
            )

    def wait(self, predicate, seconds, guard):
        # Only the adapter's completion deadline is an intervention outcome.
        # A safety/command timeout raised inside the predicate keeps its cause.
        predicate_failed = False

        def observe():
            nonlocal predicate_failed
            try:
                return predicate()
            except Exception:
                predicate_failed = True
                raise

        try:
            self.d.until(observe, seconds)
        except TimeoutError:
            if predicate_failed:
                raise
            require(False, guard)

    def release_after_deletion(self, role):
        self.wait(
            lambda: any(p["metadata"].get("deletionTimestamp") for p in self.pods(role)),
            60,
            "k4-deletion-not-observed",
        )
        self.release()

    def get(self, kind, name):
        self.safety()
        return json.loads(
            self.d.k("-n", self.ns, "get", kind, name, "-o", "json", record=False).stdout
        )

    def pods(self, role):
        return json.loads(
            self.d.k(
                "-n", self.ns, "get", "pods", "-l", "app=" + role, "-o", "json", record=False
            ).stdout
        )["items"]

    def wait_rollout(self, role, image=None):
        self.wait(
            lambda: rollout_complete(self.get("deployment", role), image),
            180,
            "k4-rollout-incomplete",
        )
        dep = self.get("deployment", role)
        require(rollout_complete(dep, image), "k4-rollout-incomplete")
        save(self.output / ("rollout-" + role + "-" + str(time.time_ns()) + ".json"), dep)
        return dep["metadata"]["annotations"]["deployment.kubernetes.io/revision"]

    def set_image(self, role, lineage):
        image = self.images[lineage]["worker" if role == "worker" else "runtime"]
        if getattr(self, "intervening", False):
            self.issue(self.pods(role))
        self.d.k("-n", self.ns, "set", "image", "deployment/" + role, role + "=" + image)
        if getattr(self, "intervening", False):
            self.release_after_deletion(role)
        return self.wait_rollout(role, image)

    def rollback_setup(self, role):
        a = self.wait_rollout(role)
        prime = self.set_image(role, "a-prime")
        require(a != prime, "k4-rollback-setup-no-revision")
        self.revisions = a, prime
        self.setup_completed = time.time()

    def kill(self, pod, *, name="k4-kill"):
        uid = pod["metadata"]["uid"]
        status = pod["status"]["containerStatuses"][0]
        require(status["containerID"].startswith("containerd://"), "k4-containerd-required")
        obj = {
            "apiVersion": "chaos-mesh.org/v1alpha1",
            "kind": "PodChaos",
            "metadata": {"name": name, "namespace": self.ns},
            "spec": {
                "action": "container-kill",
                "mode": "one",
                "selector": {"namespaces": [self.ns], "pods": {self.ns: [pod["metadata"]["name"]]}},
                "containerNames": [status["name"]],
            },
        }
        # The UID and exact current container are captured before submission.
        save(self.output / (name + "-target.json"), pod)
        self.issue([pod])
        self.d.apply(obj)
        ack = None

        def injected():
            nonlocal ack
            ack = self.get("podchaos", name)
            save(self.output / (name + "-status.json"), ack)
            return chaos_ack(ack)

        self.wait(injected, 60, "k4-chaos-ack-missing")
        require(chaos_ack(ack), "k4-chaos-ack-missing")
        self.release()
        after = None

        def restarted():
            nonlocal after
            after = self.get("pod", pod["metadata"]["name"])
            require(after["metadata"]["uid"] == uid, "k4-chaos-wrong-uid")
            return after["status"].get("containerStatuses", [{}])[0].get(
                "restartCount", 0
            ) > status["restartCount"] and ready(after)

        self.wait(restarted, 120, "k4-chaos-restart-missing")
        kill_proof(pod, after, True)
        save(self.output / (name + "-replacement.json"), after)
        self.d.k("-n", self.ns, "delete", "podchaos", name, "--wait=true", "--timeout=30s")
        return after

    def eviction(self, at, record):
        initial = [p for role in sorted(ENGINE_ROLES) for p in self.pods(role)]
        roles = []
        for role in sorted(ENGINE_ROLES):
            pods = [p for p in initial if p["metadata"]["labels"]["app"] == role]
            require(bool(pods), "k4-eviction-role-missing")
            count = len(pods)
            self.d.until(
                lambda role=role, count=count: (
                    self.get("pdb", role)["status"]["disruptionsAllowed"] == count - 1
                ),
                30,
            )
            allowed = self.get("pdb", role)["status"]["disruptionsAllowed"]
            roles.append(
                dict(
                    role=role,
                    replicas=len(pods),
                    disruptions_allowed=allowed,
                    pod_uids=[p["metadata"]["uid"] for p in pods],
                )
            )
        record.update(roles=roles, no_cordon=True, eviction_retry_interval=5.0)
        save(self.output / "confirmation.json", record)
        rolemap = {r["role"]: r for r in roles}

        lock = Lock()

        def pdb_round():
            snapshot = self.get("pdb", "")
            save(self.output / ("pdb-round-" + str(time.time_ns()) + ".json"), snapshot)
            return {
                p["metadata"]["name"]: p["status"]["disruptionsAllowed"] for p in snapshot["items"]
            }

        allowed = pdb_round()

        def request(p, previous=None):
            self.safety()
            role = p["metadata"]["labels"]["app"]
            require(role in ENGINE_ROLES, "k4-eviction-harness")
            if previous is not None:
                next_at = previous["responded"] + record["eviction_retry_interval"]
                time.sleep(max(0, next_at - time.time() - 0.1))
            before = allowed[role]
            t0 = time.time()
            body = {
                "apiVersion": "policy/v1",
                "kind": "Eviction",
                "metadata": {"name": p["metadata"]["name"], "namespace": self.ns},
                "deleteOptions": {"preconditions": {"uid": p["metadata"]["uid"]}},
            }
            result = self.d.k(
                "create",
                "--raw",
                "/api/v1/namespaces/" + self.ns + "/pods/" + p["metadata"]["name"] + "/eviction",
                "-f",
                "-",
                data=json.dumps(body),
                allowed=True,
            )
            t1 = time.time()
            status = (
                201
                if result.returncode == 0
                else (
                    429
                    if "TooManyRequests" in result.stderr or "disruption budget" in result.stderr
                    else 0
                )
            )
            row = dict(
                pod_uid=p["metadata"]["uid"],
                role=role,
                replicas=rolemap[role]["replicas"],
                requested=t0,
                scheduled=at,
                responded=t1,
                status=status,
                allowed_before=before,
                allowed_after=allowed[role],
                denial=result.stderr if status == 429 else "",
                replaced_by="",
                replacement_ready=None,
                replacement_ready_upper=None,
            )
            # Serialize append + save; retain even the request that trips a later guard.
            with lock:
                record["evictions"].append(row)
                save(self.output / "confirmation.json", record)
            require(status in (201, 429), "k4-eviction-api-failure")
            if previous is not None:
                require(
                    t0 - previous["responded"] <= record["eviction_retry_interval"],
                    "k4-eviction-retry-gap",
                )
            return p, row

        # All initial requests are dispatched together. No rollout/delete/cordon.
        self.issue(initial)
        with ThreadPoolExecutor(max_workers=len(initial)) as pool:
            futures = [pool.submit(request, pod) for pod in initial]
            responses = [f.result() for f in as_completed(futures)]
        self.release()
        pending = []
        for pod, row in responses:
            r = rolemap[row["role"]]
            if r["disruptions_allowed"] == 0:
                require(row["status"] == 429, "k4-singleton-denial-missing")
            else:
                pending.append((pod, row))
        deadline = time.monotonic() + 180

        def retry_pod(pod, row):
            while row["status"] == 429:
                require(time.monotonic() < deadline, "k4-eviction-deadline")
                pod, row = request(pod, row)
            return pod, row

        # One persistent retry worker per pending pod. A single all-PDB read
        # per observation round runs in the coordinator, never in a retry worker.
        with ThreadPoolExecutor(max_workers=len(pending) or 1) as pool:
            futures = [pool.submit(retry_pod, pod, row) for pod, row in pending]
            while not all(f.done() for f in futures):
                allowed = pdb_round()
                with lock:
                    for row in record["evictions"]:
                        row["allowed_after"] = allowed[row["role"]]
                    save(self.output / "confirmation.json", record)
                time.sleep(0.2)
            pending = [f.result() for f in futures]
        allowed = pdb_round()
        for row in record["evictions"]:
            row["allowed_after"] = allowed[row["role"]]
        save(self.output / "confirmation.json", record)
        used_replacements = set()
        while pending:
            require(time.monotonic() < deadline, "k4-eviction-deadline")
            later = []
            observations = {role: self.pods(role) for role in {r["role"] for _, r in pending}}
            for pod, row in pending:
                old = set(rolemap[row["role"]]["pod_uids"])
                replacements = sorted(
                    [
                        p
                        for p in observations[row["role"]]
                        if p["metadata"]["uid"] not in old | used_replacements and ready(p)
                    ],
                    key=lambda p: (
                        p["metadata"].get("creationTimestamp", ""),
                        p["metadata"]["uid"],
                    ),
                )
                live_uids = {p["metadata"]["uid"] for p in observations[row["role"]]}
                if (
                    row["status"] == 201
                    and pod["metadata"]["uid"] not in live_uids
                    and replacements
                ):
                    replacement = replacements[0]
                    uid = replacement["metadata"]["uid"]
                    condition = next(
                        c
                        for c in replacement["status"]["conditions"]
                        if c["type"] == "Ready" and c["status"] == "True"
                    )
                    row.update(
                        replaced_by=uid,
                        replacement_ready=datetime.fromisoformat(
                            condition["lastTransitionTime"].replace("Z", "+00:00")
                        ).timestamp(),
                        replacement_ready_upper=time.time(),
                    )
                    used_replacements.add(uid)
                else:
                    later.append((pod, row))
            save(self.output / "confirmation.json", record)
            pending = later
            if pending:
                time.sleep(0.2)
        return [
            p for p in initial if rolemap[p["metadata"]["labels"]["app"]]["disruptions_allowed"] > 0
        ]

    def apply(self, cell, overlap):
        require(bool(overlap), "k4-unexercised-overlap")
        event = cell["event"]
        kind = (
            "eviction"
            if "PDB" in event
            else "kill"
            if "SIGKILL" in event
            else "scale-down"
            if "scale-down" in event
            else "rollback"
            if "rollback" in event
            else "rollout"
        )
        role = event.split()[0].lower()
        record = empty(CONFIRMATION)
        record.update(
            schema="dur053.k4-confirmation.v1",
            episode=cell["episode"],
            kind=kind,
            target="engine-replicated-roles" if kind == "eviction" else "Deployment/" + role,
            overlap=overlap,
            intervention=time.time(),
            binary_proof=self.proof,
            binary_identity=hashlib.sha256(
                json.dumps(self.proof, sort_keys=True).encode()
            ).hexdigest(),
            source_commit=self.source,
            build="same-job-buildvcs-false",
            lineage_a="a",
            lineage_a_prime="a-prime",
            setup_completed=self.setup_completed,
        )
        image_role = "worker" if role == "worker" else "runtime"
        record.update(
            image_a=self.images["a"][image_role], image_a_prime=self.images["a-prime"][image_role]
        )
        save(self.output / "confirmation.json", record)
        self.intervening = True
        pods = self.pods(role) if kind != "eviction" else []
        if kind != "eviction":
            require(
                any(
                    o["role"] == role and any(p["metadata"]["uid"] == o["pod_uid"] for p in pods)
                    for o in overlap
                ),
                "k4-overlap-wrong-target",
            )
        if kind in {"rollout", "rollback"}:
            revision_a = self.wait_rollout(role)
            if kind == "rollout":
                revision_prime = self.set_image(role, "a-prime")
            else:
                revision_a, revision_prime = self.revisions
                self.issue(pods)
                self.d.k(
                    "-n",
                    self.ns,
                    "rollout",
                    "undo",
                    "deployment/" + role,
                    "--to-revision=" + revision_a,
                )
                self.release_after_deletion(role)
                self.wait_rollout(role, self.images["a"][image_role])
            record.update(
                revision_a=revision_a, revision_a_prime=revision_prime, rollout_status="complete"
            )
        elif kind == "scale-down":
            require(len(pods) == 4, "k4-scale-start-four")
            self.issue(pods)
            self.d.k("-n", self.ns, "scale", "deployment/worker", "--replicas=2")
            self.release_after_deletion("worker")
            self.wait_rollout("worker")
            self.wait(lambda: len(self.pods("worker")) == 2, 120, "k4-scale-removed-two")
            survivors = {p["metadata"]["uid"] for p in self.pods("worker")}
            pods = [p for p in pods if p["metadata"]["uid"] not in survivors]
            require(len(pods) == 2, "k4-scale-removed-two")
            record["overlap"] = [
                o for o in overlap if o["pod_uid"] in {p["metadata"]["uid"] for p in pods}
            ]
        elif kind == "kill":
            affected = {o["pod_uid"] for o in overlap}
            pods = [p for p in pods if p["metadata"]["uid"] in affected][:1]
            require(len(pods) == 1, "k4-kill-overlap-target")
            self.kill(pods[0])
            record.update(
                chaos_version="2.7.2",
                chaos_kind="container-kill",
                chaos_object=self.ns + "/k4-kill",
                injection_status="injected",
            )
        else:
            pods = self.eviction(record["intervention"], record)
        record.update(completed=time.time(), removed=time.time())
        save(self.output / "confirmation.json", record)
        return record, pods
