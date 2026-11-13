"""Offline K4 metrics and bidirectional committed-evidence verification."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
from collections import Counter, defaultdict
from pathlib import Path

from dur053_k3_policy import require
from dur053_k4 import (
    QUIET,
    ROOT,
    WARMUP,
    episode_identity,
    frozen,
    k7,
    parameters,
    registry,
    remaining,
)
from dur053_k4_schema import load
from dur053_payload import materialize
from dur053_pilot import finite, nearest_rank

FILES = {
    "timeline": "timeline.json",
    "leases": "leases.json",
    "confirmation": "confirmation.json",
    "arrivals": "arrivals.json",
    "kafka": "kafka.json",
    "witness": "witness.json",
    "observers": "observers.json",
}


def population(values: list[float], *, missing=0, censored=0, long=False) -> dict:
    values = [finite(v, "k4-population-finite", zero=True) for v in values]
    return dict(
        n=len(values),
        missing=missing,
        censored=censored,
        p50=nearest_rank(values, 0.50) if values else None,
        p90=nearest_rank(values, 0.90) if values else None,
        max=max(values) if values else None,
        p99=nearest_rank(values, 0.99) if len(values) >= 200 and not long else "NOT ESTIMATED",
    )


def identity(i: dict, *, attempt=False):
    keys = ("namespace", "workflow", "node", "iteration") + (
        ("attempt", "token_hash") if attempt else ()
    )
    return tuple(i[k] for k in keys)


def confirmation(c: dict, t: dict, event: str, leases=()) -> list[str]:
    issues = []

    def need(ok, message):
        if not ok:
            issues.append(message)

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
    need(
        c["kind"] == kind
        and c["target"]
        == ("engine-replicated-roles" if kind == "eviction" else f"Deployment/{role}"),
        "k4-confirmation-target",
    )
    sessions = {s["session"]: s for s in t["sessions"]}
    need(bool(c["targets"]) and bool(c["overlap"]), "k4-confirmation-overlap")
    for o in c["overlap"]:
        s = sessions.get(o["session"], {})
        need(
            s.get("pod_uid") == o["pod_uid"]
            and o["at"] <= c["intervention"] <= o["end"]
            and bool(o["acknowledgment"]),
            "k4-confirmation-boundary",
        )
        if o["role"] == "worker":
            need(
                o["claim"] is not None
                and any(
                    v["identity"] == o["claim"]
                    and v["session"] == o["session"]
                    and v["start"]
                    <= c["intervention"]
                    < (v["finish"] if v["finish"] is not None else s.get("death") or math.inf)
                    for v in t["invocations"]
                ),
                "k4-confirmation-worker-claim",
            )
        elif o["role"] == "scheduler":
            need(
                o["partition"] is not None and bool(o["owner"]) and o["epoch"] is not None,
                "k4-confirmation-acquisition-ack",
            )
            need(
                any(
                    r["kind"] == "acquire"
                    and r["session"] == o["session"]
                    and r["owner"] == o["owner"]
                    and r["epoch"] == o["epoch"]
                    and r["partition"] == o["partition"]
                    and r["at"] <= o["at"]
                    for r in leases
                ),
                "k4-confirmation-acquisition-ledger",
            )
        elif o["role"] == "api":
            need(bool(o["request"]), "k4-confirmation-api-request")
        elif o["role"] == "relay":
            need(bool(o["publication"]), "k4-confirmation-relay-publication")
    for target in c["targets"]:
        s = sessions.get(target["session"], {})
        need(
            s.get("role")
            in {"api", "worker-control", "scheduler", "relay", "demand", "worker", "operator"}
            and (kind == "eviction" or s.get("role") == role),
            "k4-confirmation-role",
        )
        need(
            s.get("pod_uid") == target["pod_uid"]
            and s.get("container_id") == target["container_id"],
            "k4-confirmation-exact-session",
        )
        need(
            s.get("death") is not None and bool(s.get("death_evidence")),
            "k4-confirmation-process-death",
        )
        need(
            s.get("start", math.inf) <= c["intervention"] <= (s.get("death") or -1)
            and (target["deleted"] is None or target["deleted"] >= c["intervention"]),
            "k4-confirmation-process-chronology",
        )
        if kind != "scale-down":
            need(
                bool(target["replacement_uid"]) and target["replacement_ready"] is not None,
                "k4-confirmation-replacement",
            )
            need(
                any(
                    s2["pod_uid"] == target["replacement_uid"]
                    and s2["session"] != target["session"]
                    and any(
                        r["present"] and r["at"] == target["replacement_ready"]
                        for r in s2["readiness"]
                    )
                    for s2 in t["sessions"]
                ),
                "k4-confirmation-independent-replacement",
            )
        if kind == "kill":
            need(
                (
                    target["deleted"] is not None
                    or target["restart_after"] > target["restart_before"]
                )
                and (s.get("exit_code") in (None, 137))
                and (s.get("exit_signal") in (None, 9)),
                "k4-confirmation-kill-identity",
            )
    need(c["completed"] is not None and c["removed"] is not None, "k4-confirmation-completion")
    need(
        (c["completed"] is None or c["completed"] >= c["intervention"])
        and (c["removed"] is None or c["removed"] >= c["intervention"]),
        "k4-confirmation-completion-chronology",
    )
    need(
        not c["harness_targeted"] and not c["pdb_bypassed"] and not c["deletion_cost_used"],
        "k4-confirmation-policy",
    )
    if kind in {"rollout", "rollback"}:
        proof = c["binary_proof"]
        need(
            proof["runtime_a"] == proof["runtime_a_prime"]
            and proof["worker_a"] == proof["worker_a_prime"],
            "k4-confirmation-binary-identity",
        )
        need(
            c["image_a"] != c["image_a_prime"]
            and "@sha256:" in c["image_a"]
            and "@sha256:" in c["image_a_prime"]
            and bool(c["build"])
            and c["lineage_a"] == "a"
            and c["lineage_a_prime"] == "a-prime"
            and c["rollout_status"] == "complete"
            and c["revision_a"] != c["revision_a_prime"],
            "k4-confirmation-image-rollout",
        )
        if kind == "rollback":
            need(
                c["setup_completed"] is not None and c["setup_completed"] < c["intervention"],
                "k4-confirmation-rollback-setup",
            )
    if kind == "kill":
        need(
            c["chaos_version"] == "2.7.2"
            and c["chaos_kind"] == "container-kill"
            and bool(c["chaos_object"])
            and c["injection_status"] == "injected",
            "k4-confirmation-chaos-ack",
        )
    if kind == "eviction":
        need(c["no_cordon"] and bool(c["evictions"]), "k4-confirmation-eviction-requests")
        need(
            bool(c["roles"]) and c["eviction_retry_interval"] > 0, "k4-confirmation-role-inventory"
        )
        by_uid = defaultdict(list)
        for e in c["evictions"]:
            by_uid[e["pod_uid"]].append(e)
            need(e["role"] not in {"observer", "loadgen", "collector"}, "k4-confirmation-harness")
            need(e["status"] != 429 or bool(e["denial"]), "k4-confirmation-pdb-denial")
            need(
                e["requested"] <= e["responded"] and e["scheduled"] == c["intervention"],
                "k4-confirmation-eviction-time",
            )
        role_by_uid = {uid: r for r in c["roles"] for uid in r["pod_uids"]}
        need(set(role_by_uid) == set(by_uid), "k4-confirmation-all-roles")
        for uid, rows in by_uid.items():
            role_record = role_by_uid.get(uid, {})
            rows = sorted(rows, key=lambda r: r["requested"])
            need(
                all(
                    b["requested"] - a["responded"] <= c["eviction_retry_interval"]
                    for a, b in zip(rows, rows[1:], strict=False)
                ),
                "k4-confirmation-bounded-eviction-retry",
            )
            if role_record.get("disruptions_allowed") == 0:
                need(
                    len(rows) == 1 and rows[0]["status"] == 429, "k4-confirmation-singleton-denial"
                )
            else:
                need(
                    any(
                        e["status"] in (200, 201) and e["replacement_ready"] is not None
                        for e in rows
                    ),
                    "k4-confirmation-eventual-drain",
                )
    return sorted(set(issues))


def run_checker(checker: Path, root: Path) -> dict:
    process = subprocess.run(
        [
            str(checker.resolve()),
            "-lifecycle",
            str(root / "timeline.json"),
            "-lease-audit",
            str(root / "leases.json"),
        ],
        capture_output=True,
        check=False,
    )
    require(process.returncode in (0, 1) and bool(process.stdout), "k4-checker-execution")
    report = json.loads(process.stdout)
    require(
        report.get("schema") == "dur053.k4-checker.v1"
        and type(report.get("valid")) is bool
        and process.returncode == (0 if report["valid"] else 1),
        "k4-checker-report",
    )
    return report


def episode(root: Path, f: dict, checker: Path) -> dict:
    m = load(root / "episode.json")
    cell = episode_identity(m["episode"])
    require(
        m["replacement_for"] == (cell["episode"] if m["episode"] != cell["episode"] else "")
        and m["arm"] == cell["arm"],
        "k4-episode-replacement-link",
    )
    p = parameters(f, cell["duration"])
    warmup = WARMUP
    if m["budget_scope"] == "engineering-validation":
        from dur053_k4_driver import SMOKE_CELLS, SMOKE_WARMUP, SMOKE_WINDOWS

        require(
            not m["measurement_claim"]
            and m["job"] == "lifecycle-smoke"
            and m["episode"] in SMOKE_CELLS,
            "k4-engineering-profile",
        )
        p["W"] = SMOKE_WINDOWS[cell["duration"]]
        warmup = SMOKE_WARMUP
    require(
        m["freeze_sha256"] == f["sha256"]
        and m["seed"] == cell["seed"]
        and all(m[k] == p[k] for k in ("rate", "L", "W")),
        "k4-episode-frozen-parameters",
    )
    require(
        m["baseline_flags"]
        == (
            ["RUNTIME_SHUTDOWN_MODE=legacy", "WORKER_DRAIN_MODE=legacy"]
            if cell["arm"] == "baseline"
            else []
        ),
        "k4-baseline-flags",
    )
    docs = {}
    for key, name in FILES.items():
        require(
            hashlib.sha256((root / name).read_bytes()).hexdigest() == m["files"][key],
            "k4-episode-file-hash",
        )
        docs[key] = load(root / name)
        require(docs[key]["episode"] == m["episode"], "k4-episode-file-identity")
    t, c = docs["timeline"], docs["confirmation"]
    require(
        t["namespace"] == m["namespace"] and t["witness"] == docs["witness"]["rows"],
        "k4-witness-inventory-binding",
    )
    issues = confirmation(c, t, cell["event"], docs["leases"]["rows"])

    def need(ok, message):
        if not ok:
            issues.append(message)

    need(c["intervention"] == m["intervention"], "k4-intervention-clock")
    need(
        m["intervention"] - m["warmup_start"] >= warmup
        and m["intervention"] - m["warmup_start"] <= warmup + p["confirmation"]
        and math.isclose(m["window_end"] - m["intervention"], p["W"], abs_tol=1e-8),
        "k4-registered-windows",
    )
    arrivals = docs["arrivals"]["rows"]
    need(
        len({a["id"] for a in arrivals}) == len(arrivals)
        and len({a["key"] for a in arrivals}) == len(arrivals),
        "k4-arrival-identity",
    )
    for phase, start, end in (
        ("warmup", m["warmup_start"], m["intervention"]),
        ("measured", m["intervention"], m["window_end"]),
    ):
        rows = [a for a in arrivals if a["phase"] == phase]
        if (root / "arrivals-journal.jsonl").exists():
            from dur053_k4_collectors import journal_rows, utc

            plan, updates, torn = journal_rows((root / "arrivals-journal.jsonl").read_text())
            planned = [r for r in plan["rows"] if start <= utc(r["scheduled_at"]) < end]
            need(
                not torn
                and plan["rate"] == m["rate"]
                and len(rows) == len(planned)
                and all(
                    a["key"] == r["key"]
                    and a["scheduled"] == utc(r["scheduled_at"])
                    and math.isclose(
                        a["scheduled"],
                        m["warmup_start"] + r["sequence"] / m["rate"],
                        rel_tol=0,
                        abs_tol=1e-6,
                    )
                    and a["outcome"] == updates.get(a["key"], {}).get("outcome")
                    for a, r in zip(rows, planned, strict=False)
                ),
                "k4-arrival-schedule",
            )
            continue
        need(
            len(rows) == math.ceil((end - start) * m["rate"] - 1e-8)
            and all(
                math.isclose(a["scheduled"], start + i / m["rate"], abs_tol=1e-7)
                for i, a in enumerate(rows)
            ),
            "k4-arrival-schedule",
        )
    wf = {w["key"]: w for w in t["workflows"]}
    need(
        len(wf) == len(t["workflows"]) and set(wf) <= {a["key"] for a in arrivals},
        "k4-durable-arrival-reconciliation",
    )
    for a in arrivals:
        need(a["outcome"] != "accepted" or a["key"] in wf, "k4-accepted-missing")
        need(
            a["outcome"] not in {"rejected", "unsent"} or a["key"] not in wf,
            "k4-outcome-durable-conflict",
        )
        need(
            a["key"] not in wf or not a["workflow"] or a["workflow"] == wf[a["key"]]["workflow"],
            "k4-workflow-identity",
        )
    deadline = max(m["window_end"], c["removed"] or m["reconciliation_end"]) + p["T"]
    need(m["reconciliation_end"] <= deadline + 1e-7, "k4-reconciliation-bound")
    activity = (
        m["witness_activity"]
        + [v["start"] for v in t["invocations"]]
        + [v["finish"] for v in t["invocations"] if v["finish"] is not None]
    )
    quiet = max(activity, default=m["warmup_start"])
    need(
        m["witness_available"]
        and m["quiet_since"] is not None
        and m["quiet_since"] >= quiet
        and m["reconciliation_end"] - m["quiet_since"] >= QUIET,
        "k4-witness-quiet",
    )
    need(
        all(w["terminal"] is not None and w["state"] == "SUCCEEDED" for w in wf.values()),
        "k4-accepted-incomplete-or-failed",
    )
    check = run_checker(checker, root)
    issues.extend(check["violations"])
    # Unique final-revision SUCCEEDED history timestamp, with enclosing DB/observer uncertainty.
    for w in wf.values():
        final = [
            h
            for h in t["history"]
            if h["identity"]["workflow"] == w["workflow"] and h["kind"] == "terminal"
        ]
        if w["state"] == "SUCCEEDED":
            need(
                len(final) == 1
                and final[0]["at"] == w["terminal"]
                and final[0]["revision"]
                == max(
                    h["revision"]
                    for h in t["history"]
                    if h["identity"]["workflow"] == w["workflow"]
                ),
                "k4-final-terminal-history",
            )
    measured = [a for a in arrivals if a["phase"] == "measured"]

    def latencies(rows):
        values, missing, censored, within, succeeded = [], 0, 0, 0, 0
        for a in rows:
            if a["key"] not in wf:
                continue
            w = wf[a["key"]]
            if w["terminal"] is None:
                censored += 1
                continue
            if w["state"] != "SUCCEEDED":
                missing += 1
                continue
            brackets = [
                b["width"] for b in m["clock_brackets"] if b["start"] <= w["terminal"] <= b["end"]
            ]
            if not brackets:
                missing += 1
                continue
            delay = w["terminal"] - a["scheduled"] + max(w["clock_width"], max(brackets))
            finite(delay, "k4-latency", zero=True)
            values.append(delay)
            succeeded += 1
            within += delay <= m["L"]
        return (
            population(values, missing=missing, censored=censored, long=cell["duration"] == "long"),
            within,
            succeeded,
        )

    latency, within, success = latencies(measured)
    need(latency["missing"] == 0, "k4-latency-evidence-missing")
    counts = Counter(a["outcome"] for a in measured)
    accepted = sum(a["key"] in wf for a in measured)
    nofault = None
    if c["completed"] is not None and c["removed"] is not None:
        start = max(c["completed"], c["removed"], m["intervention"])
        if start < m["window_end"] and not issues:
            pop, _, _ = latencies([a for a in measured if a["scheduled"] >= start])
            nofault = dict(
                label="post-intervention, no active fault",
                start=start,
                end=m["window_end"],
                latency=pop,
            )
    intervals = {r["id"]: r for r in check["intervals"]}
    all_active = [
        v
        for v in t["invocations"]
        if v["start"] <= m["intervention"] < (v["finish"] if v["finish"] is not None else math.inf)
    ]
    targeted_sessions = {v["session"] for v in c["targets"]}
    affected_workflows = {w for o in c["overlap"] for w in o["workflows"]}
    active = [
        v
        for v in all_active
        if v["session"] in targeted_sessions or v["identity"]["workflow"] in affected_workflows
    ]

    def cost(select):
        rows = [intervals[v["id"]] for v in t["invocations"] if select(v, intervals[v["id"]])]
        return dict(
            lower=math.fsum(r["lower"] for r in rows),
            upper=math.fsum(r["upper"] for r in rows)
            if all(r["upper"] is not None for r in rows)
            else None,
            unknown=sum(r["upper"] is None for r in rows),
        )

    wasted = cost(lambda v, r: r["abandoned"] and not r["reexecution"] and v in active)
    reexecuted = cost(lambda v, r: r["reexecution"])
    drain, endpoints, during = [], [], 0
    for s in t["sessions"]:
        terms = [v["at"] for v in s["signals"] if v["signal"] == 15]
        need(not terms or s["death"] is not None, "k4-term-exit-missing")
        if terms and s["death"] is not None:
            term = min(terms)
            drain.append(s["death"] - term)
            during += sum(
                r["category"] == "committed"
                and r["session"] == s["session"]
                and term <= r["at"] <= s["death"]
                for r in t["receipts"]
            )
        withdrawn = [v for v in s["readiness"] if not v["present"]]
        if withdrawn:
            first = min(withdrawn, key=lambda v: v["at"])
            removed = [v for v in s["endpoints"] if not v["present"] and v["at"] >= first["at"]]
            if removed:
                end = min(removed, key=lambda v: v["at"])
                endpoints.append(
                    dict(
                        pod_uid=s["pod_uid"],
                        lower=max(0, end["at"] - first["at"] - first["width"]),
                        upper=end["at"] - first["at"] + end["width"],
                    )
                )
            else:
                need(False, "k4-endpoint-removal-missing")
    leases = docs["leases"]["rows"]
    reacquire = partition_reacquisition(t, leases, m["intervention"])
    stalls = assignment_stalls(docs["kafka"]["rows"], arrivals, t, m["reconciliation_end"])
    need(not any(s["censored"] for s in stalls), "k4-assignment-disposition-censored")
    http = defaultdict(lambda: dict(errors=0, retries=0, requests=0))
    requests = {}
    for r in t["http"]:
        previous = requests.get((r["role"], r["request"]))
        need(previous is None or previous == r["body_hash"], "k4-http-retry-body")
        requests[r["role"], r["request"]] = r["body_hash"]
        http[r["role"]]["requests"] += 1
        http[r["role"]]["errors"] += bool(r["error"]) or r["status"] >= 400
        http[r["role"]]["retries"] += r["attempt"] > 1
    http.setdefault("worker-control", dict(errors=0, retries=0, requests=0))
    claim_requests = {
        r["request"]
        for r in t["http"]
        if r["role"] == "worker-control" and r["operation"] == "claim"
    }
    result_requests = {
        r["request"]
        for r in t["http"]
        if r["role"] == "worker-control" and r["operation"] == "result"
    }
    need(
        len(claim_requests) >= len(t["claims"]) and len(result_requests) >= len(t["receipts"]),
        "k4-http-evidence-missing",
    )
    affected = {v["identity"]["workflow"] for v in active}
    recovery = []
    for w in sorted(affected):
        orig = [v for v in active if v["identity"]["workflow"] == w]
        later = [
            cl["at"]
            for cl in t["claims"]
            if cl["identity"]["workflow"] == w
            and cl["at"] >= m["intervention"]
            and all(
                identity(cl["identity"], attempt=True) != identity(v["identity"], attempt=True)
                for v in orig
            )
        ]
        useful = [
            h["at"]
            for h in t["history"]
            if h["identity"]["workflow"] == w
            and h["kind"] in {"transition", "receipt", "terminal"}
            and h["at"] >= m["intervention"]
        ]
        terminal = next((x["terminal"] for x in wf.values() if x["workflow"] == w), None)
        recovery.append(
            dict(
                workflow=w,
                replacement=min(later) - m["intervention"] if later else None,
                first_useful=min(useful) - m["intervention"] if useful else None,
                terminal=terminal - m["intervention"] if terminal is not None else None,
                censored=terminal is None,
                deadline=deadline,
            )
        )
    observations = docs["observers"]["rows"]
    need(
        bool(observations)
        and all(a["at"] <= b["at"] for a, b in zip(observations, observations[1:], strict=False)),
        "k4-observer-evidence-missing",
    )
    need(bool(m["clock_brackets"]), "k4-clock-brackets-missing")
    age = population(
        [
            s["oldest_claimable"]
            for s in observations
            if not s["error"] and s["oldest_claimable"] is not None
        ],
        missing=sum(bool(s["error"]) or s["oldest_claimable"] is None for s in observations),
    )
    removed_cost = busy_removals(c, observations, t, intervals)
    busy = cost(lambda v, r: True)
    metrics = dict(
        schema="dur053.k4-metrics.v1",
        episode=m["episode"],
        replacement_for=m["replacement_for"],
        status=m["status"],
        complete=check["complete"] and not issues,
        valid=check["valid"] and not issues and m["status"] in {"PASS", "FAIL"},
        issues=sorted(set(issues)),
        latency=latency,
        no_active_fault=nofault,
        offered=len(measured),
        accepted=accepted,
        rejected=counts["rejected"],
        uncertain=counts["uncertain"],
        unsent=counts["unsent"],
        rates={
            k: v / m["W"]
            for k, v in dict(
                offered=len(measured),
                accepted=accepted,
                rejected=counts["rejected"],
                uncertain=counts["uncertain"],
            ).items()
        },
        throughput=success / m["W"],
        slo=dict(
            within=within,
            denominator=accepted,
            fraction=within / accepted if accepted else None,
            passed=accepted > 0
            and within / accepted >= 0.99
            and not m["timeout"]
            and not latency["censored"],
        ),
        abandoned=dict(
            count=sum(intervals[v["id"]]["abandoned"] for v in active),
            denominator=len(active),
            canceled=sum(intervals[v["id"]]["canceled"] for v in active),
            unknown=sum(intervals[v["id"]]["upper"] is None for v in active),
        ),
        wasted_original_seconds=wasted,
        reexecuted_seconds=reexecuted,
        busy_seconds=busy,
        sigterm_to_exit=population(drain),
        results_during_drain=during,
        readiness_endpoint_delay=endpoints,
        partition_reacquisition=reacquire,
        assignment_stalls=stalls,
        http=dict(http),
        submission_errors=sum(a["outcome"] != "accepted" for a in arrivals),
        recovery=recovery,
        duplicate_executions=check["duplicate_executions"],
        executed_iterations=check["executed_iterations"],
        canceled_iterations=check["canceled_iterations"],
        claimed_unexecuted=check["claimed_unexecuted"],
        accepted_results=check["winners"],
        accepted_results_per_executed_non_canceled_iteration=check["winners"]
        / check["executed_iterations"]
        if check["executed_iterations"]
        else None,
        retryable_receipts=check["retryable"],
        final_failure_receipts=check["final_failures"],
        stale_committed=check["stale_committed"],
        stale_rejected=check["stale_rejected"],
        evidence_only=check["evidence_only"],
        superseded_owner_transitions=check["superseded_transitions"],
        duplicate_effects="NOT COVERED (PURE)",
        oldest_claimable_age=age,
        busy_removals=removed_cost,
        observer_gaps=[s["gap"] for s in observations],
        scale_up_delay="NOT APPLICABLE (K4 fixed replicas)",
        limiting_component="unresolved",
        replicas=replica_metrics(t, m),
        limitations=f["limitations"],
        pair_binding=dict(
            images=m["images"],
            head_sha=m["head_sha"],
            tree_sha=m["tree_sha"],
            freeze_sha256=m["freeze_sha256"],
            rate=m["rate"],
            L=m["L"],
            W=m["W"],
        ),
    )
    if issues:
        metrics["no_active_fault"] = None
    cut = (
        max(c["completed"], c["removed"])
        if c["completed"] is not None and c["removed"] is not None
        else None
    )
    fault_rows = [a for a in measured if cut is None or a["scheduled"] < cut]
    fault_pop, _, _ = latencies(fault_rows)
    metrics["fault_window"] = dict(
        start=m["intervention"],
        end=min(cut, m["window_end"]) if cut is not None else m["window_end"],
        latency=fault_pop,
    )
    fault_tail = fault_pop["p99"]
    nofault_tail = (
        metrics["no_active_fault"]["latency"]["p99"] if metrics["no_active_fault"] else None
    )
    metrics["fault_minus_no_active_fault_p99"] = (
        fault_tail - nofault_tail
        if type(fault_tail) in (int, float) and type(nofault_tail) in (int, float)
        else "NOT ESTIMATED"
    )
    metrics["polling_observation_error"] = population(
        [
            w["first_observed"] - w["terminal"]
            for w in wf.values()
            if w["first_observed"] is not None and w["terminal"] is not None
        ],
        missing=sum(w["first_observed"] is None for w in wf.values()),
    )
    metrics["throughput"] = (
        sum(
            a["key"] in wf
            and wf[a["key"]]["state"] == "SUCCEEDED"
            and wf[a["key"]]["terminal"] is not None
            and wf[a["key"]]["terminal"] <= m["window_end"]
            for a in measured
        )
        / m["W"]
    )
    metrics["slo"]["wilson_95_descriptive"] = wilson(within, accepted)
    return metrics


def wilson(successes, n):
    if not n:
        return None
    z = 1.959963984540054
    p = successes / n
    center = (p + z * z / (2 * n)) / (1 + z * z / n)
    radius = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / (1 + z * z / n)
    return [max(0, center - radius), min(1, center + radius)]


def partition_reacquisition(t, leases, intervention):
    reacquire = []
    for s in t["sessions"]:
        if s["role"] != "scheduler" or s["death"] is None:
            continue
        termination = min(
            (v["at"] for v in s["signals"] if v["signal"] == 15), default=intervention
        )
        owned = [
            r
            for r in leases
            if r["session"] == s["session"] and r["kind"] == "acquire" and r["at"] <= termination
        ]
        owned = [
            old
            for old in owned
            if not any(
                r["partition"] == old["partition"]
                and old["order"] < r["order"]
                and r["at"] <= termination
                for r in leases
            )
        ]
        for old in owned:
            new = sorted(
                (
                    r
                    for r in leases
                    if r["kind"] == "acquire"
                    and r["partition"] == old["partition"]
                    and r["owner"] != old["owner"]
                    and r["order"] > old["order"]
                ),
                key=lambda r: r["order"],
            )
            released = [
                r
                for r in leases
                if r["kind"] == "release"
                and r["partition"] == old["partition"]
                and r["epoch"] == old["epoch"]
                and r["owner"] == old["owner"]
            ]
            release = min(released, key=lambda r: r["order"]) if released else None
            useful = [
                h["at"]
                for h in t["history"]
                if new
                and h["partition"] == old["partition"]
                and h["owner"] == new[0]["owner"]
                and h["order"] > new[0]["order"]
            ]
            reacquire.append(
                dict(
                    partition=old["partition"],
                    old_epoch=old["epoch"],
                    mode="release" if release else "TTL recovery",
                    seconds=new[0]["at"] - (release["at"] if release else intervention)
                    if new
                    else None,
                    first_useful=min(useful) if useful else None,
                    terminal_progress=[
                        w["terminal"] for w in t["workflows"] if w["terminal"] is not None
                    ],
                )
            )
    return reacquire


def assignment_stalls(rows, arrivals, t, end):
    result = []
    for change in sorted({r["change"] for r in rows}):
        group = [r for r in rows if r["change"] == change]
        triggers = [r for r in group if r["event"] in {"revoke", "rebalance_start"}]
        require(bool(triggers), "k4-assignment-trigger")
        trigger = min(triggers, key=lambda r: r["at"])
        generation = max(r["generation"] for r in group)
        survivors = set(trigger["survivors"])
        stable = [
            r
            for r in group
            if r["event"] == "stable"
            and r["generation"] == generation
            and r["consumer"] in survivors
            and r["at"] >= trigger["at"]
        ]
        stable_at = (
            max((r["at"] for r in stable), default=None)
            if survivors == {r["consumer"] for r in stable} and survivors
            else None
        )
        disposed = [
            r
            for r in group
            if r["event"] == "disposed"
            and stable_at is not None
            and r["at"] >= stable_at
            and r["generation"] == generation
            and r["consumer"] in survivors
            and any(a["id"] == r["arrival"] for a in arrivals)
            and any(
                h["order"] == r["disposition_order"]
                and h["kind"] in {"receipt", "transition", "terminal"}
                and any(
                    a["workflow"] == h["identity"]["workflow"] and a["id"] == r["arrival"]
                    for a in arrivals
                )
                for h in t["history"]
            )
        ]
        useful = min((r["at"] for r in disposed), default=None)
        active = [
            min(v["finish"] if v["finish"] is not None else end, end) - v["start"]
            for v in t["invocations"]
            if v["start"] <= trigger["at"] and (v["finish"] is None or v["finish"] >= trigger["at"])
        ]
        result.append(
            dict(
                change=change,
                trigger_uid=trigger["trigger_uid"],
                consumers=sorted(survivors),
                assignment_only=stable_at - trigger["at"] if stable_at is not None else None,
                useful_disposition=useful - stable_at if useful is not None else None,
                total=useful - trigger["at"] if useful is not None else None,
                censored=useful is None,
                longest_active=max(active, default=0),
            )
        )
    return result


def busy_removals(c, samples, t, intervals):
    busy, idle, ambiguous, interrupted = [], [], False, 0
    removed = {v["pod_uid"] for v in c["targets"]}
    for target in c["targets"]:
        at = target["deleted"]
        snapshots = [s for s in samples if s["at"] == at and not s["error"]]
        if len(snapshots) != 1:
            ambiguous = True
            continue
        sample = snapshots[0]
        claims = [r for r in sample["active_claims"] if r["pod_uid"] == target["pod_uid"]]
        joined = [
            v
            for v in t["invocations"]
            if any(v["identity"] == r["identity"] for r in claims)
            and v["start"] <= at
            and (v["finish"] is None or v["finish"] > at)
        ]
        ambiguous |= len(joined) != len(claims)
        if claims:
            busy.append(target["pod_uid"])
            interrupted += len(claims)
        claimed_uids = {r["pod_uid"] for r in sample["active_claims"]}
        idle.extend(
            p["pod_uid"]
            for p in sample["pods"]
            if p["role"] == "worker"
            and p["eligible"]
            and p["ready"]
            and p["pod_uid"] not in removed | claimed_uids
        )
    rows = [
        intervals[v["id"]]
        for v in t["invocations"]
        if any(s["session"] == v["session"] and s["pod_uid"] in removed for s in t["sessions"])
    ]
    return dict(
        busy_uids=sorted(set(busy)),
        idle_survivors=sorted(set(idle)),
        interrupted_claims=interrupted,
        ambiguous=ambiguous or any(r["upper"] is None for r in rows),
        busy_upper=math.fsum(r["upper"] for r in rows)
        if all(r["upper"] is not None for r in rows)
        else None,
    )


def replica_metrics(t, m):
    start, end = m["intervention"], m["window_end"]
    workers = [s for s in t["sessions"] if s["role"] == "worker"]
    seconds = math.fsum(
        max(0, min(s["death"] or end, end) - max(s["start"], start)) for s in workers
    )
    ready = 0
    for s in workers:
        rows = sorted(s["readiness"], key=lambda r: r["at"])
        for i, row in enumerate(rows):
            if row["present"]:
                ready += max(
                    0,
                    min(rows[i + 1]["at"] if i + 1 < len(rows) else s["death"] or end, end)
                    - max(row["at"], start),
                )
    busy = math.fsum(
        max(
            0,
            min(v["finish"] if v["finish"] is not None else v["last_alive"], end)
            - max(v["start"], start),
        )
        for v in t["invocations"]
    )
    events = Counter(s["start"] for s in workers)
    deaths = Counter(s["death"] for s in workers if s["death"] is not None)
    return dict(
        existing_seconds=seconds,
        ready_seconds=ready,
        slot_seconds=2 * seconds,
        utilisation_lower=busy / (2 * seconds) if seconds else None,
        increases=sum(v for k, v in events.items() if start <= k <= end),
        decreases=sum(v for k, v in deaths.items() if start <= k <= end),
        desired_changes="NOT APPLICABLE (K4 fixed replicas)",
        keda_fallback="NOT APPLICABLE",
    )


def summaries(metrics: list[dict]) -> dict:
    by_id = {m["replacement_for"] or m["episode"]: m for m in metrics}
    require(len(by_id) == len(metrics), "k4-summary-duplicate")
    paired = []
    for r in registry():
        if r["kind"] != "core" or r["arm"] != "baseline":
            continue
        b = by_id.get(r["episode"])
        t = by_id.get(r["unit"] + "-treatment")
        if b is None or t is None:
            continue
        require(b["pair_binding"] == t["pair_binding"], "k4-matched-pair-binding")
        diff = {}
        for q in ("p50", "p90", "max", "p99"):
            x, y = b["latency"][q], t["latency"][q]
            diff[q] = (
                y - x if type(x) in (int, float) and type(y) in (int, float) else "NOT ESTIMATED"
            )
        paired.append(
            dict(
                event=r["event"],
                duration=r["duration"],
                pair=r["pair"],
                replaced=bool(b["replacement_for"] or t["replacement_for"]),
                episodes=[b["episode"], t["episode"]],
                latency_difference=diff,
                abandoned=t["abandoned"]["count"] - b["abandoned"]["count"],
                valid=b["valid"] and t["valid"],
                scalar_difference={
                    key: t[key] - b[key]
                    for key in (
                        "throughput",
                        "offered",
                        "accepted",
                        "rejected",
                        "uncertain",
                        "unsent",
                        "duplicate_executions",
                        "accepted_results",
                        "stale_committed",
                        "stale_rejected",
                        "superseded_owner_transitions",
                        "results_during_drain",
                        "submission_errors",
                    )
                },
                cost_difference={
                    key: dict(
                        lower=t[key]["lower"] - b[key]["upper"]
                        if b[key]["upper"] is not None
                        else None,
                        upper=t[key]["upper"] - b[key]["lower"]
                        if t[key]["upper"] is not None
                        else None,
                    )
                    for key in ("wasted_original_seconds", "reexecuted_seconds", "busy_seconds")
                },
            )
        )
    gates = {}
    campaign_safe = len(by_id) == len(registry()) and all(
        m["valid"] and m["complete"] for m in metrics
    )
    for duration in ("short", "long"):
        rows = []
        complete = True
        safety = campaign_safe
        for pair in range(5):
            unit = f"{duration}-core-1-pair-{pair}"
            b, t = by_id.get(unit + "-baseline"), by_id.get(unit + "-treatment")
            complete &= b is not None and t is not None
            if t is None:
                continue
            safety &= t["valid"] and bool(b and b["valid"])
            removal = t["busy_removals"]
            rows.append(
                dict(
                    wasted_lower=t["wasted_original_seconds"]["lower"],
                    reexecuted_lower=t["reexecuted_seconds"]["lower"],
                    busy_upper=removal["busy_upper"] or 0,
                    busy_removed=bool(removal["busy_uids"]),
                    idle_survivor=bool(removal["idle_survivors"]),
                    ambiguous=removal["ambiguous"]
                    or t["wasted_original_seconds"]["unknown"] > 0
                    or t["reexecuted_seconds"]["unknown"] > 0,
                )
            )
        gates[duration] = k7(rows, complete_pairs=complete, safety=safety)
    return dict(
        schema="dur053.k4-summary.v1",
        paired=paired,
        k7=gates,
        k7_decision="GO" if any(g["decision"] == "GO" for g in gates.values()) else "NO-GO",
        disposition_counts=dict(Counter(m["status"] for m in metrics)),
        missing_episodes=sorted({r["episode"] for r in registry()} - by_id.keys()),
        cells=[
            dict(
                event=event,
                duration=duration,
                pairs=[p for p in paired if p["event"] == event and p["duration"] == duration],
            )
            for event, duration in sorted({(p["event"], p["duration"]) for p in paired})
        ],
    )


def verify_evidence(repository: Path, checker: Path) -> dict:
    from dur053_record_k4 import contributions, inventory, metric_episodes, validate_job

    f = frozen(repository)
    manifests, ledger = inventory(repository)
    metrics = []
    verified = []
    for path, m in manifests:
        with materialize(path.parent, repository) as root:
            validate_job(
                root,
                f,
                history={
                    name: v
                    for name, v in ((x[1]["job_id"], x[1]) for x in manifests)
                    if name != m["job_id"]
                },
                intake=False,
            )
            for name in metric_episodes(m):
                value = episode(root / name, f, checker)
                stored = json.loads((root / name / "metrics.json").read_bytes())
                require(value == stored, "k4-stored-metrics")
                if name in contributions(m):
                    metrics.append(value)
                    verified.append(name)
            computed = summaries([episode(root / name, f, checker) for name in metric_episodes(m)])
            require(
                computed == json.loads((root / "summary.json").read_bytes()), "k4-stored-summary"
            )
    result = summaries(metrics)
    result["retained_jobs"] = [
        dict(
            job=m["job"],
            run_id=m["run_id"],
            status=m["status"],
            dispositions=m["dispositions"],
            charged_seconds=m["charged_seconds"],
            pending=m["pending"],
        )
        for _, m in manifests
    ]
    result["schedule"] = remaining(f, verified, ledger["entries"])
    result["complete"] = not result["missing_episodes"] and all(m["complete"] for m in metrics)
    result["valid"] = result["complete"] and all(m["valid"] for m in metrics)
    return result


def main():
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["derive", "verify-evidence"])
    p.add_argument("--checker", type=Path, required=True)
    p.add_argument("--repository", type=Path, default=ROOT)
    p.add_argument("--episode", type=Path)
    a = p.parse_args()
    result = (
        verify_evidence(a.repository, a.checker)
        if a.command == "verify-evidence"
        else episode(a.episode, frozen(a.repository), a.checker)
    )
    print(json.dumps(result, sort_keys=True, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
