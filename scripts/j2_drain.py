"""J2 drain state machine and reconciliation from retained, independently checked evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
import time
import zipfile
from pathlib import Path

from dur053_pilot import witness_events
from j2_checker_report import (
    IntegrityError,
    ResourceEvidenceError,
    indexed_report,
    require_checker_pass,
)
from j2_offline import SECOND, ClockEvidenceError, attempt_key, clock_bounds, drain_identities, join
from j2_qualification_evidence import DRAIN_SECONDS, file_record, read, require, write
from j2_schedule import schedule_slots, sender_report

QUIET_NS = 30 * SECOND
POLL_SECONDS = 2


class DrainExpired(TimeoutError):
    classification = "drain_failure"

    def __init__(self, message, result):
        super().__init__(message)
        self.result = result


def json_bytes(value, *, sort_keys=True):
    return json.dumps(value, separators=(",", ":"), sort_keys=sort_keys).encode()


def expected_arrivals(raw, schedule, *, schedule_sha256=None):
    report = sender_report(raw, schedule, [], schedule_sha256=schedule_sha256)
    manifest = json.loads(raw.split(b"\n", 1)[0])["plan"]
    arrivals = {}
    for i, (row, (scheduled, _)) in enumerate(
        zip(manifest["rows"], schedule_slots(schedule), strict=True)
    ):
        key = row["key"]
        run_id = schedule["run_id"]
        activity = {8: "pure.echo", 9: "pure.add"}.get(i % 10, "dur048.sleep")
        payload = [2, 3] if activity == "pure.add" else {"run_id": run_id, "sequence": i}
        expected = 5 if activity == "pure.add" else payload
        fingerprint = {
            "definition_id": f"dur053-{run_id}-{activity}",
            "definition_version": 1,
            "initial_node_id": activity,
            "initial_input": payload,
            "payload": payload,
        }
        request = {
            **fingerprint,
            "workflow_id": key,
            "submission_key": key,
            "namespace": schedule["namespace"],
            "actor_id": "dur053-loadgen",
        }
        require(
            row["request"] == request
            and row["expected"] == expected
            and row["activity"] == activity,
            f"registered payload or activity differs: {key}",
        )
        arrivals[key] = {
            "scheduled_ns": scheduled,
            "result": expected,
            "namespace": schedule["namespace"],
            "hash": "sub-v1:"
            + hashlib.sha256(json_bytes(fingerprint, sort_keys=False)).hexdigest(),
            "request_hash": hashlib.sha256(json_bytes(request)).hexdigest(),
        }
    # Keep the fsynced intents and responses bound to their original request.
    for line in raw.splitlines(keepends=True)[1:]:
        if not line.endswith(b"\n"):
            break
        event = json.loads(line)
        if event["event"] == "arrival":
            key = event["key"]
            require(
                event["request_hash"] == arrivals[key]["request_hash"],
                f"journal request hash differs: {key}",
            )
            require(event["status"] != 429, f"unexpected HTTP 429: {key}")
        elif event["event"] == "intent":
            key = event["key"]
            require(
                event["request_sha256"] == arrivals[key]["request_hash"],
                f"journal request hash differs: {key}",
            )
        elif event["event"] == "outcome":
            require(event["status"] != 429, f"unexpected HTTP 429: {event['key']}")
    return arrivals, report


def readiness(state, expected, sender):
    submissions = state["db"]["submissions"]
    admitted = {row["id"] for row in submissions}
    unsent = set(sender["explicitly_unsent"])
    if len(admitted) != len(submissions) or admitted - expected.keys() or admitted & unsent:
        raise IntegrityError("unexpected, duplicated or explicitly unsent durable identity")
    for row in submissions:
        key = row["id"]
        if (
            row["key"] != key
            or row["hash"] != expected[key]["hash"]
            or row["namespace"] != expected[key]["namespace"]
        ):
            raise IntegrityError(f"durable key or request hash differs: {key}")
    succeeded = state["db"]["succeeded"]
    if len(set(succeeded)) != len(succeeded) or set(succeeded) - admitted:
        raise IntegrityError("duplicated or unexpected final-revision success")
    missing = (expected.keys() - unsent) - set(succeeded)
    missing_admitted = set(sender["acknowledged"]) - admitted
    pending = admitted - set(succeeded)
    unpublished, claimed = state["db"]["unpublished"], state["db"]["claimed"]
    counts = state["db"]["terminal_failures"]
    if counts:
        raise IntegrityError(f"non-success terminals: {counts}")
    return {
        "ready": not (missing or missing_admitted or unpublished or claimed),
        "missing": sorted(missing),
        "missing_acknowledged": sorted(missing_admitted),
        "pending": sorted(pending),
        "admitted": len(admitted),
        "succeeded": len(succeeded),
        "unpublished": unpublished,
        "claimed": claimed,
    }


def fingerprint(state):
    return hashlib.sha256(json_bytes({"db": state["db"], "witness": state["witness"]})).hexdigest()


def checked_cut(report, bracket, witness):
    """Derive expiry counts from the exact snapshot the locked checker read."""
    trace = report["trace"]
    tables = trace["Snapshot"]
    submissions = [
        {
            "id": row["workflow_id"],
            "key": row["submission_key"],
            "hash": row["submission_payload_hash"],
            "namespace": row["namespace"],
            "revision": row["revision"],
            "state": row["state"],
        }
        for row in tables["engine.workflow_executions"]["rows"]
    ]
    successes = {r["id"]: r["revision"] for r in submissions if r["state"] == "SUCCEEDED"}
    succeeded = [
        row["workflow_id"]
        for row in tables["engine.transition_history"]["rows"]
        if row["new_state"] == "SUCCEEDED" and successes.get(row["workflow_id"]) == row["revision"]
    ]
    require(set(succeeded) == successes.keys(), "checked success lacks a final revision")
    return {
        **bracket,
        "db": {
            "submissions": submissions,
            "succeeded": sorted(succeeded),
            "unpublished": trace["UnpublishedOutbox"],
            "claimed": sum(
                row["state"] == "CLAIMED" for row in tables["engine.activity_attempts"]["rows"]
            ),
            "terminal_failures": sum(
                row["state"] in {"FAILED", "CANCELED", "REJECTED", "ABANDONED"}
                for row in submissions
            ),
        },
        "witness": witness,
    }


def expire_drain(stop_ns, deadline, last, check_expiry, expected, sender, retain, *, now):
    state, predicates = last or (None, None)
    failure = {
        "status": "FAIL",
        "deadline_mono_ns": deadline,
        "last_verified_read": state,
        "last_verified_counts": predicates,
    }
    retain("deadline", {**failure, "check_status": "pending"})
    try:
        final = check_expiry()
        retain("final", final)
        require(
            deadline <= final["lower_mono_ns"] <= final["upper_mono_ns"],
            "expiry snapshot does not bracket a read at or after the deadline",
        )
        residuals = readiness(final, expected, sender)
        failure.update(
            **residuals,
            pending_lower_bound_ns={
                key: max(0, final["lower_unix_ns"] - expected[key]["scheduled_ns"])
                for key in residuals["pending"]
            },
            final_fingerprint=fingerprint(final),
            drain_seconds=(deadline - stop_ns) / SECOND,
            checked_export_seconds=(now() - stop_ns) / SECOND,
            expiry_read_seconds=(final["upper_mono_ns"] - stop_ns) / SECOND,
        )
        if residuals["missing_acknowledged"]:
            raise IntegrityError(
                f"acknowledged workflows absent at expiry: {residuals['missing_acknowledged']}"
            )
        if state:
            revisions = {row["id"]: row["revision"] for row in final["db"]["submissions"]}
            require(
                all(
                    revisions.get(row["id"], -1) >= row["revision"]
                    for row in state["db"]["submissions"]
                )
                and set(state["db"]["succeeded"]) <= set(final["db"]["succeeded"]),
                "durable identities, revisions or successes regressed at expiry",
            )
    except Exception as error:
        resource_gap = isinstance(error, ResourceEvidenceError)
        retain(
            "deadline",
            {
                **failure,
                "check_status": "failed",
                "classification": error.classification if resource_gap else "integrity_stop",
                "error": str(error),
            },
        )
        if resource_gap:
            raise
        raise IntegrityError(f"expiry check failed: {error}") from error
    failure.update(check_status="PASS", classification="drain_failure")
    retain("deadline", failure)
    raise DrainExpired(
        f"600 s drain deadline expired with {len(residuals['missing'])} missing IDs", failure
    )


def quiet_drain(
    *,
    stop_ns,
    observe,
    check,
    check_expiry,
    expected,
    sender,
    retain,
    now=time.monotonic_ns,
    sleep=time.sleep,
):
    """Check a candidate before starting quiet, then verify it through the last read."""
    deadline = stop_ns + DRAIN_SECONDS * SECOND
    initial = last = None
    quiet_upper = None
    checked_digest = None
    checker_completed = None
    while now() < deadline:
        try:
            state = observe()
        except (OSError, TimeoutError) as error:
            quiet_upper = checked_digest = None
            retain("read-error", {"error": str(error), "at_ns": now()})
        else:
            if state["upper_mono_ns"] > deadline or state["lower_mono_ns"] < stop_ns:
                break
            if initial is None:
                initial = state
                retain("initial", state)
            predicates = readiness(state, expected, sender)
            last = state, predicates
            digest = fingerprint(state)
            retain(
                "poll",
                {
                    **predicates,
                    "fingerprint": digest,
                    "lower_mono_ns": state["lower_mono_ns"],
                    "upper_mono_ns": state["upper_mono_ns"],
                    "lower_unix_ns": state["lower_unix_ns"],
                    "upper_unix_ns": state["upper_unix_ns"],
                },
            )
            if not predicates["ready"]:
                quiet_upper = checked_digest = None
            elif digest != checked_digest:
                quiet_upper = checked_digest = None
                problem = None
                try:
                    check(state)
                except ResourceEvidenceError as error:
                    retain("check-invalid", {"error": str(error), "at_ns": now()})
                    raise
                except (ValueError, RuntimeError, OSError) as error:
                    problem = error
                checker_completed = now()
                if checker_completed > deadline:
                    break
                try:
                    after = observe()
                except (OSError, TimeoutError) as error:
                    retain("read-error", {"error": str(error), "at_ns": now()})
                else:
                    if after["upper_mono_ns"] > deadline:
                        break
                    current = readiness(after, expected, sender)
                    last = after, current
                    if current["ready"] and digest == fingerprint(after):
                        if problem is not None:
                            raise IntegrityError(str(problem)) from problem
                        checked_digest, quiet_upper = digest, after["upper_mono_ns"]
                        retain("quiet-start", after)
                    elif problem is not None:
                        retain("check-retry", {"error": str(problem)})
            elif state["lower_mono_ns"] - quiet_upper >= QUIET_NS:
                retain("final", state)
                return {
                    "status": "PASS",
                    "deadline_mono_ns": deadline,
                    "quiet_start_upper_mono_ns": quiet_upper,
                    "quiet_end_lower_mono_ns": state["lower_mono_ns"],
                    "drain_seconds": (state["upper_mono_ns"] - stop_ns) / SECOND,
                    "checked_export_seconds": (checker_completed - stop_ns) / SECOND,
                    "final_fingerprint": digest,
                    **predicates,
                }
        delay = min(POLL_SECONDS, (deadline - now()) / SECOND)
        if delay > 0:
            sleep(delay)
    remaining = deadline - now()
    if remaining > 0:
        sleep(remaining / SECOND)
    expire_drain(stop_ns, deadline, last, check_expiry, expected, sender, retain, now=now)


def checked_submission_keys(submissions, expected):
    admitted = []
    for s in submissions:
        key = s["WorkflowID"]
        require(
            key in expected
            and s["Key"] == key
            and s["Hash"] == expected[key]["hash"]
            and s["Namespace"] == expected[key]["namespace"],
            f"checked durable key or payload hash differs: {key}",
        )
        admitted.append(key)
    return admitted


def checked_results(
    report,
    expected,
    sender,
    witness,
    polls,
    uncertainty,
    observed,
    *,
    expiry=None,
    snapshot_only=False,
):
    require_checker_pass(report)
    trace = report["trace"]
    admitted = checked_submission_keys(trace["Submissions"], expected)
    admitted_ids = set(admitted)
    if admitted_ids.intersection(sender["explicitly_unsent"]):
        raise IntegrityError("explicitly refused identity has a checked durable admission")
    pending = report["non_terminal"]
    if expiry is None:
        require(not pending, "locked checker failed terminal checks")
        require(
            admitted_ids == expected.keys() - set(sender["explicitly_unsent"]),
            "checked admission does not cover planned identities",
        )
    else:
        residuals = readiness(expiry, expected, sender)
        require(
            admitted_ids == {row["id"] for row in expiry["db"]["submissions"]}
            and len(set(pending)) == len(pending)
            and set(pending) == set(residuals["pending"]),
            "checked identities or pending set differ from the expiry snapshot",
        )
    completed = admitted_ids - set(pending)
    checked_ids = report.get("workflow_ids")
    if snapshot_only:
        require(checked_ids is None, "offline checker unexpectedly supplied a live ID list")
        checked_ids = [
            row["workflow_id"] for row in trace["Snapshot"]["engine.workflow_executions"]["rows"]
        ]
    require(checked_ids is not None, "locked checker did not identify the live scope")
    identities = drain_identities(admitted, sender["acknowledged"], admitted, checked_ids)
    require(identities["valid"], f"checked identity mismatch: {identities}")
    events = witness_events(witness)
    starts, finishes = {}, {}
    for event in events:
        require(
            event["event"] in {"start", "end"} and event["workflow_id"] in admitted_ids,
            "unexpected invocation event or workflow",
        )
        rows = starts if event["event"] == "start" else finishes
        iid = event["invocation_id"]
        require(iid not in rows and iid, "duplicate or incomplete invocation identity")
        rows[iid] = event
    require(
        finishes.keys() <= starts.keys()
        and all(
            starts[iid]["workflow_id"] not in completed for iid in starts.keys() - finishes.keys()
        ),
        "missing invocation start or finish",
    )
    successful_attempts = set()
    for iid, end in finishes.items():
        begin = starts[iid]
        require(
            attempt_key(begin) == attempt_key(end)
            and all(begin[field] == end[field] for field in ("pod_uid", "session", "claim_token"))
            and begin["sequence"] < end["sequence"]
            and begin["monotonic_ns"] <= end["monotonic_ns"],
            f"incomparable invocation finish: {iid}",
        )
        if expiry is not None and end["outcome"] == "SUCCEEDED":
            key = attempt_key(end)
            require(key not in successful_attempts, "duplicate witnessed success for one attempt")
            successful_attempts.add(key)

    joined = []
    succeeded = []
    for row in join(
        [(key, expected[key]["scheduled_ns"]) for key in admitted if key in completed],
        trace,
        events,
        polls,
        uncertainty,
        observed,
    ):
        key = row["workflow_id"]
        if row["status"] == "terminal_clock_or_observation":
            raise ClockEvidenceError(f"invalid final revision/attempt for {key}: {row['status']}")
        require(
            row["status"] == "observed",
            f"invalid final revision/attempt for {key}: {row['status']}",
        )
        require(
            row["result_receipts"][0]["payload"] == expected[key]["result"]
            and finishes[row["witness_starts"][0]["invocation_id"]]["outcome"] == "SUCCEEDED",
            f"winning result or witness outcome differs: {key}",
        )
        succeeded.append(key)
        joined.append(
            {
                name: row[name]
                for name in (
                    "workflow_id",
                    "scheduled_ns",
                    "terminal_ns",
                    "phase_ns",
                    "winning_attempt",
                    "first_observed_ns",
                    "polling_minus_durable_ns",
                )
            }
        )
        joined[-1]["winning_attempt"] = list(row["winning_attempt"])
    require(
        len(succeeded) == len(completed) and set(succeeded) == completed,
        "admitted, uniquely succeeded and checked identities differ",
    )
    return sorted(joined, key=lambda row: row["workflow_id"])


def read_polls(path):
    first = {}
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            record = json.loads(line)
            for key in record["terminal_ids"]:
                first.setdefault(key, record["upper_unix_ns"])
    return first


def cohort_counts(schedule, phases):
    start, warm = schedule["origin_ns"], schedule["warmup_end_ns"]
    stop = schedule["stop_ns"]
    hold_end = schedule.get("hold_end_ns", stop)
    windows = [
        ("warmup", start, warm),
        ("hold" if "periods" in schedule else "grid", warm, hold_end),
    ]
    if "periods" in schedule:
        windows.append(("step", hold_end, stop))
    slots = [at for at, _ in schedule_slots(schedule)]
    return {
        name: {
            "offered": sum(lower <= at < upper for at in slots),
            "checked": sum(lower <= row["scheduled_ns"] < upper for row in phases),
            "successful_in_window": sum(
                lower <= row["scheduled_ns"] <= row["terminal_ns"] < upper for row in phases
            ),
        }
        for name, lower, upper in windows
    }


def replay_episode(root, *, snapshot_only=False):
    schedule = read(root / "schedule.json")
    raw = (root / "sender-journal.jsonl").read_bytes()
    binding = file_record(root / "schedule.json")
    require(
        binding == read(root / "schedule-binding.json"),
        "schedule hash differs from journal binding",
    )
    expected, sender = expected_arrivals(raw, schedule, schedule_sha256=binding["sha256"])
    result = read(root / "drain-result.json")
    expired = result.get("classification") == "drain_failure"
    require(result["status"] == ("FAIL" if expired else "PASS"), "invalid drain replay outcome")
    report, _ = indexed_report(
        root / "live-checker.json", join_trace=True, verify_tables=True, drain_counts=expired
    )
    require_checker_pass(report)
    first = read_polls(root / "terminal-polls.jsonl")
    final = read(root / "final.json")
    witness = read(root / "witness.json")
    observed = final["upper_unix_ns"]
    require(
        fingerprint(final) == result["final_fingerprint"],
        "final DB evidence differs from retained drain result",
    )
    expiry = None
    if expired:
        require(
            result["deadline_mono_ns"] <= final["lower_mono_ns"] <= final["upper_mono_ns"]
            and result["check_status"] == "PASS",
            "invalid final expiry read",
        )
        inventories = sorted(
            (row["inventory"] for row in witness["ledgers"]), key=lambda row: row["pod_uid"]
        )
        expiry = checked_cut(report, final, inventories)
        require(expiry == final, "expiry snapshot or witness differs from the retained final read")
        residuals = readiness(expiry, expected, sender)
        require(all(result[k] == v for k, v in residuals.items()), "expiry residuals differ")
    try:
        probes = read(root / "clock.json")
    except (OSError, ValueError) as error:
        raise ClockEvidenceError("missing or malformed observer/DB clock bracket") from error
    uncertainty = clock_bounds(probes, schedule["origin_ns"], observed)
    joined = checked_results(
        report,
        expected,
        sender,
        witness,
        first,
        uncertainty,
        observed,
        expiry=expiry,
        snapshot_only=snapshot_only,
    )
    return joined, uncertainty


REPLAY_FILES = (
    "schedule.json",
    "schedule-binding.json",
    "sender-journal.jsonl",
    "live-checker.json",
    "witness.json",
    "terminal-polls.jsonl",
    "final.json",
    "clock.json",
    "drain-result.json",
    "phase-rows.json",
    "cohorts.json",
)


def replay_archive(archive, output):
    evidence = {}
    with zipfile.ZipFile(archive) as bundle:
        index = json.loads(bundle.read("freeze.json"))["files"]
        passed = (
            "status.json" in index and json.loads(bundle.read("status.json"))["status"] == "PASS"
        )
        for name in ("grid-drain", "step-drain"):
            # The Actions ZIP has already been verified. Re-bind the exact
            # members used by replay, including the two-period schedule.
            if f"{name}/phase-rows.json" not in index:
                require(not passed, f"successful run is missing replay inputs: {name}")
                evidence[name] = {"status": "unperformed"}
                continue
            with tempfile.TemporaryDirectory(dir=output.parent) as temp:
                root = Path(temp)
                for member in REPLAY_FILES:
                    key = f"{name}/{member}"
                    destination = root / member
                    with bundle.open(key) as source, destination.open("wb") as target:
                        shutil.copyfileobj(source, target)
                    require(file_record(destination) == index[key], f"replay member differs: {key}")
                joined, uncertainty = replay_episode(root)
                require(
                    joined == read(root / "phase-rows.json"), f"offline phase rows differ: {name}"
                )
                cohorts = cohort_counts(read(root / "schedule.json"), joined)
                require(cohorts == read(root / "cohorts.json"), f"offline cohorts differ: {name}")
                result = read(root / "drain-result.json")
                evidence[name] = {
                    "status": result["status"],
                    "checked": len(joined),
                    "cohorts": cohorts,
                    "clock_uncertainty_ns": uncertainty,
                    "schedule": index[f"{name}/schedule.json"],
                }
                if result.get("classification") == "drain_failure":
                    evidence[name].update(classification="drain_failure", check_status="PASS")
    write(output, evidence)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("receipt", type=Path)
    args = parser.parse_args()
    replay_archive(args.archive, args.receipt)
