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
from j2_checker_report import indexed_report
from j2_offline import SECOND, attempt_key, clock_bounds, drain_identities, join
from j2_qualification_evidence import DRAIN_SECONDS, file_record, read, require, write
from j2_schedule import k4_report, schedule_slots

QUIET_NS = 30 * SECOND
POLL_SECONDS = 2


class DrainExpired(TimeoutError):
    classification = "drain_failure"


class IntegrityError(ValueError):
    classification = "integrity_stop"


def json_bytes(value, *, sort_keys=True):
    return json.dumps(value, separators=(",", ":"), sort_keys=sort_keys).encode()


def expected_arrivals(raw, schedule):
    # k4_report verifies schedule geometry independently of the header rate.
    report = k4_report(raw, schedule, [])
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


def quiet_drain(
    *,
    stop_ns,
    observe,
    check,
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
    state, predicates = last or (None, {"missing": sorted(expected), "pending": []})
    lower = state["lower_unix_ns"] if state else None
    failure = {
        "status": "FAIL",
        "deadline_mono_ns": deadline,
        **predicates,
        "last_verified_read": state,
        "pending_lower_bound_ns": {
            key: max(0, lower - expected[key]["scheduled_ns"]) for key in predicates["pending"]
        },
    }
    retain("deadline", failure)
    raise DrainExpired(
        f"600 s drain deadline expired with {len(predicates['missing'])} missing IDs"
    )


def checked_results(report, expected, sender, witness, polls, uncertainty, observed):
    require(
        report["status"] == "PASS" and report["valid"] is True and not report["non_terminal"],
        "locked checker failed structural or terminal checks",
    )
    trace = report["trace"]
    submissions = trace["Submissions"]
    admitted = [s["WorkflowID"] for s in submissions]
    admitted_ids = set(admitted)
    for s in submissions:
        key = s["WorkflowID"]
        require(
            key in expected
            and s["Key"] == key
            and s["Hash"] == expected[key]["hash"]
            and s["Namespace"] == expected[key]["namespace"],
            f"checked durable key or payload hash differs: {key}",
        )
    identities = drain_identities(
        admitted, sender["acknowledged"], admitted, report["workflow_ids"]
    )
    require(
        identities["valid"] and admitted_ids == expected.keys() - set(sender["explicitly_unsent"]),
        f"checked identity mismatch: {identities}",
    )
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
    require(starts.keys() == finishes.keys(), "missing invocation start or finish")
    for iid, begin in starts.items():
        end = finishes[iid]
        require(
            attempt_key(begin) == attempt_key(end)
            and all(begin[field] == end[field] for field in ("pod_uid", "session", "claim_token"))
            and begin["sequence"] < end["sequence"]
            and begin["monotonic_ns"] <= end["monotonic_ns"],
            f"incomparable invocation finish: {iid}",
        )

    joined = []
    succeeded = []
    for row in join(
        [(key, expected[key]["scheduled_ns"]) for key in admitted],
        trace,
        events,
        polls,
        uncertainty,
        observed,
    ):
        key = row["workflow_id"]
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
        drain_identities(admitted, sender["acknowledged"], succeeded, report["workflow_ids"])[
            "valid"
        ],
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


def replay_episode(root):
    schedule = read(root / "schedule.json")
    raw = (root / "sender-journal.jsonl").read_bytes()
    require(
        file_record(root / "schedule.json") == read(root / "schedule-binding.json"),
        "schedule hash differs from journal binding",
    )
    expected, sender = expected_arrivals(raw, schedule)
    report, _ = indexed_report(root / "live-checker.json", join_trace=True, verify_tables=True)
    first = read_polls(root / "terminal-polls.jsonl")
    final = read(root / "final.json")
    witness = read(root / "witness.json")
    observed = final["upper_unix_ns"]
    require(
        fingerprint(final) == read(root / "drain-result.json")["final_fingerprint"],
        "final quiet evidence differs from retained drain result",
    )
    uncertainty = clock_bounds(read(root / "clock.json"), schedule["origin_ns"], observed)
    joined = checked_results(report, expected, sender, witness, first, uncertainty, observed)
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
                evidence[name] = {
                    "status": "PASS",
                    "checked": len(joined),
                    "cohorts": cohorts,
                    "clock_uncertainty_ns": uncertainty,
                    "schedule": index[f"{name}/schedule.json"],
                }
    write(output, evidence)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("receipt", type=Path)
    args = parser.parse_args()
    replay_archive(args.archive, args.receipt)
