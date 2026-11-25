"""Verify unmeasured J2 volume evidence and seal the attempt."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from collections import Counter
from pathlib import Path

from dur053_pilot import witness_events
from dur053_prerequisites import trace_document
from j2_offline import SECOND, clock_bounds, drain_identities, join, sender

VOLUMES = (26678, 21002)
POPULATION_RATE = 4
DRAIN_SECONDS = 600
CLOSEOUT_SECONDS = 1500
PROFILE = "rebuilt registered profile"
CAPACITY_EXIT = 3


class CapacityFailure(ValueError):
    """A failed volume criterion with evidence from the qualifier or replay."""

    def __init__(self, criterion, message):
        super().__init__(message)
        self.criterion = criterion


def require(condition, message):
    if not condition:
        raise ValueError(message)


def require_capacity(condition, criterion, message):
    if not condition:
        raise CapacityFailure(criterion, message)


def write(path, value):
    path.write_bytes((json.dumps(value, sort_keys=True, indent=2) + "\n").encode())


def read(path):
    return json.loads(path.read_bytes())


def file_record(path):
    with path.open("rb") as stream:
        return {
            "bytes": path.stat().st_size,
            "sha256": hashlib.file_digest(stream, "sha256").hexdigest(),
        }


def seal(directory, result):
    index = {
        p.relative_to(directory).as_posix(): file_record(p)
        for p in sorted(directory.rglob("*"))
        if p.is_file()
    }
    write(directory / "freeze.json", {"qualification": result, "files": index})


def checked_report(path):
    report = read(path)
    require(report["status"] == "PASS" and report["valid"] is True, "checker rejected evidence")
    require(report["campaign"]["valid"] is True, "checker campaign failed")
    return report


def check_replay_verdict(directory):
    live = checked_report(directory / "live-checker.json")
    replay = read(directory / "offline-checker.json")
    require(
        replay["status"] in {"PASS", "FAIL"}
        and replay["valid"] is (replay["status"] == "PASS")
        and type(replay["campaign"]["valid"]) is bool
        and isinstance(replay["trace"], dict),
        "offline checker did not emit a complete verdict",
    )
    require_capacity(
        verdict(live) == verdict(replay), "checker_mismatch", "live/offline verdict drift"
    )


def verdict(report):
    campaign = dict(report["campaign"])
    # Go sums durations in map iteration order. Preserve these fields in the
    # raw reports and compare all of the deterministic verdict fields.
    for name in ("busy_seconds", "reexecuted_seconds"):
        campaign.pop(name)
    return {
        **{k: v for k, v in report.items() if k not in {"trace", "workflow_ids", "campaign"}},
        "campaign": campaign,
    }


def extract(directory):
    live = directory / "live-checker.json"
    raw = live.read_text(encoding="utf-8")
    (directory / "snapshot.json").write_bytes(trace_document(raw).encode())


def verify(directory):
    plan = read(directory / "plan.json")
    arrivals = read(directory / "arrivals.json")
    live = checked_report(directory / "live-checker.json")
    expected_verdict = verdict(live)
    live_ids = live["workflow_ids"]
    del live
    replay = checked_report(directory / "offline-checker.json")
    require_capacity(
        expected_verdict == verdict(replay), "checker_mismatch", "live/offline verdict drift"
    )
    snapshot = replay["trace"]
    tables = snapshot["Snapshot"]
    count = plan["periods"][0][1]
    require(count in VOLUMES and arrivals["offered"] == count, "wrong volume in plan or journal")
    require_capacity(
        len(arrivals["rows"]) >= count, "missing_identity", "arrival journal is incomplete"
    )
    require(len(arrivals["rows"]) == count, "arrival journal exceeds planned volume")
    require(arrivals["status"] == "PASS", "arrival journal failed")
    for sequence, row in enumerate(arrivals["rows"]):
        activity = {8: "pure.echo", 9: "pure.add"}.get(sequence % 10, "dur048.sleep")
        expected = 5 if activity == "pure.add" else {"run_id": plan["run_id"], "sequence": sequence}
        require(
            row["sequence"] == sequence
            and row["activity"] == activity
            and row["outcome"] == "ACCEPTED"
            and row["expected"] == expected,
            f"incorrect registered workload or outcome at sequence {sequence}",
        )
    planned, delivery = sender(
        {**plan, "origin": arrivals["rows"][0]["scheduled_at"]}, arrivals, snapshot["Submissions"]
    )
    require(delivery["valid"], f"identity/admission reconciliation failed: {delivery['problems']}")
    ids = {key for key, _ in planned}
    terminal = read(directory / "terminals.json")
    for observed_ids, source in ((live_ids, "live checker"), (terminal["ids"], "DB terminals")):
        require_capacity(ids <= set(observed_ids), "missing_identity", f"{source} is missing IDs")
        require(
            set(observed_ids) == ids and len(observed_ids) == count, f"{source} has unexpected IDs"
        )
    executed = replay["campaign"]["executions"]
    reconciled = drain_identities(
        (r["WorkflowID"] for r in snapshot["Submissions"]),
        (r["workflow_id"] for r in arrivals["rows"]),
        (
            r["workflow_id"]
            for r in tables["engine.workflow_executions"]["rows"]
            if r["state"] == "SUCCEEDED"
        ),
        (r["workflow_id"] for r in executed if r["winners"] == 1 and r["invocations"] == 1),
    )
    require(
        not (reconciled["unexpected"] or reconciled["duplicated"]),
        f"invalid drain identities: {reconciled}",
    )
    require_capacity(
        reconciled["valid"], "missing_identity", f"drain identity mismatch: {reconciled}"
    )
    accepted_results = replay["campaign"]["accepted_results"]
    require_capacity(accepted_results >= count, "missing_identity", "missing checked results")
    require(accepted_results == count, "unexpected checked results")
    witness = read(directory / "witness.json")
    witnessed = witness_events(witness)
    require_capacity(len(witnessed) >= 2 * count, "truncation", "witness start/end count is short")
    require(len(witnessed) == 2 * count, "unexpected witness starts or ends")
    probes = read(directory / "clock.json")
    observed = terminal["observed_ns"]
    uncertainty = clock_bounds(probes, planned[0][1], observed)
    statuses = Counter()
    problems = {}
    for row in join(
        planned, snapshot, witnessed, dict.fromkeys(ids, observed), uncertainty, observed
    ):
        statuses[row["status"]] += 1
        if row["status"] != "observed":
            problems[row["workflow_id"]] = row["status"]
    require(statuses == {"observed": count}, f"incomplete offline phase joins: {problems}")
    return {
        "identities": count,
        "identity_sha256": hashlib.sha256(("\n".join(sorted(ids)) + "\n").encode()).hexdigest(),
        "identical_verdict": expected_verdict["status"],
        "phase_join_counts": statuses,
        "clock_bracket_ns": uncertainty,
        "witness_events": len(witnessed),
        "tables": {
            name: {k: table[k] for k in ("count", "sha256")} for name, table in tables.items()
        },
    }


def scrape_summary(series, start, end, count):
    def samples(key):
        require(len(series[key]) == 1, f"missing or multiple {key} series")
        return {t: float(value) for t, value in series[key][0]["values"]}

    timestamp = samples("j2_sample_timestamp_seconds")
    up = samples("up")
    ok = samples("j2_query_ok")
    metrics = [
        samples(k) for k in series if k not in {"up", "j2_query_ok", "j2_sample_timestamp_seconds"}
    ]
    valid = [
        (t, value)
        for t, value in sorted(timestamp.items())
        if up.get(t) == ok.get(t) == 1 and all(t in metric for metric in metrics)
    ]
    require(
        any(
            samples("j2_admissions").get(t) == samples("j2_terminals").get(t) == count
            and samples("j2_unpublished").get(t) == samples("j2_outstanding").get(t) == 0
            for t, _ in valid[-10:]
        ),
        "exporter did not observe the full quiet population",
    )

    def gaps(times):
        edges = [start, *(t for t in sorted(set(times)) if start <= t <= end), end]
        return [b - a for a, b in zip(edges, edges[1:], strict=False)]

    scrape_gaps = gaps(t for t, _ in valid)
    query_gaps = gaps(value for _, value in valid)
    return {
        "valid_scrapes": len(valid),
        "failed_scrapes": sum(value != 1 for value in up.values()),
        "failed_queries": sum(value != 1 for value in ok.values()),
        "max_scrape_gap_seconds": max(scrape_gaps),
        "max_query_gap_seconds": max(query_gaps),
        "scrape_gaps_over_5s": sum(gap > 5 for gap in scrape_gaps),
        "query_gaps_over_5s": sum(gap > 5 for gap in query_gaps),
    }


def classify(qualification, result, elapsed):
    if result.get("cleanup_error") or result.get("diagnostic_error"):
        return "invalid", result.get("cleanup_error") or result["diagnostic_error"]
    if result.get("population_started") is not True:
        return "invalid", result.get("error", "no confirmed submission")
    if qualification == "success" and result.get("status") == "PASS":
        if elapsed is None or elapsed < 0:
            return "invalid", "invalid closeout clock"
        if elapsed > CLOSEOUT_SECONDS:
            return "capacity_failure", "closeout exceeded the registered deadline"
        return "pass", "all registered criteria passed"
    if qualification == "failure" and result.get("status") == "FAIL":
        if result.get("capacity_criterion") in {
            "oom",
            "restart",
            "truncation",
            "drain_deadline",
            "closeout_deadline",
            "missing_identity",
            "checker_mismatch",
        }:
            return "capacity_failure", result["error"]
        return "invalid", result.get("error", "qualification failed without a capacity finding")
    return "invalid", "inconsistent qualification step and frozen result"


def closeout(directory):
    artifact = os.environ.get("ARTIFACT_ID", "")
    digest = os.environ.get("ARTIFACT_DIGEST", "")
    elapsed = None
    try:
        require(
            artifact.isdecimal() and re.fullmatch("[a-f0-9]{64}", digest),
            "evidence upload missing",
        )
        result = read(directory / "freeze.json")["qualification"]
        exported = result.get("export_completed_unix_ns")
        if exported is not None:
            require(type(exported) is int and exported > 0, "invalid export timestamp")
            elapsed = (time.time_ns() - exported) / SECOND
            print(f"Export completion through verified artifact upload: {elapsed:.3f} s")
        disposition, reason = classify(os.environ["QUALIFICATION"], result, elapsed)
    except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError) as error:
        disposition, reason = "invalid", str(error)

    with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as output:
        print(f"verdict={disposition}", file=output)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a", encoding="utf-8") as output:
        print(
            f"Volume {os.environ['J2_VOLUME']} · **{disposition}** · "
            f"artifact `{artifact or 'unavailable'}` · SHA-256 `{digest or 'unavailable'}`",
            file=output,
        )
    print(f"{disposition}: {reason}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("extract", "verify", "closeout"))
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    if args.operation == "verify":
        try:
            print(json.dumps(verify(args.directory), sort_keys=True))
        except CapacityFailure as error:
            print(json.dumps({"capacity_criterion": error.criterion, "error": str(error)}))
            raise SystemExit(CAPACITY_EXIT) from error
    elif args.operation == "extract":
        extract(args.directory)
    else:
        closeout(args.directory)
