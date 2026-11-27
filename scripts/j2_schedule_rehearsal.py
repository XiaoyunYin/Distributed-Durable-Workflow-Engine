"""Hosted, unmeasured J2 sender schedule and interruption rehearsals."""

import argparse
import base64
import json
import os
import re
import signal
import subprocess
import time
from contextlib import contextmanager, suppress
from pathlib import Path, PurePosixPath

import dur053_k3 as kind
from j2_build_sender import BUILD_SCHEMAS, J2_PACKAGE, PACKAGE
from j2_harness import (
    APP,
    INTERRUPTED_COUNT,
    POD_IMPORTS,
    PUBLISH_STOP,
    Harness,
    InjectedStop,
    hosted_dispatch,
    run,
    start_profile,
)
from j2_offline import SECOND
from j2_qualification_evidence import file_record, read, require, write
from j2_schedule import (
    ENGINEERING,
    MEASURED,
    WARMUP,
    continuous_schedule,
    k4_report,
    stock_schedule,
    timestamp,
)
from j3_resources import cgroup_paths

STOCK = "/tmp/j2-stock-loadgen"
CONTINUOUS = "/tmp/j2-continuous-sender"
STOP = "/evidence/j2-stock-stop"
JOURNAL = "/evidence/j2-stock-arrivals.jsonl"
STEP_JOURNAL = "/evidence/j2-step-arrivals.jsonl"
LEAD_SECONDS = 8
RESOURCE_FILES = ("cpu.stat", "io.stat", "cpu.pressure", "io.pressure")
SCHEDULE_POD_IMPORTS = (*POD_IMPORTS, "base64", "hashlib", "os")
INSTALL = """import base64,hashlib,os,sys
from pathlib import Path
path=Path(sys.argv[1])
wanted=sys.argv[2]
incoming=path.with_suffix('.incoming')
with incoming.open('xb') as file:
    file.write(base64.b64decode(sys.stdin.buffer.read(),validate=True))
with incoming.open('rb') as file:
    actual=hashlib.file_digest(file,'sha256').hexdigest()
if actual != wanted:
    sys.exit('stock sender digest mismatch')
incoming.chmod(0o500)
os.replace(incoming,path)
print(actual)
"""

# Observer subset of the J3 R405 node-side sampler. The sampler is outside
# the observer's cgroup and retains pressure as well as CPU/IO counters.
NODE_SAMPLE = r"""
group=$1
shift
while :; do
    at=$(date +%s%N)
    for stat do
        if mapfile -t values 2>/dev/null < "$group/$stat"; then
            printf -v body '%s;' "${values[@]}"
            printf '%s\t%s\tok\t%s\n' "$at" "$stat" "${body%;}"
        else
            printf '%s\t%s\terror\tunreadable\n' "$at" "$stat"
        fi
    done
    IFS= read -r -t 1 && break
    [ "$?" -gt 128 ] || break
done
"""


def install_sender(driver, binary, manifest, *, continuous=False):
    package = J2_PACKAGE if continuous else PACKAGE
    path = CONTINUOUS if continuous else STOCK
    pin = read(manifest)
    require(
        pin["schema"] == BUILD_SCHEMAS[package]
        and pin["package"] == package
        and pin["source"] == os.environ["GITHUB_SHA"]
        and pin["binary"] == file_record(binary),
        "sender build identity or hash mismatch",
    )
    result = driver.k(
        "-n",
        APP,
        "exec",
        "-i",
        driver.observer,
        "--",
        "python",
        "-c",
        INSTALL,
        path,
        pin["binary"]["sha256"],
        data=base64.b64encode(binary.read_bytes()).decode("ascii"),
        timeout=60,
    )
    require(result.stdout.strip() == pin["binary"]["sha256"], "observer sender digest mismatch")
    args = ["-n", APP, "exec", driver.observer, "--"]
    help_result = driver.k(*args, path, "-help", timeout=15)
    help_text = help_result.stdout + help_result.stderr
    (driver.output / "sender-help.txt").write_text(help_text, encoding="utf-8")
    if continuous:
        require(
            all(
                flag in help_text
                for flag in (
                    "-start",
                    "-hold-rate",
                    "-hold-duration",
                    "-step-rate",
                    "-step-duration",
                )
            ),
            "installed J2 sender is missing its two-period flags",
        )
    else:
        require(
            re.search(r"(?m)^\s+-k4[ \t]*$", help_text) is not None
            and all(flag in help_text for flag in ("-k4-start", "-k4-stop")),
            "installed stock sender is missing K4 mode",
        )
    write(
        driver.output / "sender-install.json",
        {
            "observer": driver.observer,
            "source": pin["source"],
            "path": path,
            "sha256": result.stdout.strip(),
        },
    )


def observer_target(driver):
    pod = driver.kube("pod", driver.observer, "-n", APP)
    name = pod["metadata"]["name"]
    uid = pod["metadata"]["uid"]
    (container,) = pod["status"]["containerStatuses"]
    require(container["ready"] and container["restartCount"] == 0, "observer is not fresh")
    runtime_id = container["containerID"].split("://", 1)[1]
    node = ["docker", "exec", kind.CLUSTER + "-control-plane"]
    result = driver.command([*node, "crictl", "inspect", runtime_id], record=False, timeout=5)
    pid = json.loads(result.stdout)["info"]["pid"]
    require(type(pid) is int and pid > 0, "missing observer PID for diagnostics")
    membership = driver.command([*node, "cat", f"/proc/{pid}/cgroup"], record=False, timeout=5)
    leaf, _ = cgroup_paths(membership.stdout, uid)
    require(runtime_id in PurePosixPath(leaf).name, "observer container cgroup differs")
    return {"pod": name, "pod_uid": uid, "container_id": runtime_id, "cgroup": leaf, "pid": pid}


def resource_summary(raw):
    samples = {}
    errors = []
    for line in raw.splitlines():
        at, field, status, text = line.split("\t", 3)
        require(field in RESOURCE_FILES and status in {"ok", "error"}, "invalid observer sample")
        row = samples.setdefault(int(at), {})
        require(field not in row, "duplicate observer counter sample")
        row[field] = text
        if status != "ok":
            errors.append({"at_ns": int(at), "counter": field, "error": text})
    times = list(samples)
    complete = all(set(row) == set(RESOURCE_FILES) for row in samples.values())
    return {
        "samples": len(samples),
        "complete": complete,
        "errors": errors,
        "first_sample_unix_ns": times[0] if times else None,
        "last_sample_unix_ns": times[-1] if times else None,
        "max_gap_ns": max(
            (right - left for left, right in zip(times, times[1:], strict=False)), default=None
        ),
    }


@contextmanager
def observer_resources(driver):
    target = observer_target(driver)
    output = driver.output
    write(output / "observer-resource-target.json", target)
    path = output / "observer-resources.tsv"
    with (
        path.open("wb") as out,
        (output / "observer-resources.stderr").open("wb") as err,
        subprocess.Popen(
            [
                "docker",
                "exec",
                "-i",
                kind.CLUSTER + "-control-plane",
                "bash",
                "-c",
                NODE_SAMPLE,
                "bash",
                target["cgroup"],
                *RESOURCE_FILES,
            ],
            stdin=subprocess.PIPE,
            stdout=out,
            stderr=err,
            env=driver.env,
        ) as process,
    ):
        try:
            yield
        finally:
            with suppress(BrokenPipeError):
                process.stdin.close()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
            summary = resource_summary(path.read_text(encoding="utf-8"))
            write(
                output / "observer-resource-summary.json", {"exit": process.returncode, **summary}
            )
    require(
        process.returncode == 0
        and summary["samples"] >= 2
        and summary["complete"]
        and not summary["errors"],
        "observer resource sampling incomplete",
    )
    require(observer_target(driver) == target, "observer changed during the sender rehearsal")


def run_stock(driver, schedule, *, partial):
    args = ["-n", APP, "exec", driver.observer, "--"]
    loadgen = [
        STOCK,
        "-k4",
        "-k4-start",
        timestamp(schedule["origin_ns"]),
        "-k4-stop",
        STOP,
        "-namespace",
        APP,
        "-url",
        "http://api:8080",
        "-token-file",
        "/identity/token",
        "-run-id",
        schedule["run_id"],
        "-seed",
        str(schedule["seed"]),
        "-arm",
        "short",
        "-count",
        str(schedule["count"]),
        "-rate",
        schedule["rate"],
        "-concurrency",
        str(schedule["cap"]),
        "-request-timeout",
        schedule["request_timeout"],
        "-output",
        JOURNAL,
    ]
    write(driver.output / "schedule.json", schedule)
    driver.k(
        "-n",
        APP,
        "exec",
        "-i",
        driver.observer,
        "--",
        "python",
        "-c",
        PUBLISH_STOP,
        STOP,
        data=timestamp(schedule["stop_ns"]),
        timeout=5,
    )
    require(time.time_ns() < schedule["origin_ns"], "stock stop file missed sender origin")
    return execute_sender(driver, schedule, args, loadgen, JOURNAL, partial=partial)


def run_continuous(driver, schedule, *, partial):
    hold, step = schedule["periods"]
    args = ["-n", APP, "exec", driver.observer, "--"]
    loadgen = [
        CONTINUOUS,
        "-start",
        timestamp(schedule["origin_ns"]),
        "-namespace",
        APP,
        "-url",
        "http://api:8080",
        "-token-file",
        "/identity/token",
        "-run-id",
        schedule["run_id"],
        "-seed",
        str(schedule["seed"]),
        "-hold-rate",
        hold["rate"],
        "-hold-duration",
        f"{hold['seconds']}s",
        "-step-rate",
        step["rate"],
        "-step-duration",
        f"{step['seconds']}s",
        "-output",
        STEP_JOURNAL,
    ]
    write(driver.output / "schedule.json", schedule)
    return execute_sender(driver, schedule, args, loadgen, STEP_JOURNAL, partial=partial)


def execute_sender(driver, schedule, args, loadgen, journal, *, partial):
    try:
        if partial:
            return driver.interrupted_load(args, loadgen)
        duration = (schedule["stop_ns"] - schedule["origin_ns"]) // SECOND
        return driver.k(*args, *loadgen, allowed=True, timeout=duration + LEAD_SECONDS + 30)
    finally:
        readback = driver.k(*args, "cat", journal, allowed=True, timeout=15)
        (driver.output / driver.journal_name).write_bytes(readback.stdout.encode("utf-8"))
        write(driver.output / "journal-read.json", {"exit": readback.returncode})


def exercise_sender(driver, name, *, partial, continuous=False):
    campaign = f"eng-{os.environ['GITHUB_RUN_ID']}-{os.environ['GITHUB_RUN_ATTEMPT']}"
    common = (
        campaign,
        "job-1",
        name,
        f"j2-{campaign}-{name}",
        APP,
        time.time_ns() + LEAD_SECONDS * SECOND,
    )
    if continuous:
        schedule = continuous_schedule(*common, ("1", 20 if partial else 8), ("4", 3))
        execute = run_continuous
    else:
        schedule = stock_schedule(
            *common, "1", 0 if partial else WARMUP, INTERRUPTED_COUNT if partial else MEASURED
        )
        execute = run_stock
    launched_at = time.time_ns()
    result = execute(driver, schedule, partial=partial)
    returned_at = time.time_ns()
    (driver.output / "sender-events.jsonl").write_text(result.stdout, encoding="utf-8")
    write(
        driver.output / "sender-exit.json",
        {
            "exec_exit": result.returncode,
            "runner_start_unix_ns": launched_at,
            "runner_return_with_journal_unix_ns": returned_at,
            "runner_return_minus_scheduled_end_ns": returned_at - schedule["stop_ns"],
        },
    )
    if not partial and returned_at < schedule["stop_ns"]:
        time.sleep((schedule["stop_ns"] - returned_at) / SECOND)
    durable = driver.fixture_admissions()
    write(driver.output / "durable-admissions.json", durable)
    report = k4_report((driver.output / driver.journal_name).read_bytes(), schedule, durable)
    write(driver.output / "sender-report.json", report)
    require(
        result.returncode == 0
        and read(driver.output / "journal-read.json")["exit"] == 0
        and not report["outside_slot"]
        and report["max_in_flight_from_journal"] <= schedule["cap"]
        and not report["missing_acknowledged"],
        "sender, journal or durable admission evidence failed",
    )
    if not partial:
        require(
            report["stock_verdict"] == "PASS" and not report["unresolved"],
            "engineering sender did not complete",
        )
        if continuous:
            expected = {
                "offered": schedule["count"],
                "accepted": len(report["acknowledged"]),
                "status": report["stock_verdict"],
            }
            require(json.loads(result.stdout) == expected, "J2 and stock journal verdict differ")
        return
    events = [json.loads(line) for line in result.stdout.splitlines()]
    write(driver.output / "admissions-after-stop.json", durable)
    before = read(driver.output / "admissions-before-stop.json")
    require(
        [e["event"] for e in events] == ["started", "committed", "stopped"]
        and all(events[1].get(key) == before[key] for key in ("ids", "observed_unix_ns"))
        and before["ids"]
        and set(before["ids"]) <= set(durable)
        and len(durable) < schedule["count"]
        and events[2]["exit_code"] == -signal.SIGTERM
        and not report["closed"]
        and report["stock_verdict"] == "FAIL"
        and report["unresolved"],
        "interruption did not retain a reconciled partial journal",
    )
    return schedule["run_id"]


def rehearse(driver, layouts, exporter_layout, binary, *, continuous=False):
    with driver.stage("bootstrap"):
        addresses, exporter = start_profile(driver, layouts, exporter_layout)
    driver.pod_journal = STEP_JOURNAL if continuous else JOURNAL
    driver.journal_name = "sender-journal.jsonl"
    driver.pod_imports = SCHEDULE_POD_IMPORTS
    previous = None
    names = ("step-timing", "step-interrupt") if continuous else ("grid-timing", "stock-interrupt")
    for i, name in enumerate(names):
        with driver.stage(f"fresh-{name}"):
            previous = driver.fresh_episode(name, addresses, exporter, previous)
        with driver.stage(f"install-{name}"):
            driver.observer = driver.pod(APP, "observer")["metadata"]["name"]
            install_sender(
                driver, binary, driver.evidence / "sender-build.json", continuous=continuous
            )
        with driver.stage(f"sender-{name}"), observer_resources(driver):
            run_id = exercise_sender(driver, name, partial=i == 1, continuous=continuous)
    raise InjectedStop(run_id)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("evidence", "scratch", "layouts", "exporter_layout", "sender"):
        parser.add_argument(name, type=Path)
    parser.add_argument("--continuous", action="store_true")
    args = parser.parse_args()
    hosted_dispatch()
    args.evidence.mkdir(parents=True, exist_ok=True)
    args.scratch.mkdir()
    kind.CLUSTER, kind.LABEL = "j2-harness", "j2.harness=owned"
    return run(
        Harness(args.evidence, args.scratch),
        args.layouts,
        args.exporter_layout,
        scenario=lambda driver, layouts, exporter: rehearse(
            driver, layouts, exporter, args.sender, continuous=args.continuous
        ),
        scope=f"unmeasured J2 schedule {'2b1' if args.continuous else '2a'} ({ENGINEERING})",
    )


if __name__ == "__main__":
    raise SystemExit(main())
