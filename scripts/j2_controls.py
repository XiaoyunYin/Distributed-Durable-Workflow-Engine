"""Run the four unmeasured J2 controls and independently replay their sealed evidence."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
import time
import zipfile
from contextlib import contextmanager
from pathlib import Path

import dur053_k3 as kind
from j2_control_evidence import (
    CAPPED,
    DELAY_BASE,
    DELAY_HIGH,
    EPISODES,
    HEALTHY,
    capped_control,
    checked_expiry,
    cohort_delivery,
    control_schedule,
    delay_fixture,
    gate_control,
    gates_samples,
    paired_delays,
    read_episode,
    read_lines,
    withheld_terminal,
    worker_comparison,
    worker_spec,
)
from j2_control_proxy import PORT
from j2_drain import REPLAY_FILES
from j2_harness import APP, hosted_dispatch, run
from j2_offline import SECOND
from j2_qualification_evidence import file_record, read, require, write
from j2_schedule import CAP
from j2_schedule_rehearsal import JOURNAL, LEAD_SECONDS, execute_stock, install_sender
from j2_step_rehearsal import DrainHarness, exercise_drain
from j2_verify_overlay import start_profile

SCOPE = "unmeasured J2 controls outside the campaign"
PROXY_LOG = "/evidence/j2-control-proxy.jsonl"
PROXY_STDERR = "/evidence/j2-control-proxy.stderr"
PROXY_URL = f"http://127.0.0.1:{PORT}"
PROXY_START = """
import subprocess, sys
with open(sys.argv[1], 'xb') as out, open(sys.argv[2], 'xb') as err:
    child = subprocess.Popen([sys.executable, '-c', sys.stdin.read()],
        stdin=subprocess.DEVNULL, stdout=out, stderr=err, start_new_session=True)
print(child.pid)
"""
PROXY_PROBE = """
from urllib.request import urlopen
import sys
with urlopen(sys.argv[1], timeout=1) as result:
    assert result.status == 204
"""
PROXY_IMPORTS = ("http.client", "http.server")
CONTROL_WORK_MINUTES = 64


def campaign(run_id, run_attempt):
    require(str(run_id).isdigit() and str(run_attempt).isdigit(), "invalid Actions run identity")
    return f"eng-controls-{run_id}-{run_attempt}"


def receiver_logs(driver):
    pods = driver.kube("pods", "-n", APP, "-l", "app=api")["items"]
    require(len(pods) == 2, "capped control receiver profile changed")
    for pod in pods:
        name = pod["metadata"]["name"]
        logs = driver.k("-n", APP, "logs", name, "--timestamps", timeout=30, record=False)
        (driver.output / f"receiver-{name}.log").write_text(logs.stdout, encoding="utf-8")


@contextmanager
def cap_proxy(driver):
    exec_in_pod = ("-n", APP, "exec", driver.observer, "--")
    source = Path(__file__).with_name("j2_control_proxy.py")
    write(driver.output / "proxy-source.json", file_record(source))
    launched = driver.k(
        "-n",
        APP,
        "exec",
        "-i",
        driver.observer,
        "--",
        "python",
        "-c",
        PROXY_START,
        PROXY_LOG,
        PROXY_STDERR,
        data=source.read_text(encoding="utf-8"),
        timeout=10,
    )
    pid = str(int(launched.stdout.strip()))
    try:
        driver.until(
            lambda: (
                driver.k(
                    *exec_in_pod,
                    "python",
                    "-c",
                    PROXY_PROBE,
                    PROXY_URL + "/ready",
                    allowed=True,
                    timeout=3,
                    record=False,
                ).returncode
                == 0
            ),
            15,
        )

        def execute(driver, schedule, *, partial):
            require(schedule["cell"] == CAPPED and not partial, "proxy is only for the cap control")
            return execute_stock(driver, schedule, partial=False, url=PROXY_URL)

        yield execute
    finally:
        driver.k(*exec_in_pod, "kill", pid, timeout=5, allowed=True)
        driver.copy_from_pod(driver.observer, PROXY_LOG, "proxy.jsonl")
        driver.copy_from_pod(driver.observer, PROXY_STDERR, "proxy.stderr")
        receiver_logs(driver)


def derive(directory, identifier, retain=None):
    episodes = {name: read_episode(directory / name, identifier, APP) for name in EPISODES}
    base = episodes[DELAY_BASE]
    applications = {name: read(directory / name / "application-rendered.json") for name in EPISODES}
    specs = {name: worker_spec(application) for name, application in applications.items()}
    difference = worker_comparison(specs[DELAY_BASE], specs[DELAY_HIGH])
    other_objects = {
        name: [
            obj
            for obj in application
            if not (obj["kind"] == "Deployment" and obj["metadata"]["name"] == "worker")
        ]
        for name, application in applications.items()
    }
    require(
        specs[DELAY_BASE] == specs[HEALTHY] == specs[CAPPED]
        and all(objects == other_objects[DELAY_BASE] for objects in other_objects.values()),
        "application configuration differs outside the control-only worker setting",
    )
    for name in (DELAY_BASE, DELAY_HIGH, HEALTHY):
        e = episodes[name]
        require(
            e["schedule"]["cap"] == CAP
            and e["sender"]["stock_verdict"] == "PASS"
            and len(e["rows"]) == e["schedule"]["count"],
            f"stock K4 delivery did not pass: {name}",
        )
    paired = paired_delays(base, episodes[DELAY_HIGH])
    fixture = delay_fixture(
        base,
        read(directory / DELAY_BASE / "live-checker.json")["trace"],
        read_lines(directory / DELAY_BASE / "terminal-polls.jsonl"),
        gates_samples(read(directory / DELAY_BASE / "prometheus-samples.json")),
    )
    withheld = withheld_terminal(episodes[HEALTHY])
    expiry = checked_expiry(episodes[HEALTHY])
    copies = {
        "worker-specs": specs,
        "worker-spec-diff": difference,
        "delay-fixture": fixture,
        "withheld-terminal": withheld,
        "checked-expiry": expiry,
    }
    healthy = gate_control(directory / HEALTHY, episodes[HEALTHY])
    cap_gate = gate_control(directory / CAPPED, episodes[CAPPED])
    copies["stability-gates"] = {HEALTHY: healthy, CAPPED: cap_gate}
    if retain is not None:
        for name, value in copies.items():
            retain(name, value)
    require(healthy["status"] == "pass", f"healthy control gate failed: {healthy['status']}")
    capped = capped_control(episodes[CAPPED], read_lines(directory / CAPPED / "proxy.jsonl"))
    require(
        cap_gate["status"] == "sender_invalid", "cap control did not invalidate sender inference"
    )
    summary = {
        "scope": SCOPE,
        "campaign": identifier,
        "execution_delay": {
            "status": "PASS",
            "raw_fixture": fixture["verdict"],
            "worker_diff": difference,
            **paired,
        },
        "withheld_terminal": {
            "status": "PASS",
            **withheld["verdicts"],
            "checked_expiry": expiry["verdict"],
            "journal": file_record(directory / HEALTHY / "sender-journal.jsonl"),
        },
        "healthy_low_load": {"status": "PASS", "stock_verdict": "PASS", "gate": healthy},
        "capped_sender": {"engineering_status": "PASS", **capped, "gate": cap_gate},
        "episodes": {
            name: {
                "schedule": e["schedule"],
                "quiet": e["quiet"],
                "checked": len(e["rows"]),
                "admitted": e["sender"]["durable_admissions"],
                "clock_uncertainty_ns": e["width"],
                "checker_peak_rss_kib": e["checker_peak_rss_kib"],
                "checker_vmhwm_peak_kib": e["checker_vmhwm_peak_kib"],
                "cohorts": cohort_delivery(e),
            }
            for name, e in episodes.items()
        },
    }
    return summary, copies


def rehearse(driver, layouts, exporter_layout, binary):
    identifier = campaign(os.environ["GITHUB_RUN_ID"], os.environ["GITHUB_RUN_ATTEMPT"])
    with driver.stage("bootstrap"):
        driver.pod_imports = (*driver.pod_imports, *PROXY_IMPORTS)
        addresses, exporter = start_profile(driver, layouts, exporter_layout)
    previous = None
    for name in EPISODES:
        with driver.stage(f"fresh-{name}"):
            driver.worker_sleep_ms = 2100 if name == DELAY_HIGH else 100
            previous = driver.fresh_episode(name, addresses, exporter, previous)
        with driver.stage(f"install-{name}"):
            driver.observer = driver.pod(APP, "observer")["metadata"]["name"]
            driver.workers = driver.kube("pods", "-n", APP, "-l", "app=worker")["items"]
            driver.pod_journal = JOURNAL
            driver.journal_name = "sender-journal.jsonl"
            driver.install_checker()
            install_sender(driver, binary, driver.evidence / "stock-build.json")

        def schedule(name=name):
            return control_schedule(name, identifier, APP, time.time_ns() + LEAD_SECONDS * SECOND)

        with driver.stage(name):
            if name == CAPPED:
                with cap_proxy(driver) as execute:
                    exercise_drain(driver, schedule, execute=execute)
            else:
                exercise_drain(driver, schedule)
    with driver.stage("control-verdicts"):
        summary, _ = derive(
            driver.evidence,
            identifier,
            retain=lambda name, value: write(driver.evidence / f"{name}.json", value),
        )
        write(driver.evidence / "control-verdicts.json", summary)


REPLAY_INPUTS = tuple(
    member
    for member in REPLAY_FILES
    if member not in {"drain-result.json", "phase-rows.json", "cohorts.json"}
) + (
    "application-rendered.json",
    "checker-resource-summary.json",
    "deadline-anchor.json",
    "drain-polls.jsonl",
    "live-checker-read.json",
    "live-checker-resources.tsv",
    "live-checker.stderr",
    "prometheus-samples.json",
    "sender-exit.json",
)


def replay_archive(archive, output):
    with zipfile.ZipFile(archive) as bundle, tempfile.TemporaryDirectory(dir=output.parent) as temp:
        index = json.loads(bundle.read("freeze.json"))
        status = json.loads(bundle.read("status.json")) if "status.json" in index["files"] else {}
        if "control-verdicts.json" not in index["files"]:
            require(status.get("status") != "PASS", "passing job has no control verdicts")
            write(output, {"status": "unperformed", "retained_status": status})
            return
        root = Path(temp)
        files = [f"{name}/{member}" for name in EPISODES for member in REPLAY_INPUTS]
        for key in [*files, f"{CAPPED}/proxy.jsonl"]:
            destination = root / key
            destination.parent.mkdir(exist_ok=True)
            with bundle.open(key) as source, destination.open("wb") as target:
                shutil.copyfileobj(source, target)
            require(
                file_record(destination) == index["files"][key], f"replay member differs: {key}"
            )
        result, copies = derive(root, campaign(index["run_id"], index["run_attempt"]))
        for name, expected in {"control-verdicts": result, **copies}.items():
            original = json.loads(bundle.read(f"{name}.json"))
            # Recompute before comparing runner-produced verdicts and fixture copies.
            require(
                json.loads(json.dumps(expected)) == original,
                f"independent control replay differs: {name}",
            )
    write(output, result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    hosted = commands.add_parser("hosted")
    for name in ("evidence", "scratch", "layouts", "exporter_layout", "stock", "checker"):
        hosted.add_argument(name, type=Path)
    replay = commands.add_parser("replay")
    replay.add_argument("archive", type=Path)
    replay.add_argument("output", type=Path)
    args = parser.parse_args()
    if args.command == "replay":
        replay_archive(args.archive, args.output)
        return 0
    hosted_dispatch()
    args.evidence.mkdir(parents=True, exist_ok=True)
    args.scratch.mkdir()
    kind.CLUSTER, kind.LABEL = "j2-controls", "j2.harness=owned"
    driver = DrainHarness(args.evidence, args.scratch, args.checker)
    driver.deadline = time.monotonic() + CONTROL_WORK_MINUTES * 60
    return run(
        driver,
        args.layouts,
        args.exporter_layout,
        scenario=lambda driver, layouts, exporter: rehearse(driver, layouts, exporter, args.stock),
        scope=SCOPE,
    )


if __name__ == "__main__":
    raise SystemExit(main())
