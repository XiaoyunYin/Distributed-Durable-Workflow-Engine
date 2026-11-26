"""Run both checker paths in fresh hosted 2 GiB, 1-CPU cgroups."""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from j2_checker import CONTAINER_CHECKER, MOUNTED_CHECKER, ROOT, comparison_manifest, verify_binary
from j2_checker_report import indexed_report, markers
from j2_prequal_fixture import LEASES_PER_WORKFLOW, LINKED, NAMESPACE, build_fixture
from j2_qualification_evidence import VOLUMES, read, write

GIB = 1 << 30
MEMORY_MAX = 2 * GIB
VMHWM_LIMIT_KIB = 14 * GIB // (10 * 1024)
PEAK_LIMIT_BYTES = 7 * GIB // 4
WALL_LIMIT = 240
TRIAL_LIMITS = (None, "1100MiB", "1200MiB")
WORKER_BASE = (
    "python:3.12.7-slim-bookworm@"
    "sha256:60d9996b6a8a3689d36db740b49f4327be3be09a21122bd02fb8895abb38b50d"
)
NETWORK, DATABASE = "j2-checker-prequalification", "j2-prequal-db"
PASSWORD = "j2-disposable-prequalification"


def last_marker(path, name):
    found = markers(path, name)
    if len(found) > 1:
        raise ValueError(f"repeated {name} in {path}")
    return found[0] if found else {}


def checker_gates(exit_code, report, process, cgroup, phases, digest, count, mode):
    expected_phases = [
        "load" if mode == "live" else "offline_decode",
        "hash",
        "check",
        "campaign_check",
        "write",
    ]
    campaign = report.get("campaign", {})
    quota, _, period = cgroup.get("cpu_max", "").partition(" ")
    return {
        "pass": (
            exit_code == process.get("exit_status") == 0
            and report.get("status") == "PASS"
            and report.get("valid") is True
            and campaign.get("valid") is True
            and campaign.get("accepted") == campaign.get("accepted_results") == count
        ),
        "pin": process.get("checker_sha256") == digest,
        "vmhwm": 0 < process.get("peak_rss_kib", 0) <= VMHWM_LIMIT_KIB,
        "cgroup_peak": 0 < int(cgroup.get("peak", 0)) <= PEAK_LIMIT_BYTES,
        "wall": 0 < process.get("wall_seconds", 0) <= WALL_LIMIT,
        "limits": (
            cgroup.get("max") == str(MEMORY_MAX)
            and cgroup.get("swap_max") == "0"
            and quota == period
            and quota.isdecimal()
            and int(quota) > 0
        ),
        "phases": [p["phase"] for p in phases] == expected_phases,
    }


def checker_run(evidence, checker, digest, count, mode, memory_limit):
    network = NETWORK if mode == "live" else "none"
    source = (
        ["-namespace", NAMESPACE] if mode == "live" else ["-snapshot", "/evidence/snapshot.json"]
    )
    container = f"j2-prequal-{mode}"
    command = [
        "docker",
        "run",
        "--rm",
        "--name",
        container,
        "--network",
        network,
        "--cpus=1",
        "--memory=2g",
        "--memory-swap=2g",
        "--cgroupns=private",
        "--read-only",
        "--cap-drop=ALL",
        "--security-opt=no-new-privileges",
        "--user=65532:65532",
        "--env",
        "J2_CHECKER_SAMPLE=1",
        "--env",
        "PYTHONDONTWRITEBYTECODE=1",
        "--mount",
        f"type=bind,src={checker},dst={MOUNTED_CHECKER},readonly",
        "--mount",
        f"type=bind,src={evidence},dst=/evidence,readonly",
    ]
    if mode == "live":
        command += [
            "--env",
            f"DATABASE_URL=postgres://postgres:{PASSWORD}@db/postgres?sslmode=disable",
        ]
    if memory_limit is not None:
        command += ["--env", f"J2_PREQUAL_GOMEMLIMIT={memory_limit}"]
    command += [
        WORKER_BASE,
        "python",
        "-c",
        CONTAINER_CHECKER,
        MOUNTED_CHECKER,
        digest,
        "-arrivals",
        "/evidence/arrivals.json",
        "-witness-export",
        "/evidence/witness.json",
        "-require-terminal",
        *source,
    ]
    stderr = evidence / f"{mode}-checker.stderr"
    stdout = evidence / f"{mode}-checker.json"
    started = time.monotonic()
    with stdout.open("wb") as out, stderr.open("wb") as err:
        try:
            exit_code = subprocess.run(command, stdout=out, stderr=err, timeout=330).returncode
        except subprocess.TimeoutExpired:
            subprocess.run(["docker", "rm", "-f", container], capture_output=True, timeout=30)
            exit_code = None
    wall = time.monotonic() - started
    phases = markers(stderr, "J2_PHASE")
    process = last_marker(stderr, "J2_CHECKER")
    cgroup = last_marker(stderr, "J2_CGROUP")
    samples = markers(stderr, "J2_SAMPLE")[-5:]
    step = max(
        (b["vmhwm_kib"] - a["vmhwm_kib"] for a, b in zip(samples, samples[1:], strict=False)),
        default=0,
    )
    report, tables, error = {}, {}, None
    try:
        report, tables = indexed_report(stdout)
    except (OSError, ValueError, KeyError) as failure:
        error = str(failure)
    timings = {}
    then = process.get("started_at_ns")
    for phase in phases:
        now = phase["at_ns"]
        timings[phase["phase"]] = None if then is None else (now - then) / 10**9
        then = now
    return {
        "exit_code": exit_code,
        "docker_wall_seconds": wall,
        "process": process,
        "gc_count": phases[-1].get("num_gc") if phases else None,
        "cgroup": cgroup,
        "phases": phases,
        "phase_seconds": timings,
        "tables": tables,
        "gates": checker_gates(exit_code, report, process, cgroup, phases, digest, count, mode),
        "report_error": error,
        "final_samples": samples,
        "final_vmhwm_step_kib": step,
        "diagnostics": {
            "buffering_diagnosis_refuted": step >= 300 * 1024,
            "quadratic_diagnosis_refuted": mode == "live"
            and process.get("wall_seconds", wall) > 300,
        },
    }


def profile(evidence, operation):
    timing = evidence / f"python-{operation}-profile.json"
    command = [
        "/usr/bin/time",
        "-q",
        "-f",
        '{"wall_seconds":%e,"peak_rss_kib":%M,"exit_status":%x}',
        "-o",
        timing,
        sys.executable,
        ROOT / "scripts/j2_qualification_evidence.py",
        operation,
        evidence,
    ]
    with (
        (evidence / f"python-{operation}.stdout").open("wb") as out,
        (evidence / f"python-{operation}.stderr").open("wb") as err,
    ):
        status = subprocess.run(command, stdout=out, stderr=err, timeout=300).returncode
    return {**read(timing), "process_exit": status}


def start_database(image, migrator):
    subprocess.run(["docker", "network", "create", NETWORK], check=True, timeout=30)
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            DATABASE,
            "--network",
            NETWORK,
            "--network-alias",
            "db",
            "-p",
            "127.0.0.1::5432",
            "--env",
            f"POSTGRES_PASSWORD={PASSWORD}",
            image,
        ],
        check=True,
        timeout=180,
    )
    for _ in range(60):
        status = subprocess.run(
            ["docker", "exec", DATABASE, "pg_isready", "-q", "-h", "127.0.0.1"],
            capture_output=True,
            timeout=5,
        ).returncode
        if status == 0:
            break
        time.sleep(1)
    else:
        raise RuntimeError("pre-qualification PostgreSQL did not become ready")
    endpoint = subprocess.check_output(
        ["docker", "port", DATABASE, "5432/tcp"], text=True, timeout=10
    ).strip()
    env = {
        **os.environ,
        "DATABASE_URL": f"postgres://postgres:{PASSWORD}@{endpoint}/postgres?sslmode=disable",
    }
    subprocess.run([migrator], env=env, check=True, timeout=180)


def measure_trials(count, evidence, scratch, checker, digest, limits):
    expected = {f"engine.{name}": count * n for name, (_, n) in LINKED.items()}
    expected["engine.lease_acquisitions"] = count * LEASES_PER_WORKFLOW
    trials = {}
    with tempfile.TemporaryDirectory(dir=scratch) as temp:
        directories = {}
        for memory_limit in limits:
            label = memory_limit or "unset"
            directory = evidence if memory_limit is None else Path(temp) / label
            if directory != evidence:
                directory.mkdir()
                for name in (
                    "arrivals.json",
                    "witness.json",
                    "clock.json",
                    "plan.json",
                    "terminals.json",
                ):
                    (directory / name).hardlink_to(evidence / name)
            directories[label] = directory
            live = checker_run(directory, checker, digest, count, "live", memory_limit)
            trials[label] = {
                "gomemlimit": memory_limit,
                "paths": {"live": live},
                "python": {},
                "fixture_table_counts_match": all(
                    live["tables"].get(name, {}).get("count") == wanted
                    for name, wanted in expected.items()
                ),
                "table_inventory_matches": False,
                "pass": False,
            }

        # Reuse one live snapshot so every offline run sees the same bytes,
        # including a trial whose live checker could not finish its export.
        snapshot = None
        snapshot_from = None
        for label, trial in trials.items():
            if trial["paths"]["live"]["gates"]["pass"]:
                directory = directories[label]
                trial["python"]["extract"] = profile(directory, "extract")
                if trial["python"]["extract"]["process_exit"] == 0:
                    snapshot = directory / "snapshot.json"
                    snapshot_from = label
                    if directory != evidence:
                        (evidence / "snapshot.json").hardlink_to(snapshot)
                    break
                (directory / "snapshot.json").unlink(missing_ok=True)

        for label, trial in trials.items():
            directory = directories[label]
            memory_limit = trial["gomemlimit"]
            if snapshot is None:
                trial["offline_skipped"] = "no usable live snapshot in this population"
            else:
                trial["offline_snapshot_from"] = snapshot_from
                local_snapshot = directory / "snapshot.json"
                if local_snapshot not in (snapshot, evidence / "snapshot.json"):
                    local_snapshot.hardlink_to(snapshot)
                live = trial["paths"]["live"]
                offline = checker_run(directory, checker, digest, count, "offline", memory_limit)
                trial["paths"]["offline"] = offline
                trial["table_inventory_matches"] = live["tables"] == offline["tables"]
                if live["gates"]["pass"] and offline["gates"]["pass"]:
                    trial["python"]["verify"] = profile(directory, "verify")
                trial["pass"] = (
                    all(all(path["gates"].values()) for path in trial["paths"].values())
                    and trial["python"].get("verify", {}).get("process_exit") == 0
                    and trial["fixture_table_counts_match"]
                    and trial["table_inventory_matches"]
                    and all(
                        path["process"].get("gomemlimit") == memory_limit
                        for path in trial["paths"].values()
                    )
                )
            if directory != evidence:
                retained = evidence / "gomemlimit" / label
                retained.mkdir(parents=True)
                for path in directory.iterdir():
                    if (
                        path.suffix in (".stderr", ".stdout")
                        or path.name == "live-checker-tables.json"
                    ):
                        shutil.copyfile(path, retained / path.name)
    return trials


def prequalify(count, evidence, scratch, checker, migrator, compare_gomemlimit=False):
    postgres = read(ROOT / "deploy/k8s/tools.lock.json")["postgres"]
    if compare_gomemlimit:
        lock = verify_binary(checker, comparison_manifest(checker))
        if lock["source"] != os.environ["GITHUB_SHA"]:
            raise ValueError("comparison checker does not match this dispatch")
    else:
        lock = verify_binary(checker)
    result = {
        "population": count,
        "postgres": postgres,
        "checker_container_image": WORKER_BASE,
        "checker": lock,
        "pass": False,
    }
    try:
        build_fixture(scratch, evidence, count)
        start_database(postgres, migrator)
        subprocess.run(["docker", "pull", WORKER_BASE], check=True, timeout=180)
        with (
            (scratch / "fixture.sql").open("rb") as data,
            (evidence / "fixture-load.log").open("wb") as log,
        ):
            subprocess.run(
                [
                    "docker",
                    "exec",
                    "-i",
                    DATABASE,
                    "psql",
                    "-X",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-U",
                    "postgres",
                    "-d",
                    "postgres",
                ],
                stdin=data,
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
                timeout=240,
            )
        trials = measure_trials(
            count,
            evidence,
            scratch,
            checker,
            lock["sha256"],
            TRIAL_LIMITS if compare_gomemlimit else (None,),
        )
        if compare_gomemlimit:
            result["trials"] = trials
            result["passing_settings"] = [label for label, trial in trials.items() if trial["pass"]]
            result["pass"] = bool(result["passing_settings"])
        else:
            result.update(trials["unset"])
    except Exception as error:
        result["error"] = str(error)
        raise
    finally:
        write(evidence / "prequalification.json", result)
        with (evidence / "postgres.log").open("wb") as log:
            subprocess.run(
                ["docker", "logs", DATABASE], stdout=log, stderr=subprocess.STDOUT, timeout=15
            )
        subprocess.run(
            ["docker", "rm", "-f", "j2-prequal-live", "j2-prequal-offline", DATABASE],
            capture_output=True,
            timeout=30,
        )
        subprocess.run(["docker", "network", "rm", NETWORK], capture_output=True, timeout=30)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("count", type=int, choices=VOLUMES)
    for name in ("evidence", "scratch", "checker", "migrator"):
        parser.add_argument(name, type=lambda path: Path(path).resolve())
    parser.add_argument("--compare-gomemlimit", action="store_true")
    args = parser.parse_args()
    if (
        os.environ.get("GITHUB_ACTIONS"),
        os.environ.get("RUNNER_ENVIRONMENT"),
        os.environ.get("GITHUB_EVENT_NAME"),
    ) != ("true", "github-hosted", "workflow_dispatch"):
        raise SystemExit("pre-qualification requires a GitHub-hosted dispatch")
    args.evidence.mkdir(parents=True, exist_ok=True)
    args.scratch.mkdir()
    result = prequalify(
        args.count,
        args.evidence,
        args.scratch,
        args.checker,
        args.migrator,
        args.compare_gomemlimit,
    )
    print(
        json.dumps(
            {
                "population": args.count,
                "pass": result["pass"],
                "trials": {
                    setting: {name: path["gates"] for name, path in trial["paths"].items()}
                    for setting, trial in result.get("trials", {"unset": result}).items()
                },
            },
            sort_keys=True,
        )
    )
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
