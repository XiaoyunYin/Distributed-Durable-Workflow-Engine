#!/usr/bin/env python3
"""Self-contained K2c Compose prerequisite proof, never a K3/K5 measurement.

Runs four isolated fresh stacks sequentially; exports before database deletion.
The only entry point is dur053-kubernetes.ps1. Failures are never auto-rerun.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import secrets
import shutil
import subprocess
import time
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = (
    ROOT / "experiments/portfolio/k8s-recovery/k2-prerequisites-v1/compose-prerequisites"
)
ARMS = {"short": 100, "medium": 5000, "long": 45000, "cpu": 100}
PORTS = {
    "POSTGRES_PORT": "25432",
    "KAFKA_PORT": "29092",
    "RUNTIME_A_PORT": "28080",
    "RUNTIME_B_PORT": "28081",
    "WORKER_A_PORT": "28181",
    "WORKER_B_PORT": "28182",
    "OTEL_GRPC_PORT": "24317",
    "OTEL_HTTP_PORT": "24318",
    "OTEL_HEALTH_PORT": "23133",
    "PROMETHEUS_PORT": "29090",
}


def save(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def trace_document(document: str) -> str:
    """Preserve RawMessage ordering, number spelling and escapes across export.

    The caller has already decoded the complete JSON report. Walk its top-level
    members, skipping nested values with the JSON decoder, and copy the original
    trace substring. Re-encoding the Python object changes snapshot hash inputs.
    """
    decoder = json.JSONDecoder()

    def skip_space(position: int) -> int:
        suffix = document[position:]
        return position + len(suffix) - len(suffix.lstrip())

    position = skip_space(document.index("{") + 1)
    while document[position] != "}":
        name, key_end = decoder.raw_decode(document, position)
        value_start = skip_space(document.index(":", key_end) + 1)
        _value, value_end = decoder.raw_decode(document, value_start)
        if name == "trace":
            return document[value_start:value_end] + "\n"
        position = skip_space(value_end)
        if document[position] != ",":
            break
        position = skip_space(position + 1)
    raise RuntimeError("missing top-level trace")


def execute(
    args: list[str],
    env: dict[str, str],
    *,
    stdin: str | None = None,
    timeout: float = 600,
    allow_failure: bool = False,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            args,
            cwd=ROOT,
            env=env,
            input=stdin,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        if not allow_failure:
            raise RuntimeError(f"{args[0]} could not finish: {type(error).__name__}") from error
        return subprocess.CompletedProcess(
            args, 1, "", f"{type(error).__name__}: tool did not complete"
        )
    if result.returncode and not allow_failure:
        # Do not include a command line/environment containing DB credentials.
        raise RuntimeError(f"{args[0]} failed ({result.returncode}): {result.stderr[-2000:]}")
    return result


def get(url: str) -> Any:
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


def export_witness(
    directory: Path, inventories: list[dict[str, Any]], expected: list[str]
) -> dict[str, Any]:
    ledgers = []
    seen_files = set()
    for inventory in inventories:
        if not inventory.get("complete") or inventory.get("errors") != 0:
            raise RuntimeError("witness writer reports incomplete evidence")
        if Path(inventory["file"]).name != inventory["file"]:
            raise RuntimeError("invalid witness export filename")
        path = directory / inventory["pod_name"] / inventory["file"]
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != inventory["sha256"]:
            raise RuntimeError("witness file differs from independent live inventory")
        seen_files.add(path.resolve())
        ledgers.append({"inventory": inventory, "data": base64.b64encode(data).decode("ascii")})
    if seen_files != {p.resolve() for p in directory.glob("*/*.jsonl")}:
        raise RuntimeError("unattested witness process/session file")
    if set(expected) != {i["pod_uid"] for i in inventories}:
        raise RuntimeError("missing expected witness pod")
    return {"expected_pods": expected, "ledgers": ledgers}


def reconcile(
    arrivals: dict[str, Any], worker_urls: list[str], deadline: float
) -> list[dict[str, Any]]:
    last: str | None = None
    quiet_since = time.monotonic()
    while time.monotonic() < deadline:
        inventories = [get(url + "/witness") for url in worker_urls]
        fingerprint = json.dumps(inventories, sort_keys=True)
        if fingerprint != last:
            quiet_since, last = time.monotonic(), fingerprint
        all_terminal = True
        for row in arrivals["rows"]:
            try:
                workflow = get("http://127.0.0.1:28080/v1/workflows/" + row["workflow_id"])[
                    "workflow"
                ]
                if workflow["state"] not in {
                    "SUCCEEDED",
                    "FAILED",
                    "CANCELED",
                    "REJECTED",
                    "ABANDONED",
                }:
                    all_terminal = False
            except Exception:
                all_terminal = False
        if all_terminal and time.monotonic() - quiet_since >= 30:
            return inventories
        time.sleep(0.5)
    raise RuntimeError("terminal/witness-quiet reconciliation timed out (unfinished work retained)")


def episode(
    arm: str, index: int, output: Path, scratch: Path, env: dict[str, str], bins: dict[str, str]
) -> dict[str, Any]:
    output.mkdir()
    witness = output / "witness"
    for pod in ("worker-a", "worker-b"):
        path = witness / pod
        path.mkdir(parents=True)
        path.chmod(0o777)
    episode_id = "dur053-k2c-" + arm + "-" + secrets.token_hex(4)
    database = episode_id.replace("-", "_")
    password = secrets.token_hex(32)
    config = {
        **PORTS,
        "POSTGRES_USER": "durable",
        "POSTGRES_PASSWORD": password,
        "POSTGRES_DB": database,
        "DUR053_NAMESPACE": episode_id,
        "DUR048_ACTIVITY_DELAY_MS": str(ARMS[arm]),
        "DUR053_CPU_WORK_UNITS": "10000",
        "DUR053_WITNESS_HOST": witness.as_posix(),
    }
    envfile = scratch / (arm + ".env")
    envfile.write_text(
        "".join(f"{key}={value}\n" for key, value in config.items()), encoding="utf-8"
    )
    local = {**env, **config}
    compose = [
        "docker",
        "compose",
        "--project-name",
        episode_id,
        "--env-file",
        str(envfile),
        "-f",
        "deploy/local/compose.yaml",
        "-f",
        "deploy/local/compose.dur053-prerequisites.yaml",
    ]
    summary: dict[str, Any] = {
        "arm": arm,
        "status": "FAIL",
        "namespace": episode_id,
        "seed": 530001 + index,
        "delay_ms": ARMS[arm],
        "cpu_units": 10000,
        "purpose": "prerequisite submit-to-terminal proof; no pilot/SLO result",
    }
    save(output / "manifest.json", summary)
    started = False
    db_created = False
    try:
        started = True
        # Only healthy long-running dependencies participate in this wait;
        # run the one-shot topic initializer in the foreground for its exit code.
        execute([*compose, "up", "-d", "--wait", "postgres", "kafka"], local)
        db_created = True
        execute([*compose, "run", "--rm", "--no-deps", "kafka-init"], local)
        for migration in sorted((ROOT / "migrations").glob("*.up.sql")):
            execute(
                [
                    *compose,
                    "exec",
                    "-T",
                    "postgres",
                    "psql",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-U",
                    "durable",
                    "-d",
                    database,
                ],
                local,
                stdin=migration.read_text(encoding="utf-8"),
            )
        execute([*compose, "up", "-d", "--build", "--wait"], local, timeout=1200)
        identities = {}
        for service in ("runtime-a", "runtime-b", "worker-a", "worker-b"):
            cid = execute([*compose, "ps", "-q", service], local).stdout.strip()
            inspected = json.loads(execute(["docker", "inspect", cid], local).stdout)[0]
            identities[service] = {k: inspected[k] for k in ("Id", "Image", "Created")}
            identities[service]["state"] = inspected["State"]
        save(output / "container-identities.json", identities)
        runtime_manifest = json.loads(
            execute(
                [*compose, "exec", "-T", "runtime-a", "/runtime", "activity-registry"], local
            ).stdout
        )
        worker_manifest = json.loads(
            execute(
                [
                    *compose,
                    "exec",
                    "-T",
                    "worker-a",
                    "python",
                    "-m",
                    "workers",
                    "activity-registry",
                ],
                local,
            ).stdout
        )
        expected_registry = [
            {"name": name, "version": "v1"}
            for name in ("dur048.sleep", "dur053.cpu", "pure.add", "pure.echo")
        ]
        if runtime_manifest != worker_manifest or runtime_manifest != expected_registry:
            raise RuntimeError("actual campaign image activity registry mismatch")
        save(output / "registries.json", {"runtime": runtime_manifest, "worker": worker_manifest})
        local["DATABASE_URL"] = (
            f"postgresql://durable:{password}@127.0.0.1:25432/{database}?sslmode=disable"
        )
        arrivals_path = output / "arrivals.json"
        load = execute(
            [
                bins["loadgen"],
                "-url",
                "http://127.0.0.1:28080",
                "-namespace",
                episode_id,
                "-run-id",
                episode_id,
                "-arm",
                arm,
                "-seed",
                str(530001 + index),
                "-count",
                "10" if arm != "cpu" else "3",
                "-rate",
                "1",
                "-concurrency",
                "16",
                "-cpu-units",
                "10000",
                "-output",
                str(arrivals_path),
            ],
            local,
            allow_failure=True,
        )
        summary["loadgen_exit"] = load.returncode
        (output / "loadgen-stderr.txt").write_text(load.stderr, encoding="utf-8")
        arrivals = json.loads(arrivals_path.read_text())
        inventories = reconcile(
            arrivals, ["http://127.0.0.1:28181", "http://127.0.0.1:28182"], time.monotonic() + 600
        )
        exported = export_witness(
            witness, inventories, [episode_id + "-worker-a", episode_id + "-worker-b"]
        )
        save(output / "witness-export.json", exported)
        checker_args = [
            bins["checker"],
            "-arrivals",
            str(arrivals_path),
            "-witness-export",
            str(output / "witness-export.json"),
            "-require-terminal",
        ]
        checked = execute([*checker_args, "-namespace", episode_id], local, allow_failure=True)
        (output / "checker.json").write_text(checked.stdout, encoding="utf-8")
        (output / "checker-stderr.txt").write_text(checked.stderr, encoding="utf-8")
        report = json.loads(checked.stdout)
        (output / "snapshot.json").write_text(
            trace_document(checked.stdout), encoding="utf-8", newline="\n"
        )
        if (
            checked.returncode
            or not report["valid"]
            or report["status"] != "PASS"
            or load.returncode
        ):
            raise RuntimeError("load/checker completeness or safety proof failed")
        offline = execute([*checker_args, "-snapshot", str(output / "snapshot.json")], local)
        (output / "offline-checker.json").write_text(offline.stdout, encoding="utf-8", newline="\n")
        # A partial witness export must fail against the unchanged attestation.
        damaged = json.loads(json.dumps(exported))
        damaged["ledgers"][0]["data"] = base64.b64encode(
            base64.b64decode(damaged["ledgers"][0]["data"])[:-1]
        ).decode()
        damaged_path = output / "negative-incomplete-witness.json"
        save(damaged_path, damaged)
        negative = execute(
            [
                bins["checker"],
                "-snapshot",
                str(output / "snapshot.json"),
                "-arrivals",
                str(arrivals_path),
                "-witness-export",
                str(damaged_path),
            ],
            local,
            allow_failure=True,
        )
        (output / "negative-checker.json").write_text(
            negative.stdout, encoding="utf-8", newline="\n"
        )
        negative_report = json.loads(negative.stdout)
        save(
            output / "negative-control.json",
            {
                "exit_code": negative.returncode,
                "status": negative_report.get("status"),
                "valid": negative_report.get("valid"),
                "violations": negative_report.get("violations"),
                "campaign": negative_report.get("campaign"),
            },
        )
        if negative.returncode == 0:
            raise RuntimeError("incomplete-export negative control passed")
        summary["status"] = "PASS"
        summary["metrics"] = report["campaign"]
    except Exception as error:
        summary["error"] = str(error)
    finally:
        cleanup: dict[str, Any] = {"database_dropped": False, "resources_removed": False}
        if started:
            if db_created and not (output / "snapshot.json").exists():
                failed_export = execute(
                    [bins["checker"], "-namespace", episode_id],
                    {
                        **local,
                        "DATABASE_URL": f"postgresql://durable:{password}@127.0.0.1:25432/{database}?sslmode=disable",
                    },
                    allow_failure=True,
                )
                (output / "failure-checker.json").write_text(failed_export.stdout, encoding="utf-8")
                (output / "failure-checker-stderr.txt").write_text(
                    failed_export.stderr, encoding="utf-8"
                )
                summary["failure_snapshot_exit"] = failed_export.returncode
            # Export logs before deletion even on failure; no credentials/config dumps.
            logs = execute([*compose, "logs", "--no-color"], local, allow_failure=True)
            (output / "compose-logs.txt").write_text(logs.stdout + logs.stderr, encoding="utf-8")
            execute(
                [*compose, "stop", "runtime-a", "runtime-b", "worker-a", "worker-b"],
                local,
                allow_failure=True,
            )
            drop = execute(
                [
                    *compose,
                    "exec",
                    "-T",
                    "postgres",
                    "psql",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-U",
                    "durable",
                    "-d",
                    "postgres",
                    "-c",
                    f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE);',
                ],
                local,
                allow_failure=True,
            )
            if drop.returncode == 0:
                absent = execute(
                    [
                        *compose,
                        "exec",
                        "-T",
                        "postgres",
                        "psql",
                        "-At",
                        "-U",
                        "durable",
                        "-d",
                        "postgres",
                        "-c",
                        f"SELECT count(*) FROM pg_database WHERE datname='{database}';",
                    ],
                    local,
                    allow_failure=True,
                )
                cleanup["database_dropped"] = (
                    absent.returncode == 0 and absent.stdout.strip() == "0"
                )
            elif not db_created:
                # No successful dependency startup: down removes any partial init volume.
                cleanup["database_dropped"] = "no confirmed database; partial volume removed below"
            down = execute(
                [*compose, "down", "--volumes", "--remove-orphans"], local, allow_failure=True
            )
            remaining = execute(
                [
                    "docker",
                    "ps",
                    "-aq",
                    "--filter",
                    "label=com.docker.compose.project=" + episode_id,
                ],
                local,
                allow_failure=True,
            )
            volumes = execute(
                [
                    "docker",
                    "volume",
                    "ls",
                    "-q",
                    "--filter",
                    "label=com.docker.compose.project=" + episode_id,
                ],
                local,
                allow_failure=True,
            )
            networks = execute(
                [
                    "docker",
                    "network",
                    "ls",
                    "-q",
                    "--filter",
                    "label=com.docker.compose.project=" + episode_id,
                ],
                local,
                allow_failure=True,
            )
            cleanup["resources_removed"] = (
                down.returncode
                == remaining.returncode
                == volumes.returncode
                == networks.returncode
                == 0
                and not (remaining.stdout + volumes.stdout + networks.stdout).strip()
            )
        if not cleanup["database_dropped"] or not cleanup["resources_removed"]:
            summary["status"] = "FAIL"
        summary["cleanup"] = cleanup
        save(output / "manifest.json", summary)
    return summary


def run(output: Path, scratch_root: Path) -> int:
    output.mkdir(parents=True, exist_ok=False)
    scratch_root.mkdir(parents=True, exist_ok=True)
    scratch = scratch_root / ("compose-" + secrets.token_hex(8))
    scratch.mkdir()
    env = dict(os.environ)
    env.update({key: str(scratch) for key in ("TMP", "TEMP", "TMPDIR", "GOTMPDIR")})
    env["GOCACHE"] = str(scratch / "go-cache")
    env["UV_CACHE_DIR"] = str(scratch / "uv-cache")
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    result: dict[str, Any] = {
        "schema": "dur053.k2c.prerequisites.v1",
        "status": "FAIL",
        "episodes": [],
        "command": "pwsh -File scripts/dur053-kubernetes.ps1 -Context compose -Scenario prerequisites",
        "limitations": [
            "single-host Compose prerequisite only",
            "no faults/cancellation/effects/clock-skew",
            "K3 CPU calibration pending",
        ],
    }
    save(output / "manifest.json", result)
    try:
        result["base_commit"] = execute(["git", "rev-parse", "HEAD"], env).stdout.strip()
        result["dirty"] = bool(
            execute(["git", "status", "--porcelain", "--untracked-files=no"], env).stdout.strip()
        )
        result["source_sha256"] = {
            str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
            for pattern in (
                "cmd/**/*.go",
                "internal/**/*.go",
                "python/**/*.py",
                "scripts/dur053*",
                "deploy/local/*",
            )
            for p in sorted(ROOT.glob(pattern))
            if p.is_file()
        }
        result["docker_version"] = execute(
            ["docker", "version", "--format", "{{json .}}"], env
        ).stdout.strip()
        result["compose_version"] = execute(["docker", "compose", "version"], env).stdout.strip()
        bins = {
            name: str(scratch / (name + (".exe" if os.name == "nt" else "")))
            for name in ("loadgen", "checker")
        }
        for name, package in (
            ("loadgen", "./cmd/dur053-loadgen"),
            ("checker", "./cmd/dur049-checker"),
        ):
            execute(["go", "build", "-o", bins[name], package], env)
        for index, arm in enumerate(ARMS):
            report = episode(arm, index, output / arm, scratch, env, bins)
            result["episodes"].append(report)
            if report["status"] != "PASS":
                raise RuntimeError(f"{arm} prerequisites failed; no automatic replacement")
        if len(result["episodes"]) != 4:
            raise RuntimeError("missing prerequisite arms")
        result["status"] = "PASS"
    except Exception as error:
        result["error"] = str(error)
    finally:
        # Only this invocation's child directory; never remove a computed parent.
        resolved = scratch.resolve()
        if resolved.parent != scratch_root.resolve() or not resolved.name.startswith("compose-"):
            raise RuntimeError("scratch cleanup target escaped owned root")
        shutil.rmtree(resolved)
        result["scratch_removed"] = not resolved.exists()
        result["artifact_sha256"] = {
            str(p.relative_to(output)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(output.rglob("*"))
            if p.is_file()
        }
        save(output / "manifest.json", result)
    return 0 if result["status"] == "PASS" else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--scratch-root", type=Path, required=True)
    args = parser.parse_args()
    return run(args.output.resolve(), args.scratch_root.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
