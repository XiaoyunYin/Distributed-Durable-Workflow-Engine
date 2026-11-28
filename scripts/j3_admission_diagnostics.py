"""Retain per-container resource counters and evidence for a failed J3 sender."""

import json
import subprocess
import time
from contextlib import contextmanager, suppress
from pathlib import Path, PurePosixPath
from threading import Event, Thread

import dur053_k3 as kind
from j3_replay import require
from j3_resources import cgroup_paths

STAT_FILES = ("cpu.stat", "io.stat")
NODE_SAMPLE = r"""
while :; do
    at=$(date +%s%N)
    for spec do
        name=${spec%%=*}
        group=${spec#*=}
        for stat in cpu.stat io.stat; do
            if mapfile -t values 2>/dev/null < "$group/$stat"; then
                printf -v value '%s;' "${values[@]}"
                printf '%s\t%s\t%s\tok\t%s\n' "$at" "$name" "$stat" "${value%;}"
            else
                printf '%s\t%s\t%s\terror\tunreadable\n' "$at" "$name" "$stat"
            fi
        done
    done
    IFS= read -r -t 1 && break
    [ "$?" -gt 128 ] || break
done
"""
PG_SNAPSHOT = """
BEGIN READ ONLY;
SET LOCAL statement_timeout = '3s';
SELECT json_build_object(
    'sampled_at', clock_timestamp(),
    'pg_stat_activity', (SELECT coalesce(json_agg(a), '[]') FROM (
        SELECT pid, leader_pid, datname, usename, application_name, backend_type, state,
               wait_event_type, wait_event, xact_start, query_start, state_change,
               backend_xid, backend_xmin, query_id
        FROM pg_catalog.pg_stat_activity WHERE pid <> pg_backend_pid()) AS a),
    'pg_locks', (SELECT coalesce(json_agg(l), '[]') FROM pg_catalog.pg_locks AS l)
);
COMMIT;
"""


def parse_stat(name, text):
    lines = text.splitlines()
    if name == "cpu.stat":
        counters = {key: int(value) for key, value in (line.split() for line in lines)}
        require("usage_usec" in counters, "missing cpu.stat usage_usec")
        return counters
    return {
        device: {key: int(value) for key, value in (part.split("=", 1) for part in values)}
        for device, *values in (line.split() for line in lines)
    }


def parse_diskstats(text):
    return {
        f"{major}:{minor}": {"name": device, "counters": list(map(int, counters))}
        for major, minor, device, *counters in (line.split() for line in text.splitlines())
    }


def node_resource_rows(lines):
    for line in lines:
        try:
            at, name, stat, status, data = line.rstrip("\n").split("\t", 4)
            row = {"at_ns": int(at), "cgroup": name, "stat": stat}
            require(stat in STAT_FILES and status in {"ok", "error"}, "invalid node sample")
            row.update(
                counters=parse_stat(stat, data.replace(";", "\n")) if status == "ok" else None,
                error=data if status == "error" else None,
            )
        except ValueError as error:
            row = {"error": str(error), "raw": line.rstrip("\n")}
        yield row


def runner_resource_row(groups, proc):
    row = {
        "at_ns": time.time_ns(),
        "monotonic_ns": time.monotonic_ns(),
        "cgroups": {},
        "errors": {},
    }
    for name, group in groups.items():
        counters = row["cgroups"][name] = {}
        for stat in STAT_FILES:
            try:
                counters[stat] = parse_stat(stat, (group / stat).read_text(encoding="ascii"))
            except (OSError, ValueError) as error:
                row["errors"][f"{name}/{stat}"] = str(error)
    try:
        row["diskstats"] = parse_diskstats((proc / "diskstats").read_text(encoding="ascii"))
    except (OSError, ValueError) as error:
        row["errors"]["diskstats"] = str(error)
    return row


def resource_targets(driver, namespace, proc, cgroup_root):
    node = ["docker", "exec", kind.CLUSTER + "-control-plane"]
    pods = driver.k(
        "-n",
        namespace,
        "get",
        "pods",
        "-l",
        "app in (observer,api)",
        "-o",
        "json",
        record=False,
        timeout=5,
    )
    targets = {"node": {}, "runner": {}}
    for pod in json.loads(pods.stdout)["items"]:
        metadata = pod["metadata"]
        role = metadata["labels"]["app"]
        for container in pod["status"]["containerStatuses"]:
            runtime_id = container["containerID"].split("://", 1)[1]
            info = driver.command([*node, "crictl", "inspect", runtime_id], record=False, timeout=5)
            pid = json.loads(info.stdout)["info"]["pid"]
            require(
                type(pid) is int and pid > 0, "missing node container PID for resource sampling"
            )
            membership = driver.command(
                [*node, "cat", f"/proc/{pid}/cgroup"], record=False, timeout=5
            )
            leaf, parent = cgroup_paths(membership.stdout, metadata["uid"])
            require(
                runtime_id in PurePosixPath(leaf).name, "node container cgroup identity differs"
            )
            name = f"{role}/{metadata['name']}"
            targets["node"][name + "/pod"] = {"path": parent, "pod_uid": metadata["uid"]}
            targets["node"][name + "/" + container["name"]] = {
                "path": leaf,
                "pod_uid": metadata["uid"],
                "container_id": runtime_id,
                "pid": pid,
            }
    require(
        {name.split("/", 1)[0] for name in targets["node"]} == {"observer", "api"},
        "missing observer or API resource sampling target",
    )
    for role in ("postgres", "kafka"):
        result = driver.command(
            ["docker", "inspect", "--format", "{{.Id}} {{.State.Pid}}", kind.CLUSTER + "-" + role],
            record=False,
            timeout=5,
        )
        container_id, pid_text = result.stdout.split()
        pid = int(pid_text)
        require(pid > 0, "missing Docker container PID for resource sampling")
        (relative,) = [
            PurePosixPath(line[4:])
            for line in (proc / str(pid) / "cgroup").read_text(encoding="ascii").splitlines()
            if line.startswith("0::/")
        ]
        require(
            ".." not in relative.parts and container_id in relative.name,
            "Docker container cgroup identity differs",
        )
        targets["runner"][role] = {
            "path": str(cgroup_root / relative),
            "container_id": container_id,
            "pid": pid,
        }
    return targets


@contextmanager
def arm_resources(driver, namespace, *, proc=Path("/proc"), cgroup_root=Path("/sys/fs/cgroup")):
    targets = resource_targets(driver, namespace, proc, cgroup_root)
    output = driver.output
    kind.save(output / "arm-resource-targets.json", targets)
    groups = {name: Path(row["path"]) for name, row in targets["runner"].items()}
    node_args = [f"{name}={row['path']}" for name, row in targets["node"].items()]
    raw = output / "arm-node-resources.tsv"
    stop = Event()
    with (
        raw.open("wb") as node_out,
        (output / "arm-node-resources.stderr").open("wb") as node_err,
        (output / "arm-runner-resources.jsonl").open("w", encoding="utf-8", buffering=1) as host,
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
                *node_args,
            ],
            stdin=subprocess.PIPE,
            stdout=node_out,
            stderr=node_err,
            env=driver.env,
        ) as process,
    ):

        def sample_runner():
            while True:
                host.write(json.dumps(runner_resource_row(groups, proc)) + "\n")
                if stop.wait(1):
                    return

        worker = Thread(target=sample_runner, name="j3-arm-resources", daemon=True)
        worker.start()
        try:
            yield
        finally:
            stop.set()
            worker.join()
            with suppress(BrokenPipeError):
                process.stdin.close()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            kind.save(output / "arm-node-resource-status.json", {"exit": process.returncode})
            with (
                raw.open(encoding="utf-8") as samples,
                (output / "arm-node-resources.jsonl").open("w", encoding="utf-8") as parsed,
            ):
                for row in node_resource_rows(samples):
                    parsed.write(json.dumps(row) + "\n")


def redacted(driver, text):
    for attr in ("password", "demand_token", "monitor_password", "fixture_password"):
        if secret := getattr(driver, attr, None):
            text = text.replace(secret, "[REDACTED]")
    return text


def retain_sender_context(driver, namespace, seen_ns):
    """Take the DB snapshot first while the sender's failure path stops the arm."""
    report = {"failure_seen_ns": seen_ns, "postgres_started_ns": time.time_ns()}
    try:
        result = driver.postgres(PG_SNAPSHOT, allowed=True, timeout=6)
        report["postgres_exit"] = result.returncode
        if result.returncode == 0:
            report["snapshot"] = json.loads(redacted(driver, result.stdout))
        else:
            report["postgres_error"] = redacted(driver, result.stderr)
    except (OSError, ValueError) as error:
        report["postgres_error"] = redacted(driver, str(error))
    kind.save(driver.output / "sender-postgres-snapshot.json", report)

    report = {"started_ns": time.time_ns()}
    try:
        result = driver.k(
            "-n",
            namespace,
            "logs",
            "-l",
            "app=api",
            "--all-containers=true",
            "--timestamps=true",
            "--prefix=true",
            "--tail=-1",
            allowed=True,
            record=False,
            timeout=15,
        )
        report.update(exit=result.returncode, stderr=redacted(driver, result.stderr))
        (driver.output / "sender-api.log").write_text(
            redacted(driver, result.stdout), encoding="utf-8"
        )
    except (OSError, ValueError) as error:
        report["error"] = redacted(driver, str(error))
    kind.save(driver.output / "sender-api-log-status.json", report)


def outstanding_journal(path):
    """Use append order to recover provisional rows at the first failed arrival."""
    pending = {}
    seen = set()
    outstanding = None
    terminal_after = {}
    report = {"boundary": "exported_journal_end", "terminal_after_boundary": terminal_after}
    try:
        with path.open(encoding="utf-8") as stream:
            for index, line in enumerate(stream, 1):
                if not line.endswith("\n"):
                    report["partial_line"] = index
                    break
                row = json.loads(line)
                if row["event"] != "arrival":
                    continue
                key = row["key"]
                provisional = (
                    row["outcome"] == "uncertain" and row["status"] == 0 and not row["error"]
                )
                if provisional and key not in seen:
                    seen.add(key)
                    pending[key] = row
                    continue
                if outstanding is None and row["outcome"] != "accepted":
                    outstanding = pending.copy()
                    report.update(boundary="first_failed_arrival", line=index, failed_arrival=row)
                elif outstanding is not None and key in outstanding:
                    terminal_after[key] = row
                pending.pop(key, None)
    except (OSError, ValueError, KeyError) as error:
        report["error"] = str(error)
    report["outstanding"] = list((pending if outstanding is None else outstanding).values())
    return report
