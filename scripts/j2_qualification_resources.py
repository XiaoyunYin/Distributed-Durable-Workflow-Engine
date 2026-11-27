"""Retain observer resource samples and diagnose a lost container."""

from __future__ import annotations

import json
import re
import subprocess
import time
from contextlib import contextmanager
from pathlib import PurePosixPath

from j2_qualification_evidence import require_capacity, write

POD_FILES = ("memory.events", "memory.peak", "memory.events.local")
STATUS_WAIT = 60
SAMPLE = """
group=$1
while { read -r current < "$group/memory.current" &&
        read -r peak < "$group/memory.peak"; } 2>/dev/null; do
    rss=
    if pids=$(cat "$group/cgroup.procs" 2>/dev/null); then
        for pid in $pids; do
            if [ "$(cat "/proc/$pid/comm" 2>/dev/null)" = checker ]; then
                rss=$(awk '$1 == "VmHWM:" {print $2}' "/proc/$pid/status" 2>/dev/null)
                break
            fi
        done
    fi
    printf '%s\t%s\t%s\t%s\n' "$(date +%s%N)" "$current" "$peak" "$rss"
    IFS= read -r -t 1 && exit 0
    [ "$?" -gt 128 ] || exit 0
done
exit 1
"""


class MissingContainerPID(ValueError):
    def __init__(self, pod, uid, container):
        super().__init__(f"cannot locate container PID for {pod} ({container})")
        self.pod, self.uid = pod, uid


def pod_cgroup(container_path, uid):
    parent = PurePosixPath(container_path).parent
    if parent.name.removesuffix(".slice").endswith((f"pod{uid}", f"pod{uid.replace('-', '_')}")):
        return str(parent)
    return None


def oom_kill(events):
    return any(events.get(key, 0) > 0 for key in ("oom_kill", "oom_group_kill"))


def failure_criterion(pod_status, files):
    containers = pod_status.get("containerStatuses", []) + pod_status.get(
        "initContainerStatuses", []
    )
    killed = any(
        container.get(state, {}).get("terminated", {}).get("reason") == "OOMKilled"
        for container in containers
        for state in ("state", "lastState")
    )
    for name in ("memory.events", "memory.events.local"):
        counters = dict(re.findall(r"(?m)^(oom_kill|oom_group_kill) (\d+)$", files.get(name, "")))
        killed = killed or oom_kill({key: int(value) for key, value in counters.items()})
    if killed:
        return "oom"
    if any(container["restartCount"] > 0 for container in containers):
        return "restart"
    return None


def diagnose_failure(output, node, context, pod, uid, group, probe, *, trigger):
    """Raise a capacity failure only with positive evidence tied to this pod."""
    namespace, name = pod.split("/", 1)
    deadline = time.monotonic() + STATUS_WAIT
    polls = []
    criterion = None

    def capture(args, timeout):
        try:
            result = probe(args, timeout=timeout, allowed=True, record=False)
            return {"stdout": result.stdout, "stderr": result.stderr, "exit": result.returncode}
        except (OSError, ValueError) as error:
            return {"error": str(error)}

    while (remaining := deadline - time.monotonic()) > 0:
        evidence = {"unix_ns": time.time_ns()}
        status = capture(
            ["kubectl", "--context", context, "-n", namespace, "get", "pod", name, "-o", "json"],
            min(5, remaining),
        )
        if status.get("exit") == 0:
            try:
                document = json.loads(status["stdout"])
                evidence["uid"] = document["metadata"]["uid"]
                evidence["status"] = document["status"]
            except (ValueError, KeyError, TypeError):
                evidence["kubelet_error"] = status
        else:
            evidence["kubelet_error"] = status
        files = {}
        if group and (remaining := deadline - time.monotonic()) > 0:
            capture_group = capture(
                [
                    *node,
                    "sh",
                    "-c",
                    "for f in memory.events memory.peak memory.events.local; do "
                    'echo "$f"; cat "$1/$f"; done',
                    "sh",
                    group,
                ],
                min(5, remaining),
            )
            header = None
            for line in capture_group.pop("stdout", "").splitlines():
                if line in POD_FILES:
                    header = line
                    files[header] = ""
                elif header:
                    files[header] += line + "\n"
            evidence["cgroup"] = capture_group
        evidence["files"] = files
        polls.append(evidence)
        matched_status = evidence.get("status", {}) if evidence.get("uid") == uid else {}
        criterion = failure_criterion(matched_status, files)
        if criterion:
            break
        time.sleep(min(2, max(0, deadline - time.monotonic())))

    kernel = capture([*node, "dmesg"], 8)
    kernel["lines"] = [
        line
        for line in kernel.pop("stdout", "").splitlines()
        if re.search(r"oom-kill|out of memory|oom_reaper|killed process", line, re.I)
    ]
    if group:
        # The node can see a cgroup namespace relative to its container.
        # Kernel messages can carry the full host path to the same pod slice.
        pod_slice = PurePosixPath(group).name
        for line in kernel["lines"]:
            scopes = re.findall(r"(?:oom_memcg|task_memcg)=([^,\s]+)", line)
            if "oom-kill:" in line and any(
                pod_slice in PurePosixPath(scope).parts for scope in scopes
            ):
                criterion = "oom"
                break
    record = {
        "pod": pod,
        "uid": uid,
        "pod_cgroup": group,
        "trigger": trigger,
        "polls": polls,
        "kernel": kernel,
    }
    write(output / f"capacity-{name}.json", {**record, "criterion": criterion})
    if criterion:
        require_capacity(False, criterion, f"{pod}: confirmed {criterion}")


@contextmanager
def sample_checker(output, node, cgroup, env):
    # Run outside the observer's cgroup so the sampler survives a group kill.
    path = output / "live-checker-resources.tsv"
    with (
        path.open("wb") as out,
        (output / "live-checker-resources.stderr").open("wb") as err,
        subprocess.Popen(
            ["docker", "exec", "-i", node, "timeout", "610", "bash", "-c", SAMPLE, "bash", cgroup],
            stdin=subprocess.PIPE,
            stdout=out,
            stderr=err,
            env=env,
        ) as process,
    ):
        try:
            yield process
        finally:
            process.stdin.close()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def checker_samples(output):
    path = output / "live-checker-resources.tsv"
    samples = [line.split("\t") for line in path.read_text().splitlines()]
    timestamps = [int(row[0]) for row in samples]
    summary = {
        "samples": len(samples),
        "observer_current_peak_bytes": max((int(row[1]) for row in samples), default=None),
        "observer_peak_bytes": max((int(row[2]) for row in samples), default=None),
        "checker_vmhwm_peak_kib": max((int(row[3]) for row in samples if row[3]), default=None),
        "max_sample_gap_seconds": max(
            ((right - left) / 1e9 for left, right in zip(timestamps, timestamps[1:], strict=False)),
            default=None,
        ),
    }
    stderr = output / "live-checker.stderr"
    if stderr.is_file():
        for line in stderr.read_text(errors="replace").splitlines():
            if line.startswith("J2_CHECKER "):
                summary["checker_exit_measurement"] = json.loads(line.removeprefix("J2_CHECKER "))
    return summary
