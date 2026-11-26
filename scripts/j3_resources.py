"""Retain checker and observer evidence outside the observer pod cgroup."""

import json
import re
import subprocess
import time
from pathlib import PurePosixPath

from j3_replay import SECOND, require

OBSERVER_LIMIT = 2 << 30
CGROUP_FILES = ("memory.events", "memory.events.local", "memory.peak", "memory.max")
CHECKER = """
import json,resource,subprocess,sys
status=subprocess.run(sys.argv[1:]).returncode
peak=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
print('J3_CHECKER '+json.dumps({'status':status,'rss_peak_kib':peak}),file=sys.stderr,flush=True)
sys.exit(status if status>=0 else 128-status)
"""
SAMPLER = """
while IFS= read -r current < "$1/memory.current" && IFS= read -r peak < "$1/memory.peak"; do
    rss=
    for pid in $(cat "$1/cgroup.procs" 2>/dev/null); do
        if [ "$(cat "/proc/$pid/comm" 2>/dev/null)" = "$2" ]; then
            hwm=$(awk '$1 == "VmHWM:" {print $2}' "/proc/$pid/status" 2>/dev/null)
            if [ -n "$hwm" ] && [ "${rss:-0}" -lt "$hwm" ]; then rss=$hwm; fi
        fi
    done
    printf '%s\t%s\t%s\t%s\n' "$(date +%s%N)" "$current" "$peak" "$rss"
    IFS= read -r -t 1 && break
    [ "$?" -gt 128 ] || break
done
"""


def recorded_command(probe, command, timeout):
    try:
        result = probe(command, timeout=timeout, allowed=True, record=False)
    except (OSError, TimeoutError, ValueError) as error:
        return {"error": str(error)}
    return {"exit": result.returncode, "stdout": result.stdout, "stderr": result.stderr}


def cgroup_paths(membership, pod_uid):
    (member,) = [line[3:] for line in membership.splitlines() if line.startswith("0::/")]
    container = PurePosixPath("/sys/fs/cgroup" + member)
    pod = container.parent
    require(".." not in container.parts, "invalid cgroup membership")
    suffixes = f"pod{pod_uid}", f"pod{pod_uid.replace('-', '_')}"
    require(pod.name.removesuffix(".slice").endswith(suffixes), "observer pod cgroup not resolved")
    return str(container), str(pod)


def oom_counts(files):
    return {
        (name, key): int(value)
        for name in CGROUP_FILES[:2]
        for key, value in re.findall(r"(?m)^(oom_kill|oom_group_kill) (\d+)$", files.get(name, ""))
    }


def checker_failure(uid, group, before, polls, kernel_lines):
    containers = [
        container
        for poll in polls
        if poll.get("uid") == uid
        for container in poll["status"].get("containerStatuses", [])
    ]
    oom_state = any(
        c.get(key, {}).get("terminated", {}).get("reason") == "OOMKilled"
        for c in containers
        for key in ("state", "lastState")
    )
    baseline = oom_counts(before)
    oom_count = any(
        key in baseline and value > baseline[key]
        for poll in polls
        for key, value in oom_counts(poll["files"]).items()
    )
    relative = group.removeprefix("/sys/fs/cgroup")
    # The Linux OOM summary prints kernfs paths from the host hierarchy.
    # The kind node's /proc membership can omit the Docker cgroup ancestors.
    oom_log = any(
        "oom-kill:" in line
        and any(path.endswith(relative) or relative + "/" in path for path in paths)
        for line in kernel_lines
        if (paths := re.findall(r"(?:oom_memcg|task_memcg)=([^,\s]+)", line))
    )
    if oom_state or oom_count or oom_log:
        return "oom"
    if any(c["restartCount"] > 0 for c in containers):
        return "restart"
    return "unexplained_checker_exit"


def read_group(driver, node, group):
    capture = recorded_command(
        driver.command,
        [
            *node,
            "sh",
            "-c",
            'for f do printf "[%s]\\n" "$f"; cat "$0/$f"; done',
            group,
            *CGROUP_FILES,
        ],
        5,
    )
    pieces = re.split(r"(?m)^\[(memory\.[a-z.]+)\]\n", capture.get("stdout", ""))
    return dict(zip(pieces[1::2], pieces[2::2], strict=True)), capture


def kernel_oom_lines(driver, node):
    record = recorded_command(driver.command, [*node, "dmesg"], 8)
    record["lines"] = [
        line
        for line in record.pop("stdout", "").splitlines()
        if re.search(r"oom-kill|out of memory|oom_reaper|killed process", line, re.I)
    ]
    return record


def diagnose(driver, node, namespace, name, uid, group, before, kernel_before, exit_status):
    polls = []
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        before_ns = time.time_ns()
        status = recorded_command(
            driver.command,
            ["kubectl", "-n", namespace, "get", "pod", name, "-o", "json"],
            5,
        )
        files, raw = read_group(driver, node, group)
        row = {"before_ns": before_ns, "kubelet": status, "files": files, "cgroup": raw}
        if status.get("exit") == 0:
            try:
                pod = json.loads(status["stdout"])
                row.update(uid=pod["metadata"]["uid"], status=pod["status"])
            except (KeyError, TypeError, ValueError) as error:
                row["kubelet_parse_error"] = str(error)
        polls.append(row)
        same_pod = row.get("uid") == uid
        containers = row.get("status", {}).get("containerStatuses", [])
        if same_pod and any(
            c["restartCount"] > 0 or "terminated" in c.get("state", {}) for c in containers
        ):
            break
        time.sleep(2)

    kernel = kernel_oom_lines(driver, node)
    baseline_lines = set(kernel_before["lines"])
    new_lines = (
        [line for line in kernel["lines"] if line not in baseline_lines]
        if kernel_before.get("exit") == kernel.get("exit") == 0
        else []
    )
    return {
        "checker_exit": exit_status,
        "pod_uid": uid,
        "pod_cgroup": group,
        "baseline_cgroup": before,
        "polls": polls,
        "kernel_before": kernel_before,
        "kernel_after": kernel,
        "new_kernel_oom_lines": new_lines,
        "criterion": checker_failure(uid, group, before, polls, new_lines),
    }


def write_report(path, report):
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


def run_checker(driver, node_name, namespace, observer, checker, args):
    """Run the already-installed checker and retain exit evidence before any teardown."""
    node = ["docker", "exec", node_name]
    name, uid = observer["metadata"]["name"], observer["metadata"]["uid"]
    (container,) = observer["status"]["containerStatuses"]
    _, runtime_id = container["containerID"].split("://", 1)
    info = driver.command([*node, "crictl", "inspect", runtime_id], record=False, timeout=5)
    pid = json.loads(info.stdout)["info"]["pid"]
    require(type(pid) is int and pid > 0, "observer PID missing before checker")
    membership = driver.command([*node, "cat", f"/proc/{pid}/cgroup"], record=False, timeout=5)
    group, pod_group = cgroup_paths(membership.stdout, uid)
    before, _ = read_group(driver, node, pod_group)
    limits, _ = read_group(driver, node, group)
    require(int(limits["memory.max"]) == OBSERVER_LIMIT, "observer memory limit changed")
    require(container["restartCount"] == 0, "observer restarted before checker")
    require(oom_counts(before) and not any(oom_counts(before).values()), "invalid OOM baseline")
    kernel_before = kernel_oom_lines(driver, node)
    output = driver.output
    command = [
        "kubectl",
        "-n",
        namespace,
        "exec",
        name,
        "--",
        "python",
        "-c",
        CHECKER,
        checker,
        *args,
    ]
    sample_file = output / "checker-resources.tsv"
    with (
        sample_file.open("wb") as samples,
        (output / "checker-resources.stderr").open("wb") as sample_err,
        (output / "checker.json").open("wb") as verdict,
        (output / "checker.stderr").open("wb") as checker_err,
        subprocess.Popen(
            [
                "docker",
                "exec",
                "-i",
                node_name,
                "bash",
                "-c",
                SAMPLER,
                "bash",
                group,
                PurePosixPath(checker).name[:15],
            ],
            stdin=subprocess.PIPE,
            stdout=samples,
            stderr=sample_err,
            env=driver.env,
        ) as monitor,
    ):
        try:
            result = subprocess.run(
                command, stdout=verdict, stderr=checker_err, env=driver.env, timeout=600
            )
            status = result.returncode
        except subprocess.TimeoutExpired:
            status = "deadline"
        finally:
            monitor.stdin.close()
            try:
                monitor.wait(timeout=3)
            except subprocess.TimeoutExpired:
                monitor.kill()
                monitor.wait()

    if status != 0:
        diagnosis = diagnose(
            driver, node, namespace, name, uid, pod_group, before, kernel_before, status
        )
        write_report(output / "checker-failure.json", diagnosis)
    rows = [line.split("\t") for line in sample_file.read_text().splitlines()]
    timestamps = [int(row[0]) for row in rows]
    measurements = [
        json.loads(line.removeprefix("J3_CHECKER "))
        for line in (output / "checker.stderr").read_text(errors="replace").splitlines()
        if line.startswith("J3_CHECKER ")
    ]
    final_container, _ = read_group(driver, node, group)
    report = {
        "exit": status,
        "observer_current_peak_bytes": max((int(row[1]) for row in rows), default=None),
        "observer_peak_bytes": max(
            [int(row[2]) for row in rows] + [int(final_container.get("memory.peak") or 0)]
        ),
        "observer_limit_bytes": OBSERVER_LIMIT,
        "checker_sampled_peak_rss_kib": max((int(row[3]) for row in rows if row[3]), default=None),
        "checker_exit_measurements": measurements,
        "max_sample_gap_ns": max(
            (b - a for a, b in zip(timestamps, timestamps[1:], strict=False)), default=0
        ),
    }
    write_report(output / "checker-resources.json", report)
    if status != 0:
        raise ValueError(f"checker exit {status}: {diagnosis['criterion']}")
    require(
        rows
        and report["max_sample_gap_ns"] <= 2 * SECOND
        and len(measurements) == 1
        and measurements[0]["rss_peak_kib"] > 0
        and 0 < report["observer_peak_bytes"] <= OBSERVER_LIMIT
        and oom_counts(final_container)
        and not any(oom_counts(final_container).values()),
        "missing or invalid observer/checker resource evidence",
    )
    return json.loads((output / "checker.json").read_bytes())


def checker_outcome(output):
    report = output / "checker-failure.json"
    if report.is_file():
        criterion = json.loads(report.read_bytes()).get("criterion")
        if criterion in {"oom", "restart"}:
            return "capacity"
    return "invalid"
