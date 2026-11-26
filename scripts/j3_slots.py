"""Publish the Ready-slot witness used by the J3 dashboard."""

import json
import time
from concurrent.futures import ThreadPoolExecutor

from j3_replay import require

WORKER_WITNESS = """import json,urllib.request
with urllib.request.urlopen('http://127.0.0.1:8081/witness',timeout=3) as response:
    print(json.dumps(json.load(response)))
"""


def worker_status(pod):
    metadata, status = pod["metadata"], pod["status"]
    terminating = metadata.get("deletionTimestamp") is not None
    ready = (
        not terminating
        and status.get("phase") == "Running"
        and any(
            condition["type"] == "Ready" and condition["status"] == "True"
            for condition in status.get("conditions", [])
        )
    )
    return {
        "name": metadata["name"],
        "uid": metadata["uid"],
        "ready": ready,
        "terminating": terminating,
        "containers": status.get("containerStatuses", []),
    }


def ready_slots(pods, witnesses):
    slots = []
    uids = set()
    for pod in pods:
        status = worker_status(pod)
        if not status["ready"]:
            continue
        name, uid = status["name"], status["uid"]
        witness = witnesses.get(uid, {})
        require(
            witness.get("complete") is True
            and witness.get("pod_uid") == uid
            and witness.get("pod_name") == name
            and bool(witness.get("session"))
            and uid not in uids,
            f"missing or mismatched slot witness for {name}",
        )
        (container,) = [c for c in pod["spec"]["containers"] if c["name"] == "worker"]
        (count,) = [e.get("value") for e in container["env"] if e["name"] == "WORKER_SLOTS"]
        require(count == "2", f"registered worker slots changed for {name}")
        uids.add(uid)
        slots.extend(f"{name}-{i}" for i in range(int(count)))
    require(len(slots) == len(set(slots)) <= 4, "duplicate slots or worker surge")
    return sorted(slots)


def sample_ready_slots(driver, app, receiver_namespace, receiver, pods):
    sampled = time.time_ns()
    statuses = [worker_status(pod) for pod in pods]

    def read(status):
        result = driver.k(
            "-n",
            app,
            "exec",
            status["name"],
            "-c",
            "worker",
            "--",
            "python",
            "-c",
            WORKER_WITNESS,
            record=False,
            timeout=5,
        )
        return status["uid"], json.loads(result.stdout)

    with ThreadPoolExecutor(max_workers=2) as executor:
        witnesses = dict(executor.map(read, (p for p in statuses if p["ready"])))
    record = {"sample_ns": sampled, "pods": statuses, "witness": witnesses}
    try:
        record["slots"] = ready_slots(pods, witnesses)
        metric = {"ready_slots": len(record["slots"]), "ready_slots_sample_ns": sampled}
        driver.python(
            receiver_namespace,
            receiver,
            "import os,pathlib; p=pathlib.Path('/evidence/metrics.tmp'); "
            f"p.write_text({json.dumps(metric)!r}); os.replace(p,'/evidence/metrics.json')",
            record=False,
        )
    finally:
        with (driver.output / "ready-slots.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record) + "\n")
    return record
