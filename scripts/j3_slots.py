"""Publish the Ready-slot witness used by the J3 dashboard."""

import json
import subprocess
import time
from ipaddress import ip_address

from j3_replay import SECOND, require

OUTAGE_SLOT_BOUNDARIES = (600, 900, 960)
SCRAPE_SLOT_BOUNDARIES = (600, 660)

WORKER_WITNESS = """import http.client,json,sys
from concurrent.futures import ThreadPoolExecutor

def read(item):
    uid,ip=item
    connection=http.client.HTTPConnection(ip,8081,timeout=3)
    try:
        connection.request('GET','/witness')
        response=connection.getresponse()
        if response.status != 200:
            return uid, {'error':f'witness HTTP {response.status}'}
        body=response.read(65537)
        if len(body) > 65536:
            return uid, {'error':'witness response too large'}
        return uid, {'witness':json.loads(body)}
    except (OSError,ValueError,http.client.HTTPException) as error:
        return uid, {'error':str(error)}
    finally:
        connection.close()

with ThreadPoolExecutor(max_workers=2) as pool:
    print(json.dumps(dict(pool.map(read,json.loads(sys.argv[1]).items()))))
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


def slot_sample_phase(arm, origin, started, finished):
    if origin is None or started < origin:
        return "setup"
    start, end = started - origin, finished - origin
    if arm == "outage":
        boundaries = OUTAGE_SLOT_BOUNDARIES
    elif arm == "scrape_loss":
        boundaries = SCRAPE_SLOT_BOUNDARIES
    else:
        boundaries = ()
    if any(start < bound * SECOND <= end for bound in boundaries):
        return "transition"
    if arm == "outage":
        down, restore, ready = OUTAGE_SLOT_BOUNDARIES
        if end < down * SECOND:
            return "healthy"
        if start >= ready * SECOND:
            return "restored"
        return "outage" if start < restore * SECOND else "restoring"
    # A scrape fault does not take any worker slots away.
    return "healthy"


def sample_ready_slots(
    driver,
    app,
    receiver_namespace,
    receiver,
    pods,
    *,
    observer_pod,
    receiver_pod=None,
    read_started_ns=None,
    arm=None,
    origin=None,
):
    """Read worker witnesses, retaining the qualification's pod-read start when supplied."""
    sampled = time.time_ns()
    statuses = [worker_status(pod) for pod in pods]
    record = {
        "sample_ns": sampled,
        "read_started_ns": sampled if read_started_ns is None else read_started_ns,
        "pods": statuses,
        "witness": {},
        "slots": [],
        "errors": {},
        "witness_transport": "observer_http",
        "witness_read_ns": 0,
    }
    try:
        requested = {}
        eligible = [(pod, s["uid"]) for pod, s in zip(pods, statuses, strict=True) if s["ready"]]
        for pod, uid in eligible:
            try:
                requested[uid] = str(ip_address(pod["status"].get("podIP")))
            except ValueError:
                record["errors"][uid] = "Ready worker has no valid pod IP"
        replies = {}
        if requested:
            start = time.monotonic_ns()
            try:
                result = driver.k(
                    "-n",
                    app,
                    "exec",
                    observer_pod,
                    "-c",
                    "observer",
                    "--",
                    "python",
                    "-c",
                    WORKER_WITNESS,
                    json.dumps(requested),
                    record=False,
                    timeout=5,
                )
                replies = json.loads(result.stdout)
                require(
                    isinstance(replies, dict) and replies.keys() == requested.keys(),
                    "observer returned an incomplete worker batch",
                )
            except (OSError, subprocess.SubprocessError, RuntimeError, ValueError) as error:
                replies = {uid: {"error": str(error)} for uid in requested}
            finally:
                record["witness_read_ns"] = time.monotonic_ns() - start
        for pod, uid in eligible:
            if uid not in replies:
                continue
            try:
                reply = replies[uid]
                require("error" not in reply, reply.get("error"))
                witness = reply["witness"]
                require(isinstance(witness, dict), "worker witness is not a JSON object")
                slots = ready_slots([pod], {uid: witness})
            except (KeyError, TypeError, ValueError, AttributeError) as error:
                record["errors"][uid] = str(error)
            else:
                record["slots"].extend(slots)
                record["witness"][uid] = witness
        record["slots"].sort()
        require(len(record["slots"]) == len(set(record["slots"])) <= 4, "duplicate slots or surge")
        record["read_finished_ns"] = time.time_ns()
        metric = {"ready_slots": len(record["slots"]), "ready_slots_sample_ns": sampled}
        code = (
            "import os,pathlib; p=pathlib.Path('/evidence/metrics.tmp'); "
            f"p.write_text({json.dumps(metric)!r}); os.replace(p,'/evidence/metrics.json')"
        )
        if receiver_pod is None:
            driver.python(receiver_namespace, receiver, code, record=False)
        else:
            driver.k(
                "-n",
                receiver_namespace,
                "exec",
                receiver_pod,
                "--",
                "python",
                "-c",
                code,
                record=False,
                timeout=5,
            )
    finally:
        if "read_finished_ns" not in record:
            record["read_finished_ns"] = time.time_ns()
        if arm is not None:
            record["phase"] = slot_sample_phase(
                arm, origin, record["read_started_ns"], record["read_finished_ns"]
            )
        with (driver.output / "ready-slots.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record) + "\n")
    return record
