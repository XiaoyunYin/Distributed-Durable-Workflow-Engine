"""Linux process, clock and socket probes for the synthetic J4 topology."""

from __future__ import annotations

import http.client
import json
import os
import socket
import sys
import time
from ipaddress import IPv4Address
from pathlib import Path

from workers.recorded_wire import MODE, STUB_PORT, strict_json

CASE_ROOT = Path("/etc/j4")
BOOT_ID = Path("/proc/sys/kernel/random/boot_id")
DNS_PROBE = b"j4-dns-decoy"
PORTS = {
    "stub": STUB_PORT,
    "wrong_port": 8084,
    "wrong_pod": 8082,
    "outside": 8082,
    "broker": 9092,
    "api": 8083,
    "dns": 53,
}
BASE_ENV = {
    "PATH",
    "HOSTNAME",
    "LANG",
    "HOME",
    "TERM",
    "GPG_KEY",
    "PYTHON_VERSION",
    "PYTHON_SHA256",
    "PYTHON_PIP_VERSION",
    "PYTHON_SETUPTOOLS_VERSION",
    "PYTHON_GET_PIP_URL",
    "PYTHON_GET_PIP_SHA256",
    "PYTHONPATH",
    "PYTHONDONTWRITEBYTECODE",
    "PYTHONUNBUFFERED",
    "KUBERNETES_SERVICE_HOST",
    "KUBERNETES_SERVICE_PORT",
    "KUBERNETES_SERVICE_PORT_HTTPS",
    "KUBERNETES_PORT",
    "KUBERNETES_PORT_443_TCP",
    "KUBERNETES_PORT_443_TCP_ADDR",
    "KUBERNETES_PORT_443_TCP_PORT",
    "KUBERNETES_PORT_443_TCP_PROTO",
}
PARENT_ENV = {
    "KAFKA_BOOTSTRAP_SERVERS",
    "CONTROL_API_URL",
    "WORKER_SLOTS",
    "WORKER_DRAIN_MODE",
    "POD_UID",
    "POD_NAME",
    MODE,
    "J4_PROVIDER_RESILIENCE",
}
BASE_MOUNTS = {
    "/",
    "/proc",
    "/dev",
    "/dev/pts",
    "/dev/mqueue",
    "/dev/shm",
    "/sys",
    "/sys/fs/cgroup",
    "/etc/hostname",
    "/etc/hosts",
    "/etc/resolv.conf",
    "/dev/termination-log",
    "/proc/bus",
    "/proc/fs",
    "/proc/irq",
    "/proc/sys",
    "/proc/sysrq-trigger",
    "/proc/acpi",
    "/proc/interrupts",
    "/proc/kcore",
    "/proc/keys",
    "/proc/latency_stats",
    "/proc/timer_list",
    "/proc/scsi",
    "/sys/firmware",
    "/sys/devices/virtual/powercap",
}


def clock_sample() -> dict[str, str | int]:
    boot = BOOT_ID.read_text().strip()
    if not boot:
        raise ValueError("missing J4 host boot identity")
    return {"boot_id": boot, "monotonic_ns": time.monotonic_ns()}


def inventory(role: str) -> dict[str, object]:
    # Record names, mount paths and types only. The single optional GPG_KEY
    # variable identifies the pinned image's public signing key.
    approved = {
        "worker": BASE_ENV | PARENT_ENV,
        "caller": {"PYTHONPATH"},
        "stub": BASE_ENV,
    }[role]
    names = set(os.environ)
    if names - approved:
        raise RuntimeError(f"unexpected J4 {role} environment names: {sorted(names - approved)}")
    if sys.platform != "linux":
        raise RuntimeError("J4 credential inventory requires Linux")
    mounts = {row.split(" ")[4] for row in Path("/proc/self/mountinfo").read_text().splitlines()}
    scope = BASE_MOUNTS | {"/etc/j4", "/evidence"}
    if role in {"worker", "caller"}:
        scope.add("/run/j4-egress")
    if mounts - scope:
        raise RuntimeError(f"unexpected J4 {role} mount paths: {sorted(mounts - scope)}")
    if role == "caller" and os.access("/proc/1/environ", os.R_OK):
        raise RuntimeError("provider caller can read the worker's process environment")
    return {
        "role": role,
        "uid": os.getuid(),
        "env_names": sorted(names),
        "mounts": sorted(mounts),
        "provider_credentials": [],
        "engine_credentials": [{"name": "claim_token", "type": "private control response"}]
        if role == "worker"
        else [],
    }


def probe_targets(stub: IPv4Address) -> dict[str, tuple[str, int]]:
    return parse_targets(strict_json((CASE_ROOT / "probes.json").read_bytes()), stub)


def parse_targets(value: object, stub: IPv4Address) -> dict[str, tuple[str, int]]:
    if not isinstance(value, dict) or value.keys() != PORTS.keys():
        raise ValueError("incomplete J4 probe targets")
    targets = {}
    for name, expected_port in PORTS.items():
        host, port = value[name]
        addr = IPv4Address(host)
        if (
            not addr.is_private
            or addr.is_loopback
            or addr.is_unspecified
            or addr.is_link_local
            or addr.is_multicast
            or type(port) is not int
            or port != expected_port
        ):
            raise ValueError("invalid J4 probe destination")
        targets[name] = str(addr), port
    if targets["stub"][0] != str(stub) or targets["wrong_port"][0] != str(stub):
        raise ValueError("J4 probe target disagrees with installed policy")
    distinct = {address for name, (address, _) in targets.items() if name != "wrong_port"}
    if len(distinct) != len(targets) - 1:
        raise ValueError("J4 deny control addresses must be distinct")
    return targets


def connects(target: tuple[str, int]) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.25)
            sock.connect(target)
    except OSError:
        return False
    return True


def udp_reachable(target: tuple[str, int]) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(0.25)
            sock.connect(target)
            sock.send(DNS_PROBE)
            return sock.recv(64) == DNS_PROBE
    except OSError:
        return False


def stub_ready(target: tuple[str, int]) -> dict[str, str | int]:
    api = http.client.HTTPConnection(*target, timeout=0.5)
    try:
        api.request("GET", "/readyz", headers={"Connection": "close"})
        reply = api.getresponse()
        body: object = json.loads(reply.read(1024))
        if reply.status != 200 or not isinstance(body, dict):
            raise ValueError("stub positive probe did not return its clock")
        boot_id, monotonic_ns = body.get("boot_id"), body.get("monotonic_ns")
        if not isinstance(boot_id, str) or type(monotonic_ns) is not int:
            raise ValueError("stub positive probe did not return its clock")
        return {"boot_id": boot_id, "monotonic_ns": monotonic_ns}
    finally:
        api.close()


def caller_probe(stub: IPv4Address) -> dict[str, object]:
    targets = probe_targets(stub)
    credential_inventory = inventory("caller")
    positive = stub_ready(targets["stub"])
    denied = {name: not connects(target) for name, target in targets.items() if name != "stub"}
    denied["dns_udp"] = not udp_reachable(targets["dns"])
    if not all(denied.values()):
        raise RuntimeError(f"provider caller reached forbidden J4 targets: {denied}")
    return {
        "stub": positive,
        "denied": denied,
        "inventory": credential_inventory,
        "targets": targets,
        **clock_sample(),
    }
