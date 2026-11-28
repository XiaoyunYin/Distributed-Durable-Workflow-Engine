"""Verify live topology evidence before a J4 episode can submit work."""

from __future__ import annotations

import http.client
import json
import time
from collections.abc import Callable
from functools import partial
from typing import Any

from workers.recorded_probe import PORTS, clock_sample

from deploy.j4.overlay import Overlay, local_ipv4

MAX_CLOCK_ROUND_TRIP_NS = 20_000_000
CLOCK_ENDPOINTS = {"runtime", "worker", "stub"}


def read_clock(port: int, path: str) -> dict:
    # The controller uses already-open, loopback-only kubectl forwards.
    # HTTPConnection does not take proxy settings from the job environment.
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=0.2)
    try:
        connection.request("GET", path, headers={"Connection": "close"})
        reply = connection.getresponse()
        payload = json.loads(reply.read(4096))
        if reply.status != 200 or not isinstance(payload, dict):
            raise ValueError("invalid J4 process clock response")
        return payload
    finally:
        connection.close()


def live_clock_proof(forwarded_ports: dict[str, int]) -> dict:
    if (
        set(forwarded_ports) != CLOCK_ENDPOINTS
        or any(type(port) is not int or not 1 <= port <= 65535 for port in forwarded_ports.values())
        or len(set(forwarded_ports.values())) != len(CLOCK_ENDPOINTS)
    ):
        raise ValueError("expected three distinct live J4 clock forwards")
    probes = {
        name: partial(read_clock, port, "/readyz" if name == "stub" else "/healthz")
        for name, port in forwarded_ports.items()
    }
    return clock_proof(probes, str(clock_sample()["boot_id"]))


def clock_proof(
    probes: dict[str, Callable[[], dict]],
    boot_id: str,
    monotonic_ns: Callable[[], int] = time.monotonic_ns,
) -> dict:
    if set(probes) != CLOCK_ENDPOINTS or not boot_id:
        raise ValueError("incomplete J4 clock endpoints or host boot identity")
    samples = {}
    for name, probe in probes.items():
        before = monotonic_ns()
        observed = probe()
        after = monotonic_ns()
        stamp = observed.get("monotonic_ns")
        if (
            observed.get("boot_id") != boot_id
            or type(stamp) is not int
            or not before <= stamp <= after
            or after - before > MAX_CLOCK_ROUND_TRIP_NS
        ):
            raise ValueError(f"unverified J4 {name} boot identity or clock bracket")
        samples[name] = {"before_ns": before, "after_ns": after, "remote_ns": stamp}
    return {"boot_id": boot_id, "samples": samples}


def require_clock_continuity(start: dict, end: dict) -> None:
    if (
        start["boot_id"] != end["boot_id"]
        or set(start["samples"]) != CLOCK_ENDPOINTS
        or (set(end["samples"]) != CLOCK_ENDPOINTS)
    ):
        raise ValueError("J4 clock proof changed across the episode")
    if any(
        end["samples"][name]["before_ns"] < start["samples"][name]["after_ns"]
        for name in CLOCK_ENDPOINTS
    ):
        raise ValueError("J4 end clock proof predates its start")


def require_calico_policies(overlay: Overlay, live: list[dict]) -> None:
    expected = {obj["metadata"]["name"]: obj["spec"] for obj in overlay.network()}
    observed = {}
    for obj in live:
        name = obj["metadata"]["name"]
        if obj["metadata"]["namespace"] != overlay.namespace or name in observed:
            raise ValueError("duplicate or foreign J4 network policy")
        spec = obj["spec"].copy()
        # Kubernetes may omit empty rule lists on a readback.
        for direction in ("ingress", "egress"):
            if spec.get(direction) is None:
                spec[direction] = []
        observed[name] = spec
    if observed != expected:
        raise ValueError("running J4 Calico policy differs from the frozen topology")


def require_calico_ready(daemonset: dict, pins: dict[str, str]) -> None:
    if (
        daemonset["metadata"]["name"] != "calico-node"
        or daemonset["metadata"]["namespace"] != "kube-system"
    ):
        raise ValueError("missing the J4 Calico node daemonset")
    status = daemonset["status"]
    if status.get("observedGeneration") != daemonset["metadata"]["generation"] or any(
        status.get(field) != 1
        for field in (
            "desiredNumberScheduled",
            "numberReady",
            "updatedNumberScheduled",
            "numberAvailable",
        )
    ):
        raise ValueError("Calico is not ready on the single J4 node")
    template = daemonset["spec"]["template"]["spec"]
    images = {entry["image"] for key in ("initContainers", "containers") for entry in template[key]}
    calico = {
        digest
        for name, digest in pins.items()
        if name.startswith(("docker.io/calico/node:", "docker.io/calico/cni:"))
    }
    if len(calico) != 2 or images != calico:
        raise ValueError("running J4 Calico daemonset differs from the locked images")


def require_probe_evidence(
    worker: dict[str, Any],
    reachability: dict,
    decoy_rows: dict[str, list[dict]],
    probe_ipv4: str,
    worker_ipv4: str,
) -> None:
    probe_ipv4, worker_ipv4 = map(local_ipv4, (probe_ipv4, worker_ipv4))
    caller = worker["caller"]
    boot = caller["boot_id"]
    expected_denials = (set(PORTS) - {"stub"}) | {"dns_udp"}
    parent = {
        name: True for name in ("stub", "wrong_port", "wrong_pod", "outside", "dns", "dns_udp")
    }
    if worker["parent_denied"] != parent or caller["denied"] != {
        name: True for name in expected_denials
    }:
        raise ValueError("effective J4 provider egress did not deny every control")
    if (
        reachability["boot_id"] != boot
        or caller["stub"]["boot_id"] != boot
        or reachability["reachable"] != {name: True for name in (*PORTS, "dns_udp")}
    ):
        raise ValueError("J4 deny controls lack a live, independent reachability proof")
    if probe_ipv4 == worker_ipv4:
        raise ValueError("J4 independent probe shares the worker identity")
    expected_decoys = {
        "wrong_port": (8084, {"TCP"}),
        "wrong_pod": (8082, {"TCP"}),
        "outside": (8082, {"TCP"}),
        "dns": (53, {"TCP", "UDP"}),
    }
    if set(decoy_rows) != set(expected_decoys):
        raise ValueError("incomplete J4 decoy journals")
    for name, (port, protocols) in expected_decoys.items():
        witnessed = set()
        for row in decoy_rows[name]:
            if row["boot_id"] != boot or row["port"] != port or row["source_ipv4"] != probe_ipv4:
                raise ValueError(f"unexpected J4 decoy receipt at {name}")
            witnessed.add(row["protocol"])
        if witnessed != protocols:
            raise ValueError(f"missing independent J4 decoy receipt at {name}")
