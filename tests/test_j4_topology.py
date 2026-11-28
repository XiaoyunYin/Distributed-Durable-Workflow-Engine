from __future__ import annotations

import json
from copy import deepcopy
from ipaddress import IPv4Address
from pathlib import Path

import pytest
from workers import recorded_probe

from deploy.j4 import qualification
from deploy.j4.overlay import Overlay, kind_config
from deploy.j4.qualification import (
    clock_proof,
    require_calico_policies,
    require_calico_ready,
    require_clock_continuity,
    require_probe_evidence,
)
from deploy.j4.relay_preflight import RelaySelection

PIN = "postgres@sha256:" + "e" * 64
KAFKA = "kafka@sha256:" + "f" * 64
ADDRESSES = (
    "192.168.0.2",
    "10.96.0.3",
    "10.96.0.4",
    "192.168.0.5",
    "192.168.0.6",
    "192.168.0.7",
)


def test_j4_runtime_switches_match_the_registered_profiles() -> None:
    root = Path(__file__).resolve().parents[1]
    registry = json.loads((root / "deploy/runtime-switches.json").read_bytes())
    switches = registry["switches"]
    activity = switches["J4_RECORDED_LLM_ACTIVITY"]
    resilience = switches["J4_PROVIDER_RESILIENCE"]
    protected = {"K5", "K6_before_Phase_G", "K6_Phase_G", "J5a", "J5b"}
    for entry in (activity, resilience):
        assert entry["unset_default"] == "0" and set(entry["values"]) == {"0", "1"}
        assert set(entry["required_unset_profiles"]) == protected
    assert activity["permitted_enabled_profiles"] == ["j4-core", "j4-b2"]
    assert activity["routes_added"] == activity["migrations_added"] == []
    assert resilience["permitted_enabled_profiles"] == ["j4-b2"]
    provider_route = "POST /v1/workflows/{workflowID}/nodes/{nodeID}/iterations/{iteration}"
    assert resilience["routes_added"] == [
        f"{provider_route}/provider-attempts/{action}" for action in ("reserve", "outcome")
    ]
    migration = "migrations/j4_b2/000025_j4_provider_resilience.up.sql"
    assert resilience["migrations_added"] == [migration] and (root / migration).is_file()
    inventories = registry["migration_inventories"]
    assert inventories["j4-core"] == {"base": "runtime_base", "additional": []}
    assert inventories["j4-b2"] == {"base": "runtime_base", "additional": [migration]}
    assert {"j4-core", "j4-b2"} <= set(
        switches["RUNTIME_TIMEOUT_RETRY_POLICY"]["required_unset_profiles"]
    )


def overlay(arm: str = "C2") -> Overlay:
    return Overlay(
        "j4-case",
        "run-c2",
        arm,
        "workflow",
        "j4-runtime:revision",
        "j4-worker:revision",
        RelaySelection("SYNTHETIC_RELAY", "1"),
    )


def test_kind_overlay_keeps_calico_and_infrastructure_pins(tmp_path: Path) -> None:
    kind = kind_config(tmp_path)
    assert kind["networking"]["disableDefaultCNI"] is True
    assert len(kind["nodes"]) == 1
    assert kind["nodes"][0]["extraMounts"] == [
        {"hostPath": str(tmp_path), "containerPath": "/j4-job"}
    ]
    case = overlay()
    infrastructure = case.service_pods(ADDRESSES[1], PIN, KAFKA)
    pods = {obj["metadata"]["name"]: obj["spec"] for obj in infrastructure if obj["kind"] == "Pod"}
    assert pods["postgres"]["containers"][0]["image"] == PIN
    assert pods["kafka"]["containers"][0]["image"] == KAFKA
    kafka_env = {entry["name"]: entry["value"] for entry in pods["kafka"]["containers"][0]["env"]}
    assert kafka_env["KAFKA_NUM_PARTITIONS"] == "1"
    assert kafka_env["KAFKA_DEFAULT_REPLICATION_FACTOR"] == "1"
    assert kafka_env["KAFKA_AUTO_CREATE_TOPICS_ENABLE"] == "false"
    assert pods["stub"]["volumes"][1]["hostPath"]["path"] == "/j4-job/run-c2/stub"
    assert all(
        not spec["hostNetwork"]
        and not spec["automountServiceAccountToken"]
        and not spec["enableServiceLinks"]
        for spec in pods.values()
    )


def test_worker_and_runtime_have_explicit_private_control_lanes() -> None:
    case = overlay()
    cfg, worker = case.worker(*ADDRESSES)
    assert cfg["immutable"]
    spec = worker["spec"]
    assert len(spec["containers"]) == 1
    app = spec["containers"][0]
    init = spec["initContainers"][0]
    assert init["securityContext"]["capabilities"] == {"drop": ["ALL"], "add": ["NET_ADMIN"]}
    assert app["securityContext"]["capabilities"] == {
        "drop": ["ALL"],
        "add": ["SETUID", "SETGID", "KILL"],
    }
    assert app["securityContext"]["allowPrivilegeEscalation"] is False
    assert next(v for v in app["volumeMounts"] if v["name"] == "egress")["readOnly"]
    assert all("secret" not in volume for volume in spec["volumes"])
    env = {entry["name"]: entry["value"] for entry in app["env"] if "value" in entry}
    assert env["CONTROL_API_URL"] == "http://10.96.0.4:8083"
    assert env["WORKER_SLOTS"] == "1"
    assert env["J4_PROVIDER_RESILIENCE"] == "0"
    runtime = case.runtime("10.96.0.5", ADDRESSES[1])["spec"]["containers"][0]
    assert "--j4-worker-addr=0.0.0.0:8083" in runtime["args"]
    assert runtime["args"][-2:] == ["--j4-c2-workflow", "workflow"]
    assert {"name": "SYNTHETIC_RELAY", "value": "1"} in runtime["env"]
    mutant = overlay("B5").runtime("10.96.0.5", ADDRESSES[1])["spec"]["containers"][0]
    assert "--j4-fixed-delay-control" in mutant["args"]
    assert {"name": "J4_PROVIDER_RESILIENCE", "value": "1"} in mutant["env"]


def test_calico_policy_refuses_wider_running_egress() -> None:
    case = overlay()
    live = case.network()
    require_calico_policies(case, live)
    worker = next(obj for obj in live if obj["metadata"]["name"] == "worker")
    assert {
        (r["to"][0]["podSelector"]["matchLabels"]["j4-role"], r["ports"][0]["port"])
        for r in worker["spec"]["egress"]
    } == {("runtime", 8083), ("kafka", 9092), ("stub", 8082)}
    worker["spec"]["egress"].append({})
    with pytest.raises(ValueError, match="Calico policy"):
        require_calico_policies(case, live)


def test_calico_must_be_pinned_and_ready_on_one_node() -> None:
    images = {
        "docker.io/calico/cni:v3": "cni@sha256:" + "1" * 64,
        "docker.io/calico/node:v3": "node@sha256:" + "2" * 64,
    }
    calico = {
        "metadata": {"name": "calico-node", "namespace": "kube-system", "generation": 3},
        "status": {
            "observedGeneration": 3,
            "numberReady": 1,
            "numberAvailable": 1,
            "desiredNumberScheduled": 1,
            "updatedNumberScheduled": 1,
        },
        "spec": {
            "template": {
                "spec": {
                    "initContainers": [{"image": images["docker.io/calico/cni:v3"]}],
                    "containers": [{"image": images["docker.io/calico/node:v3"]}],
                }
            }
        },
    }
    require_calico_ready(calico, images)
    calico["status"]["numberReady"] = 0
    with pytest.raises(ValueError, match="not ready"):
        require_calico_ready(calico, images)


def test_clock_proof_requires_three_shared_boot_brackets_and_continuity() -> None:
    def plan(boot: str, stamp: int):
        return {
            name: lambda: {"boot_id": boot, "monotonic_ns": stamp}
            for name in ("runtime", "worker", "stub")
        }

    before = clock_proof(plan("boot", 10), "boot", iter([0, 20] * 3).__next__)
    after = clock_proof(plan("boot", 30), "boot", iter([21, 40] * 3).__next__)
    require_clock_continuity(before, after)
    with pytest.raises(ValueError, match="changed"):
        require_clock_continuity(before, {**after, "boot_id": "reset"})
    with pytest.raises(ValueError, match="clock bracket"):
        clock_proof(plan("boot", 10), "boot", iter([0, 20_000_001] * 3).__next__)
    with pytest.raises(ValueError, match="boot identity"):
        clock_proof(plan("foreign", 10), "boot", iter([0, 20] * 3).__next__)
    with pytest.raises(ValueError, match="clock bracket"):
        clock_proof(plan("boot", 21), "boot", iter([0, 20] * 3).__next__)


def test_live_clock_paths_use_three_distinct_loopback_forwards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = []
    monkeypatch.setattr(qualification, "clock_sample", lambda: {"boot_id": "local"})

    def read(port, path):
        called.append((port, path))
        return {}

    def prove(probes, boot):
        assert boot == "local"
        for probe in probes.values():
            probe()
        return {"verified": True}

    monkeypatch.setattr(qualification, "read_clock", read)
    monkeypatch.setattr(qualification, "clock_proof", prove)
    assert qualification.live_clock_proof({"runtime": 4001, "worker": 4002, "stub": 4003}) == {
        "verified": True
    }
    assert called == [(4001, "/healthz"), (4002, "/healthz"), (4003, "/readyz")]
    with pytest.raises(ValueError, match="distinct"):
        qualification.live_clock_proof({"runtime": 4001, "worker": 4001, "stub": 4003})


def test_denial_requires_live_independent_decoys_and_zero_worker_receipts() -> None:
    boot = "boot"
    probe, worker_ipv4 = "192.168.0.9", "192.168.0.10"
    targets = set(recorded_probe.PORTS) | {"dns_udp"}
    worker = {
        "parent_denied": dict.fromkeys(
            ("stub", "wrong_port", "wrong_pod", "outside", "dns", "dns_udp"), True
        ),
        "caller": {
            "boot_id": boot,
            "stub": {"boot_id": boot},
            "denied": {name: True for name in targets - {"stub"}},
        },
    }
    reach = {"reachable": dict.fromkeys(targets, True), "boot_id": boot}

    def row(port, protocol):
        return {"source_ipv4": probe, "boot_id": boot, "port": port, "protocol": protocol}

    decoys = {
        "wrong_port": [row(8084, "TCP")],
        "wrong_pod": [row(8082, "TCP")],
        "outside": [row(8082, "TCP")],
        "dns": [row(53, "TCP"), row(53, "UDP")],
    }
    require_probe_evidence(worker, reach, decoys, probe, worker_ipv4)
    bad = deepcopy(decoys)
    bad["dns"][1]["source_ipv4"] = worker_ipv4
    with pytest.raises(ValueError, match="unexpected J4 decoy receipt"):
        require_probe_evidence(worker, reach, bad, probe, worker_ipv4)
    reach["reachable"]["outside"] = False
    with pytest.raises(ValueError, match="independent reachability"):
        require_probe_evidence(worker, reach, decoys, probe, worker_ipv4)


def test_probe_checks_are_bound_to_same_isolated_stub() -> None:
    stub = IPv4Address("192.168.0.2")
    plan = {
        "stub": [str(stub), 8082],
        "wrong_port": [str(stub), 8084],
        "wrong_pod": ["192.168.0.5", 8082],
        "outside": ["192.168.0.6", 8082],
        "broker": ["10.96.0.3", 9092],
        "api": ["10.96.0.4", 8083],
        "dns": ["192.168.0.7", 53],
    }
    assert recorded_probe.parse_targets(plan, stub)["dns"] == ("192.168.0.7", 53)
    with pytest.raises(ValueError, match="isolated|destination"):
        recorded_probe.parse_targets({**plan, "outside": ["8.8.8.8", 8082]}, stub)
    with pytest.raises(ValueError, match="disagrees"):
        recorded_probe.parse_targets({**plan, "wrong_port": ["192.168.0.8", 8084]}, stub)


@pytest.mark.parametrize("key", ["HTTP_PROXY", "OPENAI_API_KEY", "WORKER_TOKEN_FILE"])
def test_credential_inventory_rejects_unexpected_names_without_values(
    key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(recorded_probe.os, "environ", {key: "must-not-appear"})
    with pytest.raises(RuntimeError, match=key) as failure:
        recorded_probe.inventory("caller")
    assert "must-not-appear" not in str(failure.value)
