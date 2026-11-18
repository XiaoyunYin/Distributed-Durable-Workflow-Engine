"""Offline checks of Phase D's workload, deployment, evidence and admission reader."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dur053_k3  # noqa: E402
import dur053_k6_b_smoke as phase_b  # noqa: E402
import dur053_k6_c_smoke as phase_c  # noqa: E402
import dur053_k6_d_smoke as phase_d  # noqa: E402
from dur053_k6_admission import admit  # noqa: E402
from dur053_k6_b_evidence import SmokeFailure  # noqa: E402
from dur053_k6_c_render import render as c_topology  # noqa: E402
from dur053_k6_d_render import (  # noqa: E402
    ACTIVITY,
    CASES,
    compatibility_job,
    effect_topology,
    render,
)
from dur053_k6_join import identity, verify  # noqa: E402
from dur053_k6_overhead import STAGES, measurement, read_effect_overhead  # noqa: E402
from dur053_k6_registry import build_registry  # noqa: E402
from test_k6_phase_b import StubK3Driver  # noqa: E402
from test_k6_phase_c import ENDPOINTS, fixture  # noqa: E402

IMAGES = {
    kind: f"localhost:5001/d2a-{kind.replace('_', '-')}@sha256:{str(index) * 64}"
    for index, kind in enumerate(("runtime", "worker", "old_runtime", "old_worker"))
}


def test_sealed_mix_and_exact_compatibility_image_roles():
    sealed, blocks = phase_d.plan()
    for name in ("effect", "forward-control"):
        assert [item["initial_node_id"] for item in blocks[name]] == [
            *([ACTIVITY] * 8),
            "pure.echo",
            "pure.add",
        ]
        assert blocks[name][-1]["initial_input"] == [2, 3]
        assert all(row["initial_input"] == {} for row in blocks[name][:8])
    assert len(sealed["operations"]) == len({row["resource"] for row in sealed["operations"]}) == 16
    assert [row["initial_node_id"] for row in blocks["pure"]] == ["pure.echo"] * 9 + ["pure.add"]
    for name, (runtime, worker, runtime_gate, worker_gate, drift, mixed) in CASES.items():
        pod = compatibility_job(IMAGES, name)["spec"]["template"]["spec"]
        main, export = pod["containers"][0], pod["initContainers"][0]
        assert main["image"] == IMAGES[runtime] and export["image"] == IMAGES[worker]
        for gated, container in [(runtime_gate, main), (worker_gate, export)]:
            assert {"name": "DUR053_K6_ACTIVITY", "value": str(int(gated))} in container["env"]
        assert ("PURE_ACTIVITY" in export["command"][-1]) == drift
        assert ("--surviving-worker-registry" in main["command"]) == mixed
        if mixed:
            assert pod["initContainers"][1]["image"] == IMAGES["old_worker"]


def test_deployed_effect_authority_and_network_wiring():
    items = render(IMAGES, {f"192.0.2.{i}": f"172.16.0.{i}" for i in range(1, 5)}, "public", "hash")
    items += effect_topology(c_topology(phase_d.NS, IMAGES["runtime"], ENDPOINTS))
    by_name = {(item["kind"], item["metadata"]["name"]): item for item in items}
    assert not any(': "operator"' in json.dumps(obj) for obj in items)
    assert ("Pod", "k6-effect-worker") not in by_name
    assert ("Pod", "k6-engine-peer") not in by_name
    roles = {
        name: by_name["Deployment", name]["spec"]["template"]["spec"]
        for name in ("worker", "worker-control", "scheduler", "observer")
    }
    assert all(pod["automountServiceAccountToken"] is False for pod in roles.values())
    for pod in roles.values():
        present_volumes = {row["name"] for row in pod["volumes"]}
        for container in pod["containers"] + pod.get("initContainers", []):
            assert container["image"] in IMAGES.values()
            assert {row["name"] for row in container["volumeMounts"]} <= present_volumes
    assert roles["worker"]["serviceAccountName"] == "k6-worker"
    volumes = {row["name"]: row for row in roles["worker"]["volumes"]}
    assert {
        entry["serviceAccountToken"]["audience"]
        for entry in volumes["identity"]["projected"]["sources"]
    } == {
        "durable-worker-control",
        "dur053-k6-sink",
    }
    assert "allocator-source" not in volumes
    assert "k6-allocator" not in json.dumps(roles["scheduler"])
    assert (
        roles["worker-control"]["containers"][0]["readinessProbe"]["httpGet"]["scheme"] == "HTTPS"
    )
    assert by_name["NetworkPolicy", "sink-accept"]["spec"]["ingress"][0]["from"] == [
        {"podSelector": {"matchLabels": {"app": role}}}
        for role in ("k6-effect-relay", "worker-control")
    ]
    assert by_name["NetworkPolicy", "worker-effect"]["spec"]["egress"][0]["to"] == [
        {"podSelector": {"matchLabels": {"app": "k6-effect-relay"}}},
    ]


def test_joins_independent_worker_observation_and_refuses_forgery():
    records, snapshot, options = fixture()
    records = [*records[:5], {**records[-1], "sequence": 6, "high_water": 5}]
    snapshot["calls"] = snapshot["calls"][:1]
    original = options["worker_observations"][0]
    probe = {key: value for key, value in original.items() if key != "connection_hash"}
    probe.update({field: records[0][field] for field in ("request_id_hash", "invocation_hash")})
    observed = phase_d.worker_observations(records, [probe])
    assert observed[0]["connection_hash"] == original["connection_hash"]
    options["worker_observations"] = observed
    assert verify(records, snapshot, **options)["applications"] == 1
    with pytest.raises(SmokeFailure, match="D_WORKER_EXCHANGE_IDENTITY"):
        phase_d.worker_observations(records, [{**probe, "invocation_hash": identity("forged")}])
    with pytest.raises(SmokeFailure, match="D_WORKER_EXCHANGE_MISSING"):
        phase_d.worker_observations(records, [])


def test_observer_timing_collects_reads_and_witness_acks_by_independent_hash():
    sealed, _ = phase_d.plan()
    operations = sealed["operations"][:8]
    rows, calls, worker_log, grant_log, witness_log = [], [], [], [], []
    for op in operations:
        key = op["logical_key"]
        rows.append({"workflow_id": op["submission_key"], "stages_ns": {"approval": [99]}})
        for stage in ("claim", "apply"):
            worker_log.append(f"INFO k6_timing {identity(key)} {stage} 10\n")
        worker_log.append(f"INFO k6_timing {identity(key)} delay 5000000000\n")
        for operation in ("allocate", "apply"):
            body = identity(key + operation)
            request = key + operation
            calls.append(
                {
                    "operation": operation,
                    "logical_key": key,
                    "body_hash": body,
                    "request_id": request,
                }
            )
            for stage in ("read", "recheck"):
                grant_log.append(f"2026/11/18 k6_grant {body} {operation} {stage} 11\n")
        witness_log.extend(
            f"2026/11/18 k6_witness {identity(request)} {event} 12\n" for event in phase_d.EXCHANGE
        )
    observations = phase_d.collect(
        "".join(worker_log), "", "".join(grant_log), "".join(witness_log)
    )
    phase_d.add_timings(
        {"effect": rows},
        {op["submission_key"]: op for op in operations},
        {"calls": calls},
        observations,
    )
    assert rows[0]["stages_ns"] == {
        "approval": [99],
        "claim": [10],
        "grant_read": [11] * 4,
        "pre_call_delay": [5 * 10**9],
        "call": [10],
        "lookup": [],
        "witness": [12] * 5,
    }
    observations["witness_timings"].pop()
    with pytest.raises(SmokeFailure, match="D_TIMING_INVENTORY"):
        phase_d.add_timings(
            {"effect": rows},
            {op["submission_key"]: op for op in operations},
            {"calls": calls},
            observations,
        )


def test_admission_reads_verified_maximum_and_refuses_tampering(tmp_path):
    baseline = [
        {"submitted_ns": [100, 200], "terminal_ns": [1200, 1500], "stages_ns": {}}
        for _ in range(10)
    ]

    def block(delay):
        return [
            {
                **pure,
                "terminal_ns": [1200 + delay, 1500 + delay],
                **({"grant_ns": [150, 200]} if index < 8 else {}),
                "stages_ns": {stage: [] if stage == "lookup" else [100] for stage in STAGES}
                if index < 8
                else {},
            }
            for index, pure in enumerate(baseline)
        ]

    record = measurement(
        baseline, {"effect": block(5 * 10**9), "forward-control": block(6 * 10**9)}
    )
    path = tmp_path / "measurement.json"
    path.write_text(json.dumps(record))
    maximum = read_effect_overhead(path)
    assert maximum == pytest.approx((6 * 10**9 + 400) * 15 / 1e9)
    assert record["blocks"][1]["additional_activity_ns"] == [6 * 10**9 + 400] * 8
    assert record["sample_counts"]["lookup"] == 0
    assert record["p95_episode_equivalent_seconds"] == maximum
    inputs = {"ledger_actual": 0, "progress": {}}
    assert admit(build_registry(), **inputs, effect_overhead_record=path) == admit(
        build_registry(),
        **inputs,
        effect_overhead_seconds=maximum,
    )
    with pytest.raises(ValueError, match="k6-ambiguous-effect-overhead"):
        admit(build_registry(), **inputs, effect_overhead_record=path, effect_overhead_seconds=1)
    record["max_episode_equivalent_seconds"] = 0.1
    path.write_text(json.dumps(record))
    with pytest.raises(ValueError, match="k6-invalid-effect-overhead-record"):
        read_effect_overhead(path)


def test_phase_d_handoff_is_explicit_after_c_and_fail_isolated(tmp_path, monkeypatch):
    order = []
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.setenv("DUR053_K6_D_OUTPUT", str(tmp_path / "from-environment"))
    monkeypatch.setattr(dur053_k3, "matched_arms", lambda *_: None)
    monkeypatch.setattr(dur053_k3, "scratch_ownership", lambda *_: None)
    monkeypatch.setattr(dur053_k3, "artifact_hashes", lambda *_: {})
    monkeypatch.setattr(dur053_k3, "LIVE_RESERVATION", 0)

    def stub(output, scratch):
        driver = StubK3Driver(output, scratch)
        order.append(driver)
        return driver

    def accepted(output, *_):
        order.append(output)
        return 0

    def failed(output, *_):
        order.append(output)
        raise RuntimeError("synthetic D failure")

    monkeypatch.setattr(dur053_k3, "Driver", stub)
    monkeypatch.setattr(phase_b, "run", accepted)
    monkeypatch.setattr(phase_c, "run", accepted)
    monkeypatch.setattr(phase_d, "run", failed)
    k3, scratch, b, c, d = (tmp_path / name for name in ("k3", "scratch", "b", "c", "d"))
    assert dur053_k3.run("smoke", k3, scratch, b, c, d) == 1
    driver, *phases = order
    assert phases == [b, c, d]
    assert "DUR053_K6_D_OUTPUT" not in driver.unittest_env
    assert not (tmp_path / "from-environment").exists()
    assert json.loads((k3 / "manifest.json").read_bytes())["status"] == "PASS"
    report = json.loads((d / "smoke.json").read_bytes())
    assert (report["status"], report["reason"]) == ("FAIL", "UNEXPECTED_RUNTIMEERROR")
    assert (report["measurement_claim"], report["charged_seconds"]) == (False, 0)
