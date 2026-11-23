from __future__ import annotations

import json
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from dur053_k6_b_evidence import SmokeFailure  # noqa: E402
from dur053_k6_g_bundle import (  # noqa: E402
    IDLE_RELATIONS,
    canonical,
    idle_sources,
    make_plan,
    replay,
    select_cells,
    sha,
    write_fence,
)
from dur053_k6_g_registration import AUDIENCE, bind_registration, observe_runtime  # noqa: E402
from dur053_k6_registry import read_registry  # noqa: E402


def runtime_inputs() -> tuple[dict, dict, dict]:
    pod = {
        "metadata": {
            "namespace": "g-arm",
            "uid": "pod-uid",
            "name": "worker-0",
            "resourceVersion": "7",
        },
        "status": {
            "containerStatuses": [
                {
                    "name": "worker",
                    "containerID": "containerd://old",
                    "restartCount": 0,
                    "state": {"running": {"startedAt": "2026-11-22T08:00:01Z"}},
                }
            ]
        },
    }
    before = observe_runtime(pod)
    review = {
        "status": {
            "authenticated": True,
            "audiences": [AUDIENCE],
            "user": {
                "username": "system:serviceaccount:g-arm:k6-worker",
                "uid": "sa-uid",
                "extra": {
                    "authentication.kubernetes.io/pod-name": ["worker-0"],
                    "authentication.kubernetes.io/pod-uid": ["pod-uid"],
                },
            },
        }
    }
    session = {
        **before["identity"],
        "claimant": "system:serviceaccount:g-arm:k6-worker:pod:worker-0:uid:pod-uid",
        "id": "first",
    }
    return pod, review, session


def test_one_apiserver_observation_binds_start_and_container() -> None:
    pod, review, session = runtime_inputs()
    start = observe_runtime(pod)
    assert start["identity"]["container_id"] == "old"
    assert start["identity"]["container_started_utc"] == "2026-11-22T08:00:01.000000000Z"
    status = pod["status"]["containerStatuses"][0]
    status.update(
        containerID="containerd://new",
        restartCount=1,
        state={"running": {"startedAt": "2026-11-22T08:00:31Z"}},
    )
    new = observe_runtime(pod)
    restarted = {**session, **new["identity"], "id": "second"}
    assert bind_registration(new, new, review, "sa-uid", restarted)["session"] == restarted
    for field in ("container_id", "container_started_utc"):
        spliced = {**restarted, field: start["identity"][field]}
        with pytest.raises(SmokeFailure, match="G_IDENTITY_OBSERVATION_BINDING"):
            bind_registration(new, new, review, "sa-uid", spliced)
    with pytest.raises(SmokeFailure, match="G_IDENTITY_OBSERVATION_BINDING"):
        bind_registration(start, new, review, "sa-uid", session)


def test_registration_intake_rejects_another_pod() -> None:
    pod, review, session = runtime_inputs()
    observation = observe_runtime(pod)
    assert (
        bind_registration(observation, observation, review, "sa-uid", session)["identity"][
            "audience"
        ]
        == AUDIENCE
    )
    incorrect = deepcopy(review)
    incorrect["status"]["user"]["extra"]["authentication.kubernetes.io/pod-uid"] = ["other-pod"]
    with pytest.raises(SmokeFailure, match="G_REGISTRATION_INTAKE_BINDING"):
        bind_registration(observation, observation, incorrect, "sa-uid", session)


def test_fence_requires_all_admission_stops(tmp_path: Path) -> None:
    binding = {
        "episode": "hook-0-0",
        "reset_id": "reset",
        "namespace": "g",
        "barrier_id": "barrier",
    }
    stops = dict.fromkeys(("worker", "relay", "decision", "allocator"), "2026-11-22T08:00:00Z")
    fence = tmp_path / "fence.json"
    for missing in stops:
        with pytest.raises(SmokeFailure, match="G_FENCE_BEFORE_DRAIN"):
            write_fence(
                fence, binding, {key: value for key, value in stops.items() if key != missing}
            )
        assert not fence.exists()
    write_fence(fence, binding, stops)
    assert json.loads(fence.read_bytes()) == binding


def test_registered_smoke_selects_lowest_cost_per_arm() -> None:
    pure, effect = select_cells(read_registry())
    assert [pure["episode"], effect["episode"]] == ["hook-0-0", "medium-12-0"]
    binding = {"episode": effect["episode"], "namespace": "g-arm", "reset_id": "reset"}
    plan, requests, allocator = make_plan(binding, effect["seed"], True)
    assert [row["name"] for row in plan["windows"]] == ["warmup", "measured"]
    assert set(requests) == {row["submission_key"] for row in allocator["operations"]}


def test_checker_smoke_build_tag_is_confined_to_g_smoke() -> None:
    tag = "k6g2c_smoke"
    matches = subprocess.check_output(
        ["git", "grep", "-n", "-E", "-e", rf"-tags[=[:space:]'\",]+[^#]*{tag}", "--"],
        cwd=Path(__file__).resolve().parents[1],
        text=True,
    ).splitlines()
    assert len(matches) == 1, matches
    path, _, line = matches[0].split(":", 2)
    assert path == "scripts/dur053_k6_g_smoke.py"
    assert line.strip() == f'("checker-smoke", "dur049-checker", ["-tags={tag}"]),'


def test_idle_sources_must_be_present_and_sealed(tmp_path: Path) -> None:
    plan, requests, _ = make_plan(
        {"namespace": "g", "episode": "hook-0-0", "reset_id": "reset"}, 530001, False, idle=True
    )
    assert not requests
    (tmp_path / "plan.json").write_bytes(canonical(plan))
    (tmp_path / "arrivals.json").write_bytes(canonical({"rows": []}))
    (tmp_path / "engine").mkdir()
    (tmp_path / "broker").mkdir()
    (tmp_path / "witness").mkdir()
    descriptors = []
    index = []
    for name in IDLE_RELATIONS:
        relation = f"engine.{name}"
        path = f"engine/{relation}.jsonl"
        empty = {"length": 0, "sha256": sha(b"")}
        (tmp_path / path).write_bytes(b"")
        descriptors.append({**empty, "relation": relation, "count": 0})
        index.append({**empty, "path": path})
    (tmp_path / "engine/snapshot.json").write_bytes(canonical({"relations": descriptors}))
    worker = "witness/a.jsonl"
    worker_raw = (
        canonical({}) + canonical({"data": {"kind": "session"}}) + canonical({"mode": "terminal"})
    )
    (tmp_path / worker).write_bytes(worker_raw)
    index.append({"path": worker, "length": len(worker_raw), "sha256": sha(worker_raw)})
    (tmp_path / "index.json").write_bytes(canonical({"files": index}))
    broker = tmp_path / "broker/positions.jsonl"
    prefix = canonical({}) + canonical(
        {"data": {"partitions": [{"low": 0, "end": 0, "committed": None}]}}
    )
    broker.write_bytes(prefix + canonical({"mode": "terminal"}))
    report = idle_sources(tmp_path, ["a"])
    assert len(report["empty_relations"]) == len(IDLE_RELATIONS)
    assert report["worker_ledgers"][worker]["traffic_rows"] == 0
    broker.write_bytes(prefix + canonical({"mode": "checkpoint"}))
    with pytest.raises(SmokeFailure, match="G_IDLE_BROKER_TRAFFIC_OR_UNSEALED"):
        idle_sources(tmp_path, ["a"])
    (tmp_path / "index.json").write_bytes(canonical({"files": index[1:]}))
    with pytest.raises(SmokeFailure, match="G_IDLE_SOURCE_ABSENT"):
        idle_sources(tmp_path, ["a"])


@pytest.mark.parametrize(
    ("exit_code", "completeness", "extra_finding", "producer_status", "error"),
    [
        (1, "FAIL", False, "PASS", None),
        (3, "FAIL", False, "PASS", "G_MISSING_EXIT_3"),
        (1, "incomplete", False, "PASS", "G_MISSING_SOURCE_COMPLETENESS"),
        (1, "FAIL", True, "PASS", "G_EXACT_MISSING_SOURCE_REASON"),
        (1, "FAIL", False, "FAIL", "G_PRODUCER_PASS_RETAINED"),
    ],
)
def test_negative_replay_retains_pass_summary(
    tmp_path: Path,
    exit_code: int,
    completeness: str,
    extra_finding: bool,
    producer_status: str,
    error: str | None,
) -> None:
    bundle = tmp_path / "bundle"
    source = "witness/topology.json"
    path = bundle / source
    path.parent.mkdir(parents=True)
    path.write_bytes(b"source")
    (bundle / "index.json").write_text(json.dumps({"files": [{"path": source}]}))
    producer = json.dumps({"status": producer_status}).encode()
    (tmp_path / "producer-result.json").write_bytes(producer)
    baseline = json.dumps(
        {"findings": [], "labels": [{"reason": "predicate_partial:G2b"}] * 13}
    ).encode()

    def offline(root: Path, name: str) -> tuple[int, bytes]:
        if name == "offline":
            assert root == bundle
            return 3, baseline
        assert name == "missing"
        assert not (root / source).exists()
        assert json.loads((root / "index.json").read_bytes())["files"] == []
        assert (root / "producer-result.json").read_bytes() == producer
        finding = {
            "code": "required_proof_missing:" + source,
            "kind": "missing",
            "source": source,
            "predicate": "RelationIntegrity",
            "witness": None,
        }
        return exit_code, json.dumps(
            {"findings": [finding] * (2 if extra_finding else 1), "completeness": completeness}
        ).encode()

    if error is None:
        replay(tmp_path, baseline, source, offline)
    else:
        with pytest.raises(SmokeFailure, match=error):
            replay(tmp_path, baseline, source, offline)
