from __future__ import annotations

import json
import sys
from copy import deepcopy
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from dur053_k6_b_evidence import SmokeFailure  # noqa: E402
from dur053_k6_g_bundle import make_plan, replay, select_cells, write_fence  # noqa: E402
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


def test_negative_replay_retains_pass_summary(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    source = "witness/topology.json"
    path = bundle / source
    path.parent.mkdir(parents=True)
    path.write_bytes(b"source")
    (bundle / "index.json").write_text(json.dumps({"files": [{"path": source}]}))
    (tmp_path / "producer-result.json").write_text('{"status":"PASS"}')
    baseline = json.dumps(
        {"findings": [], "labels": [{"reason": "predicate_partial:G2b"}] * 13}
    ).encode()

    def offline(root: Path) -> bytes:
        if root == bundle:
            return baseline
        assert not (root / source).exists()
        assert json.loads((root / "index.json").read_bytes())["files"] == []
        assert json.loads((root / "producer-result.json").read_bytes())["status"] == "PASS"
        return json.dumps(
            {
                "findings": [
                    {
                        "code": "required_proof_missing:" + source,
                        "source": source,
                        "predicate": "RelationIntegrity",
                        "witness": None,
                    }
                ]
            }
        ).encode()

    replay(tmp_path, baseline, source, offline)
