from __future__ import annotations

import json
import subprocess
import sys
from contextlib import nullcontext
from copy import deepcopy
from pathlib import Path
from unittest.mock import Mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from dur053_k6_b_evidence import SmokeFailure  # noqa: E402
from dur053_k6_chaos import Target  # noqa: E402
from dur053_k6_g_bundle import (  # noqa: E402
    CONTROL_PATH,
    IDLE_RELATIONS,
    STALE_PATH,
    Json,
    canonical,
    idle_sources,
    make_plan,
    replay,
    select_cells,
    sha,
    write_fence,
)
from dur053_k6_g_registration import AUDIENCE, bind_registration, observe_runtime  # noqa: E402
from dur053_k6_g_smoke import Episode  # noqa: E402
from dur053_k6_g_stale import COMPANION, StaleDriver  # noqa: E402
from dur053_k6_registry import read_registry  # noqa: E402


def test_stale_call_and_continuity_use_predecessor_after_peer_registration(tmp_path: Path) -> None:
    predecessor = Target(
        "ns",
        "old-pod",
        "old-uid",
        "scheduler",
        "scheduler",
        "containerd://old",
        0,
        "node",
        20,
        "1:123",
        "old-session",
    )
    old = {"container_id": "old"}
    peer = {"container_id": "new"}
    registration = {"uid": "new-uid"}
    episode = Mock()
    episode.wait_for.side_effect = lambda condition, *args, **kwargs: condition()
    driver = object.__new__(StaleDriver)
    driver.g = episode
    driver.episode, driver.workflow = "hook-5-0", "workflow"
    driver.partition = None
    driver.receiver = Mock()
    driver.path = tmp_path / "journal"
    driver.path.write_text('{"at_ns":1}\n', encoding="utf-8")
    driver.process = Mock(side_effect=[(predecessor, old), (None, peer), (predecessor, old)])
    driver.registered = Mock(return_value=registration)
    driver.state = Mock(
        side_effect=[
            {
                "ack": {},
                "reach": {"fields": {"invocation": "call-1", "objects": {"partition": "5"}}},
            },
            {"ack": {}},
            {"state": "done"},
        ]
    )
    driver.cut = Mock(side_effect=[{"epoch": 1}, {"epoch": 2}, {"epoch": 2}])
    driver.call = Mock(return_value={"outcome": "ErrLeaseNotOwned"})

    observation, _ = driver.finish()
    driver.receiver.arm.assert_called_once_with(
        "hook-5-0",
        COMPANION,
        registration,
        {"key": "workflow", "epoch": "2"},
        60,
        companion=True,
        share_primary_deadline=True,
    )
    driver.call.assert_called_once_with("old-pod", "call-1", "old")
    assert observation["before"] == observation["after"] == old


def test_stale_parity_uses_private_fault_sources(tmp_path: Path) -> None:
    episode = object.__new__(Episode)
    episode.cell = {"cell": "hook-5"}
    episode.scratch = tmp_path
    episode.frames = tmp_path / "frames"
    episode.evidence = Mock(root=tmp_path / "results")
    episode.evidence.step.side_effect = lambda _: nullcontext()
    source = episode.frames / STALE_PATH
    source.parent.mkdir(parents=True)
    original = canonical({"call": {}, "hold": {}, "journal": "original"})
    source.write_bytes(original)
    expected = {
        "Committed": "STALE_CHALLENGE_COMMITTED",
        "MissingACK": "required_proof_missing:stale_challenge_ack",
        "MissingBackend": "required_proof_missing:stale_challenge_backend",
        "MissingProgress": "required_proof_missing:stale_challenge_successor_progress",
        "MissingRejection": "required_proof_missing:stale_challenge_rejection",
        "N5Harness": "STALE_CHALLENGE_ORIGIN",
        "WrongOrder": "STALE_CHALLENGE_ORDER",
    }

    def invoke(args: tuple[str, ...], label: str, timeout: int) -> tuple[int, bytes]:
        _, name, mode = label.split("/")
        if mode == "live":
            assert args[:6] == ("sudo", "unshare", "--mount", "--propagation", "private", "--"), (
                "live fixtures must have private mounts"
            )
            assert Path(args[11]) == source
            staged = Path(args[10])
            assert staged == tmp_path / f"stale-proof-{name}.json"
            assert staged.read_bytes() != original
            assert timeout == 140
        else:
            assert args[:4] == ("sudo", "unshare", "--net", "--")
            assert timeout == 20
        assert source.read_bytes() == original
        code = expected[name]
        kind = "violation" if code.startswith("STALE_CHALLENGE_") else "missing"
        return 1, canonical(
            {"confirmation": {"reason": code}, "findings": [{"kind": kind, "code": code}]}
        )

    episode.stale_parity({"external_dir": str(episode.frames)}, "checker", invoke)
    assert episode.evidence.step.call_count == len(expected)
    assert source.read_bytes() == original


def runtime_inputs() -> tuple[Json, Json, Json]:
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


@pytest.mark.parametrize("sidecar", [None, "regular", "restartable-init", "ephemeral"])
def test_control_snapshot_reads_apiserver_lists(
    monkeypatch: pytest.MonkeyPatch, sidecar: str | None
) -> None:
    episode = object.__new__(Episode)
    episode.namespace = "g-arm"
    pod, _, _ = runtime_inputs()
    pod["metadata"]["labels"] = {"app": "worker"}
    pod["status"]["phase"] = "Running"
    pod["spec"] = {
        "containers": [{"name": "worker", "env": [{"name": "POD_UID"}]}],
        "initContainers": [{"name": "setup", "env": [{"name": "DUR053_K6_CONTROL_CA_FILE"}]}],
    }
    if sidecar == "regular":
        pod["spec"]["containers"].append({"name": "sidecar"})
    elif sidecar == "restartable-init":
        pod["spec"]["initContainers"].append({"name": "sidecar", "restartPolicy": "Always"})
    elif sidecar == "ephemeral":
        pod["spec"]["ephemeralContainers"] = [{"name": "sidecar"}]
    lists = {
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions": {
            "metadata": {"resourceVersion": "17"},
            "items": [],
        },
        "/api/v1/namespaces/g-arm/pods": {
            "metadata": {"resourceVersion": "19"},
            "items": [pod],
        },
    }

    def kubernetes(stage: str, *args: str) -> bytes:
        assert args[:2] == ("get", "--raw"), "control snapshot must retain raw list metadata"
        return canonical(lists[args[2]])

    monkeypatch.setattr(episode, "k", kubernetes)
    if sidecar is not None:
        with pytest.raises(SmokeFailure, match="G_CONTROL_CONTAINER_COUNT"):
            episode.control_snapshot()
        return
    snapshot = episode.control_snapshot()
    assert snapshot["crd_resource_version"] == "17"
    assert snapshot["pod_resource_version"] == "19"
    assert snapshot["pods"] == [
        {
            "role": "worker",
            "runtime": {
                "pod_uid": "pod-uid",
                "container_id": "old",
                "restart": 0,
                "started_utc": "2026-11-22T08:00:01.000000000Z",
            },
            "env_names": ["DUR053_K6_CONTROL_CA_FILE", "POD_UID"],
            "env_from": False,
        }
    ]


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


def test_registered_smoke_selects_control_for_each_workload() -> None:
    pure, effect = select_cells(read_registry())
    assert [pure["episode"], effect["episode"]] == ["medium-16-0", "medium-16-3"]
    binding = {"episode": effect["episode"], "namespace": "g-arm", "reset_id": "reset"}
    plan, requests, allocator = make_plan(binding, effect["seed"], True)
    assert [row["name"] for row in plan["windows"]] == ["warmup", "measured"]
    assert set(requests) == {row["submission_key"] for row in allocator["operations"]}
    for window, operation in zip(plan["windows"], allocator["operations"], strict=True):
        entry = window["submissions"][0]
        assert entry["target"] == operation["resource"]
        assert entry["effect"] == {key: operation[key] for key in entry["effect"]}


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
@pytest.mark.parametrize(("effect", "idle"), [(False, False), (False, True), (True, False)])
def test_negative_replay_retains_pass_summary(
    tmp_path: Path,
    exit_code: int,
    completeness: str,
    extra_finding: bool,
    producer_status: str,
    error: str | None,
    effect: bool,
    idle: bool,
) -> None:
    bundle = tmp_path / "bundle"
    source = "witness/effect-ingress.jsonl" if effect else "witness/topology.json"
    path = bundle / source
    path.parent.mkdir(parents=True)
    path.write_bytes(b"source")
    control = bundle / CONTROL_PATH
    control.parent.mkdir()
    control.write_bytes(b"confirmation")
    (bundle / "index.json").write_bytes(
        canonical({"files": [{"path": source}, {"path": CONTROL_PATH}]})
    )
    (bundle / "manifest.json").write_bytes(canonical({"cell": "medium-16"}))
    producer = json.dumps({"status": producer_status}).encode()
    (tmp_path / "producer-result.json").write_bytes(producer)
    ids = [f"I{i}" for i in range(1, 14)]
    completed = {"I1", "I2", "I4", "I6", "I11"}
    if effect:
        completed.update(("I8", "I10"))
    registered = completed
    covered = [] if idle else [identity for identity in ids if identity in completed]

    def reason(identity: str) -> str:
        if identity in completed:
            return "no_positive_population" if idle else "predicate_satisfied"
        return "outside_registry_ceiling"

    baseline = json.dumps(
        {
            "findings": [],
            "confirmation": (
                {"status": "pending", "reason": "no_positive_population"}
                if idle
                else {"status": "confirmed", "reason": "cell_predicate_satisfied"}
            ),
            "safety": "incomplete" if idle else "PASS",
            "completeness": "incomplete" if idle else "PASS",
            "relation_integrity": "covered/pass",
            "status": "incomplete" if idle else "passed",
            "labels": [
                {
                    "id": identity,
                    "registered": identity in registered,
                    "outcome": "covered/pass" if identity in covered else "NOT COVERED",
                    "reason": reason(identity),
                }
                for identity in ids
            ],
            "invariants_covered": covered,
            "invariants_not_covered": [identity for identity in ids if identity not in covered],
        }
    ).encode()

    replays: list[str] = []

    def offline(root: Path, name: str) -> tuple[int, bytes]:
        replays.append(name)
        if name == "offline":
            assert root == bundle
            return (3 if idle else 0), baseline
        assert name in {"missing", "missing-confirmation"}
        dropped, retained = (source, CONTROL_PATH) if name == "missing" else (CONTROL_PATH, source)
        assert not (root / dropped).exists()
        assert json.loads((root / "index.json").read_bytes())["files"] == [{"path": retained}]
        assert (root / "producer-result.json").read_bytes() == producer
        finding = {
            "code": "required_proof_missing:" + dropped,
            "kind": "missing",
            "source": dropped,
            "predicate": "RelationIntegrity",
            "witness": None,
        }
        return (exit_code if name == "missing" else 1), json.dumps(
            {
                "findings": [finding] * (2 if extra_finding else 1),
                "completeness": completeness,
            }
        ).encode()

    if error is None:
        replay(tmp_path, baseline, source, offline, idle=idle)
        expected = ["offline", "missing"]
        if not effect and not idle:
            expected.append("missing-confirmation")
        assert replays == expected
    else:
        with pytest.raises(SmokeFailure, match=error):
            replay(tmp_path, baseline, source, offline, idle=idle)
