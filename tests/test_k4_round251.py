"""Round 251 rulings: issued outcomes, independent retries and idle generations."""

import copy
import hashlib
import json
import sys
import threading
from concurrent.futures import Future
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dur053_k4_driver as driver  # noqa: E402
import dur053_k4_evidence as evidence  # noqa: E402
import dur053_k4_interventions as interventions  # noqa: E402
from dur053_k3_policy import require  # noqa: E402
from test_k4_driver import pod  # noqa: E402
from test_k4_offline import checker as checker_fixture  # noqa: E402
from test_k4_offline import episode_fixture, write  # noqa: E402
from test_k4_offline import freeze as freeze_fixture  # noqa: E402

checker = checker_fixture
freeze = freeze_fixture


@pytest.mark.parametrize("mode", ["no-overlap", "outcome", "safety"])
def test_issued_outcome_classification(tmp_path, monkeypatch, mode):
    d = driver.K4Driver.__new__(driver.K4Driver)
    d.output, d.engineering = tmp_path, False
    d.images = {"a": dict(runtime="runtime-image", worker="worker-image")}
    d.proof, d.source, d.namespaces = {}, "a" * 40, {"ns"}
    d.setup = Mock(return_value=("ns", "db"))
    d.start_samples, d.stop_samples, d.check_observation = Mock(), Mock(), Mock()
    d.hold_state = Mock(
        return_value=dict(
            host_start=0, host_end=0, observer=0, path="hold", existed=False, exists=True, mtime=0
        )
    )
    d.overlap = Mock(return_value=[] if mode == "no-overlap" else [dict(role="worker")])
    d.load = Mock(return_value=Mock(returncode=0))
    d.base, d.sampler, d.collector = Mock(), Mock(), Mock()
    d.observer_stop = threading.Event()
    d.snapshot = Mock(return_value=dict(workflows=[]))
    d.witness_raw = Mock(return_value={})
    d.arrival_journal, d.assemble = Mock(), Mock()
    adapter = Mock(intervention_issued=None)
    partial = interventions.empty(driver.CONFIRMATION)
    partial.update(schema="dur053.k4-confirmation.v1", intervention=110, removed=None)
    partial["overlap"] = [interventions.empty(driver.CONFIRMATION["overlap"][0])]

    def apply(*args):
        adapter.intervention_issued = 111
        driver.save(d.episode_output / "confirmation.json", partial)
        driver.save(d.episode_output / "intervention-issued.json", dict(intervention_issued=111))
        require(False, "k4-log-stream-loss" if mode == "safety" else "k4-rollout-incomplete")

    adapter.apply.side_effect = apply
    monkeypatch.setattr(driver, "Interventions", lambda *args: adapter)
    monkeypatch.setattr(
        driver, "parameters", lambda *args: dict(rate=1, L=1, W=1, T=100, warmup=0, confirmation=20)
    )
    clock = iter(range(100, 10000))
    monkeypatch.setattr(driver.time, "time", lambda: next(clock))
    monkeypatch.setattr(driver.time, "sleep", lambda _: None)
    # Confirmation failures must not turn an issued outcome into a coverage gap.
    monkeypatch.setattr(
        driver,
        "derive_episode",
        lambda *args: dict(
            valid=True,
            issues=["k4-confirmation-overlap"],
            intervention_outcome="K3 guard: k4-rollout-incomplete" if mode == "outcome" else None,
        ),
    )
    cell = dict(
        episode="short-core-0-pair-0-treatment",
        duration="short",
        arm="treatment",
        seed=1,
        event="worker rollout",
    )
    job = dict(
        job="j",
        run_id="1",
        job_id="2",
        head_sha="a" * 40,
        tree_sha="b" * 40,
        budget_scope="engineering-validation",
        measurement_claim=False,
    )
    if mode == "safety":
        with pytest.raises(ValueError, match="k4-log-stream-loss"):
            d.execute_episode(cell, {}, dict(sha256="c" * 64), job, None)
        assert not (d.episode_output / "intervention-error.json").exists()
    else:
        d.execute_episode(cell, {}, dict(sha256="c" * 64), job, None)
        manifest = json.loads((d.episode_output / "episode.json").read_bytes())
        assert manifest["status"] == ("unexercised" if mode == "no-overlap" else "FAIL"), (
            "issued outcome relabelled unexercised"
        )
        assert (d.episode_output / "unexercised.json").exists() == (mode == "no-overlap")
        if mode == "outcome":
            assert json.loads((d.episode_output / "confirmation.json").read_bytes()) == partial
            assert d.assemble.call_args.args[3] == partial
            assert (
                "k4-rollout-incomplete"
                in (d.episode_output / "intervention-error.json").read_text()
            )


def test_issue_marker_precedes_mutation(tmp_path, monkeypatch):
    a = interventions.Interventions(
        Mock(), "ns", tmp_path, {"a-prime": {"worker": "img"}}, {}, "a" * 40
    )
    a.intervening = True
    a.pods = lambda role: [pod()]
    a.release_after_deletion, a.wait_rollout = Mock(), Mock()
    monkeypatch.setattr(interventions.time, "time", lambda: 10)

    def mutate(*args):
        marker = json.loads((tmp_path / "intervention-issued.json").read_bytes())
        assert marker["intervention_issued"] == 10
        assert marker["targets"][0]["metadata"]["uid"] == "old"

    a.d.k.side_effect = mutate
    a.set_image("worker", "a-prime")
    assert a.intervention_issued == 10


@pytest.mark.parametrize("guard", ["k4-rollout-incomplete", "k4-singleton-denial-missing"])
def test_outcome_is_valid_fail_with_partial_confirmation(tmp_path, freeze, checker, guard):
    episode = (
        "short-core-6-pair-0-baseline" if "singleton" in guard else "short-coverage-0-0-treatment"
    )
    m, docs = episode_fixture(tmp_path, freeze, episode=episode)
    c = docs["confirmation"]
    c.update(completed=None, removed=None, targets=[], rollout_status="")
    if "singleton" in guard:
        c.update(
            kind="eviction",
            target="engine-replicated-roles",
            no_cordon=True,
            eviction_retry_interval=5,
            roles=[dict(role="relay", replicas=1, disruptions_allowed=0, pod_uids=["relay-uid"])],
        )
        row = interventions.empty(driver.CONFIRMATION["evictions"][0])
        row.update(
            pod_uid="relay-uid",
            role="relay",
            replicas=1,
            requested=120.1,
            responded=120.2,
            scheduled=120,
            status=201,
        )
        c["evictions"] = [row]
    write(tmp_path / "confirmation.json", c)
    m["files"]["confirmation"] = hashlib.sha256(
        (tmp_path / "confirmation.json").read_bytes()
    ).hexdigest()
    write(tmp_path / "episode.json", m)
    write(tmp_path / "intervention-issued.json", dict(intervention_issued=m["intervention"] + 0.01))
    write(tmp_path / "intervention-error.json", dict(error="K3 guard: " + guard))
    value = evidence.episode(tmp_path, freeze, checker)
    assert value["valid"] and value["status"] == "FAIL", value["issues"]
    assert value["partial_confirmation_issues"]
    assert value["intervention_outcome"] == "K3 guard: " + guard


class ClockPool:
    """Execute futures on parallel virtual timelines, merging at result()."""

    now = 0.0

    def __init__(self, max_workers):
        self.origin = self.now
        self.end = self.now

    def __enter__(self):
        return self

    def __exit__(self, *args):
        ClockPool.now = self.end

    def submit(self, fn, *args):
        ClockPool.now = self.origin
        f = Future()
        try:
            f.set_result(fn(*args))
        except Exception as error:
            f.set_exception(error)
        finished = ClockPool.now
        self.end = max(self.end, finished)
        result = f.result

        def join(*args, **kwargs):
            self.origin = max(self.origin, finished)
            ClockPool.now = max(ClockPool.now, finished)
            return result(*args, **kwargs)

        f.result = join
        return f


def eviction_fake(tmp_path, monkeypatch):
    ClockPool.now = 10.0
    monkeypatch.setattr(interventions, "ThreadPoolExecutor", ClockPool)
    monkeypatch.setattr(interventions.time, "time", lambda: ClockPool.now)
    monkeypatch.setattr(interventions.time, "monotonic", lambda: ClockPool.now)
    monkeypatch.setattr(
        interventions.time, "sleep", lambda delay: setattr(ClockPool, "now", ClockPool.now + delay)
    )
    roles = {"api", "scheduler", "worker", "worker-control"}
    initial = {
        role: [pod(role + "1", role), pod(role + "2", role)] if role in roles else [pod(role, role)]
        for role in interventions.ENGINE_ROLES
    }
    live = copy.deepcopy(initial)
    counts = {}

    def k(*args, **kwargs):
        if "get" in args:
            index = args.index("get")
            kind = args[index + 1]
            name = args[index + 2] if args[index + 2] != "-o" else None
            if name == "":
                raise ValueError("error: resource name may not be empty")
            if name is None:
                return SimpleNamespace(
                    stdout=json.dumps(
                        {
                            "items": [
                                dict(
                                    metadata=dict(name=r),
                                    status=dict(disruptionsAllowed=1 if r in roles else 0),
                                )
                                for r in initial
                            ]
                        }
                    )
                )
            if kind == "pdb":
                return SimpleNamespace(
                    stdout=json.dumps(
                        dict(status=dict(disruptionsAllowed=1 if name in roles else 0))
                    )
                )
        name = json.loads(kwargs["data"])["metadata"]["name"]
        role = next(
            r for r, pods in initial.items() if any(p["metadata"]["name"] == name for p in pods)
        )
        counts[name] = counts.get(name, 0) + 1
        ClockPool.now += 1.5  # Four sequential retry commands exceed the 5 s bound.
        denied = role not in roles or (name.endswith("2") and counts[name] == 1)
        if not denied:
            live[role] = [p for p in live[role] if p["metadata"]["name"] != name] + [
                pod(name + "new", role)
            ]
        return SimpleNamespace(
            returncode=1 if denied else 0, stderr="TooManyRequests" if denied else ""
        )

    a = interventions.Interventions(
        SimpleNamespace(k=k, until=lambda pred, _: pred()), "ns", tmp_path, {}, {}, "a" * 40
    )
    a.pods = lambda role: live[role]
    return a, counts


def test_four_denied_roles_retry_concurrently(tmp_path, monkeypatch):
    a, counts = eviction_fake(tmp_path, monkeypatch)
    c = interventions.empty(driver.CONFIRMATION)
    a.eviction(10, c)
    assert len(c["evictions"]) == 15
    for name, count in counts.items():
        if count == 2:
            rows = sorted(
                [r for r in c["evictions"] if r["pod_uid"] == name], key=lambda r: r["requested"]
            )
            assert rows[1]["requested"] - rows[0]["responded"] <= 5, "sequential eviction retry"
    assert len([r for r in c["evictions"] if r["replaced_by"]]) == 8
    assert "k4-eviction-retry-gap" in interventions.SAFETY_GUARDS
    assert not interventions.intervention_outcome(ValueError("K3 guard: k4-eviction-retry-gap"))


def test_eviction_rows_survive_later_guard(tmp_path, monkeypatch):
    a, _ = eviction_fake(tmp_path, monkeypatch)

    def fail():
        rows = json.loads((tmp_path / "confirmation.json").read_bytes())["evictions"]
        assert len(rows) == 11, "incremental eviction rows lost"
        require(False, "k4-log-stream-loss")

    a.release = fail
    with pytest.raises(ValueError, match="k4-log-stream-loss"):
        a.eviction(10, interventions.empty(driver.CONFIRMATION))
    assert len(json.loads((tmp_path / "confirmation.json").read_bytes())["evictions"]) == 11


@pytest.mark.parametrize("eligible", [False, True])
def test_censored_assignment_requires_eligible_arrival(tmp_path, freeze, checker, eligible):
    m, docs = episode_fixture(tmp_path, freeze)
    stable = m["intervention"] + 1 if eligible else m["reconciliation_end"] - 1
    row = dict(
        change="change",
        at=m["intervention"],
        consumer="one",
        session="s",
        generation=1,
        partitions=[0],
        survivors=["one"],
        trigger_uid="uid",
        arrival="",
        disposition_order=None,
        event="revoke",
    )
    # The fixture has later arrivals; an empty assignment receives none of them.
    docs["kafka"]["rows"] = [
        row,
        dict(row, event="stable", at=stable, partitions=[0]),
    ]
    write(tmp_path / "kafka.json", docs["kafka"])
    m["files"]["kafka"] = hashlib.sha256((tmp_path / "kafka.json").read_bytes()).hexdigest()
    write(tmp_path / "episode.json", m)
    value = evidence.episode(tmp_path, freeze, checker)
    assert ("k4-assignment-disposition-censored" in value["issues"]) == eligible, (
        "idle generation rejected or eligible arrival ignored"
    )
    stall = value["assignment_stalls"][0]
    assert stall["censored"] and stall["total"] is None
    assert stall["total_lower"] == m["reconciliation_end"] - m["intervention"]
    assert value["assignment_change_counts"] == dict(censored=1, uncensored=0)


@pytest.mark.parametrize("status", ["PASS", "FAIL"])
def test_valid_failed_intervention_fails_job(tmp_path, monkeypatch, status):
    fake = Mock()
    fake.fatal = False
    fake.base.command.return_value = SimpleNamespace(stdout="a" * 40)
    fake.cleanup.return_value = dict(status="PASS")
    fake.execute_episode.return_value = dict(valid=True, status=status)
    monkeypatch.setattr(driver, "K4Driver", lambda *args: fake)
    monkeypatch.setattr(driver, "frozen", lambda *args: dict(sha256="a" * 64))
    monkeypatch.setattr(driver, "SMOKE_CELLS", ("short-core-0-pair-0-treatment",))
    monkeypatch.setattr(driver, "summaries", lambda *args: {})
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "pull_request")
    monkeypatch.setenv("GITHUB_RUN_ID", "100")
    out = tmp_path / "artifact"
    result = driver.run(out, tmp_path / "scratch", "lifecycle-smoke", engineering=True)
    assert result == (0 if status == "PASS" else 1), "valid FAIL hidden by job success"
    assert json.loads((out / "manifest.json").read_bytes())["status"] == status


def test_eviction_api_failure_retains_completed_requests(tmp_path, monkeypatch):
    a, _ = eviction_fake(tmp_path, monkeypatch)

    kubectl = a.d.k

    def fail_eviction(*args, **kwargs):
        if "create" in args:
            return SimpleNamespace(returncode=1, stderr="unavailable")
        return kubectl(*args, **kwargs)

    a.d.k = fail_eviction
    with pytest.raises(ValueError, match="k4-eviction-api-failure"):
        a.eviction(10, interventions.empty(driver.CONFIRMATION))
    rows = json.loads((tmp_path / "confirmation.json").read_bytes())["evictions"]
    assert len(rows) == 11 and all(r["status"] == 0 for r in rows), (
        "completed eviction request lost"
    )
