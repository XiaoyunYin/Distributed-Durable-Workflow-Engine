import json
import sys
from contextlib import nullcontext
from io import BytesIO
from pathlib import Path
from threading import Event
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import j3_admission_diagnostics as admission  # noqa: E402
import j3_diagnostics as diagnostics  # noqa: E402
import j3_episode as episode  # noqa: E402
import j3_qualification as j3  # noqa: E402
import j3_runner_gate as gate  # noqa: E402
import j3_schedule as schedule  # noqa: E402
import j3_slots as slots  # noqa: E402


def test_round_590_outage_2_refire_remains_fail_despite_unpaired_delivery():
    fixture = Path(__file__).with_name("fixtures") / "r420-outage-2.json"
    trace = json.loads(fixture.read_bytes())
    evaluations = [
        dict(zip(trace["evaluation_fields"], row, strict=True)) for row in trace["evaluations"]
    ]
    transitions = trace["transitions"]
    age = schedule.recovery_age_evidence(
        evaluations, transitions, trace["restore_ns"], trace["stop_ns"]
    )
    assert age["oldest_refires_before_stop"][0]["at_ns"] == 1795786947246000000
    outcome = {}
    with pytest.raises(j3.EpisodeFailure, match="OldestClaimable re-fired"):
        j3.check_episode(
            outcome,
            lambda: j3.require_calibration_recovery(age),
            lambda: j3.notifications(
                trace["receiver"], transitions, trace["target"], trace["origin_ns"], trace["end_ns"]
            ),
        )
    assert outcome["behavior"]["status"] == "FAIL"
    # The original receiver had start/end markers and deliveries, no periodic sequence proof.
    assert outcome["delivery"]["status"] == "INVALID"
    assert (
        "missing firing receiver delivery for OldestClaimable at 1795786972246000000"
        in (outcome["delivery"]["error"])
    )


def test_scrape_pairing_uses_distinct_campaign_id(monkeypatch):
    monkeypatch.setenv("GITHUB_RUN_ID", "42")
    assert gate.campaign_id() == "j3-r426-42"


@pytest.mark.parametrize("stage", ["calibration", "confirmation"])
def test_qualification_job_uses_only_its_registered_arms(tmp_path, monkeypatch, stage):
    (tmp_path / "sender.sha256").write_text("sender", encoding="ascii")
    (tmp_path / "run.json").write_text("{}", encoding="ascii")
    args = SimpleNamespace(evidence=tmp_path, stage=stage, sender=Path("sender"))
    driver = SimpleNamespace(reset=lambda: None, fresh_episode=lambda *_args: (None, None, None))
    monkeypatch.setenv("J3_JOB_STARTED", "1")
    monkeypatch.setenv("GITHUB_SHA", "head")
    monkeypatch.setattr(j3, "runner_pressure", lambda *_: nullcontext())
    monkeypatch.setattr(j3, "retain_postgres_log", lambda *_a, **_k: None)
    monkeypatch.setattr(j3, "retain_monitor_logs", lambda *_a, **_k: None)
    calls = []
    sealed = {"matched_healthy": {"stop_s": 2460}, "primary": {"stop_s": 2460}}

    def run_episode(
        _driver, _pins, target, arm, _run, _setup, _observer, _binary, duration, bounds, _outcome
    ):
        calls.append((target["namespace"], arm, duration))
        assert target["campaign"] == "j3-r417-example"
        if target["namespace"] in {f"j3-target-outage-{i}" for i in range(1, 4)}:
            (driver.output / "outage.json").write_text("{}", encoding="ascii")
        if target["namespace"] == "j3-target-full-volume":
            assert bounds is sealed["primary"]
            raise j3.EpisodeFailure("sealed pre-restore catch-up bound missed")
        return 10 * j3.SECOND, 20 * j3.SECOND

    def forecast(paths, measured):
        assert len(paths) == 3 and measured.keys() == schedule.CALIBRATION_MINUTES.keys()
        return {"status": "HALT", "reason": "forecast_over_cap"}

    monkeypatch.setattr(j3, "run_episode", run_episode)
    monkeypatch.setattr(j3, "restored_rate", lambda _: (j3.ARRIVAL_RATE + 1, {}))
    monkeypatch.setattr(j3, "derive_from_outages", forecast)
    if stage == "calibration":
        result = j3.run_qualification(driver, args, {}, {}, "j3-r417-example")
        assert result["status"] == "HALT"
        assert calls == [
            ("j3-target-foreign", "foreign", 1200),
            ("j3-target-outage-1", "outage", 2220),
            ("j3-target-outage-2", "outage", 2220),
            ("j3-target-outage-3", "outage", 2220),
            ("j3-target-scrape-loss", "scrape_loss", 720),
        ]
    else:
        with pytest.raises(j3.EpisodeFailure, match="sealed pre-restore catch-up bound missed"):
            j3.run_qualification(driver, args, {}, {}, "j3-r417-example", sealed)
        verdict = json.loads((tmp_path / "verdict.json").read_bytes())
        assert verdict["status"] == "FAIL"
        assert calls == [
            ("j3-target-matched-healthy", "matched_healthy", 2460),
            ("j3-target-full-volume", "outage", 2460),
        ]
    assert not (tmp_path / "sealed-schedule.json").exists()
    assert not (tmp_path / "schedule-seal.json").exists()


def test_runner_inventory_records_cpu_model_and_online_cores(tmp_path, monkeypatch):
    source = tmp_path / "cpuinfo"
    source.write_text("model name : Host CPU\nmodel name : Host CPU\n", encoding="ascii")
    monkeypatch.setattr(
        j3.os, "sysconf", lambda key: 4 if key == "SC_NPROCESSORS_ONLN" else None, raising=False
    )
    assert j3.runner_cpu(source) == {"cpu_models": ["Host CPU"], "online_cores": 4}


def test_full_volume_refuses_a_checker_that_differs_from_the_lock(tmp_path):
    checker = tmp_path / "checker"
    checker.write_bytes(b"unverified checker")

    def pod(*_args):
        pytest.fail("checker digest must be verified before accessing the observer")

    driver = SimpleNamespace(output=tmp_path, checker=checker, pod=pod)
    with pytest.raises(episode.CheckerError, match="checker outcome: invalid") as error:
        episode.check_full_volume(driver, {"namespace": "target"}, [], {}, [])
    assert "J2 checker SHA-256 mismatch" in str(error.value.__cause__)
    assert not (tmp_path / "checker.json").exists()


@pytest.mark.parametrize(
    ("arm", "started_ms", "sampled_ms", "finished_ms", "phase", "error"),
    [
        ("outage", 599_880, 599_900, 600_300, "transition", None),
        ("outage", 599_900, 600_100, 600_300, "transition", None),
        ("outage", 599_800, 599_900, 600_000, "transition", None),
        ("outage", 599_500, 599_600, 599_999, "healthy", "missing healthy worker slot witness"),
        ("outage", 600_000, 600_100, 600_300, "outage", None),
        ("outage", 899_900, 899_950, 900_300, "transition", None),
        ("outage", 959_900, 959_950, 960_300, "transition", None),
        ("outage", 960_000, 960_100, 960_300, "restored", "missing restored worker slot witness"),
        ("scrape_loss", 599_900, 599_950, 600_300, "transition", None),
        (
            "scrape_loss",
            599_500,
            599_600,
            599_999,
            "healthy",
            "missing healthy worker slot witness",
        ),
        (
            "scrape_loss",
            630_000,
            630_100,
            630_300,
            "healthy",
            "missing healthy worker slot witness",
        ),
        ("scrape_loss", 659_900, 659_950, 660_300, "transition", None),
        (
            "scrape_loss",
            660_000,
            660_100,
            660_300,
            "healthy",
            "missing healthy worker slot witness",
        ),
    ],
)
def test_slot_checks_use_the_full_read_interval(
    tmp_path, monkeypatch, arm, started_ms, sampled_ms, finished_ms, phase, error
):
    origin = 100 * j3.SECOND
    started, sampled, finished = (
        origin + offset * 1_000_000 for offset in (started_ms, sampled_ms, finished_ms)
    )
    now = started
    clock = SimpleNamespace(time_ns=lambda: now, monotonic_ns=lambda: now)
    monkeypatch.setattr(episode, "time", clock)
    monkeypatch.setattr(slots, "time", clock)
    monkeypatch.setattr(episode, "process_signature", lambda *_: {})
    worker = {
        "metadata": {"name": "worker-a", "uid": "worker-uid", "labels": {"app": "worker"}},
        "status": {
            "phase": "Running",
            "podIP": "10.0.0.1",
            "conditions": [{"type": "Ready", "status": "True"}],
        },
    }
    observer = {"metadata": {"name": "observer", "labels": {"app": "observer"}}}
    receiver = {"metadata": {"name": "receiver", "labels": {"app": episode.RECEIVER}}}

    def pods():
        nonlocal now
        now = sampled
        return [worker, observer], [receiver]

    refusal = "[Errno 111] Connection refused"

    def kubectl(*args, **_kwargs):
        nonlocal now
        if args[3] == "observer":
            now = finished
            return SimpleNamespace(stdout=json.dumps({"worker-uid": {"error": refusal}}))
        assert args[3] == "receiver"
        # Publishing is downstream of the reads and must not extend their interval.
        now += j3.SECOND

    driver = SimpleNamespace(output=tmp_path, k=kubectl)
    sampler = episode.Sampler(driver, [], arm)
    sampler.origin, sampler.processes = origin, {}
    monkeypatch.setattr(sampler, "pods", pods)
    monkeypatch.setattr(sampler, "check_live_rules", lambda _: None)
    sampler.sample()

    record = json.loads((tmp_path / "ready-slots.jsonl").read_bytes())
    assert record == sampler.latest
    assert (record["read_started_ns"], record["sample_ns"], record["read_finished_ns"]) == (
        started,
        sampled,
        finished,
    )
    assert record["phase"] == phase
    assert record["pods"][0]["ready"] and not record["pods"][0]["terminating"]
    assert record["errors"] == {"worker-uid": refusal} and record["slots"] == []
    if error:
        with pytest.raises(ValueError, match=error):
            j3.check_slot_witness(sampler.latest, {"worker-uid"})
    else:
        j3.check_slot_witness(sampler.latest, {"worker-uid"})


def test_arming_waits_for_two_valid_evaluations_and_both_alert_views(tmp_path, monkeypatch):
    target = {"namespace": "target", "campaign": "run"}
    cases = iter(
        (
            "no_scrape",
            "one_valid",
            "skipped_evaluation",
            "latest_invalid",
            "stale",
            "pending",
            "firing",
            "alertmanager",
            "settled",
        )
    )
    case = None

    def prom(query, *, at=None):
        assert 'namespace="target"' in query and 'campaign="run"' in query
        if "[12s]" in query:
            assert at == 108
            values = [[100, "1"], [105, "1"]]
            if case == "skipped_evaluation":
                values = [[95, "1"], [105, "1"]]
            if case == "stale":
                values = [[95, "1"], [100, "1"]]
            valid = {"one_valid": values[-1:], "latest_invalid": values[:1]}.get(case, values)
            return [
                {"metric": {"__name__": "j3:expected"}, "values": values},
                {"metric": {"__name__": "j3:valid"}, "values": valid},
            ]
        if query.startswith("(up{"):
            assert at == 108 and 'job="j3"' in query
            return [] if case == "no_scrape" else [{"value": [at, "1"]}]
        assert at is None and 'alertstate=~"pending|firing"' in query
        return [{"metric": {"alertstate": case}}] if case in {"pending", "firing"} else []

    def alertmanager(port, path):
        assert port == 19093 and path == "/api/v2/alerts"
        scope = target if case == "alertmanager" else {**target, "namespace": "unrelated"}
        return [{"labels": scope, "status": {"state": "active"}}]

    def until(predicate, seconds):
        nonlocal case
        assert seconds == 30
        for case in cases:
            assert predicate() is (case == "settled")

    def forward(role, ports):
        assert role == j3.ALERTMANAGER and ports == ["19093:9093"]
        return nullcontext()

    monkeypatch.setattr(j3, "time", SimpleNamespace(time_ns=lambda: 109 * j3.SECOND))
    monkeypatch.setattr(j3, "prom", prom)
    monkeypatch.setattr(j3, "json_request", alertmanager)
    j3.wait_for_settled_alerts(
        SimpleNamespace(output=tmp_path, until=until, forward=forward), target
    )
    evidence = json.loads((tmp_path / "alert-arming.json").read_bytes())
    assert evidence == {
        "query_ns": 108 * j3.SECOND,
        "valid_evaluations_ns": [100 * j3.SECOND, 105 * j3.SECOND],
    }


def test_unsettled_alert_state_fails_setup_and_keeps_receiver_log(tmp_path, monkeypatch):
    def until(predicate, seconds):
        assert seconds == 30 and not predicate()
        raise TimeoutError("setup allowance expired")

    saved = []
    driver = SimpleNamespace(output=tmp_path, until=until, forward=lambda *_: nullcontext())
    monkeypatch.setattr(j3, "settled_alert_state", lambda _target: None)
    monkeypatch.setattr(j3, "receiver_log", saved.append)
    with pytest.raises(ValueError, match="setup alert state did not settle within 30 seconds"):
        j3.wait_for_settled_alerts(driver, {})
    assert saved == [driver]
    assert not (tmp_path / "alert-arming.json").exists()


def test_two_fresh_episodes_render_separate_packages(tmp_path, monkeypatch):
    manifests = tmp_path / "manifests"
    (manifests / "base").mkdir(parents=True)
    (manifests / "base/resources.yaml").write_text('{"items": []}', encoding="utf-8")
    (manifests / "connection-budget.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(j3.kind, "K8S", manifests)
    monkeypatch.setattr(j3.kind, "package_policy", lambda objects, budget: 0)
    # The hosted driver installs PyYAML at bootstrap. The fake kustomize emits JSON.
    monkeypatch.setitem(sys.modules, "yaml", SimpleNamespace(safe_load_all=json.loads))

    service = {"kind": "Service", "metadata": {"name": "api"}, "spec": {}}
    observer = {
        "kind": "Deployment",
        "metadata": {"name": "observer"},
        "spec": {"template": {"spec": {"containers": [{"volumeMounts": []}], "volumes": []}}},
    }
    objects = [
        service,
        observer,
        *({"kind": "Job", "metadata": {"name": name}} for name in ("migrate", "compatibility")),
    ]
    job_scratch = tmp_path / "job"
    (job_scratch / "bin").mkdir(parents=True)
    (job_scratch / "bin/kind").touch()
    (job_scratch / "kubeconfig").touch()
    driver = j3.Qualification(tmp_path, job_scratch, tmp_path / "checker")
    driver.images = {"worker": "repo/worker@sha256:test"}
    environment = dict(driver.env)
    packages = []
    commands = []

    def command(args, *, data=None, **kwargs):
        commands.append(args)
        stdout = ""
        if args[0] == "kustomize":
            package = Path(args[2]).parents[1]
            assert (package / "base/resources.yaml").is_file()
            packages.append(package)
            stdout = json.dumps(objects)
        elif args[0] == "kubectl" and args[5] == "get":
            match args[6]:
                case "deployments,pods,services,networkpolicies,pdb,jobs":
                    items = [{**service, "spec": {"clusterIP": "10.96.0.10"}}]
                case "pods":
                    items = [
                        {
                            "metadata": {"name": name, "uid": name, "labels": {"app": "worker"}},
                            "status": {
                                "phase": "Running",
                                "containerStatuses": [
                                    {"imageID": driver.images["worker"], "restartCount": 0}
                                ],
                            },
                        }
                        for name in ("worker-1", "worker-2")
                    ]
                case "hpa":
                    items = []
                case _:
                    raise AssertionError(args)
            stdout = json.dumps({"items": items})
        elif data and "FROM pg_database" in data:
            stdout = "0"
        return SimpleNamespace(stdout=stdout, returncode=0)

    monkeypatch.setattr(driver, "command", command)
    monkeypatch.setattr(
        driver, "query", lambda sql: "100" if sql.startswith("SHOW") else "j3-db-exporter|1"
    )

    def prepare_overlay(*args, diagnostics):
        assert diagnostics
        return {}

    monkeypatch.setattr(j3, "prepare_overlay", prepare_overlay)
    monkeypatch.setattr(j3, "install_sender", lambda *args: ("observer", "/evidence/j3-loadgen"))

    addresses = {"192.0.2.1": "10.0.0.1", "192.0.2.2": "10.0.0.2"}
    for name in ("foreign", "outage-1"):
        driver.output = tmp_path / name
        driver.output.mkdir()
        driver.fresh_episode(addresses, {}, {"namespace": f"j3-target-{name}"}, None)
        driver.reset()
        assert not driver.databases
    assert packages == [
        job_scratch / "j3-target-foreign/package-kind",
        job_scratch / "j3-target-outage-1/package-kind",
    ]
    assert driver.env == environment
    rendered = json.loads((driver.output / "application-rendered.json").read_bytes())
    assert rendered[0]["spec"]["clusterIP"] == "10.96.0.10"

    assert driver.cleanup()["status"] == "PASS"
    assert ["kind", "delete", "cluster", "--name", j3.kind.CLUSTER] in commands


def test_pressure_is_retained_on_episode_error(tmp_path):
    proc = tmp_path / "proc"
    (proc / "pressure").mkdir(parents=True)
    (proc / "loadavg").write_text("0.03 0.02 0.01 1/20 100\n", encoding="ascii")
    (proc / "pressure/cpu").write_text("some avg10=1.00 total=3\n", encoding="ascii")
    with (
        pytest.raises(ValueError, match="episode error"),
        diagnostics.runner_pressure(tmp_path, proc),
    ):
        raise ValueError("episode error")
    sample, *_ = [
        json.loads(line) for line in (tmp_path / "runner-pressure.jsonl").read_text().splitlines()
    ]
    assert sample["loadavg"] == "0.03 0.02 0.01 1/20 100"
    assert sample["cpu"] == "some avg10=1.00 total=3"
    assert "io" in sample["errors"]
    assert sample["at_ns"] > 0 and sample["monotonic_ns"] > 0


def test_failed_postgres_log_capture_retains_partial_diagnostics(tmp_path, monkeypatch):
    driver = SimpleNamespace(output=tmp_path, env={}, password="secret")

    def run(command, **kwargs):
        assert command[:5] == ["docker", "logs", "--timestamps", "--since", "123.000000007"]
        raise diagnostics.subprocess.TimeoutExpired(command, 30, output=b"partial secret\n")

    monkeypatch.setattr(diagnostics.subprocess, "run", run)
    diagnostics.retain_postgres_log(driver, 123_000_000_007, required=False)
    assert (tmp_path / "postgres-container.log").read_text() == "partial [REDACTED]\n"
    assert "error" in json.loads((tmp_path / "postgres-log.json").read_bytes())


def test_resource_sample_parsers_retain_devices_and_distinguish_missing_counters():
    rows = list(
        admission.node_resource_rows(
            [
                "100\tobserver/observer/observer\tcpu.stat\tok\t"
                "usage_usec 783;nr_throttled 3;throttled_usec 991\n",
                "100\tapi/api-1/api\tio.stat\tok\t8:0 rbytes=12 wbytes=42;259:2 rios=3 wios=4\n",
                "100\tapi/api-1/api\tio.stat\tok\t\n",
                "100\tapi/api-2/api\tio.stat\terror\tunreadable\n",
                "bad line\n",
            ]
        )
    )
    assert rows[0]["at_ns"] == 100 and rows[0]["cgroup"] == "observer/observer/observer"
    assert rows[0]["counters"] == {"usage_usec": 783, "nr_throttled": 3, "throttled_usec": 991}
    assert rows[1]["counters"] == {
        "8:0": {"rbytes": 12, "wbytes": 42},
        "259:2": {"rios": 3, "wios": 4},
    }
    assert rows[2]["counters"] == {} and rows[3]["counters"] is None
    assert rows[3]["error"] == "unreadable" and rows[4]["raw"] == "bad line"
    assert admission.parse_diskstats("8 0 sda 0 1 2\n259 2 nvme0n1 3 4 5\n") == {
        "8:0": {"name": "sda", "counters": [0, 1, 2]},
        "259:2": {"name": "nvme0n1", "counters": [3, 4, 5]},
    }


def test_sender_miss_retains_resources_postgres_api_and_outstanding_journal(tmp_path, monkeypatch):
    proc, cgroup = tmp_path / "proc", tmp_path / "cgroup"
    for role, pid in (("postgres", 201), ("kafka", 202)):
        name = f"docker-{role}-id.scope"
        (proc / str(pid)).mkdir(parents=True)
        (proc / str(pid) / "cgroup").write_text(f"0::/system.slice/{name}\n", encoding="ascii")
        group = cgroup / "system.slice" / name
        group.mkdir(parents=True)
        (group / "cpu.stat").write_text(
            "usage_usec 123\nnr_throttled 2\nthrottled_usec 5\n", encoding="ascii"
        )
        if role == "postgres":
            (group / "io.stat").write_text("8:0 rbytes=4 wbytes=8\n", encoding="ascii")
    (proc / "diskstats").write_text("8 0 sda 1 2 3\n", encoding="ascii")
    pods = [
        {
            "metadata": {"name": name, "uid": f"u{i}", "labels": {"app": role}},
            "status": {
                "containerStatuses": [{"name": role, "containerID": f"containerd://cid{i}"}]
            },
        }
        for i, (name, role) in enumerate(
            (("observer", "observer"), ("api-1", "api"), ("api-2", "api")), 1
        )
    ]
    sent = "2026-11-26T20:00:00Z"
    first = {
        "event": "arrival",
        "key": "outstanding",
        "sent": sent,
        "outcome": "uncertain",
        "status": 0,
        "error": "",
    }
    missed = {**first, "key": "miss", "sent": None, "outcome": "unsent", "error": "capacity-missed"}
    accepted = {**first, "outcome": "accepted", "status": 200}
    journal = "".join(
        json.dumps(row) + "\n" for row in [{"event": "plan"}, first, missed, accepted]
    )
    pg_started = Event()

    def result(stdout="", stderr=""):
        return SimpleNamespace(returncode=0, stdout=stdout, stderr=stderr)

    def command(args, **_kwargs):
        if args[1] == "inspect":
            return result(
                "postgres-id 201\n" if args[-1].endswith("-postgres") else "kafka-id 202\n"
            )
        if args[3:5] == ["crictl", "inspect"]:
            return result(json.dumps({"info": {"pid": 100 + int(args[-1][-1])}}))
        index = int(args[-1].split("/")[2]) - 100
        return result(f"0::/kubepods.slice/podu{index}/cri-containerd-cid{index}.scope\n")

    def k(*args, **kwargs):
        if args[2] == "get":
            assert "app in (observer,api)" in args
            return result(json.dumps({"items": pods}))
        if args[2] == "logs":
            assert pg_started.is_set() and "app=api" in args and "--tail=-1" in args
            return result("api-1 and api-2 with fixture-password\n")
        if episode.READ_SENDER_EXIT in args:
            return result(
                json.dumps({"record": json.dumps(exit_record), "journal_last_line": None})
            )
        if episode.OBSERVER_PROCESSES in args:
            return result("[]")
        if args[-2:] == ("cat", episode.JOURNAL):
            assert pg_started.wait(1)
            return result(journal)
        assert "-i" in args and "data" in kwargs
        return result()

    def postgres(query, **kwargs):
        assert "BEGIN READ ONLY" in query and "pg_stat_activity" in query and "pg_locks" in query
        assert kwargs == {"allowed": True, "timeout": 6}
        pg_started.set()
        return result('{"pg_stat_activity":[{"wait_event":"WALSync"}],"pg_locks":[]}')

    class Process:
        pid = 42
        returncode = 1

        def poll(self):
            return self.returncode

    class NodeProcess(Process):
        stdin = BytesIO()
        returncode = 0

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def wait(self, timeout):
            assert timeout == 3

    def popen(args, **kwargs):
        if args[0] == "kubectl":
            return Process()
        assert args[:3] == ["docker", "exec", "-i"] and admission.NODE_SAMPLE in args
        assert any(arg.startswith("api/api-2/api=") for arg in args)
        kwargs["stdout"].write(
            b"100\tobserver/observer/observer\tcpu.stat\tok\t"
            b"usage_usec 5;nr_throttled 3;throttled_usec 2\n"
        )
        kwargs["stdout"].flush()
        return NodeProcess()

    driver = SimpleNamespace(
        output=tmp_path,
        env={},
        k=k,
        command=command,
        postgres=postgres,
        password="fixture-password",
    )
    exit_record = {"returncode": 1, "closed": False, "exited_ns": 100}
    rows = [{"namespace": "target", "scheduled_ns": j3.SECOND}]
    sender = episode.Sender(driver, "observer", "sender", rows, "test", 0, j3.SECOND)
    monkeypatch.setattr(admission.subprocess, "Popen", popen)
    with (
        pytest.raises(ValueError, match="sender exited with status 1"),
        admission.arm_resources(driver, "app", proc=proc, cgroup_root=cgroup),
        sender.run(),
    ):
        sender.check_running()

    def saved(filename):
        return json.loads((tmp_path / filename).read_bytes())

    node = saved("arm-node-resources.jsonl")
    host = saved("arm-runner-resources.jsonl")
    snapshot = saved("sender-postgres-snapshot.json")
    journal_report = saved("sender-outstanding.json")
    assert node["counters"]["nr_throttled"] == 3
    assert host["cgroups"]["postgres"]["io.stat"]["8:0"]["wbytes"] == 8
    assert host["cgroups"]["kafka"]["cpu.stat"]["throttled_usec"] == 5
    assert "kafka/io.stat" in host["errors"] and "8:0" in host["diskstats"]
    assert snapshot["postgres_started_ns"] >= snapshot["failure_seen_ns"]
    assert snapshot["snapshot"]["pg_stat_activity"][0]["wait_event"] == "WALSync"
    assert (tmp_path / "sender-api.log").read_text() == "api-1 and api-2 with [REDACTED]\n"
    assert journal_report["boundary"] == "first_failed_arrival"
    assert journal_report["failed_arrival"] == missed
    assert journal_report["outstanding"] == [first]
    assert journal_report["terminal_after_boundary"] == {"outstanding": accepted}
