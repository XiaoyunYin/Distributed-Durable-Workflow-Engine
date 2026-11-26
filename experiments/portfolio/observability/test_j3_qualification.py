import json
import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import j3_diagnostics as diagnostics  # noqa: E402
import j3_qualification as j3  # noqa: E402


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
    driver = j3.Qualification(tmp_path, job_scratch)
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
