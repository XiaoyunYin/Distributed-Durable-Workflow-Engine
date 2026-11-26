import json
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import j3_qualification as j3  # noqa: E402


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
    monkeypatch.setattr(j3, "prepare_overlay", lambda *args: {})
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
