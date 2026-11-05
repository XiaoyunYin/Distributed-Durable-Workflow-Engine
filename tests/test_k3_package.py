from __future__ import annotations

import copy
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import dur053_k3 as driver  # noqa: E402
import dur053_k3_policy as policy  # noqa: E402


def fixture() -> tuple[list[dict], dict]:
    objects = json.loads((ROOT / "deploy/k8s/base/resources.yaml").read_text())["items"]
    text = (
        json.dumps(objects)
        .replace("dur053/runtime:UNBOUND", "localhost:5001/runtime@sha256:" + "a" * 64)
        .replace("dur053/worker:UNBOUND", "localhost:5001/worker@sha256:" + "b" * 64)
    )
    return json.loads(text), json.loads((ROOT / "deploy/k8s/connection-budget.json").read_text())


class K3PolicyTests(unittest.TestCase):
    def test_package_accepts_registered_topology(self) -> None:
        objects, budget = fixture()
        self.assertEqual(policy.package_policy(objects, budget), 74)

    def test_unsafe_packages(self) -> None:
        changes = {
            "role-topology": lambda by, budget: by.pop(("Deployment", "relay")),
            "no-committed-secret": lambda by, budget: by.update(
                {("Secret", "bad"): {"kind": "Secret", "metadata": {"name": "bad"}}}
            ),
            "default-deny": lambda by, budget: by[("NetworkPolicy", "default-deny")]["spec"].update(
                ingress=[{}]
            ),
            "token-automount": lambda by, budget: pod(by).update(automountServiceAccountToken=True),
            "role-serviceaccount": lambda by, budget: by[("ServiceAccount", "api")].update(
                automountServiceAccountToken=True
            ),
            "host-isolation": lambda by, budget: pod(by).update(hostNetwork=True),
            "pod-security": lambda by, budget: pod(by)["securityContext"].update(
                runAsNonRoot=False
            ),
            "drain-budget": lambda by, budget: pod(by).update(terminationGracePeriodSeconds=5),
            "topology-spread": lambda by, budget: pod(by).update(topologySpreadConstraints=[]),
            "surge-budget": lambda by, budget: by[("Deployment", "api")]["spec"].update(replicas=3),
            "image-digest": lambda by, budget: pod(by, "worker")["initContainers"][0].update(
                image="runtime:latest"
            ),
            "loaded-image": lambda by, budget: container(by).update(imagePullPolicy="Always"),
            "container-security": lambda by, budget: container(by)["securityContext"].update(
                allowPrivilegeEscalation=True
            ),
            "resource-bounds": lambda by, budget: container(by)["resources"].update(requests={}),
            "explicit-pool": lambda by, budget: env(by, "RUNTIME_DB_MAX_CONNS").update(value="8"),
            "route-role": lambda by, budget: env(by, "RUNTIME_ROLES").update(value="combined"),
            "engine-namespace": lambda by, budget: env(by, "RUNTIME_NAMESPACE").update(
                valueFrom={"fieldRef": {"fieldPath": "metadata.name"}}
            ),
            "pdb": lambda by, budget: by.pop(("PodDisruptionBudget", "api")),
            "probe-semantics": lambda by, budget: container(by)["readinessProbe"]["httpGet"].update(
                path="/healthz"
            ),
            "auth-enabled": lambda by, budget: env(by, "RUNTIME_TOKEN_AUTH").update(
                value="disabled"
            ),
            "no-hostpath": lambda by, budget: pod(by)["volumes"].append(
                {"name": "escape", "hostPath": {"path": "/"}}
            ),
            "reviewer-projection": lambda by, budget: env(
                by, "RUNTIME_TOKEN_REVIEW_TOKEN_FILE"
            ).update(value="/bad"),
            "identity-boundary": lambda by, budget: env(by, "RUNTIME_TOKEN_SERVICE_ACCOUNT").update(
                value="worker"
            ),
            "worker-projection": lambda by, budget: pod(by, "worker")["volumes"][-1].update(
                unused=True
            ),
            "demand-secret-file": lambda by, budget: env(
                by, "RUNTIME_DEMAND_BEARER_TOKEN_FILE", "demand"
            ).update(value="/bad"),
            "network-coverage": lambda by, budget: by[("NetworkPolicy", "api-egress")][
                "spec"
            ].update(podSelector={}),
            "ingress-selector": lambda by, budget: by[("NetworkPolicy", "operator-ingress")][
                "spec"
            ].update(podSelector={}),
            "rbac-tokenreview": lambda by, budget: by[("ClusterRole", "dur053-tokenreview")][
                "rules"
            ][0].update(verbs=["*"]),
            "rbac-subjects": lambda by, budget: by[("ClusterRoleBinding", "dur053-tokenreview")][
                "subjects"
            ].append({"kind": "ServiceAccount", "name": "worker"}),
            "network-boundary": lambda by, budget: by[("NetworkPolicy", "operator-ingress")][
                "spec"
            ]["ingress"][0].update(
                **{"from": [{"podSelector": {"matchLabels": {"app": "worker"}}}]}
            ),
            "demand-network-boundary": lambda by, budget: by[("NetworkPolicy", "demand-ingress")][
                "spec"
            ]["ingress"][0]["from"][0].pop("podSelector"),
            "network-policy-set": lambda by, budget: by.update(
                {
                    ("NetworkPolicy", "allow-all"): {
                        "kind": "NetworkPolicy",
                        "metadata": {"name": "allow-all"},
                        "spec": {"podSelector": {}, "ingress": [{}]},
                    }
                }
            ),
            "bounded-egress": lambda by, budget: by[("NetworkPolicy", "api-egress")]["spec"][
                "egress"
            ].append({"to": [], "ports": [{"port": 1}]}),
            "external-endpoint-mask": lambda by, budget: by[("NetworkPolicy", "api-egress")][
                "spec"
            ]["egress"][0]["to"][0]["ipBlock"].update(cidr="0.0.0.0/0"),
            "job-bounds": lambda by, budget: by[("Job", "migrate")]["spec"].update(backoffLimit=2),
            "job-security": lambda by, budget: by[("Job", "migrate")]["spec"]["template"][
                "spec"
            ].update(automountServiceAccountToken=True),
            "job-images-security": lambda by, budget: by[("Job", "migrate")]["spec"]["template"][
                "spec"
            ]["containers"][0]["securityContext"].update(readOnlyRootFilesystem=False),
            "candidate-runtime-image": lambda by, budget: by[("Job", "migrate")]["spec"][
                "template"
            ]["spec"]["containers"][0].update(image="other@sha256:" + "c" * 64),
            "candidate-worker-image": lambda by, budget: by[("Job", "compatibility")]["spec"][
                "template"
            ]["spec"]["initContainers"][0].update(image="other@sha256:" + "c" * 64),
            "scaler-bounds": lambda by, budget: by[("ScaledObject", "worker-demand")][
                "spec"
            ].update(maxReplicaCount=100),
            "scaler-auth-metric": lambda by, budget: by[("ScaledObject", "worker-demand")]["spec"][
                "triggers"
            ][0]["metadata"].update(authMode="none"),
            "scaler-secret": lambda by, budget: by[("TriggerAuthentication", "campaign-demand")][
                "spec"
            ].update(secretTargetRef=[]),
            "scaler-behavior": lambda by, budget: by[("ScaledObject", "worker-demand")]["spec"][
                "advanced"
            ]["horizontalPodAutoscalerConfig"]["behavior"]["scaleDown"].update(
                stabilizationWindowSeconds=0
            ),
            "connection-budget": lambda by, budget: budget["future_clients"].update(unsafe=100),
        }
        for guard, change in changes.items():
            with self.subTest(guard=guard):
                objects, budget = fixture()
                by = {(o["kind"], o["metadata"]["name"]): o for o in objects}
                if guard == "worker-projection":
                    volumes = pod(by, "worker")["volumes"]
                    next(v for v in volumes if v["name"] == "identity")["projected"]["sources"][0][
                        "serviceAccountToken"
                    ]["audience"] = "wrong"
                else:
                    change(by, budget)
                try:
                    policy.package_policy(list(by.values()), budget)
                except ValueError:
                    pass
                else:
                    self.fail("policy accepted unsafe " + guard)

    def test_matched_arms(self) -> None:
        treatment, _ = fixture()
        baseline = copy.deepcopy(treatment)
        for o in baseline:
            if o["kind"] == "Deployment" and o["metadata"]["name"] != "observer":
                name = o["metadata"]["name"]
                o["spec"]["template"]["spec"]["containers"][0]["env"].append(
                    {
                        "name": "WORKER_DRAIN_MODE"
                        if name == "worker"
                        else "RUNTIME_SHUTDOWN_MODE",
                        "value": "legacy",
                    }
                )
        policy.matched_arms(treatment, baseline)
        damaged = copy.deepcopy(baseline)
        next(o for o in damaged if o["kind"] == "Deployment")["spec"]["replicas"] = 99
        with self.assertRaises(ValueError, msg="matched arm drift accepted"):
            policy.matched_arms(treatment, damaged)
        with self.assertRaises(ValueError, msg="legacy flags missing accepted"):
            policy.matched_arms(treatment, treatment)

    def test_ledger(self) -> None:
        empty = {"schema": "dur053.live-ledger.v1", "limit_seconds": 259200, "entries": []}
        row = {
            "job_id": "1",
            "run_id": "2",
            "head_sha": "a",
            "merge_ref_sha": "b",
            "tree_sha": "c",
            "artifact_sha256": "d",
            "status": "FAIL",
            "charged_seconds": 18900,
        }
        self.assertEqual(policy.ledger_check(empty, 3600, set()), 255600)
        full = copy.deepcopy(empty)
        full["entries"] = [{**row, "job_id": str(i)} for i in range(1, 14)]
        for guard, ledger, duration, prior in [
            ("ledger-schema", {**empty, "limit_seconds": 1}, 1, set()),
            ("job-live-ceiling", empty, 18901, set()),
            ("ledger-identity", {**empty, "entries": [row, row]}, 1, set()),
            ("ledger-provenance", {**empty, "entries": [{**row, "tree_sha": ""}]}, 1, set()),
            ("ledger-duration", {**empty, "entries": [{**row, "charged_seconds": -1}]}, 1, set()),
            ("uncommitted-live-evidence", empty, 1, {"55"}),
            ("ledger-budget", full, 18900, set()),
        ]:
            with (
                self.subTest(guard=guard),
                self.assertRaises(ValueError, msg="ledger guard bypassed " + guard),
            ):
                policy.ledger_check(ledger, duration, prior)

    def test_refusals_and_artifact_hashes(self) -> None:
        for scenario in ("pilot", "lifecycle", "scaling", "faults", "controller", "anything"):
            with self.assertRaisesRegex(ValueError, "K3b", msg="unimplemented scenario accepted"):
                policy.supported(scenario)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(ValueError, msg="existing output accepted"):
                policy.fresh_output(root)
            with self.assertRaises(ValueError, msg="repository output accepted"):
                policy.fresh_output(ROOT / "must-never-exist")
            (root / "manifest.json").write_text("old")
            (root / "data").write_bytes(b"evidence")
            self.assertEqual(
                policy.artifact_hashes(root), {"data": hashlib.sha256(b"evidence").hexdigest()}
            )
        with patch.object(driver.urllib.request, "urlopen") as request:
            request.return_value.__enter__.return_value.read.return_value = b"corrupt"
            with self.assertRaises(ValueError, msg="checksum mismatch accepted"):
                driver.download("https://example.invalid", "0" * 64)

    def test_driver_live_guards(self) -> None:
        import os
        import subprocess
        import time

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            d = driver.Driver(root, root)
            d.live_start = time.monotonic() - 4000
            with patch.object(
                driver.subprocess,
                "run",
                return_value=subprocess.CompletedProcess(["fixture"], 0, "", ""),
            ) as execute:
                with self.assertRaises(ValueError, msg="live deadline accepted"):
                    d.command(["never-execute"])
                execute.assert_not_called()
            d.live_start = None
            (root / "bin").mkdir()
            (root / "bin/kind").write_text("fixture")

            def command(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
                stdout = "dur053-k3a" if args == ["kind", "get", "clusters"] else ""
                return subprocess.CompletedProcess(args, 0, stdout, "")

            with (
                patch.object(d, "command", side_effect=command),
                self.assertRaises(ValueError, msg="cleanup accepted live resources"),
            ):
                d.cleanup()
        with patch.dict(
            os.environ,
            {
                "GITHUB_ACTIONS": "true",
                "GH_TOKEN": "fixture",
                "GITHUB_REPOSITORY": "fixture/repo",
                "GITHUB_RUN_ID": "10",
                "GITHUB_RUN_ATTEMPT": "1",
            },
        ):

            def api(path: str) -> dict:
                if path.endswith("/runs/10"):
                    return {"created_at": "2026-11-04", "head_sha": "head"}
                if "/runs/10/jobs" in path:
                    return {"jobs": [{"id": 15, "name": "DUR-053 campaign"}]}
                if "/runs?" in path:
                    return {"workflow_runs": [{"id": 9, "created_at": "2026-11-03"}]}
                return {
                    "total_count": 1,
                    "jobs": [{"id": 14, "name": "DUR-053 campaign", "conclusion": "failure"}],
                }

            with patch.object(driver, "github", side_effect=api):
                self.assertEqual(driver.job_identity_and_history(), ("15", {"14"}, "head"))
                with (
                    patch.dict(os.environ, {"GITHUB_RUN_ATTEMPT": "2"}),
                    self.assertRaises(ValueError, msg="rerun attempt accepted"),
                ):
                    driver.job_identity_and_history()
            with (
                patch.object(driver, "github", side_effect=OSError("API unavailable")),
                self.assertRaises(OSError),
            ):
                driver.job_identity_and_history()

    def test_schedule(self) -> None:
        import dur053_schedule

        schedule = dur053_schedule.plan()
        self.assertEqual(schedule["campaign_episodes"], 438)
        self.assertEqual(len(schedule["jobs"]), 15)
        self.assertAlmostEqual(schedule["expected_total_seconds"], 224983.5)
        self.assertTrue(all(j["expected_seconds"] <= 18300 for j in schedule["jobs"]))
        self.assertEqual(
            schedule,
            json.loads(
                (ROOT / "experiments/portfolio/k8s-recovery/amendment-b-schedule.json").read_text()
            ),
        )

    def test_evidence_intake(self) -> None:
        import dur053_record_smoke as intake

        def setup(root: Path) -> tuple[Path, Path, dict]:
            repo, source = root / "repository", root / "download"
            source.mkdir()
            ledger = repo / "experiments/portfolio/k8s-recovery/live-time-ledger.json"
            ledger.parent.mkdir(parents=True)
            ledger.write_text(
                json.dumps(
                    {"schema": "dur053.live-ledger.v1", "limit_seconds": 259200, "entries": []}
                )
            )
            manifest = {
                "schema": "dur053.k3a.v1",
                "scenario": "smoke",
                "run_id": "10",
                "job_id": "15",
                "head_sha": "a" * 40,
                "merge_ref_sha": "b" * 40,
                "tree_sha": "c" * 40,
                "status": "PASS",
                "registered_live_seconds": 0,
                "charged_seconds": 0,
                "elapsed_live_seconds": 10,
                "budget_scope": "engineering-validation",
                "measurement_claim": False,
                "cleanup": {"status": "PASS"},
                "artifact_sha256": {},
            }
            (source / "manifest.json").write_text(json.dumps(manifest))
            return repo, source, manifest

        with tempfile.TemporaryDirectory() as tmp:
            repo, source, manifest = setup(Path(tmp))
            target = intake.record(source, repo)
            ledger = repo / "experiments/portfolio/k8s-recovery/live-time-ledger.json"
            self.assertEqual(
                policy.committed_ledger(ledger, repo)["entries"][0]["charged_seconds"], 10
            )
            for guard in (
                "committed-ledger-artifact",
                "committed-ledger-hash",
                "committed-ledger-provenance",
            ):
                original = json.loads(ledger.read_text())
                changed = copy.deepcopy(original)
                if guard == "committed-ledger-artifact":
                    changed["entries"][0]["artifact"] = str(source / "manifest.json")
                elif guard == "committed-ledger-hash":
                    changed["entries"][0]["artifact_sha256"] = "0" * 64
                else:
                    changed["entries"][0]["head_sha"] = "wrong"
                ledger.write_text(json.dumps(changed))
                with self.assertRaises(ValueError, msg="evidence guard bypassed " + guard):
                    policy.committed_ledger(ledger, repo)
                ledger.write_text(json.dumps(original))
            self.assertEqual(
                (target / "manifest.json").read_bytes(), (source / "manifest.json").read_bytes()
            )
        for guard in (
            "intake-smoke-schema",
            "intake-artifact-hashes",
            "intake-no-symlinks",
            "intake-run-identity",
            "intake-duplicate-job",
            "intake-existing-run",
            "intake-charge",
            "intake-total-budget",
            "intake-single-acceptance",
            "intake-acceptance-evidence",
        ):
            with tempfile.TemporaryDirectory() as tmp:
                repo, source, manifest = setup(Path(tmp))
                ledger_path = repo / "experiments/portfolio/k8s-recovery/live-time-ledger.json"
                if guard == "intake-smoke-schema":
                    manifest["schema"] = "unknown"
                elif guard == "intake-artifact-hashes":
                    (source / "extra").write_text("unattested")
                elif guard == "intake-run-identity":
                    manifest["run_id"] = "../unsafe"
                elif guard == "intake-duplicate-job":
                    existing = intake.record(source, repo)
                    # A prior campaign job with this ID isolates duplicate-ID
                    # protection from the separate single-acceptance guard.
                    prior = existing.with_name("prior-campaign")
                    existing.rename(prior)
                    prior_manifest = prior / "manifest.json"
                    previous = json.loads(prior_manifest.read_text())
                    previous["charged_seconds"] = 10
                    prior_manifest.write_text(json.dumps(previous))
                    ledger = json.loads(ledger_path.read_text())
                    row = ledger["entries"][0]
                    row.pop("category")
                    row["artifact"] = prior_manifest.relative_to(repo).as_posix()
                    row["artifact_sha256"] = hashlib.sha256(prior_manifest.read_bytes()).hexdigest()
                    ledger_path.write_text(json.dumps(ledger))
                    policy.committed_ledger(ledger_path, repo)
                elif guard == "intake-existing-run":
                    (repo / "experiments/portfolio/k8s-recovery/k3-smoke-v1/10").mkdir(parents=True)
                elif guard == "intake-single-acceptance":
                    intake.record(source, repo)
                    manifest["run_id"] = "11"
                    ledger = json.loads(ledger_path.read_text())
                    ledger["entries"][0]["job_id"] = "19"
                    ledger_path.write_text(json.dumps(ledger))
                elif guard == "intake-acceptance-evidence":
                    manifest["status"] = "FAIL"
                elif guard == "intake-charge":
                    manifest["elapsed_live_seconds"] = -1
                elif guard == "intake-total-budget":
                    manifest["elapsed_live_seconds"] = 3600
                    ledger = json.loads(ledger_path.read_text())
                    ledger["entries"] = [
                        {
                            "job_id": str(i),
                            "run_id": "9",
                            "head_sha": "a",
                            "merge_ref_sha": "b",
                            "tree_sha": "c",
                            "artifact_sha256": "d",
                            "status": "FAIL",
                            "charged_seconds": 18900,
                        }
                        for i in range(1, 14)
                    ]
                    ledger["entries"].append(
                        {**ledger["entries"][0], "job_id": "99", "charged_seconds": 12000}
                    )
                    ledger_path.write_text(json.dumps(ledger))
                (source / "manifest.json").write_text(json.dumps(manifest))
                if guard == "intake-no-symlinks":
                    with (
                        patch.object(Path, "is_symlink", return_value=True),
                        self.assertRaises(ValueError, msg="evidence guard bypassed " + guard),
                    ):
                        intake.record(source, repo)
                else:
                    expected_error = (
                        FileExistsError if guard == "intake-existing-run" else ValueError
                    )
                    with self.assertRaises(expected_error, msg="evidence guard bypassed " + guard):
                        intake.record(source, repo)

    def test_inline_probe_programs_compile(self) -> None:
        import ast

        tree = ast.parse((ROOT / "scripts/dur053_k3.py").read_text())
        variables = {
            "ip": "127.0.0.1",
            "ips": ["127.0.0.1"],
            "inventory": {"pod_name": "worker", "file": "session.jsonl"},
        }
        checked = 0
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id in {"code", "probe", "sample"}
                for t in node.targets
            ):
                program = eval(
                    compile(ast.Expression(node.value), "probe-expression", "eval"),
                    {"repr": repr},
                    variables,
                )
                compile(program, "remote-python-probe", "exec")
                checked += 1
        objects, _ = fixture()
        for o in objects:
            if o["kind"] not in {"Deployment", "Job"}:
                continue
            spec = o["spec"]["template"]["spec"]
            for container in spec["containers"] + spec.get("initContainers", []):
                command = container.get("command", [])
                if len(command) >= 3 and command[:2] == ["python", "-c"]:
                    compile(command[2], "container-python-command", "exec")
                    checked += 1
        self.assertGreaterEqual(checked, 8)

    def test_worker_session_completeness(self) -> None:
        workers = [
            {"metadata": {"uid": name}, "status": {"containerStatuses": [{"restartCount": 0}]}}
            for name in ("first", "second")
        ]
        policy.stable_worker_sessions({"first", "second"}, workers)
        for changed in (
            workers[:1],
            [{**workers[0], "status": {"containerStatuses": [{"restartCount": 1}]}}, workers[1]],
        ):
            with self.assertRaises(ValueError, msg="worker session completeness bypassed"):
                policy.stable_worker_sessions({"first", "second"}, changed)
        policy.single_witness_session(["session.jsonl"], {"file": "session.jsonl"})
        with self.assertRaises(ValueError, msg="unattested witness session accepted"):
            policy.single_witness_session(["old.jsonl", "session.jsonl"], {"file": "session.jsonl"})

    def test_broker_reset_predicates(self) -> None:
        self.assertTrue(policy.application_topics_absent("__consumer_offsets\n"))
        self.assertFalse(
            policy.application_topics_absent("__consumer_offsets\ndurable-agent.tasks.v1\n"),
            "old task topic ignored",
        )
        self.assertFalse(
            policy.application_topics_absent("durable-agent.events.v1\n"), "old event topic ignored"
        )
        self.assertTrue(policy.worker_group_absent(""))
        self.assertFalse(
            policy.worker_group_absent("other\nruntime-workers-v1\n"), "old worker group ignored"
        )


def pod(by: dict, role: str = "api") -> dict:
    return by[("Deployment", role)]["spec"]["template"]["spec"]


def container(by: dict, role: str = "api") -> dict:
    return pod(by, role)["containers"][0]


def env(by: dict, name: str, role: str = "api") -> dict:
    return next(e for e in container(by, role)["env"] if e["name"] == name)


if __name__ == "__main__":
    unittest.main()
