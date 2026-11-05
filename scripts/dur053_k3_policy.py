#!/usr/bin/env python3
"""Independent fail-closed K3 package policy and cumulative live-time guards."""

from __future__ import annotations

import argparse
import copy
import hashlib
import ipaddress
import json
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
K8S = ROOT / "deploy/k8s"
DIGEST = re.compile(r"^[^\s]+@sha256:[0-9a-f]{64}$")
ROLES = {"api", "operator", "worker-control", "scheduler", "relay", "demand", "worker", "observer"}
AUTH = {"api", "operator", "worker-control"}


def require(condition: Any, guard: str) -> None:
    if not condition:
        raise ValueError("K3 guard: " + guard)


def envmap(container: dict[str, Any]) -> dict[str, Any]:
    return {
        row["name"]: {k: v for k, v in row.items() if k != "name"}
        for row in container.get("env", [])
    }


def package_policy(objects: list[dict[str, Any]], budget: dict[str, Any]) -> int:
    by = {(o["kind"], o["metadata"]["name"]): o for o in objects}
    deployments = {name: o for (kind, name), o in by.items() if kind == "Deployment"}
    require(set(deployments) == ROLES, "role-topology")
    require(not any(o["kind"] == "Secret" for o in objects), "no-committed-secret")
    policies = {n: o["spec"] for (kind, n), o in by.items() if kind == "NetworkPolicy"}
    require(
        policies.get("default-deny") == {"podSelector": {}, "policyTypes": ["Ingress", "Egress"]},
        "default-deny",
    )
    total = sum(budget["jobs"].values()) + sum(budget["future_clients"].values())
    for name, obj in deployments.items():
        spec = obj["spec"]
        pod = spec["template"]["spec"]
        require(pod.get("automountServiceAccountToken") is False, "token-automount")
        require(
            pod["serviceAccountName"] == name
            and by[("ServiceAccount", name)].get("automountServiceAccountToken") is False,
            "role-serviceaccount",
        )
        require(
            not any(pod.get(x) for x in ("hostNetwork", "hostPID", "hostIPC")), "host-isolation"
        )
        require(
            pod["securityContext"]
            == {
                "runAsNonRoot": True,
                "runAsUser": 65532,
                "runAsGroup": 65532,
                "fsGroup": 65532,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
            "pod-security",
        )
        require(pod.get("terminationGracePeriodSeconds") == 40, "drain-budget")
        require(bool(pod.get("topologySpreadConstraints")), "topology-spread")
        require(
            spec["replicas"] == budget["role_replicas"][name]
            and spec["strategy"]["rollingUpdate"]
            == {"maxSurge": budget["max_surges"][name], "maxUnavailable": 0},
            "surge-budget",
        )
        for c in pod["containers"] + pod.get("initContainers", []):
            require(
                bool(DIGEST.fullmatch(c["image"])) and "0" * 64 not in c["image"], "image-digest"
            )
            require(c.get("imagePullPolicy") == "Never", "loaded-image")
            require(
                c["securityContext"]
                == {
                    "allowPrivilegeEscalation": False,
                    "readOnlyRootFilesystem": True,
                    "capabilities": {"drop": ["ALL"]},
                },
                "container-security",
            )
            require(
                set(c.get("resources", {})) == {"requests", "limits"}
                and all(set(v) == {"cpu", "memory"} for v in c["resources"].values()),
                "resource-bounds",
            )
        c = pod["containers"][0]
        env = envmap(c)
        pool = budget["role_pools"][name]
        if name not in {"worker", "observer"}:
            require(env.get("RUNTIME_DB_MAX_CONNS") == {"value": str(pool)}, "explicit-pool")
            require(
                env["RUNTIME_ROLES"]
                == {"value": "scheduler,ingestor" if name == "scheduler" else name},
                "route-role",
            )
            require(
                env["RUNTIME_NAMESPACE"]
                == {"valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}}},
                "engine-namespace",
            )
        total += (spec["replicas"] + budget["max_surges"][name]) * (
            pool + (budget["relay_listen_per_process"] if name == "relay" else 0)
        )
        if name != "observer":
            require(("PodDisruptionBudget", name) in by, "pdb")
            for key, path in (
                ("startupProbe", "/healthz"),
                ("livenessProbe", "/healthz"),
                ("readinessProbe", "/readyz"),
            ):
                require(
                    c.get(key, {}).get("httpGet") == {"path": path, "port": "http"},
                    "probe-semantics",
                )
        if name in AUTH | {"demand"}:
            require(env.get("RUNTIME_TOKEN_AUTH") == {"value": "enabled"}, "auth-enabled")
        volumes = {v["name"]: v for v in pod.get("volumes", [])}
        require(not any("hostPath" in v for v in volumes.values()), "no-hostpath")
        if name in AUTH:
            require(
                env["RUNTIME_TOKEN_REVIEW_TOKEN_FILE"] == {"value": "/identity/reviewer"}
                and volumes["identity"]["projected"]["sources"][0]["serviceAccountToken"][
                    "expirationSeconds"
                ]
                == 3600,
                "reviewer-projection",
            )
            audience = (
                "durable-worker-control"
                if name == "worker-control"
                else ("durable-operator" if name == "operator" else "durable-client")
            )
            require(
                env["RUNTIME_TOKEN_AUDIENCE"] == {"value": audience}
                and env["RUNTIME_TOKEN_SERVICE_ACCOUNT"]
                == {"value": "worker" if name == "worker-control" else "observer"},
                "identity-boundary",
            )
        if name == "worker":
            require(
                volumes["identity"]["projected"]["sources"]
                == [
                    {
                        "serviceAccountToken": {
                            "audience": "durable-worker-control",
                            "expirationSeconds": 3600,
                            "path": "token",
                        }
                    }
                ],
                "worker-projection",
            )
        if name == "demand":
            require(
                env["RUNTIME_DEMAND_BEARER_TOKEN_FILE"] == {"value": "/bearer/token"}
                and volumes["bearer"].get("secret") == {"secretName": "campaign-demand"},
                "demand-secret-file",
            )
        require(
            policies.get(name + "-egress", {}).get("podSelector") == {"matchLabels": {"app": name}},
            "network-coverage",
        )
    rules = by[("ClusterRole", "dur053-tokenreview")]["rules"]
    require(
        rules
        == [
            {
                "apiGroups": ["authentication.k8s.io"],
                "resources": ["tokenreviews"],
                "verbs": ["create"],
            }
        ],
        "rbac-tokenreview",
    )
    binding = by[("ClusterRoleBinding", "dur053-tokenreview")]
    require(
        {s["name"] for s in binding["subjects"]} == AUTH
        and all(
            s["kind"] == "ServiceAccount"
            and s["namespace"]
            == next(o["metadata"]["name"] for o in objects if o["kind"] == "Namespace")
            for s in binding["subjects"]
        ),
        "rbac-subjects",
    )
    for target in ("api", "operator", "worker-control", "worker", "demand"):
        require(
            policies[target + "-ingress"]["podSelector"] == {"matchLabels": {"app": target}},
            "ingress-selector",
        )
    expected = {
        "api": ("observer", 8080),
        "operator": ("observer", 8080),
        "worker-control": ("worker", 8080),
        "worker": ("observer", 8081),
    }
    for target, (source, port) in expected.items():
        require(
            policies[target + "-ingress"]["ingress"]
            == [
                {
                    "from": [{"podSelector": {"matchLabels": {"app": source}}}],
                    "ports": [{"protocol": "TCP", "port": port}],
                }
            ],
            "network-boundary",
        )
    require(
        policies["demand-ingress"]["ingress"]
        == [
            {
                "from": [
                    {
                        "namespaceSelector": {
                            "matchLabels": {"kubernetes.io/metadata.name": "keda"}
                        },
                        "podSelector": {"matchLabels": {"app": "keda-operator"}},
                    }
                ],
                "ports": [{"protocol": "TCP", "port": 8091}],
            }
        ],
        "demand-network-boundary",
    )
    # Union semantics: an additional policy must not silently reopen a protected port.
    require(
        set(policies)
        == {
            "default-deny",
            "dns",
            "demand-ingress",
            *(n + "-ingress" for n in expected),
            *(n + "-egress" for n in ROLES | {"migrate", "compatibility"}),
        },
        "network-policy-set",
    )
    for name, policy in policies.items():
        for rule in policy.get("egress", []):
            require(bool(rule.get("to")) and bool(rule.get("ports")), "bounded-egress")
            for peer in rule["to"]:
                if "ipBlock" in peer:
                    require(
                        ipaddress.ip_network(peer["ipBlock"]["cidr"]).prefixlen == 32,
                        "external-endpoint-mask",
                    )
    for name in ("migrate", "compatibility"):
        job = by[("Job", name)]["spec"]
        pod = job["template"]["spec"]
        require(
            job["backoffLimit"] == 0
            and job["parallelism"] == 1
            and job["activeDeadlineSeconds"] == 180,
            "job-bounds",
        )
        require(
            pod["serviceAccountName"] == name
            and pod.get("automountServiceAccountToken") is False
            and pod["securityContext"]["runAsNonRoot"] is True,
            "job-security",
        )
        for c in pod["containers"] + pod.get("initContainers", []):
            require(
                bool(DIGEST.fullmatch(c["image"]))
                and c["securityContext"]["readOnlyRootFilesystem"]
                and c["securityContext"]["capabilities"] == {"drop": ["ALL"]},
                "job-images-security",
            )
        require(
            pod["containers"][0]["image"]
            == deployments["api"]["spec"]["template"]["spec"]["containers"][0]["image"],
            "candidate-runtime-image",
        )
    compat = by[("Job", "compatibility")]["spec"]["template"]["spec"]
    require(
        compat["initContainers"][0]["image"]
        == deployments["worker"]["spec"]["template"]["spec"]["containers"][0]["image"]
        and "default_registry" in " ".join(compat["initContainers"][0]["command"]),
        "candidate-worker-image",
    )
    so = by[("ScaledObject", "worker-demand")]["spec"]
    require(
        so["minReplicaCount"] == 1
        and so["maxReplicaCount"] == 4
        and so["pollingInterval"] == 5
        and so["fallback"] == {"failureThreshold": 3, "replicas": 2},
        "scaler-bounds",
    )
    trigger = so["triggers"][0]
    require(
        trigger
        == {
            "type": "metrics-api",
            "metricType": "AverageValue",
            "metadata": {
                "url": "http://demand."
                + by[("ScaledObject", "worker-demand")]["metadata"].get("namespace", "dur053-kind")
                + ".svc.cluster.local:8091/v1/demand",
                "format": "json",
                "valueLocation": "demand",
                "targetValue": "2",
                "authMode": "bearer",
                "unsafeSsl": "false",
            },
            "authenticationRef": {"name": "campaign-demand"},
        },
        "scaler-auth-metric",
    )
    require(
        by[("TriggerAuthentication", "campaign-demand")]["spec"]
        == {"secretTargetRef": [{"parameter": "token", "name": "campaign-demand", "key": "token"}]},
        "scaler-secret",
    )
    for direction, stabilization in (("scaleUp", 0), ("scaleDown", 60)):
        require(
            so["advanced"]["horizontalPodAutoscalerConfig"]["behavior"][direction]
            == {
                "stabilizationWindowSeconds": stabilization,
                "policies": [{"type": "Pods", "value": 1, "periodSeconds": 30}],
                "selectPolicy": "Min",
            },
            "scaler-behavior",
        )
    require(
        budget["max_connections"] == 100 and budget["reserved_and_admin"] == 8 and total + 8 <= 100,
        "connection-budget",
    )
    return total


def matched_arms(treatment: list[dict[str, Any]], baseline: list[dict[str, Any]]) -> None:
    def normalize(items: list[dict[str, Any]], legacy: bool) -> list[dict[str, Any]]:
        items = copy.deepcopy(items)
        for o in items:
            o["metadata"].pop("namespace", None)
            if o["kind"] == "Namespace":
                o["metadata"]["name"] = "ARM"
            if o["kind"] == "ScaledObject":
                url = o["spec"]["triggers"][0]["metadata"]["url"]
                o["spec"]["triggers"][0]["metadata"]["url"] = url.replace(
                    "dur053-kind", "ARM"
                ).replace("dur053-baseline", "ARM")
            if o["kind"] == "ClusterRoleBinding":
                for subject in o["subjects"]:
                    subject["namespace"] = "ARM"
            if o["kind"] == "Deployment" and o["metadata"]["name"] != "observer":
                c = o["spec"]["template"]["spec"]["containers"][0]
                flag = (
                    "WORKER_DRAIN_MODE"
                    if o["metadata"]["name"] == "worker"
                    else "RUNTIME_SHUTDOWN_MODE"
                )
                flags = [e for e in c["env"] if e["name"] == flag]
                require(
                    flags == ([{"name": flag, "value": "legacy"}] if legacy else []), "legacy-flags"
                )
                c["env"] = [e for e in c["env"] if e["name"] != flag]
        return sorted(items, key=lambda o: (o["kind"], o["metadata"]["name"]))

    require(normalize(treatment, False) == normalize(baseline, True), "matched-arms")


def ledger_check(ledger: dict[str, Any], registered: int, prior_jobs: set[str]) -> int:
    require(
        ledger.get("schema") == "dur053.live-ledger.v1" and ledger.get("limit_seconds") == 259200,
        "ledger-schema",
    )
    require(type(registered) is int and 0 < registered <= 18900, "job-live-ceiling")
    spent, seen = 0, set()
    for row in ledger["entries"]:
        job = row["job_id"]
        require(job not in seen and str(job).isdigit(), "ledger-identity")
        require(
            all(
                row.get(k)
                for k in (
                    "run_id",
                    "head_sha",
                    "merge_ref_sha",
                    "tree_sha",
                    "artifact_sha256",
                    "status",
                )
            ),
            "ledger-provenance",
        )
        require(
            type(row["charged_seconds"]) is int and 0 < row["charged_seconds"] <= 18900,
            "ledger-duration",
        )
        spent += row["charged_seconds"]
        seen.add(job)
    require(prior_jobs <= seen, "uncommitted-live-evidence")
    require(spent + registered <= 259200, "ledger-budget")
    return 259200 - spent - registered


def committed_ledger(path: Path, repository: Path = ROOT) -> dict[str, Any]:
    ledger = json.loads(path.read_text())
    for row in ledger["entries"]:
        artifact = (repository / row["artifact"]).resolve()
        require(
            artifact.is_relative_to(repository.resolve()) and artifact.is_file(),
            "committed-ledger-artifact",
        )
        raw = artifact.read_bytes()
        require(hashlib.sha256(raw).hexdigest() == row["artifact_sha256"], "committed-ledger-hash")
        manifest = json.loads(raw)
        require(
            all(
                str(manifest[key]) == str(row[key])
                for key in ("run_id", "job_id", "head_sha", "merge_ref_sha", "tree_sha", "status")
            )
            and (
                manifest["elapsed_live_seconds"]
                if row.get("category") == "k3-acceptance-smoke"
                else manifest.get("charged_seconds", manifest["registered_live_seconds"])
            )
            == row["charged_seconds"],
            "committed-ledger-provenance",
        )
    return ledger


def stable_worker_sessions(expected: set[str], pods: list[dict[str, Any]]) -> None:
    require(
        len(expected) == 2
        and {p["metadata"]["uid"] for p in pods} == expected
        and all(bool(p["status"].get("containerStatuses")) for p in pods)
        and all(c["restartCount"] == 0 for p in pods for c in p["status"]["containerStatuses"]),
        "stable-worker-sessions",
    )


def single_witness_session(files: list[str], inventory: dict[str, Any]) -> None:
    require(files == [inventory["file"]], "single-witness-session")


def application_topics_absent(listing: str) -> bool:
    return not {"durable-agent.tasks.v1", "durable-agent.events.v1"}.intersection(
        listing.splitlines()
    )


def worker_group_absent(listing: str) -> bool:
    return "runtime-workers-v1" not in listing.splitlines()


def running_image_digests(status: list[dict[str, Any]], images: dict[str, str]) -> None:
    require(
        all(
            c["imageID"].split("@")[-1] in {v.split("@")[-1] for v in images.values()}
            for p in status
            for c in p["containers"]
        ),
        "running-image-digest",
    )


def terminal_report(report: dict[str, Any]) -> None:
    require(report["valid"] is True, "terminal-checker")


def postgres_budget_setting(value: int) -> None:
    require(value == 100, "postgres-budget-setting")


def scratch_ownership(scratch: Path, root: Path) -> None:
    require(
        scratch.resolve().parent == root.resolve() and scratch.name.startswith("kind-"),
        "scratch-ownership",
    )


def supported(scenario: str) -> None:
    require(scenario in {"package-check", "smoke", "cleanup"}, "K3b: scenario not implemented")


def artifact_hashes(output: Path) -> dict[str, str]:
    return {
        p.relative_to(output).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(output.rglob("*"))
        if p.is_file() and p != output / "manifest.json"
    }


def fresh_output(output: Path) -> None:
    require(not output.exists(), "existing-output")
    require(not output.resolve().is_relative_to(ROOT), "evidence-outside-checkout")
    output.mkdir(parents=True, exist_ok=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rendered", type=Path, required=True)
    args = parser.parse_args()
    import yaml

    objects = list(yaml.safe_load_all(args.rendered.read_text()))
    print(
        "application maximum connections:",
        package_policy(objects, json.loads((K8S / "connection-budget.json").read_text())),
    )


if __name__ == "__main__":
    main()
