"""Render the K6 Phase B engineering-only identity and probe topology."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
APPROVER = "k6-approver"
BINDING = "k6-approval-binding"
REVIEWER = "k6-reviewer"
NO_REVIEW = "k6-no-review"
WORKER = "k6-worker"
PROBE = "k6-grant-probe"
ISSUER = "https://kubernetes.default.svc.cluster.local"
RBAC_VERSION = "rbac.authorization.k8s.io/v1"
REVIEW_RULE = {
    "apiGroups": ["authentication.k8s.io"],
    "resources": ["tokenreviews"],
    "verbs": ["create"],
}


def obj(kind: str, name: str, namespace: str | None = None, **fields: Any) -> dict:
    version = RBAC_VERSION if kind.startswith("ClusterRole") else "v1"
    metadata = {"name": name}
    if namespace:
        metadata["namespace"] = namespace
    return {"apiVersion": version, "kind": kind, "metadata": metadata, **fields}


def render(namespace: str, runtime_image: str, probe_image: str) -> list[dict]:
    base = json.loads((ROOT / "deploy/k8s/base/resources.yaml").read_bytes())["items"]
    migration = copy.deepcopy(
        next(
            item for item in base if item["kind"] == "Job" and item["metadata"]["name"] == "migrate"
        )
    )
    migration["metadata"]["namespace"] = namespace
    migration["spec"]["template"]["spec"]["containers"][0]["image"] = runtime_image
    role = namespace + "-review"
    return [
        obj("Namespace", namespace),
        *(
            obj("ServiceAccount", name, namespace, automountServiceAccountToken=False)
            for name in (APPROVER, REVIEWER, NO_REVIEW, WORKER, "migrate")
        ),
        obj("Secret", BINDING, namespace, type="Opaque", immutable=True),
        obj("ClusterRole", role, rules=[copy.deepcopy(REVIEW_RULE)]),
        obj(
            "ClusterRoleBinding",
            role,
            roleRef={"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": role},
            subjects=[{"kind": "ServiceAccount", "name": REVIEWER, "namespace": namespace}],
        ),
        obj(
            "Pod",
            PROBE,
            namespace,
            spec={
                "automountServiceAccountToken": False,
                "restartPolicy": "Never",
                "securityContext": {
                    "runAsNonRoot": True,
                    "runAsUser": 65532,
                    "runAsGroup": 65532,
                    "seccompProfile": {"type": "RuntimeDefault"},
                },
                "containers": [
                    {
                        "name": "probe",
                        "image": probe_image,
                        "imagePullPolicy": "Never",
                        "command": ["sleep", "1800"],
                        "securityContext": {
                            "allowPrivilegeEscalation": False,
                            "capabilities": {"drop": ["ALL"]},
                        },
                    }
                ],
            },
        ),
        migration,
    ]


def validate_rendered(objects: list[dict], namespace: str) -> None:
    role = namespace + "-review"
    keys = [(item["kind"], item["metadata"]["name"]) for item in objects]
    if any("operator" in json.dumps(item).lower() for item in objects):
        raise ValueError("K6_OPERATOR_OBJECT_OR_MOUNT")
    expected = {
        ("Namespace", namespace),
        ("Secret", BINDING),
        ("Pod", PROBE),
        ("Job", "migrate"),
        ("ClusterRole", role),
        ("ClusterRoleBinding", role),
        *(("ServiceAccount", name) for name in (APPROVER, REVIEWER, NO_REVIEW, WORKER, "migrate")),
    }
    if len(keys) != len(expected) or set(keys) != expected:
        raise ValueError("K6_RENDERED_OBJECT_INVENTORY")
    for item in objects:
        kind = item["kind"]
        if (
            kind not in {"Namespace", "ClusterRole", "ClusterRoleBinding"}
            and item["metadata"].get("namespace") != namespace
        ):
            raise ValueError("K6_RENDERED_NAMESPACE")
        if kind == "ClusterRole" and item.get("rules") != [
            {
                "apiGroups": ["authentication.k8s.io"],
                "resources": ["tokenreviews"],
                "verbs": ["create"],
            }
        ]:
            raise ValueError("K6_REVIEWER_RULE")
        if kind == "ClusterRoleBinding" and (
            item.get("roleRef")
            != {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": role}
            or item.get("subjects")
            != [{"kind": "ServiceAccount", "name": REVIEWER, "namespace": namespace}]
        ):
            raise ValueError("K6_REVIEWER_SUBJECT")
        if kind == "ServiceAccount" and item.get("automountServiceAccountToken") is not False:
            raise ValueError("K6_IDENTITY_AUTOMOUNT")
        if kind in {"Job", "Pod"}:
            spec = item["spec"]
            if kind == "Job":
                spec = spec["template"]["spec"]
            if (
                spec.get("automountServiceAccountToken") is not False
                or spec.get("serviceAccountName") in {APPROVER, REVIEWER, NO_REVIEW}
                or '"serviceAccountToken"' in json.dumps(spec)
            ):
                raise ValueError("K6_POD_TOKEN_MOUNT")


def deny_egress(namespace: str) -> dict:
    policy = obj(
        "NetworkPolicy",
        "k6-default-deny-egress",
        namespace,
        spec={"podSelector": {}, "policyTypes": ["Egress"], "egress": []},
    )
    policy["apiVersion"] = "networking.k8s.io/v1"
    return policy
