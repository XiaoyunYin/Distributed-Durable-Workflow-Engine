"""Render the J-only telemetry objects and account for their DB client."""

from __future__ import annotations

import ipaddress
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
J2 = ROOT / "experiments/portfolio/load-envelope"
PROMETHEUS = "prom/prometheus:v3.13.0@sha256:c6b27ea434f8389bfe233fbc7be381cf50587c286e871bc842008f5a1b1908a7"
EXPORTER = "j2-exporter"
PROM = "j2-prometheus"


def labels(name):
    return {"app": name}


def selector(name):
    return {"matchLabels": labels(name)}


def obj(api, kind, name, namespace, **contents):
    return {
        "apiVersion": api,
        "kind": kind,
        "metadata": {"name": name, "namespace": namespace},
        **contents,
    }


def workload(namespace, name, image, port, resources, **container):
    return obj(
        "apps/v1",
        "Deployment",
        name,
        namespace,
        spec={
            "replicas": 1,
            "strategy": {"type": "Recreate"},
            "selector": selector(name),
            "template": {
                "metadata": {"labels": labels(name)},
                "spec": {
                    "automountServiceAccountToken": False,
                    "containers": [
                        {
                            "name": name,
                            "image": image,
                            "ports": [{"containerPort": port}],
                            "resources": {
                                "requests": dict(
                                    zip(("cpu", "memory"), resources[:2], strict=True)
                                ),
                                "limits": dict(zip(("cpu", "memory"), resources[2:], strict=True)),
                            },
                            "securityContext": {
                                "runAsNonRoot": True,
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            **container,
                        }
                    ],
                },
            },
        },
    )


def render(namespace, db_ip, exporter_image):
    if not re.fullmatch(r"j2-[a-z0-9-]+[a-z0-9]", namespace):
        raise ValueError("a dedicated J2 namespace is required")
    if not re.fullmatch(r"[^@\s]+@sha256:[0-9a-f]{64}", exporter_image):
        raise ValueError("exporter image must have an immutable manifest digest")
    db_cidr = f"{ipaddress.IPv4Address(db_ip)}/32"
    exporter = workload(
        namespace,
        EXPORTER,
        exporter_image,
        9187,
        ("100m", "64Mi", "500m", "256Mi"),
        env=[
            {
                "name": "J2_MONITOR_DSN",
                "valueFrom": {"secretKeyRef": {"name": "j2-monitor", "key": "dsn"}},
            }
        ],
    )
    prom = workload(
        namespace,
        PROM,
        PROMETHEUS,
        9090,
        ("200m", "256Mi", "1000m", "1Gi"),
        args=["--config.file=/etc/j2/prometheus.yml", "--storage.tsdb.path=/prometheus"],
        volumeMounts=[{"name": "config", "mountPath": "/etc/j2", "readOnly": True}],
    )
    prom["spec"]["template"]["spec"]["volumes"] = [
        {"name": "config", "configMap": {"name": PROM}},
    ]
    # Let the image's configured numeric uid create the TSDB on an ephemeral volume.
    pod = prom["spec"]["template"]["spec"]
    pod["securityContext"] = {"fsGroup": 65534}
    pod["volumes"].append({"name": "storage", "emptyDir": {}})
    pod["containers"][0]["volumeMounts"].append({"name": "storage", "mountPath": "/prometheus"})
    pod["containers"][0]["securityContext"]["runAsUser"] = 65534
    config = obj(
        "v1",
        "ConfigMap",
        PROM,
        namespace,
        data={
            "prometheus.yml": """global:
  scrape_interval: 1s
  scrape_timeout: 1s
scrape_configs:
  - job_name: j2
    static_configs:
      - targets: [j2-exporter:9187]
"""
        },
    )
    service = obj(
        "v1",
        "Service",
        EXPORTER,
        namespace,
        spec={
            "type": "ClusterIP",
            "selector": labels(EXPORTER),
            "ports": [{"port": 9187}],
        },
    )

    def policy(name, select, **rules):
        return obj(
            "networking.k8s.io/v1",
            "NetworkPolicy",
            name,
            namespace,
            spec={
                "podSelector": select,
                "policyTypes": ["Ingress", "Egress"],
                **rules,
            },
        )

    scrape = [{"port": 9187, "protocol": "TCP"}]
    exporter_allow = policy(
        "j2-exporter-allow",
        selector(EXPORTER),
        ingress=[
            {"from": [{"podSelector": selector(PROM)}], "ports": scrape},
        ],
        egress=[
            {"to": [{"ipBlock": {"cidr": db_cidr}}], "ports": [{"port": 5432, "protocol": "TCP"}]}
        ],
    )
    prom_allow = policy(
        "j2-prometheus-allow",
        selector(PROM),
        egress=[
            {"to": [{"podSelector": selector(EXPORTER)}], "ports": scrape},
            {
                "to": [
                    {
                        "namespaceSelector": {
                            "matchLabels": {"kubernetes.io/metadata.name": "kube-system"}
                        },
                        "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}},
                    }
                ],
                "ports": [{"port": 53, "protocol": proto} for proto in ("UDP", "TCP")],
            },
        ],
    )
    items = [
        config,
        service,
        policy("j2-default-deny", {}),
        exporter_allow,
        prom_allow,
        exporter,
        prom,
    ]
    return {"apiVersion": "v1", "kind": "List", "items": items}


def budget(rendered, registered):
    deployments = {
        o["metadata"]["name"]: o["spec"] for o in rendered["items"] if o["kind"] == "Deployment"
    }
    if set(deployments) != {EXPORTER, PROM} or any(
        spec["replicas"] != 1 or spec["strategy"] != {"type": "Recreate"}
        for spec in deployments.values()
    ):
        raise ValueError("J2 monitor requires one exporter and Prometheus with no surge")
    reserved = registered["future_clients"]["monitor"]
    pools = sum(
        (replicas + registered["max_surges"][role])
        * (
            registered["role_pools"][role]
            + (registered["relay_listen_per_process"] if role == "relay" else 0)
        )
        for role, replicas in registered["role_replicas"].items()
    )
    worst = pools + sum(registered["jobs"].values()) + sum(registered["future_clients"].values())
    total = worst + registered["reserved_and_admin"]
    free = registered["max_connections"] - total
    if (reserved, worst, total, free) != (2, 74, 82, 18):
        raise ValueError("registered DB connection budget differs")
    return {
        "j2_exporter": 1,
        "prometheus": 0,
        "unused_monitor_slots": reserved - 1,
        "worst_case": worst,
        "with_admin": total,
        "free": free,
    }


def registered_budget():
    return json.loads((ROOT / "deploy/k8s/connection-budget.json").read_bytes())
