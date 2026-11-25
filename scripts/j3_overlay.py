"""Render the private overlay and its fixed target labels."""

import ipaddress
import json
import re
from pathlib import Path

from j2_overlay import PROMETHEUS, labels, obj, registered_budget, selector, workload
from j3_rules import render as rules

ROOT = Path(__file__).resolve().parents[1]
J3 = ROOT / "experiments/portfolio/observability"
NS, APP, TARGET, FOREIGN = "j3-telemetry", "dur053-kind", "j3-target", "j3-foreign"
EXPORTER, PROM, GRAFANA, ALERTMANAGER, RECEIVER = (
    "j3-exporter",
    "j3-prometheus",
    "j3-grafana",
    "j3-alertmanager",
    "j3-receiver",
)
PINS = {
    PROM: PROMETHEUS,
    GRAFANA: "grafana/grafana:12.4.1@sha256:"
    "e932bd6ed0e026595b08483cd0141e5103e1ab7ff8604839ff899b8dc54cabcb",
    ALERTMANAGER: "prom/alertmanager:v0.31.1@sha256:"
    "88b605de9aba0410775c1eb3438f951115054e0d307f23f274a4c705f51630c1",
}
PORTS = {EXPORTER: 9187, PROM: 9090, GRAFANA: 3000, ALERTMANAGER: 9093, RECEIVER: 8080}


def peer(name, namespace=NS):
    return {
        "podSelector": selector(name),
        "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": namespace}},
    }


def route(direction, target, port, protocol="TCP"):
    return {direction: [target], "ports": [{"port": port, "protocol": protocol}]}


def network(name, namespace, pod_selector, **rules):
    return obj(
        "networking.k8s.io/v1",
        "NetworkPolicy",
        name,
        namespace,
        spec={"podSelector": pod_selector, "policyTypes": [k.title() for k in rules], **rules},
    )


def render(db_ip, target, images):
    if set(images) != PORTS.keys() or any(
        not re.fullmatch(r"[^@\s]+@sha256:[a-f0-9]{64}", i) for i in images.values()
    ):
        raise ValueError("immutable overlay images required")
    manifest = []

    def deploy(name, uid, resources, *, cfg=None, paths=None, storage=None, **container):
        mounts, volumes = [], []
        if cfg:
            manifest.append(
                obj(
                    "v1",
                    "ConfigMap",
                    name,
                    NS,
                    data={
                        k: v if isinstance(v, str) else json.dumps(v, indent=2)
                        for k, v in cfg.items()
                    },
                )
            )
            volumes.append({"name": "config", "configMap": {"name": name}})
            for key, path in (paths or {None: "/etc/j3"}).items():
                mounts.append(
                    {
                        "name": "config",
                        "mountPath": path,
                        "readOnly": True,
                        **({"subPath": key} if key else {}),
                    }
                )
        if storage:
            mounts.append({"name": "storage", "mountPath": storage})
            volumes.append({"name": "storage", "emptyDir": {}})
        deployment = workload(
            NS, name, images[name], PORTS[name], resources, volumeMounts=mounts, **container
        )
        pod = deployment["spec"]["template"]["spec"]
        pod.update(volumes=volumes, securityContext={"fsGroup": uid})
        pod["containers"][0]["securityContext"]["runAsUser"] = uid
        ports = (
            [{"port": PORTS[name]}]
            if name != RECEIVER
            else [{"name": "notify", "port": 8080}, {"name": "metrics", "port": 8082}]
        )
        manifest.extend(
            [
                deployment,
                obj(
                    "v1",
                    "Service",
                    name,
                    NS,
                    spec={"type": "ClusterIP", "selector": labels(name), "ports": ports},
                ),
            ]
        )

    small = ("50m", "32Mi", "250m", "128Mi")
    deploy(
        EXPORTER,
        65532,
        ("100m", "64Mi", "500m", "256Mi"),
        storage="/evidence",
        envFrom=[{"secretRef": {"name": "j3-monitor"}}],
        env=[
            {"name": "J3_NAMESPACE", "value": target["namespace"]},
            {"name": "J3_DEMAND_URL", "value": f"http://demand.{APP}:8091/v1/demand"},
        ],
        imagePullPolicy="Never",
    )
    deploy(
        PROM,
        65534,
        ("200m", "256Mi", "1000m", "1Gi"),
        storage="/prometheus",
        cfg={
            "rules.yml": rules(target),
            "prometheus.yml": {
                "global": {
                    "scrape_interval": "1s",
                    "scrape_timeout": "1s",
                    "evaluation_interval": "5s",
                },
                "rule_files": ["/etc/j3/rules.yml"],
                "scrape_configs": [
                    {
                        "job_name": "j3",
                        "static_configs": [{"targets": [f"{EXPORTER}:9187"], "labels": target}],
                    },
                    {
                        "job_name": "j3-observer",
                        "static_configs": [{"targets": [f"{RECEIVER}:8082"], "labels": target}],
                    },
                ],
                "alerting": {
                    "alertmanagers": [{"static_configs": [{"targets": [f"{ALERTMANAGER}:9093"]}]}]
                },
            },
        },
        args=["--config.file=/etc/j3/prometheus.yml", "--storage.tsdb.path=/prometheus"],
    )
    dashboard = json.loads((J3 / "dashboard.json").read_bytes())
    for variable in dashboard["templating"]["list"]:
        value = target[variable["name"]]
        variable.update(query=value, current={"text": value, "value": value})
    deploy(
        GRAFANA,
        472,
        ("100m", "256Mi", "500m", "512Mi"),
        storage="/var/lib/grafana",
        cfg={
            "datasource.yml": (J3 / "datasource.yml").read_text(encoding="utf-8"),
            "provider.yml": {
                "apiVersion": 1,
                "providers": [
                    {
                        "name": "J3",
                        "type": "file",
                        "disableDeletion": True,
                        "allowUiUpdates": False,
                        "options": {"path": "/etc/j3-dashboards"},
                    }
                ],
            },
            "dashboard.json": dashboard,
        },
        paths={
            "datasource.yml": "/etc/grafana/provisioning/datasources/j3.yml",
            "provider.yml": "/etc/grafana/provisioning/dashboards/j3.yml",
            "dashboard.json": "/etc/j3-dashboards/j3.json",
        },
        env=[
            {"name": key, "value": value}
            for key, value in {
                "GF_AUTH_ANONYMOUS_ENABLED": "true",
                "GF_AUTH_ANONYMOUS_ORG_ROLE": "Viewer",
                "GF_USERS_ALLOW_SIGN_UP": "false",
                "GF_SECURITY_DISABLE_INITIAL_ADMIN_CREATION": "true",
                "GF_ANALYTICS_REPORTING_ENABLED": "false",
                "GF_ANALYTICS_CHECK_FOR_UPDATES": "false",
            }.items()
        ],
    )
    deploy(
        ALERTMANAGER,
        65534,
        small,
        storage="/alertmanager",
        cfg={"alertmanager.yml": (J3 / "alertmanager.yml").read_text(encoding="utf-8")},
        args=[
            "--config.file=/etc/j3/alertmanager.yml",
            "--storage.path=/alertmanager",
            "--cluster.listen-address=",
        ],
    )
    deploy(
        RECEIVER,
        65532,
        small,
        storage="/evidence",
        cfg={"receiver.py": (J3 / "receiver.py").read_text(encoding="utf-8")},
        command=["python", "/etc/j3/receiver.py"],
        imagePullPolicy="Never",
    )
    dns = peer("kube-dns", "kube-system")
    dns["podSelector"] = {"matchLabels": {"k8s-app": "kube-dns"}}
    dns_routes = [route("to", dns, 53, protocol) for protocol in ("UDP", "TCP")]
    manifest += [
        network("j3-default-deny", NS, {}, ingress=[], egress=[]),
        network(
            EXPORTER,
            NS,
            selector(EXPORTER),
            ingress=[route("from", peer(PROM), 9187)],
            egress=[
                route("to", {"ipBlock": {"cidr": f"{ipaddress.IPv4Address(db_ip)}/32"}}, 5432),
                route("to", peer("demand", APP), 8091),
                *dns_routes,
            ],
        ),
        network(
            "j3-demand-read", APP, selector("demand"), ingress=[route("from", peer(EXPORTER), 8091)]
        ),
        network(
            PROM,
            NS,
            selector(PROM),
            ingress=[route("from", peer(GRAFANA), 9090)],
            egress=[route("to", peer(n), PORTS[n]) for n in (EXPORTER, ALERTMANAGER)]
            + [route("to", peer(RECEIVER), 8082), *dns_routes],
        ),
        network(
            GRAFANA, NS, selector(GRAFANA), egress=[route("to", peer(PROM), 9090), *dns_routes]
        ),
        network(
            ALERTMANAGER,
            NS,
            selector(ALERTMANAGER),
            ingress=[route("from", peer(PROM), 9093)],
            egress=[route("to", peer(RECEIVER), 8080), *dns_routes],
        ),
        network(
            RECEIVER,
            NS,
            selector(RECEIVER),
            ingress=[route("from", peer(ALERTMANAGER), 8080), route("from", peer(PROM), 8082)],
        ),
    ]
    return {"apiVersion": "v1", "kind": "List", "items": manifest}


def budget():
    registered = registered_budget()
    pools = sum(
        (replicas + registered["max_surges"][role])
        * (
            registered["role_pools"][role]
            + (registered["relay_listen_per_process"] if role == "relay" else 0)
        )
        for role, replicas in registered["role_replicas"].items()
    )
    total = (
        pools
        + sum(registered["jobs"].values())
        + sum(registered["future_clients"].values())
        + registered["reserved_and_admin"]
    )
    if (total, registered["max_connections"] - total) != (82, 18):
        raise ValueError("registered DB budget differs")
    return {
        "used_including_admin": 82,
        "free": 18,
        "monitor_sessions": {name: int(name == EXPORTER) for name in PORTS},
    }
