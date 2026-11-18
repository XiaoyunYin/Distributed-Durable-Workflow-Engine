"""Render and audit the Phase C engineering topology in its own namespace."""

from __future__ import annotations

import ipaddress
import json

from dur053_k3_policy import DIGEST, K8S
from dur053_k6_identity import WORKER, obj

SINK = "k6-sink"
RELAY = "k6-effect-relay"
ENGINE = "k6-engine-peer"
CLIENT = "k6-effect-worker"
PORT = 8443
SINK_POOL = 4
SINK_SETUP = {"migrate": 2, "checker": 2, "exporter": 1}
SINK_USERS = {"k6_sink_service": 8, "k6_sink_exporter": 1}


def labels(name: str) -> dict:
    return {"app": name}


def selector(name: str) -> dict:
    return {"matchLabels": labels(name)}


def container(name: str, image: str, command: list[str]) -> dict:
    return {
        "name": name,
        "image": image,
        "imagePullPolicy": "Never",
        "command": command,
        "securityContext": {
            "allowPrivilegeEscalation": False,
            "readOnlyRootFilesystem": True,
            "capabilities": {"drop": ["ALL"]},
        },
        "resources": {
            "requests": {"cpu": "25m", "memory": "24Mi"},
            "limits": {"cpu": "1", "memory": "128Mi"},
        },
    }


def pod_spec(name: str, image: str) -> dict:
    spec = {
        "automountServiceAccountToken": False,
        "terminationGracePeriodSeconds": 35,
        "securityContext": {
            "runAsNonRoot": True,
            "runAsUser": 65532,
            "runAsGroup": 65532,
            "fsGroup": 65532,
            "seccompProfile": {"type": "RuntimeDefault"},
        },
    }
    if name in {CLIENT, ENGINE}:
        spec["containers"] = [container(name, image, ["/probe", "wait"])]
        if name == CLIENT:
            spec.update(
                serviceAccountName=WORKER,
                volumes=[
                    {
                        "name": "worker-identity",
                        "projected": {
                            "sources": [
                                {
                                    "serviceAccountToken": {
                                        "audience": "dur053-k6-sink",
                                        "expirationSeconds": 600,
                                        "path": "token",
                                    }
                                }
                            ]
                        },
                    }
                ],
            )
            spec["containers"][0]["volumeMounts"] = [
                {"name": "worker-identity", "mountPath": "/identity", "readOnly": True}
            ]
        return spec
    filename = "/private/config.json"
    install = ["/probe", "install", "/secret/config.json", filename]
    if name == SINK:
        install += [
            "/secret/engine.crt",
            "/private/engine.crt",
            "/secret/sink-db.crt",
            "/private/sink-db.crt",
        ]
    init = container("install-config", image, install)
    init["volumeMounts"] = [
        {"name": "private", "mountPath": "/private"},
        {"name": "config", "mountPath": "/secret", "readOnly": True},
    ]
    command = ["/sink", "serve", filename] if name == SINK else ["/relay", filename]
    handler = container(name, image, command)
    handler["volumeMounts"] = [{"name": "private", "mountPath": "/private", "readOnly": True}]
    handler["readinessProbe"] = {"tcpSocket": {"port": PORT}, "periodSeconds": 1}
    spec.update(
        containers=[handler],
        initContainers=[init],
        volumes=[
            {"name": "config", "secret": {"secretName": name + "-config", "defaultMode": 288}},
            {"name": "private", "emptyDir": {"medium": "Memory", "sizeLimit": "1Mi"}},
        ],
    )
    return spec


def policy(namespace: str, name: str, selected: str, direction: str, rules: list[dict]) -> dict:
    result = obj(
        "NetworkPolicy",
        name,
        namespace,
        spec={
            "podSelector": selector(selected),
            "policyTypes": [direction],
            direction.lower(): rules,
        },
    )
    result["apiVersion"] = "networking.k8s.io/v1"
    return result


def traffic(direction: str, port: int, *peers: dict) -> dict:
    return {direction: list(peers), "ports": [{"protocol": "TCP", "port": port}]}


def pod_peer(name: str) -> dict:
    return {"podSelector": selector(name)}


def ip_peer(address: str) -> dict:
    return {"ipBlock": {"cidr": f"{ipaddress.IPv4Address(address)}/32"}}


def default_deny(namespace: str) -> dict:
    result = obj(
        "NetworkPolicy",
        "k6-c-default-deny",
        namespace,
        spec={"podSelector": {}, "policyTypes": ["Ingress", "Egress"]},
    )
    result["apiVersion"] = "networking.k8s.io/v1"
    return result


def render(namespace: str, image: str, endpoints: dict[str, str]) -> list[dict]:
    objects = []
    for name in (SINK, RELAY):
        deployment = obj(
            "Deployment",
            name,
            namespace,
            spec={
                "replicas": 1,
                "strategy": (
                    {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": 1, "maxUnavailable": 0}}
                    if name == SINK
                    else {"type": "Recreate"}
                ),
                "selector": selector(name),
                "template": {"metadata": {"labels": labels(name)}, "spec": pod_spec(name, image)},
            },
        )
        deployment["apiVersion"] = "apps/v1"
        objects += [
            deployment,
            obj(
                "Service",
                name,
                namespace,
                spec={
                    "type": "ClusterIP",
                    "selector": labels(name),
                    "ports": [{"port": PORT, "targetPort": PORT}],
                },
            ),
        ]
    for name in (ENGINE, CLIENT):
        pod = obj("Pod", name, namespace, spec=pod_spec(name, image))
        pod["metadata"]["labels"] = labels(name)
        objects.append(pod)
    return [
        *objects,
        default_deny(namespace),
        policy(
            namespace, "worker-effect", CLIENT, "Egress", [traffic("to", PORT, pod_peer(RELAY))]
        ),
        policy(
            namespace, "engine-allocate", ENGINE, "Egress", [traffic("to", PORT, pod_peer(SINK))]
        ),
        policy(
            namespace, "relay-accept", RELAY, "Ingress", [traffic("from", PORT, pod_peer(CLIENT))]
        ),
        policy(
            namespace,
            "relay-forward-and-witness",
            RELAY,
            "Egress",
            [
                traffic("to", PORT, pod_peer(SINK)),
                traffic("to", int(endpoints["witness_port"]), ip_peer(endpoints["witness"])),
            ],
        ),
        policy(
            namespace,
            "sink-accept",
            SINK,
            "Ingress",
            [
                traffic("from", PORT, pod_peer(RELAY), pod_peer(ENGINE)),
            ],
        ),
        policy(
            namespace,
            "sink-dependencies",
            SINK,
            "Egress",
            [
                traffic("to", 5432, ip_peer(endpoints["sink_db"]), ip_peer(endpoints["engine_db"])),
                traffic("to", 443, ip_peer(endpoints["api_service"])),
                traffic("to", 6443, ip_peer(endpoints["api_endpoint"])),
            ],
        ),
        policy(
            namespace,
            "engine-migration",
            "migrate",
            "Egress",
            [
                traffic("to", 5432, ip_peer(endpoints["engine_db"])),
            ],
        ),
    ]


def require(condition: bool, code: str) -> None:
    if not condition:
        raise ValueError(code)


def verify_budget(objects: list[dict], budget: dict) -> dict:
    workloads = [
        item
        for item in objects
        if item["kind"]
        in {
            "Pod",
            "Deployment",
            "Job",
            "CronJob",
            "DaemonSet",
            "StatefulSet",
            "ReplicaSet",
            "ReplicationController",
        }
    ]
    clients = {(item["kind"], item["metadata"]["name"]): item for item in workloads}
    expected = {("Deployment", SINK), ("Deployment", RELAY), ("Pod", ENGINE), ("Pod", CLIENT)}
    require(
        set(clients) == expected and len(workloads) == len(expected), "UNACCOUNTED_SINK_DB_CLIENT"
    )
    support = [item for item in objects if item not in workloads]
    require(
        len(support) == 10
        and {item["kind"] for item in support}
        == {
            "Service",
            "NetworkPolicy",
        },
        "K6_C_OBJECT_INVENTORY",
    )
    require(
        budget
        == {
            "max_connections": 30,
            "replicas": 1,
            "max_surge": 1,
            "pool_per_process": SINK_POOL,
            "jobs": SINK_SETUP,
            "administration": 3,
            "reserved": 3,
        },
        "K6_C_REGISTERED_SINK_BUDGET",
    )
    for client in sorted(expected):
        workload = clients[client]
        name = workload["metadata"]["name"]
        spec = workload["spec"]
        if workload["kind"] == "Deployment":
            is_sink = name == SINK
            strategy = (
                {
                    "type": "RollingUpdate",
                    "rollingUpdate": {"maxSurge": budget["max_surge"], "maxUnavailable": 0},
                }
                if is_sink
                else {"type": "Recreate"}
            )
            require(spec["replicas"] == 1 and spec["strategy"] == strategy, "K6_C_SURGE_BUDGET")
            require(
                spec["selector"] == selector(name)
                and spec["template"]["metadata"]["labels"] == labels(name),
                "K6_C_POD_SELECTOR",
            )
            spec = spec["template"]["spec"]
        image = spec["containers"][0]["image"]
        require(bool(DIGEST.fullmatch(image)), "K6_C_IMAGE_DIGEST")
        require(spec == pod_spec(name, image), "UNACCOUNTED_SINK_DB_POD_CONFIG")
    committed = (
        (budget["replicas"] + budget["max_surge"]) * budget["pool_per_process"]
        + sum(budget["jobs"].values())
        + budget["administration"]
        + budget["reserved"]
    )
    require(committed == 19, "K6_C_SINK_CONNECTION_TOTAL")
    return {"committed": committed, "max_connections": 30, "headroom": 30 - committed}


def budget_for(objects: list[dict]) -> dict:
    budget = json.loads((K8S / "connection-budget.json").read_bytes())
    engine = (
        sum(budget["jobs"].values())
        + sum(budget["future_clients"].values())
        + sum(
            (budget["role_replicas"][role] + budget["max_surges"][role])
            * (pool + (budget["relay_listen_per_process"] if role == "relay" else 0))
            for role, pool in budget["role_pools"].items()
        )
    )
    require(
        engine == 74 and budget["max_connections"] == 100 and budget["reserved_and_admin"] == 8,
        "K6_C_ENGINE_CONNECTION_TOTAL",
    )
    return {
        "engine": {"committed": engine + 8, "max_connections": 100, "headroom": 18},
        "sink": verify_budget(objects, budget["sink_db"]),
    }
