"""Render the isolated J4 topology in the order its IP bindings are learned."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from ipaddress import IPv4Address
from pathlib import Path
from typing import Any

from deploy.j4.relay_preflight import RelaySelection

ROOT = Path(__file__).resolve().parents[2]
CELL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,42}[a-z0-9])?", re.ASCII)
ARMS = {*(f"C{i}" for i in range(5)), *(f"B{i}" for i in range(6))}
LABEL = "j4-role"
HOST_MOUNT = "/j4-job"
ROLE_PORTS = {
    "postgres": (5432,),
    "kafka": (9092,),
    "runtime": (8080, 8083),
    "worker": (8081,),
    "stub": (8082, 8084),
    "wrong-pod": (8082,),
    "outside": (8082,),
    "dns-decoy": (53,),
}
LANES = {
    "worker": {"runtime": (8083,), "kafka": (9092,), "stub": (8082,)},
    "runtime": {"postgres": (5432,), "kafka": (9092,)},
    "probe": ROLE_PORTS,
}
LIMITS = {
    "postgres": ("1", "1Gi"),
    "kafka": ("1", "2Gi"),
    "runtime": ("1", "1Gi"),
    "worker": ("500m", "512Mi"),
    "stub": ("250m", "256Mi"),
}
Manifest = dict[str, Any]


def local_ipv4(value: str) -> str:
    address = IPv4Address(value)
    if (
        not address.is_private
        or address.is_unspecified
        or address.is_loopback
        or address.is_multicast
        or address.is_link_local
    ):
        raise ValueError("expected an isolated J4 IPv4 address")
    return str(address)


def kind_config(host_path: Path) -> Manifest:
    resolved = host_path.resolve(strict=True)
    if not resolved.is_dir():
        raise ValueError("J4 job evidence mount must be a directory")
    kind = json.loads((ROOT / "deploy/k8s/kind-cluster.yaml").read_bytes())
    kind["nodes"][0]["extraMounts"] = [{"hostPath": str(resolved), "containerPath": HOST_MOUNT}]
    return kind


def selector(role: str) -> Manifest:
    return {"matchLabels": {LABEL: role}}


def peer(role: str, ports: tuple[int, ...], direction: str) -> Manifest:
    protocols = ("TCP", "UDP") if ports == (53,) else ("TCP",)
    return {
        direction: [{"podSelector": selector(role)}],
        "ports": [{"protocol": protocol, "port": port} for port in ports for protocol in protocols],
    }


@dataclass(frozen=True)
class Overlay:
    namespace: str
    cell: str
    arm: str
    workflow_id: str
    runtime_image: str
    worker_image: str
    relay: RelaySelection

    def __post_init__(self) -> None:
        if (
            not self.namespace.startswith("j4-")
            or CELL.fullmatch(self.namespace) is None
            or CELL.fullmatch(self.cell) is None
            or self.arm not in ARMS
            or not re.fullmatch(r"[A-Za-z0-9._:-]{1,160}", self.workflow_id)
        ):
            raise ValueError("invalid J4 overlay identity")
        # Local kind image tags are permitted at render time. The driver seals
        # their actual image IDs before admitting an episode.
        if any(
            not img.startswith("j4-") or not img.partition(":")[2]
            for img in (self.runtime_image, self.worker_image)
        ):
            raise ValueError("expected sealed local J4 image aliases")
        if self.relay.name in {"J4_RECORDED_LLM_ACTIVITY", "J4_PROVIDER_RESILIENCE"}:
            raise ValueError("invalid J4 relay binding")

    @property
    def b2(self) -> bool:
        return self.arm.startswith("B")

    def resource(self, kind: str, name: str, api: str = "v1", **fields: Any) -> Manifest:
        return {
            "apiVersion": api,
            "kind": kind,
            "metadata": {"name": name, "namespace": self.namespace},
            **fields,
        }

    def config(self, name: str, **data: Any) -> Manifest:
        return self.resource(
            "ConfigMap",
            name,
            immutable=True,
            data={
                key: json.dumps(value, sort_keys=True, separators=(",", ":"))
                for key, value in data.items()
            },
        )

    def service(self, name: str, role: str, port: int) -> Manifest:
        return self.resource(
            "Service",
            name,
            spec={"selector": {LABEL: role}, "ports": [{"port": port, "targetPort": port}]},
        )

    def network(self) -> list[Manifest]:
        policies = [
            self.resource(
                "NetworkPolicy",
                "deny-all",
                "networking.k8s.io/v1",
                spec={
                    "podSelector": {},
                    "policyTypes": ["Ingress", "Egress"],
                    "ingress": [],
                    "egress": [],
                },
            )
        ]
        roles = sorted({*ROLE_PORTS, "probe"})
        for role in roles:
            egress = [peer(target, ports, "to") for target, ports in LANES.get(role, {}).items()]
            ingress = [
                peer(source, targets[role], "from")
                for source, targets in LANES.items()
                if role in targets
            ]
            if egress or ingress:
                policies.append(
                    self.resource(
                        "NetworkPolicy",
                        role,
                        "networking.k8s.io/v1",
                        spec={
                            "podSelector": selector(role),
                            "policyTypes": ["Ingress", "Egress"],
                            "ingress": ingress,
                            "egress": egress,
                        },
                    )
                )
        return policies

    def bootstrap(self) -> list[Manifest]:
        return [
            {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": self.namespace}},
            *self.network(),
            self.service("postgres", "postgres", 5432),
            self.service("kafka", "kafka", 9092),
            self.service("api", "runtime", 8080),
            self.service("worker-control", "runtime", 8083),
        ]

    def host_volume(self, role: str) -> Manifest:
        return {
            "name": "evidence",
            "hostPath": {"path": f"{HOST_MOUNT}/{self.cell}/{role}", "type": "Directory"},
        }

    def pod(self, role: str, container: Manifest, volumes=(), init=()) -> Manifest:
        result = self.resource(
            "Pod",
            role,
            spec={
                "automountServiceAccountToken": False,
                "enableServiceLinks": False,
                "hostNetwork": False,
                "restartPolicy": "Always",
                "containers": [container],
                "volumes": list(volumes),
            },
        )
        result["metadata"]["labels"] = {LABEL: role}
        if init:
            result["spec"]["initContainers"] = list(init)
        return result

    def service_pods(self, broker_ip: str, postgres_image: str, kafka_image: str) -> list[Manifest]:
        broker = local_ipv4(broker_ip)
        for image in (postgres_image, kafka_image):
            if not re.search(r"@sha256:[0-9a-f]{64}$", image):
                raise ValueError("J4 infrastructure images require digests")
        postgres = self.pod(
            "postgres",
            container(
                "postgres",
                postgres_image,
                args=[
                    "-c",
                    "shared_preload_libraries=pg_stat_statements",
                    "-c",
                    "track_commit_timestamp=on",
                ],
                env={"POSTGRES_HOST_AUTH_METHOD": "trust", "PGDATA": "/var/lib/postgresql/data/j4"},
                mounts=[mount("data", "/var/lib/postgresql")],
            ),
            volumes=[{"name": "data", "emptyDir": {}}],
        )
        kafka = self.pod(
            "kafka",
            container(
                "kafka",
                kafka_image,
                env={
                    "KAFKA_NODE_ID": "1",
                    "KAFKA_PROCESS_ROLES": "broker,controller",
                    "KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093",
                    "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://{broker}:9092",
                    "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
                    "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@127.0.0.1:9093",
                    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": (
                        "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT"
                    ),
                    "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
                    "KAFKA_NUM_PARTITIONS": "1",
                    "KAFKA_DEFAULT_REPLICATION_FACTOR": "1",
                    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
                    "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
                    "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
                    "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
                    "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
                },
            ),
        )
        stub = self.pod(
            "stub",
            container(
                "stub",
                self.worker_image,
                command=["python", "-m", "deploy.j4.probes", "stub"],
                mounts=[mount("config", "/etc/j4", read_only=True), mount("evidence", "/evidence")],
                uid=65532,
            ),
            volumes=[config_volume("j4-stub"), self.host_volume("stub")],
        )
        decoys = [
            self.pod(
                role,
                container(
                    role,
                    self.worker_image,
                    command=["python", "-m", "deploy.j4.probes", "decoy", str(port)],
                    mounts=[mount("evidence", "/evidence")],
                    uid=65532,
                    capabilities=["NET_BIND_SERVICE"] if port == 53 else [],
                ),
                volumes=[self.host_volume(role)],
            )
            for role, port in (("wrong-pod", 8082), ("outside", 8082), ("dns-decoy", 53))
        ]
        probe = self.pod(
            "probe",
            container(
                "probe",
                self.worker_image,
                command=["python", "-c", "import time; time.sleep(21000)"],
                uid=65532,
            ),
        )
        return [
            postgres,
            kafka,
            self.config("j4-stub", **{"cells.json": {self.cell: self.arm}}),
            stub,
            *decoys,
            probe,
        ]

    def runtime(self, postgres: str, broker: str) -> Manifest:
        args = [
            "--addr=0.0.0.0:8080",
            "--roles=api,scheduler,relay,ingestor",
            "--j4-worker-addr=0.0.0.0:8083",
        ]
        if self.arm == "C2":
            args.extend(["--j4-c2-workflow", self.workflow_id])
        if self.arm == "B5":
            args.append("--j4-fixed-delay-control")
        return self.pod(
            "runtime",
            container(
                "runtime",
                self.runtime_image,
                args=args,
                uid=65532,
                mounts=[mount("evidence", "/evidence")],
                env={
                    "DATABASE_URL": f"postgresql://postgres@{local_ipv4(postgres)}/postgres?sslmode=disable",
                    "KAFKA_BOOTSTRAP_SERVERS": f"{local_ipv4(broker)}:9092",
                    "RUNTIME_NAMESPACE": self.namespace,
                    "J4_RECORDED_LLM_ACTIVITY": "1",
                    "J4_PROVIDER_RESILIENCE": str(int(self.b2)),
                    **self.relay.environment(),
                },
            ),
            volumes=[self.host_volume("runtime")],
        )

    def worker(
        self, stub: str, broker: str, control: str, wrong_pod: str, outside: str, dns: str
    ) -> list[Manifest]:
        stub, broker, control, wrong_pod, outside, dns = map(
            local_ipv4, (stub, broker, control, wrong_pod, outside, dns)
        )
        plan = {
            "stub": [stub, 8082],
            "wrong_port": [stub, 8084],
            "wrong_pod": [wrong_pod, 8082],
            "outside": [outside, 8082],
            "broker": [broker, 9092],
            "api": [control, 8083],
            "dns": [dns, 53],
        }
        cfg = self.config(
            "j4-worker",
            **{
                "worker.json": {"stub_ipv4": stub, "cell": self.cell},
                "probes.json": plan,
                "case.json": {"arm": self.arm, "workflow_id": self.workflow_id},
            },
        )
        init = container(
            "egress-init",
            self.worker_image,
            command=[
                "python",
                "-m",
                "deploy.j4.install_egress",
                stub,
                "/run/j4-egress/installed.json",
            ],
            mounts=[mount("egress", "/run/j4-egress")],
            uid=0,
            capabilities=["NET_ADMIN"],
        )
        worker = container(
            "worker",
            self.worker_image,
            command=["python", "-m", "deploy.j4.worker"],
            mounts=[
                mount("config", "/etc/j4", read_only=True),
                mount("egress", "/run/j4-egress", read_only=True),
                mount("evidence", "/evidence"),
            ],
            env={
                "KAFKA_BOOTSTRAP_SERVERS": f"{broker}:9092",
                "CONTROL_API_URL": f"http://{control}:8083",
                "WORKER_SLOTS": "1",
                "WORKER_DRAIN_MODE": "drain",
                "J4_RECORDED_LLM_ACTIVITY": "1",
                "J4_PROVIDER_RESILIENCE": str(int(self.b2)),
            },
            uid=0,
            capabilities=["SETUID", "SETGID", "KILL"],
        )
        worker["env"].extend(
            [
                {"name": name, "valueFrom": {"fieldRef": {"fieldPath": field}}}
                for name, field in [("POD_UID", "metadata.uid"), ("POD_NAME", "metadata.name")]
            ]
        )
        pod = self.pod(
            "worker",
            worker,
            volumes=[
                config_volume("j4-worker"),
                {"name": "egress", "emptyDir": {}},
                self.host_volume("worker"),
            ],
            init=[init],
        )
        return [cfg, pod]


def config_volume(name: str) -> Manifest:
    return {"name": "config", "configMap": {"name": name}}


def mount(name: str, path: str, read_only: bool = False) -> Manifest:
    return {"name": name, "mountPath": path, "readOnly": read_only}


def container(
    name: str, image: str, *, args=(), command=(), env=None, mounts=(), uid=None, capabilities=()
) -> Manifest:
    cpu, memory = LIMITS.get(name, ("250m", "256Mi"))
    result = {
        "name": name,
        "image": image,
        "imagePullPolicy": "Never" if image.startswith("j4-") else "IfNotPresent",
        "resources": {"limits": {"cpu": cpu, "memory": memory}},
    }
    if command:
        result["command"] = list(command)
    if args:
        result["args"] = list(args)
    if env:
        result["env"] = [{"name": k, "value": v} for k, v in sorted(env.items())]
    if mounts:
        result["volumeMounts"] = list(mounts)
    if uid is not None:
        result["securityContext"] = {
            "runAsUser": uid,
            "runAsGroup": uid,
            "allowPrivilegeEscalation": False,
            "readOnlyRootFilesystem": True,
            "capabilities": {"drop": ["ALL"], "add": list(capabilities)},
        }
    return result
