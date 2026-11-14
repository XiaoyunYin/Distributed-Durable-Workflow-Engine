#!/usr/bin/env python3
"""K3a CI driver. Bounded steps, immutable output, retained failures."""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import os
import platform
import re
import secrets
import shutil
import subprocess
import sys
import tarfile
import threading
import time
import urllib.request
import zipfile
from pathlib import Path
from typing import Any

from dur053_k3_policy import (
    application_groups_absent,
    application_topics_absent,
    artifact_hashes,
    fresh_output,
    matched_arms,
    package_policy,
    postgres_budget_setting,
    require,
    running_image_digests,
    scratch_ownership,
    single_witness_session,
    stable_worker_sessions,
    supported,
    terminal_report,
)
from dur053_prerequisites import export_witness, trace_document

ROOT = Path(__file__).resolve().parents[1]
K8S = ROOT / "deploy/k8s"
LOCK = json.loads((K8S / "tools.lock.json").read_text(encoding="utf-8"))
CLUSTER = "dur053-k3a"
LABEL = "dur053.k3a=owned"
LIVE_RESERVATION = 3600
CALICO_ROLLOUT_SECONDS = 600
NODE_READY_SECONDS = 300
KEDA_ROLLOUT_SECONDS = 300


def save(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n"
    )


def download(url: str, sha256: str) -> bytes:
    with urllib.request.urlopen(url, timeout=60) as response:
        data = response.read()
    require(hashlib.sha256(data).hexdigest() == sha256, "download-checksum")
    return data


def elapsed_live_seconds(scenario: str, cluster_started: bool, cleanup_seconds: int) -> int:
    if scenario != "smoke" or not cluster_started:
        return 0
    return max(1, cleanup_seconds)


def github(path: str) -> Any:
    request = urllib.request.Request(
        "https://api.github.com/" + path,
        headers={
            **(
                {"Authorization": "Bearer " + os.environ["GH_TOKEN"]}
                if os.getenv("GH_TOKEN")
                else {}
            ),
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def campaign_job_name(name: str) -> bool:
    return name in {"DUR-053 campaign", "campaign"} or bool(
        re.fullmatch(r"k3-pilot-(1a|1b|2|3|4a|4b|5a|5b|[45]c[123])(-c[1-9][0-9]*)?", name)
    )


def job_identity_and_history() -> tuple[str, set[str], str]:
    """Campaign dispatch history only; engineering smoke never calls this."""
    require(
        bool(os.getenv("GITHUB_ACTIONS")) and bool(os.getenv("GH_TOKEN")), "hosted-actions-required"
    )
    repo, run = os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_RUN_ID"]
    current = github(f"repos/{repo}/actions/runs/{run}")
    mine = [
        j
        for j in github(f"repos/{repo}/actions/runs/{run}/jobs?per_page=100")["jobs"]
        if j["name"] == "DUR-053 campaign"
    ]
    require(
        len(mine) == 1 and os.environ.get("GITHUB_RUN_ATTEMPT") == "1", "no-automatic-replacement"
    )
    prior: set[str] = set()
    for workflow in ("dur053-campaign.yml",):
        for page in range(1, 101):
            runs = github(
                f"repos/{repo}/actions/workflows/{workflow}/runs?per_page=100&page={page}"
            )["workflow_runs"]
            for row in runs:
                if str(row["id"]) == run or row["created_at"] > current["created_at"]:
                    continue
                jobs = github(f"repos/{repo}/actions/runs/{row['id']}/jobs?filter=all&per_page=100")
                require(jobs["total_count"] <= 100, "history-complete")
                for job in jobs["jobs"]:
                    if (
                        campaign_job_name(job["name"])
                        and job["conclusion"] != "skipped"
                    ):
                        prior.add(str(job["id"]))
            if len(runs) < 100:
                break
        else:
            raise ValueError("K3 guard: history pagination incomplete")
    return str(mine[0]["id"]), prior, current["head_sha"]


def lock_addon_images(objects: list[Any], registered: list[str]) -> set[str]:
    images: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, list):
            for item in value:
                visit(item)
        elif isinstance(value, dict):
            if isinstance(value.get("image"), str):
                source = value["image"]
                candidates = {key: LOCK["images"][key] for key in registered}
                require(source in candidates or source in candidates.values(), "addon-image-lock")
                pinned = candidates.get(source, source)
                require(
                    "@sha256:" in pinned and len(pinned.rsplit("@sha256:", 1)[1]) == 64,
                    "addon-image-digest",
                )
                value["image"], value["imagePullPolicy"] = pinned, "Never"
                images.add(pinned)
            for item in list(value.values()):
                visit(item)

    visit(objects)
    require(images == {LOCK["images"][key] for key in registered}, "addon-image-inventory")
    return images


def configure_calico(objects: list[Any], kind: dict[str, Any]) -> None:
    require(kind["networking"]["disableDefaultCNI"] is True, "calico-exclusive-cni")
    cidr = kind["networking"]["podSubnet"]
    for obj in objects:
        if obj and obj["kind"] == "ConfigMap" and obj["metadata"]["name"] == "calico-config":
            obj["data"]["calico_backend"] = "vxlan"
        if obj and obj["kind"] == "DaemonSet" and obj["metadata"]["name"] == "calico-node":
            container = next(
                c
                for c in obj["spec"]["template"]["spec"]["containers"]
                if c["name"] == "calico-node"
            )
            values = {
                "CLUSTER_TYPE": "k8s",
                "CALICO_IPV4POOL_CIDR": cidr,
                "CALICO_IPV4POOL_IPIP": "Never",
                "CALICO_IPV4POOL_VXLAN": "Always",
            }
            container["env"] = [e for e in container["env"] if e["name"] not in values]
            container["env"].extend({"name": key, "value": value} for key, value in values.items())
            # Match upstream's VXLAN-only manifest: no BIRD/BGP health dependency.
            for probe in ("readinessProbe", "livenessProbe"):
                command = container[probe]["exec"]["command"]
                container[probe]["exec"]["command"] = [
                    a for a in command if not a.startswith("-bird")
                ]


class Driver:
    def __init__(self, output: Path, scratch: Path) -> None:
        self.output, self.scratch = output, scratch
        self.live_reservation = LIVE_RESERVATION
        self.env = dict(os.environ)
        self.env.update({key: str(scratch) for key in ("TMP", "TEMP", "TMPDIR", "GOTMPDIR")})
        self.env["KUBECONFIG"] = str(scratch / "kubeconfig")
        self.env["PATH"] = str(scratch / "bin") + os.pathsep + self.env["PATH"]
        self.created: set[str] = set()
        self.kind_network_created = False
        self.cluster_started = False
        self.databases: set[str] = set()
        self.live_start: float | None = None
        self.sequence = 0
        self.output_lock = threading.Lock()
        self.episode_service_ips: dict[str, str] = {}
        self.images: dict[str, str] = {}
        self.addon_images: set[str] = set()
        self.cluster_attempted = False
        self.password, self.demand_token = secrets.token_hex(32), secrets.token_hex(32)

    def command(
        self,
        args: list[str],
        *,
        data: str | None = None,
        timeout: float = 180,
        allowed: bool = False,
        record: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        if self.live_start is not None:
            remaining = self.live_reservation - (time.monotonic() - self.live_start)
            require(remaining > 900, "live-job-deadline")
            timeout = min(timeout, remaining - 900)
        try:
            result = subprocess.run(
                args,
                cwd=ROOT,
                env=self.env,
                input=data,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as error:
            sequence = self.command_output(error.stdout, error.stderr)
            raise TimeoutError(f"command-{sequence:03d}-{Path(args[0]).name}-timeout") from error
        sequence = self.sequence
        if record or result.returncode != 0:
            sequence = self.command_output(result.stdout, result.stderr)
        if not allowed:
            require(
                result.returncode == 0,
                f"command-{sequence:03d}-{Path(args[0]).name}-exit-{result.returncode}",
            )
        return result

    def command_output(self, stdout: str | bytes | None, stderr: str | bytes | None) -> int:
        # Arrivals and safety sampling execute commands concurrently. Allocate
        # and write together, so each guard identifies its own retained output.
        with self.output_lock:
            self.sequence += 1
            text = "".join(
                part.decode("utf-8", errors="replace") if isinstance(part, bytes) else part or ""
                for part in (stdout, stderr)
            )
            for credential in (self.password, self.demand_token):
                text = text.replace(credential, "[REDACTED]")
            (self.output / f"command-{self.sequence:03d}.txt").write_text(
                text, encoding="utf-8", newline="\n"
            )
            return self.sequence

    def k(self, *args: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return self.command(["kubectl", "--context", "kind-" + CLUSTER, *args], **kwargs)

    def apply(self, obj: Any) -> None:
        self.k("apply", "-f", "-", data=json.dumps(obj), record=False)

    def install(self) -> None:
        bindir = self.scratch / "bin"
        bindir.mkdir()
        for name, tool in LOCK["tools"].items():
            if tool.get("k4_only"):
                continue
            data = download(tool["url"], tool["sha256"])
            if tool["member"]:
                with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
                    handle = archive.extractfile(tool["member"])
                    data = handle.read()
            path = bindir / name
            path.write_bytes(data)
            path.chmod(0o755)
        wheel = LOCK["pyyaml"]
        require(
            sys.version_info[:2] == (3, 12) and platform.machine() == "x86_64",
            "runner-python-platform",
        )
        library = self.scratch / "library"
        with zipfile.ZipFile(io.BytesIO(download(wheel["url"], wheel["sha256"]))) as archive:
            archive.extractall(library)
        sys.path.insert(0, str(library))
        for filename, digest in LOCK["schemas"].items():
            require(
                hashlib.sha256((K8S / "schemas" / filename).read_bytes()).hexdigest() == digest,
                "schema-checksum",
            )
        versions = {
            name: self.command(args).stdout.strip()
            for name, args in {
                "kind": ["kind", "version"],
                "kubectl": ["kubectl", "version", "--client", "-o", "json"],
                "kustomize": ["kustomize", "version"],
                "kubeconform": ["kubeconform", "-v"],
                "docker": ["docker", "version", "--format", "{{json .}}"],
            }.items()
        }
        versions.update(
            {
                "ImageOS": os.getenv("ImageOS"),
                "ImageVersion": os.getenv("ImageVersion"),
                "vcpu_observed": os.cpu_count(),
                "memory_observed": Path("/proc/meminfo").read_text(),
                "python": sys.version,
                "kernel": platform.platform(),
                "lock": LOCK,
            }
        )
        require(
            versions["ImageVersion"] and versions["ImageOS"] == "ubuntu24", "runner-image-observed"
        )
        save(self.output / "versions.json", versions)

    def build(self) -> None:
        name = CLUSTER + "-registry"
        self.created.add(name)
        self.command(
            [
                "docker",
                "run",
                "-d",
                "--name",
                name,
                "--label",
                LABEL,
                "-p",
                "127.0.0.1:5001:5000",
                LOCK["registry"],
            ]
        )
        for target in ("runtime", "worker"):
            tag = f"localhost:5001/dur053-{target}:candidate"
            self.command(
                [
                    "docker",
                    "build",
                    "--provenance=false",
                    "--platform=linux/amd64",
                    "--target",
                    target,
                    "-t",
                    tag,
                    "-f",
                    "deploy/k8s/Dockerfile",
                    ".",
                ],
                timeout=900,
            )
            self.command(["docker", "push", tag], timeout=180)
            refs = json.loads(
                self.command(["docker", "inspect", "--format", "{{json .RepoDigests}}", tag]).stdout
            )
            require(len(refs) == 1 and "@sha256:" in refs[0], "built-image-digest")
            self.images[target] = refs[0]
        save(self.output / "images.json", self.images)

    def render(
        self, arm: str, endpoints: dict[str, str] | None = None
    ) -> tuple[Path, list[dict[str, Any]]]:
        import yaml

        tree = self.scratch / ("package-" + arm)
        shutil.copytree(K8S, tree)
        p = tree / "base/resources.yaml"
        content = p.read_text()
        for name, digest in self.images.items():
            content = content.replace(f"dur053/{name}:UNBOUND", digest)
        for placeholder, address in (endpoints or {}).items():
            content = content.replace(placeholder + "/32", address + "/32")
        p.write_text(content)
        built = self.command(["kustomize", "build", str(tree / "overlays" / arm)]).stdout
        path = self.output / (arm + ".yaml")
        path.write_text(built, encoding="utf-8", newline="\n")
        objects = list(yaml.safe_load_all(built))
        count = package_policy(objects, json.loads((K8S / "connection-budget.json").read_text()))
        schema = str(K8S / "schemas" / "{{.ResourceKind}}{{.KindSuffix}}.json")
        self.command(
            [
                "kubeconform",
                "-strict",
                "-summary",
                "-kubernetes-version",
                "1.32.2",
                "-schema-location",
                schema,
                str(path),
            ]
        )
        save(
            self.output / (arm + "-policy.json"),
            {
                "status": "PASS",
                "maximum_application_connections": count,
                "reserved_and_admin": 8,
                "max_connections": 100,
            },
        )
        return path, objects

    def prepare_addon(self, name: str) -> None:
        import yaml

        tool = LOCK["addons"][name]
        data = download(tool["url"], tool["sha256"]).decode()
        objects = list(yaml.safe_load_all(data))
        images = lock_addon_images(objects, tool["images"])
        self.addon_images.update(images)
        if name == "calico":
            configure_calico(objects, json.loads((K8S / "kind-cluster.yaml").read_text()))
        # Retain the exact applied manifest, including pool/backend and pull policy.
        path = self.output / (name + ".yaml")
        path.write_text(yaml.safe_dump_all(objects), encoding="utf-8", newline="\n")

    def preload_addons(self) -> None:
        for index, image in enumerate(sorted(self.addon_images)):
            source = next(key for key, value in LOCK["images"].items() if value == image)
            self.command(["docker", "pull", "--platform=linux/amd64", image], timeout=300)
            self.command(["docker", "tag", image, source])
            archive = self.scratch / f"addon-{index}.tar"
            self.command(["docker", "save", "--output", str(archive), source], timeout=180)

    def load_addons(self) -> None:
        for index, image in enumerate(sorted(self.addon_images)):
            source = next(key for key, value in LOCK["images"].items() if value == image)
            canonical = source.rsplit(":", 1)[0] + "@" + image.split("@", 1)[1]
            archive = self.scratch / f"addon-{index}.tar"
            self.command(
                ["kind", "load", "image-archive", "--name", CLUSTER, str(archive)], timeout=180
            )
            # Docker archives retain tags, not registry index aliases. Bind the
            # verified host pull to the exact digest reference used by the pods.
            self.command(
                [
                    "docker",
                    "exec",
                    CLUSTER + "-control-plane",
                    "ctr",
                    "--namespace",
                    "k8s.io",
                    "images",
                    "tag",
                    "--force",
                    source,
                    canonical,
                ]
            )
            self.command(
                ["docker", "exec", CLUSTER + "-control-plane", "crictl", "inspecti", image]
            )
            archive.unlink()
        save(self.output / "addon-images.json", sorted(self.addon_images))

    def addon(self, name: str) -> None:
        path = self.output / (name + ".yaml")
        self.k("apply", "--server-side", "-f", str(path), timeout=300)

    def cluster(self) -> dict[str, str]:
        if self.live_start is None:
            self.live_start = time.monotonic()
        self.cluster_attempted = True
        save(
            self.output / "startup-limits.json",
            {
                "calico_rollout_seconds": CALICO_ROLLOUT_SECONDS,
                "node_ready_seconds": NODE_READY_SECONDS,
                "keda_rollout_seconds": KEDA_ROLLOUT_SECONDS,
                "live_ceiling_seconds": self.live_reservation,
                "diagnostic_reserve_seconds": 300,
                "teardown_reserve_seconds": 600,
            },
        )
        existing_network = self.command(
            ["docker", "network", "inspect", "kind"], allowed=True, record=False
        )
        self.kind_network_created = existing_network.returncode != 0
        self.command(
            [
                "kind",
                "create",
                "cluster",
                "--name",
                CLUSTER,
                "--config",
                str(K8S / "kind-cluster.yaml"),
                "--image",
                LOCK["node"],
                "--wait",
                "0s",
            ],
            timeout=300,
        )
        self.cluster_started = True
        self.command(["docker", "network", "connect", "kind", CLUSTER + "-registry"])
        node = CLUSTER + "-control-plane"
        self.command(
            ["docker", "exec", node, "mkdir", "-p", "/etc/containerd/certs.d/localhost:5001"]
        )
        hosts = (
            'server = "http://'
            + CLUSTER
            + '-registry:5000"\n[host."http://'
            + CLUSTER
            + '-registry:5000"]\n  capabilities = ["pull", "resolve"]\n'
        )
        self.command(
            [
                "docker",
                "exec",
                "-i",
                node,
                "sh",
                "-c",
                "cat > /etc/containerd/certs.d/localhost:5001/hosts.toml",
            ],
            data=hosts,
        )
        for image in self.images.values():
            self.command(
                [
                    "docker",
                    "exec",
                    node,
                    "ctr",
                    "--namespace",
                    "k8s.io",
                    "images",
                    "pull",
                    "--hosts-dir",
                    "/etc/containerd/certs.d",
                    image,
                ],
                timeout=180,
            )
        self.command(["docker", "exec", node, "ctr", "--namespace", "k8s.io", "images", "ls"])
        self.load_addons()
        # A fresh cluster disables kindnet; fail visibly if that contract drifts.
        kindnet = json.loads(self.k("-n", "kube-system", "get", "ds", "-o", "json").stdout)
        require(not any(o["metadata"]["name"] == "kindnet" for o in kindnet["items"]), "no-kindnet")
        self.addon("calico")
        self.k(
            "-n",
            "kube-system",
            "rollout",
            "status",
            "daemonset/calico-node",
            f"--timeout={CALICO_ROLLOUT_SECONDS}s",
            timeout=CALICO_ROLLOUT_SECONDS + 10,
        )
        self.k(
            "wait",
            "node",
            "--all",
            "--for=condition=Ready",
            f"--timeout={NODE_READY_SECONDS}s",
            timeout=NODE_READY_SECONDS + 10,
        )
        self.addon("keda")
        for dep in LOCK["addons"]["keda"]["deployments"]:
            self.k(
                "-n",
                "keda",
                "rollout",
                "status",
                "deployment/" + dep,
                f"--timeout={KEDA_ROLLOUT_SECONDS}s",
                timeout=KEDA_ROLLOUT_SECONDS + 10,
            )
        self.k("get", "pods", "-A", "-o", "json")
        return self.dependencies()

    def dependencies(self) -> dict[str, str]:
        pg, kafka = CLUSTER + "-postgres", CLUSTER + "-kafka"
        self.created.update({pg, kafka})
        self.command(
            [
                "docker",
                "run",
                "-d",
                "--name",
                pg,
                "--label",
                LABEL,
                "--network",
                "kind",
                "-e",
                "POSTGRES_USER=durable",
                "-e",
                "POSTGRES_PASSWORD=" + self.password,
                "-e",
                "POSTGRES_DB=postgres",
                LOCK["postgres"],
                "-c",
                "max_connections=100",
                "-c",
                "fsync=on",
                "-c",
                "synchronous_commit=on",
                "-c",
                "full_page_writes=on",
            ],
            record=False,
        )
        self.until(
            lambda: (
                self.command(
                    ["docker", "exec", pg, "pg_isready", "-U", "durable"],
                    allowed=True,
                    record=False,
                ).returncode
                == 0
            ),
            90,
        )
        settings = {
            "KAFKA_NODE_ID": "1",
            "KAFKA_PROCESS_ROLES": "broker,controller",
            "KAFKA_LISTENERS": "CONTROLLER://:9093,INTERNAL://:19092",
            "KAFKA_ADVERTISED_LISTENERS": "INTERNAL://kafka:19092",
            "KAFKA_INTER_BROKER_LISTENER_NAME": "INTERNAL",
            "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT",
            "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
            "KAFKA_NUM_PARTITIONS": "8",
            "KAFKA_DELETE_TOPIC_ENABLE": "true",
        }
        args = [
            "docker",
            "run",
            "-d",
            "--name",
            kafka,
            "--label",
            LABEL,
            "--network",
            "kind",
            "--network-alias",
            "kafka",
        ]
        for key, value in settings.items():
            args.extend(["-e", key + "=" + value])
        self.command([*args, LOCK["kafka"]])
        self.until(
            lambda: (
                self.command(
                    [
                        "docker",
                        "exec",
                        kafka,
                        "/opt/kafka/bin/kafka-topics.sh",
                        "--bootstrap-server",
                        "localhost:19092",
                        "--list",
                    ],
                    allowed=True,
                    record=False,
                ).returncode
                == 0
            ),
            120,
        )
        self.topics("--create")
        addresses = {}
        for placeholder, container in (("192.0.2.1", pg), ("192.0.2.2", kafka)):
            addresses[placeholder] = self.command(
                [
                    "docker",
                    "inspect",
                    "--format",
                    '{{(index .NetworkSettings.Networks "kind").IPAddress}}',
                    container,
                ]
            ).stdout.strip()
        addresses["192.0.2.3"] = json.loads(
            self.k("get", "service", "kubernetes", "-o", "json").stdout
        )["spec"]["clusterIP"]
        addresses["192.0.2.4"] = json.loads(
            self.k("get", "endpoints", "kubernetes", "-o", "json").stdout
        )["subsets"][0]["addresses"][0]["ip"]
        save(self.output / "dependency-addresses.json", addresses)
        return addresses

    def topics(self, action: str) -> None:
        for topic in ("durable-agent.tasks.v1", "durable-agent.events.v1"):
            args = [
                "docker",
                "exec",
                CLUSTER + "-kafka",
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                "localhost:19092",
                action,
                "--topic",
                topic,
            ]
            if action == "--create":
                args.extend(["--partitions", "8", "--replication-factor", "1"])
            self.command(args)

    @staticmethod
    def until(predicate: Any, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if predicate() and time.monotonic() < deadline:
                return
            time.sleep(min(1, max(0, deadline - time.monotonic())))
        raise TimeoutError("bounded condition did not become true")

    def sql(self, query: str, database: str = "postgres") -> str:
        return self.command(
            [
                "docker",
                "exec",
                "-i",
                CLUSTER + "-postgres",
                "psql",
                "-X",
                "-v",
                "ON_ERROR_STOP=1",
                "-At",
                "-U",
                "durable",
                "-d",
                database,
            ],
            data=query,
            record=False,
        ).stdout.strip()

    def pod(self, namespace: str, app: str) -> dict[str, Any]:
        items = json.loads(
            self.k(
                "-n", namespace, "get", "pods", "-l", "app=" + app, "-o", "json", record=False
            ).stdout
        )["items"]
        return next(
            p
            for p in items
            if not p["metadata"].get("deletionTimestamp") and p["status"].get("phase") == "Running"
        )

    def python(self, ns: str, app: str, code: str, *, record: bool = True) -> str:
        return self.k(
            "-n",
            ns,
            "exec",
            self.pod(ns, app)["metadata"]["name"],
            "--",
            "python",
            "-c",
            code,
            record=record,
        ).stdout.strip()

    def episode_objects(self, objects: list[dict[str, Any]]) -> None:
        """Pilot overrides only registered duration, CPU units and scaler pause."""

    def setup_episode(self, arm: str, addresses: dict[str, str]) -> tuple[str, str, str, set[str]]:
        namespace = "dur053-kind" if arm == "kind" else "dur053-baseline"
        database = namespace.replace("-", "_")
        self.demand_token = secrets.token_hex(32)
        path, objects = self.render(arm, addresses)
        service_names = {o["metadata"]["name"] for o in objects if o["kind"] == "Service"}
        require(
            not self.episode_service_ips or set(self.episode_service_ips) == service_names,
            "episode-service-ip-inventory",
        )
        for obj in objects:
            if obj["kind"] == "Service" and self.episode_service_ips:
                # A fresh namespace must not invalidate still-cached DNS A
                # answers. Recreate the frontends at their released addresses;
                # all Service objects, pods and dependency state remain fresh.
                obj["spec"]["clusterIP"] = self.episode_service_ips[obj["metadata"]["name"]]
            if obj["kind"] == "ScaledObject":
                obj["metadata"].setdefault("annotations", {})[
                    "autoscaling.keda.sh/paused-scale-in"
                ] = "true"
                obj["metadata"]["annotations"]["autoscaling.keda.sh/paused-scale-out"] = "true"
        self.episode_objects(objects)
        self.databases.add(database)
        self.sql('CREATE DATABASE "' + database + '";')
        # Submission freeze: neither public Deployment nor client exists before both Jobs pass.
        self.apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace}})
        url = f"postgresql://durable:{self.password}@postgres:5432/{database}?sslmode=disable"
        for name, values in (
            ("campaign-db", {"url": url}),
            ("campaign-demand", {"token": self.demand_token}),
        ):
            self.apply(
                {
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "metadata": {"name": name, "namespace": namespace},
                    "stringData": values,
                }
            )
        infrastructure = [
            o
            for o in objects
            if o["kind"] not in {"Deployment", "Job", "ScaledObject", "PodDisruptionBudget"}
        ]
        self.apply({"apiVersion": "v1", "kind": "List", "items": infrastructure})
        for name, placeholder, port in (
            ("postgres", "192.0.2.1", 5432),
            ("kafka", "192.0.2.2", 19092),
        ):
            self.apply(
                {
                    "apiVersion": "v1",
                    "kind": "Endpoints",
                    "metadata": {"name": name, "namespace": namespace},
                    "subsets": [
                        {
                            "addresses": [{"ip": addresses[placeholder]}],
                            "ports": [{"name": "tcp", "port": port, "protocol": "TCP"}],
                        }
                    ],
                }
            )
        for name in ("migrate", "compatibility"):
            self.apply(
                next(o for o in objects if o["kind"] == "Job" and o["metadata"]["name"] == name)
            )
            self.k(
                "-n",
                namespace,
                "wait",
                "--for=condition=complete",
                "job/" + name,
                "--timeout=180s",
                timeout=190,
            )
            self.k("-n", namespace, "logs", "job/" + name)
        self.apply({"apiVersion": "v1", "kind": "List", "items": objects})
        for role in (
            "api",
            "operator",
            "worker-control",
            "scheduler",
            "relay",
            "demand",
            "worker",
            "observer",
        ):
            self.k(
                "-n",
                namespace,
                "rollout",
                "status",
                "deployment/" + role,
                "--timeout=180s",
                timeout=190,
            )
        resources = json.loads(
            self.k(
                "-n", namespace, "get", "deployments,pods,services,networkpolicies,pdb,jobs",
                "-o", "json",
            ).stdout
        )["items"]
        observed_ips = {
            o["metadata"]["name"]: o["spec"].get("clusterIP")
            for o in resources
            if o["kind"] == "Service" and o["metadata"]["name"] in service_names
        }
        require(
            set(observed_ips) == service_names
            and all(isinstance(ip, str) and ip not in {"", "None"} for ip in observed_ips.values()),
            "episode-service-ip-inventory",
        )
        require(
            not self.episode_service_ips or observed_ips == self.episode_service_ips,
            "episode-service-ip-continuity",
        )
        self.episode_service_ips = {name: str(ip) for name, ip in observed_ips.items()}
        save(self.output / (arm + "-service-ips.json"), self.episode_service_ips)
        pods = json.loads(
            self.k("-n", namespace, "get", "pods", "-o", "json", record=False).stdout
        )["items"]
        image_status = [
            {
                "uid": p["metadata"]["uid"],
                "name": p["metadata"]["name"],
                "containers": p["status"].get("containerStatuses", []),
            }
            for p in pods
        ]
        running_image_digests(image_status, self.images)
        expected_workers = {
            p["metadata"]["uid"]
            for p in pods
            if p["metadata"].get("labels", {}).get("app") == "worker"
        }
        initial_workers = [
            p for p in pods if p["metadata"].get("labels", {}).get("app") == "worker"
        ]
        stable_worker_sessions(expected_workers, initial_workers)
        save(self.output / (arm + "-running-images.json"), image_status)
        obs = self.pod(namespace, "observer")["metadata"]["name"]
        return namespace, database, obs, expected_workers

    def batch(self, arm: str, addresses: dict[str, str]) -> None:
        namespace, database, obs, expected_workers = self.setup_episode(arm, addresses)
        self.k(
            "-n",
            namespace,
            "exec",
            obs,
            "--",
            "/loadgen",
            "-url",
            "http://api:8080",
            "-token-file",
            "/identity/token",
            "-namespace",
            namespace,
            "-run-id",
            namespace,
            "-arm",
            "short",
            "-count",
            "10",
            "-rate",
            "1",
            "-concurrency",
            "16",
            "-output",
            "/evidence/arrivals.json",
        )
        arrivals = json.loads(
            self.python(namespace, "observer", 'print(open("/evidence/arrivals.json").read())')
        )
        save(self.output / (arm + "-arrivals.json"), arrivals)
        witness_root = self.output / (arm + "-witness")
        witness_root.mkdir()
        quiet_since, last = time.monotonic(), ""
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            workers = json.loads(
                self.k(
                    "-n", namespace, "get", "pods", "-l", "app=worker", "-o", "json", record=False
                ).stdout
            )["items"]
            workers = [p for p in workers if not p["metadata"].get("deletionTimestamp")]
            ips = [p["status"]["podIP"] for p in workers]
            code = f'import json,urllib.request; print(json.dumps([json.load(urllib.request.urlopen("http://"+ip+":8081/witness",timeout=3)) for ip in {ips!r}]))'
            inventories = json.loads(self.python(namespace, "observer", code, record=False))
            fingerprint = json.dumps(inventories, sort_keys=True)
            if fingerprint != last:
                quiet_since, last = time.monotonic(), fingerprint
            terminal = (
                self.sql(
                    "SELECT count(*) FROM engine.workflow_executions WHERE namespace='"
                    + namespace
                    + "' AND state='SUCCEEDED';",
                    database,
                )
                == "10"
            )
            if terminal and time.monotonic() - quiet_since >= 30:
                break
            time.sleep(1)
        else:
            raise TimeoutError("terminal/witness quiet timeout")
        stable_worker_sessions(expected_workers, workers)
        for inventory in inventories:
            files_code = (
                "import pathlib,json; print(json.dumps(sorted(p.name for p in pathlib.Path("
                + repr("/witness/" + inventory["pod_name"])
                + ').glob("*.jsonl"))))'
            )
            filenames = json.loads(
                self.k(
                    "-n",
                    namespace,
                    "exec",
                    inventory["pod_name"],
                    "--",
                    "python",
                    "-c",
                    files_code,
                    record=False,
                ).stdout
            )
            single_witness_session(filenames, inventory)
            directory = witness_root / inventory["pod_name"]
            directory.mkdir()
            code = (
                "import base64; print(base64.b64encode(open("
                + repr("/witness/" + inventory["pod_name"] + "/" + inventory["file"])
                + ',"rb").read()).decode())'
            )
            result = self.k(
                "-n",
                namespace,
                "exec",
                inventory["pod_name"],
                "--",
                "python",
                "-c",
                code,
                record=False,
            ).stdout.strip()
            (directory / inventory["file"]).write_bytes(base64.b64decode(result))
        exported = export_witness(
            witness_root, inventories, [p["metadata"]["uid"] for p in workers]
        )
        save(self.output / (arm + "-witness-export.json"), exported)
        self.k(
            "-n",
            namespace,
            "exec",
            "-i",
            obs,
            "--",
            "python",
            "-c",
            'import sys; open("/evidence/witness.json","w").write(sys.stdin.read())',
            data=json.dumps(exported),
            record=False,
        )
        checker = [
            "-n",
            namespace,
            "exec",
            obs,
            "--",
            "/checker",
            "-arrivals",
            "/evidence/arrivals.json",
            "-witness-export",
            "/evidence/witness.json",
            "-require-terminal",
        ]
        checked = self.k(*checker, "-namespace", namespace).stdout
        terminal_report(json.loads(checked))
        (self.output / (arm + "-checker.json")).write_text(checked, encoding="utf-8", newline="\n")
        raw_trace = trace_document(checked)
        (self.output / (arm + "-snapshot.json")).write_text(
            raw_trace, encoding="utf-8", newline="\n"
        )
        self.k(
            "-n",
            namespace,
            "exec",
            "-i",
            obs,
            "--",
            "python",
            "-c",
            'import sys; open("/evidence/snapshot.json","w").write(sys.stdin.read())',
            data=raw_trace,
            record=False,
        )
        self.k(*checker, "-snapshot", "/evidence/snapshot.json")
        postgres_budget_setting(int(self.sql("SHOW max_connections;")))
        save(
            self.output / (arm + "-connections.json"),
            {
                "max_connections": 100,
                "observed": self.sql(
                    "SELECT application_name,count(*) FROM pg_stat_activity GROUP BY application_name;"
                ),
            },
        )
        if arm == "kind":
            self.probes(namespace)
        self.k("-n", namespace, "get", "scaledobject,hpa,events,pods,jobs", "-o", "json")
        self.reset_episode(namespace, database)

    def reset_episode(self, namespace: str, database: str) -> None:
        self.k("delete", "namespace", namespace, "--wait=true", "--timeout=120s", timeout=130)
        self.drop(database)
        self.topics("--delete")
        topic_command = [
            "docker",
            "exec",
            CLUSTER + "-kafka",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "localhost:19092",
            "--list",
        ]
        self.until(
            lambda: application_topics_absent(self.command(topic_command, record=False).stdout), 60
        )
        group_command = [
            "docker",
            "exec",
            CLUSTER + "-kafka",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            "localhost:19092",
        ]
        for group in ("runtime-workers-v1", "runtime-schedulers-v1"):
            result = self.command([*group_command, "--delete", "--group", group], allowed=True)
            if result.returncode:
                # Topic deletion may already have removed an empty group. A
                # refusal is benign only when an independent listing proves
                # that exact group absent; never ignore an active-group error.
                listing = self.command([*group_command, "--list"], record=False).stdout.splitlines()
                require(group not in listing, "kafka-group-delete-verified")
        self.until(
            lambda: application_groups_absent(
                self.command([*group_command, "--list"], record=False).stdout
            ),
            30,
        )
        self.topics("--create")

    def probes(self, ns: str) -> None:
        def scaler_happy() -> bool:
            obj = json.loads(
                self.k(
                    "-n", ns, "get", "scaledobject", "worker-demand", "-o", "json", record=False
                ).stdout
            )
            return any(
                c["type"] == "Ready" and c["status"] == "True"
                for c in obj.get("status", {}).get("conditions", [])
            ) and any(
                h.get("status") == "Happy" and h.get("numberOfFailures") == 0
                for h in obj.get("status", {}).get("health", {}).values()
            )

        self.until(scaler_happy, 90)
        self.k("-n", ns, "get", "scaledobject", "worker-demand", "-o", "json")
        # Explicit temporary egress isolates the ingress enforcement assertion.
        ports = [{"protocol": "TCP", "port": p} for p in (8080, 8091)]
        self.apply(
            {
                "apiVersion": "networking.k8s.io/v1",
                "kind": "NetworkPolicy",
                "metadata": {"name": "smoke-egress", "namespace": ns},
                "spec": {
                    "podSelector": {"matchLabels": {"app": "worker"}},
                    "policyTypes": ["Egress"],
                    "egress": [
                        {
                            "to": [
                                {"podSelector": {"matchLabels": {"app": target}}}
                                for target in ("operator", "demand")
                            ],
                            "ports": ports,
                        }
                    ],
                },
            }
        )
        time.sleep(3)
        code = 'import socket,json; results={};\nfor host,port in [("operator",8080),("demand",8091)]:\n try:\n  socket.create_connection((host,port),timeout=3).close(); results[host]="CONNECTED"\n except TimeoutError: results[host]="DENIED"\nprint(json.dumps(results))'
        denied = json.loads(self.python(ns, "worker", code))
        require(denied == {"operator": "DENIED", "demand": "DENIED"}, "networkpolicy-enforced")
        # Then permit the exact ingress paths to assert independent auth refusal.
        for target, port in (("operator", 8080), ("demand", 8091)):
            self.apply(
                {
                    "apiVersion": "networking.k8s.io/v1",
                    "kind": "NetworkPolicy",
                    "metadata": {"name": "smoke-auth-" + target, "namespace": ns},
                    "spec": {
                        "podSelector": {"matchLabels": {"app": target}},
                        "policyTypes": ["Ingress"],
                        "ingress": [
                            {
                                "from": [{"podSelector": {"matchLabels": {"app": "worker"}}}],
                                "ports": [{"protocol": "TCP", "port": port}],
                            }
                        ],
                    },
                }
            )
        time.sleep(3)
        code = 'import urllib.request,urllib.error,json; results={}; token=open("/identity/token").read().strip();\nfor host,port,path in [("operator",8080,"/v1/workflows/no-such-workflow"),("demand",8091,"/v1/demand")]:\n try:\n  r=urllib.request.urlopen(urllib.request.Request(f"http://{host}:{port}{path}",headers={"Authorization":"Bearer "+token}),timeout=5); results[host]=r.status\n except urllib.error.HTTPError as e: results[host]=e.code\nprint(json.dumps(results))'
        auth = json.loads(self.python(ns, "worker", code))
        require(auth == {"operator": 401, "demand": 401}, "worker-auth-denied")
        self.k(
            "-n",
            ns,
            "delete",
            "networkpolicy",
            "smoke-auth-operator",
            "smoke-auth-demand",
            "smoke-egress",
        )
        operator_positive = self.python(
            ns,
            "observer",
            'import urllib.request,urllib.error; token=open("/identity/operator").read().strip(); req=urllib.request.Request("http://operator:8080/v1/workflows/no-such-workflow",headers={"Authorization":"Bearer "+token});\ntry: print(urllib.request.urlopen(req,timeout=5).status)\nexcept urllib.error.HTTPError as e: print(e.code)',
        )
        require(operator_positive == "404", "operator-authorized-read")
        save(
            self.output / "worker-denial.json",
            {
                "network": denied,
                "auth": auth,
                "operator_observer_status": operator_positive,
                "temporary_allows_removed": True,
            },
        )
        pod = self.pod(ns, "api")
        ip = pod["status"]["podIP"]
        probe = (
            'import urllib.request,urllib.error,json; r={};\nfor path in ["/healthz","/readyz"]:\n try: r[path]=urllib.request.urlopen("http://'
            + ip
            + ':8080"+path,timeout=3).status\n except urllib.error.HTTPError as e:r[path]=e.code\nprint(json.dumps(r))'
        )
        before = json.loads(self.python(ns, "observer", probe))
        self.command(["docker", "stop", "--time", "10", CLUSTER + "-postgres"])
        during = json.loads(self.python(ns, "observer", probe))
        self.k(
            "-n",
            ns,
            "wait",
            "--for=condition=Ready=false",
            "pod/" + pod["metadata"]["name"],
            "--timeout=20s",
            timeout=25,
        )
        require(
            before == {"/healthz": 200, "/readyz": 200}
            and during == {"/healthz": 200, "/readyz": 503},
            "db-outage-readiness",
        )
        self.command(["docker", "start", CLUSTER + "-postgres"])
        self.until(
            lambda: json.loads(self.python(ns, "observer", probe, record=False))["/readyz"] == 200,
            60,
        )
        after = json.loads(self.python(ns, "observer", probe))
        self.k(
            "-n",
            ns,
            "wait",
            "--for=condition=Ready=true",
            "pod/" + pod["metadata"]["name"],
            "--timeout=60s",
            timeout=65,
        )
        current = json.loads(
            self.k(
                "-n", ns, "get", "pod", pod["metadata"]["name"], "-o", "json", record=False
            ).stdout
        )
        require(
            current["metadata"]["uid"] == pod["metadata"]["uid"]
            and current["status"]["containerStatuses"][0]["restartCount"]
            == pod["status"]["containerStatuses"][0]["restartCount"],
            "outage-no-restart",
        )
        save(
            self.output / "db-readiness.json",
            {"pod_uid": pod["metadata"]["uid"], "before": before, "during": during, "after": after},
        )
        observer = self.pod(ns, "observer")["metadata"]["name"]
        sample = (
            'import urllib.request,urllib.error,time,json; rows=[]; deadline=time.monotonic()+20;\nwhile time.monotonic()<deadline:\n try: status=urllib.request.urlopen("http://'
            + ip
            + ':8080/readyz",timeout=.5).status\n except urllib.error.HTTPError as e:status=e.code\n except OSError:status=0\n rows.append(status);\n if len(rows)==1 and status==200:open("/evidence/drain-probe-ready","w").write("ready")\n time.sleep(.05)\nprint(json.dumps(rows))'
        )
        proc = subprocess.Popen(
            [
                "kubectl",
                "--context",
                "kind-" + CLUSTER,
                "-n",
                ns,
                "exec",
                observer,
                "--",
                "python",
                "-c",
                sample,
            ],
            cwd=ROOT,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.until(
            lambda: (
                self.python(
                    ns,
                    "observer",
                    'import pathlib; print(pathlib.Path("/evidence/drain-probe-ready").exists())',
                    record=False,
                )
                == "True"
            ),
            10,
        )
        self.k(
            "-n", ns, "delete", "pod", pod["metadata"]["name"], "--wait=false", "--grace-period=40"
        )
        stdout, stderr = proc.communicate(timeout=30)
        rows = json.loads(stdout)
        require(proc.returncode == 0 and 200 in rows and 503 in rows, "drain-readiness")
        save(
            self.output / "drain-readiness.json",
            {"pod_uid": pod["metadata"]["uid"], "samples": rows, "stderr": stderr},
        )

    def failure_export(self, ns: str) -> None:
        try:
            observer = self.pod(ns, "observer")["metadata"]["name"]
            checked = self.k(
                "-n",
                ns,
                "exec",
                observer,
                "--",
                "/checker",
                "-namespace",
                ns,
                allowed=True,
                timeout=30,
            ).stdout
            (self.output / (ns + "-failed-checker.json")).write_text(checked, encoding="utf-8")
        except Exception:
            pass
        try:
            pods = json.loads(
                self.k(
                    "-n",
                    ns,
                    "get",
                    "pods",
                    "-l",
                    "app=worker",
                    "-o",
                    "json",
                    timeout=10,
                    record=False,
                ).stdout
            )["items"]
            for pod in pods:
                name = pod["metadata"]["name"]
                code = 'import pathlib,base64,json; print(json.dumps({str(p):base64.b64encode(p.read_bytes()).decode() for p in pathlib.Path("/witness").glob("*/*.jsonl")}))'
                exported = self.k(
                    "-n",
                    ns,
                    "exec",
                    name,
                    "--",
                    "python",
                    "-c",
                    code,
                    allowed=True,
                    timeout=10,
                    record=False,
                ).stdout
                (self.output / (name + "-failed-witness.json")).write_text(
                    exported, encoding="utf-8"
                )
        except Exception:
            pass

    def drop(self, database: str) -> None:
        self.sql('DROP DATABASE IF EXISTS "' + database + '" WITH (FORCE);')
        require(
            self.sql("SELECT count(*) FROM pg_database WHERE datname='" + database + "';") == "0",
            "database-drop-verified",
        )
        self.databases.discard(database)

    def diagnostics(self) -> None:
        """Best effort, independent of the expired work deadline; never raise."""
        directory = self.output / "diagnostics"
        directory.mkdir(exist_ok=True)
        deadline = time.monotonic() + 300
        index: list[dict[str, Any]] = []

        def capture(name: str, args: list[str]) -> str:
            entry: dict[str, Any] = {"file": name + ".txt", "argv": args}
            text = ""
            try:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("diagnostic total deadline exhausted")
                result = subprocess.run(
                    args,
                    cwd=ROOT,
                    env=self.env,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=min(10, remaining),
                    check=False,
                )
                entry["exit_code"] = result.returncode
                text = result.stdout + result.stderr
            except Exception as error:
                entry["error"] = str(error)
                if isinstance(error, subprocess.TimeoutExpired):
                    for partial in (error.stdout, error.stderr):
                        if partial:
                            text += (
                                partial.decode(errors="replace")
                                if isinstance(partial, bytes)
                                else partial
                            )
                text += "\n" + str(error)
            for credential in (self.password, self.demand_token):
                text = text.replace(credential, "[REDACTED]")
            (directory / entry["file"]).write_text(text, encoding="utf-8", newline="\n")
            index.append(entry)
            return text

        k = ["kubectl", "--context", "kind-" + CLUSTER, "--request-timeout=8s"]
        for name, args in (
            ("nodes-wide", ["get", "nodes", "-o", "wide"]),
            ("nodes-describe", ["describe", "node"]),
            ("pods-wide", ["get", "pods", "-A", "-o", "wide"]),
            ("events", ["get", "events", "-A", "--sort-by=.metadata.creationTimestamp"]),
        ):
            capture(name, [*k, *args])
        node = ["docker", "exec", CLUSTER + "-control-plane"]
        capture("containerd-images", [*node, "ctr", "--namespace", "k8s.io", "images", "ls"])
        capture("cri-images", [*node, "crictl", "images"])
        capture(
            "cni-config",
            [
                *node,
                "sh",
                "-c",
                "ls -la /etc/cni/net.d; cat /etc/cni/net.d/*.conflist /etc/cni/net.d/*.conf",
            ],
        )
        capture("kubelet", [*node, "journalctl", "-u", "kubelet", "--no-pager", "-n", "300"])
        pods = capture("pods-json", [*k, "get", "pods", "-A", "-o", "json"])
        try:
            for pod in json.loads(pods)["items"]:
                ready = any(
                    c["type"] == "Ready" and c["status"] == "True"
                    for c in pod.get("status", {}).get("conditions", [])
                )
                if ready:
                    continue
                ns, name = pod["metadata"]["namespace"], pod["metadata"]["name"]
                prefix = ns + "--" + name
                capture(prefix + "-describe", [*k, "-n", ns, "describe", "pod", name])
                spec = pod["spec"]
                for container in (
                    spec.get("initContainers", [])
                    + spec.get("containers", [])
                    + spec.get("ephemeralContainers", [])
                ):
                    for previous in (False, True):
                        capture(
                            prefix
                            + "--"
                            + container["name"]
                            + ("-previous" if previous else "-current"),
                            [
                                *k,
                                "-n",
                                ns,
                                "logs",
                                name,
                                "-c",
                                container["name"],
                                "--timestamps=true",
                                "--tail=300",
                                "--limit-bytes=131072",
                                *(["--previous"] if previous else []),
                            ],
                        )
        except Exception as error:
            index.append({"error": "pod inventory: " + str(error)})
        save(
            directory / "index.json",
            {"total_timeout_seconds": 300, "command_timeout_seconds": 10, "commands": index},
        )

    def cleanup(self) -> dict[str, Any]:
        started, self.live_start = self.live_start, None
        if (
            not (self.scratch / "bin/kind").exists()
            and not self.created
            and not self.databases
            and started is None
        ):
            return {"status": "PASS", "no_resources_started": True, "elapsed_live_seconds": 0}
        errors = []
        if (self.scratch / "kubeconfig").exists():
            for ns in ("dur053-kind", "dur053-baseline"):
                result = self.k(
                    "delete",
                    "namespace",
                    ns,
                    "--ignore-not-found",
                    "--wait=true",
                    "--timeout=60s",
                    allowed=True,
                    timeout=70,
                )
                if result.returncode and self.databases:
                    errors.append("namespace teardown failed")
        if self.databases:
            self.command(["docker", "start", CLUSTER + "-postgres"], allowed=True)
            try:
                self.until(
                    lambda: (
                        self.command(
                            [
                                "docker",
                                "exec",
                                CLUSTER + "-postgres",
                                "pg_isready",
                                "-U",
                                "durable",
                            ],
                            allowed=True,
                            record=False,
                        ).returncode
                        == 0
                    ),
                    30,
                )
                for database in sorted(self.databases.copy()):
                    self.drop(database)
            except Exception as error:
                errors.append(str(error))
        discovered = self.command(
            ["docker", "ps", "-a", "--format", "{{.Names}}", "--filter", "label=" + LABEL],
            allowed=True,
            record=False,
        )
        self.created.update(discovered.stdout.splitlines())
        self.command(["kind", "delete", "cluster", "--name", CLUSTER], allowed=True, timeout=120)
        for name in sorted(self.created):
            self.command(["docker", "rm", "-fv", name], allowed=True, timeout=30)
        if self.kind_network_created:
            self.command(["docker", "network", "rm", "kind"], allowed=True)
            remaining_network = self.command(
                ["docker", "network", "inspect", "kind"], allowed=True, record=False
            )
            require(remaining_network.returncode != 0, "owned-network-cleanup")
        clusters = self.command(["kind", "get", "clusters"], allowed=True)
        containers = self.command(
            ["docker", "ps", "-aq", "--filter", "label=" + LABEL], allowed=True
        )
        nodes = self.command(
            ["docker", "ps", "-aq", "--filter", "label=io.x-k8s.kind.cluster=" + CLUSTER],
            allowed=True,
        )
        require(
            clusters.returncode == containers.returncode == nodes.returncode == 0
            and CLUSTER not in clusters.stdout.splitlines()
            and not (containers.stdout + nodes.stdout).strip()
            and not self.databases
            and not errors,
            "cleanup-verified",
        )
        return {
            "status": "PASS",
            "clusters": clusters.stdout.splitlines(),
            "owned_containers": [],
            "owned_nodes": [],
            "databases_remaining": [],
            "elapsed_live_seconds": max(1, int(time.monotonic() - started) + 1) if started else 0,
        }


def run(scenario: str, output: Path, scratch_root: Path) -> int:
    supported(scenario)
    fresh_output(output)
    scratch_root.mkdir(parents=True, exist_ok=True)
    scratch = scratch_root / ("kind-" + secrets.token_hex(8))
    scratch.mkdir()
    d = Driver(output, scratch)
    manifest: dict[str, Any] = {
        "schema": "dur053.k3a.v1",
        "scenario": scenario,
        "status": "FAIL",
        "pilot": "NOT RUN (K3b)",
        "limitations": [
            "single-host lifecycle/correctness only; no capacity or HA",
            "GitHub shared-runner noise",
            "no SLO freeze or pilot result",
        ],
        "registered_live_seconds": 0,
        "live_ceiling_seconds": LIVE_RESERVATION if scenario == "smoke" else 0,
        "budget_scope": "engineering-validation",
        "measurement_claim": False,
        "cluster_started": False,
    }
    save(output / "manifest.json", manifest)
    try:
        manifest["merge_ref_sha"] = d.command(
            ["git", "rev-parse", "HEAD"], record=False
        ).stdout.strip()
        manifest["tree_sha"] = d.command(
            ["git", "rev-parse", "HEAD^{tree}"], record=False
        ).stdout.strip()
        manifest["run_id"] = os.getenv("GITHUB_RUN_ID")
        manifest["job_key"] = os.getenv("GITHUB_JOB")
        if os.getenv("GITHUB_ACTIONS"):
            repo, run_id = os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_RUN_ID"]
            job_name = "DUR-053 kind smoke" if scenario == "smoke" else "DUR-053 package check"
            jobs = github(f"repos/{repo}/actions/runs/{run_id}/jobs?per_page=100")["jobs"]
            runner_name = os.getenv("RUNNER_NAME")
            mine = [
                j
                for j in jobs
                if runner_name
                and j.get("runner_name") == runner_name
                and j.get("status") == "in_progress"
            ]
            if not mine:
                mine = [j for j in jobs if j["name"] == job_name]
            require(len(mine) == 1, "job-provenance")
            manifest["job_id"] = str(mine[0]["id"])
        manifest["head_sha"] = os.getenv("DUR053_HEAD_SHA", manifest["merge_ref_sha"])
        d.command(
            [
                sys.executable,
                "-m",
                "unittest",
                "discover",
                "-s",
                "tests",
                "-p",
                "test_k3_*.py",
            ]
        )
        d.install()
        if scenario == "cleanup":
            clusters = d.command(["kind", "get", "clusters"]).stdout.splitlines()
            if CLUSTER in clusters:
                d.command(
                    [
                        "kind",
                        "export",
                        "kubeconfig",
                        "--name",
                        CLUSTER,
                        "--kubeconfig",
                        d.env["KUBECONFIG"],
                    ]
                )
            owned = d.command(
                ["docker", "ps", "-a", "--format", "{{.Names}}", "--filter", "label=" + LABEL]
            ).stdout.splitlines()
            d.created.update(owned)
            if CLUSTER + "-postgres" in owned:
                d.command(["docker", "start", CLUSTER + "-postgres"])
                d.until(
                    lambda: (
                        d.command(
                            [
                                "docker",
                                "exec",
                                CLUSTER + "-postgres",
                                "pg_isready",
                                "-U",
                                "durable",
                            ],
                            allowed=True,
                            record=False,
                        ).returncode
                        == 0
                    ),
                    30,
                )
                d.databases.update(
                    d.sql(
                        "SELECT datname FROM pg_database WHERE datname IN ('dur053_kind','dur053_baseline');"
                    ).splitlines()
                )
        else:
            d.build()
            for name, addon in LOCK["addons"].items():
                if addon["installed_in_k3a"]:
                    d.prepare_addon(name)
            _, treatment = d.render("kind")
            _, baseline = d.render("kind-baseline")
            matched_arms(treatment, baseline)
            if scenario == "smoke":
                d.preload_addons()
                for arm in ("kind", "kind-baseline"):
                    shutil.rmtree(scratch / ("package-" + arm))
                    (output / (arm + ".yaml")).rename(output / (arm + "-unbound-endpoints.yaml"))
                addresses = d.cluster()
                manifest["cluster_started"] = d.cluster_started
                for arm in ("kind", "kind-baseline"):
                    d.batch(arm, addresses)
        manifest["status"] = "PASS"
    except Exception as error:
        manifest["error"] = (
            str(error).replace(d.password, "[REDACTED]").replace(d.demand_token, "[REDACTED]")
        )
        if d.live_start is not None:
            if d.cluster_attempted:
                try:
                    d.diagnostics()
                except Exception as diagnostic_error:
                    manifest["diagnostic_error"] = str(diagnostic_error)
            # Diagnostics cannot suppress the final manifest or cleanup.
            for ns in ("dur053-kind", "dur053-baseline"):
                try:
                    d.failure_export(ns)
                    d.k("-n", ns, "get", "pods,jobs,events", "-o", "json", allowed=True)
                    for app in ("migrate", "compatibility", "api", "worker"):
                        d.k(
                            "-n",
                            ns,
                            "logs",
                            "-l",
                            "app=" + app,
                            "--all-containers=true",
                            allowed=True,
                        )
                except Exception:
                    pass
    finally:
        try:
            manifest["cleanup"] = d.cleanup()
        except Exception as error:
            manifest["cleanup"] = {"status": "FAIL", "error": str(error)}
            manifest["status"] = "FAIL"
        scratch_ownership(scratch, scratch_root)
        shutil.rmtree(scratch)
        manifest["scratch_removed"] = not scratch.exists()
        manifest["cluster_started"] = d.cluster_started
        manifest["elapsed_live_seconds"] = elapsed_live_seconds(
            scenario,
            d.cluster_started,
            manifest["cleanup"].get("elapsed_live_seconds", LIVE_RESERVATION),
        )
        manifest["charged_seconds"] = 0
        manifest["artifact_sha256"] = artifact_hashes(output)
        save(output / "manifest.json", manifest)
    return 0 if manifest["status"] == "PASS" else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scratch-root", type=Path, required=True)
    args = parser.parse_args()
    return run(args.scenario, args.output.resolve(), args.scratch_root.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
