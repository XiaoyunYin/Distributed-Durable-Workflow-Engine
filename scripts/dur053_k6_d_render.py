"""Render the deployed Phase D roles and their pre-submission compatibility Jobs."""

from __future__ import annotations

import copy
import json

from dur053_k3 import K8S
from dur053_k3_policy import DIGEST
from dur053_k6_c_render import CLIENT, ENGINE, PORT, RELAY, pod_peer, policy, traffic
from dur053_k6_identity import REVIEWER, WORKER, obj

NS = "dur053-k6-d"
ACTIVITY = "dur053.cooperating"
CONTROL = "worker-control"
OBSERVER = "observer"
ROLES = ("api", CONTROL, "scheduler", "relay", "worker", OBSERVER)
CA_DIR = "/k6-ca"


def base() -> dict[tuple[str, str], dict]:
    return {
        (item["kind"], item["metadata"]["name"]): item
        for item in json.loads((K8S / "base/resources.yaml").read_bytes())["items"]
    }


def set_env(container: dict, **values: str) -> None:
    env = container.setdefault("env", [])
    env[:] = [row for row in env if row["name"] not in values]
    env.extend({"name": name, "value": value} for name, value in values.items())


def mount(pod: dict, name: str, directory: str, source: dict) -> None:
    pod["volumes"].append({"name": name, **source})
    pod["containers"][0]["volumeMounts"].append(
        {"name": name, "mountPath": directory, "readOnly": True}
    )


def effect_topology(objects: list[dict]) -> list[dict]:
    """Replace the Phase C fixture peers with the deployed roles."""
    result = []
    for item in copy.deepcopy(objects):
        if item["kind"] == "Pod":
            if item["metadata"]["name"] not in {CLIENT, ENGINE}:
                raise ValueError("D_EFFECT_UNACCOUNTED_POD")
            continue
        if item["kind"] == "NetworkPolicy":
            data = json.dumps(item).replace(f'"{CLIENT}"', '"worker"')
            item = json.loads(data.replace(f'"{ENGINE}"', f'"{CONTROL}"'))
        result.append(item)
    return result


def render(
    images: dict[str, str],
    endpoints: dict[str, str],
    public_key: str,
    plan_hash: str,
    *,
    namespace: str = NS,
) -> list[dict]:
    if any(not DIGEST.fullmatch(images[kind]) for kind in ("runtime", "worker")):
        raise ValueError("D_RUNTIME_IMAGE_DIGEST")
    stock = base()
    selection = [
        *(("Deployment", name) for name in ROLES),
        *(("Service", name) for name in ("api", CONTROL, "kafka")),
        *(
            ("NetworkPolicy", name)
            for name in (
                "dns",
                "api-egress",
                "worker-control-egress",
                "scheduler-egress",
                "relay-egress",
                "worker-egress",
                "compatibility-egress",
                "api-ingress",
                "worker-control-ingress",
            )
        ),
        ("ServiceAccount", OBSERVER),
        ("ServiceAccount", "compatibility"),
    ]
    result = []
    for kind, name in selection:
        item = stock[kind, name]
        item["metadata"]["namespace"] = namespace
        if kind == "NetworkPolicy":
            data = json.dumps(item)
            for placeholder, address in endpoints.items():
                data = data.replace(f"{placeholder}/32", f"{address}/32")
            item = json.loads(data)
            if name == "dns":
                item["spec"]["podSelector"] = {
                    "matchExpressions": [{"key": "app", "operator": "In", "values": list(ROLES)}]
                }
        if kind == "Deployment":
            spec = item["spec"]
            spec["replicas"] = 1
            spec["strategy"] = {"type": "Recreate"}
            pod = spec["template"]["spec"]
            container = pod["containers"][0]
            container["image"] = images["worker" if name in ("worker", OBSERVER) else "runtime"]
            if name in {"api", CONTROL}:
                pod["serviceAccountName"] = REVIEWER
            elif name in {"scheduler", "relay"}:
                pod["serviceAccountName"] = "default"
            if name in {"api", CONTROL, "scheduler", "relay"}:
                set_env(container, DUR053_K6_ACTIVITY="1")
            if name in {CONTROL, "scheduler"}:
                set_env(container, DUR053_K6_RECEIPT_PUBLIC_KEY=public_key)
            if name == "scheduler":
                mount(pod, "load-plan", "/k6-plan", {"configMap": {"name": "k6-load-plan"}})
                set_env(
                    container,
                    DUR053_K6_PLAN_FILE="/k6-plan/plan.json",
                    DUR053_K6_PLAN_SHA256=plan_hash,
                )
            if name == CONTROL:
                # ReadConfig requires an actual owner-only file even on projected volumes.
                mount(pod, "allocator", "/k6-private", {"emptyDir": {"medium": "Memory"}})
                pod["volumes"].append(
                    {"name": "allocator-source", "secret": {"secretName": "k6-allocator"}}
                )
                installer = {
                    "name": "allocator-install",
                    "image": images["worker"],
                    "imagePullPolicy": "Never",
                    "command": [
                        "sh",
                        "-ec",
                        "umask 077; cat /source/config.json > /k6-private/config.json",
                    ],
                    "volumeMounts": [
                        {"name": "allocator", "mountPath": "/k6-private"},
                        {"name": "allocator-source", "mountPath": "/source", "readOnly": True},
                    ],
                    "securityContext": container["securityContext"],
                    "resources": container["resources"],
                }
                pod["initContainers"] = [installer]
                for key in ("startupProbe", "readinessProbe", "livenessProbe"):
                    if key in container and "httpGet" in container[key]:
                        container[key]["httpGet"]["scheme"] = "HTTPS"
                set_env(
                    container,
                    RUNTIME_TOKEN_SERVICE_ACCOUNT=WORKER,
                    DUR053_K6_WORKER_CONTROL_FILE="/k6-private/config.json",
                    DUR053_K6_OBSERVE="1",
                )
            if name == "worker":
                pod["serviceAccountName"] = WORKER
                pod.pop("initContainers")  # The base initializer is only for the K3 witness PVC.
                pod["volumes"] = [row for row in pod["volumes"] if row["name"] != "witness"]
                container["volumeMounts"] = [
                    row for row in container["volumeMounts"] if row["name"] != "witness"
                ]
                container["env"] = [
                    row for row in container["env"] if row["name"] != "DUR053_WITNESS_DIR"
                ]
                identity = next(row for row in pod["volumes"] if row["name"] == "identity")
                identity["projected"]["sources"].append(
                    {
                        "serviceAccountToken": {
                            "audience": "dur053-k6-sink",
                            "expirationSeconds": 600,
                            "path": "sink",
                        }
                    }
                )
                mount(pod, "effect-ca", CA_DIR, {"configMap": {"name": "k6-effect-ca"}})
                set_env(
                    container,
                    DUR053_K6_ACTIVITY="1",
                    CONTROL_API_URL=f"https://{CONTROL}:8080",
                    DUR053_K6_CONTROL_CA_FILE=f"{CA_DIR}/ca.crt",
                    DUR053_K6_RELAY_URL=f"https://{RELAY}:{PORT}",
                    DUR053_K6_RELAY_CA_FILE=f"{CA_DIR}/ca.crt",
                    DUR053_K6_SINK_TOKEN_FILE="/identity/sink",
                )
            if name == OBSERVER:
                identity = next(row for row in pod["volumes"] if row["name"] == "identity")
                identity["projected"]["sources"] = identity["projected"]["sources"][:1]
                container["env"] = [
                    row for row in container["env"] if row["name"] != "DATABASE_URL"
                ]
        result.append(item)
    result += [
        policy(
            namespace,
            "observer-api-only",
            OBSERVER,
            "Egress",
            [traffic("to", 8080, pod_peer("api"))],
        ),
        obj(
            "Endpoints",
            "kafka",
            namespace,
            subsets=[
                {
                    "addresses": [{"ip": endpoints["192.0.2.2"]}],
                    "ports": [{"name": "tcp", "port": 19092, "protocol": "TCP"}],
                }
            ],
        ),
    ]
    return result


CASES = {
    "old-runtime": ("old_runtime", "worker", False, True, False, False),
    "old-worker": ("runtime", "old_worker", True, False, False, False),
    "class-drift": ("runtime", "worker", True, True, True, False),
    "mixed-survivors": ("runtime", "worker", True, True, False, True),
    "opt-out": ("runtime", "worker", False, False, False, False),
    "new-new": ("runtime", "worker", True, True, False, False),
}


def compatibility_job(images: dict[str, str], name: str) -> dict:
    runtime, worker, runtime_k6, worker_k6, drift, mixed = CASES[name]
    job = base()["Job", "compatibility"]
    job["metadata"].update(namespace=NS, name="compatibility-" + name)
    pod = job["spec"]["template"]["spec"]
    main = pod["containers"][0]
    main["image"] = images[runtime]
    set_env(main, DUR053_K6_ACTIVITY=str(int(runtime_k6)))
    if runtime == "runtime":
        main["command"] += (
            ["--surviving-worker-registry", "/registry/survivor.json"]
            if mixed
            else ["--no-surviving-workers"]
        )
    export = pod["initContainers"][0]
    export["image"] = images[worker]
    set_env(export, DUR053_K6_ACTIVITY=str(int(worker_k6)))
    if drift:
        export["command"][-1] = (
            "import json\nfrom workers.registry import default_registry\n"
            "rows=default_registry().manifest()\n"
            'row=next(r for r in rows if r["name"]=="dur053.cooperating")\n'
            'row["effect_class"]="PURE_ACTIVITY"\n'
            'open("/registry/worker.json","w").write(json.dumps(rows))'
        )
    if mixed:
        survivor = copy.deepcopy(export)
        survivor.update(name="surviving-worker-registry", image=images["old_worker"])
        survivor["command"][-1] = survivor["command"][-1].replace("worker.json", "survivor.json")
        set_env(survivor, DUR053_K6_ACTIVITY="0")
        pod["initContainers"].append(survivor)
    for init in pod["initContainers"]:
        filename = "survivor.json" if init["name"] == "surviving-worker-registry" else "worker.json"
        init["command"][-1] += f'\nprint(open("/registry/{filename}").read())'
    return job
