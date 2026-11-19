"""Pinned Chaos Mesh lifecycle and evidence predicates for K6."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from uuid import uuid4

from dur053_k4_interventions import chaos_ack
from dur053_k6_b_evidence import SmokeFailure
from dur053_k6_b_smoke import json_data
from dur053_k6_e1_join import expect

API = "chaos-mesh.org/v1alpha1"
HOLD_SECONDS = 60


@dataclass(frozen=True)
class Target:
    namespace: str
    pod: str
    uid: str
    role: str
    container: str
    container_id: str
    restart_count: int
    node: str
    host_pid: int
    process: str
    session: str


def select_target(active: list[Target], seed: int) -> Target:
    by_uid = {target.uid: target for target in active}
    expect("F1A_UNIQUE_ACTIVE_UIDS", len(by_uid), len(active))
    if not by_uid:
        raise SmokeFailure("F1A_NO_ACTIVE_TARGET observed=[] expected=ready_active_uid")
    return by_uid[sorted(by_uid)[seed % len(by_uid)]]


def bind(confirmed: Target, observed: Target) -> None:
    expect("F1A_TARGET_DRIFT", asdict(observed), asdict(confirmed))


def render_chaos(
    episode: dict[str, Any],
    target: Target,
    name: str,
    *,
    peers: list[str] | None = None,
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    backend, kind, *action = episode["backend"].split("/")
    expect("F1A_REGISTRY_BACKEND", backend, "chaos-mesh")
    expect("F1A_TARGET_ROLE", target.role, episode["target_role"])
    spec = {
        **(options or {}),
        "selector": {"pods": {target.namespace: [target.pod]}},
        "mode": "one",
    }
    if action:
        spec["action"] = action[0]
    if kind == "PodChaos":
        if action == ["pod-kill"]:
            spec["gracePeriod"] = 0
        elif action == ["container-kill"]:
            spec["containerNames"] = [target.container]
        else:
            raise SmokeFailure(f"F1A_UNSUPPORTED_BACKEND observed={episode['backend']!r}")
    elif kind in {"NetworkChaos", "TimeChaos"}:
        spec["duration"] = f"{HOLD_SECONDS}s"
        if action == ["partition"]:
            spec["direction"] = "to"
        if episode["cell"] == "medium-4":
            expect("F1A_CONTROL_PEERS_REQUIRED", bool(peers), True)
        if peers is not None:
            expect("F1A_VALID_PEER_NAMES", bool(peers) and len(set(peers)) == len(peers), True)
            spec["target"] = {
                "selector": {"pods": {target.namespace: sorted(peers)}},
                "mode": "all",
            }
    else:
        raise SmokeFailure(f"F1A_UNSUPPORTED_BACKEND observed={episode['backend']!r}")
    return {
        "apiVersion": API,
        "kind": kind,
        "metadata": {"name": name, "namespace": target.namespace},
        "spec": spec,
    }


def backend_selected(obj: dict[str, Any], target: Target, peers: list[str] | None = None) -> bool:
    if not chaos_ack(obj):  # type: ignore[no-untyped-call]
        return False
    source = f"{target.namespace}/{target.pod}"
    if obj["spec"].get("action") == "container-kill":
        source += f"/{target.container}"
    expected = [(".", source)]
    expected += [(".Target", f"{target.namespace}/{pod}") for pod in peers or []]
    observed = [
        (row.get("selectorKey"), row.get("id"))
        for row in obj["status"]["experiment"]["containerRecords"]
    ]
    if sorted(observed, key=repr) != sorted(expected, key=repr):
        raise SmokeFailure(f"F1A_BACKEND_SELECTION observed={observed!r} expected={expected!r}")
    return True


def prove_death(
    target: Target, launched_ns: int, old_process: str | None, exits: list[dict[str, Any]]
) -> dict[str, Any]:
    expect("F1A_PROCESS_DEATH", old_process == target.process, False)
    matching = [row for row in exits if row["container_id"] == target.container_id.split("://")[1]]
    expect("F1A_CRI_EXIT_COUNT", len(matching) <= 1, True)
    if matching:
        death = matching[0]
        expect("F1A_CRI_EXIT_AFTER_APPLY", int(death["at"] * 1e9) >= launched_ns, True)
        if target.role == "scheduler":
            expect(
                "F1A_CRI_EXIT_STATUS",
                (death["exit_status"], death["exit_status_source"]),
                (137, "explicit"),
            )
    return {"old_process": target.process, "observed_process": old_process, "cri_exit": matching}


def prove_restoration(
    absent_ns: int, observation: dict[str, Any], required: dict[str, Any]
) -> None:
    expect("F1A_RESTORE_AFTER_ABSENCE", observation.get("at_ns", 0) > absent_ns, True)
    for key, expected in required.items():
        expect(f"F1A_RESTORE_{key.upper()}", observation.get(key), expected)


class Chaos:
    """Create once, retain controller evidence, and delete the exact object."""

    def __init__(
        self,
        smoke: Any,
        schema_dir: Path,
        episode: dict[str, Any],
        target: Target,
        peers: list[str] | None = None,
        options: dict[str, Any] | None = None,
    ) -> None:
        self.smoke, self.target, self.peers = smoke, target, peers
        self.namespace = target.namespace
        self.name = f"{episode['cell']}-{uuid4().hex[:12]}"
        self.manifest = render_chaos(episode, target, self.name, peers=peers, options=options)
        self.kind = self.manifest["kind"].lower()
        self.resource = self.kind
        self.schema_dir = schema_dir
        self.issued_ns: int | None = None
        self.uid: str | None = None
        self.absent_ns: int | None = None
        self.absent_monotonic: float | None = None
        self.removal_requested_ns: int | None = None
        self.deadline = 0.0
        self.possibly_created = False
        self.history: list[dict[str, Any]] = []

    def preflight(self, crds: list[dict[str, Any]]) -> None:
        rows = [
            (crd["spec"]["names"]["plural"], version["schema"]["openAPIV3Schema"])
            for crd in crds
            if crd.get("spec", {}).get("group") == API.split("/")[0]
            and crd["spec"]["names"]["kind"] == self.manifest["kind"]
            for version in crd["spec"]["versions"]
            if version["name"] == API.split("/")[1] and version["served"]
        ]
        expect("F1A_PINNED_CRD_SCHEMA_COUNT", len(rows), 1)
        self.resource, schema = rows[0]
        # Kubeconform lowercases the resource kind in its filename template.
        (self.schema_dir / f"{self.kind}.json").write_bytes(json_data(schema))
        self.smoke.retain("chaos-manifest", self.manifest)
        raw = json_data(self.manifest)
        self.smoke.command(
            "F1A_PINNED_CRD_SCHEMA",
            "kubeconform",
            "-strict",
            "-summary",
            "-schema-location",
            str(self.schema_dir / "{{.ResourceKind}}.json"),
            data=raw,
        )
        self.smoke.k(
            "F1A_SERVER_DRY_RUN",
            "create",
            "--validate=strict",
            "--dry-run=server",
            "-f",
            "-",
            data=raw,
            timeout=6,
        )

    def apply(self, reobserve: Callable[[], Target]) -> None:
        expect("F1A_ONE_SHOT", self.issued_ns, None)
        bind(self.target, reobserve())
        self.issued_ns = time.time_ns()
        self.deadline = time.monotonic() + HOLD_SECONDS
        self.possibly_created = True
        self.smoke.retain("chaos-applied", {"issued_ns": self.issued_ns})
        created = json.loads(
            self.smoke.k(
                "F1A_CHAOS_CREATE",
                "create",
                "-f",
                "-",
                "-o",
                "json",
                data=json_data(self.manifest),
                timeout=6,
            )
        )
        self.uid = created["metadata"]["uid"]

    def sample(self, stage: str) -> dict[str, Any] | None:
        raw = self.smoke.k(
            "F1A_CHAOS_SAMPLE",
            "-n",
            self.namespace,
            "get",
            self.kind,
            self.name,
            "--ignore-not-found",
            "-o",
            "json",
            timeout=5,
        )
        obj = json.loads(raw) if raw.strip() else None
        if obj is not None and self.uid is not None:
            expect("F1A_CHAOS_UID", obj["metadata"]["uid"], self.uid)
        self.history.append({"stage": stage, "at_ns": time.time_ns(), "object": obj})
        self.smoke.retain("chaos-status", self.history)
        return obj

    def events(self) -> None:
        self.smoke.retain(
            "chaos-events",
            json.loads(
                self.smoke.k(
                    "F1A_CHAOS_EVENTS",
                    "-n",
                    self.namespace,
                    "get",
                    "events",
                    "--field-selector",
                    f"involvedObject.name={self.name}",
                    "-o",
                    "json",
                    timeout=5,
                )
            ),
        )

    def remove(self) -> int:
        before = self.sample("before-remove")
        self.removal_requested_ns = time.time_ns()
        self.smoke.retain("chaos-removal", {"requested_ns": self.removal_requested_ns})
        self.smoke.k(
            "F1A_CHAOS_REMOVE",
            "-n",
            self.namespace,
            "delete",
            self.kind,
            self.name,
            "--wait=false",
            "--ignore-not-found",
            timeout=6,
        )
        until = time.monotonic() + 12
        while time.monotonic() < until:
            if self.sample("recovering") is None:
                absent_ns = self.absent_ns = time.time_ns()
                self.absent_monotonic = time.monotonic()
                self.possibly_created = False
                self.smoke.retain(
                    "chaos-removal",
                    {
                        "requested_ns": self.removal_requested_ns,
                        "absent_ns": absent_ns,
                        "issued_ns": self.issued_ns,
                        "elapsed_seconds": (absent_ns - (self.issued_ns or absent_ns)) / 1e9,
                    },
                )
                try:
                    if before is not None:
                        self.uid = self.uid or before["metadata"]["uid"]
                        self.recovery_watch(before["metadata"]["resourceVersion"])
                finally:
                    self.events()
                return absent_ns
            time.sleep(0.25)
        raise SmokeFailure("F1A_CHAOS_REMOVAL observed=present expected=absent")

    def recovery_watch(self, version: str) -> None:
        query = urlencode(
            {
                "fieldSelector": f"metadata.name={self.name}",
                "resourceVersion": version,
                "watch": "true",
                "timeoutSeconds": "2",
            }
        )
        raw = self.smoke.k(
            "F1A_CHAOS_RECOVERY_WATCH",
            "get",
            "--raw",
            f"/apis/{API}/namespaces/{self.namespace}/{self.resource}?{query}",
            timeout=6,
        )
        rows = [json.loads(line) for line in raw.splitlines()]
        self.smoke.retain("chaos-recovery-watch", rows)
        expect(
            "F1A_CHAOS_DELETED_EVENT",
            any(
                row["type"] == "DELETED" and row["object"]["metadata"]["uid"] == self.uid
                for row in rows
            ),
            True,
        )
