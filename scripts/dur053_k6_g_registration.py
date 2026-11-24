"""Bind Phase G worker startup to the apiserver and authenticate its registration."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from dur053_k6_b_smoke import require, subject
from dur053_k6_identity import WORKER

AUDIENCE = "durable-worker-control"
INTAKE = "tokenreview-worker-intake"


def utc(at: datetime | None = None) -> str:
    at = (at or datetime.now(UTC)).astimezone(UTC)
    return f"{at:%Y-%m-%dT%H:%M:%S}.{at.microsecond:06d}000Z"


def observe_runtime(pod: dict[str, Any], container: str = "worker") -> dict[str, Any]:
    meta = pod["metadata"]
    [status] = [s for s in pod["status"]["containerStatuses"] if s["name"] == container]
    raw_id = status["containerID"]
    runtime, separator, container_id = raw_id.partition("://")
    started = status["state"]["running"]["startedAt"]
    require(separator == "://" and runtime in {"containerd", "docker"}, "G_RUNTIME_PREFIX")
    return {
        "resource_version": meta["resourceVersion"],
        "namespace": meta["namespace"],
        "pod_name": meta["name"],
        "containerID": raw_id,
        "startedAt": started,
        "identity": {
            "pod_uid": meta["uid"],
            "container_id": container_id,
            "restart": status["restartCount"],
            "container_started_utc": utc(datetime.fromisoformat(started)),
        },
    }


def bind_registration(
    observation: dict[str, Any],
    verified_after: dict[str, Any],
    review: dict[str, Any],
    account_uid: str,
    session: dict[str, Any],
) -> dict[str, Any]:
    runtime = observation["identity"]
    require(
        runtime == verified_after["identity"]
        and runtime == {field: session.get(field) for field in runtime},
        "G_IDENTITY_OBSERVATION_BINDING",
    )
    status = review.get("status", {})
    user = status.get("user", {})
    extra = user.get("extra", {})
    principal = subject(observation["namespace"], WORKER)
    claimant = f"{principal}:pod:{observation['pod_name']}:uid:{runtime['pod_uid']}"
    require(
        status.get("authenticated") is True
        and not status.get("error")
        and status.get("audiences") == [AUDIENCE]
        and user.get("username") == principal
        and user.get("uid") == account_uid
        and extra.get("authentication.kubernetes.io/pod-name") == [observation["pod_name"]]
        and extra.get("authentication.kubernetes.io/pod-uid") == [runtime["pod_uid"]]
        and session.get("claimant") == claimant,
        "G_REGISTRATION_INTAKE_BINDING",
    )
    return {
        "session": session,
        "identity": {
            "pod_uid": runtime["pod_uid"],
            "claimant": claimant,
            "service_account_uid": account_uid,
            "observer": INTAKE,
            "audience": AUDIENCE,
        },
    }
