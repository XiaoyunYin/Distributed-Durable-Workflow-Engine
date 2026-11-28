"""PID 1 worker wrapper for J4 only."""

from __future__ import annotations

import os
import time
from uuid import uuid4

from workers.__main__ import run
from workers.recorded_activity import (
    EGRESS_PROOF,
    ProviderConfig,
    isolated_caller,
    recorded_provider_modes,
)
from workers.recorded_probe import (
    CASE_ROOT,
    clock_sample,
    connects,
    inventory,
    probe_targets,
    udp_reachable,
)
from workers.recorded_wire import MAX_IPC, DeadlineReader, remaining, strict_json

from deploy.j4.install_egress import require_nft_snapshot
from deploy.j4.probes import EVIDENCE, append


def verify_provider(config: ProviderConfig) -> dict:
    parent = inventory("worker")
    stored_rules = strict_json(EGRESS_PROOF.read_bytes())
    require_nft_snapshot(config.stub_ipv4, stored_rules["effective"])
    targets = probe_targets(config.stub_ipv4)
    parent_denied = {
        name: not connects(targets[name])
        for name in ("stub", "wrong_port", "wrong_pod", "outside", "dns")
    }
    parent_denied["dns_udp"] = not udp_reachable(targets["dns"])
    if not all(parent_denied.values()):
        raise RuntimeError(f"worker parent reached forbidden J4 targets: {parent_denied}")
    deadline = time.monotonic_ns() + 5_000_000_000
    with isolated_caller(config, probe=True) as (sock, process):
        with DeadlineReader(sock, deadline).makefile("rb") as stream:
            raw = stream.readline(MAX_IPC + 1)
        if process.wait(timeout=remaining(deadline)) != 0 or not raw or len(raw) > MAX_IPC:
            raise RuntimeError("J4 provider-isolation probe failed")
    proof = strict_json(raw)
    boot = clock_sample()["boot_id"]
    if proof["boot_id"] != boot or proof["stub"]["boot_id"] != boot:
        raise RuntimeError("J4 provider-isolation probe crossed boot identities")
    return {"worker": parent, "caller": proof, "parent_denied": parent_denied}


def run_worker() -> None:
    ProviderConfig.validate_worker()
    activity, resilience = recorded_provider_modes()
    if not activity:
        raise RuntimeError("J4 worker deployment requires the recorded-provider activity")
    config = ProviderConfig.load()
    case = strict_json((CASE_ROOT / "case.json").read_bytes())
    arm, workflow = case["arm"], case["workflow_id"]
    session = uuid4().hex
    journal = EVIDENCE / "worker.jsonl"

    def emit(name: str, fields: dict) -> None:
        append(
            journal,
            {
                "hook": name,
                "fields": fields,
                "cell": config.cell,
                "process_session": session,
                "pid": os.getpid(),
                **clock_sample(),
            },
        )

    def hook(name: str, fields: dict[str, str]) -> None:
        emit(name, fields)
        same_attempt = fields.get("workflow") == workflow and fields.get("attempt") == "1"
        if same_attempt and (
            (arm == "C2" and name == "j4_result_receipt")
            or (
                arm == "B1"
                and name == "j4_provider"
                and fields.get("event") == "reserved"
                and fields.get("ordinal") == "1"
            )
        ):
            emit("cut_held", {"arm": arm, "workflow": workflow})
            # Both registered cuts end this process. A broken controller leaves
            # the gate closed until the cell deadline and cleanup.
            deadline = time.monotonic() + 300
            while time.monotonic() < deadline:
                time.sleep(0.05)
            raise RuntimeError("J4 cut gate expired without a witnessed worker kill")

    emit("isolation_verified", verify_provider(config))
    run(
        deployment_hook=hook,
        health_details=lambda: {
            "process_session": session,
            "recorded_activity": activity,
            "provider_resilience": resilience,
            **clock_sample(),
        },
    )


if __name__ == "__main__":
    run_worker()
