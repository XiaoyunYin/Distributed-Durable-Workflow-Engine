"""J4 worker activity. Claim authority stays with the worker process."""

from __future__ import annotations

import json
import os
import socket
import stat
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from ipaddress import IPv4Address
from pathlib import Path
from typing import Any
from uuid import uuid4

from workers.recorded_wire import (
    CALLER_UID,
    EXPECTED_HASH,
    IDENTITY,
    MAX_IPC,
    MISMATCH,
    MODE,
    CallIdentity,
    DeadlineReader,
    ProviderReply,
    Trace,
    canonical_request,
    remaining,
    request_hash,
    strict_json,
)

CORE_TIMEOUT_NS = 10_000_000_000
CONFIG_PATH = Path("/etc/j4/worker.json")
EGRESS_PROOF = Path("/run/j4-egress/installed.json")
RESILIENCE_MODE = "J4_PROVIDER_RESILIENCE"


def recorded_provider_modes() -> tuple[bool, bool]:
    enabled = []
    for name in (MODE, RESILIENCE_MODE):
        value = os.getenv(name, "0")
        if value not in {"0", "1"}:
            raise ValueError(f"{name} must be 0 or 1")
        enabled.append(value == "1")
    activity, resilience = enabled
    if resilience and not activity:
        raise ValueError(f"{RESILIENCE_MODE} requires {MODE}=1")
    return activity, resilience


def recorded_activity_enabled() -> bool:
    return recorded_provider_modes()[0]


@dataclass(frozen=True)
class ProviderConfig:
    stub_ipv4: IPv4Address
    cell: str

    @classmethod
    def load(cls) -> ProviderConfig:
        value = strict_json(CONFIG_PATH.read_bytes())
        if (
            not isinstance(value, dict)
            or value.keys() != {"stub_ipv4", "cell"}
            or not IDENTITY.fullmatch(value["cell"])
        ):
            raise ValueError("invalid J4 worker configuration")
        address = IPv4Address(value["stub_ipv4"])
        st = EGRESS_PROOF.lstat()
        if not stat.S_ISREG(st.st_mode) or st.st_uid != 0 or st.st_mode & 0o222:
            raise RuntimeError("expected the read-only J4 egress init proof")
        proof = strict_json(EGRESS_PROOF.read_bytes())
        if proof["stub_ipv4"] != str(address) or not proof["effective"]["nftables"]:
            raise RuntimeError("J4 egress init proof disagrees with worker configuration")
        return cls(address, value["cell"])

    @classmethod
    def validate_worker(cls) -> None:
        if sys.platform != "linux" or os.getpid() != 1 or os.geteuid() != 0:
            raise RuntimeError("J4 requires an isolated Linux worker container")
        cls.load()


@contextmanager
def isolated_caller(
    config: ProviderConfig, *, probe: bool = False
) -> Iterator[tuple[socket.socket, subprocess.Popen[bytes]]]:
    if sys.platform != "linux":
        raise RuntimeError("J4 provider caller requires Linux")
    parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    with parent, child:
        process = subprocess.Popen(
            [
                sys.executable,
                "-B",
                "-s",
                "-m",
                "workers.recorded_caller",
                str(child.fileno()),
                str(config.stub_ipv4),
                *(["--probe"] if probe else []),
            ],
            env={"PYTHONPATH": str(Path(__file__).resolve().parents[1])},
            user=CALLER_UID,
            group=CALLER_UID,
            extra_groups=(),
            close_fds=True,
            pass_fds=(child.fileno(),),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        child.close()
        try:
            yield parent, process
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()


def call_provider(
    config: ProviderConfig,
    call: CallIdentity,
    value: dict[str, str],
    deadline_ns: int,
    trace: Trace,
) -> ProviderReply:
    envelope = (
        json.dumps(
            {"call": asdict(call), "input": value, "deadline_ns": deadline_ns},
            separators=(",", ":"),
        ).encode("ascii")
        + b"\n"
    )
    if len(envelope) > MAX_IPC:
        raise ValueError("fixture IPC request too large")
    with isolated_caller(config) as (parent, process):
        # The network deadline is enforced inside the child. Leave a short IPC
        # tail for it to return the witnessed deadline event and exit.
        ipc_deadline = deadline_ns + 1_000_000_000
        reply = None
        parent.settimeout(remaining(ipc_deadline))
        parent.sendall(envelope)
        parent.shutdown(socket.SHUT_WR)
        with DeadlineReader(parent, ipc_deadline).makefile("rb") as reader:
            while line := reader.readline(MAX_IPC + 1):
                event = strict_json(line)
                kind = event.pop("event")
                if kind == "send" and reply is None:
                    trace({"event": kind, **event})
                elif kind == "reply" and reply is None:
                    reply = ProviderReply(**event)
                else:
                    raise RuntimeError("fixture HTTP caller failed")
        if process.wait(timeout=remaining(ipc_deadline)) != 0 or reply is None:
            raise RuntimeError("fixture HTTP caller did not return a reply")
        return reply


class RecordedActivity:
    def complete(
        self,
        value: Any,
        *,
        workflow_id: str,
        node_id: str,
        iteration: int,
        attempt: int,
        trace: Trace,
    ) -> tuple[str, dict[str, str]]:
        digest = request_hash(canonical_request(value))
        config = ProviderConfig.load()
        call = CallIdentity(
            config.cell, workflow_id, node_id, iteration, attempt, attempt, uuid4().hex
        )
        deadline_ns = time.monotonic_ns() + CORE_TIMEOUT_NS
        reply = call_provider(
            config,
            call,
            value,
            deadline_ns,
            lambda event: trace({**event, **asdict(call), "deadline_ns": deadline_ns}),
        )
        trace(
            {
                "event": "reply",
                **asdict(call),
                "status": reply.status,
                "at_ns": reply.observed_ns,
                "deadline_ns": deadline_ns,
            }
        )
        if reply.status == 422 and reply.payload == {"reason": MISMATCH}:
            return "FAILED_FINAL", {"reason": MISMATCH}
        expected = {
            "completion": "fixture alpha",
            "request_sha256": digest,
            "provider_call_id": call.call_id,
        }
        if reply.status != 200 or digest != EXPECTED_HASH or reply.payload != expected:
            raise ValueError("unexpected core provider outcome")
        return "SUCCEEDED", expected
