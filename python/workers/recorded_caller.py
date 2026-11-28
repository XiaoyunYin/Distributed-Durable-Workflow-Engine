"""J4 one-shot child. The only IPC descriptor is the inherited Unix socket."""

from __future__ import annotations

import json
import os
import socket
import sys
from dataclasses import asdict
from ipaddress import IPv4Address
from pathlib import Path

from workers.recorded_probe import caller_probe, inventory
from workers.recorded_wire import (
    CALLER_UID,
    MAX_IPC,
    CallIdentity,
    canonical_request,
    request_once,
    strict_json,
)


def process_status() -> dict[str, str]:
    status = {}
    for line in Path("/proc/self/status").read_text().splitlines():
        key, separator, value = line.partition(":")
        if separator:
            status[key] = value.strip()
    return status


def verify_caller(ipc: socket.socket) -> None:
    if sys.platform != "linux":
        raise RuntimeError("J4 provider caller requires Linux")
    if (
        ipc.family != socket.AF_UNIX
        or ipc.type != socket.SOCK_STREAM
        or os.getppid() != 1
        or os.getresuid() != (CALLER_UID,) * 3
        or os.getresgid() != (CALLER_UID,) * 3
        or os.getgroups()
    ):
        raise RuntimeError("isolated J4 caller identity required")
    status = process_status()
    if status["NoNewPrivs"] != "1" or any(
        int(status[key], 16) for key in ("CapInh", "CapPrm", "CapEff", "CapAmb")
    ):
        raise RuntimeError("J4 caller retains process privileges")
    for fd in Path("/proc/self/fd").iterdir():
        if int(fd.name) == ipc.fileno():
            continue
        try:
            target = fd.readlink()
        except FileNotFoundError:
            continue  # The directory enumeration's own descriptor has closed.
        if fd.name not in {"0", "1", "2"} or target.as_posix() != "/dev/null":
            raise RuntimeError("unexpected J4 caller descriptor")


def main() -> None:
    fd, addr, *flags = sys.argv[1:]
    host = IPv4Address(addr)
    with socket.socket(fileno=int(fd)) as ipc:
        verify_caller(ipc)
        if flags == ["--probe"]:
            ipc.sendall(json.dumps(caller_probe(host), separators=(",", ":")).encode() + b"\n")
            return
        if flags:
            raise ValueError("unexpected J4 caller options")
        inventory("caller")
        with ipc.makefile("rb") as reader:
            envelope = strict_json(reader.readline(MAX_IPC + 1))
            if envelope.keys() != {"call", "input", "deadline_ns"} or reader.read(1):
                raise ValueError("expected exactly one fixture IPC request")
        call = CallIdentity(**envelope["call"])
        body = canonical_request(envelope["input"])
        deadline_ns = envelope["deadline_ns"]
        if type(deadline_ns) is not int:
            raise ValueError("invalid fixture deadline")

        def emit(record: dict[str, object]) -> None:
            ipc.sendall(json.dumps(record, separators=(",", ":")).encode() + b"\n")

        try:
            reply = request_once(host, call, body, deadline_ns, emit)
            emit({"event": "reply", **asdict(reply)})
        except (OSError, ValueError) as exc:
            emit({"event": "error", "reason": type(exc).__name__})


if __name__ == "__main__":
    main()
