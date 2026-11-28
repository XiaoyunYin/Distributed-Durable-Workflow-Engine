"""J4 stub and independent reachability witnesses."""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import threading
from ipaddress import IPv4Address
from pathlib import Path

from workers import recorded_stub
from workers.recorded_probe import (
    DNS_PROBE,
    clock_sample,
    connects,
    inventory,
    parse_targets,
    udp_reachable,
)
from workers.recorded_wire import MAX_IPC, strict_json

EVIDENCE = Path("/evidence")


def append(path: Path, data: dict) -> None:
    raw = json.dumps(data, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    with path.open("ab", buffering=0) as out:
        if out.write(raw) != len(raw):
            raise OSError("incomplete J4 witness append")
        os.fsync(out.fileno())


def listen(port: int, listener_ready: threading.Event | None = None) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", port))
        server.listen()
        if listener_ready is not None:
            listener_ready.set()
        while True:
            client, (address, _) = server.accept()
            with client:
                append(
                    EVIDENCE / "decoy.jsonl",
                    {"source_ipv4": address, "port": port, "protocol": "TCP", **clock_sample()},
                )


def listen_dns_datagrams() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server:
        server.bind(("0.0.0.0", 53))
        while True:
            data, sender = server.recvfrom(64)
            append(
                EVIDENCE / "decoy.jsonl",
                {"source_ipv4": sender[0], "port": 53, "protocol": "UDP", **clock_sample()},
            )
            if data == DNS_PROBE:
                server.sendto(DNS_PROBE, sender)


def run_stub() -> None:
    append(EVIDENCE / "inventory.jsonl", {**inventory("stub"), **clock_sample()})
    ready = threading.Event()
    thread = threading.Thread(target=listen, args=(8084, ready), daemon=True)
    thread.start()
    if not ready.wait(2):
        raise RuntimeError("J4 wrong-port decoy did not start")
    sys.argv = [
        "recorded_stub",
        "--cells",
        "/etc/j4/cells.json",
        "--journal",
        str(EVIDENCE / "stub.jsonl"),
    ]
    recorded_stub.main()


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("stub")
    decoy = sub.add_parser("decoy")
    decoy.add_argument("port", type=int, choices=[53, 8082])
    reach = sub.add_parser("reachable")
    reach.add_argument("stub", type=IPv4Address)
    args = parser.parse_args()
    if args.command == "stub":
        run_stub()
    elif args.command == "decoy":
        if args.port == 53:
            threading.Thread(target=listen_dns_datagrams, daemon=True).start()
        listen(args.port)
    else:
        plan = strict_json(sys.stdin.buffer.read(MAX_IPC))
        targets = parse_targets(plan, args.stub)
        reached = {name: connects(target) for name, target in targets.items()}
        reached["dns_udp"] = udp_reachable(targets["dns"])
        print(json.dumps({"reachable": reached, **clock_sample()}, sort_keys=True))
        if not all(reached.values()):
            raise SystemExit(1)


if __name__ == "__main__":
    main()
