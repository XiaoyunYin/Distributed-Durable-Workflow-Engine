"""Install the provider UID's only egress rule in the J4 worker pod netns."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from ipaddress import IPv4Address
from pathlib import Path

from workers.recorded_wire import CALLER_UID, STUB_PORT

TABLE = "j4_provider"


def require_nft_snapshot(stub: IPv4Address, snapshot: dict) -> None:
    parent = {"family": "inet", "table": TABLE}
    records = [entry for entry in snapshot["nftables"] if "metainfo" not in entry]
    if len(records) != 5 or list(records[0]) != ["table"] or list(records[1]) != ["chain"]:
        raise ValueError("J4 egress table has unexpected entries")
    table, chain = records[0]["table"], records[1]["chain"]
    expected_chain = {
        **parent,
        "name": "output",
        "type": "filter",
        "hook": "output",
        "prio": 0,
        "policy": "accept",
    }
    if (
        table.get("family") != "inet"
        or table.get("name") != TABLE
        or any(chain.get(key) != value for key, value in expected_chain.items())
    ):
        raise ValueError("J4 egress base chain differs")

    def match(left: dict, right: object) -> dict:
        return {"match": {"left": left, "op": "==", "right": right}}

    uid = match({"meta": {"key": "skuid"}}, CALLER_UID)
    address = match({"payload": {"protocol": "ip", "field": "daddr"}}, str(stub))
    port = match({"payload": {"protocol": "tcp", "field": "dport"}}, STUB_PORT)
    ipv4_guard = match({"meta": {"key": "nfproto"}}, "ipv4")
    tcp_guard = match({"meta": {"key": "l4proto"}}, "tcp")
    counter = {"counter": None}
    expected = [
        [uid, address, port, counter, {"accept": None}],
        [uid, counter, {"drop": None}],
        [address, port, counter, {"drop": None}],
    ]
    for entry, want in zip(records[2:], expected, strict=True):
        if list(entry) != ["rule"]:
            raise ValueError("unexpected J4 egress table entry")
        rule = entry["rule"]
        if any(rule.get(key) != value for key, value in {**parent, "chain": "output"}.items()):
            raise ValueError("J4 egress rule belongs to a different chain")
        expr = [
            counter if list(item) == ["counter"] else item
            for item in rule["expr"]
            if not (item == ipv4_guard and address in want or item == tcp_guard and port in want)
        ]
        if expr != want:
            raise ValueError("J4 egress rule differs from the single provider allow tuple")


def rules(stub: IPv4Address) -> str:
    if stub.is_unspecified or stub.is_loopback or stub.is_multicast or stub.is_link_local:
        raise ValueError("expected the stub pod IPv4 address")
    return f"""table inet {TABLE} {{
    chain output {{
        type filter hook output priority filter; policy accept;
        meta skuid {CALLER_UID} ip daddr {stub} tcp dport {STUB_PORT} counter accept
        meta skuid {CALLER_UID} counter drop
        ip daddr {stub} tcp dport {STUB_PORT} counter drop
    }}
}}
"""


def install(stub: IPv4Address, proof: Path) -> None:
    if proof.exists():
        raise RuntimeError("J4 egress proof already exists in this pod")
    policy = rules(stub)
    subprocess.run(["nft", "--check", "-f", "-"], input=policy, text=True, check=True)
    subprocess.run(["nft", "-f", "-"], input=policy, text=True, check=True)
    result = subprocess.run(
        ["nft", "--json", "--numeric", "list", "table", "inet", TABLE],
        capture_output=True,
        text=True,
        check=True,
    )
    effective = json.loads(result.stdout)
    require_nft_snapshot(stub, effective)
    pending = proof.with_suffix(".pending")
    with pending.open("x", encoding="utf-8") as out:
        json.dump({"stub_ipv4": str(stub), "effective": effective}, out)
        out.flush()
        os.fsync(out.fileno())
    pending.chmod(0o444)
    pending.replace(proof)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("stub", type=IPv4Address)
    parser.add_argument("proof", type=Path)
    args = parser.parse_args()
    install(args.stub, args.proof)


if __name__ == "__main__":
    main()
