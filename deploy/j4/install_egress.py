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
        ["nft", "--json", "list", "table", "inet", TABLE],
        capture_output=True,
        text=True,
        check=True,
    )
    pending = proof.with_suffix(".pending")
    with pending.open("x", encoding="utf-8") as out:
        json.dump({"stub_ipv4": str(stub), "effective": json.loads(result.stdout)}, out)
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
