#!/usr/bin/env python3
"""Offline K3 freeze from committed accepted evidence only; never starts resources."""

import argparse
from pathlib import Path

from dur053_k3 import ROOT, save
from dur053_k3_policy import require
from dur053_pilot import EVIDENCE
from dur053_pilot_evidence import freeze


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--checker", type=Path, required=True, help="built cmd/dur049-checker binary"
    )
    args = parser.parse_args()
    target = ROOT / EVIDENCE / "freeze.json"
    require(not target.exists(), "pilot-freeze-no-overwrite")
    result = freeze(ROOT, args.checker.resolve())
    save(target, result)
    print(target)


if __name__ == "__main__":
    main()
