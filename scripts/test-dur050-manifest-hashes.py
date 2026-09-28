#!/usr/bin/env python3
"""Positive and hash-tampering controls for the DUR-050 manifest checker."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "scripts/check-dur050-manifest-hashes.py"
MANIFEST = ROOT / "experiments/m8/dur050-capacity-overload/pilot-20260924-e3d780f/cost-manifest.json"


def run(manifest: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(CHECKER), "--manifest", str(manifest), "--revision", "HEAD"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def main() -> int:
    baseline = run(MANIFEST)
    if baseline.returncode != 0:
        print(baseline.stdout, end="")
        print(baseline.stderr, end="", file=sys.stderr)
        return 1
    if "verified" not in baseline.stdout:
        raise AssertionError("checker did not report a verified committed-evidence count")

    document = json.loads(MANIFEST.read_text(encoding="utf-8"))
    record = document.get("user_deferral_closeout", {})
    if record.get("record_sha256") in (None, "REPLACE_WITH_COMMITTED_BLOB_SHA256"):
        raise AssertionError("closeout inventory record hash is not populated")
    record["record_sha256"] = "0" * 64

    with tempfile.TemporaryDirectory(prefix="dur050-manifest-hash-mutant-") as temp:
        mutant = Path(temp) / "cost-manifest.json"
        mutant.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8", newline="\n")
        result = run(mutant)
        combined = result.stdout + result.stderr
        if result.returncode == 0 or "SHA-256 mismatch" not in combined:
            raise AssertionError(f"edited manifest hash was not rejected:\n{combined}")
    print("PASS: committed manifest hashes verify; a one-hash mutation is rejected.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
