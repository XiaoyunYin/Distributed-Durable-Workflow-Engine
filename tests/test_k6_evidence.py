"""Exercise the public K6 checker process contract from a non-Go consumer."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_k6_checker_reports_missing_index_as_versioned_evidence(tmp_path: Path) -> None:
    checker = tmp_path / ("checker.exe" if os.name == "nt" else "checker")
    subprocess.run(
        ["go", "build", "-p", "1", "-o", str(checker), "./cmd/dur049-checker"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        timeout=120,
    )
    episode = tmp_path / "episode"
    episode.mkdir()
    (episode / "checker.json").write_text('{"status":"PASS"}\n', encoding="utf-8")
    invocation = [str(checker), "-k6-offline-v1", str(episode)]
    first = subprocess.run(invocation, capture_output=True, check=False, timeout=10)
    second = subprocess.run(invocation, capture_output=True, check=False, timeout=10)
    assert first.returncode == second.returncode == 1
    assert first.stderr == second.stderr == b""
    assert first.stdout == second.stdout
    report = json.loads(first.stdout)
    assert (
        first.stdout
        == (
            json.dumps(report, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
        ).encode()
    )
    assert report["schema"] == "dur053.k6-checker.v1"
    assert report["status"] == "failed"
    assert report["completeness"] == "FAIL"
    assert report["findings"] == [
        {
            "code": "required_proof_missing:index.json",
            "kind": "missing",
            "predicate": "RelationIntegrity",
            "source": "index.json",
            "witness": None,
        }
    ]
    assert [label["id"] for label in report["labels"]] == [f"I{i}" for i in range(1, 14)]
    assert report["invariants_covered"] == []
    assert report["invariants_not_covered"] == [f"I{i}" for i in range(1, 14)]
