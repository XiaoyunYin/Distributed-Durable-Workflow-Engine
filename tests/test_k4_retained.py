"""Round 245 real shapes; the old-image claim fields are recovered in the fixture.

Four overlap workflows are retained. Credentials/specs and unrelated rows are
removed; trimmed streams are resealed. The episode arrivals journal was not
retained, so assembly uses an explicitly empty test plan, never fabricated sends.
This is an assembly regression, not a replay of registered metrics.
"""

from __future__ import annotations

import base64
import copy
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import dur053_k4_collectors as collectors  # noqa: E402
import dur053_k4_driver as driver  # noqa: E402
from dur053_k4 import episode_identity  # noqa: E402


@pytest.fixture
def retained():
    return json.loads((ROOT / "tests/fixtures/k4-retained-assembly.json").read_bytes())


def retained_traces(retained):
    return [
        row
        for name, text in retained["files"].items()
        if name.endswith(".trace.jsonl")
        for line in text.splitlines()
        if (row := json.loads(line)).get("operation") == "claim"
    ]


def test_retained_claim_body_identity(retained):
    snapshot = retained["snapshot"]
    traces = retained_traces(retained)
    a = snapshot["attempts"][0]
    matching = [r for r in traces if r["request_id"] == a["worker_request_id"]]
    assert matching and matching[0]["worker_id"] != a["worker_id"]
    claims = collectors.durable_claims(
        snapshot["attempts"], traces, retained["manifest"]["namespace"], snapshot["claim_audit"]
    )
    assert len(claims) == len(snapshot["attempts"])
    assert claims[0]["session"] == matching[0]["session"]
    bad = copy.deepcopy(traces)
    next(r for r in bad if r["request_id"] == a["worker_request_id"])["body_hash"] = "0" * 64
    with pytest.raises(ValueError, match="k4-claim-body-hash-mismatch"):
        collectors.durable_claims(snapshot["attempts"], bad, "ns", snapshot["claim_audit"])
    duplicate = dict(matching[0], session="another-session")
    with pytest.raises(ValueError, match="k4-claim-session-evidence-missing"):
        collectors.durable_claims(
            snapshot["attempts"], traces + [duplicate], "ns", snapshot["claim_audit"]
        )


def test_retained_assembly(tmp_path, retained):
    d = driver.K4Driver.__new__(driver.K4Driver)
    d.episode_output = tmp_path
    d.snapshot = lambda _: retained["snapshot"]
    d.samples = retained["samples"]
    d.witness_raw = lambda _: {
        name: base64.b64encode(text.encode()).decode() for name, text in retained["files"].items()
    }
    c = collectors.Streams.__new__(collectors.Streams)
    c.pods, c.endpoints = retained["pods"], retained["endpoints"]
    c.log_text = lambda pattern="log-*.jsonl": retained[
        "cri" if pattern == "cri-exits.jsonl" else "logs"
    ]
    d.collector = c
    d.base = SimpleNamespace(python=Mock(return_value='{"event":"plan","plan":{"rows":[]}}\n'))
    m = retained["manifest"]
    d.assemble(
        m["namespace"],
        "offline",
        episode_identity(m["episode"]),
        retained["confirmation"],
        retained["targets"],
        m["warmup_start"],
        retained["outcome"]["observed_at"],
        retained["quiet"],
        retained["outcome"]["timeout"],
        m,
    )
    timeline = json.loads((tmp_path / "timeline.json").read_bytes())
    assert len(timeline["workflows"]) == 4
    assert len(timeline["claims"]) == len(retained["snapshot"]["attempts"])
    assert timeline["receipts"] and timeline["invocations"]
    assert any(
        s["death_evidence"].startswith("containerd-task-exit:") for s in timeline["sessions"]
    )
    assert any(s["endpoints"] for s in timeline["sessions"])
    assert len(json.loads((tmp_path / "confirmation.json").read_bytes())["targets"]) == 2
    assert all((tmp_path / name).exists() for name in driver.FILES.values())
    observers = json.loads((tmp_path / "observers.json").read_bytes())["rows"]
    assert len(observers[0]["active_claims"]) == 4, "authenticated worker claims lost"
    workers = {s["session"]: s["pod_uid"] for s in timeline["sessions"] if s["role"] == "worker"}
    for claim in observers[0]["active_claims"]:
        durable = next(c for c in timeline["claims"] if c["identity"] == claim["identity"])
        assert claim["pod_uid"] == workers[durable["session"]]
    # Cached journal survives any later assembly failure, without fetching the pod again.
    assert d.arrival_journal(m["namespace"])
    d.base.python.assert_called_once()
