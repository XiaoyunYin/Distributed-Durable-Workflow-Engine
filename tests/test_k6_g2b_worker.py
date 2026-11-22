from __future__ import annotations

import hashlib
import json
from pathlib import Path

from pytest import MonkeyPatch
from workers.k6_evidence import WorkerLedger, transfer


def test_k6_g2b_nested_credentials(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    settings = {
        "POD_UID": "pod-uid",
        "POD_NAME": "worker",
        "CONTAINER_ID": "container",
        "CONTAINER_RESTART_COUNT": "0",
        "CONTAINER_STARTED_NS": "1",
        "RUNTIME_NAMESPACE": "episode",
        "DUR053_K6_G_EPISODE": "medium-12-0",
        "DUR053_K6_G_RESET": "reset",
        "DUR053_K6_G_BARRIER": "barrier",
    }
    for key, value in settings.items():
        monkeypatch.setenv(key, value)
    sentinel = "eyJLeVNlY3JldA.nested-sentinel.signature"
    writer = WorkerLedger(tmp_path)
    invocation, attempt = writer.begin("workflow", "node", 0, 1, sentinel, "claim-request")
    raw = {
        "request_id": "request",
        "logical_key": "logical",
        "intent_id": "00000000-0000-0000-0000-000000000001",
        "resource_id": "resource",
        "grant_token": sentinel,
        "attempt": 1,
        "arguments": {"outer": [{"token": sentinel}]},
    }
    traffic = transfer("apply", raw, invocation, json.dumps(raw).encode())
    writer.append("send", invocation, traffic=traffic)
    writer.append("timeout", invocation, traffic=traffic)
    writer.append("finish", invocation, attempt=attempt)
    writer.checkpoint(terminal=True)
    public = writer.path.read_bytes()
    assert sentinel.encode() not in public
    digest = hashlib.sha256(sentinel.encode()).hexdigest().encode()
    assert b'"claim_hash":"' + digest in public
    assert b'"grant_hash":"' + digest in public
    lines = public.splitlines(keepends=True)
    seal = json.loads(lines[-1])
    assert seal["mode"] == "terminal"
    assert seal["last"] == len(lines) - 2
    assert seal["sha256"] == hashlib.sha256(b"".join(lines[1:-1])).hexdigest()
