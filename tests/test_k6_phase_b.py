"""Offline checks for the Phase B topology and retained evidence boundary."""

from __future__ import annotations

import base64
import copy
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from dur053_k6_b_evidence import Evidence, SmokeFailure  # noqa: E402
from dur053_k6_b_smoke import inherited_failure, run  # noqa: E402
from dur053_k6_identity import deny_egress, obj, render, validate_rendered  # noqa: E402

NS = "dur053-k6-offline"


def test_k6_phase_b_rendered_identity() -> None:
    objects = render(NS, "runtime@sha256:offline", "postgres@sha256:offline")
    validate_rendered(objects, NS)
    approver = next(item for item in objects if item["metadata"]["name"] == "k6-approver")
    assert approver == {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {"name": "k6-approver", "namespace": NS},
        "automountServiceAccountToken": False,
    }
    binding = next(item for item in objects if item["kind"] == "Secret")
    assert binding == obj("Secret", "k6-approval-binding", NS, type="Opaque", immutable=True)
    assert [item["subjects"] for item in objects if item["kind"].endswith("RoleBinding")] == [
        [{"kind": "ServiceAccount", "name": "k6-reviewer", "namespace": NS}]
    ]
    for kind in ("Deployment", "Service"):
        with pytest.raises(ValueError, match="K6_OPERATOR_OBJECT_OR_MOUNT"):
            validate_rendered([*objects, obj(kind, "operator", NS)], NS)
    leaked = copy.deepcopy(objects)
    pod = next(item for item in leaked if item["kind"] == "Pod")
    pod["spec"]["containers"][0]["volumeMounts"] = [{"name": "operator-token"}]
    with pytest.raises(ValueError, match="K6_OPERATOR_OBJECT_OR_MOUNT"):
        validate_rendered(leaked, NS)
    with pytest.raises(ValueError, match="K6_RENDERED_OBJECT_INVENTORY"):
        validate_rendered([*objects, obj("RoleBinding", "reader-binding", NS)], NS)
    assert deny_egress(NS)["spec"] == {"podSelector": {}, "policyTypes": ["Egress"], "egress": []}


@pytest.mark.parametrize(
    "leaked",
    [
        b"test-password",
        base64.b64encode(b"test-password"),
        b"-----BEGIN PRIVATE KEY-----",
        b"eyJabcdefghijk.abcdefghijkl.abcdefghijkl",
    ],
)
def test_k6_phase_b_evidence_rejects_credentials(tmp_path: Path, leaked: bytes) -> None:
    evidence = Evidence(tmp_path)
    evidence.secret("test-password")
    evidence.write("inputs.json", {"token_sha256": "0" * 64, "subject": "synthetic"})
    with (
        pytest.raises(SmokeFailure, match="CREDENTIAL_SCAN_FAILED"),
        evidence.step("forged-result") as row,
    ):
        row["result"] = leaked.decode()
    report = json.loads((tmp_path / "smoke.json").read_bytes())
    assert report["outcomes"] == [
        {"step": "forged-result", "status": "FAIL", "reason": "CREDENTIAL_SCAN_FAILED"}
    ]
    (tmp_path / "stray.txt").write_bytes(leaked)
    with pytest.raises(SmokeFailure, match="CREDENTIAL_SCAN_FAILED"):
        evidence.scan()
    assert sorted(path.name for path in tmp_path.iterdir()) == ["inputs.json", "smoke.json"]
    evidence.scan()


def test_k6_phase_b_failure_retained(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RUNNER_TEMP", raising=False)
    output = tmp_path / "standalone"
    assert run(output) == 1
    report = json.loads((output / "smoke.json").read_bytes())
    assert report["reason"] == "HOSTED_LINUX_RUNNER_REQUIRED"
    assert report["status"] == "FAIL"
    assert report["charged_seconds"] == 0
    assert report["measurement_claim"] is False
    assert [row["status"] for row in report["outcomes"]] == ["FAIL", "PASS"]
    forwarded = tmp_path / "inherited"
    inherited_failure(forwarded)
    assert json.loads((forwarded / "smoke.json").read_bytes())["reason"] == (
        "SHARED_KIND_HARNESS_FAILED_BEFORE_PHASE_B"
    )
