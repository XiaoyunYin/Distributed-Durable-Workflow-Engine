"""Offline checks for the Phase B topology and retained evidence boundary."""

from __future__ import annotations

import base64
import copy
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dur053_k3  # noqa: E402
from dur053_k6_b_evidence import Evidence, SmokeFailure  # noqa: E402
from dur053_k6_b_smoke import inherited_failure, run  # noqa: E402
from dur053_k6_identity import deny_egress, obj, render, validate_rendered  # noqa: E402

NS = "dur053-k6-offline"


class StubK3Driver:
    def __init__(self, output: Path, scratch: Path) -> None:
        self.output = output
        self.scratch = scratch
        self.env = dict(os.environ)
        self.password = "stub-password"
        self.demand_token = "stub-token"
        self.live_start = None
        self.cluster_started = False
        self.cluster_attempted = False
        self.databases: set[str] = set()
        self.unittest_env: dict[str, str] | None = None

    def command(self, args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if "unittest" in args:
            self.unittest_env = kwargs["env"]  # type: ignore[assignment]
        value = "stub-sha\n" if "rev-parse" in args else ""
        return subprocess.CompletedProcess(args, 0, value, "")

    def install(self) -> None:
        pass

    def build(self) -> None:
        pass

    def prepare_addon(self, name: str) -> None:
        pass

    def render(self, arm: str) -> tuple[None, dict]:
        (self.scratch / ("package-" + arm)).mkdir()
        (self.output / (arm + ".yaml")).write_text("stub")
        return None, {}

    def preload_addons(self) -> None:
        pass

    def cluster(self) -> dict[str, str]:
        self.cluster_started = True
        return {"192.0.2.1": "192.0.2.2"}

    def batch(self, arm: str, addresses: dict[str, str]) -> None:
        pass

    def cleanup(self) -> dict[str, object]:
        return {"status": "PASS", "elapsed_live_seconds": 0}


def run_stubbed_k3(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, k6_output: Path | None
) -> tuple[int, StubK3Driver]:
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    driver: StubK3Driver | None = None

    def make_driver(output: Path, scratch: Path) -> StubK3Driver:
        nonlocal driver
        driver = StubK3Driver(output, scratch)
        return driver

    monkeypatch.setattr(dur053_k3, "Driver", make_driver)
    monkeypatch.setattr(dur053_k3, "matched_arms", lambda *_: None)
    monkeypatch.setattr(dur053_k3, "scratch_ownership", lambda *_: None)
    monkeypatch.setattr(dur053_k3, "artifact_hashes", lambda *_: {})
    monkeypatch.setattr(dur053_k3, "LIVE_RESERVATION", 0)
    result = dur053_k3.run("smoke", tmp_path / "k3", tmp_path / "scratch", k6_output)
    assert driver is not None
    return result, driver


def test_k3_unittest_child_cannot_inherit_phase_b_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "phase-b"  # run defaults to no Phase B despite the environment.
    monkeypatch.setenv("DUR053_K6_B_OUTPUT", str(output))
    result, driver = run_stubbed_k3(tmp_path, monkeypatch, None)
    assert result == 0
    assert driver.unittest_env is not None
    assert "DUR053_K6_B_OUTPUT" not in driver.unittest_env
    assert not output.exists()


def test_k3_phase_b_handoff_failure_isolated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import dur053_k6_b_smoke

    def fail_before_smoke(*_args: object) -> int:
        raise RuntimeError("synthetic handoff failure")

    monkeypatch.setattr(dur053_k6_b_smoke, "run", fail_before_smoke)
    output = tmp_path / "phase-b"
    result, _ = run_stubbed_k3(tmp_path, monkeypatch, output)
    assert result == 1
    k3_manifest = json.loads((tmp_path / "k3" / "manifest.json").read_bytes())
    phase_b = json.loads((output / "smoke.json").read_bytes())
    assert k3_manifest["status"] == "PASS"
    assert phase_b["status"] == "FAIL"
    assert phase_b["reason"] == "UNEXPECTED_RUNTIMEERROR"
    assert phase_b["outcomes"] == [
        {
            "step": "phase-b-handoff",
            "status": "FAIL",
            "reason": "UNEXPECTED_RUNTIMEERROR",
        }
    ]


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
