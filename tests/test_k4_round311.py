"""R320 idle scale-downs and closed producer rules use raw evidence."""

import hashlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dur053_k4_evidence as evidence
from test_k4_offline import (  # noqa: F401
    checker,
    episode_fixture,
    freeze,
    seal_witness,
    write,
)


def scale_down(root, f, *, busy=False):
    m, docs = episode_fixture(
        root, f, episode="short-core-1-pair-0-baseline", workflow_stride=100000
    )
    t, c = docs["timeline"], docs["confirmation"]
    c.update(kind="scale-down", target="Deployment/worker", overlap=[], reaction=None)
    # The original relay is now the removed worker; the original worker survives.
    removed = next(s for s in t["sessions"] if s["session"] == "relay")
    removed["role"] = "worker"
    invocation = t["invocations"][0]
    invocation["finish"] = 120.01
    t["receipts"][0]["at"] = 120.011
    t["history"][0]["at"] = 120.011
    t["history"][1]["at"] = 120.02
    t["workflows"][0].update(terminal=120.02, first_observed=120.03)
    c["observed_overlap_first_at"] = invocation["start"]
    if busy:
        invocation["session"] = "relay"
        t["claims"][0]["session"] = "relay"
        t["receipts"][0]["session"] = "relay"
        c["overlap"] = [
            dict(
                role="worker",
                session="relay",
                pod_uid="relay-uid",
                at=invocation["start"],
                end=120.1,
                acknowledgment="claim boundary",
                request="",
                publication="",
                workflows=[invocation["identity"]["workflow"]],
                claim=invocation["identity"],
                partition=None,
                owner="",
                epoch=None,
            )
        ]
        c["reaction"] = c["intervention"] - invocation["start"]
    seal_witness(t)
    docs["witness"]["rows"] = t["witness"]
    m["status"] = "FAIL" if not busy else "PASS"
    for key, name in evidence.FILES.items():
        write(root / name, docs[key])
        m["files"][key] = hashlib.sha256((root / name).read_bytes()).hexdigest()
    write(root / "episode.json", m)
    return m, docs


def test_idle_scale_down_is_valid_complete(tmp_path, freeze, checker):  # noqa: F811
    scale_down(tmp_path, freeze)
    result = evidence.episode(tmp_path, freeze, checker)
    old = evidence.episode(tmp_path, freeze, checker, rules=evidence.Derivation.HISTORICAL)
    assert result["valid"] and result["complete"] and result["issues"] == [], (
        "R320 idle scale-down retained the nonempty overlap gate"
    )
    assert result["status"] == "PASS", "R320 status echoed the recorded FAIL"
    assert result["scale_down_removal"] == "idle" and result["reaction"] is None
    assert result["recovery"] == [] and result["partition_reacquisition"] == []
    assert result["abandoned"] == dict(count=0, denominator=0, canceled=0, unknown=0)
    assert result["wasted_original_seconds"] == dict(lower=0, upper=0, unknown=0)
    assert not old["valid"] and not old["complete"] and old["status"] == "FAIL"
    assert old["issues"] == ["k4-confirmation-overlap"]
    assert "scale_down_removal" not in old


def test_busy_scale_down_is_unchanged(tmp_path, freeze, checker):  # noqa: F811
    scale_down(tmp_path, freeze, busy=True)
    result = evidence.episode(tmp_path, freeze, checker)
    old = evidence.episode(tmp_path, freeze, checker, rules=evidence.Derivation.HISTORICAL)
    assert result["valid"] and result["complete"] and result["status"] == "PASS"
    assert result.pop("scale_down_removal") == "busy"
    result["safety"].pop("http_5xx_by_role")
    assert result == old
    assert result["abandoned"]["denominator"] == 1 and len(result["recovery"]) == 1


@pytest.mark.parametrize("event,kind", [("worker SIGKILL", "kill"), ("worker rollout", "rollout")])
def test_other_empty_overlaps_fail(tmp_path, freeze, event, kind):  # noqa: F811
    _, docs = scale_down(tmp_path, freeze)
    docs["confirmation"]["kind"] = kind
    assert "k4-confirmation-overlap" in evidence.confirmation(
        docs["confirmation"], docs["timeline"], event
    )
    docs["confirmation"]["targets"] = []
    docs["confirmation"]["kind"] = "scale-down"
    assert "k4-confirmation-overlap" in evidence.confirmation(
        docs["confirmation"], docs["timeline"], "worker scale-down 4 to 2"
    )


@pytest.mark.parametrize("job,run_id", sorted(evidence.HISTORICAL_PRODUCERS))
def test_registered_producer_proof(tmp_path, monkeypatch, job, run_id):
    producer = dict(job=job, run_id=run_id, budget_scope="registered-campaign")
    name = "episode"
    historical = dict(episode=name, safety=dict(http_5xx=0))
    corrected = dict(episode=name, safety=dict(http_5xx=0, http_5xx_by_role=[]))
    monkeypatch.setattr(
        evidence,
        "episode",
        lambda *args, rules: historical if rules is evidence.Derivation.HISTORICAL else corrected,
    )
    monkeypatch.setattr(evidence, "safety_evidence", lambda *args, **kwargs: None)
    monkeypatch.setattr(evidence, "summaries", lambda values, **kwargs: values)
    write(tmp_path / name / "metrics.json", historical)
    write(tmp_path / "summary.json", [historical])

    def prove():
        return evidence.recorded_derivation(
            tmp_path,
            {},
            None,
            [name],
            [name],
            producer=producer,
            metrics_guard="metrics",
            summary_guard="summary",
        )

    assert prove()[0] == [historical]
    producer["run_id"] = "unregistered"
    with pytest.raises(ValueError, match="summary"):
        prove()
    write(tmp_path / name / "metrics.json", corrected)
    write(tmp_path / "summary.json", [corrected])
    assert prove()[0] == [corrected]
    producer["run_id"] = run_id
    with pytest.raises(ValueError, match="summary"):
        prove()
    producer["budget_scope"] = "engineering-validation"
    assert prove()[0] == [corrected]
    write(tmp_path / "summary.json", [historical])
    with pytest.raises(ValueError, match="summary"):
        prove()


@pytest.mark.parametrize("status", ["invalid", "unexercised", "interrupted", "FAIL"])
def test_non_derived_statuses_are_preserved(tmp_path, freeze, checker, status):  # noqa: F811
    m, _ = scale_down(tmp_path, freeze)
    m["status"] = status
    write(tmp_path / "episode.json", m)
    if status == "FAIL":
        write(
            tmp_path / "harness-source.json",
            dict(
                status="failure",
                component="collector",
                errors=["k4-claim-ack-clock-evidence-missing"],
                missing=[],
            ),
        )
    result = evidence.episode(tmp_path, freeze, checker)
    assert result["status"] == status


def test_status_reconstructs_failure(tmp_path, freeze, checker):  # noqa: F811
    m, _ = scale_down(tmp_path, freeze)
    m.update(status="PASS", witness_available=False)
    write(tmp_path / "episode.json", m)
    result = evidence.episode(tmp_path, freeze, checker)
    assert not result["valid"] and result["status"] == "FAIL", (
        "R320 invalid issued episode kept recorded PASS"
    )
