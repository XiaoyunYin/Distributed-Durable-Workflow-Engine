import hashlib
import json
import shutil
import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZipFile

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import j3_measurement as measurement  # noqa: E402
import j3_qualification as driver_module  # noqa: E402


@pytest.fixture
def qualification(tmp_path, monkeypatch):
    root = tmp_path / "qualification"
    shutil.copytree(measurement.QUALIFICATION, root)
    monkeypatch.setattr(measurement, "QUALIFICATION", root)
    index = measurement.read(root / "sha256.json")
    published = measurement.read(root / measurement.RELEASE_INDEX)
    confirmation = measurement.read(root / "seal/confirmation-check.json")
    schedule = (root / "seal/sealed-schedule-summary.json").read_bytes()
    confirmation["seal"]["sha256"] = hashlib.sha256(schedule).hexdigest()
    archive = tmp_path / "seal.zip"
    with ZipFile(archive, "w") as bundle:
        bundle.writestr("sealed-schedule.json", schedule)
        bundle.writestr("schedule-seal.json", json.dumps(confirmation["seal"]))
    digest = measurement.digest(archive)
    (receipt,) = [r for r in index["artifacts"] if r["id"] == measurement.SEAL_ARTIFACT]
    (released,) = [
        r for r in published["artifacts"] if r["actions_id"] == measurement.SEAL_ARTIFACT
    ]
    receipt["digest"] = released["actions_digest"] = f"sha256:{digest}"
    released["sha256"] = confirmation["artifact_digest"] = digest
    measurement.save(root / measurement.RELEASE_INDEX, published)
    monkeypatch.setattr(
        measurement, "RELEASE_INDEX_SHA256", measurement.digest(root / measurement.RELEASE_INDEX)
    )
    measurement.save(root / "seal/confirmation-check.json", confirmation)
    index["files"]["seal/confirmation-check.json"]["sha256"] = measurement.digest(
        root / "seal/confirmation-check.json"
    )
    measurement.save(root / "sha256.json", index)
    return root, archive


def test_measurement_uses_the_reviewed_qualification_seal(qualification):
    root, archive = qualification
    verified = measurement.verified_schedule(archive)
    assert verified["qualification_run"] == 46050616178
    assert verified["artifact_id"] == 15235825154
    assert verified["schedule"]["delta_s"] == 480
    assert verified["schedule"]["arrival_volume"] == 19_268
    assert verified["forecast"]["forecast_seconds"] == 14_524
    assert [row[:3] for row in measurement.measurement_arms(verified["schedule"])] == [
        ("primary", "outage", 2700),
        ("matched-healthy", "matched_healthy", 2700),
        ("foreign", "foreign", 1200),
        ("scrape-loss", "scrape_loss", 720),
    ]
    archive.write_bytes(archive.read_bytes() + b"substituted")
    with pytest.raises(ValueError, match="schedule artifact digest mismatch"):
        measurement.verified_schedule(archive)
    (root / "confirmation/verdict.json").write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="qualification confirmation/verdict.json differs"):
        measurement.verified_schedule(archive)


def test_review_receipt_rejects_a_different_dispatch_head(monkeypatch):
    monkeypatch.setenv("GITHUB_SHA", "a" * 40)
    monkeypatch.setenv("J3_REVIEWED_HEAD", "b" * 40)
    monkeypatch.setenv("J3_REVIEW_ROUND", "611")
    with pytest.raises(ValueError, match="Claude-reviewed dispatch head"):
        measurement.reviewed_candidate()
    monkeypatch.setenv("J3_REVIEWED_HEAD", "a" * 40)
    assert measurement.reviewed_candidate() == {"head": "a" * 40, "review_round": 611}


def test_measurement_rejection_stops_before_bootstrap(tmp_path, monkeypatch):
    monkeypatch.setenv("GITHUB_RUN_ID", "42")
    args = SimpleNamespace(stage="measurement", evidence=tmp_path)
    monkeypatch.setattr(driver_module, "hosted_arguments", lambda *_a, **_kw: args)

    def deny(*_args):
        raise ValueError("missing current runner gate admission")

    monkeypatch.setattr(driver_module, "verify_admission", deny)
    monkeypatch.setattr(driver_module, "Qualification", lambda *_: pytest.fail("started driver"))
    with pytest.raises(ValueError, match="missing current runner gate admission"):
        driver_module.main()
    assert measurement.read(tmp_path / "verdict.json")["status"] == "INVALID"


@pytest.mark.parametrize("case", ["pass", "halt", "fail", "volume"])
def test_measurement_runs_four_arms_and_retains_failed_or_unperformed_arms(
    tmp_path, monkeypatch, case
):
    (tmp_path / "sender.sha256").write_text("sender", encoding="utf-8")
    measurement.save(tmp_path / "run.json", {})
    args = SimpleNamespace(evidence=tmp_path, stage="measurement", sender=Path("sender"))
    root = measurement.QUALIFICATION
    schedule = measurement.read(root / "seal/sealed-schedule-summary.json")
    forecast = measurement.read(root / "confirmation/measurement-forecast.json")
    calls, resets = [], []
    driver = SimpleNamespace(
        reset=lambda: resets.append(driver.output.name),
        fresh_episode=lambda *_a: (None, None, None),
    )
    monkeypatch.setenv("J3_JOB_STARTED", "1000")
    monkeypatch.setattr(driver_module.time, "time", lambda: 22_100 if case == "halt" else 1001)
    monkeypatch.setattr(driver_module, "runner_pressure", lambda *_: nullcontext())
    monkeypatch.setattr(driver_module, "retain_postgres_log", lambda *_a, **_kw: None)
    monkeypatch.setattr(driver_module, "retain_monitor_logs", lambda *_a, **_kw: None)

    def run_episode(_d, _p, target, arm, _run, _s, _o, _b, duration, bounds, outcome):
        assert target["campaign"] == "j3-r426-42"
        calls.append((driver.output.name, arm, duration))
        if case == "fail" and driver.output.name == "matched-healthy":
            raise driver_module.EpisodeFailure("registered deadline missed")
        if bounds:
            measurement.save(
                driver.output / "identity-drain.json",
                {
                    "admitted": 19_267 if case == "volume" else 19_268,
                },
            )
        outcome.update(behavior={"status": "PASS"}, delivery={"status": "PASS"})
        return 10 * measurement.SECOND, 20 * measurement.SECOND

    monkeypatch.setattr(driver_module, "run_episode", run_episode)
    if case in {"fail", "volume"}:
        message = "registered deadline" if case == "fail" else "sealed arrival volume"
        with pytest.raises(ValueError, match=message):
            driver_module.run_qualification(driver, args, {}, {}, "j3-r426-42", schedule, forecast)
    else:
        driver_module.run_qualification(driver, args, {}, {}, "j3-r426-42", schedule, forecast)
    verdict = measurement.read(tmp_path / "verdict.json")
    assert (
        verdict["status"]
        == {"pass": "PASS", "halt": "HALT", "fail": "FAIL", "volume": "INVALID"}[case]
    )
    total = {"pass": 4, "halt": 0, "fail": 2, "volume": 1}[case]
    assert calls == [row[:3] for row in measurement.measurement_arms(schedule)][:total]
    assert resets == list(
        measurement.EPISODES[: {"pass": 3, "halt": 0, "fail": 1, "volume": 0}[case]]
    )
    assert all(
        verdict["episodes"][name]["status"] == "UNPERFORMED"
        for name in measurement.EPISODES[total:]
    )
    assert not (tmp_path / "sealed-schedule.json").exists()


@pytest.mark.parametrize(
    "case", ["pass", "fail", "replay_missing", "cleanup_missing", "arm_inputs_missing", "not_armed"]
)
def test_offline_finalize_keeps_live_verdict_and_indexes_partial_evidence(
    tmp_path, monkeypatch, case
):
    monkeypatch.setenv("GITHUB_RUN_ID", "42")
    monkeypatch.setenv("GITHUB_SHA", "head")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    stage = {"fail": "FAIL", "not_armed": "NOT_ARMED"}.get(case, "PASS")
    names = measurement.EPISODES if stage == "PASS" else ()
    target = {"namespace": "target", "campaign": "j3-r426-42"}
    measurement.save(
        tmp_path / "verdict.json",
        {
            "status": stage,
            "episodes": {name: {"status": "PASS", "target": target} for name in names},
        },
    )
    if case != "cleanup_missing":
        measurement.save(tmp_path / "cleanup.json", {"status": "PASS"})
    for file in measurement.JOB_INPUTS:
        path = tmp_path / file
        path.parent.mkdir(parents=True, exist_ok=True)
        measurement.save(path, {})
    monkeypatch.setattr(measurement.time, "time_ns", lambda: 1)
    for name in names:
        path = tmp_path / name
        path.mkdir()
        for file in measurement.EPISODE_INPUTS:
            measurement.save(path / file, {})
        measurement.save(path / "timing.json", {"origin_ns": 2})
        measurement.save(
            path / "run.json",
            {
                "episode": name,
                "target": target,
                "run": "42",
                "head": "head",
                "attempt": "1",
                "campaign": "j3-r426-42",
            },
        )
        measurement.seal_arm_inputs(path, 2)
    if case == "arm_inputs_missing":
        (tmp_path / "primary/pre-arm-inputs.json").unlink()

    def replay(path, _target):
        if case == "replay_missing" and path.name == "primary":
            raise ValueError("missing replay data")
        return {"status": "PASS"}

    monkeypatch.setattr(measurement, "replay_episode", replay)
    assert measurement.finalize(tmp_path) == (case == "pass")
    verdict = measurement.read(tmp_path / "verdict.json")
    assert verdict["status"] == ("INVALID" if case.endswith("_missing") else stage)
    assert set(verdict["episodes"]) == set(measurement.EPISODES)
    index = measurement.read(tmp_path / "sha256.json")
    assert index == measurement.evidence_hashes(tmp_path)
    assert "offline-replay.json" in index and "verdict.json" in index
