import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

from j2_qualification_evidence import (  # noqa: E402
    CLOSEOUT_SECONDS,
    classify,
    closeout,
    extract,
    file_record,
    read,
    scrape_summary,
    seal,
    verdict,
    write,
)
from j2_qualify_volume import runner_cgroup  # noqa: E402


def test_runner_cgroup_resolves_nested_path_and_mount_root(tmp_path):
    (tmp_path / "cgroup").write_text("4:cpu:/old\n0::/runner.slice/job.scope\n")
    (tmp_path / "mountinfo").write_text(
        "25 22 0:23 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n"
        r"36 25 0:23 /runner.slice /my\040mount rw - cgroup2 cgroup rw"
        "\n"
        "37 25 0:24 /runner.slice/job.scope /old rw - cgroup cgroup rw\n"
    )
    group = tmp_path / "my mount" / "job.scope"
    group.mkdir(parents=True)
    values = {
        "cpu.max": "200000 100000",
        "memory.max": "2147483648",
        "cpu.pressure": "some avg10=0.00\nfull avg10=0.00",
        "memory.pressure": "some avg10=0.01",
    }
    for name, value in values.items():
        (group / name).write_text(value + "\n")

    assert runner_cgroup(tmp_path, tmp_path) == {
        "mount": "/my mount",
        "mount_root": "/runner.slice",
        "path": "/my mount/job.scope",
        **{name: {"present": True, "value": value} for name, value in values.items()},
    }


def test_runner_cgroup_records_missing_root_files(tmp_path):
    (tmp_path / "cgroup").write_text("0::/\n")
    (tmp_path / "mountinfo").write_text("25 22 0:23 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n")
    (tmp_path / "sys" / "fs" / "cgroup").mkdir(parents=True)

    assert runner_cgroup(tmp_path, tmp_path) == {
        "mount": "/sys/fs/cgroup",
        "mount_root": "/",
        "path": "/sys/fs/cgroup",
        **{
            name: {"present": False}
            for name in ("cpu.max", "memory.max", "cpu.pressure", "memory.pressure")
        },
    }


@pytest.mark.parametrize(
    "membership", ["0:cpu:/\n", "0::relative\n", "0::/../escape\n", "0::/\n0::/x\n"]
)
def test_runner_cgroup_rejects_malformed_membership(tmp_path, membership):
    (tmp_path / "cgroup").write_text(membership)
    with pytest.raises(ValueError, match="cgroup"):
        runner_cgroup(tmp_path, tmp_path)


def test_runner_cgroup_rejects_unrelated_mount(tmp_path):
    (tmp_path / "cgroup").write_text("0::/runner.scope\n")
    (tmp_path / "mountinfo").write_text(
        "25 22 0:23 /outside /sys/fs/cgroup rw - cgroup2 cgroup rw\n"
    )
    with pytest.raises(ValueError, match="no cgroup2 mount"):
        runner_cgroup(tmp_path, tmp_path)


@pytest.mark.parametrize(
    ("outcome", "result", "elapsed", "expected"),
    [
        ("success", {"status": "PASS"}, CLOSEOUT_SECONDS, "pass"),
        ("success", {"status": "PASS"}, CLOSEOUT_SECONDS + 1, "capacity_failure"),
        ("success", {"status": "PASS"}, -1, "invalid"),
        ("success", {"status": "PASS"}, None, "invalid"),
        ("failure", {"status": "PASS"}, 1, "invalid"),
        ("failure", {"status": "FAIL", "error": "kubectl timed out"}, None, "invalid"),
        ("skipped", {}, None, "invalid"),
        (
            "failure",
            {"status": "FAIL", "population_started": False, "capacity_criterion": "oom"},
            None,
            "invalid",
        ),
        (
            "failure",
            {"status": "FAIL", "capacity_criterion": "oom", "cleanup_error": "cleanup failed"},
            None,
            "invalid",
        ),
    ],
)
def test_classify(outcome, result, elapsed, expected):
    assert classify(outcome, {"population_started": True, **result}, elapsed)[0] == expected


@pytest.mark.parametrize(
    "criterion",
    [
        "oom",
        "restart",
        "truncation",
        "drain_deadline",
        "closeout_deadline",
        "missing_identity",
        "checker_mismatch",
    ],
)
def test_classify_registered_capacity_failure(criterion):
    result = {
        "status": "FAIL",
        "population_started": True,
        "capacity_criterion": criterion,
        "error": "registered criterion failed",
    }
    assert classify("failure", result, None) == ("capacity_failure", result["error"])


@pytest.mark.parametrize(
    ("has_freeze", "artifact", "outcome", "expected"),
    [
        (True, "123", "failure", "capacity_failure"),
        (False, "123", "skipped", "invalid"),
        (True, "", "failure", "invalid"),
    ],
)
def test_closeout_exports_classified_verdict(
    tmp_path, monkeypatch, has_freeze, artifact, outcome, expected
):
    if has_freeze:
        write(
            tmp_path / "freeze.json",
            {
                "qualification": {
                    "status": "FAIL",
                    "population_started": True,
                    "capacity_criterion": "drain_deadline",
                    "error": "registered deadline exceeded",
                }
            },
        )
    output = tmp_path / "output"
    monkeypatch.setenv("ARTIFACT_ID", artifact)
    monkeypatch.setenv("ARTIFACT_DIGEST", "a" * 64)
    monkeypatch.setenv("QUALIFICATION", outcome)
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "summary"))
    monkeypatch.setenv("J2_VOLUME", "26678")
    closeout(tmp_path)
    assert output.read_text() == f"verdict={expected}\n"


def test_freeze_indexes_the_original_trace_bytes(tmp_path):
    (tmp_path / "live-checker.json").write_bytes(
        b'{"status":"PASS","workflow_ids":["id"],"trace":{"Test":{"raw":1.0,"first":true}}}\n'
    )
    extract(tmp_path)
    snapshot = tmp_path / "snapshot.json"
    assert snapshot.read_bytes() == b'{"Test":{"raw":1.0,"first":true}}\n'
    seal(tmp_path, {"status": "PASS"})
    assert read(tmp_path / "freeze.json")["files"]["snapshot.json"] == file_record(snapshot)


def test_verdict_comparison_preserves_the_result_and_witness_decisions():
    live = {
        "status": "PASS",
        "workflow_ids": ["one"],
        "trace": {},
        "campaign": {
            "busy_seconds": 1.23,
            "reexecuted_seconds": 0,
            "valid": True,
            "accepted_results": 1,
        },
    }
    offline = {**live, "workflow_ids": None, "campaign": {**live["campaign"], "busy_seconds": 1.24}}
    assert verdict(live) == verdict(offline)
    offline["campaign"]["accepted_results"] = 0
    assert verdict(live) != verdict(offline)


def test_scrape_gap_uses_fresh_queries_and_required_series():
    def metric(values):
        return [{"values": [[t, str(v)] for t, v in enumerate(values)]}]

    series = {
        "up": metric([1] * 10),
        "j2_query_ok": metric([1] * 10),
        "j2_sample_timestamp_seconds": metric([0] * 8 + [8, 9]),
        "j2_admissions": metric([26678] * 10),
        "j2_terminals": metric([26678] * 10),
        "j2_outstanding": metric([0] * 10),
        "j2_unpublished": metric([0] * 10),
    }
    summary = scrape_summary(series, 0, 10, 26678)
    assert summary["max_scrape_gap_seconds"] == 1
    assert summary["max_query_gap_seconds"] == 8
    assert summary["query_gaps_over_5s"] == 1
    series["j2_terminals"] = metric([21002] * 10)
    with pytest.raises(ValueError, match="full quiet population"):
        scrape_summary(series, 0, 10, 26678)
