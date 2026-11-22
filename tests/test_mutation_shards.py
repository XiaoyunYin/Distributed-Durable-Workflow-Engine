from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import mutation_shards as shards  # noqa: E402


@pytest.mark.parametrize("platform", shards.REPORT_NAMES)
def test_all_manifest_cases_are_assigned_even_after_growth(platform: str) -> None:
    cases = shards.read_manifest(shards.MANIFEST)["cases"]
    growth = [
        {"id": "new-unmeasured-case", "command": ["ordinary-test"]},
        {"id": "k6-new-unmeasured-case", "command": ["k6-test"]},
    ]
    for source in (cases, [*cases, *growth]):
        assignment = shards.assign_shards(source, platform)
        assert set(assignment) == {case["id"] for case in source}
        assert set(assignment.values()) == set(shards.SHARDS[platform])
        assert assignment == shards.assign_shards(list(reversed(source)), platform)
        assert all(
            shard == "with-k6" for case_id, shard in assignment.items() if case_id.startswith("k6")
        )


def test_powershell_without_k6_preserves_baseline_groups() -> None:
    cases = shards.read_manifest(shards.MANIFEST)["cases"]
    assignment = shards.assign_shards(cases, "powershell")
    owners = {}
    for case in cases:
        shard = assignment[case["id"]]
        if shard == "with-k6":
            continue
        key = tuple(case["command"]), tuple(sorted(case.get("env", {}).items()))
        assert owners.setdefault(key, shard) == shard


@pytest.mark.parametrize("platform", shards.SHARDS)
def test_unmeasured_k6_case_preserves_measured_primary_shard_membership(platform: str) -> None:
    cases = shards.read_manifest(shards.MANIFEST)["cases"]
    measured = [case for case in cases if case["id"] in shards.read_costs(platform)]
    before = shards.assign_shards(measured, platform)
    after = shards.assign_shards(
        [*cases, {"id": "k6-unmeasured-test", "command": ["test"]}], platform
    )
    assert {cid for cid, shard in before.items() if shard == "with-k6"} == {
        cid for cid in before if after[cid] == "with-k6"
    }


@pytest.mark.parametrize(
    ("fault", "message"),
    [
        ("missing-report", "results-bash.json"),
        ("missing-case", "missing 1 cases"),
        ("overlap", "duplicate or unknown case"),
        ("unknown", "duplicate or unknown case"),
        ("wrong-marker", "lacks its expected detection"),
        ("survived", "lacks its expected detection"),
        ("stale-commit", "invalid report header"),
        ("k6-misplaced", "misplaced K6 case"),
        ("incomplete-powershell", "powershell: missing 1 cases"),
        ("missing-third-report", "powershell-without-k6-b-123-2"),
    ],
)
def test_aggregate_rejects_incomplete_or_invalid_reports(
    tmp_path: Path, fault: str, message: str
) -> None:
    manifest = write_reports(tmp_path, fault)
    with pytest.raises((ValueError, FileNotFoundError), match=message):
        shards.verify_reports(manifest, tmp_path, "123-2", "merge-sha")


def test_aggregate_accepts_both_complete_platforms(tmp_path: Path) -> None:
    manifest = write_reports(tmp_path, "")
    shards.verify_reports(manifest, tmp_path, "123-2", "merge-sha")


def write_reports(root: Path, fault: str) -> dict[str, Any]:
    marker = "named failure"
    k6 = ["k6-one", "other-one"]
    ordinary = ["other-two", "other-three", "other-four"]
    groups = {
        "linux": {"with-k6": k6, "without-k6": ordinary},
        "powershell": {"with-k6": k6, "without-k6-a": ordinary[:2], "without-k6-b": ordinary[2:]},
    }
    for platform, filename in shards.REPORT_NAMES.items():
        for shard, ids in groups[platform].items():
            commit = "merge-sha"
            rows = [
                {
                    "id": case_id,
                    "status": "PASS",
                    "baseline_status": "PASS",
                    "exit_code": 1,
                    "expected_failure": marker,
                }
                for case_id in ids
            ]
            if shard == "without-k6" and platform == "linux":
                match fault:
                    case "missing-report":
                        continue
                    case "missing-case":
                        rows.pop()
                    case "overlap":
                        rows[0]["id"] = "other-one"
                    case "unknown":
                        rows[0]["id"] = "unknown"
                    case "wrong-marker":
                        rows[0]["expected_failure"] = "different failure"
                    case "survived":
                        rows[0]["exit_code"] = 0
                    case "stale-commit":
                        commit = "different-sha"
                    case "k6-misplaced":
                        rows[0]["id"] = "k6-one"
            if fault == "k6-misplaced" and shard == "with-k6" and platform == "linux":
                rows[0]["id"] = "other-two"
            if (
                fault == "incomplete-powershell"
                and shard == "without-k6-a"
                and platform == "powershell"
            ):
                rows.pop()
            if fault == "missing-third-report" and shard == "without-k6-b":
                continue
            path = root / f"dur046-mutation-results-{platform}-{shard}-123-2" / filename
            path.parent.mkdir()
            path.write_text(
                json.dumps(
                    {
                        "schema": "dur-046-mutation-results.v1",
                        "status": "PASS",
                        "git_commit": commit,
                        "cases": rows,
                    }
                ),
                encoding="utf-8",
            )
    return {"cases": [{"id": case_id, "expected": marker} for case_id in [*k6, *ordinary]]}
