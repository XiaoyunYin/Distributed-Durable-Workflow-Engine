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
            shard in shards.WITH_K6_SHARDS
            for case_id, shard in assignment.items()
            if case_id.startswith("k6")
        )


@pytest.mark.parametrize("platform", shards.SHARDS)
def test_shards_preserve_baseline_groups_and_measured_placement(platform: str) -> None:
    cases = shards.read_manifest(shards.MANIFEST)["cases"]
    assignment = shards.assign_shards(cases, platform)
    owners: dict[tuple[tuple[str, ...], tuple[tuple[str, str], ...]], str] = {}
    existing_cases: dict[tuple[str, str], dict[str, Any]] = {}
    for case in cases:
        shard = assignment[case["id"]]
        family = shards.WITH_K6_SHARDS if shard in shards.WITH_K6_SHARDS else shards.ORDINARY_SHARDS
        key = (family[0], *case["command"]), tuple(sorted(case.get("env", {}).items()))
        assert owners.setdefault(key, shard) == shard
        existing_cases.setdefault(family, case)
    growth = [
        {**existing_cases[shards.ORDINARY_SHARDS], "id": "aaa-new-case-in-measured-group"},
        {**existing_cases[shards.WITH_K6_SHARDS], "id": "k6-new-case-in-measured-group"},
        {"id": "aaa-new-group", "command": ["new selector"], "env": {"FLAG": "1"}},
        {"id": "k6-new-case", "command": ["new k6 selector"]},
    ]
    grown = shards.assign_shards([*growth, *cases], platform)
    measured = set(shards.read_costs(f"{platform}_split")) | set(
        shards.read_costs(f"{platform}_with_k6")
    )
    assert all(grown[case_id] == assignment[case_id] for case_id in measured & assignment.keys())
    for case, family in zip(
        growth[:2], (shards.ORDINARY_SHARDS, shards.WITH_K6_SHARDS), strict=True
    ):
        assert grown[case["id"]] == assignment[existing_cases[family]["id"]]


@pytest.mark.parametrize("platform", shards.SHARDS)
def test_unmeasured_k6_case_preserves_measured_primary_shard_membership(platform: str) -> None:
    cases = shards.read_manifest(shards.MANIFEST)["cases"]
    measured = [case for case in cases if case["id"] in shards.read_costs(platform)]
    before = shards.assign_shards(measured, platform)
    after = shards.assign_shards(
        [*cases, {"id": "k6-unmeasured-test", "command": ["test"]}], platform
    )
    assert {cid for cid, shard in before.items() if shard in shards.WITH_K6_SHARDS} == {
        cid for cid in before if after[cid] in shards.WITH_K6_SHARDS
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
        ("missing-linux-b", "linux-without-k6-b-123-2"),
        ("missing-powershell-b", "powershell-without-k6-b-123-2"),
        ("missing-with-k6-b", "linux-with-k6-b-123-2"),
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
    groups = dict(
        zip(shards.SHARD_NAMES, (k6[:1], k6[1:], ordinary[:2], ordinary[2:]), strict=True)
    )
    for platform, filename in shards.REPORT_NAMES.items():
        for shard, ids in groups.items():
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
            if shard == "without-k6-a" and platform == "linux":
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
            if fault == "k6-misplaced" and shard == "with-k6-a" and platform == "linux":
                rows[0]["id"] = "other-two"
            if (
                fault == "incomplete-powershell"
                and shard == "without-k6-a"
                and platform == "powershell"
            ):
                rows.pop()
            if fault == f"missing-{platform}-b" and shard == "without-k6-b":
                continue
            if fault == "missing-with-k6-b" and shard == "with-k6-b" and platform == "linux":
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
