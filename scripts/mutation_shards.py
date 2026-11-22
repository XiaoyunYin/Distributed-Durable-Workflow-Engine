from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from statistics import median_high
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "mutations/manifest.json"
COSTS = ROOT / "mutations/ci-case-costs.csv"
REPORT_NAMES = {"linux": "results-bash.json", "powershell": "results-powershell.json"}
SHARD_NAMES = ("with-k6", "without-k6-a", "without-k6-b")
SHARDS = dict.fromkeys(REPORT_NAMES, SHARD_NAMES)


def read_manifest(path: Path) -> dict[str, Any]:
    manifest: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    ids = [case["id"] for case in manifest["cases"]]
    if not ids or len(ids) != len(set(ids)):
        raise ValueError("mutation manifest must have unique case IDs")
    return manifest


def read_costs(platform: str) -> dict[str, int]:
    column = f"{platform}_ms"
    with COSTS.open(encoding="utf-8", newline="") as source:
        return {row["id"]: int(row[column]) for row in csv.DictReader(source) if row[column]}


def case_weights(ids: list[str], costs: dict[str, int]) -> dict[str, int]:
    known_costs = [costs[case_id] for case_id in ids if case_id in costs]
    default = median_high(known_costs) if known_costs else 1000
    return {case_id: costs.get(case_id, default) for case_id in ids}


def assign_shards(cases: list[dict[str, Any]], platform: str) -> dict[str, str]:
    """Pin K6 and balance by descending cost, preserving measured placements on growth."""
    ids: list[str] = [case["id"] for case in cases]
    costs = read_costs(platform)
    weights = case_weights(ids, costs)
    with_k6, without_k6 = "with-k6", "without-k6"
    assigned = {}
    loads = {with_k6: 0, without_k6: 0}
    ordered = sorted(ids, key=lambda i: (i not in costs, not i.startswith("k6"), -weights[i], i))
    for case_id in ordered:
        shard = with_k6 if case_id.startswith("k6") else min(loads, key=loads.__getitem__)
        assigned[case_id] = shard
        loads[shard] += weights[case_id]
    split_ordinary(
        [case for case in cases if assigned[case["id"]] == without_k6],
        assigned,
        read_costs(f"{platform}_split"),
    )
    return assigned


def split_ordinary(
    cases: list[dict[str, Any]], assigned: dict[str, str], costs: dict[str, int]
) -> None:
    # The runner caches by command and environment on the same platform.
    groups: dict[tuple[tuple[str, ...], tuple[tuple[str, str], ...]], list[str]] = defaultdict(list)
    for case in cases:
        command = tuple(map(str, case["command"]))
        env = tuple(sorted((str(key), str(value)) for key, value in case.get("env", {}).items()))
        groups[command, env].append(case["id"])

    weights = case_weights([case["id"] for case in cases], costs)
    ordered = []
    for group in groups.values():
        measured = [case_id for case_id in group if case_id in costs]
        placement_ids = measured or group
        ordered.append(
            (
                not measured,
                -sum(weights[case_id] for case_id in placement_ids),
                min(placement_ids),
                group,
            )
        )
    loads = dict.fromkeys(SHARD_NAMES[1:], 0)
    for _, negative_weight, _, group in sorted(ordered):
        shard = min(loads, key=loads.__getitem__)
        assigned.update(dict.fromkeys(group, shard))
        loads[shard] -= negative_weight


def verify_reports(manifest: dict[str, Any], reports: Path, suffix: str, commit: str) -> None:
    expected = {case["id"]: case for case in manifest["cases"]}
    for platform, filename in REPORT_NAMES.items():
        seen: set[str] = set()
        for shard in SHARDS[platform]:
            artifact = f"dur046-mutation-results-{platform}-{shard}-{suffix}"
            path = reports / artifact / filename
            report = json.loads(path.read_text(encoding="utf-8"))
            if (
                report.get("schema") != "dur-046-mutation-results.v1"
                or report.get("status") != "PASS"
                or report.get("git_commit") != commit
                or not report.get("cases")
            ):
                raise ValueError(f"{artifact}: missing cases or invalid report header")
            for result in report["cases"]:
                case_id = result["id"]
                if case_id in seen or case_id not in expected:
                    raise ValueError(f"{artifact}: duplicate or unknown case {case_id}")
                if case_id.startswith("k6") and shard != "with-k6":
                    raise ValueError(f"{artifact}: misplaced K6 case {case_id}")
                case = expected[case_id]
                code = result.get("exit_code")
                if (
                    result.get("status") != "PASS"
                    or result.get("baseline_status") != "PASS"
                    or type(code) is not int
                    or code == 0
                    or result.get("expected_failure") != case["expected"]
                ):
                    raise ValueError(f"{artifact}: case {case_id} lacks its expected detection")
                seen.add(case_id)
            print(f"{artifact}: {len(report['cases'])} cases killed")
        missing = expected.keys() - seen
        if missing:
            raise ValueError(
                f"{platform}: missing {len(missing)} cases: {', '.join(sorted(missing))}"
            )
        print(f"{platform}: all {len(seen)} mutation cases killed exactly once")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Select CI mutation cases and verify shard reports"
    )
    parser.add_argument("--manifest", type=Path, default=MANIFEST)
    commands = parser.add_subparsers(dest="command", required=True)
    select = commands.add_parser("select")
    select.add_argument("platform", choices=REPORT_NAMES)
    select.add_argument("shard")
    select.add_argument("output", type=Path)
    verify = commands.add_parser("verify")
    verify.add_argument("reports", type=Path)
    verify.add_argument("artifact_suffix")
    verify.add_argument("commit")
    args = parser.parse_args()

    try:
        manifest = read_manifest(args.manifest)
        if args.command == "verify":
            verify_reports(manifest, args.reports, args.artifact_suffix, args.commit)
        else:
            if args.shard not in SHARDS[args.platform]:
                raise ValueError(f"unknown shard {args.platform}/{args.shard}")
            assigned = assign_shards(manifest["cases"], args.platform)
            manifest["cases"] = [
                case for case in manifest["cases"] if assigned[case["id"]] == args.shard
            ]
            args.output.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
            print(f"{args.platform}/{args.shard}: selected {len(manifest['cases'])} cases")
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(1, f"Mutation shard validation failed: {error}\n")


if __name__ == "__main__":
    main()
