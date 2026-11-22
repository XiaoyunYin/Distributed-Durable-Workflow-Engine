from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from statistics import median_high
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "mutations/manifest.json"
COSTS = ROOT / "mutations/ci-case-costs.csv"
REPORT_NAMES = {"linux": "results-bash.json", "powershell": "results-powershell.json"}
SHARDS = ("with-k6", "without-k6")


def read_manifest(path: Path) -> dict[str, Any]:
    manifest: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    ids = [case["id"] for case in manifest["cases"]]
    if not ids or len(ids) != len(set(ids)):
        raise ValueError("mutation manifest must have unique case IDs")
    return manifest


def read_costs(platform: str) -> dict[str, int]:
    with COSTS.open(encoding="utf-8", newline="") as source:
        return {row["id"]: int(row[f"{platform}_ms"]) for row in csv.DictReader(source)}


def assign_shards(ids: list[str], costs: dict[str, int]) -> dict[str, str]:
    """Pin K6, then place the largest remaining case on the lighter shard."""
    known_costs = [costs[case_id] for case_id in ids if case_id in costs]
    default = median_high(known_costs) if known_costs else 1000
    weights = {case_id: costs.get(case_id, default) for case_id in ids}
    assigned = {case_id: SHARDS[0] for case_id in ids if case_id.startswith("k6")}
    loads = {SHARDS[0]: sum(weights[case_id] for case_id in assigned), SHARDS[1]: 0}
    remaining = sorted(weights.keys() - assigned.keys(), key=lambda i: (-weights[i], i))
    for case_id in remaining:
        shard = min(SHARDS, key=loads.__getitem__)
        assigned[case_id] = shard
        loads[shard] += weights[case_id]
    return assigned


def verify_reports(manifest: dict[str, Any], reports: Path, suffix: str, commit: str) -> None:
    expected = {case["id"]: case for case in manifest["cases"]}
    for platform, filename in REPORT_NAMES.items():
        seen: set[str] = set()
        for shard in SHARDS:
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
                if case_id.startswith("k6") and shard != SHARDS[0]:
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
    select.add_argument("shard", choices=SHARDS)
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
            ids = [case["id"] for case in manifest["cases"]]
            assigned = assign_shards(ids, read_costs(args.platform))
            manifest["cases"] = [
                case for case in manifest["cases"] if assigned[case["id"]] == args.shard
            ]
            args.output.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
            print(f"{args.platform}/{args.shard}: selected {len(manifest['cases'])} cases")
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(1, f"Mutation shard validation failed: {error}\n")


if __name__ == "__main__":
    main()
