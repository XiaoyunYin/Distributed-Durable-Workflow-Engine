"""Render K4 acceptance disclosures from the unscoped verifier JSON only.

This module copies registered decisions and measured differences. It neither
reruns the checker nor reconstructs metrics absent from the verifier output.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ACCEPTANCE = Path("experiments/portfolio/k8s-recovery/k4-v1/acceptance")
NOT_DERIVED = (
    "Per-arm I1-I13 checker coverage/outcomes; witness overhead and cross-clock "
    "uncertainty populations are not exported in the verified metric records.",
)


def json_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()


def compact(value: object) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False)


def table(lines: list[str], headers: list[str], rows: list[list[object]]) -> None:
    def cell(value: object) -> str:
        text = value if isinstance(value, str) else compact(value)
        return text.replace("|", "\\|").replace("\n", " ")

    lines.extend(
        ["", "| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    )
    lines.extend("| " + " | ".join(cell(v) for v in row) + " |" for row in rows)
    lines.append("")


def populations(value: object, path: str = "") -> list[list[object]]:
    """Expose already-published population fields without calculating statistics."""
    if not isinstance(value, dict):
        return []
    if {"n", "missing", "censored", "p50", "p90", "max", "p99"} <= value.keys():
        return [
            [path, *[value[k] for k in ("n", "missing", "censored", "p50", "p90", "max", "p99")]]
        ]
    return [
        row
        for key, child in sorted(value.items())
        for row in populations(child, f"{path}.{key}" if path else key)
    ]


def display_number(value: object) -> object:
    return round(value, 3) if isinstance(value, float) else value


def headline_status(metric: dict) -> str:
    status = metric["status"]
    if not metric["valid"]:
        return f"{status} (INVALID)"
    if status == "FAIL":
        return "FAIL"
    return status


def headline_latency(metric: dict, field: str) -> str:
    latency = metric["latency"]
    value = display_number(latency[field])
    return f"{value} (n={latency['n']})"


def headline_ab(summary: dict) -> list[str]:
    lines = ["## Headline A/B by cell", ""]
    metrics = {metric["episode"]: metric for metric in summary["episode_metrics"]}
    headers = [
        "Pair",
        "A episode status",
        "B episode status",
        "Latency p50 A / B",
        "Latency p90 A / B",
        "Latency max A / B",
        "Abandoned count A / B",
        "Reexecuted seconds.lower A / B",
        "Wasted original seconds.lower A / B",
        "SLO fraction A / B",
    ]

    def episode_status(episode: str) -> str:
        metric = metrics[episode]
        return headline_status(metric)

    def pair_row(pair: dict) -> list[object]:
        a_episode, b_episode = pair["episodes"]
        a, b = metrics[a_episode], metrics[b_episode]
        return [
            pair["pair"],
            episode_status(a_episode),
            episode_status(b_episode),
            f"{headline_latency(a, 'p50')} / {headline_latency(b, 'p50')}",
            f"{headline_latency(a, 'p90')} / {headline_latency(b, 'p90')}",
            f"{headline_latency(a, 'max')} / {headline_latency(b, 'max')}",
            f"{a['abandoned']['count']} / {b['abandoned']['count']}",
            f"{display_number(a['reexecuted_seconds']['lower'])} / "
            f"{display_number(b['reexecuted_seconds']['lower'])}",
            f"{display_number(a['wasted_original_seconds']['lower'])} / "
            f"{display_number(b['wasted_original_seconds']['lower'])}",
            f"{display_number(a['slo']['fraction'])} / {display_number(b['slo']['fraction'])}",
        ]

    for cell in summary["cells"]:
        lines.extend(["", f"### {cell['duration']}: {cell['event']}"])
        table(lines, headers, [pair_row(pair) for pair in cell["pairs"]])

    coverage_rows = []
    for cell in summary["cell_metrics"]:
        if cell["kind"] != "coverage":
            continue
        for index in cell["episode_metric_indices"]:
            metric = summary["episode_metrics"][index]
            coverage_rows.append(
                [
                    f"{cell['duration']}: {cell['event']}",
                    metric["episode"],
                    headline_status(metric),
                    headline_latency(metric, "p50"),
                    headline_latency(metric, "p90"),
                    headline_latency(metric, "max"),
                    metric["abandoned"]["count"],
                    display_number(metric["reexecuted_seconds"]["lower"]),
                    display_number(metric["wasted_original_seconds"]["lower"]),
                    display_number(metric["slo"]["fraction"]),
                ]
            )
    if coverage_rows:
        lines.extend(["", "### Coverage cells (B only)"])
        table(
            lines,
            [
                "Coverage cell",
                "Episode",
                "B episode status",
                "Latency p50 B",
                "Latency p90 B",
                "Latency max B",
                "Abandoned count B",
                "Reexecuted seconds.lower B",
                "Wasted original seconds.lower B",
                "SLO fraction B",
            ],
            coverage_rows,
        )
    return lines


def decision(summary: dict, digest: str) -> dict:
    arms = {}
    classifications = summary["k7_removal_classifications"]
    metrics = {m["episode"]: m for m in summary["episode_metrics"]}
    for duration in ("short", "long"):
        gate = summary["k7"][duration]
        rows = []
        for pair, excess in enumerate(gate["excess"]):
            episode = f"{duration}-core-1-pair-{pair}-treatment"
            removal = classifications[episode]
            metric = metrics[episode]
            rows.append(
                dict(
                    episode=episode,
                    busy_uids=removal["busy_uids"],
                    idle_survivors=removal["idle_survivors"],
                    ambiguous=removal["ambiguous"],
                    wasted_unknown=metric["wasted_original_seconds"]["unknown"],
                    reexecuted_unknown=metric["reexecuted_seconds"]["unknown"],
                    wasted_lower=metric["wasted_original_seconds"]["lower"],
                    reexecuted_lower=metric["reexecuted_seconds"]["lower"],
                    busy_upper=removal["busy_upper"],
                    excess=excess,
                )
            )
        arms[duration] = dict(gate, treatment_rows=rows)
    return dict(
        schema="dur053.k4-acceptance.k7.v1",
        source="k4-summary.json",
        source_sha256=digest,
        arms=arms,
        campaign_safety_input=dict(
            registered_input_by_arm={d: g["safety"] for d, g in summary["k7"].items()},
            aggregate_passed=summary["safety"]["passed"],
            missing_episodes=summary["missing_episodes"],
            invalid_incomplete_contributing_episodes=[
                r for r in summary["episode_results"] if not r["valid"] or not r["complete"]
            ],
            invalid_pairs=[p for p in summary["paired"] if p.get("valid") is False],
            complete=summary["complete"],
            valid=summary["valid"],
        ),
        registered_campaign_decision=summary["k7_decision"],
        disclosure_if_safety_true=dict(
            label="Non-registered K4 closeout disclosure; not the registered decision",
            supports_controller_claim=False,
            long_arm_would_be="GO",
            short_arm_would_be="NO-GO",
            scope=(
                "Only the campaign safety input is hypothetically true. The published "
                "long arm has five eligible episodes, complete pairs and positive median "
                "excess; the short arm has zero eligible episodes and negative median. "
                "This disclosure does not evaluate a new gate or authorize implementation."
            ),
        ),
        consequence=(
            "K7 NO-GO means no controller implementation or controller efficacy claim."
            if summary["k7_decision"] == "NO-GO"
            else "Registered GO; controller work remains subject to its protocol gates."
        ),
        no_go_budget_release_arithmetic_only=dict(
            applicable=summary["k7_decision"] == "NO-GO",
            episode_seconds=11100,
            reset_seconds=1200,
            setup_seconds=300,
            total_seconds=11100 + 1200 + 300,
            scope="Protocol-v2 Amendment B arithmetic only; no ledger/schedule change or new cells",
        ),
        not_derived=[],
    )


def render_results(summary: dict, frozen: dict, digest: str) -> str:
    lines = [
        "# K4 acceptance results - Round 340",
        "",
        f"Source: `k4-summary.json`, SHA-256 `{digest}`. Generated by "
        "`scripts/dur053_k4_report.py render`; `check` compares exact bytes.",
        "",
        f"Verifier: complete={compact(summary['complete'])}, valid={compact(summary['valid'])}; "
        f"missing_episodes={compact(summary['missing_episodes'])}; "
        f"disposition_counts={compact(summary['disposition_counts'])}.",
        "",
        "## Safety",
        "",
        "Campaign counters include all retained safety evidence, including failed and replaced "
        "recordings. They do not establish per-arm I1-I13 coverage.",
        "",
        "```json",
        json_bytes({k: v for k, v in summary["safety"].items() if k != "episodes"})
        .decode()
        .rstrip(),
        "```",
        *headline_ab(summary),
        "## Registered cells and A/B metrics",
        "",
        "A=baseline legacy flags; B=treatment lifecycle defaults. Core cells register five "
        "matched pairs per event and duration; coverage cells register three B-only short "
        "episodes. Bindings, metric denominators and limitations are published in each row. "
        "Episode counts are recording populations, not metric denominators.",
        "",
        "All values below are copied from verified episode_metrics. Large raw lists were "
        "summarised by the verifier using its nearest-rank population helper. n, missing and "
        "censored are explicit; p99 is NOT ESTIMATED below n=200. Censored assignment timings "
        "are excluded from timing populations; lower bounds remain separate. Bounded identity "
        "lists disclose count and truncation. No episodes or episode percentiles are pooled. "
        "Null and NOT ESTIMATED remain unavailable, never zero.",
    ]
    results = {r["episode"]: r for r in summary["episode_results"]}
    for cell in summary["cell_metrics"]:
        label = " coverage" if cell["kind"] == "coverage" else ""
        lines.extend(
            [
                "",
                f"### {cell['duration']}{label}: {cell['event']} / {cell['arm']}",
                "",
                f"Registered population={cell['registered_population']}; "
                f"missing={compact(cell['missing'])}; failed={compact(cell['failed'])}.",
            ]
        )
        for index in cell["episode_metric_indices"]:
            metric = summary["episode_metrics"][index]
            lines.extend(["", f"#### {metric['episode']}"])
            table(
                lines,
                ["Verified result / provenance", "Value"],
                [[key, value] for key, value in sorted(results[metric["episode"]].items())],
            )
            table(
                lines,
                ["Metric / population (seconds unless labelled)", "Verified value"],
                [[key, value] for key, value in sorted(metric.items())],
            )
            table(
                lines,
                ["Published population", "n", "Missing", "Censored", "p50", "p90", "Max", "p99"],
                populations(metric),
            )
    lines.extend(
        [
            "",
            "## Paired B minus A differences",
            "",
            "These are the existing verifier differences and lower/upper cost bounds. "
            "They are not pooled populations or new statistics.",
        ]
    )
    for cell in summary["cells"]:
        lines.extend(["", f"### Paired {cell['duration']}: {cell['event']}"])
        for pair in cell["pairs"]:
            lines.extend(["", f"Pair {pair['pair']}: {compact(pair['episodes'])}"])
            table(
                lines,
                ["Field", "Copied paired value"],
                [[key, value] for key, value in sorted(pair.items())],
            )
            table(
                lines,
                ["Pair", "Cost (seconds)", "B minus A lower", "B minus A upper"],
                [
                    [pair["pair"], key, bounds["lower"], bounds["upper"]]
                    for key, bounds in sorted(pair.get("cost_difference", {}).items())
                ],
            )
    lines.extend(
        [
            "",
            "## Frozen K7 decision",
            "",
            frozen["consequence"],
            "",
            "Registered decisions and excess values are copied from the verifier. "
            "Cost bounds come from verified episode_metrics; busy/idle UIDs come from "
            "the R321 classifications. Removal ambiguity and unknown cost counts are "
            "copied separately in k7-decision.json. No controller claim is made.",
        ]
    )
    table(
        lines,
        ["Arm", "Eligible", "Median excess (s)", "Complete pairs", "Safety", "Decision"],
        [
            [
                duration,
                *[
                    gate[k]
                    for k in ("eligible", "median_excess", "complete_pairs", "safety", "decision")
                ],
            ]
            for duration, gate in frozen["arms"].items()
        ],
    )
    for duration, gate in frozen["arms"].items():
        lines.extend(["", f"### K7 {duration}"])
        table(
            lines,
            [
                "Episode",
                "Busy UIDs",
                "Idle survivors",
                "Ambiguous",
                "Wasted lower (s)",
                "Reexecuted lower (s)",
                "Busy upper (s)",
                "Excess (s)",
            ],
            [
                [
                    row[k]
                    for k in (
                        "episode",
                        "busy_uids",
                        "idle_survivors",
                        "ambiguous",
                        "wasted_lower",
                        "reexecuted_lower",
                        "busy_upper",
                        "excess",
                    )
                ]
                for row in gate["treatment_rows"]
            ],
        )
    lines.extend(
        [
            "",
            "Campaign safety input (including explicitly named invalid/incomplete contributors):",
            "",
            "```json",
            json_bytes(frozen["campaign_safety_input"]).decode().rstrip(),
            "```",
            "",
            "### Non-registered disclosure_if_safety_true",
            "",
            "The long arm would be GO and the short arm NO-GO if only campaign safety "
            "were true. This is **not the registered decision**, supports no controller "
            "claim, and does not authorize controller implementation.",
            "",
            "```json",
            json_bytes(frozen["disclosure_if_safety_true"]).decode().rstrip(),
            "```",
            "",
            "NO-GO budget release: 11,100 + 1,200 + 300 = **12,600 seconds**. "
            "Protocol arithmetic only; no ledger or schedule adjustment or new cells.",
            "",
            "## Retained failures, replacements and reclassifications",
            "",
            "Job dispositions preserve producer verdicts; episode_results above give "
            "the verified current verdicts and issues. Historical intake bytes remain "
            "unchanged. B.7 R319 corrects generation-end eligibility; R320 corrects idle "
            "scale-down confirmation. Round 326 retains k4-4-c1 "
            "long-core-6-pair-2-treatment FAIL for k4-assignment-disposition-censored.",
        ]
    )
    table(
        lines,
        ["Disclosure", "Verified episode result (including replacement link)"],
        [
            [
                "R319"
                if r["episode"] == "long-core-0-pair-0-treatment"
                else "R320"
                if r["episode"] in ("long-core-1-pair-2-baseline", "long-core-1-pair-4-baseline")
                else "Linked replacement"
                if r["disposition"].get("replacement_for")
                else "Retained failure",
                r,
            ]
            for r in summary["episode_results"]
            if not r["valid"]
            or not r["complete"]
            or r["disposition"].get("replacement_for")
            or r["episode"]
            in (
                "long-core-0-pair-0-treatment",
                "long-core-1-pair-2-baseline",
                "long-core-1-pair-4-baseline",
            )
        ],
    )
    for job in summary["retained_jobs"]:
        lines.extend(
            [
                "",
                f"### {job['job']} / {job['run_id']}",
                "",
                "```json",
                json_bytes(job).decode().rstrip(),
                "```",
            ]
        )
    lines.extend(
        [
            "",
            "Failed/not-derivable events:",
            "",
            "```json",
            json_bytes(summary["failed_events"]).decode().rstrip(),
            "```",
            "",
            "## Busy pods removed by event (R321)",
            "",
            "busy_pods_removed replaces the defective retained v1 busy_removals. "
            "Each row copies the complete versioned classification, including target "
            "UIDs, deletion receipt, preceding snapshot, age, issues and interrupted claims. "
            "For container SIGKILL without pod deletion, missing-deletion-event means "
            "the deletion anchor is **NOT APPLICABLE**; the published ambiguous=true "
            "and null anchor remain unchanged. No deletion or busy-at-deletion value is "
            "invented. Other per-target issues, including non-worker targets in eviction "
            "drills, remain visible rather than being relabelled as worker removals.",
        ]
    )
    for cell in summary["cell_metrics"]:
        lines.extend(["", f"### Removal {cell['event']} / {cell['duration']} / {cell['arm']}"])
        table(
            lines,
            ["Episode", "busy_pods_removed (verbatim R321)"],
            [
                [
                    summary["episode_metrics"][i]["episode"],
                    summary["episode_metrics"][i]["busy_pods_removed"],
                ]
                for i in cell["episode_metric_indices"]
            ],
        )
    lines.extend(
        [
            "",
            "## Invariant coverage by arm",
            "",
            "The registered K4 lifecycle checker (dur053.k4-checker.v1) evaluates stale-commit, "
            "superseded-owner-transition, duplicate-execution and claimed-unexecuted properties "
            "per episode (see Safety and the episode metrics). It does not emit an I1-I13 mapping, "
            "and no mapping is assigned after observation, so no I-label coverage or pass is "
            "claimed.",
        ]
    )
    table(
        lines,
        ["Invariant", "A baseline", "B treatment"],
        [
            [
                f"I{i}",
                "NOT COVERED (no I-label mapping)",
                "NOT COVERED (no I-label mapping)",
            ]
            for i in range(1, 14)
        ],
    )
    lines.extend(
        [
            "",
            "PURE I8-I10 effects/approval, cancellation races and storage loss "
            "are NOT COVERED by this lifecycle submission.",
            "",
            "## Budget and schedule",
            "",
            "```json",
            json_bytes(summary["schedule"]).decode().rstrip(),
            "```",
            "",
            "Schedule charged_seconds is the full cumulative ledger total. "
            "Each retained K4 job's charged_seconds is copied above. "
            "Planned NO-GO savings are not applied to recorded totals.",
            "",
            "## Not derived",
            "",
            "These fields are absent from the verifier output. The renderer only "
            "formats the summary and never reads intake or raw assets.",
            "",
        ]
    )
    stale_rejected_zeros = sum(
        metric["stale_rejected"] == 0 for metric in summary["episode_metrics"]
    )
    lines.extend("- " + item for item in NOT_DERIVED)
    lines.append(
        f"- Typed stale-rejection codes are not exported; stale_rejected is a total count, "
        f"and it is 0 in all {stale_rejected_zeros} episodes, so there are no rejections to type."
    )
    lines.extend(
        [
            "",
            "Scale-up delay is NOT APPLICABLE (K4 fixed replicas); "
            "limiting component remains unresolved.",
            "",
            "## Limitations",
            "",
            "Single-host kind on GitHub-hosted runners; process/container lifecycle "
            "faults and voluntary PDB-governed eviction without cordon. This does not "
            "establish independent-node rescheduling, node failure, storage loss or "
            "dependency HA. Same-source image lineage is not a cross-version upgrade. "
            "Chaos Mesh instrumentation differs from K3 calibration; b/r/L remain "
            "frozen. Per-episode limitations are copied above. Failed and censored "
            "observations remain visible; aggregate safety does not make an incomplete "
            "campaign valid. K7 campaign safety composition is unchanged.",
            "",
            "## Verification provenance",
            "",
            "Claude ran the full unscoped verification on AWS in Round 339, at "
            "745faff63bc3ac2ffb9c4553478327dc5e241151, merged as "
            "d3853ec4687033611004ba3815a5098ae4fb04df with an identical tree. "
            "Host: EC2 r7i.xlarge, us-west-1, Ubuntu 24.04 "
            "ami-054378e21be05e358, Go 1.27.4, Python 3.12.3. "
            "Exit 0; wall 3,826 seconds; max RSS 394,212 KB. "
            "The host ran verification; lifecycle measurements were collected on "
            "GitHub-hosted single-host kind runners. PR B did not rerun verification.",
            "",
            "Command: `/usr/bin/time -v python3 scripts/dur053_k4_evidence.py "
            "verify-evidence --checker /home/ubuntu/checker --repository /home/ubuntu/repo "
            "--store /home/ubuntu/store`.",
            "",
            "[provenance.json](provenance.json) records the ten release tags and asset "
            "counts, output size and SHA-256. Per-episode job/run provenance is copied "
            "above. This renderer reads only k4-summary.json.",
            "",
        ]
    )
    return "\n".join(lines)


def rendered(source: bytes) -> dict[str, bytes]:
    summary = json.loads(source)
    if summary["schema"] != "dur053.k4-summary.v1" or "verification_scope" in summary:
        raise ValueError("acceptance requires the unscoped K4 summary")
    if any("busy_removals" in metric for metric in summary["episode_metrics"]):
        raise ValueError("acceptance requires busy_pods_removed, not retained v1 busy_removals")
    digest = hashlib.sha256(source).hexdigest()
    frozen = decision(summary, digest)
    return {
        "k7-decision.json": json_bytes(frozen),
        "results.md": render_results(summary, frozen, digest).encode("utf-8"),
    }


def run(command: str, directory: Path) -> int:
    outputs = rendered((directory / "k4-summary.json").read_bytes())
    mismatches = []
    for name, expected in outputs.items():
        path = directory / name
        if command == "render":
            path.write_bytes(expected)
        elif not path.exists() or path.read_bytes() != expected:
            mismatches.append(str(path))
    if mismatches:
        print("K4 report mismatch: " + ", ".join(mismatches))
        return 1
    print(f"K4 report {command}: PASS")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("render", "check"))
    parser.add_argument("--acceptance", type=Path, default=ROOT / ACCEPTANCE)
    args = parser.parse_args()
    return run(args.command, args.acceptance)


if __name__ == "__main__":
    raise SystemExit(main())
