"""Acceptance rendering preserves registered results and refuses stale artifacts."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dur053_k4_report as report


def markdown_cells(line):
    cells = []
    start = 0
    slashes = 0
    for index, char in enumerate(line):
        if char == "|" and slashes % 2 == 0:
            cells.append(line[start:index].strip())
            start = index + 1
        if char == "\\":
            slashes += 1
        else:
            slashes = 0
    cells.append(line[start:].strip())
    if cells and cells[0] == "":
        cells.pop(0)
    if cells and cells[-1] == "":
        cells.pop()
    return cells


def is_table_delimiter(line):
    cells = markdown_cells(line)
    return bool(cells) and all("-" in cell and set(cell) <= {"-", ":"} for cell in cells)


def test_rendered_markdown_tables_have_consistent_column_counts(summary):
    text = report.rendered(report.json_bytes(summary))["results.md"].decode()
    lines = text.splitlines()
    for index, line in enumerate(lines[:-1]):
        if not is_table_delimiter(lines[index + 1]):
            continue
        expected = len(markdown_cells(lines[index + 1]))
        assert len(markdown_cells(line)) == expected
        row_index = index + 2
        while row_index < len(lines) and lines[row_index].startswith("|"):
            assert len(markdown_cells(lines[row_index])) == expected, lines[row_index]
            row_index += 1


@pytest.fixture
def summary():
    pair = dict(
        event="worker rollout",
        duration="short",
        pair=0,
        episodes=["short-core-1-pair-0-baseline", "short-core-1-pair-0-treatment"],
        replaced=False,
        valid=False,
        latency_difference=dict(p50=-2.5, p90=3.0, max=4.0, p99="NOT ESTIMATED"),
        abandoned=-1,
        scalar_difference=dict(throughput=0.25, duplicate_executions=-1),
        cost_difference=dict(reexecuted_seconds=dict(lower=-9.0, upper=12.0)),
    )
    classifications = {}
    gates = {}
    for duration in ("short", "long"):
        for i in range(5):
            classifications[f"{duration}-core-1-pair-{i}-treatment"] = dict(
                schema="dur053.k7-removal.r321.v1",
                busy_uids=[f"busy-{i}"],
                idle_survivors=["idle"] if duration == "long" else [],
                ambiguous=False,
                busy_upper=100.0,
                targets=[
                    dict(pod_uid=f"busy-{i}", removal=None, issues=["missing-deletion-event"])
                ],
                interrupted_claims=1,
            )
        gates[duration] = dict(
            decision="NO-GO",
            excess=[-5.0] * 5 if duration == "short" else [20.0] * 5,
            median_excess=-5.0 if duration == "short" else 20.0,
            eligible=0 if duration == "short" else 5,
            complete_pairs=True,
            safety=False,
        )
    metrics = [
        dict(
            episode=episode,
            abandoned=dict(count=2),
            wasted_original_seconds=dict(
                lower=7.0,
                upper=None if episode.startswith("short") else 7.0,
                unknown=1 if episode.startswith("short") else 0,
            ),
            reexecuted_seconds=dict(lower=23.0, upper=23.0, unknown=0),
            status="PASS",
            valid=True,
            complete=True,
            stale_rejected=0,
            slo=dict(fraction=0.875),
            issues=[],
            busy_pods_removed=removal,
            latency=dict(
                n=199, missing=2, censored=1, p50=1.0, p90=2.0, max=3.0, p99="NOT ESTIMATED"
            ),
        )
        for episode, removal in classifications.items()
    ]
    metrics[0].update(status="FAIL", valid=False, complete=False, issues=["named-issue"])
    results = [dict(m, job="synthetic", run_id="1", disposition={}) for m in metrics]
    baseline = dict(
        metrics[0],
        episode="short-core-1-pair-0-baseline",
        status="PASS",
        valid=True,
        complete=True,
        issues=[],
    )
    metrics.append(baseline)
    results.append(dict(baseline, job="synthetic", run_id="1", disposition={}))
    coverage_indices = []
    for index in range(3):
        coverage = dict(metrics[1], episode=f"short-coverage-{index}")
        coverage_indices.append(len(metrics))
        metrics.append(coverage)
        results.append(dict(coverage, job="synthetic", run_id="1", disposition={}))
    results[1]["disposition"] = dict(replacement_for="old", reason="linked replacement")
    return dict(
        schema="dur053.k4-summary.v1",
        safety=dict(
            passed=True,
            lost_accepted=0,
            duplicate_accepted=0,
            stale_committed=0,
            superseded_owner_transitions=0,
            episodes=["a", "b"],
            unavailable=[],
        ),
        complete=False,
        valid=False,
        missing_episodes=[],
        disposition_counts=dict(PASS=1, FAIL=1),
        k7=gates,
        k7_decision="NO-GO",
        k7_removal_classifications=classifications,
        paired=[pair],
        reactions={e: 0.125 for e in pair["episodes"]},
        failed_events=[],
        episode_metrics=metrics,
        episode_results=results,
        cell_metrics=[
            dict(
                event="worker scale-down 4 to 2",
                duration="short",
                arm="treatment",
                kind="core",
                registered_population=5,
                missing=[],
                failed=[metrics[0]["episode"]],
                episode_metric_indices=list(range(5)),
            ),
            dict(
                event="worker rollback",
                duration="short",
                arm="treatment",
                kind="coverage",
                registered_population=3,
                missing=[],
                failed=[],
                episode_metric_indices=coverage_indices,
            ),
        ],
        cells=[dict(event=pair["event"], duration=pair["duration"], pairs=[pair])],
        retained_jobs=[
            dict(
                job="synthetic",
                run_id="1",
                status="FAIL",
                charged_seconds=30,
                pending=[],
                dispositions=[
                    dict(
                        episode="old",
                        classification="infrastructure-invalid",
                        replacement_for="",
                        reason="missing recording",
                        evidence=["trace"],
                    )
                ],
            )
        ],
        schedule=dict(charged_seconds=100, remaining_units=[], expected_k4_remaining_seconds=0),
    )


def test_preserves_decision_bounds_and_unknowns(summary):
    before = report.json_bytes(summary)
    rendered = report.rendered(before)
    frozen = json.loads(rendered["k7-decision.json"])
    assert frozen["registered_campaign_decision"] == "NO-GO"
    assert frozen["arms"]["long"]["eligible"] == 5
    assert frozen["arms"]["long"]["decision"] == "NO-GO"
    assert frozen["arms"]["long"]["safety"] is False
    rows = frozen["arms"]["long"]["treatment_rows"]
    assert len(rows) == 5
    assert rows[0]["episode"] == "long-core-1-pair-0-treatment"
    assert rows[0]["busy_uids"] == ["busy-0"]
    assert rows[0]["wasted_lower"] == 7 and rows[0]["reexecuted_lower"] == 23
    assert rows[0]["ambiguous"] is False
    assert rows[0]["wasted_unknown"] == 0 and rows[0]["reexecuted_unknown"] == 0
    assert frozen["arms"]["short"]["treatment_rows"][0]["wasted_unknown"] == 1
    assert rows[0]["busy_upper"] == 100 and rows[0]["excess"] == 20
    safety = frozen["campaign_safety_input"]
    assert safety["aggregate_passed"] is True
    assert safety["invalid_incomplete_contributing_episodes"] == [summary["episode_results"][0]]
    assert safety["invalid_pairs"] == summary["paired"]
    assert frozen["no_go_budget_release_arithmetic_only"]["total_seconds"] == 12600
    disclosure = frozen["disclosure_if_safety_true"]
    assert disclosure["long_arm_would_be"] == "GO"
    assert disclosure["short_arm_would_be"] == "NO-GO"
    assert disclosure["supports_controller_claim"] is False
    assert "not the registered decision" in disclosure["label"]
    text = rendered["results.md"].decode()
    assert "| 0 | reexecuted_seconds | -9.0 | 12.0 |" in text
    assert "NOT ESTIMATED" in text and "## Not derived" in text
    assert "NOT COVERED (no I-label mapping)" in text and "missing recording" in text
    assert "## Headline A/B by cell" in text
    assert "### short: worker rollout" in text
    assert "0 (n=199)" in text and "2 / 2" in text
    assert "### Coverage cells (B only)" in text and "short: worker rollback" in text
    assert "The registered K4 lifecycle checker (dur053.k4-checker.v1)" in text
    assert "K4 remains IN_REVIEW" not in text
    assert (
        "Typed stale-rejection codes are not exported; stale_rejected is a total count, and it is "
        "0 in all 14 episodes, so there are no rejections to type."
    ) in text
    assert "### short: worker scale-down 4 to 2 / treatment" in text
    assert "named-issue" in text
    assert "linked replacement" in text
    assert "| latency | 199 | 2 | 1 | 1.0 | 2.0 | 3.0 | NOT ESTIMATED |" in text
    assert "busy_pods_removed (verbatim R321)" in text
    assert "missing-deletion-event" in text and "**NOT APPLICABLE**" in text
    assert all(m["episode"] in text for m in summary["episode_metrics"][:5])
    assert all(b"\r" not in value for value in rendered.values())
    assert report.json_bytes(summary) == before
    assert report.rendered(before) == rendered


@pytest.mark.parametrize("name", ["k7-decision.json", "results.md", "k4-summary.json"])
def test_check_detects_changed_source_and_outputs(summary, tmp_path, name):
    source = tmp_path / "k4-summary.json"
    source.write_bytes(report.json_bytes(summary))
    assert report.run("render", tmp_path) == 0
    assert report.run("check", tmp_path) == 0
    path = tmp_path / name
    path.write_bytes(path.read_bytes() + b"\n")
    assert report.run("check", tmp_path) == 1


def test_check_detects_missing_output(summary, tmp_path):
    (tmp_path / "k4-summary.json").write_bytes(report.json_bytes(summary))
    assert report.run("check", tmp_path) == 1


def test_scoped_verification_is_not_acceptance(summary):
    summary["verification_scope"] = [["one-job", "1"]]
    with pytest.raises(ValueError, match="unscoped"):
        report.rendered(report.json_bytes(summary))


def test_retained_v1_removals_are_not_current_metrics(summary):
    summary["episode_metrics"][0]["busy_removals"] = {}
    with pytest.raises(ValueError, match="requires busy_pods_removed"):
        report.rendered(report.json_bytes(summary))


def test_committed_acceptance_passes_check():
    assert report.run("check", report.ROOT / report.ACCEPTANCE) == 0


def test_render_reads_only_summary(summary, tmp_path, monkeypatch):
    source = tmp_path / "k4-summary.json"
    source.write_bytes(report.json_bytes(summary))
    read_bytes = Path.read_bytes
    reads = []

    def read(path):
        reads.append(path)
        assert path == source
        return read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", read)
    assert report.run("render", tmp_path) == 0
    assert reads == [source]
