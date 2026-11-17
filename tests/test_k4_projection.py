"""R322 CLI identity and R323 publication regressions."""

import argparse
import copy
import inspect
import json
import runpy
import sys
from contextlib import nullcontext
from pathlib import Path

import pytest
from test_k4_offline import freeze, git_commit, local_repository, record_directory  # noqa: F401
from test_k4_round266 import legacy_download  # noqa: F401

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dur053_k4 as k4
import dur053_k4_evidence as evidence
import dur053_k4_projection as projection


def test_cli_legacy_fixture(tmp_path, legacy_download, monkeypatch, capsys):  # noqa: F811
    source, _ = legacy_download
    repo = local_repository(tmp_path / "repo")
    _, store = record_directory(source, repo, Path("unused"))
    git_commit(repo)
    parse = argparse.ArgumentParser.parse_args

    def fixture_args(parser, *args, **kwargs):
        # Reuse precisely the existing fixture's three trimmed-payload seams
        # even when the old CLI creates its own module globals. Keep its local
        # producer_rules/Derivation and the real legacy_reclassification path.
        namespace = inspect.currentframe().f_back.f_globals
        for name in ("episode", "safety_evidence", "k7_removal_classification"):
            namespace[name] = getattr(evidence, name)
        return parse(parser, *args, **kwargs)

    monkeypatch.setattr(argparse.ArgumentParser, "parse_args", fixture_args)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "dur053_k4_evidence.py",
            "verify-evidence",
            "--checker",
            "unused",
            "--repository",
            str(repo),
            "--store",
            str(store),
        ],
    )
    with pytest.raises(SystemExit) as exited:
        runpy.run_path(str(k4.ROOT / "scripts/dur053_k4_evidence.py"), run_name="__main__")
    assert exited.value.code is None
    result = json.loads(capsys.readouterr().out)
    assert result["safety"]["passed"]
    assert result["schedule"]["charged_seconds"] == 10874
    assert len(result["episode_metrics"]) == len(result["episode_results"]) == 20
    for row in result["episode_metrics"]:
        confirmation = json.loads((source / row["episode"] / "confirmation.json").read_bytes())
        assert (row["busy_pods_removed"] is not None) == bool(confirmation["targets"])
        assert "busy_removals" not in row
    assert set(result["k7_removal_classifications"]) == {
        row["episode"]
        for row in result["episode_metrics"]
        if k4.episode_identity(row["episode"])["event"] == "worker scale-down 4 to 2"
        and row.get("metrics") != "not-derivable"
    }


@pytest.fixture
def metric():
    path = next((k4.ROOT / "experiments/portfolio/k8s-recovery/k4-v1/k4-5").rglob("intake.json"))
    return json.loads(path.read_bytes())["metrics"][0]


def test_closed_projection_preserves_every_field(metric):
    before = copy.deepcopy(metric)
    actual = projection.episode_projection(metric)
    assert actual.keys() == (metric.keys() - {"busy_removals"}) | {"busy_pods_removed"}
    assert "busy_removals" not in projection.COPY_FIELDS
    assert actual["busy_pods_removed"] is None
    for key in projection.COPY_FIELDS & metric.keys():
        assert actual[key] == metric[key]
    assert metric == before
    metric["future_metric"] = 1
    with pytest.raises(ValueError, match="unknown K4 metric fields: future_metric"):
        projection.episode_projection(metric)
    with pytest.raises(ValueError, match="row field"):
        projection.raw_projection("recovery", [dict(unregistered=1)])


def test_removal_projection_preserves_r321_and_never_publishes_v1(metric):
    removal = evidence.classify_k7_removals(
        dict(targets=[dict(pod_uid="sigkill-pod", deleted=None)]),
        [],
        dict(invocations=[]),
        [],
        busy_upper=12,
        clock_slack=0.1,
    )
    actual = projection.episode_projection(metric, removal)
    assert "busy_removals" not in actual and "busy_removals_v1_retained" not in actual
    assert actual["busy_pods_removed"] == removal
    assert actual["busy_pods_removed"] is not removal
    target = actual["busy_pods_removed"]["targets"][0]
    assert target["issues"] == ["missing-deletion-event"] and target["removal"] is None
    assert actual["busy_pods_removed"]["ambiguous"]


def test_bounded_labels_uses_registered_bound(monkeypatch):
    monkeypatch.setattr(projection, "LIST_BOUND", 2)
    assert projection.bounded_labels([1, 2]) == dict(count=2, values=[1, 2], truncated=False)
    assert projection.bounded_labels([1, 2, 3]) == dict(count=3, values=[1, 2], truncated=True)


@pytest.mark.parametrize("derivable", [True, False])
def test_current_contributors_classify_all_targets_without_expanding_k7(
    tmp_path, monkeypatch, metric, derivable
):
    import dur053_record_k4 as recorder

    # One synthetic job, no materialization or checker execution. Exercise the
    # current-producer branch; the CLI fixture above exercises legacy producers.
    values, dispositions, expected = [], [], {}
    confirmation = json.loads(
        (Path(__file__).parent / "fixtures/k4-round266-intake.json").read_bytes()
    )["episodes"][0]["confirmation"]
    for event in sorted({r["event"] for r in k4.registry()}):
        cell = next(r for r in k4.registry() if r["event"] == event and r["arm"] == "treatment")
        name = cell["episode"]
        value = dict(copy.deepcopy(metric), episode=name, replacement_for="")
        if not derivable:
            value["metrics"] = "not-derivable"
            value.pop("busy_removals")
        values.append(value)
        dispositions.append(dict(episode=name))
        root = tmp_path / name
        root.mkdir()
        (root / "confirmation.json").write_text(json.dumps(dict(confirmation, episode=name)))
        expected[name] = evidence.classify_k7_removals(
            confirmation,
            [],
            dict(invocations=[]),
            [],
            busy_upper=metric["busy_removals"]["busy_upper"] if derivable else None,
            clock_slack=0,
        )
    manifest = dict(job="fixture", run_id="1", job_id="job", dispositions=dispositions, episodes=[])
    manifest.update(status="PASS", charged_seconds=0, pending=[])
    monkeypatch.setattr(
        recorder, "inventory", lambda _: ([(tmp_path / "manifest.json", manifest)], {})
    )
    monkeypatch.setattr(recorder, "materialize_job", lambda *a, **kw: nullcontext(tmp_path))
    monkeypatch.setattr(recorder, "validate_job", lambda *a, **kw: None)
    monkeypatch.setattr(recorder, "validate_disposition", lambda *a, **kw: None)
    monkeypatch.setattr(recorder, "metric_episodes", lambda *a: [])
    monkeypatch.setattr(recorder, "contributions", lambda _: set(expected))
    monkeypatch.setattr(evidence, "recorded_derivation", lambda *a, **kw: (values, []))
    monkeypatch.setattr(evidence, "producer_rules", lambda _: evidence.Derivation.CORRECTED)
    monkeypatch.setattr(evidence, "frozen", lambda _: {})
    calls, k7_inputs = [], {}

    def classify(path, *, busy_upper):
        calls.append(path.name)
        assert busy_upper == expected[path.name]["busy_upper"]
        return expected[path.name]

    def summarize(rows, *, safety_rows, k7_removals):
        assert rows == values and safety_rows == []
        k7_inputs.update(k7_removals)
        return dict(missing_episodes=["unverified"], safety=dict(passed=False))

    monkeypatch.setattr(evidence, "k7_removal_classification", classify)
    monkeypatch.setattr(evidence, "summaries", summarize)
    result = evidence.verify_evidence(tmp_path, None, jobs={("fixture", "1")})
    assert set(calls) == set(expected) and len(calls) == len(expected)
    assert {r["episode"]: r["busy_pods_removed"] for r in result["episode_metrics"]} == expected
    assert k7_inputs == {
        name: removal
        for name, removal in expected.items()
        if derivable and k4.episode_identity(name)["event"] == "worker scale-down 4 to 2"
    }


@pytest.mark.parametrize("n", [0, 8, 199, 200, 201])
def test_population_and_p99(n):
    values = list(range(n))
    actual = projection.raw_projection("observer_gaps", values)
    assert actual == dict(count=n, population=evidence.population(values))
    assert (actual["population"]["p99"] == "NOT ESTIMATED") is (n < 200)


def test_censoring_components_bounds_and_counts(metric):
    stalls = metric["assignment_stalls"]
    actual = projection.raw_projection("assignment_stalls", stalls)
    observed = [row["total"] for row in stalls if not row["censored"]]
    assert actual["components"]["total"] == evidence.population(
        observed, censored=sum(row["censored"] for row in stalls)
    )
    assert actual["components"]["total_lower"] == evidence.population(
        [row["total_lower"] for row in stalls]
    )
    assert actual["labels"]["eligible_undisposed"]["count"] == sum(
        len(row["eligible_undisposed"]) for row in stalls
    )
    endpoints = metric["readiness_endpoint_delay"]
    actual = projection.raw_projection("readiness_endpoint_delay", endpoints)
    for key in ("lower", "upper"):
        assert actual["components"][key] == evidence.population([r[key] for r in endpoints])
    rows = [
        dict(
            workflow="w",
            replacement=2,
            first_useful=None,
            terminal=None,
            censored=True,
            deadline=30,
        )
    ]
    actual = projection.raw_projection("recovery", rows)
    assert actual["components"]["terminal"] == evidence.population([], censored=1)
    assert actual["components"]["replacement"] == evidence.population([2])
    rows = [
        dict(
            partition=1,
            old_epoch=2,
            mode="release",
            seconds=None,
            first_useful=None,
            terminal_progress=[3, 4],
        )
    ]
    actual = projection.raw_projection("partition_reacquisition", rows)
    assert actual["components"]["seconds"] == evidence.population([], missing=1)
    assert actual["labels"]["terminal_progress"]["population"] == evidence.population([3, 4])


def test_cells_and_invalid_contributors(metric):
    values, dispositions = [], []
    for cell in k4.registry():
        value = dict(
            copy.deepcopy(metric),
            episode=cell["episode"],
            replacement_for="",
            status="PASS",
            valid=True,
            complete=True,
            issues=[],
        )
        values.append(value)
        dispositions.append(dict(episode=value["episode"], classification="valid"))
    values[0].update(status="FAIL", valid=False, complete=False, issues=["test-issue"])
    manifests = [dict(job="fixture", run_id="1", dispositions=dispositions)]
    actual = projection.reporting_projection(values, manifests)
    expected = {(r["event"], r["duration"], r["arm"]) for r in k4.registry()}
    cells = actual["cell_metrics"]
    assert len(cells) == len(expected) == 35
    assert {(c["event"], c["duration"], c["arm"]) for c in cells} == expected
    assert sorted(i for c in cells for i in c["episode_metric_indices"]) == list(range(161))
    assert all(len(c["episodes"]) == c["registered_population"] for c in cells)
    row = actual["episode_results"][0]
    assert row["episode"] == values[0]["episode"] and row["issues"] == ["test-issue"]
    assert row["job"] == "fixture" and row["run_id"] == "1" and not row["valid"]
    assert [c["failed"] for c in cells if c["failed"]] == [[row["episode"]]]
    removed = values.pop()
    actual = projection.reporting_projection(values, manifests)
    assert [c["missing"] for c in actual["cell_metrics"] if c["missing"]] == [[removed["episode"]]]


def test_stopped_metrics_keep_unavailable_fields_explicit():
    metric = dict(
        schema="dur053.k4-metrics.v1",
        episode="short-core-0-pair-0-baseline",
        replacement_for="",
        status="FAIL",
        valid=True,
        complete=False,
        metrics="not-derivable",
        missing_exports=["timeline.json"],
        evidence="stopped",
    )
    actual = projection.episode_projection(metric)
    assert actual["issues"] is actual["intervention_outcome"] is actual["limitations"] is None
