import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import j3_benchmark as benchmark  # noqa: E402
from j3_benchmark import CYCLES, summarize  # noqa: E402


def test_cycle_target_checks_p99_max_and_failures():
    row = {
        "cycle_ns": 1_000_000_000,
        "sql_ns": 500_000_000,
        "demand_ns": 1,
        "sql_decode_ns": 1,
        "sql_bytes": 1,
    }
    rows = [dict(row) for _ in range(CYCLES)]
    rows[-1]["cycle_ns"] = 1_500_000_000
    assert summarize(rows)["meets_target"]

    rows[-1]["cycle_ns"] += 1
    assert not summarize(rows)["meets_target"]
    rows[-1]["cycle_ns"] -= 1
    rows[-2]["cycle_ns"] += 1
    assert not summarize(rows)["meets_target"]
    rows[-2]["cycle_ns"] -= 1
    rows[0]["sql_failure"] = "postgres_57014"
    assert not summarize(rows)["meets_target"]


def test_fixture_stops_when_the_installer_changes_engine_indexes(tmp_path, monkeypatch):
    indexes = iter([["registered"], ["registered", "observer_index"]])
    monkeypatch.setattr(benchmark, "engine_indexes", lambda database: next(indexes))

    def sql(database, statement, **kwargs):
        assert "SELECT j3_bench.add_targets" not in statement

    monkeypatch.setattr(benchmark, "sql", sql)
    with pytest.raises(RuntimeError, match="candidate fixture changed the engine indexes"):
        benchmark.create_fixture("candidate", "", tmp_path)
    evidence = json.loads((tmp_path / "candidate-engine-indexes.json").read_bytes())
    assert evidence == {
        "original": ["registered"],
        "installed": ["registered", "observer_index"],
        "equal": False,
    }


@pytest.mark.parametrize("jit_fires", [True, False])
def test_jit_floor_records_effective_settings_and_plan_contrast(tmp_path, monkeypatch, jit_fires):
    queries = []

    def sql(database, statement, *, role="postgres"):
        assert database == "candidate"
        if "engine.workflow_executions" in statement:
            return "{}"
        jit = "off" if statement.startswith("SET jit = off;") else "on"
        queries.append((role, jit))
        assert "jit_above_cost" in statement and "jit_optimize_above_cost" in statement
        return json.dumps({"jit": jit, "jit_above_cost": "100000", "statement_timeout": "2000"})

    def explain(database, projection, output, *, jit):
        assert (database, projection) == ("candidate", "deployed SQL")
        plan = {"JIT": {"Functions": 79}} if jit_fires and jit == "on" else {"Plan": {}}
        (output / "explain-body.json").write_text(json.dumps([plan]), encoding="utf-8")
        return True

    def run(*args, data):
        assert "j3_exporter" in args and data.count(benchmark.SAMPLE) == benchmark.JIT_SAMPLES + 1
        assert "\\timing on" in data and "jit_above_cost" not in data
        return SimpleNamespace(stdout="Time: 12.345 ms\n" * benchmark.JIT_SAMPLES, stderr="")

    monkeypatch.setattr(benchmark, "sql", sql)
    monkeypatch.setattr(benchmark, "explain", explain)
    monkeypatch.setattr(benchmark, "run", run)
    report = benchmark.measure_jit_floor("candidate", "deployed SQL", tmp_path / "jit-floor")
    assert queries == [("postgres", "on"), ("j3_exporter", "on"), ("j3_exporter", "off")]
    assert report["comparison_valid"]
    assert report["body_plans_show_jit_contrast"] == jit_fires
    assert report["variants"]["off"]["sql_ns"] == dict.fromkeys(
        ("p50_ns", "p99_ns", "max_ns"), 12_345_000
    )
