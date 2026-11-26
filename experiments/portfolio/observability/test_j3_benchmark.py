import json
import sys
from pathlib import Path

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
