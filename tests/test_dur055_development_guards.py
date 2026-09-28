from __future__ import annotations

import json
from pathlib import Path

import pytest
from incident_agent.dur055_agent import development_incident_cases
from incident_agent.dur055_budget import SpendLedger, SpendLimitError
from incident_agent.dur055_retrieval import (
    RetrievalStudyError,
    development_retrieval_queries,
    fingerprint,
    heldout_ids_fingerprint,
    tune_development,
)


def test_dur055_development_inputs_have_only_frozen_dev_counts() -> None:
    queries = development_retrieval_queries()
    cases = development_incident_cases()
    assert len(queries) == 40
    assert len(cases) == 10
    assert all(row["split"] == "development" for row in queries)
    assert all(case.split == "development" for case in cases)
    assert [row["query_id"] for row in queries] == [f"dev-q-{i:03d}" for i in range(1, 41)]
    assert heldout_ids_fingerprint() == fingerprint(
        [f"hel-q-{index:03d}" for index in range(1, 121)]
    )


def test_dur055_tuning_rejects_heldout_rows_before_database_access() -> None:
    with pytest.raises(RetrievalStudyError, match="exactly 40 development"):
        tune_development(None, [{"split": "heldout"}])  # type: ignore[arg-type]


def test_dur055_spend_ledger_rejects_oversized_request(tmp_path: Path) -> None:
    source = (
        Path(__file__).parents[1] / "experiments/m8/dur055-applied-ai-upgrade/spend-ledger.json"
    )
    data = json.loads(source.read_text(encoding="utf-8"))
    data["entries"] = []
    data["spent_usd"] = "0.00"
    data["reserved_usd"] = "0.00"
    data["uncertain_usd"] = "0.00"
    ledger_path = tmp_path / "ledger.json"
    ledger_path.write_text(json.dumps(data), encoding="utf-8")
    ledger = SpendLedger(ledger_path)
    with pytest.raises(SpendLimitError, match="100000 bytes"):
        ledger.reserve(model="gpt-6-luna", operation="test", request_bytes=100_001)
    assert ledger.snapshot()["reserved_usd"] == "0.00"
