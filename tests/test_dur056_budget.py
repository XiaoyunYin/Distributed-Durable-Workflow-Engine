from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest
from incident_agent.dur056_budget import Dur056SpendError, Dur056SpendLedger

LEDGER_SOURCE = (
    Path(__file__).parents[1]
    / "experiments"
    / "m8"
    / "dur056-solvable-agent-study"
    / "spend-ledger.json"
)


def _ledger(tmp_path: Path) -> tuple[Dur056SpendLedger, Path]:
    path = tmp_path / "spend-ledger.json"
    data = json.loads(LEDGER_SOURCE.read_text(encoding="utf-8"))
    data["entries"] = []
    data["spent_usd"] = "0.00000000"
    data["reserved_usd"] = "0.00000000"
    data["uncertain_usd"] = "0.00000000"
    path.write_text(json.dumps(data), encoding="utf-8")
    return Dur056SpendLedger(path), path


def test_dur056_ledger_reserves_settles_and_records_cost(tmp_path: Path) -> None:
    ledger, path = _ledger(tmp_path)
    call_id = ledger.reserve(
        model="gpt-6-luna",
        operation="unit-test",
        request_bytes=100,
        max_output_tokens=100,
    )
    assert ledger.snapshot()["reserved_usd"] != "0.00000000"
    cost = ledger.settle(
        call_id,
        input_tokens=50,
        output_tokens=20,
        reasoning_tokens=3,
        response_id="resp_test",
        latency_ms=12.5,
    )
    assert cost == Decimal("0.000015")
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["spent_usd"] == "0.00001500"
    assert data["reserved_usd"] == "0.00000000"
    assert data["entries"][0]["state"] == "SETTLED"


def test_dur056_ledger_refuses_a_reservation_over_the_cap(tmp_path: Path) -> None:
    _, path = _ledger(tmp_path)
    data = json.loads(path.read_text(encoding="utf-8"))
    data["spent_usd"] = "24.99999999"
    path.write_text(json.dumps(data), encoding="utf-8")
    ledger = Dur056SpendLedger(path)
    with pytest.raises(Dur056SpendError, match="cap rejected"):
        ledger.reserve(
            model="gpt-6-luna",
            operation="would-exceed-cap",
            request_bytes=200,
            max_output_tokens=1200,
        )
    assert json.loads(path.read_text(encoding="utf-8"))["entries"] == []


def test_dur056_ledger_rejects_unknown_models_and_unbounded_requests(tmp_path: Path) -> None:
    ledger, _ = _ledger(tmp_path)
    with pytest.raises(Dur056SpendError, match="no authorized price"):
        ledger.reserve(model="gpt-6-sol", operation="wrong-model", request_bytes=100)
    with pytest.raises(Dur056SpendError, match="request must be"):
        ledger.reserve(model="gpt-6-luna", operation="oversized", request_bytes=100_001)


def test_dur056_not_sent_reconciliation_appends_without_mutating_original_request(
    tmp_path: Path,
) -> None:
    ledger, path = _ledger(tmp_path)
    call_id = ledger.reserve(
        model="gpt-6-luna",
        operation="socket-blocked-pilot",
        request_bytes=100,
        max_output_tokens=100,
    )
    ledger.fail(call_id, outcome_uncertain=True, detail="transport uncertain")
    before = json.loads(path.read_text(encoding="utf-8"))
    original = dict(before["entries"][0])

    reconciliation = ledger.reconcile_not_sent(
        [call_id],
        reason="WinError 10013 at socket creation; request did not leave the machine",
        evidence="injection-pilot-r209-attempt-1-transport-assessment.json",
    )

    after = json.loads(path.read_text(encoding="utf-8"))
    assert len(after["entries"]) == 2
    assert after["entries"][0] == original
    assert after["entries"][0]["state"] == "UNCERTAIN"
    assert after["entries"][1]["state"] == "NOT_SENT"
    assert after["entries"][1]["reconciles_call_id"] == call_id
    assert reconciliation["requests_reconciled"] == 1
    assert ledger.snapshot()["uncertain_usd"] == "0.00000000"
    with pytest.raises(Dur056SpendError, match="already has"):
        ledger.reconcile_not_sent([call_id], reason="duplicate", evidence="assessment.json")
