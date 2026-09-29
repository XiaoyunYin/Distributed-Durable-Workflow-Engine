"""$25.00 write-ahead spend ledger for DUR-056 only."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from threading import Lock
from typing import Any, cast
from uuid import uuid4

CAP_USD = Decimal("25.00")
MILLION = Decimal(1_000_000)


class Dur056SpendError(RuntimeError):
    """Raised when a DUR-056 API reservation or ledger operation is invalid."""


class Dur056SpendLedger:
    """Reserve the bounded request cost before sending each paid API request."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = Lock()
        self._data = self._read()
        if self._data.get("schema") != "dur056-spend-ledger.v1":
            raise Dur056SpendError("DUR-056 spend ledger schema is invalid")
        if Decimal(str(self._data.get("cap_usd", "0"))) != CAP_USD:
            raise Dur056SpendError("DUR-056 spend cap must remain exactly $25.00")
        prices = self._data.get("price_sources")
        if not isinstance(prices, dict) or not {
            "gpt-6-luna",
            "text-embedding-3-small",
        }.issubset(prices):
            raise Dur056SpendError("DUR-056 authorized model prices are missing")

    def _read(self) -> dict[str, Any]:
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise Dur056SpendError(f"cannot read DUR-056 spend ledger: {error}") from error
        if not isinstance(value, dict) or not isinstance(value.get("entries"), list):
            raise Dur056SpendError("DUR-056 spend ledger is malformed")
        return cast(dict[str, Any], value)

    def _write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = (json.dumps(self._data, indent=2, sort_keys=True) + "\n").encode("utf-8")
        descriptor, temporary = tempfile.mkstemp(
            prefix="dur056-ledger-", suffix=".tmp", dir=self.path.parent
        )
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def _price(self, model: str) -> dict[str, Any]:
        prices = self._data.get("price_sources")
        if not isinstance(prices, dict) or not isinstance(prices.get(model), dict):
            raise Dur056SpendError(f"no authorized price is recorded for model {model}")
        return cast(dict[str, Any], prices[model])

    @staticmethod
    def _decimal(value: Any, field: str) -> Decimal:
        try:
            return Decimal(str(value))
        except Exception as error:
            raise Dur056SpendError(f"invalid ledger amount for {field}") from error

    def _total(self, field: str) -> Decimal:
        return self._decimal(self._data.get(field, "0.00"), field)

    def reserve(
        self,
        *,
        model: str,
        operation: str,
        request_bytes: int,
        max_output_tokens: int = 0,
    ) -> str:
        if not operation or len(operation) > 160:
            raise Dur056SpendError("DUR-056 operation label is invalid")
        if request_bytes <= 0 or request_bytes > 100_000:
            raise Dur056SpendError("DUR-056 request must be between 1 and 100000 bytes")
        if max_output_tokens < 0 or max_output_tokens > 2_000:
            raise Dur056SpendError("DUR-056 max_output_tokens is outside the reserved bound")
        with self._lock:
            self._data = self._read()
            price = self._price(model)
            try:
                input_rate = Decimal(str(price["input_usd_per_million_tokens"]))
            except (KeyError, ArithmeticError) as error:
                raise Dur056SpendError(f"DUR-056 price for {model} is malformed") from error
            output_rate = Decimal(str(price.get("output_usd_per_million_tokens", "0")))
            reserve = (
                Decimal(request_bytes) * input_rate + Decimal(max_output_tokens) * output_rate
            ) / MILLION
            committed = (
                self._total("spent_usd")
                + self._total("reserved_usd")
                + self._total("uncertain_usd")
            )
            if committed + reserve > CAP_USD:
                raise Dur056SpendError(
                    f"DUR-056 $25 cap rejected reservation (${committed + reserve:.8f})"
                )
            call_id = uuid4().hex
            self._data["reserved_usd"] = f"{self._total('reserved_usd') + reserve:.8f}"
            self._data["status"] = "IN_PROGRESS"
            self._data["entries"].append(
                {
                    "call_id": call_id,
                    "model": model,
                    "operation": operation,
                    "state": "RESERVED",
                    "request_bytes": request_bytes,
                    "input_token_reservation": request_bytes,
                    "output_token_reservation": max_output_tokens,
                    "reserved_usd": f"{reserve:.8f}",
                }
            )
            self._write()
            return call_id

    def settle(
        self,
        call_id: str,
        *,
        input_tokens: int,
        output_tokens: int = 0,
        reasoning_tokens: int = 0,
        response_id: str | None,
        latency_ms: float,
    ) -> Decimal:
        if min(input_tokens, output_tokens, reasoning_tokens) < 0:
            raise Dur056SpendError("DUR-056 provider usage cannot be negative")
        with self._lock:
            self._data = self._read()
            entry = self._entry(call_id)
            if entry.get("state") != "RESERVED":
                raise Dur056SpendError("only an open DUR-056 reservation can be settled")
            price = self._price(str(entry["model"]))
            input_rate = Decimal(str(price["input_usd_per_million_tokens"]))
            output_rate = Decimal(str(price.get("output_usd_per_million_tokens", "0")))
            cost = (
                Decimal(input_tokens) * input_rate + Decimal(output_tokens) * output_rate
            ) / MILLION
            self._data["reserved_usd"] = (
                f"{self._total('reserved_usd') - Decimal(str(entry['reserved_usd'])):.8f}"
            )
            new_spent = self._total("spent_usd") + cost
            self._data["spent_usd"] = f"{new_spent:.8f}"
            entry.update(
                {
                    "state": "SETTLED",
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "reasoning_tokens": reasoning_tokens,
                    "response_id": response_id,
                    "latency_ms": round(latency_ms, 3),
                    "cost_usd": f"{cost:.8f}",
                }
            )
            if new_spent + self._total("reserved_usd") + self._total("uncertain_usd") > CAP_USD:
                self._data["status"] = "CAP_BREACH_RECORDED"
            self._write()
            if self._data["status"] == "CAP_BREACH_RECORDED":
                raise Dur056SpendError("provider usage exceeded the DUR-056 cap reservation")
            return cost

    def fail(self, call_id: str, *, outcome_uncertain: bool, detail: str) -> None:
        with self._lock:
            self._data = self._read()
            entry = self._entry(call_id)
            if entry.get("state") != "RESERVED":
                return
            reserved = Decimal(str(entry["reserved_usd"]))
            self._data["reserved_usd"] = f"{self._total('reserved_usd') - reserved:.8f}"
            if outcome_uncertain:
                self._data["uncertain_usd"] = f"{self._total('uncertain_usd') + reserved:.8f}"
            entry.update(
                {
                    "state": "UNCERTAIN" if outcome_uncertain else "REJECTED",
                    "failure": detail[:500],
                }
            )
            self._data["status"] = "RECONCILIATION_REQUIRED" if outcome_uncertain else "IN_PROGRESS"
            self._write()

    def reconcile_not_sent(
        self,
        call_ids: Sequence[str],
        *,
        reason: str,
        evidence: str,
    ) -> dict[str, str | int]:
        """Append NOT_SENT reconciliation events without changing original requests."""

        if not call_ids or len(set(call_ids)) != len(call_ids):
            raise Dur056SpendError("NOT_SENT reconciliation requires unique request IDs")
        if not reason.strip() or not evidence.strip():
            raise Dur056SpendError("NOT_SENT reconciliation requires reason and evidence")
        with self._lock:
            self._data = self._read()
            entries = self._data["entries"]
            requests = {
                str(entry["call_id"]): entry
                for entry in entries
                if isinstance(entry, dict) and entry.get("call_id")
            }
            reconciled = {
                str(entry["reconciles_call_id"])
                for entry in entries
                if isinstance(entry, dict)
                and entry.get("entry_type") == "RECONCILIATION"
                and entry.get("reconciles_call_id")
            }
            if any(call_id in reconciled for call_id in call_ids):
                raise Dur056SpendError("request already has a ledger reconciliation")
            targets: list[tuple[str, dict[str, Any], Decimal]] = []
            for call_id in call_ids:
                entry = requests.get(call_id)
                if entry is None or entry.get("state") != "UNCERTAIN":
                    raise Dur056SpendError("NOT_SENT reconciliation requires UNCERTAIN requests")
                amount = self._decimal(entry.get("reserved_usd"), "request reserved_usd")
                if amount <= 0:
                    raise Dur056SpendError("NOT_SENT reconciliation amount must be positive")
                targets.append((call_id, entry, amount))
            amount_total = sum((amount for _, _, amount in targets), Decimal(0))
            uncertain = self._total("uncertain_usd")
            if amount_total > uncertain:
                raise Dur056SpendError("NOT_SENT reconciliation exceeds uncertain ledger amount")
            timestamp = datetime.now(UTC).isoformat()
            for call_id, _, amount in targets:
                entries.append(
                    {
                        "event_id": uuid4().hex,
                        "entry_type": "RECONCILIATION",
                        "reconciles_call_id": call_id,
                        "prior_state": "UNCERTAIN",
                        "state": "NOT_SENT",
                        "amount_usd": f"{amount:.8f}",
                        "reason": reason[:500],
                        "evidence": evidence[:300],
                        "reconciled_at": timestamp,
                    }
                )
            remaining = uncertain - amount_total
            self._data["uncertain_usd"] = f"{remaining:.8f}"
            self._data["status"] = (
                "RECONCILIATION_REQUIRED" if remaining > 0 else "IN_PROGRESS"
            )
            self._write()
            return {
                "requests_reconciled": len(targets),
                "amount_reconciled_usd": f"{amount_total:.8f}",
                "uncertain_usd_remaining": f"{remaining:.8f}",
                "status": str(self._data["status"]),
            }

    def _entry(self, call_id: str) -> dict[str, Any]:
        for entry in self._data["entries"]:
            if isinstance(entry, dict) and entry.get("call_id") == call_id:
                return entry
        raise Dur056SpendError("DUR-056 reservation does not exist")

    def snapshot(self) -> dict[str, str]:
        self._data = self._read()
        return {
            "cap_usd": str(self._data["cap_usd"]),
            "spent_usd": str(self._data["spent_usd"]),
            "reserved_usd": str(self._data["reserved_usd"]),
            "uncertain_usd": str(self._data["uncertain_usd"]),
            "status": str(self._data["status"]),
        }
