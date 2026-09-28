"""Persistent, conservative spend reservations for the DUR-055 API cap."""

from __future__ import annotations

import json
import os
import tempfile
from decimal import Decimal
from pathlib import Path
from threading import Lock
from typing import Any
from uuid import uuid4


class SpendLimitError(RuntimeError):
    """Raised when a DUR-055 request cannot be reserved under its fixed cap."""


_MILLION = Decimal(1_000_000)


class SpendLedger:
    """Atomically persist a reservation before each provider request."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = Lock()
        self._data = self._read()
        if self._data.get("schema") != "dur055-spend-ledger.v1":
            raise SpendLimitError("DUR-055 spend ledger schema is invalid")
        if Decimal(str(self._data.get("cap_usd", "0"))) != Decimal("100.00"):
            raise SpendLimitError("DUR-055 spend cap must remain exactly $100.00")

    def _read(self) -> dict[str, Any]:
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise SpendLimitError(f"cannot read DUR-055 spend ledger: {error}") from error
        if not isinstance(value, dict) or not isinstance(value.get("entries"), list):
            raise SpendLimitError("DUR-055 spend ledger is malformed")
        return value

    def _write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = (json.dumps(self._data, indent=2, sort_keys=True) + "\n").encode("utf-8")
        fd, name = tempfile.mkstemp(prefix="dur055-ledger-", suffix=".tmp", dir=self.path.parent)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(name, self.path)
        finally:
            if os.path.exists(name):
                os.unlink(name)

    @staticmethod
    def _rate(price: dict[str, Any], key: str) -> Decimal:
        try:
            return Decimal(str(price[key]))
        except (KeyError, ArithmeticError) as error:
            raise SpendLimitError(f"missing API price field: {key}") from error

    def _price(self, model: str) -> dict[str, Any]:
        prices = self._data.get("price_sources")
        if not isinstance(prices, dict) or model not in prices:
            raise SpendLimitError(f"no authorized price is recorded for model {model}")
        price = prices[model]
        if not isinstance(price, dict):
            raise SpendLimitError(f"price record for {model} is malformed")
        return price

    @staticmethod
    def _total(data: dict[str, Any], field: str) -> Decimal:
        return Decimal(str(data.get(field, "0.00")))

    def reserve(
        self,
        *,
        model: str,
        operation: str,
        request_bytes: int,
        max_output_tokens: int = 0,
    ) -> str:
        """Reserve a byte-count upper bound before an HTTP request is sent.

        For UTF-8 JSON, token count cannot exceed the number of request bytes.
        Requests are bounded to 100 kB to keep the cap check easy to audit.
        """

        if request_bytes <= 0 or request_bytes > 100_000:
            raise SpendLimitError("DUR-055 request must be between 1 and 100000 bytes")
        if max_output_tokens < 0 or max_output_tokens > 2_000:
            raise SpendLimitError("DUR-055 max_output_tokens is outside the reserved bound")
        with self._lock:
            self._data = self._read()
            price = self._price(model)
            input_rate = self._rate(price, "input_usd_per_million_tokens")
            output_rate = Decimal("0")
            if max_output_tokens:
                output_rate = self._rate(price, "output_usd_per_million_tokens")
            reserve = (Decimal(request_bytes) * input_rate / _MILLION) + (
                Decimal(max_output_tokens) * output_rate / _MILLION
            )
            used = sum(
                self._total(self._data, field)
                for field in ("spent_usd", "reserved_usd", "uncertain_usd")
            )
            cap = Decimal(str(self._data["cap_usd"]))
            if used + reserve > cap:
                raise SpendLimitError(
                    f"DUR-055 $100 cap reservation rejected: ${used + reserve:.8f} total"
                )
            call_id = uuid4().hex
            self._data["reserved_usd"] = f"{self._total(self._data, 'reserved_usd') + reserve:.8f}"
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
        cached_input_tokens: int = 0,
        reasoning_tokens: int = 0,
        response_id: str | None,
        latency_ms: float,
    ) -> Decimal:
        if min(input_tokens, output_tokens, cached_input_tokens, reasoning_tokens) < 0:
            raise SpendLimitError("provider usage cannot contain negative token counts")
        if cached_input_tokens > input_tokens:
            raise SpendLimitError("cached input tokens exceed total input tokens")
        with self._lock:
            self._data = self._read()
            entry = self._entry(call_id)
            if entry["state"] != "RESERVED":
                raise SpendLimitError("only an open DUR-055 reservation can be settled")
            price = self._price(str(entry["model"]))
            input_rate = self._rate(price, "input_usd_per_million_tokens")
            cached_rate = Decimal(str(price.get("cached_input_usd_per_million_tokens", input_rate)))
            output_rate = Decimal("0")
            if output_tokens:
                output_rate = self._rate(price, "output_usd_per_million_tokens")
            uncached_tokens = input_tokens - cached_input_tokens
            actual = (
                Decimal(uncached_tokens) * input_rate
                + Decimal(cached_input_tokens) * cached_rate
                + Decimal(output_tokens) * output_rate
            ) / _MILLION
            reserved = Decimal(str(entry["reserved_usd"]))
            self._data["reserved_usd"] = f"{self._total(self._data, 'reserved_usd') - reserved:.8f}"
            spent = self._total(self._data, "spent_usd") + actual
            used = (
                spent
                + self._total(self._data, "reserved_usd")
                + self._total(self._data, "uncertain_usd")
            )
            if used > Decimal(str(self._data["cap_usd"])):
                raise SpendLimitError("provider usage would exceed the DUR-055 $100 cap")
            self._data["spent_usd"] = f"{spent:.8f}"
            entry.update(
                {
                    "state": "SETTLED",
                    "input_tokens": input_tokens,
                    "cached_input_tokens": cached_input_tokens,
                    "output_tokens": output_tokens,
                    "reasoning_tokens": reasoning_tokens,
                    "response_id": response_id,
                    "latency_ms": round(latency_ms, 3),
                    "cost_usd": f"{actual:.8f}",
                }
            )
            self._write()
            return actual

    def fail(self, call_id: str, *, outcome_uncertain: bool, detail: str) -> None:
        """Release a known rejected request or retain a possibly billable call."""

        with self._lock:
            self._data = self._read()
            entry = self._entry(call_id)
            if entry["state"] != "RESERVED":
                return
            reserve = Decimal(str(entry["reserved_usd"]))
            self._data["reserved_usd"] = f"{self._total(self._data, 'reserved_usd') - reserve:.8f}"
            if outcome_uncertain:
                uncertain = self._total(self._data, "uncertain_usd") + reserve
                self._data["uncertain_usd"] = f"{uncertain:.8f}"
            entry.update(
                {
                    "state": "UNCERTAIN" if outcome_uncertain else "REJECTED",
                    "failure": detail[:500],
                }
            )
            self._data["status"] = "RECONCILIATION_REQUIRED" if outcome_uncertain else "IN_PROGRESS"
            self._write()

    def _entry(self, call_id: str) -> dict[str, Any]:
        for entry in self._data["entries"]:
            if isinstance(entry, dict) and entry.get("call_id") == call_id:
                return entry
        raise SpendLimitError("DUR-055 spend reservation does not exist")

    def snapshot(self) -> dict[str, str]:
        self._data = self._read()
        return {
            "cap_usd": str(self._data["cap_usd"]),
            "spent_usd": str(self._data["spent_usd"]),
            "reserved_usd": str(self._data["reserved_usd"]),
            "uncertain_usd": str(self._data["uncertain_usd"]),
            "status": str(self._data["status"]),
        }
