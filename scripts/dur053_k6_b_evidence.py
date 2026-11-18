"""Credential-free and incrementally retained Phase B outcomes."""

from __future__ import annotations

import base64
import json
import re
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import quote

CREDENTIAL_PATTERN = re.compile(
    rb"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----|SCRAM-SHA-256\$\d+:"
    rb"|eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}"
)


class SmokeFailure(RuntimeError):
    """A predetermined smoke stage or predicate failed."""


def reason(error: BaseException) -> str:
    if isinstance(error, SmokeFailure):
        return str(error)
    return f"UNEXPECTED_{type(error).__name__.upper()}"


class Evidence:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.secrets: set[bytes] = set()
        self.report: dict = {
            "schema": "dur053.k6-phase-b-smoke.v1",
            "status": "FAIL",
            "reason": "SMOKE_INCOMPLETE",
            "measurement_claim": False,
            "charged_seconds": 0,
            "outcomes": [],
        }
        self.checkpoint()

    def secret(self, value: str) -> None:
        if value:
            raw = value.encode()
            self.secrets.update((raw, base64.b64encode(raw), quote(value, safe="").encode()))
            if b"PRIVATE KEY" in raw:
                self.secrets.update(line for line in raw.splitlines() if len(line) >= 32)

    def check(self, data: bytes) -> None:
        if CREDENTIAL_PATTERN.search(data) or any(secret in data for secret in self.secrets):
            raise SmokeFailure("CREDENTIAL_SCAN_FAILED")

    def write(self, name: str, document: object) -> None:
        data = (json.dumps(document, indent=2, sort_keys=True) + "\n").encode()
        self.check(data)
        temporary = self.root / f".{name}.tmp"
        temporary.write_bytes(data)
        temporary.replace(self.root / name)

    def checkpoint(self) -> None:
        self.write("smoke.json", self.report)

    def safe_reason(self, error: BaseException) -> str:
        value = reason(error)
        try:
            self.check(value.encode())
        except SmokeFailure:
            return "CREDENTIAL_SCAN_FAILED"
        return value

    @contextmanager
    def step(self, name: str):
        row = {"step": name, "status": "RUNNING"}
        self.report["outcomes"].append(row)
        self.report["reason"] = f"INCOMPLETE_{name}"
        self.checkpoint()
        try:
            yield row
            row["status"] = "PASS"
            self.checkpoint()
        except BaseException as error:
            # Replace the whole row in case an invalid result contained a secret.
            code = self.safe_reason(error)
            row.clear()
            row.update(step=name, status="FAIL", reason=code)
            self.report["reason"] = code
            self.checkpoint()
            raise

    def scan(self) -> None:
        contaminated = []
        for path in self.root.rglob("*"):
            if path.is_file():
                try:
                    self.check(path.read_bytes())
                except SmokeFailure:
                    contaminated.append(path)
        for path in contaminated:
            path.unlink()
        if contaminated:
            raise SmokeFailure("CREDENTIAL_SCAN_FAILED")
