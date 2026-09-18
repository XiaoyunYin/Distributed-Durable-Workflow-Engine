"""Fixed redaction and source-boundary scanning for incident evidence."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class RedactionResult:
    text: str
    count: int
    categories: tuple[str, ...]


_PATTERNS: tuple[tuple[str, re.Pattern[str], str], ...] = (
    ("secret", re.compile(r"\b(?:sk|pk|token|secret)_[A-Za-z0-9_-]{8,}\b"), "[REDACTED:SECRET]"),
    (
        "email",
        re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
        "[REDACTED:EMAIL]",
    ),
    ("ip", re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"), "[REDACTED:IP]"),
    ("canary", re.compile(r"\bCANARY_(?:SECRET|PII)_[A-Z0-9_-]+\b"), "[REDACTED:CANARY]"),
)


def redact(text: str) -> RedactionResult:
    categories: list[str] = []
    count = 0
    redacted = text
    for category, pattern, replacement in _PATTERNS:
        matches = pattern.findall(redacted)
        if matches:
            categories.extend([category] * len(matches))
            count += len(matches)
            redacted = pattern.sub(replacement, redacted)
    return RedactionResult(redacted, count, tuple(categories))


def scan_downstream(surfaces: dict[str, str]) -> list[dict[str, str]]:
    """Find canaries outside the declared raw source-store boundary."""

    leaks: list[dict[str, str]] = []
    for surface, text in surfaces.items():
        result = redact(text)
        if result.count:
            leaks.append({"surface": surface, "categories": ",".join(result.categories)})
    return leaks
