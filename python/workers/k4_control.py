"""Per-logical-request K4 trace context, shared across uncertain retries."""

from __future__ import annotations

import hashlib
import json
import uuid
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any


@dataclass
class RequestTrace:
    request: str
    body_hash: str
    attempt: int = 0
    status: int = 0


CURRENT: ContextVar[RequestTrace | None] = ContextVar("k4_control_request", default=None)


def new_request(body: dict[str, Any]) -> RequestTrace:
    return RequestTrace(
        str(uuid.uuid4()),
        hashlib.sha256(json.dumps(body, separators=(",", ":")).encode()).hexdigest(),
    )
