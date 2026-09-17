"""Frozen workflow-to-partition mapping for the initial scheduler topology."""

from __future__ import annotations

import hashlib

MAP_VERSION = "sha256-u64-be-v1"
PARTITION_COUNT = 16


def partition_for(workflow_id: str) -> int:
    """Return the contract partition for a non-empty UTF-8 workflow ID."""
    if not workflow_id:
        raise ValueError("workflow ID must not be empty")
    digest = hashlib.sha256(workflow_id.encode("utf-8")).digest()
    value = int.from_bytes(digest[:8], byteorder="big", signed=False)
    return value % PARTITION_COUNT
