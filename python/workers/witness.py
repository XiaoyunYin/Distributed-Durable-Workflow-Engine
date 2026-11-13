"""Opt-in external execution ledger and explicit K4 campaign selection.

The historical K3 ledger logs evidence failures without changing engine calls.
One process owns one append-only file; its observer exports inventory() from each
pod/process. K4 uses the separate fail-closed, sealed CampaignWitness implementation.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from workers.k4_witness import CampaignWitness, campaign_enabled

LOG = logging.getLogger(__name__)


class InvocationWitness:
    def __init__(self, directory: str, pod_name: str, pod_uid: str) -> None:
        self.directory = Path(directory)
        self.pod_name = pod_name
        self.pod_uid = pod_uid
        self.session = str(uuid.uuid4())
        self.path = self.directory / f"{self.session}.jsonl"
        self.lock = threading.Lock()
        self.attempted = 0
        self.written = 0
        self.errors = 0
        self.digest = hashlib.sha256()
        self._append({"event": "session"})

    def _append(self, event: dict[str, Any]) -> None:
        with self.lock:
            self.attempted += 1
            event = {
                **event,
                "session": self.session,
                "pod_name": self.pod_name,
                "pod_uid": self.pod_uid,
                "sequence": self.attempted,
                "timestamp": datetime.now(UTC).isoformat(),
                "monotonic_ns": time.monotonic_ns(),
            }
            try:
                encoded = (json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n").encode()
                # Directory provisioning is the campaign's responsibility; no
                # fallback into container /tmp or the engine database.
                with self.path.open("ab", buffering=0) as ledger:
                    if ledger.write(encoded) != len(encoded):
                        raise OSError("short witness write")
                    os.fsync(ledger.fileno())
                self.digest.update(encoded)
                self.written += 1
            except Exception:
                self.errors += 1
                LOG.exception("invocation witness incomplete session=%s", self.session)

    def begin(self, fields: dict[str, Any]) -> dict[str, Any]:
        invocation = {**fields, "invocation_id": str(uuid.uuid4())}
        self._append({**invocation, "event": "start"})
        return invocation

    def end(self, invocation: dict[str, Any], outcome: str) -> None:
        self._append({**invocation, "event": "end", "outcome": outcome})

    def inventory(self) -> dict[str, Any]:
        with self.lock:
            return {
                "session": self.session,
                "pod_name": self.pod_name,
                "pod_uid": self.pod_uid,
                "file": self.path.name,
                "attempted": self.attempted,
                "written": self.written,
                "errors": self.errors,
                "sha256": self.digest.hexdigest(),
                "complete": self.errors == 0 and self.attempted == self.written,
            }


_GLOBAL: InvocationWitness | CampaignWitness | None = None
_GLOBAL_LOCK = threading.Lock()


def configured_witness() -> InvocationWitness | CampaignWitness | None:
    global _GLOBAL
    directory = os.getenv("DUR053_WITNESS_DIR", "")
    campaign = campaign_enabled()
    if campaign and (not directory or not os.getenv("RUNTIME_NAMESPACE")):
        raise ValueError("K4 campaign requires DUR053_WITNESS_DIR and RUNTIME_NAMESPACE")
    if not directory:
        return None
    with _GLOBAL_LOCK:
        if _GLOBAL is None:
            arguments = (
                directory,
                os.getenv("POD_NAME", os.getenv("WORKER_ID", "unknown")),
                os.getenv("POD_UID", os.getenv("HOSTNAME", "unknown")),
            )
            _GLOBAL = (
                CampaignWitness(
                    directory, arguments[1], os.getenv("POD_UID", ""), os.getenv("CONTAINER_ID", "")
                )
                if campaign
                else InvocationWitness(*arguments)
            )
        return _GLOBAL


def campaign_witness() -> CampaignWitness | None:
    if not campaign_enabled():
        return None
    witness = configured_witness()
    if not isinstance(witness, CampaignWitness):
        raise ValueError("K4 campaign cannot reuse a K3 witness session")
    return witness
