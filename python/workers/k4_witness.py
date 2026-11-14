"""K4-only durable session seals, readable after process death from shared storage.

Each seal authenticates fsynced prefixes of the invocation and trace files. A
SIGKILL between append and seal may leave an unacknowledged tail; retain that tail
as raw evidence, but never invent acknowledged starts from it. Observer clock
mapping and independent process-death sealing belong to the B2 collector.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NoReturn, cast


def campaign_enabled() -> bool:
    flag = os.getenv("DUR053_K4_CAMPAIGN", "0")
    if flag not in {"0", "1"}:
        raise ValueError("DUR053_K4_CAMPAIGN must be 0 or 1")
    return flag == "1"


def persistence_failure() -> NoReturn:
    # Exceptions from daemon threads, Kafka listeners and heartbeat loops can be
    # swallowed. A process exit stops every slot, identically in both arms.
    os._exit(70)


def encoded(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def sync_directory(directory: Path) -> None:
    # Windows cannot open a directory for fsync. Campaign deployments are Linux.
    if os.name != "nt":
        fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


class CampaignWitness:
    def __init__(self, directory: str, pod_name: str, pod_uid: str, container_id: str) -> None:
        if not directory or not pod_uid or not container_id:
            raise ValueError("K4 witness requires directory, POD_UID and CONTAINER_ID")
        self.directory = Path(directory)
        self.session = str(uuid.uuid4())
        self.pod_name, self.pod_uid, self.container_id = pod_name, pod_uid, container_id
        self.path = self.directory / f"{self.session}.jsonl"
        self.trace_path = self.directory / f"{self.session}.trace.jsonl"
        self.seal_path = self.directory / f"{self.session}.inventory.json"
        self.lock = threading.Lock()
        self.acknowledged: list[str] = []
        self.streams: dict[str, dict[str, Any]] = {
            "invocations": {"file": self.path.name, "bytes": 0, "rows": 0, "sha256": ""},
            "trace": {"file": self.trace_path.name, "bytes": 0, "rows": 0, "sha256": ""},
        }
        self.digests = {key: hashlib.sha256() for key in self.streams}
        for key, digest in self.digests.items():
            self.streams[key]["sha256"] = digest.hexdigest()
        self.trace("session", {})

    def _inventory(self) -> dict[str, Any]:
        return {
            "schema": "dur053.k4-session-seal.v1",
            "session": self.session,
            "pod_name": self.pod_name,
            "pod_uid": self.pod_uid,
            "container_id": self.container_id,
            "acknowledged": list(self.acknowledged),
            "complete": True,
            "errors": 0,
            "streams": self.streams,
        }

    def inventory(self) -> dict[str, Any]:
        with self.lock:
            return cast(dict[str, Any], json.loads(json.dumps(self._inventory())))

    def _append(self, stream: str, row: dict[str, Any], acknowledgment: str = "") -> None:
        with self.lock:
            try:
                data = encoded(row)
                entry = self.streams[stream]
                with (self.directory / str(entry["file"])).open("ab", buffering=0) as handle:
                    if handle.write(data) != len(data):
                        raise OSError("short K4 witness write")
                    os.fsync(handle.fileno())
                self.digests[stream].update(data)
                entry["bytes"] = int(entry["bytes"]) + len(data)
                entry["rows"] = int(entry["rows"]) + 1
                entry["sha256"] = self.digests[stream].hexdigest()
                if acknowledgment:
                    self.acknowledged.append(acknowledgment)
                temporary = self.seal_path.with_suffix(".pending")
                with temporary.open("wb", buffering=0) as handle:
                    seal = encoded(self._inventory())
                    if handle.write(seal) != len(seal):
                        raise OSError("short K4 inventory write")
                    os.fsync(handle.fileno())
                os.replace(temporary, self.seal_path)
                sync_directory(self.directory)
            except Exception:
                persistence_failure()

    def begin(self, fields: dict[str, Any]) -> dict[str, Any]:
        invocation: dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "identity": {
                "namespace": os.environ["RUNTIME_NAMESPACE"],
                "workflow": fields["workflow_id"],
                "node": fields["node_id"],
                "iteration": fields["iteration"],
                "attempt": fields["attempt_number"],
                "token_hash": hashlib.sha256(fields["claim_token"].encode()).hexdigest(),
            },
        }
        self._append(
            "invocations",
            {**invocation, "event": "start", "at": time.monotonic()},
            invocation["id"],
        )
        hold = os.getenv("DUR053_K4_HOLD_FILE", "")
        deadline = time.monotonic() + 60
        while hold and Path(hold).exists() and time.monotonic() < deadline:
            try:
                expired = time.time() >= Path(hold).stat().st_mtime + 60
            except FileNotFoundError:
                break
            if expired:
                self.trace("hold_bound_expired", {"hold": hold})
                break
            time.sleep(0.01)
        return invocation

    def end(self, invocation: dict[str, Any], outcome: str) -> None:
        self._append("invocations", {**invocation, "event": "finish", "at": time.monotonic()})
        self.trace("outcome", {"invocation": invocation["id"], "outcome": outcome})

    def trace(self, event: str, fields: dict[str, Any]) -> None:
        # Only explicitly selected metadata reaches this method; never bodies,
        # headers, response payloads or arbitrary error messages (may hold tokens).
        self._append(
            "trace",
            {
                **fields,
                "event": event,
                "session": self.session,
                "pod_uid": self.pod_uid,
                "container_id": self.container_id,
                "at": time.monotonic(),
                "timestamp": datetime.now(UTC).isoformat(),
            },
        )


def read_sealed_session(path: Path) -> dict[str, Any]:
    """Read-only collector primitive. No request to a living worker is needed."""
    inventory = json.loads(path.read_bytes())
    streams = {}
    for name, entry in inventory["streams"].items():
        filename = entry["file"]
        if Path(filename).name != filename:
            raise ValueError("unsafe K4 witness filename")
        raw = (path.parent / filename).read_bytes() if (path.parent / filename).exists() else b""
        prefix = raw[: entry["bytes"]]
        if (
            len(prefix) != entry["bytes"]
            or hashlib.sha256(prefix).hexdigest() != entry["sha256"]
            or len(prefix.splitlines()) != entry["rows"]
        ):
            raise ValueError("K4 witness differs from durable session seal")
        streams[name] = {"data": prefix.decode(), "unacknowledged_tail": raw[len(prefix) :].hex()}
    starts = [
        json.loads(line)["id"]
        for line in streams["invocations"]["data"].splitlines()
        if json.loads(line)["event"] == "start"
    ]
    if starts != inventory["acknowledged"]:
        raise ValueError("K4 acknowledged starts differ from sealed ledger")
    return {"inventory": inventory, "streams": streams}
