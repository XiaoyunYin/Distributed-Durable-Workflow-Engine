"""Environment-configured client for named fault-controller barriers."""

from __future__ import annotations

import json
import os
import socket
from typing import cast


def _object_from_json(line: bytes) -> dict[str, object] | None:
    value = json.loads(line.decode("utf-8"))
    if not isinstance(value, dict):
        return None
    return cast(dict[str, object], value)


class FailpointClient:
    """A small TCP client shared by Python activities and test fixtures."""

    def __init__(self, connection: socket.socket, token: str, seed: int) -> None:
        self._connection = connection
        self._token = token
        self._seed = seed

    @classmethod
    def from_environment(cls) -> FailpointClient | None:
        endpoint = os.getenv("DURABLE_FAULT_ENDPOINT")
        if not endpoint:
            return None
        token = os.getenv("DURABLE_FAULT_TOKEN")
        if not token:
            raise RuntimeError("DURABLE_FAULT_TOKEN is required with DURABLE_FAULT_ENDPOINT")
        seed_text = os.getenv("DURABLE_FAULT_SEED")
        if seed_text is None:
            raise RuntimeError("DURABLE_FAULT_SEED is required with DURABLE_FAULT_ENDPOINT")
        host, port_text = endpoint.rsplit(":", 1)
        connection = socket.create_connection((host, int(port_text)), timeout=5)
        connection.settimeout(None)
        return cls(connection, token, int(seed_text))

    def emit(self, event: str, boundary: str, fields: dict[str, object]) -> None:
        self._send(
            {
                "event": event,
                "boundary": boundary,
                "fields": fields,
            }
        )

    def hit(self, boundary: str, fields: dict[str, object]) -> None:
        """Report and hold at a named barrier until the controller releases it."""
        self.emit("boundary_reached", boundary, fields)
        response = self._read_line()
        if response is None:
            raise RuntimeError("fault controller disconnected before release")
        command = _object_from_json(response)
        if command is None or command.get("token") != self._token:
            raise RuntimeError("invalid fault-controller command")
        if command.get("command") != "release":
            raise RuntimeError(f"fault controller returned {command.get('command')!r}")

    def close(self) -> None:
        self._connection.close()

    def _send(self, payload: dict[str, object]) -> None:
        payload["token"] = self._token
        payload["seed"] = self._seed
        self._connection.sendall((json.dumps(payload, sort_keys=True) + "\n").encode("utf-8"))

    def _read_line(self) -> bytes | None:
        buffer = bytearray()
        while not buffer.endswith(b"\n"):
            chunk = self._connection.recv(4096)
            if not chunk:
                return None
            buffer.extend(chunk)
        return bytes(buffer)
