"""J4 fixture wire format and one-shot HTTP transport."""

from __future__ import annotations

import hashlib
import http.client
import io
import json
import re
import socket
import time
from collections.abc import Callable
from dataclasses import dataclass
from ipaddress import IPv4Address
from typing import Any

ACTIVITY = "j4.recorded-llm"
VERSION = "v1"
MODE = "J4_RECORDED_LLM_ACTIVITY"
STUB_PORT = 8082
STUB_PATH = "/v1/recorded-completions"
CALLER_UID = 65533
EXPECTED_HASH = "92b3bc136a6617117cfac28e4324b272b6bf6b72ab5d137a948cb9fc0d25e751"
MISMATCH = "RECORDED_REQUEST_MISMATCH"
MAX_BODY = 4096
MAX_IPC = 16384
IDENTITY = re.compile(r"[A-Za-z0-9._:-]{1,160}", re.ASCII)
Trace = Callable[[dict[str, Any]], None]


def strict_json(data: bytes) -> Any:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    return json.loads(data.decode("utf-8"), object_pairs_hook=unique)


def canonical_request(value: Any) -> bytes:
    if not isinstance(value, dict) or value.keys() != {"model", "prompt"}:
        raise ValueError("expected model and prompt strings")
    if any(not isinstance(s, str) or any(not " " <= c <= "~" for c in s) for s in value.values()):
        raise ValueError("fixture fields must contain only printable ASCII")
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")


@dataclass(frozen=True)
class CallIdentity:
    cell: str
    workflow_id: str
    node_id: str
    iteration: int
    attempt: int
    ordinal: int
    call_id: str

    def __post_init__(self) -> None:
        names = (self.cell, self.workflow_id, self.node_id, self.call_id)
        if any(not isinstance(s, str) or not IDENTITY.fullmatch(s) for s in names):
            raise ValueError("invalid fixture identity")
        if self.node_id != ACTIVITY or type(self.iteration) is not int or self.iteration != 0:
            raise ValueError("unexpected fixture step")
        if any(type(n) is not int or n <= 0 for n in (self.attempt, self.ordinal)):
            raise ValueError("invalid fixture ordinal")

    def headers(self) -> dict[str, str]:
        return {
            "X-J4-Cell": self.cell,
            "X-J4-Workflow": self.workflow_id,
            "X-J4-Node": self.node_id,
            "X-J4-Iteration": str(self.iteration),
            "X-J4-Attempt": str(self.attempt),
            "X-J4-Ordinal": str(self.ordinal),
            "X-J4-Call": self.call_id,
        }


@dataclass(frozen=True)
class ProviderReply:
    status: int
    observed_ns: int
    payload: Any = None
    retry_after: str | None = None


def request_hash(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def remaining(deadline_ns: int) -> float:
    seconds = (deadline_ns - time.monotonic_ns()) / 1_000_000_000
    if seconds <= 0:
        raise TimeoutError("fixture HTTP deadline")
    return seconds


class DeadlineReader(io.RawIOBase):
    def __init__(self, sock: socket.socket, deadline_ns: int) -> None:
        self.sock = sock
        self.deadline_ns = deadline_ns

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        while True:
            self.sock.settimeout(remaining(self.deadline_ns))
            try:
                return self.sock.recv_into(buffer)
            except TimeoutError:
                # A socket timeout can return one host timer tick early.
                # Keep reading this same connection until its absolute deadline.
                if time.monotonic_ns() >= self.deadline_ns:
                    raise

    def makefile(self, _mode: str) -> io.BufferedReader:
        return io.BufferedReader(self)


def request_once(
    host: IPv4Address, call: CallIdentity, body: bytes, deadline_ns: int, trace: Trace
) -> ProviderReply:
    if len(body) > MAX_BODY:
        raise ValueError("fixture request exceeds the wire limit")
    headers = {
        "Host": f"{host}:{STUB_PORT}",
        "Connection": "close",
        "Content-Type": "application/json",
        "Content-Length": str(len(body)),
        **call.headers(),
    }
    head = [f"POST {STUB_PATH} HTTP/1.1", *(f"{k}: {v}" for k, v in headers.items()), "", ""]
    wire = "\r\n".join(head).encode("ascii") + body
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(remaining(deadline_ns))
            sock.connect((str(host), STUB_PORT))
            sock.settimeout(remaining(deadline_ns))
            before = time.monotonic_ns()
            sent = sock.send(wire)
            if not sent:
                raise ConnectionError("fixture request did not send")
            trace({"event": "send", "before_ns": before, "at_ns": time.monotonic_ns()})
            sock.settimeout(remaining(deadline_ns))
            sock.sendall(wire[sent:])
            with http.client.HTTPResponse(DeadlineReader(sock, deadline_ns)) as response:
                response.begin()
                observed = time.monotonic_ns()
                remaining(deadline_ns)
                if response.status == 429:
                    dates = response.headers.get_all("Retry-After", [])
                    if len(dates) != 1:
                        raise ValueError("one Retry-After header required")
                    return ProviderReply(429, observed, retry_after=dates[0])
                if response.status not in {200, 422}:
                    raise ValueError(f"unexpected fixture response status {response.status}")
                if response.chunked or response.length is None or response.length > MAX_BODY:
                    raise ValueError("invalid fixture response length")
                payload = strict_json(response.read())
                remaining(deadline_ns)
                return ProviderReply(response.status, observed, payload=payload)
    except TimeoutError:
        observed = time.monotonic_ns()
        while observed < deadline_ns:
            time.sleep((deadline_ns - observed) / 1_000_000_000)
            observed = time.monotonic_ns()
        return ProviderReply(0, observed)
