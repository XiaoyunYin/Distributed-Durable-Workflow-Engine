"""Small deterministic TCP proxy used for finite test-profile network cuts.

The proxy is intentionally local and bounded. It is a fault fixture, not a
production network-control mechanism: it records accepted connections, byte
forwarding, injected messages, and the observed cut separately from the
requested action.
"""

from __future__ import annotations

import contextlib
import socket
import threading
from dataclasses import dataclass


@dataclass(frozen=True)
class ProxyEvent:
    name: str
    direction: str = ""
    size: int = 0


class FaultProxy:
    """Forward one local TCP connection and support a finite cut window."""

    def __init__(self, target_host: str, target_port: int, seed: int) -> None:
        self.target_host = target_host
        self.target_port = target_port
        self.seed = seed
        self._server: socket.socket | None = None
        self._client: socket.socket | None = None
        self._upstream: socket.socket | None = None
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._events: list[ProxyEvent] = []
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def _record(self, event: ProxyEvent) -> None:
        with self._lock:
            self._events.append(event)

    @property
    def address(self) -> tuple[str, int]:
        server = self._server
        if server is None:
            raise RuntimeError("proxy has not started")
        value = server.getsockname()
        return str(value[0]), int(value[1])

    def start(self) -> None:
        if self._server is not None:
            raise RuntimeError("proxy already started")
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        server.settimeout(0.2)
        self._server = server
        self._thread = threading.Thread(target=self._accept, daemon=True)
        self._thread.start()

    def wait_connected(self, timeout_seconds: float = 5.0) -> bool:
        return self._ready.wait(timeout_seconds)

    def _accept(self) -> None:
        server = self._server
        if server is None:
            return
        while not self._stop.is_set():
            try:
                client, _ = server.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            try:
                upstream = socket.create_connection((self.target_host, self.target_port), timeout=5)
            except OSError:
                client.close()
                self._record(ProxyEvent("upstream_connect_failed"))
                return
            with self._lock:
                self._client = client
                self._upstream = upstream
            self._record(ProxyEvent("connected"))
            self._ready.set()
            threading.Thread(
                target=self._pump, args=(client, upstream, "client_to_target"), daemon=True
            ).start()
            threading.Thread(
                target=self._pump, args=(upstream, client, "target_to_client"), daemon=True
            ).start()
            return

    def _pump(self, source: socket.socket, destination: socket.socket, direction: str) -> None:
        try:
            while not self._stop.is_set():
                data = source.recv(4096)
                if not data:
                    return
                destination.sendall(data)
                self._record(ProxyEvent("forwarded", direction, len(data)))
        except OSError:
            if not self._stop.is_set():
                self._record(ProxyEvent("channel_error", direction))

    def inject(self, direction: str, payload: bytes) -> None:
        if direction not in {"client_to_target", "target_to_client"}:
            raise ValueError("direction must be client_to_target or target_to_client")
        with self._lock:
            destination = self._upstream if direction == "client_to_target" else self._client
        if destination is None:
            raise RuntimeError("proxy is not connected")
        destination.sendall(payload)
        self._record(ProxyEvent("injected", direction, len(payload)))

    def cut(self) -> None:
        self._record(ProxyEvent("cut_requested"))
        self._stop.set()
        with self._lock:
            sockets = (self._client, self._upstream)
        for connection in sockets:
            if connection is not None:
                with contextlib.suppress(OSError):
                    connection.shutdown(socket.SHUT_RDWR)
                with contextlib.suppress(OSError):
                    connection.close()
        self._record(ProxyEvent("cut_observed"))

    def events(self) -> list[ProxyEvent]:
        with self._lock:
            return list(self._events)

    def close(self) -> None:
        self.cut()
        server = self._server
        if server is not None:
            with contextlib.suppress(OSError):
                server.close()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=5)
