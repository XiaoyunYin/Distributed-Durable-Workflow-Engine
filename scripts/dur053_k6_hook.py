"""Independent host receiver for authenticated, durable K6 boundary holds."""

from __future__ import annotations

import hmac
import json
import os
import re
import ssl
import threading
import time
from contextlib import contextmanager
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import BinaryIO
from uuid import uuid4

ID = re.compile(r"[a-zA-Z0-9_.:-]{1,128}")
NONCE = re.compile(r"[0-9a-f]{32}")
HASH = re.compile(r"[0-9a-f]{64}")
SELECTOR = ("key", "invocation", "request")
FIELDS = {*SELECTOR, "intent", "resource", "scope", "binding", "reply", "application", "objects"}


def check(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def valid(value, pattern=ID) -> bool:
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def encode(row: dict) -> bytes:
    return json.dumps(row, separators=(",", ":"), sort_keys=True).encode()


class Receiver:
    def __init__(self, path: Path, tokens: dict[str, str], *, companion_boundary: str = "") -> None:
        check(
            bool(tokens)
            and len(set(tokens.values())) == len(tokens)
            and all(valid(actor) and valid(token, HASH) for actor, token in tokens.items()),
            "HOOK_ACTOR_CONFIG",
        )
        self.tokens = dict(tokens)
        check(companion_boundary == "" or valid(companion_boundary), "HOOK_COMPANION_BOUNDARY")
        self.companion_boundary = companion_boundary
        self.lock = threading.Lock()
        self.file: BinaryIO = os.fdopen(
            os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb", buffering=0
        )
        # An existing journal is never reopened for control. After ambiguous
        # host loss, retain its prefix and fail the episode.
        try:
            if os.name == "posix":
                parent = os.open(path.parent, os.O_DIRECTORY)
                try:
                    os.fsync(parent)
                finally:
                    os.close(parent)
        except OSError:
            self.file.close()
            raise
        self.rows: list[dict] = []
        self.arms: dict[str, dict] = {}
        self.targets: dict[str, dict] = {}
        self.nonces: set[str] = set()
        self.hooks: set[str] = set()
        self.failed = False

    def append(self, event: str, **fields) -> dict:
        check(not self.failed, "HOOK_JOURNAL_FAILED")
        row = {"sequence": len(self.rows) + 1, "event": event, "at_ns": time.time_ns(), **fields}
        raw = encode(row)
        try:
            if self.file.write(raw + b"\n") != len(raw) + 1:
                raise OSError("HOOK_JOURNAL_SHORT_WRITE")
            os.fsync(self.file.fileno())
        except OSError:
            self.failed = True
            raise
        self.rows.append(row)
        return {"sequence": row["sequence"], "sha256": sha256(raw).hexdigest()}

    def slot(self, episode: str, companion: bool) -> str:
        # The lease handoff can have one separately named successor hold. A
        # receiver configured for a single intervention has only the primary.
        return f"{episode}/companion" if companion else episode

    def arm(
        self,
        episode: str,
        boundary: str,
        target: dict,
        selector: dict,
        seconds: int,
        *,
        companion: bool = False,
    ) -> dict:
        with self.lock:
            check(valid(episode) and valid(boundary) and 0 < seconds <= 60, "HOOK_ARM_SHAPE")
            check(
                bool(selector) and all(valid(k) and valid(v) for k, v in selector.items()),
                "HOOK_SELECTOR",
            )
            check(target == self.targets.get(target.get("actor")), "HOOK_TARGET_STALE")
            slot = self.slot(episode, companion)
            check(
                not companion or boundary == self.companion_boundary and episode in self.arms,
                "HOOK_COMPANION_BOUNDARY",
            )
            check(slot not in self.arms, "HOOK_REPEATED_ARM")
            check(
                all(
                    a["target"] != target or a["boundary"] != boundary or a["selector"] != selector
                    for a in self.arms.values()
                ),
                "HOOK_REPLAYED_ARM",
            )
            arm_id = uuid4().hex
            until = now_ms() + seconds * 1000
            ack = self.append(
                "arm",
                episode=episode,
                companion=companion,
                boundary=boundary,
                target=target,
                selector=selector,
                arm=arm_id,
                until_ms=until,
            )
            arm = dict(
                episode=episode,
                companion=companion,
                boundary=boundary,
                target=target,
                selector=selector,
                arm=arm_id,
                until_ms=until,
                arm_ack=ack,
                state="armed",
            )
            self.arms[slot] = arm
            return dict(arm)

    def expire(self, arm: dict) -> bool:
        if (
            arm["state"] in {"armed", "reached", "held", "authorized", "issued"}
            and now_ms() >= arm["until_ms"]
        ):
            self.append("expired", arm=arm["arm"])
            arm["state"] = "expired"
        return arm["state"] == "expired"

    def observe(self, episode: str, *, companion: bool = False) -> dict:
        with self.lock:
            arm = self.arms[self.slot(episode, companion)]
            self.expire(arm)
            return dict(arm)

    def authorize(
        self, episode: str, ack: dict, command: str, *, evidence: dict, companion: bool = False
    ) -> None:
        with self.lock:
            arm = self.arms[self.slot(episode, companion)]
            check(
                not self.expire(arm) and arm["state"] == "held" and arm["ack"] == ack,
                "HOOK_NOT_ACKNOWLEDGED_HELD",
            )
            check(command in {"release", "duplicate", "drop", "killed"}, "HOOK_COMMAND")
            self.append("action", arm=arm["arm"], ack=ack, command=command, evidence=evidence)
            arm.update(state="done" if command == "killed" else "authorized", command=command)

    def note(self, episode: str, ack: dict, event: str, *, companion: bool = False, **data) -> None:
        with self.lock:
            arm = self.arms[self.slot(episode, companion)]
            check(
                not self.expire(arm) and arm["state"] == "held" and arm["ack"] == ack,
                "HOOK_NOT_ACKNOWLEDGED_HELD",
            )
            self.append(event, arm=arm["arm"], ack=ack, **data)

    def event(self, raw: bytes, authorization: str) -> dict:
        m = json.loads(raw)
        check(isinstance(m, dict) and isinstance(m.get("identity"), dict), "HOOK_IDENTITY")
        target = m["identity"]
        actor = target.get("actor")
        check(
            set(target) == {"actor", "uid", "process_start_id", "session"}
            and all(valid(v) for v in target.values())
            and valid(target["session"], NONCE)
            and valid(target["process_start_id"], re.compile(r"[1-9][0-9]*:[0-9]+")),
            "HOOK_IDENTITY",
        )
        token = self.tokens.get(actor)
        check(
            token is not None and hmac.compare_digest(authorization, f"Bearer {token}"), "HOOK_AUTH"
        )
        with self.lock:
            check(not self.failed, "HOOK_JOURNAL_FAILED")
            if m.get("event") == "register":
                check(
                    target != self.targets.get(actor)
                    and not any(
                        r.get("target", {}).get("session") == target["session"]
                        for r in self.rows
                        if r["event"] == "register"
                    ),
                    "HOOK_REPLAYED_REGISTRATION",
                )
                self.append("register", target=target)
                self.targets[actor] = target
                return {"command": "registered"}
            check(target == self.targets.get(actor), "HOOK_TARGET_STALE")
            boundary, fields, hook, nonce = (
                m.get(k) for k in ("boundary", "fields", "hook_id", "nonce")
            )
            check(
                valid(boundary)
                and valid(hook, NONCE)
                and valid(nonce, NONCE)
                and isinstance(fields, dict)
                and fields.keys() <= FIELDS,
                "HOOK_REACH_SHAPE",
            )
            for k, value in fields.items():
                if k == "objects":
                    check(
                        isinstance(value, dict)
                        and len(value) <= 16
                        and all(valid(a) and valid(b) for a, b in value.items()),
                        "HOOK_OBJECTS",
                    )
                else:
                    check(value == "" or valid(value), "HOOK_FIELDS")
            return self.hit(m, raw, target, fields)

    def hit(self, m: dict, raw: bytes, target: dict, fields: dict) -> dict:
        matches = [
            a
            for a in self.arms.values()
            if a["target"] == target
            and m["boundary"] == a["boundary"]
            and all(
                fields.get(k, fields.get("objects", {}).get(k)) == v
                for k, v in a["selector"].items()
            )
        ]
        check(len(matches) <= 1, "HOOK_AMBIGUOUS_ARM")
        arm = matches[0] if matches else None
        proof = {k: m[k] for k in ("boundary", "hook_id", "nonce", "fields")}
        if m.get("event") == "boundary_reached":
            check(
                m["nonce"] not in self.nonces and m["hook_id"] not in self.hooks,
                "HOOK_REPLAYED_NONCE",
            )
            self.nonces.add(m["nonce"])
            self.hooks.add(m["hook_id"])
            if arm is None:
                return {"command": "pass"}
            check(not self.expire(arm) and arm["state"] == "armed", "HOOK_ARM_CONSUMED")
            row = self.append(
                "reach",
                arm=arm["arm"],
                target=target,
                **proof,
                message_sha256=sha256(raw).hexdigest(),
            )
            ack = {
                "sequence": row["sequence"],
                "sha256": sha256(raw).hexdigest(),
                "journal_sha256": row["sha256"],
            }
            arm.update(state="reached", ack=ack, reach=proof)
            return {"command": "ack", "arm": arm["arm"], "until_ms": arm["until_ms"], **ack}
        check(arm is not None and not self.expire(arm), "HOOK_NO_LIVE_ARM")
        assert arm is not None
        check(
            arm["state"] in {"reached", "held", "authorized", "issued"}
            and arm["reach"] == proof
            and m.get("arm") == arm["arm"]
            and all(m.get(k) == v for k, v in arm["ack"].items()),
            "HOOK_ACK_MISMATCH",
        )
        if m.get("event") == "released":
            check(arm["state"] == "issued", "HOOK_RELEASE_ORDER")
            self.append("release", arm=arm["arm"], ack=arm["ack"])
            arm["state"] = "done"
            return {"command": "done"}
        check(m.get("event") == "held", "HOOK_EVENT")
        check(arm["state"] != "issued", "HOOK_COMMAND_ALREADY_ISSUED")
        if arm["state"] == "reached":
            self.append("held", arm=arm["arm"], ack=arm["ack"])
            arm["state"] = "held"
        if arm["state"] == "authorized":
            self.append("command_issued", arm=arm["arm"], ack=arm["ack"], command=arm["command"])
            arm["state"] = "issued"
        return {
            "command": arm.get("command", "wait"),
            "arm": arm["arm"],
            "until_ms": arm["until_ms"],
            **arm["ack"],
        }


@contextmanager
def serve(receiver: Receiver, address: str, cert: Path, key: Path):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args) -> None:
            pass

        def setup(self) -> None:
            self.request.settimeout(2)
            super().setup()

        def do_POST(self) -> None:
            try:
                size = self.headers.get_all("Content-Length", [])
                auth = self.headers.get_all("Authorization", [])
                check(
                    self.path == "/v1/hook"
                    and len(size) == len(auth) == 1
                    and 0 < int(size[0]) <= 8192,
                    "HOOK_HTTP",
                )
                body = encode(receiver.event(self.rfile.read(int(size[0])), auth[0]))
            except (ValueError, KeyError, TypeError, OSError):
                self.send_error(403, "K6_HOOK_REFUSED")
                return
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    with ThreadingHTTPServer((address, 0), Handler) as server:
        server.daemon_threads = False
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        context.load_cert_chain(cert, key)
        # Wrap accepted sockets in their handler threads so an incomplete TLS
        # handshake cannot block the listener or postpone another hold's ACK.
        get_request = server.get_request

        def accept():
            conn, peer = get_request()
            return context.wrap_socket(conn, server_side=True, do_handshake_on_connect=False), peer

        server.get_request = accept  # type: ignore[method-assign]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield server.server_port
        finally:
            server.shutdown()
            thread.join()
