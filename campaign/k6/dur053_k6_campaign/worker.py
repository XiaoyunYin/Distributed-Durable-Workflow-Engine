"""Authenticated worker gate and transfer observations for the campaign image."""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import re
import ssl
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from workers.k6_effect import EffectAdapter, digest, emit_observation, endpoint


class Hook:
    def __init__(self, actor: str = "worker") -> None:
        self.host, self.port = endpoint(os.environ["DUR053_K6_HOOK_URL"])
        self.context = ssl.create_default_context(cafile=os.environ["DUR053_K6_HOOK_CA_FILE"])
        self.context.minimum_version = ssl.TLSVersion.TLSv1_3
        self.token = os.environ["DUR053_K6_HOOK_TOKEN"]
        stat = Path("/proc/self/stat").read_text().rsplit(")", 1)[1].split()
        self.identity = {
            "actor": actor,
            "uid": os.environ["POD_UID"],
            "process_start_id": f"{os.getpid()}:{stat[19]}",
            "session": uuid.uuid4().hex,
        }
        if self.send({"event": "register"})[0].get("command") != "registered":
            raise ValueError("K6_HOOK_REGISTRATION")

    def send(self, message: dict, timeout: float = 2) -> tuple[dict, str]:
        raw = json.dumps({**message, "identity": self.identity}, separators=(",", ":")).encode()
        conn = http.client.HTTPSConnection(
            self.host, self.port, timeout=timeout, context=self.context
        )
        try:
            conn.request("POST", "/v1/hook", raw, {"Authorization": f"Bearer {self.token}"})
            response = conn.getresponse()
            if response.status != 200:
                raise ValueError("K6_HOOK_REFUSED")
            return json.loads(response.read(4096)), hashlib.sha256(raw).hexdigest()
        finally:
            conn.close()

    def hit(self, boundary: str, fields: dict) -> None:
        message = dict(
            event="boundary_reached",
            boundary=boundary,
            fields=fields,
            hook_id=uuid.uuid4().hex,
            nonce=uuid.uuid4().hex,
        )
        ack, digest_of_message = self.send(message)
        if ack.get("command") == "pass":
            return
        now = time.time_ns() // 1_000_000
        until = ack.get("until_ms", 0)
        if (
            ack.get("command") != "ack"
            or not ack.get("arm")
            or ack.get("sequence", 0) <= 0
            or ack.get("sha256") != digest_of_message
            or not re.fullmatch(r"[0-9a-f]{64}", ack.get("journal_sha256", ""))
            or not now < until <= now + 60_000
        ):
            raise ValueError("K6_HOOK_ACK")
        binding = {key: ack[key] for key in ("arm", "sequence", "sha256", "journal_sha256")}
        message.update(binding, event="held")
        deadline = time.monotonic() + (until - now) / 1000
        while (remaining := deadline - time.monotonic()) > 0:
            decision, _ = self.send(message, min(2, remaining))
            if (
                any(decision.get(k) != value for k, value in binding.items())
                or decision.get("until_ms") != until
                or time.monotonic() >= deadline
            ):
                break
            if decision["command"] == "release":
                remaining = deadline - time.monotonic()
                if (
                    remaining <= 0
                    or self.send({**message, "event": "released"}, min(2, remaining))[0].get(
                        "command"
                    )
                    != "done"
                ):
                    raise ValueError("K6_HOOK_FINISH")
                return
            if decision["command"] != "wait":
                raise ValueError("K6_HOOK_COMMAND")
            time.sleep(max(0, min(0.075, deadline - time.monotonic())))
        raise ValueError("K6_HOOK_EXPIRED")


class Adapter(EffectAdapter):
    @classmethod
    def factory(cls) -> Callable[[], EffectAdapter]:
        hook = Hook()
        identities = json.loads(os.environ["DUR053_K6_HOOK_WORKLOAD"])

        def new() -> EffectAdapter:
            return cls(hook, identities)

        return new

    def __init__(self, hook: Hook, identities: dict[str, dict[str, str]]) -> None:
        super().__init__(
            os.environ["DUR053_K6_RELAY_URL"],
            os.environ["DUR053_K6_RELAY_CA_FILE"],
            os.environ["DUR053_K6_SINK_TOKEN_FILE"],
        )
        self.hook, self.identities = hook, identities

    def identifier(self, scope: dict[str, Any], phase: str) -> str:
        planned = self.identities.get(scope["logical_key"], {}).get(phase)
        if scope["attempt"] == 1 and planned is not None:
            return str(uuid.UUID(planned))
        return super().identifier(scope, phase)

    def observe(self, **data: str | int) -> None:
        data.update(self.hook.identity, at_ns=time.time_ns())
        emit_observation("k6_transfer " + json.dumps(data, separators=(",", ":")))

    def transfer(
        self,
        stage: str,
        operation: str,
        scope: dict[str, Any],
        request: dict[str, Any],
        invocation: str,
        wire: bytes,
    ) -> None:
        if stage == "pre-send" and operation == "apply":
            try:
                self.hook.hit(
                    "worker-pre-send",
                    {
                        "key": scope["logical_key"],
                        "invocation": invocation,
                        "request": request["request_id"],
                        "intent": scope["intent_id"],
                        "resource": scope["resource_id"],
                        "scope": scope["scope_hash"],
                    },
                )
            except (OSError, ValueError, http.client.HTTPException):
                self.observe(stage="hook_failed", invocation=digest(invocation))
                raise
        if stage != "pre-send":
            self.observe(
                stage=stage,
                operation=operation,
                key=digest(scope["logical_key"]),
                invocation=digest(invocation),
                request=digest(request["request_id"]),
                body=hashlib.sha256(wire).hexdigest(),
                attempt=scope["attempt"],
            )

    def result_observed(self, scope: dict[str, Any], invocation: str, state: str) -> None:
        self.observe(
            stage=state,
            key=digest(scope["logical_key"]),
            invocation=digest(invocation),
            attempt=scope["attempt"],
        )
