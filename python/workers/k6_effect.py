"""Execute the authenticated K6 claim envelope through the private effect relay."""

from __future__ import annotations

import hashlib
import http.client
import json
import logging
import os
import re
import ssl
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from workers import k6_evidence
from workers.control import Claim, ControlError
from workers.registry import K6_ACTIVITY, K6_EFFECT_CLASS

if TYPE_CHECKING:
    from workers.runner import ActivityTask, WorkerControl

LOG = logging.getLogger(__name__)
OBSERVATION_PREFIX = "DUR053_K6_OBSERVATION "
PRE_CALL_SECONDS = 5
SIGNATURE_HEADER = "X-Dur053-K6-Receipt-Signature"
VERSION_HEADER = "X-Dur053-K6-Receipt-Version"
REQUEST_FIELDS = (
    "intent_id",
    "grant_token",
    "workflow_id",
    "node_id",
    "iteration",
    "logical_key",
    "scope_hash",
    "resource_id",
    "expected_revision",
    "arguments",
    "attempt",
    "fence",
)
RECEIPT_IDENTITY = (
    "workflow_id",
    "logical_key",
    "resource_id",
    "canonical_hash",
    "scope_hash",
    "intent_id",
)


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def canonical_hash(arguments: Any) -> str:
    # Match Go encoding/json's compact, key-sorted representation including HTML
    # escapes. K6's sealed argument domain permits only exactly representable integers.
    canonical = json.dumps(arguments, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    for char in "<>&\u2028\u2029":
        canonical = canonical.replace(char, f"\\u{ord(char):04x}")
    return digest(canonical)


def identifier(value: Any) -> bool:
    return (
        isinstance(value, str)
        and 0 < len(value.encode()) <= 512
        and all(" " < char != "\x7f" for char in value)
    )


def envelope(task: ActivityTask, claim: Claim) -> dict[str, Any]:
    raw = claim.k6_dispatch
    try:
        if (
            claim.effect_class != K6_EFFECT_CLASS
            or not isinstance(raw, dict)
            or set(raw) != {*REQUEST_FIELDS, "canonical_hash", "lookup_first"}
            or not claim.claim_token
            or raw["workflow_id"] != task.workflow_id
            or raw["node_id"] != task.node_id
            or raw["node_id"] != K6_ACTIVITY
            or type(raw["iteration"]) is not int
            or raw["iteration"] != task.iteration
            or not 0 <= raw["iteration"] < 2**31
            or type(raw["attempt"]) is not int
            or not 0 < raw["attempt"] < 2**63
            or raw["attempt"] != claim.attempt_number
            or type(raw["fence"]) is not int
            or not 0 < raw["fence"] < 2**63
            or raw["lookup_first"] is not (raw["attempt"] > 1)
            or not all(identifier(raw[key]) for key in RECEIPT_IDENTITY)
            or str(uuid.UUID(raw["intent_id"])) != raw["intent_id"]
            or not identifier(raw["grant_token"])
            or not isinstance(raw["expected_revision"], str)
            or not re.fullmatch(r"0|[1-9][0-9]{0,18}", raw["expected_revision"])
            or int(raw["expected_revision"]) >= 2**63 - 1
            or canonical_hash(raw["arguments"]) != raw["canonical_hash"]
        ):
            raise ValueError
        revision, key, target = raw["expected_revision"], raw["logical_key"], raw["resource_id"]
        proposal = digest("\0".join((target, raw["canonical_hash"], revision)))
        if raw["scope_hash"] != digest("\0".join((proposal, revision, key, target))):
            raise ValueError
        return raw
    except (TypeError, ValueError, RecursionError):
        raise ControlError(403, "K6_ENVELOPE_INVALID", "invalid K6 claim envelope") from None


def endpoint(url: str) -> tuple[str, int]:
    try:
        parsed = urlsplit(url)
        if (
            parsed.scheme == "https"
            and parsed.hostname
            and parsed.username is None
            and url == f"https://{parsed.netloc}"
            and (parsed.port is None or parsed.port > 0)
        ):
            return parsed.hostname, parsed.port or 443
    except ValueError:
        pass
    raise ControlError(503, "K6_TLS_CONFIG", "invalid K6 HTTPS endpoint")


def observation(key_hash: str, stage: str, started: int) -> None:
    emit_observation(f"k6_timing {key_hash} {stage} {time.monotonic_ns() - started}")


def emit_observation(message: str) -> None:
    print(f"{OBSERVATION_PREFIX}{message}", flush=True)


class EffectAdapter:
    def __init__(self, relay_url: str, ca_file: str, token_file: str) -> None:
        self.host, self.port = endpoint(relay_url)
        if not ca_file or not token_file:
            raise ControlError(503, "K6_TLS_CONFIG", "missing K6 relay configuration")
        self.ca_file = ca_file
        self.token_file = Path(token_file)

    @classmethod
    def configured(cls) -> EffectAdapter:
        return cls(
            os.getenv("DUR053_K6_RELAY_URL", ""),
            os.getenv("DUR053_K6_RELAY_CA_FILE", ""),
            os.getenv("DUR053_K6_SINK_TOKEN_FILE", ""),
        )

    def identifier(self, scope: dict[str, Any], phase: str) -> str:
        return str(uuid.uuid4())

    def transfer(
        self,
        stage: str,
        operation: str,
        scope: dict[str, Any],
        request: dict[str, Any],
        invocation: str,
        wire: bytes,
    ) -> None:
        pass

    def result_observed(self, scope: dict[str, Any], invocation: str, state: str) -> None:
        pass

    def exchange(
        self, operation: str, scope: dict[str, Any], request: dict[str, Any], invocation: str
    ) -> tuple[dict[str, Any] | None, bool]:
        """Return proof or a confirmed lookup miss. All other replies are uncertain."""
        started = time.monotonic_ns()
        wire = b""
        proof = k6_evidence.active.get()
        traffic = None
        observed_end = False
        try:
            context = ssl.create_default_context(cafile=self.ca_file)
            context.minimum_version = ssl.TLSVersion.TLSv1_3
            token = self.token_file.read_text(encoding="ascii").strip()
            if not token or any(char.isspace() for char in token):
                return None, False
            connection = http.client.HTTPSConnection(
                self.host, port=self.port, timeout=12, context=context
            )
            try:
                wire = json.dumps(request, separators=(",", ":"), allow_nan=False).encode()
                if proof is not None:
                    traffic = k6_evidence.transfer(operation, request, invocation, wire)
                    proof[0].append("send", invocation, traffic=traffic)
                self.transfer("pre-send", operation, scope, request, invocation, wire)
                connection.request(
                    "POST",
                    f"/v1/{operation}",
                    body=wire,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Connection": "close",
                        "Content-Type": "application/json",
                        "X-Dur053-Invocation": invocation,
                    },
                )
                self.transfer("sent", operation, scope, request, invocation, wire)
                reply = connection.getresponse()
                body = reply.read(8193)
                if proof is not None and traffic is not None:
                    proof[0].append(
                        "receive", invocation, traffic=traffic, response=k6_evidence.wire(body)
                    )
                    observed_end = True
                emit_observation(
                    f"k6_exchange {digest(str(request['request_id']))} {digest(invocation)} "
                    f"{hashlib.sha256(wire).hexdigest()} {hashlib.sha256(body).hexdigest()} "
                    f"{len(body)} {reply.status}"
                )
                if operation == "lookup" and reply.status == 404 and body == b"K6_SINK_MISSING\n":
                    return None, True
                signature = reply.getheader(SIGNATURE_HEADER)
                if (
                    reply.status != 200
                    or reply.getheader(VERSION_HEADER) != "1"
                    or not signature
                    or not re.fullmatch(r"[A-Za-z0-9_-]{86}", signature)
                    or len(body) > 8192
                ):
                    return None, False
                receipt = json.loads(body)
                revision = int(scope["expected_revision"])
                if (
                    not isinstance(receipt, dict)
                    or set(receipt)
                    != {
                        *RECEIPT_IDENTITY,
                        "expected_revision",
                        "applied_revision",
                        "application_id",
                    }
                    or any(receipt[field] != scope[field] for field in RECEIPT_IDENTITY)
                    or type(receipt["expected_revision"]) is not int
                    or receipt["expected_revision"] != revision
                    or type(receipt["applied_revision"]) is not int
                    or receipt["applied_revision"] != revision + 1
                    or not identifier(receipt["application_id"])
                ):
                    return None, False
                return {"operation": operation, "receipt": receipt, "signature": signature}, False
            finally:
                connection.close()
        except TimeoutError:
            if proof is not None and traffic is not None and not observed_end:
                proof[0].append("timeout", invocation, traffic=traffic)
                observed_end = True
            self.transfer("timeout", operation, scope, request, invocation, wire)
            return None, False
        except (OSError, ValueError, http.client.HTTPException, RecursionError):
            return None, False
        finally:
            if proof is not None and traffic is not None and not observed_end:
                proof[0].append("close", invocation, traffic=traffic)
            observation(digest(scope["logical_key"]), operation, started)

    def run(
        self,
        task: ActivityTask,
        claim: Claim,
        control: WorkerControl,
        ensure_current: Callable[[], None],
        claim_started: int,
    ) -> dict[str, Any]:
        scope = envelope(task, claim)
        observation(digest(scope["logical_key"]), "claim", claim_started)
        observation_context = k6_evidence.active.get()
        invocation = (
            self.identifier(scope, "invocation")
            if observation_context is None
            else observation_context[1]
        )
        request = {field: scope[field] for field in REQUEST_FIELDS}
        request["message_id"] = invocation

        def exchange(operation: str) -> tuple[dict[str, Any] | None, bool]:
            ensure_current()
            request["request_id"] = self.identifier(scope, operation)
            return self.exchange(operation, scope, request, invocation)

        def report(state: str, payload: Any) -> dict[str, Any]:
            ensure_current()
            try:
                result = control.result(
                    task.workflow_id,
                    task.node_id,
                    task.iteration,
                    claim.attempt_number,
                    claim.claim_token,
                    state,
                    payload,
                )
                self.result_observed(scope, invocation, state)
                return result
            except ControlError as error:
                # Keep the server's fixed disposition code, without its body or exception context.
                raise ControlError(
                    error.status, error.code, "K6 result rejected", "result"
                ) from None

        before_delay = time.monotonic_ns()
        time.sleep(PRE_CALL_SECONDS)
        observation(digest(scope["logical_key"]), "delay", before_delay)
        if scope["lookup_first"]:
            proof, missing = exchange("lookup")
            if proof is not None:
                return report("SUCCEEDED", proof)
            if not missing:
                return report("OUTCOME_UNKNOWN", None)
        proof, _ = exchange("apply")
        if proof is not None:
            try:
                return report("SUCCEEDED", proof)
            except ControlError as error:
                if (error.status, error.code) != (403, "INVALID_GRANT"):
                    raise
                # A signed-looking response can still fail engine verification.
                # Keep the application uncertain and use only same-key recovery.
        unknown = report("OUTCOME_UNKNOWN", None)
        proof, _ = exchange("lookup")
        return report("SUCCEEDED", proof) if proof is not None else unknown
