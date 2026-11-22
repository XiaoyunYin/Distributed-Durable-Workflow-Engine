"""D2a worker tests at the Kafka delivery and signed sink response boundaries."""

from __future__ import annotations

import hashlib
import io
import json
import logging
import sys
import threading
import traceback
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dur053_k6_d_smoke as phase_d  # noqa: E402
from workers import k6_effect
from workers.control import ControlClient, ControlError
from workers.kafka_worker import RetryingControl, handle_delivery
from workers.registry import K6_ACTIVITY, K6_EFFECT_CLASS, K6_VERSION

GRANT = "80000000-0000-4000-8000-000000000003"
CLAIM = "sensitive-claim-token"
TOKEN = "projected-sink-audience-secret"
SIGNATURE = "A" * 86


def hash_text(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


class Delivery:
    def __init__(self, monkeypatch, tmp_path):
        self.events = []
        self.attempt = 1
        self.replies = {}
        self.payload = {
            "workflow_id": "workflow",
            "node_id": K6_ACTIVITY,
            "iteration": 0,
            "attempt_number": 1,
            "activity_name": K6_ACTIVITY,
            "activity_version": K6_VERSION,
            "input": {"resource_id": "forged-target", "logical_key": "forged-key"},
        }
        self.claim = {
            "intent_id": "a0000000-0000-4000-8000-000000000004",
            "grant_token": GRANT,
            "workflow_id": "workflow",
            "node_id": K6_ACTIVITY,
            "iteration": 0,
            "logical_key": "registered-key",
            "resource_id": "approved-target",
            "expected_revision": "0",
            "arguments": {"x": "<é>\u2028", "count": 1},
            "attempt": 1,
            "fence": 3,
            "lookup_first": False,
        }
        canonical = hash_text('{"count":1,"x":"\\u003cé\\u003e\\u2028"}')
        proposal = hash_text(f"approved-target\0{canonical}\x000")
        self.claim.update(
            canonical_hash=canonical,
            scope_hash=hash_text(f"{proposal}\x000\0registered-key\0approved-target"),
        )
        self.consumer = SimpleNamespace(commit=lambda offsets: None)
        self.message = SimpleNamespace(
            headers=[],
            value=json.dumps(self.payload["input"]).encode(),
            key=b"",
            topic="durable-agent.tasks.v1",
            partition=0,
            offset=1,
        )
        monkeypatch.delenv("DUR053_K6_ACTIVITY", raising=False)
        self.control = RetryingControl("https://control:8080", threading.Event())
        monkeypatch.setattr(self.control, "tls_opener", object())
        monkeypatch.setattr(self.control, "_post", self.post)
        monkeypatch.setenv("DUR053_K6_ACTIVITY", "1")
        monkeypatch.setenv("WORKER_TOKEN_FILE", str(tmp_path / "control-token"))
        monkeypatch.setenv("DUR053_K6_RELAY_URL", "https://effect-relay:8443")
        monkeypatch.setenv("DUR053_K6_RELAY_CA_FILE", str(tmp_path / "relay-ca"))
        path = tmp_path / "sink-token"
        path.write_text(TOKEN)
        monkeypatch.setenv("DUR053_K6_SINK_TOKEN_FILE", str(path))
        monkeypatch.setattr(k6_effect.ssl, "create_default_context", self.context)
        monkeypatch.setattr(k6_effect.http.client, "HTTPSConnection", self.connect)
        monkeypatch.setattr(
            k6_effect.time, "sleep", lambda seconds: self.events.append(("delay", seconds))
        )

    def context(self, *, cafile):
        assert cafile.endswith("relay-ca")
        return SimpleNamespace()

    def post(self, path, body):
        if path.endswith("/worker-deliveries"):
            return {"commit_offset": True, "next_offset": 2, "task": self.payload}
        if path.endswith("/claim"):
            assert body["expected_attempt"] == self.attempt
            self.events.append(("claim",))
            return {
                "attempt_number": self.attempt,
                "claim_token": CLAIM,
                "effect_class": K6_EFFECT_CLASS,
                "heartbeat_deadline": "test",
                "k6_dispatch": self.claim,
            }
        if path.endswith("/result"):
            assert body["claim_token"] == CLAIM
            self.events.append(("result", body["attempt_state"], body["payload"]))
            return {"Disposition": "PENDING_VERIFICATION"}
        pytest.fail("unexpected worker-control path")

    def connect(self, host, *, port, timeout, context):
        assert (host, port, timeout) == ("effect-relay", 8443, 12)
        self.events.append(("connect",))

        def request(method, path, *, body, headers):
            assert method == "POST" and headers["Authorization"] == f"Bearer {TOKEN}"
            assert headers["Connection"] == "close"
            operation = path.rsplit("/", 1)[-1]
            self.events.append(("request", operation, json.loads(body)))

        def response():
            operation = self.events[-1][1]
            outcome = self.replies.get(operation, "signed")
            if outcome == "timeout":
                raise TimeoutError(f"must never log {TOKEN} or {GRANT}")
            receipt = {field: self.claim[field] for field in k6_effect.RECEIPT_IDENTITY}
            receipt.update(application_id="application", expected_revision=0, applied_revision=1)
            body = b"K6_SINK_MISSING\n" if outcome == "miss" else json.dumps(receipt).encode()
            headers = {
                k6_effect.VERSION_HEADER: "1",
                k6_effect.SIGNATURE_HEADER: SIGNATURE if outcome == "signed" else None,
            }
            return SimpleNamespace(
                status=404 if outcome == "miss" else 200,
                read=lambda size: body,
                getheader=headers.get,
            )

        return SimpleNamespace(
            request=request,
            getresponse=response,
            close=lambda: self.events.append(("close",)),
        )

    def run(self):
        handle_delivery(self.consumer, self.message, self.control, "worker")

    def requests(self):
        return [row[1:] for row in self.events if row[0] == "request"]

    def results(self):
        return [row[1:] for row in self.events if row[0] == "result"]


@pytest.fixture
def delivery(monkeypatch, tmp_path):
    return Delivery(monkeypatch, tmp_path)


def test_kafka_uses_claim_envelope_instead_of_broker_target_and_key(delivery):
    delivery.run()
    [(operation, body)] = delivery.requests()
    assert operation == "apply"
    assert (body["resource_id"], body["logical_key"]) == ("approved-target", "registered-key"), (
        "broker supplied the effect target or key"
    )
    assert all(body[field] == delivery.claim[field] for field in k6_effect.REQUEST_FIELDS)
    assert not {"actor", "canonical_hash", "lookup_first"} & body.keys()


def test_waits_five_seconds_before_first_sink_request(delivery):
    delivery.run()
    assert [event[0] for event in delivery.events[:4]] == [
        "claim",
        "delay",
        "connect",
        "request",
    ], "registered pre-call delay was skipped"
    assert delivery.events[1] == ("delay", 5)


def test_success_requires_and_passes_detached_sink_signature(delivery):
    delivery.run()
    [(state, proof)] = delivery.results()
    assert state == "SUCCEEDED" and proof["operation"] == "apply"
    assert proof["signature"] == SIGNATURE and proof["receipt"]["application_id"] == "application"
    delivery.events.clear()
    delivery.replies.update(apply="unsigned", lookup="unsigned")
    delivery.run()
    assert delivery.results() == [("OUTCOME_UNKNOWN", None)], "unsigned sink response succeeded"


def test_uncertain_apply_reports_unknown_before_same_key_lookup(delivery, caplog, capsys):
    delivery.replies["apply"] = "timeout"
    with caplog.at_level(logging.INFO):
        delivery.run()
    assert [event[1] for event in delivery.events if event[0] in {"request", "result"}] == [
        "apply",
        "OUTCOME_UNKNOWN",
        "lookup",
        "SUCCEEDED",
    ], "uncertain apply was replayed instead of looked up"
    [(_, apply), (_, lookup)] = delivery.requests()
    assert apply["request_id"] != lookup.pop("request_id")
    apply.pop("request_id")
    assert apply == lookup
    assert delivery.results()[-1][1]["operation"] == "lookup"
    observations = capsys.readouterr().out
    assert f"DUR053_K6_OBSERVATION k6_timing {hash_text('registered-key')} lookup " in observations
    assert all(
        secret not in caplog.text + observations for secret in (CLAIM, GRANT, TOKEN, SIGNATURE)
    )


def test_observations_parse_with_worker_entrypoint_logging(delivery):
    output = StringIO()
    root = logging.getLogger()
    original_handlers, original_level = root.handlers[:], root.level
    root.handlers.clear()
    try:
        logging.basicConfig(level=logging.INFO)
        with redirect_stdout(output):
            delivery.run()
    finally:
        root.handlers.clear()
        root.handlers.extend(original_handlers)
        root.setLevel(original_level)
    prefixed = "".join(
        f"[pod/worker-0/worker] {line}" for line in output.getvalue().splitlines(keepends=True)
    )
    observations = phase_d.collect(prefixed, "", "", "")
    assert observations["worker"], "DUR053_K6_OBSERVATION missing exchange"
    assert observations["worker_timings"], "DUR053_K6_OBSERVATION missing timings"
    assert len(observations["worker"]) == 1
    assert {phase for _, phase, _ in observations["worker_timings"]} >= {"claim", "delay", "apply"}
    assert observations["matched_lines"]["worker"] == 4


def test_concurrent_observations_emit_complete_lines(monkeypatch):
    class Stdout(StringIO):
        def write(self, value: str) -> int:
            # Reject split writes even if scheduling happens to serialize print().
            assert value.endswith("\n") and value.count("\n") == 1, "partial observation write"
            return super().write(value)

        def flush(self) -> None:
            self.flushes += 1

        flushes = 0

    output = Stdout()
    monkeypatch.setattr(sys, "stdout", output)
    thread_count, per_thread = 32, 20
    start = threading.Barrier(thread_count)

    def emit(thread: int) -> None:
        start.wait(timeout=10)
        for number in range(thread * per_thread, (thread + 1) * per_thread):
            key = f"{number:064x}"
            k6_effect.emit_observation(f"k6_exchange {key} {key} {key} {key} {number} 200")
            k6_effect.emit_observation(f"k6_timing {key} apply {number}")

    with ThreadPoolExecutor(max_workers=thread_count) as pool:
        list(pool.map(emit, range(thread_count)))

    count = thread_count * per_thread
    log = output.getvalue()
    parsed = phase_d.collect(log, "", "", "")
    assert len(log.splitlines()) == output.flushes == parsed["matched_lines"]["worker"] == 2 * count
    assert sorted(
        (
            row["request_id_hash"],
            row["invocation_hash"],
            row["request_hash"],
            row["response_hash"],
            row["response_length"],
            row["status"],
        )
        for row in parsed["worker"]
    ) == [(f"{i:064x}", f"{i:064x}", f"{i:064x}", f"{i:064x}", i, 200) for i in range(count)]
    assert sorted(parsed["worker_timings"]) == [(f"{i:064x}", "apply", i) for i in range(count)]


def test_engine_rejected_apply_proof_is_recovered_only_by_lookup(delivery, monkeypatch):
    def post(path, body):
        reply = delivery.post(path, body)
        if path.endswith("/result") and (body["payload"] or {}).get("operation") == "apply":
            raise ControlError(403, "INVALID_GRANT", "unverified apply proof")
        return reply

    monkeypatch.setattr(delivery.control, "_post", post)
    delivery.run()
    assert [operation for operation, _ in delivery.requests()] == ["apply", "lookup"]
    assert delivery.results()[1] == ("OUTCOME_UNKNOWN", None)
    assert delivery.results()[2][1]["operation"] == "lookup"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("grant_token", None),
        ("resource_id", "forged-target"),
        ("logical_key", "forged-key"),
        ("workflow_id", "forged-workflow"),
        ("attempt", 2),
        ("fence", 0),
        ("arguments", "forged arguments"),
        ("lookup_first", True),
    ],
)
def test_refuses_missing_or_forged_claim_fields(delivery, field, value, caplog):
    if value is None:
        delivery.claim.pop(field)
    else:
        delivery.claim[field] = value
    with (
        caplog.at_level(logging.INFO),
        pytest.raises(ControlError, match="K6_ENVELOPE_INVALID") as info,
    ):
        delivery.run()
    assert delivery.events == [("claim",)]
    assert all(secret not in caplog.text + str(info.value) for secret in (CLAIM, GRANT, TOKEN))


def test_missing_envelope_and_opt_out_refuse_before_effect(delivery, monkeypatch):
    delivery.claim = None
    with pytest.raises(ControlError, match="K6_ENVELOPE_INVALID"):
        delivery.run()
    monkeypatch.delenv("DUR053_K6_ACTIVITY")
    with pytest.raises(ControlError, match="K6_ACTIVITY_DISABLED"):
        delivery.run()
    assert delivery.events == [("claim",)]


def test_private_control_error_excludes_untrusted_body_and_reason(monkeypatch, tmp_path):
    token = tmp_path / "worker-control-token"
    token.write_text(TOKEN)
    monkeypatch.setenv("WORKER_TOKEN_FILE", str(token))
    control = ControlClient("https://control:8080")

    def reject(request, **kwargs):
        body = json.dumps({"error": {"code": CLAIM, "message": GRANT}}).encode()
        raise urllib.error.HTTPError(request.full_url, 403, TOKEN, {}, io.BytesIO(body))

    monkeypatch.setattr(control, "tls_opener", SimpleNamespace(open=reject))
    with pytest.raises(ControlError, match="HTTP_ERROR") as error:
        control.claim("workflow", K6_ACTIVITY, 0, "worker", "request")
    assert all(
        value not in "".join(traceback.format_exception(error.value))
        for value in (GRANT, CLAIM, TOKEN)
    )


@pytest.mark.parametrize(
    ("outcome", "operations", "state"),
    [
        ("signed", ["lookup"], "SUCCEEDED"),
        ("miss", ["lookup", "apply"], "SUCCEEDED"),
        ("timeout", ["lookup"], "OUTCOME_UNKNOWN"),
    ],
)
def test_replacement_only_applies_after_confirmed_miss(delivery, outcome, operations, state):
    delivery.attempt = delivery.payload["attempt_number"] = 2
    delivery.claim.update(attempt=2, lookup_first=True)
    delivery.replies["lookup"] = outcome
    delivery.run()
    assert [operation for operation, _ in delivery.requests()] == operations
    assert delivery.results()[0][0] == state
