"""Exercise the Phase C sampler while PostgreSQL waits for a TCP password."""

from __future__ import annotations

import os
import socket
import struct
import sys
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path
from threading import Event
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dur053_k6_c_deploy as deployment  # noqa: E402
from dur053_k6_b_evidence import SmokeFailure  # noqa: E402
from dur053_k6_b_smoke import ENGINE_ROLE  # noqa: E402
from dur053_k6_c_smoke import PhaseCEvidence, Trace, is_starting  # noqa: E402

CLIENT_APP = "dur053-k6-c-fixture"
MONITOR_APP = "dur053-k6-c-monitor"
HOLD_SECONDS = 0.65


class FixtureDeployment(deployment.Deployment):
    def __init__(self, evidence: PhaseCEvidence) -> None:
        self.evidence = evidence
        self.deadline = datetime.now(UTC) + timedelta(seconds=20)
        self.env = dict(os.environ)
        self.first_sample = Event()

    def k(self, stage: str, *args: str, **kwargs: Any) -> bytes:
        # The fixture's TCP client runs on the host.
        return b'{"items":[]}'

    @contextmanager
    def trace_session(
        self, server: str, role: str, query: str
    ) -> Iterator[Callable[[float], bytes]]:
        with super().trace_session(server, role, query) as read:

            def signal_first_sample(timeout: float) -> bytes:
                data = read(timeout)
                self.first_sample.set()
                return data

            yield signal_first_sample


@pytest.fixture
def postgres(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> tuple[FixtureDeployment, int, str]:
    settings = [
        os.getenv(key)
        for key in ("DUR053_PHASE_C_CONTAINER", "DUR053_PHASE_C_PORT", "POSTGRES_PASSWORD")
    ]
    container, port, password = settings
    if not (container and port and password):
        if os.getenv("DUR053_PHASE_C_FIXTURE_REQUIRED") == "1":
            pytest.fail("isolated Phase C PostgreSQL endpoint is missing")
        pytest.skip("requires isolated hosted Phase C PostgreSQL")
    monkeypatch.setattr(deployment, "ENGINE_CONTAINER", container)
    output = os.getenv("DUR053_PHASE_C_STARTUP_OUTPUT")
    root = Path(output) / request.node.name if output else tmp_path
    root.mkdir(parents=True, exist_ok=True)
    evidence = PhaseCEvidence(root)
    evidence.secret(password)
    return FixtureDeployment(evidence), int(port), password


def recv_exact(peer: socket.socket, length: int) -> bytes:
    result = bytearray()
    while len(result) < length:
        chunk = peer.recv(length - len(result))
        assert chunk, "PostgreSQL closed the connection before the expected protocol response"
        result.extend(chunk)
    return bytes(result)


def message(peer: socket.socket) -> tuple[bytes, bytes]:
    kind = recv_exact(peer, 1)
    length = int.from_bytes(recv_exact(peer, 4), "big")
    return kind, recv_exact(peer, length - 4)


def release_client(
    peer: socket.socket, first_sample: Event, password: str, authenticate: bool
) -> int | None:
    assert first_sample.wait(timeout=9), "production sampler did not take the initial observation"
    time.sleep(HOLD_SECONDS)
    if not authenticate:
        peer.close()
        return None
    response = password.encode() + b"\0"
    peer.sendall(b"p" + struct.pack("!I", len(response) + 4) + response)
    assert message(peer) == (b"R", bytes(4)), "PostgreSQL did not accept the fixture password"
    pid = None
    while True:
        kind, payload = message(peer)
        if kind == b"K":
            pid = int.from_bytes(payload[:4], "big")
        elif kind == b"Z":
            assert pid is not None, "authenticated connection never supplied a backend PID"
            return pid
        else:
            assert kind == b"S", "unexpected PostgreSQL authentication response"


def observe(
    postgres: tuple[FixtureDeployment, int, str], *, authenticate: bool
) -> tuple[Trace, dict[str, Any]]:
    smoke, port, password = postgres
    version = int(smoke.psql("engine", "monitor", "SHOW server_version_num;"))
    assert version == 180006, "fixture requires the locked PostgreSQL 18.6"
    trace = Trace(smoke)
    trace.rows.append({"time": datetime.now(UTC).isoformat()})
    startup = struct.pack("!I", 196608) + (
        f"user\0{ENGINE_ROLE}\0database\0postgres\0application_name\0{CLIENT_APP}\0\0".encode()
    )
    outcome = "resolved"
    try:
        with (
            socket.create_connection(("127.0.0.1", port), timeout=5) as peer,
            ThreadPoolExecutor(max_workers=3) as executor,
        ):
            peer.sendall(struct.pack("!I", len(startup) + 4) + startup)
            assert message(peer) == (b"R", b"\0\0\0\x03"), "TCP password challenge is required"
            released = executor.submit(
                release_client, peer, smoke.first_sample, password, authenticate
            )
            try:
                trace.sample_server("engine", "monitor", 0, executor)
            except SmokeFailure as error:
                outcome = str(error)
            pid = released.result()
    finally:
        trace.finish()
    sample = trace.rows[0].get("engine", {})
    samples = [sample, *sample.get("followups", [])]
    times = [row["captured_at_us"] for row in samples if "captured_at_us" in row]
    gaps = [current - previous for previous, current in pairwise(times)]
    report: dict[str, Any] = {
        "server_version_num": version,
        "password_hold_seconds": HOLD_SECONDS,
        "registered_user": ENGINE_ROLE,
        "registered_application": CLIENT_APP,
        "expected_sampler_application": MONITOR_APP,
        "sampler_pids": [row["sampler_pid"] for row in samples if "sampler_pid" in row],
        "authenticated_pid": pid,
        "outcome": outcome,
        "gaps_us": gaps,
        "max_gap_us": max(gaps, default=None),
        "elapsed_us": times[-1] - times[0] if times else None,
    }
    smoke.evidence.write("startup-fixture.json", report)
    print(
        f"PostgreSQL 18.6 {outcome}: max_gap_us={report['max_gap_us']} "
        f"elapsed_us={report['elapsed_us']}"
    )
    assert len(trace.startups) == 1, "live PostgreSQL did not expose the expected starting backend"
    first = trace.startups[0]["observations"][0]["backends"]
    assert len(first) == 1
    row = first[0]
    assert row["usename"] is None and row["application_name"] == ""
    assert is_starting(row["usename"], row["application_name"])
    assert row["state"] == "starting"
    assert (row["wait_event_type"], row["wait_event"]) == ("Client", "ClientRead")
    assert datetime.fromisoformat(row["backend_start"]).tzinfo is not None
    assert row["pid"] > 0
    assert len(set(report["sampler_pids"])) == 1
    assert all([ENGINE_ROLE, MONITOR_APP, 1] in item["clients"] for item in samples)
    assert all(0 < gap <= 200_000 for gap in gaps)
    return trace, report


def test_phase_c_postgres_startup_resolves_in_one_session(
    postgres: tuple[FixtureDeployment, int, str],
) -> None:
    evidence = postgres[0].evidence
    with evidence.step("resolve-withheld-password"):
        trace, report = observe(postgres, authenticate=True)
        assert report["outcome"] == "resolved"
        assert 500_000 <= report["elapsed_us"] <= 2_000_000
        assert trace.rejections == []
        observations = trace.startups[0]["observations"]
        first = observations[0]["backends"][0]
        last = observations[-1]["backends"][0]
        assert first["pid"] == last["pid"] == report["authenticated_pid"]
        assert first["backend_start"] == last["backend_start"]
        assert (last["usename"], last["application_name"]) == (ENGINE_ROLE, CLIENT_APP)
        evidence.report.update(status="PASS", reason="FIXTURE_COMPLETE")


def test_phase_c_postgres_startup_vanishes_before_password(
    postgres: tuple[FixtureDeployment, int, str],
) -> None:
    evidence = postgres[0].evidence
    with evidence.step("reject-unauthenticated-disconnect"):
        trace, report = observe(postgres, authenticate=False)
        assert report["outcome"] == "DB_STARTUP_VANISHED_ENGINE"
        assert 500_000 <= report["elapsed_us"] <= 2_000_000
        assert [entry["reason"] for entry in trace.rejections] == ["DB_STARTUP_VANISHED_ENGINE"]
        assert trace.startups[0]["observations"][-1]["backends"] == []
        evidence.report.update(status="PASS", reason="FIXTURE_COMPLETE")
