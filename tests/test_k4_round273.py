"""Round 273 engineering reservation and bounded provenance retries."""

from __future__ import annotations

import io
import json
import re
import sys
import urllib.error
from email.message import Message
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import dur053_k3 as k3  # noqa: E402
import dur053_k4_driver as driver  # noqa: E402


def test_engineering_smoke_fits_reservation():
    # No static setup/overhead-probe bound exists for this engineering plan.
    # Round 273's complete nine-episode run took 2745 live seconds.
    observed_live_seconds = 2745
    closeout_reserve = 900
    assert driver.SMOKE_LIVE_RESERVATION - closeout_reserve >= 1.2 * observed_live_seconds, (
        "K4 smoke reservation lacks 20% headroom"
    )


def test_smoke_workflow_reservations_and_tokens():
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    jobs = dict(re.findall(r"(?ms)^  ([\w-]+):\n(.*?)(?=^  [\w-]+:\n|\Z)", workflow))
    assert "    timeout-minutes: 95\n" in jobs["dur053-k4-lifecycle-smoke"]
    for job, step in (("k3-package", "Run package-check"), ("k3-smoke", "Run smoke")):
        block = jobs[job]
        assert "    permissions:\n      contents: read\n" in block
        assert "actions: read" not in block
        body = block.split(f"      - name: {step}\n", 1)[1].split("      - name:", 1)[0]
        assert "          GH_TOKEN: ${{ github.token }}\n" in body
    assert "    timeout-minutes: 70\n" in jobs["k3-smoke"]
    assert k3.LIVE_RESERVATION == 3600


def http_error(status, body=b"", **headers):
    message = Message()
    for name, value in headers.items():
        message[name] = value
    return urllib.error.HTTPError(
        "https://api.github.com/test", status, "fixture", message, io.BytesIO(body)
    )


def fake_github(monkeypatch, responses):
    calls, sleeps = [], []

    def urlopen(request, *, timeout):
        calls.append((request, timeout))
        response = next(responses)
        if isinstance(response, Exception):
            raise response
        return io.BytesIO(json.dumps(response).encode())

    monkeypatch.setattr(k3.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(k3.time, "sleep", sleeps.append)
    monkeypatch.setattr(k3.time, "time", lambda: 1000)
    monkeypatch.setenv("GH_TOKEN", "fixture-token")
    return calls, sleeps


@pytest.mark.parametrize(
    "status,body,headers,delay",
    [
        (403, b"rate limit exceeded", {}, 60),
        (403, b"", {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1025"}, 25),
        (429, b"", {"Retry-After": "120"}, 60),
        (403, b"secondary rate limit", {"Retry-After": "7", "X-RateLimit-Reset": "1025"}, 7),
        (429, b"rate-limit", {"X-RateLimit-Reset": "999"}, 0),
        (403, b"rate limit", {"Retry-After": "invalid"}, 60),
    ],
)
def test_github_rate_limit_then_success(monkeypatch, status, body, headers, delay):
    calls, sleeps = fake_github(
        monkeypatch, iter([http_error(status, body, **headers), {"jobs": []}])
    )
    assert k3.github("test") == {"jobs": []}
    assert len(calls) == 2 and sleeps == [delay]
    assert all(
        request.get_header("Authorization") == "Bearer fixture-token" and timeout == 30
        for request, timeout in calls
    )


@pytest.mark.parametrize(
    "status,body",
    [
        (403, b"Resource not accessible by integration"),
        (429, b"unmarked"),
        (400, b"rate limit"),
        (500, b"rate limit"),
    ],
)
def test_github_other_errors_raise_immediately(monkeypatch, status, body):
    error = http_error(status, body)
    calls, sleeps = fake_github(monkeypatch, iter([error, {}]))
    with pytest.raises(urllib.error.HTTPError) as caught:
        k3.github("test")
    assert caught.value is error and len(calls) == 1 and not sleeps


def test_github_three_rate_limits_raise(monkeypatch):
    errors = [http_error(403, b"rate limit exceeded", **{"Retry-After": "600"}) for _ in range(3)]
    calls, sleeps = fake_github(monkeypatch, iter(errors))
    with pytest.raises(urllib.error.HTTPError) as caught:
        k3.github("test")
    assert caught.value is errors[-1] and len(calls) == 3 and sleeps == [60, 60]


def test_github_without_token(monkeypatch):
    calls, sleeps = fake_github(monkeypatch, iter([{"jobs": []}]))
    monkeypatch.delenv("GH_TOKEN")
    assert k3.github("test") == {"jobs": []}
    assert calls[0][0].get_header("Authorization") is None and not sleeps
