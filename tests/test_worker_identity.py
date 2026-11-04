from __future__ import annotations

import io
import urllib.request
from pathlib import Path
from typing import Any

import pytest
from workers.control import ControlClient, ControlError


def test_projected_token_rotation_and_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    seen: list[str | None] = []

    def urlopen(request: urllib.request.Request, **kwargs: Any) -> io.BytesIO:
        seen.append(request.get_header("Authorization"))
        return io.BytesIO(b"{}")

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    monkeypatch.delenv("WORKER_TOKEN_FILE", raising=False)
    client = ControlClient("http://localhost")
    client._post("/v1/worker-deliveries", {})
    token = tmp_path / "token"
    token.write_text("first\n")
    monkeypatch.setenv("WORKER_TOKEN_FILE", str(token))
    client._post("/v1/worker-deliveries", {})
    token.write_text("rotated\n")
    client._post("/v1/worker-deliveries", {})
    assert seen == [None, "Bearer first", "Bearer rotated"], "projected token not sent"

    for content in ("", None):
        if content is None:
            token.unlink()
        else:
            token.write_text(content)
        with pytest.raises(ControlError, match="TOKEN_UNAVAILABLE"):
            client._post("/v1/worker-deliveries", {})
    assert len(seen) == 3, "unavailable token sent an unauthenticated request"
