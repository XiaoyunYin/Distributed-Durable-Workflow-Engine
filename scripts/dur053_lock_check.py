#!/usr/bin/env python3
"""Verify that every externally pulled K3 image digest exists in its registry."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Callable
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlparse
from urllib.request import Request, urlopen

ACCEPT = ", ".join(
    (
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    )
)
DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
ROOT = Path(__file__).resolve().parents[1]


def manifest_parts(reference: str) -> tuple[str, str, str]:
    """Return registry host, repository path and digest from a pinned image."""
    if "@" not in reference:
        raise ValueError(f"image is not digest pinned: {reference}")
    name, digest = reference.rsplit("@", 1)
    if not DIGEST.fullmatch(digest):
        raise ValueError(f"invalid image digest: {reference}")
    first, separator, rest = name.partition("/")
    if not separator:
        registry, repository = "docker.io", first
    elif "." in first or ":" in first or first == "localhost":
        registry, repository = first, rest
    else:
        registry, repository = "docker.io", name
    path, slash, leaf = repository.rpartition("/")
    leaf = leaf.split(":", 1)[0]
    repository = f"{path}{slash}{leaf}"
    if registry == "docker.io":
        registry = "registry-1.docker.io"
    return registry, repository, digest


class RegistryClient:
    """Minimal anonymous Docker Registry v2 client with injectable HTTP opener."""

    def __init__(self, opener: Callable[..., object] = urlopen):
        self.opener = opener
        self.tokens: dict[tuple[str, str], str] = {}

    def _open(self, request: Request) -> object:
        return self.opener(request, timeout=20)  # type: ignore[call-arg]

    def _token(self, host: str, challenge: str) -> str:
        match = re.match(r"Bearer\s+(.+)", challenge, re.IGNORECASE)
        if not match:
            raise ValueError(f"unsupported registry authentication challenge from {host}")
        values = dict(re.findall(r'(\w+)="([^"]*)"', match.group(1)))
        realm = values.get("realm")
        if not realm or urlparse(realm).scheme != "https":
            raise ValueError(f"invalid registry token realm from {host}")
        query = dict(parse_qsl(urlparse(realm).query))
        for key in ("service", "scope"):
            if values.get(key):
                query[key] = values[key]
        token_url = realm.split("?", 1)[0] + "?" + urlencode(query)
        response = self._open(Request(token_url, headers={"Accept": "application/json"}))
        data = json.loads(response.read())  # type: ignore[attr-defined]
        token = data.get("token") or data.get("access_token")
        if not isinstance(token, str) or not token:
            raise ValueError(f"registry returned no anonymous pull token for {host}")
        return token

    def head(self, registry: str, repository: str, reference: str) -> tuple[int, dict[str, str]]:
        url = f"https://{registry}/v2/{repository}/manifests/{reference}"
        headers = {"Accept": ACCEPT}
        token_key = (registry, repository)
        token = self.tokens.get(token_key)
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            response = self._open(Request(url, method="HEAD", headers=headers))
        except HTTPError as error:
            if error.code == 401 and "www-authenticate" in error.headers:
                token = self._token(registry, error.headers["www-authenticate"])
                self.tokens[token_key] = token
                headers["Authorization"] = f"Bearer {token}"
                try:
                    response = self._open(Request(url, method="HEAD", headers=headers))
                except HTTPError as retry_error:
                    return retry_error.code, dict(retry_error.headers.items())
            else:
                return error.code, dict(error.headers.items())
        except URLError as error:
            raise RuntimeError(
                f"registry request failed for {registry}/{repository}: {error}"
            ) from error
        return response.status, dict(response.headers.items())  # type: ignore[attr-defined]


def locked_references(lock: dict[str, object]) -> list[tuple[str, str]]:
    images = lock.get("images")
    if not isinstance(images, dict):
        raise ValueError("lock has no images object")
    result = [(str(name), str(reference)) for name, reference in images.items()]
    node = lock.get("node")
    if not isinstance(node, str):
        raise ValueError("lock has no kind node image")
    result.append(("kind node", node))
    return result


def verify(lock: dict[str, object], client: RegistryClient) -> list[dict[str, object]]:
    results = []
    for name, image in locked_references(lock):
        host, repository, digest = manifest_parts(image)
        status, headers = client.head(host, repository, digest)
        results.append(
            {
                "name": name,
                "reference": image,
                "registry": host,
                "repository": repository,
                "digest": digest,
                "http_status": status,
                "registry_digest_header": headers.get("Docker-Content-Digest")
                or headers.get("docker-content-digest"),
                "ok": status == 200,
            }
        )
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock", type=Path, default=ROOT / "deploy/k8s/tools.lock.json")
    parser.add_argument("--output", type=Path, help="optional JSON report destination")
    args = parser.parse_args()
    try:
        lock = json.loads(args.lock.read_text(encoding="utf-8"))
        results = verify(lock, RegistryClient())
    except (OSError, json.JSONDecodeError, ValueError, RuntimeError) as error:
        print(json.dumps({"status": "FAIL", "error": str(error)}, indent=2), file=sys.stderr)
        return 1
    report = {
        "status": "PASS" if all(row["ok"] for row in results) else "FAIL",
        "results": results,
    }
    rendered = json.dumps(report, indent=2) + "\n"
    print(rendered, end="")
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8", newline="\n")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
