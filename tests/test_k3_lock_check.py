from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import Mock
from urllib.error import HTTPError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import dur053_lock_check as lock_check  # noqa: E402


class RegistryLockCheckTests(unittest.TestCase):
    def test_existing_digest_passes(self) -> None:
        response = Mock(status=200, headers={"Docker-Content-Digest": "sha256:" + "a" * 64})
        client = lock_check.RegistryClient(opener=Mock(return_value=response))
        results = lock_check.verify(
            {
                "images": {
                    "ghcr.io/example/image:v1": "ghcr.io/example/image:v1@sha256:" + "a" * 64
                },
                "node": "kindest/node:v1@sha256:" + "b" * 64,
            },
            client,
        )
        self.assertTrue(all(row["ok"] for row in results), "good digest was rejected")

    def test_nonexistent_digest_fails(self) -> None:
        opener = Mock(
            side_effect=HTTPError(
                "https://ghcr.io/v2/example/image/manifests/sha256:" + "f" * 64,
                404,
                "manifest unknown",
                {},
                None,
            )
        )
        results = lock_check.verify(
            {
                "images": {
                    "ghcr.io/example/image:v1": "ghcr.io/example/image:v1@sha256:" + "f" * 64
                },
                "node": "kindest/node:v1@sha256:" + "b" * 64,
            },
            lock_check.RegistryClient(opener=opener),
        )
        self.assertFalse(results[0]["ok"], "nonexistent digest was accepted")
        self.assertEqual(results[0]["http_status"], 404)


if __name__ == "__main__":
    unittest.main()
