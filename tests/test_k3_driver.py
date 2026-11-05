from __future__ import annotations

import copy
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import dur053_k3 as driver  # noqa: E402
import dur053_k3_policy as policy  # noqa: E402


class K3DriverTests(unittest.TestCase):
    def test_acceptance_guards(self) -> None:
        good_images = {"runtime": "runtime@sha256:" + "a" * 64}
        policy.running_image_digests(
            [{"containers": [{"imageID": good_images["runtime"]}]}], good_images
        )
        for guard, invoke in (
            (
                "running-image-digest",
                lambda: policy.running_image_digests(
                    [{"containers": [{"imageID": "other"}]}], good_images
                ),
            ),
            ("terminal-checker", lambda: policy.terminal_report({"valid": False})),
            ("postgres-budget-setting", lambda: policy.postgres_budget_setting(200)),
            (
                "scratch-ownership",
                lambda: policy.scratch_ownership(Path("other"), Path("unrelated")),
            ),
        ):
            with (
                self.subTest(guard=guard),
                self.assertRaises(ValueError, msg="acceptance guard bypassed " + guard),
            ):
                invoke()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            d = driver.Driver(root, root)
            with (
                patch.object(
                    driver.subprocess,
                    "run",
                    return_value=subprocess.CompletedProcess(["fixture"], 42, "", ""),
                ),
                self.assertRaises(ValueError, msg="acceptance guard bypassed command-exit"),
            ):
                d.command(["fixture"], record=False)
            with (
                patch.object(d, "sql", side_effect=["DROP DATABASE", "1"]),
                self.assertRaises(
                    ValueError, msg="acceptance guard bypassed database-drop-verified"
                ),
            ):
                d.drop("fixture")

            def result(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
                return subprocess.CompletedProcess(
                    args, 0, json.dumps(["runtime:latest"]) if "inspect" in args else "", ""
                )

            with (
                patch.object(d, "command", side_effect=result),
                self.assertRaises(ValueError, msg="acceptance guard bypassed built-image-digest"),
            ):
                d.build()
            (root / "bin").mkdir()
            (root / "bin/kind").write_text("fixture")
            d.created.clear()
            d.kind_network_created = True
            with (
                patch.object(
                    d,
                    "command",
                    side_effect=lambda args, **kw: subprocess.CompletedProcess(args, 0, "", ""),
                ),
                self.assertRaises(
                    ValueError, msg="acceptance guard bypassed owned-network-cleanup"
                ),
            ):
                d.cleanup()

    def test_probe_guards(self) -> None:
        variants = (
            "networkpolicy-enforced",
            "worker-auth-denied",
            "operator-authorized-read",
            "db-outage-readiness",
            "outage-no-restart",
            "drain-readiness",
        )
        for variant in variants:
            with tempfile.TemporaryDirectory() as tmp, self.subTest(guard=variant):
                root = Path(tmp)
                d = driver.Driver(root, root)
                pod = {
                    "metadata": {"name": "fixture", "uid": "initial"},
                    "status": {"podIP": "127.0.0.1", "containerStatuses": [{"restartCount": 0}]},
                }
                health_calls = 0

                def python(
                    ns: str, app: str, code: str, variant: str = variant, **kwargs: object
                ) -> str:
                    nonlocal health_calls
                    if "import socket" in code:
                        return json.dumps(
                            {
                                "operator": "CONNECTED"
                                if variant == "networkpolicy-enforced"
                                else "DENIED",
                                "demand": "DENIED",
                            }
                        )
                    if "/identity/operator" in code:
                        return "401" if variant == "operator-authorized-read" else "404"
                    if "results={}" in code:
                        return json.dumps(
                            {
                                "operator": 200 if variant == "worker-auth-denied" else 401,
                                "demand": 401,
                            }
                        )
                    if "/healthz" in code:
                        health_calls += 1
                        ready = 503 if health_calls == 2 else 200
                        if variant == "db-outage-readiness" and health_calls == 2:
                            ready = 200
                        return json.dumps({"/healthz": 200, "/readyz": ready})
                    return "True"

                def k(
                    *args: str, variant: str = variant, pod: dict = pod, **kwargs: object
                ) -> subprocess.CompletedProcess[str]:
                    if "scaledobject" in args:
                        value = {
                            "status": {
                                "conditions": [{"type": "Ready", "status": "True"}],
                                "health": {"metric": {"status": "Happy", "numberOfFailures": 0}},
                            }
                        }
                    elif "get" in args and "pod" in args:
                        value = copy.deepcopy(pod)
                        if variant == "outage-no-restart":
                            value["status"]["containerStatuses"][0]["restartCount"] = 1
                    else:
                        value = {}
                    return subprocess.CompletedProcess(list(args), 0, json.dumps(value), "")

                proc = Mock(returncode=0)
                proc.communicate.return_value = (
                    "[200,200]" if variant == "drain-readiness" else "[200,503]",
                    "",
                )
                with (
                    patch.object(d, "pod", return_value=pod),
                    patch.object(d, "python", side_effect=python),
                    patch.object(d, "k", side_effect=k),
                    patch.object(
                        d, "command", return_value=subprocess.CompletedProcess([], 0, "", "")
                    ),
                    patch.object(driver.time, "sleep"),
                    patch.object(driver.subprocess, "Popen", return_value=proc),
                    self.assertRaises(ValueError, msg="acceptance guard bypassed " + variant),
                ):
                    d.probes("namespace")

    def test_installer_guards(self) -> None:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w"):
            pass
        wheel = buffer.getvalue()
        original_read = Path.read_text

        def read(path: Path, *args: object, **kwargs: object) -> str:
            return (
                "MemTotal: 12345 kB"
                if path.as_posix() == "/proc/meminfo"
                else original_read(path, *args, **kwargs)
            )

        for guard in ("runner-python-platform", "runner-image-observed", "schema-checksum"):
            with tempfile.TemporaryDirectory() as tmp, self.subTest(guard=guard):
                root = Path(tmp)
                (root / "schemas").mkdir()
                (root / "schemas/schema.json").write_bytes(b"{}")
                lock = {
                    "tools": {},
                    "pyyaml": {"url": "fixture", "sha256": "fixture"},
                    "schemas": {
                        "schema.json": "0" * 64
                        if guard == "schema-checksum"
                        else hashlib.sha256(b"{}").hexdigest()
                    },
                }
                d = driver.Driver(root, root)
                with (
                    patch.object(driver, "LOCK", lock),
                    patch.object(driver, "K8S", root),
                    patch.object(driver, "download", return_value=wheel),
                    patch.object(
                        driver.sys,
                        "version_info",
                        (3, 11) if guard == "runner-python-platform" else (3, 12),
                    ),
                    patch.object(driver.sys, "path", list(sys.path)),
                    patch.object(driver.platform, "machine", return_value="x86_64"),
                    patch.dict(
                        os.environ,
                        {
                            "ImageOS": "ubuntu24",
                            "ImageVersion": "" if guard == "runner-image-observed" else "20261104",
                        },
                    ),
                    patch.object(Path, "read_text", read),
                    patch.object(
                        d, "command", return_value=subprocess.CompletedProcess([], 0, "fixture", "")
                    ),
                    self.assertRaises(ValueError, msg="acceptance guard bypassed " + guard),
                ):
                    d.install()

    def test_actions_guards(self) -> None:
        def api(path: str) -> dict:
            if path.endswith("/runs/10"):
                return {"created_at": "2026-11-04", "head_sha": "head"}
            if "/runs/10/jobs" in path:
                return {"jobs": [{"id": 15, "name": "DUR-053 campaign"}]}
            if "/runs?" in path:
                return {"workflow_runs": [{"id": 9, "created_at": "2026-11-03"}]}
            return {"total_count": 101, "jobs": []}

        env = {
            "GITHUB_ACTIONS": "true",
            "GH_TOKEN": "fixture",
            "GITHUB_REPOSITORY": "fixture/repo",
            "GITHUB_RUN_ID": "10",
            "GITHUB_RUN_ATTEMPT": "1",
        }
        with patch.dict(os.environ, env), patch.object(driver, "github", side_effect=api):
            with self.assertRaises(ValueError, msg="acceptance guard bypassed history-complete"):
                driver.job_identity_and_history()
            with (
                patch.dict(os.environ, {"GITHUB_ACTIONS": ""}),
                patch.object(
                    driver,
                    "github",
                    side_effect=lambda path: (
                        {"created_at": "2026-11-04", "head_sha": "head"}
                        if path.endswith("/runs/10")
                        else (
                            {"jobs": [{"id": 15, "name": "DUR-053 campaign"}]}
                            if "/runs/10/jobs" in path
                            else {"workflow_runs": []}
                        )
                    ),
                ),
                self.assertRaises(
                    ValueError, msg="acceptance guard bypassed hosted-actions-required"
                ),
            ):
                driver.job_identity_and_history()
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, env):
            root = Path(tmp)
            jobs = {
                "jobs": [
                    {"id": 1, "name": "DUR-053 package check"},
                    {"id": 2, "name": "DUR-053 package check"},
                ]
            }
            with (
                patch.object(driver, "github", return_value=jobs),
                patch.object(
                    driver.Driver,
                    "command",
                    return_value=subprocess.CompletedProcess([], 0, "a" * 40, ""),
                ),
                patch.object(driver.Driver, "install"),
                patch.object(driver.Driver, "build"),
                patch.object(driver.Driver, "prepare_addon"),
                patch.object(driver.Driver, "render", return_value=(root / "unused", [])),
                patch.object(driver, "matched_arms"),
                patch.object(
                    driver.Driver,
                    "cleanup",
                    return_value={"status": "PASS", "elapsed_live_seconds": 0},
                ),
            ):
                code = driver.run("package-check", root / "output", root / "scratch")
            self.assertEqual(code, 1, "acceptance guard bypassed job-provenance")

    def test_network_denial_rejects_nonpolicy_failures(self) -> None:
        import ast
        import socket

        tree = ast.parse((ROOT / "scripts/dur053_k3.py").read_text())
        expr = next(
            n.value
            for n in ast.walk(tree)
            if isinstance(n, ast.Assign)
            and isinstance(n.value, ast.Constant)
            and isinstance(n.value.value, str)
            and n.value.value.startswith("import socket,json")
        )
        program = expr.value
        with (
            patch.object(socket, "create_connection", side_effect=OSError("listener refused")),
            self.assertRaises(OSError, msg="non-policy network failure accepted"),
        ):
            exec(compile(program, "worker-network-probe", "exec"), {})


if __name__ == "__main__":
    unittest.main()
