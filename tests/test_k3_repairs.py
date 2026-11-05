from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import dur053_campaign as campaign  # noqa: E402
import dur053_k3 as driver  # noqa: E402


class K3RepairTests(unittest.TestCase):
    def test_addon_image_lock(self) -> None:
        source = next(iter(driver.LOCK["images"]))
        pinned = driver.LOCK["images"][source]
        pod = {
            "spec": {
                "initContainers": [{"image": source}],
                "containers": [{"image": pinned, "imagePullPolicy": "Always"}],
            }
        }
        self.assertEqual(driver.lock_addon_images([pod], [source]), {pinned})
        for container in pod["spec"]["initContainers"] + pod["spec"]["containers"]:
            self.assertEqual(
                container,
                {"image": pinned, "imagePullPolicy": "Never"},
                msg="addon digest rewrite bypassed",
            )
        for image in ("unknown:latest", source + "@sha256:" + "f" * 64):
            with self.assertRaisesRegex(
                ValueError, "addon-image-lock", msg="addon image lock bypassed"
            ):
                driver.lock_addon_images([{"image": image}], [source])
        with self.assertRaises(ValueError, msg="addon image inventory bypassed"):
            driver.lock_addon_images([], [source])

    def test_calico_config(self) -> None:
        container = {
            "name": "calico-node",
            "env": [],
            "readinessProbe": {
                "exec": {"command": ["/bin/calico-node", "-bird-ready", "-felix-ready"]}
            },
            "livenessProbe": {
                "exec": {"command": ["/bin/calico-node", "-bird-live", "-felix-live"]}
            },
        }
        objects = [
            {
                "kind": "ConfigMap",
                "metadata": {"name": "calico-config"},
                "data": {"calico_backend": "bird"},
            },
            {
                "kind": "DaemonSet",
                "metadata": {"name": "calico-node"},
                "spec": {"template": {"spec": {"containers": [container]}}},
            },
        ]
        kind = json.loads((ROOT / "deploy/k8s/kind-cluster.yaml").read_text())
        driver.configure_calico(objects, kind)
        self.assertEqual(objects[0]["data"]["calico_backend"], "vxlan")
        values = {e["name"]: e["value"] for e in container["env"]}
        self.assertEqual(
            values,
            {
                "CALICO_IPV4POOL_CIDR": kind["networking"]["podSubnet"],
                "CALICO_IPV4POOL_IPIP": "Never",
                "CALICO_IPV4POOL_VXLAN": "Always",
                "CLUSTER_TYPE": "k8s",
            },
        )
        self.assertEqual(
            container["readinessProbe"]["exec"]["command"], ["/bin/calico-node", "-felix-ready"]
        )

    def test_preload_and_startup_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = driver.Driver(Path(tmp), Path(tmp))
            source = next(iter(driver.LOCK["images"]))
            image = driver.LOCK["images"][source]
            d.addon_images = {image}
            calls = []

            def command(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
                calls.append(args)
                return subprocess.CompletedProcess(args, 0, '{"items": []}', "")

            with patch.object(d, "command", side_effect=command):
                d.preload_addons()
                (Path(tmp) / "addon-0.tar").write_bytes(b"fixture")
                d.load_addons()
                with (
                    patch.object(d, "dependencies", return_value={}),
                    patch.object(d, "load_addons"),
                ):
                    d.cluster()
            self.assertIn(["docker", "pull", "--platform=linux/amd64", image], calls)
            self.assertTrue(any("image-archive" in args for args in calls))
            alias = next(args[-1] for args in calls if "--force" in args)
            self.assertEqual(alias, source.rsplit(":", 1)[0] + "@" + image.split("@", 1)[1])
            ds = next(i for i, args in enumerate(calls) if "daemonset/calico-node" in args)
            ready = next(i for i, args in enumerate(calls) if "--for=condition=Ready" in args)
            self.assertLess(ds, ready)
            self.assertIn("--timeout=600s", calls[ds])

    def test_broken_addon_keeps_diagnostics_and_original_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pods = []
            for ns in ("kube-system", "keda", "calico-system", "application"):
                pods.append(
                    {
                        "metadata": {"namespace": ns, "name": "broken"},
                        "spec": {
                            "initContainers": [{"name": "init"}],
                            "containers": [{"name": "main"}],
                        },
                        "status": {},
                    }
                )
            ready = copy.deepcopy(pods[0])
            ready["metadata"]["name"] = "ready"
            ready["status"]["conditions"] = [{"type": "Ready", "status": "True"}]
            calls = []

            def process(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
                self.assertLessEqual(kwargs["timeout"], 10)
                calls.append(args)
                if "journalctl" in args:
                    raise subprocess.TimeoutExpired(args, 10, output=b"partial kubelet")
                output = (
                    json.dumps({"items": [*pods, ready]})
                    if args[-3:] == ["-A", "-o", "json"]
                    else "diagnostic fixture"
                )
                return subprocess.CompletedProcess(args, 0, output, "")

            def broken(d: driver.Driver) -> dict:
                d.live_start = time.monotonic() - 4000
                d.cluster_attempted = True
                raise ValueError("broken addon rollout")

            with (
                patch.dict(os.environ, {"GITHUB_ACTIONS": ""}),
                patch.object(
                    driver.Driver,
                    "command",
                    return_value=subprocess.CompletedProcess([], 0, "a" * 40, ""),
                ),
                patch.object(driver.Driver, "install"),
                patch.object(driver.Driver, "build"),
                patch.object(driver.Driver, "prepare_addon"),
                patch.object(driver.Driver, "preload_addons"),
                patch.object(driver.Driver, "render", return_value=(root / "unused", [])),
                patch.object(driver, "matched_arms"),
                patch.object(driver.Driver, "cluster", broken),
                patch.object(driver.Driver, "failure_export"),
                patch.object(driver.subprocess, "run", side_effect=process),
                patch.object(
                    driver.Driver,
                    "cleanup",
                    return_value={"status": "PASS", "elapsed_live_seconds": 12},
                ),
            ):
                output = root / "output"
                # The driver renames these pre-cluster rendered files.

                def render(d: driver.Driver, arm: str) -> tuple[Path, list]:
                    (d.scratch / ("package-" + arm)).mkdir()
                    path = output / (arm + ".yaml")
                    path.write_text("fixture")
                    return path, []

                with patch.object(driver.Driver, "render", render):
                    self.assertEqual(driver.run("smoke", output, root / "scratch"), 1)
            manifest = json.loads((output / "manifest.json").read_text())
            self.assertEqual(manifest["error"], "broken addon rollout")
            self.assertEqual(manifest["cleanup"]["status"], "PASS")
            for ns in ("kube-system", "keda", "calico-system", "application"):
                for suffix in (
                    "describe",
                    "-init-current",
                    "-init-previous",
                    "-main-current",
                    "-main-previous",
                ):
                    self.assertTrue(
                        (output / "diagnostics" / (ns + "--broken-" + suffix + ".txt")).exists()
                    )
            self.assertTrue((output / "diagnostics/containerd-images.txt").exists())
            self.assertIn("partial kubelet", (output / "diagnostics/kubelet.txt").read_text())
            self.assertFalse(any("ready" in args for args in calls))

    def test_pr_smoke_does_not_read_ledger_or_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            jobs = {"jobs": [{"id": 15, "name": "DUR-053 kind smoke"}]}
            with (
                patch.dict(
                    os.environ,
                    {
                        "GITHUB_ACTIONS": "true",
                        "GITHUB_EVENT_NAME": "pull_request",
                        "GITHUB_REPOSITORY": "fixture/repo",
                        "GITHUB_RUN_ID": "10",
                    },
                ),
                patch.object(driver, "github", return_value=jobs) as api,
                patch.object(
                    driver,
                    "job_identity_and_history",
                    side_effect=AssertionError("PR read history"),
                ),
                patch.object(
                    campaign, "committed_ledger", side_effect=AssertionError("PR read ledger")
                ),
                patch.object(
                    driver.Driver,
                    "command",
                    return_value=subprocess.CompletedProcess([], 0, "a" * 40, ""),
                ),
                patch.object(
                    driver.Driver, "install", side_effect=ValueError("stop after budget boundary")
                ),
                patch.object(driver.Driver, "cleanup", return_value={"status": "PASS"}),
            ):
                self.assertEqual(driver.run("smoke", root / "output", root / "scratch"), 1)
                api.assert_called_once_with("repos/fixture/repo/actions/runs/10/jobs?per_page=100")
            manifest = json.loads((root / "output/manifest.json").read_text())
            self.assertEqual(
                manifest["error"], "stop after budget boundary", "PR ledger scope bypassed"
            )
            self.assertEqual(manifest["charged_seconds"], 0, "PR charge scope bypassed")
            self.assertFalse(manifest["measurement_claim"])

    def test_dispatch_missing_prior_entry(self) -> None:
        ledger = {"schema": "dur053.live-ledger.v1", "limit_seconds": 259200, "entries": []}
        with (
            patch.dict(os.environ, {"GITHUB_EVENT_NAME": "workflow_dispatch"}),
            patch.object(campaign, "job_identity_and_history", return_value=("20", {"19"}, "head")),
            patch.object(campaign, "committed_ledger", return_value=ledger),
            self.assertRaisesRegex(
                ValueError, "uncommitted-live-evidence", msg="campaign ledger scope bypassed"
            ),
        ):
            campaign.campaign_gate()

    def test_ci_and_campaign_permissions_and_serialization(self) -> None:
        ci = (ROOT / ".github/workflows/ci.yml").read_text()
        dispatch = (ROOT / ".github/workflows/dur053-campaign.yml").read_text()
        self.assertNotIn("actions: read", ci)
        self.assertNotIn("dur053-live-ledger", ci)
        self.assertIn("group: dur053-live-ledger\n  cancel-in-progress: false", dispatch)
        self.assertIn("actions: read", dispatch)


if __name__ == "__main__":
    unittest.main()
