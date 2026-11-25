import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

from j2_overlay import budget, registered_budget, render  # noqa: E402

IMAGE = "localhost:5001/j2-exporter@sha256:" + "a" * 64


def test_only_registered_network_allows():
    objects = render("j2-test", "172.18.0.4", IMAGE)["items"]
    policies = {o["metadata"]["name"]: o["spec"] for o in objects if o["kind"] == "NetworkPolicy"}
    assert policies["j2-default-deny"] == {"podSelector": {}, "policyTypes": ["Ingress", "Egress"]}
    exporter = policies["j2-exporter-allow"]
    assert exporter["egress"] == [
        {
            "to": [{"ipBlock": {"cidr": "172.18.0.4/32"}}],
            "ports": [{"port": 5432, "protocol": "TCP"}],
        }
    ]
    assert exporter["ingress"][0]["from"] == [
        {"podSelector": {"matchLabels": {"app": "j2-prometheus"}}}
    ]
    prom = policies["j2-prometheus-allow"]
    assert len(prom["egress"]) == 2 and "ingress" not in prom
    assert prom["egress"][0]["ports"] == [{"port": 9187, "protocol": "TCP"}]
    assert prom["egress"][1]["to"] == [
        {
            "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "kube-system"}},
            "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}},
        }
    ]
    assert all(o["metadata"]["namespace"] == "j2-test" for o in objects)


def test_render_budget_charges_existing_monitor_slot():
    rendered = render("j2-test", "172.18.0.4", IMAGE)
    base = registered_budget()
    assert budget(rendered, base) == {
        "j2_exporter": 1,
        "prometheus": 0,
        "unused_monitor_slots": 1,
        "worst_case": 74,
        "with_admin": 82,
        "free": 18,
    }
    exporter = next(
        o
        for o in rendered["items"]
        if o["kind"] == "Deployment" and o["metadata"]["name"] == "j2-exporter"
    )
    exporter["spec"]["strategy"] = {"type": "RollingUpdate"}
    with pytest.raises(ValueError, match="no surge"):
        budget(rendered, base)
    exporter["spec"]["strategy"] = {"type": "Recreate"}
    base["future_clients"]["monitor"] += 1
    with pytest.raises(ValueError, match="budget differs"):
        budget(rendered, base)


def test_refuses_k3_scope_or_unpinned_image():
    with pytest.raises(ValueError, match="dedicated J2 namespace"):
        render("dur053-kind", "172.18.0.4", IMAGE)
    with pytest.raises(ValueError, match="immutable manifest digest"):
        render("j2-test", "172.18.0.4", "example/exporter:latest")
