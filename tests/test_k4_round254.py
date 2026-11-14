"""Round 254 regressions for resource names and read-round-trip bounds."""

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dur053_k4_driver as driver  # noqa: E402
import dur053_k4_interventions as interventions  # noqa: E402


def test_get_refuses_empty_resource_name_before_kubectl(tmp_path):
    kubectl = Mock()
    adapter = interventions.Interventions(
        SimpleNamespace(k=kubectl), "ns", tmp_path, {}, {}, "a" * 40
    )
    adapter.safety = Mock()

    with pytest.raises(ValueError, match="k4-empty-resource-name"):
        adapter.get("pdb", "")

    kubectl.assert_not_called()


def test_get_list_uses_name_free_kubectl_command(tmp_path):
    kubectl = Mock(return_value=SimpleNamespace(stdout='{"items":[]}'))
    adapter = interventions.Interventions(
        SimpleNamespace(k=kubectl), "ns", tmp_path, {}, {}, "a" * 40
    )
    adapter.safety = Mock()

    assert adapter.get_list("pdb") == {"items": []}
    kubectl.assert_called_once_with("-n", "ns", "get", "pdb", "-o", "json", record=False)


def test_reconciliation_activity_inside_round_trip_uses_t1(monkeypatch):
    times = iter((100.0, 101.0))
    monkeypatch.setattr(driver.time, "time", lambda: next(times))
    activity_at = 100.5

    snapshot, quiet, last, t0, t1, changed = driver.reconciliation_round(
        lambda: {"workflows": []},
        lambda: {"activity_at": activity_at},
        None,
        90.0,
    )

    assert t0 == 100.0 and t1 == 101.0
    assert changed and last
    assert snapshot == {"workflows": []}
    assert quiet >= activity_at, "quiet stamp precedes witness activity read in same round trip"
