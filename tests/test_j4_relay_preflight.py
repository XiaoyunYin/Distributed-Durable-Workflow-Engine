from __future__ import annotations

import pytest

from deploy.j4.relay_preflight import relay_selection


@pytest.fixture
def registry():
    return {
        "schema_version": 1,
        "switches": {
            "SYNTHETIC_RELAY": {
                "task": "J9a",
                "unset_default": "0",
                "values": {"0": "legacy", "1": "repaired"},
                "permitted_enabled_profiles": ["j4-core", "j4-b2"],
            }
        },
    }


def test_no_j9a_registry_entry_cannot_arm():
    for data in (None, {"schema_version": 1, "switches": {}}):
        with pytest.raises(ValueError):
            relay_selection(data, "SYNTHETIC_RELAY", "1")


def test_legacy_and_wrong_registered_profile_cannot_arm(registry):
    with pytest.raises(ValueError):
        relay_selection(registry, "SYNTHETIC_RELAY", "0")
    registry["switches"]["SYNTHETIC_RELAY"]["permitted_enabled_profiles"] = ["j4-core"]
    with pytest.raises(ValueError):
        relay_selection(registry, "SYNTHETIC_RELAY", "1")


def test_launch_selection_and_running_runtime_proof(registry):
    selection = relay_selection(registry, "SYNTHETIC_RELAY", "1")
    assert selection.environment() == {"SYNTHETIC_RELAY": "1"}
    revision, image = "1" * 40, "sha256:" + "2" * 64
    report = {
        "source_revision": revision,
        "runtime_image_id": image,
        "switch": "SYNTHETIC_RELAY",
        "value": "1",
        "effective_relay_mode": "repaired",
    }
    selection.confirm(report, revision, image)
    for field, value in (
        ("effective_relay_mode", "legacy"),
        ("source_revision", "0" * 40),
        ("runtime_image_id", "sha256:" + "3" * 64),
        ("value", "0"),
    ):
        with pytest.raises(ValueError, match="selected repaired relay mode"):
            selection.confirm({**report, field: value}, revision, image)


def test_runtime_health_requires_explicit_repaired_mode(registry):
    selection = relay_selection(registry, "SYNTHETIC_RELAY", "1")
    revision, image = "1" * 40, "sha256:" + "2" * 64
    health = {
        "status": "ok",
        "relay_switch": "SYNTHETIC_RELAY",
        "relay_value": "1",
        "effective_relay_mode": "repaired",
    }
    proof = selection.confirm_health(health, revision, image, image)
    assert proof["value"] == "1" and proof["effective_relay_mode"] == "repaired"
    legacy = {**health, "relay_value": "0", "effective_relay_mode": "legacy"}
    with pytest.raises(ValueError, match="selected repaired relay mode"):
        selection.confirm_health(legacy, revision, image, image)
    for field in ("relay_switch", "relay_value", "effective_relay_mode"):
        incomplete = {key: value for key, value in health.items() if key != field}
        with pytest.raises(ValueError, match="requires an explicit relay switch, value and mode"):
            selection.confirm_health(incomplete, revision, image, image)
