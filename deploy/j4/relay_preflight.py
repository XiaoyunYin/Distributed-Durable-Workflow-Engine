"""Fail-closed relay selection for J4 admission."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PROFILES = {"j4-core", "j4-b2"}


@dataclass(frozen=True)
class RelaySelection:
    name: str
    value: str

    def environment(self) -> dict[str, str]:
        return {self.name: self.value}

    def confirm(self, report: object, source_revision: str, runtime_image_id: str) -> None:
        if not re.fullmatch(r"[0-9a-f]{40}", source_revision) or not re.fullmatch(
            r"sha256:[0-9a-f]{64}", runtime_image_id
        ):
            raise ValueError("J4 relay admission requires pinned runtime provenance")
        if report != {
            "source_revision": source_revision,
            "runtime_image_id": runtime_image_id,
            "switch": self.name,
            "value": self.value,
            "effective_relay_mode": "repaired",
        }:
            raise ValueError("J4 requires proof that this runtime selected repaired relay mode")


def relay_selection(registry: object, name: str, value: str) -> RelaySelection:
    if not isinstance(registry, dict) or registry.get("schema_version") != 1:
        raise ValueError("J4 qualification requires the D037 switch registry")
    switches: Any = registry.get("switches")
    if not isinstance(switches, dict):
        raise ValueError("J4 qualification requires registered runtime switches")
    if not re.fullmatch(r"[A-Z][A-Z0-9_]+", name):
        raise ValueError("J4 qualification requires an explicit registered relay switch")
    entry = switches.get(name)
    if not isinstance(entry, dict) or entry.get("task") != "J9a":
        raise ValueError("J4 qualification requires a J9a switch entry")
    values = entry.get("values")
    profiles = entry.get("permitted_enabled_profiles")
    unset = entry.get("unset_default")
    if (
        not isinstance(values, dict)
        or not isinstance(profiles, list)
        or any(not isinstance(profile, str) for profile in profiles)
        or not PROFILES.issubset(profiles)
        or not isinstance(unset, str)
        or value in {"", unset}
        or value not in values
    ):
        raise ValueError("J4 requires a declared J9a opt-in value for both J4 profiles")
    return RelaySelection(name, value)


def read_json(path: Path) -> object:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate relay admission field {key}")
            result[key] = value
        return result

    return json.loads(path.read_bytes(), object_pairs_hook=unique)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", type=Path, default=Path("deploy/runtime-switches.json"))
    parser.add_argument("--switch", required=True)
    parser.add_argument("--value", required=True)
    parser.add_argument("--runtime-report", type=Path, required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--runtime-image-id", required=True)
    args = parser.parse_args()
    try:
        selection = relay_selection(read_json(args.registry), args.switch, args.value)
        selection.confirm(
            read_json(args.runtime_report), args.source_revision, args.runtime_image_id
        )
    except (OSError, ValueError) as exc:
        parser.exit(1, f"J4 refused to arm: {exc}\n")
    print(json.dumps(selection.environment(), sort_keys=True))


if __name__ == "__main__":
    main()
