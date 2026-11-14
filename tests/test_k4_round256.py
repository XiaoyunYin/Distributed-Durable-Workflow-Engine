"""Round 256 real-data regression for eviction confirmation."""

import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dur053_k4_evidence as evidence


def test_real_eviction_confirmation_validates_eventual_drain():
    fixture_path = Path(__file__).parent / "fixtures" / "k4_eviction_confirmation.json"
    raw = fixture_path.read_text(encoding="utf-8")
    assert not re.search(
        r"(?i)(bearer\s+|access[_-]?token|refresh[_-]?token|api[_-]?key|password|secret)",
        raw,
    ), "real-data fixture contains a credential"
    fixture = json.loads(raw)

    issues = evidence.confirmation(
        fixture["confirmation"], fixture["timeline"], "PDB-gated eviction", fixture["leases"]
    )

    assert "k4-confirmation-eventual-drain" not in issues, (
        "completed real eviction rejected by confirmation"
    )
