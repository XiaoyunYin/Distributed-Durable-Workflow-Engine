import json
from pathlib import Path

import pytest
from durable_contracts.partition import MAP_VERSION, PARTITION_COUNT, partition_for


def _vector_file() -> dict[str, object]:
    path = Path(__file__).parents[1] / "api" / "partition-map-v1.vectors.json"
    return json.loads(path.read_text(encoding="utf-8"))


def test_partition_contract_vectors() -> None:
    vectors = _vector_file()
    assert vectors["version"] == MAP_VERSION
    assert vectors["partition_count"] == PARTITION_COUNT
    entries = vectors["vectors"]
    assert isinstance(entries, list)
    for entry in entries:
        assert isinstance(entry, dict)
        workflow_id = entry["workflow_id"]
        partition = entry["partition"]
        assert isinstance(workflow_id, str)
        assert isinstance(partition, int)
        assert partition_for(workflow_id) == partition


def test_empty_workflow_id_is_rejected() -> None:
    with pytest.raises(ValueError, match="workflow ID must not be empty"):
        partition_for("")


def test_invalid_utf8_surrogate_is_rejected() -> None:
    with pytest.raises(UnicodeEncodeError):
        partition_for("wf-\udcff\udcfe")
