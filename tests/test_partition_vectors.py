from durable_contracts.partition import MAP_VERSION, partition_for


def test_partition_contract_vectors() -> None:
    assert MAP_VERSION == "sha256-u64-be-v1"
    vectors = {
        "workflow-0001": 8,
        "workflow-0002": 14,
        "incident-2026-09-14-a": 0,
        "retry-key/abc": 0,
    }
    assert {workflow_id: partition_for(workflow_id) for workflow_id in vectors} == vectors


def test_empty_workflow_id_is_rejected() -> None:
    try:
        partition_for("")
    except ValueError as error:
        assert str(error) == "workflow ID must not be empty"
    else:
        raise AssertionError("empty workflow ID was accepted")
