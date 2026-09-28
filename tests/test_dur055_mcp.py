from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import Any

from incident_agent.fixtures import build_corpus, build_incident_cases
from incident_agent.mcp import BoundedMCPServer
from incident_agent.mcp_protocol import MCPClientFacade, create_mcp_server
from incident_agent.retrieval import RetrievalIndex
from mcp import Client


def _run(awaitable: Awaitable[Any]) -> Any:
    return asyncio.run(awaitable)


def _client_call(server: Any, method: str, arguments: dict[str, Any]) -> tuple[bool, Any]:
    async def invoke() -> tuple[bool, Any]:
        try:
            async with Client(server, raise_exceptions=False) as client:
                result = await client.call_tool(method, arguments)
            return bool(result.is_error), result
        except Exception as error:  # protocol-level unknown-method rejection
            return True, error

    return _run(invoke())


def test_mcp_sdk_server_rejects_disallowed_tool() -> None:
    backend = BoundedMCPServer(RetrievalIndex(build_corpus()))
    server = create_mcp_server(backend)
    rejected, result = _client_call(server, "run_shell", {"command": "whoami"})
    assert rejected
    if hasattr(result, "is_error"):
        assert result.is_error
    else:
        assert "tool" in str(result).lower() or "method" in str(result).lower()


def test_mcp_sdk_server_rejects_invalid_arguments() -> None:
    backend = BoundedMCPServer(RetrievalIndex(build_corpus()))
    server = create_mcp_server(backend)
    rejected, result = _client_call(
        server,
        "query_logs",
        {"case_id": "dev-bad_configuration-01", "service": "checkout", "extra": "nope"},
    )
    assert rejected
    if hasattr(result, "is_error"):
        assert result.is_error
    else:
        assert "argument" in str(result).lower() or "input" in str(result).lower()


def test_mcp_sdk_server_rejects_argument_type_mismatch() -> None:
    backend = BoundedMCPServer(RetrievalIndex(build_corpus()))
    server = create_mcp_server(backend)
    rejected, result = _client_call(
        server,
        "query_logs",
        {"case_id": 17, "service": "checkout"},
    )
    assert rejected
    if hasattr(result, "is_error"):
        assert result.is_error


def test_mcp_schema_manifest_is_read_by_official_client() -> None:
    backend = BoundedMCPServer(RetrievalIndex(build_corpus()))
    schemas = MCPClientFacade(backend).schema_manifest()
    assert {tool["name"] for tool in schemas} == {
        "query_logs",
        "query_metrics",
        "search_runbooks",
        "remediation_status",
    }
    query_schema = next(tool["input_schema"] for tool in schemas if tool["name"] == "query_logs")
    assert query_schema["properties"]["case_id"]["type"] == "string"


def test_mcp_schema_iteration_changes_descriptions_not_arguments() -> None:
    backend = BoundedMCPServer(RetrievalIndex(build_corpus()))
    v1 = MCPClientFacade(backend, schema_profile="evidence-contract-v1").schema_manifest()
    v2 = MCPClientFacade(backend, schema_profile="evidence-contract-v2").schema_manifest()
    by_name_v1 = {tool["name"]: tool for tool in v1}
    by_name_v2 = {tool["name"]: tool for tool in v2}
    assert set(by_name_v1) == set(by_name_v2)
    assert all(
        by_name_v1[name]["input_schema"] == by_name_v2[name]["input_schema"] for name in by_name_v1
    )
    assert any(
        by_name_v1[name]["description"] != by_name_v2[name]["description"] for name in by_name_v1
    )


def test_incident_agent_calls_mcp_client_end_to_end() -> None:
    backend = BoundedMCPServer(RetrievalIndex(build_corpus()))
    agent_tools = MCPClientFacade(backend)
    case = next(case for case in build_incident_cases() if case.split == "development")

    result = agent_tools.query_logs("dur055-e2e", case.case_id, case.service)

    assert result.method == "query_logs"
    assert result.schema_version == "m6-mcp-v1"
    assert result.evidence_ids
    assert backend.calls("dur055-e2e")
    assert backend.calls("dur055-e2e")[0].method == "query_logs"
    assert result.data["rows"]


def test_mcp_tool_names_are_exact_existing_allowlist() -> None:
    backend = BoundedMCPServer(RetrievalIndex(build_corpus()))

    async def list_names() -> set[str]:
        async with Client(create_mcp_server(backend)) as client:
            tools = await client.list_tools()
        return {tool.name for tool in tools.tools}

    assert _run(list_names()) == {
        "query_logs",
        "query_metrics",
        "search_runbooks",
        "remediation_status",
    }
