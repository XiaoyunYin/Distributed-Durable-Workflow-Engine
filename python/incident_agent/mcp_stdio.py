"""Launch the bounded incident MCP server over stdio."""

from incident_agent.fixtures import build_corpus
from incident_agent.mcp import BoundedMCPServer
from incident_agent.mcp_protocol import run_stdio_server
from incident_agent.retrieval import RetrievalIndex


def main() -> None:
    backend = BoundedMCPServer(RetrievalIndex(build_corpus()))
    run_stdio_server(backend)


if __name__ == "__main__":
    main()
