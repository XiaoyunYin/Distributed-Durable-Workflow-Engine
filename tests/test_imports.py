from incident_agent import __version__ as incident_agent_version
from workers import __version__ as workers_version


def test_python_packages_import() -> None:
    assert workers_version == "0.1.0-dev"
    assert incident_agent_version == "0.1.0-dev"
