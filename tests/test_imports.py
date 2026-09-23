import os
import subprocess
import sys
from pathlib import Path

from incident_agent import __version__ as incident_agent_version
from workers import __version__ as workers_version


def test_python_packages_import() -> None:
    assert workers_version == "0.1.0-dev"
    assert incident_agent_version == "0.1.0-dev"


def test_worker_healthcheck_imports_no_runtime_or_kafka_stack() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    python_root = repo_root / "python"
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        part for part in (str(python_root), env.get("PYTHONPATH", "")) if part
    )
    probe = (
        "import sys; import workers.__main__; "
        "assert 'workers.kafka_worker' not in sys.modules; "
        "assert 'workers.registry' not in sys.modules; "
        "assert 'workers.runner' not in sys.modules; "
        "assert 'faults.client' not in sys.modules"
    )
    subprocess.run([sys.executable, "-c", probe], cwd=repo_root, env=env, check=True)
