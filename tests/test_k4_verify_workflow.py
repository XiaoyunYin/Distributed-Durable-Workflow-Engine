"""Keep the dispatched K4 verifier offline, read-only and failure-preserving."""

import ast
import re
from pathlib import Path

from test_k3_workflow import _job_steps

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/dur053-k4-verify.yml"


def test_verify_workflow_dispatch_permissions_and_offline_steps():
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert re.search(r"(?m)^on: workflow_dispatch\n", workflow)
    assert not re.search(r"(?m)^  (push|pull_request|schedule|inputs):", workflow)
    assert workflow.split("\npermissions:\n", 1)[1].split("\n\n", 1)[0] == "  contents: read"
    assert "\nconcurrency:\n  group: dur053-k4-verify\n  cancel-in-progress: false\n" in workflow
    assert re.findall(r"(?m)^  ([\w-]+):\n", workflow.split("\njobs:\n", 1)[1]) == ["verify"]
    assert "    runs-on: ubuntu-24.04\n    timeout-minutes: 300\n" in workflow
    assert "  PYTHONDONTWRITEBYTECODE: '1'\n  GOMAXPROCS: '2'\n" in workflow
    steps = dict(_job_steps(workflow, "verify"))
    assert list(steps) == [
        "Check out source",
        "Set up Go",
        "Set up Python",
        "Set up uv",
        "Install locked Python dependencies",
        "Build checker",
        "Download and verify release assets",
        "Verify evidence and retain diagnostics",
        "Retain verification output including failures",
    ]
    assert not re.search(
        r"(?i)\b(lifecycle|ledger|kind|kubectl|helm|docker|terraform)\b|dur053-k3\.sh|uv run",
        workflow,
    )
    assert "persist-credentials: false" in steps["Check out source"]
    assert "go build -p 1 -buildvcs=false" in steps["Build checker"]


def test_verify_workflow_mirrors_ci_setup_and_locked_dependencies():
    ci = dict(_job_steps((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"), "checks"))
    steps = dict(_job_steps(WORKFLOW.read_text(encoding="utf-8"), "verify"))
    for name in ("Set up Go", "Set up Python", "Set up uv"):
        assert steps[name].strip() == ci[name].strip()
    # ci.yml's check.ps1 resolves this same project's default dev group through
    # uv; an explicit locked sync exposes its interpreter to python3 here.
    install = steps["Install locked Python dependencies"]
    assert "uv sync --locked" in install
    assert 'echo "$GITHUB_WORKSPACE/.venv/bin" >> "$GITHUB_PATH"' in install


def test_verify_workflow_checks_assets_gates_json_and_uploads_failures():
    steps = dict(_job_steps(WORKFLOW.read_text(encoding="utf-8"), "verify"))
    download = steps["Download and verify release assets"]
    assert "GH_TOKEN: ${{ github.token }}" in download
    assert "experiments/portfolio/k8s-recovery/k4-v1/*/*/payload-index.json" in download
    assert '["git", "ls-files", "--", pattern]' in download
    assert 'index["store"]["tag"]' in download
    assert '"gh", "release", "download", tag, "-R", os.environ["GITHUB_REPOSITORY"]' in download
    assert '"-D", str(destination)' in download
    assert "{p.name for p in destination.iterdir()} == set(expected)" in download
    assert 'hashlib.file_digest(stream, "sha256")' in download
    assert 'digest == row["sha256"]' in download
    run = steps["Verify evidence and retain diagnostics"]
    assert (
        "/usr/bin/time -v python3 scripts/dur053_k4_evidence.py verify-evidence "
        '--checker "$RUNNER_TEMP/checker" --repository "$GITHUB_WORKSPACE" '
        '--store "$RUNNER_TEMP/store" > "$out/k4-verify.json" 2> "$out/k4-verify.stderr"'
    ) in run
    assert 'printf \'%s\\n\' "$GITHUB_SHA" > "$out/commit.txt"' in run
    assert run.index("status=$?") < run.index("sha256sum") < run.index('test "$status" -eq 0')
    assert "sha256sum commit.txt k4-verify.json k4-verify.stderr > sha256.txt" in run
    assert 'test -s "$out/k4-verify.json"' in run
    assert "json.loads(path.read_bytes(), parse_constant=invalid_constant)" in run
    upload = steps["Retain verification output including failures"]
    assert "if: always()" in upload
    assert "dur053-k4-verify-${{ github.run_id }}" in upload
    assert "path: ${{ runner.temp }}/out/" in upload and "if-no-files-found: error" in upload
    for block in (download, run):
        source = block.split("          python3 - <<'PY'\n", 1)[1].split("          PY", 1)[0]
        ast.parse("\n".join(line[10:] for line in source.splitlines()))
