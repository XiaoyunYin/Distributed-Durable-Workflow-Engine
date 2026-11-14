from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from dur053_k3_policy import REQUIRED_CI_JOBS  # noqa: E402


def _job_steps(workflow: str, job_name: str) -> list[tuple[str, str]]:
    """Read step names and bodies from a job's YAML list without extra dependencies."""
    job_match = re.search(
        rf"(?ms)^  {re.escape(job_name)}:\n(.*?)(?=^  [a-zA-Z0-9_-]+:\n|\Z)",
        workflow,
    )
    assert job_match is not None, f"workflow job {job_name!r} is missing"
    lines = job_match.group(1).splitlines()
    starts = [index for index, line in enumerate(lines) if re.match(r"^      - name:", line)]
    steps: list[tuple[str, str]] = []
    for step_index, start in enumerate(starts):
        end = starts[step_index + 1] if step_index + 1 < len(starts) else len(lines)
        block = "\n".join(lines[start:end])
        name_match = re.match(r"^      - name: (.+)$", lines[start])
        assert name_match is not None
        steps.append((name_match.group(1), block))
    return steps


def test_ci_k3_outputs_are_isolated_and_lock_evidence_is_uploaded() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    package_steps = _job_steps(workflow, "k3-package")
    smoke_steps = _job_steps(workflow, "k3-smoke")

    def assert_prior_outputs_are_isolated(
        steps: list[tuple[str, str]], target_name: str, output_root: str
    ) -> None:
        target_index = next(
            index for index, (name, _block) in enumerate(steps) if name == target_name
        )
        for name, block in steps[:target_index]:
            output_args = re.findall(r"--output\s+\"([^\"]+)\"", block)
            assert all(output_root not in argument for argument in output_args), (
                f"step {name!r} writes beneath {output_root} before {target_name!r}"
            )

    assert_prior_outputs_are_isolated(package_steps, "Run package-check", "dur053-k3-package")
    assert_prior_outputs_are_isolated(smoke_steps, "Run smoke", "dur053-k3-smoke")

    lock_check = next(
        block for name, block in package_steps if name == "Verify locked registry manifests"
    )
    lock_output = re.search(r"--output\s+\"([^\"]+)\"", lock_check)
    assert lock_output is not None, "registry lock report output is missing"
    assert "dur053-k3-lock/" in lock_output.group(1)
    assert "dur053-k3-package" not in lock_output.group(1)

    upload_index = next(
        index
        for index, (name, _block) in enumerate(package_steps)
        if name == "Retain registry lock evidence including failures"
    )
    upload = package_steps[upload_index][1]
    assert upload_index > next(
        index
        for index, (name, _block) in enumerate(package_steps)
        if name == "Verify locked registry manifests"
    )
    assert "if: always()" in upload
    assert "dur053-k3-lock-${{ github.run_id }}" in upload
    assert "${{ runner.temp }}/dur053-k3-lock/" in upload


def test_mutation_jobs_are_parallel_isolated_and_keep_both_complete_gates() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    jobs = dict(re.findall(r"(?ms)^  ([\w-]+):\n(.*?)(?=^  [\w-]+:\n|\Z)", workflow))
    services = dict(_job_steps(workflow, "services"))
    assert "    timeout-minutes: 25\n" in jobs["services"]
    assert not any("mutation" in name.lower() for name in services)
    assert list(services) == [
        "Check out source",
        "Set up Go",
        "Set up Python",
        "Set up uv",
        "Bootstrap the isolated service stack",
        "Run service-backed checks and race tests",
        "Start isolated DUR-055 PostgreSQL and pgvector",
        "Validate DUR-055 PostgreSQL FTS and pgvector retrieval",
        "Stop isolated DUR-055 PostgreSQL and pgvector",
        "Collect service diagnostics",
        "Upload service diagnostics",
        "Tear down the isolated service stack",
    ]
    assert (
        "./scripts/ci.ps1 -WithServices -WithRace"
        in services["Run service-backed checks and race tests"]
    )
    for platform, shell, output in (
        ("powershell", "PowerShell", "powershell"),
        ("linux", "Linux", "bash"),
    ):
        job = jobs[f"mutation-{platform}"]
        assert f"    name: {shell} mutation gate\n" in job
        assert "    runs-on: ubuntu-24.04\n" in job
        assert "    timeout-minutes: 100\n" in job
        assert not re.search(r"^    (needs|if):", job, re.MULTILINE)
        steps = _job_steps(workflow, f"mutation-{platform}")
        by_name = dict(steps)
        names = list(by_name)
        gate_name = f"Run {shell} mutation gate"
        assert sum("Run " in name and "mutation gate" in name for name in names) == 1
        assert (
            names.index("Bootstrap the isolated service stack")
            < names.index("Isolate deployed relays for mutation gates")
            < names.index(gate_name)
            < names.index("Drop isolated mutation databases")
            < names.index("Restore deployed relays after mutation gates")
            < names.index("Upload mutation results")
            < names.index("Tear down the isolated service stack")
        )
        assert (
            "./scripts/bootstrap.ps1 -StartServices"
            in by_name["Bootstrap the isolated service stack"]
        )
        assert (
            "stop runtime-a runtime-b worker-a worker-b"
            in by_name["Isolate deployed relays for mutation gates"]
        )
        gate = by_name[gate_name]
        assert "timeout-minutes: 75" in gate
        assert f"dur046_mutation_{platform}" in gate
        assert "DURABLE_MUTATION_DATABASE_URL" in gate
        assert "CREATE DATABASE" in gate and "migrations" in gate
        if platform == "powershell":
            assert "shell: pwsh" in gate
            assert "./scripts/mutation-gate.ps1 -Output mutations/results-powershell.json" in gate
        else:
            assert "shell: bash" in gate
            assert "bash scripts/mutation-gate.sh" in gate
        drop = by_name["Drop isolated mutation databases"]
        assert f"@('dur046_mutation_{platform}')" in drop
        assert "DROP DATABASE IF EXISTS $database WITH (FORCE)" in drop
        assert (
            "up -d --wait runtime-a runtime-b worker-a worker-b"
            in by_name["Restore deployed relays after mutation gates"]
        )
        for name in (
            "Drop isolated mutation databases",
            "Restore deployed relays after mutation gates",
            "Upload mutation results",
            "Collect service diagnostics",
            "Upload service diagnostics",
            "Tear down the isolated service stack",
        ):
            assert "if: always()" in by_name[name]
        upload = by_name["Upload mutation results"]
        assert f"name: dur046-mutation-results-{platform}-${{{{ github.run_id }}}}" in upload
        assert f"path: mutations/results-{output}.json" in upload
        assert "if-no-files-found: error" in upload
        assert f"name: mutation-{platform}-diagnostics-" in by_name["Upload service diagnostics"]
        assert "down --volumes --remove-orphans" in by_name["Tear down the isolated service stack"]
    artifacts = re.findall(r"^          name: (.+)$", workflow, re.MULTILINE)
    assert len(artifacts) == len(set(artifacts)), "artifact names collide across parallel jobs"


def test_pilot_ci_proof_names_cover_every_main_push_job() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    jobs = re.findall(
        r"(?ms)^  [\w-]+:\n(.*?)(?=^  [\w-]+:\n|\Z)", workflow.split("\njobs:\n", 1)[1]
    )
    names = set()
    for job in jobs:
        if "    if: github.event_name == 'pull_request'\n" in job:
            continue
        name = re.search(r"^    name: (.+)$", job, re.MULTILINE)
        assert name is not None
        names.add(name.group(1))
    assert names == REQUIRED_CI_JOBS


def test_k4_engineering_workflow_is_uncharged_and_retains_failures() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    steps = dict(_job_steps(workflow, "dur053-k4-lifecycle-smoke"))
    run = steps["Exercise lifecycle engineering smoke"]
    assert "lifecycle-smoke" in run
    assert "$RUNNER_TEMP/dur053-k4" in run
    assert "3600s" in run
    offline = steps["Verify engineering evidence offline"]
    assert "verify-evidence" in offline and "dur053-k4-checker" in offline
    upload = steps["Retain full lifecycle diagnostics"]
    assert "if: always()" in upload and "${{ runner.temp }}/dur053-k4/" in upload
    assert "DUR-053 K4 lifecycle smoke" in REQUIRED_CI_JOBS


def test_k4_dispatch_routes_keep_the_k3_path() -> None:
    workflow = (ROOT / ".github/workflows/dur053-campaign.yml").read_text(encoding="utf-8")
    shell = (ROOT / "scripts/dur053-k3.sh").read_text(encoding="utf-8")
    assert "^k4-[1-5](-c[1-9][0-9]*)?$" in workflow
    assert "DUR053_K4_REPLACEMENT_FOR" in workflow
    assert 'python3 scripts/dur053_campaign.py --job "$DUR053_PILOT_JOB"' in workflow
    assert "dur053_k4_driver.py" in shell and "dur053_campaign.py" in shell
