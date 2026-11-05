from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


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
