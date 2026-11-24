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


def test_phase_f_has_its_own_pr_only_kind_job_and_artifacts() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    ordinary = dict(_job_steps(workflow, "k3-smoke"))
    chaos = dict(_job_steps(workflow, "k6-phase-f-smoke"))
    jobs = dict(re.findall(r"(?ms)^  ([\w-]+):\n(.*?)(?=^  [\w-]+:\n|\Z)", workflow))
    for job in ("k3-smoke", "k6-phase-f-smoke"):
        assert "    if: github.event_name == 'pull_request'" in jobs[job]
        assert "    timeout-minutes: 70" in jobs[job]
        assert "DUR053_KIND_JOB_STARTED=" in jobs[job]
    assert "DUR053_K6_F1" not in ordinary["Run smoke"]
    assert all(
        f"DUR053_K6_{phase}_OUTPUT" in ordinary["Run smoke"] for phase in ("D2B", "E1", "E2")
    )
    assert "-Scenario chaos-smoke" in chaos["Run Phase F chaos smokes"]
    for phase in ("F1A", "F1B"):
        assert f"DUR053_K6_{phase}_OUTPUT" in chaos["Run Phase F chaos smokes"]
    for path in ("phase-f-kind", "f1a", "f1b"):
        assert any(
            f"dur053-k6-{path}/" in step and "if: always()" in step for step in chaos.values()
        )


def test_postgres_checks_and_race_fixtures_run_in_isolated_jobs() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    jobs = dict(re.findall(r"(?ms)^  ([\w-]+):\n(.*?)(?=^  [\w-]+:\n|\Z)", workflow))
    services = dict(_job_steps(workflow, "services"))
    race = dict(_job_steps(workflow, "services-race"))
    fixtures = dict(_job_steps(workflow, "services-k6"))
    legacy_step = "Verify legacy checker parity on PostgreSQL"
    k6_g2_step = "Verify K6 G2 cuts and external evidence against two PostgreSQL instances"
    k6_g3a2_upload = "Upload K6 G3a2 verdict evidence"
    k6_c_step = "Exercise K6 Phase C PostgreSQL startup sampling"
    for job in ("services", "services-race", "services-k6"):
        assert "    timeout-minutes: 25\n" in jobs[job]
        assert not re.search(r"^    (needs|if):", jobs[job], re.MULTILINE)
    setup = [
        "Check out source",
        "Set up Go",
        "Set up Python",
        "Set up uv",
        "Bootstrap the isolated service stack",
    ]
    cleanup = [
        "Collect service diagnostics",
        "Upload service diagnostics",
        "Tear down the isolated service stack",
    ]
    assert list(services) == [
        *setup,
        "Run service-backed checks",
        "Start isolated DUR-055 PostgreSQL and pgvector",
        "Validate DUR-055 PostgreSQL FTS and pgvector retrieval",
        "Stop isolated DUR-055 PostgreSQL and pgvector",
        *cleanup,
    ]
    assert list(race) == [
        *setup,
        "Run service-backed race and sink contract checks",
        *cleanup,
    ]
    assert list(fixtures) == [
        *setup,
        "Run tagged K6 Go tests against PostgreSQL",
        legacy_step,
        k6_g2_step,
        k6_g3a2_upload,
        k6_c_step,
        *cleanup,
    ]
    for steps in (services, race, fixtures):
        assert "./scripts/bootstrap.ps1 -StartServices" in steps[setup[-1]]
        for step in cleanup:
            assert "if: always()" in steps[step]
        assert "down --volumes --remove-orphans" in steps[cleanup[-1]]
    assert "name: service-diagnostics-" in services[cleanup[1]]
    assert "name: service-race-diagnostics-" in race[cleanup[1]]
    assert "name: service-k6-diagnostics-" in fixtures[cleanup[1]]
    tagged = fixtures["Run tagged K6 Go tests against PostgreSQL"]
    assert "shell: pwsh" in tagged
    assert "DURABLE_REQUIRE_DATABASE: '1'" in tagged
    assert (
        "go test -p 1 -count=1 -v -tags dur053_k6 ./internal/faults ./internal/k6hook "
        "./internal/k6ingress ./internal/state ./internal/engine ./cmd/runtime" in tagged
    )
    assert r"^--- PASS: $testName \(" in tagged
    assert "'TestK6LeaseTakeoverPinsIdleBackendAndFencesOriginalAttempt'" in tagged
    assert "'TestK6RenewalIdleTimeoutTerminatesRealTransaction'" in tagged
    assert "'TestK6SchedulerRenewalDefersFencedReleaseAfterTakeover'" in tagged
    legacy = fixtures[legacy_step]
    assert "DURABLE_REQUIRE_DATABASE: '1'" in legacy
    assert (
        "go test -p 1 -count=1 -v ./cmd/dur049-checker -run '^TestLegacyParityPostgres$'" in legacy
    )
    k6_g2 = fixtures[k6_g2_step]
    assert "DURABLE_REQUIRE_K6_PAIR: '1'" in k6_g2
    assert "postgres:18.6-bookworm" in k6_g2
    assert "go test -p 1 -count=1 -v ./cmd/dur049-checker ./internal/invariants/..." in k6_g2
    assert (
        "-run '^TestK6(Cut|Source|Sink|EngineExport|Submission|Census|Column|"
        "Roster|Session|Ingress|Decision|Broker|Quiet|Topology|Orphan)'" in k6_g2
    )
    assert "finally" in k6_g2 and "docker rm -f $sink" in k6_g2
    assert "go build -p 1 -o $env:DURABLE_K6_VERDICT_CHECKER" in k6_g2
    assert "DURABLE_K6_VERDICT_ARTIFACT" in k6_g2
    assert "if: always()" in fixtures[k6_g3a2_upload]
    assert "path: artifacts/k6-g3a2-verdicts/" in fixtures[k6_g3a2_upload]
    k6_c = fixtures[k6_c_step]
    assert "DUR053_PHASE_C_FIXTURE_REQUIRED: '1'" in k6_c
    assert "deploy/k8s/tools.lock.json" in k6_c and "127.0.0.1::5432" in k6_c
    assert "POSTGRES_HOST_AUTH_METHOD=password" in k6_c
    assert "uv run pytest -q -s tests/test_k6_phase_c_postgres.py" in k6_c
    assert "artifacts/k6-phase-c-startup" in k6_c
    assert "path: artifacts/" in fixtures[cleanup[1]]
    assert (
        services["Run service-backed checks"]
        .rstrip()
        .endswith("run: ./scripts/ci.ps1 -WithServices -KafkaPythonOnly")
    )
    assert (
        race["Run service-backed race and sink contract checks"]
        .rstrip()
        .endswith("run: ./scripts/ci.ps1 -WithServices -RaceOnly")
    )
    entrypoint = (ROOT / "scripts/ci.ps1").read_text(encoding="utf-8")
    shared = (ROOT / "scripts/check.ps1").read_text(encoding="utf-8")
    checks = dict(_job_steps(workflow, "checks"))
    assert checks["Run repository checks"].rstrip().endswith("run: ./scripts/check.ps1")
    assert "-SerialPackages -KafkaPythonOnly:$KafkaPythonOnly" in entrypoint
    assert "::test_real_kafka_assignment_during_long_activity" in shared
    assert "} else {\n        & uv run pytest\n    }" in shared
    assert "go test -race -p 1 ./..." in entrypoint
    assert "if ($WithRace -or $RaceOnly) { $sinkTestArgs += '-race' }" in entrypoint
    assert "go @sinkTestArgs ./internal/effects/k6sink -run 'K6Sink'" in entrypoint
    assert entrypoint.index("if ($RaceOnly) { return }") < entrypoint.index("/smoke.ps1")


def test_mutation_jobs_are_parallel_isolated_and_keep_both_complete_gates() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    jobs = dict(re.findall(r"(?ms)^  ([\w-]+):\n(.*?)(?=^  [\w-]+:\n|\Z)", workflow))
    matrix = "with-k6-a, with-k6-b, with-k6-c, without-k6-a, without-k6-b"
    for platform, shell, output in (
        ("powershell", "PowerShell", "powershell"),
        ("linux", "Linux", "bash"),
    ):
        job = jobs[f"mutation-{platform}"]
        assert f"    name: {shell} mutation gate (${{{{ matrix.shard }}}})\n" in job
        assert f"        shard: [{matrix}]\n" in job
        assert "      fail-fast: false\n" in job
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
            < names.index("Select mutation cases")
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
        selector = by_name["Select mutation cases"]
        assert f"mutation_shards.py select {platform} ${{{{ matrix.shard }}}}" in selector
        assert '$RUNNER_TEMP/mutation-shard.json"' in selector
        gate = by_name[gate_name]
        assert "timeout-minutes: 75" in gate
        assert f"dur046_mutation_{platform}" in gate
        assert "DURABLE_MUTATION_DATABASE_URL" in gate
        assert "CREATE DATABASE" in gate and "migrations" in gate
        if platform == "powershell":
            assert "shell: pwsh" in gate
            assert "./scripts/mutation-gate.ps1 -Output mutations/results-powershell.json" in gate
            assert '-Manifest (Join-Path $env:RUNNER_TEMP "mutation-shard.json")' in gate
        else:
            assert "shell: bash" in gate
            assert (
                "bash scripts/mutation-gate.sh mutations/results-bash.json "
                '"$RUNNER_TEMP/mutation-shard.json"' in gate
            )
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
        suffix = "${{ matrix.shard }}-${{ github.run_id }}-${{ github.run_attempt }}"
        assert f"name: dur046-mutation-results-{platform}-{suffix}" in upload
        assert f"path: mutations/results-{output}.json" in upload
        assert "if-no-files-found: error" in upload
        assert (
            f"name: mutation-{platform}-diagnostics-{suffix}"
            in by_name["Upload service diagnostics"]
        )
        assert "down --volumes --remove-orphans" in by_name["Tear down the isolated service stack"]
    coverage = jobs["mutation-coverage"]
    assert "    needs: [mutation-linux, mutation-powershell]\n" in coverage
    assert "    if: always()\n" in coverage
    steps = dict(_job_steps(workflow, "mutation-coverage"))
    download = steps["Download mutation shard reports"]
    assert "uses: actions/download-artifact@v4" in download
    assert "dur046-mutation-results-*-${{ github.run_id }}-${{ github.run_attempt }}" in download
    verify = steps["Verify both complete mutation gates"]
    assert (
        'test "$LINUX_RESULT" = success' in verify
        and 'test "$POWERSHELL_RESULT" = success' in verify
    )
    assert "mutation_shards.py verify" in verify and "$(git rev-parse HEAD)" in verify
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
        if "${{ matrix.shard }}" in name.group(1):
            matrix = re.search(r"^        shard: \[([\w, -]+)\]$", job, re.MULTILINE)
            assert matrix is not None
            names.update(
                name.group(1).replace("${{ matrix.shard }}", shard)
                for shard in matrix.group(1).split(", ")
            )
        else:
            names.add(name.group(1))
    assert names == REQUIRED_CI_JOBS


def test_phase_g_kind_smoke_runs_in_parallel_and_keeps_exact_head() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    job = workflow.split("\n  k6-phase-g-smoke:\n", 1)[1].split(
        "\n  dur053-k4-lifecycle-smoke:", 1
    )[0]
    assert "    timeout-minutes: 60\n" in job
    assert "    runs-on: ubuntu-24.04\n" in job
    assert "    if: github.event_name == 'pull_request' || github.event_name == 'push'\n" in job
    assert "    needs:" not in job and "continue-on-error" not in job
    assert "DUR053_HEAD_SHA: ${{ github.event.pull_request.head.sha || github.sha }}" in job
    steps = dict(_job_steps(workflow, "k6-phase-g-smoke"))
    run = steps["Exercise deployed Phase G collection and replay"]
    assert "-Context kind -Scenario faults-smoke -K6Phase G" in run
    assert '$env:RUNNER_TEMP "dur053-k6-phase-g-kind"' in run
    upload = steps["Retain Phase G kind evidence including failures"]
    assert "if: always()" in upload and "${{ runner.temp }}/dur053-k6-phase-g-kind/" in upload


def test_k4_engineering_workflow_is_uncharged_and_retains_failures() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    steps = dict(_job_steps(workflow, "dur053-k4-lifecycle-smoke"))
    run = steps["Exercise lifecycle engineering smoke"]
    assert "lifecycle-smoke" in run
    assert "$RUNNER_TEMP/dur053-k4" in run
    assert "4800s" in run
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
