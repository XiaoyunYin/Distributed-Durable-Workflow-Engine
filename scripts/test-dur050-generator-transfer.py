#!/usr/bin/env python3
"""Exercise DUR-050 dispatch and chunked retrieval end to end without AWS."""

from __future__ import annotations

import atexit
import hashlib
import http.server
import json
import os
import shutil
import subprocess
import tempfile
import threading
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BIN_DIR = Path(os.environ.get("DUR050_TEST_BIN_DIR", ROOT / "bin")).resolve()
CHUNK_LIMIT = 24_000


class WorkflowAPI(http.server.ThreadingHTTPServer):
    def __init__(self, address: tuple[str, int], submissions: Path):
        super().__init__(address, WorkflowHandler)
        self.submissions = submissions


class WorkflowHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        if self.path != "/v1/workflows":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        workflow_id = json.loads(self.rfile.read(length))["workflow_id"]
        with self.server.submissions.open("a", encoding="utf-8") as stream:  # type: ignore[attr-defined]
            stream.write(workflow_id + "\n")
        body = json.dumps({"created": True, "workflow": {"workflow_id": workflow_id}}).encode()
        self.send_response(201)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args: object) -> None:
        return


AWS_STUB = r"""#!/usr/bin/env python3
import base64, json, os, re, subprocess, sys, uuid
from pathlib import Path
args = sys.argv[1:]
ssm_index = args.index("ssm") if "ssm" in args else -1
ssm_operation = args[ssm_index + 1] if ssm_index >= 0 and ssm_index + 1 < len(args) else ""
if [ssm_operation] == ["get-parameter"]:
    print("observer-password")
    raise SystemExit(0)
if ssm_operation == "send-command":
    input_arg = args[args.index("--cli-input-json") + 1]
    request = json.loads(Path(input_arg.removeprefix("file://")).read_text())
    expected_timeout = os.environ.get("DUR050_EXPECTED_EXECUTION_TIMEOUT")
    actual_timeout = request.get("Parameters", {}).get("executionTimeout")
    if expected_timeout and actual_timeout != [expected_timeout]:
        print(
            "SSM executionTimeout mismatch: " + repr(actual_timeout),
            file=sys.stderr,
        )
        raise SystemExit(3)
    command = request["Parameters"]["commands"][0]
    command_id = "cmd-" + uuid.uuid4().hex
    env = os.environ.copy()
    completed = subprocess.run(
        ["bash", "-c", command],
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=180,
    )
    stdout = completed.stdout[:24000]
    scenario = os.environ.get("DUR050_STUB_TRANSFER_SCENARIO", "")
    if "DUR050_ARCHIVE_V1" in stdout and scenario == "bad-hash":
        stdout = re.sub(
            r"(?m)(DUR050_ARCHIVE_V1 size=\d+ sha256=)[0-9a-f]{64}",
            r"\g<1>" + "0" * 64,
            stdout,
        )
    if "DUR050_CHUNK_V1" in stdout and scenario == "missing-marker":
        stdout = re.sub(r"(?m)^DUR050_CHUNK_V1 .*\n?", "", stdout)
    elif "DUR050_CHUNK_V1" in stdout and scenario == "truncated-chunk":
        match = re.search(
            r"(?m)^(DUR050_CHUNK_V1 index=\d+ offset=\d+ length=\d+ data=)([A-Za-z0-9+/=]+)$",
            stdout,
        )
        if match:
            raw = base64.b64decode(match.group(2))[:-1]
            stdout = (
                stdout[: match.start(2)]
                + base64.b64encode(raw).decode()
                + stdout[match.end(2) :]
            )
    invocation = {
        "Status": "Success" if completed.returncode == 0 else "Failed",
        "StandardOutputContent": stdout,
        "StandardErrorContent": completed.stderr[:24000],
        "ResponseCode": completed.returncode}
    state = Path(os.environ["DUR050_FAKE_SSM_STATE"])
    state.mkdir(parents=True, exist_ok=True)
    (state / (command_id + ".json")).write_text(json.dumps(invocation))
    archive = re.search(r"(?m)^archive='([^']+)'$", command)
    if archive:
        (state / (command_id + ".archive-path")).write_text(archive.group(1))
    print(json.dumps({"Command": {"CommandId": command_id}}))
    raise SystemExit(0)
if ssm_operation == "get-command-invocation":
    command_id = args[args.index("--command-id") + 1]
    invocation_path = Path(os.environ["DUR050_FAKE_SSM_STATE"]) / (command_id + ".json")
    invocation = json.loads(invocation_path.read_text())
    print(json.dumps(invocation))
    raise SystemExit(0)
print("unexpected AWS stub invocation: " + repr(args), file=sys.stderr)
raise SystemExit(2)
"""

OBSERVER_STUB = r"""#!/usr/bin/env python3
import csv, os, sys, time
from datetime import datetime, timezone
args = sys.argv[1:]
workflow_id = args[args.index("-workflow-id") + 1]
output = args[args.index("-output") + 1]
columns = os.environ["DUR050_TEST_OBSERVER_COLUMNS"].split(",")
submissions = os.environ["DUR050_TEST_SUBMISSIONS"]
def row(record_type):
    values = {key: "" for key in columns}
    values.update(record_type=record_type, workflow_id=workflow_id, sequence="1",
        scheduled_at_utc=datetime.now(timezone.utc).isoformat(),
        observed_at_utc=datetime.now(timezone.utc).isoformat(), state="SUCCEEDED",
        valid="true", reason="queries=1", observer_qps="1",
        scheduled_at_monotonic_ns=str(time.monotonic_ns()),
        observed_at_monotonic_ns=str(time.monotonic_ns()), completion_marker_seen="false")
    return values
with open(output, "x", newline="", encoding="utf-8") as stream:
    writer = csv.DictWriter(stream, fieldnames=columns)
    writer.writeheader(); writer.writerow(row("poll")); stream.flush()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            if workflow_id in open(submissions, encoding="utf-8").read().splitlines():
                writer.writerow(row("first_terminal_observation"))
                writer.writerow(row("summary"))
                stream.flush()
                sys.exit(0)
        except FileNotFoundError:
            pass
        time.sleep(0.002)
sys.exit(1)
"""


def run(
    command: list[str], *, env: dict[str, str], timeout: int = 240
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
    )


def require_ok(result: subprocess.CompletedProcess[str], label: str) -> None:
    if result.returncode:
        raise RuntimeError(f"{label} failed ({result.returncode}):\n{result.stdout}")


def load_json_output(result: subprocess.CompletedProcess[str], label: str) -> dict:
    require_ok(result, label)
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{label} did not emit JSON: {result.stdout}") from exc


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def tree_digest(root: Path) -> dict[str, str]:
    return {p.relative_to(root).as_posix(): sha(p) for p in root.rglob("*") if p.is_file()}


def assert_complete_unloaded_block(output_dir: Path, submissions: Path) -> None:
    lines = sum(1 for _ in (output_dir / "unloaded-latency.csv").open(encoding="utf-8"))
    summaries = list(output_dir.glob("runs/*/submission.csv.summary.json"))
    passing = sum(
        json.loads(path.read_text(encoding="utf-8")).get("status") == "PASS" for path in summaries
    )
    summary = (output_dir / "block-summary.txt").read_text(encoding="utf-8")
    posted = len(submissions.read_text(encoding="utf-8").splitlines())
    if (
        lines != 101
        or len(summaries) != 100
        or passing != 100
        or "samples=100" not in summary
        or posted != 100
    ):
        raise AssertionError(
            "dispatch did not execute a complete 100-sample block: "
            f"lines={lines}, summaries={len(summaries)}, pass={passing}, "
            f"API posts={posted}, summary={summary}"
        )


def assert_retrieval_failure(
    result, destination: Path, record_path: Path, reason: str, label: str
) -> None:
    if result.returncode == 0 or reason not in result.stdout:
        raise AssertionError(
            f"{label} did not fail at its expected guard ({reason!r}):\n{result.stdout}"
        )
    if (
        destination.exists()
        or not record_path.exists()
        or json.loads(record_path.read_text(encoding="utf-8")).get("status") != "FAIL"
    ):
        raise AssertionError(f"{label} extracted data or omitted its FAIL record")


def assert_config_hash_failure(result, output_dir: Path, record_path: Path, label: str) -> None:
    if result.returncode == 0 or output_dir.exists():
        raise AssertionError(
            f"{label} failed to stop before running the workload:\n{result.stdout}"
        )
    if not record_path.exists():
        raise AssertionError(f"{label} did not persist a dispatch failure record")
    record = json.loads(record_path.read_text(encoding="utf-8"))
    if record.get("status") != "FAIL" or not record.get("ssm_command_id"):
        raise AssertionError(f"{label} failure record lacks FAIL status/SSM command ID: {record}")


def validate_dispatch_plan(
    plan: dict, remote: Path, config: Path, output: str, cycle_id: str, observer_parameter: str
) -> None:
    runner = remote / "scripts/dur050-run-unloaded-block.sh"
    block_id = f"{cycle_id}-01"
    expected_lines = [
        "set -euo pipefail",
        f"export DUR050_OBSERVER_SECRET_PARAMETER='{observer_parameter}'",
        "export DUR050_DATABASE_PRIVATE_IP='10.49.0.10'",
        "export DUR050_DATABASE_NAME='durable'",
        f"test \"$(sha256sum -- '{config}' | cut -d ' ' -f1)\" = '{sha(config)}'",
        f"bash '{runner}' '{config}' '{block_id}' 'seq-8' '{output}'",
    ]
    if (
        plan["remote_lines"] != expected_lines
        or plan["instance_id"] != "i-33333333333333333"
        or plan.get("cycle_id") != cycle_id
    ):
        raise AssertionError(f"dispatch plan does not match expected exports/runner argv: {plan}")


def invoke_ps(
    script: Path, args: list[str], env: dict[str, str], timeout: int = 240
) -> subprocess.CompletedProcess[str]:
    pwsh = shutil.which("pwsh")
    if not pwsh:
        raise RuntimeError("pwsh is required for the DUR-050 transfer tests")
    return run([pwsh, "-NoProfile", "-File", str(script), *args], env=env, timeout=timeout)


def main() -> int:
    if os.name != "posix" or not shutil.which("bash"):
        raise SystemExit("DUR-050 generator transfer test requires the Ubuntu/Linux CI runner")
    pwsh = shutil.which("pwsh")
    if not pwsh:
        raise SystemExit("pwsh is required")
    required_binaries = [
        BIN_DIR / "dur050-loadgen",
        BIN_DIR / "dur050-observer",
        BIN_DIR / "dur050-sink",
    ]
    if not all(path.is_file() and os.access(path, os.X_OK) for path in required_binaries):
        raise SystemExit("CI-built DUR-050 generator binaries are required")

    with tempfile.TemporaryDirectory(prefix="dur050-generator-transfer-") as tmp_text:
        tmp = Path(tmp_text)
        remote = tmp / "remote-repo"
        (remote / "scripts").mkdir(parents=True)
        (remote / "bin").mkdir()
        shutil.copy2(
            ROOT / "scripts/dur050-run-unloaded-block.sh",
            remote / "scripts/dur050-run-unloaded-block.sh",
        )
        shutil.copy2(
            ROOT / "scripts/dur050-append-observer-rows.py",
            remote / "scripts/dur050-append-observer-rows.py",
        )
        shutil.copy2(BIN_DIR / "dur050-loadgen", remote / "bin/dur050-loadgen")
        observer_path = remote / "bin/dur050-observer"
        observer_path.write_text(OBSERVER_STUB, encoding="utf-8")
        observer_path.chmod(0o755)

        submissions = tmp / "submissions.txt"
        api = WorkflowAPI(("127.0.0.1", 0), submissions)
        server_thread = threading.Thread(target=api.serve_forever, daemon=True)
        server_thread.start()

        env = os.environ.copy()
        env.pop("HOME", None)
        env.update(
            {
                "DUR050_ENABLE_TEST_HOOKS": "1",
                "DUR050_REMOTE_REPO_ROOT": str(remote),
                "DUR050_TEST_API_URL": f"http://127.0.0.1:{api.server_port}/",
                "DUR050_TEST_OBSERVER_COLUMNS": subprocess.check_output(
                    [str(BIN_DIR / "dur050-observer"), "-print-csv-header"], text=True
                ).strip(),
                "DUR050_TEST_SUBMISSIONS": str(submissions),
                "DUR050_FAKE_SSM_STATE": str(tmp / "ssm-state"),
                "DUR050_STUB_TRANSFER_SCENARIO": "",
                "DUR050_EXPECTED_EXECUTION_TIMEOUT": "5400",
                "PATH": str(tmp / "bin") + os.pathsep + os.environ.get("PATH", ""),
            }
        )
        fake_bin = Path(env["PATH"].split(os.pathsep, 1)[0])
        fake_bin.mkdir()
        aws_stub = fake_bin / "aws"
        aws_stub.write_text(AWS_STUB, encoding="utf-8")
        aws_stub.chmod(0o755)

        outputs = ROOT / "tests/fixtures/dur050-terraform-outputs.json"
        terraform_fixture = json.loads(outputs.read_text(encoding="utf-8"))
        observer_parameter = terraform_fixture["dur050_observer_secret_parameter_name"]["value"]
        if observer_parameter != "/durable-engine-dur050/dur050-observer-password":
            raise RuntimeError(
                "Terraform outputs fixture no longer carries the "
                "provider-defined observer parameter."
            )
        terraform_test = (ROOT / "deploy/aws/dur050.tftest.hcl").read_text(encoding="utf-8")
        if (
            f'aws_ssm_parameter.dur050_observer_password[0].name == "{observer_parameter}"'
            not in terraform_test
        ):
            raise RuntimeError(
                "Terraform output fixture parameter is not tied to the Terraform test assertion."
            )

        preparation = tmp / "preparation-dry-run.json"
        cycle_id = "ci-" + uuid.uuid4().hex[:16]
        prepare_result = invoke_ps(
            ROOT / "scripts/dur050-prepare-pilot.ps1",
            [
                "-TerraformOutputsPath",
                str(outputs),
                "-CycleID",
                cycle_id,
                "-TestOutputRoot",
                str(tmp / "prepare-output"),
                "-DryRun",
                "-DryRunOutputPath",
                str(preparation),
            ],
            env,
        )
        require_ok(prepare_result, "producer-generated preparation dry run")
        prepared = json.loads(preparation.read_text(encoding="utf-8"))
        if prepared.get("cycle_id") != cycle_id:
            raise RuntimeError("preparation producer omitted or changed the requested cycle_id")
        generator_stage = next(
            stage for stage in prepared["stages"] if stage["stage"] == "generator-stage"
        )
        config = Path(generator_stage["rendered_config_path"])
        expected_config = Path(f"/var/tmp/dur050-{cycle_id}/frozen-config.json")
        if config != expected_config or config.parent.exists():
            raise RuntimeError(
                f"producer config path is not the expected fresh staging path: {config}"
            )
        atexit.register(shutil.rmtree, config.parent, ignore_errors=True)
        config.parent.mkdir(parents=True, exist_ok=False)
        config.write_bytes(generator_stage["rendered_config_json"].encode("utf-8"))
        if sha(config) != generator_stage["rendered_config_sha256"]:
            raise RuntimeError(
                "producer-rendered config bytes do not match the producer's recorded hash"
            )

        dispatch_script = ROOT / "scripts/dur050-run-generator-block.ps1"
        dispatch_args = [
            "-TerraformOutputsPath",
            str(outputs),
            "-PreparationDryRunPath",
            str(preparation),
            "-CycleID",
            cycle_id,
            "-Mode",
            "unloaded",
            "-DatabaseName",
            "durable",
            "-BlockID",
            f"{cycle_id}-01",
            "-Family",
            "seq-8",
            "-OutputDirectory",
            f"/var/tmp/dur050-dispatch-{uuid.uuid4().hex}",
            "-TimeoutMinutes",
            "90",
        ]

        plan = load_json_output(
            invoke_ps(dispatch_script, dispatch_args + ["-StagePlanOnly"], env),
            "dispatch StagePlanOnly",
        )
        validate_dispatch_plan(
            plan,
            remote,
            config,
            dispatch_args[dispatch_args.index("-OutputDirectory") + 1],
            cycle_id,
            observer_parameter,
        )
        print(
            "PASS: StagePlanOnly emits the exact exports, config hash guard, "
            "and unloaded runner argv."
        )

        dispatch_source = dispatch_script.read_text(encoding="utf-8")
        old_parameter_pattern = "^/[A-Za-z0-9_.-]+/dur050-observer-password$"
        if old_parameter_pattern not in dispatch_source:
            raise RuntimeError("observer-name regression mutation no longer matches the dispatcher")
        parameter_mutant_dir = tmp / "mutant-old-observer-shape"
        parameter_mutant_dir.mkdir()
        parameter_mutant = parameter_mutant_dir / "dur050-run-generator-block.ps1"
        parameter_mutant.write_text(
            dispatch_source.replace(old_parameter_pattern, "^/dur050/[A-Za-z0-9_./-]{1,180}$", 1),
            encoding="utf-8",
        )
        shutil.copy2(
            ROOT / "scripts/dur050-ssm-wrapper.ps1",
            parameter_mutant_dir / "dur050-ssm-wrapper.ps1",
        )
        old_parameter_result = invoke_ps(parameter_mutant, dispatch_args + ["-StagePlanOnly"], env)
        if old_parameter_result.returncode == 0:
            raise RuntimeError(
                "the prior /dur050-only guard unexpectedly accepts real Terraform outputs"
            )
        print(
            "NEGATIVE CONTROL: reverting to the old observer-parameter shape "
            "rejects the real Terraform output."
        )

        old_path_block = (
            '$expectedConfigPath = "/var/tmp/dur050-$CycleID/frozen-config.json"\n'
            "if ($configPath -cne $expectedConfigPath) {\n"
            "    throw 'Recorded rendered_config_path must be exactly the "
            "preparation staging path for CycleID.'\n"
            "}\n$repoRoot = Get-RemoteRepositoryRoot"
        )
        old_path_guard = (
            "$repoRoot = Get-RemoteRepositoryRoot\n"
            "if ($configPath -notmatch ('^' + [regex]::Escape($repoRoot) + '/[A-Za-z0-9._/-]+$') "
            "-or $configPath -match '(^|/)\\.\\.?(/|$)') {\n"
            "    throw 'Recorded rendered_config_path must be a normalized file "
            "below the deployed repository root.'\n"
            "}"
        )
        if old_path_block not in dispatch_source:
            raise RuntimeError("config-path regression mutation no longer matches the dispatcher")
        path_mutant_dir = tmp / "mutant-old-config-path"
        path_mutant_dir.mkdir()
        path_mutant = path_mutant_dir / "dur050-run-generator-block.ps1"
        path_mutant.write_text(
            dispatch_source.replace(old_path_block, old_path_guard, 1), encoding="utf-8"
        )
        shutil.copy2(
            ROOT / "scripts/dur050-ssm-wrapper.ps1", path_mutant_dir / "dur050-ssm-wrapper.ps1"
        )
        old_path_result = invoke_ps(path_mutant, dispatch_args + ["-StagePlanOnly"], env)
        if old_path_result.returncode == 0:
            raise RuntimeError(
                "the prior repository-root guard unexpectedly accepts the real prepared config path"
            )
        print(
            "NEGATIVE CONTROL: reverting to the old repository-root rule "
            "rejects the producer staging path."
        )

        # The dispatch regression assertion must detect a removed environment export.
        mutant_dispatch_dir = tmp / "mutant-dispatch"
        mutant_dispatch_dir.mkdir()
        mutant_dispatch = mutant_dispatch_dir / "dur050-run-generator-block.ps1"
        dispatch_source = dispatch_script.read_text(encoding="utf-8")
        needle = (
            '    "export DUR050_DATABASE_NAME=$(ConvertTo-SafeBashWord $DatabaseName '
            "'Database name')\",\n"
        )
        if needle not in dispatch_source:
            raise RuntimeError(
                "dispatch export mutation fixture no longer matches the implementation"
            )
        mutant_dispatch.write_text(dispatch_source.replace(needle, "", 1), encoding="utf-8")
        shutil.copy2(
            ROOT / "scripts/dur050-ssm-wrapper.ps1", mutant_dispatch_dir / "dur050-ssm-wrapper.ps1"
        )
        mutated = load_json_output(
            invoke_ps(mutant_dispatch, dispatch_args + ["-StagePlanOnly"], env),
            "mutated dispatch plan",
        )
        try:
            validate_dispatch_plan(
                mutated,
                remote,
                config,
                dispatch_args[dispatch_args.index("-OutputDirectory") + 1],
                cycle_id,
                observer_parameter,
            )
        except AssertionError:
            print("NEGATIVE CONTROL: dispatch assertion rejects a removed database-name export.")
        else:
            raise RuntimeError(
                "dispatch regression assertion survived removal of a required export"
            )

        for mode_args, script_name, tail_args in (
            (
                [
                    "-Mode",
                    "window",
                    "-RunID",
                    "window-ci",
                    "-Rate",
                    "2",
                    "-RunMode",
                    "count",
                    "-Value",
                    "4",
                ],
                "dur050-run-window.sh",
                "'window-ci' '2' 'count' '4'",
            ),
            (["-Mode", "sink-check"], "dur050-run-sink-check.sh", ""),
        ):
            mode_output = f"/var/tmp/dur050-{uuid.uuid4().hex}"
            args = [
                "-TerraformOutputsPath",
                str(outputs),
                "-PreparationDryRunPath",
                str(preparation),
                "-CycleID",
                cycle_id,
                *mode_args,
                "-DatabaseName",
                "durable",
                "-OutputDirectory",
                mode_output,
                "-TimeoutMinutes",
                "2",
            ]
            mode_plan = load_json_output(
                invoke_ps(dispatch_script, args + ["-StagePlanOnly"], env),
                f"{mode_args[1]} StagePlanOnly",
            )
            tail = f" {tail_args}" if tail_args else ""
            expected_runner = (
                f"bash '{remote}/scripts/{script_name}' '{config}'{tail} '{mode_output}'"
            )
            if mode_plan["remote_lines"][-1] != expected_runner:
                raise RuntimeError(f"{mode_args[1]} dispatch argv is incorrect: {mode_plan}")
        print("PASS: window and sink-check StagePlanOnly use their expected runner argv.")

        wrong_cycle_args = dispatch_args.copy()
        wrong_cycle_args[wrong_cycle_args.index("-CycleID") + 1] = "different-cycle"
        wrong_cycle = invoke_ps(dispatch_script, wrong_cycle_args + ["-StagePlanOnly"], env)
        if (
            wrong_cycle.returncode == 0
            or "Preparation cycle_id does not match CycleID" not in wrong_cycle.stdout
        ):
            raise RuntimeError(
                f"dispatch did not reject a preparation cycle mismatch:\n{wrong_cycle.stdout}"
            )
        print("PASS: a cycle-ID mismatch is rejected before dispatch.")

        wrong_path_record = json.loads(json.dumps(prepared))
        wrong_path_stage = next(
            stage for stage in wrong_path_record["stages"] if stage["stage"] == "generator-stage"
        )
        wrong_path_stage["rendered_config_path"] = f"/opt/dur050/{cycle_id}/frozen-config.json"
        wrong_path_file = tmp / "preparation-wrong-config-path.json"
        wrong_path_file.write_text(json.dumps(wrong_path_record), encoding="utf-8")
        wrong_path_args = dispatch_args.copy()
        wrong_path_args[wrong_path_args.index("-PreparationDryRunPath") + 1] = str(wrong_path_file)
        wrong_path_result = invoke_ps(dispatch_script, wrong_path_args + ["-StagePlanOnly"], env)
        if (
            wrong_path_result.returncode == 0
            or "must be exactly the preparation staging path" not in wrong_path_result.stdout
        ):
            raise RuntimeError(
                "dispatch did not reject a generator config path outside the cycle staging path:\n"
                + wrong_path_result.stdout
            )
        print("PASS: a non-producer generator config path is rejected before dispatch.")

        bad_outputs = json.loads(outputs.read_text(encoding="utf-8"))
        invalid_cases = (
            ("instance-id", "load_generator_instance_ids", ["i-not-an-instance"]),
            ("public-db-address", "dependency_private_ip", "203.0.113.7"),
            ("secret-parameter", "dur050_observer_secret_parameter_name", "not-a-parameter"),
        )
        for case, key, value in invalid_cases:
            changed = json.loads(json.dumps(bad_outputs))
            changed[key]["value"] = value
            bad_file = tmp / f"outputs-{case}.json"
            bad_file.write_text(json.dumps(changed), encoding="utf-8")
            args = dispatch_args.copy()
            args[args.index("-TerraformOutputsPath") + 1] = str(bad_file)
            rejected = invoke_ps(dispatch_script, args + ["-StagePlanOnly"], env)
            if rejected.returncode == 0:
                raise RuntimeError(f"dispatch accepted malformed Terraform output: {case}")
        for parameter, value in (
            ("-DatabaseName", "bad;name"),
            ("-OutputDirectory", "/tmp/unsafe"),
        ):
            args = dispatch_args.copy()
            args[args.index(parameter) + 1] = value
            rejected = invoke_ps(dispatch_script, args + ["-StagePlanOnly"], env)
            if rejected.returncode == 0:
                raise RuntimeError(f"dispatch accepted invalid {parameter}={value}")
        print("PASS: malformed outputs, database name, and output path fail before dispatch.")

        # Each input-validation assertion is mutation-checked by removing only its guard.
        validation_mutations = (
            (
                "generator-instance-id",
                "Terraform must contain exactly one valid DUR-050 generator instance ID.",
                "load_generator_instance_ids",
                ["i-not-an-instance"],
                None,
            ),
            (
                "private-database-address",
                "Terraform dependency_private_ip must be RFC1918 private IPv4.",
                "dependency_private_ip",
                "203.0.113.7",
                None,
            ),
            (
                "observer-parameter",
                "Terraform observer secret parameter must be a normalized "
                "/<campaign>/dur050-observer-password SSM parameter name.",
                "dur050_observer_secret_parameter_name",
                "not-a-parameter",
                None,
            ),
            (
                "database-name",
                "DatabaseName must be a valid simple PostgreSQL identifier.",
                None,
                None,
                ("-DatabaseName", "bad;name"),
            ),
            (
                "output-directory",
                "OutputDirectory must be a normalized path below /var/tmp/dur050-*.",
                None,
                None,
                ("-OutputDirectory", "/tmp/unsafe"),
            ),
        )
        for label, message, output_key, bad_value, argument_override in validation_mutations:
            mutation_dir = tmp / f"mutant-validation-{label}"
            mutation_dir.mkdir()
            mutant_script = mutation_dir / "dur050-run-generator-block.ps1"
            source = dispatch_script.read_text(encoding="utf-8")
            guard = f"throw '{message}'"
            if guard not in source:
                raise RuntimeError(
                    f"{label} validator mutation fixture no longer matches implementation"
                )
            mutant_script.write_text(source.replace(guard, "", 1), encoding="utf-8")
            shutil.copy2(
                ROOT / "scripts/dur050-ssm-wrapper.ps1", mutation_dir / "dur050-ssm-wrapper.ps1"
            )
            mutant_args = dispatch_args.copy()
            if output_key:
                changed_outputs = json.loads(outputs.read_text(encoding="utf-8"))
                changed_outputs[output_key]["value"] = bad_value
                changed_path = tmp / f"mutated-{label}-outputs.json"
                changed_path.write_text(json.dumps(changed_outputs), encoding="utf-8")
                mutant_args[mutant_args.index("-TerraformOutputsPath") + 1] = str(changed_path)
                changed_preparation = json.loads(preparation.read_text(encoding="utf-8"))
                changed_preparation["terraform_outputs_sha256"] = sha(changed_path)
                if label == "generator-instance-id":
                    changed_stage = next(
                        stage
                        for stage in changed_preparation["stages"]
                        if stage["stage"] == "generator-stage"
                    )
                    changed_stage["instance_id"] = bad_value[0]
                changed_preparation_path = tmp / f"mutated-{label}-preparation.json"
                changed_preparation_path.write_text(
                    json.dumps(changed_preparation), encoding="utf-8"
                )
                mutant_args[mutant_args.index("-PreparationDryRunPath") + 1] = str(
                    changed_preparation_path
                )
            if argument_override:
                mutant_args[mutant_args.index(argument_override[0]) + 1] = argument_override[1]
            mutant = invoke_ps(mutant_script, mutant_args + ["-StagePlanOnly"], env)
            if mutant.returncode != 0:
                raise RuntimeError(
                    f"removing {label} validation failed for an unrelated reason:\n{mutant.stdout}"
                )
            print(
                f"NEGATIVE CONTROL: the {label} invalid-input assertion rejects its removed guard."
            )
            shutil.rmtree(mutation_dir, ignore_errors=True)

        output_dir = dispatch_args[dispatch_args.index("-OutputDirectory") + 1]
        record = tmp / "dispatch.json"
        actual = load_json_output(
            invoke_ps(dispatch_script, dispatch_args + ["-RecordPath", str(record)], env),
            "actual unloaded dispatch",
        )
        if (
            actual["status"] != "PASS"
            or not actual["ssm_command_id"]
            or actual["ssm_status"] != "Success"
        ):
            raise RuntimeError(f"dispatch record lacks successful SSM evidence: {actual}")
        remote_block = Path(output_dir)
        assert_complete_unloaded_block(remote_block, submissions)
        print("PASS: SSM-stub dispatch ran 100 samples: 101 CSV lines and 100 PASS summaries.")
        print("PASS: SSM request sets executionTimeout to 5,400 seconds for a 90-minute dispatch.")

        bad_preparation = json.loads(preparation.read_text(encoding="utf-8"))
        prepared_generator_stage = next(
            stage for stage in bad_preparation["stages"] if stage["stage"] == "generator-stage"
        )
        prepared_generator_stage["rendered_config_sha256"] = "0" * 64
        bad_preparation_path = tmp / "preparation-bad-hash.json"
        bad_preparation_path.write_text(json.dumps(bad_preparation), encoding="utf-8")
        mismatch_output = f"/var/tmp/dur050-config-mismatch-{uuid.uuid4().hex}"
        mismatch_record = tmp / "dispatch-config-mismatch.json"
        mismatch_args = dispatch_args.copy()
        mismatch_args[mismatch_args.index("-PreparationDryRunPath") + 1] = str(bad_preparation_path)
        mismatch_args[mismatch_args.index("-OutputDirectory") + 1] = mismatch_output
        mismatch_result = invoke_ps(
            dispatch_script,
            mismatch_args + ["-RecordPath", str(mismatch_record)],
            env,
        )
        assert_config_hash_failure(
            mismatch_result, Path(mismatch_output), mismatch_record, "rendered config hash mismatch"
        )
        print(
            "PASS: remote config hash mismatch fails before the runner and "
            "preserves command evidence."
        )

        # Removing the remote hash check must make the prepared-hash regression fail.
        dispatch_source = dispatch_script.read_text(encoding="utf-8")
        hash_lines = [
            line for line in dispatch_source.splitlines(keepends=True) if "sha256sum -- {0}" in line
        ]
        if len(hash_lines) != 1:
            raise RuntimeError("remote rendered-config hash check mutation fixture is ambiguous")
        mutant_dispatch_dir = tmp / "mutant-config-hash"
        mutant_dispatch_dir.mkdir()
        mutant_dispatch = mutant_dispatch_dir / "dur050-run-generator-block.ps1"
        mutant_dispatch.write_text(dispatch_source.replace(hash_lines[0], "", 1), encoding="utf-8")
        shutil.copy2(
            ROOT / "scripts/dur050-ssm-wrapper.ps1", mutant_dispatch_dir / "dur050-ssm-wrapper.ps1"
        )
        shutil.copy2(
            ROOT / "scripts/dur050-invoke-ssm-command.ps1",
            mutant_dispatch_dir / "dur050-invoke-ssm-command.ps1",
        )
        submissions.write_text("", encoding="utf-8")
        mutant_output = f"/var/tmp/dur050-mutated-config-hash-{uuid.uuid4().hex}"
        mutant_args = mismatch_args.copy()
        mutant_args[mutant_args.index("-OutputDirectory") + 1] = mutant_output
        mutant_dispatch_record = tmp / "dispatch-mutated-config-hash.json"
        mutant_result = invoke_ps(
            mutant_dispatch,
            mutant_args + ["-RecordPath", str(mutant_dispatch_record)],
            env,
        )
        if mutant_result.returncode != 0:
            raise RuntimeError(
                f"removing the config hash guard failed unexpectedly:\n{mutant_result.stdout}"
            )
        try:
            assert_config_hash_failure(
                mutant_result,
                Path(mutant_output),
                mutant_dispatch_record,
                "mutated rendered config hash guard",
            )
        except AssertionError:
            assert_complete_unloaded_block(Path(mutant_output), submissions)
            print(
                "NEGATIVE CONTROL: removing the config hash guard lets the mismatched config run."
            )
        else:
            raise RuntimeError("rendered-config mismatch test survived removal of its hash guard")
        shutil.rmtree(Path(mutant_output), ignore_errors=True)
        shutil.rmtree(mutant_dispatch_dir, ignore_errors=True)

        # A successful SSM wrapper around a no-op must not satisfy the end-to-end test.
        remote_runner = remote / "scripts/dur050-run-unloaded-block.sh"
        original_runner = remote_runner.read_bytes()
        no_op_output = f"/var/tmp/dur050-noop-{uuid.uuid4().hex}"
        no_op_args = dispatch_args.copy()
        no_op_args[no_op_args.index("-OutputDirectory") + 1] = no_op_output
        remote_runner.write_text(
            "#!/usr/bin/env bash\nset -euo pipefail\nexit 0\n", encoding="utf-8"
        )
        try:
            no_op_result = invoke_ps(
                dispatch_script,
                no_op_args + ["-RecordPath", str(tmp / "dispatch-noop.json")],
                env,
            )
            require_ok(no_op_result, "no-op dispatch mutation execution")
            try:
                assert_complete_unloaded_block(Path(no_op_output), submissions)
            except (AssertionError, FileNotFoundError):
                print(
                    "NEGATIVE CONTROL: a no-op runner cannot satisfy the "
                    "100-sample dispatch assertion."
                )
            else:
                raise RuntimeError("dispatch integration check survived a no-op runner")
        finally:
            remote_runner.write_bytes(original_runner)
            shutil.rmtree(Path(no_op_output), ignore_errors=True)

        # Random payload bytes ensure the >100 KiB fixture needs at least five chunks.
        source = Path("/var/tmp") / f"dur050-transfer-fixture-{uuid.uuid4().hex}"
        (source / "nested").mkdir(parents=True)
        (source / "nested/random.bin").write_bytes(os.urandom(140_000))
        (source / "summary.json").write_text(
            json.dumps({"status": "PASS", "rows": 100}), encoding="utf-8"
        )
        source_tree = tree_digest(source)
        retrieval_script = ROOT / "scripts/dur050-retrieve-generator-block.ps1"

        def retrieve(
            label: str, scenario: str = "", script: Path = retrieval_script
        ) -> tuple[subprocess.CompletedProcess[str], Path, Path]:
            env["DUR050_STUB_TRANSFER_SCENARIO"] = scenario
            env["DUR050_EXPECTED_EXECUTION_TIMEOUT"] = "600"
            destination = tmp / f"retrieved-{label}"
            record_path = tmp / f"retrieval-{label}.json"
            result = invoke_ps(
                script,
                [
                    "-InstanceID",
                    "i-33333333333333333",
                    "-RemoteDirectory",
                    str(source),
                    "-DestinationPath",
                    str(destination),
                    "-RecordPath",
                    str(record_path),
                    "-ChunkBytes",
                    "16500",
                    "-PollIntervalSeconds",
                    "0",
                ],
                env,
                timeout=180,
            )
            return result, destination, record_path

        success, recovered, transfer_record_path = retrieve("ok")
        transfer_record = load_json_output(success, "positive chunked retrieval")
        if transfer_record["chunk_count"] < 5 or len(transfer_record["ssm_command_ids"]) < 6:
            raise RuntimeError(
                f"fixture did not traverse at least five sequential chunks: {transfer_record}"
            )
        if tree_digest(recovered) != source_tree:
            raise RuntimeError(
                "reassembled and extracted directory differs byte-for-byte from source"
            )
        if (
            not transfer_record_path.exists()
            or transfer_record["archive_size_bytes"] <= 100_000
            or transfer_record["status"] != "PASS"
        ):
            raise RuntimeError(
                f"transfer record does not prove a multi-chunk archive: {transfer_record}"
            )
        if transfer_record["archive_sha256"] != transfer_record["verified_local_sha256"]:
            raise RuntimeError("transfer record does not bind the verified local archive hash")
        if (
            json.loads(transfer_record_path.read_text(encoding="utf-8"))["ssm_command_ids"]
            != transfer_record["ssm_command_ids"]
        ):
            raise RuntimeError("persisted retrieval record differs from emitted record")
        print(
            f"PASS: {transfer_record['archive_size_bytes']} bytes in "
            f"{transfer_record['chunk_count']} verified chunks; tree is byte-identical."
        )

        failure_reasons = {
            "truncated-chunk": "is truncated:",
            "bad-hash": "Reassembled archive SHA-256 mismatch:",
            "missing-marker": "missing its unique transfer marker",
        }
        for scenario, reason in failure_reasons.items():
            failed, destination, record_path = retrieve(scenario, scenario)
            assert_retrieval_failure(failed, destination, record_path, reason, scenario)
            print(
                f"PASS: {scenario} is rejected without extracted output and leaves a FAIL record."
            )

        # Revert both independent digest comparisons. The bad-hash negative control must then expose
        # that the integrity test would have allowed unverified bytes to reach extraction.
        mutant_transfer_dir = tmp / "mutant-transfer"
        mutant_transfer_dir.mkdir()
        mutant_retrieval = mutant_transfer_dir / "dur050-retrieve-generator-block.ps1"
        retrieval_source = retrieval_script.read_text(encoding="utf-8")
        ps_hash_guard = (
            '    if ($localSHA -cne $remoteSHA) { throw "Reassembled archive SHA-256 '
            'mismatch: $localSHA != $remoteSHA." }'
        )
        if ps_hash_guard not in retrieval_source:
            raise RuntimeError(
                "retrieval SHA mutation fixture no longer matches the implementation"
            )
        mutant_retrieval.write_text(
            retrieval_source.replace(ps_hash_guard, "    # mutation: SHA comparison disabled", 1),
            encoding="utf-8",
        )
        shutil.copy2(
            ROOT / "scripts/dur050-ssm-wrapper.ps1", mutant_transfer_dir / "dur050-ssm-wrapper.ps1"
        )
        shutil.copy2(
            ROOT / "scripts/dur050-invoke-ssm-command.ps1",
            mutant_transfer_dir / "dur050-invoke-ssm-command.ps1",
        )
        extractor_source = (ROOT / "scripts/dur050-safe-extract.py").read_text(encoding="utf-8")
        helper_guard = (
            "    if sha256(archive) != expected_sha256.lower():\n"
            '        raise ValueError("archive SHA-256 does not match the remote manifest")\n'
        )
        if helper_guard not in extractor_source:
            raise RuntimeError(
                "safe-extract SHA mutation fixture no longer matches the implementation"
            )
        (mutant_transfer_dir / "dur050-safe-extract.py").write_text(
            extractor_source.replace(
                helper_guard, "    # mutation: helper SHA comparison disabled\n", 1
            ),
            encoding="utf-8",
        )
        mutant_result, mutant_destination, mutant_record = retrieve(
            "mutated-hash-guard", "bad-hash", mutant_retrieval
        )
        if mutant_result.returncode != 0 or not mutant_destination.is_dir():
            raise RuntimeError(
                "digest mutation did not demonstrate regression-test sensitivity: "
                f"exit={mutant_result.returncode}, output={mutant_result.stdout}"
            )
        shutil.rmtree(mutant_destination)
        mutant_record.unlink(missing_ok=True)
        print(
            "NEGATIVE CONTROL: removing both digest guards makes the bad-hash "
            "case extract, so the integrity test rejects the revert."
        )

        guard_mutations = (
            (
                "length",
                (
                    '            if ($bytes.Length -ne $length) { throw "Chunk $($index + 1) '
                    'is truncated: received $($bytes.Length), expected $length bytes." }'
                ),
                "            # mutation: chunk length guard removed",
                "truncated-chunk",
            ),
            (
                "marker",
                (
                    '            if ($matches.Count -ne 1) { throw "Chunk $($index + 1) is '
                    'missing its unique transfer marker or has inconsistent metadata." }'
                ),
                "            # mutation: marker guard removed",
                "missing-marker",
            ),
        )
        for label, needle, replacement, scenario in guard_mutations:
            if needle not in retrieval_source:
                raise RuntimeError(
                    f"{label} guard mutation fixture no longer matches implementation"
                )
            mutant_dir = tmp / f"mutant-{label}-guard"
            mutant_dir.mkdir()
            mutant_script = mutant_dir / "dur050-retrieve-generator-block.ps1"
            mutant_script.write_text(
                retrieval_source.replace(needle, replacement, 1), encoding="utf-8"
            )
            shutil.copy2(
                ROOT / "scripts/dur050-ssm-wrapper.ps1", mutant_dir / "dur050-ssm-wrapper.ps1"
            )
            shutil.copy2(
                ROOT / "scripts/dur050-invoke-ssm-command.ps1",
                mutant_dir / "dur050-invoke-ssm-command.ps1",
            )
            mutant_result, mutant_output, mutant_record = retrieve(
                f"mutated-{label}-guard", scenario, mutant_script
            )
            try:
                assert_retrieval_failure(
                    mutant_result,
                    mutant_output,
                    mutant_record,
                    failure_reasons[scenario],
                    f"mutated {label} guard",
                )
            except AssertionError:
                print(
                    f"NEGATIVE CONTROL: removal of the {label} guard is caught "
                    "by its regression assertion."
                )
            else:
                raise RuntimeError(f"regression assertion survived removal of the {label} guard")
            shutil.rmtree(mutant_dir, ignore_errors=True)

        api.shutdown()
        api.server_close()
        server_thread.join(timeout=2)
        # The fake remote generator output is deliberately outside the Python temporary root.
        shutil.rmtree(remote_block := Path(output_dir), ignore_errors=True)
        shutil.rmtree(source, ignore_errors=True)
        state_dir = Path(env["DUR050_FAKE_SSM_STATE"])
        for archive_file in state_dir.glob("*.archive-path"):
            Path(archive_file.read_text(encoding="utf-8")).unlink(missing_ok=True)
    print("PASS: DUR-050 generator dispatch and chunked retrieval controls.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
