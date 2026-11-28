"""J4 workflow entrypoint, evidence replay, admission and stop/retain ledger."""

from __future__ import annotations

import argparse
import http.client
import json
import os
import re
import shutil
import subprocess
import time
import zipfile
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from workers.recorded_probe import clock_sample
from workers.recorded_wire import STUB_PATH, CallIdentity

from deploy.j4.cells import Cell, case_input
from deploy.j4.checker import check, negative_controls
from deploy.j4.evidence import (
    BOOTSTRAP,
    CELL,
    CLOSEOUT,
    RESET,
    SECOND,
    admit,
    digest,
    read_jsonl,
    schedule,
    seal,
    verify_index,
    write_json,
)
from deploy.j4.host import LOCK, Host, Topology, seal_images, wait_for
from deploy.j4.overlay import ROOT, Overlay
from deploy.j4.relay_preflight import read_json, relay_selection

PROTOCOL = ROOT / "experiments/portfolio/recorded-llm-recovery/protocol.md"
WORKFLOW = ROOT / ".github/workflows/j4.yml"
REGISTRY = ROOT / "deploy/runtime-switches.json"
HEX = "[0-9a-f]{64}"


def registered_plan(mode: str) -> list[dict]:
    return [
        {
            **case,
            "status": "unperformed",
            "path": ("provider-resilience/" if case["arm"].startswith("B") else "") + case["cell"],
        }
        for case in schedule(mode)
    ]


def runner_inventory(scratch, required):
    ram = int(Path("/proc/meminfo").read_text().splitlines()[0].split()[1]) * 1024
    free = shutil.disk_usage(scratch).free
    cpus = len(os.sched_getaffinity(0))
    if cpus < 4 or ram < 12 * (1 << 30) or free < required * (1 << 30):
        raise ValueError("J4 hosted runner minimum CPU, RAM or disk was not met")
    return {
        "cpus": cpus,
        "ram_bytes": ram,
        "free_bytes": free,
        "runner_image": os.environ["ImageOS"],  # noqa: SIM112 - GitHub's Linux names
        "runner_version": os.environ["ImageVersion"],  # noqa: SIM112
        "runner_environment": os.environ["RUNNER_ENVIRONMENT"],
        **clock_sample(),
    }


def tracked_inputs() -> dict:
    files = [
        PROTOCOL,
        WORKFLOW,
        REGISTRY,
        ROOT / "deploy/k8s/tools.lock.json",
        ROOT / "deploy/k8s/kind-cluster.yaml",
        ROOT / "deploy/k8s/Dockerfile",
        ROOT / "requirements-worker.txt",
        ROOT / "uv.lock",
        *sorted((ROOT / "python/workers").glob("recorded_*.py")),
        *sorted((ROOT / "deploy/j4").glob("*.py")),
        ROOT / "deploy/j4/Dockerfile",
        *sorted((ROOT / "migrations").glob("*.up.sql")),
        *sorted((ROOT / "migrations/j4_b2").glob("*.sql")),
    ]
    return {path.relative_to(ROOT).as_posix(): digest(path) for path in files}


def replay(path: Path) -> dict:
    verify_index(path)
    result = json.loads((path / "campaign.json").read_bytes())
    if result["status"] != "pass" or result["cleanup"] != "pass":
        raise ValueError("J4 rehearsal or campaign did not pass")
    expected = registered_plan(result["mode"])
    if len(expected) != len(result["cells"]):
        raise ValueError("J4 replay cohort population differs")
    for registered, row in zip(expected, result["cells"], strict=True):
        if any(row[k] != registered[k] for k in ("arm", "cell", "repeat", "path")):
            raise ValueError("J4 replay cohort order or identity differs")
        trace = json.loads((path / row["path"] / "cell.json").read_bytes())
        verdict = check(trace)
        if (
            trace["cell"] != row["cell"]
            or trace["arm"] != row["arm"]
            or verdict != row.get("verdict")
            or verdict["status"] != "pass"
            or row["status"] != "pass"
            or row["cleanup"] != "pass"
            or row["counts"] != cell_counts(trace)
        ):
            raise ValueError(f"J4 independent replay differed for {row['cell']}")
        check_raw(path / row["path"], trace)
    if result["counts"] != counts(result["cells"]):
        raise ValueError("J4 replay aggregate counts differ")
    if result["mode"] == "qualification":
        controls = path / "checker-controls"
        verdicts = json.loads((controls / "verdicts.json").read_bytes())
        b0 = json.loads((path / "provider-resilience/j4-b0-1/cell.json").read_bytes())
        with TemporaryDirectory() as tmp:
            if negative_controls(b0, Path(tmp)) != verdicts:
                raise ValueError("J4 qualification data-drop controls failed replay")
            for name, verdict in verdicts.items():
                filename = name + ".json"
                if (
                    digest(controls / filename) != digest(Path(tmp) / filename)
                    or check(json.loads((controls / filename).read_bytes())) != verdict
                ):
                    raise ValueError("J4 qualification mutated evidence differs")
    return result


def check_raw(path: Path, trace: dict) -> None:
    for source, raw in {
        "worker": "worker/worker.jsonl",
        "stub": "stub/stub.jsonl",
        "scheduler": "runtime/scheduler.jsonl",
    }.items():
        if read_jsonl(path / "raw" / raw) != trace[source]:
            raise ValueError(f"J4 retained {source} differs from checked evidence")
    broker = read_jsonl(path / "broker.jsonl")
    checked = trace["broker"]
    if broker[: len(checked)] != checked or any(
        r["kind"] != "offset" for r in broker[len(checked) :]
    ):
        raise ValueError("J4 retained broker evidence differs")


def github(path: str) -> dict:
    return json.loads(subprocess.check_output(["gh", "api", path], cwd=ROOT, timeout=30))


def hosted_job(repo, run_id, attempt, mode):
    jobs = github(f"repos/{repo}/actions/runs/{run_id}/attempts/{attempt}/jobs")
    matching = [row for row in jobs["jobs"] if row["name"] == f"J4 {mode}"]
    if jobs["total_count"] != len(jobs["jobs"]) or len(matching) != 1:
        raise ValueError("J4 hosted job identity is ambiguous")
    return matching[0]


def load_qualification(args, scratch: Path, revision: str) -> tuple[Path, dict]:
    if (
        args.reviewed_head != revision
        or not re.fullmatch(r"[1-9][0-9]*", args.review_round)
        or not re.fullmatch(r"[1-9][0-9]*", args.qualification_run)
        or not re.fullmatch(r"[1-9][0-9]*", args.qualification_attempt)
        or not re.fullmatch(HEX, args.qualification_digest)
    ):
        raise ValueError(
            "J4 measurement requires explicit exact-head review and qualification identity"
        )
    repo, run_id, attempt = (
        os.environ["GITHUB_REPOSITORY"],
        args.qualification_run,
        args.qualification_attempt,
    )
    base = f"repos/{repo}/actions"
    metadata = github(f"{base}/runs/{run_id}/attempts/{attempt}")
    if (metadata["head_sha"], metadata["path"].split("@")[0], metadata["conclusion"]) != (
        revision,
        ".github/workflows/j4.yml",
        "success",
    ):
        raise ValueError("J4 measurement qualification run does not match this candidate")
    artifacts = github(f"{base}/runs/{run_id}/artifacts?per_page=100")
    if artifacts["total_count"] > len(artifacts["artifacts"]):
        raise ValueError("J4 qualification artifact search is incomplete")
    name = f"j4-qualification-{run_id}-{attempt}"
    candidates = [a for a in artifacts["artifacts"] if a["name"] == name and not a["expired"]]
    if len(candidates) != 1 or candidates[0]["digest"] != "sha256:" + args.qualification_digest:
        raise ValueError("J4 reviewed qualification artifact digest does not match")
    artifact_id = candidates[0]["id"]
    archive = scratch / "qualification.zip"
    with archive.open("wb") as out:
        subprocess.run(
            ["gh", "api", f"{base}/artifacts/{artifact_id}/zip"],
            cwd=ROOT,
            stdout=out,
            check=True,
            timeout=240,
        )
    if digest(archive) != args.qualification_digest:
        raise ValueError("J4 qualification artifact bytes differ from the reviewed digest")
    target = scratch / "qualification"
    target.mkdir()
    with zipfile.ZipFile(archive) as bundle:
        filenames = set()
        for entry in bundle.infolist():
            parts = entry.filename.rstrip("/").split("/")
            if (
                entry.filename in filenames
                or any(part in {"", ".", ".."} for part in parts)
                or "\\" in entry.filename
                or (entry.external_attr >> 16) & 0o170000 == 0o120000
            ):
                raise ValueError("unsafe J4 qualification archive path")
            filenames.add(entry.filename)
        bundle.extractall(target)
    record = replay(target)
    if (record["mode"], record["source_revision"], record["run_id"], record["run_attempt"]) != (
        "qualification",
        revision,
        run_id,
        attempt,
    ):
        raise ValueError("J4 frozen qualification provenance differs")
    job = hosted_job(repo, run_id, attempt, "qualification")
    upload = [
        row for row in job["steps"] if row["name"] == "Retain complete or partial J4 evidence"
    ]
    if (
        record["job_id"] != job["id"]
        or job["conclusion"] != "success"
        or len(upload) != 1
        or upload[0]["conclusion"] != "success"
    ):
        raise ValueError("J4 frozen qualification job identity differs")
    observed = (
        datetime.fromisoformat(job["completed_at"]).timestamp() - record["closeout_started_unix"]
    )
    if observed <= 0 or observed > CLOSEOUT:
        raise ValueError("J4 qualification exceeded the registered closeout stage")
    record["allowances"]["closeout"] = max(observed, record["allowances"]["closeout"])
    record["artifact_id"] = artifact_id
    return target, record


def verify_stub_reopen(host: Host, overlay: Overlay, destination: Path) -> None:
    destination.mkdir()
    try:
        with ExitStack() as stack:
            t = Topology(host, overlay, destination, stack)
            t.apply(overlay.bootstrap())
            manifests = overlay.service_pods(t.service("kafka"), LOCK["postgres"], LOCK["kafka"])
            stub = [obj for obj in manifests if obj["metadata"]["name"] in {"j4-stub", "stub"}]
            t.apply(stub)
            t.ready("stub")
            port = t.forward("stub")
            t.http_ready("stub")
            journal = t.raw / "stub/stub.jsonl"
            call = CallIdentity(
                overlay.cell,
                overlay.workflow_id,
                "j4.recorded-llm",
                0,
                1,
                1,
                "j4-qualification-journal-probe",
            )

            def post(port):
                conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
                try:
                    headers = {
                        **call.headers(),
                        "Content-Type": "application/json",
                        "Connection": "close",
                    }
                    conn.request(
                        "POST", STUB_PATH, body=json.dumps(case_input("C0")), headers=headers
                    )
                    response = conn.getresponse()
                    return response.status, json.loads(response.read())
                finally:
                    conn.close()

            first, payload = post(port)
            before = wait_for(
                lambda: len(read_jsonl(journal)) == 3 and journal.read_bytes(),
                time.monotonic_ns() + 2 * SECOND,
                "J4 initial persisted stub journal",
            )
            uid = t.object("pod", "stub")["metadata"]["uid"]
            t.kube("delete", "pod", "stub", "--wait=true", "--timeout=30s", timeout=35)
            host.apply([obj for obj in stub if obj["kind"] == "Pod"])
            t.ready("stub")
            port = t.forward("stub")
            t.http_ready("stub")
            replacement = t.object("pod", "stub")["metadata"]["uid"]
            second, _ = post(port)
            after = wait_for(
                lambda: len(read_jsonl(journal)) == 6 and journal.read_bytes(),
                time.monotonic_ns() + 2 * SECOND,
                "J4 reopened persisted stub journal",
            )
            rows = read_jsonl(journal)
            if (
                first != 200
                or payload["provider_call_id"] != call.call_id
                or uid == replacement
                or second != 400
                or not after.startswith(before)
                or rows[3]["branch"] != "duplicate_id"
            ):
                raise ValueError("J4 stub replacement did not reopen the nonempty journal")
            write_json(
                destination / "reopen.json",
                {"old_pod": uid, "new_pod": replacement, "before": rows[:3], "after": rows},
            )
    finally:
        host.remove_namespace(overlay.namespace)


def export_raw(raw: Path, destination: Path):
    if raw.exists():
        shutil.copytree(
            raw,
            destination / "raw",
            dirs_exist_ok=True,
            ignore=shutil.ignore_patterns("release", "release.pending"),
        )
        for row in (destination / "raw").rglob("*"):
            if row.is_file():
                row.chmod(0o444)


def counts(cells: list[dict]) -> dict:
    totals = {
        key: 0
        for key in (
            "accepted",
            "pass",
            "fail",
            "invalid",
            "unperformed",
            "matched_arrivals",
            "rejected_calls",
            "calls_after_kill",
            "reservations",
            "unused",
            "completions",
            "success_receipts",
            "result_consumptions",
            "successful_result_consumptions",
            "exhaustions",
        )
    }
    for row in cells:
        totals[row["status"]] += 1
        totals["accepted"] += int(row.get("accepted", False))
        for key, value in row.get("counts", {}).items():
            totals[key] += value
    return {
        "scheduled_workflows": len(cells),
        "scheduled_steps": len(cells),
        **totals,
        "accepted_steps": totals["accepted"],
    }


def cell_counts(cell: dict) -> dict:
    arrivals = [r for r in cell.get("stub", []) if r["event"] == "receipt"]
    cut = cell.get("cut") or {}
    rows = cell.get("db", {})
    reservations = rows.get("reservations", [])
    attempts = rows.get("attempts", [])
    provider_consumptions = [
        r
        for r in rows.get("history", [])
        if r["reason"] == "RESULT_CONSUMED" and r["node_id"] == "j4.recorded-llm"
    ]
    results = {(r["node_id"], r["attempt_number"]): r["state"] for r in attempts}
    return {
        "matched_arrivals": sum(r.get("matched", False) for r in arrivals),
        "rejected_calls": sum(not r.get("matched", False) for r in arrivals),
        "calls_after_kill": sum(r["at_ns"] >= cut.get("request_ns", 1 << 65) for r in arrivals),
        "reservations": len(reservations),
        "unused": sum(
            r["outcome"] == "ambiguous" and r["call_id"] not in {a.get("call_id") for a in arrivals}
            for r in reservations
        ),
        "completions": sum(
            r["node_id"] == "pure.echo" and r["state"] == "SUCCEEDED" for r in attempts
        ),
        "success_receipts": sum(
            r["node_id"] == "j4.recorded-llm" and r["state"] == "SUCCEEDED" for r in attempts
        ),
        "result_consumptions": len(provider_consumptions),
        "successful_result_consumptions": sum(
            results.get((r["node_id"], r["attempt_number"])) == "SUCCEEDED"
            for r in provider_consumptions
        ),
        "exhaustions": sum(
            r["terminal_payload"] == {"reason": "PROVIDER_BUDGET_EXHAUSTED"} for r in reservations
        ),
    }


def run_cells(host, output, record, selection, started, allowances):
    revision, cells = record["source_revision"], record["cells"]
    known_failure = False
    for index, row in enumerate(cells):
        needed = admit(cells[index:], allowances, time.monotonic() - started, bootstrap=False)
        row["forecast_seconds"] = needed
        row["runner"] = runner_inventory(host.scratch, 10)
        began = time.monotonic()
        host.deadline_ns = time.monotonic_ns() + CELL * SECOND
        out = output / row["path"]
        out.mkdir(parents=True)
        overlay = Overlay(
            row["cell"],
            row["cell"],
            row["arm"],
            f"{host.name}-{row['cell']}",
            host.images["runtime"]["tag"],
            host.images["worker"]["tag"],
            selection,
        )
        cell = None
        row["status"] = "invalid"
        try:
            with ExitStack() as stack:
                cell = Cell(
                    Topology(host, overlay, out, stack),
                    {key: row[key] for key in ("cell", "arm", "repeat")},
                )
                try:
                    verdict = cell.run_case(selection, revision)
                except Exception:
                    if hasattr(cell.t, "snapshot_connection"):
                        try:
                            cell.run["partial_db"] = cell.t.snapshot()
                            cell.run.setdefault("db", cell.run["partial_db"]["db"])
                        except Exception as export_error:
                            row["export_error"] = type(export_error).__name__
                    raise
                row["status"], row["verdict"] = verdict["status"], verdict
                known_failure |= verdict["status"] == "fail"
        except Exception as exc:
            row["error"] = f"{type(exc).__name__}: {exc}"
            if cell:
                try:
                    cell.run.update(cell.t.traces())
                    if "db" not in cell.run and "terminal_db" in cell.run:
                        cell.run["db"] = cell.run["terminal_db"]["db"]
                    write_json(out / "cell.json", cell.run)
                except Exception as export_error:
                    row["export_error"] = type(export_error).__name__
        if cell:
            submission = cell.run.get("submit", {})
            reply = submission.get("response", {})
            row["accepted"] = (
                submission.get("status") == 201
                and reply.get("created") is True
                and reply.get("workflow", {}).get("workflow_id") == cell.workflow
            )
            row["counts"] = cell_counts(cell.run)
        row["elapsed_seconds"] = time.monotonic() - began
        allowances[row["arm"]] = max(allowances.get(row["arm"], 0), row["elapsed_seconds"])
        if row["elapsed_seconds"] > CELL and row["status"] == "pass":
            row["status"], row["error"] = "invalid", "J4 cell deadline"
        reset_started = time.monotonic()
        host.deadline_ns = time.monotonic_ns() + RESET * SECOND
        try:
            host.remove_namespace(overlay.namespace)
            export_raw(host.raw / overlay.cell, out)
            shutil.rmtree(host.raw / overlay.cell)
            if (host.raw / overlay.cell).exists():
                raise ValueError("J4 working journal survived reset")
            if row["status"] == "pass":
                check_raw(out, cell.run)
            row["cleanup"] = "pass"
        except Exception as exc:
            row["cleanup"] = "invalid"
            row["cleanup_error"] = f"{type(exc).__name__}: {exc}"
            if row["status"] == "pass":
                row["status"] = "invalid"
        row["reset_seconds"] = time.monotonic() - reset_started
        if row["reset_seconds"] > RESET and row["status"] == "pass":
            row["status"], row["cleanup"] = "invalid", "invalid"
            row["cleanup_error"] = "J4 reset deadline"
        allowances["reset"] = max(allowances.get("reset", 0), row["reset_seconds"])
        write_json(output / "campaign.json", record)
        if row["status"] != "pass":
            return "fail" if known_failure else "invalid"
    return "pass"


def campaign(args) -> int:
    output, scratch = args.output.resolve(), args.scratch.resolve()
    if output == scratch or scratch in output.parents or output in scratch.parents:
        raise ValueError("J4 evidence and job scratch must be distinct siblings")
    output.mkdir(parents=True)
    scratch.mkdir(parents=True)
    revision = os.environ["GITHUB_SHA"]
    run_id, attempt = os.environ["GITHUB_RUN_ID"], os.environ["GITHUB_RUN_ATTEMPT"]
    start = time.monotonic()
    if args.job_started:
        # Include workflow setup in the cap and bootstrap allowance.
        start -= max(0, time.time() - args.job_started)
    host = None
    name = f"j4-{run_id}-{attempt}"
    record = {
        "schema": "j4.campaign.v1",
        "mode": args.mode,
        "campaign_id": name,
        "source_revision": revision,
        "run_id": run_id,
        "run_attempt": attempt,
        "cells": registered_plan(args.mode),
        "status": "invalid",
        "cleanup": "not-started",
        "cost_usd": 0,
    }
    allowances = {}
    try:
        if (
            os.environ["RUNNER_ENVIRONMENT"] != "github-hosted"
            or not re.fullmatch(r"j4-\d+-\d+", name)
            or not re.fullmatch(r"[0-9a-f]{40}", revision)
            or subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
            != revision
            or subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT).strip()
            or github(f"repos/{os.environ['GITHUB_REPOSITORY']}")["private"]
        ):
            raise ValueError("J4 requires a clean exact-head public GitHub-hosted runner at $0")
        record["forecast_seconds"] = admit(record["cells"], {}, 0, bootstrap=True)
        # This must precede artifact loading, candidate builds and all kind work.
        selection = relay_selection(read_json(REGISTRY), args.relay_switch, args.relay_value)
        record["job_id"] = hosted_job(os.environ["GITHUB_REPOSITORY"], run_id, attempt, args.mode)[
            "id"
        ]
        record["runner"] = runner_inventory(scratch, 20)
        inputs = tracked_inputs()
        record["inputs"] = inputs
        record["relay_selection"] = selection.environment()
        images = {role: {"tag": f"j4-{role}:{revision}"} for role in ("runtime", "worker")}
        prior = None
        if args.mode == "measurement":
            prior, qualification = load_qualification(args, scratch, revision)
            if (
                qualification["inputs"] != inputs
                or qualification["relay_selection"] != selection.environment()
            ):
                raise ValueError("J4 measurement inputs differ from reviewed qualification")
            images = qualification["images"]
            allowances.update(qualification["allowances"])
            record["qualification"] = {
                key: qualification[key]
                for key in ("run_id", "run_attempt", "job_id", "artifact_id")
            } | {
                "digest": args.qualification_digest,
                "review_round": args.review_round,
                "reviewed_head": args.reviewed_head,
            }
        record["admitted_seconds"] = admit(
            record["cells"], allowances, time.monotonic() - start, bootstrap=True
        )
        raw = scratch / "live"
        raw.mkdir()
        host = Host(scratch, raw, name, images, int((start + BOOTSTRAP) * SECOND))
        record["images"] = seal_images(host, images, output, prior)
        host.bootstrap(output)
        if args.mode == "qualification":
            reopen = Overlay(
                "j4-journal",
                "j4-journal",
                "C0",
                "j4-qualification-stub-reopen",
                images["runtime"]["tag"],
                images["worker"]["tag"],
                selection,
            )
            verify_stub_reopen(host, reopen, output / "journal-reopen")
            export_raw(host.raw / reopen.cell, output / "journal-reopen")
            shutil.rmtree(host.raw / reopen.cell)
        allowances["bootstrap"] = max(allowances.get("bootstrap", 0), time.monotonic() - start)
        if time.monotonic() - start > BOOTSTRAP:
            raise TimeoutError("J4 bootstrap deadline")
        record["status"] = run_cells(host, output, record, selection, start, allowances)
        if record["status"] == "pass" and args.mode == "qualification":
            source = json.loads((output / "provider-resilience/j4-b0-1/cell.json").read_bytes())
            record["checker_controls"] = negative_controls(source, output / "checker-controls")
    except Exception as exc:
        record["error"] = f"{type(exc).__name__}: {exc}"
        if record["status"] != "fail":
            record["status"] = "invalid"
    finally:
        closeout = time.monotonic()
        record["closeout_started_unix"] = time.time()
        if host:
            host.deadline_ns = int((closeout + CLOSEOUT) * SECOND)
            try:
                host.destroy()
                export_raw(host.raw, output / "retained-after-stop")
                shutil.rmtree(host.raw)
                if host.raw.exists():
                    raise ValueError("J4 live journal directory survived teardown")
                for image in sorted(host.loaded):
                    host.cmd(["docker", "image", "rm", image])
                record["cleanup"] = "pass"
            except Exception as exc:
                if record["status"] != "fail":
                    record["status"] = "invalid"
                record["cleanup"] = "invalid"
                record["cleanup_error"] = f"{type(exc).__name__}: {exc}"
                try:
                    export_raw(host.raw, output / "retained-after-stop")
                except Exception as export_error:
                    record["retained_export_error"] = type(export_error).__name__
        try:
            shutil.rmtree(scratch)
        except OSError as exc:
            if record["status"] != "fail":
                record["status"] = "invalid"
            record["cleanup"] = "invalid"
            record["scratch_cleanup_error"] = type(exc).__name__
        for row in record["cells"]:
            if row["status"] == "unperformed":
                row["reason"] = record.get("error", "stopped at an earlier registered cell")
        record["counts"] = counts(record["cells"])
        allowances["closeout"] = max(allowances.get("closeout", 0), time.monotonic() - closeout)
        record["allowances"] = allowances
        record["elapsed_seconds"] = time.monotonic() - start
        if time.monotonic() - closeout > CLOSEOUT and record["status"] == "pass":
            record["status"] = "invalid"
            record["error"] = "J4 closeout deadline"
        write_json(output / "campaign.json", record)
        seal(output)
        if record["status"] == "pass":
            try:
                replay(output)
            except Exception as exc:
                record["status"] = "invalid"
                record["replay_error"] = f"{type(exc).__name__}: {exc}"
                write_json(output / "campaign.json", record)
                seal(output)
    return int(record["status"] != "pass")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("qualification", "measurement", "replay", "forecast"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--scratch", type=Path)
    parser.add_argument("--job-started", type=int, default=0)
    for key in (
        "relay-switch",
        "relay-value",
        "reviewed-head",
        "review-round",
        "qualification-run",
        "qualification-attempt",
        "qualification-digest",
    ):
        parser.add_argument(f"--{key}", default="")
    args = parser.parse_args()
    if args.mode == "forecast":
        print(
            json.dumps(
                {
                    mode: admit(schedule(mode), {}, 0, bootstrap=True) / 60
                    for mode in ("qualification", "measurement")
                }
            )
        )
        return 0
    if args.output is None:
        parser.error("--output is required")
    if args.mode == "replay":
        print(json.dumps(replay(args.output)["counts"], sort_keys=True))
        return 0
    if not args.scratch:
        parser.error("--scratch is required")
    return campaign(args)


if __name__ == "__main__":
    raise SystemExit(main())
