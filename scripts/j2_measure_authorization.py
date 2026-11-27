"""Read a reviewer-owned J2 authorization and account for sender traffic."""

import argparse
import base64
import json
import os
import re
import subprocess
import tempfile
import time
import zipfile
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, build_opener

from j2_barrier import (
    ActionsAPI,
    Budget,
    Campaign,
    NoRedirect,
    canonical,
    decode,
    digest,
    positive,
    require,
    schema,
    sha256,
)
from j2_checker import ROOT
from j2_qualification_evidence import file_record, read
from j2_schedule import MEASURED_CAMPAIGN, k4_report

AUTH_SCHEMA = "j2.campaign-authorization.v1"
AUTH_PATH = "authorization.json"
CONTROLS_PATHS = (
    "cmd/dur053-loadgen/main.go",
    "cmd/j2-sender/main.go",
    "internal/dur053/j2.go",
    "internal/dur053/k4.go",
    "internal/dur053/load.go",
    "scripts/j2_build_sender.py",
    "scripts/j2_schedule.py",
    "scripts/j2_schedule_rehearsal.py",
    "scripts/j2_step_rehearsal.py",
    "scripts/j2_drain.py",
    "scripts/j2_offline.py",
    "scripts/j2_controls.py",
    "scripts/j2_control_evidence.py",
    "scripts/j2_integrated_evidence.py",
    "scripts/j2_harness.py",
    "scripts/j2_measure.py",
    "scripts/j2_measure_telemetry.py",
    "scripts/j2_overlay.py",
    "scripts/j2_verify_overlay.py",
    "scripts/j2_qualify_volume.py",
    "experiments/portfolio/load-envelope/checker.lock.json",
    "experiments/portfolio/load-envelope/images.lock.json",
    "experiments/portfolio/load-envelope/exporter.lock.json",
    "deploy/k8s/tools.lock.json",
)


def git_output(root, *args):
    return subprocess.check_output(["git", *args], cwd=root, timeout=20)


def control_identity(root=ROOT, *, head=None):
    paths = {}
    for name in CONTROLS_PATHS:
        raw = (
            (root / name).read_bytes()
            if head is None
            else git_output(root, "show", f"{head}:{name}")
        )
        paths[name] = digest(raw.replace(b"\r\n", b"\n"))
    return paths


def engineering(campaign):
    return (
        isinstance(campaign, str)
        and re.fullmatch(r"eng-[a-z0-9][a-z0-9-]{0,59}", campaign) is not None
    )


def authorization_branch(campaign):
    require(campaign == MEASURED_CAMPAIGN or engineering(campaign), "invalid campaign ID")
    return f"j2-authorization/{campaign}"


def git_head(head):
    return isinstance(head, str) and re.fullmatch(r"[0-9a-f]{40}", head) is not None


def validate_ledger_document(ledger, *, repository, campaign):
    authorization_branch(campaign)
    schema(ledger, "schema repository campaign_id reviewer dispatches", "authorization ledger")
    require(
        ledger["schema"] == AUTH_SCHEMA
        and ledger["repository"] == repository
        and ledger["campaign_id"] == campaign
        and isinstance(ledger["reviewer"], str)
        and ledger["reviewer"].strip(),
        "authorization scope or reviewer differs",
    )
    history = ledger["dispatches"]
    require(isinstance(history, list) and history, "authorization dispatch history is empty")
    seen = set()
    for index, entry in enumerate(history):
        schema(
            entry,
            "run_id attempt head controls_sha256 disposition reason evidence_sha256",
            "authorization dispatch",
        )
        require(
            positive(entry["run_id"])
            and entry["run_id"] not in seen
            and type(entry["attempt"]) is int
            and entry["attempt"] == 1
            and git_head(entry["head"])
            and sha256(entry["controls_sha256"]),
            "invalid authorization dispatch history",
        )
        seen.add(entry["run_id"])
        if entry["disposition"] == "authorized" and index == len(history) - 1:
            require(
                entry["reason"] is None and entry["evidence_sha256"] is None,
                "active authorization cannot have a closeout receipt",
            )
        else:
            require(
                entry["disposition"] == "no_requests"
                and sha256(entry["evidence_sha256"])
                and isinstance(entry["reason"], str)
                and entry["reason"].strip(),
                "previous dispatch lacks reviewed proof of zero requests",
            )
    return history[-1]


def validate_ledger(ledger, *, repository, campaign, run, attempt, head, controls):
    require(
        type(attempt) is int and attempt == 1 and positive(run) and git_head(head),
        "invalid authorization run, head or attempt",
    )
    active = validate_ledger_document(ledger, repository=repository, campaign=campaign)
    require(
        active["disposition"] == "authorized",
        "campaign has no current unused authorization",
    )
    require(active["run_id"] == run, "authorization Actions run ID differs")
    require(
        active["head"] == head and active["attempt"] == attempt,
        "authorization head or attempt differs",
    )
    require(active["controls_sha256"] == controls, "control path applicability digest differs")
    return active


def check_append_only(previous, current):
    repository, campaign = current["repository"], current["campaign_id"]
    old = validate_ledger_document(previous, repository=repository, campaign=campaign)
    new = validate_ledger_document(current, repository=repository, campaign=campaign)
    before, after = previous["dispatches"], current["dispatches"]
    shared = ("schema", "repository", "campaign_id", "reviewer")
    fields = ("run_id", "attempt", "head", "controls_sha256")
    unchanged_prefix = len(after) == len(before) + 1 and before == after[:-1]
    appended = new["disposition"] == "authorized" and len(after) == len(before) + 1
    closed = new if len(after) == len(before) else after[-2] if appended else None
    closes_active = (
        closed is not None
        and old["disposition"] == "authorized"
        and closed["disposition"] == "no_requests"
        and before[:-1] == (after[:-2] if appended else after[:-1])
        and all(old[key] == closed[key] for key in fields)
    )
    require(
        all(previous[key] == current[key] for key in shared)
        and (
            closes_active or (appended and unchanged_prefix and old["disposition"] == "no_requests")
        ),
        "authorization update did not append to the prior reviewer ledger",
    )


def fetch_ledger(repository, token, campaign, *, wait_seconds=0, opener=None, check_history=False):
    require(
        re.fullmatch(r"[\w.-]+/[\w.-]+", repository) and token,
        "missing authorization repository or token",
    )
    branch = authorization_branch(campaign)
    base = f"https://api.github.com/repos/{repository}"
    transport = opener if opener is not None else build_opener(NoRedirect())
    end = time.monotonic() + wait_seconds

    def get(path):
        request = Request(
            base + path,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        )
        with transport.open(request, timeout=12) as response:
            raw = response.read(256_001)
            require(
                response.status == 200 and len(raw) <= 256_000, "invalid authorization API response"
            )
            return decode(raw)

    def read_at(revision):
        entry = get(f"/contents/{AUTH_PATH}?ref={revision}")
        require(
            entry["path"] == AUTH_PATH
            and entry["type"] == "file"
            and entry["encoding"] == "base64",
            "invalid authorization ledger entry",
        )
        raw = base64.b64decode("".join(entry["content"].splitlines()), validate=True)
        require(len(raw) == entry["size"], "authorization ledger size differs")
        return raw

    while True:
        try:
            ref = get("/branches/" + quote(branch, safe=""))
            revision = ref["commit"]["sha"]
            require(
                ref["name"] == branch and git_head(revision),
                "authorization ref is not the expected branch",
            )
            require(
                engineering(campaign) or ref.get("protected") is True,
                "registered authorization requires a protected branch",
            )
            raw = read_at(revision)
            ledger = decode(raw)
            if check_history:
                updates = get(f"/commits?sha={revision}&path={AUTH_PATH}&per_page=2")
                require(isinstance(updates, list) and updates, "missing reviewer ledger history")
                if len(updates) > 1:
                    prior = updates[1]["sha"]
                    require(git_head(prior), "prior ledger commit is invalid")
                    check_append_only(decode(read_at(prior)), ledger)
                else:
                    require(
                        len(ledger["dispatches"]) == 1, "initial authorization must name one run"
                    )
            return {
                "ref": branch,
                "revision": revision,
                "sha256": digest(raw),
                "ledger": ledger,
            }
        except HTTPError as error:
            status = error.code
            error.close()
            if status != 404 and status < 500:
                raise ValueError(f"authorization API failed with HTTP {status}") from None
        except (URLError, TimeoutError):
            pass
        remaining = end - time.monotonic()
        require(remaining > 0, "authorization was unavailable before the gate deadline")
        time.sleep(min(5, remaining))


def authorization(campaign, *, wait_seconds=0, check_history=False):
    repository = os.environ["GITHUB_REPOSITORY"]
    binding = fetch_ledger(
        repository,
        os.environ["GITHUB_TOKEN"],
        campaign,
        wait_seconds=wait_seconds,
        check_history=check_history,
    )
    validate_ledger(
        binding["ledger"],
        repository=repository,
        campaign=campaign,
        run=int(os.environ["GITHUB_RUN_ID"]),
        attempt=int(os.environ["GITHUB_RUN_ATTEMPT"]),
        head=os.environ["GITHUB_SHA"],
        controls=digest(canonical(control_identity())),
    )
    if check_history and binding["ledger"]["dispatches"][:-1]:
        api = ActionsAPI(
            repository,
            os.environ["GITHUB_TOKEN"],
            Budget(time.monotonic() + 30),
            lambda *_args, **_fields: None,
        )
        for prior in binding["ledger"]["dispatches"][:-1]:
            record = decode(
                api.request(
                    f"{api.root}/runs/{prior['run_id']}", "previous dispatch", authenticated=True
                )
            )
            require(
                record["status"] == "completed"
                and record["event"] == "workflow_dispatch"
                and record["head_sha"] == prior["head"]
                and record["run_attempt"] == prior["attempt"],
                "previous dispatch has not closed at its reviewed head and attempt",
            )
            historical = Campaign(
                repository, prior["run_id"], prior["attempt"], prior["head"], campaign, {}, 0, ""
            )
            name = f"j2-measure-dispatch-{historical.run}-{historical.attempt}"
            metadata = api.artifacts(historical, {name})
            require(name in metadata, "reviewed prior dispatch receipt is unavailable")
            with zipfile.ZipFile(BytesIO(api.download(metadata[name]))) as archive:
                member = archive.getinfo("campaign-dispatch-manifest.json")
                require(member.file_size <= 1 << 20, "prior dispatch manifest exceeds bounds")
                verify_zero_requests(archive.read(member), prior, campaign)
    return binding


def verify_zero_requests(raw, prior, campaign):
    document = decode(raw)
    require(
        isinstance(document, dict)
        and digest(raw) == prior["evidence_sha256"]
        and document.get("campaign") == campaign
        and document.get("head") == prior["head"]
        and positive(document.get("run_id"))
        and document["run_id"] == prior["run_id"]
        and type(document.get("attempt")) is int
        and document["attempt"] == prior["attempt"]
        and document.get("disposition") == "no_requests"
        and document.get("errors") == {}
        and isinstance(document.get("jobs"), dict)
        and set(document["jobs"]) == {"1", "2", "3"}
        and all(
            isinstance(job, dict) and job.get("disposition") == "no_requests"
            for job in document["jobs"].values()
        ),
        "prior dispatch receipt does not establish zero requests",
    )


def recheck(record, campaign):
    current = authorization(campaign)
    require(current == record, "authorization ref changed after bootstrap")


def cell_traffic(root: Path):
    launched = root / "sender-release.json"
    journal = root / "sender-journal.jsonl"
    if not launched.exists() and not journal.exists():
        return {"disposition": "no_requests", "reason": "sender was never released"}
    if not journal.is_file():
        return {"disposition": "unresolved", "reason": "released sender journal is absent"}
    info = {"journal": file_record(journal)}
    try:
        raw = journal.read_bytes()
        schedule = read(root / "schedule.json")
        inspected = k4_report(raw, schedule, [])
        sent = next(
            (row["key"] for row in inspected["sender_samples"] if row["phase"] == "finished"), None
        )
        if sent is not None:
            return {
                **info,
                "disposition": "consumed",
                "reason": "recorded HTTP send",
                "first_key": sent,
            }
        if (
            inspected["closed"]
            and not inspected["torn_suffix"]
            and len(inspected["explicitly_unsent"]) == inspected["offered"]
            and not inspected["remaining_intents"]
            and read(root / "journal-read.json")["exit"] == 0
        ):
            return {
                **info,
                "disposition": "no_requests",
                "reason": "all planned rows explicitly unsent",
            }
    except (OSError, ValueError, KeyError, TypeError):
        # A malformed or missing journal cannot authorize a replacement run.
        pass
    return {
        **info,
        "disposition": "unresolved",
        "reason": "sender evidence does not establish zero requests",
    }


def traffic_disposition(records):
    dispositions = {r["disposition"] for r in records}
    if "consumed" in dispositions:
        return "consumed"
    return "no_requests" if dispositions == {"no_requests"} else "unresolved"


def origin_repository(root):
    origin = git_output(root, "remote", "get-url", "origin").decode().strip()
    match = re.fullmatch(
        r"(?:https://github\.com/|git@github\.com:|ssh://git@github\.com/)"
        r"([\w.-]+/[\w.-]+?)(?:\.git)?/?",
        origin,
    )
    require(match is not None, "origin must identify a GitHub repository")
    return match[1]


def checkout_controls(root, head):
    actual = git_output(root, "rev-parse", "--verify", "HEAD").decode().strip()
    require(git_head(head) and actual == head, "checkout HEAD differs from the authorization head")
    return digest(canonical(control_identity(root, head=head)))


def write_ledger(path, ledger):
    pending = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
            pending = Path(stream.name)
            stream.write(canonical(ledger))
        pending.replace(path)
    finally:
        if pending is not None:
            pending.unlink(missing_ok=True)


def authorize_entry(path, *, root, campaign, run, head, reviewer):
    repository = origin_repository(root)
    authorization_branch(campaign)
    controls = checkout_controls(root, head)
    if path.exists():
        previous = decode(path.read_bytes())
        last = validate_ledger_document(previous, repository=repository, campaign=campaign)
        require(last["disposition"] == "no_requests", "a prior dispatch is still open")
        require(previous["reviewer"] == reviewer, "authorization reviewer differs")
    else:
        previous = {
            "schema": AUTH_SCHEMA,
            "repository": repository,
            "campaign_id": campaign,
            "reviewer": reviewer,
            "dispatches": [],
        }
    active = {
        "run_id": run,
        "attempt": 1,
        "head": head,
        "controls_sha256": controls,
        "disposition": "authorized",
        "reason": None,
        "evidence_sha256": None,
    }
    ledger = {**previous, "dispatches": [*previous["dispatches"], active]}
    validate_ledger(
        ledger,
        repository=repository,
        campaign=campaign,
        run=run,
        attempt=1,
        head=head,
        controls=controls,
    )
    if previous["dispatches"]:
        check_append_only(previous, ledger)
    write_ledger(path, ledger)


def close_no_requests(path, *, root, campaign, run, reason, manifest):
    repository = origin_repository(root)
    ledger = decode(path.read_bytes())
    last = validate_ledger_document(ledger, repository=repository, campaign=campaign)
    require(
        last["disposition"] == "authorized" and last["run_id"] == run,
        "run is not the active authorization",
    )
    proof = manifest.read_bytes()
    closed = {
        **last,
        "disposition": "no_requests",
        "reason": reason,
        "evidence_sha256": digest(proof),
    }
    verify_zero_requests(proof, closed, campaign)
    updated = {**ledger, "dispatches": [*ledger["dispatches"][:-1], closed]}
    check_append_only(ledger, updated)
    write_ledger(path, updated)


def verify_record(path, *, root):
    repository = origin_repository(root)
    ledger = decode(path.read_bytes())
    require(isinstance(ledger, dict), "invalid authorization ledger schema")
    campaign = ledger.get("campaign_id")
    last = validate_ledger_document(ledger, repository=repository, campaign=campaign)
    if last["disposition"] == "authorized":
        controls = checkout_controls(root, last["head"])
        validate_ledger(
            ledger,
            repository=repository,
            campaign=campaign,
            run=last["run_id"],
            attempt=last["attempt"],
            head=last["head"],
            controls=controls,
        )


def main(argv=None, *, root=ROOT):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("controls-digest")
    new = commands.add_parser("authorize")
    close = commands.add_parser("close-no-requests")
    for command in (new, close):
        command.add_argument("--campaign-id", required=True)
        command.add_argument("--run-id", type=int, required=True)
        command.add_argument("--output", type=Path, required=True)
    new.add_argument("--head", required=True)
    new.add_argument("--reviewer", required=True)
    close.add_argument("--reason", required=True)
    close.add_argument("--manifest", type=Path, required=True)
    verify = commands.add_parser("verify")
    verify.add_argument("path", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "controls-digest":
            print(json.dumps({"controls_sha256": digest(canonical(control_identity(root)))}))
        elif args.command == "authorize":
            authorize_entry(
                args.output,
                root=root,
                campaign=args.campaign_id,
                run=args.run_id,
                head=args.head,
                reviewer=args.reviewer,
            )
        elif args.command == "close-no-requests":
            close_no_requests(
                args.output,
                root=root,
                campaign=args.campaign_id,
                run=args.run_id,
                reason=args.reason,
                manifest=args.manifest,
            )
        else:
            verify_record(args.path, root=root)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        parser.exit(1, f"{parser.prog}: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
