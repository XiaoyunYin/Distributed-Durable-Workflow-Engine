"""Verify a same-attempt J2 grid and make an independent local barrier decision."""

from __future__ import annotations

import hashlib
import io
import json
import re
import stat
import time
import zipfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import ROUND_CEILING, Decimal
from http.client import HTTPException
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from j2_schedule import MEASURED, ORDERS, RATES, WARMUP

JOBS = (1, 2, 3)
BARRIER_SECONDS = 45 * 60
JOB_MINUTES = 75
# Two polling peers and one withholding peer share the repository token quota.
# Two list calls per poller and one per withholding peer cost about 450 over 45 min.
POLL_SECONDS = 30
PAGE_SIZE = 100
ZIP_LIMIT = 1024 * 1024
REST_LIMIT = 8 * ZIP_LIMIT
NO_RATE = "no eligible rate"
PIN_FILES = {
    "protocol": "experiments/portfolio/load-envelope/protocol.md",
    "tooling": "scripts/j2_barrier.py",
    "images": "experiments/portfolio/load-envelope/images.lock.json",
    "exporter": "experiments/portfolio/load-envelope/exporter.lock.json",
    "checker": "experiments/portfolio/load-envelope/checker.lock.json",
    "dependencies": "deploy/k8s/tools.lock.json",
}
CELL_INVALID = {"sender_invalid", "gate_input_gap", "empty_bin"}
CELL_OUTCOMES = {
    "pass",
    "gate_failure",
    "drain_failure",
    "invalid",
    "integrity_stop",
    "unperformed",
}
COUNTS = ("offered", "sent", "admitted", "terminal", "succeeded")


class BarrierStop(ValueError):
    def __init__(self, reason, detail):
        super().__init__(detail)
        self.reason = reason


class RetryLater(ValueError):
    def __init__(self, detail, delay=POLL_SECONDS):
        super().__init__(detail)
        self.delay = delay


def require(condition, message, *, reason="unverifiable_evidence"):
    if not condition:
        raise BarrierStop(reason, message)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def decode(raw):
    def object_from_pairs(pairs):
        result = dict(pairs)
        if len(result) != len(pairs):
            raise ValueError("duplicate JSON key")
        return result

    def reject_constant(value):
        raise ValueError(f"non-JSON value {value}")

    try:
        return json.loads(raw, object_pairs_hook=object_from_pairs, parse_constant=reject_constant)
    except (ValueError, RecursionError) as error:
        raise BarrierStop("unverifiable_evidence", "invalid JSON evidence") from error


def schema(value, keys, label):
    require(
        isinstance(value, dict) and value.keys() == set(keys.split()), f"invalid {label} schema"
    )


def sha256(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def positive(value):
    return type(value) is int and value > 0


def expiry(value):
    try:
        stamp = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return False
    return stamp.tzinfo is not None and stamp > datetime.now(UTC)


def barrier_deadline(ready, job_deadline):
    require(
        job_deadline - ready >= BARRIER_SECONDS,
        "less than the registered barrier allowance remains in the job",
        reason="insufficient_budget",
    )
    return ready + BARRIER_SECONDS


def population(rate, seconds):
    return int((Decimal(RATES[rate]) * seconds).to_integral_value(rounding=ROUND_CEILING))


def soak_population(rate):
    return population(rate, WARMUP) + population(rate, 1800) + population("C", 300)


def job_name(job):
    return f"J2 barrier job-{job}"


@dataclass(frozen=True)
class Campaign:
    repository: str
    run: int
    attempt: int
    head: str
    campaign: str
    pins: dict
    volume: int
    scope: str

    def name(self, kind, job):
        return f"j2-{kind}-{self.campaign}-{self.run}-{self.attempt}-job-{job}"

    def binding(self, job, job_id):
        return {
            "campaign": self.campaign,
            "run": self.run,
            "attempt": self.attempt,
            "head": self.head,
            "job": job,
            "job_id": job_id,
            "scope": self.scope,
        }

    def matches(self, binding, job, job_id):
        return canonical(binding) == canonical(self.binding(job, job_id))


@dataclass
class Budget:
    end: float
    now: object = time.monotonic
    sleep: object = time.sleep

    def remaining(self):
        left = self.end - self.now()
        require(left > 0, "barrier budget expired", reason="barrier_timeout")
        return left

    def wait(self, delay=POLL_SECONDS):
        self.sleep(min(delay, self.remaining()))


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        return None


class ActionsAPI:
    """Use authenticated repository REST and a separate anonymous storage request."""

    def __init__(self, repository, token, budget, record, *, opener=None):
        require(
            re.fullmatch(r"[\w.-]+/[\w.-]+", repository) and token, "missing repository or token"
        )
        self.root = f"https://api.github.com/repos/{repository}/actions"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        self.budget = budget
        self.record = record
        self.opener = opener if opener is not None else build_opener(NoRedirect())

    def request(self, url, operation, *, authenticated, redirect=False, limit=REST_LIMIT):
        require(
            not authenticated or url.startswith(self.root + "/"),
            "authenticated request outside repository API",
        )
        request = Request(url, headers=self.headers if authenticated else {})
        try:
            with self.opener.open(request, timeout=min(15, self.budget.remaining())) as response:
                self.record("http", operation=operation, status=response.status)
                require(
                    response.status == 200 and not redirect, f"unexpected response for {operation}"
                )
                data = response.read(limit + 1)
                self.budget.remaining()
                require(len(data) <= limit, f"{operation} response exceeds size limit")
                return data
        except HTTPError as error:
            with error:
                code, headers = error.code, error.headers
                self.record("http", operation=operation, status=code)
                if redirect and code == 302:
                    location = headers.get("Location", "")
                    try:
                        target = urlsplit(location)
                        safe = target.scheme == "https" and target.hostname and not target.username
                        safe = safe and target.password is None and not target.fragment
                    except ValueError:
                        safe = False
                    require(safe, "invalid artifact storage redirect")
                    return location
                retry_after = headers.get("Retry-After", "")
                primary_limit = headers.get("X-RateLimit-Remaining") == "0"
                secondary_limit = (
                    code == 403 and b"secondary rate limit" in error.read(2048).lower()
                )
                limited = code == 429 or (
                    code == 403 and (retry_after.isdecimal() or primary_limit or secondary_limit)
                )
                if limited or 500 <= code <= 599 or (code == 404 and redirect):
                    delay = (
                        int(retry_after)
                        if retry_after.isdecimal()
                        else (60 if limited else POLL_SECONDS)
                    )
                    reset = headers.get("X-RateLimit-Reset", "")
                    if limited and primary_limit and reset.isdecimal():
                        delay = max(delay, int(reset) - time.time() + 1)
                    raise RetryLater(
                        f"{operation}: HTTP {code}", max(POLL_SECONDS, delay)
                    ) from None
                raise BarrierStop("api_failure", f"{operation}: HTTP {code}") from None
        except (URLError, OSError, HTTPException):
            raise RetryLater(f"{operation}: transport unavailable") from None

    def pages(self, route, member):
        page = 1
        while True:
            data = decode(
                self.request(
                    f"{self.root}/{route}?per_page={PAGE_SIZE}&page={page}",
                    f"{member} page {page}",
                    authenticated=True,
                )
            )
            rows = data.get(member) if isinstance(data, dict) else None
            require(isinstance(rows, list) and len(rows) <= PAGE_SIZE, f"invalid {member} page")
            require(all(isinstance(row, dict) for row in rows), f"invalid {member} rows")
            yield from rows
            if len(rows) < PAGE_SIZE:
                return
            page += 1

    def jobs(self, campaign, *, allow_failed=False):
        jobs = {}
        names = {job_name(job): job for job in JOBS}
        for row in self.pages(f"runs/{campaign.run}/attempts/{campaign.attempt}/jobs", "jobs"):
            name = row.get("name")
            require(isinstance(name, str), "invalid job name")
            job = names.get(name)
            if job is None:
                continue
            require(job not in jobs, f"duplicate job name {job_name(job)}")
            require(
                positive(row.get("id"))
                and row.get("run_id") == campaign.run
                and row.get("head_sha") == campaign.head,
                f"job {job} identity drift",
            )
            status, conclusion = row.get("status"), row.get("conclusion")
            require(
                isinstance(status, str)
                and status
                in {"queued", "in_progress", "completed", "waiting", "requested", "pending"}
                and (
                    (status == "completed" and isinstance(conclusion, str) and bool(conclusion))
                    or (status != "completed" and conclusion is None)
                ),
                f"invalid job {job} status",
            )
            self.record("peer", job=job, job_id=row["id"], status=status, conclusion=conclusion)
            if not allow_failed and status == "completed" and conclusion != "success":
                raise BarrierStop("peer_failure", f"job {job} completed with {conclusion}")
            jobs[job] = row
        require(len({row["id"] for row in jobs.values()}) == len(jobs), "duplicate job ID")
        return jobs

    def artifacts(self, campaign, names):
        artifacts = {}
        for row in self.pages(f"runs/{campaign.run}/artifacts", "artifacts"):
            name = row.get("name")
            require(isinstance(name, str), "invalid artifact name")
            if name not in names:
                continue
            require(name not in artifacts, f"duplicate artifact {name}")
            run = row.get("workflow_run")
            value = row.get("digest")
            require(
                positive(row.get("id"))
                and row.get("expired") is False
                and expiry(row.get("expires_at"))
                and isinstance(value, str)
                and value.startswith("sha256:")
                and sha256(value[7:])
                and isinstance(run, dict)
                and run.get("id") == campaign.run
                and run.get("head_sha") == campaign.head,
                f"invalid metadata for {name}",
            )
            self.record("artifact", name=name, artifact_id=row["id"], sha256=value[7:])
            artifacts[name] = row
        require(
            len({row["id"] for row in artifacts.values()}) == len(artifacts),
            "duplicate artifact ID",
        )
        return artifacts

    def download(self, artifact):
        artifact_id = artifact["id"]
        location = self.request(
            f"{self.root}/artifacts/{artifact_id}/zip",
            f"artifact {artifact_id} redirect",
            authenticated=True,
            redirect=True,
        )
        data = self.request(
            location, f"artifact {artifact_id} storage", authenticated=False, limit=ZIP_LIMIT
        )
        require(
            "sha256:" + digest(data) == artifact["digest"],
            f"artifact {artifact_id} ZIP digest mismatch",
        )
        return data


def read_bundle(raw, kind):
    json_name, sha_name = f"{kind}.json", f"{kind}.sha256"
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            entries = archive.infolist()
            require(
                len(entries) == 2
                and {entry.filename for entry in entries} == {json_name, sha_name}
                and all(
                    not stat.S_ISLNK(entry.external_attr >> 16)
                    and entry.file_size <= (65 if entry.filename == sha_name else ZIP_LIMIT)
                    for entry in entries
                ),
                f"invalid {kind} ZIP members",
            )
            data = archive.read(json_name)
            sha = digest(data)
            require(archive.read(sha_name) == (sha + "\n").encode(), f"{kind}.json digest mismatch")
    except (OSError, zipfile.BadZipFile, RuntimeError, NotImplementedError) as error:
        raise BarrierStop("unverifiable_evidence", f"unreadable {kind} ZIP") from error
    body = decode(data)
    try:
        encoded = canonical(body)
    except (ValueError, RecursionError) as error:
        raise BarrierStop("unverifiable_evidence", f"unencodable {kind}.json") from error
    require(encoded == data, f"noncanonical {kind}.json")
    return body, sha


def validate_grid(body, campaign, job, job_id):
    schema(body, "binding pins qualified_volume evidence cells stop", "grid")
    require(
        campaign.matches(body["binding"], job, job_id), "grid campaign/run/attempt/head/job drift"
    )
    require(
        body["pins"] == campaign.pins
        and type(body["qualified_volume"]) is int
        and body["qualified_volume"] == campaign.volume,
        "grid pins or qualification drift",
    )
    schema(body["evidence"], "name id sha256 index_sha256", "grid evidence")
    evidence = body["evidence"]
    require(
        evidence["name"] == campaign.name("grid-evidence", job)
        and positive(evidence["id"])
        and sha256(evidence["sha256"])
        and sha256(evidence["index_sha256"]),
        "invalid grid evidence reference",
    )
    if body["stop"] is not None:
        schema(body["stop"], "classification reason", "grid stop")
        classification, detail = body["stop"]["classification"], body["stop"]["reason"]
        require(
            isinstance(classification, str)
            and classification in {"integrity_stop", "invalid"}
            and isinstance(detail, str)
            and detail,
            "invalid grid stop",
        )
        raise BarrierStop(classification, f"job {job}: {detail}")
    cells = body["cells"]
    require(isinstance(cells, list) and len(cells) == len(RATES), "incomplete five-cell grid")
    for cell, rate in zip(cells, ORDERS[job - 1], strict=True):
        schema(
            cell,
            "rate offered_rate counts outcome reason checked reset_verified evidence_sha256",
            "cell",
        )
        require(
            cell["rate"] == rate and cell["offered_rate"] == RATES[rate], "grid rate or order drift"
        )
        outcome = cell["outcome"]
        require(isinstance(outcome, str) and outcome in CELL_OUTCOMES, "unknown cell outcome")
        if outcome == "integrity_stop":
            raise BarrierStop("integrity_stop", f"job {job} cell {rate}: {cell['reason']}")
        require(outcome != "unperformed", "grid includes an unperformed cell")
        require(
            type(cell["checked"]) is bool
            and cell["reset_verified"] is True
            and sha256(cell["evidence_sha256"]),
            f"missing check/reset or evidence for cell {rate}",
        )
        schema(cell["counts"], " ".join(COUNTS), "counts")
        counts = [cell["counts"][name] for name in COUNTS]
        require(
            all(type(n) is int and n >= 0 for n in counts)
            and counts == sorted(counts, reverse=True)
            and counts[0] == population(rate, WARMUP + MEASURED),
            "invalid cell counts",
        )
        require(
            (
                outcome == "pass"
                and cell["reason"] is None
                and cell["checked"]
                and len(set(counts)) == 1
            )
            or (
                outcome in {"drain_failure", "gate_failure"}
                and cell["checked"]
                and isinstance(cell["reason"], str)
                and bool(cell["reason"])
            )
            or (
                outcome == "invalid"
                and isinstance(cell["reason"], str)
                and cell["reason"] in CELL_INVALID
            ),
            f"invalid evidence or classification for cell {rate}",
        )
    return body


def decide(grids, grid_hashes, volume):
    require(set(grids) == set(grid_hashes) == set(JOBS), "three verified grids required")
    passed = set(RATES)
    for job in JOBS:
        passed.intersection_update(
            cell["rate"] for cell in grids[job]["cells"] if cell["outcome"] == "pass"
        )
    eligible = [rate for rate in RATES if rate in passed and soak_population(rate) <= volume]
    return {
        "grid_sha256": [grid_hashes[job] for job in JOBS],
        "selected_rate": max(eligible, key=lambda rate: Decimal(RATES[rate]), default=NO_RATE),
    }


@dataclass
class Barrier:
    campaign: Campaign
    api: ActionsAPI
    loaded: dict = field(default_factory=dict)

    def retry(self, operation):
        while True:
            try:
                self.api.budget.remaining()
                return operation()
            except RetryLater as error:
                self.api.record("retry", detail=str(error), delay=error.delay)
                self.api.budget.wait(error.delay)

    def bundle(self, kind, artifact):
        key = (artifact["id"], artifact["digest"], kind)
        if key not in self.loaded:
            self.loaded[key] = read_bundle(self.api.download(artifact), kind)
        return self.loaded[key]

    def grids(self):
        jobs = self.api.jobs(self.campaign)
        grid_names = {job: self.campaign.name("grid", job) for job in JOBS}
        evidence_names = {job: self.campaign.name("grid-evidence", job) for job in JOBS}
        artifacts = self.api.artifacts(
            self.campaign, set(grid_names.values()) | set(evidence_names.values())
        )
        grids, hashes = {}, {}
        for job, name in grid_names.items():
            if job not in jobs or name not in artifacts:
                continue
            body, sha = self.bundle("grid", artifacts[name])
            grids[job] = validate_grid(body, self.campaign, job, jobs[job]["id"])
            hashes[job] = sha
            evidence = body["evidence"]
            if evidence["name"] not in artifacts:
                continue
            metadata = artifacts[evidence["name"]]
            require(
                metadata["id"] == evidence["id"]
                and metadata["digest"] == "sha256:" + evidence["sha256"],
                f"job {job} grid evidence artifact drift",
            )
            index, index_hash = self.bundle("evidence", metadata)
            require(
                index_hash == evidence["index_sha256"]
                and isinstance(index, dict)
                and index.keys() == {"binding", "cells"}
                and self.campaign.matches(index["binding"], job, jobs[job]["id"])
                and index["cells"]
                == {
                    cell["rate"]: {k: v for k, v in cell.items() if k != "evidence_sha256"}
                    for cell in body["cells"]
                }
                and all(
                    digest(canonical(index["cells"][cell["rate"]])) == cell["evidence_sha256"]
                    for cell in body["cells"]
                ),
                f"job {job} grid evidence index mismatch",
            )
        missing = [
            job
            for job in JOBS
            if job not in grids
            or evidence_names[job] not in artifacts
            or jobs[job]["status"] not in {"in_progress", "completed"}
        ]
        if missing:
            raise RetryLater(f"waiting for grids and evidence from jobs {missing}")
        decision = decide(grids, hashes, self.campaign.volume)
        self.api.budget.remaining()
        return decision, jobs

    def decisions(self, expected, jobs, *, only=JOBS, receipt=None):
        names = {job: self.campaign.name("decision", job) for job in only}
        artifacts = self.api.artifacts(self.campaign, set(names.values()))
        missing = sorted(set(names.values()) - artifacts.keys())
        if missing:
            raise RetryLater("waiting for decision upload visibility")
        receipts = {}
        for job, name in names.items():
            artifact = artifacts[name]
            body, _ = self.bundle("decision", artifact)
            schema(body, "binding decision", "decision")
            require(
                self.campaign.matches(body["binding"], job, jobs[job]["id"]),
                "decision identity drift",
            )
            require(
                body["decision"] == expected and canonical(body["decision"]) == canonical(expected),
                "local decisions disagree",
                reason="decision_disagreement",
            )
            receipts[job] = {"id": artifact["id"], "sha256": artifact["digest"][7:]}
        if receipt is not None:
            require(
                len(only) == 1 and receipts[only[0]] == receipt, "decision upload receipt drift"
            )
        self.api.budget.remaining()
        return receipts
