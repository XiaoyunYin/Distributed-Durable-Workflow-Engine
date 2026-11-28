import io
import sys
import zipfile
from dataclasses import replace
from email.message import Message
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import j2_barrier as barrier  # noqa: E402
import j2_barrier_rehearsal as rehearsal  # noqa: E402


class Clock:
    seconds = 0

    def now(self):
        return self.seconds

    def sleep(self, seconds):
        self.seconds += seconds


class Response(io.BytesIO):
    status = 200


def archive(kind, body, *, bad_hash=False, extra=None):
    raw = barrier.canonical(body)
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as bundle:
        bundle.writestr(f"{kind}.json", raw)
        bundle.writestr(f"{kind}.sha256", ("0" * 64 if bad_hash else barrier.digest(raw)) + "\n")
        if extra:
            bundle.writestr(extra, b"extra")
    return out.getvalue()


class GitHub:
    def __init__(self, scenario="eligible", volume=26678, seconds=barrier.BARRIER_SECONDS):
        self.campaign = barrier.Campaign(
            "owner/project",
            72,
            3,
            "f" * 40,
            f"eng-{scenario}",
            {"checker": "c" * 64},
            volume,
            "synthetic",
        )
        self.clock = Clock()
        self.log, self.requests, self.rows, self.blobs, self.errors = [], [], {}, {}, {}
        self.jobs = [
            {
                "id": 100 + job,
                "run_id": 72,
                "head_sha": "f" * 40,
                "name": barrier.job_name(job),
                "status": "in_progress",
                "conclusion": None,
            }
            for job in barrier.JOBS
        ]
        self.api = barrier.ActionsAPI(
            self.campaign.repository,
            "test-token",
            barrier.Budget(seconds, self.clock.now, self.clock.sleep),
            self.record,
            opener=self,
        )
        self.barrier = barrier.Barrier(self.campaign, self.api)

    def record(self, event, **fields):
        self.log.append({"event": event, **fields})

    def add(self, kind, job, raw):
        name = self.campaign.name(kind, job)
        artifact_id = len(self.blobs) + 1
        row = {
            "name": name,
            "id": artifact_id,
            "digest": "sha256:" + barrier.digest(raw),
            "expires_at": "2099-02-27T00:00:00Z",
            "expired": False,
            "workflow_run": {"id": self.campaign.run, "head_sha": self.campaign.head},
        }
        self.rows[name] = row
        self.blobs[artifact_id] = raw
        return row

    def open(self, request, timeout):
        target = urlsplit(request.full_url)
        self.requests.append(
            (target.netloc, target.path, target.query, request.get_header("Authorization"), timeout)
        )
        if target.netloc == "storage.example":
            assert request.get_header("Authorization") is None
            return Response(self.blobs[int(target.path.split("/")[-1])])
        assert target.netloc == "api.github.com"
        assert request.get_header("Authorization") == "Bearer test-token"
        if target.path in self.errors:
            status, headers = self.errors.pop(target.path)
            raise self.http_error(request, status, headers)
        if target.path.endswith("/zip"):
            artifact_id = target.path.split("/")[-2]
            raise self.http_error(
                request, 302, {"Location": f"https://storage.example/{artifact_id}?secret=signed"}
            )
        assert target.path.endswith(("attempts/3/jobs", "runs/72/artifacts"))
        member = target.path.split("/")[-1]
        rows = self.jobs if member == "jobs" else list(self.rows.values())
        page = int(parse_qs(target.query)["page"][0])
        start = (page - 1) * barrier.PAGE_SIZE
        return Response(barrier.canonical({member: rows[start : start + barrier.PAGE_SIZE]}))

    @staticmethod
    def http_error(request, status, headers):
        message = Message()
        for key, value in headers.items():
            message[key] = value
        return HTTPError(request.full_url, status, str(status), message, io.BytesIO())

    def grid(self, job, *, change=None, bad_hash=False):
        campaign = self.campaign
        scenario = campaign.campaign.removeprefix("eng-")
        cells = rehearsal.fixture_cells(scenario, job)
        binding = campaign.binding(job, 100 + job)
        evidence = {"binding": binding, "cells": cells}
        row = self.add("grid-evidence", job, archive("evidence", evidence))
        body = {
            "binding": dict(binding),
            "pins": campaign.pins,
            "qualified_volume": campaign.volume,
            "evidence": {
                "name": row["name"],
                "id": row["id"],
                "sha256": row["digest"][7:],
                "index_sha256": barrier.digest(barrier.canonical(evidence)),
            },
            "cells": [
                {**cell, "evidence_sha256": barrier.digest(barrier.canonical(cell))}
                for cell in cells.values()
            ],
            "stop": None,
        }
        if change:
            change(body)
        return self.add("grid", job, archive("grid", body, bad_hash=bad_hash))

    def decision(self, job, decision, *, binding=None):
        return self.add(
            "decision",
            job,
            archive(
                "decision",
                {
                    "binding": binding or self.campaign.binding(job, 100 + job),
                    "decision": decision,
                },
            ),
        )


@pytest.mark.parametrize(
    ("scenario", "volume", "rate"),
    [
        ("eligible", 26678, "AB"),
        ("eligible", 26677, "A"),
        ("eligible", 21002, "A"),
        ("no-eligible", 26678, barrier.NO_RATE),
    ],
)
def test_three_local_decisions_and_matching_reporting_bytes(scenario, volume, rate):
    gh = GitHub(scenario, volume)
    for job in barrier.JOBS:
        gh.grid(job)
    independent = [barrier.Barrier(gh.campaign, gh.api).grids()[0] for _ in barrier.JOBS]
    assert len({barrier.canonical(value) for value in independent}) == 1
    decision = independent[0]
    assert decision["selected_rate"] == rate
    assert tuple(map(barrier.soak_population, ("A", "AB", "B"))) == (21002, 26678, 34703)
    assert len(set(decision["grid_sha256"])) == 3
    for job in barrier.JOBS:
        gh.decision(job, decision)
    _, jobs = gh.barrier.grids()
    assert gh.barrier.decisions(decision, jobs).keys() == set(barrier.JOBS)
    assert any(host == "storage.example" for host, *_ in gh.requests)
    assert all("secret" not in str(event) for event in gh.log)


@pytest.mark.parametrize(
    "change",
    [
        None,
        "bound",
        "checked_count",
        "window_count",
        "resource_invalid",
        "unverified_reset",
        "incomplete",
    ],
)
def test_registered_grid_binds_actual_window_counts(change):
    gh = GitHub()
    gh.campaign = replace(gh.campaign, scope=barrier.REGISTERED_GRID_SCOPE)

    def population_rows(body):
        for cell in body["cells"]:
            offered = cell["counts"]["offered"]
            checked = cell["counts"]["succeeded"]
            warmup = barrier.population(cell["rate"], barrier.WARMUP)
            warm_checked = min(warmup, checked)
            cell["population"] = dict(
                bound=offered,
                scheduled=offered,
                windows={
                    "warmup": dict(
                        scheduled=warmup, checked_success=warm_checked, in_window_success=0
                    ),
                    "grid": dict(
                        scheduled=offered - warmup,
                        checked_success=checked - warm_checked,
                        in_window_success=0,
                    ),
                },
            )
            cell["recovery"] = None
        population = body["cells"][0]["population"]
        if change == "bound":
            population["bound"] -= 1
        elif change == "checked_count":
            population["windows"]["grid"]["checked_success"] -= 1
        elif change == "window_count":
            population["windows"]["warmup"]["scheduled"] += 1
        elif change in {"resource_invalid", "unverified_reset"}:
            body["cells"][0].update(
                outcome="invalid",
                reason="resource_evidence",
                recovery={
                    "replaced_with": "AB",
                    "telemetry_sha256": "a" * 64,
                    "identity_sha256": "b" * 64,
                    "series": ["j2_outstanding"],
                }
                if change == "resource_invalid"
                else None,
            )
        elif change == "incomplete":
            body["cells"].pop()

    meta = gh.grid(1, change=population_rows)
    body, _ = barrier.read_bundle(gh.blobs[meta["id"]], "grid")
    if change in {None, "resource_invalid"}:
        assert barrier.validate_grid(body, gh.campaign, 1, 101) == body
        if change:
            assert (
                barrier.decide(
                    dict.fromkeys(barrier.JOBS, body), dict.fromkeys(barrier.JOBS, "hash"), 26678
                )["selected_rate"]
                == "AB"
            )
    else:
        with pytest.raises(
            barrier.BarrierStop, match="population|invalid cell recovery|incomplete"
        ):
            barrier.validate_grid(body, gh.campaign, 1, 101)


def test_paginated_lists_and_delayed_visibility_use_the_same_local_path(monkeypatch):
    monkeypatch.setattr(barrier, "PAGE_SIZE", 2)
    gh = GitHub()
    gh.grid(1)
    gh.grid(2)
    original = gh.clock.sleep

    def appear_after_poll(seconds):
        original(seconds)
        gh.grid(3)

    gh.api.budget.sleep = appear_after_poll
    decision, _ = gh.barrier.retry(gh.barrier.grids)
    assert decision["selected_rate"] == "AB"
    assert gh.clock.seconds == barrier.POLL_SECONDS
    assert any("page=2" in query for _, path, query, *_ in gh.requests if path.endswith("/jobs"))
    assert any(
        "page=3" in query for _, path, query, *_ in gh.requests if path.endswith("/artifacts")
    )


@pytest.mark.parametrize(
    "fault", ["retry_after", "rate_limit", "bad_auth", "throttled", "upstream"]
)
def test_bounded_api_backoff_and_permanent_auth_failure(fault):
    gh = GitHub()
    for job in barrier.JOBS:
        gh.grid(job)
    path = "/repos/owner/project/actions/runs/72/attempts/3/jobs"
    gh.errors[path] = {
        "retry_after": (403, {"Retry-After": "8"}),
        "rate_limit": (403, {"X-RateLimit-Remaining": "0"}),
        "bad_auth": (403, {}),
        "throttled": (429, {}),
        "upstream": (503, {}),
    }[fault]
    if fault == "bad_auth":
        with pytest.raises(barrier.BarrierStop, match="HTTP 403") as stopped:
            gh.barrier.retry(gh.barrier.grids)
        assert stopped.value.reason == "api_failure"
        assert gh.clock.seconds == 0
    else:
        assert gh.barrier.retry(gh.barrier.grids)[0]["selected_rate"] == "AB"
        assert gh.clock.seconds == (
            60 if fault in {"throttled", "rate_limit"} else barrier.POLL_SECONDS
        )


@pytest.mark.parametrize(("seconds", "foreign_name"), [(1, True), (barrier.BARRIER_SECONDS, False)])
def test_missing_peer_grid_never_decides_and_budget_is_not_restarted(seconds, foreign_name):
    gh = GitHub(seconds=seconds)
    gh.grid(1)
    gh.grid(2)
    if foreign_name:
        row = gh.grid(3)
        row["name"] = row["name"].replace("-72-3-job-3", "-72-2-job-3")
    with pytest.raises(barrier.BarrierStop) as stopped:
        gh.barrier.retry(gh.barrier.grids)
    assert stopped.value.reason == "barrier_timeout"
    assert gh.clock.seconds == seconds
    assert any(row.get("job") == 3 and row.get("status") == "in_progress" for row in gh.log)


def test_checked_ab_expiry_only_disqualifies_ab(monkeypatch):
    original = rehearsal.fixture_cells

    def cells_with_expiry(scenario, job):
        cells = original(scenario, job)
        if job == 2:
            cells["AB"].update(outcome="drain_failure", reason="checked expiry")
        return cells

    monkeypatch.setattr(rehearsal, "fixture_cells", cells_with_expiry)
    gh = GitHub()
    for job in barrier.JOBS:
        gh.grid(job)
    assert gh.barrier.grids()[0]["selected_rate"] == "A"


def test_grid_preparation_rejects_insufficient_remaining_job_time():
    ready = 1500
    end = ready + barrier.BARRIER_SECONDS
    assert barrier.barrier_deadline(ready, end) == end
    with pytest.raises(barrier.BarrierStop) as stopped:
        barrier.barrier_deadline(ready, end - 1)
    assert stopped.value.reason == "insufficient_budget"


@pytest.mark.parametrize("conclusion", ["failure", "cancelled", "timed_out", "skipped"])
def test_terminal_peer_blocks_even_after_its_manifest_is_visible(conclusion):
    gh = GitHub()
    for job in barrier.JOBS:
        gh.grid(job)
    gh.jobs[2].update(status="completed", conclusion=conclusion)
    with pytest.raises(barrier.BarrierStop) as stopped:
        gh.barrier.retry(gh.barrier.grids)
    assert stopped.value.reason == "peer_failure"
    assert gh.clock.seconds == 0


@pytest.mark.parametrize(
    ("fault", "reason"),
    [
        ("wrong_attempt", "unverifiable_evidence"),
        ("wrong_pins", "unverifiable_evidence"),
        ("bad_manifest_hash", "unverifiable_evidence"),
        ("bad_zip_hash", "unverifiable_evidence"),
        ("duplicate_artifact", "unverifiable_evidence"),
        ("duplicate_job", "unverifiable_evidence"),
        ("expired", "unverifiable_evidence"),
        ("unchecked_drain", "unverifiable_evidence"),
        ("not_reset", "unverifiable_evidence"),
        ("integrity_stop", "integrity_stop"),
        ("invalid", "invalid"),
    ],
)
def test_invalid_grids_never_produce_a_decision(fault, reason):
    gh = GitHub()
    gh.grid(1)
    gh.grid(2)

    def change(body):
        if fault == "wrong_attempt":
            body["binding"]["attempt"] -= 1
        elif fault == "wrong_pins":
            body["pins"] = {}
        elif fault == "unchecked_drain":
            next(cell for cell in body["cells"] if cell["rate"] == "C")["checked"] = False
        elif fault == "not_reset":
            body["cells"][0]["reset_verified"] = False
        elif fault in {"integrity_stop", "invalid"}:
            body["stop"] = {"classification": fault, "reason": "fixture stop"}

    row = gh.grid(3, change=change, bad_hash=fault == "bad_manifest_hash")
    if fault == "bad_zip_hash":
        row["digest"] = "sha256:" + "0" * 64
    elif fault == "expired":
        row["expired"] = True
    elif fault == "duplicate_artifact":
        gh.rows["duplicate"] = dict(row)
    elif fault == "duplicate_job":
        gh.jobs.append(dict(gh.jobs[2]))
    with pytest.raises(barrier.BarrierStop) as stopped:
        gh.barrier.retry(gh.barrier.grids)
    assert stopped.value.reason == reason
    assert gh.clock.seconds == 0


def test_decision_disagreement_or_foreign_receipt_blocks_reporting():
    gh = GitHub()
    for job in barrier.JOBS:
        gh.grid(job)
    decision, jobs = gh.barrier.grids()
    gh.decision(1, decision)
    gh.decision(2, decision)
    gh.decision(3, {**decision, "selected_rate": "A"})
    with pytest.raises(barrier.BarrierStop) as stopped:
        gh.barrier.decisions(decision, jobs)
    assert stopped.value.reason == "decision_disagreement"
    with pytest.raises(barrier.BarrierStop, match="receipt drift"):
        gh.barrier.decisions(decision, jobs, only=(1,), receipt={"id": 99, "sha256": "f" * 64})


def test_zip_member_set_rejects_extra_and_duplicate_members():
    with pytest.raises(barrier.BarrierStop, match="ZIP members"):
        barrier.read_bundle(archive("grid", {}, extra="../outside"), "grid")
    with pytest.warns(UserWarning, match="Duplicate name"):
        raw = archive("grid", {}, extra="grid.json")
    with pytest.raises(barrier.BarrierStop, match="ZIP members"):
        barrier.read_bundle(raw, "grid")


@pytest.mark.parametrize("disagree", [False, True])
def test_reporting_job_independently_verifies_decision_artifacts(tmp_path, monkeypatch, disagree):
    gh = GitHub()
    for job in barrier.JOBS:
        gh.grid(job)
    decision, _ = gh.barrier.grids()
    for job in barrier.JOBS:
        selected = {**decision, "selected_rate": "A"} if disagree and job == 3 else decision
        upload = gh.decision(job, selected)
        gh.add(
            "outcome",
            job,
            archive(
                "outcome",
                {
                    "binding": gh.campaign.binding(job, 100 + job),
                    "soak": "unperformed",
                    "outcome": "would_start",
                    "decision": selected,
                    "decision_upload": {"id": upload["id"], "sha256": upload["digest"][7:]},
                },
            ),
        )
        gh.jobs[job - 1].update(status="completed", conclusion="success")
    monkeypatch.setattr(rehearsal, "context", lambda _: gh.campaign)
    monkeypatch.setattr(rehearsal, "client", lambda *_: gh.barrier)
    report = rehearsal.report_round(tmp_path, "eligible", barrier.BARRIER_SECONDS)
    assert report["soak"] == "unperformed"
    if disagree:
        assert report["status"] == "FAIL"
        assert report["reason"] == "decision_disagreement"
        assert "agreement" not in report
    else:
        assert report["status"] == "PASS"
        assert report["agreement"] == decision


def test_report_synthesizes_unperformed_when_a_runner_loses_its_archive(tmp_path, monkeypatch):
    gh = GitHub(seconds=1)
    for job in barrier.JOBS:
        gh.jobs[job - 1].update(status="completed", conclusion="success")
    for job in (1, 2):
        gh.add(
            "outcome",
            job,
            archive(
                "outcome",
                {
                    "binding": gh.campaign.binding(job, 100 + job),
                    "soak": "unperformed",
                    "outcome": "no_eligible_rate",
                },
            ),
        )
    monkeypatch.setattr(rehearsal, "context", lambda _: gh.campaign)
    monkeypatch.setattr(rehearsal, "client", lambda *_: gh.barrier)
    report = rehearsal.report_round(tmp_path, "no-eligible", 1)
    assert report["status"] == "FAIL"
    assert report["outcomes"][3] == {
        "outcome": "evidence_lost",
        "soak": "unperformed",
        "job": gh.jobs[2],
    }
    assert gh.clock.seconds == 1
