import base64
import io
import json
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import j2_dispatch_manifest as packet  # noqa: E402
import j2_measure as measure  # noqa: E402
import j2_measure_authorization as auth  # noqa: E402
from j2_barrier import NO_RATE, BarrierStop, barrier_deadline, canonical, digest  # noqa: E402
from j2_checker_report import ResourceEvidenceError  # noqa: E402
from j2_measure_authorization import (  # noqa: E402
    AUTH_SCHEMA,
    authorization_branch,
    cell_traffic,
    check_append_only,
    fetch_ledger,
    traffic_disposition,
    validate_ledger,
    verify_zero_requests,
)
from j2_measure_telemetry import (  # noqa: E402
    METRICS,
    cell_samples,
    checked_configuration,
    coverage_gaps,
    runner_inventory,
)
from j2_overlay import PROM, render  # noqa: E402
from j2_qualification_evidence import read, write  # noqa: E402
from test_j2_schedule import arrival, encoded, stock_plan  # noqa: E402

S = 10**9
CAMPAIGN = "eng-inc2-real-grid"
HEAD = "a" * 40
CONTROLS = "b" * 64
CONTEXT = dict(
    repository="owner/project", campaign=CAMPAIGN, run=91, attempt=1, head=HEAD, controls=CONTROLS
)


@pytest.fixture
def frozen_dispatch_checkout(tmp_path, monkeypatch):
    basis = "86f62726d54b9589d1d7f202b1deaefb4c284565"
    original_git = packet.git_output
    root = tmp_path / "frozen-checkout"
    # This is the accepted incarnation of the packet. 5a changes the source
    # identities, and 5c will publish the next packet.
    paths = {
        *packet.PIN_FILES.values(),
        *packet.TOOLING_FILES,
        *auth.LEGACY_CONTROLS_PATHS,
        (packet.PACKET / "manifest.json").as_posix(),
        *((packet.PACKET / name).as_posix() for name in packet.EVIDENCE_FILES),
    }
    for name in paths:
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(original_git(measure.ROOT, "show", f"{basis}:{name}"))
    monkeypatch.setattr(
        packet, "git_output", lambda _root, *args: original_git(measure.ROOT, *args)
    )
    return root


def test_frozen_dispatch_packet_matches_the_registered_schedule_and_prerequisites(
    frozen_dispatch_checkout,
):
    root = frozen_dispatch_checkout
    pins = {label: packet.text_sha(root / path) for label, path in packet.PIN_FILES.items()}
    proof = packet.verify_package(root, pins)
    assert len(proof["prerequisites"]) == 3
    results = packet.schedule_contract()["continuous"]
    assert results["AB"]["bound"] == 26678 and results["AB"]["scheduled"] == 26677


def test_r422_packet_binds_both_control_archives(frozen_dispatch_checkout, monkeypatch):
    root = frozen_dispatch_checkout
    marker = root / auth.R422_MARKER
    marker.touch()
    monkeypatch.setattr(auth, "CONTROLS_PATHS", auth.LEGACY_CONTROLS_PATHS + (auth.R422_MARKER,))
    pins = {label: packet.text_sha(root / path) for label, path in packet.PIN_FILES.items()}
    directory = root / packet.PACKET
    control_path = directory / "controls" / "verification.json"
    manifest_path = directory / "manifest.json"
    controls, manifest = read(control_path), read(manifest_path)
    controls["controls_sha256"] = digest(canonical(auth.control_identity(root)))
    with pytest.raises(ValueError, match="supplemental J2 controls source"):
        packet.verify_package(root, pins)

    run = 1234
    source = {
        "schema": "j2.r422-controls.v1",
        "run": run,
        "attempt": 1,
        "head": HEAD,
        "artifacts": {},
    }
    for role, suffix, artifact, checksum in (
        ("bundle", "", 4321, "a" * 64),
        ("receipt", "-receipt", 4322, "b" * 64),
    ):
        name = f"j2-controls-r422{suffix}-{run}-1"
        source["artifacts"][role] = {"id": artifact, "name": name, "bytes": 678, "sha256": checksum}
        entry = {"id": artifact, "digest": f"sha256:{checksum}"}
        controls["archives"][name] = {
            "source": {
                **entry,
                "name": name,
                "size_in_bytes": 678,
                "source_attempt": 1,
                "workflow_run": {"id": run, "head_sha": HEAD},
            },
        }
        manifest["prerequisites"][name] = entry
    controls["supplemental_controls"] = source

    def reseal():
        write(control_path, controls)
        manifest["evidence"]["controls/verification.json"] = packet.text_sha(control_path)
        write(manifest_path, manifest)

    reseal()
    proof = packet.verify_package(root, pins)
    assert proof["supplemental_controls"] == source
    assert len(proof["prerequisites"]) == 5
    controls["archives"][source["artifacts"]["receipt"]["name"]]["source"]["size_in_bytes"] += 1
    reseal()
    with pytest.raises(ValueError, match="bytes or attempt differ"):
        packet.verify_package(root, pins)


@pytest.mark.parametrize("change", ["missing", "digest", "seed"])
def test_frozen_dispatch_packet_refuses_missing_or_changed_evidence(
    tmp_path, monkeypatch, change, frozen_dispatch_checkout
):
    root = frozen_dispatch_checkout
    destination = tmp_path / "packet"
    shutil.copytree(root / packet.PACKET, destination)
    monkeypatch.setattr(packet, "PACKET", destination)
    pins = {label: packet.text_sha(root / path) for label, path in packet.PIN_FILES.items()}
    manifest_path, seed_path = (
        destination / name for name in ("manifest.json", "seed-vectors.json")
    )
    if change == "missing":
        manifest_path.unlink()
        error = "manifest is missing"
    else:
        seeds = read(seed_path)
        seeds["vectors"]["j2-load-envelope-v1/job-1/A"] += 1
        write(seed_path, seeds)
        error = "evidence hash differs"
        if change == "seed":
            manifest = read(manifest_path)
            manifest["evidence"]["seed-vectors.json"] = packet.text_sha(seed_path)
            write(manifest_path, manifest)
            error = "seed vectors differ"
    with pytest.raises(ValueError, match=error):
        packet.verify_package(root, pins)


@pytest.mark.parametrize("problem", [None, "expired", "inaccessible"])
def test_live_prerequisites_are_rechecked_before_bootstrap(problem):
    records = {}
    for name, (ident, checksum, run, head, _) in packet.prerequisite_archives().items():
        records[ident] = dict(
            id=ident,
            name=name,
            digest=f"sha256:{checksum}",
            workflow_run={"id": run, "head_sha": head},
            expired=problem == "expired",
            expires_at="2099-01-01T00:00:00Z",
        )

    def get(url, name, *, authenticated, redirect=False):
        assert authenticated
        if redirect:
            assert name == packet.QUALIFICATION_NAME
            return "https://example.invalid/qualification.zip"
        return canonical(records[int(url.rsplit("/", 1)[1])])

    class Reply(io.BytesIO):
        status = 206

    def open_qualification(request, *, timeout):
        assert request.get_header("Range") == "bytes=0-0" and timeout > 0
        return Reply(b"" if problem == "inaccessible" else b"P")

    api = SimpleNamespace(
        root="https://example.invalid/actions",
        request=get,
        opener=SimpleNamespace(open=open_qualification),
        budget=SimpleNamespace(remaining=lambda: 10),
    )
    if problem:
        with pytest.raises(ValueError, match=problem):
            packet.confirm_prerequisites(api)
    else:
        assert set(packet.confirm_prerequisites(api)) == set(packet.prerequisite_archives())


@pytest.mark.parametrize(
    "campaign", [packet.MEASURED_CAMPAIGN, packet.FINAL_REHEARSAL, f"eng-r422-final-{HEAD[:8]}"]
)
def test_missing_packet_blocks_traffic_before_authorization(tmp_path, monkeypatch, campaign):
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    monkeypatch.setenv("GITHUB_RUN_ID", "91")
    monkeypatch.setattr(measure, "ROOT", tmp_path)
    if campaign.startswith("eng-r422-final-"):
        marker = tmp_path / auth.R422_MARKER
        marker.parent.mkdir()
        marker.touch()
    monkeypatch.setattr(measure, "hosted_dispatch", lambda: None)
    monkeypatch.setattr(measure, "context", lambda _: SimpleNamespace(head=HEAD, pins={}))
    monkeypatch.setattr(measure.subprocess, "check_output", lambda *_, **__: HEAD)
    monkeypatch.setattr(
        measure, "authorization", lambda *_, **__: pytest.fail("read authorization")
    )
    result = tmp_path / "runner"
    with pytest.raises(ValueError, match="manifest is missing"):
        measure.begin(result, 1, campaign)
    assert read(result / "final/authorization-stop.json")["disposition"] == "no_requests"
    assert not (result / "job.json").exists()


def entry(**fields):
    return dict(
        dict(
            run_id=91,
            attempt=1,
            head=HEAD,
            controls_sha256=CONTROLS,
            disposition="authorized",
            reason=None,
            evidence_sha256=None,
        ),
        **fields,
    )


def ledger(*dispatches):
    return dict(
        schema=AUTH_SCHEMA,
        repository="owner/project",
        campaign_id=CAMPAIGN,
        reviewer="reviewer",
        dispatches=list(dispatches),
    )


@pytest.mark.parametrize(
    "change",
    [
        {"run": 92},
        {"attempt": 2},
        {"head": "c" * 40},
        {"controls": "d" * 64},
        {"campaign": "j2-load-envelope-v1"},
    ],
)
def test_only_exact_engineering_binding_authorizes(change):
    assert validate_ledger(ledger(entry()), **CONTEXT)["run_id"] == 91
    with pytest.raises(BarrierStop):
        validate_ledger(ledger(entry()), **(CONTEXT | change))


@pytest.mark.parametrize("prior", ["consumed", "unresolved", "authorized", "no_requests"])
def test_replacement_requires_reviewed_zero_requests(prior):
    previous = entry(
        run_id=90, disposition=prior, reason="preflight parent stopped", evidence_sha256="c" * 64
    )
    if prior == "no_requests":
        assert validate_ledger(ledger(previous, entry()), **CONTEXT)["run_id"] == 91
        with pytest.raises(BarrierStop, match="zero requests"):
            validate_ledger(ledger(dict(previous, evidence_sha256=None), entry()), **CONTEXT)
    else:
        with pytest.raises(BarrierStop, match="zero requests"):
            validate_ledger(ledger(previous, entry()), **CONTEXT)


def test_authorization_ref_update_cannot_rewrite_an_earlier_run():
    old = ledger(entry(run_id=90))
    closed = entry(
        run_id=90, disposition="no_requests", reason="before bootstrap", evidence_sha256="c" * 64
    )
    check_append_only(old, ledger(closed, entry()))
    with pytest.raises(BarrierStop, match="append"):
        check_append_only(old, ledger(entry()))
    with pytest.raises(BarrierStop, match="append"):
        check_append_only(old, ledger(dict(closed, run_id=89), entry()))


@pytest.mark.parametrize(
    ("campaign", "protected", "accepted"),
    [
        (CAMPAIGN, True, True),
        (CAMPAIGN, False, True),
        ("j2-load-envelope-v1", True, True),
        ("j2-load-envelope-v1", False, False),
    ],
)
def test_authorization_reads_expected_ref_then_immutable_revision(campaign, protected, accepted):
    revision = "c" * 40
    branch = authorization_branch(campaign)
    record = {**ledger(entry()), "campaign_id": campaign}
    raw = json.dumps(record).encode()
    requests = []

    class Result(io.BytesIO):
        status = 200

    class API:
        def open(self, request, timeout):
            requests.append(request.full_url)
            if len(requests) == 1:
                return Result(
                    json.dumps(
                        dict(name=branch, protected=protected, commit={"sha": revision})
                    ).encode()
                )
            return Result(
                json.dumps(
                    dict(
                        path="authorization.json",
                        type="file",
                        encoding="base64",
                        size=len(raw),
                        content=base64.b64encode(raw).decode(),
                    )
                ).encode()
            )

    if accepted:
        fetched = fetch_ledger("owner/project", "fake", campaign, opener=API())
        assert fetched["ledger"] == record
        assert requests[1].endswith(f"/contents/authorization.json?ref={revision}")
    else:
        with pytest.raises(BarrierStop, match="protected branch"):
            fetch_ledger("owner/project", "fake", campaign, opener=API())
        assert len(requests) == 1


@pytest.fixture
def authorization_checkout(tmp_path, monkeypatch):
    contents = {"scripts/controlled.py": b"reviewed\r\n"}
    monkeypatch.setattr(auth, "CONTROLS_PATHS", tuple(contents))

    def git(root, command, *args):
        assert root == tmp_path
        if command == "remote":
            assert args == ("get-url", "origin")
            return b"https://github.com/owner/project.git\n"
        if command == "rev-parse":
            assert args == ("--verify", "HEAD")
            return (HEAD + "\n").encode()
        if command == "ls-tree":
            assert args == (HEAD, "--", auth.R422_MARKER)
            return b"tracked marker"
        assert command == "show" and args[0].startswith(HEAD + ":")
        return contents[args[0].split(":", 1)[1]]

    monkeypatch.setattr(auth, "git_output", git)
    return tmp_path, digest(canonical({"scripts/controlled.py": digest(b"reviewed\n")}))


def authorize_args(path, *, run=91, head=HEAD):
    return [
        "authorize",
        "--campaign-id",
        CAMPAIGN,
        "--run-id",
        str(run),
        "--head",
        head,
        "--reviewer",
        "reviewer",
        "--output",
        str(path),
    ]


def close_args(path, proof):
    return [
        "close-no-requests",
        "--campaign-id",
        CAMPAIGN,
        "--run-id",
        "91",
        "--reason",
        "stopped before bootstrap",
        "--manifest",
        str(proof),
        "--output",
        str(path),
    ]


def zero_request_proof(**overrides):
    return dict(
        dict(
            campaign=CAMPAIGN,
            run_id=91,
            attempt=1,
            head=HEAD,
            errors={},
            disposition="no_requests",
            jobs={str(job): {"disposition": "no_requests"} for job in (1, 2, 3)},
        ),
        **overrides,
    )


def test_authorize_and_verify_round_trip_with_the_workflow(authorization_checkout, capsys):
    root, controls = authorization_checkout
    path = root / "authorization.json"
    assert auth.main(authorize_args(path), root=root) == 0
    raw = path.read_bytes()
    record = json.loads(raw)
    assert raw == canonical(record)
    active = validate_ledger(record, **(CONTEXT | {"controls": controls}))
    assert active["controls_sha256"] == controls
    other = root / "second-output.json"
    assert auth.main(authorize_args(other), root=root) == 0
    assert other.read_bytes() == raw
    assert auth.main(["verify", str(path)], root=root) == 0
    active["controls_sha256"] = "e" * 64
    path.write_bytes(canonical(record))
    with pytest.raises(SystemExit, match="1"):
        auth.main(["verify", str(path)], root=root)
    assert "control path applicability digest differs" in capsys.readouterr().err


def test_authorize_refuses_wrong_head_or_open_dispatch(authorization_checkout, capsys):
    root, _ = authorization_checkout
    path = root / "authorization.json"
    with pytest.raises(SystemExit, match="1"):
        auth.main(authorize_args(path, head="b" * 40), root=root)
    assert not path.exists()
    assert "checkout HEAD differs" in capsys.readouterr().err
    auth.main(authorize_args(path), root=root)
    original = path.read_bytes()
    with pytest.raises(SystemExit, match="1"):
        auth.main(authorize_args(path, run=92), root=root)
    assert path.read_bytes() == original
    assert "prior dispatch is still open" in capsys.readouterr().err


def test_close_and_append_preserve_both_workflow_history_paths(authorization_checkout):
    root, controls = authorization_checkout
    path = root / "authorization.json"
    proof = root / "campaign-dispatch-manifest.json"
    auth.main(authorize_args(path), root=root)
    initial = json.loads(path.read_bytes())
    write(proof, zero_request_proof())
    assert auth.main(close_args(path, proof), root=root) == 0
    assert auth.main(["verify", str(path)], root=root) == 0
    closed = json.loads(path.read_bytes())
    assert closed["dispatches"][0]["evidence_sha256"] == digest(proof.read_bytes())
    assert closed["dispatches"][0]["reason"] == "stopped before bootstrap"
    assert closed["dispatches"][0]["disposition"] == "no_requests"
    verify_zero_requests(proof.read_bytes(), closed["dispatches"][0], CAMPAIGN)
    assert auth.main(authorize_args(path, run=92), root=root) == 0
    appended = json.loads(path.read_bytes())
    check_append_only(initial, closed)
    check_append_only(closed, appended)
    check_append_only(initial, appended)
    assert appended["dispatches"][0] == closed["dispatches"][0]
    assert (
        validate_ledger(appended, **(CONTEXT | {"run": 92, "controls": controls}))["run_id"] == 92
    )


@pytest.mark.parametrize(
    "overrides",
    [
        {"run_id": 90},
        {"disposition": "consumed"},
        {"jobs": {"1": {"disposition": "no_requests"}, "2": {"disposition": "no_requests"}}},
        {"errors": {"3": "missing receipt"}},
    ],
)
def test_close_rejects_an_incomplete_or_wrong_zero_request_proof(
    authorization_checkout, capsys, overrides
):
    root, _ = authorization_checkout
    path = root / "authorization.json"
    proof = root / "campaign-dispatch-manifest.json"
    auth.main(authorize_args(path), root=root)
    original = path.read_bytes()
    write(proof, zero_request_proof(**overrides))
    with pytest.raises(SystemExit, match="1"):
        auth.main(close_args(path, proof), root=root)
    assert path.read_bytes() == original
    assert "receipt does not establish zero requests" in capsys.readouterr().err


def test_a_mismatched_dispatch_cannot_reach_bootstrap(tmp_path, monkeypatch):
    root = tmp_path / "job"
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    monkeypatch.setenv("GITHUB_RUN_ID", "92")
    monkeypatch.setattr(measure, "hosted_dispatch", lambda: None)
    monkeypatch.setattr(measure, "context", lambda _: SimpleNamespace(head=HEAD))
    monkeypatch.setattr(measure.subprocess, "check_output", lambda *_, **__: HEAD)
    row = {"started_at": "2026-11-27T00:00:00Z", "status": "in_progress", "id": 42}
    monkeypatch.setattr(measure.time, "time", lambda: 1795737600)
    monkeypatch.setattr(
        measure,
        "client",
        lambda *_: SimpleNamespace(
            retry=lambda operation: operation(),
            api=SimpleNamespace(jobs=lambda *_, **__: {1: row}),
        ),
    )
    monkeypatch.setattr(
        measure,
        "authorization",
        lambda *_args, **_kw: validate_ledger(ledger(entry()), **(CONTEXT | {"run": 92})),
    )
    with pytest.raises(BarrierStop, match="run ID"):
        measure.begin(root, 1, CAMPAIGN)
    assert not (root / "job.json").exists() and not (root / "driver.pid").exists()
    assert (
        json.loads((root / "final/authorization-stop.json").read_bytes())["disposition"]
        == "no_requests"
    )


def test_first_http_send_consumes_even_if_the_job_stops(tmp_path):
    from j2_schedule import stock_schedule

    assert cell_traffic(tmp_path)["disposition"] == "no_requests"
    write(tmp_path / "sender-release.json", {})
    assert cell_traffic(tmp_path)["disposition"] == "unresolved"
    schedule = stock_schedule(CAMPAIGN, "job-1", "A", "run", "ns", S, "1", 0, 1)
    write(tmp_path / "schedule.json", schedule)
    plan = stock_plan(schedule)
    row = plan["plan"]["rows"][0]
    journal = tmp_path / "sender-journal.jsonl"
    intent = arrival(row, sent_ns=S)
    journal.write_bytes(encoded(plan, intent))
    assert cell_traffic(tmp_path)["disposition"] == "unresolved"
    finished = dict(intent, outcome="accepted", status=201)
    journal.write_bytes(encoded(plan, intent, finished))
    assert cell_traffic(tmp_path)["disposition"] == "consumed"
    assert (
        traffic_disposition([cell_traffic(tmp_path), {"disposition": "unresolved"}]) == "consumed"
    )
    journal.write_bytes(encoded(plan, arrival(row, outcome="unsent"), {"event": "closed"}))
    write(tmp_path / "journal-read.json", {"exit": 0})
    assert cell_traffic(tmp_path)["disposition"] == "no_requests"


@pytest.mark.parametrize("job_disposition", ["no_requests", "consumed", "unresolved"])
def test_prior_receipt_requires_three_verified_zero_request_jobs(job_disposition):
    raw = canonical(
        dict(
            campaign=CAMPAIGN,
            run_id=90,
            attempt=1,
            head=HEAD,
            errors={},
            disposition="no_requests",
            jobs={
                str(job): {"disposition": job_disposition if job == 1 else "no_requests"}
                for job in (1, 2, 3)
            },
        )
    )
    prior = entry(run_id=90, evidence_sha256=digest(raw))
    if job_disposition == "no_requests":
        verify_zero_requests(raw, prior, CAMPAIGN)
        with pytest.raises(BarrierStop, match="zero requests"):
            verify_zero_requests(raw + b" ", prior, CAMPAIGN)
    else:
        with pytest.raises(BarrierStop, match="zero requests"):
            verify_zero_requests(raw, prior, CAMPAIGN)


def telemetry():
    return {
        name: [
            {
                "values": [
                    [i, str(i if name == "j2_sample_timestamp_seconds" else 1)] for i in range(52)
                ]
            }
        ]
        for name in METRICS
    }


def test_sample_edge_gap_empty_bin_and_recurrence():
    schedule = dict(origin_ns=0, warmup_end_ns=10 * S, stop_ns=46 * S)
    data = telemetry()
    first = "j2_outstanding"
    data[first][0]["values"] = [p for p in data[first][0]["values"] if not 10 <= p[0] < 16]
    report = cell_samples(data, schedule)
    assert report["failed_scrapes"] == report["failed_queries"] == 0
    assert report["coverage"]["grid"][first] == {"maximum_gap_ns": 6 * S, "empty_bins": [1]}
    assert coverage_gaps(report) == {first}
    measure.verify_recovery(set(), {first})
    measure.verify_recovery({first}, {"j2_unpublished"})
    with pytest.raises(ResourceEvidenceError, match="breached again"):
        measure.verify_recovery({first}, coverage_gaps(report))
    del data["j2_unpublished"]
    with pytest.raises(ResourceEvidenceError, match="unverifiable"):
        cell_samples(data, schedule)


def test_scrape_errors_are_counted_only_in_the_cell():
    schedule = dict(origin_ns=10 * S, warmup_end_ns=15 * S, stop_ns=46 * S)
    data = telemetry()
    for i in (0, 16, 46):
        data["up"][0]["values"][i][1] = "0"
    data["j2_query_ok"][0]["values"][21][1] = "0"
    result = cell_samples(data, schedule)
    assert (result["failed_scrapes"], result["failed_queries"]) == (1, 1)
    assert not coverage_gaps(result)


def test_measured_barrier_keeps_the_complete_episode_allowance():
    assert barrier_deadline(10, 2800, allow_early=True) == 2710
    assert barrier_deadline(10, 2710, allow_early=True) == 2710
    assert barrier_deadline(10, 2709, allow_early=True) == 2709
    with pytest.raises(BarrierStop, match="no budget"):
        barrier_deadline(10, 10, allow_early=True)


@pytest.mark.parametrize("selected", ["A", NO_RATE])
def test_soak_follows_real_decision_without_an_ab_default(tmp_path, monkeypatch, selected):
    root = tmp_path
    for name in ("decision", "final"):
        (root / name).mkdir()
    write(root / "final/barrier.json", {"deadline": 300})
    write(root / "decision/decision.json", {"decision": {"selected_rate": selected}})
    state = dict(job=1, job_id=42, work_end=500, limits={"episode": 100})
    gate = SimpleNamespace(
        retry=lambda fn: fn(),
        decisions=lambda *_, **__: None,
        api=SimpleNamespace(jobs=lambda *_: {1: {"id": 42}}),
    )
    requested = []
    monkeypatch.setattr(measure, "client", lambda *_: gate)
    monkeypatch.setattr(measure, "receipt", lambda _: {"id": 8})
    monkeypatch.setattr(measure.time, "monotonic", lambda: 200)
    monkeypatch.setattr(measure, "request_cell", lambda *args: requested.append(args[-3:]))
    measure.start_soak(root, state, object())
    assert requested == ([("soak", "A", 100)] if selected == "A" else [])


def test_stopped_driver_cannot_publish_a_grid(tmp_path):
    (tmp_path / "driver").mkdir()
    write(tmp_path / "driver/status.json", {"stop": {"classification": "invalid"}})
    with pytest.raises(ValueError, match="incomplete grid"):
        measure.prepare_grid(tmp_path, {}, None)


def test_runner_inventory_includes_before_traffic_resource_facts(tmp_path, monkeypatch):
    import j2_measure_telemetry as telemetry_module

    (tmp_path / "cpuinfo").write_text("model name : runner cpu\n")
    (tmp_path / "meminfo").write_text("MemTotal: 100 kB\nMemAvailable: 50 kB\n")
    (tmp_path / "pressure").mkdir()
    for name in ("cpu", "memory", "io"):
        (tmp_path / "pressure" / name).write_text("some avg10=0.00")
    monkeypatch.setenv("ImageOS", "ubuntu24")
    monkeypatch.setenv("ImageVersion", "1")
    monkeypatch.setattr(telemetry_module.os, "sysconf", lambda _: 4, raising=False)
    monkeypatch.setattr(telemetry_module.os, "sched_getaffinity", lambda _: {0, 1}, raising=False)
    monkeypatch.setattr(
        telemetry_module.os, "uname", lambda: SimpleNamespace(release="kernel"), raising=False
    )
    monkeypatch.setattr(
        telemetry_module,
        "runner_cgroup",
        lambda: {"cpu.max": {"present": True, "value": "100000 100000"}},
    )
    receipt = runner_inventory(tmp_path)
    assert receipt["cpu_model"] == "runner cpu" and receipt["available_cpu_ids"] == [0, 1]
    assert receipt["memory_bytes"] == {"MemTotal": 102400, "MemAvailable": 51200}
    assert receipt["ImageOS"] == "ubuntu24" and receipt["kernel"] == "kernel"
    assert set(receipt["host_pressure"]) == {"cpu", "memory", "io"}


@pytest.mark.parametrize("bad", [None, "cadence", "timeout", "concurrency", "session", "budget"])
def test_running_telemetry_configuration_must_match_the_profile(bad):
    rendered = render("j2-telemetry", "192.0.2.1", "registry/j2@sha256:" + "a" * 64)
    config = next(
        item["data"]["prometheus.yml"]
        for item in rendered["items"]
        if item["kind"] == "ConfigMap" and item["metadata"]["name"] == PROM
    )
    source = (Path(__file__).resolve().parent / "exporter/main.go").read_bytes()
    live = dict(
        role_limit=1,
        settings=["statement_timeout=2s", "default_transaction_read_only=on"],
        exporter_sessions=["j2-db-exporter"],
        max_connections=100,
        client_backends=20,
    )
    if bad == "cadence":
        config = config.replace("scrape_interval: 1s", "scrape_interval: 2s")
    elif bad == "timeout":
        live["settings"][0] = "statement_timeout=3s"
    elif bad == "concurrency":
        source = source.replace(b"db.SetMaxOpenConns(1)", b"db.SetMaxOpenConns(2)")
    elif bad == "session":
        live["exporter_sessions"].append("unknown")
    elif bad == "budget":
        live["client_backends"] = 83
    if bad:
        with pytest.raises(ValueError):
            checked_configuration(rendered, config, live, source)
    else:
        record = checked_configuration(rendered, config, live, source)
        assert record["budget"]["with_admin"] == 82 and record["budget"]["free"] == 18
        assert record["max_concurrent_queries"] == 1 and record["statement_timeout_ms"] == 2000
