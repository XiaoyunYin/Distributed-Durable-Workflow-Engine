"""K4 Phase A uses synthetic, offline evidence only; no cluster is contacted."""

from __future__ import annotations

import hashlib
import json
import math
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import dur053_k4 as k4  # noqa: E402
import dur053_k4_evidence as evidence  # noqa: E402
import dur053_k4_schema as schema  # noqa: E402
import dur053_record_k4 as recorder  # noqa: E402


def default(spec):
    if isinstance(spec, dict):
        return {k: default(s) for k, s in spec.items()}
    if isinstance(spec, list):
        return []
    if isinstance(spec, tuple):
        return None
    if isinstance(spec, frozenset):
        return sorted(spec)[0]
    if spec in (schema.HASH, schema.SHA):
        return "a" * (64 if spec == schema.HASH else 40)
    if spec == schema.NUMBER:
        return 0.0
    if spec == "hashmap":
        return {}
    return {str: "", int: 0, bool: False}[spec]


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes((json.dumps(value, sort_keys=True, indent=2) + "\n").encode())


@pytest.fixture(scope="session")
def freeze():
    # Works in the mutation runner's git-archive export as well as the checkout.
    # Committed-byte and registered-rate refusal have their own real-git test.
    raw = (ROOT / k4.FREEZE).read_bytes()
    f = json.loads(raw)
    f["sha256"] = hashlib.sha256(raw).hexdigest()
    return f


@pytest.fixture(scope="session")
def checker(tmp_path_factory):
    target = tmp_path_factory.mktemp("checker") / "checker.exe"
    env = dict(os.environ, GOFLAGS="-mod=mod", GOPROXY="off")
    subprocess.run(
        ["go", "build", "-p", "1", "-o", str(target), "./cmd/dur049-checker"],
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
    )
    return target


def seal_witness(timeline):
    timeline["witness"] = []
    for s in timeline["sessions"]:
        rows = []
        ack = []
        for v in timeline["invocations"]:
            if v["session"] != s["session"]:
                continue
            ack.append(v["id"])
            rows.append(dict(event="start", id=v["id"], identity=v["identity"], at=v["start"]))
            if v["finish"] is not None:
                rows.append(
                    dict(event="finish", id=v["id"], identity=v["identity"], at=v["finish"])
                )
        data = "".join(json.dumps(r, sort_keys=True) + "\n" for r in rows)
        timeline["witness"].append(
            dict(
                session=s["session"],
                pod_uid=s["pod_uid"],
                container_id=s["container_id"],
                sha256=hashlib.sha256(data.encode()).hexdigest(),
                data=data,
                acknowledged=ack,
                complete=True,
                errors=0,
            )
        )


def episode_fixture(root, freeze, *, episode="short-coverage-0-0-treatment"):
    m = default(schema.EPISODE)
    p = k4.parameters(freeze, "short")
    m.update(
        schema="dur053.k4-episode.v1",
        episode=episode,
        arm=next(r["arm"] for r in k4.registry() if r["episode"] == episode),
        replacement_for="",
        job="k4-4",
        run_id="100",
        job_id="200",
        namespace="ns",
        status="PASS",
        budget_scope="registered-campaign",
        measurement_claim=True,
        freeze_sha256=freeze["sha256"],
        seed=next(r["seed"] for r in k4.registry() if r["episode"] == episode),
        rate=p["rate"],
        L=p["L"],
        W=p["W"],
        confirmation_deadline=120 + p["confirmation"],
        intervention=120,
        window_end=300,
        reconciliation_end=331,
        witness_available=True,
        quiet_since=301,
        images=dict(runtime="runtime@sha256:" + "a" * 64, worker="worker@sha256:" + "b" * 64),
        clock_brackets=[
            dict(start=0, end=0, width=0),
            dict(start=399, end=399.001, width=0.001),
        ],
    )
    if m["arm"] == "baseline":
        m["baseline_flags"] = ["RUNTIME_SHUTDOWN_MODE=legacy", "WORKER_DRAIN_MODE=legacy"]
    t = default(schema.SCHEMAS["dur053.k4-timeline.v1"])
    t.update(schema="dur053.k4-timeline.v1", episode=episode, namespace="ns")
    for sid, role in (
        ("worker", "worker"),
        ("relay", "relay"),
        ("scheduler", "scheduler"),
        ("new-relay", "relay"),
    ):
        s = default(schema.SESSION)
        s.update(
            session=sid,
            pod_uid=sid + "-uid",
            container_id=sid + "-cid",
            role=role,
            start=0,
            last_alive=330,
            readiness=[dict(at=0, present=True, width=0)],
            endpoints=[dict(at=0, present=True, width=0)],
        )
        if sid == "relay":
            s.update(
                last_alive=120,
                death=120.2,
                death_evidence="independent process watch",
                exit_code=0,
                signals=[dict(at=120, signal=15, evidence="delivery")],
                readiness=[dict(at=120, present=False, width=0.01)],
                endpoints=[dict(at=120.1, present=False, width=0.01)],
            )
        if sid == "new-relay":
            s.update(start=120.2, readiness=[dict(at=120.3, present=True, width=0.01)])
        t["sessions"].append(s)
    arrivals = []
    for phase, start, end in (("warmup", 0, 120), ("measured", 120, 300)):
        for j in range(math.ceil((end - start) * p["rate"] - 1e-8)):
            at = start + j / p["rate"]
            aid = f"{phase}-{j}"
            arrivals.append(
                dict(
                    id=aid,
                    key=aid,
                    workflow=aid,
                    phase=phase,
                    scheduled=at,
                    sent=at,
                    outcome="accepted",
                    status=201,
                    error="",
                    request_hash="a" * 64,
                    attempts=1,
                )
            )
            ident = dict(
                namespace="ns", workflow=aid, node="n", iteration=0, attempt=1, token_hash="a" * 64
            )
            order = 10 + 3 * len(arrivals)
            t["claims"].append(
                dict(
                    identity=ident,
                    session="worker",
                    order=order,
                    at=at + 0.001,
                    commit_bracket=None,
                )
            )
            t["http"].extend(
                [
                    dict(
                        role="worker-control",
                        request=aid + "-" + op,
                        body_hash="a" * 64,
                        attempt=1,
                        operation=op,
                        at=at,
                        status=200,
                        error="",
                    )
                    for op in ("claim", "result")
                ]
            )
            t["invocations"].append(
                dict(
                    id=aid,
                    identity=ident,
                    session="worker",
                    start=at + 0.002,
                    finish=at + 0.01,
                    last_alive=at + 0.005,
                    abort=None,
                    abort_evidence="",
                    outcome="success",
                )
            )
            t["receipts"].append(
                dict(
                    id=aid,
                    identity=ident,
                    session="worker",
                    order=order + 1,
                    at=at + 0.011,
                    category="committed",
                    outcome="success",
                    rejection="",
                )
            )
            t["history"].extend(
                [
                    dict(
                        identity=ident,
                        order=order + 1,
                        at=at + 0.011,
                        kind="receipt",
                        receipt=aid,
                        partition=0,
                        owner="",
                        epoch=0,
                        scheduler_epoch=None,
                        actor_kind="worker",
                        revision=1,
                    ),
                    dict(
                        identity=ident,
                        order=order + 2,
                        at=at + 0.02,
                        kind="terminal",
                        receipt="",
                        partition=0,
                        owner="owner",
                        epoch=1,
                        scheduler_epoch=1,
                        actor_kind="scheduler",
                        revision=2,
                    ),
                ]
            )
            t["workflows"].append(
                dict(
                    workflow=aid,
                    key=aid,
                    state="SUCCEEDED",
                    terminal=at + 0.02,
                    clock_width=0.001,
                    first_observed=at + 0.03,
                )
            )
    seal_witness(t)
    c = default(schema.CONFIRMATION)
    c.update(
        schema="dur053.k4-confirmation.v1",
        episode=episode,
        kind="rollout",
        target="Deployment/relay",
        intervention=120,
        completed=120.3,
        removed=120.3,
        image_a="relay@sha256:" + "a" * 64,
        image_a_prime="relay@sha256:" + "b" * 64,
        build="same-build",
        lineage_a="a",
        lineage_a_prime="a-prime",
        revision_a="1",
        revision_a_prime="2",
        rollout_status="complete",
        targets=[
            dict(
                session="relay",
                pod_uid="relay-uid",
                container_id="relay-cid",
                deleted=120,
                replacement_uid="new-relay-uid",
                replacement_ready=120.3,
                restart_before=0,
                restart_after=0,
            )
        ],
        overlap=[
            dict(
                role="relay",
                session="relay",
                pod_uid="relay-uid",
                at=119.9,
                end=120.1,
                acknowledgment="publication boundary",
                request="",
                publication="publication-1",
                workflows=[],
                claim=None,
                partition=None,
                owner="",
                epoch=None,
            )
        ],
    )
    documents = dict(
        timeline=t,
        confirmation=c,
        leases=dict(
            schema="dur053.k4-lease-audit.v1",
            episode=episode,
            rows=[
                dict(
                    order=1,
                    at=0,
                    kind="acquire",
                    partition=0,
                    owner="owner",
                    epoch=1,
                    session="scheduler",
                    transaction="tx",
                )
            ],
        ),
        arrivals=dict(schema="dur053.k4-arrivals.v1", episode=episode, rows=arrivals),
        kafka=dict(schema="dur053.k4-kafka.v1", episode=episode, rows=[]),
        witness=dict(schema="dur053.k4-witness.v1", episode=episode, rows=t["witness"]),
        observers=dict(
            schema="dur053.k4-observers.v1",
            episode=episode,
            rows=[
                dict(
                    at=at,
                    source="persistent-watch",
                    width=0,
                    gap=0 if at == 0 else 1,
                    error="",
                    oldest_claimable=0,
                    active_claims=[],
                    pods=[dict(pod_uid="worker-uid", role="worker", ready=True, eligible=True)],
                    replicas=1,
                    busy_slots=0,
                    total_slots=2,
                )
                for at in range(332)
            ],
        ),
    )
    for key, name in evidence.FILES.items():
        write(root / name, documents[key])
        m["files"][key] = hashlib.sha256((root / name).read_bytes()).hexdigest()
    write(root / "episode.json", m)
    return m, documents


def test_registry_and_frozen_bounds(freeze):
    rows = k4.registry()
    assert len(rows) == 161, "K4 episode allocation changed"
    assert rows[0]["arm"] == "baseline" and rows[2]["arm"] == "treatment", "K4 order changed"
    assert rows[0]["seed"] == 530001 and rows[2]["seed"] == 530002, "K4 seed changed"
    assert [rows[i * 10]["event"] for i in range(7)] == [
        "worker rollout",
        "worker scale-down 4 to 2",
        "scheduler rollout",
        "API rollout",
        "worker SIGKILL",
        "scheduler SIGKILL",
        "node drain/PDB eviction",
    ], "K4 event mapping changed"
    groups = k4.schedule(freeze)["groups"]
    assert [len(g["units"]) for g in groups] == [23, 19, 14, 15, 20], "K4 expected packing changed"
    assert k4.parameters(freeze, "short") == dict(
        rate=7.136100570066256, L=2.854, warmup=120, W=180, confirmation=5.708, T=600, reset=60
    ), "K4 short bounds changed"
    p = k4.parameters(freeze, "long")
    assert p["T"] == 2163.1 and math.isclose(p["W"], 360.0166001941), "K4 long bounds shortened"
    assert [r["full_window_reservation_seconds"] for r in k4.schedule(freeze)["units"]][::35] == [
        1931.416,
        6271.4732003882,
        965.708,
    ], "K4 reservation changed"


def test_admission_and_progress(freeze):
    unit = k4.registry()[0]["unit"]
    bound = k4.admit(freeze, unit, elapsed=0, ledger_remaining=218140)
    assert bound == 2831.416, "whole pair or reserves omitted"
    with pytest.raises(ValueError, match="k4-whole-pair-fit"):
        k4.admit(freeze, unit, elapsed=18900 - bound + 0.1, ledger_remaining=218140)
    with pytest.raises(ValueError, match="k4-ledger-fit"):
        k4.admit(freeze, unit, elapsed=100, ledger_remaining=bound)
    ids = [r["episode"] for r in k4.registry()[:2]]
    charge = [dict(job_id="1", charged_seconds=1000, k4_episodes=ids)]
    r = k4.remaining(freeze, ids, charge)
    assert unit not in r["remaining_units"], "K4 progress counted twice"
    assert r["expected_total_seconds"] == r["expected_remaining_seconds"] + 1000, (
        "actual charges omitted"
    )
    with pytest.raises(ValueError, match="k4-progress-ledger-binding"):
        k4.remaining(freeze, ids, [])
    with pytest.raises(ValueError, match="k4-charge-duplicate"):
        k4.remaining(freeze, ids, charge * 2)
    with pytest.raises(ValueError, match="k4-progress-identity"):
        k4.remaining(freeze, ids * 2, charge)
    partial = k4.remaining(
        freeze, ids[:1], [dict(job_id="1", charged_seconds=1000, k4_episodes=ids[:1])]
    )
    assert unit in partial["remaining_units"]
    assert (
        partial["expected_k4_remaining_seconds"]
        < k4.remaining(freeze, [], [])["expected_k4_remaining_seconds"]
    )


def test_continuation(freeze):
    with pytest.raises(ValueError, match="k4-predecessor-committed"):
        k4.assignment(freeze, "k4-1-c1", {})
    history = {"k4-1": dict(status="CARRY_OVER", pending=["short-core-4-pair-2"])}
    assert k4.assignment(freeze, "k4-1-c1", history) == history["k4-1"]["pending"], (
        "continuation suffix changed"
    )
    with pytest.raises(ValueError, match="k4-job-name"):
        k4.job_name("k4-1-c0")


@pytest.mark.parametrize("name", list(schema.SCHEMAS))
def test_closed_schemas(name):
    spec = schema.SCHEMAS[name]
    value = default(spec)
    schema.validate(value, spec)
    value["extra"] = 1
    with pytest.raises(ValueError, match="k4-schema"):
        schema.validate(value, spec)
    del value["extra"]
    del value["schema"]
    with pytest.raises(ValueError, match="k4-schema"):
        schema.validate(value, spec)


@pytest.mark.parametrize("n", [0, 1, 199, 200])
def test_tails(n):
    pop = evidence.population(list(range(n)), missing=3, censored=2)
    assert pop["n"] == n and pop["missing"] == 3 and pop["censored"] == 2, "population strata lost"
    assert pop["p99"] == ("NOT ESTIMATED" if n < 200 else 197), (
        "p99 emitted below n=200 or wrong rank"
    )
    assert evidence.population(list(range(n)), long=True)["p99"] == "NOT ESTIMATED", (
        "long p99 estimated"
    )


def test_gate():
    rows = [
        dict(
            wasted_lower=10,
            reexecuted_lower=0,
            busy_upper=10,
            busy_removed=True,
            idle_survivor=True,
            ambiguous=False,
        )
        for _ in range(5)
    ]
    assert k4.k7(rows, complete_pairs=True, safety=True)["decision"] == "GO", (
        "eligible K7 gate rejected"
    )
    for i in range(3):
        rows[i]["wasted_lower"] = 0
    assert k4.k7(rows, complete_pairs=True, safety=True)["decision"] == "NO-GO", (
        "zero/negative excess dropped"
    )
    for r in rows:
        r["wasted_lower"] = 10
        r["busy_removed"] = False
    rows[0]["busy_removed"] = True
    assert k4.k7(rows, complete_pairs=True, safety=True)["decision"] == "NO-GO", (
        "GO with one eligible removal"
    )
    for r in rows:
        r["busy_removed"] = True
        r["idle_survivor"] = False
    assert k4.k7(rows, complete_pairs=True, safety=True)["decision"] == "NO-GO", (
        "GO without idle survivor"
    )
    for r in rows:
        r["idle_survivor"] = True
    rows[0]["ambiguous"] = True
    assert k4.k7(rows, complete_pairs=True, safety=True)["decision"] == "NO-GO", (
        "ambiguous evidence produced GO"
    )
    rows[0]["ambiguous"] = False
    assert k4.k7(rows, complete_pairs=False, safety=True)["decision"] == "NO-GO", (
        "incomplete cell produced GO"
    )
    for r, w, b in zip(rows, [1, 2, 10, 11, 12], [0, 0, 1000, 0, 0], strict=True):
        r.update(wasted_lower=w, busy_upper=b)
    assert k4.k7(rows, complete_pairs=True, safety=True)["median_excess"] == -3, (
        "median of costs substituted for episode excess"
    )


def test_episode_metrics_roundtrip(tmp_path, freeze, checker):
    m, _ = episode_fixture(tmp_path, freeze)
    value = evidence.episode(tmp_path, freeze, checker)
    assert value["valid"], value["issues"]
    assert value["latency"]["n"] >= 200 and value["latency"]["p99"] < 0.03, (
        "latency derivation failed"
    )
    assert value["no_active_fault"]["label"] == "post-intervention, no active fault", (
        "no-fault delimitation missing"
    )
    assert value["duplicate_effects"] == "NOT COVERED (PURE)", "PURE effects invented"
    assert value["results_during_drain"] == 0 and value["sigterm_to_exit"]["n"] == 1, (
        "drain metrics failed"
    )
    m["files"]["timeline"] = "0" * 64
    write(tmp_path / "episode.json", m)
    with pytest.raises(ValueError, match="k4-episode-file-hash"):
        evidence.episode(tmp_path, freeze, checker)


def test_confirmation_and_quiet(tmp_path, freeze, checker):
    m, d = episode_fixture(tmp_path, freeze)
    d["confirmation"]["rollout_status"] = "sleep-completed"
    assert "k4-confirmation-image-rollout" in evidence.confirmation(
        d["confirmation"], d["timeline"], "relay rollout"
    ), "fixed sleep accepted as rollout"
    d["confirmation"]["kind"] = "kill"
    d["confirmation"]["target"] = "Deployment/relay"
    assert "k4-confirmation-chaos-ack" in evidence.confirmation(
        d["confirmation"], d["timeline"], "relay SIGKILL"
    ), "kill without backend acknowledgement accepted"
    m["witness_activity"] = [330]
    write(tmp_path / "episode.json", m)
    value = evidence.episode(tmp_path, freeze, checker)
    assert "k4-witness-quiet" in value["issues"], "quiet timer ignored late activity"
    assert value["no_active_fault"] is None, "invalid evidence published no-fault window"


def test_confirmation_identity(tmp_path, freeze):
    _, docs = episode_fixture(tmp_path, freeze)
    c, t = docs["confirmation"], docs["timeline"]
    c["targets"][0]["container_id"] = "wrong"
    assert "k4-confirmation-exact-session" in evidence.confirmation(c, t, "relay rollout"), (
        "wrong target container accepted"
    )
    c["targets"][0]["container_id"] = "relay-cid"
    c["targets"][0]["replacement_uid"] = "missing"
    assert "k4-confirmation-independent-replacement" in evidence.confirmation(
        c, t, "relay rollout"
    ), "missing replacement accepted"
    c["overlap"][0]["end"] = 119
    assert "k4-confirmation-boundary" in evidence.confirmation(c, t, "relay rollout"), (
        "stale overlap accepted"
    )


def test_kill_confirmation(tmp_path, freeze):
    _, d = episode_fixture(tmp_path, freeze)
    c, t = d["confirmation"], d["timeline"]
    c.update(
        kind="kill",
        chaos_version="2.7.2",
        chaos_kind="container-kill",
        chaos_object="object",
        injection_status="injected",
    )
    target = next(s for s in t["sessions"] if s["session"] == "relay")
    target.update(exit_code=137, exit_signal=9)
    assert not evidence.confirmation(c, t, "relay SIGKILL")
    c["injection_status"] = "pending"
    assert evidence.confirmation(c, t, "relay SIGKILL"), (
        "kill without injection acknowledgement accepted"
    )
    c["injection_status"] = "injected"
    target.update(exit_code=0, exit_signal=15)
    assert evidence.confirmation(c, t, "relay SIGKILL"), "TERM mislabeled KILL"


def test_schema_nested_and_finite():
    s = default(schema.SESSION)
    s["signals"] = [dict(at=0, signal=15, evidence="source", extra=True)]
    with pytest.raises(ValueError, match="k4-schema"):
        schema.validate(s, schema.SESSION)
    for value in (float("nan"), float("inf"), -1, True):
        with pytest.raises(ValueError):
            evidence.population([value])


def test_assignment_requires_delivery():
    base = dict(
        change="c",
        at=1,
        consumer="one",
        session="s",
        generation=1,
        partitions=[0],
        survivors=["one"],
        trigger_uid="uid",
        arrival="",
        disposition_order=None,
    )
    rows = [dict(base, event="revoke"), dict(base, event="stable", at=2)]
    result = evidence.assignment_stalls(rows, [], dict(history=[], invocations=[]), 5)
    assert result[0]["censored"] and result[0]["total"] is None, "callback-only stall accepted"
    ident = dict(workflow="w")
    arrivals = [dict(id="a", workflow="w")]
    timeline = dict(history=[dict(order=3, kind="receipt", identity=ident)], invocations=[])
    rows.append(dict(base, event="disposed", at=3, arrival="a", disposition_order=3, generation=2))
    assert evidence.assignment_stalls(rows, arrivals, timeline, 5)[0]["censored"], (
        "wrong-generation delivery accepted"
    )
    rows[-1]["generation"] = 1
    # Stable callback must be in the same resulting generation.
    assert evidence.assignment_stalls(rows, arrivals, timeline, 5)[0]["total"] == 2, (
        "useful assignment stall missing"
    )


def test_interrupted_charge_and_engineering(tmp_path, freeze):
    m = default(schema.JOB)
    m.update(
        schema="dur053.k4-job.v1",
        job="k4-1",
        run_id="1",
        job_id="2",
        status="interrupted",
        budget_scope="engineering-validation",
        measurement_claim=False,
        freeze_sha256=freeze["sha256"],
    )
    write(tmp_path / "manifest.json", m)
    with pytest.raises(ValueError, match="k4-engineering-not-contribution"):
        recorder.validate_job(tmp_path, freeze, history={})
    m.update(
        budget_scope="registered-campaign",
        measurement_claim=True,
        registered_live_seconds=18900,
        charged_seconds=1,
        cleanup="UNTRUSTWORTHY",
    )
    write(tmp_path / "manifest.json", m)
    with pytest.raises(ValueError, match="k4-intake-charge"):
        recorder.validate_job(tmp_path, freeze, history={})


def test_committed_freeze_changed(tmp_path):
    # A disposable local repository proves committed-byte refusal without touching history.
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    fpath = tmp_path / k4.FREEZE
    fpath.parent.mkdir(parents=True)
    fpath.write_bytes((ROOT / k4.FREEZE).read_bytes())
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        cwd=tmp_path,
        check=True,
    )
    f = json.loads(fpath.read_bytes())
    f["arms"]["short"]["r"] = 7
    f["arms"]["short"]["q"] = 3.5
    f["arms"]["short"]["registered_check_values"] = {"r": "7.000000", "q": "3.500000"}
    write(fpath, f)
    with pytest.raises(ValueError, match="pilot-committed-bytes"):
        k4.frozen(tmp_path)
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-qm",
            "changed",
        ],
        cwd=tmp_path,
        check=True,
    )
    with pytest.raises(ValueError, match="k4-registered-rate"):
        k4.frozen(tmp_path)


def git_commit(repository):
    subprocess.run(["git", "add", "."], cwd=repository, check=True, capture_output=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-qm",
            "offline fixture",
        ],
        cwd=repository,
        check=True,
        capture_output=True,
    )


def local_repository(path):
    path.mkdir()
    subprocess.run(["git", "init", "-q", str(path)], check=True)
    subprocess.run(["git", "config", "core.autocrlf", "false"], cwd=path, check=True)
    write(path / k4.LEDGER, dict(schema="dur053.live-ledger.v1", limit_seconds=259200, entries=[]))
    dest = path / k4.FREEZE
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes((ROOT / k4.FREEZE).read_bytes())
    git_commit(path)
    return path


def actions(source, *, run="100", job="200", conclusion="failure"):
    write(source / "progress.json", dict(schema="dur053.k4-progress.v1", job="k4-1", started=[]))
    write(
        source / "actions-run.json",
        dict(id=int(run), head_sha="a" * 40, event="workflow_dispatch", run_attempt=1),
    )
    write(
        source / "actions-job.json",
        dict(id=int(job), run_id=int(run), name="DUR-053 campaign", conclusion=conclusion),
    )
    write(source / "actions-commit.json", dict(sha="a" * 40, commit=dict(tree=dict(sha="a" * 40))))


def test_archive_ledger_roundtrip(tmp_path, checker, monkeypatch):
    # Compression size is irrelevant to these small semantic fixtures; production
    # archive limits, deterministic container and all integrity checks remain active.
    import lzma

    import dur053_payload

    monkeypatch.setattr(
        dur053_payload, "FILTERS", [dict(id=lzma.FILTER_LZMA2, preset=0, dict_size=1 << 20)]
    )
    repo = local_repository(tmp_path / "repo")
    downloaded = tmp_path / "downloaded"
    actions(downloaded)
    (downloaded / "partial.log").write_bytes(b"retained partial fault evidence\r\n")
    target = recorder.interrupted(downloaded, repo, "k4-1", checker)
    m = schema.load(target / "manifest.json")
    assert m["charged_seconds"] == 18900 and m["status"] == "interrupted", (
        "interrupted job refunded or erased"
    )
    git_commit(repo)
    verified = evidence.verify_evidence(repo, checker)
    assert verified["schedule"]["charged_seconds"] == 18900, "actual interrupted charge omitted"
    assert len(verified["missing_episodes"]) == 161 and not verified["valid"], (
        "passing subset accepted"
    )
    with dur053_payload.materialize(target, repo) as raw:
        assert (
            raw / "downloaded/partial.log"
        ).read_bytes() == b"retained partial fault evidence\r\n", "failure bytes erased"
    with pytest.raises(ValueError, match="k4-job-replay"):
        recorder.interrupted(downloaded, repo, "k4-1", checker)
    archive = next((target / "payload").glob("*.tar.xz"))
    archive.write_bytes(archive.read_bytes() + b"changed")
    with pytest.raises(ValueError):
        evidence.verify_evidence(repo, checker)


def test_bidirectional_inventory(tmp_path):
    repo = local_repository(tmp_path / "repo")
    ledger = json.loads((repo / k4.LEDGER).read_bytes())
    ledger["entries"].append(
        dict(
            category="k4-1",
            artifact=(k4.EVIDENCE_ROOT / "k4-1/1/manifest.json").as_posix(),
            job_id="1",
            charged_seconds=18900,
        )
    )
    write(repo / k4.LEDGER, ledger)
    git_commit(repo)
    with pytest.raises(ValueError, match="k4-ledger-directory-missing"):
        recorder.inventory(repo)
    write(repo / k4.LEDGER, dict(schema="dur053.live-ledger.v1", limit_seconds=259200, entries=[]))
    git_commit(repo)
    orphan = repo / k4.EVIDENCE_ROOT / "hidden.txt"
    orphan.parent.mkdir(parents=True)
    orphan.write_text("retained but unbound", encoding="utf8")
    with pytest.raises(ValueError, match="k4-directory-orphan"):
        recorder.inventory(repo)


def test_infrastructure_replacement(freeze):
    ids = [r["episode"] for r in k4.registry()[:4]]
    ds = [
        dict(episode=e, classification=c, replacement_for="", reason="", evidence=[])
        for e, c in zip(
            ids, ["valid", "infrastructure-invalid", "valid", "unexercised"], strict=True
        )
    ]
    prior = dict(
        job="k4-1",
        job_id="10",
        status="CARRY_OVER",
        replacement_for="",
        dispositions=ds,
        pending=["suffix"],
    )
    assert recorder.contributions(prior) == [ids[0], ids[2]], "valid outcomes discarded"
    expected = [ids[1] + "-replacement-1"]
    assert k4.assignment(freeze, "k4-1", {"10": prior}, replacement_for="10") == expected, (
        "replacement includes valid or unstarted episodes"
    )
    unit = k4.registry()[0]["unit"]
    reserve = k4.admit(freeze, expected[0], elapsed=0, ledger_remaining=18900)
    assert reserve == (k4.admit(freeze, unit, elapsed=0, ledger_remaining=18900) - 900) / 2 + 900, (
        "replacement reserves a whole pair"
    )
    assert k4.assignment(freeze, "k4-1-c1", {"10": prior}) == ["suffix"]
    failed = dict(prior, status="FAIL")
    assert k4.assignment(freeze, "k4-1-c1", {"10": failed}) == ["suffix"], (
        "failed job lost unstarted suffix"
    )
    bad = dict(prior, dispositions=[dict(d, classification="valid") for d in ds])
    with pytest.raises(ValueError, match="k4-infrastructure-replacement"):
        k4.assignment(freeze, "k4-1", {"10": bad}, replacement_for="10")
    replacement = dict(prior, job_id="11", replacement_for="10")
    with pytest.raises(ValueError, match="k4-infrastructure-replacement"):
        k4.assignment(freeze, "k4-1", {"10": prior, "11": replacement}, replacement_for="10")
    with pytest.raises(ValueError, match="k4-infrastructure-replacement"):
        k4.assignment(freeze, "k4-1", {"11": replacement}, replacement_for="11")


def test_completed_archive_rederivation(tmp_path, checker, monkeypatch):
    import lzma

    import dur053_payload

    monkeypatch.setattr(
        dur053_payload, "FILTERS", [dict(id=lzma.FILTER_LZMA2, preset=0, dict_size=1 << 20)]
    )
    repo = local_repository(tmp_path / "repo")
    f = k4.frozen(repo)
    source = tmp_path / "raw"
    name = "short-coverage-0-1-treatment"
    ep, _ = episode_fixture(source / name, f, episode=name)
    ep["job"] = "k4-5"
    write(source / name / "episode.json", ep)
    value = evidence.episode(source / name, f, checker)
    assert value["valid"], value["issues"]
    write(source / name / "metrics.json", value)
    write(source / "summary.json", evidence.summaries([value]))
    actions(source, conclusion="success")
    assigned = k4.assignment(f, "k4-5", {})
    m = default(schema.JOB)
    m.update(
        schema="dur053.k4-job.v1",
        job="k4-5",
        run_id="100",
        job_id="200",
        freeze_sha256=f["sha256"],
        budget_scope="registered-campaign",
        measurement_claim=True,
        status="CARRY_OVER",
        registered_live_seconds=18900,
        charged_seconds=400,
        elapsed_live_seconds=400,
        live_start=0,
        live_end=400,
        cleanup="PASS",
        episodes=[name],
        assigned=assigned,
        pending=assigned[1:],
        dispositions=[],
        replacement_for="",
    )
    m["dispositions"] = [disposition(name)]
    m["artifact_sha256"] = recorder.artifact_hashes(source)
    write(source / "manifest.json", m)
    value.pop("stale_committed")
    write(source / name / "metrics.json", value)
    m["artifact_sha256"] = recorder.artifact_hashes(source)
    write(source / "manifest.json", m)
    with pytest.raises(ValueError, match="k4-intake-metrics"):
        recorder.record(source, repo, checker)
    value = evidence.episode(source / name, f, checker)
    write(source / name / "metrics.json", value)
    m["artifact_sha256"] = recorder.artifact_hashes(source)
    write(source / "manifest.json", m)
    target = recorder.record(source, repo, checker)
    git_commit(repo)
    result = evidence.verify_evidence(repo, checker)
    assert len(result["missing_episodes"]) == 160 and result["disposition_counts"] == {"PASS": 1}, (
        "valid episode omitted or failure inventory hidden"
    )
    assert result["schedule"]["charged_seconds"] == 400, "completed work charged twice"
    with pytest.raises(ValueError):
        recorder.record(source, repo, checker)
    assert (target / "manifest.json").read_bytes() == (source / "manifest.json").read_bytes(), (
        "immutable destination overwritten"
    )


def empty_job(source, f):
    actions(source)
    assigned = k4.assignment(f, "k4-1", {})
    m = default(schema.JOB)
    m.update(
        schema="dur053.k4-job.v1",
        job="k4-1",
        run_id="100",
        job_id="200",
        freeze_sha256=f["sha256"],
        status="interrupted",
        budget_scope="registered-campaign",
        measurement_claim=True,
        registered_live_seconds=18900,
        charged_seconds=18900,
        cleanup="UNTRUSTWORTHY",
        assigned=assigned,
        pending=assigned,
        dispositions=[],
        replacement_for="",
    )
    m["artifact_sha256"] = recorder.artifact_hashes(source)
    write(source / "manifest.json", m)
    return m


@pytest.mark.parametrize(
    "change,guard",
    [
        ("engineering", "k4-engineering-not-contribution"),
        ("refund", "k4-intake-charge"),
        ("hash", "k4-intake-artifact-hashes"),
        ("assignment", "k4-intake-assignment"),
        ("provenance", "k4-intake-actions"),
    ],
)
def test_intake_guards(tmp_path, freeze, change, guard):
    m = empty_job(tmp_path, freeze)
    assert recorder.validate_job(tmp_path, freeze, history={}) == m
    if change == "engineering":
        m.update(budget_scope="engineering-validation", measurement_claim=False)
    elif change == "refund":
        m["charged_seconds"] = 1
    elif change == "hash":
        m["artifact_sha256"]["actions-run.json"] = "0" * 64
    elif change == "assignment":
        m["assigned"] = []
    elif change == "provenance":
        m["tree_sha"] = "0" * 40
    write(tmp_path / "manifest.json", m)
    try:
        recorder.validate_job(tmp_path, freeze, history={})
    except ValueError as e:
        assert guard in str(e)
    else:
        pytest.fail(guard + " accepted")


def test_lifecycle_cli_is_explicit(checker):
    for args in (
        ["-lifecycle", "missing"],
        ["-lease-audit", "missing"],
        ["-lifecycle", "a", "-lease-audit", "b", "-arrivals", "c"],
    ):
        result = subprocess.run([str(checker), *args], capture_output=True, check=False)
        assert (
            result.returncode == 1
            and b"lifecycle requires timeline and lease-audit and excludes legacy modes"
            in result.stderr
        ), "lifecycle CLI accepted mixed/partial mode"


def test_partition_release_precedes_exit():
    timeline = dict(
        sessions=[
            dict(role="scheduler", session="old", death=125, signals=[dict(at=120, signal=15)])
        ],
        history=[dict(at=123, partition=0, owner="new", order=4)],
        workflows=[],
    )
    leases = [
        dict(session="old", kind="acquire", at=110, order=1, owner="old", epoch=1, partition=0),
        dict(session="old", kind="release", at=121, order=2, owner="old", epoch=1, partition=0),
        dict(session="new", kind="acquire", at=122, order=3, owner="new", epoch=2, partition=0),
    ]
    rows = evidence.partition_reacquisition(timeline, leases, 120)
    assert len(rows) == 1 and rows[0]["mode"] == "release" and rows[0]["seconds"] == 1, (
        "release before process exit disappeared"
    )
    leases.pop(1)
    rows = evidence.partition_reacquisition(timeline, leases, 120)
    assert rows[0]["mode"] == "TTL recovery" and rows[0]["seconds"] == 2, (
        "NULL owner substituted for release"
    )


def test_missing_control_trace(tmp_path, freeze, checker):
    m, d = episode_fixture(tmp_path, freeze)
    d["timeline"]["http"] = []
    write(tmp_path / "timeline.json", d["timeline"])
    m["files"]["timeline"] = hashlib.sha256((tmp_path / "timeline.json").read_bytes()).hexdigest()
    write(tmp_path / "episode.json", m)
    result = evidence.episode(tmp_path, freeze, checker)
    assert not result["complete"] and "k4-http-evidence-missing" in result["issues"], (
        "missing control trace reported zero errors"
    )


def disposition(name, classification="valid", *, replacement_for="", reason="", evidence=()):
    return dict(
        episode=name,
        replacement_for=replacement_for,
        classification=classification,
        reason=reason,
        evidence=list(evidence),
    )


def harness_incident(source, name, reason="collection-failure", *, component="collector", raw=None):
    ref = f"incidents/{name}.json"
    rawref = f"incidents/{name}-source.json"
    write(
        source / rawref,
        raw
        if raw is not None
        else dict(
            component="collector", errors=["export failed"], missing=[name + "/timeline.json"]
        ),
    )
    write(
        source / ref,
        dict(
            schema="dur053.k4-harness.v1",
            episode=name,
            reason=reason,
            component=component,
            at=1,
            warmup_start=2,
            source=rawref,
        ),
    )
    return disposition(name, "infrastructure-invalid", reason=reason, evidence=[ref])


@pytest.mark.parametrize(
    "reason,component,raw",
    [
        ("setup-before-warmup", "setup", dict(status="failure")),
        (
            "harness-oom",
            "driver",
            dict(
                metadata=dict(labels={"k4-component": "driver"}),
                status=dict(
                    containerStatuses=[dict(state=dict(terminated=dict(reason="OOMKilled")))]
                ),
            ),
        ),
        ("harness-timeout", "driver", dict(component="driver", exit_code=124, command=["drive"])),
        (
            "collection-failure",
            "collector",
            dict(component="collector", missing=["e/timeline.json"], errors=[]),
        ),
        ("runner-loss", "runner", dict(id=200, conclusion="failure", runner_failure="runner-lost")),
    ],
)
def test_infrastructure_content(tmp_path, reason, component, raw):
    actions(tmp_path)
    d = harness_incident(tmp_path, "e", reason, component=component, raw=raw)
    recorder.validate_disposition(tmp_path, d)
    write(tmp_path / "incidents/e-source.json", {})
    with pytest.raises(ValueError, match="k4-infrastructure-content"):
        recorder.validate_disposition(tmp_path, d)


def test_bare_infrastructure_refused(tmp_path):
    d = disposition("e", "infrastructure-invalid")
    with pytest.raises(ValueError, match="k4-infrastructure-evidence"):
        recorder.validate_disposition(tmp_path, d)
    m = default(schema.JOB)
    m["classification"] = "infrastructure"
    with pytest.raises(ValueError, match="k4-schema"):
        schema.validate(m, schema.JOB)


def test_engine_outcome_cannot_be_excluded(tmp_path, freeze):
    name = "short-coverage-0-0-treatment"
    ep, _ = episode_fixture(tmp_path / name, freeze, episode=name)
    ep["status"] = "FAIL"
    write(tmp_path / name / "episode.json", ep)
    d = harness_incident(
        tmp_path, name, raw=dict(component="collector", errors=["export failed"], missing=[])
    )
    with pytest.raises(ValueError, match="k4-engine-outcome-not-infrastructure"):
        recorder.validate_disposition(tmp_path, d)
    recorder.validate_disposition(tmp_path, disposition(name))
    ep["status"] = "invalid"
    write(tmp_path / name / "episode.json", ep)
    recorder.validate_disposition(tmp_path, d)  # Proven harness failure, no engine failure.
    timeline = json.loads((tmp_path / name / "timeline.json").read_bytes())
    timeline["workflows"][0]["state"] = "FAILED"
    write(tmp_path / name / "timeline.json", timeline)
    with pytest.raises(ValueError, match="k4-engine-outcome-not-infrastructure"):
        recorder.validate_disposition(tmp_path, d)


@pytest.mark.parametrize("field,bad", [("seed", 0), ("arm", "baseline"), ("replacement_for", "")])
def test_replacement_identity_binding(tmp_path, freeze, field, bad):
    original = "short-core-0-pair-0-treatment"
    new_id = original + "-replacement-1"
    prior = dict(
        job="k4-1",
        job_id="199",
        replacement_for="",
        dispositions=[
            disposition(
                original, "infrastructure-invalid", reason="runner-loss", evidence=["incident.json"]
            )
        ],
    )
    ep, _ = episode_fixture(tmp_path / new_id, freeze, episode=original)
    ep.update(episode=new_id, replacement_for=original, job="k4-1")
    write(tmp_path / new_id / "episode.json", ep)
    actions(tmp_path)
    m = default(schema.JOB)
    m.update(
        schema="dur053.k4-job.v1",
        job="k4-1",
        run_id="100",
        job_id="200",
        freeze_sha256=freeze["sha256"],
        budget_scope="registered-campaign",
        measurement_claim=True,
        status="FAIL",
        registered_live_seconds=18900,
        charged_seconds=400,
        elapsed_live_seconds=400,
        live_start=0,
        live_end=400,
        cleanup="PASS",
        assigned=[new_id],
        pending=[],
        episodes=[new_id],
        replacement_for="199",
        dispositions=[disposition(new_id, replacement_for=original)],
    )
    m["artifact_sha256"] = recorder.artifact_hashes(tmp_path)
    write(tmp_path / "manifest.json", m)
    recorder.validate_job(tmp_path, freeze, history={"199": prior})
    ep[field] = bad
    write(tmp_path / new_id / "episode.json", ep)
    m["artifact_sha256"] = recorder.artifact_hashes(tmp_path)
    write(tmp_path / "manifest.json", m)
    with pytest.raises(ValueError, match="k4-intake-episode-binding"):
        recorder.validate_job(tmp_path, freeze, history={"199": prior})


def test_episode_replacement_roundtrip(tmp_path, checker, monkeypatch):
    import lzma

    import dur053_payload

    monkeypatch.setattr(
        dur053_payload, "FILTERS", [dict(id=lzma.FILTER_LZMA2, preset=0, dict_size=1 << 20)]
    )
    repo = local_repository(tmp_path / "repo")
    f = k4.frozen(repo)
    source = tmp_path / "raw"
    assigned = k4.assignment(f, "k4-1", {})
    ids = [r["episode"] for r in k4.registry() if r["unit"] in assigned[:2]]
    values, ds = [], []
    for index, name in enumerate(ids):
        if index == 1:
            ds.append(harness_incident(source, name))
            continue
        ep, docs = episode_fixture(source / name, f, episode=name)
        # This intake fixture uses worker cells; match its synthetic rollout targets.
        docs["confirmation"]["target"] = "Deployment/worker"
        for session in docs["timeline"]["sessions"]:
            if session["role"] == "relay":
                session["role"] = "worker"
        for key, file in evidence.FILES.items():
            write(source / name / file, docs[key])
            ep["files"][key] = hashlib.sha256((source / name / file).read_bytes()).hexdigest()
        ep.update(job="k4-1", status="FAIL" if index == 2 else "PASS")
        write(source / name / "episode.json", ep)
        value = evidence.episode(source / name, f, checker)
        write(source / name / "metrics.json", value)
        values.append(value)
        ds.append(disposition(name))
    actions(source)
    m = default(schema.JOB)
    m.update(
        schema="dur053.k4-job.v1",
        job="k4-1",
        run_id="100",
        job_id="200",
        freeze_sha256=f["sha256"],
        budget_scope="registered-campaign",
        measurement_claim=True,
        status="CARRY_OVER",
        registered_live_seconds=18900,
        charged_seconds=400,
        elapsed_live_seconds=400,
        live_start=0,
        live_end=400,
        cleanup="PASS",
        assigned=assigned,
        pending=assigned[2:],
        episodes=ids,
        dispositions=ds,
    )
    write(source / "summary.json", evidence.summaries(values))
    m["artifact_sha256"] = recorder.artifact_hashes(source)
    write(source / "manifest.json", m)
    recorder.record(source, repo, checker)
    git_commit(repo)
    first = evidence.verify_evidence(repo, checker)
    assert first["disposition_counts"] == {"PASS": 2, "FAIL": 1}, "valid fault outcome lost"
    assert recorder.contributions(m) == [ids[0], ids[2], ids[3]], "valid partner discarded"
    replacement = tmp_path / "replacement"
    new_id = ids[1] + "-replacement-1"
    ep, docs = episode_fixture(replacement / new_id, f, episode=ids[1])
    docs["confirmation"]["target"] = "Deployment/worker"
    for session in docs["timeline"]["sessions"]:
        if session["role"] == "relay":
            session["role"] = "worker"
    for key, file in evidence.FILES.items():
        write(replacement / new_id / file, docs[key])
    ep.update(episode=new_id, replacement_for=ids[1], job="k4-1", run_id="101", job_id="201")
    # Every exported document binds to the new identity, while seed and arm stay fixed.
    for file in evidence.FILES.values():
        doc = json.loads((replacement / new_id / file).read_bytes())
        doc["episode"] = new_id
        write(replacement / new_id / file, doc)
    ep["files"] = {
        k: hashlib.sha256((replacement / new_id / file).read_bytes()).hexdigest()
        for k, file in evidence.FILES.items()
    }
    write(replacement / new_id / "episode.json", ep)
    value = evidence.episode(replacement / new_id, f, checker)
    write(replacement / new_id / "metrics.json", value)
    write(replacement / "summary.json", evidence.summaries([value]))
    actions(replacement, run="101", job="201")
    r = dict(
        m,
        run_id="101",
        job_id="201",
        status="FAIL",
        replacement_for="200",
        assigned=[new_id],
        pending=[],
        episodes=[new_id],
        dispositions=[disposition(new_id, replacement_for=ids[1])],
    )
    r["artifact_sha256"] = recorder.artifact_hashes(replacement)
    write(replacement / "manifest.json", r)
    for field, bad in (("seed", ep["seed"] + 1), ("arm", "baseline"), ("replacement_for", "")):
        original = ep[field]
        ep[field] = bad
        write(replacement / new_id / "episode.json", ep)
        r["artifact_sha256"] = recorder.artifact_hashes(replacement)
        write(replacement / "manifest.json", r)
        with pytest.raises(ValueError, match="k4-intake-episode-binding"):
            recorder.validate_job(replacement, f, history={"200": m})
        ep[field] = original
    write(replacement / new_id / "episode.json", ep)
    r["artifact_sha256"] = recorder.artifact_hashes(replacement)
    write(replacement / "manifest.json", r)
    recorder.record(replacement, repo, checker)
    git_commit(repo)
    result = evidence.verify_evidence(repo, checker)
    pair = next(p for p in result["paired"] if p["pair"] == 0)
    assert pair["replaced"] and pair["episodes"] == [ids[0], new_id], "replaced pair not flagged"
    assert ids[0] not in result["missing_episodes"] and ids[1] not in result["missing_episodes"]
    assert result["schedule"]["charged_seconds"] == 800
    assert k4.assignment(f, "k4-1-c1", {"200": m, "201": r}) == assigned[2:]
    with pytest.raises(ValueError, match="k4-infrastructure-replacement"):
        recorder.validate_job(replacement, f, history={"200": m, "201": r})


def test_interrupted_lost_episodes_are_invalid(tmp_path, checker, monkeypatch):
    import lzma

    import dur053_payload

    monkeypatch.setattr(
        dur053_payload, "FILTERS", [dict(id=lzma.FILTER_LZMA2, preset=0, dict_size=1 << 20)]
    )
    repo = local_repository(tmp_path / "repo")
    source = tmp_path / "lost"
    actions(source)
    first = k4.assignment(k4.frozen(repo), "k4-1", {})[0]
    write(
        source / "progress.json", dict(schema="dur053.k4-progress.v1", job="k4-1", started=[first])
    )
    target = recorder.interrupted(source, repo, "k4-1", checker)
    m = schema.load(target / "manifest.json")
    assert len(m["dispositions"]) == 2 and all(
        d["classification"] == "infrastructure-invalid" for d in m["dispositions"]
    ), "lost episodes not classified"
    assert m["pending"][0] != first and not recorder.contributions(m)
    # A journal cannot erase an episode whose exported bytes survived.
    name = k4.registry()[0]["episode"]
    episode_fixture(source / name, k4.frozen(repo), episode=name)
    write(source / "progress.json", dict(schema="dur053.k4-progress.v1", job="k4-1", started=[]))
    other = local_repository(tmp_path / "other")
    with pytest.raises(ValueError, match="k4-interrupted-progress-omission"):
        recorder.interrupted(source, other, "k4-1", checker)
