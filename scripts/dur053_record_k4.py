"""Offline, immutable K4 B.4 intake. Never dispatches or discards failed evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
from contextlib import contextmanager
from pathlib import Path

from dur053_k3_policy import artifact_hashes, ledger_check, require
from dur053_k4 import (
    CEILING,
    EVIDENCE_ROOT,
    LEDGER,
    ROOT,
    assignment,
    episode_identity,
    frozen,
    job_name,
    registry,
)
from dur053_k4_schema import load
from dur053_payload_v2 import materialize, pack, scratch, validate_index
from dur053_pilot import committed_bytes


def save(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes((json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode())


LEGACY_PIN = dict(
    job="k4-1",
    run_id="43536422415",
    job_id="132266427561",
    head_sha="cfdbf1af0f08851a37dd8b07cc8e60728db8ed52",
    freeze_sha256="cf7fd60e77a0a995994fd593ef9b2bab6d3f3f443d26d2a2fe927f8feb42c14b",
)


def legacy_form(source, m):
    path = source / "summary.json"
    legacy = path.is_file() and "safety" not in json.loads(path.read_bytes())
    if legacy:
        require(all(m.get(k) == v for k, v in LEGACY_PIN.items()), "k4-legacy-pin")
    return legacy


def legacy_reclassification(source, f, checker, m):
    """Re-derive the pinned download without writing any retained byte."""
    from dur053_k4_evidence import FILES, episode, safety_evidence, summaries

    require(legacy_form(source, m), "k4-legacy-form")
    require(m["charged_seconds"] == 10874, "k4-legacy-charge")
    require(artifact_hashes(source) == m["artifact_sha256"], "k4-intake-artifact-hashes")
    values, dispositions, incidents = [], [], {}
    for original in m["dispositions"]:
        name = original["episode"]
        if original["classification"] != "infrastructure-invalid" or all(
            (source / name / file).is_file() for file in FILES.values()
        ):
            computed = episode(source / name, f, checker)
            retained = json.loads((source / name / "metrics.json").read_bytes())
            episode_manifest = load(source / name / "episode.json")
            retained_safety = safety_evidence(source / name, checker)
            expected_metrics = dict(
                retained,
                reaction=None,
                budget_scope=episode_manifest.get("budget_scope", m["budget_scope"]),
                safety=retained_safety,
            )
            require(
                not {"reaction", "budget_scope", "safety"}.intersection(retained)
                and computed == expected_metrics,
                "k4-legacy-metric-fields",
            )
            values.append(computed)
            derived = derive_disposition(source, name, computed)
            require(
                derived == original
                or (
                    original == dict(derived, classification="valid", reason="", evidence=[])
                    and derived["classification"] == "infrastructure-invalid"
                    and derived["reason"] == "harness-confirmation-miss"
                ),
                "k4-legacy-disposition-difference",
            )
            if derived != original:
                with scratch() as output:
                    for file in ("episode.json", "confirmation.json"):
                        path = output / name / file
                        path.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copyfile(source / name / file, path)
                    save_disposition_evidence(output, derived)
                    validate_disposition(output, derived, metrics=computed)
                    for ref in derived["evidence"]:
                        incidents[ref] = load(output / ref)
            else:
                validate_disposition(source, derived, metrics=computed)
        else:
            # The unissued treatment's collection failure is retained as produced.
            validate_disposition(source, original)
            derived = dict(original)
        dispositions.append(derived)
    old_summary = summaries(values)
    del old_summary["safety"]
    del old_summary["reactions"]
    require(
        old_summary == json.loads((source / "summary.json").read_bytes()),
        "k4-legacy-summary",
    )
    safety_rows = [
        s for name in m["episodes"] if (s := safety_evidence(source / name, checker)) is not None
    ]
    contributing = {d["episode"] for d in dispositions if d["classification"] == "valid"}
    summary = summaries(
        [v for v in values if v["episode"] in contributing], safety_rows=safety_rows
    )
    return dict(
        schema="dur053.k4-reclassification.v1",
        pin=LEGACY_PIN,
        manifest_sha256=hashlib.sha256((source / "manifest.json").read_bytes()).hexdigest(),
        legacy_form_verified=True,
        covered_artifact_sha256=m["artifact_sha256"],
        charged_seconds=m["charged_seconds"],
        dispositions=dispositions,
        incidents=incidents,
        safety=summary["safety"],
        summary=summary,
        metrics=[v for v in values if v["episode"] in contributing],
        safety_rows=safety_rows,
        collection_failure_root_cause="R301 cumulative overlap reparse exhausted confirmation window",
    )


def bound_documents(directory, repository=None):
    """Read only the committed, ledger-bound envelope; never fetch raw assets."""
    index = validate_index(directory)
    allowed = {"manifest.json", "payload-index.json", "intake.json"}
    if (directory / "reclassification.json").exists():
        allowed.add("reclassification.json")
    require(
        {p.name for p in directory.iterdir()} == allowed
        and all(p.is_file() for p in directory.iterdir()),
        "k4-payload-exact-layout",
    )
    m = load(directory / "manifest.json")
    intake_document = json.loads((directory / "intake.json").read_bytes())
    require(
        intake_document["manifest_sha256"]
        == hashlib.sha256((directory / "manifest.json").read_bytes()).hexdigest(),
        "k4-intake-manifest-binding",
    )
    if repository is not None:
        for name in allowed:
            committed_bytes(repository, directory / name)
        ledger = json.loads(committed_bytes(repository, repository / LEDGER))
        rows = [
            r
            for r in ledger["entries"]
            if r["artifact"] == (directory / "manifest.json").relative_to(repository).as_posix()
        ]
        require(len(rows) == 1, "k4-directory-ledger-missing")
        row = rows[0]
        for name, key in [
            ("manifest.json", "artifact_sha256"),
            ("payload-index.json", "payload_index_sha256"),
            ("intake.json", "intake_sha256"),
            ("reclassification.json", "reclassification_sha256"),
        ]:
            require((name in allowed) == (key in row), "k4-intake-ledger-binding")
            if name in allowed:
                require(
                    row[key] == hashlib.sha256((directory / name).read_bytes()).hexdigest(),
                    "k4-intake-ledger-binding",
                )
    document = (
        json.loads((directory / "reclassification.json").read_bytes())
        if "reclassification.json" in allowed
        else None
    )
    if document:
        require(LEGACY_PIN.items() <= m.items(), "k4-legacy-pin")
        require(
            document["manifest_sha256"]
            == hashlib.sha256((directory / "manifest.json").read_bytes()).hexdigest()
            and document["legacy_form_verified"] is True
            and document["covered_artifact_sha256"] == m["artifact_sha256"],
            "k4-legacy-manifest-binding",
        )
    return index, document


@contextmanager
def materialize_job(directory, repository=None, *, store="github"):
    bound_documents(directory, repository)
    with materialize(directory, repository, store=store) as raw:
        yield raw


def classified_manifest(m, document):
    return dict(m, dispositions=document["dispositions"]) if document else m


def inventory(repository: Path) -> tuple[list[tuple[Path, dict]], dict]:
    ledger = json.loads(committed_bytes(repository, repository / LEDGER))
    found = []
    for path in sorted((repository / EVIDENCE_ROOT).glob("*/*/manifest.json")):
        committed_bytes(repository, path)
        m = load(path)
        sidecar = path.parent / "reclassification.json"
        document = json.loads(sidecar.read_bytes()) if sidecar.exists() else None
        m = classified_manifest(m, document)
        require(
            path.relative_to(repository / EVIDENCE_ROOT).as_posix()
            == f"{m['job']}/{m['run_id']}/manifest.json",
            "k4-directory-identity",
        )
        rows = [
            r for r in ledger["entries"] if r["artifact"] == path.relative_to(repository).as_posix()
        ]
        require(len(rows) == 1, "k4-directory-ledger-missing")
        row = rows[0]
        require(
            row["artifact_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
            and all(
                row[k] == m[k]
                for k in (
                    "run_id",
                    "job_id",
                    "head_sha",
                    "tree_sha",
                    "merge_ref_sha",
                    "status",
                    "charged_seconds",
                )
            )
            and row["category"] == m["job"]
            and row.get("k4_episodes") == contributions(m),
            "k4-ledger-binding",
        )
        bound_documents(path.parent, repository)
        found.append((path, m))
    paths = {p.relative_to(repository).as_posix() for p, _ in found}
    rows = [
        r
        for r in ledger["entries"]
        if r["category"].startswith("k4-")
        or r["artifact"].startswith(EVIDENCE_ROOT.as_posix() + "/")
    ]
    require(
        len(rows) == len(paths) and {r["artifact"] for r in rows} == paths,
        "k4-ledger-directory-missing",
    )
    require(len({m["job_id"] for _, m in found}) == len(found), "k4-duplicate-job")
    by_name = {}
    for _, m in found:
        by_name.setdefault(m["job"], []).append(m)
    for group in by_name.values():
        require(
            len(group) <= 2
            and (
                len(group) == 1
                or any(
                    a["replacement_for"] == b["job_id"] and bool(b["dispositions"])
                    for a in group
                    for b in group
                )
            ),
            "k4-duplicate-job",
        )
    seen = []
    for _, m in found:
        seen.extend(contributions(m))
    require(len(seen) == len(set(seen)), "k4-duplicate-episode")
    # Files outside the expected two-level immutable layout cannot hide from inventory.
    expected_files = {p for path, _ in found for p in path.parent.rglob("*") if p.is_file()}
    require(
        {p for p in (repository / EVIDENCE_ROOT).rglob("*") if p.is_file()} == expected_files,
        "k4-directory-orphan",
    )
    acknowledge_stopped(repository, found)
    return found, ledger


def stopped_intake(source, ep):
    """Charge/provenance only; no numerical metric may be inferred."""
    from dur053_k4_evidence import FILES

    name = ep["episode"]
    report = json.loads(evidence_path(source, name + "/intake-stopped.json").read_bytes())
    missing = [file for file in FILES.values() if not (source / name / file).is_file()]
    require(
        ep["status"] == "FAIL"
        and ep["metrics"] == "not-derivable"
        and missing
        and sorted(ep["missing_exports"]) == sorted(missing)
        and report.get("missing_exports") == ep["missing_exports"]
        and report.get("error") == "k4-engine-outcome-with-export-loss",
        "k4-stopped-intake-content",
    )
    # Independently read the observed engine evidence, not the marker's assertion.
    require(observed_fault_outcome(source, name), "k4-stopped-intake-engine-evidence")
    return dict(
        schema="dur053.k4-metrics.v1",
        episode=name,
        replacement_for=ep["replacement_for"],
        status="FAIL",
        valid=True,
        complete=False,
        metrics="not-derivable",
        missing_exports=missing,
        evidence=name + "/intake-stopped.json",
    )


def observed_fault_outcome(source, name):
    cpath = source / name / "confirmation.json"
    if not cpath.is_file():
        return False
    c = load(cpath)
    applied = (
        c["injection_status"] == "injected"
        or c["completed"] is not None
        or c["removed"] is not None
        or any(e["status"] in {200, 201} for e in c["evictions"])
    )
    if not applied:
        return False
    tpath = source / name / "timeline.json"
    if tpath.is_file():
        t = load(tpath)
        if (
            any(w["state"] in {"FAILED", "CANCELED"} for w in t["workflows"])
            or any(
                r["category"] == "committed" and r["outcome"] == "failure" for r in t["receipts"]
            )
            or any(r["status"] >= 500 for r in t["http"])
        ):
            return True
    # A retained reconciliation snapshot independently proves failure or timeout.
    path = source / name / "engine-outcome.json"
    if not path.is_file():
        return False
    outcome = json.loads(path.read_bytes())
    rows = outcome.get("snapshot", {}).get("workflows", [])
    return any(w["state"] in {"FAILED", "CANCELED"} for w in rows) or (
        outcome.get("timeout") is True
        and any(
            w["state"] not in {"SUCCEEDED", "FAILED", "CANCELED", "ABANDONED", "REJECTED"}
            for w in rows
        )
        and outcome.get("observed_at", 0) >= outcome.get("deadline", float("inf"))
    )


def acknowledge_stopped(repository, manifests):
    stopped = [m for _, m in manifests if m.get("intake_stopped")]
    if not stopped:
        return
    path = repository / "experiments/portfolio/k8s-recovery/k4-continuation-acknowledgements.json"
    committed_bytes(repository, path)
    document = load(path)
    require(document.get("schema") == "dur053.k4-continuation-acks.v1", "k4-acknowledgement-schema")
    for m in stopped:
        manifest = repository / EVIDENCE_ROOT / m["job"] / m["run_id"] / "manifest.json"
        digest = hashlib.sha256(committed_bytes(repository, manifest)).hexdigest()
        matches = [
            a
            for a in document["acknowledgements"]
            if a.get("job_id") == m["job_id"]
            and a.get("manifest_sha256") == digest
            and a.get("reviewer") == "Claude"
            and a.get("acknowledged") is True
            and type(a.get("review_round")) is int
            and a["review_round"] > 239
        ]
        m["claude_acknowledgement"] = len(matches) == 1


def contributions(m: dict) -> list[str]:
    return [d["episode"] for d in m["dispositions"] if d["classification"] == "valid"]


def evidence_path(source: Path, name: str) -> Path:
    path = source / name
    require(
        bool(name)
        and not Path(name).is_absolute()
        and path.resolve().is_relative_to(source.resolve())
        and path.is_file(),
        "k4-classification-evidence-path",
    )
    return path


TIMING_ISSUES = {"k4-confirmation-boundary", "k4-harness-confirmation-deadline"}


def confirmation_miss(ep, c, metrics):
    issues = set(metrics["issues"])
    return (
        bool(issues & TIMING_ISSUES)
        and (
            c["intervention"] > ep["confirmation_deadline"]
            or (
                bool(c["overlap"])
                and all(not o["at"] <= c["intervention"] <= o["end"] for o in c["overlap"])
            )
        )
    )


def derive_disposition(source, name, metrics):
    """Read content without modifying source or consulting engine outcomes."""
    ep = load(source / name / "episode.json")
    c = load(source / name / "confirmation.json")
    d = dict(
        episode=name,
        replacement_for=ep["replacement_for"],
        classification="unexercised" if ep["status"] == "unexercised" else "valid",
        reason="",
        evidence=[],
    )
    if confirmation_miss(ep, c, metrics):
        incident = name + "/confirmation-miss.json"
        d.update(
            {"classification": "infrastructure-invalid"},
            reason="harness-confirmation-miss",
            evidence=[incident],
        )
    return d


def save_disposition_evidence(output, d):
    """Materialize derived evidence only in the caller's output/raw copy."""
    if d["reason"] == "harness-confirmation-miss":
        name = d["episode"]
        ep = load(output / name / "episode.json")
        save(
            output / name / "confirmation-miss.json",
            dict(
                schema="dur053.k4-harness.v1",
                episode=name,
                reason=d["reason"],
                component="driver",
                at=ep["reconciliation_end"],
                warmup_start=ep["warmup_start"],
                source=name + "/confirmation.json",
            ),
        )


def validate_disposition(source: Path, d: dict, *, metrics=None) -> None:
    """Validate independent harness records and their retained source content."""
    name = d["episode"]
    episode_path = source / name / "episode.json"
    ep = load(episode_path) if episode_path.exists() else None
    if metrics is None and (source / name / "metrics.json").exists():
        # record()/verify_evidence() independently recompute and compare this file.
        metrics = json.loads((source / name / "metrics.json").read_bytes())
    if d["classification"] != "infrastructure-invalid":
        require(
            not d["reason"] and not d["evidence"] and ep is not None, "k4-classification-content"
        )
        require(
            ep["status"]
            in ({"PASS", "FAIL"} if d["classification"] == "valid" else {"unexercised"}),
            "k4-classification-status",
        )
        if ep["metrics"] == "not-derivable":
            require(d["classification"] == "valid", "k4-stopped-intake-disposition")
            stopped_intake(source, ep)
        else:
            require(not ep["missing_exports"], "k4-unmarked-export-loss")
            if d["classification"] == "valid" and metrics is not None:
                require(
                    not confirmation_miss(ep, load(source / name / "confirmation.json"), metrics),
                    "k4-valid-disposition-confirmation-miss",
                )
        return
    require(bool(d["reason"]) and bool(d["evidence"]), "k4-infrastructure-evidence")
    if d["reason"] == "harness-confirmation-miss":
        for ref in d["evidence"]:
            incident = load(evidence_path(source, ref))
            require(
                incident["schema"] == "dur053.k4-harness.v1"
                and incident["episode"] == name
                and incident["reason"] == d["reason"]
                and incident["component"] == "driver",
                "k4-infrastructure-evidence",
            )
            raw = json.loads(evidence_path(source, incident["source"]).read_bytes())
            if (
                metrics is not None
                and ep is not None
                and (source / name / "confirmation.json").exists()
            ):
                c = load(source / name / "confirmation.json")
                ok = incident["source"] == name + "/confirmation.json" and confirmation_miss(
                    ep, c, metrics
                )
            else:
                # A pre-issue poll observed a boundary but exhausted its deadline.
                ok = (
                    ep is not None
                    and ep["intervention"] == 0
                    and raw.get("component") == "driver"
                    and "k4-harness-confirmation-deadline" in raw.get("errors", [])
                    and raw.get("observed_overlap") is True
                    and raw.get("deadline") == ep["confirmation_deadline"]
                    and raw.get("observed_at", 0) > ep["confirmation_deadline"]
                    and not (source / name / "intervention-issued.json").exists()
                )
            require(ok, "k4-harness-confirmation-miss-content")
        return
    # A recorded engine outcome after intervention cannot be excluded by a later
    # harness incident. Missing exports use collection/runner source evidence.
    confirmation = source / name / "confirmation.json"
    applied = False
    if confirmation.exists():
        c = load(confirmation)
        applied = bool(
            c["injection_status"] == "injected"
            or c["completed"] is not None
            or c["removed"] is not None
            or any(
                t["deleted"] is not None or t["restart_after"] > t["restart_before"]
                for t in c["targets"]
            )
            or any(e["status"] in {200, 201} for e in c["evictions"])
        )
    engine_failure = False
    timeline = source / name / "timeline.json"
    if timeline.exists():
        t = load(timeline)
        engine_failure = (
            any(w["state"] in {"FAILED", "CANCELED"} for w in t["workflows"])
            or any(
                r["category"] == "committed" and r["outcome"] == "failure" for r in t["receipts"]
            )
            or any(r["status"] >= 500 for r in t["http"])
        )
    require(
        not (
            applied
            and (engine_failure or (ep and (ep["status"] in {"PASS", "FAIL"} or ep["timeout"])))
        ),
        "k4-engine-outcome-not-infrastructure",
    )
    for ref in d["evidence"]:
        incident = load(evidence_path(source, ref))
        require(
            incident["schema"] == "dur053.k4-harness.v1"
            and incident["episode"] == name
            and incident["reason"] == d["reason"],
            "k4-infrastructure-evidence",
        )
        raw = json.loads(evidence_path(source, incident["source"]).read_bytes())
        reason, component = incident["reason"], incident["component"]
        if reason == "setup-before-warmup":
            ok = (
                component == "setup"
                and raw.get("status") == "failure"
                and incident["warmup_start"] is not None
                and incident["at"] < incident["warmup_start"]
                and (ep is None or incident["warmup_start"] == ep["warmup_start"])
                and not applied
            )
        elif reason == "harness-oom":
            ok = (
                component in {"collector", "driver"}
                and raw.get("metadata", {}).get("labels", {}).get("k4-component") == component
                and any(
                    s.get("state", {}).get("terminated", {}).get("reason") == "OOMKilled"
                    for s in raw.get("status", {}).get("containerStatuses", [])
                )
            )
        elif reason == "harness-timeout":
            ok = (
                component in {"collector", "driver"}
                and raw.get("component") == component
                and raw.get("exit_code") == 124
                and bool(raw.get("command"))
            )
        elif reason == "collection-failure":
            missing = raw.get("missing", [])
            errors = raw.get("errors", [])
            ok = (
                component == "collector"
                and raw.get("component") == "collector"
                and (bool(errors) or bool(missing))
                and type(errors) is list
                and all(type(e) is str and e for e in errors)
                and type(missing) is list
                and all(
                    type(p) is str
                    and p.startswith(name + "/")
                    and (source / p).resolve().is_relative_to(source.resolve())
                    and not (source / p).exists()
                    for p in missing
                )
            )
        else:
            ok = (
                component == "runner"
                and str(raw.get("id"))
                == str(json.loads((source / "actions-job.json").read_bytes())["id"])
                and raw.get("conclusion") in {"cancelled", "timed_out", "failure"}
                and raw.get("runner_failure") in {"runner-lost", "runner-disconnected"}
            )
        require(ok, "k4-infrastructure-content")


def validate_job(source: Path, f: dict, *, history: dict[str, dict], intake=True) -> dict:
    require(
        not source.is_symlink() and all(not p.is_symlink() for p in source.rglob("*")),
        "k4-intake-symlinks",
    )
    m = load(source / "manifest.json")
    legacy = legacy_form(source, m)
    require(m["schema"] == "dur053.k4-job.v1", "k4-intake-schema")
    job_name(m["job"])
    require(
        m["budget_scope"] == "registered-campaign" and m["measurement_claim"],
        "k4-engineering-not-contribution",
    )
    require(
        m["run_id"].isdigit() and m["job_id"].isdigit() and m["freeze_sha256"] == f["sha256"],
        "k4-intake-provenance",
    )
    require(m["registered_live_seconds"] == CEILING, "k4-intake-reservation")
    expected_charge = (
        m["elapsed_live_seconds"]
        if m["cleanup"] == "PASS" and m["status"] != "interrupted"
        else CEILING
    )
    require(
        type(expected_charge) is int
        and 0 < expected_charge <= CEILING
        and m["charged_seconds"] == expected_charge,
        "k4-intake-charge",
    )
    if m["cleanup"] == "PASS" and m["status"] != "interrupted":
        require(
            m["live_start"] is not None
            and m["live_end"] is not None
            and m["live_end"] >= m["live_start"]
            and math.ceil(m["live_end"] - m["live_start"]) == expected_charge,
            "k4-intake-live-interval",
        )
    require(m["artifact_sha256"] == artifact_hashes(source), "k4-intake-artifact-hashes")
    run = json.loads((source / "actions-run.json").read_bytes())
    job = json.loads((source / "actions-job.json").read_bytes())
    commit = json.loads((source / "actions-commit.json").read_bytes())
    require(
        str(run["id"]) == m["run_id"]
        and str(job["id"]) == m["job_id"]
        and str(job["run_id"]) == m["run_id"]
        and run["event"] == "workflow_dispatch"
        and run["run_attempt"] == 1
        and run["head_sha"] == m["head_sha"] == m["merge_ref_sha"] == commit["sha"]
        and commit["commit"]["tree"]["sha"] == m["tree_sha"]
        and job["name"] == "DUR-053 campaign",
        "k4-intake-actions",
    )
    if m["status"] == "interrupted":
        require(
            job["conclusion"] in {"cancelled", "timed_out", "failure"}, "k4-interrupted-actions"
        )
    assigned = assignment(f, m["job"], history, replacement_for=m["replacement_for"])
    require(m["assigned"] == assigned, "k4-intake-assignment")
    require(
        len(m["pending"]) <= len(assigned)
        and (not m["pending"] or assigned[-len(m["pending"]) :] == m["pending"]),
        "k4-intake-pending-suffix",
    )
    done = assigned[: len(assigned) - len(m["pending"])]
    ids = done if m["replacement_for"] else [r["episode"] for r in registry() if r["unit"] in done]
    previously_started = {eid for prior in history.values() for eid in prior.get("episodes", [])}
    if not m["replacement_for"]:
        ids = [eid for eid in ids if eid not in previously_started]
        if m["intake_stopped"] and m["pending"]:
            partial = [
                r["episode"]
                for r in registry()
                if r["unit"] == m["pending"][0] and r["episode"] not in previously_started
            ]
            # Only a stopped intake can retain a prefix of an admitted pair.
            extra = len(m["episodes"]) - len(ids)
            require(
                0 < extra < len(partial) and m["episodes"][-1] in m["intake_stopped"],
                "k4-stopped-pair-prefix",
            )
            ids += partial[:extra]
    require(m["episodes"] == ids, "k4-intake-whole-pair")
    require(m["status"] != "CARRY_OVER" or bool(m["pending"]), "k4-intake-carry-over")
    require(m["status"] != "PASS" or not m["pending"], "k4-intake-pass-pending")
    if m["replacement_for"]:
        prior = next((v for v in history.values() if v["job_id"] == m["replacement_for"]), None)
        require(
            prior is not None
            and not m["pending"]
            and not any(x["replacement_for"] == m["replacement_for"] for x in history.values()),
            "k4-infrastructure-replacement",
        )
    require([d["episode"] for d in m["dispositions"]] == ids, "k4-disposition-inventory")
    stopped = [
        name
        for name in ids
        if (source / name / "episode.json").is_file()
        and load(source / name / "episode.json")["metrics"] == "not-derivable"
    ]
    require(
        stopped == m["intake_stopped"] and (not stopped or m["status"] == "FAIL"),
        "k4-stopped-job-fail",
    )
    for d in m["dispositions"]:
        cell = episode_identity(d["episode"])
        require(
            d["replacement_for"] == (cell["episode"] if m["replacement_for"] else ""),
            "k4-episode-replacement-link",
        )
        if not legacy or d["classification"] != "valid":
            validate_disposition(source, d)
    for name in m["episodes"]:
        if not (source / name / "episode.json").exists():
            continue  # Validated collection/runner loss; bytes remain in the archive.
        ep = load(source / name / "episode.json")
        cell = episode_identity(name)
        require(
            all(
                ep[k] == m[k]
                for k in (
                    "job",
                    "run_id",
                    "job_id",
                    "head_sha",
                    "tree_sha",
                    "freeze_sha256",
                    "budget_scope",
                    "measurement_claim",
                )
            )
            and ep["episode"] == name
            and ep["seed"] == cell["seed"]
            and ep["arm"] == cell["arm"]
            and ep["replacement_for"] == (cell["episode"] if m["replacement_for"] else ""),
            "k4-intake-episode-binding",
        )
    require(
        {p.parent.name for p in source.glob("*/episode.json")} <= set(m["episodes"]),
        "k4-intake-episode-directory",
    )
    return m


def prepare(source: Path, repository: Path, checker: Path):
    from dur053_k4_evidence import episode, safety_evidence, summaries

    f = frozen(repository)
    prior, ledger = inventory(repository)
    history = {m["job_id"]: m for _, m in prior}
    m = validate_job(source, f, history=history)
    document = legacy_reclassification(source, f, checker, m) if legacy_form(source, m) else None
    m = classified_manifest(m, document)
    target = repository / EVIDENCE_ROOT / m["job"] / m["run_id"]
    require(not target.exists(), "k4-intake-existing")
    require(
        m["job_id"] not in {str(x["job_id"]) for x in ledger["entries"]}, "k4-intake-duplicate-job"
    )
    previous = {e for _, v in prior for e in contributions(v)}
    require(not previous.intersection(contributions(m)), "k4-intake-duplicate-episode")
    ledger_check(ledger, m["charged_seconds"], set())
    values = []
    for name in [] if document else metric_episodes(m, source):
        computed = episode(source / name, f, checker)
        require(
            computed == json.loads((source / name / "metrics.json").read_bytes()),
            "k4-intake-metrics",
        )
        values.append(computed)
        validate_disposition(
            source, next(d for d in m["dispositions"] if d["episode"] == name), metrics=computed
        )
    safety_rows = (
        [s for name in m["episodes"] if (s := safety_evidence(source / name, checker)) is not None]
        if not document
        else []
    )
    require(
        document is not None
        or summaries(values, safety_rows=safety_rows)
        == json.loads((source / "summary.json").read_bytes()),
        "k4-intake-summary",
    )
    require(
        m["status"] != "PASS"
        or (all(v["valid"] for v in values) and len(contributions(m)) == len(m["episodes"])),
        "k4-intake-false-pass",
    )
    raw = (source / "manifest.json").read_bytes()
    row = {
        k: m[k]
        for k in (
            "run_id",
            "job_id",
            "head_sha",
            "merge_ref_sha",
            "tree_sha",
            "status",
            "charged_seconds",
        )
    }
    row.update(
        category=m["job"],
        k4_episodes=contributions(m),
        artifact=(target / "manifest.json").relative_to(repository).as_posix(),
        artifact_sha256=hashlib.sha256(raw).hexdigest(),
    )
    intake_document = dict(
        schema="dur053.k4-intake.v1",
        manifest_sha256=hashlib.sha256(raw).hexdigest(),
        metrics=document["metrics"]
        if document
        else [v for v in values if v["episode"] in contributions(m)],
        safety_rows=document["safety_rows"] if document else safety_rows,
    )
    return target, m, document, intake_document, row, ledger


def stage(source: Path, staging: Path, repository: Path, checker: Path) -> dict:
    require(not staging.resolve().is_relative_to(repository.resolve()), "k4-stage-external")
    require(not staging.exists(), "k4-stage-existing")
    _, _, document, intake_document, _, _ = prepare(source, repository, checker)
    staging.mkdir(parents=True)
    index = pack(source, staging)
    save(staging / "intake.json", intake_document)
    if document:
        save(staging / "reclassification.json", document)
    with scratch(repository) as directory_store:
        shutil.copytree(staging / "assets", directory_store / index["store"]["tag"])
        with materialize(staging, store=directory_store):
            pass
    return index


def record(staging: Path, repository: Path, checker: Path, *, store="github") -> Path:
    """Commit only after fetching and fully validating the uploaded release."""
    # Staging alone includes assets/; committed evidence must never include it.
    validate_index(staging)
    require(
        {p.name for p in staging.iterdir()}
        <= {
            "manifest.json",
            "payload-index.json",
            "intake.json",
            "reclassification.json",
            "assets",
        },
        "k4-stage-layout",
    )
    with materialize(staging, store=store) as source:
        target, m, document, intake_document, row, ledger = prepare(source, repository, checker)
        require(
            json.loads((staging / "intake.json").read_bytes()) == intake_document,
            "k4-intake-document",
        )
        require(
            (staging / "reclassification.json").exists() == (document is not None),
            "k4-intake-document",
        )
        if document:
            require(
                json.loads((staging / "reclassification.json").read_bytes()) == document,
                "k4-legacy-reclassification",
            )
        names = ["manifest.json", "payload-index.json", "intake.json"]
        if document:
            names.append("reclassification.json")
        for name in names[1:]:
            key = {
                "payload-index.json": "payload_index_sha256",
                "intake.json": "intake_sha256",
                "reclassification.json": "reclassification_sha256",
            }[name]
            row[key] = hashlib.sha256((staging / name).read_bytes()).hexdigest()
        target.mkdir(parents=True)
        for name in names:
            shutil.copyfile(staging / name, target / name)
    ledger["entries"].append(row)
    try:
        save(repository / LEDGER, ledger)
    except Exception:
        raise RuntimeError(
            "k4 ledger write failed; retained evidence requires reconciliation"
        ) from None
    return target


def metric_episodes(m: dict, source: Path | None = None) -> list[str]:
    return [
        d["episode"]
        for d in m["dispositions"]
        if d["classification"] != "infrastructure-invalid"
        or (
            d["reason"] == "harness-confirmation-miss"
            and (source is None or (source / d["episode"] / "metrics.json").exists())
        )
    ]


def interrupted(source: Path, repository: Path, name: str, checker: Path, destination: Path):
    """Retain an incomplete download verbatim, without trusting its partial manifest."""
    f = frozen(repository)
    prior, _ = inventory(repository)
    history = {m["job_id"]: m for _, m in prior}
    assigned = assignment(f, name, history)
    progress = load(source / "progress.json")
    require(
        progress["schema"] == "dur053.k4-progress.v1"
        and progress["job"] == name
        and progress["started"] == assigned[: len(progress["started"])],
        "k4-interrupted-progress",
    )
    run = json.loads((source / "actions-run.json").read_bytes())
    job = json.loads((source / "actions-job.json").read_bytes())
    commit = json.loads((source / "actions-commit.json").read_bytes())
    with scratch(repository) as staging:
        raw = staging / "raw"
        raw.mkdir()
        require(
            not source.is_symlink() and all(not p.is_symlink() for p in source.rglob("*")),
            "k4-interrupted-symlinks",
        )
        shutil.copytree(source, raw / "downloaded")
        for file in ("actions-run.json", "actions-job.json", "actions-commit.json"):
            shutil.copyfile(source / file, raw / file)
        prior_episodes = {eid for _, prior_manifest in prior for eid in prior_manifest["episodes"]}
        ids = [
            r["episode"]
            for r in registry()
            if r["unit"] in progress["started"] and r["episode"] not in prior_episodes
        ]
        require(
            {p.parent.name for p in source.glob("*/episode.json")} <= set(ids),
            "k4-interrupted-progress-omission",
        )
        dispositions = []
        values = []
        from dur053_k4_evidence import FILES, episode, safety_evidence, summaries

        for eid in ids:
            epdir = source / eid
            complete = all((epdir / file).is_file() for file in ("episode.json", *FILES.values()))
            if epdir.exists():
                shutil.copytree(epdir, raw / eid)
            if complete:
                ep = load(epdir / "episode.json")
                value = episode(raw / eid, f, checker)
                save(raw / eid / "metrics.json", value)
                values.append(value)
                disposition = derive_disposition(raw, eid, value)
                save_disposition_evidence(raw, disposition)
                dispositions.append(disposition)
            else:
                report = f"incidents/{eid}-collection.json"
                incident = f"incidents/{eid}.json"
                save(
                    raw / report,
                    dict(
                        component="collector",
                        errors=["interrupted download"],
                        missing=[
                            f"{eid}/{file}"
                            for file in ("episode.json", *FILES.values())
                            if not (raw / eid / file).exists()
                        ],
                    ),
                )
                save(
                    raw / incident,
                    dict(
                        schema="dur053.k4-harness.v1",
                        episode=eid,
                        reason="collection-failure",
                        component="collector",
                        at=0,
                        warmup_start=None,
                        source=report,
                    ),
                )
                dispositions.append(
                    dict(
                        episode=eid,
                        replacement_for="",
                        classification="infrastructure-invalid",
                        reason="collection-failure",
                        evidence=[incident],
                    )
                )
        m = dict(
            schema="dur053.k4-job.v1",
            job=name,
            run_id=str(run["id"]),
            job_id=str(job["id"]),
            head_sha=run["head_sha"],
            merge_ref_sha=run["head_sha"],
            tree_sha=commit["commit"]["tree"]["sha"],
            freeze_sha256=f["sha256"],
            budget_scope="registered-campaign",
            measurement_claim=True,
            status="interrupted",
            registered_live_seconds=CEILING,
            charged_seconds=CEILING,
            elapsed_live_seconds=None,
            live_start=None,
            live_end=None,
            cleanup="UNTRUSTWORTHY",
            episodes=ids,
            assigned=assigned,
            pending=assigned[len(progress["started"]) :],
            replacement_for="",
            dispositions=dispositions,
            intake_stopped=[],
            artifact_sha256={},
        )
        safety_rows = [s for eid in ids if (s := safety_evidence(raw / eid, checker)) is not None]
        save(raw / "summary.json", summaries(values, safety_rows=safety_rows))
        m["artifact_sha256"] = artifact_hashes(raw)
        save(raw / "manifest.json", m)
        return stage(raw, destination, repository, checker)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["stage", "commit"])
    p.add_argument("--downloaded", type=Path)
    p.add_argument("--staging", type=Path, required=True)
    p.add_argument("--repository", type=Path, default=ROOT)
    p.add_argument("--checker", type=Path, required=True)
    p.add_argument("--interrupted-job")
    p.add_argument("--store", default="github")
    a = p.parse_args()
    if a.command == "stage":
        require(a.downloaded is not None, "k4-stage-download-required")
        index = (
            interrupted(a.downloaded, a.repository, a.interrupted_job, a.checker, a.staging)
            if a.interrupted_job
            else stage(a.downloaded, a.staging, a.repository, a.checker)
        )
        print(json.dumps(index, sort_keys=True, indent=2))
    else:
        print(record(a.staging, a.repository, a.checker, store=a.store))


if __name__ == "__main__":
    main()
