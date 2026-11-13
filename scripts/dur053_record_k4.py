"""Offline, immutable K4 B.4 intake. Never dispatches or discards failed evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
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
from dur053_payload import materialize, pack, scratch
from dur053_pilot import committed_bytes


def save(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes((json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode())


def inventory(repository: Path) -> tuple[list[tuple[Path, dict]], dict]:
    ledger = json.loads(committed_bytes(repository, repository / LEDGER))
    found = []
    for path in sorted((repository / EVIDENCE_ROOT).glob("*/*/manifest.json")):
        committed_bytes(repository, path)
        m = load(path)
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
        with materialize(path.parent, repository):
            pass
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
    return found, ledger


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


def validate_disposition(source: Path, d: dict) -> None:
    """Validate independent harness records and their retained source content."""
    name = d["episode"]
    episode_path = source / name / "episode.json"
    ep = load(episode_path) if episode_path.exists() else None
    if d["classification"] != "infrastructure-invalid":
        require(
            not d["reason"] and not d["evidence"] and ep is not None, "k4-classification-content"
        )
        require(
            ep["status"]
            in ({"PASS", "FAIL"} if d["classification"] == "valid" else {"unexercised"}),
            "k4-classification-status",
        )
        return
    require(bool(d["reason"]) and bool(d["evidence"]), "k4-infrastructure-evidence")
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
    for d in m["dispositions"]:
        cell = episode_identity(d["episode"])
        require(
            d["replacement_for"] == (cell["episode"] if m["replacement_for"] else ""),
            "k4-episode-replacement-link",
        )
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


def record(source: Path, repository: Path, checker: Path) -> Path:
    from dur053_k4_evidence import episode, summaries

    f = frozen(repository)
    prior, ledger = inventory(repository)
    history = {m["job_id"]: m for _, m in prior}
    m = validate_job(source, f, history=history)
    target = repository / EVIDENCE_ROOT / m["job"] / m["run_id"]
    require(not target.exists(), "k4-intake-existing")
    require(
        m["job_id"] not in {str(x["job_id"]) for x in ledger["entries"]}, "k4-intake-duplicate-job"
    )
    previous = {e for _, v in prior for e in contributions(v)}
    require(not previous.intersection(contributions(m)), "k4-intake-duplicate-episode")
    ledger_check(ledger, m["charged_seconds"], set())
    values = []
    for name in metric_episodes(m):
        computed = episode(source / name, f, checker)
        require(
            computed == json.loads((source / name / "metrics.json").read_bytes()),
            "k4-intake-metrics",
        )
        values.append(computed)
    require(
        summaries(values) == json.loads((source / "summary.json").read_bytes()), "k4-intake-summary"
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
    with scratch(repository) as staging:
        pack(source, staging)
        row["payload_index_sha256"] = hashlib.sha256(
            (staging / "payload-index.json").read_bytes()
        ).hexdigest()
        # Do all validation/compression before making the evidence visible.
        shutil.copytree(staging, target)
    ledger["entries"].append(row)
    try:
        save(repository / LEDGER, ledger)
    except Exception:
        # Retain evidence on write failure; inventory refuses the unbound directory.
        raise RuntimeError(
            "k4 ledger write failed; retained evidence requires reconciliation"
        ) from None
    return target


def metric_episodes(m: dict) -> list[str]:
    return [
        d["episode"] for d in m["dispositions"] if d["classification"] != "infrastructure-invalid"
    ]


def interrupted(source: Path, repository: Path, name: str, checker: Path) -> Path:
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
        ids = [r["episode"] for r in registry() if r["unit"] in progress["started"]]
        require(
            {p.parent.name for p in source.glob("*/episode.json")} <= set(ids),
            "k4-interrupted-progress-omission",
        )
        dispositions = []
        values = []
        from dur053_k4_evidence import FILES, episode, summaries

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
                dispositions.append(
                    dict(
                        episode=eid,
                        replacement_for="",
                        classification="valid"
                        if ep["status"] in {"PASS", "FAIL"}
                        else "unexercised",
                        reason="",
                        evidence=[],
                    )
                )
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
            artifact_sha256={},
        )
        save(raw / "summary.json", summaries(values))
        m["artifact_sha256"] = artifact_hashes(raw)
        save(raw / "manifest.json", m)
        return record(raw, repository, checker)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--downloaded", type=Path, required=True)
    p.add_argument("--repository", type=Path, default=ROOT)
    p.add_argument("--checker", type=Path, required=True)
    p.add_argument("--interrupted-job")
    a = p.parse_args()
    print(
        interrupted(a.downloaded, a.repository, a.interrupted_job, a.checker)
        if a.interrupted_job
        else record(a.downloaded, a.repository, a.checker)
    )


if __name__ == "__main__":
    main()
