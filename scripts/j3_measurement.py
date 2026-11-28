"""Bind the registered J3 measurement to its reviewed seal and replay its evidence."""

import argparse
import hashlib
import json
import os
import re
import time
from pathlib import Path

from j3_evidence import jsonl, notifications
from j3_overlay import J3
from j3_prometheus import evaluation_rows
from j3_replay import DEADLINE, DOMAIN, SECOND, burn, on_time, reconcile, replay, require
from j3_runner_gate import campaign_id
from j3_schedule import CALIBRATION_ARMS, FORECAST_CAP, verify_seal

QUALIFICATION = J3 / "j3-r421-46050616178"
SEAL_ARTIFACT = 15235825154
RELEASE_INDEX = "j3-r421-46050616178-index.json"
RELEASE_INDEX_SHA256 = "7cdae1077e6665f3e7e2bf8355c16b09d5be8ec38c933af4edf82d7d5773440f"
EPISODES = ("primary", "matched-healthy", "foreign", "scrape-loss")
JOB_INPUTS = (
    "run.json",
    "review.json",
    "verified-schedule.json",
    "versions.json",
    "sender.sha256",
    "runner-gate/runner-gate.json",
    "promtool/rules.yml",
    "promtool/tests.yml",
)
EPISODE_INPUTS = (
    "run.json",
    "application-rendered.json",
    "overlay.json",
    "kind-running-images.json",
    "running-images.json",
    "exporter-role-settings.json",
    "db-denials.json",
    "network-denials.json",
    "db-sessions.json",
    "alert-arming.json",
    "plan.json",
    "timing.json",
)


def read(path):
    return json.loads(path.read_bytes())


def save(path, record):
    path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def reviewed_candidate():
    head = os.environ.get("J3_REVIEWED_HEAD", "")
    review = os.environ.get("J3_REVIEW_ROUND", "")
    require(
        re.fullmatch(r"[0-9a-f]{40}", head)
        and head == os.environ["GITHUB_SHA"]
        and re.fullmatch(r"[1-9][0-9]*", review),
        "measurement requires the Claude-reviewed dispatch head and review round",
    )
    return {"head": head, "review_round": int(review)}


def verified_schedule(archive):
    """Check the original bytes and the reviewed confirmation bound to those bytes."""
    root = QUALIFICATION
    index = read(root / "sha256.json")
    require(
        index["review_round"] == 607
        and index["workflow_attempt"] == 1
        and digest(root / RELEASE_INDEX) == RELEASE_INDEX_SHA256,
        "reviewed qualification or Release index differs",
    )
    record_names = (
        "calibration/verdict.json",
        "confirmation/verdict.json",
        "seal/confirmation-check.json",
        "confirmation/measurement-forecast.json",
    )
    for name in record_names:
        require(
            digest(root / name) == index["files"][name]["sha256"], f"qualification {name} differs"
        )
    calibration, confirmation, verified, forecast = (read(root / name) for name in record_names)
    require(
        calibration["status"] == confirmation["status"] == confirmation["full_volume"] == "PASS"
        and calibration["campaign"] == confirmation["campaign"] == index["campaign"],
        "missing passing gated qualification",
    )
    (released,) = [
        r for r in read(root / RELEASE_INDEX)["artifacts"] if r["actions_id"] == SEAL_ARTIFACT
    ]
    (artifact,) = [r for r in index["artifacts"] if r["id"] == SEAL_ARTIFACT]
    require(
        released["actions_digest"] == artifact["digest"] == f"sha256:{verified['artifact_digest']}"
        and released["sha256"] == verified["artifact_digest"]
        and verified["schedule_equals_seal_artifact"]
        and verified["artifact_id"] == SEAL_ARTIFACT,
        "confirmation and Release bind different seal ZIPs",
    )
    schedule, seal = verify_seal(archive, artifact["digest"], index["campaign"], index["head"])
    require(seal == verified["seal"], "confirmation consumed a different schedule")
    require(
        forecast["forecast_seconds"] >= schedule["forecast_seconds"]
        and forecast["forecast_seconds"] <= FORECAST_CAP * 60
        and forecast["forecast_seconds"]
        == forecast["forecast_allowance_seconds"]["bootstrap"]
        + remaining_forecast(forecast, EPISODES),
        "confirmed measurement forecast over cap or below the sealed allowance",
    )
    return {
        "qualification_run": index["run_id"],
        "qualification_review_round": index["review_round"],
        "artifact_id": SEAL_ARTIFACT,
        "artifact_digest": artifact["digest"],
        "seal": seal,
        "schedule": schedule,
        "forecast": forecast,
    }


def measurement_arms(schedule):
    foreign, scrape = CALIBRATION_ARMS["foreign"], CALIBRATION_ARMS["scrape-loss"]
    primary, healthy = schedule["primary"], schedule["matched_healthy"]
    return (
        ("primary", "outage", primary["stop_s"], primary),
        ("matched-healthy", "matched_healthy", healthy["stop_s"], healthy),
        ("foreign", foreign[0], foreign[1], None),
        ("scrape-loss", scrape[0], scrape[1], None),
    )


def remaining_forecast(forecast, remaining):
    allowances = forecast["forecast_allowance_seconds"]
    extended = forecast["forecast_extended_arm_seconds"]
    by_episode = {
        "primary": extended["primary"] + allowances["primary_export_reset"],
        "matched-healthy": extended["matched_healthy"] + allowances["matched_healthy_export_reset"],
        "foreign": allowances["foreign"] + allowances["foreign_export_reset"],
        "scrape-loss": allowances["scrape_loss"],
    }
    return allowances["final_evidence"] + sum(by_episode[name] for name in remaining)


def arm_input_hashes(root, name):
    paths = (*JOB_INPUTS, *(f"{name}/{file}" for file in EPISODE_INPUTS))
    return {path: digest(root / path) for path in paths}


def seal_arm_inputs(episode, origin):
    hashes = arm_input_hashes(episode.parent, episode.name)
    sealed = time.time_ns()
    require(sealed < origin, "measurement inputs were not sealed before origin")
    save(
        episode / "pre-arm-inputs.json", {"sealed_ns": sealed, "origin_ns": origin, "files": hashes}
    )


def primary_timing(data, outage, result, bounds, witness):
    origin, restore = outage["origin_ns"], outage["restore_ns"]
    down = outage["scale_down_ns"]
    rows, _ = reconcile(data)
    pre_restore = [r for r in outage["admissions"] if r["admitted_ns"] <= restore]
    published = result["published_attainment"]
    width = max(published["setup_width_ns"], published["closing_width_ns"])
    last_bad = max(
        (
            rows[r["workflow_id"]]["scheduled_ns"] + DEADLINE
            for r in pre_restore
            if not on_time(rows[r["workflow_id"]], width, data["end_ns"])
        ),
        default=None,
    )
    valid = [r for r in data["evaluations"] if r["valid"]]
    oldest = next(
        r["at_ns"]
        for r in valid
        if r["claimable_attempts"] > 0 and r["oldest_claimable_age_seconds"] > 28.540
    )
    burn_input = next(r["at_ns"] for r in valid if burn(r["windows"], "bad")[0])
    clear_input = next(
        r["at_ns"]
        for r in valid
        if r["at_ns"] > restore
        and (r["claimable_attempts"] == 0 or r["oldest_claimable_age_seconds"] <= 28.540)
    )
    burn_clear = next(
        r["at_ns"] for r in valid if r["at_ns"] > restore and burn(r["windows"], "bad")[1]
    )
    transitions = result["alert_inputs"]["transitions"]
    return {
        "origin_ns": origin,
        "scale_down_ns": down,
        "restore_ns": restore,
        "scale_command_interval_ns": restore - down,
        "witnessed_zero_to_restore_ns": restore - witness["zero_workers_ns"],
        "first_fire_inputs_ns": dict(zip(DOMAIN, (oldest, burn_input), strict=True)),
        "first_oldest_clear_input_ns": clear_input,
        "first_burn_clear_input_ns": burn_clear,
        "last_pre_restore_terminal_ns": max(r["terminal_ns"] for r in pre_restore),
        "last_bad_pre_restore_deadline_ns": last_bad,
        "subsequent_window_counts": [
            {"at_ns": row["at_ns"], "windows": row["windows"]}
            for row in valid
            if last_bad is not None and row["at_ns"] >= last_bad
        ],
        "worker_witness": witness,
        "transitions": [
            {
                **row,
                "seconds_from_origin": (row["at_ns"] - origin) / SECOND,
                "seconds_from_command": (row["at_ns"] - (down if row["firing"] else restore))
                / SECOND,
            }
            for row in transitions
        ],
        "bounds_s_from_origin": bounds,
    }


def replay_episode(root, target):
    data = read(root / "replay-input.json")
    require(data["namespace"] == target["namespace"], "offline replay scope differs")
    raw = read(root / "prometheus-raw.json")
    rebuilt = evaluation_rows(
        raw["matrices"],
        jsonl(root / "visibility.jsonl"),
        target,
        data["start_ns"],
        data["end_ns"],
        raw["rule_group"]["rules"],
        raw["scrape_timeout_ns"],
    )
    require(rebuilt == data["evaluations"], "offline Prometheus evaluations differ")
    result = replay(data, jsonl(root / "visibility.jsonl"))
    require(result == read(root / "replay.json"), "offline alert or attainment replay differs")
    transitions = result["alert_inputs"]["transitions"]
    delivery = notifications(
        list(jsonl(root / "notifications.jsonl")),
        transitions,
        target,
        data["start_ns"],
        data["end_ns"],
    )
    require(
        delivery["matches"] == read(root / "notification-pairs.json"), "offline pairings differ"
    )
    require(
        delivery["setup_incidents"] == read(root / "setup-incidents.json")["setup_incidents"],
        "offline setup incidents differ",
    )
    return {
        "status": "PASS",
        "transitions": transitions,
        "paired_deliveries": len(delivery["matches"]),
        "ambiguous_evaluations": result["alert_inputs"]["ambiguous_count"],
        "committed_timeouts": len(result["committed_timeout_keys"]),
        "published_attainment": {
            k: v for k, v in result["published_attainment"].items() if k != "raw_durable_latency_ns"
        },
    }


def replay_campaign(root, verdict):
    episodes = {}
    for name in EPISODES:
        row = verdict["episodes"][name]
        try:
            if row["status"] != "PASS" and not (root / name / "replay-input.json").is_file():
                episodes[name] = {"status": "UNPERFORMED"}
                continue
            profile = read(root / name / "run.json")
            require(
                all(profile[k] == verdict[k] for k in ("run", "head", "attempt", "campaign"))
                and profile["episode"] == name
                and profile["target"] == row["target"],
                "offline episode provenance differs",
            )
            if row["status"] == "PASS":
                seal = read(root / name / "pre-arm-inputs.json")
                require(
                    seal["sealed_ns"]
                    < seal["origin_ns"]
                    == read(root / name / "timing.json")["origin_ns"]
                    and seal["files"] == arm_input_hashes(root, name),
                    "offline pre-arm input seal differs",
                )
            episodes[name] = replay_episode(root / name, row["target"])
        except Exception as error:
            episodes[name] = {"status": "INVALID", "error": str(error)}
    return {"campaign": verdict["campaign"], "episodes": episodes}


def evidence_hashes(root):
    return {
        p.relative_to(root).as_posix(): digest(p)
        for p in sorted(root.rglob("*"))
        if p.is_file() and p != root / "sha256.json"
    }


def finalize(root):
    """Retain a complete index even when an episode or a prerequisite failed."""
    root.mkdir(parents=True, exist_ok=True)
    path = root / "verdict.json"
    try:
        verdict = read(path)
    except FileNotFoundError:
        verdict = {"status": "INVALID", "reason": "measurement prerequisites did not complete"}
    except ValueError:
        path.rename(root / "invalid-live-verdict.json")
        verdict = {"status": "INVALID", "reason": "unreadable live verdict"}
    verdict.update(
        stage="measurement",
        campaign=campaign_id(),
        run=os.environ["GITHUB_RUN_ID"],
        head=os.environ["GITHUB_SHA"],
        attempt=os.environ["GITHUB_RUN_ATTEMPT"],
    )
    episodes = verdict.setdefault("episodes", {})
    for name in EPISODES:
        episodes.setdefault(name, {"status": "UNPERFORMED"})
    report = replay_campaign(root, verdict)
    verdict["offline_replay"] = (
        "PASS"
        if all(row["status"] == "PASS" for row in report["episodes"].values())
        else "UNPERFORMED"
        if all(row["status"] == "UNPERFORMED" for row in report["episodes"].values())
        else "INCOMPLETE"
    )
    cleanup = root / "cleanup.json"
    try:
        verdict["cleanup"] = read(cleanup)["status"]
    except FileNotFoundError:
        verdict["cleanup"] = "UNPERFORMED"
    except (ValueError, KeyError):
        verdict["cleanup"] = "INVALID"
    if verdict["status"] == "PASS" and (
        any(episodes[name]["status"] != "PASS" for name in EPISODES)
        or verdict["offline_replay"] != "PASS"
        or verdict["cleanup"] != "PASS"
    ):
        verdict.update(status="INVALID", reason="offline replay or cleanup incomplete")
    save(root / "offline-replay.json", report)
    save(path, verdict)
    save(root / "sha256.json", evidence_hashes(root))
    return verdict["status"] == "PASS"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("review").add_argument("evidence", type=Path)
    seal = actions.add_parser("seal")
    seal.add_argument("archive", type=Path)
    seal.add_argument("evidence", type=Path)
    actions.add_parser("finalize").add_argument("evidence", type=Path)
    actions.add_parser("replay").add_argument("evidence", type=Path)
    args = parser.parse_args()
    root = args.evidence
    if args.action == "review":
        review = reviewed_candidate()
        root.mkdir(parents=True, exist_ok=True)
        save(root / "review.json", review)
    elif args.action == "seal":
        save(root / "verified-schedule.json", verified_schedule(args.archive))
    elif args.action == "finalize":
        return 0 if finalize(root) else 1
    else:
        require(
            read(root / "sha256.json") == evidence_hashes(root), "campaign evidence index differs"
        )
        report = replay_campaign(root, read(root / "verdict.json"))
        require(report == read(root / "offline-replay.json"), "campaign offline report differs")
        print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
