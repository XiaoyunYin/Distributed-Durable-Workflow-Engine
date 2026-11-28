import hashlib
import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).with_name("j3-r421-46050616178")
MEASUREMENT = ROOT.with_name("j3-r426-46122607713")
RELEASES = "https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/releases/tag"
RECORDS = (
    pytest.param(
        ROOT,
        46050616178,
        "2a3451a2a10b8e002086ff1ba69f40f6d369733a",
        607,
        "7cdae1077e6665f3e7e2bf8355c16b09d5be8ec38c933af4edf82d7d5773440f",
        {15223759432: 1, 15235390969: 1, 15235740301: 2, 15235825154: 1, 15239329022: 3},
        id="qualification",
    ),
    pytest.param(
        MEASUREMENT,
        46122607713,
        "28d4109eafb95514d03032baca3819e26bfa331a",
        647,
        "7275f268f2e1c8eae604ff6a6a59c024eb9e9de06ec974f41c4bf4d1da2945b5",
        {15254950390: 1, 15260625530: 4},
        id="measurement",
    ),
)


def read(name, root=ROOT):
    return json.loads((root / name).read_bytes())


@pytest.mark.parametrize("root,run,head,review_round,release_sha,part_counts", RECORDS)
def test_summary_hashes_and_artifact_bindings(
    root, run, head, review_round, release_sha, part_counts
):
    index = read("sha256.json", root)
    release_index = f"{root.name}-index.json"
    release_url = f"{RELEASES}/{root.name}-evidence"
    assert (index["run_id"], index["head"], index["workflow_attempt"], index["review_round"]) == (
        run,
        head,
        1,
        review_round,
    )
    artifacts = {row["id"]: row for row in index["artifacts"]}
    assert len(artifacts) == len(index["artifacts"])
    assert {row["id"] for row in artifacts.values() if row["run_id"] == run} == part_counts.keys()
    for artifact in artifacts.values():
        assert re.fullmatch(r"sha256:[0-9a-f]{64}", artifact["digest"])
        assert artifact["size_in_bytes"] > 0
        assert artifact["url"].endswith(f"/runs/{artifact['run_id']}/artifacts/{artifact['id']}")
        if artifact["run_id"] == run:
            assert artifact["head"] == head

    retained = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and path.name not in {"results.md", "sha256.json"}
    }
    assert retained == index["files"].keys()
    assert index["files"][release_index]["kind"] == "release-asset"
    used_artifacts = set()
    for name, entry in index["files"].items():
        contents = (root / name).read_bytes()
        assert (len(contents), hashlib.sha256(contents).hexdigest()) == (
            entry["size_in_bytes"],
            entry["sha256"],
        ), name
        sources = entry["sources"]
        if entry["kind"] == "release-asset":
            assert name == release_index
            assert entry["sha256"] == release_sha
            assert sources == [{"asset": release_index, "release_url": release_url}]
            continue
        assert entry["kind"] in {"archive-member", "projection"}
        assert sources
        for source in sources:
            used_artifacts.add(source["artifact_id"])
            assert source["member"]
            assert source["size_in_bytes"] > 0
            assert re.fullmatch(r"[0-9a-f]{64}", source["sha256"])
        if entry["kind"] == "archive-member":
            (source,) = sources
            assert (source["sha256"], source["size_in_bytes"]) == (entry["sha256"], len(contents))
    assert used_artifacts == artifacts.keys()


@pytest.mark.parametrize("root,run,head,_review_round,release_sha,part_counts", RECORDS)
def test_release_index_matches_each_actions_zip_and_orders_its_parts(
    root, run, head, _review_round, release_sha, part_counts
):
    index = read("sha256.json", root)
    release_path = root / f"{root.name}-index.json"
    assert hashlib.sha256(release_path.read_bytes()).hexdigest() == release_sha
    release = json.loads(release_path.read_bytes())
    assert (release["run"], release["head"]) == (run, head)
    actions = {row["id"]: row for row in index["artifacts"] if row["run_id"] == run}
    backed_up = {row["actions_id"]: row for row in release["artifacts"]}
    assert len(release["artifacts"]) == len(backed_up) == len(part_counts)
    assert backed_up.keys() == actions.keys()
    max_part_size = 64 * 1024**2
    for artifact_id, row in backed_up.items():
        original = actions[artifact_id]
        assert (row["name"], row["size"]) == (original["name"], original["size_in_bytes"])
        assert f"sha256:{row['sha256']}" == row["actions_digest"] == original["digest"]

        parts = row["parts"]
        count = part_counts[artifact_id]
        assert len(parts) == count
        assert sum(part["size"] for part in parts) == row["size"]
        assert all(part["size"] == max_part_size for part in parts[:-1])
        assert 0 < parts[-1]["size"] <= max_part_size
        assert all(re.fullmatch(r"[0-9a-f]{64}", part["sha256"]) for part in parts)
        zip_name = f"{row['name']}.zip"
        if count == 1:
            assert parts[0]["asset"] == zip_name and parts[0]["sha256"] == row["sha256"]
        else:
            assert [part["asset"] for part in parts] == [
                f"{zip_name}.part-{number:03d}-of-{count:03d}" for number in range(1, count + 1)
            ]


def test_qualification_record_links_seal_replays_and_deliveries():
    index = read("sha256.json")
    receipt = read("seal/schedule-seal.json")
    (original,) = [
        source
        for source in index["files"]["seal/sealed-schedule-summary.json"]["sources"]
        if source["member"] == "sealed-schedule.json"
    ]
    assert receipt["sha256"] == original["sha256"]
    assert {
        source["member"]: source["sha256"]
        for source in index["files"]["seal/sealed-schedule-summary.json"]["sources"]
        if source["artifact_id"] == 15235740301
    } == receipt["inputs"]
    confirmation = read("seal/confirmation-check.json")
    (seal_zip,) = (r for r in index["artifacts"] if r["id"] == original["artifact_id"])
    assert confirmation["artifact_id"] == seal_zip["id"]
    assert f"sha256:{confirmation['artifact_digest']}" == seal_zip["digest"]
    assert confirmation["seal"] == receipt and confirmation["schedule_equals_seal_artifact"]

    episodes, states = read("episodes.json"), read("alert-state.json")
    assert len(episodes) == 7 and episodes.keys() == states.keys()
    for stage in ("calibration", "confirmation"):
        verdict = read(f"{stage}/verdict.json")
        assert verdict["status"] == "PASS"
        assert set(verdict["episodes"]) == {
            key for key, row in episodes.items() if row["stage"] == stage
        }
        for row in verdict["episodes"].values():
            assert row["status"] == row["behavior"]["status"] == row["delivery"]["status"] == "PASS"

    for name, episode in episodes.items():
        assert (episode["run"], episode["head"], episode["attempt"]) == (
            str(index["run_id"]),
            index["head"],
            str(index["workflow_attempt"]),
        )
        state = states[name]
        step = state["evaluation_interval_ns"]
        transitions = []
        for alert, segments in state["firing_runs_ns_inclusive"].items():
            for first, last in segments:
                assert state["first_evaluation_ns"] < first <= last < state["last_evaluation_ns"]
                transitions.extend([(alert, True, first), (alert, False, last + step)])
        observed = [(row["alert"], row["firing"], row["at_ns"]) for row in episode["transitions"]]
        assert sorted(transitions) == sorted(observed), name
        pairs = read(f"notifications/{name}.json")
        assert [pair["transition"] for pair in pairs] == episode["transitions"], name
        for pair in pairs:
            assert (
                0 <= pair["delivery"]["received_ns"] - pair["transition"]["at_ns"] <= 30_000_000_000
            )

        receiver = episode["receiver_log"]
        assert receiver["continuous_sequence"] and receiver["periodic_heartbeats"] > 0
        assert receiver["start_heartbeat_ns"] <= episode["timing"]["origin_ns"]
        assert receiver["export_end_ns"] >= episode["timing"]["observe_end_ns"]
        if name in {"foreign", "matched-healthy"}:
            assert not pairs and receiver["delivery_batches"] == 0

    reconciled = read("full-volume/identity-reconciliation.json")
    volume = read("seal/sealed-schedule-summary.json")["arrival_volume"]
    assert reconciled["admitted_count"] == reconciled["checked_successes"] == volume == 19_268
    assert reconciled["valid"] and not any(
        reconciled[key] for key in ("missing", "duplicated", "unexpected")
    )


def test_measurement_record_links_fault_witnesses_and_replayed_deliveries():
    timing = read("primary/timing-summary.json", MEASUREMENT)
    witness = read("primary/outage-witness.json", MEASUREMENT)
    down, restore = map(
        json.loads, (MEASUREMENT / "primary/faults.jsonl").read_bytes().splitlines()
    )
    assert down["replicas"] == 0 and restore["replicas"] == 2
    assert timing["worker_witness"] == witness
    assert down["command_ns"] < witness["zero_workers_ns"] < restore["command_ns"]
    assert timing["witnessed_zero_to_restore_ns"] == (
        restore["command_ns"] - witness["zero_workers_ns"]
    )
    assert timing["scale_command_interval_ns"] == restore["command_ns"] - down["command_ns"]

    schedule = read("verified-schedule-summary.json", MEASUREMENT)
    seal = read("seal/confirmation-check.json")
    assert schedule["seal"] == seal["seal"]
    assert schedule["artifact_id"] == seal["artifact_id"]
    assert schedule["artifact_digest"] == f"sha256:{seal['artifact_digest']}"
    assert schedule["schedule"]["delta_s"] == 480
    assert schedule["schedule"]["arrival_volume"] == 19_268

    verdict = read("verdict.json", MEASUREMENT)
    assert verdict["status"] == verdict["offline_replay"] == verdict["cleanup"] == "PASS"
    prior = read("prior-measurement/verdict.json", MEASUREMENT)
    assert (prior["run"], prior["status"], prior["offline_replay"], prior["cleanup"]) == (
        "46109429112",
        "INVALID",
        "INCOMPLETE",
        "PASS",
    )
    replays = read("offline-replay.json", MEASUREMENT)["episodes"]
    states = read("alert-state.json", MEASUREMENT)
    counts = {
        "primary": (13_302, 19_268, 4),
        "matched-healthy": (19_268, 19_268, 0),
        "foreign": (8_564, 8_564, 0),
        "scrape-loss": (5_138, 5_138, 2),
    }
    assert counts.keys() == replays.keys() == states.keys() == verdict["episodes"].keys()
    for episode, (on_time, eligible, deliveries) in counts.items():
        replay, state = replays[episode], states[episode]
        pairs = read(f"{episode}/notification-pairs.json", MEASUREMENT)
        attainment = replay["published_attainment"]
        assert (attainment["on_time"], attainment["eligible"], len(pairs)) == (
            on_time,
            eligible,
            deliveries,
        )
        assert replay["status"] == "PASS"
        assert attainment["incomplete"] == replay["ambiguous_evaluations"] == 0
        assert [pair["transition"] for pair in pairs] == replay["transitions"]
        step = state["evaluation_interval_ns"]
        transitions = sorted(
            (alert, firing, at)
            for alert, runs in state["firing_runs_ns_inclusive"].items()
            for first, last in runs
            for firing, at in ((True, first), (False, last + step))
        )
        assert transitions == sorted(
            (row["alert"], row["firing"], row["at_ns"]) for row in replay["transitions"]
        )
        for pair in pairs:
            assert 0 <= pair["delivery"]["received_ns"] - pair["transition"]["at_ns"] <= 30 * 10**9

    for row in timing["transitions"]:
        command = down if row["firing"] else restore
        assert row["seconds_from_command"] == (row["at_ns"] - command["command_ns"]) / 10**9
    assert len(states["scrape-loss"]["invalid_evaluations_ns"]) == 12
