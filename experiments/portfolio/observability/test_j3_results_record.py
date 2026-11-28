import hashlib
import json
import re
from pathlib import Path

ROOT = Path(__file__).with_name("j3-r421-46050616178")
RUN = 46050616178
HEAD = "2a3451a2a10b8e002086ff1ba69f40f6d369733a"
RELEASE_INDEX = "j3-r421-46050616178-index.json"
RELEASE_INDEX_SHA256 = "7cdae1077e6665f3e7e2bf8355c16b09d5be8ec38c933af4edf82d7d5773440f"
RELEASE_URL = (
    "https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/"
    "releases/tag/j3-r421-46050616178-evidence"
)


def read(name):
    return json.loads((ROOT / name).read_bytes())


def test_qualification_summary_hashes_and_artifact_bindings():
    index = read("sha256.json")
    assert (index["run_id"], index["head"], index["workflow_attempt"], index["review_round"]) == (
        RUN,
        HEAD,
        1,
        607,
    )
    artifacts = {row["id"]: row for row in index["artifacts"]}
    assert len(artifacts) == len(index["artifacts"])
    assert {row["id"] for row in artifacts.values() if row["run_id"] == RUN} == {
        15223759432,
        15235390969,
        15235740301,
        15235825154,
        15239329022,
    }
    for artifact in artifacts.values():
        assert re.fullmatch(r"sha256:[0-9a-f]{64}", artifact["digest"])
        assert artifact["size_in_bytes"] > 0
        assert artifact["url"].endswith(f"/runs/{artifact['run_id']}/artifacts/{artifact['id']}")
        if artifact["run_id"] == RUN:
            assert artifact["head"] == HEAD

    retained = {
        path.relative_to(ROOT).as_posix()
        for path in ROOT.rglob("*")
        if path.is_file() and path.name not in {"results.md", "sha256.json"}
    }
    assert retained == index["files"].keys()
    used_artifacts = set()
    for name, entry in index["files"].items():
        contents = (ROOT / name).read_bytes()
        assert (len(contents), hashlib.sha256(contents).hexdigest()) == (
            entry["size_in_bytes"],
            entry["sha256"],
        ), name
        sources = entry["sources"]
        if entry["kind"] == "release-asset":
            assert name == RELEASE_INDEX
            assert entry["sha256"] == RELEASE_INDEX_SHA256
            assert sources == [{"asset": RELEASE_INDEX, "release_url": RELEASE_URL}]
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


def test_release_index_matches_each_actions_zip_and_orders_its_parts():
    index, release = read("sha256.json"), read(RELEASE_INDEX)
    assert (release["run"], release["head"]) == (RUN, HEAD)
    assert index["files"][RELEASE_INDEX]["kind"] == "release-asset"
    actions = {row["id"]: row for row in index["artifacts"] if row["run_id"] == RUN}
    backed_up = {row["actions_id"]: row for row in release["artifacts"]}
    assert len(release["artifacts"]) == len(backed_up) == 5
    assert backed_up.keys() == actions.keys()
    part_counts = {15235740301: 2, 15239329022: 3}
    max_part_size = 64 * 1024**2
    for artifact_id, row in backed_up.items():
        original = actions[artifact_id]
        assert (row["name"], row["size"]) == (original["name"], original["size_in_bytes"])
        assert f"sha256:{row['sha256']}" == row["actions_digest"] == original["digest"]

        parts = row["parts"]
        count = part_counts.get(artifact_id, 1)
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
        assert (episode["run"], episode["head"], episode["attempt"]) == (str(RUN), HEAD, "1")
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
