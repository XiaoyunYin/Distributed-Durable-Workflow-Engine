import json
from contextlib import nullcontext
from types import SimpleNamespace

import j2_relay_diagnostics as relay
import pytest
from j2_drain import DrainExpired
from j2_harness import PARTITIONS, TOPICS, partition_end_offsets
from j2_offline import SECOND
from j2_schedule_rehearsal import resource_summary


def test_publications_use_durable_timestamps_and_include_zero_seconds():
    def database(sql):
        assert "FROM engine.outbox" in sql
        assert "published_at >= TIMESTAMPTZ '1970-01-01T00:00:01.000001Z'" in sql
        assert "published_at < TIMESTAMPTZ '1970-01-01T00:00:04.500001Z'" in sql
        return "0|8\n2|10\n3|1"

    timeline = relay.publication_timeline(
        SimpleNamespace(read_database=database), SECOND + 1, 4 * SECOND + SECOND // 2 + 1
    )
    assert timeline["published_rows_by_second"] == [8, 0, 10, 1]
    assert timeline["total_published_rows"] == 19
    assert timeline["lower_mono_ns"] <= timeline["upper_mono_ns"]


def test_end_offsets_require_all_partitions_of_both_topics():
    rows = [f"{topic}:{part}:{part + 1}" for topic in TOPICS.values() for part in PARTITIONS]
    samples = partition_end_offsets("\n".join([*rows, "__consumer_offsets:0:43"]))
    assert set(samples) == set(TOPICS.values())
    assert all(set(partitions) == PARTITIONS for partitions in samples.values())
    with pytest.raises(ValueError, match="missing Kafka offsets"):
        partition_end_offsets("\n".join(rows[:-1]))


@pytest.mark.parametrize("close_offset", [12, 9])
def test_kafka_offset_brackets_survive_closeout(tmp_path, monkeypatch, close_offset):
    def kafka(*args, **kwargs):
        assert args == ("get-offsets", "--time", "-1")
        assert kwargs == {"timeout": relay.OFFSET_TIMEOUT, "record": False}
        offset = next(end_offsets)
        return SimpleNamespace(
            stdout="\n".join(
                f"{topic}:{part}:{offset}" for topic in TOPICS.values() for part in PARTITIONS
            )
        )

    def bracketed(probe):
        lower = next(times)
        return probe(), {"lower_mono_ns": lower, "upper_mono_ns": lower + 1}

    end_offsets = iter([10, close_offset])
    times = iter([SECOND, 60 * SECOND])
    monkeypatch.setattr(relay, "bracketed", bracketed)
    monkeypatch.setattr(relay.DrainOffsets, "poll", lambda _: None)
    offsets = relay.DrainOffsets(SimpleNamespace(output=tmp_path, kafka=kafka), SECOND)
    offsets.thread.start()
    offsets.capture("during-drain")
    if close_offset == 9:
        with pytest.raises(ValueError, match="end offset regressed"):
            offsets.close(16 * SECOND)
    else:
        summary = offsets.close(16 * SECOND)
        assert summary["samples"] == 2 and summary["closeout_delay_ns"] == 44 * SECOND
    raw = (tmp_path / "drain-kafka-offsets.jsonl").read_text(encoding="utf-8")
    assert [json.loads(line)["phase"] for line in raw.splitlines()] == [
        "during-drain",
        "closeout",
    ]


def test_attribution_retrieves_each_container_log(tmp_path):
    calls = []

    def kube(*args, **_):
        role = args[-1].split("=")[1]
        count = 1 if role == "relay" else 2
        return {
            "items": [
                {"metadata": {"name": f"{role}-{i}", "uid": f"uid-{role}-{i}"}}
                for i in range(count)
            ]
        }

    def kubectl(*args, **_):
        calls.append(args)
        return SimpleNamespace(returncode=0, stdout="logged test-secret", stderr="")

    driver = SimpleNamespace(
        output=tmp_path, kube=kube, k=kubectl, password="test-secret", demand_token=""
    )
    records = relay.retain_logs(driver)
    assert [(rec["pod"], rec["container"]) for rec in records] == [
        ("relay-0", "relay"),
        ("scheduler-0", "scheduler"),
        ("scheduler-1", "scheduler"),
    ]
    assert [call[3:7] for call in calls] == [
        (rec["pod"], "-c", rec["container"], "--timestamps=true") for rec in records
    ]
    assert all(
        (tmp_path / rec["file"]).read_text(encoding="utf-8") == "logged [REDACTED]"
        for rec in records
    )


def test_relay_cpu_reads_cumulative_usage_and_throttling(tmp_path):
    path = tmp_path / "relay-resources.tsv"
    lines = [
        f"{i * SECOND}\tcpu.stat\tok\tusage_usec {i * 100};nr_periods {i * 10};"
        f"nr_throttled {i};throttled_usec {i * 25}"
        for i in range(1, 6)
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
    relay.write(
        tmp_path / "relay-resource-summary.json",
        {"exit": 0, **resource_summary("\n".join(lines), ("cpu.stat",))},
    )
    assert relay.cpu_evidence(tmp_path, 2 * SECOND, 4 * SECOND)["delta"] == {
        "usage_usec": 200,
        "nr_periods": 20,
        "nr_throttled": 2,
        "throttled_usec": 50,
    }
    with pytest.raises(ValueError, match="do not cover"):
        relay.cpu_evidence(tmp_path, 2 * SECOND, 8 * SECOND)


@pytest.mark.parametrize("publication_error", [False, True])
def test_expiry_retains_attribution_and_preserves_drain_failure(
    tmp_path, monkeypatch, publication_error
):
    deadline = 601 * SECOND
    relay.write(tmp_path / "drain-result.json", {"status": "FAIL"})
    relay.write(
        tmp_path / "final.json", {"upper_mono_ns": 650 * SECOND, "upper_unix_ns": 660 * SECOND}
    )
    driver = SimpleNamespace(output=tmp_path, deadline=601)
    target = {"pod": "relay-r", "pod_uid": "relay-uid"}
    monkeypatch.setattr(relay.time, "monotonic_ns", lambda: deadline)
    monkeypatch.setattr(relay, "container_resources", lambda *_: nullcontext())
    monkeypatch.setattr(relay, "container_target", lambda *_: target)
    monkeypatch.setattr(relay, "cpu_evidence", lambda *_: {"delta": {"usage_usec": 10}})
    monkeypatch.setattr(relay, "retain_logs", lambda *_: [{"role": "relay", "exit": 0}])
    monkeypatch.setattr(relay.DrainOffsets, "poll", lambda _: None)

    def finish_offsets(_offsets, end):
        _offsets.thread.join()
        assert end == deadline and driver.deadline == 750
        return {"samples": 41}

    monkeypatch.setattr(relay.DrainOffsets, "close", finish_offsets)

    def publications(_, start, end):
        assert (start, end) == (11 * SECOND, 611 * SECOND)
        if publication_error:
            raise TimeoutError("outbox read timed out")
        return {"total_published_rows": 6000}

    monkeypatch.setattr(relay, "publication_timeline", publications)
    with (
        pytest.raises(DrainExpired, match="600 s"),
        relay.relay_attribution(
            driver, {"origin_ns": SECOND, "stop_ns": 11 * SECOND}, SECOND, 750, target
        ),
    ):
        raise DrainExpired("600 s drain failure", {})
    record = json.loads((tmp_path / "relay-attribution.json").read_text(encoding="utf-8"))
    assert record["observed_through_mono_ns"] == deadline
    assert record["kafka_end_offsets"]["samples"] == 41
    assert record["relay_cpu"]["delta"]["usage_usec"] == 10
    assert record["status"] == ("FAIL" if publication_error else "PASS")
    if publication_error:
        assert record["errors"]["outbox_publications"] == "outbox read timed out"
    else:
        assert record["outbox_publications"]["rows"] == 6000
