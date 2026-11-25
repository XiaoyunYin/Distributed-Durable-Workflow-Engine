import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

from j2_qualification_evidence import (  # noqa: E402
    extract,
    file_record,
    read,
    scrape_summary,
    seal,
    verdict,
)


def test_freeze_indexes_the_original_trace_bytes(tmp_path):
    (tmp_path / "live-checker.json").write_bytes(
        b'{"status":"PASS","workflow_ids":["id"],"trace":{"Test":{"raw":1.0,"first":true}}}\n'
    )
    extract(tmp_path)
    snapshot = tmp_path / "snapshot.json"
    assert snapshot.read_bytes() == b'{"Test":{"raw":1.0,"first":true}}\n'
    seal(tmp_path, {"status": "PASS"})
    assert read(tmp_path / "freeze.json")["files"]["snapshot.json"] == file_record(snapshot)


def test_verdict_comparison_preserves_the_result_and_witness_decisions():
    live = {
        "status": "PASS",
        "workflow_ids": ["one"],
        "trace": {},
        "campaign": {
            "busy_seconds": 1.23,
            "reexecuted_seconds": 0,
            "valid": True,
            "accepted_results": 1,
        },
    }
    offline = {**live, "workflow_ids": None, "campaign": {**live["campaign"], "busy_seconds": 1.24}}
    assert verdict(live) == verdict(offline)
    offline["campaign"]["accepted_results"] = 0
    assert verdict(live) != verdict(offline)


def test_scrape_gap_uses_fresh_queries_and_required_series():
    def metric(values):
        return [{"values": [[t, str(v)] for t, v in enumerate(values)]}]

    series = {
        "up": metric([1] * 10),
        "j2_query_ok": metric([1] * 10),
        "j2_sample_timestamp_seconds": metric([0] * 8 + [8, 9]),
        "j2_admissions": metric([26678] * 10),
        "j2_terminals": metric([26678] * 10),
        "j2_outstanding": metric([0] * 10),
        "j2_unpublished": metric([0] * 10),
    }
    summary = scrape_summary(series, 0, 10, 26678)
    assert summary["max_scrape_gap_seconds"] == 1
    assert summary["max_query_gap_seconds"] == 8
    assert summary["query_gaps_over_5s"] == 1
    series["j2_terminals"] = metric([21002] * 10)
    with pytest.raises(ValueError, match="full quiet population"):
        scrape_summary(series, 0, 10, 26678)
