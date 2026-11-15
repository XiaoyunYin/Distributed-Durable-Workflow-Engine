"""R316: bound stop observation by the first eligible scheduled tick."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from test_k4_offline import episode_fixture
from test_k4_review import journal_fixture, write_journal

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import dur053_k4_collectors as collectors  # noqa: E402
import dur053_k4_evidence as evidence  # noqa: E402

pytest_plugins = ["test_k4_offline"]


def hosted(root):
    fixture = json.loads((Path(__file__).parent / "fixtures/k4-r316-stop.json").read_bytes())
    write_journal(root, fixture["journal"])
    return fixture, fixture["journal"]


def test_hosted_stop_tick_allows_wake_latency(tmp_path):
    m, rows = hosted(tmp_path)
    assert m["window_end"] == 1794701876.7155557
    assert m["rate"] == 7.136100570066256
    assert rows[0]["plan"]["rows"][0]["scheduled_at"] == "2026-11-15T00:17:56.71550983Z"
    assert rows[0]["plan"]["rows"][1]["scheduled_at"] == "2026-11-15T00:17:56.855642384Z"
    assert rows[-1]["at"] == "2026-11-15T00:17:56.856439532Z"
    assert collectors.utc(rows[-1]["at"]) > m["window_end"] + 1 / m["rate"] + 1e-6
    assert evidence.schedule_stop_timely(tmp_path, m, m["rate"]), (
        "R316 hosted stop rejected despite observation within first stop tick interval"
    )


@pytest.mark.parametrize("at", ["01.000000000", "01.000000001"])
def test_observed_stop_upper_bound(tmp_path, at):
    rows = boundary_journal()
    rows[-1]["at"] = "2026-11-15T00:00:" + at + "Z"
    write_journal(tmp_path, rows)
    assert not evidence.schedule_stop_timely(tmp_path, {"window_end": 1794700800}, 1), (
        "R316 stop observation at or after tick deadline accepted"
    )


def boundary_journal():
    return [
        dict(
            event="plan",
            plan=dict(
                rows=[
                    dict(scheduled_at="2026-11-14T23:59:59.999999999Z"),
                    dict(scheduled_at="2026-11-15T00:00:00Z"),
                    dict(scheduled_at="2026-11-15T00:00:01Z"),
                ]
            ),
        ),
        dict(
            event="stop",
            reason="stop-file",
            stop="2026-11-15T00:00:00Z",
            at="2026-11-15T00:00:00.000000001Z",
        ),
    ]


def test_exact_tick_is_first_eligible_at_nanosecond_precision(tmp_path):
    rows = boundary_journal()
    write_journal(tmp_path, rows)
    assert evidence.schedule_stop_timely(tmp_path, {"window_end": 1794700800}, 1), (
        "R316 exact stop tick skipped or nanosecond predecessor selected"
    )


def test_submicrosecond_tick_selection_and_arrival_comparison(tmp_path):
    rows = [
        dict(
            event="plan",
            plan=dict(
                rows=[
                    dict(scheduled_at="2026-11-15T00:00:00.000000499Z"),
                    dict(scheduled_at="2026-11-15T00:00:00.000000501Z"),
                ]
            ),
        ),
        dict(event="arrival", scheduled="2026-11-15T00:00:00.000000499Z"),
        dict(
            event="stop",
            reason="stop-file",
            stop="2026-11-15T00:00:00.000000500Z",
            at="2026-11-15T00:00:00.000000501Z",
        ),
    ]
    write_journal(tmp_path, rows)
    assert evidence.schedule_stop_timely(tmp_path, {"window_end": 1794700800.0000005}, 1)
    rows[-1]["at"] = "2026-11-15T00:00:00.000000500Z"
    write_journal(tmp_path, rows)
    assert not evidence.schedule_stop_timely(tmp_path, {"window_end": 1794700800.0000005}, 1)


def test_stop_cannot_use_a_later_tick(tmp_path):
    rows = boundary_journal()
    rows[-1]["at"] = "2026-11-15T00:00:01.000000001Z"
    write_journal(tmp_path, rows)
    assert not evidence.schedule_stop_timely(tmp_path, {"window_end": 1794700800}, 1)


@pytest.mark.parametrize("scheduled", ["00", "01"])
def test_arrival_at_or_after_stop_tick_fails(tmp_path, scheduled):
    rows = boundary_journal()
    rows.insert(1, dict(event="arrival", scheduled=f"2026-11-15T00:00:{scheduled}Z"))
    write_journal(tmp_path, rows)
    assert not evidence.schedule_stop_timely(tmp_path, {"window_end": 1794700800}, 1)


@pytest.mark.parametrize("change", ["missing", "duplicate", "reason", "before", "wrong-window"])
def test_stop_identity_and_tick_bounds(tmp_path, change):
    rows = boundary_journal()
    m = {"window_end": 1794700800}
    if change == "missing":
        rows.pop()
    elif change == "duplicate":
        rows.append(dict(rows[-1]))
    elif change == "reason":
        rows[-1]["reason"] = "exhausted"
    elif change == "before":
        rows[-1]["at"] = "2026-11-14T23:59:59.999999999Z"
    else:
        m["window_end"] -= 2
    write_journal(tmp_path, rows)
    assert not evidence.schedule_stop_timely(tmp_path, m, 1)


def test_exhausted_plan_has_distinct_issue(tmp_path, freeze, checker):
    m, docs = episode_fixture(tmp_path, freeze)
    rows = journal_fixture(tmp_path, m, docs)
    rows[0]["plan"]["rows"].pop()
    write_journal(tmp_path, rows)
    assert evidence.schedule_stop_timely(tmp_path, m, m["rate"]) is None
    issues = evidence.episode(tmp_path, freeze, checker)["issues"]
    assert "k4-arrival-stop-unscheduled" in issues
    assert "k4-arrival-stop-late" not in issues
