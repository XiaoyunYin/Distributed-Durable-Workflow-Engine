from pathlib import Path

from faults.control import FaultController


def _run_release(trace_path: Path) -> list[dict[str, object]]:
    controller = FaultController(seed=23, boundary="after-effect", trace_path=trace_path)
    controller.start()
    reached = controller.wait_for_boundary(timeout_seconds=5)
    assert reached is not None
    assert reached.name == "after-effect"
    assert reached.seed == 23
    controller.pause()
    controller.release()
    assert controller.finish() == 0
    return controller.trace()


def test_release_is_gated_by_named_boundary(tmp_path: Path) -> None:
    trace = _run_release(tmp_path / "release.jsonl")
    assert [event["event"] for event in trace] == [
        "process_started",
        "boundary_reached",
        "command",
        "command",
        "process_exited",
    ]
    assert trace[2]["command"] == "pause"
    assert trace[3]["command"] == "release"


def test_kill_is_gated_by_named_boundary(tmp_path: Path) -> None:
    controller = FaultController(seed=23, boundary="after-effect")
    controller.start()
    assert controller.wait_for_boundary(timeout_seconds=5) is not None
    controller.kill()
    assert controller.finish() != 0
    events = controller.trace()
    assert events[2]["command"] == "kill"


def test_timeout_is_recorded_when_boundary_is_not_reported(tmp_path: Path) -> None:
    trace_path = tmp_path / "timeout.jsonl"
    controller = FaultController(seed=23, boundary="after-effect", trace_path=trace_path)
    controller.start(skip_boundary=True)
    assert controller.wait_for_boundary(timeout_seconds=0.1) is None
    controller.kill()
    controller.finish()
    assert any(event["event"] == "boundary_timeout" for event in controller.trace())
    assert trace_path.read_text(encoding="utf-8").count("boundary_timeout") == 1


def test_same_seed_repeats_same_boundary_trace(tmp_path: Path) -> None:
    first = _run_release(tmp_path / "first.jsonl")
    second = _run_release(tmp_path / "second.jsonl")
    stable_fields = ("event", "seed", "boundary", "fingerprint", "command")

    def normalize(events: list[dict[str, object]]) -> list[tuple[object, ...]]:
        return [tuple(event.get(field) for field in stable_fields) for event in events]

    assert normalize(first) == normalize(second)
