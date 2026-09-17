import json
import sys
from pathlib import Path

from faults.control import FaultController


def _python_target(*extra: str) -> list[str]:
    return [
        sys.executable,
        "-m",
        "faults.target",
        "--seed",
        "23",
        "--boundary",
        "after-effect",
        *extra,
    ]


def _run_release(seed: int, trace_path: Path) -> list[dict[str, object]]:
    controller = FaultController(seed=seed, boundary="after-effect", trace_path=trace_path)
    controller.start()
    reached = controller.wait_for_boundary(timeout_seconds=5)
    assert reached is not None
    assert reached.name == "after-effect"
    assert reached.seed == seed
    controller.pause()
    controller.release()
    assert controller.wait_for_event("released", timeout_seconds=5)
    assert controller.finish() == 0
    return controller.trace()


def _run_kill(seed: int) -> tuple[list[dict[str, object]], dict[str, object]]:
    controller = FaultController(seed=seed, boundary="after-effect")
    controller.start()
    reached = controller.wait_for_boundary(timeout_seconds=5)
    assert reached is not None
    controller.kill()
    assert controller.finish() != 0
    return controller.trace(), reached.fields


def test_release_is_gated_by_named_boundary_and_acknowledged(tmp_path: Path) -> None:
    trace_path = tmp_path / "release.jsonl"
    trace = _run_release(23, trace_path)
    events = [event["event"] for event in trace]
    assert events.index("boundary_reached") < events.index("command")
    commands = [event.get("command") for event in trace if event["event"] == "command"]
    assert commands == ["pause", "release"]
    assert events.index("released") > events.index("command")
    assert events.index("process_exited") > events.index("released")
    records = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
    assert [record["sequence"] for record in records] == list(range(1, len(records) + 1))


def test_kill_crash_repeats_with_same_seed_and_changes_with_new_seed() -> None:
    first_trace, first_fields = _run_kill(23)
    second_trace, second_fields = _run_kill(23)
    third_trace, third_fields = _run_kill(24)

    def normalized(trace: list[dict[str, object]]) -> list[tuple[object, ...]]:
        return [
            (
                event["event"],
                event.get("boundary"),
                event.get("fields"),
                event.get("command"),
            )
            for event in trace
            if event["event"] in {"boundary_reached", "command"}
        ]

    assert normalized(first_trace) == normalized(second_trace)
    assert first_fields == second_fields
    assert first_fields != third_fields


def test_go_process_uses_the_same_failpoint_protocol() -> None:
    controller = FaultController(seed=23, boundary="after-effect")
    controller.start(
        command=["go", "run", "./cmd/fault-fixture", "--seed", "23", "--boundary", "after-effect"]
    )
    reached = controller.wait_for_boundary(timeout_seconds=30)
    assert reached is not None
    assert reached.fields["workflow_id"] == "go-fixture-00000017"
    controller.kill()
    assert controller.finish() != 0


def test_early_exit_is_distinct_from_boundary_timeout() -> None:
    controller = FaultController(seed=23, boundary="after-effect")
    controller.start(command=_python_target("--exit-before-boundary"))
    assert controller.wait_for_boundary(timeout_seconds=5) is None
    assert controller.finish() == 17
    events = [event["event"] for event in controller.trace()]
    assert "process_exited_before_boundary" in events
    assert "boundary_timeout" not in events


def test_non_protocol_stdout_does_not_hide_boundary() -> None:
    controller = FaultController(seed=23, boundary="after-effect")
    controller.start(command=_python_target("--noise"))
    assert controller.wait_for_boundary(timeout_seconds=5) is not None
    controller.release()
    assert controller.wait_for_event("released", timeout_seconds=5)
    assert controller.finish() == 0
    assert any(event["event"] == "target_stdout" for event in controller.trace())


def test_heavy_stderr_is_drained() -> None:
    controller = FaultController(seed=23, boundary="after-effect")
    controller.start(command=_python_target("--stderr-lines", "2000"))
    assert controller.wait_for_boundary(timeout_seconds=5) is not None
    controller.release()
    assert controller.wait_for_event("released", timeout_seconds=5)
    assert controller.finish() == 0
    stderr_events = [event for event in controller.trace() if event["event"] == "target_stderr"]
    assert len(stderr_events) == 2000


def test_timeout_is_recorded_when_boundary_is_not_reported(tmp_path: Path) -> None:
    trace_path = tmp_path / "timeout.jsonl"
    controller = FaultController(seed=23, boundary="after-effect", trace_path=trace_path)
    controller.start(skip_boundary=True)
    assert controller.wait_for_boundary(timeout_seconds=0.1) is None
    controller.kill()
    controller.finish()
    assert any(event["event"] == "boundary_timeout" for event in controller.trace())
    assert trace_path.read_text(encoding="utf-8").count("boundary_timeout") == 1
