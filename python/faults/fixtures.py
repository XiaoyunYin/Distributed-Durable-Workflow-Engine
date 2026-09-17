"""Seeded workflow fixture and fake activity used by DUR-003 tests."""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass

from faults.client import FailpointClient


@dataclass(frozen=True)
class WorkflowFixture:
    seed: int
    workflow_id: str
    activity_input: tuple[int, ...]
    expected_sum: int

    def failpoint_fields(self) -> dict[str, object]:
        input_text = ",".join(str(value) for value in self.activity_input)
        input_hash = hashlib.sha256(input_text.encode("ascii")).hexdigest()[:16]
        return {
            "workflow_id": self.workflow_id,
            "activity_input": list(self.activity_input),
            "expected_sum": self.expected_sum,
            "input_hash": input_hash,
        }


def fixture_for(seed: int) -> WorkflowFixture:
    generator = random.Random(seed)
    activity_input = tuple(generator.randrange(10, 100) for _ in range(4))
    return WorkflowFixture(
        seed=seed,
        workflow_id=f"fixture-{seed:08x}",
        activity_input=activity_input,
        expected_sum=sum(activity_input),
    )


def fake_activity(fixture: WorkflowFixture, failpoint: str) -> int:
    """Hit a named boundary before returning a deterministic pure result."""
    client = FailpointClient.from_environment()
    if client is not None:
        try:
            client.hit(failpoint, fixture.failpoint_fields())
            client.emit("released", failpoint, fixture.failpoint_fields())
        finally:
            client.close()
    return fixture.expected_sum
