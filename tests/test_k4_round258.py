"""Round 258 regression for stopping arrivals before intervention waits."""

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from dur053_k4_interventions import Interventions


def test_stop_is_written_before_blocking_intervention(tmp_path):
    class Driver:
        def k(self, *args, **kwargs):
            return {"metadata": {"name": "observer"}}

    adapter = Interventions(
        Driver(),
        "ns",
        tmp_path,
        {"a": {"runtime": "a", "worker": "a"}, "a-prime": {"runtime": "b", "worker": "b"}},
        {},
        "source",
    )
    adapter.pods = lambda role: [{"metadata": {"uid": "pod-1", "labels": {"app": role}}}]
    adapter.wait_rollout = lambda role: "1"
    stop_values = []
    adapter.stop_load = lambda intervention: stop_values.append(intervention + 0.1)

    def blocking_set_image(role, lineage):
        assert len(stop_values) == 1, "stop file was not marked before intervention block"
        intervention = json.loads((tmp_path / "confirmation.json").read_bytes())["intervention"]
        window_end = intervention + 0.1
        scheduled = [
            intervention + i / 100 for i in range(20) if intervention + i / 100 < window_end
        ]
        time.sleep(0.12)  # The blocking operation outlasts the measured window.
        assert scheduled
        assert max(scheduled) < window_end
        return "2"

    adapter.set_image = blocking_set_image
    adapter.apply(
        {"event": "worker rollout", "episode": "test-episode"},
        [{"role": "worker", "pod_uid": "pod-1"}],
    )
