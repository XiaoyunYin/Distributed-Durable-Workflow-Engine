"""Worker entry point installed only in the K6 campaign image."""

import os
from threading import Thread

from workers.__main__ import run

from dur053_k6_campaign.worker import Adapter, Deployment

if __name__ == "__main__":
    if os.getenv("WORKER_DRAIN_MODE", "drain") != "drain":
        raise SystemExit("K6_HOOK_WORKER_MODE")
    if os.getenv("DUR053_K6_CHAOS_SMOKE") == "1":
        from dur053_k6_campaign.chaos_smoke import SmokeControl, activity_gate

        if os.getenv("DUR053_K6_CLOCK") == "1":
            from dur053_k6_campaign.clock import server

            responder = server("worker")
            Thread(target=responder.serve_forever, daemon=True).start()
        run(deployment_hook=activity_gate, control_factory=SmokeControl)
    elif os.getenv("DUR053_K6_DEPLOYED_HOOKS") == "1":
        run(deployment_hook=Deployment().observe)
    else:
        run(Adapter.factory())
