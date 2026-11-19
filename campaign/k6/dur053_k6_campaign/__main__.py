"""Worker entry point installed only in the K6 campaign image."""

import os

from workers.__main__ import run

from dur053_k6_campaign.worker import Adapter, Deployment

if __name__ == "__main__":
    if os.getenv("WORKER_DRAIN_MODE", "drain") != "drain":
        raise SystemExit("K6_HOOK_WORKER_MODE")
    if os.getenv("DUR053_K6_DEPLOYED_HOOKS") == "1":
        run(deployment_hook=Deployment().observe)
    else:
        run(Adapter.factory())
