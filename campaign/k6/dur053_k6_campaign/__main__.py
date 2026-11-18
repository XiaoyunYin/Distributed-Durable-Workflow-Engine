"""Worker entry point installed only in the K6 campaign image."""

import os

from workers.__main__ import run

from dur053_k6_campaign.worker import Adapter

if __name__ == "__main__":
    if os.getenv("WORKER_DRAIN_MODE", "drain") != "drain":
        raise SystemExit("K6_HOOK_WORKER_MODE")
    run(Adapter.factory())
