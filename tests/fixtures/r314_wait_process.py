"""Exercise the actual main wait, with directed or published signal receipt."""

import faulthandler
import json
import os
import signal
import sys
import threading
import time

import workers.__main__ as process
import workers.kafka_worker as kafka
from workers.k4_witness import read_sealed_session
from workers.lifecycle import WorkerLifecycle
from workers.witness import campaign_witness

mode, drain_mode = sys.argv[1:]
os.environ.update(
    WORKER_ADDR="127.0.0.1:0",
    KAFKA_BOOTSTRAP_SERVERS="stub",
    WORKER_DRAIN_MODE=drain_mode,
    WORKER_DRAIN_SECONDS="2",
    DUR053_K4_CAMPAIGN="1",
    RUNTIME_NAMESPACE="r314",
    POD_UID="r314-wait",
    CONTAINER_ID="r314-wait",
)
faulthandler.dump_traceback_later(3)
waiting = threading.Event()
finished = threading.Event()
handlers = {}
receipts = []
begins = []
original_thread = threading.Thread
original_begin = WorkerLifecycle.begin_drain

if mode == "published":
    # Isolate the main-thread fallback. The real coordinator remains covered by
    # the pthread case and the lock-boundary cases.
    def thread(*args, **kw):
        if kw.get("name") == "worker-signal-coordinator":
            kw["target"] = lambda: None
        return original_thread(*args, **kw)

    process.threading.Thread = thread
    process.signal.signal = lambda n, handler: handlers.update({n: handler})


def begin(self):
    witness = campaign_witness()
    sealed = read_sealed_session(witness.seal_path)
    rows = [json.loads(row) for row in sealed["streams"]["trace"]["data"].splitlines()]
    signals = [row for row in rows if row["event"] == "signal"]
    assert len(signals) == 1, "repeated SIGTERM produced duplicate signal rows"
    assert time.monotonic() - receipts[0] < 1, "signal drain exceeded one second"
    begins.append(time.monotonic())
    original_begin(self)
    finished.set()


WorkerLifecycle.begin_drain = begin


def start(*args):
    lifecycle = args[-1]
    target = original_thread(target=lambda: finished.wait(4), daemon=True)
    target.start()

    def send():
        assert waiting.wait(2)
        time.sleep(0.03)  # main is now in the actual Event condition wait
        receipts.append(time.monotonic())
        if mode == "pthread":
            signal.pthread_kill(target.ident, signal.SIGTERM)
        else:
            handlers[signal.SIGTERM](signal.SIGTERM, None)
            handlers[signal.SIGTERM](signal.SIGTERM, None)

    if mode != "repeated":
        original_thread(target=send, daemon=True).start()

    def trace(frame, event, arg):
        if (
            event == "call"
            and frame.f_code is threading.Event.wait.__code__
            and frame.f_locals.get("self") is lifecycle.drain
        ):
            if mode == "repeated" and not receipts:
                receipts.append(time.monotonic())
                signal.raise_signal(signal.SIGTERM)
                signal.raise_signal(signal.SIGTERM)
            waiting.set()
        return trace

    sys.settrace(trace)
    return []


kafka.start_pool = start
process.run()
sys.settrace(None)
assert len(begins) == 1, "repeated SIGTERM began drain more than once"
faulthandler.cancel_dump_traceback_later()
print("BOUNDED_SIGNAL_DRAIN_COMPLETE", flush=True)
