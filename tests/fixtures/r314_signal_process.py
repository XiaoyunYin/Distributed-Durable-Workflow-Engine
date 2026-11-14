"""Real entrypoint with Kafka replaced by contending, sealed witness writers."""

import faulthandler
import inspect
import json
import os
import random
import signal
import sys
import threading
import time
from pathlib import Path

import workers.__main__ as process
import workers.k4_witness as sealing
import workers.kafka_worker as kafka
from workers.witness import campaign_witness

faulthandler.dump_traceback_later(2)
mode = sys.argv[1]
if source := os.getenv("R314_OLD_SOURCE"):
    exec(compile(Path(source).read_bytes(), source, "exec"), process.__dict__)
os.environ.update(
    WORKER_ADDR="127.0.0.1:0",
    KAFKA_BOOTSTRAP_SERVERS="stub",
    WORKER_DRAIN_MODE=os.getenv("R314_MODE", "drain"),
    WORKER_DRAIN_SECONDS="1",
    DUR053_K4_CAMPAIGN="1",
    RUNTIME_NAMESPACE="r314",
    POD_UID="r314-pod",
    CONTAINER_ID="r314-pod",
)

original_encoded = sealing.encoded


def encoded(row):
    if (
        mode == "nested"
        and row.get("event") == "signal"
        and threading.current_thread() is threading.main_thread()
    ):
        # The first handler owns CampaignWitness.lock, before its first write.
        # In the fix this executes on the coordinator, never in the handler.
        signal.raise_signal(signal.SIGTERM)
    return original_encoded(row)


sealing.encoded = encoded
wait_line = inspect.getsourcelines(threading.Event.wait)[1] + next(
    i
    for i, line in enumerate(inspect.getsourcelines(threading.Event.wait)[0])
    if "signaled = self._flag" in line
)


def start(*args):
    lifecycle = args[-1]
    witness = campaign_witness()
    assert witness is not None
    threads = []
    for slot in range(3):

        def write(slot=slot):
            while not lifecycle.drain.is_set():
                witness.trace("result_disposition", {"slot": slot})
                time.sleep(0.002)

        thread = threading.Thread(target=write, daemon=True)
        thread.start()
        threads.append(thread)

    if mode == "condition":
        fired = False

        def at_wait(frame, event, arg):
            nonlocal fired
            if (
                not fired
                and event == "line"
                and frame.f_code is threading.Event.wait.__code__
                and frame.f_locals.get("self") is lifecycle.drain
                and frame.f_lineno == wait_line
            ):
                fired = True
                signal.raise_signal(signal.SIGTERM)
            return at_wait

        sys.settrace(at_wait)
    else:
        # Exercise a held witness invocation and concurrent result traces.
        hold = Path(os.environ["DUR053_WITNESS_DIR"]) / "hold"
        hold.touch()
        os.environ["DUR053_K4_HOLD_FILE"] = str(hold)
        held = threading.Thread(
            target=lambda: witness.begin(
                dict(
                    workflow_id="wf", node_id="n", iteration=0, attempt_number=1, claim_token="test"
                )
            ),
            daemon=True,
        )
        held.start()
        time.sleep(random.Random(314).uniform(0.005, 0.02))
        signal.raise_signal(signal.SIGTERM)
        hold.unlink()
    return threads


kafka.start_pool = start
process.run()
sys.settrace(None)
faulthandler.cancel_dump_traceback_later()
witness = campaign_witness()
rows = [json.loads(line) for line in witness.trace_path.read_text().splitlines()]
signals = [row for row in rows if row["event"] == "signal"]
assert len(signals) == 1
print("SIGNAL_DRAIN_COMPLETE", signals[0]["signal"], flush=True)
