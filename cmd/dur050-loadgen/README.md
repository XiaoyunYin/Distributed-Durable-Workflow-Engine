# DUR-050 open-loop load generator

`dur050-loadgen` submits the `seq-8` and `fanout-8` families to the deployed
HTTP API on an arrival schedule that does not wait for prior workflows to
finish. It has a fixed 64-request in-flight limit. If all slots are occupied at
an arrival deadline, it records that arrival as `generator_capacity_missed`
instead of silently shifting it or creating an unbounded client queue.

The campaign config supplies the definitions installed in the target database:

```json
{
  "api_url": "http://runtime.private:8080",
  "namespace": "dur050-example",
  "run_id": "pilot-001",
  "seed": 50050,
  "families": [
    {
      "name": "seq-8",
      "definition_id": "<installed-seq-8-definition-id>",
      "definition_version": 1,
      "initial_node_id": "activity-0",
      "payload": {"profile": "seq-8"},
      "initial_input": {}
    },
    {
      "name": "fanout-8",
      "definition_id": "<installed-fanout-8-definition-id>",
      "definition_version": 1,
      "initial_node_id": "root",
      "payload": {"profile": "fanout-8"},
      "initial_input": {}
    }
  ]
}
```

Run from the Linux load-generator host. Exactly one of `-count` or `-duration`
is required. A fixed `-count` must be even so each family receives half. A
fixed `-duration` may produce an odd cohort; in that case the seed selects which
family receives the single extra arrival, and the shuffled mix differs by at
most one. The family order is deterministically shuffled by `seed`.

```sh
./bin/dur050-loadgen \
  -config /var/tmp/dur050-pilot.json \
  -rate 0.25 \
  -count 100 \
  -output /var/tmp/dur050-pilot-submissions.csv
```

For the pilot's unloaded-latency calibration only, submit one workflow of a
single family, wait for its fine-mode observer to see a terminal state, then
repeat with a new run ID. This mode is not used for the balanced capacity runs:

```sh
./bin/dur050-loadgen \
  -config /var/tmp/dur050-pilot.json \
  -single-family seq-8 \
  -run-id unloaded-seq8-0001 \
  -rate 1 \
  -count 1 \
  -output /var/tmp/unloaded-seq8-0001.csv
```

For an eight-workflow reset warmup, `scripts/dur050-warmup.sh` invokes this
binary with a unique per-run ID and writes the accepted IDs to the path supplied
by `DUR050_WARMUP_WORKFLOW_IDS_FILE`; it expects the frozen config path in
`DUR050_LOADGEN_CONFIG_FILE`.

The command refuses to run unless Linux `CLOCK_MONOTONIC` is available. It
records that shared host clock in every request row so the separate observer
process can use the same boot-time clock; UTC values are provenance only. A
network error, server error, or uncertain success response is retried at most
twice after the first attempt, with the same client-supplied workflow ID,
submission key, and serialized request body. Explicit admission rejections and
other 4xx responses are not retried. If retries cannot resolve an uncertain
outcome, the row is `ambiguous` and the run fails validation.

The immutable CSV contains one row for every scheduled arrival and the sibling
`.summary.json` reports counts, nearest-rank p99 schedule delay, the fixed
in-flight limit, and validity. The CLI exits nonzero for a missed arrival, an
unresolved ambiguous outcome, or p99 scheduled-to-submit delay over 50 ms, but
retains both artifacts so an invalid run remains inspectable. The generator
does not declare workflows terminal; use `dur050-observer` for completion and
latency observations.

For a concurrent one-second batch observation, prebuild the exact scheduled ID
file and start the observer before the load generator. Pass a unique
`-done-file`; create that marker only after the generator exits. The observer
checks and latches the marker before each query. Until a query starts after the
marker was observed, missing IDs remain `SUBMISSION_PENDING`; only a later
query may record them as `NOT_FOUND`. Present workflows are observed through
terminal state. `dur050-run-window.sh` then uses observer mode `reconcile` to
look up every scheduled ID directly with the read-only role and runs a
fail-closed reconciliation: every accepted ID must have exactly one first
terminal observation, and no accepted ID may be `NOT_FOUND`.

For the protocol's five-minute generator-only calibration, run this from the
repository root on the generator host with a unique output directory:

```sh
scripts/dur050-run-sink-check.sh \
  /var/tmp/dur050-pilot.json \
  /var/tmp/dur050-calibration/sink-check-001
```

The helper starts the loopback-only sink, rewrites only the copied config's
`api_url` and run ID, and runs the five-minute, 64/s open-loop test with the
observer off. It retains `generator-requests.csv`, its CPU-enforced summary,
timestamped `pidstat.txt`, logs, and `generator-sink.json`. The JSON status is
`PASS` only for exactly 19,200 accepted requests and a passing internal CPU
series; a failed run retains a `FAIL` artifact. Observer QPS is explicitly not
applicable to this sink-only calibration.

For manual diagnosis, the exact underlying process and config relationship is:

```sh
./bin/dur050-sink -addr 127.0.0.1:8787
# In another shell, use a config copy with api_url=http://127.0.0.1:8787:
./bin/dur050-loadgen \
  -config /var/tmp/dur050-sink-check.json \
  -rate 64 \
  -duration 5m \
  -request-timeout 2s \
  -output /var/tmp/dur050-sink-requests.csv
```

The generator samples its own process CPU once per second and embeds every
interval in the summary JSON. CPU is normalized per logical core to match
`pidstat -I`; a run fails closed if CPU exceeds 80% for more than 1% of the
measurement window, if samples are missing, or if a sampling gap exceeds two
seconds. Run the independent host-side check under
`pidstat -I -h -r -u -p <loadgen-pid> 1` as well and retain its timestamped
samples beside the request artifacts. The internal series is the validity gate;
`pidstat` is a cross-check and records memory/host context.

The sink refuses non-loopback binds and acknowledges request IDs without a
database, broker, or workflow mutation. Preserve the request CSV, summary JSON,
and timestamped `pidstat` output together as calibration evidence; do not
combine these sink measurements with application capacity results.
