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
is required; the number of arrivals must be even so each family receives half.
The family order is deterministically shuffled by `seed`.

```sh
./bin/dur050-loadgen \
  -config /var/tmp/dur050-pilot.json \
  -rate 0.25 \
  -count 100 \
  -output /var/tmp/dur050-pilot-submissions.csv
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

For the protocol's five-minute generator-only calibration, start the separate
loopback acknowledgement process in one shell:

```sh
./bin/dur050-sink -addr 127.0.0.1:8787
```

Use a copy of the frozen config with `api_url` set to `http://127.0.0.1:8787`,
then run from a second shell (the observer must remain off):

```sh
./bin/dur050-loadgen \
  -config /var/tmp/dur050-sink-check.json \
  -rate 64 \
  -duration 5m \
  -request-timeout 2s \
  -output /var/tmp/dur050-sink-requests.csv
```

Run the load generator under `pidstat -I -h -r -u -p <loadgen-pid> 1` and
retain its timestamped samples beside the request artifacts.

The sink refuses non-loopback binds and acknowledges request IDs without a
database, broker, or workflow mutation. Preserve the request CSV, summary JSON,
and timestamped `pidstat` output together as calibration evidence; do not
combine these sink measurements with application capacity results.
