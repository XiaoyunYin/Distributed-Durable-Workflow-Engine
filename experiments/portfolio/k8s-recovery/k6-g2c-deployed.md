# K6 G2c deployed collection

`DUR-053 Phase G kind smoke` runs alongside Phase F on pull requests and
runs again on the main push. It uses the stock kind harness with a runner
scratch mount. The fixed 60-minute job has a 48-minute acceptance ceiling.
All runs are engineering evidence with `measurement_claim=false` and
`charged_seconds=0`.

The smoke selects the lowest registered window reservation in each workload
class and breaks ties by ordinal. It uses hook-0-0 (PURE) and medium-12-0
(COOPERATING_EFFECT) for ordinary, two-submission workloads. The PURE arm
restarts one worker container in place before submissions. The effect arm
deploys the separate PostgreSQL sink, TLS ingress relay, receiver, approval
adapter and direct allocator. Fault-specific confirmation remains G3 work.

The collector opens the apiserver list/watch before worker creation. The host
intake checks a projected `durable-worker-control` token with TokenReview
and binds the response to the pod UID, pod name and ServiceAccount UID. An
apiserver container status carries both `containerID` and `startedAt`. The
host supplies those two values and the restart count through a nonce-specific,
read-only mount, then checks the reported session against the observation
and a second status read. The collector registers only observed runtimes.
The Go roles use pod UID audit tags as in Phase F. The G worker inventory
uses the apiserver container identity described above.
Classic-group generation comes from worker coordinator callbacks and is
marked producer-side. Broker metadata, group descriptions and positions are
collected separately. R354 corroboration belongs to G3.

The artifact root has `source.json` with source and checkout SHA and tree.
Each `smoke.json` reports elapsed monotonic time for successful stages.
Each `episodes/<id>/` holds the job intake, reset and fence records, raw
producer and receiver journals, and `bundle/` with the checker's inventory,
DB cuts and external quiet samples. Canonical checker stdout is captured
for live and network-isolated offline runs. `missing-source/` is an edited
copy with its original producer PASS and one required external file removed
from the bundle and its index. The replay exits 1 with completeness FAIL
and exactly one `required_proof_missing:<path>` finding. Its exit code and
verdict are retained in `missing.exit.json` and `checker-missing-source.json`.
With all sources present, both checker modes exit 3 with a partial G2b verdict.

The reset controller scales workers, the outbox publisher, and any effect
relay and allocator to zero. It closes the decision adapter and verifies the
pods have disappeared before publishing the reset-bound writer fence.

## G2c2 idle and retake evidence

`episodes/hook-0-0-idle/` is a fresh deployment of the registered PURE cell
with its own namespace, reset, barrier and broker. Both sealed plan windows
carry empty submission arrays. `bundle/arrivals.json` has zero arrivals.
`idle-sources.json` records zero broker traffic throughout the sampled positions
and the indexed zero-byte traffic relations with their snapshot descriptors.
The worker registrations, roster, topology, fence and broker positions use
the same acquisition path as the ordinary PURE episode. The live and
network-isolated offline checker outputs and exit files sit alongside `bundle/`.

`episodes/medium-12-0/` retains its two ordinary effect submissions and exercises
both post-fence paths. A separate short-lived Kafka producer writes one marker
to partition 0 of `durable-agent.events.v1` after quiet has started and a second
after both DB snapshots commit and before the generation's post-cut. The
checker already treats the events end offset as activity. The harness drains
the normal writers first so each marker has an unambiguous offset and no
workflow delivery. The separate checker build pauses only at the observed
boundary. The harness releases it when the independently polled collector
frame contains the new offset. The checker reads the collector frame normally.
`inject-before-pre.json` and `inject-mid-cut.json` retain broker acknowledgments,
assigned offsets, observer boundaries and the observing frame. The checker
restarts quiet after the first write, records `CUT_RACED:external` after the
second, and takes both snapshots in a fresh generation. `quiet-generations.json`
summarizes both windows, pre/post broker and DB activity digests, local snapshot
digests, the original deadline and the selected generation. Its inputs are
`bundle/barrier.json` and `bundle/samples/external.json`.

## Worker ledger cost

`worker-ledger-timings.json` retains individual worker monotonic measurements.
`worker-ledger-cost.json` reports per-episode count, nearest-rank p50, p99 and
max, ledger file bytes and the append breakdown by row kind. Every persisted
append is matched to its ledger sequence. The one-second and terminal checkpoint
measurements are reported separately. The hard-restarted PURE process can lose
its last timing batch if killed while publishing it. Phase G requires its
startup append and at least one checkpoint measurement before the restart.

Each measured rewrite includes row assembly, file write, file fsync, replace
and directory fsync, after acquiring the ledger lock. The worker queues the
timing in memory and publishes it from the checkpoint thread after releasing
the lock. The synchronous `start` append is between claim and payload entry.
PURE `finish` is before result submission. Effect `send` and `receive` are
synchronous in the exchange before result submission. Effect `finish` follows
the result RPC and still gates return to the worker loop. The periodic
checkpoint shares the ledger lock and can delay a concurrent activity-path
append. The PURE duration measures the payload callback. The effect duration
includes the pre-call delay, effect exchange and result RPC.
