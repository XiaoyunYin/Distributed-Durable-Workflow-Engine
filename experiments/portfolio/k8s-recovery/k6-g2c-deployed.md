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
In each PURE episode, including idle, the harness also replays the same
bundle in the isolated network namespace with the default checker build.
It requires exit 3 and byte-for-byte equality with the smoke checker output.
`default-offline.stdout.json` and `default-offline.exit.json` retain the result.

The reset controller scales workers, the outbox publisher, and any effect
relay and allocator to zero. It closes the decision adapter and verifies the
pods have disappeared before publishing the reset-bound writer fence.

## G2c2 idle and retake evidence

`episodes/hook-0-0-idle/` is a fresh deployment of the registered PURE cell
with its own namespace, reset, barrier and broker. Both sealed plan windows
carry empty submission arrays. `bundle/arrivals.json` has zero arrivals.
The plan seal anchors the original reconciliation deadline for the empty schedule.
`idle-sources.json` records zero broker traffic throughout the sampled positions,
the indexed zero-byte traffic relations with their snapshot descriptors, and
the registered worker ledgers with final seals and zero activity traffic.
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

The live checker can select the final attempt as censored evidence when both
cuts complete, staged files are present, quiet is continuous where required,
and `CUT_DEADLINE` is the attempt's only reason. It indexes both snapshots
for diagnosis. Offline verification still reports `CUT_DEADLINE` and fails
the episode. A race, interruption, binding failure or unclassified column
disqualifies the attempt from selection.

The first deployed G2c2 run completed the G job in 10m31s. The before-pre
marker at offset 3 advanced the end to 4 and restarted quiet. The mid-cut
marker at offset 4 advanced the end to 5. Generation 2 closed at 14:58:07.893Z
against the unchanged 15:06:18.546Z deadline. Both databases kept stable activity
digests within and across that retake. The broker digest changed in generation 1
and stayed stable in generation 2. [Hosted evidence, run 45034815638](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45034815638/artifacts/14758993129)

| Episode | Generation | Quiet min(UTC, monotonic) | Largest sample gap | Disposition |
| --- | ---: | ---: | ---: | --- |
| `hook-0-0` | 1 | 30.235 s | 2.017 s | Selected |
| `hook-0-0-idle` | 1 | 30.168 s | 2.012 s | Selected |
| `medium-12-0` | 1 | 30.383 s | 2.029 s | Discarded with `CUT_RACED:external` |
| `medium-12-0` | 2 | 30.402 s | 2.028 s | Selected |

## Worker ledger cost

`worker-ledger-timings.json` retains individual worker monotonic measurements.
`worker-ledger-cost.json` reports per-episode count, nearest-rank p50, p99 and
max, complete ledger file bytes and the append breakdown by row kind. Every persisted
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

Each timing cell lists count / p50 / p99 / max in milliseconds from the same
deployed run. PURE start and finish rewrites had medians of 0.557 ms and
0.371 ms against a 0.0011 ms payload median. Effect send and receive rewrites
had medians of 0.761 ms and 0.741 ms within a 5,027 ms effect-run median.
The synchronous ledger adds latency on both activity paths. [Hosted timing evidence](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45034815638/artifacts/14758993129)

| Episode | Ledger bytes | Appends | 1 s checkpoints | Terminal checkpoints | Activity duration |
| --- | ---: | ---: | ---: | ---: | ---: |
| `hook-0-0` | 5,390 | 7 / 0.588 / 0.931 / 0.931 | 29 / 0.774 / 4.619 / 4.619 | 2 / 0.599 / 0.764 / 0.764 | 2 / 0.0011 / 0.0020 / 0.0020 |
| `hook-0-0-idle` | 3,483 | 3 / 0.687 / 0.707 / 0.707 | 21 / 0.779 / 1.344 / 1.344 | 2 / 0.771 / 0.822 / 0.822 | 0 / — / — / — |
| `medium-12-0` | 7,883 | 10 / 0.714 / 1.640 / 1.640 | 31 / 0.803 / 5.353 / 5.353 | 2 / 0.724 / 1.364 / 1.364 | 2 / 5,027.293 / 5,033.680 / 5,033.680 |
