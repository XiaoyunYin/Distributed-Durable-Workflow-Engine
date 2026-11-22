# K6 G2c1 deployed collection

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
The dedicated idle-pair and deterministic post-fence mid-cut write/retake
demonstrations and worker ledger overhead measurement remain G2c2 work.
