# K4 B1 producer interfaces

This is engine/worker instrumentation for Amendment B.7. It installs no cluster
driver, fault backend, observer, workload, or CI job. All campaign switches and
identity values must be the same in both lifecycle arms. K3 evidence and frozen
b, r and L are unchanged; K4 must retain its new image digests.

## Configuration

`DUR053_K4_CAMPAIGN=1` enables the Go audit and Python campaign witness/traces.
Unset or `0` preserves the existing behavior. Other values fail configuration.
Every runtime writer and worker needs `POD_UID` and `CONTAINER_ID` identifying
the actual container instance. The collector must independently verify these
against Kubernetes; hostname is not accepted as a campaign pod UID. Go creates
one random process session UUID; Python creates one random witness session UUID.
Restarts in the same pod have distinct sessions. B2 must provision the container
identity before application startup; no Kubernetes API lookup is added here.

Workers also require `DUR053_WITNESS_DIR` on the existing shared witness volume
and `RUNTIME_NAMESPACE`. No directory or fallback storage is created. With the
campaign flag off, `DUR053_WITNESS_DIR` still selects the historical K3 witness,
including its historical failure handling and raw internal identity format.

The existing `RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS` hold still works when the
campaign flag is off. With both enabled, each hook logs structured JSON in the
`data` field of `k4 scheduler acknowledgment`: a unique acquisition acknowledgment
UUID, owner UUID, epoch, partition, process session, pod UID, container ID, PID,
UTC time, process-relative monotonic seconds, and hold-enter/hold-exit boundary.
Cancellation runs the exit acknowledgment too. B2 must join enter/exit by that
UUID and independently prove intervention overlap and process death; a log alone
does not prove an intervention. Both runtime lifecycle paths use this same hook.

## Database audit and order

Additive migration `000020_k4_audit.up.sql` installs an immutable `k4_audit` table
containing lease and history events, a staging queue, deferred finalization,
shared ordering sequence, and read-only lease/history views. The final table has
guards against UPDATE, DELETE and TRUNCATE and no cascading foreign keys.
Operational tables and historical migrations are unchanged.

Each successful existing AcquireLease UPDATE queues `acquire`, including
same-owner AcquireLease renewals and expired-owner takeovers. ReleaseLease queues
`release` after its unchanged UPDATE. Queue insertion uses that exact pgx
transaction. RenewLease never changes epoch, so it creates no acquire row.
Rejected acquisitions/releases create no row. Every campaign transition-history
INSERT queues its transition UUID, partition and history timestamp. The existing
successful lockLease boundary sets transaction-local audit owner metadata; this
captures the authorizing owner UUID rather than the scheduler's actor label.
No ownership, epoch, expiry, claim or fencing predicate changes.

Deferred constraint triggers drain staging rows into the immutable table **inside
the same transaction**, before commit. Finalization takes advisory transaction
lock `(530004,1)`, then obtains the next sequence value. The lock remains held
until commit or rollback. A second audited transaction cannot allocate its first
final order until the first ends. Committed transactions therefore have strictly
ordered, disjoint ranges, independent of transaction start order. Deferred events
within a transaction preserve statement/event order, supplying the intra-transaction
tie-breaker. Abort gaps are harmless. The staging sequence, transaction ID,
workflow revision, and wall clock are never used as commit order.

Finalization only inserts/deletes instrumentation rows and takes its own lock;
it does not lock or update business rows. Normal code leaves constraints deferred
until commit. Explicit early `SET CONSTRAINTS` still holds the shared lock through
commit, although callers must not use that to interleave further business work.
Audit persistence failure aborts the containing transaction. Campaign overhead
and serialization must be disclosed; there is no claim that K3 timing is unchanged.

Read-only B2 interfaces:

- `Store.ExportK4LeaseAudit(ctx, databaseOrigin)` emits the exact
  `dur053.k4-lease-audit.v1` object. It uses a single SELECT/MVCC snapshot and also
  works through the existing read-only checker pool. `databaseOrigin` is the DB
  wall-clock value corresponding to the observer's monotonic zero; retain its
  clock brackets. An origin later than any exported row is rejected.
- `SELECT * FROM engine.k4_lease_audit ORDER BY "order"` exposes the raw DB times.
- `engine.k4_history_order` joins `transition_id` to existing
  `engine.transition_history` and uses **the very same event_order sequence**.
- `engine.k4_audit` additionally retains pod UID, container ID and process session
  for independent session mapping. `transaction_id` is the full PostgreSQL xid8.

B2 must verify that every exported history row has ordering metadata and that
all writers had campaign mode enabled. `at` is an in-transaction statement time,
not a separately observed commit instant. Commit comparisons use `order`; elapsed
times retain the observer/DB clock bracket uncertainty. Use one consistent export
snapshot for timeline/history/receipts/leases when collecting during activity.

## Worker seals and traces

Each session owns `SESSION.jsonl`, `SESSION.trace.jsonl`, and
`SESSION.inventory.json`. Invocation JSONL contains exactly start/finish, id,
full namespace/workflow/node/iteration/attempt/token_hash identity, and source-local
monotonic `at`. Token hashing is SHA-256 over the original UTF-8 token string,
without normalization. The raw token never enters the campaign files. Trace rows
carry session/pod/container, UTC and monotonic times, plus explicitly selected
metadata. Bodies, authorization headers, result payloads and arbitrary error
messages are excluded.

Each append checks short writes and fsyncs the stream. It then fsyncs a complete
new inventory, atomically replaces the prior inventory, and fsyncs the parent
directory on Linux. The seal includes lengths, row counts, SHA-256 digests and
acknowledged start IDs. A start returns only after this seal is durable. Any
persistence failure exits the **entire process with status 70**, including failures
in heartbeat, executor, Kafka callback and polling threads; exceptions in those
threads could otherwise be swallowed. Startup configuration errors stop startup.

`workers.k4_witness.read_sealed_session(inventory_path)` is a read-only collector
primitive requiring no live worker. It authenticates the sealed prefixes and
acknowledgment list. An interrupted later append may leave an unacknowledged tail;
the reader returns it separately and never promotes it to acknowledged data.
Pending inventory files and tails must remain in the raw evidence. `complete`
and `errors=0` describe the last durable prefix, not an assertion that later writes
succeeded. B2 must use independent session/death/exit evidence (including exit 70),
inventory every expected session, and fail completeness for missing seals or
unexplained discrepancies. A missing session is never replaced by surviving
workers' inventories. Windows local tests omit directory fsync; deployments use
Linux storage and require live durability validation in B2's engineering smoke.

Both worker paths record assign/revoke callbacks. The pinned kafka-python 2.2.16
has no native lost-listener dispatch. Campaign mode wraps that consumer instance's
existing `reset_generation`, calls the original unchanged, then records `lost`
with the preceding generation/member and assigned partitions when membership was
valid. It also implements a lost listener for an explicit dispatch. Callback rows
carry consumer ID, a UUID per consumer instance, process session, actual broker
group generation, member ID and topic/partition identities. The legacy path uses
the same subscription and polling parameters, registering the observation-only
listener; treatment retains its existing Assignment validity/generation logic.
No additional poll, seek, pause, commit, claim or workload invocation is added.

`disposed` traces retain successful worker-delivery HTTP disposition, event ID,
topic/partition/offset/next-offset and returned task identity **before** offset
commit or an assignment validity return. `result_disposition` retains the returned
durable result disposition plus that delivery's logical attempt identity and
consumer generation. B2 joins registered arrivals through workflow/event identity,
then independently joins persisted inbox/result/history rows. It must distinguish
ACCEPTED from RECORDED_AS_EVIDENCE, cached replies from new receipts, and useful
history after stable assignment from earlier commits. Callback evidence or HTTP
success alone is insufficient for the R225 useful-disposition metric. No sentinel
work is added. Survivors, trigger UIDs, stable-group evidence and censoring are B2
collector responsibilities, not invented by the worker.

Every control HTTP attempt has durable `http_start` and `http` outcome records.
They include operation, logical request UUID, SHA-256 of the exact serialized
body, attempt, status, and error code (or exception type). An interrupted request
has only its pending start; it cannot become a successful/zero-error request.
Retries preserve the UUID, serialized body, and campaign `X-Dur053-Request-ID`
header. Context variables separate concurrent activity and heartbeat requests.

Source clocks are deliberately retained raw. B2 maps them with retained brackets,
normalizes witness JSONL into the episode clock domain, recomputes its published
digest, and retains the original sealed bytes and digests for that transformation.
It assembles the closed Phase A timeline, witness and Kafka schemas, including
independent death, receipt, arrival and stable-assignment joins. These raw producer
interfaces do not assert that a complete lifecycle episode has already been collected.
