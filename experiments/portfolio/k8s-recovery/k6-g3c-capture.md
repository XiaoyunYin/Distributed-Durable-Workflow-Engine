# DUR-053 K6a Phase G3c engine capture

G3c1 installs and exports the engine journal and checks its closure. G3c2 owns the evaluator awards and the exact deployed remainder. Round 451 places independent broker and shutdown collection in G3d.

## Installation and commit order

The Phase G deployment builds the runtime with `dur053_k6`, applies the optional `migrations/k6_capture/000024_k6_capture.up.sql` in the fresh episode database, and sets `DUR053_K6_CAPTURE=1` and `DUR053_K4_CAMPAIGN=1` on the engine writers. The restricted approval adapter receives the two corresponding database flags on its episode connection. The ordinary migration runner reads the top-level migrations, so the capture tables, sequence and write triggers are absent from the default database. Application-side capture is a compiled no-op without the tag.

| Exported relation | Contents |
| --- | --- |
| `engine.k6_write_capture` | Durable row key, operation, transaction, workflow and partition where applicable, and redacted before/after images. Node and attempt changes also carry a digest of the PostgreSQL JSONB image with the claim token replaced by its hash. |
| `engine.k6_owner_write_transactions` | Partition, owner and epoch from the transaction-local lease fence. The journal order and transaction bind the inventory item to its observed write. Lease acquire, renew, takeover and release carry the lease row’s authority. |
| `engine.k6_capture_queue` | Pre-commit capture rows. A nonempty committed queue is a closure violation. |

Row triggers pick the columns in the public projection. Claim and grant tokens become SHA-256 digests before the queue write. Result records hash the token presented to the result transaction and resolve the worker request ID through the authenticated attempt. The cooperating receipt ID is the verified receipt’s `application_id`. Ordinary worker result acknowledgements have no receipt identifier.

The initially deferred queue trigger takes `pg_advisory_xact_lock(530004, 1)` before allocating from `engine.k4_commit_order`. It writes the journal and inventory and deletes the pending row within the business transaction. Neither finalizer locks a business row. Rollback discards the business and evidence rows together, and event order can contain gaps after rollback. History, node, attempt and outbox rows join on the committed transaction and workflow. Outbox rows additionally join on the revision.

The outbox UUID is the durable row key, stable event ID and Kafka record key. The capture also records the topic and digest of the payload PostgreSQL returns to the relay. The journal retains the relay claim, a committed publication-start record before each broker call, and any subsequent publication mark. The relay claim is its own fence. Owner-fenced outbox creation uses the partition epoch. Physical delivery records retain consumer, topic, partition, offset and event ID, including an already-handled duplicate that creates no new inbox row. The same transaction identifies the inbox, quarantine, watermark and initial wakeup writes. Later wakeup and reconciliation changes retain the event or reconciliation reference. The journal also retains unknown attempt outcomes, effect resolution and result dispositions in order.

G2 exports each relation with a row count, canonical JSONL digest and schema census entry. The barrier allows a stable prefix followed solely by ordinary lease renewals. A takeover or other appended write invalidates the cut. The independent decoder uses checker-owned projections under `k6v1`.

## Closure predicates

| Row | Evidence joined | Finding codes |
| --- | --- | --- |
| Deferred queue | Independently exported queue and its expected empty endpoint | `CAPTURE_QUEUE_NOT_DRAINED` |
| Tracked business row | Journal insert/update/delete images and final observed row | `CAPTURE_IMAGE_CHAIN`, `CAPTURE_OPERATION`, `CAPTURE_ROW_MISSING`, `CAPTURE_ENDPOINT`, `CAPTURE_ORPHAN` |
| Journal and K4 audit | Shared order and contiguous transaction groups | `CAPTURE_ORDER_COLLISION`, `CAPTURE_TRANSACTION_ORDER` |
| Scheduler-fenced write | Journal, owner inventory and committed lease authority | `OWNER_INVENTORY_WRITE_MISSING`, `OWNER_INVENTORY_ORPHAN`, `OWNER_INVENTORY_BINDING`, `OWNER_LEASE_AUDIT_BINDING`, `OWNER_INVENTORY_STALE_EPOCH` |
| History, node and attempt write | Journal, committed history audit and transition in the same transaction | `CAPTURE_HISTORY_TRANSACTION`, `CAPTURE_NODE_TRANSACTION`, `CAPTURE_ATTEMPT_TRANSACTION` |
| Outbox creation | Durable event key and history revision in the same transaction | `CAPTURE_OUTBOX_TRANSACTION` |
| State-changing result | Committed history, attempt images, presented token hash, claim request and receipt identity | `CAPTURE_RESULT_MISSING`, `CAPTURE_RESULT_TRANSACTION`, `CAPTURE_RESULT_ATTEMPT_WRITE`, `CAPTURE_RESULT_IDENTITY` |

AtomicHistory still reports an absent committed audit link. The capture closure also checks the transaction of each observed history write. Worker heartbeat, allocation and proof-staging writes can have no history transition. An independent broker or kubelet observation corroborates claims about external behavior. The capture itself is the engine’s own record.

## Runtime cost

`TestK6SourceCaptureParity` publishes the paired PostgreSQL timings at `TestK6SourceCaptureParity/capture-overhead.json` in the verdict artifact. Each class has 20 pairs on one connection, alternating which mode runs first, with K4 audit enabled on both sides. Fenced statements include the inventory insert. The three explicit journal entries use a single round trip with a one-row baseline query. Median paired deltas include the deferred trigger and COMMIT. The test measures single-client overhead for the G4/H disclosure alongside the worker ledger’s approximately 0.5–3.4 ms per append.

## Replay remainder

G3c1 retains the deployed G3a2b replay contract while recording the new evidence. On exercised PURE and effect bundles, G3c2 owns the engine-side missing proofs for `node_attempt_write_transactions`, `result_commit_identity`, `outbox_creation_transactions`, `publication_attempt_identities` and `dispatch_disposition_transactions`. It also owns using captured `owner_write_transactions` in the registered cells, engine-side `offset_disposition_transactions` for hook-3, and N21’s result, uncertainty/resolution and shutdown result bindings.

| G3d evidence | Collector obligation |
| --- | --- |
| `broker_event_copies` (I6 and hook-3 I7) | Read independent records by topic ID, partition and offset. Bind the event ID, key and payload digest. |
| `broker_reset_hold_restart_offsets` (hook-3 I7) | Capture reset, hold and restart offsets for the exact group, keyed by topic ID and partition. |
| Independent shutdown signal or exit (N21) | Capture the roster/kubelet observation with session and invocation IDs, the attempt, request ID and token hash. Order it before the matched abort, then join the engine disposition. |

I6 and hook-3 I7 stay `NOT COVERED` while their independent sources are missing. The idle bundle retains zero findings and exit 3. Confirmation stays `pending` with `confirmation_pending:G3a4`. Exit 0 remains unavailable in production replay.
