# DUR-053 K6a Phase G3a2b verdicts

G3a2b evaluates the evidence exported by G2 and uses the finalized
`engine.k4_audit` order from G3a2. Both checker modes evaluate the same
indexed public bundle. The registry includes I6 in every cell and I7 in
hook-3 only. I5, I9 and I13 stay `NOT COVERED` with
`outside_registry_ceiling`, including when a linked violation fails
episode safety.

| Row | Predicate and sources | Findings |
| --- | --- | --- |
| I6, N7 | **DispatchDisposition** joins the checker’s event contract, history and audit to outbox rows by workflow and revision, then follows event ID through publication marks, inbox and wakeups. The inbox and quarantine positions join the durable consumer watermark. Poison quarantine and outbox records join their reconciliation references in both directions. | `DISPATCH_OUTBOX_MISSING`, `DISPATCH_OUTBOX_UNMATCHED`, `DISPATCH_MULTIPLE_EVENTS`, `DISPATCH_EVENT_TYPE`, `DISPATCH_EVENT_TOPIC`, `DISPATCH_EVENT_PAYLOAD`, `DISPATCH_PUBLICATION_UNMATCHED`, `DISPATCH_PUBLICATION_BINDING`, `DISPATCH_PUBLICATION_MISSING`, `DISPATCH_INBOX_UNMATCHED`, `DISPATCH_OFFSET_MISSING`, `DISPATCH_DISPOSITION_MISSING`, `DISPATCH_WAKEUP_UNMATCHED`, `DISPATCH_WAKEUP_MISSING`, `DISPATCH_QUARANTINE_UNMATCHED`, `DISPATCH_RECONCILIATION_MISSING`, `DISPATCH_RECONCILIATION_UNMATCHED`. |
| I7, N8 | **LowerOffsetsDisposed** compares the worker group's observed committed offset progression by Kafka topic ID and partition to the closing database watermark for the same group, topic and partition. | `OFFSET_BEYOND_DURABLE_DISPOSITION` on the first observed uncovered position. Its witness key includes broker sequence, topic ID, partition and offset. |
| I5, N21 | **LostResultReplay** checks distinct state-changing result history transitions committed against the same durable attempt, in audit order. | `RESULT_REAPPLIED` on the second history transition. |
| I9, N21 | **UnknownEffectVisible** checks committed non-cooperating timeout history and the exact attempt's still-open reconciliation item against a later committed non-cooperating result for the iteration. | `UNRESOLVED_EFFECT_REAPPLIED` on the later state-changing result. |
| I13, N21 | **ShutdownDisposition** uses the roster's corroborated process death, the receiver seal and matched pure invocation start/abort, database claimant/request/token binding, failed attempt and finalized result transition. | `SHUTDOWN_RECORDED_AS_FAILURE` on the result transition with the abort proof linked in I13. An effect-runner abort can also follow a lost reply. |

The N7 removed-row witness names the history transition, since it cannot
name the absent outbox event ID. A moved retry event can be detected when
it creates a second durable event for the same obligation. The existing
outbox and publication rows cannot establish the wire key, the payload
digest at each attempt or whether an unacknowledged attempt produced a copy.
A duplicate delivery can advance a watermark without storing a second inbox
row. The checker needs physical broker records and a commit-ordered
per-copy disposition ledger to identify an extra broker copy or a missing
earlier disposition hidden by a later durable watermark. The I7 endpoint
comparison cannot establish whether the durable watermark caught up after a
successful broker commit.

## Missing collections for G3c

Engine-side collections, reported as `required_proof_missing:<source>` on
the first affected public row:

- `outbox_creation_transactions` binds the outbox row, stable event ID,
  key, payload digest and topic to the creating history transaction.
- `publication_attempt_identities` binds each attempted publication,
  stable event/key/payload/topic, acknowledgement and durable mark to its
  transaction. Include claims whose broker result or mark was lost.
- `dispatch_disposition_transactions` and
  `offset_disposition_transactions` bind inbox, offset, wakeup, quarantine
  and reconciliation writes to the event and physical copy, including
  duplicate delivery advancement. Preserve order relative to offset commits.
- `result_commit_identity` carries the authenticated presented token hash,
  result request ID and receipt ID linked to the changed state and commit.
  G3c also owns the existing `node_attempt_write_transactions` and
  `owner_write_transactions` gaps, including writes without history.
- `effect_uncertainty_resolution_transactions` carries the uncertainty,
  its disposition or resolution, and any reapplication in commit order.
- The engine side of `shutdown_result_binding` carries the result receipt
  and recorded disposition for the exact attempt.

Broker and collector-side collections:

- `broker_event_copies` records topic ID, partition and offset with event
  ID, message key and payload digest from independently read broker bytes.
- `broker_reset_hold_restart_offsets` binds the exact reset, hold and
  restart samples to the group and topic IDs. It dates each advancement
  relative to the engine's committed disposition. G2 retains independent
  observer brackets but lacks these phase and commit bindings.
- The collector side of `shutdown_result_binding` binds the observed signal
  or exit to the worker session and interrupted attempt. An invocation
  abort at a corroborated process death is currently the usable subset.

The exercised deployed PURE and effect replays assert the I2/I4 gaps plus
the four I6 gaps for outbox creation, publication attempts, broker copies
and dispatch disposition transactions. Each remains exit 1 with
completeness FAIL. The idle control has zero findings and exits 3. Every
production verdict still carries `confirmation_pending:G3a4`, so exit 0
is unavailable. Missing evidence uses exit 1, invocation errors use exit 2,
and clean incomplete evidence uses exit 3.

## Verification and remainder

The hosted PostgreSQL artifact
`dur053-k6-g3a2-verdicts-<run>-<attempt>` retains
`TestK6SourceTransportParity/<variant>/{public,live.json,offline.json,offline.stderr,exit.json}`.
Offline replay runs in a new network namespace. The deployed Phase G
artifact retains its positive, missing-source and default-build replays
under `episodes/`. The fixture assertions reconcile the finding digests
to the public row bytes.

G3c adds the campaign-gated engine capture and completes the I3/I4
passing paths. It also scopes the broker and collector inputs above for
I6/I7 and N21. Then G3a3 owns effect delivery closure and I10. G3a4 owns
confirmation, DeployedStaleChallenge and I12. G3b owns deployed effects,
both worker clock signs with R354 and R368, and default-build effect
replay. G4 reruns the full falsification catalogue. Reset-inventory
production remains with H.
