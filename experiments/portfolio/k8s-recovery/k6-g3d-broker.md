# DUR-053 K6a Phase G3d broker and shutdown evidence

## Independent collection

The Phase G observer reads Kafka metadata with topic IDs before and after each group sample. It enumerates the task and event topics and reads the physical records directly from their partition leaders after writer admission is fenced. It retains the exact key bytes, the event ID from the `event-id` header, and SHA-256 of the value bytes. The public copy stream keys records by topic ID, partition and offset. It retains the observed low and end and a completion bit for every engine partition, including empty partitions. The reset provides offset zero. A nonzero low, failed fetch, hole, changed topic identity or a range that fails to match the closing observation leaves a typed proof gap. The probe for the effect episode’s barrier retake uses a separate observed topic.

The collector has no database login. The checker joins its records with the outbox insert’s key and raw payload digest, committed relay starts and marks, and physical delivery identities. The engine capture already checks the associated inbox, quarantine, offset, wakeup and reconciliation transactions. Acknowledged marks require a physical copy. Committed starts give the upper bound on copies for that event. An ambiguous start can account for a zero-copy attempt or a copy without a mark. The evidence does not select which ambiguous start produced a given identical record.

For hook-3, the controller sends an episode-bound `offset_phase` request for each reset, held commit and restart. It supplies the request time and a committed engine-journal prefix sampled before the broker read. The collector records the exact worker group’s committed offsets, including null commits, and actual topic IDs in the corresponding `broker/positions.jsonl` row. It rereads the phase request after sampling and retains the phase on one observation. The checker compares the first engine insert with the broker reset position and requires lower dispositions within the journal prefix observed before releasing the hold. The hosted PostgreSQL hook-3 fixtures supply these phase observations. The deployed Phase G parity smoke exercises the PURE and effect controls. G4 owns deployment of the fault catalogue.

The roster watch retains the kubelet’s exit code, signal and reported finish time alongside the time it observed the exact pod and container runtime. The observer joins an invocation active at that observation to the registered session, attempt, request ID and claim-token hash. The checker repeats the join against the acknowledged ledger start and abort. It uses the roster observation time for ordering. A death seal authenticates the ledger’s prefix. It cannot stand in for a pre-abort exit observation. A shutdown result can affect episode safety while I13 stays outside the registry ceiling.

The public bundle binds the sources and selected quiet cut by canonical bytes. Offline verification uses only those bytes. Transport readers and kubelet access stay outside `k6v1`.

## Predicates and awards

| Row | Sources | Finding codes |
| --- | --- | --- |
| I6 range closure | Copy scan bounds and rows, closing broker positions, observed topic IDs | `required_proof_missing:broker_event_copies`, `required_proof_missing:broker_event_copy_range` |
| I6 publication | Physical copy, outbox insert, starts, acknowledged marks | `BROKER_COPY_MISSING`, `BROKER_COPY_WITHOUT_ATTEMPT`, `BROKER_COPY_KEY_MISMATCH`, `BROKER_COPY_PAYLOAD_MISMATCH` |
| I6 delivery | Physical position and identity, captured delivery and its durable disposition | `BROKER_DELIVERY_WITHOUT_COPY`, plus the [engine capture findings](k6-g3c2-awards.md) |
| I7 baseline | Same group and topic ID across reset, hold and restart, committed journal prefix, first offset insert | `required_proof_missing:broker_reset_hold_restart_offsets`, `OFFSET_BRACKET_TOPIC_CHANGED`, `OFFSET_ENGINE_BASELINE_MISMATCH` |
| I7 lower records | Physical copies and dispositions present in the journal prefix at hold | `OFFSET_BRACKET_UNDISPOSED_COPY`, `CAPTURE_OFFSET_DISPOSITION_GAP`, `OFFSET_BEYOND_DURABLE_DISPOSITION`, typed missing copy or disposition proof |
| N21 shutdown | Roster exit and invocation binding, ledger start/abort, claim identity and captured result disposition | `required_proof_missing:shutdown_signal_or_exit`, `required_proof_missing:shutdown_result_binding`, `SHUTDOWN_RECORDED_AS_FAILURE` |

I6 awards `covered/pass` on a positive dispatch population with complete, consistent physical-copy and engine capture evidence. I7 awards `covered/pass` on hook-3 with an observed hold-to-restart commit advance and complete lower-disposition proof. Each keeps the registry ceiling. A counterexample fails safety. Missing proof fails completeness.

## Exact replay remainder

`replay` pins an empty finding sequence for the exercised PURE, cooperating-effect and idle controls. There is no finding kind, path, first-row witness key or row digest in that sequence. Removing the selected topology or ingress source still yields exactly one `RelationIntegrity/required_proof_missing:<path>` finding of kind `missing`, sourced at that path with a null witness, and exit 1.

| Remaining record | Owner |
| --- | --- |
| I8 and I10 `NOT COVERED` with `predicate_pending:G3a3` in the cooperating-effect control | G3a3 |
| `confirmation_pending:G3a4` in each control and the I12 award in its registered cells | G3a4 |

The clean controls and clean hook-3 fixture exit 3. Exit 0 remains unavailable while confirmation is pending. Invalid invocation remains exit 2.

## R371

The prior traces identify sink PID 300 and engine PID 580, but retained neither client address nor port, and both pod lookups returned null. The authenticated sink and grant-reader sessions in the same samples also had no pod match. The sampler now retains the TCP peer and resolves it to the registered sink pod IP directly or through the node’s original and translated TCP tuples. At most two attributable disconnects per server sampling window can close this way. Every starting backend counts toward the server’s connection budget while observed. Missing, ambiguous and unrelated sources still fail.
