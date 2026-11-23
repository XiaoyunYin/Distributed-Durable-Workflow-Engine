# DUR-053 K6a Phase G3a2 ordering verdicts

The checker evaluates engine commit order from the G2 public relations. The engine audit is finalized by a deferred trigger under a transaction-scoped serialization lock. The checker follows `event_order` and checks that rows for one transaction remain contiguous. Neither statement time nor wall-clock timestamps establish commit order. The verdict package still uses only the standard library.

| Label and falsification row | Predicate and sources | Findings |
| --- | --- | --- |
| I2, N1 and N20 raw edge | **AtomicHistory** reads execution revision/state, full history and reason/actor, finalized audit by transition ID, and persisted node/attempt coordinates. | `HISTORY_REVISION_GAP`, `HISTORY_EDGE`, `HISTORY_FINAL_REVISION`, `HISTORY_REASON_ACTOR`, `HISTORY_AUDIT_UNMATCHED`, `HISTORY_AUDIT_TRANSACTION_ORDER`, `HISTORY_AUDIT_BINDING`, `HISTORY_NODE_MISSING`, `HISTORY_ATTEMPT_MISSING`. Missing evidence is `required_proof_missing:engine/engine.k4_audit.jsonl` or `required_proof_missing:node_attempt_write_transactions`. |
| I11, N1 | **UniqueTerminal** joins the offered population, execution endpoint and ordered history. | `TERMINAL_FINAL_UNIQUE` if revision 1,001 disappeared while the execution remained at 1,001. The same execution witnesses `HISTORY_FINAL_REVISION`. |
| I3, N5 safety | **OwnerAtCommit** folds acquisitions, releases and owner-authorized history in finalized audit order, checking acquisition coverage against exported lease acquisitions. | `OWNER_ACQUISITION_ORDER`, `OWNER_NOT_CURRENT_AT_COMMIT`, `required_proof_missing:engine/engine.k4_audit.jsonl`, `required_proof_missing:owner_write_transactions`. **DeployedStaleChallenge** belongs to G3a4. |
| I4, N6 and N22 order | **CurrentClaimWinner** follows committed authenticated claims, attempt creation/replacement, cancellation and state-changing result history, then checks the completed iteration against the winning result. | `CLAIM_NOT_CURRENT_AT_COMMIT`, `CLAIM_PARTITION_BINDING`, `CLAIM_ATTEMPT_BINDING`, `RESULT_AFTER_CANCELLATION`, `RESULT_SUPERSEDED_AT_COMMIT`, `RESULT_CLAIM_ACTOR`, `RESULT_ATTEMPT_IDENTITY`, `RESULT_WITHOUT_OUTCOME`, `ITERATION_MULTIPLE_WINNERS`, `ITERATION_WINNER_RESULT`, and missing-proof reasons for audit, `result_commit_identity` and `CurrentClaimWinner`. |
| I8, Round 447 ruling 1 | **RevisionChain** reverse-joins the fixture-only reset inventory in the manifest to the exported resources. | `SINK_RESET_RESOURCE_MISSING`, with the manifest digest and missing resource ID. An absent resource relation follows the source-missing path. |

The winner check never uses final current flags. Retryable attempts remain distinct from success and final failure. An audited consumption that exhausts retries may legitimately leave a failed node. Evidence-only results do not advance the winner state. A stale result rejected before commit supplies no state-changing history row. With both the old attempt and replacement in the audit, a result naming the old attempt is a safety violation even if the final workflow state is terminal and the old attempt is flagged current.

## Confirmation and missing collections

Each episode emits `confirmation.status=pending` and `confirmation.reason=confirmation_pending:G3a4`. A clean PURE result with I1, I2, I4, I6 and I11 all `covered/pass` returns exit 3 while that status is pending. The pass branch requires confirmed cell evidence, intact relations, every registered label passing and no failed check. A violation or required-proof gap still returns exit 1. The confirmation gate is independently exercised in an all-covered PURE result fixture.

- `node_attempt_write_transactions` is the commit-linked before/after inventory of node and attempt writes. G2 exports the endpoint rows and the transition’s transaction identity. A matching endpoint cannot prove which transaction performed the change.
- `owner_write_transactions` is the commit inventory of owner-authorized mutations that can occur without a workflow history row. The epoch order of audited owner writes remains checkable.
- `result_commit_identity` is the independently authenticated result request and token digest bound to its state-changing commit and receipt. G2 has the authenticated claim and the committed result’s workflow/node/iteration/attempt and actor. It lacks the result request’s presented token and receipt identity.
- The deployed PURE and effect engineering smokes expect exactly the I2 and I4 proof gaps above, intact evidence, and exit 1. Their idle control expects no findings and exit 3. The missing-source trial retains these findings and adds exactly the removed source finding.

Hosted evidence is under `dur053-k6-g3a2-verdicts-<run>-<attempt>`. The PostgreSQL fixtures retain public bundles, live and offline stdout and exit codes. The offline process runs in a separate network namespace. N1 and N20 are under `TestK6EngineExport/`. N5, N6 and N22 are under `TestK6SourceOrderingVerdictParity/`. The unmatched reset resource is under `TestK6SinkVerdictParity/ResetResourceMissing/`.

## Remaining work

- Engine-side follow-up under the slice limit covers DispatchDisposition and LowerOffsetsDisposed (N7/N8) and the three out-of-ceiling N21 predicates. G2 broker frames provide topic identities and offset endpoints but lack physical records keyed by topic ID, partition and offset, publication retry copies and per-copy dispositions. N8 also needs the reset baseline and hold/restart brackets bound to the exact group. N21 needs result replay identity, resolution and reapplication order, and a shutdown signal/disposition joined to the attempt.
- G3a3 covers effect DeliveryClosure and I10, including the Round 447 engine-proof-to-sink receipt/journal full join.
- G3a4 covers cell confirmation, DeployedStaleChallenge and I12. G3b owns deployed duplicate and lost-reply episodes, default-build effect replay, and both worker clock signs with R354 and R368.
