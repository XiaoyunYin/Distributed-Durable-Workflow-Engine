# DUR-053 K6a Phase G3a3b write authority and dispatch

R375 splits the Bash and PowerShell `with-k6` pools into three shards each. Command and environment groups stay together for baseline reuse. The aggregate checks that each platform killed every manifest case exactly once. The 30m43s split threshold and the 38m24s ceiling measure job wall time, including setup and teardown.

## Predicates

| Row | Sources | Findings |
| --- | --- | --- |
| I10 AuthorizedAtWrite, proposal and approval (N15) | Sealed submission operation and allowlisted arguments, durable intent, authenticated decision adapter, and decision identity captured before and after the owner’s grant write | `I10_UNAPPROVED_WRITE`, `I10_WRONG_ACTOR`, `I10_GRANT_DECISION_BINDING`, `I10_WRONG_OPERATION`, `I10_WRONG_TARGET`, `I10_WRONG_ARGUMENT`, `I10_WRONG_KEY`, `I10_WRONG_SCOPE`, `I10_WRONG_REVISION`, `I10_WRONG_TOKEN` |
| I10 AuthorizedAtWrite, claim and fence (N15) | Authenticated worker start and send, committed claim audit, attempt creation and subsequent attempt images, sink allocation and matching calls in resource-lock order | `I10_WRITE_CLAIM`, `I10_STALE_FENCE` |
| I10 AuthorizedAtWrite, ordering and expiry (N15) | Owner grant transaction and history, absolute installation expiry, exact read from the engine DB, relay ingress open, sink commit and journal transaction, cancellation history and requests | `I10_GRANT_ORDER`, `I10_CANCELLATION_ORDER`, `I10_EXPIRED_WRITE`, `I10_WRITE_BRACKET`, `I10_WRITE_TRANSACTION` |
| I10 AuthorizedAtWrite, continuity (N16) | Committed grant-token and expiry images and the matching request on lookup or replacement | `I10_GRANT_REPLACED`, `I10_GRANT_CONTINUITY` |
| I10 DispatchAcknowledged (N16) | Every applied journal key, matching final signed receipt, committed `DISPATCHED` image and same-transaction result acknowledgement | `I10_DISPATCH_UNACKNOWLEDGED`, `I10_DISPATCH_RECEIPT`, `I10_DISPATCH_ACK_ORDER`, plus the shared `ReceiptSignature` findings |

An unavailable operand gives typed `required_proof_missing:<source>` on the first affected public row. This includes `sealed_effect_operation`, `exact_grant_read`, `sink_commit_transaction`, `relay_ingress_open`, `committed_decision_identity`, `owner_grant_transaction`, the attempt and recovery joins, and `dispatch_ack_transaction`. Findings preserve the row key and digest when a row is present.

## Write bracket and trust boundary

The engine’s exact-grant recheck supplies its database time and the sink records the typed request used for that read. The relay’s acknowledged ingress open sequence and timestamp bound the start of the write. The sink inserts its call ID and timestamp under the resource lock in the same database transaction as the journal transition. The checker joins the call to that transition and compares its in-transaction time directly with the grant’s absolute expiry. Reply delivery can be withheld past expiry. A commit at or after expiry is still a safety violation. A missing in-transaction timestamp is missing proof.

The checker independently hashes number-preserving Go JSON for the no-LF argument digest, then uses the M4 NUL-delimited proposal and scope formulas. The sealed plan is the expected operation. The strict public projection accepts only the registered business argument shape and rejects nested credentials. Claim and grant tokens appear only as SHA-256 digests. The typed decision stream binds the approved actor and proposal to the owner’s commit. Worker, relay and sink observations bind that authority to the applied request. Model or tool output, logs, runbook text and producer summaries have no authority in these joins.

## Award and replay

I10 requires a positive applied-key population, registered effect coverage, an I8 pass, closed engine capture, and complete, consistent authority, claim, fence, receipt and acknowledgement joins. A violation fails safety and missing proof fails completeness. The fixture matrix runs the effect and authority parity suites with ten concurrent fixtures. Shared PostgreSQL role changes stay serialized, and the 16m00s job target still applies.

| Episode | Ordered findings | Remaining state | Owner |
| --- | --- | --- | --- |
| Exercised effect | Empty | I8 and I10 `covered/pass` | Phase G3a3b |
| Exercised PURE | Empty | I8 and I10 outside the registry ceiling | Registry |
| Idle | Empty | No positive population | Registry |
| All three | Empty | `confirmation_pending:G3a4` | G3a4 |

The deliberate removal control expects one `missing` finding with a null witness at `witness/effect-ingress.jsonl` or `witness/topology.json`. It exits 1. The intact bundles, including idle, exit 3. Invalid invocation exits 2. Exit 0 remains unavailable while confirmation is pending.

G3a4 owns cell exercise and removal, the deployed stale-owner challenge, and both process-skew signs with database deadline proof and registered I12 awards. G3b owns deployed duplicate and lost replies and classic-group redelivery evidence. G4 owns deployment of the fault catalogue and shutdown-watch race measurement. The reset-fence baseline remains a follow-up if a registered workload begins reusing resources across episodes.
