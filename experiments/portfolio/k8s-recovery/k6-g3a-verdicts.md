# DUR-053 K6a Phase G3a verdicts

This engineering slice adds independent verdicts for submission identity and terminal history, detects duplicate sink mutations, verifies the observed beginning of the sink revision chain, and checks signed receipts and classic-group corroboration. The registry pins the coverage ceiling for all 115 episode identities. G3a2 owns the remaining catalogue and confirmation rules within the original G3a scope.

## Predicates

| Label and fixture | Predicate | Public evidence | Finding codes |
| --- | --- | --- | --- |
| I1, N2–N3 | SubmissionIdentity | Sealed plan and arrivals, namespace executions, definitions, approval-intent targets when observed | `SUBMISSION_*` for contradictions, `required_proof_missing:<source>` for an unavailable source, and `required_proof_missing:SubmissionIdentity` for an omitted scheduled arrival. A complete nonempty offered population can earn `covered/pass`. |
| I11, N4 | UniqueTerminal | Durable namespace executions joined to the submission population, ordered history, recorded barrier deadline | `TERMINAL_FINAL_UNIQUE` for a contradictory terminal count, revision or state. `required_proof_missing:UniqueTerminal` for an observed pending workflow and `TERMINAL_DEADLINE_CENSORED` when the barrier also records the deadline. An observed pending identity is preserved in the finding. |
| I8, N11 | OneMutationPerKey | Journal revisions and the workflow identity of each joined approval intent | `SINK_KEY_MUTATED_TWICE` for the second and each following mutation under a workflow and logical key, including when the receipt count remains one. |
| I8, N12 and Round 425 | RevisionChain | First journal row or post-reset resource inventory, subsequent journal rows, safe resource state and receipts | `SINK_INITIAL_ENDPOINT` for a first journal row proving a pre-seeded revision or hash without an inventory. `required_proof_missing:RevisionChain` for an unjournaled resource without an inventory entry. Existing gap, endpoint and orphan codes remain. |
| I8 and I10, Round 421 | ReceiptSignature | Reset-provisioned public key in the manifest, attempt node, iteration, number and fence, operation and signed receipt | `RECEIPT_SIGNATURE_INVALID`, `RECEIPT_SIGNATURE_WRONG_ATTEMPT`, `required_proof_missing:ReceiptSignature`, `required_proof_missing:receipt_public_key`. |
| I12, R354 | GroupMembership | Bracketed independent broker descriptions of stable members, client IDs and assignments, plus separate producer callback claims in the same broker record | `GROUP_COORDINATOR_DESCRIPTION_MISMATCH` for inconsistent evidence. `required_proof_missing:GroupMembership` when the worker-clock cell lacks a stable description with coordinator evidence. |

When a selected cut contains no execution identity for an unfinished workflow, the checker retains the barrier and source findings. It cannot name that workflow from a schedule entry alone. An empty engineering control confirms the plumbing and has no positive I1 or I11 population.

The existing sink view publishes a constant `initial_state_hash`. The checker follows the actual journal's `old_revision` and `old_state_hash` or a matching manifest `sink_reset_resources` entry. That optional inventory contains sorted resource IDs with observed revisions and state hashes. A reset operator using this alternative must independently record the inventory and anchor the manifest through the retained job index. No deployed reset-inventory producer is added here.

Classic Kafka DescribeGroups does not return a numeric generation. The checker corroborates the callback claim through member IDs, client IDs and every assigned topic ID and partition, with independent broker descriptions bracketing collection. The callback is the generation source. Stable publication alone cannot establish what happened between samples. Clock-stage and redelivery or typed rejection joins remain open, so I12 stays pending even when every supplied stable description matches. A mismatch outside the ceiling still fails safety and has a witness.

## Receipt v1 signed bytes

The bytes are the ASCII domain `dur053-k6-sink-receipt/v1`, a single NUL, and Go standard JSON with its default escaping and no trailing newline. The outer object has keys `Claim`, `Operation`, `Receipt` in that order. The claim has `NodeID`, `Iteration`, `Attempt`, `Fence`. The operation is `apply` or `lookup`. The receipt has these fields in order.

```
workflow_id, logical_key, resource_id, canonical_hash, scope_hash, intent_id,
expected_revision, applied_revision, application_id
```

The 32-byte manifest public key uses padded standard base64, matching runtime provisioning. The 64-byte signature uses unpadded URL-safe base64. The reset controller generates the key pair, records only the public key in reset evidence and the manifest, and provisions the same key to the verifier. Offline trust relies on the independently retained job index anchoring the manifest. The production signer generates fixed vectors in its own test package. The pure checker reads the committed fixture with its own standard-library decoder and verifier. The vectors cover both operations, Go's default string escaping, wrong key, a changed byte, alternate JSON framing and absent signature.

## Results and replay

| Exit | Result |
| ---: | --- |
| 0 | Reserved for a complete pass with every registered label `covered/pass`. No G3a path reaches 0 because other registered predicates and cell confirmation remain pending. |
| 1 | Evaluated safety or completeness failure, including missing required source/proof, censors and integrity failures. |
| 2 | Invalid checker invocation or live configuration. |
| 3 | Genuinely incomplete verdict with no failing check. |

Labels are emitted in numeric order. An in-ceiling violation yields `covered/fail`. Otherwise missing required proof yields `NOT COVERED` with its source or predicate reason. Pending catalogue work is explicit as `predicate_pending:G3a2`. Out-of-ceiling rows remain `NOT COVERED` with witness links for any observed safety violation.

PostgreSQL fixture artifacts retain public bundle inputs, live and offline JSON and both exit codes under `TestK6SinkVerdictParity/`, `TestK6SourceTerminalVerdictParity/`, and `TestK6QuietLiveParity/Group*/` in `dur053-k6-g3a-verdicts-<run>-<attempt>`. The offline binary runs in a fresh network namespace. SQL fixtures exercise the real read projections and independent paired databases for sink cases. Receipt fixtures deliberately corrupt one field at a time.

## Remaining work

- G3a2 covers N5–N9, N14–N16, N18 and N21, transaction-linked AtomicHistory, the remaining I8 and I10 joins, and full cell confirmations. This includes the ordered owner and claim checks, lower-offset disposition, lost worker result replay, unresolved non-cooperating work, shutdown disposition, lost effect response and same-key lookup, approval/fence/acknowledgement, and signed clock and DB-time proof. Add live and offline twins and named mutations for these rules.
- G3b adds deployed duplicate and lost-reply episodes, both worker-clock signs with bracketed group and redelivery evidence, and default-build replay of the deployed effect bundle. The R354 review remains open pending those episodes.
