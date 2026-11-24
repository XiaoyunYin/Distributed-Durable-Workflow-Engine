# DUR-053 K6a Phase G3a3 effect closure

R374 publishes the K4 stop timestamp through a synced sibling temporary file and an atomic replacement. The intervention accepts an existing timestamp only when its contents match exactly. The load generator treats a missing file as an absent stop and rejects malformed content, including an empty file.

## Predicates

| Row | Sources | Findings |
| --- | --- | --- |
| I8 OneMutationPerKey, N11 | Sealed plan and durable successful workflows, cooperating attempts, intent operation, journal transitions and receipts keyed by workflow and logical key | `EFFECT_WORKFLOW_WITHOUT_WINNER`, `EFFECT_SUCCESS_WITHOUT_MUTATION_RECEIPT`, `SINK_KEY_MUTATED_TWICE`, `SINK_DUPLICATE_REVISION_RECEIPT` |
| I8 RevisionChain, N12 | Reset endpoint or first journal revision, ordered journal revisions and state hashes, final resource projection, allocations and matched calls in sink call ID order | `SINK_INITIAL_ENDPOINT`, `SINK_REVISION_GAP`, `SINK_REVISION_ENDPOINT`, `SINK_RECEIPT_JOURNAL_BINDING`, `SINK_FENCE_SEQUENCE`, `SINK_APPLIED_STALE_FENCE`, `SINK_FENCE_ENDPOINT`, `required_proof_missing:RevisionChain` |
| I8 reverse closure, N12–N13 | Engine intent and logical key, authenticated worker invocation, sealed ingress, distinct connection for each delivered copy, sink calls, request bindings, allocations and lookup misses | `SINK_ORPHAN_RESOURCE`, `SINK_ORPHAN_JOURNAL`, `SINK_ORPHAN_RECEIPT`, `SINK_ORPHAN_AUTHORITY`, `DELIVERY_WITHOUT_ENGINE_KEY`, `DELIVERY_LOGICAL_KEY`, `INGRESS_UNACKNOWLEDGED`, `INGRESS_ORDINAL_GAP`, `INGRESS_COPY_ORDER`, `INGRESS_CALL_COPY`, `SINK_CALL_WITHOUT_INGRESS`, `DELIVERY_REQUEST_BINDING` |
| I8 application and recovery, N14 | ACKed sink and caller outcomes, matching reply digest or durable nondelivery, same-key and same-grant FOUND/MISSING call, receipt identity and proof operation | `COMMITTED_NONDELIVERY_UNPROVEN`, `LOST_APPLICATION_WITHOUT_LOOKUP`, `UNRESOLVED_APPLY_DELIVERY`, `SINK_REPLACEMENT_WITHOUT_MISS`, `DELIVERY_DUPLICATE_APPLICATION`, `DELIVERY_RECEIPT_IDENTITY`, `DELIVERY_RECEIPT_OUTCOME`, `DELIVERY_ALLOCATION_OUTCOME`, `EFFECT_SUCCESS_WITHOUT_DELIVERY` |
| I8 ReceiptSignature | Manifest receipt key, attempt coordinates and fence, signed application or lookup proof, independent receipt row | `RECEIPT_SIGNATURE_INVALID`, `RECEIPT_SIGNATURE_WRONG_ATTEMPT`, `EFFECT_RECEIPT_MISMATCH`, typed missing receipt proof |
| I10 AuthorizedAtWrite and DispatchAcknowledged, N15–N16 | Future proof-sourced grant read and sink write bracket, sealed effect arguments, authenticated claim and decision, owner-issued grant and acknowledgement history | `predicate_pending:I10`. Existing decision and receipt checks still run. |

The sink assigns call IDs while holding the resource lock. The checker folds new allocations in that order and permits replay of an already issued allocation without advancing the fence. Each applied call must use the most recently issued fence for its resource. An applied call whose reply is lost requires a matching acknowledged lookup of the committed receipt later in sink order. A noncommitting apply with independently recorded nondelivery requires a later caller-observed FOUND or MISSING lookup. A replacement apply requires a delivered MISSING on that attempt and grant before the write. For same-attempt recovery the worker ledger orders the new lookup after the original caller outcome.

I8 awards `covered/pass` in registered effect cells when the plan, engine and independent sink cuts, terminal ingress and decision streams, and authenticated worker sessions support the joins. The population consists of distinct successful cooperating keys. A typed violation fails safety and an evidence gap fails completeness. An intact empty population earns no award. The existing receipt signature verifier and the registry ceiling also apply.

## Trust boundary

The checker uses the sealed plan, typed public database projections, independently authenticated worker entries and decision observations, and the receiver's durable ingress prefix. Relay nondelivery is projected from a separate receiver-acknowledged `worker_drop` event. A worker timeout alone cannot establish loss. Model and tool output, logs, runbook text and producer summaries may diagnose a trial. Only authenticated typed evidence participates in the verdict. Opaque claim and grant tokens remain SHA-256 digests. The existing recursive projection rejects nested secrets and unknown structures at export.

## Deployed replay

The exact finding sequence is empty in the exercised effect, PURE and idle controls. The effect control awards I8 and retains I10 as `predicate_pending:I10`. PURE is outside the registry ceiling for both labels. The deliberate removal control expects exactly one finding in position 1, of kind `missing`, sourced at `witness/effect-ingress.jsonl` for effect or `witness/topology.json` for PURE. The witness is null and has no row key or digest. The idle control has zero findings and exits 3. Exit 0 remains unavailable while confirmation is `pending` with `confirmation_pending:G3a4`.

| Remaining state | Owner |
| --- | --- |
| I10 write authority and dispatch acknowledgement, including N15–N16 | G3a3 continuation under the scope guard. The current bundle lacks the engine read sample and its exact typed request, a bound sink write bracket and an independently sealed proposal with arguments. |
| Cell confirmation and registered I12 awards | G3a4 |

The I10 continuation should publish the exact plan operation, the committed grant and claim identities and engine read sample, and the independent sink's matching request projection and bounded write. It should then verify expiry, cancellation and revision order, grant continuity on replacements, and the durable dispatch acknowledgement against the authenticated receipt. G3a4 can use those verdicts for confirmation. G3b and G4 retain the deployed fault and catalogue work.

The PostgreSQL parity artifact retains `TestK6EffectLiveParity/<case>/public/`, both checker outputs and their exit codes. Its clean and recovered-reply twins isolate the effect joins while retaining the legacy synthetic fixture's I2 and I4 proof gaps. The deployed effect control exercises an intact capture-backed bundle.
