# K6 G2a paired PostgreSQL cuts and submission evidence

The [G1 format](k6-g1-checker.md) still describes the engine row encoding and the checker CLI. G2a extends the required v1 source list and the live configuration. Clean engineering evidence exits 3 and keeps I1, I2, I8 (when registered), and I11 at NOT COVERED with `predicate_partial:G2a`. An observed violation of a registered predicate still gives `covered/fail` and episode safety FAIL.

## Live configuration and database identity

Add `plan_file` and `arrivals_file` to the private live configuration. They point to canonical files supplied before acquisition. Add `sink_database_url` with the dedicated `k6_sink_exporter` login for COOPERATING_EFFECT. The value is an empty string for PURE. The manifest adds `engine_instance`, `plan_sha256` and `sink`. The sink value is `null` for PURE or an object with `database` and `instance` for effect episodes. Database names and expected instance IDs come from independent episode provisioning. The administrator sets the shared database comment on each fresh DB to `dur053.k6.v1|<instance>|<reset_id>`. The checker reads the comment, name and OID from PostgreSQL at each snapshot open. Provision instance IDs independently for the two servers and bind them with the reset ID before sending. The outer index binds both descriptors, the barrier, plan, arrivals and all required relation bytes.

The checker uses one engine connection and one sink connection. Each snapshot is read-only REPEATABLE READ. The sink schema exports `resource_states`, `journal`, `receipts`, `calls`, `allocations`, `request_bindings` and `lookup_misses` without selecting by engine keys. The allocation and lookup-miss primary keys are both `(intent_id, grant_hash, attempt)`. The resource/fence allocation constraint is a secondary unique key. Existing sink hash columns are SHA-256 values. Migration 000003 extends the safe resource view with the last intent ID and logical key, grants SELECT on lookup misses, and grants column SELECT on the base resource ID for an independent count. The exporter cannot read the raw resource state.

Both cut transactions record a catalog census. The allowed exported source columns equal the deduplicated pinned projection columns plus declared exclusions. The current exported projections exclude zero columns. The excluded base table `k6_sink.resources` has a pinned column list and its count must agree with the safe view. The census rejects unclassified relations, including foreign tables, and rejects drift in the column set with `COLUMN_UNCLASSIFIED`. A JSONL line exceeding the 1 MiB reader buffer fails with `RELATION_ROW_TOO_LARGE`.

## Paired barrier

Each attempt takes both DB-local pre-watermarks before opening either cut. The engine and sink cuts commit in that order, followed by both post-watermarks. PURE runs the same sequence on the engine alone. Brackets use the common observer's UTC and monotonic nanoseconds. The local snapshot visibility, DB OID, opening clocks, closing clock and commit outcome describe each database independently.

| Watermark class | Relations | Comparison |
| --- | --- | --- |
| Append-only, engine | `schema_migrations`, `attempt_result_evidence`, `transition_history`, `lease_acquisitions`, `k4_audit`, `checkpoints`, `outbox_publications`, `event_inbox`, `transport_quarantine`, both schemas' `effect_call_attempts` and `effect_resolution_audit` | Count, maximum key and rolling SHA-256 over fixed-width hex row hashes in key order |
| Quiescent, engine | `workflow_definitions`, `workflow_executions`, `node_instances`, `retry_policies`, `dur050_admission_slots`, `activity_attempts`, `k4_audit_queue`, `cancellation_requests`, `timers`, `approval_action_intents`, `k6_auth.episode`, `outbox`, `consumer_offsets`, `scheduler_wakeups`, `reconciliation_items`, both schemas' `effect_records`, `effect_resource_fences` and `sandbox_effect_state` | Count and full-content digest in key order |
| Idle churn, engine | `partition_leases` | Stable partition ID, owner ID and epoch. Only `lease_expires_at` and `updated_at` can differ |
| Idle audit suffix, engine | `k6_f3.deadline_writes` when declared | Immutable, monotonically ordered prefix plus only new `renewal` rows. The pinned K4 audit has no renewal kind, so it must remain identical |
| Append-only, sink | `allocations`, `calls`, `journal`, `lookup_misses`, `receipts`, `request_bindings` | Count, maximum key and ordered rolling digest |
| Quiescent, sink | `resource_states` | Count and full safe-view digest, including state commitment, revision, fence, intent and logical key |

Each comparison also uses a rolling digest of SQL-computed source row fingerprints over the same stable columns. SQL hashes token fields before computing those fingerprints. The public cut digest is independently rebuilt from JSONL offline. F3 uses its fully public row hashes and ordered kind inventory. The checker records `CUT_RACED:<db>:<relation>` for a changed relation. A race discards both cuts as candidates and retries with 25 ms back-off capped at 200 ms. `barrier.json` retains attempted generations, watermarks and reasons. A single selected generation binds both cut descriptors to its record. Completed attempts and observer brackets are checked offline.

Deadline is 600 s after the later of the last scheduled arrival and confirmed removal. Controls have no removal. A missing removal uses the sealed hold/job deadline for acquisition and fails completeness. Commit and post-watermark observations must finish by the reconciliation deadline. Offline checks recorded brackets and can run later. External seal timing and quiet closure are required G2b sources.

A write followed by its reversal between observations can escape DB-local watermarks. G2b external quiet sources and commit-ordered audit are required to close this gap. The PURE topology inventory is also a required G2b source. It must independently prove absence of a sink Deployment, Service, DB instance, relay and ingress journal against the rendered arm. G2a fails any indexed or filesystem sink artifact in a registry PURE episode.

## Plan and arrivals

Both sources use canonical v1 JSON with exact keys, UTC strings with nine fractional digits, lowercase SHA-256 hex, explicit nulls and arrays. The manifest commits to the full pre-send plan bytes. The arrival source binds the same episode, reset and plan hash. The checker joins both to all durable workflow submissions and expected definition versions. An uncertain response allows either a durable acceptance or no durable row. An explicit unsent or definitive rejection alone requires absence.

| Object | Fields |
| --- | --- |
| `plan.json` | `schema=dur053.k6-plan.v1`, `episode`, `namespace`, `reset_id`, `sealed_utc`, `fault_mode` (`control` or `injected`), nullable `hold_deadline_utc`, and `windows` |
| Window | `name` (`warmup` then `measured`), distinct `load_run_id`, and nonempty ordered `submissions` with zero-based contiguous sequences |
| Planned submission | `sequence`, `scheduled_utc`, `submission_key`, `body_sha256`, `request`, typed `body`, `target` and `expected`. The scheduled key is `dur053-<load_run_id>-<registry seed>-<six digit sequence>`. The workflow ID equals the key |
| Fingerprinted body | `definition_id`, `definition_version`, `initial_node_id`, `initial_input` and `payload`. The checker independently applies the API's ordered canonical fingerprint and compares `sub-v1:<body_sha256>` with the durable payload hash |
| `arrivals.json` | `schema=dur053.k6-arrivals.v1`, `episode`, `reset_id`, `plan_sha256`, nullable `removal_utc`, `sealed_utc` and `rows` |
| Observed arrival | `window`, `sequence`, `submission_key`, `disposition`, nullable `unsent_reason` and `attempts`. Explicitly unsent rows carry a typed reason and an empty list. Sent rows carry one or more observed attempts |
| Attempt | One-based `attempt`, `sent_utc`, `observed_utc`, `body_sha256`, `request`, nullable `response`, `outcome` (`accepted`, `rejected` or `uncertain`), and nullable `http_status`, `workflow_id` and `payload_hash`. Wire digests carry `length` and `sha256`. Accepted responses include the typed workflow ID and API fingerprint |

The Go load generator in `internal/dur053/load.go` already computes per-run keys and scheduled offsets and records requests, actual sends, response bytes, HTTP status, and unsent/missed outcomes. The Phase C–F harnesses produce bounded smoke-specific records. H must seal both windows and retain the independent plan/index anchor, resolve absolute observer times, normalize typed workload bodies and responses, and hash request/response wire bytes as observed. H must preserve every scheduled row, record each actual retry if enabled, distinguish a definite application rejection from transport or decode uncertainty, and supply independently confirmed removal or a prebound hold deadline. Any effect-specific scheduled request identity outside the current load generator needs the same producer contract. G2a fixtures exercise the schemas and joins with engineering schedules.
