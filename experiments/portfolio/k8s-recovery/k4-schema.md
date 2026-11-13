# K4 offline evidence contract (Phase A)

Registered under Amendment B.7, Round 223, before measurement. These are Phase B
producer contracts; this change installs no instrumentation, driver, fault backend,
workflow routing or smoke. All elapsed times use one unskewed observer monotonic
seconds origin per episode. Process-local monotonic clocks must be mapped into
that domain with retained clock brackets. Database `order` is a persisted total
commit ordering across lease transactions and all workflows in a partition, with
an intra-transaction sequence where necessary. Wall clock timestamps, polling a
NULL owner, and workflow revision numbers alone cannot supply that ordering.

`scripts/dur053_k4_schema.py::SCHEMAS` is the executable closed field registry.
Every declared field is required, including empty arrays and nullable fields.
Unknown fields, missing fields, wrong primitive types, booleans used as numbers,
nonfinite times, duplicate JSON keys, and malformed lowercase hashes are rejected.
Time values are nonnegative. Null means unavailable, never zero. `SHA256` means
64 lowercase hex characters; Git SHA means 40. Schema revisions require a new
version. The complete field reference follows below.

Each original episode directory is its canonical registry identity (unit plus lifecycle
arm). A replacement appends `-replacement-1` and links through `replacement_for`;
its explicit `arm` and seed must equal the original registry entry. The registry has 140 core episodes and 21 coverage episodes. The first
70 ordinals are global core pair ordinals; both arms share seed `530001+i`.
Coverage units continue the ordinals 70..90, with one treatment episode each.
Registry list order is execution order, including alternating arm order. Artifact
paths never contain raw claim tokens. Identity is namespace/workflow/node/iteration;
attempt identity adds attempt number and SHA256 of the token. The same hash
transformation is used in every export and its independently checked source.

An episode contains these seven source files plus `episode.json` and derived
`metrics.json`:

| Schema | File | Producer obligation |
| --- | --- | --- |
| `dur053.k4-episode.v1` | episode.json | Frozen identity, provenance, windows, every source-file digest, quiet/reconciliation evidence and disposition. |
| `dur053.k4-confirmation.v1` | confirmation.json | Exact affected process identities, independent overlap and intervention completion/removal. |
| `dur053.k4-timeline.v1` | timeline.json | All process sessions, durable claims, witnessed invocations, ordered history, receipts, workflow terminals, control HTTP requests and witness inventories. |
| `dur053.k4-lease-audit.v1` | leases.json | Additive acquire/release audit written in the existing transaction; owner/session, partition, epoch, transaction and commit ordering. No decision changes. |
| `dur053.k4-kafka.v1` | kafka.json | Both legacy and treatment callback paths: change, generation, consumer/session, survivors, assignment, trigger UID and existing-stream sentinel disposition. |
| `dur053.k4-arrivals.v1` | arrivals.json | Absolute schedule, warmup/measured cohort, request hash/identity, send time and every accepted/rejected/uncertain/unsent outcome. Uncertain acceptance is reconciled by durable submission key. |
| `dur053.k4-witness.v1` | witness.json | Every old/new UID/container session, exact hashed-token JSONL and independent acknowledged-start inventory, including dead sessions. Must equal the timeline inventory. |
| `dur053.k4-observers.v1` | observers.json | Persistent watch samples with actual gaps/errors, claim identities joined to pod UIDs, readiness, eligibility, replica/slot counts and authoritative oldest claimable age. |

The job-level `manifest.json` uses `dur053.k4-job.v1`. Its digest inventory includes
all source and derived files and raw Actions run/job/commit responses. Intake
checks run/job/head/tree, freeze hash, registered scope, assignment, whole-pair
prefix, unchanged pending suffix, and charge. `summary.json` uses
`dur053.k4-summary.v1`. `metrics.json` uses `dur053.k4-metrics.v1`. Stored derived
objects must equal complete recomputation: missing and extra fields both fail.
The checker report is `dur053.k4-checker.v1` and includes bounded intervals and
separate claimed-unexecuted, canceled, rejected, evidence-only and committed strata.

## Confirmation and lifecycle evidence

All targets are application pods; harness pods are excluded. API means only
Deployment/api. Overlap records contain a per-role boundary acknowledgment tied
to the exact session: worker durable claim plus active witnessed invocation;
scheduler per-acquisition owner/epoch/partition; API active request; relay active
publication. `at..end` must enclose intervention. `workflows` names affected
registered work for request/publication boundaries. Stale samples or missed overlap
make an episode unexercised. Absence of evidence cannot confirm an intervention.

Rollout changes image A to A-prime. Both digests share source/build and binary
identity, differing only in the registered OCI lineage label. Keep both revisions
and observed completion status. Rollback completes A to A-prime before warmup,
then undoes to the recorded A revision. Fixed sleep and annotation-only restart
are insufficient. Targets retain deletion, TERM/exit and independent replacement
readiness. Scale-down records selected UIDs without deletion-cost steering.

SIGKILL uses Chaos Mesh 2.7.2 container-kill, exact container identity and injection
status, ID/restart change or UID deletion, observed independent death and replacement
progress. Keep exit 137/signal 9 where available; null means runtime did not report
it. A TERM exit cannot be relabeled KILL. Never target the kind node. Engineering
failure to validate the backend requires a Claude ruling before substitution.

Eviction is the B.7 single-node drill without cordon. Inventory every engine role,
submit requests simultaneously for replicated targets, retain every response,
disruptionsAllowed before/after, 429 denial and bounded retries. Every replicated
target must be evicted/replaced. Every zero-disruption role receives exactly one
denied request, with no bypass or forced deletion. The registration does not test
node maintenance/failure or movement to another node. It does not lower any PDB.

Independent collectors inventory every process session, including restarts under
the same pod UID. Witness starts must be durably acknowledged before workload
logic; persistence failure aborts. Inventory is not synthesized from surviving
pods. Witness JSONL rows have exactly `event` (start/finish), `id`, full hashed
`identity`, and `at`. An invocation with no finish requires independent death or
abort for its exact session. Its busy interval is [last-confirmed invocation alive
minus start, independent death/abort minus start]; absent upper bound remains
unknown and fails completeness. Configured activity duration is never a duration
observation. Claimed attempts without starts remain unexecuted.

Receipt exports contain one row per persisted receipt, never HTTP retry responses.
Committed receipts must have matching ordered receipt history and attempt/hash.
Supersession/cancellation before commit makes a stale committed receipt; this must
be zero. Typed stale rejections and evidence-only rows remain separate. Fencing
reconstruction joins all workflows by partition against prior committed takeovers
and the same owner/epoch release. A release without intervening re-acquisition
makes subsequent authorized transitions superseded; this count must be zero.
Every authorized owner/epoch needs its own prior acquisition, including an epoch
greater than the newest acquisition; missing acquisition makes the report incomplete. The new mode is invoked as:

```text
dur049-checker -lifecycle timeline.json -lease-audit leases.json
```

It excludes the historical flags; default campaign and fence behavior is unchanged.

## Metrics, tails and accounting

`dur053_k4_evidence.py derive --episode DIR --checker BINARY` derives every
section-10 K4 metric from source exports and the independently rerun checker.
Distributions use shared nearest rank. Each population publishes n, missing and
censored separately. p99 needs n >= 200 within that population; no episodes are
pooled. Long latency and paired p99 differences are NOT ESTIMATED. Latency uses the
unique final-revision terminal timestamp plus the larger enclosing observer/DB
bracket width, with first-observed times retained for polling-error disclosure.

Report abandonment and cancellation denominators; measured original/reexecution
interval bounds; TERM-to-exit; distinct results committed during drain; bounded
readiness-to-endpoint removal; release-based versus TTL partition recovery;
assignment-only and useful-disposition stall components; per-role request errors
and retries; submission outcomes; affected-work recovery and censoring; duplicate
executions; winning receipts and retryable/final-failure strata; stale committed,
rejected and evidence-only counts; accepted latency, throughput, rates and SLO;
busy removal/idle-survivor joins and conservative K7 costs. PURE effects are NOT
COVERED. Fixed-replica scale-up and KEDA controls are NOT APPLICABLE. Supporting
oldest-age, replica/ready-seconds, pod events and slot utilisation retain their
observation limits. Limiting-component attribution remains unresolved without
the required resource/queue traces.

No-fault latency is available only on the explicitly delimited suffix of W after
both observed completion and fault removal, labelled exactly “post-intervention,
no active fault”. Warmup never contributes measured latency. Quiet reconciliation
includes all durable accepted warmup/uncertain work. Any witness activity resets
the 30 s timer, and unavailable witness access fails completeness. Registered T is
a timeout from the later of final arrival and confirmed removal, not a mandatory
wait. K7 uses all five episode-wise conservative excesses, including zeros and
negative values, median > 0, two busy-removal/idle-survivor episodes, complete
matched pairs and safety/completeness. Ambiguity forces NO-GO. Freeze its table
at campaign closeout, before controller implementation.

`dur053_record_k4.py --downloaded DIR --checker BINARY` retains PASS, FAIL,
invalid, unexercised and interrupted dispositions under immutable
`k4-v1/JOB/RUN/`. `--interrupted-job JOB` wraps an incomplete download verbatim;
no partial manifest can authorize a refund. Interrupted/untrustworthy cleanup is
charged the full 18,900 s reservation. Trusted completed cleanup charges actual
elapsed live seconds. Engineering evidence cannot contribute campaign replicates.
B.4 pack/materialize enforces archive/index/member hashes and 45 MB/archive,
90 MB total; the ledger binds manifest and payload-index digests.

Each expected group has its own immutable assignment. Admission reserves both
arms' full bounds plus diagnostics and teardown against actual elapsed time and
remaining ledger budget. An unchanged unstarted suffix goes to k4-N-cM only after
its predecessor is committed. One classified infrastructure replacement may reuse
the logical job name with a distinct run/job ID and `replacement_for` pointing to
the original Actions job ID. Its assignment contains exactly the new linked IDs of
infrastructure-invalid episodes, in original execution order. Each original can
be replaced once; a replacement cannot be replaced. The valid pair partner stays
in the population, and paired summaries expose `replaced` plus both actual IDs.
Replacement admission reserves that episode's full bound plus diagnostics and
teardown. Normal admission still reserves whole pairs. The carry-over suffix is
computed from unstarted units and always refers to the original job, independently
of replacement jobs. Failed/interrupted predecessors may retain a pending suffix;
any validated non-PASS predecessor with a pending suffix authorizes its continuation.

The manifest has no job classification. `dispositions` is an ordered, exact
inventory of attempted episodes (`episodes`), each with `episode`,
`replacement_for` (empty for originals), `classification`, `reason` and `evidence`.
`valid` includes PASS and fault-outcome FAIL and always contributes, even when the
independent checker reports unsafe behavior. `unexercised` needs an episode with
that status, contributes no replicate, and cannot be replaced. Both require empty
reason/evidence. `infrastructure-invalid` needs one of the following reasons and
one or more retained `dur053.k4-harness.v1` incident records. The recorder validates
the incident's episode/reason and the content of its `source` JSON, and all paths
must be relative files inside the hashed artifact inventory:

| Reason | Required content |
| --- | --- |
| `setup-before-warmup` | setup component, source status failure, incident time strictly before the recorded warmup start, matching episode warmup when available; no applied intervention. |
| `harness-oom` | driver/collector component, Kubernetes pod label `k4-component` matching that harness component, container terminated reason OOMKilled. Engine pods do not qualify. |
| `harness-timeout` | driver/collector source component matching the incident, nonempty command, exit code 124. |
| `collection-failure` | collector source, nonempty error list or missing-path list; every listed path belongs to that episode and is actually absent. |
| `runner-loss` | runner source with the same Actions job ID, failure/cancelled/timed_out conclusion and explicit runner-lost or runner-disconnected failure. A job failure alone does not qualify. |

A bare infrastructure label is refused. An applied intervention with a retained
PASS/FAIL, timeout or engine failure evidence cannot be
excluded as infrastructure-invalid, including after relabeling its episode status.
Original bytes, dispositions and all live charges remain immutable. The ledger's
`k4_episodes` binds only the valid contribution IDs; schedule progress resolves
linked IDs to originals and reserves only an unresolved partner's remaining cost.
No K3-specific replay exceptions apply.

`--interrupted-job` requires a separately retained `dur053.k4-progress.v1`
`progress.json` with the logical job and exact started unit prefix. This durable
admission journal distinguishes lost attempted evidence from an unstarted suffix;
a missing journal is refused rather than guessed. The wrapper retains the entire
download verbatim, rederives complete episode exports (keeping PASS and fault FAIL
contributions), and records collection-failure incidents for lost attempted
episodes. Unstarted units remain pending, never replacement assignments. Partial
manifests supply no classification or refund authority.

`dur053_k4_evidence.py verify-evidence --checker BINARY` materializes committed
B.4 archives, reruns the checker, rederives and compares every stored metric and
summary, and reconciles registry/ledger/directories in both directions. It reports
all missing identities and retained failed/interrupted jobs; a passing subset
cannot close K4. Actual charges replace completed work's expected cost; remaining
units retain group boundaries, without counting finished units twice.

## Complete closed field reference

The following generated type reference mirrors the executable SCHEMAS registry.
Every object listed is closed. `T?` permits explicit null, `[T]` is an array,
and enum alternatives are exact strings.

### IDENTITY

| Required field | Type |
| --- | --- |
| `namespace` | `string` |
| `workflow` | `string` |
| `node` | `string` |
| `iteration` | `integer` |
| `attempt` | `integer` |
| `token_hash` | `sha256` |

### MEMBER

| Required field | Type |
| --- | --- |
| `at` | `number` |
| `present` | `boolean` |
| `width` | `number` |

### SESSION

| Required field | Type |
| --- | --- |
| `session` | `string` |
| `pod_uid` | `string` |
| `container_id` | `string` |
| `role` | `string` |
| `start` | `number` |
| `last_alive` | `number` |
| `death` | `number?` |
| `death_evidence` | `string` |
| `exit_code` | `integer?` |
| `exit_signal` | `integer?` |
| `signals` | `[{ at: number, signal: integer, evidence: string }]` |
| `readiness` | `[MEMBER]` |
| `endpoints` | `[MEMBER]` |

### INVOCATION

| Required field | Type |
| --- | --- |
| `id` | `string` |
| `identity` | `IDENTITY` |
| `session` | `string` |
| `start` | `number` |
| `finish` | `number?` |
| `last_alive` | `number` |
| `abort` | `number?` |
| `abort_evidence` | `string` |
| `outcome` | `string` |

### CLAIM

| Required field | Type |
| --- | --- |
| `identity` | `IDENTITY` |
| `session` | `string` |
| `order` | `integer` |
| `at` | `number` |

### HISTORY

| Required field | Type |
| --- | --- |
| `identity` | `IDENTITY` |
| `order` | `integer` |
| `at` | `number` |
| `kind` | `'cancel' / 'claim' / 'receipt' / 'supersede' / 'terminal' / 'transition'` |
| `receipt` | `string` |
| `partition` | `integer` |
| `owner` | `string` |
| `epoch` | `integer` |
| `revision` | `integer` |

### RECEIPT

| Required field | Type |
| --- | --- |
| `id` | `string` |
| `identity` | `IDENTITY` |
| `session` | `string` |
| `order` | `integer` |
| `at` | `number` |
| `category` | `'committed' / 'evidence_only' / 'rejected'` |
| `outcome` | `'failure' / 'retryable' / 'success'` |
| `rejection` | `string` |

### WORKFLOW

| Required field | Type |
| --- | --- |
| `workflow` | `string` |
| `key` | `string` |
| `state` | `string` |
| `terminal` | `number?` |
| `clock_width` | `number` |
| `first_observed` | `number?` |

### HTTP

| Required field | Type |
| --- | --- |
| `role` | `string` |
| `request` | `string` |
| `body_hash` | `sha256` |
| `attempt` | `integer` |
| `operation` | `string` |
| `at` | `number` |
| `status` | `integer` |
| `error` | `string` |

### WITNESS

| Required field | Type |
| --- | --- |
| `session` | `string` |
| `pod_uid` | `string` |
| `container_id` | `string` |
| `sha256` | `sha256` |
| `data` | `string` |
| `acknowledged` | `[string]` |
| `complete` | `boolean` |
| `errors` | `integer` |

### LEASE

| Required field | Type |
| --- | --- |
| `order` | `integer` |
| `at` | `number` |
| `kind` | `'acquire' / 'release'` |
| `partition` | `integer` |
| `owner` | `string` |
| `epoch` | `integer` |
| `session` | `string` |
| `transaction` | `string` |

### CALLBACK

| Required field | Type |
| --- | --- |
| `change` | `string` |
| `event` | `'assigned' / 'disposed' / 'rebalance_start' / 'revoke' / 'stable'` |
| `at` | `number` |
| `consumer` | `string` |
| `session` | `string` |
| `generation` | `integer` |
| `partitions` | `[integer]` |
| `survivors` | `[string]` |
| `trigger_uid` | `string` |
| `arrival` | `string` |
| `disposition_order` | `integer?` |

### ARRIVAL

| Required field | Type |
| --- | --- |
| `id` | `string` |
| `key` | `string` |
| `workflow` | `string` |
| `phase` | `'measured' / 'warmup'` |
| `scheduled` | `number` |
| `sent` | `number?` |
| `outcome` | `'accepted' / 'rejected' / 'uncertain' / 'unsent'` |
| `status` | `integer` |
| `error` | `string` |
| `request_hash` | `sha256` |
| `attempts` | `integer` |

### SAMPLE

| Required field | Type |
| --- | --- |
| `at` | `number` |
| `source` | `string` |
| `width` | `number` |
| `gap` | `number` |
| `error` | `string` |
| `oldest_claimable` | `number?` |
| `active_claims` | `[{ identity: IDENTITY, pod_uid: string }]` |
| `pods` | `[{ pod_uid: string, role: string, ready: boolean, eligible: boolean }]` |
| `replicas` | `integer` |
| `busy_slots` | `integer` |
| `total_slots` | `integer` |

### OVERLAP

| Required field | Type |
| --- | --- |
| `role` | `'api' / 'relay' / 'scheduler' / 'worker'` |
| `session` | `string` |
| `pod_uid` | `string` |
| `at` | `number` |
| `end` | `number` |
| `acknowledgment` | `string` |
| `request` | `string` |
| `publication` | `string` |
| `workflows` | `[string]` |
| `claim` | `IDENTITY?` |
| `partition` | `integer?` |
| `owner` | `string` |
| `epoch` | `integer?` |

### EVICTION

| Required field | Type |
| --- | --- |
| `pod_uid` | `string` |
| `role` | `string` |
| `replicas` | `integer` |
| `requested` | `number` |
| `scheduled` | `number` |
| `responded` | `number` |
| `status` | `integer` |
| `allowed_before` | `integer` |
| `allowed_after` | `integer` |
| `denial` | `string` |
| `replaced_by` | `string` |
| `replacement_ready` | `number?` |

### TARGET

| Required field | Type |
| --- | --- |
| `session` | `string` |
| `pod_uid` | `string` |
| `container_id` | `string` |
| `deleted` | `number?` |
| `replacement_uid` | `string` |
| `replacement_ready` | `number?` |
| `restart_before` | `integer` |
| `restart_after` | `integer` |

### CONFIRMATION

| Required field | Type |
| --- | --- |
| `schema` | `'dur053.k4-confirmation.v1'` |
| `episode` | `string` |
| `kind` | `'eviction' / 'kill' / 'rollback' / 'rollout' / 'scale-down'` |
| `target` | `string` |
| `intervention` | `number` |
| `completed` | `number?` |
| `removed` | `number?` |
| `overlap` | `[OVERLAP]` |
| `targets` | `[TARGET]` |
| `image_a` | `string` |
| `image_a_prime` | `string` |
| `binary_identity` | `sha256` |
| `binary_proof` | `{ runtime_a: sha256, runtime_a_prime: sha256, worker_a: sha256, worker_a_prime: sha256 }` |
| `source_commit` | `sha1` |
| `build` | `string` |
| `lineage_a` | `string` |
| `lineage_a_prime` | `string` |
| `revision_a` | `string` |
| `revision_a_prime` | `string` |
| `rollout_status` | `string` |
| `setup_completed` | `number?` |
| `chaos_version` | `string` |
| `chaos_kind` | `string` |
| `chaos_object` | `string` |
| `injection_status` | `string` |
| `evictions` | `[EVICTION]` |
| `eviction_retry_interval` | `number` |
| `roles` | `[{ role: string, replicas: integer, disruptions_allowed: integer, pod_uids: [string] }]` |
| `no_cordon` | `boolean` |
| `pdb_bypassed` | `boolean` |
| `harness_targeted` | `boolean` |
| `deletion_cost_used` | `boolean` |

### EPISODE

| Required field | Type |
| --- | --- |
| `schema` | `'dur053.k4-episode.v1'` |
| `episode` | `string` |
| `replacement_for` | `string` |
| `arm` | `'baseline' / 'treatment'` |
| `job` | `string` |
| `run_id` | `string` |
| `job_id` | `string` |
| `head_sha` | `sha1` |
| `tree_sha` | `sha1` |
| `freeze_sha256` | `sha256` |
| `namespace` | `string` |
| `status` | `'FAIL' / 'PASS' / 'interrupted' / 'invalid' / 'unexercised'` |
| `budget_scope` | `'engineering-validation' / 'registered-campaign'` |
| `measurement_claim` | `boolean` |
| `seed` | `integer` |
| `rate` | `number` |
| `L` | `number` |
| `W` | `number` |
| `warmup_start` | `number` |
| `intervention` | `number` |
| `window_end` | `number` |
| `reconciliation_end` | `number` |
| `timeout` | `boolean` |
| `witness_available` | `boolean` |
| `witness_activity` | `[number]` |
| `quiet_since` | `number?` |
| `clock_brackets` | `[{ start: number, end: number, width: number }]` |
| `baseline_flags` | `[string]` |
| `images` | `{ runtime: string, worker: string }` |
| `files` | `{ timeline: sha256, leases: sha256, confirmation: sha256, arrivals: sha256, kafka: sha256, witness: sha256, observers: sha256 }` |

### DISPOSITION

| Required field | Type |
| --- | --- |
| `episode` | `string` |
| `replacement_for` | `string` |
| `classification` | `'infrastructure-invalid' / 'unexercised' / 'valid'` |
| `reason` | `'' / 'collection-failure' / 'harness-oom' / 'harness-timeout' / 'runner-loss' / 'setup-before-warmup'` |
| `evidence` | `[string]` |

### JOB

| Required field | Type |
| --- | --- |
| `schema` | `'dur053.k4-job.v1'` |
| `job` | `string` |
| `run_id` | `string` |
| `job_id` | `string` |
| `head_sha` | `sha1` |
| `merge_ref_sha` | `sha1` |
| `tree_sha` | `sha1` |
| `freeze_sha256` | `sha256` |
| `budget_scope` | `'engineering-validation' / 'registered-campaign'` |
| `measurement_claim` | `boolean` |
| `status` | `'CARRY_OVER' / 'FAIL' / 'PASS' / 'interrupted' / 'invalid' / 'unexercised'` |
| `registered_live_seconds` | `integer` |
| `charged_seconds` | `integer` |
| `elapsed_live_seconds` | `integer?` |
| `live_start` | `number?` |
| `live_end` | `number?` |
| `cleanup` | `'PASS' / 'UNTRUSTWORTHY'` |
| `episodes` | `[string]` |
| `assigned` | `[string]` |
| `pending` | `[string]` |
| `replacement_for` | `string` |
| `dispositions` | `[DISPOSITION]` |
| `artifact_sha256` | `hashmap` |

### dur053.k4-progress.v1

| Required field | Type |
| --- | --- |
| `schema` | `'dur053.k4-progress.v1'` |
| `job` | `string` |
| `started` | `[string]` |

### dur053.k4-harness.v1

| Required field | Type |
| --- | --- |
| `schema` | `'dur053.k4-harness.v1'` |
| `episode` | `string` |
| `reason` | `'collection-failure' / 'harness-oom' / 'harness-timeout' / 'runner-loss' / 'setup-before-warmup'` |
| `component` | `'collector' / 'driver' / 'runner' / 'setup'` |
| `at` | `number` |
| `warmup_start` | `number?` |
| `source` | `string` |

### dur053.k4-episode.v1

| Required field | Type |
| --- | --- |
| `schema` | `'dur053.k4-episode.v1'` |
| `episode` | `string` |
| `replacement_for` | `string` |
| `arm` | `'baseline' / 'treatment'` |
| `job` | `string` |
| `run_id` | `string` |
| `job_id` | `string` |
| `head_sha` | `sha1` |
| `tree_sha` | `sha1` |
| `freeze_sha256` | `sha256` |
| `namespace` | `string` |
| `status` | `'FAIL' / 'PASS' / 'interrupted' / 'invalid' / 'unexercised'` |
| `budget_scope` | `'engineering-validation' / 'registered-campaign'` |
| `measurement_claim` | `boolean` |
| `seed` | `integer` |
| `rate` | `number` |
| `L` | `number` |
| `W` | `number` |
| `warmup_start` | `number` |
| `intervention` | `number` |
| `window_end` | `number` |
| `reconciliation_end` | `number` |
| `timeout` | `boolean` |
| `witness_available` | `boolean` |
| `witness_activity` | `[number]` |
| `quiet_since` | `number?` |
| `clock_brackets` | `[{ start: number, end: number, width: number }]` |
| `baseline_flags` | `[string]` |
| `images` | `{ runtime: string, worker: string }` |
| `files` | `{ timeline: sha256, leases: sha256, confirmation: sha256, arrivals: sha256, kafka: sha256, witness: sha256, observers: sha256 }` |

### dur053.k4-job.v1

| Required field | Type |
| --- | --- |
| `schema` | `'dur053.k4-job.v1'` |
| `job` | `string` |
| `run_id` | `string` |
| `job_id` | `string` |
| `head_sha` | `sha1` |
| `merge_ref_sha` | `sha1` |
| `tree_sha` | `sha1` |
| `freeze_sha256` | `sha256` |
| `budget_scope` | `'engineering-validation' / 'registered-campaign'` |
| `measurement_claim` | `boolean` |
| `status` | `'CARRY_OVER' / 'FAIL' / 'PASS' / 'interrupted' / 'invalid' / 'unexercised'` |
| `registered_live_seconds` | `integer` |
| `charged_seconds` | `integer` |
| `elapsed_live_seconds` | `integer?` |
| `live_start` | `number?` |
| `live_end` | `number?` |
| `cleanup` | `'PASS' / 'UNTRUSTWORTHY'` |
| `episodes` | `[string]` |
| `assigned` | `[string]` |
| `pending` | `[string]` |
| `replacement_for` | `string` |
| `dispositions` | `[DISPOSITION]` |
| `artifact_sha256` | `hashmap` |

### dur053.k4-confirmation.v1

| Required field | Type |
| --- | --- |
| `schema` | `'dur053.k4-confirmation.v1'` |
| `episode` | `string` |
| `kind` | `'eviction' / 'kill' / 'rollback' / 'rollout' / 'scale-down'` |
| `target` | `string` |
| `intervention` | `number` |
| `completed` | `number?` |
| `removed` | `number?` |
| `overlap` | `[OVERLAP]` |
| `targets` | `[TARGET]` |
| `image_a` | `string` |
| `image_a_prime` | `string` |
| `binary_identity` | `sha256` |
| `binary_proof` | `{ runtime_a: sha256, runtime_a_prime: sha256, worker_a: sha256, worker_a_prime: sha256 }` |
| `source_commit` | `sha1` |
| `build` | `string` |
| `lineage_a` | `string` |
| `lineage_a_prime` | `string` |
| `revision_a` | `string` |
| `revision_a_prime` | `string` |
| `rollout_status` | `string` |
| `setup_completed` | `number?` |
| `chaos_version` | `string` |
| `chaos_kind` | `string` |
| `chaos_object` | `string` |
| `injection_status` | `string` |
| `evictions` | `[EVICTION]` |
| `eviction_retry_interval` | `number` |
| `roles` | `[{ role: string, replicas: integer, disruptions_allowed: integer, pod_uids: [string] }]` |
| `no_cordon` | `boolean` |
| `pdb_bypassed` | `boolean` |
| `harness_targeted` | `boolean` |
| `deletion_cost_used` | `boolean` |

### dur053.k4-timeline.v1

| Required field | Type |
| --- | --- |
| `schema` | `'dur053.k4-timeline.v1'` |
| `episode` | `string` |
| `namespace` | `string` |
| `sessions` | `[SESSION]` |
| `claims` | `[CLAIM]` |
| `invocations` | `[INVOCATION]` |
| `history` | `[HISTORY]` |
| `receipts` | `[RECEIPT]` |
| `workflows` | `[WORKFLOW]` |
| `http` | `[HTTP]` |
| `witness` | `[WITNESS]` |

### dur053.k4-lease-audit.v1

| Required field | Type |
| --- | --- |
| `schema` | `'dur053.k4-lease-audit.v1'` |
| `episode` | `string` |
| `rows` | `[LEASE]` |

### dur053.k4-kafka.v1

| Required field | Type |
| --- | --- |
| `schema` | `'dur053.k4-kafka.v1'` |
| `episode` | `string` |
| `rows` | `[CALLBACK]` |

### dur053.k4-arrivals.v1

| Required field | Type |
| --- | --- |
| `schema` | `'dur053.k4-arrivals.v1'` |
| `episode` | `string` |
| `rows` | `[ARRIVAL]` |

### dur053.k4-witness.v1

| Required field | Type |
| --- | --- |
| `schema` | `'dur053.k4-witness.v1'` |
| `episode` | `string` |
| `rows` | `[WITNESS]` |

### dur053.k4-observers.v1

| Required field | Type |
| --- | --- |
| `schema` | `'dur053.k4-observers.v1'` |
| `episode` | `string` |
| `rows` | `[SAMPLE]` |

