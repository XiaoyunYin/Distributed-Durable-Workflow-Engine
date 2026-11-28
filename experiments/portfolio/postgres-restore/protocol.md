# J7 PostgreSQL restore drill

## Scope and population

Run one complete qualification drill, then three measured drills, `R1`
through `R3`, in order. Each drill starts with fresh inputs and destroys its
own PostgreSQL source volume. Restore the engine on a new volume from a
physical base backup and the full contiguous archived WAL available at loss.
Keep the external acknowledgment ledger and effect sink on separate storage.
This is a forced storage-loss rehearsal on one GitHub-hosted runner with one
kind node. The runner, its local backup storage and its sink survive the
fault. Qualification and measurement are separate jobs and populations.

Each drill uses one sealed workload manifest. Derive stable IDs and
idempotency keys from the campaign, drill and cohort member. Use a fresh
engine cluster, archive, sink database, broker and evidence directory each
time. Pin the graph definitions, activity versions, canonical inputs and
expected outputs. Submit through the ordinary API using the fixture's
authorized client.

| Cohort per drill | Exact count | Required state and outcome |
| --- | ---: | --- |
| Archived marker workflows `M01`–`M12` | 12 | Use `pure.echo@v1`. Acknowledge all before sealing the archive. All twelve must exist immediately after restore and succeed by the terminal deadline. `M01`–`M08` must already be successful at the archive boundary. |
| Unarchived marker workflows `M13`–`M15` | 3 | Acknowledge all on the source after the archive boundary and before the kill. All three, including their related engine rows, must be absent on the restored timeline. |
| Redispatch witnesses `D1`, `D2` | 2 | Archive each with its current first attempt unclaimed and original dispatch durably published. Require both to succeed through verified redispatch on fresh Kafka. |
| Worker fence `F1` and scheduler fence `L1` | 2 | Archive a live first claim for `F1` and a held owner lease on `L1`'s partition. Require both stale denials, both fresh positive controls and successful workflows. |
| Cooperating effect `E1` | 1 | Archive the authorized current first attempt. Apply once at the retained sink after the archive boundary. Recover using that receipt and require successful workflow completion. |
| New pure workflows `N1`–`N4` | 4 | Acknowledge and complete all four on the restored endpoint while recovery work is still being reconciled. |

The denominator per measured drill is 20 source acknowledgments and four
target acknowledgments. Expect 17 surviving source workflows, three known
lost IDs and 21 successful terminal target workflows in total. Qualification
has the same population and is excluded from those counts. Use gates driven
by observed attempt and publication state for the held cohorts. A gate may
delay worker intake or execution. It cannot create a claim, fabricate a
receipt, change a heartbeat deadline, or make the expected outcome true by
editing PostgreSQL rows. Freeze the exact gate placements and fixture digests
before qualification.

## External acknowledgment ledger

Use a host controller on a separate filesystem path from every database
volume, archive and broker. The controller owns an append-only
`j7.ack-ledger.v1` JSONL journal, with canonical UTF-8 rows and a sealed
expected-workload manifest. Use an exclusive writer, monotonically numbered
rows, previous-row hashes and a sealed final digest. Fsync the creation and
directory entry, each send record before sending, and each outcome record
before acting on it. Keep an independent join key for each operation. An
HTTP ack means the validated full 2xx response was observed and recorded,
with the expected identity and successful creation disposition. A timeout or
missing response is ambiguous until independent reconciliation. Neither a
database read nor a broker publication supplies a client acknowledgment.

- The header binds run and attempt, drill, host boot ID, source/target instance
  IDs, source revision, sealed workload, topology and policy digests.
- Each operation records its kind, phase, sequence where applicable, request
  ID, workflow ID, namespace, submission key and request hash. Record scheduled,
  send and response-complete times on the same host monotonic clock, UTC for
  correlation, outcome, response status/hash and observed durable identity.
  Record fsync completion separately. Seal SQL and WAL observations as
  separate linked witness rows. Keep authority, timeline and sampled LSN
  with each SQL witness. A sampled flush LSN is a bound, not a commit-record
  address.
- Record sends without an ack as a separate in-flight or ambiguous set.
  Preserve every send and disposition, including rejected probes. A missing
  expected ack, missing ledger row, invalid chain, or unexplained operation
  fails admission or reconciliation. J7 permits zero ambiguous client
  operations in the measured population. An intentionally rejected stale
  operation must have a positive denial receipt.

The marker is a real workflow submission, carrying its ordinal and drill
identifier in the immutable input and submission identity. No marker table
is needed. Send `M01`–`M15` on a 5 s open-loop schedule across the archive
boundary, with absolute send deviation at most 100 ms. Run with at most one
marker in flight, a 1 s response deadline from send and a 250 ms
ack-to-fsync bound. Missed sends or late acknowledgments invalidate the
drill. Do not shift the schedule for a slow archive or catch up with a burst.
Join every marker in both directions by ordinal, immutable input, submission
key, request hash and workflow ID. Export ordered rows before enabling
writers on the target. Require the exact contiguous prefix `M01`–`M12`
and the exact missing suffix `M13`–`M15`. A later marker behind a hole is a
failure, even if the maximum recovered ordinal looks correct.

### Reuse in J10

The ledger has operation and witness types for submissions, lease acquisition,
claim and result receipts, authority changes, read-only state probes and WAL
observations. Bind each operation to its original or serving instance.
J10 keeps the same ack and ambiguity rules and uses its own registered
cadence and cohorts. It adds primary flush and standby receive, flush and
replay observations with per-sample time brackets. The same independent join
and missing-ack control apply to standby promotion. Retain full partition,
owner and epoch references and bind claims by workflow, node, iteration,
attempt and token fingerprint. Hold raw fixture tokens privately for exact
challenge and audit, and use fingerprints in the public artifact. Record
renewal versus reacquisition, each attempted use, the authority's disposition,
and the before/after state. This preserves the evidence needed to distinguish
replicated leases from a lost takeover, including J10's same-surviving-owner
case. The J7 drill uses a different replacement owner and supplies no result
for that same-owner case. [Lease allocation](../../../internal/state/store.go#L587)

## Archive boundary, outage and restore

1. Apply the sealed migrations and seed the fixtures. Enable WAL archiving
   from cluster initialization with `wal_level=replica`, `archive_mode=on`,
   `archive_timeout=0`, and 16 MiB WAL segments. Verify effective
   `fsync=on`, `synchronous_commit=on` and `full_page_writes=on`.
   Make a full plain-format `pg_basebackup` with a SHA-256 manifest and
   `--wal-method=none` before marker admission. Keep the only required WAL
   on separate archive storage. The archive writer publishes full segments
   atomically, fsyncs the file and directory before success, and rejects a
   mismatched duplicate filename. Validate system ID, version, timeline,
   backup manifest, required WAL and archived segment sizes/hashes with the
   pinned PostgreSQL tools, including `pg_verifybackup` with the archived
   WAL directory. Retain the manifest WAL ranges and verification output.
   After the backup, create two named archive anchors in distinct closed
   segments using a WAL switch after each, before the marker schedule
   starts. Wait for both archive receipts. Require at least three complete
   contiguous required segments through `C`, including an interior segment
   that can be removed for the archive-gap control.
2. Before `M12` completes, witness `D1` and `D2` as current
   `DISPATCHABLE` attempts with zero claims. Bind each durable `PUBLISHED`
   outbox row to an original broker event, topic ID, partition and offset.
   Witness the held `F1` claim and `L1` lease. Require `E1`'s authorization,
   allocation, current first claim and stable effect identity in the source,
   a pre-send hold on its apply, and a zero-revision sink resource with
   zero prior receipts. Take these witnesses after the base backup completes.
   Keep old worker admission for `D1` and `D2` closed through the fault.
3. After the fsynced `M12` ack, write a drill-specific named restore point and
   force one WAL switch. While the marker sender waits for its scheduled next
   slot, independently confirm that the segment containing the restore point
   is complete in the archive. Require proof before the `M13` send and within
   3 s of the fsynced ack. Record the named point LSN, containing segment and
   exclusive segment-end address as boundary `C`. Freeze the expected
   contiguous archive inventory through that segment. For all writes expected
   to survive, retain pre-switch identity and visibility evidence. Do not
   request another switch or copy the open WAL tail into the archive.
4. Release `E1` after `C` is witnessed. Require one committed mutation and
   signed receipt at the sink and accepted proof and consumed successful
   result on the source, all before the fault. Keep the sink running on its
   own DB. Require source ack and source readback for `M13`–`M15`; their
   requests start after `C` is sealed. Obtain independent source visibility
   and a flushed-WAL high-water sample past `C`. The tail must remain inside
   the next open segment. The source-side effect result and the three late
   markers are deliberately committed beyond the available archive.
5. Within 2 s after the fsynced `M15` ack, hard-kill the original postmaster
   container with automatic restart disabled. Require confirmation of exit
   and failed SQL probes from the client path within 2 s of kill request.
   Set `T0` to the first controller timestamp at which both observations
   are available. Retain the
   kill request, last successful SQL probe, first unavailable probe and
   container exit evidence. Require the last-success to first-unavailable
   bracket to be at most 3 s. Freeze old application writers and prevent
   reconnection. Before deletion, verify the source volume against the
   preflight job labels, mount inventory and PostgreSQL's reported data
   directory. Remove the source container and its named engine data volume,
   then prove the volume is absent and no container references it.
   Deletion precedes any target volume creation or restore. After source
   loss, independently enumerate and seal the entire available archive.
   Require the expected ending segment and no later available segment.
   Unexpected WAL, a gap or a partially archived file invalidates the cut.
6. Copy the verified backup into a fresh target volume and replay from the
   sealed archive mounted read-only. Use `recovery.signal`, a restore command
   restricted to verified archive files, and the source timeline. Recover
   through archive end without setting an earlier recovery target. Disable
   streaming, access to the original data or open WAL and any target write
   back into the source archive. Keep regular connections blocked until a
   restricted observer proves the target identity, recovery completion, last
   replay position and new timeline. Compare archive inventory, verified WAL
   record ranges and replay/end-of-archive logs. The expected end is the last
   replayable record through `C`, with switch padding accounted for. A
   missing interior file is an error, even if PostgreSQL accepts connections.
   Read-only snapshots taken before admitting target writers must include the
   marker prefix, the persisted claims, lease, outbox and authorization
   witnesses, and `E1`'s missing engine result alongside its retained sink
   receipt. Require the original PostgreSQL system identifier, a new
   timeline and a distinct target volume and endpoint. Record all those
   identities and the last sampled source timeline.

[PostgreSQL continuous archiving and recovery](https://www.postgresql.org/docs/18/continuous-archiving.html)
[PostgreSQL backup](https://www.postgresql.org/docs/18/app-pgbasebackup.html)
[Backup verification](https://www.postgresql.org/docs/18/app-pgverifybackup.html)
[WAL and recovery configuration](https://www.postgresql.org/docs/18/runtime-config-wal.html)

## Fresh Kafka, fencing and effects

Use **fresh Kafka** in every drill. Retire the old broker and its data
volume. Create fresh topics and offsets after `T0` using a new broker and
storage identity. Witness distinct cluster/topic IDs, zero initial offsets
and empty group state before enabling producers and consumers. Keep the
original broker evidence already captured and disallow any old broker
endpoint, retained offset import, notice copy or replay into the new broker.
A retained-broker arm would need a separate reviewed protocol with durable
poison or rejection evidence for lost-interval messages.

Use a new consumer identity per role on the target, in Kafka and in the
database. The restored inbox and offset keys include consumer ID,
topic and partition, so a fresh broker can reuse offsets that are already
present under the source IDs. Preserve the old rows as evidence. On the
source use the existing `runtime-schedulers-v1` and `runtime-workers-v1`
identities. On the target use `runtime-schedulers-v1-j7-restore` and
`runtime-workers-v1-j7-restore` for both Kafka group and durable consumer ID,
including the worker-control API's server-selected consumer ID. Keep
topic names stable, with eight partitions per topic and replication factor
one on both brokers. Start the target groups at earliest
with automatic commits disabled. Require positive source watermarks for
at least one partition in each group before `C`, absent target IDs in
the pre-write export, and first fresh dispositions at offset zero in each
populated partition with contiguous advancement under the target IDs.
Verify source watermark rows and inbox records are
unchanged by target delivery. Reject any mismatch before opening writers.
[Durable offset keys](../../../migrations/000006_m3_transport.up.sql)
[Scheduler consumer](../../../cmd/runtime/scheduler.go#L58)
[Worker delivery identity](../../../internal/api/delivery.go#L36)

Open the restored endpoint to new runtime identities only after target
inspection and old-endpoint exclusion. Start the repaired relay, normal
scheduler repair scan and worker intake against the restored DB and fresh
broker. For each `D` witness, follow the archived heartbeat deadline, one
`REDISPATCH_UNCLAIMED` transition, a new `attempt.redispatch` outbox row, its
fresh-broker publication, claim of the same attempt number and accepted
result. Require the first post-restore task notice for each witness to be
that redispatch. Zero original notice replays and zero direct seeding of
dispatch, claim or result rows. Both notices are due within 45 s of opening
the scheduler endpoint, with the normal 30 s dispatch lease and 500 ms
scheduler cadence. Count the waiting, publication and claim delays in RTO.
Submit the four new workflows immediately after endpoint admission,
holding `F1` until `N1` is acknowledged so new work overlaps recovery.
[Runtime scan](../../../internal/state/runtime.go#L9)
[Timeout and redispatch](../../../internal/state/store.go#L1690)
[Service cadence](../../../internal/engine/service.go#L53)

- **Scheduler fence.** Prove that the archived owner reference for `L1` was
  live on the source. Wait for expiry according to target DB time, acquire
  ownership using a distinct owner on the restored authority and require a
  higher epoch. Use the same normal fenced repository mutation in the stale
  and fresh probes, with current revision and payload. Require
  `ErrLeaseNotOwned` before any workflow, lease, attempt, history or outbox
  change. The fresh reference must commit the intended transition and the
  workflow must finish after the fresh probe owner releases its lease.
  Record lease and rejected operation authority in the external ledger.
- **Worker fence.** After natural claim expiry and a fenced replacement, hold
  the live `F1` replacement while presenting the original attempt number and
  exact old token through the authenticated worker result path. Require a
  typed stale or noncurrent denial and compare pre/post DB and outbox state
  while the positive control is held. A valid result from the live claim must
  commit and finish. Endpoint unavailability, wrong authentication or a
  missing workflow gives no credit for either negative probe.
  [Lease and result fencing](../../../internal/state/store.go)
- **Orphan receipt.** List `E1` as a sink receipt without a matching restored
  engine result in the pre-write export. Join workflow, node, intent, resource,
  logical key, canonical and scope hashes, fence, application ID and resource
  revision to the external source witnesses. Rebind the sink grant reader to
  the verified restored endpoint with the same approval identity and record
  the switchover. Use the existing signed-receipt and replacement lookup path,
  with the valid original grant and same effect identity. Fix the approval
  window to 20 min from drill start and require completion within it. Keep
  two worker slots and use the existing 60 s worker claim lease with 5 s
  activity heartbeats.
  Require a real replacement claim and a `FOUND` lookup returning the same
  application ID, zero replacement apply calls and one accepted restored
  result consumption. The independently exported mutation journal must have
  exactly the original mutation, one receipt and a resource revision of one
  with unchanged post-apply state hash through the final observation.
  Compare source and restored results separately and list any unresolved
  effect instead of reconstructing its state from logs.
  [Sink deduplication](../../../internal/effects/k6sink/store.go)
  [Read-only sink export](../../../internal/effects/k6sink/export.go)
  [Replacement lookup](../../../python/workers/k6_effect.py)

## Clocks, cutoffs and independent verdict

Capture all phase instants and observation brackets on one host monotonic
clock. Retain the UTC mapping and bounded SQL samples, with the controller's
send and receive bracketing the DB clock. A DB clock sample used to prove a
lease or grant deadline must have at most 100 ms round-trip uncertainty. Use
the DB's own time for lease and claim expiry checks. Record `T0`, source volume
absence, first target replay and verified replay end, fresh broker readiness,
endpoint open, first successful fresh durable write, the redispatch milestones,
the last required terminal commit, final observation and checker exit. Do not
subtract waiting, new volume or broker creation, startup or checker time.

| Gate | Bound measured from confirmed outage `T0` |
| --- | ---: |
| Source volume proven absent | 30 s |
| Isolated target recovered and initial read-only snapshot complete | 180 s |
| Restored application endpoint open and ready | 300 s |
| First new accepted write and both redispatch notices published | 360 s |
| Every surviving and new workflow terminal | 480 s |
| Quiet observation and independent checker pass | 600 s |

After the last required terminal commit, observe at least 30 s with no
unexpected target workflow transitions, task or result publications, sink
applications or unresolved reconciliation. Require no pending J7 workflow,
unpublished J7 outbox event or unread J7 notice. Report the bound for each
observation and any fresh workload still incomplete at cutoff. RTO is
the monotonic interval from `T0` to the passing independent checker verdict
after this observation. Record phase splits and capped or incomplete outcomes
separately. A database-ready timestamp has a separate phase column.

For RPO let `n` be the last source-acknowledged marker and `k` the last
marker in the verified recovered prefix. Report the prefix shortfall `n-k`,
the exact missing IDs, and the difference `A_n - A_k` between the client
response-complete instants. If no marker survives, report the empty prefix
and missing identities with an uncaptured time bound. If `k = n`, both
marker losses are zero.
With send instants `S` and response-complete instants `A`, bracket the
corresponding commit-time difference by
`[max(0, S_n - A_k), max(0, A_n - S_k)]`. Report the actual send and response
jitter and that bracket. Under the registered deadlines the commit-time
bracket width is at most 2 s for two distinct markers. The cadence resolves
the boundary only between adjacent markers, whose commit instants can be
up to 6.2 s apart. Expected loss is three markers and the acknowledged
time loss bound is 16.2 s. Confirm both separately. Also report archive-end
LSN, last source flush LSN and the difference in bytes on the source
timeline. Record engine DB bytes at backup, just before the kill, and in
the initial and final target exports. Report cluster and WAL bytes, backup
size, archive bytes, sink size and each unresolved or ambiguous identity
for every drill.

Use the read-only namespace export and existing invariant checker for the
recovered and new work. A separate J7 verifier joins the full pre-write and
terminal exports to the sealed acknowledgment journal, archive manifest,
outage and endpoint witnesses, original and fresh broker observations, and
sink export. Join in both directions across all scheduled IDs and roles.
Keep the invariant check intact and classify the three directly witnessed
losses against the archive boundary. Fail on any additional missing,
duplicate or unexplained identity, unexpected post-cut row, stale mutation,
or unaccounted sink receipt. A successful drill has zero unresolved outcomes.
[Engine snapshot](../../../internal/invariants/snapshot.go#L18)
[Invariant checker](../../../internal/invariants/checker.go#L189)

Qualification uses the complete drill and four deliberate rejection
controls. Run the controls against copies, with a healthy sibling replay.

| Control | Required rejection |
| --- | --- |
| Remove pre-cut `M07` as a whole identity from an otherwise consistent export, rebuilding the snapshot envelope with the original expected ledger intact | The identity join rejects the gap as `PRE_CUT_MARKER_MISSING`. A checksum-only rejection gives no credit. |
| Insert the witnessed post-cut `M15` identity from the source into an otherwise consistent pre-write export | The verifier rejects it as `POST_CUT_MARKER_PRESENT`. |
| Remove a required interior archived segment from a copy of the recovery media | The contiguous-media check rejects it as `WAL_ARCHIVE_GAP` before target admission. |
| Render the target with the old durable consumer ID for one group | Admission rejects `CONSUMER_GENERATION_MISMATCH` with no target writers admitted. |

The actual absent post-cut marker, both stale and fresh authority challenges,
and positive orphan lookup and redispatch witnesses are required in all
three measured drills as well as qualification.

## Profiles, hosted admission and budget

Use separate public standard GitHub-hosted `ubuntu-24.04` jobs at **$0**.
One node runs kind with Calico and the J7 runtime, scheduler, relay and two
worker slots. The source or target PostgreSQL, Kafka and separate sink DB
run in isolated containers beside kind on that runner. Follow the existing
dependency-container topology and pinned binaries/images, including
PostgreSQL 18.6 for backup, recovery and verification. Seal the exact
candidate, rendered overlay, digests, routes, credentials layout and
expected migration inventories before qualification. Retain effective mode,
schema, dependency and workload image identities. Bound the runtime
connection inventory at 100 engine connections maximum and 16 sink DB
connections maximum, with at least 25% capacity unused under the witnessed
peak. Use least-privilege users, namespace and network restrictions for the
sink and probe roles. Verify old- and foreign-endpoint denial and
restore-target writer isolation in qualification.
[Existing harness](../../../scripts/dur053_k3.py#L596)
[Pinned tools and images](../../../deploy/k8s/tools.lock.json)
[Public runner specification](https://docs.github.com/en/actions/reference/runners/github-hosted-runners#standard-github-hosted-runners-for-public-repositories)

Register the following in `deploy/runtime-switches.json` once the registry
from PR #99 and the reviewed J9a switch are available. This slice records
the intended entries in the protocol.

| Registry entry | Frozen J7 expectation |
| --- | --- |
| J9a repaired relay mode | Add `J7_source` and `J7_target` to J9a's permitted enabled profiles. Use the exact registered name and enabled value in both. Its unset default remains legacy. Refuse to arm if the reviewed J9a implementation and matching registration have not landed, or if intended and effective modes differ. |
| `RUNTIME_KAFKA_GENERATION` (J7) | Register the unset default as an empty suffix preserving the existing Kafka group and durable consumer IDs. Permit only `j7-restore` as a nonempty value, only in `J7_target`. Require unset in `J7_source`, `K5`, `K6_before_Phase_G`, `K6_Phase_G`, `J5a` and `J5b`. Require runtime, worker and worker-control server agreement. Add no routes or migrations. Register an unset-equivalence check for both consumers and the worker-control API. |
| Other runtime modes | Use the existing K6 effect path and private probe credentials in the J7 overlay. Leave K6 write capture and unrelated J opt-ins unset, including `RUNTIME_TIMEOUT_RETRY_POLICY`. |
| J7 migration inventories | Register `J7_source` and `J7_target` with engine `000001`–`000023` inclusive and record sink `000001`–`000004` separately. J7 adds zero migrations. Require the same engine inventory on source and restore. If accepted J9a requires an opt-in migration, register its exact inventory in a reviewed amendment before qualification. |

Complete the registered J2 measurement on its own profile before J9a.
The K5/K6 and J5a/J5b profiles keep their registered values and byte-identical
manifests. Their checks fail closed on unknown opt-in variables, rendered or
live mode or route drift, and missing or extra migrations. Their inventory
still accounts for the K6 capture migration and the independent DUR-055 and
DUR-056 inventories where applicable. Keep required unset-mode equivalence
checks outside the PostgreSQL K6 fixtures job and obtain the reviewed
append-only disclosure before any registered K rerun from changed code. J7
keeps its own jobs, populations and time outside the K ledger. Relay and J19
default flips remain at K closeout.

- **Resources.** Require at least four logical CPUs, 12 GiB installed
  memory and 8 GiB free on the runner filesystem holding Docker, backups and
  exports after bootstrap and before each drill. Measure these values on the
  job and verify at least 4 GiB free throughout and at least 2 GiB available
  host memory, without OOM, swap growth or container restarts unrelated to
  the drill. Use limits of 1 CPU and 1 GiB for the active engine DB, 0.5 CPU
  and 512 MiB for the sink DB, and 1 CPU and 1.5 GiB for Kafka with a
  512 MiB heap. Limit each engine runtime role to 0.5 CPU and 256 MiB, each
  one-slot worker to 0.5 CPU and 512 MiB, and the sink service to 0.5 CPU
  and 512 MiB. Retain CPU throttling and actual usage. Require at most
  128 MiB engine DB size, 512 MiB plain backup
  and 512 MiB archived WAL per drill. Qualification must demonstrate backup,
  replacement and export coexistence within that headroom. Retain peak
  resource, I/O, connection and filesystem samples at 5 s intervals and at
  backup, outage, replay, export and cleanup boundaries.
- **Qualification.** Before the measured job, require one complete drill,
  all controls, sound clock brackets, sealed mode and migration inventories,
  reverse-join replay, artifact upload and verified cleanup. Pin the passing
  run, artifact hash and reviewed measurement head. Admit a measured job
  only after exact-candidate review. Changing a threshold, population, mode
  or sealed input requires a reviewed amendment and fresh qualification.
- **Forecast and cap.** Freeze a 350 min cap per hosted job. Reserve 35 min
  for bootstrap, 20 min per complete drill, 5 min between drills, and
  35 min for final upload and teardown. Qualification adds 15 min for the
  rejection controls, for `35 + 20 + 15 + 35 = 105 min`. Measurement is
  `35 + 3 × 20 + 2 × 5 + 35 = 140 min`. Treat these as forecasts. Before
  arming and each drill, use the greater of the registered allowance and
  qualified or observed time for each remaining matching stage, including
  the full final reserve. Stop if it exceeds time left. Require confirmed
  loss within 8 min of drill start, and finish the drill and seal its
  evidence within 20 min. The RTO gates also apply.
- **Stop and retain.** Stop at a violated invariant, missed cutoff,
  unexpected population or outcome, witness or capacity failure, corrupt
  recovery media, or failed cleanup. A proved wrong result is failed.
  Missing proof or infrastructure failure is invalid. Preserve a known
  failure across later export or cleanup trouble. Record all unstarted
  drill IDs and reasons as unperformed. There are zero measurement retries,
  substitutions or dropped outliers. Export partial evidence in an
  always-run finalizer, attempt teardown, verify the absence of job-owned
  pods, containers, volumes, topics, credentials and listeners, and record
  any residuals.

## Evidence and public claims

Retain under this directory at `<campaign-id>/` a digest-bound evidence
index, source and image digests, job/run/attempt and artifact IDs, expected
identities and configuration, qualification and forecast, the external
ledger, clock brackets, fault and volume-deletion proofs, archive inventory
and backup manifest, verification and replay logs, broker snapshots, raw
read-only engine and sink exports, independent verdicts, missing ID lists,
phase times and cleanup receipts. Export redacted operating settings and
token fingerprints. Keep physical backups and WAL private on the job
filesystem because they contain fixture credentials. Retained evidence
supports independent logical replay of the verdict. Physical replay from
the private media is witnessed in each running drill.

After all three measured drills pass and Claude reviews their evidence,
update the claim map. The permitted claim is that on one GitHub-hosted
runner with one kind node, three witnessed source-volume deletions of the
reported database sizes were restored from base backup and archived WAL.
Report the measured per-drill RPO and RTO, the exact reconciled and lost
populations, fresh-broker redispatch, both stale rejections and the retained
effect receipt with one application in each drill. If qualification fails
or any drill is invalid, failed or unperformed, publish its disposition and
withhold that three-drill success claim. Claims about host or site loss,
independent failure domains, HA, zero data loss, production readiness or
recovery SLOs, capacity, throughput or general exactly-once effects are
outside this evidence. [Public claim map](../recruiter/claim-evidence.md)
