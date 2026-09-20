# DUR-031 interview evidence and personal walkthroughs

Status: IN_PROGRESS for the outstanding personal-participation component (R092,
Claude round 48). The engineering pack retains round-47 acceptance at fab88ac.
At the user's explicit request, Codex completed a user-role simulation of all
three exercises on 2026-09-20. This is reproducible engineering evidence, not
evidence that the user personally performed or explained the exercises. No
acceptance deferral is assumed.

## Pending user evidence (R092)

If personal fluency is required, the user must still record their own actions
and explanations for all three:

- [ ] Explain and mutate one lease/attempt rule in a disposable copy, capture
      the expected test failure, and explain why the original fence matters.
- [ ] Walk through the ambiguous non-cooperating effect, explaining why the
      outcome stays unknown, how late evidence is retained, and why retry is
      not automatic.
- [ ] Independently run the offline result reproduction below, capture its
      output, and explain the workload/crash-model limits.

For each, add the user attribution, date, commit, commands/output and the
user's explanation here. The Codex role-play record below is not a substitute
for personal evidence. DUR-031 cannot be re-recorded DONE on the basis of
role-play alone; Claude must decide whether the user-directed simulation is
acceptable for this project.

Baseline for this task: 242cdcb, the DUR-030 closeout. This task adds
documentation and indexes existing evidence; it makes no provider calls, paid
calls, external actions, experiment reruns, or engine changes.

## How to use this pack

The claim table is the short answer to “what did I build, how do I know, and
what does it not prove?” The walkthroughs are interview preparation: each one
names the code boundary, the experiment or test, the expected failure or
result, and the limit of the conclusion.

The three proposed resume-level findings are deliberately bounded:

1. With a fixed four-process worker pool, two schedulers sustained the tested
   2/s offered rate where one scheduler did not on the DUR-026 engine path.
2. In the quoted DUR-035 campaign, Kafka was slower than direct notification
   at the resolved terminal stage; the dispatch-stage comparison was
   unresolved and varied across campaigns.

3. At one SHA-256 work unit per chunk, every-chunk checkpointing took 7.5552
   times the boundary-only median in the recorded in-process panic workload.
   This is one measured cost point, not a general policy or crossover estimate.

The 48/48 controller/checker campaign is a separate bounded validation claim.
Safeguard cost is not a headline finding: DUR-034 resolved no cost effect.
Lease, retrieval, approval, and production-path claims below are supporting
interview evidence, not broader production claims.

## Claim-to-evidence map

| Claim | Implementation and reviewed target | Artifact and population | Reproduction | Boundary of the claim |
|---|---|---|---|---|
| Named durable fault cases recovered and were independently checked | internal/invariants/m5.go, cmd/fault-checker, controller in scripts/m5-campaign.ps1; M5 target acb28ba; residual correction target 0002e75 | experiments/m5/f01-f11-results.json: 16 named cases across F01–F11, 3 seeds each, 48/48 PASS; traces and offline durable snapshots are committed. The corrected F01 submission boundary has zero activity attempts, and the checker rejects overshoot. | go run ./cmd/fault-checker -offline -trace experiments/m5/traces/F07-crash-resume-seed11.jsonl -durable-trace experiments/m5/durable/F07-crash-resume-seed11.json | Bounded declared cases and fixtures. It is not exactly-once, multi-host durability, hard-kill equivalence for every case, or a production-scale claim. R057 is verified closed in Claude's round-50 review. |
| Scheduler capacity separated from worker capacity | cmd/dur026-benchmark, cmd/dur026-worker, scripts/m7-dur026.ps1; reviewed target 50d4b13 | experiments/m7/dur026/results.json: T1/T2, 1 or 2 schedulers, fixed 4 worker subprocesses, 24 measured workflows, 4 warmups, 3 repeats, 120 s SLO, 576 terminal and 0 pending | pwsh ./scripts/m7-dur026.ps1 -StartServices (writes the results artifact; use -Pilot for the pilot) | Single-node WSL2 engine-path study with synthetic workloads. It excludes API, relay, Kafka, runtime containers, multi-host deployment, and maximum sustainable throughput. |
| Notification and Kafka dispatch stages were decomposed | cmd/dur035-dispatch, production relay path, scripts/m7-dur035.ps1; reviewed target 6679585 | experiments/m7/dur035/results.json: 4 arms, 24 workflows per arm, 4 worker subprocesses, 3 repeats, 288 terminal, 0 pending/failed | pwsh ./scripts/m7-dur035.ps1 -StartServices | The quoted campaign resolves the terminal comparison only: Kafka is slower than direct notification there. Ready-to-claim direct versus Kafka was unresolved in the quoted campaign and varied across campaigns; this is not a universal Kafka latency claim. |
| Lease takeover safety and useful progress were measured for both pause and crash faults | scripts/m7-dur027.ps1, cmd/dur027-lease, self-exiting crash fixture; reviewed substantive target 71fd54a | experiments/m7/dur027/results.json: 3 TTLs × 2 fault types × 10 episodes, 60 takeovers, 0 false takeovers, 180 fenced stale writes | pwsh ./scripts/m7-dur027.ps1 -StartServices | Local fixture and database evidence on one host. TTLs are measurement settings, not deployment recommendations; the study is not multi-host or production-throughput evidence. |
| At one measured chunk cost, checkpointing did not pay for the pure workload | cmd/dur028-checkpoint, scripts/m7-dur028.ps1; reviewed target 7f66d88 | experiments/m7/dur028/results.json: 200 SHA-256 chunks, 1 work unit per chunk, 18 runs; boundary-only median 0.2913056 s versus every-chunk median 2.200874 s, ratio 7.5552 | Offline recomputation shown in the next section; full rerun is pwsh ./scripts/m7-dur028.ps1 -StartServices | The failure is an in-process panic, not an OS kill or host failure. The result is scoped to 1 work unit per chunk and does not identify a general checkpoint policy or crossover point. |
| Approval and effect binding held on the production path | internal/incident/production.go, internal/api, internal/effects; DUR-033A target 95948cb | Production-path integration uses PostgreSQL source_corpus, the real API/engine/effect service, and a scheduler-owned approval grant. The attack matrix rejects changed resource, canonical arguments, revision, grant reuse, and pre-approval dispatch | $env:DURABLE_REQUIRE_DATABASE=1; go test -race ./internal/incident -run '^TestDUR033AProductionPath$' -count=1 -v | Synthetic source corpus and no external action or live model. It demonstrates the integration boundary and approval semantics, not authentication, provider safety, or production deployment. |
| Retrieval and incident-agent evidence is claim-bounded | M6/M7 retrieval, redaction, approval and live-evaluation artifacts; reviewed target 5b9d65c | experiments/m7/dur029/retrieval-final.json and live-evaluation.json: 120 held-out retrieval queries; live model 4/20 overall and 0/16 document-dependent per arm; fixture control 20/20; 27/27 proposals approved and 0 completed without approval | No paid rerun is required or authorized for DUR-031. Inspect the artifacts and docs/TECHNICAL_REPORT.md | One OpenAI gpt-4o-mini model, one prompt/schema and one sampling regime. The adversarial rates must travel with counts: defended 2/20 above its clean-clean baseline versus plain 6/20; these are not general model-quality or prompt-injection guarantees. |

## Independent reproduction already checked by Codex

The simplest offline result to reproduce is the DUR-028 ratio. The command
reads the committed artifact, selects the two observed crash rows, and divides
their median completion times; it does not rerun the study or infer a value
from source code.

    $o = Get-Content experiments/m7/dur028/results.json -Raw | ConvertFrom-Json
    $b = $o.summary | Where-Object { $_.checkpoint_setting -eq 'activity_boundary_only' -and $_.failure_condition -eq 'crash_mid_activity' }
    $e = $o.summary | Where-Object { $_.checkpoint_setting -eq 'every_chunk' -and $_.failure_condition -eq 'crash_mid_activity' }
    [math]::Round($e.total_completion_seconds.median / $b.total_completion_seconds.median, 4)

Codex ran this against the current committed artifact and obtained 7.5552.
The result is bounded because the input is one fixed artifact, the workload is
one 200-chunk SHA-256 activity, and work_units_per_chunk is 1. It is not a
claim that every checkpoint policy costs 7.6 times more.

## Walkthrough A — mutate the lease/attempt rule in a scratch copy

Purpose: explain why a scheduler must own the partition lease before it can
advance a workflow, and why a returned lease record is not proof that the
caller acquired it.

1. Make a disposable worktree or copy from commit 242cdcb; do not edit the
   reviewed worktree. In the scratch tree, open
   internal/engine/engine.go at the AcquireLease call in Engine.Run.
2. Make one deliberately unsafe mutation: when AcquireLease returns
   acquired == false, continue with the returned lease instead of returning
   state.ErrLeaseNotOwned and RunResult{Blocked: true}. Leave the rest of the
   transaction and release logic unchanged.
3. With the database-backed integration prerequisites available, run:

       go test -race ./internal/engine -run '^TestM1RunRequiresPartitionLease$' -count=1

4. Explain the expected failure: the test creates a lease owned by another
   scheduler, then requires the second engine to be blocked and verifies that
   the holder's lease and epoch are unchanged. The unsafe mutation lets the
   second engine borrow the lease and can also make its deferred release act
   on the holder's lease.
5. Restore or delete only the scratch tree. The reviewed repository must remain
   unchanged.

The production rule is visible in the test at
internal/engine/engine_integration_test.go and in the Engine.Run
lease-acquisition boundary. The explanation should distinguish lease
ownership/fencing from worker claim tokens: the lease authorizes
scheduler-side mutation, while the attempt token fences worker results.

## Walkthrough B — ambiguous non-cooperating effect

Purpose: explain why a timeout after an external request is issued cannot be
reported as “nothing happened”.

Read the contract's timeout and reconciliation rules in docs/CONTRACTS.md,
then walk this sequence:

1. A scheduler owns the lease and creates a non-cooperating effect attempt.
2. A worker claims it and may send the irreversible request.
3. The worker disappears before its receipt is durably recorded.
4. The scheduler times out the claimed attempt. The correct result is
   RECONCILIATION_REQUIRED, one reconciliation obligation, and no automatic
   replacement attempt. The attempt outcome is OUTCOME_UNKNOWN when a claimed
   effect may have happened.
5. A late applied report is retained as evidence exactly once. It does not
   reopen or advance the canceled/reconciliation workflow, and it must not be
   interpreted as proof that the earlier request was harmless.

The database-backed regression test is
internal/state/m4_integration_test.go,
TestM4NonCooperatingTimeoutIsReconciliationOnly. With the service database
prerequisites available, run:

    go test -race -p 1 ./internal/state -run '^TestM4NonCooperatingTimeoutIsReconciliationOnly$' -count=1

The test checks the reconciliation state, the absence of a replacement, one
obligation, and one late-evidence row. This is the point to explain the
difference between a pure activity (safe to retry), a cooperating sink (retry
with the same effect key and grant scope), and a non-cooperating effect (stop
and reconcile).

## Walkthrough C — independently reproduce one result

The user may run the offline DUR-028 ratio command above in a separate
PowerShell session, obtain 7.5552, and explain both the arithmetic and the
scope. The required explanation is: every-chunk checkpointing saved 99 replayed
chunks in this crash model but added 200 checkpoint writes and 19,692 bytes;
the result is measured at one SHA-256 work unit per chunk and does not identify
where a different workload's crossover point lies.

This is intentionally a user action when personal fluency is being assessed.
Codex's matching value is recorded as a sanity check, not as evidence that the
user independently reproduced it.

## Recorded walkthrough evidence

Codex performed these walkthroughs on 2026-09-19 at the user's request.
The reviewed worktree remained unchanged throughout.

### Lease/attempt mutation

A disposable worktree was created from d4cb1b8. In its copy of
internal/engine/engine.go, the `!acquired` branch was changed to continue
with the returned lease instead of returning ErrLeaseNotOwned. The isolated
run used:

    $env:GOCACHE = '<scratch cache>'
    $env:DURABLE_REQUIRE_DATABASE = '1'
    go test -race ./internal/engine -run '^TestM1RunRequiresPartitionLease$' -count=1

The first run hit a pre-existing Go build-cache initialization collision before
compilation. Rerunning with the isolated cache reached the test and failed as
expected:

    borrowed lease: result={... State:SUCCEEDED ... Steps:1 Blocked:false} err=<nil>

This demonstrates the safety property rather than merely inspecting it: the
mutated engine ran a workflow while another owner held the partition lease,
where the committed test requires `Blocked:true` and `ErrLeaseNotOwned`. The
disposable worktree and cache were then removed. The reviewed worktree has no
mutation from this exercise.

### Ambiguous non-cooperating effect

The production state test was run with the service database required:

    $env:GOCACHE = '<isolated cache>'
    $env:DURABLE_REQUIRE_DATABASE = '1'
    go test -race -p 1 ./internal/state -run '^TestM4NonCooperatingTimeoutIsReconciliationOnly$' -count=1

Observed result: `ok durable-agent-execution-engine/internal/state 1.777s`.
The test confirmed the claimed non-cooperating attempt becomes
`RECONCILIATION_REQUIRED`, no replacement is dispatched, one reconciliation
obligation exists, and the late applied report is retained as one evidence row.
The walkthrough conclusion is that an uncertain irreversible effect is never
collapsed into a zero outcome: pure work may retry, a cooperating sink retries
with the same key and grant scope, and a non-cooperating effect stops for
reconciliation.

### Independent DUR-028 reproduction

In a separate PowerShell invocation, the artifact-only calculation produced:

    boundary_only_median=0.2913056
    every_chunk_median=2.2008739999999998
    ratio=7.5552

The arithmetic matches the committed summary. The explanation recorded was
that every-chunk checkpointing saved 99 replayed chunks in this crash model but
added 200 checkpoint writes and 19,692 bytes; the result is measured at one
SHA-256 work unit per chunk and does not identify another workload's crossover
point.

## User-requested role-play evidence — 2026-09-20

At the user's explicit instruction to "act like real user", Codex performed
the three exercises below. The actor was Codex, using the repository and local
throwaway services; this section deliberately does not claim that the user
personally performed the exercises.

### A — lease/attempt mutation

A detached disposable worktree at `703e2a6` was created under
`bin/r092-user-lease`. In that copy only, the `!acquired` branch in
`internal/engine/engine.go` was changed to continue with the returned lease.
Against a newly migrated `codex_r092_20260920` database, the command was:

    $env:DURABLE_DATABASE_URL = '<throwaway database URL>'
    $env:DURABLE_REQUIRE_DATABASE = '1'
    $env:DURABLE_RUN_INTEGRATION = '1'
    go test -race ./internal/engine -run '^TestM1RunRequiresPartitionLease$' -count=1 -v

Observed failure:

    borrowed lease: result={... State:SUCCEEDED ... Steps:1 Blocked:false} err=<nil>

This is the intended negative control: the unsafe copy completed a workflow
while another owner held the partition, where the committed test requires
`Blocked:true` and `ErrLeaseNotOwned`. The disposable worktree was removed
afterward. The reviewed worktree was not mutated.

### B — ambiguous non-cooperating effect

Against the same throwaway database, the committed test was run with:

    go test -race -p 1 ./internal/state -run '^TestM4NonCooperatingTimeoutIsReconciliationOnly$' -count=1 -v

Observed result: `PASS`, package time `1.788s`. The test exercised a claimed
non-cooperating attempt whose request may already have been issued. Timeout
produced `RECONCILIATION_REQUIRED`, no replacement dispatch, one reconciliation
obligation, and one late applied report stored as evidence. The explanation is
that pure work may retry, a cooperating sink retries only with the same effect
identity, and a non-cooperating effect must stop and preserve uncertainty.

### C — independent checkpoint reproduction

In a separate PowerShell invocation over the committed artifact, the exact
offline calculation produced:

    7.5552
    boundary_median=0.2913056; every_chunk_median=2.2008739999999998; work_units_per_chunk=1

The explanation is that every-chunk checkpointing saved 99 replayed chunks but
added 200 checkpoint writes and 19,692 bytes. The ratio is scoped to one
SHA-256 work unit per chunk and the in-process panic model; it is not a
crossover estimate or general checkpoint policy.

### Attribution and cleanup

This was an agent-executed, user-requested role-play, not personal user
evidence. The throwaway database ended with zero workflows and definitions and
was dropped. The disposable worktree and mutation files were removed. The
development database, containers and reviewed source were left untouched.

## Completion checklist

- [x] Claim map names implementation targets, artifacts, populations,
  configurations, limits, and reproduction commands.
- [x] Codex independently recomputed the DUR-028 ratio from the committed
  artifact (7.5552).
- [x] Codex performed the lease/attempt mutation in a scratch copy at the
  user's explicit request; observed failure and cleanup are recorded.
- [x] Codex walked through the ambiguous-effect timeout and late-evidence
  rule; the distinction between pure, cooperating, and non-cooperating effects
  is recorded.
- [x] Codex independently ran the DUR-028 offline reproduction and explained
  why the result is bounded.
- [ ] User personally performs and explains the three exercises, if personal
  interview fluency remains an acceptance requirement.
- [x] Claude reviews the committed DUR-031 pack and the recorded walkthrough
  evidence.

DUR-031's engineering pack was accepted in round 47. The role-play evidence
above addresses reproducibility at the user's instruction but does not
establish the user's personal interview fluency. This attribution correction
does not rewrite Claude's review; R092 remains for Claude to assess.
