# J9a relay throughput protocol

Slice 0 registration for review, 2026-11-28. Hosted qualification and
measurement require Claude's review of the protocol and sealed candidate,
and a completed registered J2 measurement. Record the campaign ID, immutable
inputs and qualification evidence before arming any measured job. J9b
requalification and soak require a separate reviewed registration.

## Modes and failure model

- `RUNTIME_RELAY_FULL_BATCH_LOOP` unset or literally `0` selects one pass
  per wakeup. Literally `1` selects the repaired relay. All other
  present values fail runtime startup. J4 reports the canonical value
  and effective mode from the constructed relay.
- A pass may run immediately again only if
  `err == nil && Claimed == BatchSize && Failed == 0`. Check context
  cancellation and drain before each repaired pass, including between
  full passes. Startup, periodic polling and PostgreSQL NOTIFY still
  trigger the same wakeup path. Notifications can coalesce.
- Hold the current batch size (10), poll interval (1 s), retry backoff
  (1 s), claim lease (60 s), claim order (`created_at, event_id`), per-row
  synchronous Kafka publish and durable finalization fixed. The loop does
  not change the attempt, lease, broker acknowledgement or crash boundary.
- In the failing-broker control, seed at least ten full batches of
  initially claimable supported rows with zero attempts, hold new arrivals
  and distinct later wakeups, and return a broker error for every publish.
  Require one pass on the failed wakeup. Relative to the pre-wakeup
  snapshot, `sum(delta RelayAttempts) <= BatchSize` and
  `max(delta RelayAttempts) <= 1` (at defaults, at most ten total and one
  per row). With a complete claim and failure finalization, require exactly
  ten attempts on that first batch and zero on the remainder. This bound
  applies per wakeup. A later independent wakeup can retry eligible rows.
- The measured comparison is healthy PostgreSQL and Kafka on a single
  GitHub-hosted runner per job. Separate controls cover a burst followed
  by no arrivals, short claims, broker rejection or an acknowledgement
  error, claim/finalization error and cancellation/drain at pass boundaries.
  A broker error can be ambiguous and an acknowledgement can precede a
  crash before finalization. Stable event IDs and durable obligations still
  govern retries. The comparison does not establish broker exactly-once,
  multi-host behavior, HA, production throughput or capacity.
- Keep K manifests byte-identical and enforce the runtime-switch registry
  and existing K migration, route and mode checks. K5/K6 and J5a/J5b
  require absence or literal `0`. The default flips only at K5/K6 closeout.
  Subsequent J runtime profiles select `1` explicitly, with the registered
  legacy controls below and the J5 exceptions.

## Paired measurement

Use three first-attempt hosted jobs, in job-number order `off → on`,
`on → off`, `off → on`. Each pair runs serially on its own runner and
uses the same immutable candidate runtime digest, worker digest, build
inputs, pinned dependencies and resources. Use the same candidate across
all three jobs. Recreate application namespace, PostgreSQL state, Kafka
topics, consumer state and witnesses between arms. Retain intended and
running digests, environment and effective mode evidence, rendered overlay,
schema and migration inventories, resource limits and instrument settings.
The two arms differ only in the registered relay mode. Verify each reset
and use equal seeds within a pair. Record the three pair seeds beforehand.
Qualify the instrumentation and reset at the exact candidate first.

### Seeded service rate

1. Use a sealed 3,600-row fixture of J2 short-workflow outbox obligations
   drawn from 600 reconciled workflows with six rows each. Record the
   canonical ordered manifest of event ID, event type, topic, aggregate
   revision, payload digest and durable creation timestamp. Restore one
   identical clean image of its claimable rows in each arm, with fresh
   broker topics and no prior publication or attempt evidence for those
   IDs. Require identical claimable IDs, row types, payloads and ordering
   inputs. Verify the same ordered batch membership and publication-start
   order from the witness. Hold all other publishers, scheduling, workers
   and sources of new outbox rows or notifications inactive.
2. Durably record a database-clock activation marker in the harness
   journal while the relay is held. Immediately release the same initial
   wakeup mechanism in both arms and leave the regular ticker running.
   Bracket the release and require its
   database-clock upper bound to be within 100 ms of the marker. Retain
   the raw timestamps and a first-to-last-publication sensitivity. Observe
   through completion or a 600 s cutoff. A healthy repaired arm should
   finish on its initial wakeup. Count wakeups and their source to verify
   that independently of elapsed time.
3. Count distinct seeded event IDs with committed `PUBLISHED` state and
   `published_at` in the closed interval from activation to activation
   plus 600 s, joined to the broker witness by stable ID. For completed
   backlog, active drain seconds are the last such
   `published_at` minus the durable activation marker, on the same DB
   clock. Include poll waits. Use no quiet, setup or checker time in this
   denominator. Require every seeded ID, a positive interval, checked
   outbox/publication and broker evidence, no final active claims and no
   unexpected IDs or arrivals. Retain broker duplicate counts independently
   of the distinct numerator. For a valid cutoff with residuals, report
   the observed distinct count over 600 s and the checked residual as
   diagnostics, and mark the completed-backlog criterion failed.
4. The registered gain is **at least 3.00×** the legacy distinct publication
   rate in **each** of the three valid, completed pairs, using unrounded
   ratios. This requires a material gain above the poll-limited legacy
   rate of about 10 rows/s observed in R411's no-arrival window (rehearsal
   45946992943) and limits reliance on a fast runner or one favorable
   ordering. Use all three jobs and no selective repeats. A valid failure
   stays in the table. An invalid or unperformed pair leaves the planned
   three-pair claim incomplete. Repair a qualification problem under
   review and use a newly registered campaign if repeating measurement.

### Matched workflow drain and historical bridge

In each paired job, run a fresh matched load episode in each arm using
J2's final registered short-workflow mix, sender, worker settings and
checker. Preselect J2's A hold and C step: 120 s warm-up and 1,800 s hold
at A (7.136100570066256/s), then 300 s at C (24.33102566649034/s), with
the exact rates and arrival rounding taken from J2's registered tooling.
Use identical slot offsets, seeds, workload generation and worker
topology, with a new absolute origin and unique J2 run ID per arm.
Preserve each arm's planned keys, workflow IDs, payloads, phase and
transition evidence. Arm only after full-population sender, observer,
export and live/offline
checker qualification under these resource limits. Use at least J2's
21,002-identity A qualification bound.

Run one separate fresh bridge job with that identical episode on the
**J2 rebuilt registered profile**, using the immutable runtime and worker
digests from its completed campaign, its registered overlay and the legacy
relay. Reserve the first pair's load seed for this bridge. Bind the J2
result receipt, image IDs, tools, workload and schema before dispatch.
Use the same independent external observation and checker across the
bridge and the candidate arms where the registered profile permits.
Record any unavailable internal diagnostics and all version or topology
differences. Use the bridge for context on legacy workflow drain, outside
the same-runner gain criterion.

Apply J2 sender-validity checks for every phase, including actual
on-time sends before each next nominal slot, complete journals and no
unexplained misses, cap refusals or unresolved admissions. Reconcile
scheduled, sent, acknowledged and committed keys and workflow IDs.
Preserve admitted timeouts as timeouts. Compare the planned and delivered
cohorts across arms by phase, slot sequence, activity and payload inputs
after accounting for run-specific identifiers. Require identity equality
within each arm's J2 reconciliation. Missing or mismatched load
invalidates the comparison. Use J2's bracketed, identity-reconciled
drain from scheduled send stop through the final checked read, with a
600 s cutoff **including 30 continuous seconds of quiet**. Track
expected and actual terminal identities, final revisions, receipts,
payloads, invocation witnesses, outstanding work, unpublished rows and
active claims. Restart quiet on fingerprint changes. On expiry, retain a
checked residual snapshot inside the reserved closeout time and classify
an integrity violation or uncheckable cut ahead of a clean drain expiry.
Report the J2 step verdict, including a valid step with overload
unconfirmed, and the observed drain seconds or cutoff and residuals per
arm. A valid checked expiry is a result alongside seeded relay service.

## Stage attribution and tuning

- Collect wall-clock timings for claim (including DB wait), publication
  start capture, synchronous broker send/ack, finalize, and wakeup
  wait. Join J2 end-to-end stages using durable times, read brackets and
  stable IDs offline. Record samples, counts, sums and coverage for each
  stage plus queue age by row type, active claims, broker offsets, DB wait
  and I/O, runtime CPU and throttling. Qualification reconciles histogram
  bucket/count/sum observations with witnessed durations. Use classic text
  histograms, bounded labels, the current telemetry renderer, and an opt-in
  distinct from this relay mode. Verify the K scrape contract with
  instrumentation disabled. Treat missing required coverage as invalid.
- Set `RUNTIME_J9A_STAGE_TIMINGS=1` on the candidate relay in **both**
  arms. Absent or literal `0` preserves the legacy scrape. Other
  values fail startup. This independent switch gives the off/on arms
  the same instrumentation while the historical J2 bridge uses its
  pinned runtime and external evidence.
- Scrape `durable_j9a_relay_stage_seconds` by runtime role and fixed
  stage. Claim, capture, synchronous broker publish, and finalize count
  each call to that stage, including errors. For the seeded arm retain
  the start and end scrape cuts and the startup, poll and NOTIFY counts.
  The startup trigger contributes a zero duration. The poll and NOTIFY
  waits run from entering the select to consuming the trigger. An
  immediate follow-on pass adds no wakeup observation.
- Replay the J2 checker and phase join first. Feed each checked
  `phase-rows.json` array to `cmd/j9a-stage-histograms` on stdin and
  archive the resulting `durable_j9a_workflow_stage_seconds` text beside
  the runtime scrapes. This renders the four raw winning-attempt spans
  and scheduled-to-success total with a fixed offline role. Publish
  censoring, uncertainty and J2's quantitative results from the checked
  evidence. The histogram projection counts the complete joined rows
  only and leaves the raw durations available for replay.
- Both families use bucket upper bounds of 0.0001, 0.0005, 0.001, 0.005,
  0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300 and
  600 seconds, plus `+Inf`. Check counts, sums and cumulative buckets
  against the witnessed fixture and joined phase rows.
- In matched diagnostic runs during qualification, take relay CPU profiles
  and goroutine stacks while the queue is nonempty using the existing
  private pprof listener. Use equal five-second CPU captures and two
  bracketing goroutine snapshots per arm. Retain raw pprof, build identity,
  window and occupancy evidence. Disable active profiling during the rated
  windows. Use wall time and stage/queue evidence to distinguish blocking
  DB or broker time from CPU time. Review oldest dispatch age against the
  event queue and choose any priority change only from demonstrated
  contention and with fairness and checker review. If the evidence cannot
  separate competing stages, report the limiter as unresolved.
- For a demonstrated SQL hotspot retain representative before/after
  `EXPLAIN (ANALYZE, BUFFERS)` plans, query parameters, data shape, cache
  conditions and server/version settings. Execute mutating plans on
  disposable fixtures and keep this work outside measured windows.
  Publish “no tunable SQL hotspot” when supported. Any opt-in index
  requires registered migration scope, matched write-overhead measurement
  and independent checker results. Confirm one reviewed tuning candidate
  at most, on three fresh paired jobs with the same order, workload and
  eligible gain criterion. Preserve and publish the original comparison.

## Hosted budget and publication

Use only public GitHub-hosted Actions at $0 and a separate J namespace,
time and evidence ledger. Reserve the following runner minutes.

| Job | Count | Per-job forecast | Per-job hard cap |
| --- | ---: | ---: | ---: |
| Qualification and diagnostic controls | 1 | 125 | 150 |
| Initial pairs | 3 | 195 | 210 |
| J2 bridge | 1 | 110 | 120 |
| Fresh tuning confirmation, if admitted | Up to 3 | 195 | 210 |

A pair reserves 30 min setup, 75 min per arm (10 seeded, 37 load, 10
workflow drain, 5 checked closeout/export and 13 reset and verification)
and 15 min final replay, cleanup and upload. The bridge reserves 30 min
setup, 65 min for its single load arm and 15 min final closeout.
Qualification reserves 30 min setup, 80 min for a full A-volume episode,
controls, profiling and reset qualification, and 15 min closeout.
The initial forecast is 820 runner minutes with a 900-minute cap.
Conditional tuning raises these to 1,405 and 1,530 minutes. Cap complete
compressed evidence at 500 MiB per job and 4,000 MiB total. Before
dispatch and before each arm, forecast all remaining stages with the
greater of registered reservations and observed comparable stage times,
including final closeout. Stop with remaining cells unperformed if time,
storage, integrity, cleanup or admission checks cannot be met. Keep
collected evidence and the stop reason. Run no replacement automatically.

Publish under `experiments/portfolio/relay-throughput/<campaign-id>/`.
For every arm retain pins, commands, workload manifests, raw timestamps,
stage and resource evidence, rates and denominators, checked residuals,
checker verdicts and replay, budget, cleanup and artifact hashes. Publish
the ledger of planned, completed, invalid and unperformed IDs and
stage evidence, profiles and applicable query plans. Record all paired
ratios, individual rates and the min–max spread, all workflow-drain
outcomes and the separate bridge result.

- **Pass.** Report the registered gain only with three valid passing
  pairs, the behavioral controls and independent checks, and a valid bridge
  outcome, including a checked bridge expiry.
- **Fail or null.** Publish valid observations and which criterion failed.
  Keep valid checked expiries, absent gains and unresolved attribution in
  the record.
- **Invalid or unperformed.** Publish the failed gate, available diagnostics
  and excluded or unstarted cells. Withhold the campaign gain claim when
  its required evidence is incomplete. Preserve independently valid
  results with their exact scope.
