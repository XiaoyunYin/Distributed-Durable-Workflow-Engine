# J2 slice 2 harness closeout, 2026-11-27

Round 589 accepted the slice 2 harness at `fc8119bb8747a1b25b8a9f38877d998624c02e38` with no blocking findings. This record covers unmeasured engineering rehearsals and their observed wall times. The slice 3 load-envelope results and both measured soaks remain pending. [PR #96](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/pull/96) · [Registered protocol](protocol.md)

The grid path uses the separately built and hash-verified stock K4 sender. The continuous path uses the two-period J2 sender with one origin, consecutive sequence numbers, one concurrency cap and the same workers across the step. Replay binds the two periods to the retained schedule and verifies the locked checker evidence. The qualified population remains 26,678 identities, with A and AB eligible on volume. [Registered qualification](slice-1b-qualification.md#slice-1c-registered-qualification) · [Checker lock](checker.lock.json)

## Hosted engineering dispatches

Each row is attempt 1. The outcome column is the Actions conclusion. The dispatch inventory includes the failed attempts and the deliberately failed barrier fixture.

| Run | Head | Mode | Outcome | Key evidence |
| --- | --- | --- | --- | --- |
| [45926610848](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45926610848) | `05ca6ce` | `harness-reset` | FAIL | Observer lacked `psycopg` during the interruption fixture (R408). Seal, verified upload and receipt succeeded. Rounds 554–555. |
| [45929556915](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45929556915) | `08f6241` | `harness-reset` | PASS | Verified reset and teardown. Standard-library supervisor stopped the sender after one durable admission from a 30-row plan. The pre-send plan still reported `NOT_SENT`; durable evidence settled the admitted row. Rounds 556–557. |
| [45931996888](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45931996888) | `79fa8e2` | `harness-schedule` | PASS | Hash-verified stock K4 build, 720/720 acknowledged and admitted in slot at 1/s, complete observer samples. Interruption left one admission and 29 unresolved rows. Rounds 558–559. |
| [45935231001](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45935231001) | `edfa151` | `harness-sender` | PASS | Single process offered 8 at 1/s then 12 at 4/s; all 20 reconciled. Interruption left one durable admission and 31 unresolved rows. Replay binds both periods to the hashed schedule. Rounds 560–561. |
| [45937621081](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45937621081) | `2df6807` | `harness-step (1/s hold)` | FAIL | Grid quiet pass. With 150 ms sleep and a Kafka pause, all 9,220 continuous-episode identities succeeded. The last in-budget poll at +593 s still showed 17,521 unpublished rows, so the drain expired. Verified closeout. Round 566. |
| [45946992943](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45946992943) | `968d5d7` | `harness-step (1/s hold)` | FAIL | Registered 100 ms sleep and no Kafka pause. Grid quiet pass. All 9,220 continuous-episode identities succeeded; 16,996 outbox rows remained unpublished at drain expiry. Relay publication, resource, log and Kafka evidence isolated the engineering backlog pattern. No natural carry-in found. Rounds 567–568. |
| [45954556522](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45954556522) | `968d5d7` | `harness-step (4/s hold requested)` | FAIL | Grid checker exited 0 in 0.54 s. The 1 Hz sampler missed process VmHWM and the resource gap was incorrectly classified as `integrity_stop` (R415). The step was unperformed. Cleanup verified. Round 570. |
| [45959322570](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45959322570) | `71ce57e` | `harness-step (4/s hold)` | FAIL | Grid quiet pass with exit-RSS evidence. At step expiry, all 14,980 had succeeded, 16,261 rows were unpublished, and the locked check passed. The valid reads found no carry-in. Rounds 572–573. |
| [45968932565](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45968932565) | `d2438e6` | `harness-step (8/s hold)` | FAIL | All 22,660 succeeded; 12,744 outbox rows were unpublished at the checked expiry. Durable replay recovered the last hold identity, ending `015359`. Cleanup verified. Rounds 577 and 580. |
| [45971780256](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45971780256) | `431277f` | `barrier-success` | PASS | All three local decisions and the reporter passed. The synthetic fixtures selected AB or no eligible rate, and blocked on integrity, bad digest or mixed attempt as applicable. Rounds 574 and 581. |
| [45971784262](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45971784262) | `431277f` | `barrier-peer-failure` | Expected FAIL | Job 3 failed after publication. Jobs 1 and 2 recorded `peer_failure`; the reporter passed. All soaks were unperformed. Rounds 574 and 581. |
| [45974446414](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45974446414) | `431277f` | `barrier-missing-peer` | PASS | Jobs 1 and 2 reached the 2,700 s timeout. Job 3 remained live and withheld its manifest. The reporter passed and all soaks were unperformed. Round 581. |
| [45978022336](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45978022336) | `2e0909d` | `controls` | FAIL | All four controls produced their expected engineering outcomes. Artifact metadata returned HTTP 404 after upload; the sealed ZIP survived and independent local replay reproduced every control. Rounds 576 and 582. |
| [45983781724](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45983781724) | `e6b1f20` | `rehearsal` | PASS | Three matrix jobs checkpointed all 15 grid cells and all three continuous episodes, exercised checked expiry and verified reset, and passed teardown. The dependent replay and 26,678-identity fixture passed. Rounds 581 and 589. |
| [45986168460](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45986168460) | `fc8119b` | `controls` | PASS | The hosted receipt logged HTTP 200 for metadata and ZIP on their first attempts and included the independent control replay. Round 589. |

The hosted controls replay verified the exact +2 s fixture shift and approximately +2 s paired execution shift. Removing a terminal reported its missing identity; substituting another workflow's terminal to preserve the count produced `integrity_stop`. Healthy low load passed all three gates and quiet. The capped sender recorded three capacity misses, two elapsed send deadlines, zero shedding and 717 reconciled admissions. Stock K4 reported `FAIL` and J2 reported `sender_invalid` for that control. Round 589. [Hosted controls attempt](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45986168460)

The integrated rehearsal used a 15 s warm-up plus 60 s per grid cell, then 15 s warm-up, 115 s at AB and a 60 s C step. All jobs independently returned `integrity_stop`, `no_eligible_rate` and finally `would_start` at AB in the three synthetic eligibility rounds. Job 1 checkpointed the labelled B quiet-visibility expiry and reset before BC. All three continuous episodes reconciled 2,772 admissions, verified two hold identities crossing the step, and completed the quiet drain. The frozen replay report verifies decision agreement, all 18 checked episodes, the historical qualification archive at 26,678 identities, and a fresh 26,678-identity full-volume check. The fixture's live and offline checker processes took 22.09 s and 6.77 s. Round 589. [Independent replay artifact](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45983781724/artifacts/15194491405)

The replay ZIP matched its Actions SHA-256 `314ddd2b87d4894b743ccbb4ac2d8b41da626e73ddc775d0f1a7e003010fcef0` and 300,540,259-byte size. Actions recorded expiry at 2027-02-25T10:38:19Z.

## Findings at closeout

| Finding | Final status | Resolution and evidence |
| --- | --- | --- |
| R410 | VERIFIED | `968d5d7` restored 100 ms sleep, removed the Kafka pause and added relay attribution. `10eb707` verifies a short crossing from a valid before-step read, checked terminal and clock bracket, while retaining after-step reconciliation. The recovered 8/s identity ended 21.93 ms after the step with a 0.568 ms bracket. The integrated run confirmed two crossings per job. Rounds 566–568, 577, 580 and 589. |
| R411 | OPEN, deferred | No resolving commit in J2. Round 568 traced the tail to one batch per wakeup and approximately 10 publications/s once workflow activity stopped. After J2 slice 3, loop while a pass claims a full batch, evaluate dispatch priority, add unit tests, then run the separately registered before/after comparison against the J2 baseline. |
| R412 | VERIFIED | `71ce57e` records a bracketed locked-checker cut on expiry. Checked residuals produce `drain_failure` and integrity violations retain precedence. The integrated B fixture showed checkpoint, verified reset and continuation. Rounds 568, 573 and 589. |
| R413 | VERIFIED | `71ce57e` requires every acknowledged workflow in the checked expiry cut. The missing-acknowledgment integrity branch is covered locally. The hosted checked expiry exercised the complete-acknowledgment path. Rounds 569 and 573. |
| R414 | VERIFIED | `71ce57e` replaced the protocol's local review link with a plain-text round citation. Rounds 569 and 572. |
| R415 | VERIFIED | `71ce57e` uses checker exit peak RSS with sampled VmHWM as corroboration. Resource-evidence gaps are `invalid` and checker violations remain `integrity_stop`. The hosted sub-second checker passed with one observer sample and absent VmHWM. Rounds 570 and 573. |
| R416 | VERIFIED | `71ce57e` restored the registered qualification mapping and offline exit-1 verdict comparison. `checker_mismatch` and `missing_identity` remain capacity outcomes for qualification. Code and regression tests verified the restoration. Rounds 571–572. |
| R417 | OPEN, J3 | `dfdfb5a` implements the accepted recovery-tail amendment on the J3 branch. Hosted calibration and confirmation are still required for that finding. Rounds 575, 578–579 and 583. |
| R418 | VERIFIED | `fc8119b` bounded metadata and ZIP retries to HTTP 404 and 5xx. The hosted controls re-run produced the receipt and independent replay, with both lookups succeeding on the first attempt. The retry branches were exercised locally. Rounds 582 and 589. |

## 2026-11-26 protocol clarification

- A qualified soak requires a valid hold, passing gate, complete evidence and a passing episode drain. On a drain failure, publish the independently verified in-window numerator over 1,800 s and bounded latency as diagnostic observations alongside the step and drain verdicts and residuals. Withhold the qualified-soak sentence.
- A checked grid drain expiry is a `drain_failure` for that cell. Preserve the checked evidence, verify a fresh reset and continue. A job or step budget overrun follows the stop rules. Integrity stops block all soaks.
- Attribute relay publication-limited backlog only when the registered gate and sustained-growth criteria establish saturation and publication lag coincides with the growth. Retain the durable `published_at` timeline, relay CPU/throttling, Kafka offsets and relay and scheduler logs.

Round 568. [Registered clarification](protocol.md#2026-11-26-pre-measurement-clarification)

## Engineering timing ledger

Drain and export times use the scheduled stop as time zero. A successful drain includes quiet and the live check. On expiry the verdict is set at 600 s and the checked export runs in the separate closeout reserve.

| Run | Case | Drain verdict and time | Through checked export | Export overhead beyond 600 s |
| --- | --- | --- | ---: | ---: |
| 45937621081 | Full-clock grid shape at 1/s, 720 identities | PASS, 36.99 s | 3.64 s | — |
| 45946992943 | Full-clock grid shape at 1/s, 720 identities | PASS, 37.62 s | 4.24 s | — |
| 45959322570 | Full-clock grid shape at 1/s, 720 identities | PASS, 33.87 s | 2.68 s | — |
| 45968932565 | Full-clock grid shape at 1/s, 720 identities | PASS, 33.84 s | 2.69 s | — |
| 45959322570 | Full-clock step episode, 14,980 identities | Checked expiry, 600.00 s | 624.51 s | 24.51 s |
| 45968932565 | Full-clock step episode, 22,660 identities | Checked expiry, 600.00 s | 637.01 s | 37.01 s |
| 45983781724 | Job 1 B, short-clock quiet-visibility fixture, 1,071 identities | Checked expiry, 600.00 s | 602.95 s | 2.95 s |

The short-clock integrated grid's longest natural drain was C on job 3 at 432.90 s. The 600 s B fixture expired on quiet visibility with zero unpublished rows and all 1,071 succeeded. The full-clock expiry overhead measurements came from real residual backlogs. Rounds 566, 568, 573, 577 and 589. [Integrated replay artifact](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45983781724/artifacts/15194491405)

| Integrated job | Job wall time | Ready for first grid from job start | From barrier anchor to soak setup | Final drain |
| --- | ---: | ---: | ---: | ---: |
| [Job 1](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45983781724/job/140454058046) | 51m 32s | 152 s | 62 s | 273.43 s |
| [Job 2](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45983781724/job/140454058468) | 51m 09s | 144 s | 665 s | 242.24 s |
| [Job 3](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45983781724/job/140454058489) | 52m 47s | 171 s | 238 s | 316.49 s |
| [Dependent replay](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45983781724/job/140479060528) | 6m 15s | — | — | — |

The dependent job used 175 s for archive and qualification replay and 146 s for the bulk-loaded volume check. In the barrier-only success fixture, the longest eligible decision took 42.74 s and the matrix jobs took 126–153 s wall. The peer-failure observers stopped in 32.89–34.08 s. The missing-peer observers waited the full 2,700 s; those two jobs each took 2,713 s wall and the withholding job took 2,718 s wall. Round 581. [Barrier success](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45971780256) · [Peer failure](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45971784262) · [Missing peer](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45974446414)

## Slice 3 per-job re-forecast

**The forecast fits at 292 min per live job, with 58 min remaining against the 350 min cap.** It uses the slowest relevant engineering observations, checks them against the existing allowances, and keeps the registered full-clock windows and barrier. The short-clock integrated wall times exercise the paths. The larger qualification and full-clock expiry evidence set the operational comparisons below.

| Stage | Protocol (min) | Re-forecast (min) | Timing check and margin within the allowance |
| --- | ---: | ---: | --- |
| Bootstrap | 25 | 25 | Slowest integrated job reached profile readiness in 171 s, leaving 22m 09s. |
| Five grid arrival windows | 60 | 60 | Five fixed 120 s warm-ups and 600 s windows. |
| Five grid drains and live checks | 50 | 50 | Reserve five full 600 s drains. Expect B, BC and C may use the full allowance. Charge any post-expiry export below. |
| Grid exports, verified resets and pre-soak setup | 40 | 40 | For each cell use rounded independent maxima of 145 s reset, 77 s first probe/install, 38 s checked expiry overhead, 74 s qualification export-to-verified-upload proxy and 25 s for lead and other composite-step work. The integrated overhang was at most about 24 s after subtracting the fresh setup, install, arrivals and drain/export. The 359 s cell envelope fits within 480 s. Five such envelopes plus 143 s pre-soak setup and 18 s of barrier setup/retention slack total 32m 36s, leaving 7m 24s. |
| Cross-job barrier | 45 | 45 | Longest integrated barrier took 665 s. The missing-peer fixture used the entire registered wait, so reserve all 45 min. |
| Soak warm-up, hold and C step | 37 | 37 | Fixed 2 min warm-up, 30 min hold and 5 min at C. Reserve the conditional episode in each job. |
| Final drain and live check | 10 | 10 | Reserve all 600 s. Full-clock rehearsal expiry is the controlling timing case. |
| Final export, teardown and upload | 25 | 25 | Allow 38 s post-expiry export, 74 s for the larger qualification replay/upload proxy, 61 s integrated teardown plus final index upload, and 25 s for lead and other episode overhead. The comparison totals 3m 18s, leaving 21m 42s. |
| **Total** | **292** | **292** | **58 min against the 350 min cap.** |

The qualification proxy is the 73.7 s from live export to verified upload at 26,678 identities, with a 241,102,239-byte ZIP. Its live export completed at 94.3 s after the scheduled stop. The separately hosted dependent replay's 6m 15s wall time sits outside the three live job forecasts. The forecast reserves time to record a checked failure, with any full-rate grid drain, saturation boundary or qualified-soak outcome left to slice 3. [Protocol forecast rule](protocol.md#stop-rules-time-and-evidence) · [Full-volume timing evidence](slice-1b-qualification.md#slice-1c-registered-qualification)
