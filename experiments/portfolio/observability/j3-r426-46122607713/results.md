# J3 registered measurement — j3-r426-46122607713

[Actions run 46122607713](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46122607713),
attempt 1, measured head `28d4109eafb95514d03032baca3819e26bfa331a`, [PR #98](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/pull/98).
Round 632 authorized this head. Claude accepted the registered measurement in REVIEW.md Round 647.
All four episodes passed behavior and receiver checks. Offline replay and cleanup both passed.
[Run profile and pinned images](run.json) · [Dispatch authorization](review.json) · [Verdict](verdict.json) · [Offline replay](offline-replay.json) · [Cleanup](cleanup.json)

## Registered claim

> On single-node kind on one shared GitHub-hosted runner (rebuilt registered profile, admission disabled), at 7.136100570066256 offered workflows/s with 19,268 accepted short workflows, 13,302/19,268 succeeded within the registered 2.854 s deadline. During a 298.018 s confirmed worker outage, oldest claimable work and deadline-budget burn fired at 30.246 s and 95.246 s after scale-down and cleared at 575.246 s and 645.246 s after restore, respectively. The matched-healthy and unrelated-namespace controls fired neither alert, and the scrape-loss control fired and cleared only ScrapeAbsent. [Registered campaign j3-r426-46122607713](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/releases/tag/j3-r426-46122607713-evidence).

The runner passed the 120 s idle I/O-full gate at a strict 0.5% cutoff. Across the gated dispatches, 1 of 7 candidate stages was rejected before arming. On the registered images the clear time is governed by outbox relay drain (R411) and would change if R411 were fixed. [Protocol and amendments](../protocol.md) · [Gate history](#runner-gate-and-history)

## Deadline, cohort, and reconciliation

The deadline **L = 2.854 s** runs from scheduled arrival. The denominator is
accepted target workflow identities with matured deadlines. The numerator is
conservatively witnessed durable success by that deadline. Late success still
consumes the **1% error budget**, and incomplete accepted work is counted at
reconciliation. The fixture objective is 99% on time with zero incomplete.
The deliberately faulted primary reached **69.04%** on time and passed the alert
behavior and delivery checks. The **2 min recent / 10 min confirmation** burn
windows are the disclosed scaled form of 5 min / 1 h. Both require at least 100
eligible deadlines and burn strictly above 14.4× to fire.
[Registered SLI](../protocol.md#registered-sli-and-exporter-projection) · [Alert predicates](../protocol.md#three-alert-rules)

| Episode | Behavior / delivery | Accepted | On time / eligible | Incomplete | Ambiguous evaluations | Paired deliveries |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Primary | PASS / PASS | 19,268 | 13,302 / 19,268 | 0 | 0 | 4 |
| Matched-healthy | PASS / PASS | 19,268 | 19,268 / 19,268 | 0 | 0 | 0 |
| Unrelated namespace | PASS / PASS | 8,564 | 8,564 / 8,564 | 0 | 0 | 0 |
| Scrape-loss | PASS / PASS | 5,138 | 5,138 / 5,138 | 0 | 0 | 2 |

The primary and matched-healthy arms each reconciled all 19,268 admissions
against the independent checker and final successes, with zero missing,
duplicate or unexpected identities. All four arms recorded at least 30 s of
quiet drain with zero active claims and unpublished work. Offline replay also
reported zero committed timeouts and zero clock-width sensitivity in each
episode. [Primary identity reconciliation](primary/identity-reconciliation.json) · [Matched-healthy identity reconciliation](matched-healthy/identity-reconciliation.json) · [Episode evidence](episode-evidence.json)

## Primary outage, recovery, and deliveries

The scale-to-zero command started at **08:04:17.000153880 UTC** on
2026-11-28. The worker witness confirmed zero workers at **08:04:18.982173409
UTC**. The scale-to-two command started at **08:09:17.000144153 UTC**. This is
**298.017970744 s** from confirmed zero workers to the restore command, with
**299.999990273 s** between the two commands. The two restored workers had new
UIDs. Alert offsets below subtract the actual command timestamps from the
Prometheus transition timestamps. Receiver delays are measured from the
Prometheus events, with the 30 s bound applied to every pair.
[Fault commands](primary/faults.jsonl) · [Worker witness](primary/outage-witness.json) · [Primary timing](primary/timing-summary.json) · [Primary notification pairs](primary/notification-pairs.json)

| Alert | Transition | Prometheus UTC | Time after command (s) | Time after origin (s) | Bound from origin (s) | Receiver UTC | Receiver delay (s) |
| --- | --- | --- | ---: | ---: | ---: | --- | ---: |
| OldestClaimable | Fire | 08:04:47.246 | 30.246 after scale-down | 630.246 | ≤700 | 08:04:47.261 | 0.015 |
| DeadlineBudgetBurn | Fire | 08:05:52.246 | 95.246 after scale-down | 695.246 | ≤765 | 08:05:52.261 | 0.015 |
| OldestClaimable | Clear | 08:18:52.246 | 575.246 after restore | 1,475.246 | ≤1,930 | 08:18:52.766 | 0.520 |
| DeadlineBudgetBurn | Clear | 08:20:02.246 | 645.246 after restore | 1,545.246 | ≤2,050 | 08:20:02.809 | 0.563 |

The first valid oldest clear input was **460.246 s after restore**. The
registered 24-evaluation hold completed 115 s later on the five-second grid,
and the transition aligned with the 24th evaluation. The first qualifying
burn clear input coincided with its clear at **645.246 s after restore**.
Both alerts fired at the first qualifying evaluation and remained firing at
restore. Both stayed clear during the final healthy window and through
observation closeout. ScrapeAbsent stayed inactive.
[Actual clear inputs](primary/actual-clears.json) · [Held clear rule](../protocol.md#2026-11-27-clear-hold-amendment) · [Alert state](alert-state.json)

| Recovery check | Actual from origin | Registered bound from origin |
| --- | ---: | ---: |
| Scale down / restore | +600.000154 s / +900.000144 s | +600 s / +900 s, each within 5 s |
| Confirm zero workers | +601.982 s | ≤660 s |
| Two Ready workers / first subsequent durable progress | +906.081 s / +920.062 s | ≤960 s |
| Last pre-restore admission terminal | +1,075.183 s | <1,920 s |
| First valid below-threshold oldest input | +1,360.246 s | <1,920 s |
| Final healthy window / offer stop | [2,100, 2,700) s / +2,700 s | Registered interval / +2,700 s |
| Primary quiet drain complete | +2,732.272 s | ≤3,300 s |
| Observe through | +3,300 s | +3,300 s |

The last pre-restore backlog observation had 2,134 unfinished target
admissions. The last bad deadline from that cohort was +902.785 s from
origin. The full timing verdict in the Actions ZIP retains the subsequent
window counts. [Primary timing summary](primary/timing-summary.json)

## Controls

- **Matched-healthy.** The same 19,268-arrival schedule ran with two workers
  at the same stop and observation bounds. Neither domain alert nor
  ScrapeAbsent fired. The receiver recorded heartbeats and zero deliveries.
  [Matched-healthy notification pairs](matched-healthy/notification-pairs.json) · [Alert state](alert-state.json)
- **Unrelated namespace.** The synthetic foreign DB fixture seeded 2,141
  workflows. The independent positive witness saw 1,741 claimable attempts,
  oldest age 243.897 s, and 856/856 and 1,741/1,741 bad deadlines in the 2 min
  and 10 min windows. It coincided with a valid quiet target evaluation.
  Foreign claims, outbox rows and post-seed revision changes were all zero.
  The four-slot target ledger and 8,564/8,564 target attainment agreed with
  the target-only replay. All three alerts stayed quiet during the episode
  and there were zero measured deliveries. A setup-only ScrapeAbsent
  incident fired at 09:55:22.246 UTC and cleared at 09:55:27.246 UTC. The
  receiver got the firing and resolved deliveries at 09:55:22.254 and
  09:55:32.258 UTC, both before the 09:55:57 UTC origin.
  [Foreign control witness and comparison](foreign/control-summary.json) · [Measured notification pairs](foreign/notification-pairs.json) · [Setup deliveries](foreign/setup-incidents.json)
- **Scrape-loss.** The real scrape path was cut and restored at +600.000155 s
  and +660.000173 s from origin, both within the 1 s actuation bound. There
  were 12 invalid evaluations inside the intended control. Domain state was
  unknown during the gap, with zero credited domain clears. Only ScrapeAbsent
  fired, at 10:39:32.246 UTC, **+8.246 s after the cut** (bound 5–20 s), and
  cleared on fresh data at 10:40:27.246 UTC, **+3.246 s after reopening**
  (bound 0–15 s). The receiver got the firing and resolved deliveries at
  10:39:32.269 UTC and 10:40:27.315 UTC, delays of 0.023 s and 0.069 s.
  Neither domain alert fired. [Scrape fault commands](scrape-loss/faults.jsonl) · [Scrape timing and bounds](scrape-loss/timing-verdict.json) · [Scrape-loss notification pairs](scrape-loss/notification-pairs.json) · [Alert state](alert-state.json)

## Sealed schedule and execution

Measurement verified the seal from the Round 607 qualification (run
46050616178, artifact 15235825154) and used it at **Δ = 480 s**. Calibration
restored rates were 16.752151, 16.885697 and 16.764116 successes/s, with
recovery tails of 479.245852, 476.245848 and 469.245908 s. The median
**μ = 16.764116/s** gives **T = 222.354251 s**. The 480 s extension brought the
15,843-arrival baseline to 19,268 arrivals over 2,700 s. The 242m04s forecast
fit the 350 min job cap. The live measurement harness recorded 181m28s
through cleanup, with zero remaining owned clusters, containers, nodes or
databases. [Verified schedule and seal](verified-schedule-summary.json) · [Qualification record](../j3-r421-46050616178/results.md#recovery-and-sealed-schedule) · [Cleanup receipt](cleanup.json)

## Runner gate and history

The pre-bootstrap gate admits strictly below **X = 0.5% idle I/O-full**
over the fixed 120 s window. Measurement attempt 1 was admitted at
**0.041630%** on Intel Xeon Platinum 8370C, four online cores, image
`20261122.23.1`. CPU models are covariates.

| Dispatch | Stage attempt | Idle I/O-full | Gate outcome |
| --- | --- | ---: | --- |
| 45993299758 | Calibration 1 | 0.064623% | [Admitted](../j3-r421-46050616178/gates/45993299758-attempt-1.json) |
| 46016570882 | Calibration 1 | 10.768440% | [NOT_ARMED](../j3-r421-46050616178/gates/46016570882-attempt-1.json), no episode outcome |
| 46016570882 | Calibration 2 | 0.055751% | [Admitted](../j3-r421-46050616178/gates/46016570882-attempt-2.json) |
| 46050616178 | Calibration 1 | 0.078347% | [Admitted](../j3-r421-46050616178/gates/calibration.json) |
| 46050616178 | Confirmation 1 | 0.006842% | [Admitted](../j3-r421-46050616178/gates/confirmation.json) |
| 46109429112 | Measurement 1 | 0.004986% | [Admitted](gates/46109429112-attempt-1.json) |
| 46122607713 | Measurement 1 | 0.041630% | [Admitted](gates/46122607713-attempt-1.json) |

The cumulative rejection count is **1 of 7 candidate stages**, with **0 of 1**
in this run. The first baseline had **2 of 10** exceedances at **5.7% and 9.0%**.
The initial formula gave 27.1%. The cutoff was amended to 0.5% after the first
baseline and before any gated run. That value also excluded the first
baseline's AMD EPYC 9V74 runner, the host class of the ungated FAIL. The
independent validation baseline had **6 of 20** exceedances and **14 of 20**
admissions, passing the frozen 60% criterion.
[Original baseline](../j3-r421-46050616178/gates/original-baseline.json) · [Validation baseline](../j3-r421-46050616178/gates/validation-baseline.json) · [Rejected candidate's idle trace](../j3-r421-46050616178/gates/46016570882-attempt-1-idle-pressure.jsonl)

**In-arm pressure is a published covariate only.** Each share uses the
`/proc/pressure` counter delta between the first and last runner samples
inside its offer interval, divided by elapsed monotonic time.
[Indexed episode evidence](episode-evidence.json)

| Episode | I/O full (%) | CPU some (%) |
| --- | ---: | ---: |
| Primary | 0.794 | 30.505 |
| Matched-healthy | 0.625 | 26.141 |
| Unrelated namespace | 0.732 | 20.699 |
| Scrape-loss | 0.754 | 17.222 |

## Preserved outcomes and limitations

| Run | Preserved outcome |
| --- | --- |
| [45980467098](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45980467098) | Ungated qualification **FAIL** (8,456/8,564 on time, 98.74%, zero incomplete). Its 4.82% pre-origin I/O-full included setup, so the figure is informational against the idle cutoff. |
| [45993299758](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45993299758) | Armed **FAIL** from the original oldest rule's re-fire. The harness's emitted INVALID is also preserved. The 120 s hold was registered in the subsequent clear-hold amendment. |
| [46109429112](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46109429112) | First registered measurement **INVALID**. Primary PASS, matched-healthy INVALID on the sub-millisecond scrape comparison (R426), foreign and scrape-loss UNPERFORMED, offline replay INCOMPLETE, cleanup PASS. [Archived verdict](prior-measurement/verdict.json) · [Archived replay](prior-measurement/offline-replay.json) · [Original artifact 15253411862](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46109429112/artifacts/15253411862) · [Replay clarification](../protocol.md) |

The [qualification history](../j3-r421-46050616178/results.md#preserved-earlier-outcomes)
retains the other qualification outcomes, including partial or unperformed
episodes. No prior verdict was rewritten.

This is one registered single-node kind campaign on one shared GitHub-hosted
runner, with hosts at or above the idle I/O-full cutoff outside its scope.
The shared PostgreSQL gives no I/O isolation between namespaces, so target
attainment on a shared runner depends on host I/O. The synthetic foreign
fixture tests alert scope and labels. On the registered images the clear
time is governed by outbox relay drain (R411) and would change if R411 were
fixed. Scope is worker outage and the registered controls at the tested
offered rate and cohort. Multi-host recovery, dependency failover and full
K8 acceptance are outside this campaign. Round 647 used the hosted offline
replay outputs. This record verifies artifact and summary provenance and
recomputes observed timings without rerunning the full durable replay.

## Dashboard, alert state, and artifact provenance

The campaign captured the live Grafana API definition, the private Prometheus
datasource, two fault annotations and 17 primary panel queries. The summary
records the returned series labels, counts and edges. The two diagnostic
takeover and stale-rejection query results were empty. The full archive
retains the panel values and source timestamps. The alert-state summary
preserves the five-second evaluation grid, missing or invalid intervals, and
inclusive firing runs for all episodes. [Grafana API](primary/dashboard-api.json) · [Fault annotations](primary/dashboard-annotations.json) · [Panel range summary](primary/dashboard-range-summary.json) · [Alert state](alert-state.json)

The [SHA-256 index](sha256.json) maps every retained machine-readable file to
its size, digest and source. `archive-member` entries preserve the member
bytes. `projection` entries record both the digest of the summary and
digests of its source members. `release-asset` preserves the Release index
byte-for-byte. The full archives retain the rendered manifests, rules,
raw samples, worker and visibility witnesses, schedule and HTTP journal,
durable history and receipts, teardown and complete replay inputs. After
checking or reassembling and extracting the measurement ZIP, run
`python scripts/j3_measurement.py replay <extracted-evidence-directory>`
from the campaign's measured checkout to verify the internal index and
offline report. Actions artifacts expire after 90 days. The Actions API
records the measurement ZIP and its gate expiry at `2027-02-26T07:44:50Z`.

| Actions artifact ID | Name | Size (bytes) | ZIP digest |
| --- | --- | ---: | --- |
| [15260625530](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46122607713/artifacts/15260625530) | `j3-measurement-46122607713-1` | 222,150,707 | `sha256:18898626626b0bd7144439d5965d98afd0910659d4eea58f1362c4e980709f3c` |
| [15254950390](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46122607713/artifacts/15254950390) | `j3-runner-gate-measurement-46122607713-1-admitted` | 4,389 | `sha256:79a34dad0939c1cb8497ce2acdc28eece424b788651dbccf10c2cfc69f1ab1c0` |

### Durable copy

The public [measurement evidence pre-release](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/releases/tag/j3-r426-46122607713-evidence)
targets `28d4109eafb95514d03032baca3819e26bfa331a` and contains byte-for-byte
copies of both measurement-run ZIPs. The retained [Release index](j3-r426-46122607713-index.json)
has SHA-256 `7275f268f2e1c8eae604ff6a6a59c024eb9e9de06ec974f41c4bf4d1da2945b5`.
Both ZIP sizes and digests in the index match the verified Actions downloads.

| Release asset | Size (bytes) |
| --- | ---: |
| `j3-measurement-46122607713-1.zip.part-001-of-004` | 67,108,864 |
| `j3-measurement-46122607713-1.zip.part-002-of-004` | 67,108,864 |
| `j3-measurement-46122607713-1.zip.part-003-of-004` | 67,108,864 |
| `j3-measurement-46122607713-1.zip.part-004-of-004` | 20,824,115 |
| `j3-runner-gate-measurement-46122607713-1-admitted.zip` | 4,389 |

Verify each asset's size and SHA-256 against the Release index. Use the gate
ZIP as downloaded. Concatenate the four measurement parts as raw bytes in
the index's order, then check the reconstructed size and digest above.

```sh
cat j3-measurement-46122607713-1.zip.part-001-of-004 \
    j3-measurement-46122607713-1.zip.part-002-of-004 \
    j3-measurement-46122607713-1.zip.part-003-of-004 \
    j3-measurement-46122607713-1.zip.part-004-of-004 \
    > j3-measurement-46122607713-1.zip
sha256sum j3-measurement-46122607713-1.zip j3-runner-gate-measurement-46122607713-1-admitted.zip
```
