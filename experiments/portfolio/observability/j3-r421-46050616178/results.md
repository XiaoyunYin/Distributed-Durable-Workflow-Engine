# J3 gated harness qualification — 46050616178

**Slice 2b qualification PASS.** Claude reviewed calibration and confirmation in Round 607 at `2a3451a2a10b8e002086ff1ba69f40f6d369733a` (PR #94). Both jobs ran on attempt 1. Calibration passed five episodes, and confirmation on a fresh runner passed matched-healthy and full-volume. All seven episodes passed both behavior and receiver delivery. All seven senders exited zero and both jobs reported clean teardown. The [registered protocol](../protocol.md) reserves the public claim and claim-map update for slice 3 after Claude reviews the registered campaign.

## Deadline cohort and attainment

The **rebuilt registered profile, admission disabled**, ran on single-node kind on one shared GitHub-hosted runner per stage, with PostgreSQL and Kafka colocated. It offered **7.136100570066256 short workflows/s**. The frozen deadline **L = 2.854 s** runs from scheduled arrival. The SLI denominator is the accepted target workflow identities whose deadlines have passed. This includes durable admissions if an HTTP acknowledgement was lost. The numerator is durable successes whose final-history time plus the larger of the setup and closing clock brackets was at or before the deadline. The fixture objective is **99% on time and zero incomplete** at reconciliation, with a **1% error budget**. An empty cohort gives an undefined ratio.

DeadlineBudgetBurn uses the **scaled 2 min and 10 min windows**. Both must have at least 100 eligible deadlines and exceed 14.4× the 1% budget (14.4% bad) to fire. These windows describe the short fixture. The following episode-wide counts come from the published-attainment replay. Alert and delivery verdicts are separate from attainment. Late successful workflows still consume budget.

| Episode | On time / eligible | Incomplete | Setup / closing bracket (ms) | Changed classifications using larger bracket |
| --- | ---: | ---: | ---: | ---: |
| Foreign | 8,564 / 8,564 | 0 | 56.393 / 65.866 | 0 |
| Outage-1 | 10,734 / 15,843 | 0 | 57.516 / 70.309 | 0 |
| Outage-2 | 10,741 / 15,843 | 0 | 59.881 / 77.038 | 1 |
| Outage-3 | 10,735 / 15,843 | 0 | 61.983 / 73.039 | 2 |
| Scrape-loss | 5,138 / 5,138 | 0 | 58.827 / 61.784 | 0 |
| Matched-healthy | 19,268 / 19,268 | 0 | 58.657 / 69.561 | 0 |
| Full-volume | 14,023 / 19,268 | 0 | 61.922 / 84.181 | 0 |

Each episode had zero committed timeout keys and zero visibility-ambiguous alert evaluations. The raw durable latencies and per-identity evidence are in the Actions archives. Episode counts, brackets, fault timestamps, and replay transitions are retained in [episodes.json](episodes.json).

## Alerts and receiver delivery

Times are seconds after the **actual scale-down command** for fire and after the **actual restore command** for clear, or after the **scrape cut** and **re-open** commands in scrape-loss. Each cell gives Prometheus transition → matching receiver receipt. The outage commands were 300 s apart to millisecond precision. Zero workers were witnessed 601.876, 602.340, 601.962, and 602.008 s after origin in outage-1/2/3 and full-volume respectively, before the +660 s bound. Ready slots and new durable progress were witnessed by +960 s in all four. Exact evidence includes the fault time, transition, fingerprint, receiver `startsAt`/`endsAt`, and receipt time. All measured receipts arrived within 30 s of their transitions. [Alert state and rule health](alert-state.json) · [Episode and fault timestamps](episodes.json) · [Calibration verdict](calibration/verdict.json) · [Confirmation verdict](confirmation/verdict.json)

| Episode | Alert | Fire → receipt (s) | Clear → receipt (s) |
| --- | --- | ---: | ---: |
| [Foreign](notifications/foreign.json) | Both domain alerts and ScrapeAbsent | No fire or receipt | No clear or receipt |
| [Outage-1](notifications/outage-1.json) | OldestClaimable | 34.246 → 34.260 | 479.246 → 479.702 |
| Outage-1 | DeadlineBudgetBurn | 94.246 → 94.256 | 524.246 → 524.732 |
| [Outage-2](notifications/outage-2.json) | OldestClaimable | 31.246 → 31.254 | 476.246 → 476.650 |
| Outage-2 | DeadlineBudgetBurn | 91.246 → 91.255 | 526.246 → 526.677 |
| [Outage-3](notifications/outage-3.json) | OldestClaimable | 29.246 → 29.255 | 469.246 → 469.729 |
| Outage-3 | DeadlineBudgetBurn | 94.246 → 94.259 | 524.246 → 524.763 |
| [Scrape-loss](notifications/scrape-loss.json) | OldestClaimable and DeadlineBudgetBurn | No fire or receipt | No clear or receipt |
| Scrape-loss | ScrapeAbsent | 7.246 → 7.279 | 2.246 → 2.291 |
| [Matched-healthy](notifications/matched-healthy.json) | Both domain alerts and ScrapeAbsent | No fire or receipt | No clear or receipt |
| [Full-volume](notifications/full-volume.json) | OldestClaimable | 33.246 → 33.257 | 488.246 → 488.743 |
| Full-volume | DeadlineBudgetBurn | 93.246 → 93.254 | 543.246 → 543.752 |

ScrapeAbsent stayed inactive after origin in all four worker outages. Outage-2 retained a setup-only ScrapeAbsent incident whose fire and resolve receipts arrived 33.747 and 28.747 s before origin. OldestClaimable's first valid clear inputs after restore occurred at **364.246, 361.246, 354.246, and 373.246 s** in outage-1/2/3 and full-volume respectively. The later alert clears in the table include the 24-evaluation hold. Neither domain alert re-fired in these four outages. On the registered images the clear time is governed by outbox relay drain (R411) and would change if R411 were fixed.

## Controls and full volume

- **Unrelated namespace.** The synthetic foreign writer seeded 2,141 workflows. A simultaneous positive witness found 1,726 claimable attempts and an oldest age of 241.787 s, with 856/856 and 1,726/1,726 bad deadlines in the 2 min and 10 min windows. Foreign claims, revision changes, and outbox rows were all zero. The target stayed quiet, and excluding foreign rows left its slot occupancy and 8,564/8,564 on-time count identical. This is a namespace alert and dispatch isolation check for the seeded fixture. [Foreign isolation projection](episodes.json) · [Empty measured delivery pairs](notifications/foreign.json)
- **Absent scrape.** The 60 s cut produced 12 invalid evaluations. Only ScrapeAbsent fired and resolved. Domain state during the gap was unknown for verdicts, and no domain clear was credited during it. Both deliveries paired to real rule transitions. This run retained zero in-flight scrape pairings. The raced-cut path has fixture coverage from the earlier run. [Measured alert history](alert-state.json) · [Receiver deliveries](notifications/scrape-loss.json) · [R421 fixture](../fixtures/r421-scrape-cut.json)
- **Matched healthy.** The same 2,700 s offer schedule and 3,300 s observation and drain bound as full-volume produced 19,268 accepted workflows. Neither domain alert nor ScrapeAbsent fired. There were no paired measured receiver deliveries. [Healthy episode and sealed bounds](episodes.json) · [Measured delivery pairs](notifications/matched-healthy.json)
- **Full-volume reconciliation.** The independent checker matched all **19,268 admitted identities** to **19,268 checked successes** and the expected results, with zero missing, duplicated, or unexpected identities. The drain recorded zero active claims and unpublished rows and 30.485 s of continuous quiet. The locked checker exited zero under `GOMEMLIMIT=1200MiB`. It ran for 22.226 s, with peak RSS 993,636 KiB and observer peak 1,094,004,736 bytes against 2 GiB. These are qualification resource observations at the sealed volume. [Identity reconciliation](full-volume/identity-reconciliation.json) · [Quiet drain](full-volume/identity-drain.json) · [Checker pin](full-volume/checker-binary.json) · [Resource receipt](full-volume/checker-resources.json)

## Recovery and sealed schedule

Each restored rate counts all target durable successes from the actual restore command to the earlier of +540 s or the last terminal commit among the pre-restore admissions. Rᵢ uses the later of that terminal commit and the hold-completing OldestClaimable clear while offers continue.

| Calibration outage | Restored rate (successes/s) | Last pre-restore admission terminal (s after restore) | Rᵢ (s) |
| --- | ---: | ---: | ---: |
| Outage-1 | 16.752151 | 153.533 | 479.245852 |
| Outage-2 | 16.885697 | 152.614 | 476.245848 |
| Outage-3 | 16.764116 | 152.826 | 469.245908 |

The median **μ = 16.764116/s** gives **T = 222.354251 s**. The strict recovery-tail term is **2 × max(Rᵢ) + 5 = 963.491705 s**. The smallest registered 60 s multiple satisfying both bounds is **Δ = 480 s**, with 540 + Δ = 1,020 s. Confirmation verified and used the seal at **19,268 arrivals**, from a baseline of 15,843. Bounds after episode origin were +1,920 s exclusive catch-up, +1,930 s oldest clear, +2,050 s burn clear, [2,100, 2,700) s healthy window, +2,700 s offer stop, and +3,300 s drain and observation end. Full-volume's oldest and burn cleared at +1,388.246 and +1,443.246 s respectively. Calibration and confirmation forecasts were **293m05s** and **180m02s** against 300 min each. The measurement forecast was **242m04s** against 350 min. [Sealed schedule projection](seal/sealed-schedule-summary.json) · [Seal receipt](seal/schedule-seal.json) · [Confirmation digest check](seal/confirmation-check.json) · [Measurement forecast](confirmation/measurement-forecast.json)

## Runner gate and history

The 120 s pre-bootstrap **idle I/O-full gate** admits strictly below **X = 0.5%**. Both stages in this run were admitted on stage attempt 1. Both runners advertised Intel Xeon 6975P-C, four online cores, and image `20261122.23.1`. Confirmation used a fresh runner.

| Dispatch | Stage and attempt | Idle I/O-full share | Gate outcome |
| --- | --- | ---: | --- |
| 45993299758 | Calibration 1 | 0.064623% | [Admitted](gates/45993299758-attempt-1.json) |
| 46016570882 | Calibration 1 | 10.768440% | [NOT_ARMED](gates/46016570882-attempt-1.json), no episode outcome |
| 46016570882 | Calibration 2 | 0.055751% | [Admitted](gates/46016570882-attempt-2.json) |
| 46050616178 | Calibration 1 | 0.078347% | [Admitted](gates/calibration.json) |
| 46050616178 | Confirmation 1 | 0.006842% | [Admitted](gates/confirmation.json) |

Across all gated dispatches through this qualification, **1 of 5 candidate stages was rejected** (four calibration candidates and one confirmation candidate). This run rejected **0 of 2**. The earlier rejection's [idle trace](gates/46016570882-attempt-1-idle-pressure.jsonl) is retained. The original baseline exceeded the fixed cutoff on **2 of 10** runners at **5.7% and 9.0%**. Its registered formula yielded **27.1%**, superseded before use. The cutoff was amended after that baseline and before any gated run. Its new value also excluded the first baseline's AMD EPYC 9V74 runner, the host class of the ungated FAIL. The independent validation baseline had **6 of 20** exceedances and **14 of 20** admissions, passing the frozen 60% criterion. CPU models are covariates. The two baseline records retain every reading and its CPU and image. [Original baseline](gates/original-baseline.json) · [Superseded candidate](gates/superseded-candidate.json) · [Validation baseline](gates/validation-baseline.json)

**In-arm pressure is a published covariate only.** The following shares use `/proc/pressure` counter deltas between the first and last runner samples inside each offer interval, divided by elapsed monotonic time. The interval and full trace locator for each row are indexed.

| Episode | I/O full (%) | CPU some (%) |
| --- | ---: | ---: |
| Foreign | 0.346 | 11.258 |
| Outage-1 | 0.560 | 18.366 |
| Outage-2 | 0.559 | 19.284 |
| Outage-3 | 0.554 | 19.392 |
| Scrape-loss | 0.365 | 10.974 |
| Matched-healthy | 0.326 | 13.531 |
| Full-volume | 0.524 | 19.372 |

### Preserved earlier outcomes

| Run | Preserved outcome |
| --- | --- |
| [45915019609](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45915019609) | Six alert episodes PASS, full-volume checker CAPACITY. |
| [45942670753](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45942670753) | Six alert episodes PASS, full-volume recovery-tail FAIL. |
| [45980467098](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45980467098) | Ungated qualification FAIL, 8,456/8,564 on time (98.74%) in foreign, zero incomplete. Its 4.82% pre-origin I/O-full included setup and is informational against the 0.5% idle cutoff. Remaining episodes were UNPERFORMED and there was no seal. |
| [45993299758](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45993299758) | Armed behavioral FAIL from the original rule's re-fire, with the harness's emitted INVALID preserved alongside it. Outage-3 and scrape-loss were UNPERFORMED, with no seal or confirmation. |
| [46016570882](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46016570882) | Attempt 1 NOT_ARMED. Attempt 2 passed foreign and all three outages, then scrape-loss was INVALID on the old pairing logic. No seal or confirmation. |

## Scope and limitations

This is a **rebuilt registered profile** harness qualification on the named shared runners. Results do not cover hosts at or above the gate. The shared PostgreSQL gives no I/O isolation between namespaces, so target attainment on a shared runner depends on host I/O. The fault-arm counts describe this deliberate worker outage, and the healthy counts describe their registered controls. These results cover neither node/storage failure nor dependency failover. Slice 3 measurement and claim review, and full K8 acceptance, are separate gates. Round 607 checked the hosted replays and selected inputs. The publication verified archive and member digests and cross-checked the retained summaries. It did not independently rerun the complete durable-history replays. R396 (pre-episode job-time guard) remains open and non-blocking.

## Dashboard, alert state, and artifact provenance

The foreign control captured the live Grafana API definition and datasource metadata, and 17 live range-query results, plus source times joined to retained exporter samples. Alert-state evidence preserves the five-second evaluation grid, invalid evaluation times, and inclusive runs of firing samples for all three alerts in each episode. Together with the precise receiver deliveries, these records show active-to-inactive transitions and distinguish silence from missing telemetry. [Grafana API capture](dashboard/dashboard-api.json) · [Panel query results](dashboard/dashboard-ranges.json) · [Panel source times](dashboard/dashboard-source-times.json) · [Alert state](alert-state.json)

The [SHA-256 index](sha256.json) records every retained machine-readable file's hash, length and source. Entries from Actions map each source member to its artifact ID and verified ZIP digest. `archive-member` entries preserve the original bytes. For `projection` entries, the source hash is over the original archive member, and the output hash is over the retained projection. The schedule projection replaces identity lists with counts, and the attainment projection retains counts and clock brackets while the original `replay.json` holds per-identity latencies. A `release-asset` entry preserves the published Release index byte-for-byte. The full archives retain the manifests, raw Prometheus matrices, visibility stream, worker witnesses, journals, explicit teardown evidence, and other inputs independently of these summaries. Actions artifacts for this run expire after **90 days**, at `2027-02-25T18:35:31Z` per the API.

| Actions artifact ID | Name | Size (bytes) | ZIP digest |
| --- | --- | ---: | --- |
| [15223759432](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46050616178/artifacts/15223759432) | `j3-runner-gate-calibration-46050616178-1-admitted` | 4,348 | `sha256:9640cbc6180f82f070cc0d9d1b50db325e68e8c7cf4f97553a07d7155813e4a6` |
| [15235390969](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46050616178/artifacts/15235390969) | `j3-runner-gate-confirmation-46050616178-1-admitted` | 4,395 | `sha256:b650e03ab461e235a23c522489fd45cd7a32e31699ec0e526cfc769eca68c0c7` |
| [15235740301](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46050616178/artifacts/15235740301) | `j3-calibration-46050616178-1` | 130,964,790 | `sha256:e9e2b2b42e45c48c98424d90e422b98b2c173588cfc031b58a82ac896e47a406` |
| [15235825154](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46050616178/artifacts/15235825154) | `j3-schedule-seal-46050616178-1` | 76,391 | `sha256:0b6234659285790064d3d3d78855e34d3b3485deb13c4aefa65ec481df26ff79` |
| [15239329022](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46050616178/artifacts/15239329022) | `j3-confirmation-46050616178-1` | 147,045,973 | `sha256:84bd1624a83d96071af876ade0ec9e0c5cfbff3ecb54d8760e473e5f44625465` |

### Durable copy

The public [qualification evidence Release](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/releases/tag/j3-r421-46050616178-evidence) targets `2a3451a2a10b8e002086ff1ba69f40f6d369733a` and contains byte-for-byte copies of the five Actions ZIPs above. The publisher verified the parts by download and reassembly. The retained [Release index](j3-r421-46050616178-index.json) has SHA-256 `7cdae1077e6665f3e7e2bf8355c16b09d5be8ec38c933af4edf82d7d5773440f`. All five ZIP sizes and SHA-256 digests in that index match the Actions records.

| ZIP | Release asset | Size (bytes) |
| --- | --- | ---: |
| Calibration | `j3-calibration-46050616178-1.zip.part-001-of-002` | 67,108,864 |
| Calibration | `j3-calibration-46050616178-1.zip.part-002-of-002` | 63,855,926 |
| Confirmation | `j3-confirmation-46050616178-1.zip.part-001-of-003` | 67,108,864 |
| Confirmation | `j3-confirmation-46050616178-1.zip.part-002-of-003` | 67,108,864 |
| Confirmation | `j3-confirmation-46050616178-1.zip.part-003-of-003` | 12,828,245 |
| Schedule seal | `j3-schedule-seal-46050616178-1.zip` | 76,391 |
| Calibration gate | `j3-runner-gate-calibration-46050616178-1-admitted.zip` | 4,348 |
| Confirmation gate | `j3-runner-gate-confirmation-46050616178-1-admitted.zip` | 4,395 |

After downloading the assets, check each asset's length and SHA-256 against the Release index. Use the three single-file ZIPs directly. For the split ZIPs, concatenate the raw bytes in the index's `parts` order.

```sh
cat j3-calibration-46050616178-1.zip.part-001-of-002 \
    j3-calibration-46050616178-1.zip.part-002-of-002 \
    > j3-calibration-46050616178-1.zip
cat j3-confirmation-46050616178-1.zip.part-001-of-003 \
    j3-confirmation-46050616178-1.zip.part-002-of-003 \
    j3-confirmation-46050616178-1.zip.part-003-of-003 \
    > j3-confirmation-46050616178-1.zip
sha256sum j3-calibration-46050616178-1.zip j3-confirmation-46050616178-1.zip
```

Compare the reconstructed sizes and hashes with the Actions ZIP table before opening them.
