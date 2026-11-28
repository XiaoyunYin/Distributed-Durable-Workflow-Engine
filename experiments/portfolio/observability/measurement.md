# J3 registered campaign runbook

Slice 3 uses the [reviewed slice 2b qualification](j3-r421-46050616178/results.md). Protocol 2026-11-27 requires sealing before confirmation and measurement. The measurement job uses the passing seal from run 46050616178, attempt 1. It checks the original ZIP SHA-256, the inner schedule hash, the qualification campaign and head, the passing confirmation's seal receipt, and the confirmed forecast. Qualification and measurement use distinct run-derived campaign IDs. The measurement ID is `j3-r421-<measurement Actions run ID>`.

## Before dispatch

Claude reviews the exact committed measurement head and its green J3 tooling checks. Check earlier gate artifacts and apply the cross-dispatch review in the [protocol](protocol.md#2026-11-27-runner-gate-cutoff-amendment) before any dispatch. A previous armed FAIL remains decisive for its campaign. The dispatch inputs record Claude's reviewed head and round. The workflow checks them against its head, and the reviewer supplies the applicable round from the canonical review log.

From `codex/j3-slice3`, with the reviewed commit already published on that ref, set `$reviewRound` to Claude's approval round. The command is for the reviewer to run once the prerequisites are satisfied.

```powershell
$head = (git rev-parse HEAD).Trim()
gh workflow run j3-observability.yml --repo XiaoyunYin/Distributed-Durable-Workflow-Engine `
  --ref codex/j3-slice3 -f mode=measurement -f "reviewed_head=$head" -f "review_round=$reviewRound"
```

The one `ubuntu-24.04` measurement job applies the existing 120 s idle gate before bootstrap and publishes its stage receipt. A rejection is `NOT_ARMED`, with no episode outcome. Use **Re-run failed jobs** only for the existing pre-arm retry path, up to three stage attempts. Any new dispatch remains subject to cross-dispatch review. The job restores the pinned OCI archives and checker and tests the pinned Prometheus rules before sending.

## Verified forecast and schedule

The seal is Actions artifact `15235825154`, ZIP SHA-256 `0b6234659285790064d3d3d78855e34d3b3485deb13c4aefa65ec481df26ff79`, also available in the [durable qualification Release](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/releases/tag/j3-r421-46050616178-evidence). It fixes Δ = 480 s and 19,268 arrivals in each matched arm. The primary and matched-healthy stops are +2,700 s with observation and drain through +3,300 s. The primary's catch-up, OldestClaimable clear and DeadlineBudgetBurn clear bounds are +1,920 s exclusive, +1,930 s and +2,050 s. The final healthy window is `[2100,2700)` s. Foreign and scrape-loss offer for 1,200 s and 720 s, each with its registered 600 s closeout interval.

| Component | Confirmed allowance |
| --- | ---: |
| Bootstrap | 25m00s |
| Primary and its export/reset | 65m01s |
| Matched healthy and its export/reset | 65m01s |
| Foreign and its export/reset | 40m01s |
| Scrape loss | 22m01s |
| Final replay, evidence, cleanup and upload | 25m00s |
| **Total** | **242m04s of 350m00s** |

The remaining headroom is **107m56s**. The job checks the confirmed allowance against elapsed time before bootstrap and rechecks the remaining allowance before each episode. It runs primary, matched-healthy, foreign and scrape-loss serially, with resets between episodes and verified cleanup at the end. An over-cap check halts before the next episode. Later episodes stay `UNPERFORMED` after an earlier stop.

## Evidence and publication review

The workflow uploads `j3-measurement-<run>-<attempt>` even after an episode or prerequisite failure. The separate `j3-runner-gate-measurement-<run>-<attempt>-<decision>` artifact publishes the idle trace and admission or rejection. The measurement bundle contains the reviewed-head receipt, original seal ZIP and verification, per-episode run/head/attempt and pre-arm hashes of the runner, pinned inputs, rendered configuration and installed plan, cutoff and budget checks, both live verdicts per episode, fault and timing records, manifests and pins, Grafana API and range results, a fault/restore annotation sidecar with exact timestamps, Prometheus matrices and rule health, raw receiver log and pairs, durable and worker evidence, independent checker receipts for both matched arms, cleanup, `offline-replay.json` and `sha256.json`. The SHA-256 index covers all files except itself. Retain the complete indexed evidence and its Actions artifact ID and digest under the campaign ID before publication.

After verifying the downloaded ZIP digest against Actions metadata, extract the bundle to a separate evidence directory. From the reviewed checkout, recheck the index and reproduce the offline report without cluster or database access.

```powershell
python scripts/j3_measurement.py replay <extracted-evidence-directory>
```

Claude's review before publishing the protocol sentence and claim-map row includes these checks.

1. Bind run, head, attempt and campaign ID to the reviewed candidate. Verify the Actions ZIP and member index, the gate receipt and prior attempts, the original seal and confirmation provenance, the confirmed forecast, pins and rule tests, and the private route, scope and denial receipts.
2. Reproduce offline alert-input and published-attainment replay and receiver pairing. Check every episode's behavior and delivery verdicts. Preserve any FAIL, INVALID, NOT_ARMED or UNPERFORMED outcomes and the full attempt history. Check completed teardown.
3. Read the primary's actual scale, zero-worker witness, restore, readiness and first durable progress, both fire/clear bounds and receipt times, first clear inputs and the hold-completing OldestClaimable clear. Check the recovery-backlog deadline record, sustained recovery and lack of re-fire. Read the Grafana range evidence with the fault/restore sidecar. A screenshot alone cannot establish a transition.
4. Check identity, outcomes and quiet drain for both matched arms. Confirm matched-healthy silence, the foreign fixture's simultaneous positive witness and zero dispatch with unchanged target slot use and attainment, and the separate scrape-loss incident with unknown domain state during the gap. Verify exact accepted, eligible, on-time and incomplete populations, both clock brackets and their sensitivity, and the raw durable latencies.
5. Use the [protocol's scoped sentence](protocol.md) with actual measurement population, elapsed origins and controls. State the rebuilt registered profile, admission disabled, single shared runner, registered deadline and scaled windows, and separate attainment from alert verdict. Disclose the 0.5% idle I/O-full gate, cumulative candidates and rejections, both validation baselines, the retained ungated FAIL and original-rule flap FAIL. Use CPU model and in-arm pressure as covariates. Disclose the shared PostgreSQL I/O limit, the cutoff amendment, and R411's effect on clear time. Scope the row to this campaign and review outcome.
