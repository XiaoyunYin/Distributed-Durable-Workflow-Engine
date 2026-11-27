# J2 exact-head rehearsal and approval

The manifest binds frozen inputs. The candidate SHA is the commit that contains this packet. Record that full SHA in the engineering run, the written approval, and the later reviewer ledger. A commit cannot name its own SHA inside the manifest. The base SHA and committed hashes identify what can be verified before the candidate commit exists.

## Short-clock rehearsal

Use the helper from [measurement authorization](../measurement-authorization.md#engineering-dispatches) in the reviewed checkout. Use a fresh `eng-inc4-final` ledger and publish it only after identifying the exact attempt-1 run and confirming its head. Finish within the six-minute authorization wait.

```powershell
gh workflow run j2-load-envelope.yml --repo $repo --ref codex/j2-slice3 -f mode=measure -f campaign_id=eng-inc4-final
$runId = [long](Read-Host 'Exact new eng-inc4-final Actions run ID')
$run = gh run view $runId --repo $repo --json headSha,attempt,event,workflowName | ConvertFrom-Json
if ($run.headSha -ne $head -or $run.attempt -ne 1 -or $run.event -ne 'workflow_dispatch' -or $run.workflowName -ne 'J2 load-envelope tooling') { throw 'Unexpected workflow run or dispatch head.' }
Publish-J2EngineeringLedger 'eng-inc4-final' $runId
```

## Required review before authorizing measurement

1. Match every job and the replay to the exact candidate SHA and attempt. Confirm green candidate PR checks and the three `pre-dispatch-package.json`, authorization, runner inventory and exact-head controls receipts. Verify the qualification archive and its live availability, all four control verdicts and all 18 frozen seed strings. Confirm the rehearsal kept its engineering identity and produced no performance sentences.
2. Review each runner’s restored/pulled image IDs and rendered hashes, live readiness, Calico allowed/denied probes, cgroup pressure and before-workload fresh state. Verify the one-second exporter and Prometheus cadences, serialized two-second statement timeout, one exporter DB connection, zero Prometheus connections, and 82/100 with reserved/admin and 18 free. Match both sender builds and the locked checker to their install/hash evidence.
3. Review all five real grid cells in each registered order, including schedule bindings, identity/cut reconciliation, gate/drain classification, sample and error evidence, and any verified recovery or job stop. Check each grid publication receipt, the 45-minute-or-earlier barrier cutoff, and all three independently computed decisions. For an eligible soak, inspect the single continuous sender, worker identity across the step, bracketed carry-in (including zero when observed) and final drain. For an unperformed soak, review the recorded reason.
4. Require verified teardown and finished disposition for all three jobs. Confirm the independent reporter replays the frozen checkpoints and decisions and reproduces the attribution fixtures, including `unresolved` for missing evidence. Compare the actual live job/stage and reporter timing, disk peaks and upload verification against the 292-minute forecast, 350-minute live cap and 90-minute reporter cap, including its nine-minute upload reserve. Confirm capacity for three concurrent standard public jobs and adequate staging space. Keep the verified split Release evidence as a tested recovery route.
5. Record the evidence links and written approval at this exact SHA before the registered dispatch. A different head requires new same-head proof. A missing, expired, inaccessible, unresolved or unverified prerequisite keeps approval pending.

## Repository owner and reviewer actions

- The owner enables branch protection for `j2-authorization/**`. Give updates only to the designated reviewer, keep the workflow token read-only, and disallow force pushes and branch deletion. Confirm the registered authorization ref will report `protected: true`.
- The reviewer fixes and reviews the candidate commit, makes it available on the dispatch ref, obtains green checks, binds and reviews the engineering rehearsal, and records the approval for the registered phase. The user authorizes that phase separately.
- After approval, the reviewer dispatches the registered campaign once from the exact approved head. Resolve its exact attempt-1 run ID before publishing the generated run-bound registered ledger on the protected ref. An ambiguous run stays blocked. A sent or unresolved run cannot be replaced. A verified zero-request run requires the reviewed append-only closeout before any replacement.

## Frozen prediction and reporting wording

If all three soaks start, the expected outcome is checked expiry at the 600-second final drain and withheld qualified sentences. Use the all-three limitation only when replay verifies that all three ran and each matches the outcome. Apply the [protocol](../protocol.md#2026-11-27-pre-measurement-clarification) to other coverage.

> **Limitation — rebuilt registered profile.** In all three J2 jobs, the continuous soak-to-C episode expired its 600 s drain with unpublished outbox rows. Each job used single-node kind with PostgreSQL, Kafka and workers colocated on its GitHub-hosted runner, admission disabled. Zero of three jobs qualified for a soak observation.

> **Diagnostic only — rebuilt registered profile, job [n].** [completed] distinct hold-scheduled workflows reached final-revision SUCCEEDED inside the 1,800 s hold ([completed]/1,800 = [rate] workflows/s). Bounded end-to-end [p99 or labelled p95] was [latency] s for [observed] observed hold-scheduled successes, including completions in the step or drain ([eligible] eligible, [missing] missing, [censored] censored). The C-step verdict was [verdict]. The episode drain verdict was [verdict]. The verified cut had [unpublished] unpublished rows, [outstanding] outstanding workflows, [claimed] active claims and [missing keys/count].
