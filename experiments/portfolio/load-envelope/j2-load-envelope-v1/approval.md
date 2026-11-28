# J2 exact-head rehearsal and approval

The manifest binds frozen inputs. The candidate SHA is the commit that contains this packet. Record that full SHA in the engineering run, the written approval, and the later reviewer ledger. A commit cannot name its own SHA inside the manifest. The base SHA and committed hashes identify what can be verified before the candidate commit exists.

## Verified supplemental controls

The packet binds the successful attempt-one supplemental `controls-r422` run at `2656671`, including bundle `15265153937` and receipt `15265129261`. The independent replay agrees with the hosted receipt. Both packet verifiers must pass at the packet commit with the same covered-component digest. [Run 46148424125](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/46148424125)

## Candidate engineering dispatches

Run this setup from the reviewed commit containing the packet. Confirm the two full-clock runs and the short-clock rehearsal report its full `headSha`, attempt 1, `workflow_dispatch` and workflow name `J2 load-envelope tooling`.

```powershell
$repo = 'XiaoyunYin/Distributed-Durable-Workflow-Engine'
$ref = 'codex/j2-slice3'
$head = (git rev-parse --verify HEAD).Trim()

function Assert-J2CandidateRef {
    $remoteHead = gh api "repos/$repo/git/ref/heads/$ref" --jq .object.sha
    if ($LASTEXITCODE -ne 0 -or $remoteHead -ne $head) {
        throw 'The dispatch branch must point to the reviewed candidate commit.'
    }
}
```

```powershell
Assert-J2CandidateRef
gh workflow run j2-load-envelope.yml --repo $repo --ref $ref -f mode=r422-full-clock -f campaign_id=eng-r422-a-final3 -f r422_hold=A
Assert-J2CandidateRef
gh workflow run j2-load-envelope.yml --repo $repo --ref $ref -f mode=r422-full-clock -f campaign_id=eng-r422-ab-final3 -f r422_hold=AB
```

Leave `authorization_ref` empty for both full-clock runs. Each uses three Ubuntu 24.04 jobs with 120 s warm-up, 1,800 s hold and 300 s at C. Review Rounds 617, 625 and 648 as historical evidence. Approval requires the two `-final3` runs at the packet commit.

After reviewing both full-clock runs, load `Publish-J2EngineeringLedger` from [measurement authorization](../measurement-authorization.md#engineering-dispatches) in the same checkout. The short-clock ID uses the first eight characters of the candidate SHA. Bind the fresh engineering ledger to the exact attempt-one run within the six-minute authorization wait.

```powershell
$campaign = "eng-r422-final-$($head.Substring(0, 8))"
Assert-J2CandidateRef
gh workflow run j2-load-envelope.yml --repo $repo --ref $ref -f mode=measure -f "campaign_id=$campaign"
$runId = [long](Read-Host "Exact new $campaign Actions run ID")
$run = gh run view $runId --repo $repo --json headSha,attempt,event,workflowName | ConvertFrom-Json
if ($run.headSha -ne $head -or $run.attempt -ne 1 -or $run.event -ne 'workflow_dispatch' -or $run.workflowName -ne 'J2 load-envelope tooling') {
    throw 'Unexpected workflow run or dispatch head.'
}
Publish-J2EngineeringLedger $campaign $runId
```

## Required review before authorizing measurement

1. Match all engineering jobs and their replays to the candidate SHA and attempt. Confirm green candidate PR checks and the three short-clock `pre-dispatch-package.json`, authorization, inventory and exact-head controls receipts. Match the covered-component digest to the fresh R427/R428 supplemental receipt, check live archive availability and the qualification at 26,678 identities, and verify all four control verdicts and 18 unchanged seed vectors. The engineering runs must produce no measurement claims.
2. Check all six full-clock jobs and both independent replays, including concurrent offering, precommit duration, bytes, peak RSS and completion at least 2 s before the first slot. Reconcile every scheduled identity. Record each C-step `capacity-missed` refusal at `in_flight_at_release == 16` as engineering backpressure and leave it out of the R422 timing failures. Require exactly one successful HTTP request write inside every other half-open slot and fail the lane for every other refusal, duplicate, missing or out-of-slot write. For measurement any such C-step miss still makes the step sender-invalid. Evaluate the independently valid hold and withhold the qualified sentence. Overall and within warm-up, hold and C, require intent-to-write nearest-rank p99.9 ≤ 12 ms and max ≤ 20 ms. Require every slot-to-write ≤ min(20 ms, floor(3 × slot width / 4)). Review the split at transport entry. The registered sender-validity cutoff remains the next same-rate nominal slot, with the last hold slot ending at C's first slot.
3. Review `state-reads.jsonl`, `drain-polls.jsonl`, `state_read_binding` and the replay in every full-clock job and each short-clock episode that reaches drain. Confirm all state-read durations and brackets, failed-read diagnostics for timeouts, and the first successful in-deadline bracket in both `scheduled-stop-read.json` and `initial.json`. A failed read contributes no quiet. Verify the real quiet interval or the checked export at expiry, with residual IDs and unpublished, outstanding and claimed counts. Where the diagnostic probe fails, require its bracket and error. Persistent read failure must reach the same 600 s checked expiry. A checked R411 expiry remains a drain outcome and is permitted in the full-clock engineering timing lane.
   Verify each grid or soak deadline anchor reserves the full 130 s beyond its 600 s drain. For an expired episode, bind `expiry-export-budget.json` to `deadline-anchor.json` and the checked read bracket. Verify that the exporter started within 10 s of the unchanged drain cutoff and had its full 120 s from actual start, within the recorded work deadline. Review measured completion against the reserved cutoff. A clamped final state read and its diagnostic may finish during the handoff. The evidence should show the full export allowance and zero quiet credit for that read.
4. Review restored image IDs, rendered hashes, readiness, Calico probes, cgroup pressure and fresh state. Confirm the one-second exporter and Prometheus cadences, serialized two-second statement timeout, one exporter DB connection, zero Prometheus connections, 82/100 with reserved/admin and 18 free. Match the candidate-built `cmd/j2-sender` in `precommit-v1` mode and its hash-verified installs for both schedule shapes. Bind the locked checker to its installed digest.
5. Review all 15 real short-clock grid cells in their registered orders, including schedule bindings, write evidence, identity/cut reconciliation, classifications, samples and any verified recovery or job stop. Check grid publication, the 45-minute-or-earlier barrier cutoff and the three independent decisions. For an eligible soak, check the single sender, worker identity across the step, bracketed carry-in and final drain. For an unperformed soak, review the recorded reason.
6. Require verified teardown, sealed uploads and finished disposition for all jobs. Confirm independent replay reproduces checkpoints, decisions, sender evidence and attribution fixtures, including `unresolved` where evidence is incomplete. Review the actual timings and disk peaks against [the budget](budget.json). Confirm three concurrent standard public jobs and the nine-minute replay upload reserve. Keep the verified split Release evidence as a recovery route.
7. Record the engineering evidence links and written approval at the exact candidate SHA before registered dispatch. A head change requires fresh same-head proof. Any missing, expired, inaccessible, unresolved or unverified prerequisite keeps approval pending.

## Repository owner and reviewer actions

- The owner enables branch protection for `j2-authorization/**`. Give updates only to the designated reviewer, keep the workflow token read-only, and disallow force pushes and branch deletion. Confirm the registered authorization ref will report `protected: true`.
- The reviewer fixes and reviews the candidate commit, makes it available on the dispatch ref, obtains green checks, binds and reviews the engineering rehearsal, and records the approval for the registered phase. The user authorizes that phase separately.
- After approval, the reviewer dispatches the registered campaign once from the exact approved head. Resolve its exact attempt-1 run ID before publishing the generated run-bound registered ledger on the protected ref. An ambiguous run stays blocked. A sent or unresolved run cannot be replaced. A verified zero-request run requires the reviewed append-only closeout before any replacement.

## Frozen prediction and reporting wording

If all three soaks start, the expected outcome is checked expiry at the 600-second final drain and withheld qualified sentences. Use the all-three limitation only when replay verifies that all three ran and each matches the outcome. Apply the [protocol](../protocol.md#2026-11-27-pre-measurement-clarification) to other coverage.

> **Limitation — rebuilt registered profile.** In all three J2 jobs, the continuous soak-to-C episode expired its 600 s drain with unpublished outbox rows. Each job used single-node kind with PostgreSQL, Kafka and workers colocated on its GitHub-hosted runner, admission disabled. Zero of three jobs qualified for a soak observation.

> **Diagnostic only — rebuilt registered profile, job [n].** [completed] distinct hold-scheduled workflows reached final-revision SUCCEEDED inside the 1,800 s hold ([completed]/1,800 = [rate] workflows/s). Bounded end-to-end [p99 or labelled p95] was [latency] s for [observed] observed hold-scheduled successes, including completions in the step or drain ([eligible] eligible, [missing] missing, [censored] censored). The C-step verdict was [verdict]. The episode drain verdict was [verdict]. The verified cut had [unpublished] unpublished rows, [outstanding] outstanding workflows, [claimed] active claims and [missing keys/count].
