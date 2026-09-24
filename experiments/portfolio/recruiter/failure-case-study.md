# Failure case study: fencing a worker after a network partition

## Scenario

During a controlled AWS recovery episode, an app host lost its network path to
PostgreSQL and Kafka while its runtime and Python worker processes were kept
alive. A peer took over the partition. When the original worker later submitted
its result, the old attempt identity had to be rejected rather than overwrite
the peer's recovery.

## Timeline and evidence

The full four-arm campaign is recorded in
[`protocol.json`](../cloud-recovery/dur049-aws-20260923-full-0adbac0/protocol.json);
this case is the preserved-process episode in
[`network-isolation.json`](../cloud-recovery/dur049-aws-20260923-full-0adbac0/network-isolation.json).

1. **Before the fault:** workflow `dur049-r71-network-0adbac0-1` was assigned to
   partition 5. The runtime and worker were observed running. The campaign
   records original owner epoch 37 and a later pre-fault ledger boundary of 51;
   scheduler passes can update the epoch, so those are distinct observations.
2. **Inject and confirm:** the controller blackholed the app host's PostgreSQL
   and Kafka network path without replacing the original process. The fault
   observation is timestamped `2026-09-23T23:01:10.5254089Z`.
3. **Takeover:** the acquisition ledger records the first acquisition by the
   peer at epoch 56 (`2026-09-23T23:01:17.9897830Z` in the database and
   `23:01:31.0289333Z` as controller-observed). The campaign's
   fault-observed-to-acquisition-observed interval is 20,503.524 ms.
4. **Useful recovery:** recovery made progress on attempt 2. The original
   worker's correlated log records its late attempt-1 result rejected as HTTP
   409 `STALE_ATTEMPT`. The independent checker found zero superseded-epoch
   transitions after the recovery boundary.

These are one episode's observations, not a general recovery SLO. The peer was
a controller-started cold standby, not an always-running active scheduler.

## Why it matters

The network fault leaves the old process alive with stale in-memory identity.
Lease fencing protects scheduler mutations, while the attempt token protects
the worker result path. Both checks matter: observing peer progress alone would
not show that the old worker was prevented from recording a late result.

The activity in this fixture is pure `dur048.sleep`. This episode does not
establish idempotency or safety for an irreversible external effect; those
properties have separate cooperating/non-cooperating effect tests and approval
evidence.

## What the result does not establish

- The peer was cold-started after the fault, so this is not warm-active failover
  latency.
- The test used one AWS region and one dependency host. It does not test
  PostgreSQL host loss, database durability, multi-region recovery, or
  production availability.
- The measured latency is tied to the configured attempt lease and this
  campaign's observation boundaries; it is not a published failover SLO.
- A pure activity's stale-result rejection does not prove non-cooperating
  external effects are exactly once.

## Reproduction

The committed artifacts can be audited offline with this PowerShell snippet
from the repository root:

```powershell
$d = 'experiments/portfolio/cloud-recovery/dur049-aws-20260923-full-0adbac0'
$n = (Get-Content "$d/network-isolation.json" -Raw | ConvertFrom-Json).result
[pscustomobject]@{
  first_new_owner_epoch = $n.first_new_owner_acquisition.epoch
  fault_to_acquisition_observed_ms = $n.fault_observed_to_first_new_owner_acquisition_observed_ms
  late_worker_result = $n.original_worker_late_result_rejection.api_rejection
  recovery_attempt = $n.first_useful_progress_transition.attempt_number
  superseded_epoch_transitions = $n.superseded_epoch_transitions_after_recovery_boundary
} | Format-List
```

Observed output: epoch `56`, `20503.524` ms, `STALE_ATTEMPT`, recovery attempt
`2`, and `0` superseded-epoch transitions. For a new AWS run, follow the
fixture and preflight procedure in
[`deploy/aws/README.md`](../../../deploy/aws/README.md), create fresh claimed
fixtures, and use a new output directory. That run provisions resources and
must remain within the D022 budget and cleanup procedure.
