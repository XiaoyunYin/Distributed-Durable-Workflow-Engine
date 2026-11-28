# J7 hosted workflow and evidence contract

`j7.yml` runs tooling on pull requests and by manual dispatch. Tooling builds
the checker, tests the media and ledger stages, and runs the physical media
fixture inside the pinned PostgreSQL 18.6 image. It needs no J9a registration.

Qualification and measurement share the candidate gate and the 350-minute
runner cap. Qualification uses `q1`. Measurement uses `r1`, `r2` and `r3`
once each. The measurement input pins the successful qualification's run ID,
artifact ID and service-reported SHA-256 digest. The workflow requires the
same reviewed checkout in both jobs and compares the downloaded ZIP with
that digest before reading its seal.

## Admission

The current checkout can run tooling. Qualification and measurement are
blocked by the missing J9a registry entry and reviewed fixture candidate.
The capability contract for that candidate is below.

- Register the reviewed J9a switch for both `J7_source` and `J7_target` and
  select its registered nondefault value. Keep the registered K profiles.
- Seal `deploy/j7/candidate.json` with schema `j7.hosted-candidate.v1`,
  `relay_switch`, `relay_value`, `runtime_mode_source` and `sealed_files`.
  The map binds the runtime reader, `deploy/j7/controller`, the source and
  target overlays, the workload, the registry, the tool lock and this
  experiment's protocol. The checkout and rules must be reviewed before
  qualification.
- The tracked fixture controller still needs implementation. It owns the
  actual one-node kind deployment and its private PostgreSQL, Kafka and
  sink resources. It opens app writers only after observing effective
  repaired-relay mode, migration inventories, endpoint isolation and
  generation agreement. It owns marker sequencing, fault and restore
  calls, source and target worker holds, fixture credentials, resource and
  endpoint observations and the single external ledger writer.

The workflow calls the candidate with the following argument order.

| Command | Arguments after the command | Expected result |
| --- | --- | --- |
| `bootstrap` | Private workspace | Prepare the pinned runner topology and private tooling without arming an outage. |
| `drill` | Drill ID, private workspace, drill evidence directory, `j7-media` path | Run one physical drill and produce its original observation artifacts and sealed ledger. |
| `between` | Finished drill ID, private workspace, finished drill evidence directory | Tear down the completed fixture before a fresh drill is admitted. |
| `cleanup` | Private workspace | Attempt all owned teardown and write `cleanup.json` with verified absence or explicit residuals. |

The wrapper owns runner admission, the per-stage forecast, per-drill
timeouts, independent verdict, indexed upload and the stop list.
The wrapper publishes only its generated summaries and files accepted
by the evidence allowlist. Keep deploy files, raw logs, credentials,
backups and WAL under the private workspace.

## Observation and verdict sequence

1. Seal the planned non-marker requests, marker plan, policy and topology
   in the acknowledgment ledger header. Capture the original stage
   receipts and the backup manifest bytes named in the backup receipt.
2. Record the verified target-open receipt with its restore, broker,
   endpoint isolation and clock bindings as the `j7-target-endpoint`
   ledger witness. Run the existing redispatch, fencing and E1 probes.
3. Once all 21 target workflows terminate and all notices drain, run
   `j7-media observe-quiet --phase start`. Wait 30 seconds from that receipt's
   completion, leave the sink and its verified grant reader live, and
   seal the effect ingress witness. Verify orphan reconciliation and make
   its final sink export available before `observe-quiet --phase end`.
   Record both observations in the live ledger as `quiet-start` and
   `quiet-end`. Verify redispatch against that final engine export.
4. Seal the ledger and present `resource-samples.json` with a shared
   monotonic bracket, effective limits and boundary samples. The wrapper
   calls `index-evidence` and `verify-drill` on the outage boot. RTO includes
   the verifier, with the result timed from confirmed outage. The saved
   verdict and every evidence member are sealed in the final index.
5. In qualification, preserve the source-tail export and ledger witness
   `j7-source-tail` and the private archive under the drill directory.
   `verify-controls` checks the healthy sibling and separately runs the
   two identity mutations, the copied WAL gap and the legacy group
   counterexample. Each control must return its registered rejection.

`publish-evidence` produces a separate upload bundle with the exact
allowlisted file inventory and only claim and grant fingerprints in the
engine snapshots. Its index records the original and projected digests
for each redacted trace, including missing evidence. The wrapper runs
`verify-drill --replay` against that kept bundle and requires the same
verdict. This replays the logical checks and the recorded outage clock.
Physical media replay is witnessed solely inside the running drill.

Tooling can be dispatched once GitHub has registered the workflow.

```sh
gh workflow run j7.yml --ref codex/j7-slice0 -f mode=tooling
```
