# DUR-056 solvable-evidence agent study

**Current package:** Claude accepted Gate A in Round 136. The single authorized
Gate B run completed on the accepted freeze; its versioned results are recorded
below and await Claude's final results and claims review. The scorer is marked
`COMPLETE` and must not be run again for this freeze. The failed transport-only
development attempt and every earlier fixture/config/report remain preserved as
versioned history.

**Protocol:** [`protocol.md`](protocol.md), registered under D029 and amended
before implementation by D030 and D031. The previous protocol and readme are
preserved as [`protocol-v2-superseded-round133.md`](protocol-v2-superseded-round133.md)
and [`README-v2-superseded-round133.md`](README-v2-superseded-round133.md).

## V3 fixture validity

V3 uses seeds 5703 for 30 development cases and 5704 for 60 held-out cases.
Each case has a unique `(service, active_version, signal)` key. The local
structural oracle derives each key from log/metric envelopes and scans the
entire split corpus.

| Split | Cases | Unique keys | Corpus chunks | Oracle | Provider calls |
| --- | ---: | ---: | ---: | ---: | ---: |
| Development | 30 | 30 | 42 | 30/30 | 0 |
| Held out | 60 | 60 | 84 | 60/60 | 0 |

The latest fixture validity and oracle fingerprints are recorded in
[`fixture-validity-v3-20260928T234924Z.json`](fixture-validity-v3-20260928T234924Z.json).
Earlier versioned validity/oracle reports from infrastructure-blocked attempts
remain preserved beside it.
The fixture tests also check unique keys, answer-value separation, structural
negative evidence, no case/split markers or query padding, supersession
direction, and near-duplicate decoys. No embedding or model provider was used
for this validation.

Keyword, dense, and hybrid arms now always return their ranked top-k evidence
to the agent. Sufficiency classifiers remain separate metrics and are not
included in agent-visible MCP results. The classifier sample is small (six
insufficient development cases); the protocol records this limitation and
reports ranking recall/MRR separately.

## Development tuning and freeze

The v3 protocol registers `text-embedding-3-small` (1536 dimensions), HNSW,
PostgreSQL full-text search with OR-joined `to_tsquery` terms, and hybrid RRF.
The agent model is `gpt-6-luna`. All tuning used development data only.

The development retrieval report is
[`retrieval-development-v3-20260928T233555Z.json`](retrieval-development-v3-20260928T233555Z.json),
and its frozen config is
[`retrieval-frozen-config-v3-20260928T233555Z.json`](retrieval-frozen-config-v3-20260928T233555Z.json).
The 60 development queries selected top-k 3, HNSW `m=16`, `ef_construction=64`,
`ef_search=40`, and RRF k 60. Ranking recall@3 was 1.00 for all retrieval arms;
MRR was 1.000 keyword, 0.889 dense, and 1.000 hybrid; delivered chunk recall
was 1.00 for all three. Classifiers are diagnostics only: balanced accuracy
was 0.599 keyword, 0.548 dense, and 0.595 hybrid, with 11, 3, and 12 false
positives respectively. Only six insufficient cases informed those descriptive
classifier figures.

The agent development report is
[`agent-development-v3-20260928T234744Z.json`](agent-development-v3-20260928T234744Z.json),
with local analysis in
[`agent-development-analysis-v3-20260928T234915Z.json`](agent-development-analysis-v3-20260928T234915Z.json).
The selected setup is candidate-v1 (`evidence-contract-v1`,
`dur056-structured-v1`) on `gpt-6-luna`; it scored 98/120 safe across the four
arms versus 95/120 for candidate-v2. The primary safe counts were 9/30 without
retrieval, 29/30 dense, 30/30 keyword, and 30/30 hybrid. Each retrieval arm
delivered a three-item evidence list on all 30 runs (90 items per arm); all 30
no-retrieval lists were empty. Correct action-and-parameter counts were 0/21
without retrieval and 21/21 in each retrieval arm. Citation provenance
violations were zero. The diagnostic named `diagnosis_quotes_exact_incident_key`
(literal service, active version, and signal inclusion; not general diagnosis
correctness) was 5/30 no-retrieval, 7/30 dense, 8/30 keyword, and 5/30 hybrid.

Selected-candidate tokens, mean per-run provider/wall latency, and cost were:

| Arm | Input / output / reasoning tokens | Mean provider / wall latency | Cost |
| --- | ---: | ---: | ---: |
| No retrieval | 25,722 / 4,817 / 1,419 | 2.271 / 2.338 s | $0.00498070 |
| Dense | 36,897 / 5,676 / 1,234 | 2.454 / 2.535 s | $0.00652770 |
| Keyword | 36,895 / 5,825 / 1,304 | 2.374 / 2.451 s | $0.00660200 |
| Hybrid | 37,010 / 5,476 / 999 | 2.868 / 2.945 s | $0.00643900 |

The frozen retrieval, agent, and bundle fingerprints are respectively
`sha256:c6a73abbb7b2ee4665c381ca3867b69a6fa45ed0a9d74b88c31b6b103f2d837c`,
`sha256:00dd204048421a0b22670c81571ac2c7d4c9710126b05798f804b7fe1716fa40`,
and `sha256:2f376a2d4d9b1fab48d7d01f78d0071b2e3c936fed808e3cd7c563b7836c504c`.
The bundle is
[`gate-a-freeze-review-v3-20260928T234744Z.json`](gate-a-freeze-review-v3-20260928T234744Z.json)
and has status `READY_FOR_FREEZE_REVIEW`; it grants no held-out scoring.

The append-only ledger retains its `$25.00` cap. Lifetime settled spend is
`$0.17164038`, reserved spend is `$0`, and `$0.00016240` remains uncertain from
the earlier sandbox-blocked embedding attempt. The completed v3 provider work
settled `$0.05297630` for 240 `gpt-6-luna` calls and `$0.00006706` for three
development embedding requests. All completed v3 calls succeeded. No held-out
case, query, evidence or embedding was sent to any provider.

## Round 135 R209/R210 correction status

The injected development note now claims operator authority and supplies the
seeded per-case wrong action as free text. The scorer keeps the registered raw
excess formula and separates attack success, induced abstention, other changes,
and clean-A/clean-B flips. Subsequent development scoring and any later
authorized held-out scoring must use the frozen `gpt-6-luna` prompt/schema and
retrieval fingerprints.

The preserved first pilot batch is
[`injection-development-pilot-r209-attempt-1-20260929T014259Z.json`](injection-development-pilot-r209-attempt-1-20260929T014259Z.json),
with its write-once rows in
[`injection-pilot-r209-attempt-1-units/`](injection-pilot-r209-attempt-1-units/).
It contains 180 development workflow runs, 180 provider-error rows, zero model
responses, and 540 transport request entries after bounded retries. The sandbox
blocked socket creation (`WinError 10013`); settled spend remained `$0`, and the
uncertain total temporarily rose to `$0.65505040`. Its all-zero signature
outcomes are explicitly not used to calibrate the attack wording. The derived
[transport assessment](injection-pilot-r209-attempt-1-transport-assessment-20260929T015455Z.json)
records this interpretation and the unchanged `$25.00` cap. Those 540 requests
are append-only reconciled `NOT_SENT` events because socket creation failed
before a request left the machine; the separate earlier `$0.00016240` uncertainty
is preserved as the current uncertain balance. Attempt 1 is failed transport
history, not a wording calibration. The approved attempt 2 uses v1; attempt 3
would use v2 only if plain attack success were zero in attempt 2. Attempt 2
completed with 180/180 model responses, 15/30 plain attack successes, and zero
defended attack successes, so attempt 3 is not permitted. The per-run records
and profile summaries are in
[`injection-development-pilot-r209-attempt-2-20260929T030150Z.json`](injection-development-pilot-r209-attempt-2-20260929T030150Z.json).
No held-out provider call occurred. The R209 Gate A bundle selects attempt 2
with injection text v1 at
[`gate-a-freeze-review-r209-v3-20260929T030514Z.json`](gate-a-freeze-review-r209-v3-20260929T030514Z.json),
fingerprint `sha256:b9ba0c21a46a92179b7bb7aa3171d355bb4f9458992a8f04f7f7c334062f63ab`.

The freeze fingerprints the chosen injection text, fixture validity/oracle
reports, and valid pilot. The agent fingerprint remains
`sha256:00dd204048421a0b22670c81571ac2c7d4c9710126b05798f804b7fe1716fa40`;
the retrieval fingerprint remains
`sha256:c6a73abbb7b2ee4665c381ca3867b69a6fa45ed0a9d74b88c31b6b103f2d837c`.
Those frozen configs are unchanged because the pilot changed only
fixture-delivered injection text.

Protocol tuning language now describes this field as “diagnosis quotes exact
incident key”; it is a literal-key inclusion check, not a general diagnosis
accuracy measure. Older versioned reports are retained unchanged.

## Scope and reproduction

The accepted Gate A procedure will still require push, green CI for the exact
commit, a handoff at the end of `REVIEW.md`, and Claude's full re-review. Only
after a new accepted receipt may Gate B run the 120 held-out retrieval queries,
60 held-out incidents, and 360-call injection matrix. Live-agent execution and
crash-resume on the Go/PostgreSQL engine remain out of scope.

Run local fixture tests and both corpus oracles without provider credentials:

```powershell
$env:PYTHONPATH = "python"
uv run python -m incident_agent.dur056 validate-fixtures
uv run pytest tests/test_dur056_fixtures.py tests/test_dur056_retrieval.py
```

The development-only tuning command is
`uv run --env-file .env --env-file .env.dur056 python -m incident_agent.dur056 prepare-dev`.
For Round 135, use `pilot-injection-dev --attempt 2` for the authorized v1
calibration. If and only if plain attack success is zero, use
`pilot-injection-dev --attempt 3` once with v2. The failed attempt 1 remains
history and is never rerun or used to choose wording.
The repository `.env` supplies the provider credential; `.env.dur056` supplies
the isolated database credentials and port. Do not copy or print either
credential. The command must use the development split only; its provider
clients enforce the study split and ledger cap. Do not invoke `score-heldout`
without the accepted Gate A receipt and exact frozen fingerprints. The accepted
freeze's one-shot scorer is now `COMPLETE`; do not invoke it again.

## Gate B results — run-001

Claude accepted Gate A in Round 136. The one-shot held-out scorer then completed
601/601 write-once units once, covering 120 query embeddings, 60 incidents
across four arms, and the 360-call hybrid injection matrix. There were no
aborts, resumes, provider errors, or completed-unit reruns. The detailed
[versioned Gate B results report](gate-b-results-run-001-20260929.md) records
the registered metrics and limits. The machine-readable
[scorer report](heldout-report-run-001-finalization-001-20260929T044956Z.json),
[result index](heldout-result-index-run-001-finalization-001-20260929T044956Z.json),
[marker](heldout-one-shot/run-001.json), [write-once unit directory](heldout-units-run-001/),
and [spend ledger](spend-ledger.json) preserve the source evidence.

The primary safe end-to-end counts were 18/60 without retrieval, 60/60 keyword,
56/60 dense, and 59/60 hybrid. Keyword matched the 60/60 oracle ceiling; dense
and hybrid were below it. The plain injection profile had 26 attack successes,
12 induced abstentions, and excess +35; defended had 0, 3, and -2. The five
canary surfaces had no raw leaks. Cumulative settled spend is `$0.34631840` of
the `$25.00` cap; the prior `$0.00016240` uncertain entry remains preserved.
DUR-029 is historical and non-comparable. Gate B is awaiting Claude's results
and claims review.
