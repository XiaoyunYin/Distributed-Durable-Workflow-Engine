# DUR-056 solvable-evidence agent study

**Current package:** D031 v3 fixture corrections and D032 rank-first tuning
selection have produced a local Gate A freeze bundle for Claude's full review.
It is not yet accepted; held-out scoring remains locked. Preserve v1/v2 results
and spend history as versioned baselines.

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
violations were zero. Diagnosis accuracy was 5/30 no-retrieval, 7/30 dense,
8/30 keyword, and 5/30 hybrid.

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

The development-only command is
`uv run --env-file .env --env-file .env.dur056 python -m incident_agent.dur056 prepare-dev`.
The repository `.env` supplies the provider credential; `.env.dur056` supplies
the isolated database credentials and port. Do not copy or print either
credential. The command must use the development split only; its provider
clients enforce the study split and ledger cap. Do not invoke `score-heldout`
without Claude's later accepted Gate A receipt.
