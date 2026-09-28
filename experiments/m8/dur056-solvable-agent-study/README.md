# DUR-056 solvable-evidence agent study

**Current package:** D031 v3 fixture and retrieval corrections for Claude's
full Gate A re-review. Held-out scoring remains locked; there is no v3 Gate A
receipt. Preserve v1/v2 results and spend history as versioned baselines.

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

Fixture validity and oracle fingerprints are recorded in
[`fixture-validity-v3-20260928T224401Z.json`](fixture-validity-v3-20260928T224401Z.json).
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
The agent model is `gpt-6-luna`; the existing append-only ledger enforces the
**$25.00 USD** hard cap and preserves its $0.11859702 prior settled spend.

The isolated PostgreSQL service is healthy and its v3 migration and 42
development corpus rows have been prepared. Development embeddings, retrieval
tuning, agent prompt/schema tuning, and frozen configs/fingerprints are pending.
The repository's gitignored `.env` provides development credentials through
`uv run --env-file .env`; the API key is not copied into artifacts or printed.
No held-out material has been sent to any provider. The package is not ready
for Gate A review until development tuning is complete and the v3 retrieval and
agent configurations are frozen.

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

The development-only command is `uv run --env-file .env python -m incident_agent.dur056
prepare-dev`. It must use the configured DUR-056 PostgreSQL service and the
development split only; its provider clients enforce the study split and
ledger cap. Do not invoke `score-heldout` without Claude's later accepted
Gate A receipt.
