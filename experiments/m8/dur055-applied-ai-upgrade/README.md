# DUR-055 applied-AI upgrade

Status: **READY_FOR_FREEZE_REVIEW — Gate A**. No held-out
retrieval query or incident case has been scored in this study.

This is a new versioned study. DUR-029 artifacts, including the 4/20
`gpt-4o-mini` held-out result, remain unchanged and are the comparison baseline.

The current Gate A bundle is [`gate-a-freeze-review-20260928T050959Z.json`](gate-a-freeze-review-20260928T050959Z.json)
with retrieval config fingerprint `sha256:04695baf2218305d0b7d8697223501181a1ff641f1a317fabfa8d409657ede7f`
and agent config fingerprint `sha256:d01d9a37c48de3410547e1b38609c27be344326a40cb6466cee03a3bed96f47c`.
The supersession audit at
[`gate-a-supersession-audit-20260928T051136Z.json`](gate-a-supersession-audit-20260928T051136Z.json)
explains two earlier preserved development bundles that were superseded before
freeze review. All their calls remain in the spend ledger.

## Development-only measurements

The selected retrieval settings are top-k 5, HNSW `ef_search` 40, and RRF-k 60.
Across 40 dev queries, balanced accuracy was 0.70 for keyword and 1.00 for
dense and hybrid; no-answer false-positive rate was 0 for all three. Mean
delivered Recall@K was 0.40, 0.90, and 0.9333, respectively. The dense model is
`text-embedding-3-small` (1536 dimensions), indexed with HNSW.

The final agent development iteration used `gpt-6-luna` on the same 10 dev
incidents for each candidate. The selected `evidence-contract-v1` scored
0.3654 under the preregistered dev selection objective; v2 scored 0.3560.
Both had 0 unsafe proposals on insufficient cases and 100% valid citations.
These small dev results do not establish improvement over the unchanged 4/20
held-out `gpt-4o-mini` baseline.

| Prompt/schema | Input tokens | Output tokens | Reasoning tokens | Mean latency | API cost |
| --- | ---: | ---: | ---: | ---: | ---: |
| evidence-contract-v1 | 12,179 | 1,798 | 345 | 2,628 ms | $0.00102349 |
| evidence-contract-v2 | 13,489 | 2,417 | 491 | 2,902 ms | $0.00134609 |

Every response ID and run-level usage record is in
[`agent-development-20260928T050959Z.json`](agent-development-20260928T050959Z.json).
The ledger records $0.00961044 settled spend against the $100.00 cap and
$0.00022412 conservatively reserved for one socket-blocked attempt with no API
response. It records all three dev iterations and embedding calls.

## Frozen inputs and intended setup

- Existing source corpus: 60 documents / 300 chunks; corpus version and content
  fingerprint are recorded in `study-manifest.json` after initialization.
- Existing retrieval queries: the same 40 development and 120 held-out query
  IDs. Only the 40 development queries may be embedded, tuned, or scored before
  Gate A. The held-out set is not read by the development runner.
- Keyword: PostgreSQL `tsvector` and `ts_rank` over `source_corpus.chunks`.
- Dense: pgvector cosine search with `text-embedding-3-small`, 1536 dimensions,
  HNSW; previous 64-dimensional deterministic vectors are left untouched.
- Hybrid: reciprocal-rank fusion of the FTS and dense rankings.
- Agent model: `gpt-6-luna`. `gpt-4o-mini` remains the historical baseline.
- MCP: official Python SDK 2.0.0. Only existing allowlisted methods are
  registered; backend argument checks, provenance envelopes, and redaction stay
  in the existing implementation.

The exact tunable retrieval settings, selected development-only values, agent
prompt/schema, and SHA-256 fingerprints will be stored as new Gate A artifacts.
The selected model/configuration is not presumed to improve quality.

## Spend limit

`spend-ledger.json` is the independent DUR-055 cap: $100.00 USD total for
embeddings and model calls. It does not include or rewrite DUR-029's historical
spend. The ledger is initialized before provider calls. Each call reserves a
worst-case amount before sending, then records the provider's reported usage,
latency, and list-price estimate. Uncertain network outcomes retain their
reservation until reconciled.

The starting public list prices captured on 2026-09-27 are $0.10/$0.50 per
million input/output tokens for `gpt-6-luna`, and $0.02 per million input tokens
for `text-embedding-3-small`. The live pricing page should be checked again
before Gate B provider calls; any rate change must be reflected in the ledger
before more calls.

## Local PostgreSQL

The isolated `deploy/local/dur055-postgres.yaml` service uses a new volume and
binds only to loopback port 55432 by default. It does not replace the existing
local engine database or its volume. Supply the existing local credentials via
`.env`; do not print or commit them.

```powershell
docker compose --env-file .env -f deploy/local/dur055-postgres.yaml up -d
$env:PYTHONPATH = 'python'
uv run --env-file .env python -m incident_agent.dur055 prepare-dev
```

`score-heldout` must fail closed without an accepted Gate A review receipt that
names the exact freeze commit. After review, the retrieval and incident
held-out sets are each scored once; the new prompt-injection matrix is run once
on the frozen setup.

## Gate A evidence

Before requesting freeze review, commit and push the code, dev-only results,
spend ledger, model/prompt/schema fingerprints, frozen retrieval config, and
tests. Confirm CI passes on that exact commit and append the Codex handoff to
the end of `REVIEW.md`. Stop there. Do not generate any held-out score until
Claude accepts the freeze.

## Limitations

The corpus and its labels are synthetic. This is a local retrieval/index and
bounded agent-quality study, not a production relevance, availability, or cost
claim. HNSW is approximate. Results stay tied to the recorded corpus, model IDs,
settings, query population, and sample counts.
