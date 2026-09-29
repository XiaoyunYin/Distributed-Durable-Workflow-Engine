# DUR-055 applied-AI upgrade

Status: **Gate B results complete; final Claude review pending.** The one-shot
held-out scorer completed on 2026-09-28 with no infrastructure aborts or
resumes. The result index is
[`heldout-evaluation-index-run-001-20260928T142857Z.json`](heldout-evaluation-index-run-001-20260928T142857Z.json).

This is a new versioned study. DUR-029 artifacts, including the 4/20
`gpt-4o-mini` held-out result, remain unchanged and are the comparison baseline.

The accepted Gate A bundle is [`gate-a-freeze-review-20260928T050959Z.json`](gate-a-freeze-review-20260928T050959Z.json)
with retrieval config fingerprint `sha256:04695baf2218305d0b7d8697223501181a1ff641f1a317fabfa8d409657ede7f`
and agent config fingerprint `sha256:d01d9a37c48de3410547e1b38609c27be344326a40cb6466cee03a3bed96f47c`.
The supersession audit at
[`gate-a-supersession-audit-20260928T051136Z.json`](gate-a-supersession-audit-20260928T051136Z.json)
explains two earlier preserved development bundles that were superseded before
freeze review. All their calls remain in the spend ledger.

## Held-out results

The scorer used the exact Gate A fingerprints listed above. Its model was
`gpt-6-luna`, using the frozen hybrid retrieval arm. It abstained on all 20
held-out cases. The four primary-safe outcomes are the four fixture-labeled
`insufficient_evidence` cases where abstention was expected. The historical
`gpt-4o-mini` primary-safe result remains 4/20 per arm, so there was no measured
improvement in primary safety. The document-dependent subset was 0/16,
matching the historical 0/16.

This benchmark cannot distinguish a careful model from a non-functional one
on those 16 answerable cases. The case logs and metrics contain family signals,
not the expected actions or their parameters. The runbook/postmortem chunks
provide generic guidance but likewise omit the expected action and parameters;
the case builder assigns those labels from the family. Thus the reported 16
"false abstentions" are false relative to fixture labels, not demonstrated
model errors. See `python/incident_agent/fixtures.py:106-112` for chunk text,
`:147-177` for case evidence, and `:179-188` for family-derived expected
actions. Both development candidates, `evidence-contract-v1` and
`evidence-contract-v2`, abstained on all 10/10 development cases.

| Retrieval arm | MRR | Ranking Recall@K | Delivered Recall@K | Distractor hit rate | No-answer false-positive rate | Median / max latency |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| PostgreSQL keyword | 0.4000 | 0.4000 | 0.4000 | 0 | 0 | 5.111 / 22.569 ms |
| pgvector dense | 0.4528 | 0.8667 | 0.6556 | 0 | 0 | 5.002 / 14.238 ms |
| Hybrid RRF | 0.6574 | 0.8889 | 0.6778 | 0 | 0 | 5.002 / 16.234 ms |

The keyword arm filters with conjunctive `plainto_tsquery('simple', query)`
(`python/incident_agent/dur055_retrieval.py:517`). In this template it matches
only two of five families: per-family Recall@K is 1.0, 1.0, 0, 0, 0 for
`bad_configuration`, `connection_pool`, `downstream_latency`, `disk_pressure`,
and `insufficient_evidence`, respectively. The overall 0.400 keyword result
is therefore determined by fixture construction, not a general lexical-
retrieval comparison. The more informative within-study comparison is hybrid
versus dense: MRR 0.657 versus 0.453 and ranking Recall@K 0.889 versus 0.867.
Those results remain specific to this synthetic corpus and query set.

Each arm scored the same 120 held-out queries: 90 answerable and 30 no-answer.
Secondary counts were 8/20 diagnosis accuracy, 4 correct abstentions, and 16
fixture-relative false-abstention labels; the latter are not demonstrated model
errors because answerable evidence omits expected actions and parameters.
Citation-provenance violations were zero.
The 20 agent runs used 24,077 input, 3,672 output, and 791 reasoning tokens;
API latency averaged 2,981.655 ms/run and wall latency averaged 3,043.924
ms/run. Their known cost was $0.00424370 total ($0.000212185/run).

The injection matrix contained 120 runs (20 cases x two profiles x three
conditions). All 120 runs abstained with `NO_PROPOSAL`. Using the baseline
canonical signature and
`(clean-A != injected) - (clean-A != clean-B)`, defended raw clean-clean flips
were 0/20, defended clean-A-to-injected changes were 0/20, and defended excess
was 0/20. The corresponding plain counts were 0/20, 0/20, and 0/20. Diagnosis
diverged in 20/20 cases under each profile. Since all signatures were
`NO_PROPOSAL`, the 0/20 excess is uninformative about injection resistance in
either direction; this study provides no evidence of injection resistance.
Canary leaks were zero across 120 runs x five surfaces:
`workflow_payload`, `rendered_prompt`, `persisted_model_record`, `mcp_response`,
and `exported_span`. This canary scan is a separate measurement from the
uninformative proposal-change metric.

Those 120 runs used 149,122 input, 20,846 output, and 4,244 reasoning tokens;
API latency averaged 2,754.620 ms/run and wall latency averaged 2,810.109
ms/run. Known cost was $0.01872641 ($0.000156053/run). Retrieval embeddings
used 1,810 tokens across four requests, cost $0.00003620, and took 11,523.966
ms in aggregate. Per-run values and raw outcomes are preserved in the three
held-out result files linked by the result index.

The run added $0.02300631 in settled API cost. The ledger now records
$0.03261675 settled spend against the $100.00 cap, with $0.00022412 still
reserved for an earlier unresolved development request and no active
reservations. The one-shot marker records zero aborts and zero resumes.

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

## Frozen inputs and scored setup

- Existing source corpus: 60 documents / 300 chunks; corpus version and content
  fingerprint are recorded in `study-manifest.json` after initialization.
- Existing retrieval queries: the same 40 development and 120 held-out query
  IDs. Only development queries were embedded, tuned, or scored before Gate A;
  held-out scoring is recorded in the result files above.
- Keyword: PostgreSQL `tsvector` and `ts_rank` over `source_corpus.chunks`.
- Dense: pgvector cosine search with `text-embedding-3-small`, 1536 dimensions,
  HNSW; previous 64-dimensional deterministic vectors are left untouched.
- Hybrid: reciprocal-rank fusion of the FTS and dense rankings.
- Agent model: `gpt-6-luna`. `gpt-4o-mini` remains the historical baseline.
- MCP: official Python SDK 2.0.0. Only existing allowlisted methods are
  registered; backend argument checks, provenance envelopes, and redaction stay
  in the existing implementation.

The selected retrieval settings, agent prompt/schema, and SHA-256 fingerprints
are stored as the Gate A artifacts linked above. The measured results do not
show a primary safety improvement over the baseline.

## Spend limit

`spend-ledger.json` is the independent DUR-055 cap: $100.00 USD total for
embeddings and model calls. It does not include or rewrite DUR-029's historical
spend. The ledger is initialized before provider calls. Each call reserves a
worst-case amount before sending, then records the provider's reported usage,
latency, and list-price estimate. Uncertain network outcomes retain their
reservation until reconciled.

The ledger's `pricing_checked_date` is 2026-09-27; its recorded standard list
prices are $0.10/$0.50 per million input/output tokens for `gpt-6-luna`, and
$0.02 per million input tokens for `text-embedding-3-small`. This README does
not assert a later price check before Gate B. The ledger and per-run
`known_cost_usd`/`model` fields are authoritative for cost and model identity.
Timeline actor `model-fixture` and `model_usage.cost_cents: 0.0` are legacy
labels and are not authoritative for live provider usage.

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

`score-heldout` fails closed without an accepted Gate A review receipt that
names the exact scorer commit and frozen fingerprints. Round 130 authorized
the single completed run recorded above. Do not rerun this freeze; its marker is
`COMPLETE`.

## Gate A and Gate B evidence

Gate A was accepted in Claude Round 130 for exact scorer commit
`1770384ed33b770937069d795736c37dfdfeec6d`. The Gate B result bundle includes
the accepted receipt, one-shot marker, all 140 write-once unit rows, three
summary artifacts, result index, and updated spend ledger. The committed
handoff records the result commit, exact-target CI, limitations, and the
request for final results/claims review.

## Limitations

The corpus and its labels are synthetic. This is a local retrieval/index and
bounded agent-quality study, not a production relevance, availability, or cost
claim. HNSW is approximate. The agent benchmark does not demonstrate model
quality on answerable cases because the fixture evidence omits the expected
actions and parameters. Results stay tied to the recorded corpus, model IDs,
settings, query population, and sample counts.
