# DUR-055 protocol v1

Date frozen for development work: 2026-09-27. This protocol implements D027
and the DUR-055 task in `PLAN.md`. Gate A review must accept the committed
development-only freeze before any held-out scoring.

## Retrieval corpus and splits

Use `build_corpus()` and `build_retrieval_queries()` unchanged. The corpus must
remain 60 documents / 300 chunks with the existing stable IDs. The benchmark
must retain exactly 40 development and 120 held-out query IDs. Before Gate A,
the runner may read/query/embed only the development set. Its code must check
`split == "development"` before every retrieval call. It may record held-out
counts and ID-only fingerprints without invoking retrieval, embedding, or
labels on held-out query rows.

The existing fixture's identifier prefix is derived from `split[:3]`; thus its
held-out IDs are `hel-q-001` through `hel-q-120`. The pre-freeze ID fingerprint
must use that exact sequence without materializing held-out query records.

The database is a dedicated DUR-055 local PostgreSQL instance using the
versioned `source_corpus` schema. Existing `embedding vector(64)` and all
DUR-029 output files are untouched. The new embedding relation has a study
version key so this path never replaces old vectors.

## Retrieval arms

1. Keyword: `source_corpus.chunks.search_vector` is the existing stored
   `tsvector`; use a parameterized `plainto_tsquery('simple', query)` and
   `ts_rank(search_vector, query)` in PostgreSQL, with the existing GIN index.
2. Dense: embed every frozen corpus chunk and each development query with
   `text-embedding-3-small`. Store exact 1536-dimensional vectors separately
   under `source_corpus`, and query pgvector using cosine distance over an HNSW
   `vector_cosine_ops` index. Store `hnsw.ef_search` in the frozen config.
3. Hybrid: fuse the top keyword and dense ranks using reciprocal-rank fusion,
   `sum(1 / (rrf_k + rank))`; ties resolve by stable chunk ID.

All arms share candidate top-k. Candidate top-k values are 3, 5, and 8;
candidate `hnsw.ef_search` values are 40 and 80; candidate `rrf_k` values are
30, 60, and 90. Pick per-arm score thresholds and the shared ranking settings
using development queries only. For keyword and dense sufficiency, maximize
balanced accuracy for answerable/no-answer labels; ties go to lower no-answer
false-positive rate, then higher delivered Recall@K. Hybrid does not threshold
its fused score; it passes sufficiency iff either frozen constituent threshold
passes. Pick the shared ranking configuration by mean balanced accuracy across
the three arms, then lower summed no-answer false-positive rate, higher mean
delivered Recall@K, lower top-k, lower `ef_search`, and `rrf_k` nearest 60.

The frozen config stores all candidate ranges, selected values, the selection
objective, corpus/query fingerprints, database and extension versions, embedding
model/dimension/normalization, HNSW parameters, and its canonical JSON SHA-256.
No dev tuning code is called by the held-out scorer.

## Agent development and freeze

Use only the 10 existing development incident cases for iterative agent work.
Use `gpt-6-luna`; compare with historical DUR-029 evidence but do not call the
old model as part of this new study. Evaluate prompt and structured-output
schema variants only on the development cases. The MCP schema candidates keep
the registered method names, argument names, types, and backend validation
identical; they vary tool descriptions only. The frozen setup includes exact
model ID, prompt bytes and hash, MCP method schemas, structured-output JSON
Schema and hash, retrieval arm selected on development data, and all
decoding/API parameters.

Each live model run records case ID, prompt/schema fingerprints, response ID,
input/output/reasoning token counts when supplied, wall latency, and attributed
cost. Embedding requests record corpus/query IDs, batch and response IDs, usage,
latency, and cost. API requests use the shared $100 cap ledger, `store: false`
for Responses API calls, and no real operational data.

## Post-review evaluation (not authorized before Gate A)

After Claude accepts the exact pushed, CI-green freeze commit:

- score the 120 held-out retrieval queries once with frozen settings;
- score the 20 held-out incident cases once with the frozen agent;
- run `clean-A`, `clean-B`, and `injected` once each for each of the 20 cases
  under both `defended` and `plain` profiles (120 runs total);
- preserve and cite the existing 4/20 `gpt-4o-mini` study unchanged; and
- produce Gate B results and claims for final Claude review.

Every live call, including the later study, consumes the same new $100 cap.
The injector alters untrusted synthetic evidence only. Existing approval,
redaction, tool authorization, and effect contracts remain fixed. Running the
live agent against Go/PostgreSQL for crash-resume is out of scope.
