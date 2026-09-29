# DUR-056 solvable-evidence agent study

**Status:** Gate A freeze review requested. Held-out scoring is locked.

**Protocol:** [`protocol.md`](protocol.md), registered under D029.

**Implementation revision used for development:**
`ed73542005e17806fa47f2b11b3e64bba62b9a37`.

DUR-056 is a new synthetic study. It addresses the DUR-055 fixture gap where
expected actions and parameters were not stated in the evidence. DUR-029 and
DUR-055 fixtures, ledgers, configurations, unit rows and results remain
preserved. The historical comparison remains DUR-029 `gpt-4o-mini` at 4/20;
this Gate A development sample is not a held-out comparison or a published
model-quality claim.

## Pre-Gate-A fixture check

| Split | Cases | Answerable | Insufficient | Stale/conflicting | Near-duplicate decoys | Local oracle |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Development | 30 | 12 | 6 | 6 | 6 | 30/30 |
| Held out | 60 | 24 | 12 | 12 | 12 | 60/60 |

The deterministic local oracle and fixture assertions read held-out gold
evidence locally. The held-out queries and evidence were not sent to a model or
embedding provider. The database contains only 42 development evidence chunks
and 60 development query vectors; the held-out row count is zero in both
tables. The development embedding requests contain only `dev-` IDs.

The development-only export check examined 34,318 characters generated from
seed 5601. All 30 synthetic canaries were redacted at the MCP boundary; the
serialized MCP responses contained zero canary leaks, and scans found zero
email, credential, or external-IP patterns. This confirms the fixture data is
synthetic and the existing redaction path was active before model input.

## Retrieval development

The isolated database reports PostgreSQL 18.6 and pgvector 0.8.6. The dense
arm uses OpenAI `text-embedding-3-small` at 1536 dimensions and an HNSW
`vector_cosine_ops` index (`m=16`, `ef_construction=64`). PostgreSQL keyword
retrieval uses `tsvector`/`ts_rank`; hybrid retrieval fuses those two result
lists with RRF. The embedding requests were three development-only batches: 42
corpus chunks and 60 query vectors.

Thresholds were selected from development scores only. The selected settings
are top-k 8, HNSW `ef_search` 40, RRF k 60, keyword threshold 0.99999964, and
dense cosine-similarity threshold 0.6851199267. The retrieval tuning artifact
records all 18 rank-setting candidates and the development-only selection
objective/results.

**Frozen retrieval fingerprint:**
`sha256:0942e5d48e8bd51207c4f126bbf60aeef9e996239e63886bce0ce8082e2d431e`.

## Agent development

The exact model ID is `gpt-6-luna`, using the Responses API and structured
output. Two prompt/tool-schema/output-schema candidates were evaluated on the
30 development cases only, across all four arms: 240 model runs total. The
selection rule in the protocol chose `candidate-v1` with schema
`dur056-structured-v1`. The frozen request uses low reasoning effort,
`max_output_tokens=1200`, `store=false`, and two bounded transport retries; the
existing MCP argument validation remains unchanged.

| Candidate | Arm | Safe | Correct action + parameters | Diagnosis | Unsafe negative proposals | Citation violations | Cost (USD) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| v1 | No retrieval | 9/30 | 9/30 | 13/30 | 0 | 0 | 0.00492630 |
| v1 | PostgreSQL keyword | 29/30 | 29/30 | 16/30 | 0 | 0 | 0.00531380 |
| v1 | pgvector dense | 18/30 | 18/30 | 26/30 | 2 | 0 | 0.00942390 |
| v1 | Hybrid RRF | 18/30 | 18/30 | 24/30 | 3 | 0 | 0.00908990 |
| v2 | No retrieval | 9/30 | 9/30 | 16/30 | 0 | 0 | 0.00575430 |
| v2 | PostgreSQL keyword | 29/30 | 29/30 | 20/30 | 0 | 0 | 0.00703230 |
| v2 | pgvector dense | 17/30 | 17/30 | 25/30 | 3 | 0 | 0.01044690 |
| v2 | Hybrid RRF | 18/30 | 18/30 | 21/30 | 4 | 0 | 0.01042890 |

The full development artifact has one row per model run, including individual
input/output/reasoning token counts, provider latency, workflow wall latency,
cost, abstention and action details, citations, and error status. There were
zero provider-error rows. These development outcomes selected the frozen
candidate; they are not held-out results.

## Spend and frozen artifacts

The separate ledger has a hard **$25.00 USD** cap. Development used 240
`gpt-6-luna` calls and three `text-embedding-3-small` calls. Settled spend is
**$0.06253810**; reserved and uncertain spend are both zero. The ledger records
the official list rates checked on 2026-09-28: $0.10/$0.50 per million
input/output tokens for `gpt-6-luna` and $0.02 per million input tokens for
`text-embedding-3-small` ([OpenAI API pricing](https://developers.openai.com/api/docs/pricing)).

Gate A review files:

- [`gate-a-freeze-review-20260928T181507Z.json`](gate-a-freeze-review-20260928T181507Z.json)
- [`retrieval-frozen-config-20260928T180213Z.json`](retrieval-frozen-config-20260928T180213Z.json)
- [`retrieval-development-20260928T180213Z.json`](retrieval-development-20260928T180213Z.json)
- [`agent-frozen-config-20260928T181507Z.json`](agent-frozen-config-20260928T181507Z.json)
- [`agent-development-20260928T181507Z.json`](agent-development-20260928T181507Z.json)
- [`study-manifest-20260928T180213Z.json`](study-manifest-20260928T180213Z.json)
- [`spend-ledger.json`](spend-ledger.json)

**Frozen agent fingerprint:**
`sha256:98e1ae89d7c806ccdf577fab7d6d1135ae011cd4cd5ae0181ca0445c9cac70ee`.

The freeze bundle says `heldout_scoring_authorized: false`. No
`gate-a-accepted.json`, one-shot marker, held-out unit row, or held-out result
exists. Gate A review and green CI on the exact pushed commit are pending.
After that review, only the accepted receipt may unlock held-out scoring under
the frozen protocol. A pre-gate `score-heldout` CLI attempt was refused because
the receipt is absent; it created no marker or unit rows.

## Reproduction

Run the fixture validation without API access:

```powershell
$env:PYTHONPATH = "python"
uv run python -m incident_agent.dur056 validate-fixtures
```

For development preparation, copy
`deploy/local/dur056-postgres.env.example` to the gitignored `.env.dur056`,
replace its placeholder password, and start the isolated database:

```powershell
docker compose --env-file .env.dur056 -f deploy/local/dur056-postgres.yaml up -d --wait
```

Load the local database variables into the process environment, provide
`OPENAI_API_KEY` in the gitignored root `.env`, then run
`uv run --env-file .env python -m incident_agent.dur056 prepare-dev`:

```powershell
Get-Content .env.dur056 | ForEach-Object {
    $parts = $_.Split('=', 2)
    if ($parts.Length -eq 2) {
        [Environment]::SetEnvironmentVariable($parts[0], $parts[1], 'Process')
    }
}
$env:PYTHONPATH = "python"
uv run --env-file .env python -m incident_agent.dur056 prepare-dev
```

The runner refuses to overwrite an existing frozen bundle. Held-out provider
use is blocked until the exact Gate A receipt is present and verified.
