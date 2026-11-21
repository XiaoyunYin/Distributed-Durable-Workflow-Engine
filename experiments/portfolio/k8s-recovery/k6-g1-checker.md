# K6 G1 engine evidence and checker

Run one mode per invocation. The live configuration is private and contains exactly
`schema` (`dur053.k6-live-config.v1`), `database_url` (a PostgreSQL connection URL),
`episode_dir` (a new directory under an existing parent), and `manifest` (the public
object described below).

```sh
dur049-checker -k6-live-v1 /private/episode-config.json > live.json
dur049-checker -k6-offline-v1 /public/episode-dir > offline.json
```

| Exit | Meaning |
| --- | --- |
| 0 | Reserved for a complete pass in a later slice |
| 1 | A safety finding or a failed acquisition, relation or bundle integrity check |
| 2 | Invalid invocation or private configuration |
| 3 | G1 checks found no contradiction, and the episode verdict remains incomplete |

Stdout is the comparable `dur053.k6-checker.v1` result. Stderr carries acquisition
and usage diagnostics. Live verifies the same public bytes as offline. A cut interrupted
before sealing leaves a partial directory whose missing index produces a failed verdict.
Both modes always emit the 13 registered labels when producing a verdict. I2 and I11
evaluate the engine history portion of the design. Other labels carry
`slice_not_implemented:G1`. All results are engineering evidence with
`measurement_claim=false` and `charged_seconds=0`.

## Manifest

The public manifest has schema `dur053.k6-evidence.v1` and the following fields.

| Fields | Values |
| --- | --- |
| `campaign`, `job`, `run_id` | Public run identifiers |
| `episode`, `ordinal`, `seed`, `cell`, `workload` | The corresponding row in [the pinned registry](k6-registry-v1.json) |
| `namespace`, `reset_id`, `barrier_id`, `database` | Episode namespace, provisioned reset identity, fresh cut identifier, and expected PostgreSQL database name |
| `registry_sha256`, `source_tree_sha256`, `image_set_sha256`, `freeze_sha256`, `topology_sha256` | Lowercase SHA-256 hex strings. The checker pins the registered schedule and its byte digest. |
| `instrumentation` | `[]` or `["k6_f3.deadline_writes"]` |
| `required_sources` | Sorted array of `manifest.json`, `engine/snapshot.json` and `engine/<qualified-relation>.jsonl` for the [engine inventory](k6-phase-g-design.md#evidence-bundle-and-barrier). Include the declared instrumentation file when present. |

The checker opens a single read-only REPEATABLE READ transaction and brackets
the first query and the close clock sample with observer UTC and monotonic
nanoseconds. The descriptor records the commit outcome, PostgreSQL visibility
set, DB name and OID, and both server clocks at open. The connection needs SELECT
on the pinned engine, effects, authorization, fixture corpus and any declared
instrumentation tables. Row-level filtering fails acquisition.

## Census and projection

The catalog census distinguishes tables, partitioned tables and their partitions,
views, materialized views and sequences outside `pg_catalog` and
`information_schema`. A relation outside the pinned inventory and exclusions
fails with `RELATION_UNCLASSIFIED`. Sequence and view exclusions cover named
allocators and projections of exported rows, including the credential-bearing
grant view. The pinned public statistics views are extension-owned monitoring
projections. The three named `source_corpus` relations are static fixture data.
The descriptor records each of their SQL counts and a local SHA-256 over sorted,
fixed-width hex SHA-256 values of PostgreSQL binary row records. Fixture
documents and embeddings stay in the database.

The public engine format uses qualified filenames, independent SQL counts,
native number and UUID order, C collation for text keys, canonical UTF-8
objects, explicit nulls and LF. SHA-256 covers the exact file bytes. The
canonical descriptor and manifest are themselves indexed. Credential fields
are SHA-256 of the exact PostgreSQL UTF-8 token representation. Known
free-text diagnostic fields become presence bits. Quarantined wire payloads
contribute only byte length and digest. Structured business and evidence values
use closed schemas, which reject unknown objects, encoded JSON strings and
unexpected arrays. A row-shape failure reports the relation name.

G1 verifies consecutive revision, legal edges and final state against the
whole exported execution set, including foreign workflows. It counts terminal
transitions at the durable final revision and reports wrong-namespace and
orphan rows. G2 supplies sealed plan and arrival inputs for N2/N3, the paired
cuts and barrier, and the external anchor for the episode index. Later slices
finish commit-linked and external proofs and supply the full episode and arm
verdicts.
