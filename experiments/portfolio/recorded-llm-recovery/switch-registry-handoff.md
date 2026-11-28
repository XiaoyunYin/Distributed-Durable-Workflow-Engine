# J4 entries for the D037 switch registry

Merge these rows into `deploy/runtime-switches.json` after the J19 registry
from PR #99 lands. The switch names and contracts come from the reviewed J4
protocol. Both switches accept `0` and `1`. Unset has the meaning of `0`,
and other values fail closed.

| Switch | Task | Value 0 | Value 1 | Permitted enabled profiles |
| --- | --- | --- | --- | --- |
| `J4_RECORDED_LLM_ACTIVITY` | J4 | Omit the recorded-provider activity and use existing behavior | Admit `j4.recorded-llm@v1` in Go and Python | `j4-core`, `j4-b2` |
| `J4_PROVIDER_RESILIENCE` | J4 B2 | No B2 reservation or retry policy. J4 core makes one send per claim. | Apply the durable J4 provider budget and retry policy | `j4-b2`, with `J4_RECORDED_LLM_ACTIVITY=1` and the B2 migration applied |

For both switches, `required_unset_profiles` lists `K5`,
`K6_before_Phase_G`, `K6_Phase_G`, `J5a` and `J5b`. The activity switch
has no engine routes or migrations. The J4 stub's private listener serves
`POST /v1/recorded-completions` on 8082 in either J4 profile.

For B2, add two POST actions to the **internal worker listener only** under
`/v1/workflows/{workflowID}/nodes/{nodeID}/iterations/{iteration}/`.
The suffixes are `provider-attempts/reserve` and `provider-attempts/outcome`.
Add `migrations/j4_b2/000025_j4_provider_resilience.up.sql` to B2's
`migrations_added`. Its paired down migration is for rollback.

Register `j4-core` with base `runtime_base` and an empty additional
migration list. Register `j4-b2` with the same base and only the named
J4 B2 migration as an addition. Keep the existing K, J5 and retrieval
profile inventories. Both J4 profiles explicitly select the repaired
relay mode and leave the J19 policy unset.

[Reviewed J4 protocol](protocol.md)
