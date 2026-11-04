# DUR-053 K2 prerequisites

The latest K2b demand/registry evidence is in the section below. K2 remains
IN_PROGRESS; K2b is uncommitted and PROVISIONAL. K2a is accepted at c36a2dd.

## Historical K2a evidence

Scope: R218 identity/route isolation and R222 database clock construction.
Original evidence below describes the earlier K2a working tree on base
2406292fa890096603274cf6bdfb315e83232011, subsequently committed by Claude at
d007d26507fee4ca50e68bcdc389de160fbf647f. It is historical evidence, not a
claim that those commands were rerun in the Round 158 fix pass.

Round 158 R233/R234 evidence was PROVISIONAL and uncommitted on base d007d26;
Claude subsequently committed it at c36a2dd and accepted K2a in Round 159.
Files with `r158` in their names contain only that fix pass's executed checks. The authoritative
check/skip record is validation-r158.json; mutation results, full race output,
identity output, source hashes, and cleanup are separately suffixed `r158`.
No cluster, Compose, hosted CI, or real TokenReview signature proof is claimed.

Identity: Go TLS TokenReview fixtures test route absence, authenticated actor
binding, missing/wrong/expired credentials, trust configuration, reviewer
rotation and fail-closed outages. Python checks projected-token rotation,
defaults and missing/empty files. These are local contract tests; no Kubernetes
API server, kind, pod-to-port probe, deployed RBAC or real signature campaign ran.

Skew: native PostgreSQL with all 18 existing migrations; scheduler clocks at
+30 s and -30 s with opposite-skew peers. Tests independently measure persisted
timer/dispatch/retry/replacement intervals, reject early firing/timeout, recover
after expiry, preserve redispatch identity, reject stale result/owner writes and
require terminal progress. No TimeChaos injection ran.

See validation.json for actual checks/cleanup and mutations.json for the shared
gate. Demand, compatibility, checker/witness, CPU activity, open-loop generator,
Compose workload proof and prerequisite CLI are outside this part.

## K2b demand and registry prerequisites

Base `c36a2dd0d794d17b918c18dab19c7d6cdcfb5314` on `codex/k8s-k2`;
K2a accepted by Claude Round 159. K2b is uncommitted; Claude commits.
K2 remains IN_PROGRESS and the handoff is PROVISIONAL.

[Demand and registry contract/evidence](demand-registry-k2b.md) describes only
R224/R226. Files suffixed `k2b` record this pass, separately from historical
K2a and Round 158 results. The final check results are in validation-k2b.json;
mutation detections in mutations-k2b.json and cleanup in cleanup-k2b.json.
No K2c checker/witness, CPU activity, open-loop generator, Compose proof or
prerequisite CLI implementation is included. No cluster or scaler ran here.
