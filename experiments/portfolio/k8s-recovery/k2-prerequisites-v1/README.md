# DUR-053 K2a prerequisites

Scope: R218 identity/route isolation and R222 database clock construction.
Original evidence below describes the earlier K2a working tree on base
2406292fa890096603274cf6bdfb315e83232011, subsequently committed by Claude at
d007d26507fee4ca50e68bcdc389de160fbf647f. It is historical evidence, not a
claim that those commands were rerun in the Round 158 fix pass.

Round 158 R233/R234 follow-up uses base d007d26 and stays uncommitted;
Claude commits. K2 stays IN_PROGRESS; handoff PROVISIONAL. Files with `r158`
in their names contain only this pass's executed checks. The authoritative
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
