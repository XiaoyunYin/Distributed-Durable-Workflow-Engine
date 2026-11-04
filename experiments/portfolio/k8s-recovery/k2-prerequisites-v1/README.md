# DUR-053 K2a prerequisites

Scope: R218 identity/route isolation and R222 database clock construction.
Base 2406292fa890096603274cf6bdfb315e83232011, uncommitted working tree;
Claude commits. K2 stays IN_PROGRESS; handoff PROVISIONAL.

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
