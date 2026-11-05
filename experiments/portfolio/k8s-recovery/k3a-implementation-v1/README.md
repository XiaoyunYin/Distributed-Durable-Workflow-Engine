# K3a native implementation validation

PROVISIONAL, uncommitted on base 0296ac59d90732c38d3f4a4f71ec82cb60dabc63.
This directory contains native test evidence only. No image/cluster/pilot/SLO
result is claimed. Claude commits the review target and CI supplies deployment
proof under a fresh RUNNER_TEMP output directory.

- [Validation matrix](validation.json): full fresh 19-migration Go integration
  race suite, vet, gofmt, Ruff, mypy, 236 pytest passes / three optional skips,
  four registry profiles, static archive/schema/workflow checks.
- [Mutations](mutations.json): 354 unique detections. A full Bash run covered
  329 cases; final K3 and driver follow-ups covered 108 and 21 respectively.
  Every selected case's normalized UTF-8 source hash matches the final source.
  This is the union of restored-source runs, not one claimed native monolithic
  354-case run. CI exercises the final manifest monolithically.
- [Source inventory](source-sha256.json): uncommitted source/tool/protocol hashes.
- cleanup.json records actual owned database and scratch absence after teardown.

CI must show both overlays rendered with real built digests, strict schemas,
policy and actual PostgreSQL budget; enforcing Calico; pre-rollout migrate and
exact-candidate compatibility Jobs; two ten-arrival terminal batches with complete
external witness and live/offline checks; real DB outage Pod readiness/liveness
and recovery; actual SIGTERM readiness; worker network timeouts and independent
401 refusals with positive operator/KEDA controls; and verified cleanup.
Failures stay artifacts. Claude downloads them, verifies hashes and records
run/job/head/merge-ref/tree identity with the cumulative live charge. No rerun
replaces a failure. No local Docker or kind run occurred.

Design boundaries: one host and one kind node; node-local witness PVC; fresh
namespaces only; no node-drain, upgrade/rollback, dead-worker witness, scaling,
fallback timing, SLO or HA evidence. K3b must finalize campaign topology and
reservations before its pilots; all measurement entry points currently refuse.
