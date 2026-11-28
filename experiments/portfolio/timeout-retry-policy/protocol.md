# J19 expired-attempt replacement policy

## Slice 0 decision

An expired claimed pure or cooperating attempt spends a separate expiry
allowance. The node instance's retry policy supplies the limit for each
allowance independently. With the default `max_retries = 3`, three claimed
losses can dispatch a fourth attempt even if the node has already used its
reported-failure retries. Sharing one allowance would leave fewer chances
after a mix of retryable activity failures and infrastructure kills during a
rollout, scale-down, or the J13 upgrade. The independent allowances preserve
those chances while bounding a poison activity. The fourth claimed loss
exhausts the default expiry allowance.

- The counter is per workflow, node and iteration. Earlier pure or cooperating
  `TIMED_OUT` attempts are the durable charges. With a current claimed attempt
  present, each earlier eligible timeout committed a replacement. Legacy
  replacements count when the switch is enabled on an existing workflow.
  Retrying a scheduler write, changing owners or changing the policy preserves
  the count. Reported failures, unclaimed redispatch and non-cooperating
  reconciliation spend zero expiry charges.
- Permit a replacement when the count is strictly below `max_retries`. Zero
  denies the first replacement. The expiry write, new attempt, current
  pointer, revision, history and dispatch commit in the same fenced
  transaction. A successful replacement contributes one charge.
- Both retry paths use the node policy's absolute `total_deadline_at`.
  For claimed timeout recovery, sample PostgreSQL's clock in the fenced
  transaction after reading the policy and allowance. A null deadline is
  unlimited. At or after the deadline, deny the next replacement, with
  deadline taking precedence if both bounds are reached. The deadline is an
  admission check for replacement and leaves result acceptance and heartbeat
  handling on their existing paths.
- A denied replacement retires the expired attempt as noncurrent `TIMED_OUT`
  and clears the node's current pointer. The node and workflow become
  `FAILED`. Any live sibling nodes and their current attempts are canceled
  using the usual cancellation dispositions, and pending timers are retired.
  The terminal revision records
  the scheduler, lease epoch, node, iteration, expired attempt and the reason
  `TIMEOUT_REPLACEMENT_BUDGET_EXHAUSTED` or
  `TIMEOUT_REPLACEMENT_DEADLINE_EXCEEDED`. Operators can read the reason in
  workflow history. That revision creates zero outbox rows. Existing outbox
  rows remain available for audit and stale notices cannot claim terminal work.
- The switch is `RUNTIME_TIMEOUT_RETRY_POLICY=1`. Unset, empty and `0` select
  legacy behavior. See the [runtime switch registry](../../../deploy/runtime-switches.json).
  J8 selects the policy on. K5/K6 retain the unset setting through closeout.
  Use the same setting on every scheduler that can own a partition. During
  rollout, the current owner's setting governs recovery.

## Frozen validation

Run the PostgreSQL regressions in the `services-race` CI job. Verify both
eligible effect classes through repeated claimed expiry, the last allowed
replacement and the first denied one, a zero allowance, and absence of any
new outbox rows on exhaustion or stale scheduler retry. Verify the failed
node, workflow, retired attempt and terminal history reason, and check that the
reported-failure counter stays independent. Check that enabling the policy
counts prior legacy replacements and that terminal failure settles live
siblings and pending timers. Verify a future deadline allows
replacement and a passed deadline denies one. Pin equality and deadline
precedence with a deterministic Go test.

Verify successful completion below the bound after claimed worker losses by
expiring claims without sending a worker result, then accepting a result from
a fresh worker. In the engine regression, kill a real worker subprocess after
its claim commits, let the claim expire, and finish with the replacement worker.
Verify unclaimed redispatch and non-cooperating reconciliation
with both bounds already reached. The unset legacy-equivalence test uses both
eligible classes and checks the exact legacy transition and outbox sequence
after both bounds are exceeded. Run the environment parsing test without
PostgreSQL. The broader K profile admission checks remain a sequencing gate
for registered runs.

The hosted tests are `TestPostgresTimeoutRetryPolicy` and
`TestPostgresTimeoutRetryUnsetLegacyEquivalence` in `internal/state`, and
`TestPostgresTimeoutRetryWorkerKill` in `internal/engine`.
