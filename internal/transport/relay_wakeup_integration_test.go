package transport

import (
	"context"
	"testing"

	"durable-agent-execution-engine/internal/state"
)

func TestJ9aRelayWakeupPostgresBacklog(t *testing.T) {
	const backlog = 10 * relayTestBatchSize
	for _, tc := range []struct {
		name          string
		brokerErrors  int
		wantPasses    int
		wantPublished int
		wantAttempts  int
	}{
		{"healthy", 0, 11, backlog, backlog},
		{"failing_broker", backlog, 1, 0, relayTestBatchSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(RelayFullBatchLoopEnv, "1")
			ctx, store := openM3TransportDatabase(t)
			defer store.Close()
			definitionID, workflowID, lease := createM3TransportWorkflow(t, ctx, store)
			defer cleanupM3TransportWorkflow(t, ctx, store, definitionID, workflowID, lease)
			// CreateWorkflow inserted the first row. Seed the rest before
			// exercising one wakeup, with no listener, ticks or new arrivals.
			if _, err := store.Pool().Exec(ctx, `
				INSERT INTO engine.outbox
					(event_id, workflow_id, aggregate_revision, event_type, topic, payload)
				SELECT gen_random_uuid(), $1, revision, 'workflow.created', $2, '{}'::jsonb
				FROM generate_series(2, $3::integer) AS series(revision)`,
				workflowID, state.EventTopic, backlog); err != nil {
				t.Fatal(err)
			}
			broker := &MemoryBroker{Failures: tc.brokerErrors}
			relay := NewRelay(store, broker, RelayConfig{PartitionID: &lease.PartitionID,
				OnError: func(err error) { t.Fatalf("unexpected relay pass error: %v", err) }})
			passes := 0
			relay.runWakeup(ctx, func(ctx context.Context) (RelayReport, error) {
				passes++
				if passes > tc.wantPasses {
					t.Fatalf("one wakeup ran more than %d passes", tc.wantPasses)
				}
				report, err := relay.RunOnce(ctx)
				if tc.brokerErrors > 0 && (report.Claimed != relayTestBatchSize || report.Failed != relayTestBatchSize) {
					t.Fatalf("first batch did not record every publish failure: %+v (%v)", report, err)
				}
				return report, err
			})
			if passes != tc.wantPasses || len(broker.Events()) != tc.wantPublished {
				t.Fatalf("passes = %d, broker publications = %d", passes, len(broker.Events()))
			}
			rows, err := store.ListOutbox(ctx, workflowID, backlog+1)
			if err != nil || len(rows) != backlog {
				t.Fatalf("outbox count = %d, err = %v", len(rows), err)
			}
			attempts, published := 0, 0
			for _, row := range rows {
				if row.RelayAttempts > 1 || row.PublishState == state.OutboxClaimed {
					t.Fatalf("outbox attempts = %d, state = %s for %s", row.RelayAttempts, row.PublishState, row.EventID)
				}
				attempts += row.RelayAttempts
				if row.PublishState == state.OutboxPublished {
					published++
				}
			}
			if attempts != tc.wantAttempts || published != tc.wantPublished {
				t.Fatalf("durable attempts = %d, published = %d", attempts, published)
			}
		})
	}
}
