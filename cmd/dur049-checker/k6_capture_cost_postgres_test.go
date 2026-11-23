package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// Paired wall times include a committed statement and all deferred triggers.
// This is a single-connection microbenchmark with K4 audit enabled in both arms.
func measureCapture(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	execK6SQL(t, conn, `
		INSERT INTO engine.approval_action_intents(intent_id,workflow_id,node_id,iteration,proposal_hash,target,
			canonical_arguments,valid_until) SELECT md5('intent')::uuid, workflow_id, 'pure.echo',0,'hash','target','{}',now()
			FROM engine.workflow_executions;
		INSERT INTO engine.outbox_publications(publication_id,event_id,relay_owner,relay_attempt,broker_acknowledged)
			VALUES (md5('publication')::uuid,md5('o1')::uuid,'`+captureOwnerID+`',1,true);
		INSERT INTO engine.event_inbox(consumer_id,event_id,disposition,topic,kafka_partition,kafka_offset)
			VALUES ('runtime-schedulers-v1',md5('o1')::uuid,'ACCEPTED','durable-agent.events.v1',0,0);
		INSERT INTO engine.consumer_offsets(consumer_id,topic,kafka_partition,next_offset)
			VALUES ('runtime-schedulers-v1','durable-agent.events.v1',0,1);
		INSERT INTO engine.scheduler_wakeups(wakeup_id,workflow_id,partition_id,event_id,reason)
			SELECT md5('wakeup')::uuid, workflow_id, 0, md5('o1')::uuid,'EVENT:workflow.created' FROM engine.workflow_executions;
		INSERT INTO engine.transport_quarantine(quarantine_id,consumer_id,topic,kafka_partition,kafka_offset,payload,reason)
			VALUES (md5('quarantine')::uuid,'runtime-schedulers-v1','durable-agent.events.v1',0,1,''::bytea,'fixture');
		INSERT INTO engine.reconciliation_items(item_id,workflow_id,partition_id,kind,reference)
			SELECT md5('reconciliation')::uuid,workflow_id,0,'DUE_TIMER','fixture-timer' FROM engine.workflow_executions`)
	type row struct {
		Class       string  `json:"class"`
		Pairs       int     `json:"pairs"`
		OffMedianUS float64 `json:"off_median_us"`
		OnMedianUS  float64 `json:"on_median_us"`
		DeltaUS     float64 `json:"paired_median_delta_us"`
	}
	const n = 20
	var timings []row
	for _, test := range []struct{ class, statement string }{
		{"history", "UPDATE engine.transition_history SET reason=reason WHERE revision=1"},
		{"node", "UPDATE engine.node_instances SET state=state"},
		{"attempt", "UPDATE engine.activity_attempts SET updated_at=updated_at"},
		{"lease", "UPDATE engine.partition_leases SET lease_expires_at=lease_expires_at WHERE partition_id=0"},
		{"timer", "UPDATE engine.timers SET due_at=due_at"},
		{"approval_grant", "UPDATE engine.approval_action_intents SET grant_expires_at=grant_expires_at"},
		{"outbox", "UPDATE engine.outbox SET publish_state=publish_state WHERE aggregate_revision=1"},
		{"publication_mark", "UPDATE engine.outbox_publications SET broker_acknowledged=broker_acknowledged"},
		{"inbox", "UPDATE engine.event_inbox SET disposition=disposition"},
		{"consumer_offset", "UPDATE engine.consumer_offsets SET next_offset=next_offset"},
		{"wakeup", "UPDATE engine.scheduler_wakeups SET state=state"},
		{"quarantine", "UPDATE engine.transport_quarantine SET reason=reason"},
		{"reconciliation", "UPDATE engine.reconciliation_items SET status=status"},
	} {
		var off, on, delta []float64
		for i := 0; i < n; i++ {
			var measured [2]float64
			for step := 0; step < 2; step++ {
				mode := (step + i) % 2
				execK6SQL(t, conn, "SET durable.k6_capture = "+[]string{"off", "on"}[mode])
				start := time.Now()
				tag, err := conn.Exec(t.Context(), test.statement)
				measured[mode] = float64(time.Since(start).Nanoseconds()) / 1000
				if err != nil || tag.RowsAffected() != 1 {
					t.Fatalf("measure %s: %s %v", test.class, tag, err)
				}
			}
			off, on, delta = append(off, measured[0]), append(on, measured[1]), append(delta, measured[1]-measured[0])
		}
		slices.Sort(off)
		slices.Sort(on)
		slices.Sort(delta)
		timings = append(timings, row{test.class, n, off[n/2], on[n/2], delta[n/2]})
	}
	raw, err := json.MarshalIndent(timings, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, '\n')
	t.Logf("paired PostgreSQL capture overhead (microseconds): %s", raw)
	writeVerdictArtifact(t, filepath.Join(os.Getenv("DURABLE_K6_VERDICT_ARTIFACT"), t.Name(), "capture-overhead.json"), raw)
}
