package main

import (
	"bytes"
	"testing"
	"testing/fstest"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6live"
	"durable-agent-execution-engine/internal/invariants/k6v1"
)

func TestK6SubmissionIdentity(t *testing.T) {
	engine, dsn := k6TestDatabase(t)
	bindK6Database(t, engine, "engine-instance", "reset-a")
	m := k6Manifest(engine.Config().Database, false)
	config := k6live.Config{Schema: k6live.ConfigVersion, DatabaseURL: dsn, Manifest: m}
	last := time.Now().Add(-time.Minute)
	plan, arrivals := k6Sources(m, last, "pure.echo")
	warmup, uncertain := plan.Windows[0].Submissions[0], plan.Windows[1].Submissions[0]
	arrivals.Rows[1].Disposition, arrivals.Rows[1].UnsentReason = "sent", nil
	arrivals.Rows[1].Attempts = []k6v1.Send{
		{Attempt: 1, SentUTC: uncertain.ScheduledUTC, ObservedUTC: k6v1.UTC(last.Add(time.Second)), BodySHA256: uncertain.BodySHA256, Request: uncertain.Request, Outcome: "uncertain"},
		{Attempt: 2, SentUTC: k6v1.UTC(last.Add(time.Second)), ObservedUTC: arrivals.SealedUTC, BodySHA256: uncertain.BodySHA256, Request: uncertain.Request, Outcome: "uncertain"},
	}
	rejected := uncertain
	rejected.Sequence, rejected.Key = 1, "dur053-fixture-measured-530001-000001"
	rejected.Request = testWire(rejected.Key)
	unsent := uncertain
	unsent.Sequence, unsent.Key = 2, "dur053-fixture-measured-530001-000002"
	unsent.Request = testWire(unsent.Key)
	plan.Windows[1].Submissions = append(plan.Windows[1].Submissions, rejected, unsent)
	capacity, reason := 429, "overload"
	response := testWire("capacity")
	arrivals.Rows = append(arrivals.Rows,
		k6v1.Arrival{Window: "measured", Sequence: 1, Key: rejected.Key, Disposition: "sent", Attempts: []k6v1.Send{
			{Attempt: 1, SentUTC: rejected.ScheduledUTC, ObservedUTC: arrivals.SealedUTC, BodySHA256: rejected.BodySHA256, Request: rejected.Request, Response: &response, Outcome: "rejected", HTTPStatus: &capacity},
		}},
		k6v1.Arrival{Window: "measured", Sequence: 2, Key: unsent.Key, Disposition: "unsent", UnsentReason: &reason, Attempts: []k6v1.Send{}},
	)
	execK6SQL(t, engine, `INSERT INTO engine.workflow_definitions(definition_id,version,definition_hash,graph,activity_versions,effect_classes)
		VALUES ('definition',1,'hash','{"nodes":["pure.echo"]}','{"pure.echo":"v1"}','{"pure.echo":"PURE_ACTIVITY"}')`)
	minimalExecution(t, engine, warmup)
	minimalExecution(t, engine, uncertain)
	bundle := acquireK6Fixture(t, config, plan, arrivals, k6v1.Incomplete)
	if label := k6v1.Verify(bundle).Labels[0]; label.Outcome != "covered/pass" || label.Reason != "predicate_satisfied" || label.PopulationCount != 4 {
		t.Fatalf("clean submission label: %+v", label)
	}
	const relation = "engine.workflow_executions"
	filterRows := func(raw []byte, key string) []byte {
		var kept []byte
		needle := []byte(`"workflow_id":"` + key + `"`)
		for _, line := range bytes.SplitAfter(raw, []byte{'\n'}) {
			if !bytes.Contains(line, needle) {
				kept = append(kept, line...)
			}
		}
		return kept
	}
	expect := func(t *testing.T, data fstest.MapFS, codes ...string) {
		t.Helper()
		result := k6v1.Verify(data)
		for _, code := range codes {
			requireK6Finding(t, result, code)
		}
		if result.ExitCode() != k6v1.Fail || result.Safety != "FAIL" || result.Labels[0].Outcome != "covered/fail" {
			t.Fatalf("submission counterexample failed to reach I1: %+v", result)
		}
	}
	t.Run("N2RemoveAccepted", func(t *testing.T) {
		mutant := cloneBundle(bundle)
		resealK6Relation(t, mutant, relation, filterRows(mutant[k6v1.RelationPath(relation)].Data, warmup.Key), 1)
		expect(t, mutant, "SUBMISSION_ACCEPT_MISSING")
		execK6SQL(t, engine, "DELETE FROM engine.workflow_executions WHERE workflow_id=$1", warmup.Key)
		defer minimalExecution(t, engine, warmup)
		expect(t, acquireK6Fixture(t, config, plan, arrivals, k6v1.Fail), "SUBMISSION_ACCEPT_MISSING")
	})
	t.Run("N2ExtraDurable", func(t *testing.T) {
		extra := uncertain
		extra.Key = "unscheduled-workflow"
		minimalExecution(t, engine, extra)
		defer execK6SQL(t, engine, "DELETE FROM engine.workflow_executions WHERE workflow_id='unscheduled-workflow'")
		expect(t, acquireK6Fixture(t, config, plan, arrivals, k6v1.Fail), "SUBMISSION_DURABLE_UNPLANNED")
	})
	t.Run("N2Alias", func(t *testing.T) {
		mutant := cloneBundle(bundle)
		observed := arrivals
		observed.Rows = append([]k6v1.Arrival{}, arrivals.Rows...)
		observed.Rows[1].Attempts = []k6v1.Send{arrivals.Rows[0].Attempts[0]}
		observed.Rows[1].Attempts[0].SentUTC = uncertain.ScheduledUTC
		observed.Rows[1].Attempts[0].ObservedUTC = arrivals.SealedUTC
		observed.Rows[1].Attempts[0].Request = uncertain.Request
		observed.PlanSHA256 = k6v1.Hash(bundle[k6v1.PlanPath].Data)
		resealK6File(t, mutant, k6v1.ArrivalsPath, observed)
		expect(t, mutant, "SUBMISSION_WORKFLOW_ALIAS")
		expect(t, acquireK6Fixture(t, config, plan, observed, k6v1.Fail), "SUBMISSION_WORKFLOW_ALIAS")
	})
	t.Run("N2BodyHash", func(t *testing.T) {
		execK6SQL(t, engine, "UPDATE engine.workflow_executions SET submission_payload_hash='wrong' WHERE workflow_id=$1", uncertain.Key)
		defer execK6SQL(t, engine, "UPDATE engine.workflow_executions SET submission_payload_hash=$1 WHERE workflow_id=$2", "sub-v1:"+uncertain.BodySHA256, uncertain.Key)
		expect(t, acquireK6Fixture(t, config, plan, arrivals, k6v1.Fail), "SUBMISSION_DURABLE_BODY_HASH")
	})
	t.Run("N2OmitUncertainDurable", func(t *testing.T) {
		mutant := cloneBundle(bundle)
		observed := arrivals
		observed.Rows = append([]k6v1.Arrival{arrivals.Rows[0]}, arrivals.Rows[2:]...)
		observed.PlanSHA256 = k6v1.Hash(bundle[k6v1.PlanPath].Data)
		resealK6File(t, mutant, k6v1.ArrivalsPath, observed)
		expect(t, mutant, "SUBMISSION_ARRIVAL_MISSING", "SUBMISSION_DURABLE_WITHOUT_SEND")
		expect(t, acquireK6Fixture(t, config, plan, observed, k6v1.Fail), "SUBMISSION_ARRIVAL_MISSING", "SUBMISSION_DURABLE_WITHOUT_SEND")
	})
	t.Run("N3ConsistentSubset", func(t *testing.T) {
		mutant := cloneBundle(bundle)
		observed := arrivals
		observed.Rows = append([]k6v1.Arrival{arrivals.Rows[0]}, arrivals.Rows[2:]...)
		observed.PlanSHA256 = k6v1.Hash(bundle[k6v1.PlanPath].Data)
		resealK6File(t, mutant, k6v1.ArrivalsPath, observed)
		resealK6Relation(t, mutant, relation, filterRows(mutant[k6v1.RelationPath(relation)].Data, uncertain.Key), 1)
		resealK6Relation(t, mutant, k6History, filterRows(mutant[k6v1.RelationPath(k6History)].Data, uncertain.Key), 2)
		expect(t, mutant, "SUBMISSION_ARRIVAL_MISSING", "NAMESPACE_COUNT")
	})
	t.Run("UnsentAndRejectedDurables", func(t *testing.T) {
		minimalExecution(t, engine, unsent)
		minimalExecution(t, engine, rejected)
		result := k6v1.Verify(acquireK6Fixture(t, config, plan, arrivals, k6v1.Fail))
		var violations int
		for _, finding := range result.Findings {
			if finding.Code == "SUBMISSION_DURABLE_WITHOUT_SEND" {
				violations++
			}
		}
		if violations != 2 {
			t.Fatalf("expected a violation for rejected and unsent submissions: %+v", result.Findings)
		}
	})
}

func TestK6CutDeadlineLive(t *testing.T) {
	_, _, config, _, _ := pairedK6Fixture(t)
	plan, arrivals := k6Sources(config.Manifest, time.Now().Add(-605*time.Second), "resource")
	bundle := acquireK6Fixture(t, config, plan, arrivals, k6v1.Fail)
	requireK6Finding(t, k6v1.Verify(bundle), "CUT_DEADLINE")
}
