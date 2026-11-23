package k6v1

import (
	"maps"
	"testing"
)

func assertNoPromotion(t *testing.T, label Label) {
	t.Helper()
	if label.Registered || label.Outcome != "NOT COVERED" || label.Reason != "outside_registry_ceiling" {
		t.Fatalf("registry ceiling changed: %+v", label)
	}
}

func TestK6OutsideCeilingSafety(t *testing.T) {
	n := testNumber
	for _, tc := range []struct {
		name, predicate, code, revision, transition string
		index                                       int
	}{
		{"OrdinaryResult", "", "", "", "", 4},
		{"ResultReplay", replayPredicate, "RESULT_REAPPLIED", "6", "end", 4},
		{"UnknownEffect", "", "", "", "", 8},
		{"UnresolvedReapply", uncertaintyPredicate, "UNRESOLVED_EFFECT_REAPPLIED", "5", "result", 8},
		{"ResolvedLaterAttempt", "", "", "", "", 8},
		{"ShutdownAbort", "", "", "", "", 12},
		{"ShutdownFailure", shutdownPredicate, "SHUTDOWN_RECORDED_AS_FAILURE", "5", "result", 12},
		{"EffectAbortAmbiguous", "", "", "", "", 12},
		{"UnboundAbort", "", "", "", "", 12},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := orderFixture()
			var aborts []observedWorker
			switch tc.name {
			case "ResultReplay":
				h := rows[history][5]
				h["reason"], h["actor_kind"], h["actor_id"] = resultRecorded, "worker", "worker"
			case "UnknownEffect", "UnresolvedReapply", "ResolvedLaterAttempt":
				rows[history][3]["reason"] = "TIMEOUT_RECONCILIATION_REQUIRED"
				for _, row := range rows[activityAttempts] {
					row["effect_class"] = "NON_COOPERATING_EFFECT"
				}
				item := maps.Clone(rows[activityAttempts][0])
				item["kind"], item["status"] = "EXPIRED_ATTEMPT", "OPEN"
				rows[reconciliation] = []Row{item}
				if tc.name == "UnknownEffect" {
					rows[history][4]["reason"] = "NON_RESULT"
				} else if tc.name == "ResolvedLaterAttempt" {
					item["status"] = "RESOLVED"
				}
			case "ShutdownAbort", "ShutdownFailure", "EffectAbortAmbiguous", "UnboundAbort":
				a := rows[activityAttempts][1]
				a["effect_class"], a["state"] = "PURE_ACTIVITY", "FAILED_RETRYABLE"
				identity := AttemptIdentity{Workflow: "wf", Node: "node", Attempt: 2, ClaimHash: "token-2", RequestID: "request-2"}
				aborts = []observedWorker{{session: WorkerSession{ID: "session", Claimant: "worker"},
					row: Record[WorkerRow]{Position: Position{Sequence: 3}, Data: WorkerRow{Kind: "abort", Attempt: &identity}}}}
				switch tc.name {
				case "ShutdownAbort":
					rows[history][4]["reason"] = "NON_RESULT"
				case "EffectAbortAmbiguous":
					a["effect_class"] = "COOPERATING_EFFECT"
				case "UnboundAbort":
					identity.RequestID = "wrong-request"
				}
			}
			r := transportOrder(t, Manifest{Cell: "hook-0"}, rows, func(e engineOrder) {
				e.checkReplayAndUncertainty()
				e.checkShutdown(aborts)
			})
			label := r.Labels[tc.index]
			assertNoPromotion(t, label)
			switch tc.name {
			case "ResultReplay":
				requireTransportFinding(t, r, replayPredicate, "required_proof_missing:result_commit_identity", MissingProof,
					RelationPath(history), []any{"wf", n("6"), "end"})
			case "UnknownEffect":
				requireTransportFinding(t, r, uncertaintyPredicate, "required_proof_missing:effect_uncertainty_resolution_transactions", MissingProof,
					RelationPath(history), []any{"wf", n("4"), "replacement"})
			case "ShutdownAbort":
				requireTransportFinding(t, r, shutdownPredicate, "required_proof_missing:shutdown_result_binding", MissingProof,
					SessionPath("session"), []any{int64(3)})
			}
			if tc.code == "" {
				if r.Safety == "FAIL" {
					t.Fatalf("ordinary out-of-ceiling rows failed safety: %+v", r.Findings)
				}
				return
			}
			requireTransportFinding(t, r, tc.predicate, tc.code, Violation, RelationPath(history),
				[]any{"wf", n(tc.revision), tc.transition})
			if r.Safety != "FAIL" || r.ExitCode() != Fail || len(label.Witnesses) < 1 {
				t.Fatalf("missing linked out-of-ceiling finding: %+v", r)
			}
		})
	}
}
