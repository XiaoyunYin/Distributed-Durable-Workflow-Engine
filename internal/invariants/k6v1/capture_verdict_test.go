package k6v1

import (
	"maps"
	"slices"
	"testing"
	"time"
)

func capturedRow(subject, operation, order, tx string, key []any, before, after Row) Row {
	row := Row{"subject": subject, "operation": operation, "event_order": testNumber(order),
		"transaction_id": tx, "row_key": key}
	if before != nil {
		row["before_image"] = map[string]any(before)
	}
	if after != nil {
		row["after_image"] = map[string]any(after)
	}
	return row
}

func TestK6CaptureAwards(t *testing.T) {
	t.Run("EmptyCapture", func(t *testing.T) {
		c := newClosureFixture()
		c.workflows = map[string]Row{"workflow": {"workflow_id": "workflow"}}
		c.rows[WriteCapture], c.rows[OwnerWrites] = nil, nil
		if c.checkCaptureClosure() {
			t.Fatal("empty capture became eligible for awards")
		}
		for _, relation := range []string{WriteCapture, OwnerWrites} {
			if !slices.ContainsFunc(c.result.Findings, func(f Finding) bool {
				return f.Predicate == capturePredicate && f.Kind == MissingProof && f.Source == RelationPath(relation)
			}) {
				t.Fatalf("missing capture relation was not typed: %s %+v", relation, c.result.Findings)
			}
		}
	})
	t.Run("HistoryChanges", func(t *testing.T) {
		c := newClosureFixture()
		c.checkHistoryChanges(orderFixture()[history][2], historyChanges{})
		wantCaptureCode(t, c, "CAPTURE_HISTORY_CHANGES")
	})
	t.Run("ClaimTransaction", func(t *testing.T) {
		c := newClosureFixture()
		c.rows[commitAudit] = orderFixture()[commitAudit]
		c.checkCapturedClaims()
		wantCaptureCode(t, c, "CAPTURE_CLAIM_TRANSACTION")
	})
	t.Run("AtomicHistory", func(t *testing.T) {
		rows := orderFixture()
		result := transportOrder(t, Manifest{Cell: "hook-0"}, rows, func(e engineOrder) {
			e.capture, e.captureReady = true, true
			e.checkAtomicHistory()
		})
		if len(result.Findings) != 0 {
			t.Fatalf("complete atomic history was blocked: %+v", result.Findings)
		}
	})
	t.Run("CompleteAndBound", func(t *testing.T) {
		rows := orderFixture()
		rows[OwnerWrites] = []Row{{"event_order": testNumber("9")}}
		for _, stale := range []bool{false, true} {
			result := transportOrder(t, Manifest{Cell: "hook-5"}, rows, func(e engineOrder) {
				e.capture, e.captureReady = true, true
				claim := maps.Clone(rows[activityAttempts][1])
				if stale {
					claim["claim_token_hash"] = "stale"
				}
				e.resultCommits["result"] = capturedResult{Row{"after_image": map[string]any{"attempt_state": "SUCCEEDED"}}, claim}
				e.checkOwnerAtCommit()
				e.checkCurrentClaimWinner()
			})
			if label := result.Labels[2]; label.Outcome != "NOT COVERED" ||
				label.Reason != "required_proof_missing:DeployedStaleChallenge" || label.PopulationCount != 1 {
				t.Fatalf("owner writes alone awarded I3: %+v", label)
			}
			if !stale && result.Labels[3].Outcome != "covered/pass" {
				t.Fatalf("complete capture was not awarded: %+v", result.Labels[2:4])
			}
			if stale {
				requireTransportFinding(t, result, "CurrentClaimWinner", "CAPTURE_RESULT_CLAIM", Violation,
					RelationPath(history), []any{"wf", testNumber("5"), "result"})
			}
		}
	})
	t.Run("ProofReceipt", func(t *testing.T) {
		rows := orderFixture()
		h, attempt := rows[history][4], rows[activityAttempts][1]
		attempt["effect_class"], attempt["logical_effect_key"] = "COOPERATING_EFFECT", "effect"
		attempt["result"] = map[string]any{"application_id": "payload"}
		attempt["k6_proof"] = map[string]any{"receipt": map[string]any{"application_id": "verified"}}
		result := transportOrder(t, Manifest{Cell: "hook-0"}, rows, func(e engineOrder) {
			e.checkWinnerReceipt(h, attempt, capturedResult{attempt: Row{"receipt_id": "verified"}}, map[any]Row{
				"verified": {"workflow_id": "wf", "logical_key": "effect"},
				"payload":  {"workflow_id": "foreign", "logical_key": "other"},
			})
		})
		if len(result.Findings) != 0 {
			t.Fatalf("receipt join ignored the proof identity: %+v", result.Findings)
		}
	})
}

func TestK6CaptureRemainder(t *testing.T) {
	rows := dispatchFixture()
	result := transportOrder(t, Manifest{Cell: "hook-3"}, rows, func(e engineOrder) {
		e.capture, e.captureReady = true, true
		e.checkDispatch()
		e.checkLowerOffsets(nil)
	})
	want := []string{"broker_event_copies", "broker_reset_hold_restart_offsets", "broker_event_copies"}
	if len(result.Findings) != len(want) {
		t.Fatalf("capture changed the broker-only remainder: %+v", result.Findings)
	}
	for i, g := range want {
		if f := result.Findings[i]; f.Kind != MissingProof || f.Code != "required_proof_missing:"+g {
			t.Fatalf("capture changed the broker-only remainder: %+v", result.Findings)
		}
	}
}

func TestK6CaptureAbsentWatermark(t *testing.T) {
	spec := CaptureRelations()[2]
	absent := Watermark{Relation: spec.Name, Count: -1}
	empty := NewWatermark(spec).Finish()
	if !absent.valid(spec) || !equalWatermark(absent, absent) || equalWatermark(absent, empty) || equalWatermark(empty, absent) {
		t.Fatal("absent capture was treated as an empty relation")
	}
}

func TestK6CapturedPublication(t *testing.T) {
	n := testNumber
	for _, tc := range []struct{ name, code string }{
		{"LostAck", ""}, {"ChangedRetry", "CAPTURE_PUBLICATION_IDENTITY"},
		{"MarkWithoutStart", "CAPTURE_PUBLICATION_MARK"}, {"ClaimGap", "CAPTURE_PUBLICATION_CLAIM"},
		{"UncommittedStart", "CAPTURE_PUBLICATION_START"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newClosureFixture()
			key := []any{"event"}
			initial := Row{"key": "event", "payload_hash": "payload", "topic": taskTopic, "relay_attempts": n("0"), "publish_state": "PENDING"}
			first, second, closed := maps.Clone(initial), maps.Clone(initial), maps.Clone(initial)
			first["publish_state"], first["claim_owner"], first["relay_attempts"] = "CLAIMED", "relay", n("1")
			second["publish_state"], second["claim_owner"], second["relay_attempts"] = "CLAIMED", "relay", n("2")
			closed["publish_state"], closed["relay_attempts"] = "PUBLISHED", n("2")
			start := Row{"key": "event", "payload_hash": "payload", "topic": taskTopic, "relay_owner": "relay"}
			retry := maps.Clone(start)
			startTx := "start-1"
			switch tc.name {
			case "ChangedRetry":
				retry["payload_hash"] = "changed"
			case "UncommittedStart":
				startTx = "claim-1"
			case "ClaimGap":
				initial["relay_attempts"] = n("-1")
			}
			rows := []Row{
				capturedRow(outbox, "insert", "1", "create", key, nil, initial),
				capturedRow(outbox, "update", "2", "claim-1", key, initial, first),
				capturedRow(publicationStart, "start", "3", startTx, []any{"event", n("1")}, nil, start),
				capturedRow(outbox, "update", "4", "claim-2", key, first, second),
			}
			if tc.name != "MarkWithoutStart" {
				rows = append(rows, capturedRow(publicationStart, "start", "5", "start-2", []any{"event", n("2")}, nil, retry))
			}
			c.rows[WriteCapture] = append(rows,
				capturedRow(publications, "insert", "6", "mark", []any{"mark"}, nil, Row{
					"event_id": "event", "relay_attempt": n("2"), "relay_owner": "relay", "broker_acknowledged": true}),
				capturedRow(outbox, "update", "7", "mark", key, second, closed))
			c.checkCapturedPublications()
			if tc.code != "" {
				wantCaptureCode(t, c, tc.code)
			} else if len(c.result.Findings) != 0 {
				t.Fatalf("lost acknowledgement rejected: %+v", c.result.Findings)
			}
		})
	}
}

func TestK6CapturedDisposition(t *testing.T) {
	n := testNumber
	for _, tc := range []struct{ name, code, predicate string }{
		{"OmittedInbox", "CAPTURE_DELIVERY_BINDING", dispatchPredicate},
		{"OffsetWithoutDelivery", "CAPTURE_OFFSET_TRANSACTION", dispatchPredicate},
		{"UncapturedLower", "CAPTURE_OFFSET_DISPOSITION_GAP", lowerOffsetsPredicate},
		{"Reconciliation", "CAPTURE_RECONCILIATION_BINDING", dispatchPredicate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newClosureFixture()
			partition := []any{workerGroup, taskTopic, n("0")}
			key := append(slices.Clone(partition), n("0"))
			zero, one := Row{"next_offset": n("0")}, Row{"next_offset": n("1")}
			if tc.name == "UncapturedLower" {
				one["next_offset"] = n("2")
			}
			c.rows[WriteCapture] = []Row{
				capturedRow(offsets, "insert", "1", "delivery", partition, nil, zero),
				capturedRow(offsets, "update", "2", "delivery", partition, zero, one),
			}
			if tc.name != "OffsetWithoutDelivery" {
				c.rows[WriteCapture] = append(c.rows[WriteCapture], capturedRow(physicalDelivery, "ACCEPTED", "3", "delivery", key, nil, Row{"event_id": "event"}))
			}
			if tc.name != "OmittedInbox" {
				c.rows[WriteCapture] = append(c.rows[WriteCapture], capturedRow(inbox, "insert", "4", "delivery", []any{workerGroup, "event"}, nil,
					Row{"topic": taskTopic, "kafka_partition": n("0"), "kafka_offset": n("0"), "disposition": "ACCEPTED"}))
			}
			if tc.name == "Reconciliation" {
				c.rows[WriteCapture] = append(c.rows[WriteCapture], capturedRow(reconciliation, "insert", "5", "reconcile", []any{"item"}, nil,
					Row{"kind": "PENDING_OUTBOX", "reference": "outbox/missing"}))
			}
			c.checkCapturedDispositions(map[any]Row{"event": {"topic": taskTopic}})
			if !slices.ContainsFunc(c.result.Findings, func(f Finding) bool { return f.Code == tc.code && f.Predicate == tc.predicate }) {
				t.Fatalf("expected capture violation %s: %+v", tc.code, c.result.Findings)
			}
		})
	}
}

func TestK6CapturedOutsideCeiling(t *testing.T) {
	n := testNumber
	for _, name := range []string{"ResultReplay", "LateResolution", "ResolutionBeforeReplay", "UnboundResolution", "ShutdownFailure", "ShutdownNoObservation"} {
		t.Run(name, func(t *testing.T) {
			rows := orderFixture()
			old, current := rows[activityAttempts][0], rows[activityAttempts][1]
			var aborts []observedWorker
			var external External
			result := transportOrder(t, Manifest{Cell: "hook-0"}, rows, func(e engineOrder) {
				e.captureReady = true
				result := capturedResult{Row{"after_image": map[string]any{"worker_request_id": "request-2", "token_hash": "token-2", "attempt_state": "FAILED_FINAL"}}, current}
				e.resultCommits["result"] = result
				linkedHistory := func(id, order, tx string) Row {
					row := capturedRow(history, "insert", order, tx, []any{id}, nil, nil)
					row["workflow_id"] = "wf"
					return row
				}
				resultHistory := linkedHistory("result", "40", "result")
				switch name {
				case "ResultReplay":
					rows[history][5]["reason"] = resultRecorded
					e.resultCommits["end"] = result
					rows[WriteCapture] = []Row{resultHistory, linkedHistory("end", "50", "replay")}
				case "ShutdownFailure", "ShutdownNoObservation":
					current["effect_class"] = "PURE_ACTIVITY"
					identity := AttemptIdentity{Workflow: "wf", Node: "node", Attempt: 2, ClaimHash: "token-2", RequestID: "request-2"}
					aborts = []observedWorker{{session: WorkerSession{ID: "session", Claimant: "worker"}, startSequence: 2,
						row: Record[WorkerRow]{Position: Position{Sequence: 3, UTC: UTC(fixed.Add(time.Second))}, Data: WorkerRow{Kind: "abort", Invocation: "invocation", Attempt: &identity}}}}
					if name == "ShutdownFailure" {
						external = shutdownFixture(aborts[0])
					}
				default:
					old["effect_class"], current["effect_class"], old["logical_effect_key"] = "NON_COOPERATING_EFFECT", "NON_COOPERATING_EFFECT", "effect"
					rows[history][3]["reason"] = "TIMEOUT_RECONCILIATION_REQUIRED"
					e.captureWrites[captureTransaction{"timeout", "wf"}] = []Row{
						capturedRow(activityAttempts, "update", "10", "timeout", []any{"wf", "node", n("0"), n("1")}, nil,
							Row{"effect_class": "NON_COOPERATING_EFFECT", "outcome_disposition": "OUTCOME_UNKNOWN"}),
						capturedRow(reconciliation, "insert", "11", "timeout", []any{"item"}, nil,
							Row{"kind": "EXPIRED_ATTEMPT", "status": "OPEN", "node_id": "node", "iteration": n("0"), "attempt_number": n("1")}),
					}
					resolution := capturedRow("effects.effect_resolution_audit", "insert", "35", "resolution", []any{"audit"}, nil,
						Row{"logical_effect_key": "effect", "disposition": "CONFIRMED_APPLIED"})
					resolution["workflow_id"] = "wf"
					if name != "UnboundResolution" {
						e.captureWrites[captureTransaction{"resolution", "wf"}] = []Row{
							capturedRow("effects.effect_records", "update", "34", "resolution", []any{"wf", "effect"},
								Row{"outcome": "OUTCOME_UNKNOWN", "attempt_number": n("1")}, Row{"outcome": "APPLIED", "attempt_number": n("1")}),
						}
					}
					rows[WriteCapture] = []Row{linkedHistory("replacement", "12", "timeout"), resolution, resultHistory}
					if name == "LateResolution" {
						resolution["event_order"] = n("45")
						rows[WriteCapture][1], rows[WriteCapture][2] = resultHistory, resolution
					}
				}
				e.checkReplayAndUncertainty()
				e.checkShutdown(aborts, external)
			})
			for _, i := range []int{4, 8, 12} {
				assertNoPromotion(t, result.Labels[i])
			}
			switch name {
			case "ResultReplay":
				requireTransportFinding(t, result, replayPredicate, "RESULT_REAPPLIED", Violation, RelationPath(history), []any{"wf", n("6"), "end"})
			case "LateResolution", "UnboundResolution":
				requireTransportFinding(t, result, uncertaintyPredicate, "UNRESOLVED_EFFECT_REAPPLIED", Violation, RelationPath(history), []any{"wf", n("5"), "result"})
				if name == "UnboundResolution" {
					requireTransportFinding(t, result, uncertaintyPredicate, "required_proof_missing:effect_uncertainty_resolution_transactions", MissingProof,
						RelationPath(WriteCapture), []any{n("35")})
				}
			case "ResolutionBeforeReplay":
				if len(result.Findings) != 0 {
					t.Fatalf("earlier captured resolution was ignored: %+v", result.Findings)
				}
			case "ShutdownFailure":
				requireTransportFinding(t, result, shutdownPredicate, "SHUTDOWN_RECORDED_AS_FAILURE", Violation, RelationPath(history), []any{"wf", n("5"), "result"})
			case "ShutdownNoObservation":
				requireTransportFinding(t, result, shutdownPredicate, "required_proof_missing:shutdown_signal_or_exit", MissingProof, SessionPath("session"), []any{int64(3)})
			}
		})
	}
}
