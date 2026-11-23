package k6v1

import (
	"fmt"
	"strings"
	"time"
)

const (
	WriteCapture = "engine.k6_write_capture"
	CaptureQueue = "engine.k6_capture_queue"
	OwnerWrites  = "engine.k6_owner_write_transactions"

	leases           = "engine.partition_leases"
	acquisitions     = "engine.lease_acquisitions"
	timers           = "engine.timers"
	approvalIntents  = "engine.approval_action_intents"
	resultRequest    = "result_request"
	physicalDelivery = "physical_delivery"
	publicationStart = "publication_start"
)

func CaptureRelations() []Relation {
	const images = "transaction_id:text subject:text operation:text row_key:capture_key workflow_id:text? partition_id:int? before_image:capture_image? after_image:capture_image?"
	return []Relation{
		relation(CaptureQueue, "id", "id:int "+images+" owner_partition:int? owner_id:text? epoch:int?"),
		relation(OwnerWrites, "event_order", "event_order:int transaction_id:text subject:text operation:text partition_id:int owner_id:text epoch:int"),
		relation(WriteCapture, "event_order", "event_order:int "+images),
	}
}

func IsCaptureRelation(name string) bool {
	return name == CaptureQueue || name == WriteCapture || name == OwnerWrites
}

type captureProjection struct {
	keys   []Column
	fields []Column
}

// This inventory is checker-owned. Ordinary row keys and field types use the
// public snapshot schema, including the redacted claim and grant columns.
var captureProjections = func() map[string]captureProjection {
	fields := map[string]string{
		executions:                        "state revision",
		history:                           "revision scheduler_epoch reason",
		nodeRows:                          "state revision current_attempt_number retry_count timer_fired updated_at",
		activityAttempts:                  "state worker_id worker_request_id is_current effect_class outcome_disposition k6_fence claim_token_hash heartbeat_deadline updated_at",
		"engine.attempt_result_evidence":  "node_id iteration attempt_number disposition",
		leases:                            "owner_id epoch lease_expires_at updated_at",
		acquisitions:                      "owner_id epoch",
		"engine.retry_policies":           "retries_used max_retries",
		"engine.dur050_admission_slots":   "",
		timers:                            "node_id iteration purpose owning_revision consumed_at",
		"engine.cancellation_requests":    "status",
		approvalIntents:                   "decision dispatch_status grant_scope_hash node_id iteration grant_expires_at grant_token_hash",
		outbox:                            "aggregate_revision topic event_type publish_state relay_attempts claim_owner",
		publications:                      "event_id relay_owner relay_attempt broker_acknowledged",
		inbox:                             "topic kafka_partition kafka_offset disposition",
		offsets:                           "next_offset",
		wakeups:                           "event_id state claim_owner claim_epoch",
		quarantine:                        "consumer_id event_id topic kafka_partition kafka_offset",
		reconciliation:                    "kind reference status node_id iteration attempt_number",
		"engine.effect_records":           "outcome attempt_number",
		"effects.effect_records":          "outcome attempt_number",
		"engine.effect_resolution_audit":  "logical_effect_key disposition",
		"effects.effect_resolution_audit": "logical_effect_key disposition",
	}
	projections := make(map[string]captureProjection, len(fields)+3)
	for _, spec := range EngineRelations(false) {
		names, tracked := fields[spec.Name]
		if !tracked {
			continue
		}
		columns := map[string]Column{}
		for _, col := range spec.Columns {
			columns[col.Name] = col
		}
		p := captureProjection{}
		keys := spec.Keys
		if spec.Name == history {
			keys = []string{"transition_id"}
		}
		for _, name := range keys {
			p.keys = append(p.keys, columns[name])
		}
		for _, name := range strings.Fields(names) {
			p.fields = append(p.fields, columns[name])
		}
		switch spec.Name {
		case nodeRows, activityAttempts:
			p.fields = append(p.fields, Column{Name: "image_hash", Kind: Digest})
		case outbox:
			p.fields = append(p.fields, Column{Name: "key", Kind: UUID}, Column{Name: "payload_hash", Kind: Digest})
		}
		if spec.Name == activityAttempts {
			p.fields = append(p.fields, Column{Name: "receipt_id", Kind: UUID, Nullable: true})
		}
		projections[spec.Name] = p
	}
	attempt := projections[activityAttempts].keys
	position := append(append([]Column{}, projections[offsets].keys...), Column{Kind: Integer})
	projections[resultRequest] = captureProjection{attempt, relation("", "",
		"worker_request_id:text? token_hash:digest receipt_id:uuid? attempt_state:text").Columns}
	projections[physicalDelivery] = captureProjection{position, relation("", "", "event_id:text?").Columns}
	projections[publicationStart] = captureProjection{[]Column{{Kind: UUID}, {Kind: Integer}},
		relation("", "", "topic:text relay_owner:uuid key:uuid payload_hash:digest").Columns}
	return projections
}()

func validateCaptureShape(row Row) error {
	p, ok := captureProjections[row["subject"].(string)]
	key := row["row_key"].([]any)
	if !ok || len(key) != len(p.keys) {
		return fmt.Errorf("%w: capture key", errShape)
	}
	for i, col := range p.keys {
		if !validValue(col.Kind, key[i]) || key[i] == "" {
			return fmt.Errorf("%w: capture key", errShape)
		}
	}
	for _, name := range []string{"before_image", "after_image"} {
		if row[name] == nil {
			continue
		}
		image := row[name].(map[string]any)
		fields := p.fields
		if row["subject"] == publicationStart && len(image) == 2 {
			fields = fields[:2] // G3c1 starts lacked the wire identity.
		}
		if len(image) != len(fields) {
			return fmt.Errorf("%w: %s", errShape, name)
		}
		for _, col := range fields {
			value, exists := image[col.Name]
			if !exists || value == nil && !col.Nullable || value != nil && !validCaptureField(col.Kind, value) {
				return fmt.Errorf("%w: capture field %s", errShape, col.Name)
			}
		}
	}
	return nil
}

func validCaptureField(kind Kind, value any) bool {
	if kind == Timestamp {
		s, ok := value.(string)
		_, err := time.Parse(time.RFC3339Nano, s)
		return ok && err == nil
	}
	return validValue(kind, value)
}
