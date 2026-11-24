package k6v1

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

const DeadlineWrites = "k6_f3.deadline_writes"

type Kind string

const (
	Text         Kind = "text"
	Integer      Kind = "int"
	Boolean      Kind = "bool"
	Timestamp    Kind = "time"
	UUID         Kind = "uuid"
	Digest       Kind = "digest"
	Token        Kind = "token"
	Presence     Kind = "present"
	WireHash     Kind = "wire_hash"
	WireLength   Kind = "wire_length"
	AttemptScope Kind = "attempt_scope"
)

type Column struct {
	Source   string
	Name     string
	Kind     Kind
	Nullable bool
}

type Relation struct {
	Name            string
	Keys            []string
	Columns         []Column
	ExcludedColumns []string
}

type Row map[string]any

// PostgreSQL column projections, independent of production model types. A
// source:kind:name entry renames a field whose public representation differs.
func relation(name, keys, columns string) Relation {
	spec := Relation{Name: name, Keys: strings.Fields(keys)}
	for _, field := range strings.Fields(columns) {
		parts := strings.Split(field, ":")
		kind, nullable := strings.CutSuffix(parts[1], "?")
		column := Column{Source: parts[0], Name: parts[0], Kind: Kind(kind), Nullable: nullable}
		if len(parts) == 3 {
			column.Name = parts[2]
		}
		spec.Columns = append(spec.Columns, column)
	}
	return spec
}

func EngineRelations(instrumented bool, capture ...bool) []Relation {
	relations := []Relation{
		relation("engine.schema_migrations", "version", "version:int applied_at:time"),
		relation("engine.workflow_definitions", "definition_id version",
			"definition_id:text version:int definition_hash:text graph:graph activity_versions:versions effect_classes:classes created_at:time"),
		relation("engine.workflow_executions", "workflow_id",
			"workflow_id:text namespace:text submission_key:text submission_payload_hash:text definition_id:text definition_version:int partition_id:int state:text revision:int created_at:time updated_at:time traceparent:traceparent"),
		relation("engine.node_instances", "workflow_id node_id iteration",
			"workflow_id:text node_id:text iteration:int state:text dependencies:identities input:workload accepted_result:workload? current_attempt_number:int? retry_count:int deadline_at:time? revision:int created_at:time updated_at:time timer_fired:bool"),
		relation("engine.retry_policies", "workflow_id node_id iteration",
			"workflow_id:text node_id:text iteration:int max_retries:int retries_used:int total_deadline_at:time? checkpoint_schema_version:int updated_at:time"),
		relation("engine.dur050_admission_slots", "workflow_id", "workflow_id:text namespace:text admitted_at:time"),
		relation("engine.activity_attempts", "workflow_id node_id iteration attempt_number",
			"workflow_id:text node_id:text iteration:int attempt_number:int state:text effect_class:text claim_token:token?:claim_token_hash worker_id:text? worker_request_id:text? heartbeat_deadline:time? logical_effect_key:text? grant_scope_hash:attempt_scope? outcome_disposition:text result:workload? is_current:bool created_at:time updated_at:time k6_fence:int? k6_proof:proof?"),
		relation("engine.attempt_result_evidence", "evidence_id",
			"evidence_id:uuid workflow_id:text node_id:text iteration:int attempt_number:int claim_token:token?:claim_token_hash reconciliation_reference:text payload:workload disposition:text recorded_at:time"),
		relation("engine.transition_history", "workflow_id revision transition_id",
			"workflow_id:text revision:int transition_id:uuid actor_kind:text actor_id:text scheduler_epoch:int? node_id:text? iteration:int? attempt_number:int? old_state:text? new_state:text reason:text created_at:time"),
		relation("engine.partition_leases", "partition_id", "partition_id:int owner_id:uuid? epoch:int lease_expires_at:time? updated_at:time"),
		relation("engine.lease_acquisitions", "acquisition_id", "acquisition_id:uuid partition_id:int owner_id:uuid epoch:int lease_expires_at:time acquired_at:time"),
		relation("engine.k4_audit", "event_order", "event_order:int finalized_at:time? "+auditColumns),
		relation("engine.k4_audit_queue", "id", "id:int "+auditColumns),
		relation("engine.cancellation_requests", "request_id", "request_id:uuid workflow_id:text client_key:text observed_revision:int status:text created_at:time applied_at:time?"),
		relation("engine.timers", "timer_id", "timer_id:uuid workflow_id:text node_id:text iteration:int due_at:time purpose:text consumed_at:time? owning_revision:int"),
		relation("engine.checkpoints", "workflow_id node_id iteration sequence",
			"workflow_id:text node_id:text iteration:int sequence:int schema_version:int source_attempt_number:int payload:workload payload_hash:text created_at:time"),
		relation("engine.approval_action_intents", "intent_id",
			"intent_id:uuid workflow_id:text node_id:text iteration:int proposal_hash:text target:text canonical_arguments:arguments expected_resource_revision:text? decision:text approver_id:text? valid_until:time dispatch_status:text created_at:time decided_at:time? proposal_signature:present:proposal_signature_present grant_scope_hash:digest? grant_token:token?:grant_token_hash grant_expires_at:time? decision_reason:present:decision_reason_present k6_logical_key:text?"),
		relation("k6_auth.episode", "singleton", "singleton:bool namespace:text approver_subject:text valid_until:time uncertainty_us:int"),
		relation("engine.outbox", "event_id",
			"event_id:uuid workflow_id:text aggregate_revision:int event_type:text payload:event publish_state:text relay_attempts:int claim_owner:uuid? claim_expires_at:time? created_at:time published_at:time? topic:text schema_version:int next_attempt_at:time last_error:present:last_error_present traceparent:traceparent"),
		relation("engine.outbox_publications", "publication_id", "publication_id:uuid event_id:uuid relay_owner:uuid relay_attempt:int broker_acknowledged:bool published_at:time error:present:error_present"),
		relation("engine.event_inbox", "consumer_id event_id", "consumer_id:text event_id:uuid disposition:text reconciliation_reference:text? recorded_at:time topic:text? kafka_partition:int? kafka_offset:int?"),
		relation("engine.consumer_offsets", "consumer_id topic kafka_partition", "consumer_id:text topic:text kafka_partition:int next_offset:int updated_at:time"),
		relation("engine.scheduler_wakeups", "wakeup_id",
			"wakeup_id:uuid workflow_id:text partition_id:int event_id:uuid reason:text state:text claim_owner:uuid? claim_epoch:int? claim_expires_at:time? created_at:time consumed_at:time?"),
		relation("engine.reconciliation_items", "item_id",
			"item_id:uuid workflow_id:text? partition_id:int? node_id:text? iteration:int? attempt_number:int? kind:text reference:text status:text detail:detail created_at:time updated_at:time resolved_at:time?"),
		relation("engine.transport_quarantine", "quarantine_id",
			"quarantine_id:uuid consumer_id:text event_id:text? workflow_id:text? partition_id:int? topic:text kafka_partition:int kafka_offset:int event_type:text? payload:wire_hash:payload_sha256 payload:wire_length:payload_length reason:present:reason_present recorded_at:time"),
	}
	for _, schema := range []string{"engine", "effects"} {
		records := "workflow_id:text logical_effect_key:text argument_hash:text attempt_number:int grant_scope_hash:digest? outcome:text receipt:legacy_receipt? created_at:time updated_at:time"
		if schema == "effects" {
			records += " intent_id:uuid? resource_id:text?"
		}
		relations = append(relations,
			relation(schema+".effect_records", "workflow_id logical_effect_key", records),
			relation(schema+".effect_call_attempts", "call_id", "call_id:uuid workflow_id:text logical_effect_key:text argument_hash:text attempt_number:int request_id:text outcome:text receipt:legacy_receipt? created_at:time"),
			relation(schema+".effect_resource_fences", "resource_id", "resource_id:text next_token:int observed_token:int updated_at:time"),
			relation(schema+".sandbox_effect_state", "resource_id", "resource_id:text state:arguments resource_revision:int updated_at:time"),
			relation(schema+".effect_resolution_audit", "resolution_id", "resolution_id:uuid workflow_id:text logical_effect_key:text argument_hash:text actor_id:text disposition:text receipt:legacy_receipt? created_at:time"),
		)
	}
	if instrumented {
		relations = append(relations, relation(DeadlineWrites, "id",
			"id:int written_at:time application:text session:text? branch:text partition_id:int? owner_id:text? epoch:int? workflow_id:text? node_id:text? iteration:int? attempt_number:int? worker_id:text? token_hash:digest? row_id:text deadline:time? old_deadline:time?"))
	}
	if len(capture) != 0 && capture[0] {
		relations = append(relations, CaptureRelations()...)
	}
	slices.SortFunc(relations, func(a, b Relation) int { return strings.Compare(a.Name, b.Name) })
	return relations
}

const auditColumns = "at:time kind:text partition_id:int owner_id:text epoch:int session_id:text pod_uid:text container_id:container_id transaction_id:text transition_id:uuid? claim_identity:claim?"

const (
	SinkResources    = "k6_sink.resource_states"
	SinkJournal      = "k6_sink.journal"
	SinkReceipts     = "k6_sink.receipts"
	SinkCalls        = "k6_sink.calls"
	SinkAllocations  = "k6_sink.allocations"
	SinkBindings     = "k6_sink.request_bindings"
	SinkLookupMisses = "k6_sink.lookup_misses"
)

func SinkRelations() []Relation {
	return []Relation{
		relation(SinkAllocations, "intent_id grant_hash attempt",
			"intent_id:uuid grant_hash:digest attempt:int resource_id:text fence:int"),
		relation(SinkCalls, "call_id",
			"call_id:int operation:text workflow_id:text logical_key:text intent_id:uuid resource_id:text message_id:text attempt:int request_id:text body_hash:digest connection_hash:digest outcome:text application_id:uuid? response_hash:digest? observed_at:time transaction_id:text? committed_at:time? grant_read_at:time? grant_hash:digest? node_id:text? iteration:int? scope_hash:digest? expected_revision:text? arguments:arguments? fence:int?"),
		relation(SinkJournal, "resource_id revision",
			"resource_id:text old_revision:int revision:int old_state_hash:digest state_hash:digest logical_key:text intent_id:uuid transaction_id:text?"),
		relation(SinkLookupMisses, "intent_id grant_hash attempt", "intent_id:uuid grant_hash:digest attempt:int"),
		relation(SinkReceipts, "workflow_id logical_key",
			"workflow_id:text logical_key:text resource_id:text canonical_hash:digest scope_hash:digest intent_id:uuid expected_revision:int applied_revision:int application_id:uuid"),
		relation(SinkBindings, "request_id", "request_id:text body_hash:digest"),
		relation(SinkResources, "resource_id",
			"resource_id:text revision:int latest_fence:int initial_state_hash:digest final_state_hash:digest logical_key:text? intent_id:uuid?"),
	}
}

var errShape = errors.New("K6_PUBLIC_ROW_SCHEMA")

func ValidateRow(spec Relation, row Row) error {
	columns := spec.Columns
	if spec.Name == SinkCalls || spec.Name == SinkJournal {
		// Earlier bundles ended before the transaction projection. Preserve
		// their row digests and let the authority predicate report missing proof.
		if first := slices.IndexFunc(columns, func(c Column) bool { return c.Name == "transaction_id" }); first == len(row) {
			columns = columns[:first]
		}
	}
	if len(row) != len(columns) {
		return errShape
	}
	for _, col := range columns {
		value, exists := row[col.Name]
		if !exists || (value == nil && !col.Nullable) || (value != nil && !validValue(col.Kind, value)) {
			return fmt.Errorf("%w: %s", errShape, col.Name)
		}
	}
	for _, key := range spec.Keys {
		if row[key] == nil || row[key] == "" {
			return errShape
		}
	}
	if spec.Name == WriteCapture || spec.Name == CaptureQueue {
		return validateCaptureShape(row)
	}
	return nil
}

func validValue(kind Kind, value any) bool {
	s, isString := value.(string)
	switch kind {
	case Text:
		return isString && publicIdentity(s)
	case "traceparent":
		return isString && traceparent.MatchString(s)
	case "container_id":
		scheme, identifier, url := strings.Cut(s, "://")
		return isString && (publicIdentity(s) || (url && slices.Contains([]string{"containerd", "docker", "cri-o"}, scheme) && digest.MatchString(identifier)))
	case UUID:
		return isString && uuid.MatchString(s)
	case Digest, Token, WireHash, AttemptScope:
		return isString && digest.MatchString(s)
	case Timestamp:
		return isString && validTime(s)
	case Boolean, Presence:
		_, ok := value.(bool)
		return ok
	case Integer, WireLength:
		n, ok := value.(json.Number)
		return ok && integer.MatchString(n.String())
	default:
		return validStructure(kind, value)
	}
}

func number(value any) int64 {
	n, _ := strconv.ParseInt(string(value.(json.Number)), 10, 64)
	return n
}
