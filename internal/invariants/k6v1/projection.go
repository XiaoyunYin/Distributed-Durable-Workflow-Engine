package k6v1

import (
	"encoding/base64"
	"encoding/json"
	"strings"
)

// The v1 workload admits only the declared scalar, array and object shapes.
// In particular a JSON string is never interpreted as a second JSON document.
func validStructure(kind Kind, value any) bool {
	switch kind {
	case "identities", "graph_nodes", "next_nodes", "numbers":
		items, ok := value.([]any)
		if !ok {
			return false
		}
		itemKind := map[Kind]Kind{"identities": Text, "graph_nodes": "graph_node", "next_nodes": "next_node", "numbers": Integer}[kind]
		for _, item := range items {
			if !validValue(itemKind, item) {
				return false
			}
		}
		return true
	case "graph_node":
		return validValue(Text, value) || object(value, "id:text kind:text next:next branches:identities join:text", false)
	case "next":
		return value == nil || validValue(Text, value) || validValue("identities", value)
	case "next_nodes_or_null":
		return value == nil || validValue("next_nodes", value)
	case "next_node":
		return object(value, "NodeID:text Iteration:int Dependencies:dependencies Input:workload", true)
	case "dependencies":
		return value == nil || validValue("identities", value)
	case "graph":
		return object(value, "entry:text nodes:graph_nodes", false)
	case "versions", "classes":
		m, ok := value.(map[string]any)
		if !ok {
			return false
		}
		for k, v := range m {
			if k == "" || !publicIdentity(k) || !validValue(Text, v) {
				return false
			}
		}
		return true
	case "workload":
		return validValue(Integer, value) || validValue("numbers", value) ||
			validValue("receipt", value) || validValue("arguments", value) ||
			object(value, "digest:digest work_units:int", true) ||
			object(value, "error_present:bool type:text", true)
	case "arguments":
		return object(value, "position:int value:k6_literal cell:cell_label", false)
	case "k6_literal":
		return value == "k6"
	case "cell_label":
		s, ok := value.(string)
		return ok && (strings.HasPrefix(s, "medium-") || strings.HasPrefix(s, "hook-")) && publicIdentity(s)
	case "claim":
		return object(value, "workflow_id:text node_id:text iteration:int attempt_number:int token_hash:digest worker_id:text worker_request_id:text", true)
	case "proof":
		return object(value, "operation:proof_operation receipt:receipt signature:signature", true) ||
			object(value, "operation:proof_operation receipt:receipt", true)
	case "proof_operation":
		return value == "apply" || value == "lookup"
	case "signature":
		s, ok := value.(string)
		if !ok || (s != "" && len(s) != base64.RawURLEncoding.EncodedLen(64)) {
			return false
		}
		_, err := base64.RawURLEncoding.Strict().DecodeString(s)
		return err == nil
	case "receipt":
		return object(value, "workflow_id:text logical_key:text resource_id:text canonical_hash:digest scope_hash:digest intent_id:uuid expected_revision:int applied_revision:int application_id:uuid", true)
	case "legacy_receipt":
		return object(value, "resource_id:text resource_revision:int logical_effect_key:text", true)
	case "event":
		return validValue("workload", value) || object(value,
			"workflow_id:text node_id:text iteration:int attempt_number:int attempt_state:text reference:text timer_id:uuid purpose:text from_node_id:text next:next_nodes_or_null intent_id:uuid grant_scope_hash:digest request_id:uuid", false)
	case "detail":
		return object(value, "event_id:text event_type:text outcome:text reason_present:bool", false)
	default:
		return false
	}
}

func object(value any, fields string, required bool) bool {
	m, ok := value.(map[string]any)
	if !ok {
		return false
	}
	matched := 0
	spec := strings.Fields(fields)
	for _, field := range spec {
		name, kind, _ := strings.Cut(field, ":")
		item, exists := m[name]
		if !exists {
			if required {
				return false
			}
			continue
		}
		if !validValue(Kind(kind), item) {
			return false
		}
		matched++
	}
	return matched == len(m)
}

// DecodeDatabaseStructure strips known free-text errors after decoding their
// types. The public schema admits only the presence bit for those fields.
func DecodeDatabaseStructure(kind Kind, raw []byte) (any, error) {
	value, err := parse(raw)
	if err != nil || value == nil {
		return nil, errShape
	}
	if m, ok := value.(map[string]any); ok {
		switch kind {
		case "detail":
			if !redactText(m, "reason", "reason_present") {
				return nil, errShape
			}
		case "workload", "event":
			if _, hasError := m["error"]; hasError && (len(m) != 2 || !redactText(m, "error", "error_present")) {
				return nil, errShape
			}
		}
	}
	if !validValue(kind, value) {
		return nil, errShape
	}
	return value, nil
}

func redactText(m map[string]any, name, presence string) bool {
	raw, exists := m[name]
	if !exists {
		return true
	}
	_, text := raw.(string)
	_, alreadyRedacted := m[presence]
	if !text || alreadyRedacted {
		return false
	}
	delete(m, name)
	m[presence] = true
	return true
}

func decodeRow(spec Relation, line []byte) (Row, error) {
	value, err := parse(line)
	obj, ok := value.(map[string]any)
	if err != nil || !ok || ValidateRow(spec, obj) != nil {
		return nil, errShape
	}
	raw, err := encode(obj)
	if err != nil || string(raw) != string(line) {
		return nil, errShape
	}
	return obj, nil
}

func compareKeys(spec Relation, a, b Row) int {
	for _, key := range spec.Keys {
		switch left := a[key].(type) {
		case string:
			if order := strings.Compare(left, b[key].(string)); order != 0 {
				return order
			}
		case json.Number:
			right := number(b[key])
			if number(left) < right {
				return -1
			}
			if number(left) > right {
				return 1
			}
		case bool:
			if left != b[key].(bool) {
				if left {
					return 1
				}
				return -1
			}
		}
	}
	return 0
}
