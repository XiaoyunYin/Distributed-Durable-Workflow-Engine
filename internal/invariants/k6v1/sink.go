package k6v1

type resourceRevision struct {
	resource string
	revision int64
}

func (r *Result) checkSink(rows map[string][]Row, workflows map[string]Row, haveWorkflows bool, witness func(string, Row) Witness, violate rowViolation) {
	resources, haveResources := rows[SinkResources]
	journal, haveJournal := rows[SinkJournal]
	receipts, haveReceipts := rows[SinkReceipts]
	intents, haveIntents := rows["engine.approval_action_intents"]
	label := &r.Labels[7]
	label.PopulationUnit = "resource"
	label.PopulationCount = len(resources)
	for _, path := range []string{SinkResources, SinkJournal, SinkReceipts, "engine.approval_action_intents", executions} {
		if _, ok := rows[path]; !ok {
			label.Reason = "required_proof_missing:" + RelationPath(path)
			break
		}
	}
	flag := func(predicate, code, relation string, row Row) {
		label.Outcome, label.Reason = "covered/fail", "contract_violation"
		violate(predicate, code, relation, row)
	}
	byIntent := map[any]Row{}
	targets := map[any]bool{}
	for _, intent := range intents {
		if !haveWorkflows || workflows[intent["workflow_id"].(string)] != nil {
			byIntent[intent["intent_id"]] = intent
			targets[intent["target"]] = true
		}
	}
	joinedIntent := func(row Row) bool {
		intent := byIntent[row["intent_id"]]
		return intent != nil && row["resource_id"] == intent["target"] &&
			(row["workflow_id"] == nil || row["workflow_id"] == intent["workflow_id"])
	}

	byResource := map[string][]Row{}
	writes := map[resourceRevision]Row{}
	claims := map[resourceRevision]Row{}
	for _, row := range journal {
		resource := row["resource_id"].(string)
		byResource[resource] = append(byResource[resource], row)
		writes[resourceRevision{resource, number(row["revision"])}] = row
		if haveIntents && !joinedIntent(row) {
			flag("RevisionChain", "SINK_ORPHAN_JOURNAL", SinkJournal, row)
		}
	}
	for _, row := range receipts {
		at := resourceRevision{row["resource_id"].(string), number(row["applied_revision"])}
		if haveIntents && !joinedIntent(row) {
			flag("OneMutationPerKey", "SINK_ORPHAN_RECEIPT", SinkReceipts, row)
		}
		if claims[at] != nil {
			flag("OneMutationPerKey", "SINK_DUPLICATE_REVISION_RECEIPT", SinkReceipts, row)
		}
		claims[at] = row
		write := writes[at]
		if haveJournal && (write == nil || row["expected_revision"] != write["old_revision"] ||
			row["intent_id"] != write["intent_id"] || row["logical_key"] != write["logical_key"]) {
			flag("RevisionChain", "SINK_RECEIPT_JOURNAL_BINDING", SinkReceipts, row)
		}
	}
	seen := map[string]bool{}
	emptyState := Hash([]byte("{}"))
	for _, row := range resources {
		label.Witnesses = append(label.Witnesses, witness(SinkResources, row))
		resource := row["resource_id"].(string)
		seen[resource] = true
		if haveIntents && (!targets[resource] || (row["intent_id"] != nil && !joinedIntent(row))) {
			flag("RevisionChain", "SINK_ORPHAN_RESOURCE", SinkResources, row)
		}
		if row["initial_state_hash"] != emptyState || number(row["revision"]) < 0 || number(row["latest_fence"]) < 0 {
			flag("RevisionChain", "SINK_RESOURCE_BASELINE", SinkResources, row)
		}
		if !haveJournal {
			continue
		}
		var revision int64
		var state, key, intent any = emptyState, nil, nil
		for _, write := range byResource[resource] {
			if number(write["old_revision"]) != revision || number(write["revision"]) != revision+1 || write["old_state_hash"] != state {
				flag("RevisionChain", "SINK_REVISION_GAP", SinkJournal, write)
			}
			revision, state, key, intent = number(write["revision"]), write["state_hash"], write["logical_key"], write["intent_id"]
		}
		if number(row["revision"]) != revision || row["final_state_hash"] != state || row["logical_key"] != key || row["intent_id"] != intent {
			flag("RevisionChain", "SINK_REVISION_ENDPOINT", SinkResources, row)
		}
	}
	for _, row := range journal {
		resource := row["resource_id"].(string)
		if haveResources && !seen[resource] {
			flag("RevisionChain", "SINK_JOURNAL_RESOURCE_MISSING", SinkJournal, row)
		}
		if haveReceipts && claims[resourceRevision{resource, number(row["revision"])}] == nil {
			flag("RevisionChain", "SINK_JOURNAL_RECEIPT_MISSING", SinkJournal, row)
		}
	}
	for _, spec := range []string{SinkCalls, SinkAllocations} {
		for _, row := range rows[spec] {
			if haveIntents && !joinedIntent(row) {
				flag("DeliveryClosure", "SINK_ORPHAN_AUTHORITY", spec, row)
			}
		}
	}
}
