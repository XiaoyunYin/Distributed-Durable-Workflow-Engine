package k6v1

import (
	"slices"
)

const (
	ControlPath    = "fault/control.json"
	ControlVersion = "dur053.k6-control-observation.v1"
)

type ControlObservation struct {
	Schema  string          `json:"schema"`
	Binding SourceBinding   `json:"binding"`
	Before  ControlSnapshot `json:"before"`
	After   ControlSnapshot `json:"after"`
}

type ControlSnapshot struct {
	BeforeUTC string       `json:"before_utc"`
	AfterUTC  string       `json:"after_utc"`
	CRDList   string       `json:"crd_resource_version"`
	PodList   string       `json:"pod_resource_version"`
	ChaosCRDs []string     `json:"chaos_crds"`
	Pods      []ControlPod `json:"pods"`
}

type ControlPod struct {
	Role      string            `json:"role"`
	Runtime   RuntimeIdentity   `json:"runtime"`
	EnvNames  []string          `json:"env_names"`
	EnvFrom   bool              `json:"env_from"`
	LegacyEnv map[string]string `json:"legacy_env,omitempty"`
}

// These switches may be present only with a witnessed literal legacy value.
var controlLegacyEnv = map[string]string{
	"J4_PROVIDER_RESILIENCE":       "0",
	"J4_RECORDED_LLM_ACTIVITY":     "0",
	"RUNTIME_PUBLIC_API_AUTH":      "0",
	"RUNTIME_TIMEOUT_RETRY_POLICY": "0",
}

// Reviewed PodSpec env names from the medium-16 inventories in runs
// 45473340210 and 45481123251, with the activity delay removed from G controls.
// Kubernetes-generated process env is not in these PodSpec lists.
var controlConfigEnv = map[string]bool{
	"CONTAINER_ID":                     true, // internal/state/k4_audit.go binds the commit-audit identity.
	"CONTROL_API_URL":                  true, // python/workers/__main__.py addresses the worker-control API.
	"DATABASE_URL":                     true, // cmd/runtime/lifecycle.go connects to the engine database.
	"DUR048_ALLOW_FIXTURE_ACTIVITY":    true, // internal/state/registry.go enables the sleep fixture definition.
	"DUR049_RECORD_LEASE_ACQUISITIONS": true, // internal/state/store.go records lease acquisitions.
	"DUR053_ALLOW_FIXTURE_ACTIVITY":    true, // internal/state/registry.go and python/workers/registry.py enable the CPU fixture.
	"DUR053_K4_CAMPAIGN":               true, // internal/state/k4_audit.go enables the engine audit used by K6 write capture.
	"DUR053_K6_ACTIVITY":               true, // internal/state/registry.go and python/workers/registry.py enable cooperating activity.
	"DUR053_K6_CAPTURE":                true, // internal/state/k6_capture.go enables database write capture.
	"DUR053_K6_CONTROL_CA_FILE":        true, // python/workers/kafka_worker.py loads the worker-control TLS trust root.
	"DUR053_K6_G_ALLOCATOR_FILE":       true, // cmd/runtime/k6.go opens the allocator evidence journal.
	"DUR053_K6_G_BARRIER":              true, // cmd/runtime/k6.go and python/workers/k6_evidence.py bind the evidence barrier.
	"DUR053_K6_G_EPISODE":              true, // cmd/runtime/k6.go and python/workers/k6_evidence.py bind the evidence episode.
	"DUR053_K6_G_RESET":                true, // cmd/runtime/k6.go and python/workers/k6_evidence.py bind the evidence reset.
	"DUR053_K6_G_WORKER_DIR":           true, // python/workers/k6_evidence.py opens the worker ledger directory.
	"DUR053_K6_OBSERVE":                true, // cmd/runtime/k6.go enables allocator observations.
	"DUR053_K6_PLAN_FILE":              true, // cmd/runtime/k6.go loads the sealed effect operation plan.
	"DUR053_K6_PLAN_SHA256":            true, // cmd/runtime/k6.go verifies the sealed effect operation plan.
	"DUR053_K6_RECEIPT_PUBLIC_KEY":     true, // cmd/runtime/k6.go configures receipt verification.
	"DUR053_K6_RELAY_CA_FILE":          true, // python/workers/k6_effect.py and campaign/k6 worker.py load relay TLS trust.
	"DUR053_K6_RELAY_URL":              true, // python/workers/k6_effect.py and campaign/k6 worker.py address the effect relay.
	"DUR053_K6_SINK_TOKEN_FILE":        true, // python/workers/k6_effect.py and campaign/k6 worker.py read the sink credential.
	"DUR053_K6_WORKER_CONTROL_FILE":    true, // cmd/runtime/k6.go loads the allocator and worker-control TLS configuration.
	"KAFKA_BOOTSTRAP_SERVERS":          true, // cmd/runtime/lifecycle.go and python/workers/__main__.py configure broker endpoints.
	"POD_NAME":                         true, // python/workers/lifecycle.py derives the worker identity.
	"POD_UID":                          true, // internal/state/k4_audit.go and python/workers/k6_evidence.py bind records to the pod.
	"PYTHONDONTWRITEBYTECODE":          true, // The Python interpreter avoids writing bytecode on the read-only image.
	"RUNTIME_ADDR":                     true, // cmd/runtime/lifecycle.go configures the HTTP listener.
	"RUNTIME_DB_MAX_CONNS":             true, // internal/state/store.go sets the engine pool ceiling.
	"RUNTIME_NAMESPACE":                true, // cmd/runtime/lifecycle.go and python/workers/k6_evidence.py bind the workload namespace.
	"RUNTIME_ROLE":                     true, // cmd/runtime/lifecycle.go names the role.
	"RUNTIME_ROLES":                    true, // cmd/runtime/lifecycle.go selects the deployed role set.
	"RUNTIME_TOKEN_AUDIENCE":           true, // internal/api/identity.go binds TokenReview to the expected audience.
	"RUNTIME_TOKEN_AUTH":               true, // internal/api/identity.go configures token authentication.
	"RUNTIME_TOKEN_ISSUER":             true, // internal/api/identity.go validates the token issuer.
	"RUNTIME_TOKEN_NAMESPACE":          true, // internal/api/identity.go binds the authenticated service account namespace.
	"RUNTIME_TOKEN_REVIEW_CA":          true, // internal/api/identity.go loads the TokenReview trust root.
	"RUNTIME_TOKEN_REVIEW_TOKEN_FILE":  true, // internal/api/identity.go reads the TokenReview credential.
	"RUNTIME_TOKEN_REVIEW_URL":         true, // internal/api/identity.go addresses the TokenReview API.
	"RUNTIME_TOKEN_SERVICE_ACCOUNT":    true, // internal/api/identity.go binds the authenticated service account name.
	"WORKER_DRAIN_SECONDS":             true, // python/workers/__main__.py uses the registered drain duration.
	"WORKER_SLOTS":                     true, // python/workers/__main__.py sizes the Kafka worker pool.
	"WORKER_TOKEN_FILE":                true, // python/workers/control.py reads the worker-control credential.
}

func (c ControlObservation) valid(m Manifest) bool {
	return c.Schema == ControlVersion && c.Binding == Binding(m) &&
		c.Before.valid() && c.After.valid()
}

func (s ControlSnapshot) valid() bool {
	if !validTime(s.BeforeUTC) || !validTime(s.AfterUTC) || s.BeforeUTC > s.AfterUTC ||
		!identities(s.CRDList, s.PodList) || s.ChaosCRDs == nil || s.Pods == nil {
		return false
	}
	last := ""
	for _, pod := range s.Pods {
		id := pod.Runtime
		if !identities(pod.Role, id.PodUID, id.ContainerID) || id.PodUID <= last || id.Restart < 0 ||
			!validTime(id.StartedUTC) || id.StartedUTC > s.AfterUTC || pod.EnvNames == nil {
			return false
		}
		last = id.PodUID
	}
	return true
}

func (s ControlSnapshot) faultConfigured() bool {
	return len(s.ChaosCRDs) > 0 || slices.ContainsFunc(s.Pods, func(p ControlPod) bool {
		for name, value := range p.LegacyEnv {
			if legacy, ok := controlLegacyEnv[name]; !ok || legacy != value || !slices.Contains(p.EnvNames, name) {
				return true
			}
		}
		return p.EnvFrom || slices.ContainsFunc(p.EnvNames, func(name string) bool {
			if legacy, ok := controlLegacyEnv[name]; ok {
				return p.LegacyEnv[name] != legacy
			}
			return !controlConfigEnv[name]
		})
	})
}

func (s ControlSnapshot) complete(effect bool, roster Stream[WatchRow]) bool {
	roles := []string{"api", "relay", "scheduler", "worker", "worker-control"}
	if effect {
		roles = append(roles, "k6-effect-relay", "k6-sink")
	}
	for _, role := range roles {
		if !slices.ContainsFunc(s.Pods, func(p ControlPod) bool { return p.Role == role }) {
			return false
		}
	}
	running, registered := map[RuntimeIdentity]bool{}, map[RuntimeIdentity]bool{}
	for _, row := range roster.Rows {
		if row.Position.UTC > s.AfterUTC {
			break
		}
		switch w := row.Data; w.Kind {
		case "runtime":
			running[*w.Runtime] = true
		case "exit":
			delete(running, *w.Runtime)
		case "session":
			registered[w.Session.Runtime()] = true
		case "death":
			delete(running, w.Session.Runtime())
		}
	}
	for _, pod := range s.Pods {
		if pod.Role != "worker" {
			continue
		}
		if !running[pod.Runtime] || !registered[pod.Runtime] {
			return false
		}
		delete(running, pod.Runtime)
	}
	return len(running) == 0
}

func (m Manifest) control() bool { return m.Cell == "medium-16" }

func confirmationProof(cell string) string {
	switch cell {
	case "hook-0":
		return "before_publish"
	case "hook-1":
		return "after_broker_ack"
	case "hook-2":
		return "attempt_claimed"
	case "hook-3":
		return "before_offset_ack"
	case "hook-4":
		return "result_recorded"
	case "hook-5", "medium-2", "medium-3":
		return "DeployedStaleChallenge"
	case "medium-0":
		return "worker_pod_kill"
	case "medium-1":
		return "scheduler_container_kill"
	case "medium-4":
		return "worker_api_partition"
	case "medium-5":
		return "relay_hold"
	case "medium-6":
		return "postgres_restart"
	case "medium-7":
		return "kafka_restart"
	case "medium-8", "medium-9", "medium-10", "medium-11":
		return "SignedProcessSkew"
	case "medium-12":
		return "effect_pre_send_kill"
	case "medium-13":
		return "effect_reply_kill"
	case "medium-14":
		return "effect_duplicate"
	case "medium-15":
		return "effect_lost_reply"
	}
	return "CellConfirmation"
}

func (r *Result) checkConfirmation(m Manifest, plan Plan, arrivals Arrivals, issues []Finding, e External) {
	if !m.control() {
		if plan.FaultMode == "injected" {
			proof := confirmationProof(m.Cell)
			code := "required_proof_missing:" + proof
			r.Confirmation = Confirmation{"missing", code}
			r.record(MissingProof, proof, code, proof, nil)
		}
		return
	}
	for _, issue := range issues {
		status := "failed"
		if issue.Kind == MissingProof {
			status = "missing"
		}
		r.Confirmation = Confirmation{status, issue.Code}
		return
	}

	missing := func(reason string) {
		code := "required_proof_missing:" + reason
		r.Confirmation = Confirmation{"missing", code}
		r.record(MissingProof, "CellConfirmation", code, ControlPath, nil)
	}
	c := e.Control
	if c == nil {
		missing(ControlPath)
		return
	}
	violate := func(code string) {
		raw, _ := Canonical(c)
		ref := Witness{Path: ControlPath, RowKey: []any{}, SHA256: Hash(raw)}
		r.Confirmation = Confirmation{"failed", code}
		r.record(Violation, "CellConfirmation", code, ControlPath, &ref)
	}
	if plan.FaultMode != "control" || arrivals.RemovalUTC != nil {
		violate("CONTROL_PLAN_MISMATCH")
		return
	}
	if c.Before.faultConfigured() || c.After.faultConfigured() {
		missing("control_fault_inventory")
		return
	}
	if c.Before.BeforeUTC < plan.SealedUTC || c.Before.AfterUTC > c.After.BeforeUTC ||
		c.After.BeforeUTC < arrivals.SealedUTC || e.QuietStartUTC == "" || c.After.AfterUTC > e.QuietStartUTC {
		missing("control_window")
		return
	}
	for _, row := range arrivals.Rows {
		for _, attempt := range row.Attempts {
			if attempt.SentUTC < c.Before.AfterUTC {
				missing("control_window")
				return
			}
		}
	}
	for _, ledger := range e.Sessions {
		for _, row := range ledger.Rows {
			if row.Data.Kind == "start" &&
				(row.Position.UTC < c.Before.AfterUTC || row.Position.UTC > c.After.BeforeUTC) {
				missing("control_window")
				return
			}
		}
	}
	if !slices.EqualFunc(c.Before.Pods, c.After.Pods, func(a, b ControlPod) bool {
		return a.Role == b.Role && a.Runtime == b.Runtime && slices.Equal(a.EnvNames, b.EnvNames)
	}) {
		violate("CONTROL_RUNTIME_CHANGED")
		return
	}
	if !c.Before.complete(m.Sink != nil, e.Roster) || !c.After.complete(m.Sink != nil, e.Roster) {
		missing("control_runtime")
		return
	}
	if r.Namespace == nil || r.Namespace.TerminalExecutions <= r.Namespace.UnsuccessfulTerminalExecutions ||
		r.Labels[0].Outcome != "covered/pass" || r.Labels[10].Outcome != "covered/pass" {
		r.Confirmation = Confirmation{"pending", "no_positive_population"}
		return
	}
	r.Confirmation = Confirmation{"confirmed", "cell_predicate_satisfied"}
}
