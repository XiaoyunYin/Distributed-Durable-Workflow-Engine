package k6v1

import (
	"slices"
	"strings"
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
	Role     string          `json:"role"`
	Runtime  RuntimeIdentity `json:"runtime"`
	EnvNames []string        `json:"env_names"`
	EnvFrom  bool            `json:"env_from"`
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
		return p.EnvFrom || slices.ContainsFunc(p.EnvNames, func(name string) bool {
			return strings.HasPrefix(name, "DUR053_K6_HOOK_") ||
				name == "DUR053_K6_DEPLOYED_HOOKS" || name == "DUR053_K6_RELAY_ARM"
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
