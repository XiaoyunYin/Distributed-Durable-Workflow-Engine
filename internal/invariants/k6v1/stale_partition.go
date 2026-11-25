package k6v1

import (
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"time"
)

type StalePartition struct {
	Pod          string          `json:"pod"`
	PodUID       string          `json:"pod_uid"`
	ContainerID  string          `json:"container_id"`
	HostPID      int64           `json:"host_pid"`
	Address      string          `json:"address"`
	Lock         *StaleLockProbe `json:"lock"`
	IssuedNS     int64           `json:"issued_ns"`
	ConfirmedNS  int64           `json:"confirmed_ns"`
	AbsentNS     int64           `json:"absent_ns"`
	KernelGoneNS int64           `json:"kernel_gone_ns"`
	Controller   json.RawMessage `json:"controller"`
	Daemon       json.RawMessage `json:"daemon"`
	RulesBefore  string          `json:"rules_before"`
	RulesDuring  string          `json:"rules_during"`
	RulesHeld    string          `json:"rules_held"`
	RulesAfter   string          `json:"rules_after"`
	Sets         string          `json:"sets"`
	Failed       *StaleRoute     `json:"failed"`
	Restored     *StaleRoute     `json:"restored"`
	Execution    *StaleExecution `json:"execution"`
}

type StaleLockProbe struct {
	StartNS  int64  `json:"start_ns"`
	EndNS    int64  `json:"end_ns"`
	Epoch    int64  `json:"epoch"`
	SQLState string `json:"sqlstate"`
	Rollback string `json:"rollback"`
}

type StaleRoute struct {
	StartNS int64  `json:"start_ns"`
	EndNS   int64  `json:"end_ns"`
	Address string `json:"address"`
	Port    int64  `json:"port"`
	HostPID int64  `json:"host_pid"`
	Exit    *int   `json:"exit"`
}

type StaleExecution struct {
	StaleBackend
	DBTime      string  `json:"db_time"`
	QueryStart  string  `json:"query_start"`
	Query       string  `json:"query"`
	WaitType    string  `json:"wait_type"`
	BlockedBy   []int64 `json:"blocked_by"`
	ObserverPID int64   `json:"observer_pid"`
	StartNS     int64   `json:"start_ns"`
	EndNS       int64   `json:"end_ns"`
	RolledBack  string  `json:"rolled_back"`
}

type staleChaosMeta struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	UID       string `json:"uid"`
}

func (p StalePartition) controllerSource(namespace string) string {
	var cr struct {
		APIVersion, Kind string
		Metadata         staleChaosMeta
		Spec             struct {
			Action, Direction, Mode string
			ExternalTargets         []string
			Selector                struct{ Pods map[string][]string }
		}
		Status struct {
			Conditions []struct{ Type, Status string }
			Experiment struct {
				ContainerRecords []struct{ ID, SelectorKey, Phase string }
			}
		}
	}
	if json.Unmarshal(p.Controller, &cr) != nil {
		return ""
	}
	source := namespace + "/" + p.Pod
	if cr.APIVersion != "chaos-mesh.org/v1alpha1" || cr.Kind != "NetworkChaos" ||
		!staleIPv4(p.Address) || !identities(cr.Metadata.Name, cr.Metadata.UID, p.Pod) ||
		cr.Metadata.Namespace != namespace || cr.Spec.Action != "partition" || cr.Spec.Direction != "to" || cr.Spec.Mode != "one" ||
		len(cr.Spec.Selector.Pods) != 1 || !slices.Equal(cr.Spec.Selector.Pods[namespace], []string{p.Pod}) ||
		!slices.Equal(cr.Spec.ExternalTargets, []string{p.Address}) ||
		!slices.ContainsFunc(cr.Status.Conditions, func(c struct{ Type, Status string }) bool {
			return c.Type == "AllInjected" && c.Status == "True"
		}) {
		return ""
	}
	rows := cr.Status.Experiment.ContainerRecords
	if len(rows) != 1 || rows[0].ID != source || rows[0].SelectorKey != "." ||
		(rows[0].Phase != "Injected" && rows[0].Phase != "Wait") {
		return ""
	}
	return namespace + "/" + cr.Metadata.Name
}

func staleIPv4(address string) bool {
	parts := strings.Split(address, ".")
	if len(parts) != 4 {
		return false
	}
	for _, octet := range parts {
		value, err := strconv.ParseUint(octet, 10, 8)
		if err != nil || strconv.FormatUint(value, 10) != octet {
			return false
		}
	}
	return true
}

func (p StalePartition) dropRule(namespace, source string) (chain, set string) {
	var daemon struct {
		Metadata staleChaosMeta
		Spec     struct {
			IPTables []struct {
				Name, Source, Direction string
				IPSets                  []string
			}
		}
	}
	if source == "" || json.Unmarshal(p.Daemon, &daemon) != nil ||
		daemon.Metadata.Namespace != namespace || daemon.Metadata.Name != p.Pod {
		return "", ""
	}
	for _, rule := range daemon.Spec.IPTables {
		if rule.Source == source && rule.Direction == "output" && len(rule.IPSets) == 1 &&
			identities(rule.Name, rule.IPSets[0]) {
			if chain != "" {
				return "", ""
			}
			chain, set = rule.Name, rule.IPSets[0]
		}
	}
	return chain, set
}

func staleSetMembers(raw, set string) []string {
	var members []string
	for _, line := range strings.Split(raw, "\n") {
		parts := strings.Fields(line)
		if len(parts) >= 3 && parts[0] == "add" && parts[1] == set {
			members = append(members, parts[2])
		}
	}
	return members
}

func (p StalePartition) destination(set string) bool {
	members := staleSetMembers(p.Sets, set)
	if len(members) != 2 {
		return false
	}
	first, second := staleSetMembers(p.Sets, members[0]), staleSetMembers(p.Sets, members[1])
	if len(first) == 0 {
		first, second = second, first
	}
	return len(second) == 0 && len(first) == 1 && (first[0] == p.Address || first[0] == p.Address+"/32")
}

func staleAttached(rules, chain string) bool {
	target := " -A CHAOS-OUTPUT -j " + chain
	for _, line := range strings.Split(rules, "\n") {
		if strings.HasSuffix(line, target) {
			return true
		}
	}
	return false
}

func staleDropCounters(rules, chain, set string) (packets, bytes int64) {
	if !staleAttached(rules, chain) {
		return -1, -1
	}
	for _, line := range strings.Split(rules, "\n") {
		parts := strings.Fields(line)
		if len(parts) < 8 || !slices.Equal(parts[1:3], []string{"-A", chain}) ||
			!slices.Equal(parts[len(parts)-2:], []string{"-j", "DROP"}) {
			continue
		}
		match := slices.Index(parts, "--match-set")
		if match < 0 || match+2 >= len(parts) || !slices.Equal(parts[match+1:match+3], []string{set, "dst,dst"}) {
			return -1, -1
		}
		counts := strings.Trim(parts[0], "[]")
		a, b, ok := strings.Cut(counts, ":")
		packets, err1 := strconv.ParseInt(a, 10, 64)
		bytes, err2 := strconv.ParseInt(b, 10, 64)
		if ok && err1 == nil && err2 == nil && packets >= 0 && bytes >= 0 {
			return packets, bytes
		}
	}
	return -1, -1
}

func (p StalePartition) route(r *StaleRoute, exit int) bool {
	return r != nil && r.Exit != nil && *r.Exit == exit && r.HostPID == p.HostPID &&
		p.HostPID > 0 && r.Address == p.Address && r.Port == 5432 && r.StartNS < r.EndNS
}

func checkStalePartition(m Manifest, s StaleObservation, rows []staleJournalRow) string {
	const partition = "required_proof_missing:stale_challenge_partition"
	const restoration = "required_proof_missing:stale_challenge_route_restoration"
	const execution = "required_proof_missing:stale_challenge_backend_execution"
	const order = "STALE_CHALLENGE_ORDER"
	p := s.Partition
	if p == nil {
		return partition
	}
	if p.PodUID != s.Before.PodUID || p.ContainerID != s.Before.ContainerID {
		return "STALE_CHALLENGE_ORIGIN"
	}
	if p.Lock == nil || p.Lock.Epoch != s.Hold.Epoch || p.Lock.SQLState != "00000" || p.Lock.Rollback != "00000" {
		return "required_proof_missing:stale_challenge_lock_probe"
	}
	chain, set := p.dropRule(m.Namespace, p.controllerSource(m.Namespace))
	if chain == "" || !p.destination(set) || !p.route(p.Failed, 124) {
		return partition
	}
	beforePackets, beforeBytes := staleDropCounters(p.RulesBefore, chain, set)
	duringPackets, duringBytes := staleDropCounters(p.RulesDuring, chain, set)
	heldPackets, heldBytes := staleDropCounters(p.RulesHeld, chain, set)
	if beforePackets < 0 || duringPackets <= beforePackets || duringBytes <= beforeBytes ||
		heldPackets < duringPackets || heldBytes < duringBytes {
		return partition
	}
	if p.RulesAfter == "" || p.KernelGoneNS == 0 || p.AbsentNS == 0 ||
		staleAttached(p.RulesAfter, chain) || !p.route(p.Restored, 0) {
		return restoration
	}
	if !(s.Hold.EndNS < p.Lock.StartNS && p.Lock.StartNS < p.Lock.EndNS &&
		p.Lock.EndNS < p.IssuedNS && p.IssuedNS < p.Failed.StartNS && p.Failed.EndNS < p.ConfirmedNS &&
		UTC(time.Unix(0, p.ConfirmedNS)) < s.Takeover.Updated &&
		s.Takeover.EndNS < p.AbsentNS && p.AbsentNS < p.KernelGoneNS &&
		p.KernelGoneNS < p.Restored.StartNS && p.Restored.EndNS < rows[7].AtNS) {
		return order
	}
	x := rows[1].Fields.Objects
	b := p.Execution
	if b == nil || b.PID != staleObject(x, "backend_pid") || b.StartUS != staleObject(x, "backend_start_us") ||
		b.Application != "dur053-e2-"+s.Before.Session || b.State != "active" || !b.Transaction || b.WaitType != "Lock" ||
		b.ObserverPID <= 0 || !slices.Equal(b.BlockedBy, []int64{b.ObserverPID}) || b.RolledBack != "00000" ||
		strings.Join(strings.Fields(b.Query), " ") !=
			"SELECT owner_id::text, epoch, lease_expires_at, clock_timestamp() FROM engine.partition_leases WHERE partition_id = $1 FOR UPDATE" ||
		!validTime(b.QueryStart) || !validTime(b.DBTime) || b.QueryStart > b.DBTime {
		return execution
	}
	if !(rows[9].AtNS < b.StartNS && b.StartNS < b.EndNS && b.EndNS < s.Call.AtNS &&
		UTC(time.Unix(0, p.Restored.EndNS)) < b.QueryStart && b.DBTime < s.Rejected.DBTime) {
		return order
	}
	return ""
}
