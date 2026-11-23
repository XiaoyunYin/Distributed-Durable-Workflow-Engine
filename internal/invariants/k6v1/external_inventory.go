package k6v1

import "slices"

type BrokerPartition struct {
	Topic     string `json:"topic"`
	TopicID   string `json:"topic_id"`
	Partition int64  `json:"partition"`
	Low       int64  `json:"low"`
	End       int64  `json:"end"`
	Committed *int64 `json:"committed"`
}

type BrokerMember struct {
	ID          string              `json:"id"`
	ClientID    string              `json:"client_id"`
	Generation  int64               `json:"generation"`
	Assignments []PartitionIdentity `json:"assignments"`
}

type PartitionIdentity struct {
	TopicID   string `json:"topic_id"`
	Partition int64  `json:"partition"`
}

// Versions are observer positions over the group state, separate from Kafka's
// group generation. Members and broker positions are captured independently.
type BrokerRow struct {
	Cluster     string                  `json:"cluster"`
	Group       string                  `json:"group"`
	Generation  int64                   `json:"generation"`
	State       string                  `json:"state"`
	BeforeUTC   string                  `json:"before_utc"`
	AfterUTC    string                  `json:"after_utc"`
	Partitions  []BrokerPartition       `json:"partitions"`
	Members     []BrokerMember          `json:"members"`
	Coordinator *CoordinatorObservation `json:"coordinator,omitempty"`
}

// Classic DescribeGroups provides membership and assignments, but no numeric
// generation. Keep the callbacks used to corroborate it as separate evidence.
type CoordinatorObservation struct {
	Generation int64          `json:"generation"`
	Members    []BrokerMember `json:"members"`
}

func (r *Result) checkGroupMembership(m Manifest, broker *Stream[BrokerRow]) {
	const predicate = "GroupMembership"
	required := m.Cell == "medium-10" || m.Cell == "medium-11"
	corroborated := false
	if broker != nil {
		for _, record := range broker.Rows {
			b := record.Data
			if b.State != "stable" {
				continue
			}
			if b.Coordinator == nil || b.Coordinator.Members == nil {
				if required {
					raw, _ := Canonical(record)
					ref := Witness{Path: BrokerPath, RowKey: []any{record.Position.Sequence}, SHA256: Hash(raw)}
					r.record(MissingProof, predicate, "required_proof_missing:GroupMembership", BrokerPath, &ref)
				}
				continue
			}
			if !b.CoordinatorMatches() {
				flagExternal(r, 11, predicate, "GROUP_COORDINATOR_DESCRIPTION_MISMATCH", BrokerPath, record)
				continue
			}
			corroborated = true
		}
	}
	if required && !corroborated && !slices.ContainsFunc(r.Findings, func(f Finding) bool { return f.Predicate == predicate }) {
		r.record(MissingProof, predicate, "required_proof_missing:GroupMembership", BrokerPath, nil)
	}
}

// CoordinatorMatches compares a callback claim with a classic-group description.
func (b BrokerRow) CoordinatorMatches() bool {
	return b.Coordinator != nil && b.Generation > 0 && b.Coordinator.Generation == b.Generation &&
		slices.EqualFunc(b.Coordinator.Members, b.Members, func(claim, observed BrokerMember) bool {
			return claim.ID == observed.ID && claim.ClientID == observed.ClientID && claim.Generation == b.Generation &&
				claim.Assignments != nil && slices.Equal(claim.Assignments, observed.Assignments)
		})
}

func (b BrokerRow) valid() bool {
	if !identities(b.Cluster, b.Group) || b.Generation < 0 || len(b.Partitions) == 0 ||
		!validTime(b.BeforeUTC) || !validTime(b.AfterUTC) || b.BeforeUTC > b.AfterUTC ||
		b.Members == nil || !slices.Contains([]string{"stable", "empty", "preparing_rebalance", "completing_rebalance"}, b.State) ||
		(b.State == "empty" && len(b.Members) != 0) || (b.State == "stable" && len(b.Members) == 0) {
		return false
	}
	if b.Coordinator != nil {
		if b.Coordinator.Generation < 0 {
			return false
		}
		lastID := ""
		for _, member := range b.Coordinator.Members {
			if !identities(member.ID, member.ClientID) || member.ID <= lastID || member.Generation < 0 {
				return false
			}
			lastID = member.ID
			for _, assignment := range member.Assignments {
				if !nonempty(assignment.TopicID) || assignment.Partition < 0 {
					return false
				}
			}
		}
	}
	positions := map[PartitionIdentity]bool{}
	topics := map[string]string{}
	var prior BrokerPartition
	for _, p := range b.Partitions {
		if !identities(p.Topic, p.TopicID) || p.Partition < 0 || p.Low < 0 || p.End < p.Low ||
			(p.Committed != nil && (*p.Committed < 0 || *p.Committed > p.End)) ||
			(p.Topic < prior.Topic || p.Topic == prior.Topic && p.Partition <= prior.Partition) {
			return false
		}
		key := PartitionIdentity{p.TopicID, p.Partition}
		if positions[key] || (topics[p.Topic] != "" && topics[p.Topic] != p.TopicID) {
			return false
		}
		positions[key], topics[p.Topic], prior = true, p.TopicID, p
	}
	lastID := ""
	for _, member := range b.Members {
		if !identities(member.ID, member.ClientID) || member.ID <= lastID || member.Assignments == nil || member.Generation != b.Generation {
			return false
		}
		lastID = member.ID
		var last PartitionIdentity
		for _, assignment := range member.Assignments {
			if !positions[assignment] || assignment.TopicID < last.TopicID ||
				assignment.TopicID == last.TopicID && assignment.Partition <= last.Partition {
				return false
			}
			delete(positions, assignment)
			last = assignment
		}
	}
	return true
}

func brokerRegression(old, next BrokerRow) bool {
	if old.Cluster != next.Cluster || old.Group != next.Group || next.Generation < old.Generation ||
		len(old.Partitions) != len(next.Partitions) {
		return true
	}
	for i, p := range old.Partitions {
		n := next.Partitions[i]
		if n.Topic != p.Topic || n.TopicID != p.TopicID || n.Partition != p.Partition ||
			n.Low < p.Low || n.End < p.End || (p.Committed != nil && (n.Committed == nil || *n.Committed < *p.Committed)) {
			return true
		}
	}
	return false
}

type Component struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
	// Role is established by the rendered intent or by the independent
	// inventory classifier. Unknown observed components fail closure.
	Role string `json:"role"`
}

type TopologyRow struct {
	Kind       string      `json:"kind"`
	Observer   string      `json:"observer"`
	Revision   string      `json:"revision"`
	Components []Component `json:"components"`
	Scanned    []string    `json:"scanned"`
}

var topologyKinds = []string{"database", "deployment", "journal", "relay", "service"}

func (t TopologyRow) valid() bool {
	if !identities(t.Observer, t.Revision) || !slices.Equal(t.Scanned, topologyKinds) ||
		(t.Kind != "rendered" && t.Kind != "observed") || len(t.Components) == 0 {
		return false
	}
	previous := ""
	for _, c := range t.Components {
		key := c.Kind + "/" + c.Name
		if !slices.Contains(topologyKinds, c.Kind) || !identities(c.Name, c.Role) || key <= previous {
			return false
		}
		previous = key
	}
	return true
}

func pureTopology(m Manifest, stream Stream[TopologyRow]) string {
	if len(stream.Rows) < 2 || stream.Rows[0].Data.Kind != "rendered" {
		return "PURE_TOPOLOGY_PROOF"
	}
	rendered := stream.Rows[0].Data
	raw, err := Canonical(rendered.Components)
	if err != nil || Hash(raw) != m.TopologySHA256 {
		return "PURE_TOPOLOGY_BINDING"
	}
	for _, row := range stream.Rows {
		for _, component := range row.Data.Components {
			if pureEffectComponent(component) {
				return "PURE_EFFECT_COMPONENT"
			}
			if component.Role == "unclassified" {
				return "PURE_TOPOLOGY_INVENTORY"
			}
		}
	}
	for _, row := range stream.Rows[1:] {
		observed := row.Data
		if observed.Kind != "observed" || observed.Observer == rendered.Observer || !slices.Equal(observed.Components, rendered.Components) {
			return "PURE_TOPOLOGY_INVENTORY"
		}
	}
	return ""
}

func pureEffectComponent(c Component) bool {
	return c.Role == "sink" || c.Role == "relay" || c.Kind == "relay" || c.Kind == "journal"
}
