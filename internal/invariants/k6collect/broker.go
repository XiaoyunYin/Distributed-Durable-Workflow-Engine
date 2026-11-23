package k6collect

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6v1"
	"github.com/segmentio/kafka-go"
)

type BrokerClient interface {
	Metadata(context.Context, *kafka.MetadataRequest) (*kafka.MetadataResponse, error)
	ListOffsets(context.Context, *kafka.ListOffsetsRequest) (*kafka.ListOffsetsResponse, error)
	OffsetFetch(context.Context, *kafka.OffsetFetchRequest) (*kafka.OffsetFetchResponse, error)
	DescribeGroups(context.Context, *kafka.DescribeGroupsRequest) (*kafka.DescribeGroupsResponse, error)
}

// MemberGenerations reads the producer callbacks. Classic DescribeGroups
// omits the generation. Keep both observations for offline corroboration.
type MemberGenerations func(context.Context) (last int64, members map[string]int64, observation *k6v1.CoordinatorObservation, err error)

func BrokerSnapshot(ctx context.Context, client BrokerClient, group string, topicIDs map[string]string,
	generations MemberGenerations) (k6v1.BrokerRow, error) {
	failure := errors.New("K6_BROKER_DESCRIBE")
	if group == "" || len(topicIDs) == 0 || generations == nil {
		return k6v1.BrokerRow{}, failure
	}
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	start := time.Now()
	firstGeneration, firstMembers, firstObservation, err := generations(ctx)
	if err != nil {
		return k6v1.BrokerRow{}, failure
	}
	groupReq := &kafka.DescribeGroupsRequest{GroupIDs: []string{group}}
	before, err := client.DescribeGroups(ctx, groupReq)
	if err != nil || len(before.Groups) != 1 || before.Groups[0].Error != nil {
		return k6v1.BrokerRow{}, failure
	}
	names := make([]string, 0, len(topicIDs))
	for name, id := range topicIDs {
		if name == "" || id == "" {
			return k6v1.BrokerRow{}, failure
		}
		names = append(names, name)
	}
	slices.Sort(names)
	meta, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: names})
	if err != nil || meta.ClusterID == "" || len(meta.Topics) != len(names) {
		return k6v1.BrokerRow{}, failure
	}
	topics := map[string][]int{}
	offsetRequests := map[string][]kafka.OffsetRequest{}
	for _, t := range meta.Topics {
		if topicIDs[t.Name] == "" || t.Error != nil || len(t.Partitions) == 0 || topics[t.Name] != nil {
			return k6v1.BrokerRow{}, failure
		}
		for _, p := range t.Partitions {
			if p.Error != nil || slices.Contains(topics[t.Name], p.ID) {
				return k6v1.BrokerRow{}, failure
			}
			topics[t.Name] = append(topics[t.Name], p.ID)
			offsetRequests[t.Name] = append(offsetRequests[t.Name], kafka.FirstOffsetOf(p.ID), kafka.LastOffsetOf(p.ID))
		}
	}
	actual, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: offsetRequests})
	if err != nil {
		return k6v1.BrokerRow{}, failure
	}
	commits, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{GroupID: group, Topics: topics})
	if err != nil || commits.Error != nil {
		return k6v1.BrokerRow{}, failure
	}
	after, err := client.DescribeGroups(ctx, groupReq)
	if err != nil || len(after.Groups) != 1 || after.Groups[0].Error != nil ||
		!reflect.DeepEqual(groupView(before.Groups[0], topicIDs), groupView(after.Groups[0], topicIDs)) {
		return k6v1.BrokerRow{}, errors.New("K6_BROKER_GROUP_RACED")
	}
	generation, memberGeneration, observation, err := generations(ctx)
	if err != nil || generation < 0 || generation != firstGeneration || !reflect.DeepEqual(firstMembers, memberGeneration) ||
		!reflect.DeepEqual(firstObservation, observation) {
		return k6v1.BrokerRow{}, errors.New("K6_BROKER_GENERATION_RACED")
	}
	out := groupView(after.Groups[0], topicIDs)
	out.Cluster, out.Generation, out.BeforeUTC, out.AfterUTC = meta.ClusterID, generation, k6v1.UTC(start), k6v1.UTC(time.Now())
	out.Coordinator = observation
	for i := range out.Members {
		key := out.Members[i].ID
		g, found := memberGeneration[key]
		if !found || g != generation {
			return k6v1.BrokerRow{}, errors.New("K6_BROKER_GENERATION_UNPROVED")
		}
		out.Members[i].Generation = g
	}
	if len(memberGeneration) != len(out.Members) {
		return k6v1.BrokerRow{}, errors.New("K6_BROKER_MEMBERS_UNJOINED")
	}
	// A classic-group rebalance may keep every member ID and still change
	// assignments. A cached callback from the earlier generation cannot
	// corroborate this bracket, even when both descriptions agree.
	if out.State == "stable" && observation != nil && !out.CoordinatorMatches() {
		return k6v1.BrokerRow{}, errors.New("K6_BROKER_ASSIGNMENTS_UNCORROBORATED")
	}
	out.Partitions = []k6v1.BrokerPartition{}
	for _, name := range names {
		slices.Sort(topics[name])
		for _, id := range topics[name] {
			oi := slices.IndexFunc(actual.Topics[name], func(p kafka.PartitionOffsets) bool { return p.Partition == id })
			ci := slices.IndexFunc(commits.Topics[name], func(p kafka.OffsetFetchPartition) bool { return p.Partition == id })
			if oi < 0 || ci < 0 || actual.Topics[name][oi].Error != nil || commits.Topics[name][ci].Error != nil {
				return k6v1.BrokerRow{}, failure
			}
			p, c := actual.Topics[name][oi], commits.Topics[name][ci]
			part := k6v1.BrokerPartition{Topic: name, TopicID: topicIDs[name], Partition: int64(id), Low: p.FirstOffset, End: p.LastOffset}
			if c.CommittedOffset >= 0 {
				part.Committed = &c.CommittedOffset
			}
			out.Partitions = append(out.Partitions, part)
		}
	}
	return out, nil
}

func groupView(snapshot kafka.DescribeGroupsResponseGroup, ids map[string]string) k6v1.BrokerRow {
	states := map[string]string{"Stable": "stable", "Empty": "empty", "PreparingRebalance": "preparing_rebalance",
		"CompletingRebalance": "completing_rebalance"}
	out := k6v1.BrokerRow{Group: snapshot.GroupID, State: states[snapshot.GroupState], Members: []k6v1.BrokerMember{}}
	for _, b := range snapshot.Members {
		member := k6v1.BrokerMember{ID: b.MemberID, ClientID: b.ClientID, Assignments: []k6v1.PartitionIdentity{}}
		for _, topic := range b.MemberAssignments.Topics {
			for _, partition := range topic.Partitions {
				member.Assignments = append(member.Assignments, k6v1.PartitionIdentity{TopicID: ids[topic.Topic], Partition: int64(partition)})
			}
		}
		slices.SortFunc(member.Assignments, func(a, b k6v1.PartitionIdentity) int {
			if a.TopicID != b.TopicID {
				return strings.Compare(a.TopicID, b.TopicID)
			}
			return int(a.Partition - b.Partition)
		})
		out.Members = append(out.Members, member)
	}
	slices.SortFunc(out.Members, func(a, b k6v1.BrokerMember) int { return strings.Compare(a.ID, b.ID) })
	return out
}

func RecordBroker(s *k6v1.Stream[k6v1.BrokerRow], row k6v1.BrokerRow, terminal bool) error {
	at, err := time.Parse(k6v1.TimeLayout, row.AfterUTC)
	if err != nil {
		return errors.New("K6_BROKER_CLOCK")
	}
	state := func(v k6v1.BrokerRow) k6v1.BrokerRow { v.BeforeUTC, v.AfterUTC = "", ""; return v }
	if len(s.Rows) == 0 || !reflect.DeepEqual(state(s.Rows[len(s.Rows)-1].Data), state(row)) {
		Append(s, at, row)
	}
	mode := "checkpoint"
	if terminal {
		mode = "terminal"
	}
	return k6v1.SealStream(s, mode, row.AfterUTC, int64(len(s.Rows)), 0)
}
