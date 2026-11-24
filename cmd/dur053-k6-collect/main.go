// dur053-k6-collect runs the Phase G external observer without a DB login.
package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6collect"
	"durable-agent-execution-engine/internal/invariants/k6v1"
	"github.com/segmentio/kafka-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

type config struct {
	Manifest        k6v1.Manifest     `json:"manifest"`
	Output          string            `json:"output"`
	Context         string            `json:"context"`
	WorkerSelector  string            `json:"worker_selector"`
	WorkerContainer string            `json:"worker_container"`
	Receiver        string            `json:"receiver"`
	Registrations   string            `json:"registrations"`
	Brokers         []string          `json:"brokers"`
	Group           string            `json:"group"`
	Topics          map[string]string `json:"topics"`
	Members         string            `json:"members"`
	Rendered        string            `json:"rendered"`
	Platform        string            `json:"platform"`
	RelayJournal    string            `json:"relay_journal"`
	RelayPeer       string            `json:"relay_peer"`
	SinkPeer        string            `json:"sink_peer"`
	Allocator       string            `json:"allocator"`
	Decisions       string            `json:"decisions"`
	Fence           string            `json:"fence"`
	WatchReady      string            `json:"watch_ready,omitempty"`
	OffsetPhase     string            `json:"offset_phase,omitempty"`
}

type processRegistration struct {
	Session  k6v1.WorkerSession `json:"session"`
	Identity k6v1.Registration  `json:"identity"`
}

type collector struct {
	config     config
	cluster    k6collect.Kubernetes
	watch      *k6collect.PodWatch
	sources    k6v1.External
	registered map[string]bool
	lastPhase  string
}

func load(path string, out any) error {
	raw, err := os.ReadFile(path)
	if err != nil || k6v1.DecodeStrict(raw, out) != nil {
		return errors.New("K6_COLLECT_INPUT")
	}
	return nil
}

func run(ctx context.Context, c config) error {
	if !c.Manifest.Valid() || c.Output == "" || c.Receiver == "" || c.Registrations == "" || c.Fence == "" ||
		len(c.Brokers) == 0 || len(c.Topics) == 0 || c.Group == "" || c.Members == "" ||
		c.Manifest.Sink == nil && (c.Rendered == "" || c.Platform == "") ||
		c.Manifest.Sink != nil && (c.RelayJournal == "" || c.Allocator == "" || c.Decisions == "" || c.RelayPeer == "" || c.SinkPeer == "") {
		return errors.New("K6_COLLECT_CONFIG")
	}
	start := time.Now()
	m := c.Manifest
	observer := collector{config: c, cluster: k6collect.Kubernetes{Context: c.Context},
		watch: k6collect.NewPodWatch(m, "kubernetes-pods", start), registered: map[string]bool{}}
	broker := k6collect.NewStream[k6v1.BrokerRow](m, "broker", "kafka-describe", start)
	observer.sources.Broker = &broker
	if m.Sink != nil {
		// The receiver is started before the live observer and may already
		// contain durable rows. The bound episode source opened at reset.
		ingress := k6collect.NewStream[k6v1.IngressRow](m, "ingress", "host-ingress-receiver", time.Unix(0, 0))
		observer.sources.Ingress = &ingress
	} else {
		topology := k6collect.NewStream[k6v1.TopologyRow](m, "topology", "independent-topology", start)
		var rendered k6v1.TopologyRow
		if load(c.Rendered, &rendered) != nil || rendered.Kind != "rendered" {
			return errors.New("K6_COLLECT_RENDERED")
		}
		k6collect.Append(&topology, start, rendered)
		observer.sources.Topology = &topology
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	watchDone := make(chan error, 1)
	go func() {
		watchDone <- observer.cluster.Watch(ctx, m, c.WorkerSelector, c.WorkerContainer, observer.watch, func(cursor string) error {
			if c.WatchReady == "" {
				return nil
			}
			return os.WriteFile(c.WatchReady, []byte(cursor+"\n"), 0o600)
		})
	}()
	transport := &kafka.Transport{DialTimeout: 2 * time.Second, IdleTimeout: 5 * time.Second}
	defer transport.CloseIdleConnections()
	client := &kafka.Client{Addr: kafka.TCP(c.Brokers...), Transport: transport, Timeout: 4 * time.Second}
	metadata, err := kgo.NewClient(kgo.SeedBrokers(c.Brokers...))
	if err != nil {
		return errors.New("K6_COLLECT_BROKER_METADATA")
	}
	defer metadata.Close()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var generation int64
	published := false
	var failedAt time.Time
	var lastError string
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-watchDone:
			return err
		case <-ticker.C:
		}
		generation++
		err := observer.collect(ctx, client, metadata, generation)
		if err == nil {
			published = true
			failedAt = time.Time{}
			lastError = ""
		} else if failedAt.IsZero() {
			failedAt = time.Now()
		} else if published && observer.drained() && time.Since(failedAt) > k6v1.MaxSampleGap {
			return err
		}
		if err != nil && lastError != err.Error() {
			lastError = err.Error()
			fmt.Fprintln(os.Stderr, lastError)
		}
	}
}

func (o *collector) drained() bool {
	var fence k6v1.SourceBinding
	return load(o.config.Fence, &fence) == nil && fence == k6v1.Binding(o.config.Manifest)
}

func (o *collector) collect(ctx context.Context, client *kafka.Client, metadata *kgo.Client, generation int64) error {
	c, m := o.config, o.config.Manifest
	var registrations []processRegistration
	if load(c.Registrations, &registrations) != nil || registrations == nil {
		return errors.New("K6_COLLECT_REGISTRATIONS")
	}
	for _, r := range registrations {
		if !o.registered[r.Session.ID] && o.watch.Register(time.Now(), r.Session, r.Identity) == nil {
			o.registered[r.Session.ID] = true
		}
	}
	// The reset controller writes this fence after stopping external writer
	// admissions and waiting for their shutdown acknowledgements.
	drained := o.drained()
	roster, err := o.watch.Snapshot(time.Now(), drained)
	if err != nil {
		return err
	}
	o.sources.Roster = roster
	receiver, err := os.OpenRoot(c.Receiver)
	if err != nil {
		return errors.New("K6_COLLECT_RECEIVER")
	}
	defer receiver.Close()
	if issues := k6collect.FetchLedgers(receiver.FS(), &o.sources, m); len(issues) > 0 {
		return fmt.Errorf("K6_COLLECT_%s:%s", issues[0].Code, issues[0].Source)
	}
	for _, r := range registrations {
		l, ok := o.sources.Sessions[r.Session.ID]
		if !ok {
			continue
		}
		o.watch.BindShutdown(time.Now(), r.Session, l)
		if l.Seal.Mode == "checkpoint" && o.watch.ConfirmDeath(time.Now(), r.Session, &l) == nil {
			o.sources.Sessions[r.Session.ID] = l
			// The watch will publish the death after its next progress marker.
			return errors.New("K6_COLLECT_DEATH_PENDING")
		}
	}
	for _, row := range o.sources.Roster.Rows {
		if row.Data.Death != nil {
			id := row.Data.Session.ID
			l, ok := o.sources.Sessions[id]
			if !ok || l.Seal.Last != row.Data.Death.FinalSequence || l.Seal.SHA256 != row.Data.Death.FinalSHA256 {
				return errors.New("K6_COLLECT_DEATH_CHANGED")
			}
			l.Seal.Mode = "receiver_final"
			l.Seal.ThroughUTC = max(l.Seal.ThroughUTC, row.Data.Death.ObservedUTC)
			o.sources.Sessions[id] = l
		}
	}
	brokerCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	var phase k6v1.OffsetPhase
	phased := c.OffsetPhase != "" && load(c.OffsetPhase, &phase) == nil && phase.Valid(m) && phase.Name != o.lastPhase
	names := slices.Sorted(maps.Keys(c.Topics))
	ids, err := k6collect.BrokerTopicIDs(brokerCtx, metadata, names)
	if err != nil {
		return err
	}
	row, err := k6collect.BrokerSnapshot(brokerCtx, client, c.Group, ids, func(context.Context) (int64, map[string]int64, *k6v1.CoordinatorObservation, error) {
		var state struct {
			Generation  int64                        `json:"generation"`
			Members     map[string]int64             `json:"members"`
			Coordinator *k6v1.CoordinatorObservation `json:"coordinator,omitempty"`
		}
		err := load(c.Members, &state)
		return state.Generation, state.Members, state.Coordinator, err
	})
	if err != nil {
		return err
	}
	copies := o.sources.BrokerCopies
	if drained && copies == nil {
		stream := k6collect.NewStream[k6v1.BrokerCopyRow](m, "broker-copies", "kafka-physical-fetch", time.Now())
		// Leave time to close the metadata bracket even when a fetch stalls.
		readCtx, stop := context.WithTimeout(brokerCtx, 2*time.Second)
		k6collect.ReadBrokerCopies(readCtx, client, &stream, row.Partitions)
		stop()
		copies = &stream
	}
	idsAfter, err := k6collect.BrokerTopicIDs(brokerCtx, metadata, names)
	if err != nil {
		return err
	}
	if !maps.Equal(ids, idsAfter) {
		row.TopicIDsAfter = idsAfter
	}
	row.AfterUTC = k6v1.UTC(time.Now())
	if phased {
		var current k6v1.OffsetPhase
		if load(c.OffsetPhase, &current) == nil && current == phase && phase.RequestedUTC <= row.BeforeUTC {
			row.OffsetPhase = &phase
		}
	}
	if copies != nil {
		if err := k6v1.SealStream(copies, "terminal", row.AfterUTC, int64(len(copies.Rows)), 0); err != nil {
			return err
		}
		o.sources.BrokerCopies = copies
	}
	if err := k6collect.RecordBroker(o.sources.Broker, row, drained); err != nil {
		return err
	}
	if row.OffsetPhase != nil {
		o.lastPhase = row.OffsetPhase.Name
	}
	if m.Sink == nil {
		var platform k6collect.PlatformInventory
		if load(c.Platform, &platform) != nil {
			return errors.New("K6_COLLECT_PLATFORM")
		}
		row, err := o.cluster.Topology(ctx, m.Namespace, platform)
		if err != nil {
			return err
		}
		if err := k6collect.ObserveTopology(o.sources.Topology, row, time.Now(), drained); err != nil {
			return err
		}
	} else if err := o.effect(m, drained); err != nil {
		return err
	}
	return k6collect.Publish(c.Output, generation, m, o.sources)
}

func (o *collector) effect(m k6v1.Manifest, drained bool) error {
	c := o.config
	open := func(path string) (*os.Root, string, error) {
		root, err := os.OpenRoot(filepath.Dir(path))
		return root, filepath.Base(path), err
	}
	auth, name, err := open(c.Decisions)
	if err != nil {
		return errors.New("K6_COLLECT_DECISIONS")
	}
	defer auth.Close()
	decisions, issues := k6v1.ReadDecisionStream(auth.FS(), m, name)
	if len(issues) > 0 {
		return errors.New("K6_COLLECT_DECISIONS")
	}
	if decisions.Seal.Mode == "terminal" || drained {
		decisions.Seal.Mode, decisions.Seal.ThroughUTC = "terminal", k6v1.UTC(time.Now())
	}
	o.sources.Decisions = decisions
	allocator, name, err := open(c.Allocator)
	if err != nil {
		return errors.New("K6_COLLECT_ALLOCATOR")
	}
	defer allocator.Close()
	direct, issues := k6v1.ReadAllocatorStream(allocator.FS(), m, name)
	if len(issues) > 0 {
		return errors.New("K6_COLLECT_ALLOCATOR")
	}
	relay, err := os.Open(c.RelayJournal)
	if err != nil {
		return errors.New("K6_COLLECT_INGRESS")
	}
	defer relay.Close()
	return k6collect.IngressSnapshot(relay, o.sources.Ingress, c.RelayPeer, c.SinkPeer, time.Now(), slices.Clone(direct.Rows), drained)
}

func main() {
	if len(os.Args) == 3 && os.Args[1] == "--required-sources" {
		arm, capture := strings.CutPrefix(os.Args[2], "capture-")
		if arm == "pure" || arm == "effect" {
			raw, err := k6v1.Canonical(k6v1.RequiredSources(false, arm == "effect", capture))
			if err != nil {
				os.Exit(1)
			}
			_, _ = os.Stdout.Write(raw)
			return
		}
	}
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: dur053-k6-collect PRIVATE_CONFIG")
		os.Exit(2)
	}
	var c config
	if err := load(os.Args[1], &c); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, c); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
