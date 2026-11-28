package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/restoreledger"
	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/transport"
	"github.com/segmentio/kafka-go"
)

const (
	kafkaData         = "/var/lib/kafka/data"
	kafkaInternalPort = 19092
	kafkaHostPort     = 29092
	sourceKafkaRole   = "source-kafka"
	targetKafkaRole   = "target-kafka"
)

type kafkaPlacement struct {
	Volume string `json:"volume"`
	Alias  string `json:"network_alias"`
	Port   int    `json:"host_port"`
}

func (p kafkaPlacement) hostEndpoint() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(p.Port))
}

func (p kafkaPlacement) runtimeEndpoint() string {
	return net.JoinHostPort(p.Alias, strconv.Itoa(kafkaInternalPort))
}

type kafkaConfig struct {
	Ledger          string         `json:"ledger"`
	Image           string         `json:"image"`
	SourceID        string         `json:"source_container_id"`
	Source          kafkaPlacement `json:"source"`
	Target          kafkaPlacement `json:"target"`
	SourceReceipt   string         `json:"source_kafka_receipt"`
	RestoreConfig   string         `json:"restore_config"`
	TargetConsumers string         `json:"target_consumers"`
	FreshReceipt    string         `json:"fresh_kafka_receipt"`
}

func (c kafkaConfig) validate() error {
	paths := []string{c.Ledger, c.SourceReceipt, c.RestoreConfig, c.TargetConsumers, c.FreshReceipt}
	for i, path := range paths {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || slices.Contains(paths[:i], path) {
			return errors.New("Kafka drill metadata requires distinct absolute paths")
		}
		parent, err := filepath.EvalSymlinks(filepath.Dir(path))
		if err != nil || parent != filepath.Dir(path) {
			return errors.New("Kafka drill metadata paths need canonical parents")
		}
	}
	image, ok := strings.CutPrefix(c.Image, "apache/kafka:4.3.1@sha256:")
	if !ok || !dockerID.MatchString(image) || !dockerID.MatchString(c.SourceID) ||
		!kafkaNames(c.Source) || !kafkaNames(c.Target) || c.Source.Alias == c.Target.Alias ||
		c.Source.Volume == c.Target.Volume || c.Source.Port == c.Target.Port {
		return errors.New("Kafka requires a pinned 4.3.1 image and distinct owned source and target resources")
	}
	return nil
}

func kafkaNames(p kafkaPlacement) bool {
	return volumeName.MatchString(p.Volume) && fixtureName.MatchString(p.Alias) &&
		strings.HasPrefix(p.Alias, "j7-") && !strings.Contains(p.Alias, "_") &&
		p.Port >= 1024 && p.Port <= 65535
}

type kafkaContainer struct {
	sourceContainer
	Config struct {
		Labels map[string]string
		Env    []string
	}
	HostConfig struct {
		NetworkMode   string
		AutoRemove    bool
		Memory        int64
		NanoCpus      int64
		Privileged    bool
		RestartPolicy struct{ Name string }
		PortBindings  map[string][]struct{ HostIP, HostPort string }
	}
	NetworkSettings struct {
		Networks map[string]struct{ Aliases []string }
	}
}

type kafkaBinding struct {
	ID              string       `json:"container_id"`
	StartedAt       string       `json:"started_at"`
	Image           string       `json:"image_id"`
	Volume          sourceVolume `json:"volume"`
	RuntimeEndpoint string       `json:"runtime_endpoint"`
}

type sourceKafkaReceipt struct {
	Schema    string        `json:"schema"`
	ConfigSHA string        `json:"config_sha256"`
	BootID    string        `json:"boot_id"`
	Authority string        `json:"authority"`
	Binding   kafkaBinding  `json:"binding"`
	Broker    kafkaSnapshot `json:"broker"`
}

type freshKafkaReceipt struct {
	Schema              string        `json:"schema"`
	ConfigSHA           string        `json:"config_sha256"`
	BootID              string        `json:"boot_id"`
	Authority           string        `json:"authority"`
	SourceSHA           string        `json:"source_kafka_receipt_sha256"`
	RestoreSHA          string        `json:"restore_receipt_sha256"`
	TargetConsumersSHA  string        `json:"rendered_target_consumers_sha256"`
	T0NS                int64         `json:"t0_ns"`
	OldVolumeAbsentNS   int64         `json:"old_broker_volume_absent_ns,omitempty"`
	OldEndpointDeniedNS int64         `json:"old_broker_endpoint_denied_ns,omitempty"`
	Target              kafkaBinding  `json:"target"`
	Broker              kafkaSnapshot `json:"broker"`
	CompletedNS         int64         `json:"completed_ns,omitempty"`
}

func brokerLabels(h restoreledger.Header, role string) map[string]string {
	labels := faultLabels(h)
	labels[roleLabel] = role
	return labels
}

func (d localDocker) bindKafka(ctx context.Context, id, image string, p kafkaPlacement,
	labels map[string]string) (kafkaBinding, containerState, error) {
	var b kafkaBinding
	raw, err := d.command(ctx, "container", "inspect", id)
	if err != nil {
		return b, containerState{}, err
	}
	var c kafkaContainer
	if err := decodeInspection(raw, &c); err != nil {
		return b, containerState{}, err
	}
	b.ID, b.Image, b.StartedAt, b.RuntimeEndpoint = c.ID, c.Image, c.State.StartedAt, p.runtimeEndpoint()
	if c.ID != id || c.Image != image || c.State.StartedAt == "" || c.RestartCount != 0 ||
		c.HostConfig.AutoRemove || c.HostConfig.Privileged || c.HostConfig.RestartPolicy.Name != "no" ||
		c.HostConfig.NetworkMode != "kind" || len(c.NetworkSettings.Networks) != 1 ||
		!slices.Contains(c.NetworkSettings.Networks["kind"].Aliases, p.Alias) ||
		!matchesLabels(c.Config.Labels, labels) || c.HostConfig.Memory != 1536<<20 || c.HostConfig.NanoCpus != 1_000_000_000 {
		return b, c.State, errors.New("Kafka container identity, network, limits or ownership drifted")
	}
	ports := c.HostConfig.PortBindings[fmt.Sprintf("%d/tcp", kafkaHostPort)]
	if len(c.HostConfig.PortBindings) != 1 || len(ports) != 1 || ports[0].HostIP != "127.0.0.1" ||
		ports[0].HostPort != strconv.Itoa(p.Port) {
		return b, c.State, errors.New("Kafka host observer requires one sealed loopback listener")
	}
	for _, value := range []string{"KAFKA_LOG_DIRS=" + kafkaData, "KAFKA_METADATA_LOG_DIR=" + kafkaData,
		"KAFKA_HEAP_OPTS=-Xms512m -Xmx512m", "KAFKA_AUTO_CREATE_TOPICS_ENABLE=false",
		"KAFKA_ADVERTISED_LISTENERS=INTERNAL://" + p.runtimeEndpoint() + ",HOST://" + p.hostEndpoint()} {
		if !slices.Contains(c.Config.Env, value) {
			return b, c.State, errors.New("Kafka effective log directory, heap or listener configuration differs")
		}
	}
	b.Volume, err = d.volume(ctx, p.Volume)
	if err != nil {
		return b, c.State, err
	}
	if b.Volume.Driver != "local" || b.Volume.Scope != "local" || b.Volume.CreatedAt == "" ||
		len(b.Volume.Options) != 0 || !matchesLabels(b.Volume.Labels, labels) {
		return b, c.State, errors.New("Kafka volume does not match the owned named local resource")
	}
	owned := 0
	for _, mount := range c.Mounts {
		if mount.Destination == kafkaData && mount.Name == p.Volume && mount.Source == b.Volume.Mountpoint &&
			mount.Driver == "local" && mount.Type == "volume" && mount.RW {
			owned++
		} else if pathContains(mount.Destination, kafkaData) || pathContains(kafkaData, mount.Destination) {
			return b, c.State, errors.New("unexpected mount overlaps Kafka data")
		}
	}
	refs, err := d.refs(ctx, p.Volume)
	if err != nil || owned != 1 || !slices.Equal(refs, []string{id}) {
		return b, c.State, errors.New("Kafka data volume must belong only to its sealed container")
	}
	return b, c.State, nil
}

func observeSourceKafka(ctx context.Context, d localDocker, c kafkaConfig, sha string,
	h restoreledger.Header, now restoreledger.Clock) (sourceKafkaReceipt, error) {
	r := sourceKafkaReceipt{Schema: "j7.source-kafka.v1", ConfigSHA: sha, BootID: h.BootID, Authority: h.Source}
	image, err := d.command(ctx, "image", "inspect", "--format", "{{.Id}}", c.Image)
	imageID := strings.TrimSpace(string(image))
	if err != nil || !strings.HasPrefix(imageID, "sha256:") || !dockerID.MatchString(strings.TrimPrefix(imageID, "sha256:")) {
		return r, errors.New("pinned Kafka image is unavailable locally")
	}
	binding, process, err := d.bindKafka(ctx, c.SourceID, imageID, c.Source, brokerLabels(h, sourceKafkaRole))
	r.Binding = binding
	if err != nil {
		return r, err
	}
	if !runningSource(process) {
		return r, errors.New("source Kafka is unavailable for the pre-cut observation")
	}
	b, err := openJ7Broker(c.Source.hostEndpoint())
	if err != nil {
		return r, err
	}
	defer b.ids.Close()
	r.Broker, err = b.snapshot(ctx, now, true)
	return r, err
}

func (d localDocker) retireKafka(ctx context.Context, c kafkaConfig, old sourceKafkaReceipt,
	h restoreledger.Header, now restoreledger.Clock, r *freshKafkaReceipt) error {
	bound, status, err := d.bindKafka(ctx, c.SourceID, old.Binding.Image, c.Source, brokerLabels(h, sourceKafkaRole))
	if err != nil {
		return err
	}
	if bound.ID != old.Binding.ID || bound.StartedAt != old.Binding.StartedAt ||
		!sameVolume(bound.Volume, old.Binding.Volume) || status.OOMKilled {
		return errors.New("source Kafka was replaced since the original broker observation")
	}
	names, err := d.volumes(ctx)
	if err != nil || slices.Contains(names, c.Target.Volume) {
		return errors.New("fresh Kafka volume must be absent before retirement")
	}
	if runningSource(status) {
		out, err := d.command(ctx, "container", "stop", "--time=10", c.SourceID)
		if err != nil || strings.TrimSpace(string(out)) != c.SourceID {
			return errors.New("could not stop sealed source Kafka")
		}
	} else if status.Status != "exited" || status.Pid != 0 {
		return errors.New("unexpected sealed source Kafka status")
	}
	out, err := d.command(ctx, "container", "rm", c.SourceID)
	if err != nil || strings.TrimSpace(string(out)) != c.SourceID {
		return errors.New("could not retire sealed source Kafka container")
	}
	refs, err := d.refs(ctx, c.Source.Volume)
	if err != nil || len(refs) != 0 {
		return errors.New("source Kafka volume still has container references")
	}
	out, err = d.command(ctx, "volume", "rm", c.Source.Volume)
	if err != nil || strings.TrimSpace(string(out)) != c.Source.Volume {
		return errors.New("could not remove the sealed source Kafka data volume")
	}
	names, err = d.volumes(ctx)
	if err != nil || slices.Contains(names, c.Source.Volume) || slices.Contains(names, c.Target.Volume) {
		return errors.New("old and new Kafka volumes must both be absent at retirement")
	}
	r.OldVolumeAbsentNS, err = now()
	if err != nil {
		return err
	}
	out, err = d.command(ctx, "container", "ls", "--all", "--no-trunc", "--filter", "id="+c.SourceID, "--format", "{{.ID}}")
	if err != nil || len(strings.Fields(string(out))) != 0 {
		return errors.New("retired Kafka container still exists")
	}
	dial := net.Dialer{Timeout: time.Second}
	conn, err := dial.DialContext(ctx, "tcp", c.Source.hostEndpoint())
	if err == nil {
		conn.Close()
		return errors.New("old Kafka host endpoint still accepts connections")
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	r.OldEndpointDeniedNS, err = now()
	return err
}

func (d localDocker) launchFreshKafka(ctx context.Context, c kafkaConfig, old kafkaBinding,
	h restoreledger.Header, now restoreledger.Clock, r *freshKafkaReceipt) error {
	labels := brokerLabels(h, targetKafkaRole)
	args := append([]string{"volume", "create", "--driver=local"}, labelArgs(labels)...)
	out, err := d.command(ctx, append(args, c.Target.Volume)...)
	if err != nil || strings.TrimSpace(string(out)) != c.Target.Volume {
		return errors.New("could not create fresh named Kafka volume")
	}
	r.Target.Volume, err = d.volume(ctx, c.Target.Volume)
	if err != nil || r.Target.Volume.Mountpoint == old.Volume.Mountpoint ||
		!matchesLabels(r.Target.Volume.Labels, labels) {
		return errors.New("could not prove distinct target Kafka storage")
	}
	var clusterBytes [16]byte
	if _, err := rand.Read(clusterBytes[:]); err != nil {
		return err
	}
	clusterID := base64.RawURLEncoding.EncodeToString(clusterBytes[:])
	p := c.Target
	args = []string{"container", "create", "--pull=never", "--name=" + p.Alias, "--network=kind", "--network-alias=" + p.Alias,
		"--restart=no", "--cpus=1", "--memory=1536m", "--publish=" + p.hostEndpoint() + ":" + strconv.Itoa(kafkaHostPort),
		"--mount=type=volume,src=" + p.Volume + ",dst=" + kafkaData}
	args = append(args, labelArgs(labels)...)
	for _, entry := range kafkaEnv(p, clusterID) {
		args = append(args, "--env="+entry)
	}
	out, err = d.command(ctx, append(args, old.Image)...)
	r.Target.ID = strings.TrimSpace(string(out))
	if err != nil || !dockerID.MatchString(r.Target.ID) || r.Target.ID == old.ID {
		return errors.New("could not create fresh Kafka container")
	}
	out, err = d.command(ctx, "container", "start", r.Target.ID)
	if err != nil || strings.TrimSpace(string(out)) != r.Target.ID {
		return errors.New("could not start sealed target Kafka")
	}
	bound, process, err := d.bindKafka(ctx, r.Target.ID, old.Image, p, labels)
	if err != nil {
		return err
	}
	if !runningSource(process) || !sameVolume(r.Target.Volume, bound.Volume) {
		return errors.New("target Kafka is not running on the created volume")
	}
	r.Target = bound
	b, err := openJ7Broker(p.hostEndpoint())
	if err != nil {
		return err
	}
	defer b.ids.Close()
	if err := awaitNewKafka(ctx, b.api, clusterID); err != nil {
		return err
	}
	r.Broker, err = b.snapshot(ctx, now, false)
	if err != nil {
		return err
	}
	if r.Broker.Cluster != clusterID {
		return errors.New("target Kafka cluster identifier differs from the newly generated one")
	}
	return nil
}

func kafkaEnv(p kafkaPlacement, cluster string) []string {
	return []string{
		"CLUSTER_ID=" + cluster, "KAFKA_NODE_ID=1", "KAFKA_PROCESS_ROLES=broker,controller",
		"KAFKA_LISTENERS=CONTROLLER://:9093,INTERNAL://:19092,HOST://:29092",
		"KAFKA_ADVERTISED_LISTENERS=INTERNAL://" + p.runtimeEndpoint() + ",HOST://" + p.hostEndpoint(),
		"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,HOST:PLAINTEXT",
		"KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER", "KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL",
		"KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
		"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
		"KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR=1", "KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR=1",
		"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0", "KAFKA_NUM_PARTITIONS=8", "KAFKA_AUTO_CREATE_TOPICS_ENABLE=false",
		"KAFKA_HEAP_OPTS=-Xms512m -Xmx512m", "KAFKA_LOG_DIRS=" + kafkaData, "KAFKA_METADATA_LOG_DIR=" + kafkaData,
	}
}

func awaitNewKafka(ctx context.Context, api *kafka.Client, cluster string) error {
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		metadata, err := api.Metadata(ctx, &kafka.MetadataRequest{})
		if err == nil && metadata.ClusterID == cluster && len(metadata.Brokers) == 1 {
			if len(metadata.Topics) != 0 {
				return errors.New("fresh Kafka broker already contains topics before creation")
			}
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for fresh Kafka: %w", ctx.Err())
		case <-tick.C:
		}
	}
	cfg := make([]kafka.TopicConfig, 0, 2)
	for _, topic := range j7Topics {
		cfg = append(cfg, kafka.TopicConfig{Topic: topic, NumPartitions: 8, ReplicationFactor: 1})
	}
	out, err := api.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: cfg})
	if err != nil || len(out.Errors) != 2 {
		return errors.New("fresh Kafka topic creation response was incomplete")
	}
	for _, topic := range j7Topics {
		if e, ok := out.Errors[topic]; !ok || e != nil {
			return fmt.Errorf("fresh Kafka topic creation failed for %s: %v", topic, e)
		}
	}
	for {
		meta, err := api.Metadata(ctx, &kafka.MetadataRequest{})
		if err == nil && meta.ClusterID == cluster && len(meta.Topics) == 2 &&
			!slices.ContainsFunc(meta.Topics, func(t kafka.Topic) bool {
				return t.Error != nil || len(t.Partitions) != 8 ||
					slices.ContainsFunc(t.Partitions, func(p kafka.Partition) bool { return p.Error != nil || len(p.Isr) != 1 })
			}) {
			groups, err := api.ListGroups(ctx, &kafka.ListGroupsRequest{})
			if err == nil && groups.Error == nil {
				if len(groups.Groups) != 0 {
					return errors.New("consumer groups joined before fresh Kafka admission")
				}
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for fresh Kafka partition and group metadata: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

type renderedEnv struct {
	Name      string          `json:"name"`
	Value     *string         `json:"value,omitempty"`
	ValueFrom json.RawMessage `json:"valueFrom,omitempty"`
}

type renderedConsumer struct {
	Role    string        `json:"role"`
	EnvFrom bool          `json:"env_from"`
	Env     []renderedEnv `json:"env"`
}

func verifyGenerationRender(rows []renderedConsumer, endpoint string) error {
	roles := map[string]bool{"scheduler": true, "worker": true, "worker-control": true}
	for _, row := range rows {
		if row.EnvFrom || !roles[row.Role] {
			return errors.New("CONSUMER_GENERATION_MISMATCH: unbound consumer role or opaque environment")
		}
		delete(roles, row.Role)
		literal := map[string]string{}
		for _, e := range row.Env {
			switch e.Name {
			case "RUNTIME_KAFKA_GENERATION", "KAFKA_BOOTSTRAP_SERVERS":
				if _, seen := literal[e.Name]; seen || e.Value == nil || present(e.ValueFrom) {
					return errors.New("CONSUMER_GENERATION_MISMATCH: consumer wiring must use one literal value")
				}
				literal[e.Name] = *e.Value
			}
		}
		if literal["RUNTIME_KAFKA_GENERATION"] != transport.J7RestoreGeneration {
			return fmt.Errorf("CONSUMER_GENERATION_MISMATCH: %s", row.Role)
		}
		broker, configured := literal["KAFKA_BOOTSTRAP_SERVERS"]
		if (row.Role != "worker-control" || configured) && broker != endpoint {
			return fmt.Errorf("CONSUMER_GENERATION_MISMATCH: %s broker route", row.Role)
		}
	}
	if len(roles) != 0 {
		return errors.New("CONSUMER_GENERATION_MISMATCH: missing required role")
	}
	return nil
}

func loadTraceArtifact(path, expected string) (invariants.Trace, error) {
	var t invariants.Trace
	f, err := openRegular(path, os.O_RDONLY)
	if err != nil {
		return t, err
	}
	defer f.Close()
	raw, err := io.ReadAll(f)
	if err != nil {
		return t, err
	}
	if restoreledger.SHA256(raw) != expected {
		return t, errors.New("engine snapshot differs from its sealed digest")
	}
	err = json.Unmarshal(raw, &t)
	return t, err
}

func runKafkaLifecycle(ctx context.Context, args []string, out, diagnostics io.Writer) error {
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	config := flags.String("config", "", "absolute private Kafka drill configuration")
	sourceDigest := flags.String("source-sha256", "", "digest of the sealed pre-cut Kafka observation")
	restoreDigest := flags.String("restore-sha256", "", "digest of the completed isolated restore")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || !filepath.IsAbs(*config) {
		return errors.New("Kafka stage requires a private absolute configuration path")
	}
	now, boot, err := restoreledger.HostClock()
	if err != nil {
		return err
	}
	var cfg kafkaConfig
	sha, err := readFaultJSON(*config, &cfg)
	if err != nil {
		return err
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	d := localDocker{run: execTool}
	if args[0] == "kafka-source" {
		report, err := readLiveLedger(cfg.Ledger, boot)
		if err != nil {
			return err
		}
		_, digest, err := faultReceipt(cfg.SourceReceipt, func() (sourceKafkaReceipt, error) {
			return observeSourceKafka(ctx, d, cfg, sha, report.Header, now)
		})
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(map[string]string{"source_kafka_sha256": digest})
	}
	in, err := loadRestoreInputs(cfg.RestoreConfig, boot)
	if err != nil {
		return err
	}
	if cfg.Ledger != in.fault.Ledger || cfg.Source.Volume == in.fault.Resources.SourceVolume ||
		cfg.Source.Volume == in.fault.Resources.TargetVolume || cfg.Target.Volume == in.fault.Resources.SourceVolume ||
		cfg.Target.Volume == in.fault.Resources.TargetVolume {
		return errors.New("Kafka restore input collides with PostgreSQL storage or the sealed ledger")
	}
	var restored restoreReceipt
	restoredSHA, err := readFaultJSON(filepath.Join(in.config.Evidence, "restore.json"), &restored)
	if err != nil {
		return err
	}
	if restoredSHA != *restoreDigest || restored.Schema != "j7.isolated-restore.v1" ||
		restored.BootID != boot || restored.LossSHA != in.config.LossSHA || restored.Target != in.header.Target ||
		restored.Source != in.header.Source || restored.ConfigSHA != in.configSHA ||
		restored.CompletedNS < in.loss.ArchiveSealNS || restored.CompletedNS-in.loss.T0NS > int64(restoreWindow) ||
		len(restored.Verdict.RetainedWorkflows) != 17 {
		return errors.New("fresh Kafka requires a completed isolated restore receipt")
	}
	var old sourceKafkaReceipt
	oldSHA, err := readFaultJSON(cfg.SourceReceipt, &old)
	if err != nil {
		return err
	}
	if oldSHA != *sourceDigest || old.Schema != "j7.source-kafka.v1" || old.ConfigSHA != sha ||
		old.BootID != boot || old.Authority != in.header.Source || !old.Broker.CopiesComplete ||
		old.Broker.AfterNS <= 0 || old.Broker.AfterNS > in.cut.StartedNS || old.Binding.ID != cfg.SourceID {
		return errors.New("original Kafka observation is not bound or was taken after the cut")
	}
	legacy := transport.ConsumerGeneration{}
	if !slices.Equal(old.Broker.Groups, []string{legacy.SchedulerID(), legacy.WorkerID()}) {
		return errors.New("source Kafka must contain only the registered legacy consumer groups")
	}
	for _, hold := range []dispatchAtCut{in.cohorts.D1, in.cohorts.D2} {
		if err := verifySourceDispatch(old.Broker, hold); err != nil {
			return err
		}
	}
	trace, err := loadTraceArtifact(filepath.Join(in.config.Evidence, "engine-pre-write.json"), restored.SnapshotSHA)
	if err != nil {
		return err
	}
	if err := checkKafkaPrewrite(trace); err != nil {
		return err
	}
	var consumers []renderedConsumer
	consumerSHA, err := readFaultJSON(cfg.TargetConsumers, &consumers)
	if err != nil {
		return err
	}
	if err := verifyGenerationRender(consumers, cfg.Target.runtimeEndpoint()); err != nil {
		return err
	}
	binary, err := os.Executable()
	if err != nil {
		return err
	}
	binary, err = filepath.EvalSymlinks(binary)
	if err != nil {
		return err
	}
	isolated := restoreDocker{localDocker: d, binary: binary}
	if err := isolated.verifyIsolation(ctx, in, filepath.Join(in.config.Evidence, "replay-files.json"), &restored, true); err != nil {
		return fmt.Errorf("fresh Kafka requires the live target writer gate to remain closed: %w", err)
	}
	_, digest, err := faultReceipt(cfg.FreshReceipt, func() (freshKafkaReceipt, error) {
		return replaceKafka(ctx, d, cfg, in, old, sha, oldSHA, restoredSHA, consumerSHA, now, restored.CompletedNS)
	})
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(map[string]string{"fresh_kafka_sha256": digest})
}

func replaceKafka(ctx context.Context, d localDocker, cfg kafkaConfig, in restoreInputs, old sourceKafkaReceipt,
	cfgSHA, srcSHA, restoredSHA, consumerSHA string, now restoreledger.Clock, restoredNS int64) (freshKafkaReceipt, error) {
	r := freshKafkaReceipt{Schema: "j7.fresh-kafka.v1", ConfigSHA: cfgSHA, BootID: in.header.BootID,
		Authority: in.header.Target, SourceSHA: srcSHA, RestoreSHA: restoredSHA, T0NS: in.loss.T0NS, TargetConsumersSHA: consumerSHA}
	at, err := now()
	if err != nil {
		return r, err
	}
	if at < restoredNS {
		return r, errors.New("Kafka replacement precedes isolated restore completion")
	}
	ctx, cancel, err := phaseDeadline(ctx, at, in.loss.T0NS, 300*time.Second)
	if err != nil {
		return r, err
	}
	defer cancel()
	if err := d.retireKafka(ctx, cfg, old, in.header, now, &r); err != nil {
		return r, err
	}
	if err := d.launchFreshKafka(ctx, cfg, old.Binding, in.header, now, &r); err != nil {
		return r, err
	}
	if err := verifyFreshKafka(old.Broker, r.Broker); err != nil {
		return r, err
	}
	r.CompletedNS, err = now()
	if err == nil && (r.CompletedNS-in.loss.T0NS > int64(300*time.Second) || ctx.Err() != nil) {
		err = errors.New("fresh Kafka admission missed the endpoint-ready gate")
	}
	return r, err
}

func verifySourceDispatch(s kafkaSnapshot, want dispatchAtCut) error {
	for _, e := range s.Events {
		if e.EventID == want.EventID && e.Topic == want.Topic && e.TopicID == want.TopicID &&
			e.Partition == want.Partition && e.Offset == want.Offset && e.Kind == "attempt.dispatch" && sameAttempt(e.Attempt, want.Attempt) {
			return nil
		}
	}
	return fmt.Errorf("source Kafka evidence lacks original dispatch for %s", want.Attempt.WorkflowID)
}

func sameAttempt(got *eventAttempt, want attemptAtCut) bool {
	return got != nil && got.WorkflowID == want.WorkflowID && got.NodeID == want.NodeID &&
		got.Iteration == want.Iteration && got.Attempt == want.Attempt
}

func checkKafkaPrewrite(trace invariants.Trace) error {
	if err := invariants.SnapshotIntegrity(trace); err != nil {
		return err
	}
	old := transport.ConsumerGeneration{}
	target, _ := transport.ParseConsumerGeneration(transport.J7RestoreGeneration)
	for _, row := range trace.Inbox {
		if row.ConsumerID == target.SchedulerID() || row.ConsumerID == target.WorkerID() {
			return errors.New("CONSUMER_GENERATION_MISMATCH: restored database already contains target inbox rows")
		}
	}
	found := map[string]bool{}
	for _, row := range trace.ConsumerOffsets {
		switch row.ConsumerID {
		case target.SchedulerID(), target.WorkerID():
			return errors.New("CONSUMER_GENERATION_MISMATCH: restored database already contains target offsets")
		case old.SchedulerID():
			found[old.SchedulerID()] = found[old.SchedulerID()] || (row.Topic == state.EventTopic && row.NextOffset > 0)
		case old.WorkerID():
			found[old.WorkerID()] = found[old.WorkerID()] || (row.Topic == state.TaskTopic && row.NextOffset > 0)
		}
	}
	if !found[old.SchedulerID()] || !found[old.WorkerID()] {
		return errors.New("pre-write snapshot must preserve positive source offsets for both groups")
	}
	return nil
}
