package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"

	"durable-agent-execution-engine/internal/restoreledger"
)

type controllerDependency struct {
	Role     string `json:"role"`
	Volume   string `json:"volume"`
	Endpoint string `json:"dns_alias"`
	HostPort int    `json:"host_port"`
	Image    string `json:"image"`
	Quota    quota  `json:"quota"`
}

func (h fixtureHost) dependencies(drill string) []controllerDependency {
	name := h.cluster + "-" + drill
	work := h.plan.Work.Roles
	return []controllerDependency{
		{sourceRole, name + "-source-db", name + "-source-db", 15432, h.plan.Lock.Postgres, work["engine-db"]},
		{targetRole, name + "-target-db", name + "-target-db", 25432, h.plan.Lock.Postgres, work["engine-db"]},
		{"sink-db", name + "-sink-db", name + "-sink-db", 35432, h.plan.Lock.Postgres, work["sink-db"]},
		{sourceKafkaRole, name + "-source-kafka", name + "-source-kafka", 19093, h.plan.Lock.Kafka, work["kafka"]},
		{targetKafkaRole, name + "-target-kafka", name + "-target-kafka", 19094, h.plan.Lock.Kafka, work["kafka"]},
	}
}

func (h fixtureHost) dependencyLabels(drill, role string) map[string]string {
	return map[string]string{roleLabel: role, "j7.restore.campaign": h.campaign, "j7.restore.drill": drill,
		"j7.restore.run": h.runID, "j7.restore.attempt": "1"}
}

func (h fixtureHost) postgresCommand(drill string, d controllerDependency) ([]string, error) {
	if !validControllerDrill(drill) || (d.Role != sourceRole && d.Role != "sink-db") {
		return nil, errors.New("only source engine and sink PostgreSQL may use the normal startup command")
	}
	secure := filepath.Join(h.private, drill+"-private", d.Role)
	c := []string{"container", "create", "--pull=never", "--name=" + d.Endpoint,
		"--network=kind", "--network-alias=" + d.Endpoint, "--restart=no",
		"--cpus=" + strconv.FormatFloat(float64(d.Quota.MilliCPU)/1000, 'f', -1, 64),
		"--memory=" + strconv.FormatInt(d.Quota.Bytes, 10),
		"--publish=127.0.0.1:" + strconv.Itoa(d.HostPort) + ":5432",
		"--env-file=" + filepath.Join(secure, "postgres.env"),
		"--env=PGDATA=/var/lib/postgresql/18/docker",
		"--mount=type=volume,source=" + d.Volume + ",target=/var/lib/postgresql",
		bindRO(filepath.Join(secure, "tls"), "/j7/tls")}
	c = append(c, labelArgs(h.dependencyLabels(drill, d.Role))...)
	cap := h.plan.Work.SinkConnections
	if d.Role == sourceRole {
		cap = h.plan.Work.EngineConnections
		c = append(c, "--mount=type=bind,src="+filepath.Join(h.private, drill, "media", "archive")+",dst="+archiveDest,
			bindRO(filepath.Join(h.private, "j7-media"), "/j7/bin/j7-media"))
	}
	c = append(c, d.Image, "postgres", "-c", "max_connections="+strconv.Itoa(cap),
		"-c", "ssl=on", "-c", "ssl_cert_file=/j7/tls/server.crt", "-c", "ssl_key_file=/j7/tls/server.key",
		"-c", "hba_file=/j7/tls/pg_hba.conf")
	if d.Role == sourceRole {
		c = append(c, "-c", "wal_level=replica", "-c", "archive_mode=on", "-c",
			"archive_command=/j7/bin/j7-media archive /j7/archive %p %f")
	}
	return c, nil
}

func (h fixtureHost) sourceKafkaCommand(drill string, d controllerDependency, clusterID string) ([]string, error) {
	p := kafkaPlacement{Volume: d.Volume, Alias: d.Endpoint, Port: d.HostPort}
	if !validControllerDrill(drill) || d.Role != sourceKafkaRole || !kafkaNames(p) || clusterID == "" {
		return nil, errors.New("source Kafka requires sealed placement and a fresh cluster identity")
	}
	args := []string{"container", "create", "--pull=never", "--name=" + p.Alias, "--network=kind",
		"--network-alias=" + p.Alias, "--restart=no", "--cpus=1", "--memory=1536m",
		"--publish=" + p.hostEndpoint() + ":" + strconv.Itoa(kafkaHostPort),
		"--mount=type=volume,src=" + p.Volume + ",dst=" + kafkaData}
	args = append(args, labelArgs(h.dependencyLabels(drill, sourceKafkaRole))...)
	for _, env := range kafkaEnv(p, clusterID) {
		args = append(args, "--env="+env)
	}
	return append(args, d.Image), nil
}

// These commands use the existing stage implementations. Output becomes
// evidence only after the original stage has returned its own binding.
type controllerStage struct {
	Name string   `json:"name"`
	Args []string `json:"args"`
}

type stageFiles struct {
	Root                                  string
	ConfigRoot                            string
	Drill                                 string
	PGBin                                 string
	Source, Restore, Fresh, Watch, Engine string
	OpenNS                                int64
}

func (f stageFiles) config(name string) string {
	root := f.ConfigRoot
	if root == "" {
		root = filepath.Join(f.Root, "private")
	}
	return filepath.Join(root, name+".json")
}

func (f stageFiles) mediaStage(name string) (controllerStage, error) {
	cfg := func(group string) []string {
		return []string{name, "--config", f.config(group)}
	}
	stage := controllerStage{Name: name}
	switch name {
	case "prepare":
		if !validControllerDrill(f.Drill) || !filepath.IsAbs(f.PGBin) {
			return stage, errors.New("J7 backup needs the sealed drill and PostgreSQL tooling")
		}
		stage.Args = []string{"prepare", "--pg-bin", f.PGBin, "--source-service", "j7-source",
			"--backup-service", "j7-backup", "--backup", filepath.Join(f.Root, "media", "base"),
			"--archive", filepath.Join(f.Root, "media", "archive"), "--drill", f.Drill}
	case "source-preflight", "cut", "source-loss":
		stage.Args = cfg("fault")
	case "orphan-source":
		stage.Args = cfg("orphan")
	case "orphan-rebind", "verify-orphan":
		stage.Args = append(cfg("orphan"), "--restore-sha256", f.Restore)
	case "restore":
		stage.Args = cfg("restore")
	case "kafka-source":
		stage.Args = cfg("kafka")
	case "fresh-kafka":
		stage.Args = append(cfg("kafka"), "--source-sha256", f.Source, "--restore-sha256", f.Restore)
	case "watch-redispatch":
		if f.OpenNS <= 0 {
			return stage, errors.New("J7 redispatch watch requires the observed scheduler opening")
		}
		stage.Args = append(cfg("kafka"), "--fresh-sha256", f.Fresh, "--scheduler-open-ns", strconv.FormatInt(f.OpenNS, 10),
			"--watch-receipt", filepath.Join(f.Root, "redispatch-watch.json"))
	case "verify-redispatch":
		stage.Args = append(cfg("kafka"), "--fresh-sha256", f.Fresh, "--watch-receipt", filepath.Join(f.Root, "redispatch-watch.json"),
			"--watch-sha256", f.Watch, "--engine-snapshot", filepath.Join(f.Root, "engine-final.json"), "--engine-sha256", f.Engine,
			"--receipt", filepath.Join(f.Root, "redispatch.json"))
	case "quiet-start", "quiet-end":
		stage.Args = []string{"observe-quiet", "--config", f.config("quiet"),
			"--phase", strings.TrimPrefix(name, "quiet-"), "--restore-sha256", f.Restore, "--fresh-sha256", f.Fresh}
	default:
		return stage, fmt.Errorf("unrecognized J7 media stage %s", name)
	}
	for i, arg := range stage.Args {
		if strings.HasSuffix(arg, "-sha256") && (i+1 >= len(stage.Args) || !restoreledger.IsSHA256(stage.Args[i+1])) {
			return stage, fmt.Errorf("J7 %s lacks a bound upstream digest", name)
		}
	}
	return stage, nil
}

func invokeControllerStage(ctx context.Context, files stageFiles, name string, out, diagnostics io.Writer) error {
	stage, err := files.mediaStage(name)
	if err != nil {
		return err
	}
	return run(ctx, stage.Args, out, diagnostics)
}
