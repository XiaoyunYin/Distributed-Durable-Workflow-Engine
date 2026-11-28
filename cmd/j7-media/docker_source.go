package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"durable-agent-execution-engine/internal/restoreledger"
)

const (
	roleLabel   = "j7.restore.role"
	sourceRole  = "source-engine-db"
	archiveDest = "/j7/archive"
)

var (
	dockerID   = regexp.MustCompile("^[a-f0-9]{64}$")
	volumeName = regexp.MustCompile("^j7-[a-z0-9][a-z0-9_-]{0,91}$")
)

type faultConfig struct {
	Container    string `json:"source_container_id"`
	SourceVolume string `json:"source_volume"`
	TargetVolume string `json:"target_volume"`
}

func (f faultConfig) validate() error {
	if !dockerID.MatchString(f.Container) || !volumeName.MatchString(f.SourceVolume) ||
		!volumeName.MatchString(f.TargetVolume) || f.SourceVolume == f.TargetVolume {
		return errors.New("preflight requires the exact source container ID and distinct J7 volume names")
	}
	return nil
}

func faultLabels(h restoreledger.Header) map[string]string {
	return map[string]string{
		roleLabel:             sourceRole,
		"j7.restore.campaign": h.Campaign,
		"j7.restore.drill":    h.Drill,
		"j7.restore.run":      strconv.FormatUint(h.RunID, 10),
		"j7.restore.attempt":  strconv.FormatUint(uint64(h.RunAttempt), 10),
	}
}

type dockerMount struct {
	Type        string
	Name        string
	Source      string
	Destination string
	Driver      string
	RW          bool
}

type containerState struct {
	Status     string
	Running    bool
	Restarting bool
	Paused     bool
	OOMKilled  bool
	Pid        int
	ExitCode   int
	StartedAt  string
	FinishedAt string
}

type sourceContainer struct {
	ID           string
	Image        string
	RestartCount int
	Config       struct{ Labels map[string]string }
	HostConfig   struct {
		AutoRemove    bool
		RestartPolicy struct{ Name string }
	}
	Mounts []dockerMount
	State  containerState
}

type sourceVolume struct {
	Name       string
	Driver     string
	Mountpoint string
	Scope      string
	Options    map[string]string
	Labels     map[string]string
	CreatedAt  string
}

type sourceBinding struct {
	Schema       string            `json:"schema"`
	BootID       string            `json:"boot_id"`
	Authority    string            `json:"authority"`
	BackupSHA256 string            `json:"backup_receipt_sha256"`
	ConfigSHA256 string            `json:"fault_config_sha256"`
	Run          faultConfig       `json:"resources"`
	Labels       map[string]string `json:"job_labels"`
	Image        string            `json:"container_image_id"`
	StartedAt    string            `json:"container_started_at"`
	Data         dockerMount       `json:"data_mount"`
	Archive      dockerMount       `json:"archive_mount"`
	Volume       sourceVolume      `json:"volume_inspect"`
	ObservedNS   int64             `json:"observed_ns"`
}

type localDocker struct{ run runTool }

// Always use the hosted runner's local daemon. No shell, force removal, prune,
// names in place of container IDs, or implicit remote Docker context.
func (d localDocker) command(ctx context.Context, args ...string) ([]byte, error) {
	return d.run(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
}

func (d localDocker) container(ctx context.Context, id string) (sourceContainer, error) {
	var c sourceContainer
	out, err := d.command(ctx, "container", "inspect", id)
	if err != nil {
		return c, fmt.Errorf("inspect J7 source container: %w", err)
	}
	if err := decodeInspection(out, &c); err != nil || c.ID != id {
		return c, errors.New("source container inspection did not return the sealed ID")
	}
	return c, nil
}

func (d localDocker) volume(ctx context.Context, name string) (sourceVolume, error) {
	var v sourceVolume
	out, err := d.command(ctx, "volume", "inspect", name)
	if err != nil {
		return v, fmt.Errorf("inspect J7 source volume: %w", err)
	}
	if err := decodeInspection(out, &v); err != nil || v.Name != name {
		return v, errors.New("source volume inspection did not return the sealed name")
	}
	return v, nil
}

func decodeInspection[T any](data []byte, to *T) error {
	var items []T
	if err := json.Unmarshal(data, &items); err != nil || len(items) != 1 {
		return errors.New("Docker inspection must return one object")
	}
	*to = items[0]
	return nil
}

func matchesLabels(got, want map[string]string) bool {
	for name, value := range want {
		if got[name] != value {
			return false
		}
	}
	return true
}

func (d localDocker) refs(ctx context.Context, volume string) ([]string, error) {
	out, err := d.command(ctx, "container", "ls", "--all", "--no-trunc",
		"--filter", "volume="+volume, "--format", "{{.ID}}")
	if err != nil {
		return nil, fmt.Errorf("list source volume references: %w", err)
	}
	refs := strings.Fields(string(out))
	for _, ref := range refs {
		if !dockerID.MatchString(ref) {
			return nil, errors.New("unexpected Docker container list output")
		}
	}
	return refs, nil
}

func (d localDocker) volumes(ctx context.Context) ([]string, error) {
	out, err := d.command(ctx, "volume", "ls", "--format", "{{.Name}}")
	if err != nil {
		return nil, fmt.Errorf("enumerate named Docker volumes: %w", err)
	}
	return strings.Fields(string(out)), nil
}

func (d localDocker) bind(ctx context.Context, s mediaSource, f faultConfig, h restoreledger.Header,
	source sourceState, backupSHA string) (sourceBinding, error) {
	b := sourceBinding{Schema: "j7.source-binding.v1", BootID: h.BootID, Authority: h.Source,
		Run: f, Labels: faultLabels(h), BackupSHA256: backupSHA}
	if err := f.validate(); err != nil {
		return b, err
	}
	c, err := d.container(ctx, f.Container)
	if err != nil {
		return b, err
	}
	v, err := d.volume(ctx, f.SourceVolume)
	if err != nil {
		return b, err
	}
	b.Volume, b.Image, b.StartedAt = v, c.Image, c.State.StartedAt
	b.Data, b.Archive, err = ownedMounts(c, v, f, b.Labels, source.DataDirectory, s.archive)
	if err != nil {
		return b, err
	}
	if !runningSource(c.State) || c.RestartCount != 0 {
		return b, errors.New("source has not been continuously running since preflight")
	}
	out, err := d.command(ctx, "container", "exec", f.Container, "cat", "/proc/1/comm")
	if err != nil || strings.TrimSpace(string(out)) != "postgres" {
		return b, errors.New("PostgreSQL postmaster must be PID 1 in the source container")
	}
	out, err = d.command(ctx, "container", "exec", f.Container, "realpath", source.DataDirectory)
	if err != nil || strings.TrimSpace(string(out)) != source.DataDirectory {
		return b, errors.New("PostgreSQL data directory must be canonical inside the source container")
	}
	if err := d.checkVolumeSet(ctx, f, []string{f.Container}, false); err != nil {
		return b, err
	}
	b.ObservedNS, err = s.now()
	return b, err
}

func ownedMounts(c sourceContainer, v sourceVolume, f faultConfig, labels map[string]string,
	dataDir, hostArchive string) (dockerMount, dockerMount, error) {
	var data, archive dockerMount
	if !matchesLabels(c.Config.Labels, labels) || !matchesLabels(v.Labels, labels) ||
		v.Driver != "local" || v.Scope != "local" || len(v.Options) != 0 || v.CreatedAt == "" ||
		!path.IsAbs(v.Mountpoint) || c.HostConfig.AutoRemove || c.HostConfig.RestartPolicy.Name != "no" ||
		!strings.HasPrefix(c.Image, "sha256:") || !dockerID.MatchString(strings.TrimPrefix(c.Image, "sha256:")) ||
		!path.IsAbs(dataDir) || path.Clean(dataDir) != dataDir {
		return data, archive, errors.New("source ownership, local volume, restart or PostgreSQL data settings mismatch")
	}
	for _, m := range c.Mounts {
		switch {
		case m.Name == f.SourceVolume:
			if data.Type != "" || m.Type != "volume" || m.Driver != "local" || !m.RW ||
				m.Source != v.Mountpoint || !pathContains(m.Destination, dataDir) ||
				m.Destination == "/" || pathContains(m.Destination, archiveDest) {
				return data, archive, errors.New("named source volume does not uniquely contain PostgreSQL data")
			}
			data = m
		case m.Destination == archiveDest:
			if archive.Type != "" || m.Type != "bind" || !m.RW || m.Source != hostArchive ||
				pathContains(m.Destination, dataDir) {
				return data, archive, errors.New("archive is not the independent writable source bind")
			}
			archive = m
		case pathContains(m.Destination, dataDir) || pathContains(dataDir, m.Destination) ||
			pathContains(m.Destination, archiveDest) || pathContains(archiveDest, m.Destination):
			return data, archive, errors.New("unsealed mount overlaps PostgreSQL data or archive")
		}
	}
	if data.Type == "" || archive.Type == "" || pathContains(data.Source, archive.Source) ||
		pathContains(archive.Source, data.Source) {
		return data, archive, errors.New("independent data and archive mounts are required")
	}
	return data, archive, nil
}

func pathContains(dir, file string) bool {
	return path.IsAbs(dir) && path.IsAbs(file) &&
		(path.Clean(dir) == path.Clean(file) || strings.HasPrefix(path.Clean(file), strings.TrimSuffix(path.Clean(dir), "/")+"/"))
}

func runningSource(s containerState) bool {
	return s.Status == "running" && s.Running && !s.Restarting && !s.Paused && s.Pid > 0 && s.StartedAt != ""
}

func killedSource(s containerState, startedAt string) bool {
	return s.Status == "exited" && !s.Running && !s.Restarting && !s.Paused && !s.OOMKilled &&
		s.Pid == 0 && s.ExitCode == 137 && s.StartedAt == startedAt &&
		s.FinishedAt != "" && s.FinishedAt != "0001-01-01T00:00:00Z"
}

func (d localDocker) checkVolumeSet(ctx context.Context, f faultConfig, expectedRefs []string, absent bool) error {
	refs, err := d.refs(ctx, f.SourceVolume)
	if err != nil {
		return err
	}
	volumes, err := d.volumes(ctx)
	if err != nil {
		return err
	}
	if !slices.Equal(refs, expectedRefs) || slices.Contains(volumes, f.SourceVolume) == absent ||
		slices.Contains(volumes, f.TargetVolume) {
		return errors.New("source volume references or source/target volume existence differ from the sealed drill")
	}
	return nil
}
