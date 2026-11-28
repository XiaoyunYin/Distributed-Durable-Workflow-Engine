package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/restoreledger"
)

const targetRole = "target-engine-db"

type restoreDocker struct {
	localDocker
	binary string
	logs   localDocker
}

func targetLabels(h restoreledger.Header) map[string]string {
	labels := faultLabels(h)
	labels[roleLabel] = targetRole
	return labels
}

func (d restoreDocker) startIsolated(ctx context.Context, in restoreInputs, manifest string,
	now restoreledger.Clock, r *restoreReceipt) error {
	f := in.fault.Resources
	if err := d.checkVolumeSet(ctx, f, nil, true); err != nil {
		return err
	}
	out, err := d.command(ctx, "container", "ls", "--all", "--no-trunc", "--filter", "id="+f.Container, "--format", "{{.ID}}")
	if err != nil || len(strings.Fields(string(out))) != 0 {
		return errors.New("source container must be absent before allocating a target")
	}
	labels := targetLabels(in.header)
	args := append([]string{"volume", "create", "--driver=local"}, labelArgs(labels)...)
	out, err = d.command(ctx, append(args, f.TargetVolume)...)
	if err != nil || strings.TrimSpace(string(out)) != f.TargetVolume {
		return errors.New("target volume creation failed")
	}
	r.VolumeCreatedNS, err = now()
	if err != nil {
		return err
	}
	r.Volume, err = d.volume(ctx, f.TargetVolume)
	if err != nil {
		return err
	}
	if r.Volume.Name != f.TargetVolume || r.Volume.Driver != "local" || r.Volume.Scope != "local" ||
		len(r.Volume.Options) != 0 || !matchesLabels(r.Volume.Labels, labels) ||
		r.Volume.Mountpoint == in.binding.Volume.Mountpoint || r.Volume.CreatedAt == "" {
		return errors.New("target volume does not match the fresh named local resource")
	}
	labels[roleLabel] = "target-backup-stage"
	r.StagingID, err = d.createContainer(ctx, in, manifest, labels, f.TargetVolume+"-stage", true)
	if err != nil {
		return err
	}
	out, err = d.logs.command(ctx, "container", "start", "--attach", r.StagingID)
	r.CopyLog = string(out)
	if err != nil || !strings.Contains(r.CopyLog, "backup successfully verified") {
		return errors.New("target backup copy did not pass physical verification")
	}
	// Remove only the exited staging helper. Its volume is named and kept.
	out, err = d.command(ctx, "container", "rm", r.StagingID)
	if err != nil || strings.TrimSpace(string(out)) != r.StagingID {
		return errors.New("target staging helper did not exit and detach")
	}
	labels[roleLabel] = targetRole
	r.TargetID, err = d.createContainer(ctx, in, manifest, labels, f.TargetVolume+"-pg", false)
	if err != nil {
		return err
	}
	r.Endpoint = "docker-exec:" + r.TargetID + ":" + targetSocket
	if err := d.verifyIsolation(ctx, in, manifest, r, false); err != nil {
		return err
	}
	out, err = d.command(ctx, "container", "start", r.TargetID)
	if err != nil || strings.TrimSpace(string(out)) != r.TargetID {
		return errors.New("isolated target did not start")
	}
	return d.verifyIsolation(ctx, in, manifest, r, true)
}

func labelArgs(labels map[string]string) []string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	args := make([]string, 0, len(keys))
	for _, key := range keys {
		args = append(args, "--label="+key+"="+labels[key])
	}
	return args
}

func bindRO(src, dest string) string {
	return "--mount=type=bind,src=" + src + ",dst=" + dest + ",readonly"
}

func (d restoreDocker) createContainer(ctx context.Context, in restoreInputs, manifest string,
	labels map[string]string, name string, staging bool) (string, error) {
	args := []string{"container", "create", "--pull=never", "--network=none",
		"--name=" + name, "--restart=no", "--read-only", "--no-healthcheck", "--memory=1g", "--cpus=1",
		"--tmpfs=/tmp:rw,nosuid,noexec,mode=1777,size=16m",
		"--mount=type=volume,src=" + in.fault.Resources.TargetVolume + ",dst=" + targetVolumeDir + ",volume-nocopy",
		bindRO(in.fault.Archive, archiveDest), bindRO(d.binary, targetBinary), bindRO(manifest, targetManifest)}
	args = append(args, labelArgs(labels)...)
	if staging {
		args = append(args, bindRO(in.fault.BackupDir, targetBackup), "--user=0:0",
			"--entrypoint="+targetBinary, in.binding.Image, "stage-target")
	} else {
		args = append(args, "--user=postgres", "--entrypoint=postgres", in.binding.Image,
			"-D", targetData)
	}
	out, err := d.command(ctx, args...)
	id := strings.TrimSpace(string(out))
	if err != nil || !dockerID.MatchString(id) || id == in.fault.Resources.Container {
		return "", errors.New("Docker did not create a distinct isolated target resource")
	}
	return id, nil
}

type isolatedContainer struct {
	ID           string
	Image        string
	Path         string
	Args         []string
	RestartCount int
	State        containerState
	Config       struct {
		User   string
		Labels map[string]string
	}
	HostConfig struct {
		NetworkMode    string
		AutoRemove     bool
		ReadonlyRootfs bool
		Privileged     bool
		Memory         int64
		NanoCpus       int64
		PortBindings   map[string][]any
		RestartPolicy  struct{ Name string }
	}
	NetworkSettings struct{ Networks map[string]any }
	Mounts          []dockerMount
}

func (d restoreDocker) verifyIsolation(ctx context.Context, in restoreInputs, manifest string,
	r *restoreReceipt, running bool) error {
	out, err := d.command(ctx, "container", "inspect", r.TargetID)
	if err != nil {
		return err
	}
	var target isolatedContainer
	if err := decodeInspection(out, &target); err != nil {
		return err
	}
	if err := checkIsolatedContainer(target, in, manifest, d.binary, r, running); err != nil {
		return err
	}
	refs, err := d.refs(ctx, in.fault.Resources.TargetVolume)
	if err != nil || !slices.Equal(refs, []string{r.TargetID}) {
		return errors.New("isolated target must hold the only reference to its volume")
	}
	return nil
}

func checkIsolatedContainer(c isolatedContainer, in restoreInputs, manifest, binary string, r *restoreReceipt, running bool) error {
	network := c.NetworkSettings.Networks
	if c.ID != r.TargetID || c.Image != in.binding.Image || c.Path != "postgres" ||
		!slices.Equal(c.Args, []string{"-D", targetData}) || c.Config.User != "postgres" ||
		!matchesLabels(c.Config.Labels, targetLabels(in.header)) || c.HostConfig.NetworkMode != "none" ||
		len(c.HostConfig.PortBindings) != 0 || len(network) > 1 ||
		(len(network) == 1 && network["none"] == nil) || c.HostConfig.Privileged ||
		!c.HostConfig.ReadonlyRootfs || c.HostConfig.AutoRemove || c.HostConfig.RestartPolicy.Name != "no" ||
		c.HostConfig.Memory != 1<<30 || c.HostConfig.NanoCpus != 1_000_000_000 || c.RestartCount != 0 ||
		(running && !runningSource(c.State)) || (!running && c.State.Status != "created") {
		return errors.New("target container has unexpected image, process, networking or isolation")
	}
	want := map[string]dockerMount{
		targetVolumeDir: {Type: "volume", Name: in.fault.Resources.TargetVolume, Source: r.Volume.Mountpoint,
			Destination: targetVolumeDir, Driver: "local", RW: true},
		archiveDest:    {Type: "bind", Source: in.fault.Archive, Destination: archiveDest},
		targetBinary:   {Type: "bind", Source: binary, Destination: targetBinary},
		targetManifest: {Type: "bind", Source: manifest, Destination: targetManifest},
	}
	if len(c.Mounts) != len(want) {
		return errors.New("target mount inventory is not isolated")
	}
	for _, m := range c.Mounts {
		if expected, exists := want[m.Destination]; !exists || expected != m {
			return fmt.Errorf("unexpected target mount at %s", m.Destination)
		}
		delete(want, m.Destination)
	}
	return nil
}

func (d restoreDocker) awaitReplay(ctx context.Context, c cutReceipt, now restoreledger.Clock, r *restoreReceipt) error {
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		raw, err := d.logs.command(ctx, "container", "logs", r.TargetID)
		if err != nil {
			return err
		}
		r.ReplayLogs = string(raw)
		if len(raw) > 1<<20 || strings.Contains(r.ReplayLogs, "j7-archive integrity error:") ||
			strings.Contains(r.ReplayLogs, "PANIC:") || strings.Contains(r.ReplayLogs, "FATAL:") {
			return errors.New("isolated replay logged a fatal error or oversized output")
		}
		if r.FirstReplayNS == 0 && strings.Contains(r.ReplayLogs, "redo starts at ") {
			r.FirstReplayNS, err = now()
			if err != nil {
				return err
			}
		}
		if r.FirstReplayNS != 0 && strings.Contains(r.ReplayLogs, "archive recovery complete") &&
			strings.Contains(r.ReplayLogs, "database system is ready to accept connections") {
			r.ReadyObservedNS, err = now()
			if err != nil {
				return err
			}
			return checkReplaySegments(r.ReplayLogs, c, r.WAL)
		}
		status, err := d.container(ctx, r.TargetID)
		if err != nil || !runningSource(status.State) {
			return errors.New("isolated target exited before archive recovery completed")
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for isolated archive recovery: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

func checkReplaySegments(logs string, c cutReceipt, proof walProof) error {
	start, startErr := parseLSN(proof.Start)
	end, endErr := parseLSN(proof.End)
	if startErr != nil || endErr != nil || end <= start || end%walSegmentBytes != 0 {
		return errors.New("invalid replay range in WAL verification")
	}
	names, err := segmentRange(c.PreSwitch.Timeline, start/walSegmentBytes, (end-1)/walSegmentBytes)
	if err != nil {
		return err
	}
	for _, name := range names {
		if !strings.Contains(logs, "j7-archive restored "+name+"\n") {
			return fmt.Errorf("missing target replay witness for sealed segment %s", name)
		}
	}
	if !strings.Contains(logs, "j7-archive absent "+proof.NextSegment+"\n") {
		return errors.New("target did not reach the first absent segment after C")
	}
	return nil
}

func (d restoreDocker) verifyIsolated(ctx context.Context, in restoreInputs, manifest string, r *restoreReceipt) error {
	if err := d.verifyIsolation(ctx, in, manifest, r, true); err != nil {
		return err
	}
	current, err := d.volume(ctx, in.fault.Resources.TargetVolume)
	if err != nil || !sameVolume(current, r.Volume) {
		return errors.New("target volume identity changed during replay")
	}
	refs, err := d.refs(ctx, in.fault.Resources.SourceVolume)
	if err != nil || len(refs) != 0 {
		return errors.New("source volume acquired references during restore")
	}
	volumes, err := d.volumes(ctx)
	if err != nil || slices.Contains(volumes, in.fault.Resources.SourceVolume) {
		return errors.New("source volume reappeared while target was isolated")
	}
	return nil
}
