package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

type sampledContainer struct {
	ID           string
	RestartCount int
	State        containerState
	Config       struct{ Labels map[string]string }
	HostConfig   struct{ Memory, NanoCpus int64 }
}
type sampledPod struct {
	Metadata struct {
		Name   string
		Labels map[string]string
	}
	Spec struct {
		Containers []struct {
			Resources struct{ Limits map[string]string }
		}
	}
	Status struct {
		ContainerStatuses []struct {
			ContainerID  string
			RestartCount int
			State        struct {
				Running    *struct{}
				Terminated *struct {
					ExitCode int
					Reason   string
				}
			}
		}
	}
}

func (d *liveDrill) sample(ctx context.Context) (resourceSample, error) {
	var r resourceSample
	roles := map[string]string{}
	if err := d.sampleContainers(ctx, roles, &r); err != nil {
		return r, err
	}
	if err := d.samplePods(ctx, roles, &r); err != nil {
		return r, err
	}
	cgroups, err := findControllerCgroups("/sys/fs/cgroup", roles)
	if err != nil {
		return r, err
	}
	phase := d.depPhase.Load()
	if phase > 0 {
		r.SinkSessions, err = countPGSessions(ctx, d.db.sinkAdmin.local(d.db.sink, sinkDatabase, d.db.ca, true), d.h.plan.Work.SinkConnections)
		if err != nil {
			return r, err
		}
	}
	switch phase {
	case 1:
		r.EngineSessions, err = countPGSessions(ctx, d.db.sourceAdmin.local(d.db.source, engineDatabase, d.db.ca, true), d.h.plan.Work.EngineConnections)
	case 3:
		for id, role := range roles {
			if role != targetRole {
				continue
			}
			raw, queryErr := d.h.docker(ctx, "container", "exec", "--user=postgres", id, "psql", "-X", "-qAt", "--host="+targetSocket,
				"--username="+observerRole, "--dbname="+engineDatabase, "-c", "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database()")
			if queryErr != nil {
				return r, errors.New("restricted restore observer did not expose connection usage")
			}
			if _, err = fmt.Sscan(strings.TrimSpace(string(raw)), &r.EngineSessions); err != nil {
				return r, err
			}
		}
	case 5:
		r.EngineSessions, err = countPGSessions(ctx, d.db.observer.local(d.db.target, engineDatabase, d.db.ca, true), d.h.plan.Work.EngineConnections)
	}
	if err != nil {
		return r, err
	}
	stats, err := hostResourceSample(ctx, d.h, cgroups, r.EngineSessions, r.SinkSessions)
	stats.OOM += r.OOM
	stats.UnexpectedKills = r.UnexpectedKills
	return stats, err
}
func countPGSessions(ctx context.Context, dsn string, want int) (int, error) {
	var maxConnections, count int
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return 0, errors.New("active PostgreSQL session inventory is unavailable")
	}
	defer c.Close(ctx)
	err = c.QueryRow(ctx, `SELECT current_setting('max_connections')::integer,
		(SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend')`).Scan(&maxConnections, &count)
	if err != nil || maxConnections != want {
		return 0, errors.New("effective PostgreSQL connection ceiling differs from the sealed limits")
	}
	return count, nil
}
func (d *liveDrill) sampleContainers(ctx context.Context, active map[string]string, r *resourceSample) error {
	ids, err := d.h.ownedResources(ctx, "container", d.id)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	cmd := append([]string{"container", "inspect"}, ids...)
	raw, err := d.h.docker(ctx, cmd...)
	if err != nil {
		// Retirement may remove a resource while we query. A second inventory
		// is required to distinguish intentional deletion from inspect failure.
		next, retry := d.h.ownedResources(ctx, "container", d.id)
		if retry != nil || len(next) >= len(ids) {
			return errors.New("could not inspect owned dependency containers")
		}
		if len(next) == 0 {
			return nil
		}
		raw, err = d.h.docker(ctx, append([]string{"container", "inspect"}, next...)...)
		if err != nil {
			return err
		}
	}
	var rows []sampledContainer
	if err := json.Unmarshal(raw, &rows); err != nil {
		return err
	}
	for _, c := range rows {
		role := c.Config.Labels[roleLabel]
		switch role {
		case "target-backup-stage", "pg-tools":
		case sourceKafkaRole, targetKafkaRole, "sink-db", sourceRole, targetRole:
			limit := "engine-db"
			if role == "sink-db" {
				limit = "sink-db"
			}
			if role == sourceKafkaRole || role == targetKafkaRole {
				limit = "kafka"
			}
			q := d.h.plan.Work.Roles[limit]
			if c.HostConfig.Memory != q.Bytes || c.HostConfig.NanoCpus != int64(q.MilliCPU)*1_000_000 {
				return errors.New("owned container resource limits drifted")
			}
		default:
			return errors.New("unregistered J7 container role")
		}
		if c.State.OOMKilled {
			r.OOM++
		}
		if c.RestartCount != 0 || (c.State.Status == "exited" && !(role == "target-backup-stage" && c.State.ExitCode == 0) &&
			!(role == sourceRole && d.depPhase.Load() >= 2 && c.State.ExitCode == 137) &&
			!(role == targetRole && d.depPhase.Load() == 4 && c.State.ExitCode == 0) &&
			!(role == sourceKafkaRole && d.depPhase.Load() >= 3 && (c.State.ExitCode == 0 || c.State.ExitCode == 143))) {
			r.UnexpectedKills++
		}
		if c.State.Running {
			active[c.ID] = role
		}
	}
	return nil
}
func (d *liveDrill) samplePods(ctx context.Context, active map[string]string, r *resourceSample) error {
	raw, err := d.h.kubectl(ctx, "get", "pods", "-A", "--field-selector=metadata.namespace="+kubeNamespace(d.id), "-o", "json")
	if err != nil {
		return err
	}
	var list struct{ Items []sampledPod }
	if err := json.Unmarshal(raw, &list); err != nil {
		return err
	}
	for _, pod := range list.Items {
		role := pod.Metadata.Labels["j7-role"]
		quota := d.h.plan.Work.Roles[role]
		switch role {
		case "client":
			quota.MilliCPU, quota.Bytes = 100, 32<<20
		case "api", "worker-control":
			quota.MilliCPU, quota.Bytes = 250, 128<<20
		}
		if quota.Bytes == 0 || len(pod.Spec.Containers) != 1 {
			return errors.New("J7 pod role lacks an explicit quota")
		}
		actual := pod.Spec.Containers[0].Resources.Limits
		// Kubernetes canonicalizes byte quantities (for example 128Mi).
		memory, err := memoryBytes(actual["memory"])
		if err != nil || memory != quota.Bytes || actual["cpu"] != fmt.Sprintf("%dm", quota.MilliCPU) {
			return errors.New("effective Kubernetes container resources drifted")
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.State.Terminated != nil && status.State.Terminated.Reason == "OOMKilled" {
				r.OOM++
			}
			if status.RestartCount != 0 {
				r.UnexpectedKills++
			}
			if status.State.Terminated != nil && status.State.Terminated.ExitCode != 0 &&
				!(d.freezing.Load() && strings.HasPrefix(pod.Metadata.Name, "source-")) {
				r.UnexpectedKills++
			}
			if status.State.Running != nil {
				id, ok := strings.CutPrefix(status.ContainerID, "containerd://")
				if !ok || !dockerID.MatchString(id) {
					return errors.New("running fixture pod lacks a containerd identity")
				}
				active[id] = role
			}
		}
	}
	return nil
}
func memoryBytes(text string) (int64, error) {
	var n int64
	suffix := int64(1)
	switch {
	case strings.HasSuffix(text, "Mi"):
		text = strings.TrimSuffix(text, "Mi")
		suffix = 1 << 20
	case strings.HasSuffix(text, "Gi"):
		text = strings.TrimSuffix(text, "Gi")
		suffix = 1 << 30
	}
	if _, err := fmt.Sscan(text, &n); err != nil || n < 0 || n > 1<<50 {
		return 0, errors.New("invalid fixture memory limit")
	}
	return n * suffix, nil
}
