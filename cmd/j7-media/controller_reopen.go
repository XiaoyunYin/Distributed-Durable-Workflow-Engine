package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"durable-agent-execution-engine/internal/restoreledger"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const targetSecurityDir = "target-tls"

func (h fixtureHost) reopenTargetCommand(drill, config string, d controllerDependency, restored restoreReceipt) ([]string, error) {
	if !validControllerDrill(drill) || d.Role != targetRole || restored.Volume.Name != d.Volume ||
		!volumeName.MatchString(d.Volume) || !dockerID.MatchString(restored.TargetID) ||
		!filepath.IsAbs(config) {
		return nil, errors.New("target reopening requires the verified isolated target and owned TLS configuration")
	}
	args := []string{"container", "create", "--pull=never", "--name=" + d.Endpoint, "--network=kind",
		"--network-alias=" + d.Endpoint, "--restart=no", "--read-only", "--no-healthcheck", "--memory=1g", "--cpus=1",
		"--tmpfs=/tmp:rw,nosuid,noexec,mode=1777,size=16m",
		"--publish=127.0.0.1:" + strconv.Itoa(d.HostPort) + ":5432",
		"--mount=type=volume,src=" + d.Volume + ",dst=" + targetVolumeDir + ",volume-nocopy",
		bindRO(config, "/j7/tls"), "--user=postgres", "--entrypoint=postgres"}
	args = append(args, labelArgs(h.dependencyLabels(drill, targetRole))...)
	return append(args, d.Image, "-D", targetData, "-c", "listen_addresses=*",
		"-c", "restore_command=", "-c", "archive_mode=off",
		"-c", "ssl=on", "-c", "ssl_cert_file=/j7/tls/server.crt", "-c", "ssl_key_file=/j7/tls/server.key",
		"-c", "hba_file=/j7/tls/pg_hba.conf", "-c", "default_transaction_read_only=off"), nil
}

func (h fixtureHost) restartVerifiedTarget(ctx context.Context, drill, config string, d controllerDependency,
	r restoreReceipt, admittedImage string) (string, error) {
	args, err := h.reopenTargetCommand(drill, config, d, r)
	if err != nil {
		return "", err
	}
	docker := localDocker{run: h.run}
	if !r.State.Restricted || !r.State.SocketOnly || !r.State.Recovered || r.CompletedNS <= 0 {
		return "", errors.New("target reopening requires completed socket-only recovery")
	}
	prior, err := docker.container(ctx, r.TargetID)
	if err != nil {
		return "", err
	}
	if !runningSource(prior.State) || prior.RestartCount != 0 || !matchesLabels(prior.Config.Labels, h.dependencyLabels(drill, targetRole)) ||
		prior.Image != admittedImage {
		return "", errors.New("isolated target identity or process changed after verification")
	}
	volume, err := docker.volume(ctx, d.Volume)
	if err != nil || !sameVolume(volume, r.Volume) {
		return "", errors.New("target volume changed since the initial restore")
	}
	refs, err := docker.refs(ctx, d.Volume)
	if err != nil || !slices.Equal(refs, []string{r.TargetID}) {
		return "", errors.New("isolated container is not the only holder of the recovered volume")
	}
	for _, action := range [][]string{{"container", "stop", "--timeout=15", r.TargetID}, {"container", "rm", r.TargetID}} {
		out, err := h.docker(ctx, action...)
		if err != nil || strings.TrimSpace(string(out)) != r.TargetID {
			return "", errors.New("isolated target could not stop and detach cleanly")
		}
	}
	refs, err = docker.refs(ctx, d.Volume)
	if err != nil || len(refs) != 0 {
		return "", errors.New("reopened volume must have zero references before starting the networked target")
	}
	response, err := h.docker(ctx, args...)
	id := strings.TrimSpace(string(response))
	if err != nil || !dockerID.MatchString(id) || r.TargetID == id {
		return "", errors.New("Docker did not create the distinct networked target")
	}
	response, err = h.docker(ctx, "container", "start", id)
	if err != nil || strings.TrimSpace(string(response)) != id {
		return "", errors.New("Docker did not start the networked target")
	}
	refs, err = docker.refs(ctx, d.Volume)
	if err != nil || !slices.Equal(refs, []string{id}) {
		return "", errors.New("networked target is not the exclusive volume holder")
	}
	raw, err := h.docker(ctx, "container", "inspect", id)
	if err != nil {
		return "", err
	}
	var live isolatedContainer
	if err := decodeInspection(raw, &live); err != nil {
		return "", err
	}
	if live.ID != id || live.Image != admittedImage || !runningSource(live.State) || live.RestartCount != 0 ||
		live.HostConfig.NetworkMode != "kind" || len(live.NetworkSettings.Networks) != 1 || live.NetworkSettings.Networks["kind"] == nil ||
		!live.HostConfig.ReadonlyRootfs || len(live.HostConfig.PortBindings) != 1 ||
		!matchesLabels(live.Config.Labels, h.dependencyLabels(drill, targetRole)) || len(live.Mounts) != 2 {
		return "", errors.New("opened target does not match the sealed local process, mounts and network")
	}
	for _, mount := range live.Mounts {
		if !(mount.Type == "volume" && mount.Name == d.Volume && mount.Source == r.Volume.Mountpoint && mount.Destination == targetVolumeDir && mount.RW) &&
			!(mount.Type == "bind" && mount.Source == config && mount.Destination == "/j7/tls" && !mount.RW) {
			return "", errors.New("opened target mounts unapproved media")
		}
	}
	return id, nil
}

type endpointProbes struct {
	oldDenied     func(context.Context) error
	foreignDenied func(context.Context) error
	observer      func(context.Context) (uint64, uint32, error)
}

func observeReopenedEndpoint(ctx context.Context, now restoreledger.Clock, p endpointProbes) (endpointObservation, error) {
	var o endpointObservation
	if err := p.oldDenied(ctx); err != nil {
		return o, err
	}
	var err error
	o.OldDeniedNS, err = now()
	if err != nil {
		return o, err
	}
	// A successful read on the target before and after the foreign attempt
	// distinguishes authorization denial from a transient outage.
	system, timeline, err := p.observer(ctx)
	if err != nil {
		return o, err
	}
	if err := p.foreignDenied(ctx); err != nil {
		return o, err
	}
	o.ForeignDeniedNS, err = now()
	if err != nil {
		return o, err
	}
	o.SystemID, o.Timeline, err = p.observer(ctx)
	if err != nil {
		return o, err
	}
	if system != o.SystemID || timeline != o.Timeline {
		return o, errors.New("target identity changed across the foreign credential denial")
	}
	o.OpenNS, err = now()
	return o, err
}

func liveEndpointProbes(oldPort int, foreignDSN, observerDSN string) endpointProbes {
	return endpointProbes{
		oldDenied: func(ctx context.Context) error {
			dial := net.Dialer{Timeout: time.Second}
			conn, err := dial.DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(oldPort)))
			if err == nil {
				conn.Close()
			}
			if !errors.Is(err, syscall.ECONNREFUSED) {
				return errors.New("old source PostgreSQL listener did not refuse the connection")
			}
			return nil
		},
		foreignDenied: func(ctx context.Context) error {
			conn, err := pgx.Connect(ctx, foreignDSN)
			if err == nil {
				conn.Close(ctx)
			}
			var sql *pgconn.PgError
			if !errors.As(err, &sql) || sql.Code != "28000" {
				return errors.New("target did not reject the foreign identity at pg_hba")
			}
			return nil
		},
		observer: func(ctx context.Context) (uint64, uint32, error) {
			var system uint64
			var timeline uint32
			var ready bool
			conn, err := pgx.Connect(ctx, observerDSN)
			if err != nil {
				return 0, 0, errors.New("verified TLS target observer could not connect")
			}
			defer conn.Close(ctx)
			err = conn.QueryRow(ctx, `SELECT (SELECT system_identifier FROM pg_control_system()),
				(SELECT timeline_id FROM pg_control_checkpoint()), NOT pg_is_in_recovery()
				AND current_setting('transaction_read_only')='on' AND current_user='j7_observer'`).Scan(&system, &timeline, &ready)
			secure, tlsOK := conn.PgConn().Conn().(*tls.Conn)
			if err != nil || !ready || !tlsOK || !secure.ConnectionState().HandshakeComplete {
				return 0, 0, fmt.Errorf("target observer did not prove PostgreSQL identity over TLS")
			}
			return system, timeline, nil
		},
	}
}
