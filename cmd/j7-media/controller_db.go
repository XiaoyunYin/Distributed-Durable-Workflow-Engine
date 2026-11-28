package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/k6auth"
	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/migrations"
	"github.com/jackc/pgx/v5"
)

const (
	engineDatabase   = "j7_engine"
	sinkDatabase     = "j7_sink"
	sourceWriterRole = "j7_source_writer"
	targetWriterRole = "j7_target_writer"
	backupRole       = "j7_backup"
)

type dbLogin struct{ role, pass string }

func newDBLogin(role string) dbLogin { return dbLogin{role, rand.Text()} }
func (a dbLogin) dsn(host string, port int, database, ca string, readonly bool) string {
	query := url.Values{"sslmode": {"verify-full"}, "sslrootcert": {ca}, "connect_timeout": {"2"}}
	if readonly {
		query.Set("default_transaction_read_only", "on")
	}
	return (&url.URL{Scheme: "postgresql", User: url.UserPassword(a.role, a.pass),
		Host: net.JoinHostPort(host, strconv.Itoa(port)), Path: "/" + database, RawQuery: query.Encode()}).String()
}
func (a dbLogin) local(dep controllerDependency, database, ca string, readonly bool) string {
	return a.dsn("127.0.0.1", dep.HostPort, database, ca, readonly)
}

type drillDatabases struct {
	source, target, sink, broker, fresh                                                                                             controllerDependency
	ids                                                                                                                             map[string]string
	sourceAdmin, sinkAdmin, sinkService, sinkExport, sourceWriter, targetWriter, backup, observer, orphanObserver, reader, decision dbLogin
	ca, sourceService, sinkExportPath, sourceOrphanPath, targetOrphanPath, targetObserverPath                                       string
}

func initDrillDB(h fixtureHost, id string, p drillPaths) drillDatabases {
	d := h.dependencies(id)
	return drillDatabases{source: d[0], target: d[1], sink: d[2], broker: d[3], fresh: d[4], ids: map[string]string{},
		sourceAdmin: newDBLogin("j7_engine_admin"), sinkAdmin: newDBLogin("j7_sink_admin"),
		sinkService: newDBLogin("k6_sink_service"), sinkExport: newDBLogin("k6_sink_exporter"),
		sourceWriter: newDBLogin(sourceWriterRole), targetWriter: newDBLogin(targetWriterRole), backup: newDBLogin(backupRole),
		observer: newDBLogin(observerRole), orphanObserver: newDBLogin(orphanObserverRole),
		ca: p.cfg("ca.crt"), sourceService: p.cfg("pg_service.conf"), sinkExportPath: p.cfg("sink-export.dsn"),
		sourceOrphanPath: p.cfg("source-effect.dsn"), targetOrphanPath: p.cfg("target-effect.dsn"), targetObserverPath: p.cfg("target-observer.dsn")}
}
func secureWrite(path string, v []byte, mode fs.FileMode) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	_, err = f.Write(v)
	return errors.Join(err, f.Sync(), f.Close())
}
func (h fixtureHost) createOwnedContainer(ctx context.Context, drill string, d controllerDependency, args []string) (string, error) {
	docker := localDocker{run: h.run}
	names, err := docker.volumes(ctx)
	if err != nil || slices.Contains(names, d.Volume) {
		return "", errors.New("dependency requires new storage")
	}
	raw, err := h.docker(ctx, append(append([]string{"volume", "create", "--driver=local"}, labelArgs(h.dependencyLabels(drill, d.Role))...), d.Volume)...)
	if err != nil || strings.TrimSpace(string(raw)) != d.Volume {
		return "", errors.New("failed to allocate owned dependency volume")
	}
	if d.Role == sourceRole || d.Role == "sink-db" {
		account, err := user.Current()
		if err != nil {
			return "", err
		}
		owner := account.Uid + ":" + account.Gid
		for _, id := range []string{account.Uid, account.Gid} {
			if _, err := strconv.ParseUint(id, 10, 32); err != nil {
				return "", err
			}
		}
		if _, err := h.docker(ctx, "run", "--rm", "--pull=never", "--network=none", "--user=0:0",
			"--mount=type=volume,src="+d.Volume+",dst=/j7/database", "--entrypoint=chown", d.Image, owner, "/j7/database"); err != nil {
			return "", err
		}
		args = append(append(append([]string{}, args[:2]...), "--user="+owner), args[2:]...)
	}
	raw, err = h.docker(ctx, args...)
	id := strings.TrimSpace(string(raw))
	if err != nil || !dockerID.MatchString(id) {
		return "", errors.New("failed to create owned dependency container")
	}
	raw, err = h.docker(ctx, "container", "start", id)
	if err != nil || strings.TrimSpace(string(raw)) != id {
		return "", errors.New("failed to start owned dependency container")
	}
	return id, nil
}
func hba(database string, roles []string, replication string, init bool) string {
	var lines []string
	if init {
		lines = append(lines, "local all all trust")
	}
	lines = append(lines, "hostssl "+database+" "+strings.Join(roles, ",")+" all scram-sha-256")
	if replication != "" {
		lines = append(lines, "hostssl replication "+replication+" all scram-sha-256")
	}
	lines = append(lines, "host all all all reject")
	return strings.Join(lines, "\n") + "\n"
}
func (h fixtureHost) writeDBTLS(ctx context.Context, path, name, rules string, ca controllerCA) error {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return err
	}
	if err := os.Chmod(path, 0o755); err != nil {
		return err
	}
	tls, err := ca.certificate(name, "")
	if err != nil {
		return err
	}
	for name, body := range map[string][]byte{"server.crt": tls.Certificate, "server.key": tls.Key, "pg_hba.conf": []byte(rules)} {
		mode := fs.FileMode(0o644)
		if name == "server.key" {
			mode = 0o600
		}
		file := filepath.Join(path, name)
		if err := secureWrite(file, body, mode); err != nil {
			return err
		}
		if err := os.Chmod(file, mode); err != nil {
			return err
		}
	}
	if filepath.Base(path) != targetSecurityDir {
		return nil
	}
	// The mount is private to the job. Change only the server key's ownership.
	_, err = h.docker(ctx, "run", "--rm", "--pull=never", "--network=none", "--user=0:0",
		"--mount=type=bind,src="+path+",dst=/j7/tls", "--entrypoint=chown", h.plan.Lock.Postgres, "postgres:postgres", "/j7/tls/server.key")
	return err
}
func (d *drillDatabases) start(ctx context.Context, h fixtureHost, drill string, p drillPaths, ca controllerCA,
	w fixtureWorkload, clock orphanClock) error {
	if err := secureWrite(d.ca, ca.pem, 0o600); err != nil {
		return err
	}
	if err := os.MkdirAll(p.raw("media/archive"), 0o755); err != nil {
		return err
	}
	specs := []struct {
		dep     controllerDependency
		admin   dbLogin
		db, hba string
	}{
		{d.source, d.sourceAdmin, engineDatabase, hba(engineDatabase, []string{d.sourceAdmin.role, sourceWriterRole, observerRole, orphanObserverRole, k6auth.ReaderRole, k6auth.DecisionRole}, backupRole, true)},
		{d.sink, d.sinkAdmin, sinkDatabase, hba(sinkDatabase, []string{d.sinkAdmin.role, d.sinkService.role, d.sinkExport.role}, "", true)},
	}
	for _, s := range specs {
		dir := p.cfg(s.dep.Role)
		if err := h.writeDBTLS(ctx, filepath.Join(dir, "tls"), s.dep.Endpoint, s.hba, ca); err != nil {
			return err
		}
		if err := secureWrite(filepath.Join(dir, "postgres.env"), []byte("POSTGRES_USER="+s.admin.role+"\nPOSTGRES_PASSWORD="+s.admin.pass+"\nPOSTGRES_DB="+s.db+"\nPOSTGRES_INITDB_ARGS=--wal-segsize=16\n"), 0o600); err != nil {
			return err
		}
		args, err := h.postgresCommand(drill, s.dep)
		if err != nil {
			return err
		}
		id, err := h.createOwnedContainer(ctx, drill, s.dep, args)
		if err != nil {
			return err
		}
		d.ids[s.dep.Role] = id
	}
	if err := h.writeDBTLS(ctx, p.cfg(targetSecurityDir), d.target.Endpoint, hba(engineDatabase,
		[]string{targetWriterRole, observerRole, orphanObserverRole, k6auth.ReaderRole}, "", false), ca); err != nil {
		return err
	}
	source, err := waitPG(ctx, d.sourceAdmin.local(d.source, engineDatabase, d.ca, false))
	if err != nil {
		return err
	}
	defer source.Close(ctx)
	sink, err := waitPG(ctx, d.sinkAdmin.local(d.sink, sinkDatabase, d.ca, false))
	if err != nil {
		return err
	}
	defer sink.Close(ctx)
	if err := d.migrateAndSeed(ctx, h, source, sink, w, clock); err != nil {
		return err
	}
	for _, item := range []struct{ path, dsn string }{
		{d.sinkExportPath, d.sinkExport.local(d.sink, sinkDatabase, d.ca, true)},
		{d.sourceOrphanPath, d.orphanObserver.local(d.source, engineDatabase, d.ca, true)},
		{d.targetOrphanPath, d.orphanObserver.local(d.target, engineDatabase, d.ca, true)},
		{d.targetObserverPath, d.observer.local(d.target, engineDatabase, d.ca, true)},
	} {
		if err := secureWrite(item.path, []byte(item.dsn+"\n"), 0o600); err != nil {
			return err
		}
	}
	if err := d.startPGTools(ctx, h, drill, p); err != nil {
		return err
	}
	return d.startBroker(ctx, h, drill)
}
func waitPG(ctx context.Context, dsn string) (*pgx.Conn, error) {
	bound, stop := context.WithTimeout(ctx, 35*time.Second)
	defer stop()
	for {
		conn, err := pgx.Connect(bound, dsn)
		if err == nil {
			return conn, nil
		}
		select {
		case <-bound.Done():
			return nil, errors.New("PostgreSQL did not accept the sealed TLS login")
		case <-time.After(150 * time.Millisecond):
		}
	}
}
func setupLogin(ctx context.Context, c *pgx.Conn, a dbLogin, create bool, flags string) error {
	if strings.ContainsAny(a.pass, "'\x00") || a.pass == "" {
		return errors.New("unsafe fixture credential")
	}
	kind := "ALTER ROLE "
	if create {
		kind = "CREATE ROLE "
	}
	if _, err := c.Exec(ctx, kind+pgx.Identifier{a.role}.Sanitize()+" LOGIN "+flags+" PASSWORD '"+a.pass+"'"); err != nil {
		return fmt.Errorf("cannot provision fixture role %s", a.role)
	}
	return nil
}
func applyFixtureMigrations(ctx context.Context, c *pgx.Conn, src fs.FS, count int) error {
	files, err := fs.Glob(src, "*.up.sql")
	if err != nil || len(files) != count {
		return errors.New("migration inventory does not match the frozen J7 profile")
	}
	for i, file := range files {
		if !strings.HasPrefix(file, fmt.Sprintf("%06d_", i+1)) {
			return errors.New("migration sequence is not contiguous")
		}
		raw, err := fs.ReadFile(src, file)
		if err != nil {
			return err
		}
		if _, err := c.PgConn().Exec(ctx, string(raw)).ReadAll(); err != nil {
			return fmt.Errorf("J7 migration %s: %w", file, err)
		}
	}
	return nil
}
func (d *drillDatabases) migrateAndSeed(ctx context.Context, h fixtureHost, source, sink *pgx.Conn, w fixtureWorkload, clock orphanClock) error {
	if err := applyFixtureMigrations(ctx, source, migrations.SQL, 23); err != nil {
		return err
	}
	if err := checkEngineMigrations(ctx, source, 23); err != nil {
		return err
	}
	if err := applyFixtureMigrations(ctx, sink, os.DirFS(filepath.Join(h.root, "sink-migrations")), 4); err != nil {
		return err
	}
	for _, a := range []dbLogin{d.sourceWriter, d.targetWriter, d.observer, d.orphanObserver, d.backup} {
		flags := "NOINHERIT"
		if a.role == backupRole {
			flags += " REPLICATION"
		}
		if err := setupLogin(ctx, source, a, true, flags); err != nil {
			return err
		}
	}
	for _, a := range []dbLogin{d.sinkService, d.sinkExport} {
		if err := setupLogin(ctx, sink, a, false, "NOINHERIT"); err != nil {
			return err
		}
	}
	for _, sql := range []string{
		"GRANT USAGE ON SCHEMA engine,effects,k6_auth TO j7_source_writer,j7_target_writer",
		"GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA engine,effects TO j7_source_writer,j7_target_writer",
		"GRANT SELECT ON k6_auth.episode TO j7_source_writer,j7_target_writer,j7_e1_observer",
		"GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA engine,effects TO j7_source_writer,j7_target_writer",
		"GRANT USAGE ON SCHEMA engine,effects,k6_auth TO j7_observer,j7_e1_observer",
		"GRANT SELECT ON ALL TABLES IN SCHEMA engine,effects TO j7_observer,j7_e1_observer",
		"GRANT pg_read_all_stats TO j7_e1_observer,j7_observer WITH INHERIT TRUE",
		"GRANT EXECUTE ON FUNCTION pg_control_system(),pg_control_checkpoint(),pg_ls_waldir() TO j7_observer,j7_e1_observer,j7_source_writer,j7_target_writer",
		"ALTER ROLE j7_observer SET default_transaction_read_only=on",
		"ALTER ROLE j7_e1_observer SET default_transaction_read_only=on",
	} {
		if _, err := source.Exec(ctx, sql); err != nil {
			return fmt.Errorf("engine fixture privileges: %w", err)
		}
	}
	cred, err := k6auth.Provision(ctx, source, w.work.Fixture.Namespace, clock.StartUTC.Add(grantWindow),
		10*time.Millisecond, d.sourceAdmin.role, sourceWriterRole, targetWriterRole, observerRole, orphanObserverRole, backupRole)
	if err != nil {
		return err
	}
	d.reader = dbLogin{k6auth.ReaderRole, cred.ReaderPassword}
	d.decision = dbLogin{k6auth.DecisionRole, cred.DecisionPassword}
	store, err := state.NewK6RuntimeFromURL(ctx, d.sourceAdmin.local(d.source, engineDatabase, d.ca, false))
	if err != nil {
		return err
	}
	defer store.Close()
	for _, def := range w.definitions {
		if err := store.CreateDefinition(ctx, def); err != nil {
			return err
		}
	}
	_, err = sink.Exec(ctx, `INSERT INTO k6_sink.resources(resource_id,state)
		VALUES($1,'{}'::jsonb)`, w.effect.Resource)
	return err
}
func (d *drillDatabases) startBroker(ctx context.Context, h fixtureHost, drill string) error {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return err
	}
	cluster := base64.RawURLEncoding.EncodeToString(entropy[:])
	args, err := h.sourceKafkaCommand(drill, d.broker, cluster)
	if err != nil {
		return err
	}
	id, err := h.createOwnedContainer(ctx, drill, d.broker, args)
	if err != nil {
		return err
	}
	d.ids[sourceKafkaRole] = id
	p := kafkaPlacement{Volume: d.broker.Volume, Alias: d.broker.Endpoint, Port: d.broker.HostPort}
	b, err := openJ7Broker(p.hostEndpoint())
	if err != nil {
		return err
	}
	defer b.ids.Close()
	bounded, cancel := context.WithTimeout(ctx, 50*time.Second)
	defer cancel()
	return awaitNewKafka(bounded, b.api, cluster)
}
func (d *drillDatabases) startPGTools(ctx context.Context, h fixtureHost, drill string, p drillPaths) error {
	service := ""
	for _, r := range []struct {
		name string
		a    dbLogin
	}{{"j7-source", d.sourceAdmin}, {"j7-backup", d.backup}} {
		service += fmt.Sprintf("[%s]\nhost=127.0.0.1\nport=%d\nuser=%s\npassword=%s\ndbname=%s\nsslmode=verify-full\nsslrootcert=%s\nconnect_timeout=2\n",
			r.name, d.source.HostPort, r.a.role, r.a.pass, engineDatabase, d.ca)
	}
	if err := secureWrite(d.sourceService, []byte(service), 0o600); err != nil {
		return err
	}
	account, err := user.Current()
	if err != nil {
		return err
	}
	if _, err := strconv.Atoi(account.Uid); err != nil {
		return err
	}
	if _, err := strconv.Atoi(account.Gid); err != nil {
		return err
	}
	name := h.cluster + "-" + drill + "-pg-tools"
	args := []string{"container", "create", "--pull=never", "--name=" + name, "--network=host", "--restart=no",
		"--cpus=0.5", "--memory=256m", "--tmpfs=/var/lib/postgresql", "--user=" + account.Uid + ":" + account.Gid, "--entrypoint=sleep",
		"--env=PGSERVICEFILE=" + d.sourceService, "--mount=type=bind,src=" + p.secret + ",dst=" + p.secret + ",readonly",
		"--mount=type=bind,src=" + p.raw("media") + ",dst=" + p.raw("media")}
	args = append(args, labelArgs(h.dependencyLabels(drill, "pg-tools"))...)
	raw, err := h.docker(ctx, append(args, h.plan.Lock.Postgres, "infinity")...)
	id := strings.TrimSpace(string(raw))
	if err != nil || !dockerID.MatchString(id) {
		return errors.New("pinned PostgreSQL client container was not created")
	}
	d.ids["pg-tools"] = id
	if _, err := h.docker(ctx, "container", "start", id); err != nil {
		return err
	}
	if err := os.Mkdir(p.bin, 0o700); err != nil {
		return err
	}
	for _, tool := range []string{"pg_basebackup", "pg_verifybackup", "pg_waldump", "pg_isready", "psql"} {
		// exec deliberately preserves the PostgreSQL client's exit status.
		body := "#!/bin/sh\nexec docker --host=unix:///var/run/docker.sock container exec -i " + id +
			" /usr/lib/postgresql/18/bin/" + tool + " \"$@\"\n"
		if err := secureWrite(filepath.Join(p.bin, tool), []byte(body), 0o700); err != nil {
			return err
		}
	}
	return nil
}

func checkEngineMigrations(ctx context.Context, c *pgx.Conn, expected int) error {
	rows, err := c.Query(ctx, "SELECT version FROM engine.schema_migrations ORDER BY version")
	if err != nil {
		return errors.New("engine schema migration inventory is unreadable")
	}
	got, err := pgx.CollectRows(rows, pgx.RowTo[int])
	if err != nil || len(got) != expected {
		return errors.New("effective engine migration inventory differs from the reviewed profile")
	}
	for i, version := range got {
		if version != i+1 {
			return errors.New("unexpected engine migration version")
		}
	}
	return nil
}
