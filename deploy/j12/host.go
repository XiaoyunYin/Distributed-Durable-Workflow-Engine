package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const sharedMount = "/j12-keys"

var listeners = []string{"public", "operator", "worker", "demand"}
var pods = []string{"public", "operator", "worker", "demand", "relay", "operator-probe", "worker-probe"}

type toolPin struct {
	URL string `json:"url"`
	SHA string `json:"sha256"`
}
type toolLock struct {
	Runner   string             `json:"runner"`
	Tools    map[string]toolPin `json:"tools"`
	Node     string             `json:"node"`
	Postgres string             `json:"postgres"`
	Kafka    string             `json:"kafka"`
}

type host struct {
	in                                         inputs
	name, image, kafkaIP, dbIP, dbURL, imageID string
	bin, keys                                  string
	lock                                       toolLock
	env                                        []string
	scratch, cluster, pg, kafka, built         bool
	admin, observer                            *pgx.Conn
	rendered                                   map[string]object
	podUIDs                                    map[string]string
}

type object = map[string]any

func newHost(in inputs) *host {
	name := "j12-" + in.run + "-" + in.attempt
	return &host{in: in, name: name, image: name + ":fixture", bin: filepath.Join(in.scratch, "bin"),
		keys: filepath.Join(in.scratch, "keys"), env: append(os.Environ(), "KUBECONFIG="+filepath.Join(in.scratch, "kubeconfig"))}
}

func (h *host) cmd(ctx context.Context, stage, program string, input []byte, args ...string) ([]byte, error) {
	budget := 60 * time.Second
	if program == "docker" || filepath.Base(program) == "kind" {
		budget = 4 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	output, err := execute(ctx, h.env, input, program, args...)
	if err != nil {
		return nil, fmt.Errorf("J12 %s failed: %w", stage, err)
	}
	return output, nil
}

func (h *host) kubectl(ctx context.Context, stage string, input []byte, args ...string) ([]byte, error) {
	return h.cmd(ctx, stage, filepath.Join(h.bin, "kubectl"), input, append([]string{"-n", h.name}, args...)...)
}

func downloadTool(ctx context.Context, pin toolPin, target string) error {
	if !strings.HasPrefix(pin.URL, "https://") || len(pin.SHA) != 64 {
		return errors.New("missing J12 tool pin")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, pin.URL, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: time.Minute}
	r, err := client.Do(request)
	if err != nil {
		return errors.New("download pinned J12 tool")
	}
	defer r.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(r.Body, 100<<20))
	if err != nil || r.StatusCode != 200 || hexDigest(raw) != pin.SHA {
		return errors.New("J12 tool pin mismatch")
	}
	return os.WriteFile(target, raw, 0700)
}

func (h *host) bootstrap(ctx context.Context, migrations []string, c *credentials) error {
	if err := os.Mkdir(h.in.scratch, 0700); err != nil {
		return err
	}
	h.scratch = true
	if err := readFileJSON("deploy/k8s/tools.lock.json", &h.lock); err != nil {
		return err
	}
	imagePin := regexp.MustCompile(`@sha256:[a-f0-9]{64}$`)
	if h.lock.Runner != "ubuntu-24.04" || !imagePin.MatchString(h.lock.Node) || !imagePin.MatchString(h.lock.Kafka) || !imagePin.MatchString(h.lock.Postgres) {
		return errors.New("J12 tools lock lacks a pinned hosted topology")
	}
	for _, dir := range []string{h.bin, h.keys, filepath.Join(h.in.scratch, "image")} {
		if err := os.Mkdir(dir, 0755); err != nil {
			return err
		}
	}
	for _, name := range []string{"kind", "kubectl"} {
		if err := downloadTool(ctx, h.lock.Tools[name], filepath.Join(h.bin, name)); err != nil {
			return err
		}
	}
	if err := h.setKeys(c.jwks("current")); err != nil {
		return err
	}
	if err := h.buildImage(ctx); err != nil {
		return err
	}
	if _, err := execute(ctx, h.env, nil, "docker", "network", "inspect", "kind"); err == nil {
		return errors.New("J12 requires a fresh runner without an existing kind network")
	}
	config, _ := json.Marshal(object{"kind": "Cluster", "apiVersion": "kind.x-k8s.io/v1alpha4",
		"nodes": []object{{"role": "control-plane", "extraMounts": []object{{"hostPath": h.keys, "containerPath": sharedMount, "readOnly": true}}}},
	})
	kindConfig := filepath.Join(h.in.scratch, "kind.json")
	if err := os.WriteFile(kindConfig, config, 0600); err != nil {
		return err
	}
	h.cluster = true
	if _, err := h.cmd(ctx, "kind bootstrap", filepath.Join(h.bin, "kind"), nil, "create", "cluster", "--name", h.name, "--image", h.lock.Node, "--config", kindConfig, "--wait", "120s"); err != nil {
		return err
	}
	if _, err := h.cmd(ctx, "load runtime image", filepath.Join(h.bin, "kind"), nil, "load", "docker-image", h.image, "--name", h.name); err != nil {
		return err
	}
	if err := h.startPostgres(ctx, migrations); err != nil {
		return err
	}
	if err := h.startKafka(ctx); err != nil {
		return err
	}
	items := h.render(c.demand)
	manifest, err := json.Marshal(object{"apiVersion": "v1", "kind": "List", "items": items})
	if err != nil {
		return err
	}
	// Apply secrets over stdin without a client-side last-applied copy.
	if _, err := h.kubectl(ctx, "create J12 profile", manifest, "create", "-f", "-"); err != nil {
		return err
	}
	_, err = h.kubectl(ctx, "wait for J12 pods", nil, "wait", "--for=condition=Ready", "pod", "--all", "--timeout=50s")
	return err
}

func (h *host) buildImage(ctx context.Context) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	dir := filepath.Join(h.in.scratch, "image")
	for name, source := range map[string]string{"runtime": h.in.runtime, "j12": self} {
		raw, err := os.ReadFile(source)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, name), raw, 0755); err != nil {
			return err
		}
	}
	const dockerfile = "FROM scratch\nCOPY runtime j12 /\nUSER 65532:65532\nENTRYPOINT [\"/runtime\"]\n"
	h.built = true
	if _, err := h.cmd(ctx, "build runtime image", "docker", []byte(dockerfile), "build", "-f", "-", "-t", h.image, dir); err != nil {
		return err
	}
	id, err := h.cmd(ctx, "runtime image ID", "docker", nil, "image", "inspect", "--format", "{{.Id}}", h.image)
	h.imageID = strings.TrimSpace(string(id))
	return err
}

func (h *host) serviceIP(ctx context.Context, name string) (string, error) {
	raw, err := h.cmd(ctx, "inspect dependency network", "docker", nil, "inspect", "--format", `{{(index .NetworkSettings.Networks "kind").IPAddress}}`, name)
	ip := strings.TrimSpace(string(raw))
	if err != nil || net.ParseIP(ip).To4() == nil {
		return "", errors.New("J12 dependency IPv4 was unavailable")
	}
	return ip, nil
}

func poll(ctx context.Context, budget time.Duration, probe func(context.Context) bool) bool {
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		if probe(ctx) {
			return true
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
		}
	}
	return false
}

func (h *host) startPostgres(ctx context.Context, migrations []string) error {
	name := h.name + "-postgres"
	h.pg = true
	if _, err := h.cmd(ctx, "start PostgreSQL", "docker", nil, "run", "-d", "--name", name, "--network", "kind", "-e", "POSTGRES_HOST_AUTH_METHOD=trust", h.lock.Postgres); err != nil {
		return err
	}
	var err error
	h.dbIP, err = h.serviceIP(ctx, name)
	if err != nil {
		return err
	}
	h.dbURL = "postgresql://postgres@" + h.dbIP + ":5432/postgres?sslmode=disable&connect_timeout=2"
	if !poll(ctx, 45*time.Second, func(ctx context.Context) bool {
		h.admin, err = pgx.Connect(ctx, h.dbURL)
		return err == nil
	}) {
		return errors.New("J12 PostgreSQL unavailable")
	}
	for _, path := range migrations {
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if _, err := h.admin.Exec(ctx, string(raw)); err != nil {
			return fmt.Errorf("apply J12 migration %s: %w", filepath.Base(path), err)
		}
	}
	h.observer, err = openObserver(ctx, h.admin)
	return err
}

func openObserver(ctx context.Context, admin *pgx.Conn) (*pgx.Conn, error) {
	if _, err := admin.Exec(ctx, `CREATE ROLE j12_witness LOGIN;
		GRANT USAGE ON SCHEMA engine, effects TO j12_witness;
		GRANT SELECT ON ALL TABLES IN SCHEMA engine, effects TO j12_witness;
		ALTER ROLE j12_witness SET default_transaction_read_only = on;`); err != nil {
		return nil, errors.New("create read-only J12 witness role")
	}
	config := admin.Config().Copy()
	config.User, config.Password = "j12_witness", ""
	return pgx.ConnectConfig(ctx, config)
}

func (h *host) startKafka(ctx context.Context) error {
	name := h.name + "-kafka"
	settings := []string{"KAFKA_NODE_ID=1", "KAFKA_PROCESS_ROLES=broker,controller", "KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093",
		"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://j12-kafka:9092", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
		"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
		"KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
		"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1", "KAFKA_AUTO_CREATE_TOPICS_ENABLE=false", "KAFKA_NUM_PARTITIONS=1", "KAFKA_LOG_DIRS=/var/lib/kafka/data"}
	args := []string{"run", "-d", "--name", name, "--network", "kind", "--network-alias", "j12-kafka"}
	for _, pair := range settings {
		args = append(args, "-e", pair)
	}
	h.kafka = true
	if _, err := h.cmd(ctx, "start Kafka", "docker", nil, append(args, h.lock.Kafka)...); err != nil {
		return err
	}
	var err error
	h.kafkaIP, err = h.serviceIP(ctx, name)
	if err != nil {
		return err
	}
	for _, topic := range []string{"durable-agent.tasks.v1", "durable-agent.events.v1"} {
		if !poll(ctx, 60*time.Second, func(ctx context.Context) bool {
			_, err := h.cmd(ctx, "create Kafka topic", "docker", nil, "exec", name, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
				"--create", "--if-not-exists", "--topic", topic, "--partitions", "1", "--replication-factor", "1")
			return err == nil
		}) {
			return errors.New("J12 Kafka topic unavailable")
		}
	}
	return nil
}

func kubeObject(version, kind, name, namespace string, fields object) object {
	metadata := object{"name": name}
	if namespace != "" {
		metadata["namespace"] = namespace
	}
	result := object{"apiVersion": version, "kind": kind, "metadata": metadata}
	for key, value := range fields {
		result[key] = value
	}
	return result
}

func projected(name, path, audience string) object {
	token := object{"path": path, "expirationSeconds": 3600}
	if audience != "" {
		token["audience"] = audience
	}
	return object{"name": name, "projected": object{"sources": []object{{"serviceAccountToken": token}}}}
}

func (h *host) render(secret string) []object {
	const v1 = "v1"
	const rbac = "rbac.authorization.k8s.io/v1"
	items := []object{kubeObject(v1, "Namespace", h.name, "", nil),
		kubeObject(rbac, "ClusterRole", h.name, "", object{"rules": []object{{"apiGroups": []string{"authentication.k8s.io"}, "resources": []string{"tokenreviews"}, "verbs": []string{"create"}}}}),
		kubeObject(rbac, "ClusterRoleBinding", h.name, "", object{"roleRef": object{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": h.name},
			"subjects": []object{{"kind": "ServiceAccount", "name": "reviewer", "namespace": h.name}}}),
		kubeObject(v1, "Secret", "demand", h.name, object{"type": "Opaque", "stringData": object{"bearer": secret}}),
	}
	for _, account := range []string{"unprivileged", "reviewer", "operator-caller", "worker-caller"} {
		items = append(items, kubeObject(v1, "ServiceAccount", account, h.name, object{"automountServiceAccountToken": false}))
	}
	h.rendered = map[string]object{}
	for _, name := range pods {
		pod := h.renderPod(name)
		h.rendered[name] = pod["spec"].(object)
		items = append(items, pod)
		if slices.Contains(listeners, name) {
			items = append(items, kubeObject(v1, "Service", name, h.name, object{"spec": object{"selector": object{"j12-listener": name},
				"ports": []object{{"port": 8080, "targetPort": 8080}}}}))
		}
	}
	return items
}

func (h *host) renderPod(name string) object {
	account := "unprivileged"
	env := map[string]string{}
	volumes, mounts := []object{}, []object{}
	mount := func(volume object, path string) {
		volumes = append(volumes, volume)
		mounts = append(mounts, object{"name": volume["name"], "mountPath": path, "readOnly": true})
	}
	container := object{"name": "j12", "image": h.image, "imagePullPolicy": "Never",
		"securityContext": object{"allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true, "capabilities": object{"drop": []string{"ALL"}}}}
	if strings.HasSuffix(name, "-probe") {
		role := strings.TrimSuffix(name, "-probe")
		account = role + "-caller"
		mount(projected("caller", "token", "durable-j12-"+role), "/caller")
		container["command"] = []string{"/j12", "hold"}
	} else {
		role := map[string]string{"public": "api", "operator": "operator", "worker": "worker-control", "demand": "demand", "relay": "relay"}[name]
		env = map[string]string{authSwitch: "1", h.in.relayName: h.in.relayValue, "RUNTIME_ROLES": role, "RUNTIME_ROLE": name,
			"RUNTIME_ADDR": "0.0.0.0:8080", "RUNTIME_NAMESPACE": "tenant-a", "DATABASE_URL": h.dbURL, "RUNTIME_SHUTDOWN_MODE": "drain"}
		container["readinessProbe"] = object{"httpGet": object{"path": "/readyz", "port": 8080}, "periodSeconds": 1}
		switch name {
		case "public":
			env["RUNTIME_PUBLIC_JWT_ISSUER"], env["RUNTIME_PUBLIC_JWT_AUDIENCE"], env["RUNTIME_PUBLIC_JWT_JWKS_FILE"] = issuer, audience, sharedMount+"/jwks.json"
			mount(object{"name": "jwks", "hostPath": object{"path": sharedMount, "type": "Directory"}}, sharedMount)
		case "operator", "worker":
			account = "reviewer"
			mount(projected("reviewer-token", "token", ""), "/reviewer")
			mount(object{"name": "reviewer-ca", "configMap": object{"name": "kube-root-ca.crt"}}, "/reviewer-ca")
			for key, value := range map[string]string{"RUNTIME_TOKEN_AUTH": "enabled", "RUNTIME_TOKEN_REVIEW_URL": "https://kubernetes.default.svc",
				"RUNTIME_TOKEN_REVIEW_CA": "/reviewer-ca/ca.crt", "RUNTIME_TOKEN_REVIEW_TOKEN_FILE": "/reviewer/token", "RUNTIME_TOKEN_ISSUER": clusterIssuer,
				"RUNTIME_TOKEN_AUDIENCE": "durable-j12-" + name, "RUNTIME_TOKEN_NAMESPACE": h.name, "RUNTIME_TOKEN_SERVICE_ACCOUNT": name + "-caller"} {
				env[key] = value
			}
		case "demand":
			env["RUNTIME_TOKEN_AUTH"], env["RUNTIME_DEMAND_BEARER_TOKEN_FILE"] = "enabled", "/demand/bearer"
			mount(object{"name": "bearer", "secret": object{"secretName": "demand"}}, "/demand")
		case "relay":
			env["KAFKA_BOOTSTRAP_SERVERS"] = "j12-kafka:9092"
		}
	}
	var names []string
	for key := range env {
		names = append(names, key)
	}
	slices.Sort(names)
	environment := []object{}
	for _, key := range names {
		environment = append(environment, object{"name": key, "value": env[key]})
	}
	container["env"], container["volumeMounts"] = environment, mounts
	spec := object{"serviceAccountName": account, "automountServiceAccountToken": false, "enableServiceLinks": false, "restartPolicy": "Never", "volumes": volumes,
		"securityContext": object{"runAsNonRoot": true, "runAsUser": 65532, "runAsGroup": 65532, "seccompProfile": object{"type": "RuntimeDefault"}},
		"containers":      []object{container}}
	if name == "relay" {
		spec["hostAliases"] = []object{{"ip": h.kafkaIP, "hostnames": []string{"j12-kafka"}}}
	}
	pod := kubeObject("v1", "Pod", name, h.name, object{"spec": spec})
	pod["metadata"].(object)["labels"] = object{"j12-listener": name}
	return pod
}

func (h *host) setKeys(raw []byte) error {
	path := filepath.Join(h.keys, "jwks.json")
	if raw == nil {
		return os.Remove(path)
	}
	if err := os.WriteFile(path+".new", raw, 0644); err != nil {
		return err
	}
	return os.Rename(path+".new", path)
}

func (h *host) inventory(ctx context.Context, relaySource string) (object, error) {
	var versions []int
	var account string
	var readOnly string
	if err := h.observer.QueryRow(ctx, `SELECT current_user, current_setting('transaction_read_only'), array_agg(version ORDER BY version) FROM engine.schema_migrations`).Scan(&account, &readOnly, &versions); err != nil {
		return nil, err
	}
	var expected []int
	for i := 1; i <= 23; i++ {
		expected = append(expected, i)
	}
	expected = append(expected, 26)
	if account != "j12_witness" || readOnly != "on" || !slices.Equal(versions, expected) {
		return nil, errors.New("running migration or witness identity mismatch")
	}
	raw, err := h.kubectl(ctx, "read live pods", nil, "get", "pod", "-o", "json")
	if err != nil {
		return nil, err
	}
	var live struct {
		Items []struct {
			Metadata struct{ Name, UID string }
			Spec     struct {
				ServiceAccountName           string
				AutomountServiceAccountToken *bool
				Containers                   []struct {
					Image        string
					Command      []string
					Env          []object
					VolumeMounts []object
				}
			}
			Status struct {
				Phase             string
				ContainerStatuses []struct {
					ImageID      string
					Ready        bool
					RestartCount int
				}
			}
		}
	}
	if json.Unmarshal(raw, &live) != nil || len(live.Items) != len(pods) {
		return nil, errors.New("live J12 pod inventory mismatch")
	}
	views := map[string]object{}
	h.podUIDs = map[string]string{}
	var liveImage string
	for _, pod := range live.Items {
		name := pod.Metadata.Name
		rendered := h.rendered[name]
		if rendered == nil || views[name] != nil || pod.Metadata.UID == "" || pod.Spec.ServiceAccountName != rendered["serviceAccountName"] ||
			pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken || len(pod.Spec.Containers) != 1 || len(pod.Status.ContainerStatuses) != 1 {
			return nil, errors.New("live J12 pod identity mismatch")
		}
		container, status := pod.Spec.Containers[0], pod.Status.ContainerStatuses[0]
		planned := rendered["containers"].([]object)[0]
		command, _ := planned["command"].([]string)
		if container.Image != h.image || !status.Ready || status.RestartCount != 0 || status.ImageID == "" ||
			!slices.Equal(container.Command, command) || (liveImage != "" && liveImage != status.ImageID) ||
			!sameObjects(container.Env, planned["env"].([]object)) || !sameObjects(container.VolumeMounts, planned["volumeMounts"].([]object)) {
			return nil, errors.New("live J12 container settings mismatch")
		}
		liveImage = status.ImageID
		view := object{"uid": pod.Metadata.UID, "service_account": pod.Spec.ServiceAccountName, "image_id": status.ImageID}
		h.podUIDs[name] = pod.Metadata.UID
		if env := planned["env"].([]object); len(env) != 0 {
			safe := object{}
			for _, pair := range env {
				key := pair["name"].(string)
				if key != "DATABASE_URL" && key != "KAFKA_BOOTSTRAP_SERVERS" {
					safe[key] = pair["value"]
				}
			}
			view["expected_environment"], view["effective_environment"] = safe, safe
		}
		views[name] = view
	}
	services, err := h.liveServices(ctx)
	if err != nil {
		return nil, err
	}
	dependencies := map[string]object{}
	for _, target := range []struct{ role, name, image string }{
		{"kind", h.name + "-control-plane", h.lock.Node}, {"postgres", h.name + "-postgres", h.lock.Postgres}, {"kafka", h.name + "-kafka", h.lock.Kafka},
	} {
		raw, err := h.cmd(ctx, "read running dependency image", "docker", nil, "inspect", "--format", "{{.Config.Image}} {{.Image}}", target.name)
		actual := strings.Fields(string(raw))
		if err != nil || len(actual) != 2 || actual[0] != target.image || !strings.HasPrefix(actual[1], "sha256:") {
			return nil, errors.New("live J12 dependency image mismatch")
		}
		dependencies[target.role] = object{"pinned_image": actual[0], "image_id": actual[1]}
	}
	return object{"runner": h.lock.Runner, "dependencies": dependencies, "runtime_image_id": h.imageID,
		"relay_reader_sha256": relaySource, "migration_versions": versions, "observer": account, "read_only": true, "pods": views, "services": services}, nil
}

func (h *host) liveServices(ctx context.Context) ([]string, error) {
	raw, err := h.kubectl(ctx, "read live listener services", nil, "get", "service", "-o", "json")
	if err != nil {
		return nil, err
	}
	var live struct {
		Items []struct {
			Metadata struct{ Name string }
			Spec     struct {
				Type     string
				Selector map[string]string
				Ports    []struct {
					Port, TargetPort int
					Protocol         string
				}
			}
		}
	}
	if json.Unmarshal(raw, &live) != nil || len(live.Items) != len(listeners) {
		return nil, errors.New("J12 listener service count mismatch")
	}
	var names []string
	for _, service := range live.Items {
		name := service.Metadata.Name
		ports := service.Spec.Ports
		if !slices.Contains(listeners, name) || slices.Contains(names, name) || service.Spec.Type != "ClusterIP" ||
			len(service.Spec.Selector) != 1 || service.Spec.Selector["j12-listener"] != name || len(ports) != 1 || ports[0].Port != 8080 || ports[0].TargetPort != 8080 || ports[0].Protocol != "TCP" {
			return nil, errors.New("live J12 listener does not match the rendered topology")
		}
		names = append(names, name)
	}
	slices.Sort(names)
	return names, nil
}

func sameObjects(live, expected []object) bool {
	if len(live) != len(expected) {
		return false
	}
	for i, item := range live {
		if len(item) != len(expected[i]) {
			return false
		}
		for key, value := range item {
			if value != expected[i][key] {
				return false
			}
		}
	}
	return true
}

func (h *host) cleanup(ctx context.Context) bool {
	clean := true
	if h.observer != nil {
		_ = h.observer.Close(ctx)
	}
	if h.admin != nil {
		_ = h.admin.Close(ctx)
	}
	for name, started := range map[string]bool{h.name + "-postgres": h.pg, h.name + "-kafka": h.kafka} {
		if started {
			_, _ = h.cmd(ctx, "remove J12 dependency", "docker", nil, "rm", "-f", "-v", name)
			raw, err := h.cmd(ctx, "verify dependency cleanup", "docker", nil, "ps", "-aq", "--filter", "name=^/"+name+"$")
			clean = clean && err == nil && strings.TrimSpace(string(raw)) == ""
		}
	}
	if h.cluster {
		_, err := h.cmd(ctx, "delete kind cluster", filepath.Join(h.bin, "kind"), nil, "delete", "cluster", "--name", h.name)
		clean = clean && err == nil
		_, _ = h.cmd(ctx, "remove kind network", "docker", nil, "network", "rm", "kind")
		raw, err := h.cmd(ctx, "verify kind cleanup", "docker", nil, "network", "ls", "--filter", "name=^kind$", "--format", "{{.Name}}")
		clean = clean && err == nil && strings.TrimSpace(string(raw)) == ""
	}
	if h.built {
		_, _ = h.cmd(ctx, "remove J12 image", "docker", nil, "image", "rm", h.image)
		raw, err := h.cmd(ctx, "verify image cleanup", "docker", nil, "image", "ls", "-q", h.image)
		clean = clean && err == nil && strings.TrimSpace(string(raw)) == ""
	}
	if h.scratch {
		clean = os.RemoveAll(h.in.scratch) == nil && clean
	}
	return clean
}
