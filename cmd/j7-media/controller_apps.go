package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"

	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/k6auth"
	"durable-agent-execution-engine/internal/k6ingress"
	"durable-agent-execution-engine/internal/restoreledger"
)

const (
	sinkURI      = "spiffe://j7.local/sink"
	relayURI     = "spiffe://j7.local/relay"
	allocatorURI = "spiffe://j7.local/allocator"
)

type liveApps struct {
	h                                                     fixtureHost
	p                                                     drillPaths
	ns                                                    string
	ca                                                    controllerCA
	db                                                    *drillDatabases
	work                                                  *fixtureWorkload
	receiptKey                                            ed25519.PrivateKey
	sinkTLS, allocatorTLS                                 k6ingress.TLSFiles
	reviewerCA                                            []byte
	reviewerToken, workerToken, workerIdentity, sinkToken string
	api, control                                          fixtureHTTP
	forwards                                              []*heldProcess
	sinkForward                                           *heldProcess
	imageDigest                                           string
}

func fixtureClientTransport(root []byte) (*http.Transport, error) {
	ca := x509.NewCertPool()
	if !ca.AppendCertsFromPEM(root) {
		return nil, errors.New("J7 fixture TLS CA is unavailable")
	}
	return &http.Transport{TLSClientConfig: &tls.Config{RootCAs: ca, MinVersion: tls.VersionTLS13}}, nil
}

func (a *liveApps) stopForwards() {
	for _, p := range a.forwards {
		p.stop()
	}
	a.forwards = nil
}
func (a *liveApps) close() {
	a.stopForwards()
	a.sinkForward.stop()
}
func (a *liveApps) init(ctx context.Context) error {
	if err := a.h.installNamespace(ctx, a.p, a.ns); err != nil {
		return err
	}
	_, ca, token, err := a.h.kubeConnection(ctx, a.ns)
	if err != nil {
		return err
	}
	a.reviewerCA, a.reviewerToken = ca, token
	a.sinkTLS, err = a.ca.certificate("j7-sink", sinkURI)
	if err != nil {
		return err
	}
	a.allocatorTLS, err = a.ca.certificate("j7-allocator", allocatorURI)
	return err
}
func (a *liveApps) ownedAliases(ctx context.Context, target bool) (map[string]string, error) {
	d := a.db
	pairs := map[string]string{d.sink.Endpoint: d.ids[d.sink.Role]}
	if target {
		pairs[d.target.Endpoint] = d.ids[targetRole]
		pairs[d.fresh.Endpoint] = d.ids[targetKafkaRole]
	} else {
		pairs[d.source.Endpoint] = d.ids[sourceRole]
		pairs[d.broker.Endpoint] = d.ids[sourceKafkaRole]
	}
	out := map[string]string{}
	for name, id := range pairs {
		ip, err := a.h.inspectDependencyIP(ctx, id)
		if err != nil {
			return nil, err
		}
		out[name] = ip
	}
	return out, nil
}
func (a *liveApps) policy(ctx context.Context, aliases map[string]string) error {
	o := kubeObject("networking.k8s.io/v1", "NetworkPolicy", a.ns, "fixture-private")
	dependencies := []fixtureObject{}
	sinkDBs := []fixtureObject{}
	for name, ip := range aliases {
		peer := fixtureObject{"ipBlock": fixtureObject{"cidr": ip + "/32"}}
		dependencies = append(dependencies, peer)
		if name == a.db.source.Endpoint || name == a.db.target.Endpoint || name == a.db.sink.Endpoint {
			sinkDBs = append(sinkDBs, peer)
		}
	}
	// Service DNAT can expose either address to the policy engine.
	apiPeers := []fixtureObject{}
	for _, query := range [][]string{{"-n", "default", "get", "service", "kubernetes", "-o", "jsonpath={.spec.clusterIP}"},
		{"-n", "default", "get", "endpoints", "kubernetes", "-o", "jsonpath={.subsets[0].addresses[0].ip}"}} {
		raw, err := a.h.kubectl(ctx, query...)
		ip := strings.TrimSpace(string(raw))
		if err != nil || net.ParseIP(ip) == nil {
			return errors.New("cannot bind owned kube API egress")
		}
		apiPeers = append(apiPeers, fixtureObject{"ipBlock": fixtureObject{"cidr": ip + "/32"}})
	}
	dns := fixtureObject{"to": []fixtureObject{{"namespaceSelector": fixtureObject{"matchLabels": fixtureObject{"kubernetes.io/metadata.name": "kube-system"}},
		"podSelector": fixtureObject{"matchLabels": fixtureObject{"k8s-app": "kube-dns"}}}},
		"ports": []fixtureObject{{"port": 53, "protocol": "UDP"}, {"port": 53, "protocol": "TCP"}}}
	review := fixtureObject{"to": apiPeers, "ports": []fixtureObject{{"port": 443}, {"port": 6443}}}
	nsPeers := []fixtureObject{{"podSelector": fixtureObject{}}}
	o["spec"] = fixtureObject{"podSelector": fixtureObject{"matchExpressions": []fixtureObject{{"key": "j7-role", "operator": "NotIn", "values": []string{"sink"}}}},
		"policyTypes": []string{"Ingress", "Egress"}, "ingress": []fixtureObject{{"from": nsPeers}},
		"egress": []fixtureObject{{"to": nsPeers}, {"to": dependencies, "ports": []fixtureObject{{"port": 5432}, {"port": 19092}}}, review, dns}}
	sink := kubeObject("networking.k8s.io/v1", "NetworkPolicy", a.ns, "sink-private")
	sink["spec"] = fixtureObject{"podSelector": fixtureObject{"matchLabels": fixtureObject{"j7-role": "sink"}}, "policyTypes": []string{"Ingress", "Egress"},
		"ingress": []fixtureObject{{"from": []fixtureObject{{"podSelector": fixtureObject{"matchLabels": fixtureObject{"j7-role": "worker-control"}}}},
			"ports": []fixtureObject{{"port": 8443}}}},
		"egress": []fixtureObject{{"to": sinkDBs, "ports": []fixtureObject{{"port": 5432}}}, review, dns},
	}
	return a.h.applyFixture(ctx, a.p, o, sink)
}

type effectSinkConfig struct {
	Address                                string
	Worker                                 struct{ Namespace, ServiceAccount, ServiceAccountUID string }
	APIServer                              string
	APICA                                  []byte
	ReviewerToken                          string
	EnginePeerURI, IngressPeerURI          string
	ServerCertificate, ServerKey, ClientCA []byte
	SinkDSN, GrantReaderDSN                string
	ClockUncertaintyMicros                 int64
	ReceiptSigningSeed                     []byte
}

func (a *liveApps) startSink(ctx context.Context, target bool) error {
	aliases, err := a.ownedAliases(ctx, target)
	if err != nil {
		return err
	}
	if err := a.policy(ctx, aliases); err != nil {
		return err
	}
	d := a.db
	dep := d.source
	if target {
		dep = d.target
		a.sinkForward.stop()
		a.sinkForward = nil
		_, err := a.h.kubectl(ctx, "-n", a.ns, "delete", "pod/j7-sink", "secret/j7-sink-config", "--wait=true", "--timeout=20s")
		if err != nil {
			return err
		}
	}
	uid, err := a.h.kubectl(ctx, "-n", a.ns, "get", "serviceaccount", fixtureWorkerSA, "-o", "jsonpath={.metadata.uid}")
	if err != nil || len(uid) == 0 {
		return errors.New("J7 sink worker service account is unavailable")
	}
	cfg := effectSinkConfig{Address: ":8443", APIServer: clusterAPI, APICA: a.reviewerCA, ReviewerToken: a.reviewerToken,
		EnginePeerURI: allocatorURI, IngressPeerURI: relayURI, ServerCertificate: a.sinkTLS.Certificate, ServerKey: a.sinkTLS.Key,
		ClientCA: a.sinkTLS.CA, SinkDSN: d.sinkService.dsn(d.sink.Endpoint, 5432, sinkDatabase, "/private/ca.crt", false),
		GrantReaderDSN:         d.reader.dsn(dep.Endpoint, 5432, engineDatabase, "/private/ca.crt", false),
		ClockUncertaintyMicros: 10000, ReceiptSigningSeed: a.receiptKey.Seed()}
	cfg.Worker.Namespace, cfg.Worker.ServiceAccount, cfg.Worker.ServiceAccountUID = a.ns, fixtureWorkerSA, string(uid)
	body, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	pods := fixturePod(fixtureImage(a.h), podFixture{name: "j7-sink", namespace: a.ns, role: "sink",
		limits: a.h.plan.Work.Roles["sink"], command: []string{"/sink", "serve", "/private/sink.json"}, serve: 8443,
		private: map[string]string{"sink.json": string(body), "ca.crt": string(a.ca.pem)}, aliases: aliases})
	if err := a.h.applyFixture(ctx, a.p, pods...); err != nil {
		return err
	}
	if err := a.h.waitPod(ctx, a.ns, "j7-sink"); err != nil {
		return err
	}
	a.sinkForward, err = a.h.portForward(ctx, a.ns, "j7-sink", 18444, 8443)
	return err
}

func (a *liveApps) runtimeSpec(name, role, phase string, dep, kafka controllerDependency, aliases map[string]string, login dbLogin) podFixture {
	env := map[string]string{"RUNTIME_ROLES": role, "RUNTIME_ROLE": name, "RUNTIME_ADDR": ":8080", "RUNTIME_NAMESPACE": a.ns,
		"DATABASE_URL":       login.dsn(dep.Endpoint, 5432, engineDatabase, "/private/ca.crt", false),
		"DUR053_K6_ACTIVITY": "1", "KAFKA_BOOTSTRAP_SERVERS": net.JoinHostPort(kafka.Endpoint, "19092"),
		"DUR053_K6_RECEIPT_PUBLIC_KEY": base64.StdEncoding.EncodeToString(a.receiptKey.Public().(ed25519.PublicKey))}
	layer := a.h.plan.Source
	if phase == "target" {
		layer = a.h.plan.Target
	}
	maps.Copy(env, layer.Environment)
	quota := a.h.plan.Work.Roles[name]
	if name == "api" || name == "worker-control" {
		quota.MilliCPU, quota.Bytes = 250, 128<<20
	}
	return podFixture{name: phase + "-" + name, namespace: a.ns, role: name, limits: quota, command: []string{"/runtime"}, serve: 8080,
		private: map[string]string{"ca.crt": string(a.ca.pem)}, env: env, aliases: aliases}
}
func configureFixtureAuth(s *podFixture, sa, audience string) {
	s.review, s.account = true, fixtureReviewSA
	maps.Copy(s.env, map[string]string{"RUNTIME_TOKEN_AUTH": "enabled", "RUNTIME_TOKEN_REVIEW_URL": clusterAPI,
		"RUNTIME_TOKEN_REVIEW_CA":         "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
		"RUNTIME_TOKEN_REVIEW_TOKEN_FILE": "/var/run/secrets/kubernetes.io/serviceaccount/token",
		"RUNTIME_TOKEN_ISSUER":            k6auth.Issuer, "RUNTIME_TOKEN_AUDIENCE": audience, "RUNTIME_TOKEN_NAMESPACE": s.namespace,
		"RUNTIME_TOKEN_SERVICE_ACCOUNT": sa})
}
func renderedTargetConsumers(h fixtureHost, broker string) []renderedConsumer {
	var rows []renderedConsumer
	for _, role := range []string{"scheduler", "worker", "worker-control"} {
		gen := h.plan.Target.Environment["RUNTIME_KAFKA_GENERATION"]
		rows = append(rows, renderedConsumer{Role: role, Env: []renderedEnv{
			{Name: "RUNTIME_KAFKA_GENERATION", Value: &gen}, {Name: "KAFKA_BOOTSTRAP_SERVERS", Value: &broker},
		}})
	}
	return rows
}
func (a *liveApps) startRuntime(ctx context.Context, target bool) error {
	aliases, err := a.ownedAliases(ctx, target)
	if err != nil {
		return err
	}
	d := a.db
	dep, kafka, login, phase := d.source, d.broker, d.sourceWriter, "source"
	if target {
		dep, kafka, login, phase = d.target, d.fresh, d.targetWriter, "target"
	}
	api := a.runtimeSpec("api", "api", phase, dep, kafka, aliases, login)
	configureFixtureAuth(&api, fixtureSubmitSA, submitAudience)
	control := a.runtimeSpec("worker-control", "worker-control", phase, dep, kafka, aliases, login)
	configureFixtureAuth(&control, fixtureWorkerSA, workerAudience)
	controlTLS, err := a.ca.certificate(control.name, "")
	if err != nil {
		return err
	}
	conf, err := json.Marshal(struct {
		SinkURL, SinkPeer              string
		AllocatorTLS, WorkerControlTLS k6ingress.TLSFiles
	}{"https://j7-sink:8443", sinkURI, a.allocatorTLS, controlTLS})
	if err != nil {
		return err
	}
	control.private["control.json"] = string(conf)
	control.env["DUR053_K6_WORKER_CONTROL_FILE"] = "/private/control.json"
	scheduler := a.runtimeSpec("scheduler", "scheduler,ingestor", phase, dep, kafka, aliases, login)
	plan, err := json.Marshal(struct {
		Namespace  string               `json:"namespace"`
		Operations []engine.K6Operation `json:"operations"`
	}{a.ns, []engine.K6Operation{a.work.effect}})
	if err != nil {
		return err
	}
	scheduler.private["plan.json"] = string(plan)
	scheduler.env["DUR053_K6_PLAN_FILE"] = "/private/plan.json"
	scheduler.env["DUR053_K6_PLAN_SHA256"] = restoreledger.SHA256(plan)
	relay := a.runtimeSpec("relay", "relay", phase, dep, kafka, aliases, login)
	objects := []fixtureObject{}
	for _, s := range []podFixture{api, control, scheduler, relay} {
		objects = append(objects, fixturePod(fixtureImage(a.h), s)...)
	}
	held := []string{a.work.submission("f1").ID, a.work.submission("e1").ID}
	if !target {
		held = append(held, a.work.submission("d1").ID, a.work.submission("d2").ID)
	}
	for _, role := range []string{"worker-1", "worker-2"} {
		conf, err := json.Marshal(workerFixtureConfig{ControlOrigin: "https://" + control.name + ":8080",
			Broker: net.JoinHostPort(kafka.Endpoint, "19092"), CA: string(a.ca.pem), Held: held})
		if err != nil {
			return err
		}
		workerEnv := map[string]string{"KAFKA_BOOTSTRAP_SERVERS": net.JoinHostPort(kafka.Endpoint, "19092")}
		if target {
			workerEnv["RUNTIME_KAFKA_GENERATION"] = a.h.plan.Target.Environment["RUNTIME_KAFKA_GENERATION"]
		}
		s := podFixture{name: phase + "-" + role, namespace: a.ns, role: role, account: fixtureWorkerSA, command: []string{"/j7", "fixture-worker", "/private/worker.json"},
			limits: a.h.plan.Work.Roles[role], private: map[string]string{"worker.json": string(conf)}, aliases: aliases, env: workerEnv, workerTokens: true}
		objects = append(objects, fixturePod(fixtureImage(a.h), s)...)
	}
	if target {
		var generation []renderedConsumer
		for _, obj := range objects {
			if obj["kind"] != "Pod" {
				continue
			}
			m := obj["metadata"].(fixtureObject)
			role := m["labels"].(fixtureObject)["j7-role"].(string)
			if !slices.Contains([]string{"scheduler", "worker-1", "worker-2", "worker-control"}, role) {
				continue
			}
			spec := obj["spec"].(fixtureObject)
			env := spec["containers"].([]fixtureObject)[0]["env"]
			raw, err := json.Marshal(env)
			if err != nil {
				return err
			}
			var row renderedConsumer
			if err := json.Unmarshal(raw, &row.Env); err != nil {
				return err
			}
			row.Role = role
			if role == "worker-1" || role == "worker-2" {
				row.Role = "worker"
			}
			if err := checkRenderedRole(row, kafka.Endpoint+":19092"); err != nil {
				return err
			}
			if role != "worker-2" {
				generation = append(generation, row)
			}
		}
		if err := verifyGenerationRender(generation, kafka.Endpoint+":19092"); err != nil {
			return err
		}
	}
	if err := a.h.applyFixture(ctx, a.p, objects...); err != nil {
		return err
	}
	for _, name := range []string{api.name, control.name, scheduler.name, relay.name, phase + "-worker-1", phase + "-worker-2"} {
		if err := a.h.waitPod(ctx, a.ns, name); err != nil {
			return err
		}
	}
	if err := a.witnessImage(ctx, api.name, control.name, scheduler.name, relay.name, phase+"-worker-1", phase+"-worker-2", "j7-sink"); err != nil {
		return err
	}
	liveRelay, err := a.h.kubectl(ctx, "-n", a.ns, "get", "pod", relay.name, "-o",
		"jsonpath={.spec.containers[0].env[?(@.name==\""+repairedRelaySwitch+"\")].value}")
	if err != nil || string(liveRelay) != "1" {
		return errors.New("J9A_ADMISSION: running relay pod differs from the reviewed selection")
	}
	user, _, err := a.h.podToken(ctx, a.ns, fixtureSubmitSA, "submitter", submitAudience)
	if err != nil {
		return err
	}
	token, identity, err := a.h.podToken(ctx, a.ns, fixtureWorkerSA, phase+"-worker-1", workerAudience)
	if err != nil {
		return err
	}
	a.workerToken, a.workerIdentity = token, identity
	a.sinkToken, _, err = a.h.podToken(ctx, a.ns, fixtureWorkerSA, phase+"-worker-1", sinkAudience)
	if err != nil {
		return err
	}
	for _, f := range []struct {
		pod  string
		port int
	}{{api.name, 18080}, {control.name, 18443}} {
		proc, err := a.h.portForward(ctx, a.ns, f.pod, f.port, 8080)
		if err != nil {
			return err
		}
		a.forwards = append(a.forwards, proc)
	}
	tlsClient, err := fixtureClientTransport(a.ca.pem)
	if err != nil {
		return err
	}
	a.api = newFixtureHTTP("http://127.0.0.1:18080", user, nil)
	a.control = newFixtureHTTP("https://127.0.0.1:18443", token, tlsClient)
	return nil
}

func (a *liveApps) witnessImage(ctx context.Context, names ...string) error {
	args := []string{"-n", a.ns, "get", "pod"}
	args = append(args, names...)
	args = append(args, "-o", "jsonpath={.items[*].status.containerStatuses[0].imageID}")
	raw, err := a.h.kubectl(ctx, args...)
	got := strings.Fields(string(raw))
	if err != nil || len(got) != len(names) {
		return errors.New("could not observe the running fixture image inventory")
	}
	for _, image := range got {
		i := strings.LastIndex(image, "sha256:")
		if i < 0 || !restoreledger.IsSHA256(image[i+7:]) {
			return errors.New("running J7 image has no resolved digest")
		}
		digest := image[i:]
		if a.imageDigest == "" {
			a.imageDigest = digest
		}
		if a.imageDigest != digest {
			return errors.New("source and target app image identities differ")
		}
	}
	return nil
}

func checkRenderedRole(row renderedConsumer, broker string) error {
	want := map[string]string{"RUNTIME_KAFKA_GENERATION": "j7-restore", "KAFKA_BOOTSTRAP_SERVERS": broker}
	for _, entry := range row.Env {
		if v, ok := want[entry.Name]; ok {
			if entry.Value == nil || *entry.Value != v {
				return errors.New("live task configuration changed its Kafka generation or broker")
			}
			delete(want, entry.Name)
		}
	}
	if len(want) != 0 {
		return errors.New("live task configuration omitted a Kafka selection")
	}
	return nil
}

func approveThroughAdapter(ctx context.Context, handler http.Handler, token, workflow, intent, proposal string) error {
	raw, err := json.Marshal(map[string]any{"workflow_id": workflow, "node_id": "dur053.cooperating", "iteration": 0,
		"intent_id": intent, "proposal_hash": proposal, "decision": "APPROVED"})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost/v1/approval-decision", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	var decision k6auth.DecisionIdentity
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &decision) != nil || decision.IntentID != intent || decision.Decision != "APPROVED" {
		return fmt.Errorf("authenticated E1 approval did not commit (%d)", response.Code)
	}
	return nil
}
