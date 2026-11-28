package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/k6auth"
)

const (
	fixtureWorkerSA = "k6-worker"
	fixtureSubmitSA = "j7-client"
	fixtureReviewSA = "j7-token-reviewer"
	workerAudience  = "dur053-worker-control"
	submitAudience  = "j7-submissions"
	sinkAudience    = "dur053-k6-sink"
	clusterAPI      = "https://kubernetes.default.svc"
)

type fixtureObject map[string]any

func kubeObject(version, kind, namespace, name string) fixtureObject {
	m := fixtureObject{"name": name}
	if namespace != "" {
		m["namespace"] = namespace
	}
	return fixtureObject{"apiVersion": version, "kind": kind, "metadata": m}
}
func (h fixtureHost) applyFixture(ctx context.Context, p drillPaths, objects ...fixtureObject) error {
	raw, err := json.Marshal(fixtureObject{"apiVersion": "v1", "kind": "List", "items": objects})
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(p.secret, "kube-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, writeErr := f.Write(raw)
	if err := errors.Join(writeErr, f.Close()); err != nil {
		return err
	}
	if _, err := h.kubectl(ctx, "apply", "-f", f.Name()); err != nil {
		return fmt.Errorf("apply owned Kubernetes fixture: %w", err)
	}
	return nil
}
func kubeNamespace(id string) string    { return "j7-" + id }
func fixtureImage(h fixtureHost) string { return "j7-fixture:" + h.runID + "-1" }
func (h fixtureHost) loadFixtureImage(ctx context.Context) error {
	dir := filepath.Join(h.private, "image")
	if err := os.Mkdir(dir, 0o700); err != nil {
		return err
	}
	for _, build := range []struct{ name, path string }{{"runtime", "./cmd/runtime"}, {"sink", "./cmd/dur053-sink"}} {
		path := filepath.Join(dir, build.name)
		if _, err := h.run(ctx, "env", "CGO_ENABLED=0", "go", "-C", h.root, "build", "-p", "1", "-buildvcs=false", "-o", path, build.path); err != nil {
			return fmt.Errorf("build J7 %s: %w", build.name, err)
		}
		if err := os.Chmod(path, 0o555); err != nil {
			return err
		}
	}
	// Check the staged binary before pulling any workload pod. The invalid
	// value must be rejected by the actual compiled reader before a listener
	// or durable store is created.
	probe, cancel := context.WithTimeout(ctx, 3*time.Second)
	_, probeErr := h.run(probe, "env", repairedRelaySwitch+"=j7-admission-probe", filepath.Join(dir, "runtime"))
	cancel()
	var exit *exec.ExitError
	if !errors.As(probeErr, &exit) || !strings.Contains(string(exit.Stderr), repairedRelaySwitch+" must be 0 or 1") {
		return errors.New("J9A_ADMISSION: staged runtime did not reject a nonregistered relay value")
	}
	binary, err := os.ReadFile(filepath.Join(h.private, "j7-media"))
	if err != nil {
		return err
	}
	if err := secureWrite(filepath.Join(dir, "j7"), binary, 0o755); err != nil {
		return err
	}
	if err := os.Chmod(filepath.Join(dir, "j7"), 0o755); err != nil {
		return err
	}
	if err := os.Chmod(filepath.Join(h.private, "j7-media"), 0o755); err != nil {
		return err
	}
	script := []byte("FROM scratch\nCOPY j7 runtime sink /\nUSER 65532:65532\nENTRYPOINT [\"/j7\"]\n")
	if err := secureWrite(filepath.Join(dir, "Dockerfile"), script, 0o600); err != nil {
		return err
	}
	if _, err := h.docker(ctx, "build", "--network=none", "--label=j7.restore.run="+h.runID, "--label=j7.restore.attempt=1", "--label=j7.restore.campaign="+h.campaign, "-t", fixtureImage(h), dir); err != nil {
		return err
	}
	_, err = h.kind(ctx, "load", "docker-image", fixtureImage(h))
	return err
}
func fixtureEnv(m map[string]string) []fixtureObject {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	slices.Sort(names)
	e := make([]fixtureObject, 0, len(m))
	for _, key := range names {
		e = append(e, fixtureObject{"name": key, "value": m[key]})
	}
	return e
}

type podFixture struct {
	name, namespace, role, account string
	limits                         quota
	command                        []string
	env                            map[string]string
	private                        map[string]string
	aliases                        map[string]string
	serve                          int
	review, workerTokens           bool
}

func fixturePod(image string, s podFixture) []fixtureObject {
	pod := kubeObject("v1", "Pod", s.namespace, s.name)
	pod["metadata"].(fixtureObject)["labels"] = fixtureObject{"app": s.name, "j7-role": s.role}
	containerSecurity := fixtureObject{"readOnlyRootFilesystem": true, "allowPrivilegeEscalation": false,
		"capabilities": fixtureObject{"drop": []string{"ALL"}}}
	container := fixtureObject{"name": s.name, "image": image, "imagePullPolicy": "Never", "command": s.command,
		"env": fixtureEnv(s.env), "securityContext": containerSecurity,
		"resources": fixtureObject{"limits": fixtureObject{"cpu": strconv.Itoa(s.limits.MilliCPU) + "m", "memory": strconv.FormatInt(s.limits.Bytes, 10)},
			"requests": fixtureObject{"cpu": "30m", "memory": "24Mi"}}}
	spec := fixtureObject{"restartPolicy": "Never", "automountServiceAccountToken": s.review,
		"securityContext": fixtureObject{"runAsNonRoot": true, "runAsUser": 65532, "runAsGroup": 65532, "fsGroup": 65532,
			"seccompProfile": fixtureObject{"type": "RuntimeDefault"}},
		"containers": []fixtureObject{container}, "enableServiceLinks": false}
	pod["spec"] = spec
	if s.account != "" {
		spec["serviceAccountName"] = s.account
	}
	if s.serve != 0 {
		if len(s.command) > 0 && s.command[0] == "/runtime" {
			scheme := "HTTP"
			if s.role == "worker-control" {
				scheme = "HTTPS"
			}
			container["readinessProbe"] = fixtureObject{"httpGet": fixtureObject{"path": "/readyz", "port": s.serve, "scheme": scheme}, "periodSeconds": 1}
		} else {
			container["readinessProbe"] = fixtureObject{"tcpSocket": fixtureObject{"port": s.serve}, "periodSeconds": 1}
		}
	}
	aliases := []fixtureObject{}
	for name, ip := range s.aliases {
		aliases = append(aliases, fixtureObject{"ip": ip, "hostnames": []string{name}})
	}
	if len(aliases) > 0 {
		spec["hostAliases"] = aliases
	}
	var vols, mounts []fixtureObject
	objs := []fixtureObject{}
	if len(s.private) > 0 {
		secret := kubeObject("v1", "Secret", s.namespace, s.name+"-config")
		secret["stringData"] = s.private
		secret["immutable"] = true
		objs = append(objs, secret)
		vols = append(vols, fixtureObject{"name": "sealed", "secret": fixtureObject{"secretName": s.name + "-config", "defaultMode": 288}},
			fixtureObject{"name": "private", "emptyDir": fixtureObject{"medium": "Memory", "sizeLimit": "1Mi"}})
		mounts = append(mounts, fixtureObject{"name": "private", "mountPath": "/private", "readOnly": true})
		spec["initContainers"] = []fixtureObject{{"name": "install", "image": image, "imagePullPolicy": "Never",
			"command": []string{"/j7", "fixture-install"}, "securityContext": containerSecurity,
			"volumeMounts": []fixtureObject{{"name": "sealed", "mountPath": "/sealed", "readOnly": true}, {"name": "private", "mountPath": "/private"}}}}
	}
	if s.workerTokens {
		vols = append(vols, fixtureObject{"name": "tokens", "projected": fixtureObject{"sources": []fixtureObject{
			{"serviceAccountToken": fixtureObject{"audience": workerAudience, "path": "worker", "expirationSeconds": 1800}},
		}}})
		mounts = append(mounts, fixtureObject{"name": "tokens", "mountPath": "/tokens", "readOnly": true})
	}
	if len(vols) > 0 {
		spec["volumes"] = vols
		container["volumeMounts"] = mounts
	}
	objs = append(objs, pod)
	if s.serve != 0 {
		svc := kubeObject("v1", "Service", s.namespace, s.name)
		svc["spec"] = fixtureObject{"selector": fixtureObject{"app": s.name}, "ports": []fixtureObject{{"port": s.serve, "targetPort": s.serve}}}
		objs = append(objs, svc)
	}
	return objs
}
func installFixtureSecrets() error {
	rows, err := os.ReadDir("/sealed")
	if err != nil {
		return err
	}
	for _, file := range rows {
		name := file.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join("/sealed", name))
		if err != nil || len(raw) > 128<<10 {
			return errors.New("unreadable J7 mounted private input")
		}
		if err := secureWrite(filepath.Join("/private", name), raw, 0o600); err != nil {
			return err
		}
	}
	return nil
}

func (h fixtureHost) installNamespace(ctx context.Context, p drillPaths, ns string) error {
	objects := []fixtureObject{kubeObject("v1", "Namespace", "", ns)}
	for _, sa := range []string{fixtureWorkerSA, fixtureSubmitSA, fixtureReviewSA, "k6-approver"} {
		o := kubeObject("v1", "ServiceAccount", ns, sa)
		o["automountServiceAccountToken"] = false
		objects = append(objects, o)
	}
	role := kubeObject("rbac.authorization.k8s.io/v1", "ClusterRole", "", h.cluster+"-"+ns+"-review")
	role["rules"] = []fixtureObject{{"apiGroups": []string{"authentication.k8s.io"}, "resources": []string{"tokenreviews"}, "verbs": []string{"create"}}}
	bind := kubeObject("rbac.authorization.k8s.io/v1", "ClusterRoleBinding", "", h.cluster+"-"+ns+"-review")
	bind["roleRef"] = fixtureObject{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": h.cluster + "-" + ns + "-review"}
	bind["subjects"] = []fixtureObject{{"kind": "ServiceAccount", "name": fixtureReviewSA, "namespace": ns}}
	objects = append(objects, role, bind, kubeObject("v1", "Secret", ns, "approval-binding"))
	for _, client := range []string{"submitter"} {
		pod := fixturePod(fixtureImage(h), podFixture{name: client, namespace: ns, role: "client", account: fixtureSubmitSA,
			limits: quota{100, 32 << 20}, command: []string{"/j7", "fixture-idle"}})
		objects = append(objects, pod...)
	}
	return h.applyFixture(ctx, p, objects...)
}

func (h fixtureHost) podToken(ctx context.Context, ns, sa, pod, audience string) (string, string, error) {
	uid, err := h.kubectl(ctx, "-n", ns, "get", "pod", pod, "-o", "jsonpath={.metadata.uid}")
	if err != nil || len(uid) == 0 {
		return "", "", errors.New("fixture token has no bound pod")
	}
	raw, err := h.kubectl(ctx, "-n", ns, "create", "token", sa, "--audience="+audience, "--duration=30m",
		"--bound-object-kind=Pod", "--bound-object-name="+pod, "--bound-object-uid="+string(uid))
	if err != nil || len(strings.Fields(string(raw))) != 1 {
		return "", "", errors.New("could not mint pod-bound fixture token")
	}
	return strings.TrimSpace(string(raw)), "system:serviceaccount:" + ns + ":" + sa + ":pod:" + pod + ":uid:" + string(uid), nil
}

func (h fixtureHost) waitPod(ctx context.Context, ns, name string) error {
	_, err := h.kubectl(ctx, "-n", ns, "wait", "--for=condition=Ready", "pod/"+name, "--timeout=40s")
	if err != nil {
		return fmt.Errorf("fixture pod %s did not become ready: %w", name, err)
	}
	return nil
}

type heldProcess struct {
	cancel context.CancelFunc
	done   chan error
}

func (p *heldProcess) stop() {
	if p != nil {
		p.cancel()
		<-p.done
	}
}
func (h fixtureHost) portForward(ctx context.Context, ns, pod string, local, remote int) (*heldProcess, error) {
	run, stop := context.WithCancel(ctx)
	args := []string{"--kubeconfig", h.kubeconfig, "--context", "kind-" + h.cluster, "-n", ns,
		"port-forward", "--address=127.0.0.1", "pod/" + pod, fmt.Sprintf("%d:%d", local, remote)}
	cmd := exec.CommandContext(run, filepath.Join(h.private, "bin", "kubectl"), args...)
	ch := make(chan error, 1)
	if err := cmd.Start(); err != nil {
		stop()
		return nil, err
	}
	go func() { ch <- cmd.Wait(); close(ch) }()
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(local))
	p := &heldProcess{stop, ch}
	wait, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	for {
		conn, err := (&net.Dialer{Timeout: 100 * time.Millisecond}).DialContext(wait, "tcp", address)
		if err == nil {
			conn.Close()
			return p, nil
		}
		select {
		case err := <-ch:
			stop()
			return nil, fmt.Errorf("fixture port-forward exited before binding: %v", err)
		case <-wait.Done():
			p.stop()
			return nil, errors.New("fixture port-forward did not bind")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (h fixtureHost) inspectDependencyIP(ctx context.Context, id string) (string, error) {
	if !dockerID.MatchString(id) {
		return "", errors.New("unbound dependency container")
	}
	raw, err := h.docker(ctx, "container", "inspect", "--format", `{{(index .NetworkSettings.Networks "kind").IPAddress}}`, id)
	ip := strings.TrimSpace(string(raw))
	if p := net.ParseIP(ip); err != nil || p == nil || p.To4() == nil || p.IsLoopback() {
		return "", errors.New("dependency is not attached only to the private kind route")
	}
	return ip, nil
}
func (h fixtureHost) kubeConnection(ctx context.Context, ns string) (endpoint string, ca []byte, token string, err error) {
	raw, err := h.kubectl(ctx, "config", "view", "--minify", "--raw", "-o", "json")
	if err != nil {
		return "", nil, "", err
	}
	var cfg struct {
		Clusters []struct {
			Cluster struct {
				Server string `json:"server"`
				CA     string `json:"certificate-authority-data"`
			} `json:"cluster"`
		} `json:"clusters"`
	}
	if json.Unmarshal(raw, &cfg) != nil || len(cfg.Clusters) != 1 {
		return "", nil, "", errors.New("cannot bind kube API connection")
	}
	ca, err = base64.StdEncoding.DecodeString(cfg.Clusters[0].Cluster.CA)
	if err != nil {
		return "", nil, "", err
	}
	credential, err := h.kubectl(ctx, "-n", ns, "create", "token", fixtureReviewSA, "--duration=30m")
	if err != nil {
		return "", nil, "", errors.New("cannot issue reviewer credential")
	}
	return cfg.Clusters[0].Cluster.Server, ca, strings.TrimSpace(string(credential)), nil
}
func (h fixtureHost) approveFixture(ctx context.Context, ns string, clock orphanClock, d drillDatabases,
	workflow, intent, proposal string) error {
	uid := func(kind, name string) (string, error) {
		raw, err := h.kubectl(ctx, "-n", ns, "get", kind, name, "-o", "jsonpath={.metadata.uid}")
		return string(raw), err
	}
	sa, err := uid("serviceaccount", "k6-approver")
	if err != nil {
		return err
	}
	secret, err := uid("secret", "approval-binding")
	if err != nil {
		return err
	}
	raw, err := h.kubectl(ctx, "-n", ns, "create", "token", "k6-approver", "--duration=30m", "--audience="+k6auth.Audience,
		"--bound-object-kind=Secret", "--bound-object-name=approval-binding", "--bound-object-uid="+secret)
	if err != nil {
		return err
	}
	endpoint, ca, reviewerToken, err := h.kubeConnection(ctx, ns)
	if err != nil {
		return err
	}
	reviewer, err := k6auth.NewKubernetesReviewer(endpoint, ca, reviewerToken)
	if err != nil {
		return err
	}
	writer, err := k6auth.OpenDecisionWriter(ctx, d.decision.local(d.source, engineDatabase, d.ca, false))
	if err != nil {
		return err
	}
	defer writer.Close()
	adapter, err := k6auth.NewAdapter(k6auth.Episode{Namespace: ns, ServiceAccountUID: sa, SecretName: "approval-binding",
		SecretUID: secret, WarmupStart: clock.StartUTC, LastPlannedArrival: clock.StartUTC, TwoL: grantWindow}, reviewer, writer)
	if err != nil {
		return err
	}
	return approveThroughAdapter(ctx, adapter, strings.TrimSpace(string(raw)), workflow, intent, proposal)
}

func (h fixtureHost) teardownNamespace(ctx context.Context, drill string) error {
	ns := kubeNamespace(drill)
	var failures []error
	for _, resource := range []string{"namespace/" + ns, "clusterrolebinding/" + h.cluster + "-" + ns + "-review",
		"clusterrole/" + h.cluster + "-" + ns + "-review"} {
		_, err := h.kubectl(ctx, "delete", resource, "--ignore-not-found", "--wait=true", "--timeout=40s")
		if err != nil {
			failures = append(failures, err)
		}
		raw, err := h.kubectl(ctx, "get", resource, "--ignore-not-found", "-o", "name")
		if err != nil || len(strings.TrimSpace(string(raw))) > 0 {
			failures = append(failures, fmt.Errorf("unverified fixture resource removal: %s", resource))
		}
	}
	return errors.Join(failures...)
}
