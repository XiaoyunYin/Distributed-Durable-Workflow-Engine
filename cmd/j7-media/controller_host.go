package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/restoreledger"
)

type fixtureHost struct {
	root, private, cluster, kubeconfig string
	runID                              string
	campaign                           string
	plan                               controllerPlan
	run                                runTool
}

type hostTeardown struct {
	Verified  bool     `json:"verified"`
	Residuals []string `json:"residuals"`
}

type runnerCapacity struct {
	CPUs         int              `json:"logical_cpu_count"`
	InstalledKiB int64            `json:"installed_memory_kib"`
	AvailableKiB int64            `json:"available_memory_kib"`
	SwapKiB      int64            `json:"swap_used_kib"`
	Filesystems  map[string]int64 `json:"free_kib_by_filesystem"`
}

type bootstrapWitness struct {
	Schema       string            `json:"schema"`
	CandidateSHA string            `json:"candidate_sha256"`
	Revision     string            `json:"revision"`
	Boot         string            `json:"boot_id"`
	StartNS      int64             `json:"started_ns"`
	EndNS        int64             `json:"completed_ns"`
	ControlPlane string            `json:"control_plane"`
	NodeImage    string            `json:"node_image"`
	Images       map[string]string `json:"dependency_image_ids"`
	Before       runnerCapacity    `json:"capacity_before_bootstrap"`
	After        runnerCapacity    `json:"capacity_after_bootstrap"`
}

type hostedRunnerIdentity struct {
	actions, environment, system, attempt         string
	workspace, scratch, runID, campaign, revision string
}

func runHostedController(ctx context.Context, args []string, out io.Writer) error {
	if len(args) < 3 || !filepath.IsAbs(args[1]) {
		return errors.New("hosted-controller requires an absolute checkout and a fixed action")
	}
	root := args[1]
	if len(args) == 3 {
		switch args[2] {
		case "dry-run":
			return controllerOverlayJSON(root, out)
		case "reseal":
			return resealCandidate(root, out)
		}
	}
	if args[2] != "live" || len(args) < 14 {
		return errors.New("invalid J7 live controller launcher arguments")
	}
	env := hostedRunnerIdentity{args[3], args[4], args[5], args[6], args[7], args[8], args[9], args[10], args[11]}
	action := args[12:]
	cmd, private := action[0], ""
	switch {
	case (cmd == "pre-arm" || cmd == "bootstrap" || cmd == "cleanup") && len(action) == 2:
		private = action[1]
	case cmd == "between" && len(action) == 4, cmd == "drill" && len(action) == 5:
		private = action[2]
	default:
		return errors.New("invalid J7 controller action or argument count")
	}
	host, err := newFixtureHost(root, private, env)
	if err != nil {
		return err
	}
	switch cmd {
	case "cleanup":
		return host.teardown(ctx, "")
	case "between":
		if !validControllerDrill(action[1]) || action[3] != filepath.Join(private, action[1]) {
			return errors.New("between requires the exact finished drill directory")
		}
		return errors.Join(host.teardownNamespace(ctx, action[1]), host.teardown(ctx, action[1]))
	}
	_, seal, err := checkCandidate(root)
	if err != nil {
		return err
	}
	if cmd == "bootstrap" {
		clock, boot, err := restoreledger.HostClock()
		if err != nil {
			return err
		}
		return host.bootstrap(ctx, clock, boot, seal, env.revision)
	}
	if cmd == "pre-arm" {
		return ctx.Err()
	}
	return host.runDrill(ctx, action[1], action[3], action[4], env.revision, seal)
}

func validControllerDrill(id string) bool {
	return slices.Contains([]string{"q1", "r1", "r2", "r3"}, id)
}

func newFixtureHost(root, private string, env hostedRunnerIdentity) (fixtureHost, error) {
	var h fixtureHost
	if runtime.GOOS != "linux" || env.actions != "true" ||
		env.environment != "github-hosted" || env.system != "Linux" ||
		env.attempt != "1" {
		return h, errors.New("live J7 controller requires the first attempt of a Linux GitHub-hosted job")
	}
	if root != env.workspace || !filepath.IsAbs(env.scratch) ||
		private != filepath.Join(env.scratch, "j7-private") {
		return h, errors.New("J7 workspace is outside the dedicated GitHub runner paths")
	}
	for _, dir := range []string{root, private} {
		if err := checkedEvidenceDir(dir); err != nil {
			return h, err
		}
	}
	run, campaign := env.runID, env.campaign
	id, err := strconv.ParseUint(run, 10, 64)
	if err != nil || id == 0 || strconv.FormatUint(id, 10) != run || !fixtureName.MatchString(campaign) {
		return h, errors.New("missing owned J7 run or campaign identity")
	}
	rev, err := hex.DecodeString(env.revision)
	if err != nil || len(rev) != 20 || env.revision != strings.ToLower(env.revision) {
		return h, errors.New("missing GitHub checkout identity")
	}
	p, err := readControllerPlan(root)
	if err != nil {
		return h, err
	}
	return fixtureHost{
		root: root, private: private, cluster: "j7-" + run + "-1", kubeconfig: filepath.Join(private, "kubeconfig"),
		runID: run, campaign: campaign, plan: p, run: execTool,
	}, nil
}

func (h fixtureHost) docker(ctx context.Context, args ...string) ([]byte, error) {
	return (localDocker{run: h.run}).command(ctx, args...)
}

func (h fixtureHost) kind(ctx context.Context, args ...string) ([]byte, error) {
	// kind uses the Docker CLI, whose environment must address the same local daemon.
	argv := []string{"-u", "DOCKER_CONTEXT", "DOCKER_HOST=unix:///var/run/docker.sock",
		"KIND_EXPERIMENTAL_PROVIDER=docker", filepath.Join(h.private, "bin", "kind")}
	return h.run(ctx, "env", append(append(argv, args...), "--name", h.cluster)...)
}

func (h fixtureHost) kubectl(ctx context.Context, args ...string) ([]byte, error) {
	return h.run(ctx, filepath.Join(h.private, "bin", "kubectl"),
		append([]string{"--kubeconfig", h.kubeconfig, "--context", "kind-" + h.cluster}, args...)...)
}

func readHostMemory(input io.Reader) (map[string]int64, error) {
	want := []string{"MemTotal:", "MemAvailable:", "SwapTotal:", "SwapFree:"}
	seen := make(map[string]int64, len(want))
	scan := bufio.NewScanner(input)
	for scan.Scan() {
		f := strings.Fields(scan.Text())
		if len(f) != 3 || !slices.Contains(want, f[0]) {
			continue
		}
		n, err := strconv.ParseInt(f[1], 10, 64)
		if err != nil || n < 0 || f[2] != "kB" {
			return nil, errors.New("invalid runner memory observation")
		}
		if _, ok := seen[f[0]]; ok {
			return nil, errors.New("duplicate runner memory observation")
		}
		seen[f[0]] = n
	}
	if err := scan.Err(); err != nil {
		return nil, err
	}
	if len(seen) != len(want) || seen["SwapFree:"] > seen["SwapTotal:"] {
		return nil, errors.New("incomplete runner memory observation")
	}
	return seen, nil
}

func (h fixtureHost) hostCapacity(ctx context.Context, bootstrapping bool) (runnerCapacity, error) {
	var sample runnerCapacity
	raw, err := os.Open("/proc/meminfo")
	if err != nil {
		return sample, err
	}
	defer raw.Close()
	mem, err := readHostMemory(raw)
	if err != nil {
		return sample, err
	}
	output, err := h.run(ctx, "nproc")
	if err != nil {
		return sample, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(output)))
	if err != nil || n < 4 || mem["MemTotal:"] < 12<<20 || mem["MemAvailable:"] < 2<<20 {
		return sample, errors.New("J7_CAPACITY: runner CPU or memory floor")
	}
	sample = runnerCapacity{
		CPUs: n, InstalledKiB: mem["MemTotal:"], AvailableKiB: mem["MemAvailable:"],
		SwapKiB: mem["SwapTotal:"] - mem["SwapFree:"], Filesystems: map[string]int64{},
	}
	freeFloor := int64(8 << 20)
	if bootstrapping {
		freeFloor = 4 << 20
	}
	for role, p := range map[string]string{"workspace": h.private, "docker": "/var/lib/docker"} {
		out, err := h.run(ctx, "df", "-Pk", "--", p)
		if err != nil {
			return sample, err
		}
		lines := strings.Split(strings.TrimSpace(string(out)), "\n")
		f := strings.Fields(lines[len(lines)-1])
		if len(lines) != 2 || len(f) < 6 {
			return sample, errors.New("invalid runner filesystem observation")
		}
		n, err := strconv.ParseInt(f[3], 10, 64)
		if err != nil || n < freeFloor {
			return sample, errors.New("J7_CAPACITY: runner filesystem floor")
		}
		sample.Filesystems[role] = n
	}
	if _, err := os.Stat(filepath.Join(h.private, "resource-stop")); !errors.Is(err, os.ErrNotExist) {
		return sample, errors.New("J7_CAPACITY: resource stop or unavailable stop witness")
	}
	return sample, nil
}

func (h fixtureHost) fetch(ctx context.Context, d controllerTool, path string, executable bool) error {
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return errors.New("pinned J7 tooling destination must be new")
	}
	_, err := h.run(ctx, "curl", "--fail", "--silent", "--show-error", "--location", "--proto", "=https",
		"--proto-redir", "=https", "--max-time", "120", "--output", path, d.URL)
	if err != nil {
		return err
	}
	raw, err := evidenceFile(filepath.Dir(path), filepath.Base(path))
	if err != nil || restoreledger.SHA256(raw) != d.Digest {
		return fmt.Errorf("J7 pinned tooling digest mismatch for %s", filepath.Base(path))
	}
	if executable {
		return os.Chmod(path, 0o700)
	}
	return nil
}

func (h fixtureHost) bootstrap(ctx context.Context, clock restoreledger.Clock, boot, candidate, revision string) (err error) {
	before, err := h.hostCapacity(ctx, true)
	if err != nil {
		return err
	}
	start, err := clock()
	if err != nil {
		return err
	}
	w := bootstrapWitness{Schema: "j7.bootstrap.v1", Boot: boot, CandidateSHA: candidate,
		Revision: revision, StartNS: start, NodeImage: h.plan.Lock.Node, Images: map[string]string{}, Before: before}
	bin := filepath.Join(h.private, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		return err
	}
	for _, name := range []string{"kind", "kubectl"} {
		if err := h.fetch(ctx, h.plan.Lock.Tools[name], filepath.Join(bin, name), true); err != nil {
			return err
		}
	}
	calico := filepath.Join(h.private, "calico.yaml")
	if err := h.fetch(ctx, h.plan.Lock.Addons["calico"], calico, false); err != nil {
		return err
	}
	for _, image := range []string{h.plan.Lock.Node, h.plan.Lock.Postgres, h.plan.Lock.Kafka} {
		if _, err := h.docker(ctx, "image", "pull", image); err != nil {
			return err
		}
		r, err := h.docker(ctx, "image", "inspect", "--format", "{{.Id}}", image)
		id, ok := strings.CutPrefix(strings.TrimSpace(string(r)), "sha256:")
		if err != nil || !ok || !restoreledger.IsSHA256(id) {
			return errors.New("pinned dependency image identity could not be resolved")
		}
		w.Images[image] = strings.TrimSpace(string(r))
	}
	pinned := filepath.Join(h.private, "calico-pinned.yaml")
	raw, err := evidenceFile(h.private, "calico.yaml")
	if err != nil {
		return err
	}
	manifest, err := pinCalicoImages(string(raw), h.plan.Lock.Images)
	if err != nil {
		return err
	}
	if err := os.WriteFile(pinned, []byte(manifest), 0o600); err != nil {
		return err
	}
	if err := h.createCluster(ctx, pinned, &w); err != nil {
		return err
	}
	if err := h.loadFixtureImage(ctx); err != nil {
		return errors.Join(err, h.cleanupAfterFailure())
	}
	w.After, err = h.hostCapacity(ctx, false)
	if err != nil {
		return errors.Join(err, h.cleanupAfterFailure())
	}
	if w.After.SwapKiB > w.Before.SwapKiB {
		return errors.Join(errors.New("J7_CAPACITY: swap grew during kind bootstrap"), h.cleanupAfterFailure())
	}
	w.EndNS, err = clock()
	if err != nil {
		return errors.Join(err, h.cleanupAfterFailure())
	}
	if err := writeControllerJSON(filepath.Join(h.private, "bootstrap.json"), w); err != nil {
		return errors.Join(err, h.cleanupAfterFailure())
	}
	return nil
}

func (h fixtureHost) createCluster(ctx context.Context, calico string, w *bootstrapWitness) (err error) {
	config := filepath.Join(h.root, "deploy/j7/kind.json")
	// Exact name and kind label, then inspect again on failure even when the
	// creation command did not report a successful response.
	if err := h.checkClusterAbsent(ctx); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, h.cleanupAfterFailure())
		}
	}()
	if _, err := h.kind(ctx, "create", "cluster", "--image", h.plan.Lock.Node, "--config", config, "--kubeconfig", h.kubeconfig); err != nil {
		return err
	}
	if _, err := h.kubectl(ctx, "apply", "--filename", calico); err != nil {
		return err
	}
	if _, err := h.kubectl(ctx, "wait", "--for=condition=Ready", "node/"+h.cluster+"-control-plane", "--timeout=5m"); err != nil {
		return err
	}
	nodes, err := h.kubectl(ctx, "get", "nodes", "-o", "name")
	w.ControlPlane = "node/" + h.cluster + "-control-plane"
	if err != nil || !slices.Equal(strings.Fields(string(nodes)), []string{w.ControlPlane}) {
		return errors.New("J7 fixture does not have the sealed one-node inventory")
	}
	return nil
}

func (h fixtureHost) cleanupAfterFailure() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	return h.teardown(ctx, "")
}

func writeControllerJSON[T any](name string, value T) error {
	_, _, err := faultReceipt(name, func() (T, error) { return value, nil })
	return err
}

func (h fixtureHost) checkClusterAbsent(ctx context.Context) error {
	nodes, err := h.docker(ctx, "container", "ls", "-aq", "--filter", "label=io.x-k8s.kind.cluster="+h.cluster)
	if err != nil || len(strings.Fields(string(nodes))) != 0 {
		return errors.New("one-node kind name must be vacant before bootstrap")
	}
	return nil
}

func pinCalicoImages(manifest string, locked map[string]string) (string, error) {
	seen := map[string]bool{}
	lines := strings.Split(manifest, "\n")
	for i, line := range lines {
		image, ok := strings.CutPrefix(strings.TrimSpace(line), "image:")
		if !ok {
			continue
		}
		image = strings.Trim(strings.TrimSpace(image), "'\"")
		pinned := locked[image]
		digest, ok := strings.CutPrefix(pinned, image+"@sha256:")
		if !strings.HasPrefix(image, "docker.io/calico/") || !ok || !restoreledger.IsSHA256(digest) {
			return "", fmt.Errorf("unsealed Calico image %q", image)
		}
		seen[image] = true
		lines[i] = line[:len(line)-len(strings.TrimLeft(line, " "))] + "image: " + pinned
	}
	if len(seen) != 3 {
		return "", errors.New("Calico must declare all three pinned images")
	}
	return strings.Join(lines, "\n"), nil
}
