package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"durable-agent-execution-engine/internal/restoreledger"
)

func controllerTestRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestControllerCandidateAdmission(t *testing.T) {
	source := controllerTestRoot(t)
	dir := filepath.Clean(t.TempDir())
	var c hostedCandidate
	if _, err := readFaultJSON(filepath.Join(source, "deploy/j7/candidate.json"), &c); err != nil {
		t.Fatal(err)
	}
	copied := slices.Clone(hostedCandidateFiles)
	put := func(name string, data []byte) {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		c.Files[name] = restoreledger.SHA256(data)
	}
	putJSON := func(name string, value any) {
		t.Helper()
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		put(name, data)
	}
	seal := func() {
		t.Helper()
		raw, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		// The candidate is the outer seal, not a member of itself.
		if err := os.WriteFile(filepath.Join(dir, "deploy/j7/candidate.json"), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range copied {
		data, err := evidenceFile(source, name)
		if err != nil || restoreledger.SHA256(data) != c.Files[name] {
			t.Fatalf("production candidate seal differs at %s: %v", name, err)
		}
		put(name, data)
	}
	registryPath := "deploy/runtime-switches.json"
	data, err := evidenceFile(dir, registryPath)
	if err != nil {
		t.Fatal(err)
	}
	var registry struct {
		Schema   int                        `json:"schema_version"`
		Switches map[string]json.RawMessage `json:"switches"`
	}
	if err := json.Unmarshal(data, &registry); err != nil {
		t.Fatal(err)
	}
	row := json.RawMessage(`{
		"task":"J9a","unset_default":"0","values":{"0":"legacy","1":"repaired"},
		"permitted_enabled_profiles":["J7_source","J7_target"],
		"required_unset_profiles":["K5","K6_before_Phase_G","K6_Phase_G"]
	}`)
	put(relayReaderPath, []byte("package main\nconst relaySwitch = \""+repairedRelaySwitch+"\"\n"))
	for _, tc := range []struct {
		name string
		edit func()
		want string
	}{
		{"premerge", func() { delete(registry.Switches, repairedRelaySwitch) }, "J9A_ADMISSION"},
		{"positive", func() { registry.Switches[repairedRelaySwitch] = row }, ""},
		{"wrong-profile", func() {
			registry.Switches[repairedRelaySwitch] = json.RawMessage(bytes.Replace(row, []byte("J7_source"), []byte("none"), 1))
		}, "J9A_ADMISSION"},
		{"digest-drift", func() { registry.Switches[repairedRelaySwitch] = row; c.Files["deploy/j7/workload.json"] = testDigest }, "unbound"},
		{"source-generation", func() {
			registry.Switches[repairedRelaySwitch] = row
			p, err := readControllerPlan(source)
			if err != nil {
				t.Fatal(err)
			}
			p.Source.Environment["RUNTIME_KAFKA_GENERATION"] = "j7-restore"
			putJSON("deploy/j7/source-overlay.json", p.Source)
			work, err := evidenceFile(source, "deploy/j7/workload.json")
			if err != nil {
				t.Fatal(err)
			}
			put("deploy/j7/workload.json", work)
		}, "invalid J7_source"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.edit()
			putJSON(registryPath, registry)
			seal()
			if tc.name == "positive" || tc.name == "premerge" {
				before, err := evidenceFile(dir, candidatePath)
				if err != nil {
					t.Fatal(err)
				}
				err = resealCandidate(dir, io.Discard)
				if tc.name == "positive" {
					if err != nil {
						t.Fatal(err)
					}
				} else {
					after, readErr := evidenceFile(dir, candidatePath)
					if err == nil || readErr != nil || !bytes.Equal(before, after) {
						t.Fatalf("unregistered switch changed the candidate: %v", err)
					}
				}
			}
			var out bytes.Buffer
			err := runHostedGate([]string{"hosted-gate", "--root", dir, "--mode", "qualification"}, &out, io.Discard)
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) || out.Len() != 0 {
					t.Fatalf("gate %v: %s", err, out.String())
				}
				return
			}
			var verdict struct {
				Candidate string `json:"candidate_sha256"`
				Forecast  int    `json:"forecast_minutes"`
			}
			if err != nil || json.Unmarshal(out.Bytes(), &verdict) != nil || !restoreledger.IsSHA256(verdict.Candidate) || verdict.Forecast != 105 {
				t.Fatalf("positive candidate gate %v: %s", err, out.String())
			}
		})
	}
}

func TestControllerDependencyAndStageCommands(t *testing.T) {
	root := controllerTestRoot(t)
	var out bytes.Buffer
	if err := runHostedController(t.Context(), []string{"hosted-controller", root, "dry-run"}, &out); err != nil {
		t.Fatal(err)
	}
	var p controllerDryRun
	if err := json.Unmarshal(out.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	if p.Plan.Work.Topology.Nodes != 1 || len(p.Dependencies) != 5 ||
		len(p.Commands) != 3 || len(p.Stages) != 14 || len(p.WorkloadIDs) != 9 ||
		!slices.Equal(p.Sequence, drillSequence) || p.SourceActions[12] != "release-e1" ||
		!slices.Contains(p.Reopen, "--network=kind") || !slices.Contains(p.Reopen, "archive_mode=off") ||
		!slices.Equal(p.ResourcePhases, []string{"start", "backup", "outage", "replay", "export"}) || p.Plan.Source.Environment["RUNTIME_KAFKA_GENERATION"] != "" ||
		p.Plan.Target.Environment["RUNTIME_KAFKA_GENERATION"] != "j7-restore" {
		t.Fatalf("dry run incomplete: %s", out.String())
	}
	for _, args := range p.Commands {
		if !slices.Contains(args, "--pull=never") || !slices.Contains(args, "--network=kind") ||
			!slices.Contains(args, "--label=j7.restore.run=1") {
			t.Fatalf("unsealed dependency command %v", args)
		}
		for _, arg := range args {
			if strings.HasPrefix(arg, "--publish=") && !strings.HasPrefix(arg, "--publish=127.0.0.1:") {
				t.Fatalf("dependency opened a non-loopback listener: %s", arg)
			}
		}
	}
	if !slices.Contains(p.Commands[0], "archive_command=/j7/bin/j7-media archive /j7/archive %p %f") ||
		!slices.Contains(p.Commands[1], "max_connections=16") ||
		!slices.Contains(p.Commands[2], "--env=KAFKA_HEAP_OPTS=-Xms512m -Xmx512m") {
		t.Fatalf("source command drift: %v", p.Commands)
	}
	files := stageFiles{Root: t.TempDir(), Restore: testDigest, Fresh: testDigest, Watch: testDigest, Engine: testDigest}
	stage, err := files.mediaStage("verify-redispatch")
	if err != nil || !slices.Contains(stage.Args, filepath.Join(files.Root, "engine-final.json")) {
		t.Fatalf("redispatch witness stage %+v: %v", stage, err)
	}
	files.Watch = ""
	if _, err := files.mediaStage("verify-redispatch"); err == nil {
		t.Fatal("redispatch accepted no watch seal")
	}
}

type endpointTestJournal struct {
	h restoreledger.Header
	w []restoreledger.Witness
}

func (j *endpointTestJournal) Header() restoreledger.Header { return j.h }
func (j *endpointTestJournal) RecordWitness(w restoreledger.Witness) error {
	j.w = append(j.w, w)
	return nil
}

func TestControllerEndpointBinding(t *testing.T) {
	f := buildDrillFixture(t)
	dir := filepath.Clean(t.TempDir())
	for _, name := range []string{"restore.json", "kafka-fresh.json"} {
		f.write(dir, name, f.files[name])
	}
	var p readyEndpoint
	if err := json.Unmarshal(f.files["endpoint.json"], &p); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []string{"bound", "wrong-restore", "wrong-timeline", "early-open", "early-denial", "persist-error"} {
		t.Run(tc, func(t *testing.T) {
			sha := p.RestoreSHA
			o := endpointObservation{OldDeniedNS: p.OldDeniedNS, ForeignDeniedNS: p.ForeignDeniedNS,
				OpenNS: p.OpenNS, SystemID: p.SystemID, Timeline: p.Timeline}
			switch tc {
			case "wrong-restore":
				sha = testDigest
			case "wrong-timeline":
				o.Timeline--
			case "early-open":
				o.OpenNS = o.OldDeniedNS
			case "early-denial":
				o.ForeignDeniedNS = f.ns(88)
			}
			j := endpointTestJournal{h: f.header}
			err := bindControllerEndpoint(dir, &j, sha, p.KafkaSHA, o, func(path string, got readyEndpoint) (string, error) {
				if tc == "persist-error" {
					return "", io.ErrUnexpectedEOF
				}
				if tc != "bound" {
					t.Fatalf("invalid endpoint passed binding: %+v", got)
				}
				if path != filepath.Join(dir, "endpoint.json") || p != got {
					t.Fatalf("endpoint %+v", got)
				}
				return restoreledger.SHA256(f.files["endpoint.json"]), nil
			})
			if tc != "bound" {
				if err == nil || len(j.w) != 0 {
					t.Fatalf("invalid endpoint recorded %v (%v)", j.w, err)
				}
				return
			}
			if err != nil || len(j.w) != 1 || j.w[0].EvidenceSHA != restoreledger.SHA256(f.files["endpoint.json"]) ||
				j.w[0].Phase != "j7-target-endpoint" || j.w[0].CompletedNS != p.OpenNS {
				t.Fatalf("endpoint ledger binding %v (%v)", j.w, err)
			}
		})
	}
}

func TestControllerTeardownContinuesAfterFailure(t *testing.T) {
	id := strings.Repeat("a", 64)
	var calls [][]string
	h := fixtureHost{runID: "55", campaign: "rehearsal", cluster: "j7-55-1"}
	h.run = func(_ context.Context, command string, args ...string) ([]byte, error) {
		calls = append(calls, append([]string{command}, args...))
		switch args[1] + " " + args[2] {
		case "container ls":
			if slices.Contains(args, "label=io.x-k8s.kind.cluster=j7-55-1") {
				return nil, nil
			}
			return []byte(id), nil
		case "container inspect":
			return []byte("true"), nil
		case "container stop":
			return nil, errors.New("simulated stop failure")
		case "container rm":
			return nil, errors.New("simulated still running")
		case "volume ls":
			for _, c := range calls {
				if c[2] == "volume" && c[3] == "rm" {
					return nil, nil
				}
			}
			return []byte("j7-volume"), nil
		case "volume rm":
			return nil, nil
		default:
			t.Fatalf("unexpected command %s %v", command, args)
			return nil, nil
		}
	}
	w, err := h.observeTeardown(t.Context(), "r1")
	if err == nil || w.Verified || !slices.Equal(w.Residuals, []string{"container:" + id}) {
		t.Fatalf("teardown %+v: %v", w, err)
	}
	if len(calls) != 8 || calls[5][2] != "volume" || calls[5][3] != "rm" {
		t.Fatalf("teardown failed to attempt later removal: %v", calls)
	}
	for _, c := range calls {
		if c[0] != "docker" || c[1] != "--host=unix:///var/run/docker.sock" || slices.Contains(c, "--force") {
			t.Fatalf("unsafe cleanup %v", c)
		}
		if c[3] == "ls" && !slices.Contains(c, "label=j7.restore.drill=r1") {
			t.Fatalf("cleanup escaped drill %v", c)
		}
	}
}

func TestControllerCalicoPinning(t *testing.T) {
	p, err := readControllerPlan(controllerTestRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	const manifest = "  image: docker.io/calico/node:v3.29.3\n  image: docker.io/calico/cni:v3.29.3\n  image: docker.io/calico/kube-controllers:v3.29.3\n"
	pinned, err := pinCalicoImages(manifest, p.Lock.Images)
	if err != nil || strings.Count(pinned, "@sha256:") != 3 {
		t.Fatalf("Calico pinning: %v %s", err, pinned)
	}
	if _, err := pinCalicoImages(manifest+"  image: unregistered\n", p.Lock.Images); err == nil {
		t.Fatal("Calico admitted an unregistered image")
	}
}
