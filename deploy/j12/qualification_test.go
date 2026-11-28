package main

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/state"
)

func checkedRegistry(t *testing.T) registry {
	t.Helper()
	var parsed registry
	if err := readFileJSON("../runtime-switches.json", &parsed); err != nil {
		t.Fatal(err)
	}
	return parsed
}

func TestJ12RelayAdmission(t *testing.T) {
	for _, tc := range []struct {
		name   string
		edit   func(registry)
		key, v string
		pass   bool
	}{
		{"accepted", func(registry) {}, relaySwitch, "1", true},
		{"absent", func(r registry) { delete(r.Switches, relaySwitch) }, relaySwitch, "1", false},
		{"ambiguous", func(r registry) { r.Switches["RUNTIME_SECOND"] = r.Switches[relaySwitch] }, relaySwitch, "1", false},
		{"missing-profile", func(r registry) {
			row := r.Switches[relaySwitch]
			row.Profiles = []string{"J9a"}
			r.Switches[relaySwitch] = row
		}, relaySwitch, "1", false},
		{"forbidden-profile", func(r registry) {
			row := r.Switches[relaySwitch]
			row.Unset = []string{"J12"}
			r.Switches[relaySwitch] = row
		}, relaySwitch, "1", false},
		{"unregistered-value", func(r registry) { row := r.Switches[relaySwitch]; delete(row.Values, "1") }, relaySwitch, "1", false},
		{"default", func(registry) {}, relaySwitch, "0", false},
		{"blank", func(registry) {}, "", "", false},
		{"unregistered-switch", func(registry) {}, authSwitch, "1", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := checkedRegistry(t)
			for key, row := range r.Switches {
				if row.Task == "J9a" {
					delete(r.Switches, key)
				}
			}
			r.Switches[relaySwitch] = switchRow{Task: "J9a", Default: "0", Values: map[string]string{"0": "legacy", "1": "repaired"}, Profiles: []string{"J12"}}
			tc.edit(r)
			if err := selectRelay(r, tc.key, tc.v); (err == nil) != tc.pass {
				t.Fatalf("admission result %v, want pass=%t", err, tc.pass)
			}
		})
	}
}

func TestJ12MigrationIsolation(t *testing.T) {
	r := checkedRegistry(t)
	base, err := filepath.Glob("../../migrations/*.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	files, err := migrationInventory(r, base)
	if err != nil || len(files) != 24 || files[23] != requesterMigration {
		t.Fatalf("migration inventory %v: %v", files, err)
	}
	if _, err := os.Stat(filepath.Join("../..", requesterMigration)); err != nil {
		t.Fatal(err)
	}
	if _, err := migrationInventory(r, base[1:]); err == nil {
		t.Fatal("missing base migration passed")
	}
	base[10] = "migrations/123456_foreign.up.sql"
	if _, err := migrationInventory(r, base); err == nil {
		t.Fatal("unexpected base version passed")
	}
	profile := r.Inventories["J12"]
	r.Inventories["K6_Phase_G"] = profile
	if _, err := migrationInventory(r, files[:23]); err == nil {
		t.Fatal("J-only schema escaped into K6")
	}
}

func TestJ12HostedMatrixMatchesRegisteredMethods(t *testing.T) {
	raw, err := os.ReadFile("../../experiments/portfolio/public-api-auth/protocol.md")
	if err != nil {
		t.Fatal(err)
	}
	type methodPath struct{ method, path string }
	want := map[methodPath]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "| GET") && !strings.HasPrefix(line, "| POST") {
			continue
		}
		cells := strings.Split(strings.Trim(line, "|"), "|")
		if len(cells) != 6 {
			t.Fatalf("wrong registered matrix shape: %s", line)
		}
		for _, method := range strings.Split(strings.TrimSpace(cells[0]), ", ") {
			for i, cell := range cells[2:] {
				if strings.TrimSpace(cell) != "—" {
					want[methodPath{method, strings.Trim(strings.TrimSpace(cells[1]), "`")}] = listeners[i]
				}
			}
		}
	}
	f := fixtures{cancelID: "test-request", intents: map[string]state.ApprovalIntent{"j12-own": {IntentID: "test-intent"}}}
	paths := strings.NewReplacer("j12-own", "{workflowID}", "j12-cancel", "{workflowID}", "j12-approval-create", "{workflowID}", "j12-worker", "{workflowID}",
		"test-request", "{requestID}", "test-intent", "{intentID}", node, "{nodeID}", "/iterations/0/", "/iterations/{iteration}/")
	for _, r := range matrix {
		key := methodPath{r.method, paths.Replace(f.requestFor(r).Path)}
		if want[key] != r.listener {
			t.Fatalf("unregistered matrix pairing: %+v %s", key, r.listener)
		}
		delete(want, key)
		allowed := 0
		for _, listener := range listeners {
			for _, credential := range credentialNames {
				got, code := expectation(r, listener, credential)
				if listener != r.listener && got != 404 || listener == r.listener && credential != r.credential && got != 401 {
					t.Fatalf("unexpected matrix denial %+v %s %s", r, listener, credential)
				}
				if got < 400 {
					allowed++
				}
				if r.method == "HEAD" && code != "" {
					t.Fatal("HEAD should have no HTTP body")
				}
			}
		}
		if allowed != 1 {
			t.Fatalf("%s has %d accepted credential paths", r.id, allowed)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing registered routes: %v", want)
	}
	ids := controlIDs()
	slices.Sort(ids)
	if len(slices.Compact(ids)) != len(ids) || len(matrix) != 16 || len(controlIDs()) != 389 {
		t.Fatalf("incorrect or duplicate control inventory: %d", len(controlIDs()))
	}
}

func TestJ12RenderedIdentityBoundary(t *testing.T) {
	h := newHost(inputs{run: "123", attempt: "1", relayName: relaySwitch, relayValue: "1"})
	h.dbURL = "fixture-db"
	h.kafkaIP = "172.20.0.5"
	items := h.render("demand-secret-in-memory")
	services := 0
	for _, item := range items {
		if item["kind"] == "Service" {
			services++
			spec := item["spec"].(object)
			if spec["type"] != nil || spec["externalIPs"] != nil {
				t.Fatal("service escapes in-cluster fixture")
			}
		}
	}
	if services != 4 || len(h.rendered) != 7 {
		t.Fatal("unexpected sandbox topology")
	}
	for name, pod := range h.rendered {
		containers := pod["containers"].([]object)
		if pod["automountServiceAccountToken"] != false || len(containers) != 1 {
			t.Fatalf("incorrect pod identity settings: %s", name)
		}
		env := map[string]string{}
		for _, pair := range containers[0]["env"].([]object) {
			env[pair["name"].(string)] = pair["value"].(string)
		}
		switch name {
		case "operator-probe", "worker-probe":
			role := strings.TrimSuffix(name, "-probe")
			volume := pod["volumes"].([]object)[0]
			claim := volume["projected"].(object)["sources"].([]object)[0]["serviceAccountToken"].(object)
			if claim["audience"] != "durable-j12-"+role || pod["serviceAccountName"] != role+"-caller" || len(env) != 0 {
				t.Fatalf("unsafe projected probe: %s", name)
			}
		default:
			if env[authSwitch] != "1" || env[relaySwitch] != "1" || env["RUNTIME_SHUTDOWN_MODE"] != "drain" {
				t.Fatalf("incorrect effective mode: %s", name)
			}
			if (env["RUNTIME_PUBLIC_JWT_JWKS_FILE"] != "") != (name == "public") || (env["RUNTIME_DEMAND_BEARER_TOKEN_FILE"] != "") != (name == "demand") ||
				(env["RUNTIME_TOKEN_REVIEW_TOKEN_FILE"] != "") != (name == "operator" || name == "worker") {
				t.Fatalf("credential mounted to wrong runtime: %s", name)
			}
		}
	}
}

func TestJ12EphemeralKeysPublishOnlyPublicMaterial(t *testing.T) {
	c, err := newCredentials()
	if err != nil {
		t.Fatal(err)
	}
	set := c.jwks("current", "next")
	var read struct{ Keys []map[string]string }
	if json.Unmarshal(set, &read) != nil || len(read.Keys) != 2 || read.Keys[0]["n"] == read.Keys[1]["n"] {
		t.Fatal("fixture key IDs do not identify distinct public keys")
	}
	for _, key := range read.Keys {
		if len(key) != 6 || key["e"] != "AQAB" || key["kty"] != "RSA" || key["alg"] != "RS256" || key["use"] != "sig" {
			t.Fatal("published JWKS shape mismatch")
		}
		n, err := base64.RawURLEncoding.DecodeString(key["n"])
		if err != nil || len(n) != 256 {
			t.Fatal("fixture RSA modulus is not 2048-bit")
		}
	}
	token, err := c.token("current", "current", "RS256", claims("requester", "tenant-a", time.Now()))
	if err != nil || len(strings.Split(token, ".")) != 3 || strings.Contains(string(set), c.demand) {
		t.Fatal("unexpected credential fixture output")
	}
}

func TestJ12DurableWitnessRejectsAnyDecisionGrantOrEffect(t *testing.T) {
	var s snapshot
	if err := json.Unmarshal([]byte(`{"approvals":[{"intent_id":"bound","workflow_id":"workflow","decision":"PENDING","dispatch_status":"NOT_GRANTED"}],
	    "effects":{"engine_records":0,"engine_calls":0,"engine_state":0,"engine_fences":0,"engine_resolution":0,"service_records":0,"service_calls":0,"service_state":0,"service_fences":0,"service_resolution":0}}`), &s); err != nil {
		t.Fatal(err)
	}
	if !pending(s, "workflow", "bound") || pending(s, "wrong-workflow", "bound") || pending(s, "workflow", "unbound") {
		t.Fatal("pending intent was not bound to stored IDs")
	}
	for _, change := range []struct {
		name string
		edit func(*approvalRow)
	}{
		{"decision", func(r *approvalRow) { r.Decision = "APPROVED" }}, {"approver", func(r *approvalRow) { r.Approver = "requester" }},
		{"decision-time", func(r *approvalRow) { r.Decided = true }}, {"grant", func(r *approvalRow) { r.Grant = true }},
		{"grant-state", func(r *approvalRow) { r.Dispatch = "GRANTED" }}, {"grant-scope", func(r *approvalRow) { r.Scope = true }},
		{"grant-expiry", func(r *approvalRow) { r.Expiry = true }},
	} {
		t.Run(change.name, func(t *testing.T) {
			altered := s
			altered.Approvals = slices.Clone(s.Approvals)
			change.edit(&altered.Approvals[0])
			if pending(altered, "workflow", "bound") {
				t.Fatal("unexpected durable approval state passed the witness")
			}
		})
	}
	for name := range s.Effects {
		s.Effects[name] = 1
		if pending(s, "workflow", "bound") {
			t.Fatalf("unexpected effect passed the witness: %s", name)
		}
		s.Effects[name] = 0
	}
	if strings.Contains(witnessSQL, "to_jsonb(a)") || strings.Contains(witnessSQL, "grant_token,") || strings.Contains(witnessSQL, "claim_token,") {
		t.Fatal("SQL witness projects a bearer secret")
	}
}

func TestJ12OfflineAndEvidenceVerdictsFailClosed(t *testing.T) {
	if sawGoPass([]byte(`{"Action":"pass","Test":"TestJ12Else"}`), "TestJ12") ||
		sawGoPass([]byte(`{"Action":"skip","Test":"TestJ12"}`), "TestJ12") ||
		!sawGoPass([]byte(`{"Action":"pass","Test":"TestJ12"}`), "TestJ12") {
		t.Fatal("offline evidence did not require an exact passing test")
	}
	if allPassed(nil) || allPassed([]result{{Verdict: "pass"}, {Verdict: "invalid"}}) || allPassed([]result{{Verdict: "unperformed"}}) {
		t.Fatal("an incomplete campaign passed")
	}
	dir := t.TempDir()
	r := report{Verdict: "pass", Results: []result{{ID: "id", Verdict: "pass"}}, SQL: map[string]json.RawMessage{"large": []byte(`"` + strings.Repeat("x", maxEvidence) + `"`)}}
	if err := writeReport(dir, r); err == nil {
		t.Fatal("oversize evidence passed")
	}
	var safe report
	if err := readFileJSON(filepath.Join(dir, "qualification.json"), &safe); err != nil || safe.Verdict != "invalid" || len(safe.SQL) != 0 || len(safe.Results) != 1 {
		t.Fatalf("oversize run lost its invalid verdict or inventory: %v", err)
	}
}

func TestJ12HeadProbeRetainsNoBody(t *testing.T) {
	raw, err := json.Marshal(response{Status: 200})
	if err != nil {
		t.Fatal(err)
	}
	var head response
	if err := json.Unmarshal(raw, &head); err != nil || head.Status != 200 || len(head.Body) != 0 {
		t.Fatalf("bodyless response changed across the probe bridge: %s %v", raw, err)
	}
}

func TestJ12CredentialProbePlacement(t *testing.T) {
	for _, caller := range []string{"tenant", "demand", "none"} {
		for _, listener := range listeners {
			request := probeRequest{Listener: listener}
			if pod := placeProbe(caller, &request); pod != listener || !request.Loopback || request.Projected {
				t.Fatalf("%s bearer path was not loopback on %s", caller, listener)
			}
		}
	}
	for _, caller := range []string{"operator", "worker"} {
		request := probeRequest{Listener: "public"}
		if pod := placeProbe(caller, &request); pod != caller+"-probe" || !request.Projected || request.Loopback {
			t.Fatalf("%s projected token did not originate in its own pod", caller)
		}
	}
}
