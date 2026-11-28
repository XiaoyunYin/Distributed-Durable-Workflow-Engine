package state

import (
	"encoding/json"
	"os"
	"testing"
)

func TestK2RegistryParity(t *testing.T) {
	t.Setenv("DUR053_K6_ACTIVITY", "0")
	good := []ActivityVersion{{"pure.echo", "v1", ""}, {"pure.add", "v1", ""}}
	if CheckRegistryParity(nil, nil) == nil {
		t.Fatal("registry parity guard bypassed: both empty")
	}
	for _, bad := range [][]ActivityVersion{nil, {}, {{"", "v1", ""}}, {{"pure.echo", "", ""}}, {{"pure.echo", "v1", ""}, {"pure.echo", "v1", ""}}, {{"pure.echo", "v1", ""}}, {{"pure.echo", "v2", ""}, {"pure.add", "v1", ""}}, {{"extra", "v1", ""}, {"pure.echo", "v1", ""}, {"pure.add", "v1", ""}}} {
		if CheckRegistryParity(good, bad) == nil {
			t.Fatalf("registry parity guard bypassed: %v", bad)
		}
		if CheckRegistryParity(bad, good) == nil {
			t.Fatalf("registry parity guard bypassed: %v", bad)
		}
	}
	for _, invalid := range [][]ActivityVersion{nil, {{"", "v1", ""}}, {{"pure.echo", "", ""}}, {{"pure.echo", "v1", ""}, {"pure.echo", "v1", ""}}} {
		if _, err := registrySet(invalid); err == nil {
			t.Fatalf("registry parity guard bypassed: invalid manifest %v", invalid)
		}
	}
	if err := CheckRegistryParity(good, []ActivityVersion{good[1], good[0]}); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []bool{false, true} {
		if fixture {
			t.Setenv("DUR048_ALLOW_FIXTURE_ACTIVITY", "1")
		} else {
			t.Setenv("DUR048_ALLOW_FIXTURE_ACTIVITY", "0")
		}
		registry := RuntimeRegistry()
		for _, name := range []string{"unknown", "pure.echo"} {
			definition := Definition{ActivityVersions: []byte(`{"unknown":"v1","pure.echo":"v2"}`), EffectClasses: map[string]EffectClass{name: EffectPure}}
			if _, ok := RuntimeVersion(definition, name); ok {
				t.Fatal("runtime version guard bypassed")
			}
		}
		for _, entry := range registry {
			raw, _ := json.Marshal(map[string]string{entry.Name: entry.Version})
			definition := Definition{ActivityVersions: raw, EffectClasses: map[string]EffectClass{entry.Name: EffectPure}}
			if _, ok := RuntimeVersion(definition, entry.Name); !ok {
				t.Fatal("exported registry disagrees with runtime allowlist")
			}
			definition.EffectClasses[entry.Name] = EffectCooperating
			if _, ok := RuntimeVersion(definition, entry.Name); ok {
				t.Fatal("effect class guard bypassed")
			}
		}
	}
}

func TestRecordedLLMRegistryLegacyEquivalence(t *testing.T) {
	t.Setenv("DUR048_ALLOW_FIXTURE_ACTIVITY", "0")
	t.Setenv("DUR053_ALLOW_FIXTURE_ACTIVITY", "0")
	t.Setenv("DUR053_K6_ACTIVITY", "0")
	t.Setenv(RecordedLLMMode, "0")
	if err := os.Unsetenv(RecordedLLMMode); err != nil {
		t.Fatal(err)
	}
	legacy := []ActivityVersion{{"pure.add", "v1", ""}, {"pure.echo", "v1", ""}}
	if err := CheckRegistryParity(legacy, RuntimeRegistry()); err != nil {
		t.Fatal(err)
	}
	def := Definition{
		ActivityVersions: []byte(`{"j4.recorded-llm":"v1","pure.echo":"v1"}`),
		EffectClasses:    map[string]EffectClass{RecordedLLMActivity: EffectPure, "pure.echo": EffectPure},
	}
	if _, ok := RuntimeVersion(def, "pure.echo"); !ok {
		t.Fatal("unset mode lost the existing pure.echo activity")
	}
	if _, ok := RuntimeVersion(def, RecordedLLMActivity); ok {
		t.Fatal("unset mode admitted the recorded-provider fixture")
	}
	t.Setenv(RecordedLLMMode, "0")
	if err := CheckRegistryParity(legacy, RuntimeRegistry()); err != nil {
		t.Fatal(err)
	}
	t.Setenv(RecordedLLMMode, "1")
	enabled := RuntimeRegistry()
	if err := CheckRegistryParity(append(legacy, ActivityVersion{RecordedLLMActivity, "v1", ""}), enabled); err != nil {
		t.Fatal(err)
	}
	if _, ok := RuntimeVersion(def, RecordedLLMActivity); !ok {
		t.Fatal("enabled mode did not admit the registered v1")
	}
	if CheckRegistryParity(legacy, enabled) == nil || CheckRegistryParity(enabled, legacy) == nil {
		t.Fatal("one-sided opt-in passed")
	}
	badVersion := []ActivityVersion{{RecordedLLMActivity, "v2", ""}}
	if CheckRegistryParity(badVersion, badVersion) == nil {
		t.Fatal("unregistered v2 passed")
	}
	t.Setenv(RecordedLLMMode, "enabled")
	if ValidateRecordedLLMMode() == nil {
		t.Fatal("invalid configuration passed")
	}
	t.Setenv(RecordedLLMMode, "")
	if _, present := os.LookupEnv(RecordedLLMMode); present && ValidateRecordedLLMMode() == nil {
		t.Fatal("explicit empty configuration passed")
	}
}

func TestK2ReferencedVersionsGate(t *testing.T) {
	store, ctx := k2Store(t)
	ns := "k2-registry-" + NewID()
	id := k2DemandFixture(t, store, ns, 10)
	candidate := RuntimeRegistry()
	if err := store.CheckActivityCompatibility(ctx, ns, candidate); err != nil {
		t.Fatal(err)
	}
	// A future node's version matters even before any attempt/node exists.
	_, err := store.pool.Exec(ctx, `UPDATE engine.workflow_definitions SET activity_versions='{"pure.echo":"v1","future":"v2"}' WHERE definition_id=$1`, id)
	if err != nil {
		t.Fatal(err)
	}
	extended := append(append([]ActivityVersion{}, candidate...), ActivityVersion{"future", "v2", ""})
	for _, status := range []string{"RUNNABLE", "WAITING_ACTIVITY", "WAITING_TIMER", "WAITING_APPROVAL", "PAUSED_UNSUPPORTED_VERSION", "RECONCILIATION_REQUIRED"} {
		_, err := store.pool.Exec(ctx, `UPDATE engine.workflow_executions SET state=$2 WHERE workflow_id=$1`, id, status)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.CheckActivityCompatibility(ctx, ns, candidate); err == nil {
			t.Fatalf("referenced version gate bypassed: %s", status)
		}
		if err := store.CheckActivityCompatibility(ctx, ns, extended); err != nil {
			t.Fatal(err)
		}
	}
	// Terminal references can be retired; other namespaces are independent.
	for _, status := range []string{"SUCCEEDED", "FAILED", "REJECTED", "CANCELED", "ABANDONED"} {
		_, err := store.pool.Exec(ctx, `UPDATE engine.workflow_executions SET state=$2 WHERE workflow_id=$1`, id, status)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.CheckActivityCompatibility(ctx, ns, candidate); err != nil {
			t.Fatalf("terminal version blocked rollout: %v", err)
		}
	}
	_, err = store.pool.Exec(ctx, `UPDATE engine.workflow_executions SET state='RUNNABLE' WHERE workflow_id=$1`, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CheckActivityCompatibility(ctx, ns+"-other", candidate); err != nil {
		t.Fatal(err)
	}
	if err := store.CheckActivityCompatibility(ctx, "", candidate); err == nil {
		t.Fatal("compatibility namespace guard bypassed")
	}
	for _, raw := range []string{`null`, `[]`, `{"pure.echo":2}`, `{"pure.echo":""}`} {
		_, err = store.pool.Exec(ctx, `UPDATE engine.workflow_definitions SET activity_versions=$2 WHERE definition_id=$1`, id, raw)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.CheckActivityCompatibility(ctx, ns, candidate); err == nil {
			t.Fatalf("malformed version gate bypassed: %s", raw)
		}
	}
	// Restore before t.Cleanup even when the negative-control assertion exits
	// through t.Fatal. Otherwise its workflow leaks into later backlog tests.
	defer reopenK2FixturePool(t, ctx, store)
	store.pool.Close()
	if err := store.CheckActivityCompatibility(ctx, ns, candidate); err == nil {
		t.Fatal("registry DB failure accepted")
	}
}
