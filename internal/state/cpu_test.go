package state

import (
	"encoding/json"
	"testing"
)

func TestK2cCPURegistry(t *testing.T) {
	for _, enabled := range []string{"0", "1"} {
		t.Setenv("DUR053_ALLOW_FIXTURE_ACTIVITY", enabled)
		d := Definition{ActivityVersions: json.RawMessage(`{"dur053.cpu":"v1"}`), EffectClasses: map[string]EffectClass{"dur053.cpu": EffectPure}}
		_, ok := RuntimeVersion(d, "dur053.cpu")
		if ok != (enabled == "1") {
			t.Fatal("CPU fixture opt-in guard bypassed")
		}
		d.ActivityVersions = json.RawMessage(`{"dur053.cpu":"v2"}`)
		if _, ok := RuntimeVersion(d, "dur053.cpu"); ok {
			t.Fatal("CPU version guard bypassed")
		}
	}
}
