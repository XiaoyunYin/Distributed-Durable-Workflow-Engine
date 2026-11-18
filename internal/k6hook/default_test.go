//go:build !dur053_k6

package k6hook

import (
	"strings"
	"testing"
)

func TestOrdinaryBuildIgnoresAllCampaignSettings(t *testing.T) {
	t.Setenv("DUR053_K6_HOOK_URL", "https://127.0.0.1:1")
	t.Setenv("DUR053_K6_HOOK_CA_FILE", "does-not-exist")
	t.Setenv("DUR053_K6_HOOK_TOKEN", strings.Repeat("a", 64))
	t.Setenv("DUR053_K6_HOOK_WORKLOAD", "{}")
	t.Setenv("POD_UID", "ordinary")
	for _, actor := range []string{"relay", "sink", "worker"} {
		if hook, err := FromEnvironment(actor, ""); hook != nil || err != nil {
			t.Fatal("ordinary build enabled campaign fault configuration")
		}
	}
}
