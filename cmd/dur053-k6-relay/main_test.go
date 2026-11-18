package main

import (
	"context"
	"testing"
)

func TestK6RelayOrdinaryHasNoFaultFlags(t *testing.T) {
	t.Setenv("DUR053_K6", "1")
	t.Setenv("DUR053_K6_RELAY_ARM", "1")
	for _, flag := range []string{"--arm", "--hold", "--fault", "--fault-address", "-tags=dur053_k6"} {
		if err := run(context.Background(), []string{flag}); err == nil || err.Error() != usage {
			t.Fatalf("ordinary relay accepted flag %s", flag)
		}
	}
}
