package state

import (
	"context"
	"testing"
)

func TestK3RolePool(t *testing.T) {
	ctx := context.Background()
	url := integrationDatabaseURL(t)
	t.Setenv("RUNTIME_DB_MAX_CONNS", "3")
	t.Setenv("RUNTIME_ROLE", "pool-test")
	store, err := NewFromURL(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	if store.Pool().Config().MaxConns != 3 {
		t.Fatal("role pool bound bypassed")
	}
	var name string
	if err := store.Pool().QueryRow(ctx, `SHOW application_name`).Scan(&name); err != nil || name != "durable-pool-test" {
		t.Fatal("role pool attribution bypassed", name, err)
	}
	store.Close()
	for _, value := range []string{"0", "9", "invalid"} {
		t.Setenv("RUNTIME_DB_MAX_CONNS", value)
		if s, err := NewFromURL(ctx, url); err == nil {
			s.Close()
			t.Fatal("invalid role pool accepted")
		}
	}
}
