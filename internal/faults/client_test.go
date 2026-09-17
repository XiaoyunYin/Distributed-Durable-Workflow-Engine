package faults

import "testing"

func TestFromEnvironmentDisabled(t *testing.T) {
	t.Setenv("DURABLE_FAULT_ENDPOINT", "")
	client, err := FromEnvironment()
	if err != nil {
		t.Fatalf("FromEnvironment: %v", err)
	}
	if client != nil {
		t.Fatal("disabled fault control returned a client")
	}
}
