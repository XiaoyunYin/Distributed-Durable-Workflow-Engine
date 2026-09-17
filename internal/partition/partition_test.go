package partition

import "testing"

func TestStableVectors(t *testing.T) {
	tests := []struct {
		id        string
		partition uint64
	}{
		{id: "workflow-0001", partition: 8},
		{id: "workflow-0002", partition: 14},
		{id: "incident-2026-09-14-a", partition: 0},
		{id: "retry-key/abc", partition: 0},
	}

	for _, test := range tests {
		t.Run(test.id, func(t *testing.T) {
			partition, err := ID(test.id)
			if err != nil {
				t.Fatalf("ID(%q): %v", test.id, err)
			}
			if partition != test.partition {
				t.Fatalf("ID(%q) = %d, want %d", test.id, partition, test.partition)
			}
		})
	}
}

func TestEmptyIDRejected(t *testing.T) {
	if _, err := ID(""); err == nil {
		t.Fatal("ID(\"\") succeeded, want validation error")
	}
}
