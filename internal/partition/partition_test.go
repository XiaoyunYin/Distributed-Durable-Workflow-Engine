package partition

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type vectorFile struct {
	Version        string `json:"version"`
	PartitionCount uint64 `json:"partition_count"`
	Vectors        []struct {
		WorkflowID string `json:"workflow_id"`
		Partition  uint64 `json:"partition"`
	} `json:"vectors"`
}

func loadVectors(t *testing.T) vectorFile {
	t.Helper()
	path := filepath.Join("..", "..", "api", "partition-map-v1.vectors.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var vectors vectorFile
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatalf("decode vectors: %v", err)
	}
	return vectors
}

func TestStableVectors(t *testing.T) {
	vectors := loadVectors(t)
	if vectors.Version != MapVersion {
		t.Fatalf("vector version = %q, want %q", vectors.Version, MapVersion)
	}
	if vectors.PartitionCount != PartitionCount {
		t.Fatalf("partition count = %d, want %d", vectors.PartitionCount, PartitionCount)
	}
	for _, vector := range vectors.Vectors {
		t.Run(vector.WorkflowID, func(t *testing.T) {
			partition, err := ID(vector.WorkflowID)
			if err != nil {
				t.Fatalf("ID(%q): %v", vector.WorkflowID, err)
			}
			if partition != vector.Partition {
				t.Fatalf("ID(%q) = %d, want %d", vector.WorkflowID, partition, vector.Partition)
			}
		})
	}
}

func TestInvalidUTF8Rejected(t *testing.T) {
	invalid := string([]byte{'w', 'f', '-', 0xff, 0xfe})
	if _, err := ID(invalid); err == nil {
		t.Fatal("invalid UTF-8 workflow ID was accepted")
	}
}

func TestEmptyIDRejected(t *testing.T) {
	if _, err := ID(""); err == nil {
		t.Fatal("ID(\"\") succeeded, want validation error")
	}
}
