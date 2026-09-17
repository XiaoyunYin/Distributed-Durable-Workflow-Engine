// Package partition implements the frozen workflow-to-partition mapping used
// by scheduler ownership. It intentionally has no runtime-specific hashing.
package partition

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

const (
	// MapVersion identifies the wire/documented mapping contract.
	MapVersion = "sha256-u64-be-v1"
	// PartitionCount is frozen for the initial scheduler topology.
	PartitionCount uint64 = 16
)

// ID returns the stable partition for a workflow ID under MapVersion.
// Workflow IDs are UTF-8 strings; SHA-256's first eight digest bytes are read
// as an unsigned big-endian integer and reduced modulo PartitionCount.
func ID(workflowID string) (uint64, error) {
	if workflowID == "" {
		return 0, fmt.Errorf("workflow ID must not be empty")
	}
	digest := sha256.Sum256([]byte(workflowID))
	return binary.BigEndian.Uint64(digest[:8]) % PartitionCount, nil
}
