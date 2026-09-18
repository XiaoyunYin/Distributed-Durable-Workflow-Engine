// Command dur026-worker is the fixed-capacity synthetic worker used by the
// DUR-026 measurement harness. It has no repository access: scheduler
// goroutines submit deterministic CPU activities over stdin/stdout, keeping
// worker CPU out of the scheduler process measurement.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
)

type request struct {
	Seed      int64  `json:"seed"`
	NodeID    string `json:"node_id"`
	WorkUnits int    `json:"work_units"`
}

type response struct {
	OK     bool   `json:"ok"`
	Digest uint64 `json:"digest,omitempty"`
	Error  string `json:"error,omitempty"`
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	buffer := make([]byte, 0, 64*1024)
	scanner.Buffer(buffer, 1024*1024)
	encoder := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var item request
		if err := json.Unmarshal(scanner.Bytes(), &item); err != nil {
			_ = encoder.Encode(response{Error: err.Error()})
			continue
		}
		if item.WorkUnits <= 0 || item.NodeID == "" {
			_ = encoder.Encode(response{Error: "invalid worker request"})
			continue
		}
		digest := busyWork(context.Background(), item.Seed, item.NodeID, item.WorkUnits)
		if err := encoder.Encode(response{OK: true, Digest: digest}); err != nil {
			return
		}
	}
}

func busyWork(ctx context.Context, seed int64, nodeID string, units int) uint64 {
	value := uint64(seed) ^ uint64(len(nodeID))
	for index := 0; index < units; index++ {
		value ^= value << 13
		value ^= value >> 7
		value ^= value << 17
		if index%4096 == 0 {
			select {
			case <-ctx.Done():
				return value
			default:
			}
		}
	}
	return value
}
