package effects

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
)

// ErrResponseLost models the only contract exposed by a non-cooperating
// endpoint: the mutation may have happened, but the caller cannot query a
// receipt or ask the endpoint to deduplicate a retry.
var ErrResponseLost = errors.New("non-cooperating effect response was lost")

// NonCooperatingEndpoint is a deliberately small test endpoint. It has a
// private mutable state and exposes no lookup method to the engine. Tests may
// inspect AppliedCount as an external oracle, while recovery must use the
// engine's reconciliation path rather than this private ledger.
type NonCooperatingEndpoint struct {
	mu       sync.Mutex
	state    map[string]json.RawMessage
	counts   map[string]int
	loseNext bool
}

func NewNonCooperatingEndpoint() *NonCooperatingEndpoint {
	return &NonCooperatingEndpoint{state: make(map[string]json.RawMessage), counts: make(map[string]int)}
}

func (e *NonCooperatingEndpoint) Apply(ctx context.Context, resourceID string, value json.RawMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if e == nil || resourceID == "" || len(value) == 0 || !json.Valid(value) {
		return errors.New("non-cooperating effect identity and valid value are required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.state[resourceID] = append(json.RawMessage(nil), value...)
	e.counts[resourceID]++
	if e.loseNext {
		e.loseNext = false
		return ErrResponseLost
	}
	return nil
}

func (e *NonCooperatingEndpoint) LoseNextResponse() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.loseNext = true
}

func (e *NonCooperatingEndpoint) AppliedCount(resourceID string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.counts[resourceID]
}
