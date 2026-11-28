package transport

import (
	"context"
	"errors"
	"os"
)

const RelayFullBatchLoopEnv = "RUNTIME_RELAY_FULL_BATCH_LOOP"

// RelayFullBatchLoopRequested reads the configured relay mode.
func RelayFullBatchLoopRequested() bool {
	return os.Getenv(RelayFullBatchLoopEnv) == "1"
}

// FullBatchLoopEnabled reports the mode selected when this relay was built.
func (r *Relay) FullBatchLoopEnabled() bool {
	return r.fullBatchLoop
}

func (r *Relay) runWakeup(ctx context.Context, runPass func(context.Context) (RelayReport, error)) {
	for {
		if r.fullBatchLoop {
			select {
			case <-ctx.Done():
				return
			case <-r.Config.Drain:
				return
			default:
			}
		}
		report, err := runPass(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				r.errorMu.Lock()
				r.runErrors++
				r.errorMu.Unlock()
				if r.Config.OnError != nil {
					r.Config.OnError(err)
				}
			}
			return
		}
		if !r.fullBatchLoop || report.Claimed != r.Config.BatchSize || report.Failed != 0 {
			// Retry failed rows only on a later wakeup.
			return
		}
	}
}
