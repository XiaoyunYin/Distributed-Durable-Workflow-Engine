package transport

import (
	"context"
	"errors"
)

func (r *Relay) runWakeup(ctx context.Context, runPass func(context.Context) (RelayReport, error)) {
	for {
		if r.Config.FullBatchLoop {
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
		if !r.Config.FullBatchLoop || report.Claimed != r.Config.BatchSize || report.Failed != 0 {
			// Retry failed rows only on a later wakeup.
			return
		}
	}
}
