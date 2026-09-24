//go:build !linux

package dur050

import (
	"sync"
	"time"
)

var (
	processClockOnce sync.Once
	processClockBase time.Time
)

// SharedMonotonicNanoseconds is only a process-local fallback for unit tests
// and development. Campaign binaries refuse to run without the Linux shared
// host clock because this value cannot be compared across processes.
func SharedMonotonicNanoseconds() (int64, error) {
	processClockOnce.Do(func() { processClockBase = time.Now() })
	return time.Since(processClockBase).Nanoseconds(), nil
}

func HasSharedMonotonicClock() bool { return false }
