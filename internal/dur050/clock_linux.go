//go:build linux

package dur050

import "golang.org/x/sys/unix"

// SharedMonotonicNanoseconds reads the Linux host's CLOCK_MONOTONIC clock.
// Separate processes on the same boot can compare these values.
func SharedMonotonicNanoseconds() (int64, error) {
	var value unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &value); err != nil {
		return 0, err
	}
	return value.Sec*1_000_000_000 + value.Nsec, nil
}

func HasSharedMonotonicClock() bool { return true }
