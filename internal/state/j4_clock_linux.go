//go:build linux

package state

import (
	"errors"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

func newJ4HostClock() (string, func() (int64, error), error) {
	data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", nil, err
	}
	bootID := strings.TrimSpace(string(data))
	if bootID == "" {
		return "", nil, errors.New("empty J4 host boot identity")
	}
	return bootID, func() (int64, error) {
		var now unix.Timespec
		err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &now)
		return now.Nano(), err
	}, nil
}
