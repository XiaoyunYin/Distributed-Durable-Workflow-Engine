//go:build linux

package dur053

import "syscall"

func precommitPeakRSS() int64 {
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) != nil {
		return 0
	}
	// Linux ru_maxrss is the process high-water mark in KiB, including setup.
	return usage.Maxrss
}
