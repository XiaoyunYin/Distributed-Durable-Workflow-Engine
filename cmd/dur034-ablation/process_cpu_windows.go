//go:build dur034_ablation && windows

package main

import (
	"fmt"
	"syscall"
)

func processCPUSeconds() (float64, error) {
	var creation, exit, kernel, user syscall.Filetime
	process, err := syscall.GetCurrentProcess()
	if err != nil {
		return 0, fmt.Errorf("get scheduler process handle: %w", err)
	}
	if err := syscall.GetProcessTimes(process, &creation, &exit, &kernel, &user); err != nil {
		return 0, fmt.Errorf("read scheduler process CPU: %w", err)
	}
	return float64(kernel.Nanoseconds()+user.Nanoseconds()) / 1e9, nil
}
