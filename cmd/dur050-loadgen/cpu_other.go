//go:build !linux

package main

import (
	"errors"
	"time"
)

func readProcessCPUTime() (time.Duration, error) {
	return 0, errors.New("process CPU validation is supported only on Linux")
}
