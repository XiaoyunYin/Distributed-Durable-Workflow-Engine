//go:build !windows

package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func processCPUSeconds() (float64, error) {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0, fmt.Errorf("read process CPU: %w", err)
	}
	closeName := strings.LastIndexByte(string(data), ')')
	if closeName < 0 || closeName+2 >= len(data) {
		return 0, errors.New("malformed /proc/self/stat")
	}
	fields := strings.Fields(string(data)[closeName+2:])
	if len(fields) < 13 {
		return 0, errors.New("short /proc/self/stat")
	}
	userTicks, err := strconv.ParseFloat(fields[11], 64)
	if err != nil {
		return 0, fmt.Errorf("parse user CPU ticks: %w", err)
	}
	systemTicks, err := strconv.ParseFloat(fields[12], 64)
	if err != nil {
		return 0, fmt.Errorf("parse system CPU ticks: %w", err)
	}
	return (userTicks + systemTicks) / 100.0, nil
}
