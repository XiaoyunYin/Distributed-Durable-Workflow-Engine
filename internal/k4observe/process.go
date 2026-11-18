package k4observe

import (
	"errors"
	"os"
	"strconv"
	"strings"
)

// ProcessStartID identifies a Linux process within its pod UID and container.
// /proc stat field 22 is the start time in clock ticks since boot.
func ProcessStartID() (string, error) {
	raw, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return "", errors.New("K6_HOOK_PROCESS_STAT")
	}
	end := strings.LastIndexByte(string(raw), ')')
	fields := strings.Fields(string(raw[end+1:]))
	if end < 0 || len(fields) < 20 {
		return "", errors.New("K6_HOOK_PROCESS_STAT")
	}
	return strconv.Itoa(os.Getpid()) + ":" + fields[19], nil
}
