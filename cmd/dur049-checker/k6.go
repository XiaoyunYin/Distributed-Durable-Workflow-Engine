package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"durable-agent-execution-engine/internal/invariants/k6live"
	"durable-agent-execution-engine/internal/invariants/k6v1"
)

const (
	k6Live    = "-k6-live-v1"
	k6Offline = "-k6-offline-v1"
)

func hasK6Selector(args []string) bool {
	for _, arg := range args {
		if strings.HasPrefix(arg, "-k6-") || strings.HasPrefix(arg, "--k6-") {
			return true
		}
	}
	return false
}

func k6Mode(args []string, stdout, stderr io.Writer) int {
	usage := func() int {
		fmt.Fprintln(stderr, "K6_USAGE: use exactly one of -k6-live-v1 <private-config> or -k6-offline-v1 <episode-dir>")
		return k6v1.Usage
	}
	selector, path, equal := strings.Cut(args[0], "=")
	selector = strings.Replace(selector, "--", "-", 1)
	if selector != k6Live && selector != k6Offline {
		return usage()
	}
	if (equal && len(args) != 1) || (!equal && len(args) != 2) {
		return usage()
	}
	if !equal {
		path = args[1]
	}
	if path == "" || strings.HasPrefix(path, "-") {
		return usage()
	}
	emit := func(result k6v1.Result) int {
		raw, err := k6v1.Canonical(result)
		if err != nil {
			fmt.Fprintln(stderr, "K6_RESULT_ENCODING")
			return k6v1.Fail
		}
		if _, err := stdout.Write(raw); err != nil {
			fmt.Fprintln(stderr, "K6_RESULT_WRITE")
			return k6v1.Fail
		}
		return result.ExitCode()
	}
	if selector == k6Live {
		var config k6live.Config
		raw, err := os.ReadFile(path)
		if err != nil || k6v1.DecodeStrict(raw, &config) != nil || !config.Valid() {
			fmt.Fprintln(stderr, "K6_LIVE_CONFIG")
			return k6v1.Usage
		}
		if err := k6live.Acquire(context.Background(), config); err != nil {
			fmt.Fprintln(stderr, err)
			return emit(k6v1.AcquisitionFailed())
		}
		path = config.EpisodeDir
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		fmt.Fprintln(stderr, "K6_BUNDLE_OPEN")
		return emit(k6v1.AcquisitionFailed())
	}
	defer root.Close()
	return emit(k6v1.Verify(root.FS()))
}
