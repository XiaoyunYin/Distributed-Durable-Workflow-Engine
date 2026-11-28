// j7-media prepares source media and witnesses source loss for the J7 drill.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func run(ctx context.Context, args []string, out, diagnostics io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: j7-media archive ARCHIVE_DIR SOURCE FILE, prepare [flags], or {source-preflight|cut|source-loss} --config FILE")
	}
	switch args[0] {
	case "archive":
		if len(args) != 4 || !filepath.IsAbs(args[1]) {
			return errors.New("archive requires an absolute archive directory, source path and PostgreSQL filename")
		}
		return archiveWAL(args[1], args[2], args[3], syncDirectory)
	case "prepare":
		var in prepareInput
		flags := flag.NewFlagSet("prepare", flag.ContinueOnError)
		flags.SetOutput(diagnostics)
		flags.StringVar(&in.pgBin, "pg-bin", "", "absolute directory of the pinned PostgreSQL binaries")
		flags.StringVar(&in.sourceService, "source-service", "", "libpq service for the source probe and archive anchors")
		flags.StringVar(&in.backupService, "backup-service", "", "libpq service for the source replication login")
		flags.StringVar(&in.backup, "backup", "", "absolute path for a new base backup directory")
		flags.StringVar(&in.archive, "archive", "", "absolute path to the existing archive directory")
		flags.StringVar(&in.drill, "drill", "", "sealed drill ID used for archive anchor names")
		budget := flags.Duration("timeout", 4*time.Minute, "media preparation deadline")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 || *budget <= 0 || *budget > 8*time.Minute {
			return errors.New("prepare requires flags only and a positive timeout within eight minutes")
		}
		ctx, cancel := context.WithTimeout(ctx, *budget)
		defer cancel()
		receipt, err := prepareBackup(ctx, in, execTool, syncDirectory)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(receipt)
	case "source-preflight", "cut", "source-loss":
		return runFault(ctx, args, out, diagnostics)
	default:
		return fmt.Errorf("unknown j7-media command %q", args[0])
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil && !errors.Is(err, flag.ErrHelp) {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
