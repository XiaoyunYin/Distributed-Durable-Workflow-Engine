// Package k6collect projects independent observer data into public K6 frames.
// It owns no database pool and is outside the checker's verdict dependency.
package k6collect

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6v1"
)

func NewStream[T any](m k6v1.Manifest, schema, source string, opened time.Time) k6v1.Stream[T] {
	return k6v1.Stream[T]{Header: k6v1.StreamHeader{
		Schema: "dur053.k6-" + schema + ".v1", Binding: k6v1.Binding(m), Source: source, OpenedUTC: k6v1.UTC(opened),
	}, Rows: []k6v1.Record[T]{}}
}

func Append[T any](s *k6v1.Stream[T], at time.Time, value T) {
	s.Rows = append(s.Rows, k6v1.Record[T]{Position: k6v1.Position{Sequence: int64(len(s.Rows) + 1), UTC: k6v1.UTC(at)}, Data: value})
}

// FetchLedgers obtains explicitly rostered ledgers from receiver-owned
// storage. It also retains any extra ledger for the reverse join.
func FetchLedgers(receiver fs.FS, e *k6v1.External, m k6v1.Manifest) []k6v1.Finding {
	ledgers, issues := k6v1.ReadWorkerLedgers(receiver, m, e.Roster)
	e.Sessions = ledgers
	return issues
}

// Publish atomically exposes an immutable public frame. The current pointer
// is switched only after every file is written and synced.
func Publish(root string, generation int64, m k6v1.Manifest, sources k6v1.External) error {
	if generation < 1 || !m.Valid() {
		return errors.New("K6_COLLECT_FRAME")
	}
	files, err := sources.Files()
	if err != nil {
		return errors.New("K6_COLLECT_SCHEMA")
	}
	name := "frame-" + strconv.FormatInt(generation, 10)
	dir := filepath.Join(root, name)
	if os.Mkdir(dir, 0o700) != nil {
		return errors.New("K6_COLLECT_FRAME")
	}
	for path, raw := range files {
		target := filepath.Join(dir, filepath.FromSlash(path))
		if os.MkdirAll(filepath.Dir(target), 0o700) != nil || syncFile(target, raw) != nil {
			return errors.New("K6_COLLECT_WRITE")
		}
	}
	frame, err := os.OpenRoot(dir)
	if err != nil {
		return errors.New("K6_COLLECT_FRAME")
	}
	_, issues := k6v1.ReadExternal(frame.FS(), m)
	_ = frame.Close()
	if len(issues) != 0 {
		return fmt.Errorf("K6_COLLECT_%s:%s", issues[0].Code, issues[0].Source)
	}
	tmp := filepath.Join(root, "."+name)
	if err := syncFile(tmp, []byte(name+"\n")); err != nil {
		return errors.New("K6_COLLECT_POINTER")
	}
	if os.Rename(tmp, filepath.Join(root, "current")) != nil {
		return errors.New("K6_COLLECT_POINTER")
	}
	return nil
}

func syncFile(path string, raw []byte) error {
	out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, err = out.Write(raw)
	return errors.Join(err, out.Sync(), out.Close())
}
