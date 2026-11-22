// Package k6observation persists opt-in, already projected K6 observations.
package k6observation

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6v1"
)

type Journal[T any] struct {
	mu     sync.Mutex
	stream k6v1.Stream[T]
	path   string
	failed bool
	closed bool
}

func Open[T any](path string, binding k6v1.SourceBinding, schema, source string) (*Journal[T], error) {
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return nil, errors.New("K6_OBSERVATION_PATH")
	}
	now := time.Now()
	j := &Journal[T]{path: path, stream: k6v1.Stream[T]{Header: k6v1.StreamHeader{
		Schema: "dur053.k6-" + schema + ".v1", Binding: binding, Source: source, OpenedUTC: k6v1.UTC(now),
	}, Rows: []k6v1.Record[T]{}}}
	if err := j.write("checkpoint", now); err != nil {
		return nil, err
	}
	return j, nil
}

func (j *Journal[T]) Append(row T) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failed || j.closed {
		return errors.New("K6_OBSERVATION_STOPPED")
	}
	now := time.Now()
	j.stream.Rows = append(j.stream.Rows, k6v1.Record[T]{
		Position: k6v1.Position{Sequence: int64(len(j.stream.Rows) + 1), UTC: k6v1.UTC(now)}, Data: row,
	})
	return j.write("checkpoint", now)
}

func (j *Journal[T]) Checkpoint(terminal bool) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failed || j.closed {
		return errors.New("K6_OBSERVATION_STOPPED")
	}
	mode := "checkpoint"
	if terminal {
		mode, j.closed = "terminal", true
	}
	return j.write(mode, time.Now())
}

func (j *Journal[T]) write(mode string, now time.Time) error {
	err := j.persist(mode, now)
	if err != nil {
		j.failed = true
		return errors.New("K6_OBSERVATION_PERSIST")
	}
	return nil
}

func (j *Journal[T]) persist(mode string, now time.Time) error {
	if err := k6v1.SealStream(&j.stream, mode, k6v1.UTC(now), int64(len(j.stream.Rows)), 0); err != nil {
		return err
	}
	raw, err := k6v1.EncodeStream(j.stream)
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(j.path), ".k6-observation-")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	_, err = temp.Write(raw)
	if err := errors.Join(err, temp.Sync(), temp.Close()); err != nil {
		return err
	}
	return os.Rename(temp.Name(), j.path)
}
