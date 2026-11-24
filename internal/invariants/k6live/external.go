package k6live

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6v1"
)

const quietPoll = 2 * time.Second

type externalObserver struct {
	dir     string
	m       k6v1.Manifest
	observe observer
	samples k6v1.ExternalSamples
	last    k6v1.External
}

func newExternal(dir string, m k6v1.Manifest, observe observer) *externalObserver {
	return &externalObserver{dir: dir, m: m, observe: observe, samples: k6v1.ExternalSamples{
		Schema: k6v1.SamplesVersion, Binding: k6v1.Binding(m), Observer: "dur049-checker",
	}}
}

// The optional current pointer switches between immutable collector frames.
// Reading directly from a directory is useful for a frozen fixture.
func (e *externalObserver) read() (k6v1.External, bool) {
	if e.dir == "" {
		return k6v1.External{}, false
	}
	root, err := os.OpenRoot(e.dir)
	if err != nil {
		return k6v1.External{}, false
	}
	defer root.Close()
	source := root
	raw, err := root.ReadFile("current")
	if err == nil {
		name := strings.TrimSuffix(string(raw), "\n")
		n, ok := strings.CutPrefix(name, "frame-")
		if _, err := strconv.ParseUint(n, 10, 64); !ok || err != nil {
			return k6v1.External{}, false
		}
		source, err = root.OpenRoot(name)
		if err != nil {
			return k6v1.External{}, false
		}
		defer source.Close()
	} else if !errors.Is(err, os.ErrNotExist) {
		return k6v1.External{}, false
	}
	data, issues := k6v1.ReadExternal(source.FS(), e.m)
	return data, len(issues) == 0
}

func (e *externalObserver) append(s k6v1.QuietSample) {
	s.Sequence = int64(len(e.samples.Rows) + 1)
	e.samples.Rows = append(e.samples.Rows, s)
}

func (e *externalObserver) waitQuiet(ctx context.Context, generation int, floor string, databases []database, gate observationGate) (k6v1.QuietSample, bool, error) {
	var previous, initial k6v1.QuietSample
	notified := false
	for ctx.Err() == nil {
		// Read the database first so the collector frame covers the end of
		// that observation. Every database read uses its existing connection.
		data, ok := e.read()
		e.last = data
		if !ok {
			return k6v1.QuietSample{}, false, nil
		}
		s := k6v1.QuietSample{Generation: generation, Phase: "quiet", Terminal: true}
		for _, db := range databases {
			mark, err := sample(ctx, db, e.m, e.observe)
			if err != nil {
				return k6v1.QuietSample{}, false, nil
			}
			s.Terminal = s.Terminal && mark.Terminal
			s.Databases = append(s.Databases, k6v1.QuietDB(db.name, mark.Relations))
		}
		data, ok = e.read()
		e.last = data
		if !ok {
			return k6v1.QuietSample{}, false, nil
		}
		s.At, s.Marks, s.Terminal = e.observe(), data.Marks(), s.Terminal && data.Terminal()
		activity, regression := k6v1.ExternalStep(previous.Marks, s.Marks)
		if regression != "" || previous.At.UTC != "" && k6v1.SampleGap(previous.At, s.At) {
			e.append(s)
			return s, false, nil
		}
		if !s.Terminal || s.At.UTC < floor {
			initial = k6v1.QuietSample{}
		} else if initial.At.UTC == "" || activity || !slices.Equal(previous.Databases, s.Databases) {
			initial = s
		} else if k6v1.QuietElapsed(initial.At, s.At) {
			s.Phase = "pre"
			e.append(s)
			return e.samples.Rows[len(e.samples.Rows)-1], true, nil
		}
		e.append(s)
		previous = s
		if gate != nil && !notified && initial.At.UTC != "" && s.At.MonotonicNS-initial.At.MonotonicNS >= int64(quietPoll) {
			notified = true
			if err := gate(ctx, "before-pre", e.samples.Rows[len(e.samples.Rows)-1]); err != nil {
				return s, false, err
			}
			// Sample immediately after release so the observed write resets quiet.
			continue
		}
		if !waitPoll(ctx, quietPoll) {
			break
		}
	}
	return k6v1.QuietSample{}, false, nil
}

func waitPoll(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (e *externalObserver) cutSample(at k6v1.Observer, pre k6v1.QuietSample, cuts []k6v1.DatabaseCut) (k6v1.QuietSample, bool, bool) {
	data, ok := e.read()
	e.last = data
	post := k6v1.QuietSample{Generation: pre.Generation, Phase: "post", At: at, Marks: data.Marks(), Terminal: data.Terminal()}
	raced, regression := k6v1.ExternalStep(pre.Marks, post.Marks)
	for i, d := range cuts {
		mark := k6v1.QuietDB(d.DB, d.Post.Relations)
		post.Databases = append(post.Databases, mark)
		raced = raced || k6v1.QuietDB(d.DB, d.Pre.Relations) != pre.Databases[i] || mark != pre.Databases[i] || !d.Post.Terminal
		post.Terminal = post.Terminal && d.Post.Terminal
	}
	post.At = e.observe()
	e.append(post)
	return post, raced || !post.Terminal, ok && regression == "" && !k6v1.SampleGap(pre.At, post.At)
}

func (e *externalObserver) finish(ctx context.Context, post k6v1.QuietSample, databases []database) {
	for {
		data, ok := e.read()
		if !ok {
			return
		}
		e.last = data
		end := post
		end.At, end.Phase, end.Marks, end.Terminal = e.observe(), "export", data.Marks(), data.Terminal() && post.Terminal
		changed, regression := k6v1.ExternalStep(post.Marks, end.Marks)
		if data.SealedThrough(post.At.UTC) || changed || regression != "" ||
			k6v1.SampleGap(post.At, end.At) || !waitPoll(ctx, 100*time.Millisecond) {
			end.Databases = nil
			for _, db := range databases {
				watermark, err := sample(ctx, db, e.m, e.observe)
				if err != nil {
					return
				}
				end.Terminal = end.Terminal && watermark.Terminal
				end.Databases = append(end.Databases, k6v1.QuietDB(db.name, watermark.Relations))
			}
			final, ok := e.read()
			if !ok {
				return
			}
			e.last = final
			end.At, end.Marks, end.Terminal = e.observe(), final.Marks(), end.Terminal && final.Terminal()
			e.append(end)
			return
		}
	}
}

func (e *externalObserver) publish(dir string, index *k6v1.Index) error {
	files, err := e.last.Files()
	if err != nil {
		return err
	}
	for path, raw := range files {
		if err := os.MkdirAll(filepath.Join(dir, filepath.Dir(filepath.FromSlash(path))), 0o755); err != nil {
			return errors.New("K6_BUNDLE_WRITE")
		}
		entry, err := writePublic(dir, path, raw)
		if err != nil {
			return err
		}
		index.Files = append(index.Files, entry)
	}
	if e.m.Cell == "medium-16" && e.dir != "" {
		// The host observer closes this fixed workload window before live
		// acquisition. Preserve its original bytes for the common verifier.
		root, err := os.OpenRoot(e.dir)
		if err != nil {
			return err
		}
		defer root.Close()
		raw, err := root.ReadFile(k6v1.ControlPath)
		if err == nil {
			if err := os.Mkdir(filepath.Join(dir, "fault"), 0o755); err != nil {
				return err
			}
			entry, err := writePublic(dir, k6v1.ControlPath, raw)
			if err != nil {
				return err
			}
			index.Files = append(index.Files, entry)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if len(e.samples.Rows) == 0 {
		return nil
	}
	if err := e.samples.SealAt(e.samples.Rows[len(e.samples.Rows)-1].At.UTC); err != nil {
		return err
	}
	if err := os.Mkdir(filepath.Join(dir, "samples"), 0o755); err != nil {
		return errors.New("K6_BUNDLE_WRITE")
	}
	entry, err := writeJSON(dir, k6v1.SamplesPath, e.samples)
	if err != nil {
		return err
	}
	index.Files = append(index.Files, entry)
	return nil
}
