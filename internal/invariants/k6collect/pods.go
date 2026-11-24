package k6collect

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6v1"
)

var objectName = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{0,252}$`)

type Kubernetes struct {
	Context string
}

func (k Kubernetes) raw(ctx context.Context, path string) *exec.Cmd {
	return exec.CommandContext(ctx, "kubectl", "--context", k.Context, "get", "--raw", path)
}

// Watch opens the Kubernetes list/watch pair, following the API's actual
// cursor. Registration must come from a separately authenticated process
// source. The watcher never discovers sessions by enumerating receiver files.
// A closed watch is resumed from its last cursor; an expired cursor fails.
func (k Kubernetes) Watch(ctx context.Context, m k6v1.Manifest, selector, container string, watcher *PodWatch, ready func(string) error) error {
	if !objectName.MatchString(m.Namespace) || !objectName.MatchString(container) || selector == "" {
		return errors.New("K6_POD_WATCH_CONFIG")
	}
	path := "/api/v1/namespaces/" + m.Namespace + "/pods"
	q := url.Values{"labelSelector": {selector}}
	raw, err := k.raw(ctx, path+"?"+q.Encode()).Output()
	if err != nil || watcher.Baseline(raw, container, time.Now()) != nil {
		return errors.New("K6_POD_LIST")
	}
	q.Set("watch", "1")
	q.Set("allowWatchBookmarks", "true")
	q.Set("timeoutSeconds", "2")
	for ctx.Err() == nil {
		q.Set("resourceVersion", watcher.Cursor())
		cmd := k.raw(ctx, path+"?"+q.Encode())
		out, err := cmd.StdoutPipe()
		if err != nil || cmd.Start() != nil {
			return errors.New("K6_POD_WATCH")
		}
		err = watcher.Read(out, container, time.Now)
		if err != nil {
			_ = cmd.Process.Kill()
		}
		exit := cmd.Wait()
		if ctx.Err() != nil {
			return nil
		}
		if err != nil || exit != nil {
			watcher.Gap(time.Now())
			return errors.New("K6_POD_WATCH_GAP")
		}
		watcher.progressAt(time.Now())
		if ready != nil {
			if err := ready(watcher.Cursor()); err != nil {
				return errors.New("K6_POD_WATCH_READY")
			}
			ready = nil
		}
	}
	return nil
}

type pod struct {
	Metadata struct {
		UID             string `json:"uid"`
		Namespace       string `json:"namespace"`
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Status struct {
		Containers []struct {
			Name        string `json:"name"`
			ContainerID string `json:"containerID"`
			Restart     int64  `json:"restartCount"`
			State       struct {
				Running *struct {
					Started time.Time `json:"startedAt"`
				} `json:"running"`
				Terminated *terminated `json:"terminated"`
			} `json:"state"`
			Last struct {
				Terminated *terminated `json:"terminated"`
			} `json:"lastState"`
		} `json:"containerStatuses"`
	} `json:"status"`
}

type terminated struct {
	ID       string    `json:"containerID"`
	Start    time.Time `json:"startedAt"`
	Finished time.Time `json:"finishedAt"`
	ExitCode *int      `json:"exitCode"`
	Signal   int       `json:"signal"`
}

type podList struct {
	Metadata struct {
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Items []pod `json:"items"`
}

// PodWatch is fed by one sequential apiserver stream. Its state should be
// snapshotted by the same observer goroutine or behind the caller's lock.
type PodWatch struct {
	mu          sync.Mutex
	Stream      k6v1.Stream[k6v1.WatchRow]
	cursor      string
	progress    time.Time
	known       map[k6v1.RuntimeIdentity]bool
	dead        map[k6v1.RuntimeIdentity]time.Time
	sessions    map[string]bool
	deathSealed map[string]bool
	exits       map[k6v1.RuntimeIdentity]k6v1.Position
	shutdowns   map[[2]string]bool
}

func NewPodWatch(m k6v1.Manifest, observer string, started time.Time) *PodWatch {
	return &PodWatch{Stream: NewStream[k6v1.WatchRow](m, "roster", observer, started), known: map[k6v1.RuntimeIdentity]bool{},
		dead: map[k6v1.RuntimeIdentity]time.Time{}, sessions: map[string]bool{}, deathSealed: map[string]bool{},
		exits: map[k6v1.RuntimeIdentity]k6v1.Position{}, shutdowns: map[[2]string]bool{}}
}

func (w *PodWatch) Cursor() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cursor
}

func (w *PodWatch) progressAt(at time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.progress = at
	w.append(at, k6v1.WatchRow{Kind: "bookmark"})
}

func (w *PodWatch) append(at time.Time, row k6v1.WatchRow) {
	if row.Cursor == "" {
		row.Cursor = w.cursor
	}
	row.From = w.cursor
	Append(&w.Stream, at, row)
	w.cursor = row.Cursor
}

func (w *PodWatch) Gap(at time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.append(at, k6v1.WatchRow{Kind: "gap"})
}

func (w *PodWatch) Baseline(raw []byte, container string, at time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var list podList
	if json.Unmarshal(raw, &list) != nil || list.Metadata.ResourceVersion == "" || list.Items == nil || w.cursor != "" {
		return errors.New("K6_POD_LIST")
	}
	w.progress = at
	w.append(at, k6v1.WatchRow{Kind: "baseline", Cursor: list.Metadata.ResourceVersion})
	for _, p := range list.Items {
		w.observe(p, container, at)
	}
	return nil
}

func (w *PodWatch) Read(in io.Reader, container string, now func() time.Time) error {
	dec := json.NewDecoder(in)
	for {
		var event struct {
			Type   string `json:"type"`
			Object pod    `json:"object"`
		}
		err := dec.Decode(&event)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil || !slices.Contains([]string{"ADDED", "MODIFIED", "DELETED", "BOOKMARK"}, event.Type) ||
			event.Object.Metadata.ResourceVersion == "" {
			return errors.New("K6_POD_WATCH_GAP")
		}
		at := now()
		w.mu.Lock()
		w.progress = at
		w.append(at, k6v1.WatchRow{Kind: "bookmark", Cursor: event.Object.Metadata.ResourceVersion})
		if event.Type != "BOOKMARK" {
			w.observe(event.Object, container, at)
		}
		w.mu.Unlock()
	}
}

func (w *PodWatch) observe(p pod, wanted string, at time.Time) {
	if p.Metadata.Namespace != w.Stream.Header.Binding.Namespace || !objectName.MatchString(p.Metadata.UID) {
		w.append(at, k6v1.WatchRow{Kind: "gap"})
		return
	}
	register := func(container string, restart int64, started time.Time) k6v1.RuntimeIdentity {
		i := k6v1.RuntimeIdentity{PodUID: p.Metadata.UID, ContainerID: strings.TrimPrefix(strings.TrimPrefix(container, "containerd://"), "docker://"),
			Restart: restart, StartedUTC: k6v1.UTC(started)}
		if !w.known[i] {
			w.append(at, k6v1.WatchRow{Kind: "runtime", Runtime: &i})
			w.known[i] = true
		}
		return i
	}
	for _, status := range p.Status.Containers {
		if status.Name != wanted {
			continue
		}
		termination := func(t *terminated, restart int64) {
			if t != nil && t.ID != "" && !t.Start.IsZero() && !t.Finished.IsZero() && restart >= 0 {
				i := register(t.ID, restart, t.Start)
				if w.dead[i].IsZero() {
					w.dead[i] = at
				}
				if t.ExitCode != nil && *t.ExitCode >= 0 && t.Signal >= 0 && w.exits[i].Sequence == 0 && !t.Finished.Before(t.Start) {
					w.append(at, k6v1.WatchRow{Kind: "exit", Runtime: &i, Exit: &k6v1.RuntimeExit{
						FinishedUTC: k6v1.UTC(t.Finished), Code: *t.ExitCode, Signal: t.Signal,
					}})
					w.exits[i] = w.Stream.Rows[len(w.Stream.Rows)-1].Position
				}
			}
		}
		termination(status.Last.Terminated, status.Restart-1)
		termination(status.State.Terminated, status.Restart)
		if status.State.Running != nil && status.ContainerID != "" && !status.State.Running.Started.IsZero() {
			register(status.ContainerID, status.Restart, status.State.Running.Started)
		}
	}
}

// BindShutdown joins the kubelet observation to starts present in the
// receiver at that time. The checker independently repeats the join.
func (w *PodWatch) BindShutdown(at time.Time, session k6v1.WorkerSession, ledger k6v1.Stream[k6v1.WorkerRow]) {
	w.mu.Lock()
	defer w.mu.Unlock()
	exit := w.exits[session.Runtime()]
	if exit.Sequence == 0 || !w.sessions[session.ID] || exit.UTC < session.StartedUTC {
		return
	}
	active := map[string]k6v1.Record[k6v1.WorkerRow]{}
	for _, row := range ledger.Rows {
		if row.Position.UTC > exit.UTC {
			break
		}
		switch row.Data.Kind {
		case "start":
			active[row.Data.Invocation] = row
		case "finish", "abort":
			delete(active, row.Data.Invocation)
		}
	}
	for _, row := range ledger.Rows {
		id := [2]string{session.ID, row.Data.Invocation}
		if active[id[1]].Position.Sequence != row.Position.Sequence || w.shutdowns[id] {
			continue
		}
		w.append(at, k6v1.WatchRow{Kind: "shutdown", Session: &session, Shutdown: &k6v1.ShutdownBinding{
			Invocation: id[1], Attempt: *row.Data.Attempt, StartSequence: row.Position.Sequence, ExitSequence: exit.Sequence,
		}})
		w.shutdowns[id] = true
	}
}

// Register joins authenticated process startup to an independently observed
// container. The caller's registration receiver authenticates claimant.
func (w *PodWatch) Register(at time.Time, session k6v1.WorkerSession, registration k6v1.Registration) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	i := session.Runtime()
	if !w.known[i] || w.sessions[session.ID] || registration.PodUID != session.PodUID || registration.Claimant != session.Claimant {
		return errors.New("K6_POD_REGISTRATION")
	}
	w.sessions[session.ID] = true
	w.append(at, k6v1.WatchRow{Kind: "session", Session: &session, Registration: &registration})
	return nil
}

// ConfirmDeath uses the kubelet's termination observation and the receiver's
// independently read persisted high-water mark for the exact runtime.
func (w *PodWatch) ConfirmDeath(at time.Time, session k6v1.WorkerSession, ledger *k6v1.Stream[k6v1.WorkerRow]) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	death := w.dead[session.Runtime()]
	seal := ledger.Seal
	if death.IsZero() || !w.sessions[session.ID] || w.deathSealed[session.ID] || seal.Last < 1 {
		return errors.New("K6_POD_DEATH")
	}
	w.deathSealed[session.ID] = true
	ledger.Seal.Mode, ledger.Seal.ThroughUTC = "receiver_final", k6v1.UTC(at)
	w.append(at, k6v1.WatchRow{Kind: "death", Session: &session, Death: &k6v1.Death{
		Observer: w.Stream.Header.Source, ObservedUTC: k6v1.UTC(death), Receiver: seal.Source, FinalSequence: seal.Last, FinalSHA256: seal.SHA256,
	}})
	return nil
}

func (w *PodWatch) Snapshot(at time.Time, terminal bool) (k6v1.Stream[k6v1.WatchRow], error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := w.Stream
	out.Rows = slices.Clone(out.Rows)
	mode := "checkpoint"
	if terminal {
		mode = "terminal"
	}
	if w.cursor == "" || at.Sub(w.progress) > k6v1.MaxSampleGap ||
		out.Rows[len(out.Rows)-1].Position.UTC > k6v1.UTC(w.progress) {
		return out, errors.New("K6_POD_WATCH_STALE:" + strconv.Itoa(len(out.Rows)))
	}
	return out, k6v1.SealStream(&out, mode, k6v1.UTC(w.progress), int64(len(out.Rows)), 0)
}
