package main

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

func writeSegment(t *testing.T, dir, name string, size int64, start byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := f.Write([]byte{start})
	if err := errors.Join(writeErr, f.Truncate(size), f.Close()); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestArchivePublicationAndRetry(t *testing.T) {
	dir := t.TempDir()
	name := segmentName(1, 5)
	srcDir := t.TempDir()
	src := writeSegment(t, srcDir, name, walSegmentBytes, 5)
	injected := errors.New("directory sync interrupted")
	if err := archiveWAL(dir, src, name, func(path string) error {
		if path != dir {
			t.Errorf("sync directory %q = want %q", path, dir)
		}
		if _, err := readArchive(dir, []string{name}); err != nil {
			t.Errorf("final archive is incomplete at sync: %v", err)
		}
		return injected
	}); !errors.Is(err, injected) {
		t.Fatalf("publication acknowledged an interrupted directory sync: %v", err)
	}
	calls := 0
	syncDir := func(string) error { calls++; return nil }
	if err := archiveWAL(dir, src, name, syncDir); err != nil || calls != 1 {
		t.Fatalf("identical retry: err=%v, directory syncs=%d", err, calls)
	}
	before, err := readArchive(dir, []string{name})
	if err != nil {
		t.Fatal(err)
	}
	src = writeSegment(t, srcDir, name, walSegmentBytes, 6)
	if err := archiveWAL(dir, src, name, syncDir); err == nil || !strings.Contains(err.Error(), "filename conflict") {
		t.Fatalf("conflicting retry = %v", err)
	}
	after, err := readArchive(dir, []string{name})
	if err != nil || !slices.Equal(before, after) {
		t.Fatalf("conflicting retry changed archive: %v %v", after, err)
	}
	contents, err := os.ReadDir(dir)
	if err != nil || len(contents) != 1 || contents[0].Name() != name {
		t.Fatalf("archive contains leftover staging files: %v %v", contents, err)
	}
}

func TestArchiveRejectsNonSegments(t *testing.T) {
	dir, srcDir := t.TempDir(), t.TempDir()
	for _, tc := range []struct {
		name string
		size int64
	}{
		{segmentName(1, 9), walSegmentBytes - 1},
		{segmentName(1, 9), walSegmentBytes + 1},
		{segmentName(1, 9) + ".partial", walSegmentBytes},
		{"000000010000000000000100", walSegmentBytes},
		{"escape", 1},
	} {
		src := writeSegment(t, srcDir, tc.name, tc.size, 2)
		if err := archiveWAL(dir, src, tc.name, func(string) error {
			t.Error("invalid file reached directory sync")
			return nil
		}); err == nil {
			t.Errorf("accepted %s of size %d", tc.name, tc.size)
		}
	}
	contents, err := os.ReadDir(dir)
	if err != nil || len(contents) != 0 {
		t.Fatalf("invalid files appeared in archive: %v %v", contents, err)
	}
}

func TestArchiveConcurrentConflict(t *testing.T) {
	dir := t.TempDir()
	name := "00000002.history"
	paths := []string{
		writeSegment(t, t.TempDir(), name, 16, 1),
		writeSegment(t, t.TempDir(), name, 16, 2),
	}
	var wg sync.WaitGroup
	results := make([]error, len(paths))
	for i, path := range paths {
		wg.Go(func() {
			results[i] = archiveWAL(dir, path, name, func(string) error { return nil })
		})
	}
	wg.Wait()
	if (results[0] == nil) == (results[1] == nil) {
		t.Fatalf("want one winner and one conflict: %v", results)
	}
	stored, err := readArchive(dir, []string{name})
	if err != nil {
		t.Fatal(err)
	}
	winner := 0
	if results[0] != nil {
		winner = 1
	}
	want, err := readArchive(filepath.Dir(paths[winner]), []string{name})
	if err != nil || !slices.Equal(stored, want) {
		t.Fatalf("archive does not match the winner: %v %v", stored, err)
	}
}

func TestWALRangeHasNoLogBoundaryGap(t *testing.T) {
	start, err := parseLSN("0/FF000000")
	if err != nil || start != 255*walSegmentBytes {
		t.Fatalf("log boundary decode = %X, %v", start, err)
	}
	names, err := segmentRange(1, start/walSegmentBytes, (start/walSegmentBytes)+1)
	want := []string{"0000000100000000000000FF", "000000010000000100000000"}
	if err != nil || !slices.Equal(names, want) {
		t.Fatalf("range = %v, %v", names, err)
	}
	for _, invalid := range []string{"0/", "0/100000000", "0/00/G", "GG/FE"} {
		if _, err := parseLSN(invalid); err == nil {
			t.Errorf("accepted invalid LSN %q", invalid)
		}
	}
	if _, err := segmentRange(1, 0, maxArchiveBytes/walSegmentBytes); err == nil {
		t.Fatal("accepted an oversized archive range")
	}
}
