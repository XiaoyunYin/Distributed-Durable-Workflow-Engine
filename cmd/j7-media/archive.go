package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const (
	walSegmentBytes = 16 << 20
	maxArchiveBytes = 512 << 20
	maxMetadataSize = 64 << 10
	segmentsPerLog  = (1 << 32) / walSegmentBytes
)

var archiveName = regexp.MustCompile(
	`^(?:[0-9A-F]{24}(?:\.[0-9A-F]{8}\.backup)?|[0-9A-F]{8}\.history)$`,
)

type archivedFile struct {
	Name   string `json:"name"`
	Size   int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

func segmentName(timeline uint32, index uint64) string {
	return fmt.Sprintf("%08X%08X%08X", timeline, index/segmentsPerLog, index%segmentsPerLog)
}

func parseLSN(value string) (uint64, error) {
	high, low, ok := strings.Cut(value, "/")
	if !ok || len(high) > 8 || len(low) > 8 {
		return 0, fmt.Errorf("invalid WAL position %q", value)
	}
	left, leftErr := strconv.ParseUint(high, 16, 32)
	right, rightErr := strconv.ParseUint(low, 16, 32)
	if leftErr != nil || rightErr != nil {
		return 0, fmt.Errorf("invalid WAL position %q", value)
	}
	return left<<32 | right, nil
}

func segmentRange(timeline uint32, first, last uint64) ([]string, error) {
	if timeline == 0 || last < first || last > ^uint64(0)/walSegmentBytes ||
		last-first >= maxArchiveBytes/walSegmentBytes {
		return nil, fmt.Errorf("invalid or oversized archive range on timeline %d", timeline)
	}
	names := make([]string, 0, last-first+1)
	for index := first; index <= last; index++ {
		names = append(names, segmentName(timeline, index))
	}
	return names, nil
}

func archiveSizeLimit(name string) (int64, error) {
	if !archiveName.MatchString(name) || name[:8] == "00000000" {
		return 0, fmt.Errorf("invalid archive filename %q", name)
	}
	if len(name) >= 24 {
		segment, _ := strconv.ParseUint(name[16:24], 16, 32)
		if segment >= segmentsPerLog {
			return 0, fmt.Errorf("invalid 16 MiB segment number in %q", name)
		}
	}
	if len(name) == 24 {
		return walSegmentBytes, nil
	}
	return maxMetadataSize, nil
}

func streamArchive(dst io.Writer, src *os.File, name string) (archivedFile, error) {
	limit, err := archiveSizeLimit(name)
	if err != nil {
		return archivedFile{}, err
	}
	sha := sha256.New()
	size, err := io.Copy(io.MultiWriter(dst, sha), io.LimitReader(src, limit+1))
	if err != nil {
		return archivedFile{}, err
	}
	if size == 0 || size > limit || (len(name) == 24 && size != walSegmentBytes) {
		return archivedFile{}, fmt.Errorf("incomplete or oversized archive file %s (%d bytes)", name, size)
	}
	return archivedFile{Name: name, Size: size, SHA256: hex.EncodeToString(sha.Sum(nil))}, nil
}

func openRegular(path string, flags int) (*os.File, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", filepath.Base(path))
	}
	f, err := os.OpenFile(path, flags, 0)
	if err != nil {
		return nil, err
	}
	after, err := f.Stat()
	if err != nil || !os.SameFile(before, after) {
		_ = f.Close()
		return nil, fmt.Errorf("%s changed while opening", filepath.Base(path))
	}
	return f, nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// A synced staging file is published with a no-replace hard link on the
// archive filesystem. An identical retry also syncs the existing file and
// directory, including when the previous process died before acknowledging.
func archiveWAL(dir, source, name string, syncDir func(string) error) error {
	if _, err := archiveSizeLimit(name); err != nil {
		return err
	}
	if filepath.Base(source) != name {
		return errors.New("archive source basename differs from the PostgreSQL filename")
	}
	src, err := openRegular(source, os.O_RDONLY)
	if err != nil {
		return err
	}
	defer src.Close()
	staged, err := os.CreateTemp(dir, ".j7-archive-")
	if err != nil {
		return err
	}
	defer os.Remove(staged.Name())
	proposed, copyErr := streamArchive(staged, src, name)
	if copyErr != nil {
		return errors.Join(copyErr, staged.Close())
	}
	if err := errors.Join(staged.Sync(), staged.Close()); err != nil {
		return err
	}

	destination := filepath.Join(dir, name)
	if err := os.Link(staged.Name(), destination); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		if err := verifyArchiveRetry(destination, proposed); err != nil {
			return err
		}
	}
	if err := os.Remove(staged.Name()); err != nil {
		return err
	}
	return syncDir(dir)
}

func verifyArchiveRetry(path string, proposed archivedFile) error {
	f, err := openRegular(path, os.O_RDWR)
	if err != nil {
		return err
	}
	defer f.Close()
	existing, err := streamArchive(io.Discard, f, proposed.Name)
	if err != nil {
		return err
	}
	if existing != proposed {
		return fmt.Errorf("archive filename conflict for %s", proposed.Name)
	}
	return f.Sync()
}

func readArchive(dir string, names []string) ([]archivedFile, error) {
	files := make([]archivedFile, 0, len(names))
	for _, name := range names {
		if _, err := archiveSizeLimit(name); err != nil {
			return nil, err
		}
		f, err := openRegular(filepath.Join(dir, name), os.O_RDONLY)
		if err != nil {
			return nil, fmt.Errorf("read archive %s: %w", name, err)
		}
		row, err := streamArchive(io.Discard, f, name)
		if err := errors.Join(err, f.Close()); err != nil {
			return nil, err
		}
		files = append(files, row)
	}
	return files, nil
}
