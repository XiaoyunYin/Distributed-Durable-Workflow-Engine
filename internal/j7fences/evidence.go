package j7fences

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

// DirectoryEvidence stores the exact bytes referenced by the ledger in a
// private controller directory. The caller creates that directory outside
// the database, broker and archive storage.
func DirectoryEvidence(dir string) (SaveEvidence, error) {
	if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return nil, errors.New("evidence directory must be an absolute canonical path")
	}
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil || resolved != dir {
		return nil, errors.New("evidence directory must resolve without symlinks")
	}
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("evidence requires a private controller directory")
	}
	return func(name string, body []byte) error {
		if name == "." || name == ".." || filepath.Base(name) != name {
			return errors.New("evidence requires a single file name")
		}
		file, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		n, writeErr := file.Write(body)
		if n != len(body) && writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
			return err
		}
		directory, err := os.Open(dir)
		if err != nil {
			return err
		}
		return errors.Join(directory.Sync(), directory.Close())
	}, nil
}
