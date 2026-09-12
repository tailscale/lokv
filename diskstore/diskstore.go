// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

// Package diskstore provides a filesystem-backed lokv.Store. Values stream to
// disk and are published using atomic, create-only hard links. Multiple Store
// instances and processes may share a directory on a local filesystem that
// supports hard links and consistent directory reads.
//
// The directory must be private to diskstore: callers must prevent modification
// or deletion of published files, including through symlinks. Keys are encoded
// as hexadecimal path components, so slashes, arbitrary bytes, and case
// differences remain distinct even on case-insensitive filesystems. Key length
// is subject to the filesystem's total path length limit.
//
// Files are synced before publication. On non-Windows systems, directory
// entries are also synced before Create succeeds. Callers are responsible for
// durably provisioning the store's root directory. Interrupted creates may
// leave temporary files or empty directories; these are invisible to Get and
// List. There is no automatic cleanup or eviction.
package diskstore

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/tailscale/lokv"
)

// Store implements lokv.Store on disk. Use New to open one.
type Store struct {
	dir string
}

var _ lokv.Store = (*Store)(nil)

const (
	keyChunk  = 128 // hex characters per directory component
	valueName = "!" // sorts before all encoded key components
)

// New opens or creates dir. Existing values are preserved. Relative paths are
// resolved at New time. Newly created directories have mode 0700 and value
// files have mode 0600 (before applying the process umask).
func New(dir string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("diskstore: empty directory")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(abs, "v1"), 0700); err != nil {
		return nil, fmt.Errorf("diskstore: open: %w", err)
	}
	return &Store{dir: abs}, nil
}

func (s *Store) path(key string) string {
	path := filepath.Join(s.dir, "v1")
	encoded := hex.EncodeToString([]byte(key))
	for len(encoded) > 0 {
		n := min(len(encoded), keyChunk)
		path = filepath.Join(path, encoded[:n])
		encoded = encoded[n:]
	}
	return filepath.Join(path, valueName)
}

// List returns the first at most limit complete keys matching the literal
// prefix, in ascending bytewise order. An empty prefix matches every key.
// limit must be positive. Listing walks only branches overlapping prefix, but
// reads and sorts the directory entries in each visited directory.
func (s *Store) List(ctx context.Context, prefix string, limit int) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, errors.New("diskstore: limit must be positive")
	}
	encodedPrefix := hex.EncodeToString([]byte(prefix))
	var keys []string
	var walk func(dir, encoded string, descend bool) error
	walk = func(dir, encoded string, descend bool) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			name := entry.Name()
			switch {
			case name == valueName && strings.HasPrefix(encoded, encodedPrefix):
				if !entry.Type().IsRegular() {
					return fmt.Errorf("diskstore: non-regular value: %s", dir)
				}
				key, err := hex.DecodeString(encoded)
				if err != nil {
					return err
				}
				keys = append(keys, string(key))
			case descend && validChunk(name):
				next := encoded + name
				if !strings.HasPrefix(next, encodedPrefix) && !strings.HasPrefix(encodedPrefix, next) {
					continue
				}
				if !entry.IsDir() {
					return fmt.Errorf("diskstore: non-directory key component: %s", filepath.Join(dir, name))
				}
				if err := walk(filepath.Join(dir, name), next, len(name) == keyChunk); err != nil {
					return err
				}
			}
			if len(keys) == limit {
				return nil
			}
		}
		return nil
	}
	if err := walk(filepath.Join(s.dir, "v1"), "", true); err != nil {
		return nil, fmt.Errorf("diskstore: list: %w", err)
	}
	return keys, nil
}

func validChunk(name string) (ok bool) {
	if len(name) == 0 || len(name) > keyChunk || len(name)%2 != 0 {
		return false
	}
	for _, ch := range name {
		if !(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f') {
			return false
		}
	}
	return true
}

// Get opens an independent stream, or returns an error matching lokv.ErrNotFound.
// The caller must close it. ctx governs subsequent reads.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := os.Open(s.path(key))
	if errors.Is(err, os.ErrNotExist) {
		err = errors.Join(lokv.ErrNotFound, err)
	}
	if err != nil {
		return nil, fmt.Errorf("diskstore: get %q: %w", key, err)
	}
	return &reader{ctx: ctx, file: f}, nil
}

// Create streams value into a temporary file and atomically links the complete
// file into place. A competing creator returns lokv.ErrExists. An error after
// publication (such as a directory sync failure) may leave a complete value.
// The caller retains ownership of value.
func (s *Store) Create(ctx context.Context, key string, value lokv.SizeReaderAt) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if value == nil || value.Size() < 0 {
		return errors.New("diskstore: invalid value size")
	}
	path := s.path(key)
	// Avoid copying a large value that is already present. The link below still
	// arbitrates races with creators that publish after this check.
	if _, err := os.Stat(path); err == nil {
		return lokv.ErrExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.Remove(f.Name())) }()
	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, f.Close())
		}
	}()
	size := value.Size()
	n, err := io.Copy(f, contextReader{ctx, io.NewSectionReader(value, 0, size)})
	if err != nil {
		return err
	}
	if n != size {
		return io.ErrUnexpectedEOF
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	closed = true
	if err := f.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Link(f.Name(), path); err != nil {
		if errors.Is(err, os.ErrExist) {
			err = errors.Join(lokv.ErrExists, err)
		}
		return fmt.Errorf("diskstore: create %q: %w", key, err)
	}
	// Sync newly created ancestors as well as the value's immediate directory.
	for {
		if err := syncDir(dir); err != nil {
			return err
		}
		if dir == s.dir {
			return nil
		}
		dir = filepath.Dir(dir)
	}
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

type reader struct {
	ctx  context.Context
	file *os.File
}

func (r *reader) Read(p []byte) (int, error) {
	return (contextReader{r.ctx, r.file}).Read(p)
}

func (r *reader) Close() error { return r.file.Close() }
