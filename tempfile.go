// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"context"
	"errors"
	"io"
	"os"
	"runtime"
)

// tempFile is an append-only spool with independent readers. It is unlinked
// immediately except on Windows, where Close removes it after closing the FD.
type tempFile struct {
	file tempHandle
	size int64
	name string // nonempty while the file still has a directory entry
}

// tempHandle permits fault injection and resource accounting in tests.
type tempHandle interface {
	io.ReaderAt
	io.Writer
	io.Closer
	Name() string
}

func newTempFile() (*tempFile, error) {
	return newTempFileIn("")
}

func newTempFileIn(dir string) (*tempFile, error) {
	f, err := os.CreateTemp(dir, "lokv-*")
	if err != nil {
		return nil, err
	}
	tmp := &tempFile{file: f, name: f.Name()}
	if runtime.GOOS != "windows" {
		if err := os.Remove(tmp.name); err != nil {
			return nil, errors.Join(err, tmp.Close())
		}
		tmp.name = ""
	}
	return tmp, nil
}

func (f *tempFile) Size() int64 { return f.size }

func (f *tempFile) ReadAt(p []byte, off int64) (int, error) {
	return f.file.ReadAt(p, off)
}

func (f *tempFile) Write(p []byte) (int, error) {
	n, err := f.file.Write(p)
	f.size += int64(n)
	return n, err
}

func (f *tempFile) Close() error {
	err := f.file.Close()
	if f.name != "" {
		err = errors.Join(err, os.Remove(f.name))
	}
	return err
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

type contextWriter struct {
	ctx context.Context
	w   io.Writer
}

func (w contextWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.w.Write(p)
}
