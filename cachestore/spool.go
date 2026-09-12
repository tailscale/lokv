// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package cachestore

import (
	"errors"
	"io"
	"os"
	"runtime"
)

// spool holds a downloaded object without buffering the whole value in memory.
type spool struct {
	file spoolFile
	size int64
	name string // nonempty until the file is unlinked
}

type spoolFile interface {
	io.ReaderAt
	io.Writer
	io.Closer
}

func newSpool(dir string) (*spool, error) {
	f, err := os.CreateTemp(dir, "lokv-cache-*")
	if err != nil {
		return nil, err
	}
	tmp := &spool{file: f, name: f.Name()}
	if runtime.GOOS != "windows" {
		if err := os.Remove(tmp.name); err != nil {
			return nil, errors.Join(err, tmp.Close())
		}
		tmp.name = ""
	}
	return tmp, nil
}

func (f *spool) Size() int64 { return f.size }

func (f *spool) ReadAt(p []byte, off int64) (int, error) {
	return f.file.ReadAt(p, off)
}

func (f *spool) Write(p []byte) (int, error) {
	n, err := f.file.Write(p)
	f.size += int64(n)
	if n != len(p) && err == nil {
		err = io.ErrShortWrite
	}
	return n, err
}

func (f *spool) Close() error {
	err := f.file.Close()
	if f.name != "" {
		err = errors.Join(err, os.Remove(f.name))
	}
	return err
}
