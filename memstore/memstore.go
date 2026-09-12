// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

// Package memstore provides a concurrency-safe in-memory lokv.Store for
// tests and ephemeral logs. It has no delete or overwrite operations.
package memstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/tailscale/lokv"
)

// Store is ready to use at its zero value. It must not be copied after use.
type Store struct {
	mu      sync.Mutex // not worth RWMutex overhead
	objects map[string][]byte
	keys    []string
}

var _ lokv.Store = (*Store)(nil)

func (s *Store) List(ctx context.Context, prefix string, limit int) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, errors.New("memstore: limit must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	i := sort.SearchStrings(s.keys, prefix)
	var keys []string
	for ; i < len(s.keys) && len(keys) < limit && strings.HasPrefix(s.keys[i], prefix); i++ {
		keys = append(keys, s.keys[i])
	}
	return keys, nil
}

func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.objects[key]
	if !ok {
		return nil, lokv.ErrNotFound
	}
	return io.NopCloser(reader{ctx, bytes.NewReader(b)}), nil
}

func (s *Store) Create(ctx context.Context, key string, value lokv.SizeReaderAt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if value == nil || value.Size() < 0 {
		return errors.New("memstore: invalid value size")
	}
	size := value.Size()
	b, err := io.ReadAll(reader{ctx, io.NewSectionReader(value, 0, size)})
	if err != nil {
		return err
	}
	if int64(len(b)) != size {
		return io.ErrUnexpectedEOF
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := s.objects[key]; ok {
		return lokv.ErrExists
	}
	if s.objects == nil {
		s.objects = make(map[string][]byte)
	}
	s.objects[key] = b
	i := sort.SearchStrings(s.keys, key)
	s.keys = append(s.keys, "")
	copy(s.keys[i+1:], s.keys[i:])
	s.keys[i] = key
	return nil
}

// reader keeps context cancellation effective after Get returns.
type reader struct {
	ctx context.Context
	r   io.Reader
}

func (r reader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}
