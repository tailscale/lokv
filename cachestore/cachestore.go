// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

// Package cachestore wraps an authoritative lokv.Store with an optional local
// copy of its immutable objects. Get checks the cache first; List always reads
// the origin so head discovery sees other writers. Create writes the origin
// first and fills the cache only after confirmed success.
//
// Each cache namespace must belong exclusively to one origin namespace. Do not
// reuse it for another bucket or write unrelated objects into it: cache hits
// are trusted without contacting the origin. Cache misses are not remembered.
//
// Read misses stream to the caller while spooling to temporary disk. A complete
// read followed by a successful Close publishes the cache entry synchronously
// in Close. Early Close does not drain the stream or fill the cache. Writes fill
// the cache synchronously in Create using the caller's existing SizeReaderAt.
// There are no background workers; concurrent misses may fetch the same object.
//
// Cache lookup and fill failures are best effort and can be observed through
// Config.OnCacheError. Once a cache hit's reader has been returned, its read or
// close errors are returned to the caller; the stream cannot safely switch to
// the origin midway. This package does not detect or repair corrupt cache bytes.
//
// There is no automatic eviction or capacity limit. With diskstore, use a
// dedicated directory and stop using it before removing it to reclaim space or
// recover from corruption. A cache is disposable; the origin remains subject
// to lokv.Store's immutability requirements.
package cachestore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/tailscale/lokv"
)

// Config specifies the authoritative store and its private cache namespace.
type Config struct {
	Origin lokv.Store // source of truth for every key
	Cache  lokv.Store // stores copies of successful origin reads and writes

	// TempDir holds scratch files for read misses. Empty uses the OS default.
	// Files are immediately unlinked except on Windows, which removes them on
	// Close. Scratch usage grows with the objects being read concurrently.
	TempDir string

	// OnCacheError optionally observes cache lookup, fill, and cleanup errors.
	// Ordinary cache misses and competing fills (ErrExists) are not reported.
	// Calls are synchronous and may occur concurrently; the callback must be
	// safe for concurrent use. Errors include the operation and key, not values.
	OnCacheError func(error)
}

// Store implements lokv.Store with a best-effort cache. Use New to create one.
type Store struct {
	origin  lokv.Store
	cache   lokv.Store
	onError func(error)
	newTemp func() (*spool, error)
}

var _ lokv.Store = (*Store)(nil)

// New validates cfg without performing I/O. Both Origin and Cache are required.
func New(cfg Config) (*Store, error) {
	if nilStore(cfg.Origin) || nilStore(cfg.Cache) {
		return nil, errors.New("cachestore: origin and cache are required")
	}
	return &Store{
		origin:  cfg.Origin,
		cache:   cfg.Cache,
		onError: cfg.OnCacheError,
		newTemp: func() (*spool, error) { return newSpool(cfg.TempDir) },
	}, nil
}

func nilStore(s lokv.Store) (ok bool) {
	if s == nil {
		return true
	}
	v := reflect.ValueOf(s)
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Interface, reflect.Chan:
		return v.IsNil()
	}
	return false
}

func (s *Store) report(op, key string, err error) {
	if err != nil && s.onError != nil {
		s.onError(fmt.Errorf("cachestore: %s %q: %w", op, key, err))
	}
}

// List returns the origin's first at most limit complete keys matching the
// literal prefix, in ascending bytewise order. Empty prefix matches every key;
// limit must be positive. The cache is never consulted.
func (s *Store) List(ctx context.Context, prefix string, limit int) ([]string, error) {
	return s.origin.List(ctx, prefix, limit)
}

// Get opens an independent cache or origin stream, or returns an origin error
// such as lokv.ErrNotFound. The caller must close the reader. ctx governs reads
// and cache fills. On an origin read, reaching EOF and closing successfully
// attempts to fill the cache; a failed fill does not fail Close.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if body, err := s.cache.Get(ctx, key); err == nil {
		return &cacheReader{s: s, key: key, body: body}, nil
	} else if !errors.Is(err, lokv.ErrNotFound) {
		s.report("get", key, err)
	}
	body, err := s.origin.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	tmp, err := s.newTemp()
	s.report("create scratch file", key, err)
	return &fillReader{s: s, ctx: ctx, key: key, body: body, tmp: tmp}, nil
}

// Create creates the origin object first, then best-effort caches its bytes.
// If the origin returns any error, including lokv.ErrExists or an ambiguous
// write error, the attempted bytes are never cached: another value may have won.
// The caller retains ownership of value; neither store may retain it on return.
func (s *Store) Create(ctx context.Context, key string, value lokv.SizeReaderAt) error {
	if err := s.origin.Create(ctx, key, value); err != nil {
		return err
	}
	s.fill(ctx, key, value)
	return nil
}

func (s *Store) fill(ctx context.Context, key string, value lokv.SizeReaderAt) {
	if err := s.cache.Create(ctx, key, value); !errors.Is(err, lokv.ErrExists) {
		s.report("fill", key, err)
	}
}

type fillReader struct {
	s        *Store
	ctx      context.Context
	key      string
	body     io.ReadCloser
	tmp      *spool
	eof      bool // observed a complete origin stream
	closed   bool
	closeErr error
}

func (r *fillReader) discard() {
	if r.tmp != nil {
		r.s.report("close scratch file", r.key, r.tmp.Close())
		r.tmp = nil
	}
}

func (r *fillReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	if err := r.ctx.Err(); err != nil {
		r.discard()
		return 0, err
	}
	n, err := r.body.Read(p)
	if r.tmp != nil && n > 0 {
		if _, writeErr := r.tmp.Write(p[:n]); writeErr != nil {
			r.s.report("write scratch file", r.key, writeErr)
			r.discard()
		}
	}
	if err == io.EOF {
		r.eof = true
	} else if err != nil {
		r.discard()
	}
	return n, err
}

func (r *fillReader) Close() error {
	if r.closed {
		return r.closeErr
	}
	r.closed = true
	r.closeErr = r.body.Close()
	defer r.discard()
	if r.tmp != nil && r.eof && r.closeErr == nil && r.ctx.Err() == nil {
		r.s.fill(r.ctx, r.key, r.tmp)
	}
	return r.closeErr
}

type cacheReader struct {
	s    *Store
	key  string
	body io.ReadCloser
}

func (r *cacheReader) Read(p []byte) (int, error) {
	n, err := r.body.Read(p)
	if err != io.EOF {
		r.s.report("read", r.key, err)
	}
	return n, err
}

func (r *cacheReader) Close() error {
	err := r.body.Close()
	r.s.report("close", r.key, err)
	return err
}
