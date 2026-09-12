// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package cachestore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tailscale/lokv"
	"github.com/tailscale/lokv/diskstore"
	"github.com/tailscale/lokv/memstore"
	"github.com/tailscale/lokv/storetest"
)

type testStore struct {
	lokv.Store
	get    func(context.Context, string) (io.ReadCloser, error)
	create func(context.Context, string, lokv.SizeReaderAt) error
	list   func(context.Context, string, int) ([]string, error)
}

func (s testStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if s.get != nil {
		return s.get(ctx, key)
	}
	return s.Store.Get(ctx, key)
}

func (s testStore) Create(ctx context.Context, key string, value lokv.SizeReaderAt) error {
	if s.create != nil {
		return s.create(ctx, key, value)
	}
	return s.Store.Create(ctx, key, value)
}

func (s testStore) List(ctx context.Context, prefix string, limit int) ([]string, error) {
	if s.list != nil {
		return s.list(ctx, prefix, limit)
	}
	return s.Store.List(ctx, prefix, limit)
}

func newStore(t *testing.T, cfg Config) *Store {
	t.Helper()
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func readAll(t *testing.T, store lokv.Store, key string) string {
	t.Helper()
	body, err := store.Get(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(body)
	if err := errors.Join(err, body.Close()); err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func create(t *testing.T, store lokv.Store, key, data string) {
	t.Helper()
	if err := store.Create(t.Context(), key, strings.NewReader(data)); err != nil {
		t.Fatal(err)
	}
}

func missing(t *testing.T, store lokv.Store, key string) {
	t.Helper()
	body, err := store.Get(t.Context(), key)
	if body != nil {
		body.Close()
	}
	if !errors.Is(err, lokv.ErrNotFound) {
		t.Fatalf("Get(%q) = %v, want ErrNotFound", key, err)
	}
}

func TestConformance(t *testing.T) {
	for _, backend := range []string{"memory", "disk"} {
		t.Run(backend, func(t *testing.T) {
			var cache lokv.Store = new(memstore.Store)
			if backend == "disk" {
				var err error
				cache, err = diskstore.New(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
			}
			s := newStore(t, Config{Origin: new(memstore.Store), Cache: cache, TempDir: t.TempDir(), OnCacheError: func(err error) {
				if !errors.Is(err, context.Canceled) {
					t.Errorf("unexpected cache error: %v", err)
				}
			}})
			storetest.Test(t, s, "conformance/")
		})
	}
}

func TestRouting(t *testing.T) {
	origin, cache := new(memstore.Store), new(memstore.Store)
	var gets, lists atomic.Int64
	s := newStore(t, Config{Origin: testStore{
		Store: origin,
		get: func(ctx context.Context, key string) (io.ReadCloser, error) {
			gets.Add(1)
			return origin.Get(ctx, key)
		},
		list: func(ctx context.Context, prefix string, limit int) ([]string, error) {
			lists.Add(1)
			return origin.List(ctx, prefix, limit)
		},
	}, Cache: testStore{Store: cache, list: func(context.Context, string, int) ([]string, error) {
		t.Fatal("listed cache")
		return nil, nil
	}}})
	missing(t, s, "a")
	create(t, origin, "a", "external writer")
	if got := readAll(t, s, "a"); got != "external writer" {
		t.Fatal(got)
	}
	if got := readAll(t, cache, "a"); got != "external writer" {
		t.Fatal(got)
	}
	if got := readAll(t, s, "a"); got != "external writer" || gets.Load() != 2 {
		t.Fatalf("cache hit = %q; origin gets = %d, want 2", got, gets.Load())
	}
	create(t, origin, "b", "not cached")
	for range 2 {
		got, err := s.List(t.Context(), "", 10)
		if err != nil || !reflect.DeepEqual(got, []string{"a", "b"}) {
			t.Fatalf("List = %q, %v", got, err)
		}
	}
	if lists.Load() != 2 {
		t.Fatalf("origin lists = %d, want 2", lists.Load())
	}
	create(t, s, "c", "write through")
	if got := readAll(t, cache, "c"); got != "write through" {
		t.Fatal(got)
	}
}

func TestOriginCreateErrorsDoNotCache(t *testing.T) {
	ambiguous := errors.New("lost origin response")
	for _, tc := range []struct {
		name   string
		stored string
		err    error
	}{
		{"exists", "winner", lokv.ErrExists},
		{"ambiguous loser", "winner", ambiguous},
		{"ambiguous winner", "attempted", ambiguous},
		{"failure", "", ambiguous},
	} {
		t.Run(tc.name, func(t *testing.T) {
			origin, cache := new(memstore.Store), new(memstore.Store)
			s := newStore(t, Config{Origin: testStore{Store: origin, create: func(ctx context.Context, key string, _ lokv.SizeReaderAt) error {
				if tc.stored != "" {
					create(t, origin, key, tc.stored)
				}
				return tc.err
			}}, Cache: cache})
			if err := s.Create(t.Context(), "key", strings.NewReader("attempted")); err != tc.err {
				t.Fatalf("Create error = %v, want original %v", err, tc.err)
			}
			missing(t, cache, "key")
			if tc.stored != "" {
				if got := readAll(t, s, "key"); got != tc.stored {
					t.Fatalf("resolved bytes = %q, want %q", got, tc.stored)
				}
				if got := readAll(t, cache, "key"); got != tc.stored {
					t.Fatalf("cached resolved bytes = %q", got)
				}
			}
		})
	}
}

type testBody struct {
	io.Reader
	closeErr error
	closes   int
}

func (b *testBody) Close() error {
	b.closes++
	return b.closeErr
}

type terminalReader struct {
	data string
	err  error
}

func (r *terminalReader) Read(p []byte) (int, error) {
	n := copy(p, r.data)
	r.data = r.data[n:]
	if r.data == "" {
		return n, r.err
	}
	return n, nil
}

func TestFillRequiresCompleteReadAndClose(t *testing.T) {
	readErr, closeErr := errors.New("read failed"), errors.New("close failed")
	for _, name := range []string{"complete", "data with EOF", "empty", "unread", "early close", "no EOF", "read error", "read error then EOF", "close error", "cancel during read", "cancel before close"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			body := &testBody{Reader: strings.NewReader("value")}
			if name == "data with EOF" {
				body.Reader = &terminalReader{data: "value", err: io.EOF}
			}
			if name == "empty" {
				body.Reader = strings.NewReader("")
			}
			if strings.HasPrefix(name, "read error") {
				body.Reader = &terminalReader{data: "value", err: readErr}
			}
			if name == "close error" {
				body.closeErr = closeErr
			}
			cache := new(memstore.Store)
			dir := t.TempDir()
			s := newStore(t, Config{Origin: testStore{get: func(context.Context, string) (io.ReadCloser, error) { return body, nil }}, Cache: cache, TempDir: dir})
			r, err := s.Get(ctx, "key")
			if err != nil {
				t.Fatal(err)
			}
			if name == "cancel during read" {
				cancel()
			}
			switch name {
			case "unread":
			case "early close", "no EOF":
				count := 2
				if name == "no EOF" {
					count = 5
				}
				if _, err := io.ReadFull(r, make([]byte, count)); err != nil {
					t.Fatal(err)
				}
			default:
				_, err = io.ReadAll(r)
				var want error
				if strings.HasPrefix(name, "read error") {
					want = readErr
				} else if name == "cancel during read" {
					want = context.Canceled
				}
				if !errors.Is(err, want) {
					t.Fatalf("Read = %v, want %v", err, want)
				}
			}
			if name == "read error then EOF" {
				body.Reader = strings.NewReader("")
				if _, err := io.Copy(io.Discard, r); err != nil {
					t.Fatal(err)
				}
			}
			missing(t, cache, "key")
			if name == "cancel before close" {
				cancel()
			}
			for range 2 {
				if err := r.Close(); err != body.closeErr {
					t.Fatalf("Close = %v, want %v", err, body.closeErr)
				}
			}
			if body.closes != 1 {
				t.Fatalf("origin closed %d times", body.closes)
			}
			if name == "complete" || name == "data with EOF" || name == "empty" {
				want := "value"
				if name == "empty" {
					want = ""
				}
				if got := readAll(t, cache, "key"); got != want {
					t.Fatalf("cache = %q, want %q", got, want)
				}
			} else {
				missing(t, cache, "key")
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 0 {
				t.Fatalf("scratch cleanup: %v, %v", entries, err)
			}
		})
	}
}

func TestBestEffortFailures(t *testing.T) {
	failure := errors.New("cache unavailable")
	origin := new(memstore.Store)
	create(t, origin, "read", "origin value")
	var reported []error
	s := newStore(t, Config{Origin: origin, Cache: testStore{
		get:    func(context.Context, string) (io.ReadCloser, error) { return nil, failure },
		create: func(context.Context, string, lokv.SizeReaderAt) error { return failure },
	}, OnCacheError: func(err error) { reported = append(reported, err) }})
	if got := readAll(t, s, "read"); got != "origin value" {
		t.Fatal(got)
	}
	create(t, s, "write", "new value")
	if got := readAll(t, origin, "write"); got != "new value" {
		t.Fatal(got)
	}
	if len(reported) != 3 {
		t.Fatalf("reported errors = %v, want lookup and two fill failures", reported)
	}
	for _, err := range reported {
		if !errors.Is(err, failure) {
			t.Fatal(err)
		}
	}
	// Losing a competing fill is expected and does not call the error hook.
	reported = nil
	s.cache = testStore{get: func(context.Context, string) (io.ReadCloser, error) { return nil, lokv.ErrNotFound }, create: func(context.Context, string, lokv.SizeReaderAt) error { return lokv.ErrExists }}
	readAll(t, s, "read")
	if len(reported) != 0 {
		t.Fatalf("reported competing fill: %v", reported)
	}
}

func TestCacheHitStreamErrors(t *testing.T) {
	failure := errors.New("bad cached stream")
	var reported []error
	body := &testBody{Reader: &terminalReader{data: "prefix", err: failure}, closeErr: failure}
	s := newStore(t, Config{Origin: testStore{get: func(context.Context, string) (io.ReadCloser, error) {
		t.Fatal("unexpected origin fallback")
		return nil, nil
	}}, Cache: testStore{get: func(context.Context, string) (io.ReadCloser, error) { return body, nil }}, OnCacheError: func(err error) { reported = append(reported, err) }})
	r, err := s.Get(t.Context(), "key")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	if string(got) != "prefix" || !errors.Is(err, failure) {
		t.Fatalf("cached read = %q, %v", got, err)
	}
	if err := r.Close(); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if len(reported) != 2 {
		t.Fatalf("reported = %v, want read and close failures", reported)
	}
}

func TestConcurrentMisses(t *testing.T) {
	origin, cache := new(memstore.Store), new(memstore.Store)
	create(t, origin, "key", strings.Repeat("value", 10000))
	const readers = 16
	var opened atomic.Int64
	allOpen := make(chan struct{})
	s := newStore(t, Config{Origin: testStore{Store: origin, get: func(ctx context.Context, key string) (io.ReadCloser, error) {
		if opened.Add(1) == readers {
			close(allOpen)
		}
		<-allOpen
		return origin.Get(ctx, key)
	}}, Cache: cache, OnCacheError: func(err error) { t.Errorf("cache error: %v", err) }})
	var wg sync.WaitGroup
	for range readers {
		wg.Go(func() {
			if got := readAll(t, s, "key"); got != strings.Repeat("value", 10000) {
				t.Errorf("read length %d", len(got))
			}
		})
	}
	wg.Wait()
	if got := readAll(t, cache, "key"); got != strings.Repeat("value", 10000) {
		t.Fatal("incorrect cached value")
	}
}

type faultySpool struct {
	spoolFile
	writeErr error
	short    bool
	closeErr error
	closed   bool
}

func (f *faultySpool) Write(p []byte) (int, error) {
	if f.short {
		return 0, nil
	}
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	return f.spoolFile.Write(p)
}

func (f *faultySpool) Close() error {
	f.closed = true
	return errors.Join(f.spoolFile.Close(), f.closeErr)
}

func TestScratchFailures(t *testing.T) {
	failure := errors.New("scratch failure")
	for _, name := range []string{"create", "write", "short write", "close"} {
		t.Run(name, func(t *testing.T) {
			origin, cache := new(memstore.Store), new(memstore.Store)
			create(t, origin, "key", "value")
			var reported []error
			dir := t.TempDir()
			cfg := Config{Origin: origin, Cache: cache, TempDir: dir, OnCacheError: func(err error) { reported = append(reported, err) }}
			if name == "create" {
				cfg.TempDir = filepath.Join(dir, "missing")
			}
			s := newStore(t, cfg)
			var file *faultySpool
			if name != "create" {
				s.newTemp = func() (*spool, error) {
					tmp, err := newSpool(dir)
					if err != nil {
						return nil, err
					}
					file = &faultySpool{spoolFile: tmp.file}
					switch name {
					case "write":
						file.writeErr = failure
					case "short write":
						file.short = true
					case "close":
						file.closeErr = failure
					}
					tmp.file = file
					return tmp, nil
				}
			}
			if got := readAll(t, s, "key"); got != "value" {
				t.Fatal(got)
			}
			if name == "close" {
				if got := readAll(t, cache, "key"); got != "value" {
					t.Fatal(got)
				}
			} else {
				missing(t, cache, "key")
			}
			if len(reported) != 1 {
				t.Fatalf("reported = %v, want one scratch error", reported)
			}
			if file != nil && !file.closed {
				t.Fatal("scratch file not closed")
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 0 {
				t.Fatalf("scratch cleanup: %v, %v", entries, err)
			}
		})
	}
}

func TestSpoolUnlinkAndRandomAccess(t *testing.T) {
	dir := t.TempDir()
	tmp, err := newSpool(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tmp.Close()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && len(entries) != 0 {
		t.Fatal("scratch file still linked")
	}
	if _, err := tmp.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := tmp.Write([]byte("second")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 6)
	if _, err := tmp.ReadAt(got, 5); err != nil || string(got) != "second" || tmp.Size() != 11 {
		t.Fatalf("ReadAt = %q, %v; size = %d", got, err, tmp.Size())
	}
}

func TestConfigValidation(t *testing.T) {
	var typedNil *memstore.Store
	for _, cfg := range []Config{{}, {Origin: new(memstore.Store)}, {Cache: new(memstore.Store)}, {Origin: typedNil, Cache: new(memstore.Store)}, {Origin: new(memstore.Store), Cache: typedNil}} {
		if _, err := New(cfg); err == nil {
			t.Fatal("accepted missing or nil store")
		}
	}
}

func TestCreateUsesOriginalSource(t *testing.T) {
	source := bytes.NewReader([]byte("body"))
	var order []string
	s := newStore(t, Config{Origin: testStore{create: func(_ context.Context, _ string, value lokv.SizeReaderAt) error {
		if value != source {
			t.Fatal("origin did not receive original source")
		}
		order = append(order, "origin")
		return nil
	}}, Cache: testStore{create: func(_ context.Context, _ string, value lokv.SizeReaderAt) error {
		if value != source {
			t.Fatal("cache did not receive original source")
		}
		order = append(order, "cache")
		return nil
	}}})
	if err := s.Create(t.Context(), "key", source); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"origin", "cache"}) {
		t.Fatalf("create order = %v", order)
	}
}

type generatedReader struct {
	remaining  int64
	read       int64
	maxRequest int
}

func (r *generatedReader) Read(p []byte) (int, error) {
	r.maxRequest = max(r.maxRequest, len(p))
	n := int(min(int64(len(p)), r.remaining))
	for i := range n {
		p[i] = byte((r.read + int64(i)) % 251)
	}
	r.remaining -= int64(n)
	r.read += int64(n)
	if r.remaining == 0 {
		return n, io.EOF
	}
	return n, nil
}

func TestReadMissStreamsToDisk(t *testing.T) {
	const size = 16 << 20
	source := &generatedReader{remaining: size}
	body := &testBody{Reader: source}
	fills := 0
	s := newStore(t, Config{Origin: testStore{get: func(context.Context, string) (io.ReadCloser, error) { return body, nil }}, Cache: testStore{
		get: func(context.Context, string) (io.ReadCloser, error) { return nil, lokv.ErrNotFound },
		create: func(_ context.Context, _ string, value lokv.SizeReaderAt) error {
			fills++
			if body.closes != 1 || value.Size() != size {
				t.Fatalf("fill: origin closes = %d, size = %d", body.closes, value.Size())
			}
			// Cache can reread arbitrary offsets without a whole-object buffer.
			for _, off := range []int64{size - 2048, 0, size / 2} {
				var got [1024]byte
				if _, err := value.ReadAt(got[:], off); err != nil {
					t.Fatal(err)
				}
				for i, ch := range got {
					if ch != byte((off+int64(i))%251) {
						t.Fatalf("bad byte at offset %d", off+int64(i))
					}
				}
			}
			return nil
		},
	}, OnCacheError: func(err error) { t.Errorf("cache error: %v", err) }})
	r, err := s.Get(t.Context(), "large")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if source.read != 0 {
		t.Fatal("Get eagerly downloaded the body")
	}
	var first [8]byte
	if _, err := io.ReadFull(r, first[:]); err != nil {
		t.Fatal(err)
	}
	if source.read != 8 {
		t.Fatalf("read ahead: %d bytes", source.read)
	}
	if _, err := io.Copy(io.Discard, r); err != nil {
		t.Fatal(err)
	}
	if source.read != size || source.maxRequest > 32<<10 || fills != 0 {
		t.Fatalf("before Close: read = %d, largest request = %d, fills = %d", source.read, source.maxRequest, fills)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if fills != 1 {
		t.Fatalf("fills = %d, want 1", fills)
	}
}
