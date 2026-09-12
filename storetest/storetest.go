// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

// Package storetest provides the lokv.Store behavioral conformance suite.
package storetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tailscale/lokv"
)

// Test exercises an empty, disposable namespace. It permanently creates objects
// and never deletes them. Each invocation must receive a fresh store or prefix.
func Test(t *testing.T, store lokv.Store, prefix string) {
	t.Helper()
	ctx := context.Background()
	key := func(s string) string { return prefix + s }
	if _, err := store.Get(ctx, key("missing")); !errors.Is(err, lokv.ErrNotFound) {
		t.Fatalf("missing GET: %v", err)
	}
	for _, suffix := range []string{"c", "a", "b", "aa"} {
		body := []byte(suffix)
		if err := store.Create(ctx, key(suffix), bytes.NewReader(body)); err != nil {
			t.Fatal(err)
		}
		body[0] = '!'
		actual, err := readAll(ctx, store, key(suffix))
		if err != nil || string(actual) != suffix {
			t.Fatalf("visibility/ownership: %q, %v", actual, err)
		}
		actual[0] = '!'
		actual, err = readAll(ctx, store, key(suffix))
		if err != nil || string(actual) != suffix {
			t.Fatalf("GET ownership: %q, %v", actual, err)
		}
		if err := store.Create(ctx, key(suffix), bytes.NewReader([]byte("overwrite"))); !errors.Is(err, lokv.ErrExists) {
			t.Fatalf("duplicate: %v", err)
		}
	}
	for _, limit := range []int{1, 2, 4, 10} {
		keys, err := store.List(ctx, prefix, limit)
		expected := []string{key("a"), key("aa"), key("b"), key("c")}
		expected = expected[:min(limit, len(expected))]
		if err != nil || !reflect.DeepEqual(keys, expected) {
			t.Fatalf("LIST %d: %q, %v", limit, keys, err)
		}
	}
	keys, err := store.List(ctx, key("a"), 10)
	if err != nil || !reflect.DeepEqual(keys, []string{key("a"), key("aa")}) {
		t.Fatalf("prefix LIST: %q, %v", keys, err)
	}
	var wins atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 32; i++ {
		wg.Go(func() {
			<-start
			body := bytes.Repeat([]byte(fmt.Sprintf("%02d", i)), 32768)
			err := store.Create(ctx, key("race"), bytes.NewReader(body))
			if err == nil {
				wins.Add(1)
			} else if !errors.Is(err, lokv.ErrExists) {
				t.Errorf("race CREATE: %v", err)
				return
			}
			actual, err := readAll(ctx, store, key("race"))
			if err != nil || len(actual) != len(body) {
				t.Errorf("race GET: length %d, %v", len(actual), err)
				return
			}
			if !bytes.Equal(actual, bytes.Repeat(actual[:2], 32768)) {
				t.Error("partial or interleaved value")
			}
			keys, err := store.List(ctx, key("race"), 1)
			if err != nil || !reflect.DeepEqual(keys, []string{key("race")}) {
				t.Errorf("race visibility: %q, %v", keys, err)
			}
		})
	}
	close(start)
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("winners: %d", wins.Load())
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.Get(cancelled, key("a")); !errors.Is(err, context.Canceled) {
		t.Errorf("GET cancellation: %v", err)
	}
	if _, err := store.List(cancelled, prefix, 1); !errors.Is(err, context.Canceled) {
		t.Errorf("LIST cancellation: %v", err)
	}
	if err := store.Create(cancelled, key("cancelled"), nil); !errors.Is(err, context.Canceled) {
		t.Errorf("CREATE cancellation: %v", err)
	}
	// A source can implement only ReaderAt and Size, with no sequential cursor.
	source := readerAtOnly{strings.NewReader("streaming value"), int64(len("streaming value"))}
	if err := store.Create(ctx, key("reader-at"), source); err != nil {
		t.Fatal(err)
	}
	first, err := store.Get(ctx, key("reader-at"))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := store.Get(ctx, key("reader-at"))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	var prefixBytes [3]byte
	if _, err := io.ReadFull(first, prefixBytes[:]); err != nil {
		t.Fatal(err)
	}
	if b, err := io.ReadAll(second); err != nil || string(b) != "streaming value" {
		t.Fatalf("Get readers share a cursor: %q, %v", b, err)
	}
	if b, err := io.ReadAll(first); err != nil || string(b) != "eaming value" {
		t.Fatalf("first reader: %q, %v", b, err)
	}
	readCtx, cancelRead := context.WithCancel(ctx)
	r, err := store.Get(readCtx, key("reader-at"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	cancelRead()
	if _, err := r.Read(prefixBytes[:]); !errors.Is(err, context.Canceled) {
		t.Fatalf("read after cancellation: %v", err)
	}
	for _, tc := range []struct {
		name string
		body lokv.SizeReaderAt
	}{
		{"short-source", readerAtOnly{strings.NewReader("short"), 20}},
		{"failed-source", readerAtOnly{failingReaderAt{}, 20}},
	} {
		if err := store.Create(ctx, key(tc.name), tc.body); err == nil {
			t.Fatalf("accepted %s", tc.name)
		}
		if r, err := store.Get(ctx, key(tc.name)); !errors.Is(err, lokv.ErrNotFound) {
			if r != nil {
				r.Close()
			}
			t.Fatalf("published incomplete %s: %v", tc.name, err)
		}
	}
}

type readerAtOnly struct {
	io.ReaderAt
	size int64
}

func (r readerAtOnly) Size() int64 { return r.size }

type failingReaderAt struct{}

func (failingReaderAt) ReadAt([]byte, int64) (int, error) {
	return 0, errors.New("test source failure")
}

func readAll(ctx context.Context, store lokv.Store, key string) ([]byte, error) {
	r, err := store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	b, readErr := io.ReadAll(r)
	return b, errors.Join(readErr, r.Close())
}
