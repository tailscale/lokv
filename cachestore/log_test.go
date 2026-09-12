// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package cachestore

import (
	"context"
	"io"
	"sync/atomic"
	"testing"

	"github.com/tailscale/lokv"
	"github.com/tailscale/lokv/diskstore"
	"github.com/tailscale/lokv/memstore"
)

func TestLogUsesPersistentCache(t *testing.T) {
	ctx := t.Context()
	origin := new(memstore.Store)
	writer, err := lokv.Open[int](lokv.Config{Store: origin})
	if err != nil {
		t.Fatal(err)
	}
	// Cross two compaction levels so scans must read packed objects too.
	for i := 1; i <= 257; i++ {
		if _, err := writer.Append(ctx, i); err != nil {
			t.Fatal(err)
		}
	}
	var gets, lists atomic.Int64
	counted := testStore{Store: origin, get: func(ctx context.Context, key string) (io.ReadCloser, error) {
		gets.Add(1)
		return origin.Get(ctx, key)
	}, list: func(ctx context.Context, prefix string, limit int) ([]string, error) {
		lists.Add(1)
		return origin.List(ctx, prefix, limit)
	}}
	dir := t.TempDir()
	var reader *lokv.Log[int]
	var state *lokv.State[int, int]
	for pass := range 2 {
		// Reopening both stores models a client restarting with a warm cache.
		cache, err := diskstore.New(dir)
		if err != nil {
			t.Fatal(err)
		}
		s := newStore(t, Config{Origin: counted, Cache: cache, OnCacheError: func(err error) { t.Errorf("cache error: %v", err) }})
		reader, err = lokv.Open[int](lokv.Config{Store: s})
		if err != nil {
			t.Fatal(err)
		}
		state, err = lokv.LoadState(ctx, reader, 0, func(sum *int, value int) error {
			*sum += value
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if state.Revision() != 257 || state.Value() != 257*258/2 {
			t.Fatalf("state = revision %d, value %d", state.Revision(), state.Value())
		}
		if pass == 0 && gets.Load() != 2 {
			t.Fatalf("cold full scan: %d GETs, want head and level 2", gets.Load())
		}
		if pass == 1 && gets.Load() != 0 {
			t.Fatalf("warm full scan: %d GETs, want 0", gets.Load())
		}
		if lists.Load() != 1 {
			t.Fatalf("full scan: %d LISTs, want 1", lists.Load())
		}
		gets.Store(0)
		lists.Store(0)
	}
	if _, err := state.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if gets.Load() != 0 || lists.Load() != 1 {
		t.Fatalf("idle Sync: %d GETs, %d LISTs", gets.Load(), lists.Load())
	}
	if _, err := writer.Append(ctx, 258); err != nil {
		t.Fatal(err)
	}
	gets.Store(0)
	lists.Store(0)
	if _, err := state.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if state.Revision() != 258 || state.Value() != 258*259/2 || gets.Load() != 1 || lists.Load() != 1 {
		t.Fatalf("external append Sync: revision %d, value %d, %d GETs, %d LISTs", state.Revision(), state.Value(), gets.Load(), lists.Load())
	}
	// Cached writes include packed compaction output. After adding enough
	// records for the next carry, Verify and a full scan need no new GETs for
	// objects that have been either written or previously read by this client.
	for i := 259; i <= 273; i++ {
		if _, err := reader.Append(ctx, i); err != nil {
			t.Fatal(err)
		}
	}
	// Warm any historical objects newly needed by a full verification.
	head, err := reader.LoadHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Verify(ctx, head); err != nil {
		t.Fatal(err)
	}
	gets.Store(0)
	if err := reader.Verify(ctx, head); err != nil {
		t.Fatal(err)
	}
	if err := reader.Scan(ctx, head, lokv.All(), func(lokv.Record[int]) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if gets.Load() != 0 {
		t.Fatalf("warm Verify and Scan: %d GETs", gets.Load())
	}
}
