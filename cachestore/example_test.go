// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package cachestore_test

import (
	"context"
	"fmt"
	"os"

	"github.com/tailscale/lokv"
	"github.com/tailscale/lokv/cachestore"
	"github.com/tailscale/lokv/diskstore"
	"github.com/tailscale/lokv/memstore"
)

func Example() {
	ctx := context.Background()
	// This example uses an in-memory origin; an s3store.Store works the same way.
	origin := new(memstore.Store)
	dir, err := os.MkdirTemp("", "lokv-cache-example-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	// Use a persistent directory dedicated to this origin in an application.
	cache, err := diskstore.New(dir)
	if err != nil {
		panic(err)
	}
	store, err := cachestore.New(cachestore.Config{Origin: origin, Cache: cache})
	if err != nil {
		panic(err)
	}
	lg, err := lokv.Open[string](lokv.Config{Store: store})
	if err != nil {
		panic(err)
	}
	if _, err := lg.Append(ctx, "alice", "bob"); err != nil {
		panic(err)
	}
	state, err := lokv.LoadState(ctx, lg, 0, func(count *int, _ string) error {
		*count++
		return nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(state.Revision(), state.Value())
	// List still discovers the current origin head. Its unchanged bytes are
	// already cached, so this idle Sync needs no origin Get.
	if _, err := state.Sync(ctx); err != nil {
		panic(err)
	}
	fmt.Println(state.Revision(), state.Value())
	// Output:
	// 1 2
	// 1 2
}
