// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv_test

import (
	"context"
	"fmt"

	"github.com/tailscale/lokv"
	"github.com/tailscale/lokv/memstore"
)

func Example() {
	ctx := context.Background()
	lg, err := lokv.Open[string](lokv.Config{Store: new(memstore.Store), Prefix: "audit"})
	if err != nil {
		panic(err)
	}
	// The first two values commit atomically at revision 1, in one object.
	snapshot, err := lg.AppendTo(ctx, nil, "created", "updated")
	if err != nil {
		panic(err)
	}
	snapshot, err = lg.AppendTo(ctx, snapshot, "archived")
	if err != nil {
		panic(err)
	}
	err = lg.Scan(ctx, snapshot, lokv.All(), func(record lokv.Record[string]) error {
		fmt.Println(record.Revision, record.Value)
		return nil
	})
	if err != nil {
		panic(err)
	}
	// Output:
	// 1 [created updated]
	// 2 [archived]
}
