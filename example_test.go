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
	log, err := lokv.Open[string](lokv.Config{Store: new(memstore.Store), Prefix: "audit"})
	if err != nil {
		panic(err)
	}
	var snapshot *lokv.Snapshot[string]
	for _, event := range []string{"created", "updated", "archived"} {
		snapshot, err = log.AppendTo(ctx, snapshot, event)
		if err != nil {
			panic(err)
		}
	}
	err = log.Scan(ctx, snapshot, lokv.Range{First: 0, Last: snapshot.Revision()}, func(record lokv.Record[string]) error {
		fmt.Println(record.Revision, record.Value)
		return nil
	})
	if err != nil {
		panic(err)
	}
	// Output:
	// 0 created
	// 1 updated
	// 2 archived
}
