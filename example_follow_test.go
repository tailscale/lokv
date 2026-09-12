// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/tailscale/lokv"
	"github.com/tailscale/lokv/memstore"
)

// This example maintains an in-memory key/value index by applying log events.
// The same catchUp function handles the initial load, polling, and wakeups from
// an application-provided notification source.
func ExampleLog_Scan_follow() {
	type change struct {
		Key    string
		Value  string
		Delete bool
	}
	ctx := context.Background()
	lg, err := lokv.Open[change](lokv.Config{Store: new(memstore.Store)})
	if err != nil {
		panic(err)
	}
	appendChanges := func(value ...change) {
		if _, err := lg.Append(ctx, value...); err != nil {
			panic(err)
		}
	}
	appendChanges(change{Key: "alice", Value: "reader"}, change{Key: "bob", Value: "reader"})

	index := make(map[string]string)
	var applied int64 // Zero means no records have been applied yet.
	var indexedHead *lokv.Snapshot[change]
	catchUp := func() error {
		head, err := lg.LoadHead(ctx)
		if err != nil {
			return err
		}
		if head.Revision() < applied {
			return fmt.Errorf("head moved behind applied revision %d", applied)
		}
		if head.Revision() > applied {
			err = lg.Scan(ctx, head, lokv.After(applied), func(r lokv.Record[change]) error {
				// Only this goroutine accesses the index. If readers share it,
				// hold their mutex across the entire batch and checkpoint update.
				for _, c := range r.Value {
					if c.Delete {
						delete(index, c.Key)
					} else {
						index[c.Key] = c.Value
					}
				}
				// Advance only after applying the whole batch. If Scan later fails,
				// the next catchUp resumes after the last successfully applied batch.
				applied = r.Revision
				return nil
			})
			if err != nil {
				return err
			}
		}
		// Only use this snapshot for AppendTo once the index has fully caught up.
		indexedHead = head
		return nil
	}

	// Initial LoadHead and Scan read all records into the index. An empty log
	// would return a nil head with revision 0, so catchUp would skip Scan.
	if err := catchUp(); err != nil {
		panic(err)
	}
	fmt.Println("initial:", applied, index["alice"], index["bob"])

	// Another writer appends. In a service, call catchUp from a single goroutine
	// on a ticker tick or external wakeup. Neither Log nor Store provides Watch.
	appendChanges(change{Key: "alice", Value: "admin"}, change{Key: "bob", Delete: true})
	if err := catchUp(); err != nil {
		panic(err)
	}
	// This scan applied only the batch at revision 2; it did not replay revision 1.
	fmt.Println("caught up:", applied, index["alice"], len(index))

	// A poll with no new entries still uses LoadHead's one List and one Get,
	// but skips Scan. A poll with exactly one new batch needs no extra Get for
	// Scan: the head already contains that batch. Larger catch-ups choose between
	// packed ranges and historical roots with smaller ranges. A short catch-up
	// across a large carry need not download the whole compacted history.
	if err := catchUp(); err != nil {
		panic(err)
	}
	fmt.Println("unchanged:", indexedHead.Revision(), len(index))

	// Output:
	// initial: 1 reader reader
	// caught up: 2 admin 1
	// unchanged: 2 1
}

func ExampleLog_AppendTo() {
	ctx := context.Background()
	lg, err := lokv.Open[string](lokv.Config{Store: new(memstore.Store)})
	if err != nil {
		panic(err)
	}

	// Suppose each event reserves a name. Read the current log and decide
	// whether "alice" is available against exactly this snapshot.
	base, err := lg.LoadHead(ctx)
	if err != nil {
		panic(err)
	}
	taken := make(map[string]bool)
	err = lg.Scan(ctx, base, lokv.All(), func(r lokv.Record[string]) error {
		for _, name := range r.Value {
			taken[name] = true
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	if taken["alice"] {
		panic("name already taken")
	}

	// Another writer wins after our read but before our write.
	if _, err := lg.Append(ctx, "alice"); err != nil {
		panic(err)
	}
	_, err = lg.AppendTo(ctx, base, "alice", "bob")
	fmt.Println("must recheck:", errors.Is(err, lokv.ErrConflict))

	// Catch up the index and recheck the decision instead of blindly retrying
	// the reservation. For repeated catch-ups, use the Scan follow example.
	head, err := lg.LoadHead(ctx)
	if err != nil {
		panic(err)
	}
	err = lg.Scan(ctx, head, lokv.After(base.Revision()), func(r lokv.Record[string]) error {
		for _, name := range r.Value {
			taken[name] = true
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("name already taken:", taken["alice"])
	fmt.Println("bob reserved:", taken["bob"])
	fmt.Println("committed reservations:", head.Revision())

	// Output:
	// must recheck: true
	// name already taken: true
	// bob reserved: false
	// committed reservations: 1
}
