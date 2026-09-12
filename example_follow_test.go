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
	log, err := lokv.Open[change](lokv.Config{Store: new(memstore.Store)})
	if err != nil {
		panic(err)
	}
	appendChange := func(c change) {
		if _, err := log.Append(ctx, c); err != nil {
			panic(err)
		}
	}
	appendChange(change{Key: "alice", Value: "reader"})
	appendChange(change{Key: "bob", Value: "reader"})

	index := make(map[string]string)
	var applied int64 // Zero means no records have been applied yet.
	var indexedHead *lokv.Snapshot[change]
	catchUp := func() error {
		head, err := log.LoadHead(ctx)
		if err != nil {
			return err
		}
		if head.Revision() < applied {
			return fmt.Errorf("head moved behind applied revision %d", applied)
		}
		if head.Revision() > applied {
			err = log.Scan(ctx, head, lokv.After(applied), func(r lokv.Record[change]) error {
				if r.Value.Delete {
					delete(index, r.Value.Key)
				} else {
					index[r.Value.Key] = r.Value.Value
				}
				// Advance only after applying this record. If Scan later fails, the
				// next catchUp resumes after the records already applied successfully.
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
	appendChange(change{Key: "alice", Value: "admin"})
	appendChange(change{Key: "bob", Delete: true})
	if err := catchUp(); err != nil {
		panic(err)
	}
	// This scan applied only revisions 3 and 4; it did not replay 1 and 2.
	fmt.Println("caught up:", applied, index["alice"], len(index))

	// A poll with no new entries still uses LoadHead's one List and one Get,
	// but skips Scan. A poll with exactly one new entry needs no extra Get for
	// Scan: the head already contains that entry. A larger catch-up reads only
	// intersecting tree objects; a packed segment may also contain older entries.
	if err := catchUp(); err != nil {
		panic(err)
	}
	fmt.Println("unchanged:", indexedHead.Revision(), len(index))

	// Output:
	// initial: 2 reader reader
	// caught up: 4 admin 1
	// unchanged: 4 1
}

func ExampleLog_AppendTo() {
	ctx := context.Background()
	log, err := lokv.Open[string](lokv.Config{Store: new(memstore.Store)})
	if err != nil {
		panic(err)
	}

	// Suppose each event reserves a name. Read the current log and decide
	// whether "alice" is available against exactly this snapshot.
	base, err := log.LoadHead(ctx)
	if err != nil {
		panic(err)
	}
	taken := make(map[string]bool)
	err = log.Scan(ctx, base, lokv.All(), func(r lokv.Record[string]) error {
		taken[r.Value] = true
		return nil
	})
	if err != nil {
		panic(err)
	}
	if taken["alice"] {
		panic("name already taken")
	}

	// Another writer wins after our read but before our write.
	if _, err := log.Append(ctx, "alice"); err != nil {
		panic(err)
	}
	_, err = log.AppendTo(ctx, base, "alice")
	fmt.Println("must recheck:", errors.Is(err, lokv.ErrConflict))

	// Catch up the index and recheck the decision instead of blindly retrying
	// the reservation. For repeated catch-ups, use the Scan follow example.
	head, err := log.LoadHead(ctx)
	if err != nil {
		panic(err)
	}
	err = log.Scan(ctx, head, lokv.After(base.Revision()), func(r lokv.Record[string]) error {
		taken[r.Value] = true
		return nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("name already taken:", taken["alice"])
	fmt.Println("committed reservations:", head.Revision())

	// Output:
	// must recheck: true
	// name already taken: true
	// committed reservations: 1
}
