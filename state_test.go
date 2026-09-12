// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"
)

func TestStateSync(t *testing.T) {
	ctx := context.Background()
	store := newStore()
	lg := testLog[int](t, store)
	var head *Snapshot[int]
	var want []int
	appendBatch := func() {
		t.Helper()
		batch := []int{len(want), len(want) + 1}
		var err error
		head, err = lg.AppendTo(ctx, head, batch...)
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, batch...)
	}
	for range 17 {
		appendBatch()
	}
	apply := func(value *[]int, item int) error {
		if item != len(*value) {
			t.Fatalf("applied item %d after %v", item, *value)
		}
		*value = append(*value, item)
		return nil
	}
	beforeLists, beforeGets := store.lists, store.gets
	state, err := LoadState(ctx, lg, []int(nil), apply)
	if err != nil {
		t.Fatal(err)
	}
	check := func(lists, gets int) {
		t.Helper()
		if err := state.Err(); err != nil {
			t.Fatal(err)
		}
		if state.Revision() != head.Revision() || !reflect.DeepEqual(state.Value(), want) {
			t.Fatalf("state at %d = %v; want revision %d, %v", state.Revision(), state.Value(), head.Revision(), want)
		}
		if gotLists, gotGets := store.lists-beforeLists, store.gets-beforeGets; gotLists != lists || gotGets != gets {
			t.Fatalf("used %d lists, %d gets; want %d, %d", gotLists, gotGets, lists, gets)
		}
	}
	check(1, 2) // Head plus the complete level-1 segment.

	appendBatch()
	beforeLists, beforeGets = store.lists, store.gets
	base, err := state.Sync(ctx)
	if err != nil || base.Revision() != head.Revision() {
		t.Fatalf("Sync = %v, %v", base, err)
	}
	check(1, 1) // One new batch is already in the loaded head.

	beforeLists, beforeGets = store.lists, store.gets
	if _, err := state.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	check(1, 1) // An idle poll applies nothing.

	appendBatch()
	beforeLists, beforeGets = store.lists, store.gets
	if err := state.SyncTo(ctx, head); err != nil {
		t.Fatal(err)
	}
	check(0, 0) // Reuse the AppendTo result without any I/O.
	if err := state.SyncTo(ctx, head); err != nil {
		t.Fatal(err)
	}
	check(0, 0)

	appendBatch()
	appendBatch()
	beforeLists, beforeGets = store.lists, store.gets
	if err := state.SyncTo(ctx, head); err != nil {
		t.Fatal(err)
	}
	check(0, 1) // Only the preceding new batch needs a Get.

	for head.Revision() < 33 {
		appendBatch()
	}
	beforeLists, beforeGets = store.lists, store.gets
	if err := state.SyncTo(ctx, head); err != nil {
		t.Fatal(err)
	}
	check(0, 1) // A partially overlapping segment does not reapply old batches.
}

type stateReadErrorStore struct {
	Store
	key string
	err error
}

func (s *stateReadErrorStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if key == s.key && s.err != nil {
		return nil, s.err
	}
	return s.Store.Get(ctx, key)
}

func TestStateResume(t *testing.T) {
	for _, failure := range []string{"head", "read", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			store := &stateReadErrorStore{Store: newStore()}
			lg := testLog[int](t, store)
			var head *Snapshot[int]
			for batch := range 5 {
				var err error
				head, err = lg.AppendTo(context.Background(), head, batch*2, batch*2+1)
				if err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			boom := errors.New("injected failure")
			wantErr, wantRevision := boom, int64(2)
			switch failure {
			case "head":
				store.key, store.err = lg.logKey(5), boom
				wantRevision = 0
			case "read":
				store.key, store.err = lg.logKey(3), boom
			case "cancel":
				wantErr = context.Canceled
			}
			fail := true
			state, err := LoadState(ctx, lg, []int{}, func(value *[]int, item int) error {
				*value = append(*value, item)
				if fail && failure == "cancel" && item == 2 {
					// Cancel halfway through batch 2. Item 3 must still be
					// applied before cancellation takes effect between batches.
					cancel()
				}
				return nil
			})
			if !errors.Is(err, wantErr) || state == nil {
				t.Fatalf("LoadState = %v, %v; want partial state and %v", state, err, wantErr)
			}
			if state.Err() != nil {
				t.Fatalf("non-apply error poisoned state: %v", state.Err())
			}
			want := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
			if state.Revision() != wantRevision || !reflect.DeepEqual(state.Value(), want[:wantRevision*2]) {
				t.Fatalf("partial state at %d = %v", state.Revision(), state.Value())
			}
			fail, store.err = false, nil
			head, err = state.Sync(context.Background())
			if err != nil || head.Revision() != 5 || state.Revision() != 5 || !reflect.DeepEqual(state.Value(), want) {
				t.Fatalf("resumed state at %d = %v; head %v, error %v", state.Revision(), state.Value(), head, err)
			}
		})
	}
}

func TestStateApplyErrorPoisons(t *testing.T) {
	for _, method := range []string{"load", "sync", "sync_to"} {
		for _, records := range []int{2, 3, 17} {
			t.Run(fmt.Sprintf("%s/%d", method, records), func(t *testing.T) {
				ctx := context.Background()
				store := newStore()
				lg := testLog[int](t, store)
				head, err := lg.AppendTo(ctx, nil, 1, 2)
				if err != nil {
					t.Fatal(err)
				}
				base := head
				boom := errors.New("negative value")
				var calls int
				fail := true
				apply := func(value *[]int, item int) error {
					calls++
					*value = append(*value, item) // Even the failing call may mutate.
					if fail && item < 0 {
						return boom
					}
					return nil
				}
				var state *State[int, []int]
				if method != "load" {
					state, err = LoadState(ctx, lg, []int{}, apply)
					if err != nil {
						t.Fatal(err)
					}
				}
				head, err = lg.AppendTo(ctx, head, 3, -4, 5)
				if err != nil {
					t.Fatal(err)
				}
				want := []int{1, 2, 3, -4, 5}
				for head.Revision() < int64(records) {
					item := len(want) + 1
					head, err = lg.AppendTo(ctx, head, item)
					if err != nil {
						t.Fatal(err)
					}
					want = append(want, item)
				}
				switch method {
				case "load":
					state, err = LoadState(ctx, lg, []int{}, apply)
				case "sync":
					var snap *Snapshot[int]
					snap, err = state.Sync(ctx)
					if snap != nil {
						t.Fatal("failed Sync returned a snapshot")
					}
				case "sync_to":
					err = state.SyncTo(ctx, head)
				}
				if !errors.Is(err, boom) || state == nil || state.Err() != err {
					t.Fatalf("apply failure: state %v, error %v", state, err)
				}
				if err.Error() != "lokv: apply revision 2 item 2: negative value" {
					t.Fatalf("error location: %v", err)
				}
				if state.Revision() != 1 || calls != 4 || !reflect.DeepEqual(state.value, []int{1, 2, 3, -4}) {
					t.Fatalf("failed batch: revision %d, calls %d, value %v", state.Revision(), calls, state.value)
				}

				// Repairing the callback cannot unpoison this State. Poison wins
				// over cancellation, old/foreign snapshots, and an unchanged head.
				fail = false
				foreign := build(t, testLog[int](t, newStore()), records)
				beforeLists, beforeGets := store.lists, store.gets
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				for _, retryCtx := range []context.Context{ctx, canceled} {
					if snap, retryErr := state.Sync(retryCtx); snap != nil || retryErr != err {
						t.Fatalf("poisoned Sync = %v, %v; want nil, %v", snap, retryErr, err)
					}
					for _, snap := range []*Snapshot[int]{nil, base, head, foreign} {
						if retryErr := state.SyncTo(retryCtx, snap); retryErr != err {
							t.Fatalf("poisoned SyncTo = %v; want %v", retryErr, err)
						}
					}
				}
				if calls != 4 || store.lists != beforeLists || store.gets != beforeGets {
					t.Fatal("poisoned State performed I/O or called apply")
				}
				rebuilt, err := LoadState(ctx, lg, []int{}, apply)
				if err != nil || rebuilt.Err() != nil || rebuilt.Revision() != int64(records) || !reflect.DeepEqual(rebuilt.Value(), want) {
					t.Fatalf("rebuilt state: %v, %v", rebuilt, err)
				}
			})
		}
	}
}

func TestStateApplyContextErrorPoisons(t *testing.T) {
	ctx := context.Background()
	store := newStore()
	lg := testLog[int](t, store)
	build(t, lg, 1)
	state, err := LoadState(ctx, lg, 0, func(*int, int) error { return context.Canceled })
	if !errors.Is(err, context.Canceled) || state.Err() != err || state.Revision() != 0 {
		t.Fatalf("apply context error: %v, %v", state, err)
	}
	beforeLists, beforeGets := store.lists, store.gets
	if snap, retryErr := state.Sync(ctx); snap != nil || retryErr != err || store.lists != beforeLists || store.gets != beforeGets {
		t.Fatalf("apply context error was not sticky: %v, %v", snap, retryErr)
	}
}

func TestStateEmptyAndInvalid(t *testing.T) {
	ctx := context.Background()
	store := newStore()
	lg := testLog[int](t, store)
	apply := func(sum *int, value int) error {
		*sum += value
		return nil
	}
	if state, err := LoadState(ctx, nil, 0, apply); state != nil || err == nil {
		t.Fatalf("nil log: %v, %v", state, err)
	}
	if state, err := LoadState[int, int](ctx, lg, 0, nil); state != nil || err == nil {
		t.Fatalf("nil apply: %v, %v", state, err)
	}
	if store.lists != 0 || store.gets != 0 {
		t.Fatal("invalid arguments performed I/O")
	}
	state, err := LoadState(ctx, lg, 0, apply)
	if err != nil || state.Revision() != 0 || state.Value() != 0 || store.lists != 1 || store.gets != 0 {
		t.Fatalf("empty state: %v, %v; lists %d, gets %d", state, err, store.lists, store.gets)
	}
	if head, err := state.Sync(ctx); head != nil || err != nil {
		t.Fatalf("empty catch-up: %v, %v", head, err)
	}
	head := build(t, lg, 2)
	old, err := lg.LoadRevision(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.SyncTo(ctx, head); err != nil {
		t.Fatal(err)
	}
	foreign := build(t, testLog[int](t, newStore()), 2)
	beforeLists, beforeGets := store.lists, store.gets
	for _, snap := range []*Snapshot[int]{nil, old} {
		if err := state.SyncTo(ctx, snap); !errors.Is(err, ErrRange) {
			t.Fatalf("rewind to %d: %v", snap.Revision(), err)
		}
	}
	if err := state.SyncTo(ctx, foreign); err == nil {
		t.Fatal("accepted snapshot from another log")
	}
	if err := state.SyncTo(ctx, new(Snapshot[int])); err == nil {
		t.Fatal("accepted uninitialized snapshot")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := state.SyncTo(canceled, head); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled catch-up: %v", err)
	}
	if snap, err := state.Sync(canceled); snap != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled poll: %v, %v", snap, err)
	}
	if state.Revision() != 2 || state.Value() != 1 || store.lists != beforeLists || store.gets != beforeGets {
		t.Fatal("invalid catch-up changed state or performed I/O")
	}
}
