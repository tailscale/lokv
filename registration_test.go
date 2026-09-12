// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv_test

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tailscale/lokv"
	"github.com/tailscale/lokv/memstore"
)

// Hold both clients at their first Create so they must attempt the same revision.
type registrationRaceStore struct {
	lokv.Store
	creates atomic.Int32
	arrived chan struct{}
	release chan struct{}
}

func (s *registrationRaceStore) Create(ctx context.Context, key string, value lokv.SizeReaderAt) error {
	if s.creates.Add(1) <= 2 {
		s.arrived <- struct{}{}
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Store.Create(ctx, key, value)
}

func TestRegistrationConflict(t *testing.T) {
	for _, names := range [][2]string{{"alice", "alice"}, {"alice", "bob"}} {
		t.Run(names[0]+"_"+names[1], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			store := &registrationRaceStore{
				Store:   new(memstore.Store),
				arrived: make(chan struct{}, 2),
				release: make(chan struct{}),
			}
			type result struct {
				name string
				id   int64
				err  error
			}
			results := make(chan result, 2)
			var clients []*userClient
			for _, name := range names {
				lg, err := lokv.Open[userRegistration](lokv.Config{Store: store})
				if err != nil {
					t.Fatal(err)
				}
				client, err := newUserClient(ctx, lg)
				if err != nil {
					t.Fatal(err)
				}
				clients = append(clients, client)
				go func() {
					id, err := client.register(ctx, name)
					results <- result{name, id, err}
				}()
			}
			for range 2 {
				select {
				case <-store.arrived:
				case <-ctx.Done():
					t.Fatal("clients did not reach concurrent creation")
				}
			}
			close(store.release)
			byName, byID := make(map[string]int64), make(map[int64]string)
			var taken int
			for range 2 {
				res := <-results
				if errors.Is(res.err, errUsernameTaken) {
					taken++
					continue
				}
				if res.err != nil {
					t.Fatal(res.err)
				}
				if byName[res.name] != 0 || byID[res.id] != "" {
					t.Fatalf("duplicate allocation: %+v", res)
				}
				byName[res.name], byID[res.id] = res.id, res.name
			}
			wantUsers, wantTaken := 2, 0
			if names[0] == names[1] {
				wantUsers, wantTaken = 1, 1
			}
			if len(byName) != wantUsers || taken != wantTaken {
				t.Fatalf("registered %v; taken %d", byName, taken)
			}
			for id := int64(1); id <= int64(wantUsers); id++ {
				if byID[id] == "" {
					t.Fatalf("missing ID %d after conflict", id)
				}
			}
			fresh, err := newUserClient(ctx, clients[0].lg)
			if err != nil {
				t.Fatal(err)
			}
			for _, client := range append(clients, fresh) {
				if _, err := client.state.Sync(ctx); err != nil {
					t.Fatal(err)
				}
				idx := client.state.Value()
				if client.state.Revision() != int64(wantUsers) || idx.MaxID != int64(wantUsers) || !reflect.DeepEqual(idx.ByName, byName) || !reflect.DeepEqual(idx.ByID, byID) {
					t.Fatalf("indexes disagree after conflict: %+v", idx)
				}
			}
		})
	}
}

func TestRegistrationBatchValidation(t *testing.T) {
	for _, bad := range []userRegistration{{"alice", 4}, {"charlie", 4}, {"dave", 2}, {"", 4}} {
		ctx := context.Background()
		lg, err := lokv.Open[userRegistration](lokv.Config{Store: new(memstore.Store)})
		if err != nil {
			t.Fatal(err)
		}
		head, err := lg.AppendTo(ctx, nil, userRegistration{"alice", 1}, userRegistration{"bob", 2})
		if err != nil {
			t.Fatal(err)
		}
		client, err := newUserClient(ctx, lg)
		if err != nil {
			t.Fatal(err)
		}
		idx := client.state.Value()
		if idx.MaxID != 2 || client.state.Revision() != 1 || !reflect.DeepEqual(idx.ByName, map[string]int64{"alice": 1, "bob": 2}) || !reflect.DeepEqual(idx.ByID, map[int64]string{1: "alice", 2: "bob"}) {
			t.Fatalf("valid batch: %+v", idx)
		}
		// Simulate a writer that ignores the registration protocol. Even if an
		// earlier item was applied, an invalid registration poisons the State.
		head, err = lg.AppendTo(ctx, head, userRegistration{"charlie", 3}, bad)
		if err != nil {
			t.Fatal(err)
		}
		poison := client.state.SyncTo(ctx, head)
		if poison == nil || client.state.Err() != poison || client.state.Revision() != 1 {
			t.Fatalf("invalid batch ending in %+v: %v", bad, poison)
		}
		if _, err := client.register(ctx, "eve"); err != poison {
			t.Fatalf("registration used poisoned index: %v; want %v", err, poison)
		}
	}
}
