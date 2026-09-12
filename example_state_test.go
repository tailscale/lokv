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

type userRegistration struct {
	Username string
	UserID   int64
}

// userIndex maps usernames to IDs and back. Its zero value is ready for use.
type userIndex struct {
	ByName map[string]int64
	ByID   map[int64]string
	MaxID  int64
}

// apply handles one registration; State handles iteration over batches. Invalid
// stored registrations poison the State, requiring a rebuild after addressing
// the cause. Names are case-sensitive and exact; normalize before registering if
// the application needs different rules.
func (idx *userIndex) apply(reg userRegistration) error {
	// This application chooses the same safe-integer limit for user IDs.
	if reg.UserID != idx.MaxID+1 || reg.UserID > lokv.MaxRevision {
		return fmt.Errorf("invalid user ID %d; want %d", reg.UserID, idx.MaxID+1)
	}
	if reg.Username == "" {
		return errors.New("empty username")
	}
	if _, exists := idx.ByName[reg.Username]; exists {
		return fmt.Errorf("duplicate username %q", reg.Username)
	}
	if _, exists := idx.ByID[reg.UserID]; exists {
		return fmt.Errorf("duplicate user ID %d", reg.UserID)
	}
	if idx.ByName == nil {
		idx.ByName = make(map[string]int64)
	}
	if idx.ByID == nil {
		idx.ByID = make(map[int64]string)
	}
	idx.ByName[reg.Username] = reg.UserID
	idx.ByID[reg.UserID] = reg.Username
	idx.MaxID = reg.UserID
	return nil
}

var errUsernameTaken = errors.New("username already registered")

// Each client owns a separate State. A service sharing one client across
// goroutines would lock across the entire register operation and index reads.
type userClient struct {
	lg    *lokv.Log[userRegistration]
	state *lokv.State[userRegistration, userIndex]
}

func newUserClient(ctx context.Context, lg *lokv.Log[userRegistration]) (*userClient, error) {
	state, err := lokv.LoadState(ctx, lg, userIndex{}, (*userIndex).apply)
	if err != nil {
		return nil, err
	}
	return &userClient{lg: lg, state: state}, nil
}

// register allocates the next ID only if the username is still free. All writers
// must follow this protocol; lokv does not enforce application-level constraints.
// A transport or cancellation error may leave a committed registration, just as
// with AppendTo. Durable retries would include an application request ID in the
// event and index it to distinguish a retry from someone else's registration.
func (c *userClient) register(ctx context.Context, username string) (int64, error) {
	if username == "" {
		return 0, errors.New("empty username")
	}
	for {
		base, err := c.state.Sync(ctx)
		if err != nil {
			return 0, err
		}
		idx := c.state.Value()
		if _, exists := idx.ByName[username]; exists {
			return 0, errUsernameTaken
		}
		if idx.MaxID >= lokv.MaxRevision {
			return 0, errors.New("user ID space exhausted")
		}
		id := idx.MaxID + 1
		head, err := c.lg.AppendTo(ctx, base, userRegistration{Username: username, UserID: id})
		if errors.Is(err, lokv.ErrConflict) {
			// Another writer may have taken either this name or this ID.
			// Catch up, recheck uniqueness, and choose a new ID before retrying.
			continue
		}
		if err != nil {
			return 0, err
		}
		// The new snapshot contains our complete batch, so applying it to the
		// local index needs no store reads. Never update indexes before commit.
		if err := c.state.SyncTo(ctx, head); err != nil {
			return 0, fmt.Errorf("registration committed but local catch-up failed: %w", err)
		}
		return id, nil
	}
}

func ExampleState() {
	ctx := context.Background()
	lg, err := lokv.Open[userRegistration](lokv.Config{Store: new(memstore.Store)})
	if err != nil {
		panic(err)
	}
	first, err := newUserClient(ctx, lg)
	if err != nil {
		panic(err)
	}
	second, err := newUserClient(ctx, lg)
	if err != nil {
		panic(err)
	}
	for _, req := range []struct {
		client *userClient
		name   string
	}{{first, "alice"}, {second, "bob"}, {second, "alice"}} {
		id, err := req.client.register(ctx, req.name)
		fmt.Println(req.name, id, err)
	}

	// On a ticker tick or an external wakeup, the first client catches up only
	// the missing records. This also returns the snapshot for conditional writes.
	head, err := first.state.Sync(ctx)
	if err != nil {
		panic(err)
	}
	idx := first.state.Value()
	fmt.Println("revision:", first.state.Revision(), "head:", head.Revision())
	fmt.Println("bob's ID:", idx.ByName["bob"], "user 1:", idx.ByID[1])

	// A new client builds both indexes from the whole log in one LoadState call.
	third, err := newUserClient(ctx, lg)
	if err != nil {
		panic(err)
	}
	fmt.Println("reloaded:", third.state.Revision(), "users:", len(third.state.Value().ByID))

	// Output:
	// alice 1 <nil>
	// bob 2 <nil>
	// alice 0 username already registered
	// revision: 2 head: 2
	// bob's ID: 2 user 1: alice
	// reloaded: 2 users: 2
}
