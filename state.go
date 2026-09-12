// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"context"
	"errors"
	"fmt"
)

// State maintains an in-memory projection (materialized view) S by applying
// events from a Log[T]. This derives application state through event sourcing.
// It tracks the last successfully applied revision, starting at zero for an
// empty log. Construct a State with [LoadState]; its zero value is not usable.
// An apply error permanently poisons the State; see [State.Err].
//
// State is mutable and must not be copied or used concurrently. Callers sharing
// a State must synchronize access to both its methods and the application value,
// including references returned by [State.Value]. Catch-up updates the existing
// value; it does not produce independent historical copies.
type State[T, S any] struct {
	lg       *Log[T]
	value    S
	apply    func(*S, T) error
	revision int64 // last fully applied batch's revision; zero before any batch is applied
	err      error // sticky apply error; nil while the state is usable
}

// LoadState loads the current head and applies the whole log to initial, which
// must represent the application's empty state. It takes ownership of initial,
// including any maps, slices, or pointers within it; it does not clone them.
// Both lg and apply must be non-nil.
//
// apply is the event handler, sometimes called a reducer. It receives a pointer
// to the application value and one T at a time, in revision order and then
// append argument order within each batch. The applied revision advances only
// after every item in a batch succeeds. Cancellation is checked between batches,
// so it does not interrupt a partially applied batch.
//
// Any error returned by apply, including a context error, permanently poisons
// the State. Updates already made by apply are not rolled back, including those
// from the failing call. [State.Err], [State.Sync], and [State.SyncTo] return the
// same sticky error, wrapping the apply error with its revision and one-based
// item number. Discard the poisoned State and rebuild with a fresh initial value
// after addressing the cause; its application value is no longer valid.
//
// On a load, scan, or apply error, LoadState returns the partially loaded State
// and the error. If [State.Err] is nil, [State.Sync] can resume after the last
// successfully applied batch. Invalid arguments return a nil State. New records
// appended after head discovery are left for a later sync. See the username
// registration example for State.
func LoadState[T, S any](ctx context.Context, lg *Log[T], initial S, apply func(*S, T) error) (*State[T, S], error) {
	if lg == nil {
		return nil, errors.New("lokv: nil state log")
	}
	if apply == nil {
		return nil, errors.New("lokv: nil state apply callback")
	}
	s := &State[T, S]{lg: lg, value: initial, apply: apply}
	_, err := s.Sync(ctx)
	return s, err
}

// Value returns the application value after [State.Revision]. It is a shallow
// copy: maps, slices, and pointers still refer to the State's data. Callers must
// not mutate that data outside apply or access it concurrently with catch-up.
// If [State.Err] is non-nil, the value may contain a partially applied batch and
// mutations from the failing apply call; it is invalid and must not be used.
func (s *State[T, S]) Value() S { return s.value }

// Revision returns the projection's position: the last successfully applied
// batch's revision, or zero if no batch has been applied. A failing apply call
// does not advance this revision, but leaves Value invalid; see [State.Err].
func (s *State[T, S]) Revision() int64 { return s.revision }

// Err returns the sticky apply error, or nil if the State has not been poisoned.
// Once non-nil, it never changes: Sync and SyncTo return it without store I/O or
// further apply calls. Rebuild with [LoadState] and a fresh initial value.
// Store, scan, and context errors originating outside apply do not poison State.
func (s *State[T, S]) Err() error { return s.err }

// Sync loads the current head and applies only batches after [State.Revision].
// It updates only the local state and does not write to the log. Records appended
// after head discovery are left for a later Sync.
//
// On success it returns the snapshot matching the updated value and revision;
// the snapshot is nil for an empty log. Use it as the base for [Log.AppendTo] when
// deciding what to append depends on the state. On [ErrConflict], catch up and
// recompute the decision. The State example demonstrates username registration.
//
// Polling costs one List and, for a nonempty log, one head Get. An unchanged
// head or exactly one new batch requires no further I/O. Larger catch-ups have
// [Log.Scan]'s range costs; use [State.SyncTo] if a snapshot is already known.
// Callers arrange their own polling or wakeups; Sync does not wait for writes.
//
// On error it returns a nil snapshot. If [State.Err] is nil, a later Sync resumes
// after the last successfully applied batch. Otherwise it returns the sticky
// apply error without I/O or further apply calls, even if ctx is canceled.
// Sync never rewinds the state.
func (s *State[T, S]) Sync(ctx context.Context) (*Snapshot[T], error) {
	if s.err != nil {
		return nil, s.err
	}
	head, err := s.lg.LoadHead(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.SyncTo(ctx, head); err != nil {
		return nil, err
	}
	return head, nil
}

// SyncTo applies batches after [State.Revision] through snap's revision.
// snap must belong to the State's Log; nil represents an empty log. A snapshot
// behind the applied revision returns [ErrRange] without I/O. On success the
// value and revision match snap, which can then be used with [Log.AppendTo].
//
// SyncTo performs no head lookup. Applying a [Log.AppendTo] result one
// revision ahead of the state needs no store I/O. Other scans read only objects
// intersecting the unapplied range, as described by [Log.Scan]. On error, a retry
// resumes after the last successful batch unless apply poisoned the State.
// If [State.Err] is non-nil, SyncTo returns it without I/O or further apply calls,
// even if ctx is canceled or snap is invalid.
func (s *State[T, S]) SyncTo(ctx context.Context, snap *Snapshot[T]) error {
	if s.err != nil {
		return s.err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.lg.checkSnapshot(snap); err != nil {
		return err
	}
	if snap.Revision() < s.revision {
		return fmt.Errorf("lokv: snapshot revision %d precedes applied revision %d: %w", snap.Revision(), s.revision, ErrRange)
	}
	err := s.lg.Scan(ctx, snap, After(s.revision), func(r Record[T]) error {
		// Finish the batch before observing cancellation. Only an apply error
		// can leave value partly updated, in which case the State is poisoned.
		for i, value := range r.Value {
			if err := s.apply(&s.value, value); err != nil {
				s.err = fmt.Errorf("lokv: apply revision %d item %d: %w", r.Revision, i+1, err)
				return s.err
			}
		}
		s.revision = r.Revision
		return nil
	})
	// Scan may wrap callback errors with object details. Always return the same
	// sticky error for an apply failure, including on the first failed sync.
	if s.err != nil {
		return s.err
	}
	return err
}
