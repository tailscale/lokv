// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"context"
	"errors"
	"fmt"
)

// Range is an inclusive interval of revisions. First and Last must both be in
// [1, MaxRevision], with First <= Last. As a special case, the zero Range is
// empty. [Log.Scan] visits the intersection of this range and its snapshot.
type Range struct{ First, Last int64 }

// All returns the range [1, MaxRevision]. Use it with [Log.Scan] to visit every
// record in a snapshot, including an empty snapshot.
func All() Range { return Range{1, MaxRevision} }

// StartingAt returns the range [revision, MaxRevision], including revision.
// A revision outside [1, MaxRevision] produces an invalid range, which [Log.Scan]
// rejects with ErrRange. Use [After] when the revision has already been applied.
func StartingAt(revision int64) Range { return Range{revision, MaxRevision} }

// After returns the range of revisions strictly greater than revision.
// After(0) is equivalent to [All], and After(MaxRevision) returns an empty range.
// A revision outside [0, MaxRevision] produces an invalid range, which [Log.Scan]
// rejects with ErrRange. See the follow example for Log.Scan for using After to
// catch up an in-memory index.
func After(revision int64) Range {
	if revision < 0 || revision > MaxRevision {
		return StartingAt(revision) // Preserve invalid bounds without incrementing.
	}
	if revision == MaxRevision {
		return Range{}
	}
	return StartingAt(revision + 1)
}

// Scan visits the records in both r and snap in increasing revision order,
// stopping immediately on callback error. It returns ErrRange for invalid bounds:
// First and Last must be in [1, MaxRevision], with First <= Last, except that
// the zero Range is empty. An empty range, a valid range on an empty snapshot,
// or one starting past the snapshot's revision yields nothing and performs no
// store I/O. yield must be non-nil.
//
// Use [All] to scan the whole snapshot, [StartingAt] to include a revision, or
// [After] to resume after an already-applied revision. Scan does not wait for
// future records. It validates each fetched object, including complete segments
// that partially intersect the range. Disjoint subtrees are skipped.
// Each callback receives one entire batch; ranges and revisions count batches,
// not individual values within them.
//
// The follow example shows an initial full scan into an in-memory index and
// later catch-up scans. Keep the last successfully applied revision and scan
// only the interval after it. Scan performs no List calls and does not refetch
// the snapshot's own record: a range containing only that record needs no I/O.
// Other reads are limited to intersecting index nodes, packed segments, and raw
// tail commits. A segment is read and validated in full even when its first few
// records were already applied; only requested records reach yield.
//
// Successfully yielded records are not rolled back if a later read or callback
// fails. Advance an application's last-applied revision only after the entire
// batch succeeds, and keep it consistent with the application's index on retries.
// Applications must provide any atomicity needed for their own index updates.
func (lg *Log[T]) Scan(ctx context.Context, snap *Snapshot[T], r Range, yield func(Record[T]) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := lg.checkSnapshot(snap); err != nil {
		return err
	}
	if r != (Range{}) && (r.First <= 0 || r.First > r.Last || r.Last > MaxRevision) {
		return ErrRange
	}
	if yield == nil {
		return errors.New("lokv: nil scan callback")
	}
	if r == (Range{}) || r.First > snap.Revision() {
		return nil
	}
	r.Last = min(r.Last, snap.Revision())
	if err := lg.validateFrontier(snap.commit.Frontier, snap.Revision(), snap.commit.project().PreviousRecordHash); err != nil {
		return err
	}
	next, chain, done := r.First, "", false
	emit := func(p projection) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		revision, err := parseRevision(p.Revision)
		if err != nil {
			return err
		}
		if revision < r.First || revision > r.Last {
			return nil
		}
		if done || revision != next || chain != "" && p.PreviousRecordHash != chain {
			return corrupt("scan record chain mismatch")
		}
		record, err := lg.record(p)
		if err != nil {
			return err
		}
		if err := yield(record); err != nil {
			return err
		}
		chain = p.RecordHash
		if revision == r.Last {
			done = true
		} else {
			next++
		}
		return nil
	}
	var visit func(objectRef) error
	visit = func(ref objectRef) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		start, end, err := lg.validateRef(ref)
		if err != nil {
			return err
		}
		if end < r.First || start > r.Last {
			return nil
		}
		if ref.Level == 0 {
			c, err := lg.loadCommitRef(ctx, ref, nil)
			if err != nil {
				return fmt.Errorf("scan %s: %w", ref.Key, err)
			}
			return emit(c.project())
		}
		body, err := lg.get(ctx, ref.Key, true)
		if err != nil {
			return err
		}
		if ref.Level == 1 {
			seg, err := lg.decodeSegment(ref, body)
			if err != nil {
				return fmt.Errorf("scan %s: %w", ref.Key, err)
			}
			for _, p := range seg.Records {
				if err := emit(p); err != nil {
					return err
				}
			}
			return nil
		}
		node, err := lg.decodeIndex(ref, body)
		if err != nil {
			return fmt.Errorf("scan %s: %w", ref.Key, err)
		}
		for _, child := range node.Children {
			if err := visit(child); err != nil {
				return err
			}
		}
		return nil
	}
	for i := len(snap.commit.Frontier) - 1; i >= 0; i-- {
		for _, ref := range snap.commit.Frontier[i].Refs {
			if err := visit(ref); err != nil {
				return err
			}
		}
	}
	if err := emit(snap.commit.project()); err != nil {
		return err
	}
	if !done {
		return corrupt("incomplete scan")
	}
	return ctx.Err()
}

// Verify checks every reachable tree object's structure and digest and the full
// record chain. Empty snapshots verify successfully. Superseded raw commits and
// unreachable carry objects are not part of the snapshot's tree.
func (lg *Log[T]) Verify(ctx context.Context, snap *Snapshot[T]) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := lg.checkSnapshot(snap); err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	return lg.Scan(ctx, snap, All(), func(Record[T]) error { return nil })
}
