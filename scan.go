// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"context"
	"errors"
	"fmt"
)

// Range is an inclusive interval of revisions within a snapshot.
type Range struct{ First, Last uint64 }

// Scan visits exactly the requested revisions in increasing order and stops
// immediately on callback error. It validates each fetched object, including
// complete segments that partially intersect the range. Disjoint subtrees are
// skipped. Any range on an empty snapshot is invalid.
func (l *Log[T]) Scan(ctx context.Context, snap *Snapshot[T], r Range, yield func(Record[T]) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := l.checkSnapshot(snap); err != nil {
		return err
	}
	if snap == nil || r.First > r.Last || r.Last > snap.Revision() {
		return ErrRange
	}
	if yield == nil {
		return errors.New("lokv: nil scan callback")
	}
	if err := l.validateFrontier(snap.commit.Frontier, snap.Revision(), snap.commit.project().PreviousRecordHash); err != nil {
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
		record, err := l.record(p)
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
		start, end, err := l.validateRef(ref)
		if err != nil {
			return err
		}
		if end < r.First || start > r.Last {
			return nil
		}
		if ref.Level == 0 {
			c, err := l.loadCommitRef(ctx, ref, nil)
			if err != nil {
				return fmt.Errorf("scan %s: %w", ref.Key, err)
			}
			return emit(c.project())
		}
		body, err := l.get(ctx, ref.Key, true)
		if err != nil {
			return err
		}
		if ref.Level == 1 {
			seg, err := l.decodeSegment(ref, body)
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
		node, err := l.decodeIndex(ref, body)
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
func (l *Log[T]) Verify(ctx context.Context, snap *Snapshot[T]) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := l.checkSnapshot(snap); err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	return l.Scan(ctx, snap, Range{0, snap.Revision()}, func(Record[T]) error { return nil })
}
