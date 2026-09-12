// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
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
// not individual values within them. For N > 0 records, a full scan uses the sum
// of the hexadecimal digits of N-1 Get calls, in addition to loading the snapshot
// (at most 75 Gets including the head for up to one million batches, excluding
// retries).
//
// [State] manages an application's value and applied revision for catch-ups.
// The follow example shows how to track them directly: keep the last successfully
// applied revision and scan only the interval after it. Scan performs no List
// calls and does not refetch the snapshot's own record: a range containing only
// that record needs no I/O.
// Full scans read each frontier object once. For suffixes, Scan estimates
// payload, metadata, and request costs and may load a segment's historical end
// commit to find smaller packed ranges or raw commits. For example, catching up
// from revision 65,535 to 65,537 reads commit 65,536 and uses the loaded head's
// batch, avoiding the 65,536-record segment. The target snapshot bounds all reads;
// Scan never discovers a newer head.
//
// Each fetched segment is validated in full before yielding any of its records.
// Historical roots must match the original range's hash boundaries, and their
// selected records must form a verified chain to its end before being yielded.
// A range ending inside a segment uses that segment's packed object. Missing or
// corrupt objects fail the scan; Scan does not switch paths to hide a failure.
// The cost estimate cannot know historical batch sizes or compression ratios
// before reading them and does not promise the minimum possible byte count.
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
	visit := func(ref objectRef) error {
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
		if err := lg.readRange(ctx, ref, r, int64(len(snap.commit.Event)), emit); err != nil {
			return fmt.Errorf("scan %s: %w", ref.Key, err)
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

// scanReadCost estimates transfer and request overhead without additional I/O.
// Packed records allow 64 bytes for compressed identity/chain fields plus the
// current head's batch size as a payload estimate. Raw commit frontiers add
// about 384 bytes per ref. Each GET carries a 64 KiB allowance for latency.
// Actual historical batch sizes, compression, and network costs can differ.
// Full scans always use packed objects, regardless of these estimates.
type scanReadCost struct{ records, gets, refs int64 }

func (c scanReadCost) weight(batchBytes int64) float64 {
	return float64(c.records)*(64+float64(batchBytes)) + 384*float64(c.refs) + (64<<10)*float64(c.gets)
}

func commitReadCost(revision int64) scanReadCost {
	cost := scanReadCost{records: 1, gets: 1}
	for n := revision - 1; n > 0; n >>= 4 {
		cost.refs += n & 15
	}
	return cost
}

func rangeReadPlan(start int64, level uint8, r Range, batchBytes int64) (cost scanReadCost, split bool) {
	size := int64(1) << (4 * level)
	end := start + size - 1
	if r.Last < start || r.First > end {
		return scanReadCost{}, false
	}
	if level == 0 {
		return commitReadCost(start), false
	}
	packed := scanReadCost{records: size, gets: 1}
	// A suffix can be validated through the known last-record hash. A range
	// ending earlier still needs the packed object's digest for its anchor.
	if r.First <= start && r.Last >= end || r.Last < end {
		return packed, false
	}
	// The historical commit at end has fifteen refs at every smaller level,
	// followed by its own batch. Its earlier, disjoint frontier refs are free.
	historical := commitReadCost(end)
	next := start
	for childLevel := int(level) - 1; childLevel >= 0; childLevel-- {
		childSize := int64(1) << (4 * childLevel)
		for range 15 {
			child, _ := rangeReadPlan(next, uint8(childLevel), r, batchBytes)
			historical.records += child.records
			historical.gets += child.gets
			historical.refs += child.refs
			next += childSize
		}
	}
	if historical.weight(batchBytes) < packed.weight(batchBytes) {
		return historical, true
	}
	return packed, false
}

// readRange chooses a representation before I/O. It never retries corruption
// or missing data through another representation. The historical end commit
// must match the segment's last record hash, and its smaller frontier must
// reproduce the segment's range and hash boundaries before any records escape.
func (lg *Log[T]) readRange(ctx context.Context, ref objectRef, r Range, batchBytes int64, yield func(projection) error) error {
	start, end, err := lg.validateRef(ref)
	if err != nil {
		return err
	}
	_, split := rangeReadPlan(start, ref.Level, r, batchBytes)
	if !split || r.First == end {
		return lg.readRangeParts(ctx, ref, r, batchBytes, yield)
	}
	// Historical frontiers are not covered by the last record's logical hash.
	// Validate the entire selected suffix through that known hash before
	// exposing records from this alternate representation. No prefix preceding
	// the requested range has to be replayed to establish this chain.
	f, err := lg.newTemp()
	if err != nil {
		return err
	}
	return lg.readRangeSpool(ctx, f, ref, r, batchBytes, yield)
}

func (lg *Log[T]) readRangeSpool(ctx context.Context, f *tempFile, ref objectRef, r Range, batchBytes int64, yield func(projection) error) (err error) {
	defer func() { err = errors.Join(err, f.Close()) }()
	enc := jsontext.NewEncoder(contextWriter{ctx, f}, json.DefaultOptionsV1())
	next, chain := r.First, ""
	err = lg.readRangeParts(ctx, ref, r, batchBytes, func(p projection) error {
		revision, err := parseRevision(p.Revision)
		if err != nil {
			return err
		}
		if revision < r.First {
			return nil
		}
		if revision != next || chain != "" && p.PreviousRecordHash != chain {
			return corrupt("historical record chain mismatch")
		}
		next, chain = revision+1, p.RecordHash
		if err := jsonv2.MarshalEncode(enc, &p, json.DefaultOptionsV1()); err != nil {
			return &eventCodecError{"spool", err}
		}
		return nil
	})
	if err != nil {
		return err
	}
	end, _ := parseRevision(ref.End)
	if next != end+1 || chain != ref.LastRecordHash {
		return corrupt("historical record chain boundary mismatch")
	}
	return (&recordFile{f}).each(ctx, yield)
}

// readRangeParts yields internal projections. Its caller must establish the
// complete suffix's chain before publishing anything from historical frontiers.
func (lg *Log[T]) readRangeParts(ctx context.Context, ref objectRef, r Range, batchBytes int64, yield func(projection) error) error {
	start, end, err := lg.validateRef(ref)
	if err != nil {
		return err
	}
	if end < r.First || start > r.Last {
		return nil
	}
	_, split := rangeReadPlan(start, ref.Level, r, batchBytes)
	if !split {
		return lg.readRecords(ctx, ref, nil, yield)
	}
	key := lg.logKey(end)
	body, err := lg.get(ctx, key, true)
	if err != nil {
		return err
	}
	c, err := lg.decodeCommit(key, body)
	if err != nil {
		return err
	}
	if c.RecordHash != ref.LastRecordHash {
		return corrupt("historical commit does not match segment")
	}
	var children []objectRef
	next, chain := start, ref.FirstPrevHash
	for i := len(c.Frontier) - 1; i >= 0; i-- {
		for _, child := range c.Frontier[i].Refs {
			first, last, err := lg.validateRef(child)
			if err != nil {
				return err
			}
			if last < start {
				continue
			}
			if first != next || child.Level >= ref.Level || child.FirstPrevHash != chain {
				return corrupt("historical frontier does not match segment")
			}
			next, chain = last+1, child.LastRecordHash
			children = append(children, child)
		}
	}
	if next != end || chain != c.project().PreviousRecordHash {
		return corrupt("historical frontier boundary mismatch")
	}
	for _, child := range children {
		if err := lg.readRangeParts(ctx, child, r, batchBytes, yield); err != nil {
			return err
		}
	}
	return yield(c.project())
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
