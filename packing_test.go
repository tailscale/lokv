// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
)

func TestPackedLevels(t *testing.T) {
	level := uint8(4)
	if raceEnabled {
		level = 2 // Keep the 65,536-record packing workload out of race runs.
	}
	s := newStore()
	lg := testLog[int](t, s)
	ctx := context.Background()
	childSize := int64(1) << (4 * (level - 1))
	count := childSize * 16
	// Seed complete child segments directly so this test exercises a level-4
	// carry without paying for 65,536 individual append operations.
	var children []objectRef
	chain := zeroHash
	for child := int64(0); child < 16; child++ {
		start := child*childSize + 1
		end := start + childSize - 1
		ref := objectRef{Level: level - 1, Start: hexRevision(start), End: hexRevision(end), FirstPrevHash: chain}
		seg := segment{Format: segmentFormat, Level: ref.Level, Start: ref.Start, End: ref.End}
		for revision := start; revision <= end; revision++ {
			id := fmt.Sprintf("%032x", revision)
			event := json.RawMessage(fmt.Sprintf("[%d,%d]", revision, -revision))
			hash := recordHash(revision, id, chain, event)
			seg.Records = append(seg.Records, projection{hexRevision(revision), id, chain, hash, event})
			chain = hash
		}
		raw, err := json.Marshal(seg)
		if err != nil {
			t.Fatal(err)
		}
		ref.SHA256, ref.LastRecordHash = digest(raw), chain
		ref.Key = lg.treeKey(ref)
		s.objects[ref.Key] = compressTest(t, raw)
		children = append(children, ref)
	}
	ref, err := lg.createSegment(ctx, children, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Level != level || s.gets != 16 || s.creates != 1 {
		t.Fatalf("carry: level %d, %d GETs, %d creates", ref.Level, s.gets, s.creates)
	}
	// The packed range is self-contained. Reading it must not depend on any of
	// its constituent objects, even when those objects are unavailable.
	for _, child := range children {
		delete(s.objects, child.Key)
	}
	id := fmt.Sprintf("%032x", count+1)
	event := json.RawMessage(fmt.Sprintf("[%d,%d]", count+1, -(count + 1)))
	c := commit{
		Format: commitFormat, Revision: hexRevision(count + 1), CommitID: id,
		Previous: &previous{lg.logKey(count), chain}, Event: event,
		RecordHash: recordHash(count+1, id, chain, event),
		Frontier:   []frontierLevel{{level, []objectRef{ref}}},
	}
	body, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	s.objects[lg.logKey(count+1)] = body
	s.gets, s.lists, s.getKeys = 0, 0, nil
	snap, err := lg.LoadHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seen := int64(0)
	if err := lg.Scan(ctx, snap, All(), func(r Record[int]) error {
		seen++
		if r.Revision != seen || !slices.Equal(r.Value, []int{int(seen), -int(seen)}) {
			t.Fatalf("batch %d changed: %v", r.Revision, r.Value)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seen != count+1 || s.lists != 1 || !slices.Equal(s.getKeys, []string{lg.logKey(count + 1), ref.Key}) {
		t.Fatalf("cold scan: %d records, %d LISTs, GETs %v", seen, s.lists, s.getKeys)
	}
	// Add lower-level segments and raw tail commits alongside the large range.
	for revision := count + 2; revision <= count+35; revision++ {
		snap, err = lg.AppendTo(ctx, snap, int(revision), -int(revision))
		if err != nil {
			t.Fatal(err)
		}
	}
	s.gets, s.lists, s.getKeys = 0, 0, nil
	snap, err = lg.LoadHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seen = count
	if err := lg.Scan(ctx, snap, After(count), func(r Record[int]) error {
		seen++
		if r.Revision != seen || !slices.Equal(r.Value, []int{int(seen), -int(seen)}) {
			t.Fatalf("catch-up batch %d changed: %v", r.Revision, r.Value)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// The head, two level-1 segments, and two level-0 commits cover the suffix.
	if seen != count+35 || s.lists != 1 || s.gets != 5 || slices.Contains(s.getKeys, ref.Key) {
		t.Fatalf("catch-up: %d records, %d LISTs, GETs %v", seen-count, s.lists, s.getKeys)
	}
}
