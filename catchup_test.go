// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
)

func TestAdaptiveScan(t *testing.T) {
	store := newStore()
	lg := testLog[int](t, store)
	snap := build(t, lg, 513)
	for _, r := range []Range{
		All(), {256, 257}, {255, 257}, {257, 257}, {511, 513},
		{1, 2}, {128, 129}, {249, 265}, {3, 450}, {17, 256},
	} {
		t.Run(fmt.Sprint(r), func(t *testing.T) {
			store.getKeys = nil
			next := r.First
			err := lg.Scan(t.Context(), snap, r, func(record Record[int]) error {
				if record.Revision != next || !slices.Equal(record.Value, []int{int(next - 1)}) {
					t.Fatalf("record %d: %v", next, record)
				}
				next++
				return nil
			})
			if err != nil || next != min(r.Last, snap.Revision())+1 {
				t.Fatalf("scan ended at %d: %v", next, err)
			}
			if r == (Range{256, 257}) && (len(store.getKeys) != 2 || store.getKeys[0] != lg.logKey(256) || !strings.Contains(store.getKeys[1], "/tree/2/")) {
				// One raw boundary commit plus a small packed range is cheaper
				// than descending through multiple historical roots.
				t.Fatalf("middle range GETs: %v", store.getKeys)
			}
		})
	}
}

func TestAdaptiveScanFailures(t *testing.T) {
	store := newStore()
	lg := testLog[int](t, store)
	snap := build(t, lg, 257)
	key := lg.logKey(256)
	original := slices.Clone(store.objects[key])
	for _, kind := range []string{"missing", "malformed", "wrong record", "wrong boundary", "callback", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			store.objects[key] = slices.Clone(original)
			defer func() { store.objects[key] = original }()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			want := ErrCorrupt
			calls := 0
			switch kind {
			case "missing":
				delete(store.objects, key)
			case "malformed":
				store.objects[key] = []byte("{")
			case "wrong record", "wrong boundary":
				var c commit
				if err := json.Unmarshal(original, &c); err != nil {
					t.Fatal(err)
				}
				if kind == "wrong record" {
					c.Event = json.RawMessage("[999]")
					c.RecordHash = recordHash(256, c.CommitID, c.Previous.RecordHash, c.Event)
				} else {
					c.Frontier[0].Refs[0].FirstPrevHash = zeroHash
				}
				store.objects[key], _ = json.Marshal(c)
			case "callback":
				want = errors.New("apply failed")
			case "cancel":
				want = context.Canceled
				cancel()
			}
			err := lg.Scan(ctx, snap, After(255), func(Record[int]) error {
				calls++
				if kind == "callback" {
					return want
				}
				return nil
			})
			if !errors.Is(err, want) || kind != "callback" && calls != 0 || kind == "callback" && calls != 1 {
				t.Fatalf("scan error=%v, callbacks=%d", err, calls)
			}
		})
	}
}

func TestHistoricalRangeBoundary(t *testing.T) {
	store := newStore()
	lg := testLog[int](t, store)
	head := build(t, lg, 513)
	var c commit
	if err := json.Unmarshal(head.body, &c); err != nil {
		t.Fatal(err)
	}
	// Keep the root's frontier internally adjacent, but make its second
	// segment disagree with the historical root at revision 512. Checking
	// only the historical commit's last hash would miss this mismatch.
	c.Frontier[0].Refs[0].LastRecordHash = zeroHash
	c.Frontier[0].Refs[1].FirstPrevHash = zeroHash
	body, _ := json.Marshal(c)
	snap, err := lg.snapshot(lg.logKey(513), body)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	err = lg.Scan(t.Context(), snap, After(511), func(Record[int]) error { calls++; return nil })
	if !errors.Is(err, ErrCorrupt) || calls != 0 {
		t.Fatalf("historical boundary mismatch yielded %d records: %v", calls, err)
	}
}

func TestHistoricalChainBeforeYield(t *testing.T) {
	store := newStore()
	lg := testLog[string](t, store)
	var head *Snapshot[string]
	for range 257 {
		var err error
		head, err = lg.AppendTo(t.Context(), head, strings.Repeat("x", 1024))
		if err != nil {
			t.Fatal(err)
		}
	}
	var changed, historical commit
	if err := json.Unmarshal(store.objects[lg.logKey(254)], &changed); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(store.objects[lg.logKey(256)], &historical); err != nil {
		t.Fatal(err)
	}
	changed.Event = json.RawMessage(`["forged"]`)
	changed.RecordHash = recordHash(254, changed.CommitID, changed.Previous.RecordHash, changed.Event)
	body, _ := json.Marshal(changed)
	store.objects[lg.logKey(254)] = body
	for i := range historical.Frontier[0].Refs {
		ref := &historical.Frontier[0].Refs[i]
		if ref.Start == hexRevision(254) {
			ref.SHA256, ref.LastRecordHash = digest(body), changed.RecordHash
		}
		if ref.Start == hexRevision(255) {
			ref.FirstPrevHash = changed.RecordHash
		}
	}
	body, _ = json.Marshal(historical)
	store.objects[lg.logKey(256)] = body
	if _, err := lg.LoadRevision(t.Context(), 256); err != nil {
		t.Fatalf("test requires a structurally valid historical frontier: %v", err)
	}
	calls := 0
	err := lg.Scan(t.Context(), head, After(253), func(Record[string]) error { calls++; return nil })
	if !errors.Is(err, ErrCorrupt) || calls != 0 {
		t.Fatalf("forged historical suffix yielded %d records: %v", calls, err)
	}
}

type catchupStore struct {
	Store
	mu     sync.Mutex
	gets   int
	bytes  int64
	getErr error
}

func (s *catchupStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	s.gets++
	getErr := s.getErr
	s.mu.Unlock()
	if getErr != nil {
		return nil, getErr
	}
	body, err := s.Store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return &catchupBody{ReadCloser: body, store: s}, nil
}

type catchupBody struct {
	io.ReadCloser
	store *catchupStore
}

func (b *catchupBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.store.mu.Lock()
	b.store.bytes += int64(n)
	b.store.mu.Unlock()
	return n, err
}

func TestAdaptiveStateResume(t *testing.T) {
	store := &catchupStore{Store: newStore()}
	lg := testLog[int](t, store)
	build(t, lg, 255)
	state, err := LoadState(t.Context(), lg, 0, func(sum *int, value int) error { *sum += value; return nil })
	if err != nil {
		t.Fatal(err)
	}
	target, err := lg.Append(t.Context(), 255)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lg.Append(t.Context(), 256); err != nil {
		t.Fatal(err)
	}
	head, err := lg.LoadHead(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// Newer writes must not change a SyncTo target that was already loaded.
	if _, err := lg.Append(t.Context(), 257); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("temporary read failure")
	store.getErr = failure
	if err := state.SyncTo(t.Context(), head); !errors.Is(err, failure) || state.Revision() != target.Revision-1 || state.Err() != nil {
		t.Fatalf("failed sync: revision=%d, err=%v, sticky=%v", state.Revision(), err, state.Err())
	}
	store.getErr, store.gets, store.bytes = nil, 0, 0
	if err := state.SyncTo(t.Context(), head); err != nil {
		t.Fatal(err)
	}
	if state.Revision() != 257 || state.Value() != 256*257/2 || store.gets != 1 {
		t.Fatalf("retry: revision=%d value=%d gets=%d", state.Revision(), state.Value(), store.gets)
	}
}

// rangeFixture writes only the historical roots and complete segments needed
// by a scan. Seeding a level-4 boundary this way avoids 65,536 appends and their
// unrelated carries while retaining real hashes, frontiers, and record bytes.
type rangeFixture struct {
	tb        testing.TB
	lg        *Log[int]
	records   []projection
	refs      map[[2]int64]objectRef
	snapshots map[int64]*Snapshot[int]
}

func newRangeFixture(tb testing.TB, store Store, level uint8) *rangeFixture {
	tb.Helper()
	f := &rangeFixture{tb: tb, lg: testLog[int](tb, store), refs: make(map[[2]int64]objectRef), snapshots: make(map[int64]*Snapshot[int])}
	count := int64(1) << (4 * level)
	chain := zeroHash
	for revision := int64(1); revision <= count+1; revision++ {
		id := fmt.Sprintf("%032x", revision)
		event := json.RawMessage(fmt.Sprintf("[%d,%d]", revision, -revision))
		hash := recordHash(revision, id, chain, event)
		f.records = append(f.records, projection{hexRevision(revision), id, chain, hash, event})
		chain = hash
	}
	return f
}

func (f *rangeFixture) ref(start int64, level uint8) objectRef {
	f.tb.Helper()
	id := [2]int64{start, int64(level)}
	if ref, ok := f.refs[id]; ok {
		return ref
	}
	var ref objectRef
	if level == 0 {
		ref = f.lg.commitRef(f.snapshot(start))
	} else {
		end := start + (int64(1) << (4 * level)) - 1
		ref = objectRef{Level: level, Start: hexRevision(start), End: hexRevision(end), FirstPrevHash: f.records[start-1].PreviousRecordHash, LastRecordHash: f.records[end-1].RecordHash}
		raw, err := json.Marshal(segment{segmentFormat, level, ref.Start, ref.End, f.records[start-1 : end]})
		if err != nil {
			f.tb.Fatal(err)
		}
		ref.SHA256 = digest(raw)
		ref.Key = f.lg.treeKey(ref)
		if err := f.lg.store.Create(context.Background(), ref.Key, bytes.NewReader(compressTest(f.tb, raw))); err != nil {
			f.tb.Fatal(err)
		}
	}
	f.refs[id] = ref
	return ref
}

func (f *rangeFixture) snapshot(revision int64) *Snapshot[int] {
	f.tb.Helper()
	if snap := f.snapshots[revision]; snap != nil {
		return snap
	}
	p := f.records[revision-1]
	c := commit{Format: commitFormat, Revision: p.Revision, CommitID: p.CommitID, Event: p.Event, RecordHash: p.RecordHash, Frontier: []frontierLevel{}}
	if revision > 1 {
		c.Previous = &previous{f.lg.logKey(revision - 1), p.PreviousRecordHash}
	}
	next := int64(1)
	for level := 13; level >= 0; level-- {
		n := int((revision - 1) >> (4 * level) & 15)
		if n == 0 {
			continue
		}
		entry := frontierLevel{Level: uint8(level)}
		for range n {
			entry.Refs = append(entry.Refs, f.ref(next, uint8(level)))
			next += int64(1) << (4 * level)
		}
		c.Frontier = append([]frontierLevel{entry}, c.Frontier...)
	}
	body, err := json.Marshal(c)
	if err != nil {
		f.tb.Fatal(err)
	}
	key := f.lg.logKey(revision)
	if err := f.lg.store.Create(context.Background(), key, bytes.NewReader(body)); err != nil {
		f.tb.Fatal(err)
	}
	snap, err := f.lg.snapshot(key, body)
	if err != nil {
		f.tb.Fatal(err)
	}
	f.snapshots[revision] = snap
	return snap
}

func (f *rangeFixture) prepare(ref objectRef, r Range) {
	start, end, err := f.lg.validateRef(ref)
	if err != nil {
		f.tb.Fatal(err)
	}
	if _, split := rangeReadPlan(start, ref.Level, r, int64(len(f.records[len(f.records)-1].Event))); !split {
		return
	}
	snap := f.snapshot(end)
	for _, entry := range snap.commit.Frontier {
		for _, child := range entry.Refs {
			first, last, _ := f.lg.validateRef(child)
			if first >= start && last >= r.First && first <= r.Last {
				f.prepare(child, r)
			}
		}
	}
}

func TestHighLevelCatchUp(t *testing.T) {
	level := uint8(4)
	if raceEnabled {
		level = 2
	}
	store := &catchupStore{Store: newStore()}
	f := newRangeFixture(t, store, level)
	count := int64(1) << (4 * level)
	head := f.snapshot(count + 1)
	root := head.commit.Frontier[0].Refs[0]
	for _, behind := range []int64{1, 2, 3, 17, 257, count + 1} {
		if behind > count+1 {
			continue
		}
		r := After(count + 1 - behind)
		f.prepare(root, r)
		store.gets, store.bytes = 0, 0
		next := r.First
		if err := f.lg.Scan(t.Context(), head, r, func(record Record[int]) error {
			if record.Revision != next || !slices.Equal(record.Value, []int{int(next), -int(next)}) {
				t.Fatalf("record at %d: %v", next, record)
			}
			next++
			return nil
		}); err != nil || next != count+2 {
			t.Fatalf("behind=%d next=%d: %v", behind, next, err)
		}
		if (behind <= 2 || level == 4 && behind == 3) && store.gets != int(behind-1) || behind == count+1 && store.gets != 1 {
			t.Fatalf("behind=%d: %d GETs", behind, store.gets)
		}
		t.Logf("level=%d behind=%d GETs=%d bytes=%d", level, behind, store.gets, store.bytes)
	}
}
