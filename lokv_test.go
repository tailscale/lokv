// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// Unlike a production Store this fake has test-only mutation access for fault
// injection. Normal interface operations obey the complete Store contract.
type testStore struct {
	mu                   sync.Mutex
	objects              map[string][]byte
	creates, gets, lists int
	getKeys              []string
	before               func(string, []byte) error
	after                func(string, []byte) error
}

func newStore() *testStore { return &testStore{objects: map[string][]byte{}} }

func (s *testStore) List(ctx context.Context, prefix string, limit int) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, errors.New("bad limit")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lists++
	var keys []string
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys[:min(limit, len(keys))], nil
}

func (s *testStore) Get(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	s.getKeys = append(s.getKeys, key)
	b, ok := s.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	return bytes.Clone(b), nil
}

func (s *testStore) Create(ctx context.Context, key string, body []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.creates++
	s.mu.Unlock()
	if s.before != nil {
		if err := s.before(key, body); err != nil {
			return err
		}
	}
	s.mu.Lock()
	if _, ok := s.objects[key]; ok {
		s.mu.Unlock()
		return ErrExists
	}
	s.objects[key] = bytes.Clone(body)
	s.mu.Unlock()
	if s.after != nil {
		return s.after(key, body)
	}
	return nil
}

func testLog[T any](t testing.TB, s Store) *Log[T] {
	t.Helper()
	lg, err := Open[T](Config{Store: s})
	if err != nil {
		t.Fatal(err)
	}
	return lg
}

func build(t testing.TB, lg *Log[int], n int) *Snapshot[int] {
	t.Helper()
	var snap *Snapshot[int]
	for i := 0; i < n; i++ {
		var err error
		snap, err = lg.AppendTo(context.Background(), snap, i)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	return snap
}

func TestReverseKeys(t *testing.T) {
	lg := testLog[int](t, newStore())
	revisions := []int64{1, 2, 15, 16, 17, MaxRevision - 1, MaxRevision}
	keys := make([]string, len(revisions))
	for i, r := range revisions {
		keys[i] = lg.logKey(r)
		actual, err := lg.parseLogKey(keys[i])
		if err != nil || actual != r {
			t.Fatalf("%d: %d %v", r, actual, err)
		}
	}
	sort.Strings(keys)
	for i, key := range keys {
		r, _ := lg.parseLogKey(key)
		if r != revisions[len(revisions)-1-i] {
			t.Fatal("incorrect lexical order")
		}
	}
	for _, key := range []string{"v1/log/FFFFFFFFFFFFFFFF.json", "v1/log/fff.json", "else/v1/log/ffffffffffffffff.json", "v1/log/ffffffffffffffff.json/", "v1/log/000000000000000g.json"} {
		if _, err := lg.parseLogKey(key); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("accepted %s", key)
		}
	}
}

func TestBoundariesAndCounts(t *testing.T) {
	for _, n := range []int{0, 1, 15, 16, 17, 255, 256, 257, 4097} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			if raceEnabled && n > 257 {
				t.Skip("two carry levels suffice under the race detector")
			}
			s := newStore()
			lg := testLog[int](t, s)
			var snap *Snapshot[int]
			for i := 0; i < n; i++ {
				before := s.creates
				var err error
				snap, err = lg.AppendTo(context.Background(), snap, i)
				if err != nil {
					t.Fatal(err)
				}
				depth := 0
				for x := i; x > 0 && x%16 == 0; x /= 16 {
					depth++
				}
				if got := s.creates - before; got != 1+depth {
					t.Fatalf("revision %d creations %d; want %d", i+1, got, 1+depth)
				}
				if err := lg.validateFrontier(snap.commit.Frontier, int64(i+1), snap.commit.project().PreviousRecordHash); err != nil {
					t.Fatal(err)
				}
			}
			beforeLists, beforeGets := s.lists, s.gets
			head, ok, err := lg.Head(context.Background())
			if err != nil || ok != (n > 0) {
				t.Fatalf("head: %v %v", ok, err)
			}
			if s.lists-beforeLists != 1 || s.gets-beforeGets != min(1, n) {
				t.Fatal("HEAD request count")
			}
			if n == 0 {
				if err := lg.Verify(context.Background(), nil); err != nil {
					t.Fatal(err)
				}
				if err := lg.Scan(context.Background(), nil, All(), func(Record[int]) error { t.Fatal("empty scan yielded a record"); return nil }); err != nil {
					t.Fatal(err)
				}
				return
			}
			if head.Revision != int64(n) || len(head.Value) != 1 || head.Value[0] != n-1 {
				t.Fatal(head)
			}
			var got []int
			err = lg.Scan(context.Background(), snap, Range{1, int64(n)}, func(r Record[int]) error {
				if r.Revision != int64(len(got)+1) {
					t.Fatal("unordered scan")
				}
				got = append(got, r.Value...)
				return nil
			})
			if err != nil || len(got) != n {
				t.Fatalf("scan count %d: %v", len(got), err)
			}
			for i, v := range got {
				if i != v {
					t.Fatalf("record %d = %d", i, v)
				}
			}
			if err := lg.Verify(context.Background(), snap); err != nil {
				t.Fatal(err)
			}
			if n > 16 {
				lists := s.lists
				old, err := lg.LoadRevision(context.Background(), 16)
				if err != nil {
					t.Fatal(err)
				}
				if s.lists != lists {
					t.Fatal("historical load listed")
				}
				if err := lg.Verify(context.Background(), old); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

type number int
type customJSON struct{ N int }

func (v customJSON) MarshalJSON() ([]byte, error) { return json.Marshal(fmt.Sprintf("custom:%d", v.N)) }

func (v *customJSON) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	_, err := fmt.Sscanf(s, "custom:%d", &v.N)
	return err
}

func roundTrip[T any](t *testing.T, value T) {
	t.Helper()
	s := newStore()
	lg := testLog[T](t, s)
	for i := 0; i < 17; i++ {
		if _, err := lg.Append(context.Background(), value); err != nil {
			t.Fatal(err)
		}
	}
	snap, err := lg.LoadHead(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := lg.Scan(context.Background(), snap, Range{1, 17}, func(r Record[T]) error {
		if !reflect.DeepEqual(r.Value, []T{value}) {
			t.Fatalf("round trip: %#v != %#v", r.Value, value)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestGenericJSON(t *testing.T) {
	roundTrip(t, struct {
		Name string
		N    int
	}{"a<&>", 42})
	roundTrip(t, map[string]any{"number": float64(4), "nil": nil})
	roundTrip(t, "a\n<&>\u2028")
	roundTrip(t, number(3))
	roundTrip(t, byte(255))
	roundTrip(t, []byte{1, 2, 255})
	roundTrip(t, []string{"a", "b"})
	v := 7
	roundTrip(t, &v)
	roundTrip(t, (*int)(nil))
	roundTrip(t, customJSON{7})
	roundTrip[any](t, nil)
}

func TestMarshalFailureNoIO(t *testing.T) {
	s := newStore()
	lg := testLog[any](t, s)
	if _, err := lg.Append(context.Background(), 1, func() {}); err == nil {
		t.Fatal("marshal succeeded")
	}
	if s.creates+s.gets+s.lists != 0 {
		t.Fatal("I/O before marshal")
	}
}

func TestConcurrentAppend(t *testing.T) {
	for _, n := range []int{2, 32} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			s := newStore()
			lg, err := Open[int](Config{Store: s, MaxConflictRetries: 128})
			if err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := 0; i < n; i++ {
				wg.Go(func() {
					<-start
					if _, err := lg.Append(context.Background(), i, i+n); err != nil {
						t.Errorf("append: %v", err)
					}
				})
			}
			close(start)
			wg.Wait()
			snap, err := lg.LoadHead(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if snap.Revision() != int64(n) {
				t.Fatalf("head %d", snap.Revision())
			}
			seen := map[int]bool{}
			err = lg.Scan(context.Background(), snap, Range{1, int64(n)}, func(r Record[int]) error {
				if len(r.Value) != 2 || r.Value[1] != r.Value[0]+n {
					t.Fatal("batch was split or mixed with another writer")
				}
				if seen[r.Value[0]] {
					t.Error("duplicate input")
				}
				seen[r.Value[0]] = true
				return nil
			})
			if err != nil || len(seen) != n {
				t.Fatalf("scan %d: %v", len(seen), err)
			}
		})
	}
}

func TestAppendToConflict(t *testing.T) {
	s := newStore()
	lg := testLog[int](t, s)
	base := build(t, lg, 16)
	winner, err := lg.AppendTo(context.Background(), base, 16)
	if err != nil {
		t.Fatal(err)
	}
	lists := s.lists
	if _, err := lg.AppendTo(context.Background(), base, 999); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if s.lists != lists {
		t.Fatal("AppendTo listed")
	}
	if _, err := lg.AppendTo(context.Background(), nil, 999); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if err := lg.Verify(context.Background(), winner); err != nil {
		t.Fatal(err)
	}
	if base.Revision() != 16 || len(base.commit.Frontier) != 1 || len(base.commit.Frontier[0].Refs) != 15 {
		t.Fatal("base was mutated")
	}
}

func TestAmbiguousSuccess(t *testing.T) {
	for _, reported := range []error{errors.New("lost response"), ErrExists} {
		t.Run(reported.Error(), func(t *testing.T) {
			s := newStore()
			lg := testLog[int](t, s)
			build(t, lg, 16)
			s.after = func(string, []byte) error { return reported }
			r, err := lg.Append(context.Background(), 42, 43)
			if err != nil || r.Revision != 17 || !reflect.DeepEqual(r.Value, []int{42, 43}) {
				t.Fatalf("append: %v %v", r, err)
			}
			snap, err := lg.LoadHead(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if snap.Revision() != 17 {
				t.Fatal("duplicate commit")
			}
			if err := lg.Verify(context.Background(), snap); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCrashAfterAggregate(t *testing.T) {
	levels := 3
	if raceEnabled {
		levels = 2
	}
	n := 1 << (4 * levels)
	s := newStore()
	lg := testLog[int](t, s)
	base := build(t, lg, n)
	original := make(map[string][]byte, len(s.objects))
	for k, v := range s.objects {
		original[k] = v
	}
	failure := errors.New("injected crash")
	// The next revision carries through a segment and higher index levels.
	// Fail the creation after each durable aggregate, including the final commit.
	for after := 1; after <= levels; after++ {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			s.objects = make(map[string][]byte, len(original))
			for k, v := range original {
				s.objects[k] = v
			}
			calls := 0
			s.before = func(key string, b []byte) error {
				calls++
				if calls == after+1 {
					return failure
				}
				return nil
			}
			if _, err := lg.AppendTo(context.Background(), base, n); !errors.Is(err, failure) {
				t.Fatalf("fault: %v", err)
			}
			head, err := lg.LoadHead(context.Background())
			if err != nil || head.Revision() != int64(n) {
				t.Fatalf("published incomplete commit: %v", err)
			}
			if err := lg.Verify(context.Background(), head); err != nil {
				t.Fatal(err)
			}
			s.before = nil
			next, err := lg.AppendTo(context.Background(), base, n)
			if err != nil {
				t.Fatal(err)
			}
			if err := lg.Verify(context.Background(), next); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRangesAndPruning(t *testing.T) {
	s := newStore()
	lg := testLog[int](t, s)
	snap := build(t, lg, 513)
	// All and open-ended ranges must stop at snap even when newer records exist.
	if _, err := lg.AppendTo(context.Background(), snap, 513); err != nil {
		t.Fatal(err)
	}
	for _, r := range []Range{
		{1, 1}, {16, 17}, {17, 32}, {256, 257}, {257, 272}, {512, 513}, {513, 513}, {1, 513},
		{1, 514}, {512, 1000}, {514, 1000}, All(), StartingAt(1), StartingAt(16),
		StartingAt(513), StartingAt(514), StartingAt(MaxRevision), After(0), After(16),
		After(512), After(513), After(MaxRevision), {},
	} {
		beforeLists, beforeCreates := s.lists, s.creates
		s.getKeys = nil
		var got []int64
		if err := lg.Scan(context.Background(), snap, r, func(v Record[int]) error { got = append(got, v.Revision); return nil }); err != nil {
			t.Fatal(err)
		}
		last := min(r.Last, snap.Revision())
		count := max(int64(0), last-r.First+1)
		if r == (Range{}) {
			count = 0
		}
		if int64(len(got)) != count {
			t.Fatalf("range %+v: %v", r, got)
		}
		for i, revision := range got {
			if revision != r.First+int64(i) {
				t.Fatalf("range %+v: %v", r, got)
			}
		}
		if s.lists != beforeLists || s.creates != beforeCreates {
			t.Fatal("scan listed or created objects")
		}
		for _, key := range s.getKeys {
			if !strings.Contains(key, "/tree/") {
				continue
			}
			suffix := key[strings.LastIndex(key, "/")+1:]
			start, _ := parseRevision(suffix[:16])
			end, _ := parseRevision(suffix[17:33])
			if end < r.First || start > r.Last {
				t.Fatalf("fetched disjoint subtree %s for %+v", key, r)
			}
		}
		if (count == 0 || r.First == 513) && len(s.getKeys) != 0 {
			t.Fatal("empty or head-only scan fetched objects")
		}
		if r.First == 257 && r.Last == 272 && len(s.getKeys) != 2 {
			t.Fatalf("GETs: %v", s.getKeys)
		}
	}
	for _, r := range []Range{{1, 0}, {0, 513}, {-1, 1}, {1, -1}, {2, 1}, {math.MinInt64, math.MaxInt64}, {math.MaxInt64, math.MaxInt64}} {
		if err := lg.Scan(context.Background(), snap, r, func(Record[int]) error { return nil }); !errors.Is(err, ErrRange) {
			t.Fatal(err)
		}
	}
	stop := errors.New("stop")
	s.getKeys = nil
	calls := 0
	if err := lg.Scan(context.Background(), snap, Range{1, 513}, func(Record[int]) error { calls++; return stop }); !errors.Is(err, stop) || calls != 1 || len(s.getKeys) != 2 {
		t.Fatalf("early stop: %d %d %v", calls, len(s.getKeys), err)
	}
}

func TestRangeHelpersEmptyAndInvalid(t *testing.T) {
	s := newStore()
	lg := testLog[int](t, s)
	ctx := context.Background()
	first := build(t, lg, 1)
	for _, snap := range []*Snapshot[int]{nil, first} {
		for _, r := range []Range{{}, After(MaxRevision), After(1), StartingAt(2)} {
			before := s.gets + s.lists + s.creates
			if err := lg.Scan(ctx, snap, r, func(Record[int]) error {
				t.Fatalf("empty scan %+v yielded a record", r)
				return nil
			}); err != nil {
				t.Fatalf("empty scan %+v: %v", r, err)
			}
			if s.gets+s.lists+s.creates != before {
				t.Fatal("empty scan performed store I/O")
			}
		}
		for _, r := range []Range{
			StartingAt(math.MinInt64), StartingAt(-1), StartingAt(0),
			StartingAt(MaxRevision + 1), StartingAt(math.MaxInt64),
			After(math.MinInt64), After(-1), After(MaxRevision + 1), After(math.MaxInt64),
		} {
			before := s.gets + s.lists + s.creates
			if err := lg.Scan(ctx, snap, r, func(Record[int]) error {
				t.Fatal("invalid range yielded a record")
				return nil
			}); !errors.Is(err, ErrRange) {
				t.Fatalf("invalid scan %+v: %v", r, err)
			}
			if s.gets+s.lists+s.creates != before {
				t.Fatal("invalid scan performed store I/O")
			}
		}
	}
}

func TestConfiguration(t *testing.T) {
	var typedNil *testStore
	for _, cfg := range []Config{{}, {Store: typedNil}, {Store: newStore(), MaxConflictRetries: -1}, {Store: newStore(), MaxEventBytes: -1}, {Store: newStore(), MaxObjectBytes: -1}, {Store: newStore(), MaxEventBytes: 20, MaxObjectBytes: 10}, {Store: newStore(), MaxObjectBytes: 257 << 20}, {Store: newStore(), Prefix: "a/../b"}, {Store: newStore(), Prefix: "a//b"}, {Store: newStore(), Prefix: "x\x00y"}, {Store: newStore(), Prefix: "x\\y"}, {Store: newStore(), Prefix: string([]byte{255})}} {
		if _, err := Open[int](cfg); err == nil {
			t.Fatalf("accepted %+v", cfg)
		}
	}
	lg, err := Open[int](Config{Store: newStore(), Prefix: "/some/path/"})
	if err != nil {
		t.Fatal(err)
	}
	if lg.logKey(1) != "some/path/v1/log/7ffffffffffffffe.json" {
		t.Fatal(lg.logKey(1))
	}
	if _, err := lg.Append(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	other := testLog[int](t, lg.store)
	s, err := lg.LoadHead(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.AppendTo(context.Background(), s, 2); err == nil {
		t.Fatal("foreign snapshot accepted")
	}
	if _, err := lg.AppendTo(context.Background(), new(Snapshot[int]), 2); err == nil {
		t.Fatal("uninitialized snapshot accepted")
	}
}

func TestLimits(t *testing.T) {
	s := newStore()
	lg, err := Open[string](Config{Store: s, MaxEventBytes: 8, MaxObjectBytes: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lg.Append(context.Background(), "1234567"); !errors.Is(err, ErrTooLarge) {
		t.Fatal(err)
	}
	if s.creates+s.gets+s.lists != 0 {
		t.Fatal("oversize event performed I/O")
	}
	tiny, err := Open[int](Config{Store: s, MaxEventBytes: 4, MaxObjectBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tiny.Append(context.Background(), 0); !errors.Is(err, ErrTooLarge) {
		t.Fatal(err)
	}
	if s.creates != 0 {
		t.Fatal("oversize commit was uploaded")
	}
	s.objects[lg.logKey(1)] = bytes.Repeat([]byte("x"), 1001)
	if _, err := lg.LoadHead(context.Background()); !errors.Is(err, ErrTooLarge) || !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}

func TestCancellation(t *testing.T) {
	s := newStore()
	lg := testLog[int](t, s)
	base := build(t, lg, 16)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := lg.Append(ctx, 16); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := lg.AppendTo(ctx, base, 16); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := lg.LoadHead(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := lg.Verify(ctx, base); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	calls := 0
	err := lg.Scan(ctx, base, Range{1, 16}, func(Record[int]) error { calls++; cancel(); return nil })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatal(err, calls)
	}
}

func TestExhaustion(t *testing.T) {
	lg := testLog[int](t, newStore())
	s := &Snapshot[int]{owner: lg, commit: &commit{}, record: Record[int]{Revision: MaxRevision}}
	if _, err := lg.AppendTo(context.Background(), s, 1); !errors.Is(err, ErrExhausted) {
		t.Fatal(err)
	}
}

type countingJSON struct {
	N     int
	Count *atomic.Int32
}

func (v countingJSON) MarshalJSON() ([]byte, error)  { v.Count.Add(1); return json.Marshal(v.N) }
func (v *countingJSON) UnmarshalJSON(b []byte) error { return json.Unmarshal(b, &v.N) }

func TestMarshalOnceAcrossConflict(t *testing.T) {
	s := newStore()
	lg := testLog[countingJSON](t, s)
	var calls atomic.Int32
	injected := false
	s.before = func(key string, body []byte) error {
		if injected || !strings.Contains(key, "/log/") {
			return nil
		}
		injected = true
		var c commit
		if err := json.Unmarshal(body, &c); err != nil {
			t.Fatal(err)
		}
		c.CommitID = strings.Repeat("a", 32)
		c.Event = json.RawMessage("[99]")
		c.RecordHash = recordHash(1, c.CommitID, zeroHash, c.Event)
		b, _ := json.Marshal(c)
		s.mu.Lock()
		s.objects[key] = b
		s.mu.Unlock()
		return ErrExists
	}
	r, err := lg.Append(context.Background(), countingJSON{7, &calls}, countingJSON{8, &calls})
	if err != nil {
		t.Fatal(err)
	}
	if r.Revision != 2 || len(r.Value) != 2 || r.Value[0].N != 7 || r.Value[1].N != 8 || calls.Load() != 2 {
		t.Fatalf("%+v marshals %d", r, calls.Load())
	}
}

func TestRecordHashVector(t *testing.T) {
	got := recordHash(1, "000102030405060708090a0b0c0d0e0f", zeroHash, []byte(`[{"test":true}]`))
	const want = "1463a1d96fc2a96a9eec2aa9d6b10f1de120805e73aeeaf92a6a977873ef340c"
	if got != want {
		t.Fatalf("record hash %s", got)
	}
}

func TestInvalidRevisions(t *testing.T) {
	s := newStore()
	lg := testLog[int](t, s)
	ctx := context.Background()
	var empty *Snapshot[int]
	if empty.Revision() != 0 || empty.Record().Revision != 0 {
		t.Fatal("empty snapshot must report the invalid zero revision")
	}
	for _, revision := range []int64{math.MinInt64, -1, 0, MaxRevision + 1, math.MaxInt64} {
		if _, err := lg.LoadRevision(ctx, revision); !errors.Is(err, ErrRange) {
			t.Fatalf("LoadRevision(%d): %v", revision, err)
		}
		if err := lg.validateFrontier(nil, revision, zeroHash); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("frontier revision %d: %v", revision, err)
		}
		invalid := &Snapshot[int]{owner: lg, commit: &commit{}, record: Record[int]{Revision: revision}}
		if _, err := lg.AppendTo(ctx, invalid, 1); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("AppendTo revision %d: %v", revision, err)
		}
		if err := lg.Verify(ctx, invalid); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Verify revision %d: %v", revision, err)
		}
	}
	if s.creates+s.gets+s.lists != 0 {
		t.Fatal("invalid revisions performed store I/O")
	}
	first, err := lg.AppendTo(ctx, nil, 42)
	if err != nil {
		t.Fatal(err)
	}
	if first.Revision() != 1 || first.commit.Previous != nil || len(first.commit.Frontier) != 0 {
		t.Fatal("first record must be revision 1 with no predecessor or frontier")
	}
	before := s.creates + s.gets + s.lists
	for _, r := range []Range{{0, 1}, {-1, 1}, {1, 0}, {1, -1}, {math.MinInt64, 1}, {1, MaxRevision + 1}, {MaxRevision + 1, MaxRevision + 1}} {
		if err := lg.Scan(ctx, first, r, func(Record[int]) error { t.Fatal("invalid range yielded a record"); return nil }); !errors.Is(err, ErrRange) {
			t.Fatalf("Scan(%+v): %v", r, err)
		}
	}
	if s.creates+s.gets+s.lists != before {
		t.Fatal("invalid ranges performed store I/O")
	}
	for _, revision := range []string{"0000000000000000", "0020000000000000", "7fffffffffffffff", "8000000000000000", "ffffffffffffffff", "-000000000000001"} {
		if _, err := parseRevision(revision); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("accepted revision %q", revision)
		}
		c := *first.commit
		c.Revision = revision
		if _, err := lg.validateProjection(c.project()); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("accepted record revision %q", revision)
		}
		ref := lg.commitRef(first)
		ref.Start, ref.End = revision, revision
		if _, _, err := lg.validateRef(ref); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("accepted reference revision %q", revision)
		}
	}
	for _, key := range []string{"v1/log/7fffffffffffffff.json", "v1/log/7fdfffffffffffff.json", "v1/log/0000000000000000.json", "v1/log/8000000000000000.json", "v1/log/ffffffffffffffff.json"} {
		if _, err := lg.parseLogKey(key); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("accepted key %q", key)
		}
	}
}

func TestFinalRevision(t *testing.T) {
	if MaxRevision != 9007199254740991 {
		t.Fatal("MaxRevision must equal JavaScript's Number.MAX_SAFE_INTEGER")
	}
	s := newStore()
	lg := testLog[int](t, s)
	ctx := context.Background()
	const revision int64 = MaxRevision - 1
	c := commit{
		Format:   commitFormat,
		Revision: hexRevision(revision),
		CommitID: strings.Repeat("0", 32),
		Previous: &previous{lg.logKey(revision - 1), zeroHash},
		Event:    json.RawMessage("[0]"),
		Frontier: syntheticFrontier(lg, revision),
	}
	c.RecordHash = recordHash(revision, c.CommitID, zeroHash, c.Event)
	body, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Create(ctx, lg.logKey(revision), body); err != nil {
		t.Fatal(err)
	}
	final, err := lg.Append(ctx, 1)
	if err != nil || final.Revision != MaxRevision {
		t.Fatalf("final append: %+v, %v", final, err)
	}
	if lg.logKey(final.Revision) != "v1/log/7fe0000000000000.json" {
		t.Fatal("wrong final revision key")
	}
	snap, err := lg.LoadHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Revision() != MaxRevision {
		t.Fatalf("head: %d", snap.Revision())
	}
	if start, end, err := lg.validateRef(lg.commitRef(snap)); err != nil || start != MaxRevision || end != MaxRevision {
		t.Fatalf("final reference: %d..%d: %v", start, end, err)
	}
	var got []Record[int]
	if err := lg.Scan(ctx, snap, Range{revision, MaxRevision}, func(r Record[int]) error { got = append(got, r); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Revision != revision || len(got[0].Value) != 1 || got[0].Value[0] != 0 || !reflect.DeepEqual(got[1], final) {
		t.Fatalf("final range: %+v", got)
	}
	for _, tt := range []struct {
		r    Range
		want []Record[int]
	}{
		{StartingAt(MaxRevision), []Record[int]{final}},
		{After(MaxRevision - 1), []Record[int]{final}},
		{After(MaxRevision), nil},
	} {
		before := s.gets + s.lists + s.creates
		var got []Record[int]
		if err := lg.Scan(ctx, snap, tt.r, func(r Record[int]) error { got = append(got, r); return nil }); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Fatalf("final range %+v: got %+v, want %+v", tt.r, got, tt.want)
		}
		if s.gets+s.lists+s.creates != before {
			t.Fatal("final-record or empty scan performed store I/O")
		}
	}
	loaded, err := lg.LoadRevision(ctx, MaxRevision)
	if err != nil || !reflect.DeepEqual(loaded.Record(), final) {
		t.Fatalf("load final revision: %v", err)
	}
	before := s.creates + s.gets + s.lists
	if err := lg.Scan(ctx, snap, Range{MaxRevision, MaxRevision + 1}, func(Record[int]) error {
		t.Fatal("out-of-range scan yielded a record")
		return nil
	}); !errors.Is(err, ErrRange) {
		t.Fatalf("Scan beyond maximum: %v", err)
	}
	if s.creates+s.gets+s.lists != before {
		t.Fatal("out-of-range scan performed store I/O")
	}
	creates := s.creates
	if _, err := lg.Append(ctx, 2); !errors.Is(err, ErrExhausted) {
		t.Fatalf("Append beyond maximum: %v", err)
	}
	if _, err := lg.AppendTo(ctx, snap, 2); !errors.Is(err, ErrExhausted) {
		t.Fatalf("AppendTo beyond maximum: %v", err)
	}
	if s.creates != creates {
		t.Fatal("exhausted append attempted a creation")
	}
}
