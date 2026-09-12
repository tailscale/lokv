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
	l, err := Open[T](Config{Store: s})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func build(t testing.TB, l *Log[int], n int) *Snapshot[int] {
	t.Helper()
	var snap *Snapshot[int]
	for i := 0; i < n; i++ {
		var err error
		snap, err = l.AppendTo(context.Background(), snap, i)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	return snap
}

func TestReverseKeys(t *testing.T) {
	l := testLog[int](t, newStore())
	revisions := []uint64{0, 1, 15, 16, math.MaxUint64 - 1, math.MaxUint64}
	keys := make([]string, len(revisions))
	for i, r := range revisions {
		keys[i] = l.logKey(r)
		actual, err := l.parseLogKey(keys[i])
		if err != nil || actual != r {
			t.Fatalf("%d: %d %v", r, actual, err)
		}
	}
	sort.Strings(keys)
	for i, key := range keys {
		r, _ := l.parseLogKey(key)
		if r != revisions[len(revisions)-1-i] {
			t.Fatal("incorrect lexical order")
		}
	}
	for _, key := range []string{"v1/log/FFFFFFFFFFFFFFFF.json", "v1/log/fff.json", "else/v1/log/ffffffffffffffff.json", "v1/log/ffffffffffffffff.json/", "v1/log/000000000000000g.json"} {
		if _, err := l.parseLogKey(key); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("accepted %s", key)
		}
	}
}

func TestBoundariesAndCounts(t *testing.T) {
	for _, n := range []int{0, 1, 15, 16, 17, 255, 256, 257, 4097} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			s := newStore()
			l := testLog[int](t, s)
			var snap *Snapshot[int]
			for i := 0; i < n; i++ {
				before := s.creates
				var err error
				snap, err = l.AppendTo(context.Background(), snap, i)
				if err != nil {
					t.Fatal(err)
				}
				depth := 0
				for x := i; x > 0 && x%16 == 0; x /= 16 {
					depth++
				}
				if got := s.creates - before; got != 1+depth {
					t.Fatalf("revision %d creations %d; want %d", i, got, 1+depth)
				}
				if err := l.validateFrontier(snap.commit.Frontier, uint64(i), snap.commit.project().PreviousRecordHash); err != nil {
					t.Fatal(err)
				}
			}
			beforeLists, beforeGets := s.lists, s.gets
			head, ok, err := l.Head(context.Background())
			if err != nil || ok != (n > 0) {
				t.Fatalf("head: %v %v", ok, err)
			}
			if s.lists-beforeLists != 1 || s.gets-beforeGets != min(1, n) {
				t.Fatal("HEAD request count")
			}
			if n == 0 {
				if err := l.Verify(context.Background(), nil); err != nil {
					t.Fatal(err)
				}
				if err := l.Scan(context.Background(), nil, Range{}, func(Record[int]) error { return nil }); !errors.Is(err, ErrRange) {
					t.Fatal(err)
				}
				return
			}
			if head.Revision != uint64(n-1) || head.Value != n-1 {
				t.Fatal(head)
			}
			var got []int
			err = l.Scan(context.Background(), snap, Range{0, uint64(n - 1)}, func(r Record[int]) error {
				if r.Revision != uint64(len(got)) {
					t.Fatal("unordered scan")
				}
				got = append(got, r.Value)
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
			if err := l.Verify(context.Background(), snap); err != nil {
				t.Fatal(err)
			}
			if n > 16 {
				lists := s.lists
				old, err := l.LoadRevision(context.Background(), 15)
				if err != nil {
					t.Fatal(err)
				}
				if s.lists != lists {
					t.Fatal("historical load listed")
				}
				if err := l.Verify(context.Background(), old); err != nil {
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
	l := testLog[T](t, s)
	for i := 0; i < 17; i++ {
		if _, err := l.Append(context.Background(), value); err != nil {
			t.Fatal(err)
		}
	}
	snap, err := l.LoadHead(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Scan(context.Background(), snap, Range{0, 16}, func(r Record[T]) error {
		if !reflect.DeepEqual(r.Value, value) {
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
	roundTrip(t, []string{"a", "b"})
	v := 7
	roundTrip(t, &v)
	roundTrip(t, (*int)(nil))
	roundTrip(t, customJSON{7})
	roundTrip[any](t, nil)
}

func TestMarshalFailureNoIO(t *testing.T) {
	s := newStore()
	l := testLog[any](t, s)
	if _, err := l.Append(context.Background(), func() {}); err == nil {
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
			l, err := Open[int](Config{Store: s, MaxConflictRetries: 128})
			if err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := 0; i < n; i++ {
				wg.Go(func() {
					<-start
					if _, err := l.Append(context.Background(), i); err != nil {
						t.Errorf("append: %v", err)
					}
				})
			}
			close(start)
			wg.Wait()
			snap, err := l.LoadHead(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if snap.Revision() != uint64(n-1) {
				t.Fatalf("head %d", snap.Revision())
			}
			seen := map[int]bool{}
			err = l.Scan(context.Background(), snap, Range{0, uint64(n - 1)}, func(r Record[int]) error {
				if seen[r.Value] {
					t.Error("duplicate input")
				}
				seen[r.Value] = true
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
	l := testLog[int](t, s)
	base := build(t, l, 16)
	winner, err := l.AppendTo(context.Background(), base, 16)
	if err != nil {
		t.Fatal(err)
	}
	lists := s.lists
	if _, err := l.AppendTo(context.Background(), base, 999); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if s.lists != lists {
		t.Fatal("AppendTo listed")
	}
	if _, err := l.AppendTo(context.Background(), nil, 999); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if err := l.Verify(context.Background(), winner); err != nil {
		t.Fatal(err)
	}
	if base.Revision() != 15 || len(base.commit.Frontier) != 1 || len(base.commit.Frontier[0].Refs) != 15 {
		t.Fatal("base was mutated")
	}
}

func TestAmbiguousSuccess(t *testing.T) {
	for _, reported := range []error{errors.New("lost response"), ErrExists} {
		t.Run(reported.Error(), func(t *testing.T) {
			s := newStore()
			l := testLog[int](t, s)
			build(t, l, 16)
			s.after = func(string, []byte) error { return reported }
			r, err := l.Append(context.Background(), 42)
			if err != nil || r.Revision != 16 {
				t.Fatalf("append: %v %v", r, err)
			}
			snap, err := l.LoadHead(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if snap.Revision() != 16 {
				t.Fatal("duplicate commit")
			}
			if err := l.Verify(context.Background(), snap); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCrashAfterAggregate(t *testing.T) {
	s := newStore()
	l := testLog[int](t, s)
	base := build(t, l, 4096)
	original := make(map[string][]byte, len(s.objects))
	for k, v := range s.objects {
		original[k] = v
	}
	failure := errors.New("injected crash")
	// Revision 4096 carries through segment, level 2, and level 3. Fail the
	// following creation after each durable aggregate, including final commit.
	for after := 1; after <= 3; after++ {
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
			if _, err := l.AppendTo(context.Background(), base, 4096); !errors.Is(err, failure) {
				t.Fatalf("fault: %v", err)
			}
			head, err := l.LoadHead(context.Background())
			if err != nil || head.Revision() != 4095 {
				t.Fatalf("published incomplete commit: %v", err)
			}
			if err := l.Verify(context.Background(), head); err != nil {
				t.Fatal(err)
			}
			s.before = nil
			next, err := l.AppendTo(context.Background(), base, 4096)
			if err != nil {
				t.Fatal(err)
			}
			if err := l.Verify(context.Background(), next); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRangesAndPruning(t *testing.T) {
	s := newStore()
	l := testLog[int](t, s)
	snap := build(t, l, 513)
	for _, r := range []Range{{0, 0}, {15, 16}, {16, 31}, {255, 256}, {256, 271}, {511, 512}, {512, 512}, {0, 512}} {
		s.getKeys = nil
		var got []uint64
		if err := l.Scan(context.Background(), snap, r, func(v Record[int]) error { got = append(got, v.Revision); return nil }); err != nil {
			t.Fatal(err)
		}
		if len(got) != int(r.Last-r.First+1) || got[0] != r.First || got[len(got)-1] != r.Last {
			t.Fatalf("range %+v: %v", r, got)
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
		if r.First == 512 && len(s.getKeys) != 0 {
			t.Fatal("head-only scan fetched objects")
		}
		if r.First == 256 && r.Last == 271 && len(s.getKeys) != 2 {
			t.Fatalf("GETs: %v", s.getKeys)
		}
	}
	for _, r := range []Range{{1, 0}, {0, 513}, {math.MaxUint64, math.MaxUint64}} {
		if err := l.Scan(context.Background(), snap, r, func(Record[int]) error { return nil }); !errors.Is(err, ErrRange) {
			t.Fatal(err)
		}
	}
	stop := errors.New("stop")
	s.getKeys = nil
	calls := 0
	if err := l.Scan(context.Background(), snap, Range{0, 512}, func(Record[int]) error { calls++; return stop }); !errors.Is(err, stop) || calls != 1 || len(s.getKeys) != 2 {
		t.Fatalf("early stop: %d %d %v", calls, len(s.getKeys), err)
	}
}

func TestConfiguration(t *testing.T) {
	var typedNil *testStore
	for _, cfg := range []Config{{}, {Store: typedNil}, {Store: newStore(), MaxConflictRetries: -1}, {Store: newStore(), MaxEventBytes: -1}, {Store: newStore(), MaxObjectBytes: -1}, {Store: newStore(), MaxEventBytes: 20, MaxObjectBytes: 10}, {Store: newStore(), MaxObjectBytes: 257 << 20}, {Store: newStore(), Prefix: "a/../b"}, {Store: newStore(), Prefix: "a//b"}, {Store: newStore(), Prefix: "x\x00y"}, {Store: newStore(), Prefix: "x\\y"}, {Store: newStore(), Prefix: string([]byte{255})}} {
		if _, err := Open[int](cfg); err == nil {
			t.Fatalf("accepted %+v", cfg)
		}
	}
	l, err := Open[int](Config{Store: newStore(), Prefix: "/some/path/"})
	if err != nil {
		t.Fatal(err)
	}
	if l.logKey(0) != "some/path/v1/log/ffffffffffffffff.json" {
		t.Fatal(l.logKey(0))
	}
	if _, err := l.Append(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	other := testLog[int](t, l.store)
	s, err := l.LoadHead(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.AppendTo(context.Background(), s, 2); err == nil {
		t.Fatal("foreign snapshot accepted")
	}
	if _, err := l.AppendTo(context.Background(), new(Snapshot[int]), 2); err == nil {
		t.Fatal("uninitialized snapshot accepted")
	}
}

func TestLimits(t *testing.T) {
	s := newStore()
	l, err := Open[string](Config{Store: s, MaxEventBytes: 8, MaxObjectBytes: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append(context.Background(), "1234567"); !errors.Is(err, ErrTooLarge) {
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
	s.objects[l.logKey(0)] = bytes.Repeat([]byte("x"), 1001)
	if _, err := l.LoadHead(context.Background()); !errors.Is(err, ErrTooLarge) || !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}

func TestCancellation(t *testing.T) {
	s := newStore()
	l := testLog[int](t, s)
	base := build(t, l, 16)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := l.Append(ctx, 16); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := l.AppendTo(ctx, base, 16); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := l.LoadHead(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := l.Verify(ctx, base); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	calls := 0
	err := l.Scan(ctx, base, Range{0, 15}, func(Record[int]) error { calls++; cancel(); return nil })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatal(err, calls)
	}
}

func TestExhaustion(t *testing.T) {
	l := testLog[int](t, newStore())
	s := &Snapshot[int]{owner: l, commit: &commit{}, record: Record[int]{Revision: math.MaxUint64}}
	if _, err := l.AppendTo(context.Background(), s, 1); !errors.Is(err, ErrExhausted) {
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
	l := testLog[countingJSON](t, s)
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
		c.Event = json.RawMessage("99")
		c.RecordHash = recordHash(0, c.CommitID, zeroHash, c.Event)
		b, _ := json.Marshal(c)
		s.mu.Lock()
		s.objects[key] = b
		s.mu.Unlock()
		return ErrExists
	}
	r, err := l.Append(context.Background(), countingJSON{7, &calls})
	if err != nil {
		t.Fatal(err)
	}
	if r.Revision != 1 || r.Value.N != 7 || calls.Load() != 1 {
		t.Fatalf("%+v marshals %d", r, calls.Load())
	}
}

func TestRecordHashVector(t *testing.T) {
	got := recordHash(0, "000102030405060708090a0b0c0d0e0f", zeroHash, []byte(`{"test":true}`))
	const want = "ebfd51fa3d3a7a9bbca3234cddf1d33b58c2139663ceab750adda16a469def0d"
	if got != want {
		t.Fatalf("record hash %s", got)
	}
}
