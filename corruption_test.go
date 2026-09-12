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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func TestCorruptCommit(t *testing.T) {
	s := newStore()
	l := testLog[int](t, s)
	snap := build(t, l, 17)
	original := bytes.Clone(snap.body)
	key := l.logKey(17)
	tests := map[string]func(map[string]any){
		"format":                  func(c map[string]any) { c["format"] = "unknown" },
		"revision":                func(c map[string]any) { c["revision"] = hexRevision(18) },
		"revision uppercase":      func(c map[string]any) { c["revision"] = "000000000000001A" },
		"commit ID":               func(c map[string]any) { c["commit_id"] = "abc" },
		"event":                   func(c map[string]any) { c["event"] = 99 },
		"record hash":             func(c map[string]any) { c["record_hash"] = zeroHash },
		"previous missing":        func(c map[string]any) { c["previous"] = nil },
		"previous revision":       func(c map[string]any) { c["previous"].(map[string]any)["key"] = l.logKey(14) },
		"previous hash":           func(c map[string]any) { c["previous"].(map[string]any)["record_hash"] = zeroHash },
		"frontier missing":        func(c map[string]any) { c["frontier"] = []any{} },
		"frontier null":           func(c map[string]any) { c["frontier"] = nil },
		"frontier level missing":  func(c map[string]any) { delete(c["frontier"].([]any)[0].(map[string]any), "level") },
		"wrong reference level":   func(c map[string]any) { refMap(c)["level"] = 2 },
		"foreign key":             func(c map[string]any) { refMap(c)["key"] = "https://example.org/stolen" },
		"range":                   func(c map[string]any) { refMap(c)["start"] = hexRevision(2) },
		"boundary":                func(c map[string]any) { refMap(c)["last_record_hash"] = zeroHash },
		"reference missing field": func(c map[string]any) { delete(refMap(c), "sha256") },
	}
	for _, name := range []string{"format", "revision", "commit_id", "previous", "event", "record_hash", "frontier"} {
		tests["missing "+name] = func(c map[string]any) { delete(c, name) }
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			var c map[string]any
			if err := json.Unmarshal(original, &c); err != nil {
				t.Fatal(err)
			}
			mutate(c)
			b, _ := json.Marshal(c)
			s.objects[key] = b
			if _, err := l.LoadHead(context.Background()); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("accepted corruption: %v", err)
			}
		})
	}
	for name, b := range map[string][]byte{"malformed": []byte("{"), "trailing": append(bytes.Clone(original), []byte(" {}")...), "duplicate": append([]byte(`{"format":"bad",`), original[1:]...)} {
		t.Run(name, func(t *testing.T) {
			s.objects[key] = b
			if _, err := l.LoadHead(context.Background()); !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
		})
	}
	s.objects[key] = original
	s.objects[l.logPrefix()+"!shadow"] = []byte("{}")
	if _, err := l.LoadHead(context.Background()); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("shadow HEAD: %v", err)
	}
}

func refMap(c map[string]any) map[string]any {
	return c["frontier"].([]any)[0].(map[string]any)["refs"].([]any)[0].(map[string]any)
}

func TestMissingAndDigestCorruption(t *testing.T) {
	for _, n := range []int{16, 17, 257} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			s := newStore()
			l := testLog[int](t, s)
			snap := build(t, l, n)
			ref := snap.commit.Frontier[len(snap.commit.Frontier)-1].Refs[0]
			original := s.objects[ref.Key]
			delete(s.objects, ref.Key)
			if err := l.Verify(context.Background(), snap); !errors.Is(err, ErrCorrupt) || !errors.Is(err, ErrNotFound) {
				t.Fatalf("missing: %v", err)
			}
			s.objects[ref.Key] = append(bytes.Clone(original), '!')
			if err := l.Verify(context.Background(), snap); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("digest: %v", err)
			}
		})
	}
	s := newStore()
	l := testLog[int](t, s)
	if _, err := l.LoadRevision(context.Background(), 123); !errors.Is(err, ErrNotFound) || errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}

func compressTest(t testing.TB, raw []byte) []byte {
	t.Helper()
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	return enc.EncodeAll(raw, nil)
}

func TestAggregateValidation(t *testing.T) {
	s := newStore()
	l := testLog[int](t, s)
	snap := build(t, l, 257)
	indexRef := snap.commit.Frontier[0].Refs[0]
	node, err := l.decodeIndex(indexRef, s.objects[indexRef.Key])
	if err != nil {
		t.Fatal(err)
	}
	segRef := node.Children[0]
	raw, err := l.decompress(s.objects[segRef.Key])
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*segment){
		"format": func(s *segment) { s.Format = "bad" }, "level": func(s *segment) { s.Level = 2 }, "count": func(s *segment) { s.Records = s.Records[:15] },
		"range": func(s *segment) { s.End = hexRevision(17) }, "record hash": func(s *segment) { s.Records[3].RecordHash = zeroHash },
		"chain": func(s *segment) {
			p := &s.Records[3]
			p.PreviousRecordHash = zeroHash
			p.RecordHash = recordHash(4, p.CommitID, zeroHash, p.Event)
		},
		"order": func(s *segment) { s.Records[3], s.Records[4] = s.Records[4], s.Records[3] },
	} {
		t.Run("segment/"+name, func(t *testing.T) {
			var seg segment
			if err := json.Unmarshal(raw, &seg); err != nil {
				t.Fatal(err)
			}
			mutate(&seg)
			b, _ := json.Marshal(seg)
			ref := segRef
			ref.SHA256 = digest(b)
			ref.Key = l.treeKey(ref)
			if _, err := l.decodeSegment(ref, compressTest(t, b)); !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
		})
	}
	for name, mutate := range map[string]func(*indexNode){
		"format": func(n *indexNode) { n.Format = "bad" }, "level": func(n *indexNode) { n.Level = 3 }, "count": func(n *indexNode) { n.Children = n.Children[:15] },
		"range": func(n *indexNode) { n.End = hexRevision(254) }, "child level": func(n *indexNode) { n.Children[3].Level = 2 },
		"adjacency": func(n *indexNode) { n.Children[3] = n.Children[4] }, "chain": func(n *indexNode) { n.Children[3].FirstPrevHash = zeroHash },
		"boundary": func(n *indexNode) { n.Children[15].LastRecordHash = zeroHash },
	} {
		t.Run("index/"+name, func(t *testing.T) {
			var n indexNode
			if err := json.Unmarshal(s.objects[indexRef.Key], &n); err != nil {
				t.Fatal(err)
			}
			mutate(&n)
			b, _ := json.Marshal(n)
			ref := indexRef
			ref.SHA256 = digest(b)
			ref.Key = l.treeKey(ref)
			if _, err := l.decodeIndex(ref, b); !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
		})
	}
	// A correctly hashed segment with trailing JSON still must fail decoding.
	b := append(bytes.Clone(raw), []byte(" {}")...)
	ref := segRef
	ref.SHA256 = digest(b)
	ref.Key = l.treeKey(ref)
	if _, err := l.decodeSegment(ref, compressTest(t, b)); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}

func TestDecompressionLimits(t *testing.T) {
	l, err := Open[int](Config{Store: newStore(), MaxEventBytes: 1024, MaxObjectBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	valid := compressTest(t, []byte(`{"ok":true}`))
	if raw, err := l.decompress(valid); err != nil || string(raw) != `{"ok":true}` {
		t.Fatal(string(raw), err)
	}
	for name, b := range map[string][]byte{
		"second frame": append(bytes.Clone(valid), valid...), "trailing byte": append(bytes.Clone(valid), 0), "truncated": valid[:len(valid)-1], "invalid": []byte("not zstd"),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := l.decompress(b); !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
		})
	}
	bomb := compressTest(t, bytes.Repeat([]byte("a"), 1<<20))
	if _, err := l.decompress(bomb); !errors.Is(err, ErrTooLarge) || !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if _, err := l.decompress(bytes.Repeat([]byte("a"), 4097)); !errors.Is(err, ErrTooLarge) {
		t.Fatal(err)
	}
	// A streaming frame need not advertise its decompressed size.
	var buf bytes.Buffer
	enc, err := zstd.NewWriter(&buf, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := enc.Write(bytes.Repeat([]byte("a"), 1<<20)); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := l.decompress(buf.Bytes()); !errors.Is(err, ErrTooLarge) {
		t.Fatal(err)
	}
}

func TestFullWidthFrontier(t *testing.T) {
	l := testLog[int](t, newStore())
	frontier := syntheticFrontier(l, math.MaxInt64)
	var count int
	for _, f := range frontier {
		count += len(f.Refs)
	}
	if count != 231 {
		t.Fatalf("maximum frontier has %d references", count)
	}
	if err := l.validateFrontier(frontier, math.MaxInt64, zeroHash); err != nil {
		t.Fatal(err)
	}
	reverse := append([]frontierLevel(nil), frontier...)
	reverse[0], reverse[1] = reverse[1], reverse[0]
	if err := l.validateFrontier(reverse, math.MaxInt64, zeroHash); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	bad := frontier[15].Refs[0]
	bad.Start = hexRevision(math.MaxInt64)
	bad.End = hexRevision(math.MaxInt64)
	if _, _, err := l.validateRef(bad); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	// This aligned block would end at MaxInt64+1. Reject before adding its size.
	bad.Start = hexRevision(math.MaxInt64 - (1 << 60) + 2)
	if _, _, err := l.validateRef(bad); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("overflowing aligned range: %v", err)
	}
}

// syntheticFrontier builds metadata for boundary tests without storing the
// referenced history. Tests only fetch the real records appended after this root.
func syntheticFrontier(l *Log[int], revision int64) []frontierLevel {
	var levels [16][]objectRef
	next := int64(1)
	for level := 15; level >= 0; level-- {
		size := int64(1) << (4 * level)
		for i := int64(0); i < ((revision-1)>>(4*level))&15; i++ {
			ref := objectRef{uint8(level), hexRevision(next), hexRevision(next + (size - 1)), "", zeroHash, zeroHash, zeroHash}
			if level == 0 {
				ref.Key = l.logKey(next)
			} else {
				ref.Key = l.treeKey(ref)
			}
			levels[level] = append(levels[level], ref)
			next += size
		}
	}
	var frontier []frontierLevel
	for level, refs := range levels {
		if len(refs) != 0 {
			frontier = append(frontier, frontierLevel{uint8(level), refs})
		}
	}
	return frontier
}

type blockingGetStore struct {
	Store
	active  atomic.Int32
	started chan struct{}
}

func (s *blockingGetStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.active.Add(1)
	defer s.active.Add(-1)
	s.started <- struct{}{}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestBoundedGETCancellation(t *testing.T) {
	s := newStore()
	l := testLog[int](t, s)
	base := build(t, l, 16)
	blocked := &blockingGetStore{Store: s, started: make(chan struct{}, 16)}
	l.store = blocked
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := l.AppendTo(ctx, base, 16); done <- err }()
	for i := 0; i < 4; i++ {
		select {
		case <-blocked.started:
		case <-time.After(2 * time.Second):
			t.Fatal("workers did not start")
		}
	}
	if got := blocked.active.Load(); got != 4 {
		t.Fatalf("active workers %d", got)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("workers leaked")
	}
	if blocked.active.Load() != 0 {
		t.Fatal("GET still running")
	}
	if len(blocked.started) != 0 {
		t.Fatal("started extra workers")
	}
}

func TestCreateFailureAndInvisibility(t *testing.T) {
	failure := errors.New("permanent backend failure")
	s := newStore()
	l := testLog[int](t, s)
	s.before = func(string, []byte) error { return failure }
	if _, err := l.Append(context.Background(), 1); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if s.creates != 1 {
		t.Fatal("retried an unclassified error")
	}
	s.before = func(string, []byte) error { return ErrExists }
	if _, err := l.Append(context.Background(), 1); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}

func TestSnapshotValueMutation(t *testing.T) {
	l := testLog[map[string]int](t, newStore())
	s, err := l.AppendTo(context.Background(), nil, map[string]int{"x": 1})
	if err != nil {
		t.Fatal(err)
	}
	s.Record().Value["x"] = 99
	next, err := l.AppendTo(context.Background(), s, map[string]int{"x": 2})
	if err != nil {
		t.Fatal(err)
	}
	err = l.Scan(context.Background(), next, Range{1, 2}, func(r Record[map[string]int]) error {
		if r.Value["x"] != int(r.Revision) {
			t.Fatal("snapshot mutation changed wire data")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRequiredGenesisFields(t *testing.T) {
	l := testLog[int](t, newStore())
	s := build(t, l, 1)
	for _, part := range []string{`"previous":null,`, `"event":0,`, `"frontier":[]`} {
		body := strings.Replace(string(s.body), part, "", 1)
		// Removing the final field needs its preceding comma removed as well.
		body = strings.Replace(body, ",}", "}", 1)
		if _, err := l.decodeCommit(l.logKey(1), []byte(body)); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
	}
}

var privateCodecFailure = errors.New("private payload text")

type failedCodec int

func (failedCodec) MarshalJSON() ([]byte, error) { return nil, privateCodecFailure }

func TestCodecErrorRedaction(t *testing.T) {
	l := testLog[failedCodec](t, newStore())
	_, err := l.Append(context.Background(), 0)
	if !errors.Is(err, privateCodecFailure) || strings.Contains(err.Error(), "private payload") {
		t.Fatal(err)
	}
}

func TestEventBytesAndCaseAliases(t *testing.T) {
	l := testLog[string](t, newStore())
	s, err := l.AppendTo(context.Background(), nil, "safe")
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range []json.RawMessage{json.RawMessage(`"<"`), json.RawMessage(`{ "x": 1 }`)} {
		c := *s.commit
		c.Event = event
		c.RecordHash = recordHash(1, c.CommitID, zeroHash, c.Event)
		// Build the noncanonical envelope without RawMessage marshaling normalizing it.
		b := bytes.Replace(s.body, []byte(`"safe"`), event, 1)
		b = bytes.Replace(b, []byte(s.commit.RecordHash), []byte(c.RecordHash), 1)
		if _, err := l.decodeCommit(l.logKey(1), b); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
	}
	b := append([]byte(`{"Format":"lokv/commit/v1",`), s.body[1:]...)
	if _, err := l.decodeCommit(l.logKey(1), b); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	b = append([]byte(`{"future_field":{"any":"value"},`), s.body[1:]...)
	if _, err := l.decodeCommit(l.logKey(1), b); err != nil {
		t.Fatal(err)
	}
}

func TestAggregateEventLimit(t *testing.T) {
	s := newStore()
	l, err := Open[string](Config{Store: s, MaxEventBytes: 2000, MaxObjectBytes: 16000})
	if err != nil {
		t.Fatal(err)
	}
	var snap *Snapshot[string]
	for i := 0; i < 16; i++ {
		snap, err = l.AppendTo(context.Background(), snap, strings.Repeat("x", 1500))
		if err != nil {
			t.Fatal(err)
		}
	}
	creates := s.creates
	if _, err := l.AppendTo(context.Background(), snap, "next"); !errors.Is(err, ErrTooLarge) {
		t.Fatal(err)
	}
	if s.creates != creates {
		t.Fatal("oversized segment was uploaded")
	}
	head, err := l.LoadHead(context.Background())
	if err != nil || head.Revision() != 16 {
		t.Fatal(err)
	}
}
