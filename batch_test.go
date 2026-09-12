// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func TestBatchPacking(t *testing.T) {
	s := newStore()
	lg := testLog[int](t, s)
	ctx := context.Background()
	var snap *Snapshot[int]
	var want []Record[int]
	for revision := 1; revision <= 17; revision++ {
		value := []int{revision * 10, revision*10 + 1, revision*10 + 2}
		creates := s.creates
		var err error
		snap, err = lg.AppendTo(ctx, snap, value...)
		if err != nil {
			t.Fatal(err)
		}
		if snap.Revision() != int64(revision) || !reflect.DeepEqual(snap.Record().Value, value) {
			t.Fatalf("batch %d: %+v", revision, snap.Record())
		}
		wantCreates := 1
		if revision == 17 {
			wantCreates = 2 // One segment of 16 batches, plus the new commit.
		}
		if s.creates-creates != wantCreates {
			t.Fatalf("batch %d created %d objects; want %d", revision, s.creates-creates, wantCreates)
		}
		want = append(want, snap.Record())
		value[0] = -1 // Caller mutations must not change the committed batch.
	}
	var got []Record[int]
	if err := lg.Scan(ctx, snap, All(), func(r Record[int]) error { got = append(got, r); return nil }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("packing changed batch boundaries, order, values, or identity")
	}
	first, err := lg.LoadRevision(ctx, 1)
	if err != nil || !reflect.DeepEqual(first.Record(), want[0]) {
		t.Fatalf("historical batch: %v", err)
	}
}

func TestBatchAtomicPublication(t *testing.T) {
	s := newStore()
	lg := testLog[int](t, s)
	ctx := context.Background()
	base := build(t, lg, 1)
	value := []int{10, 20, 30}
	var observed bool
	s.before = func(key string, body []byte) error {
		head, err := lg.LoadHead(ctx)
		if err != nil || head.Revision() != base.Revision() {
			t.Fatalf("batch was visible before commit: %v", err)
		}
		var c commit
		if err := json.Unmarshal(body, &c); err != nil {
			t.Fatal(err)
		}
		if string(c.Event) != "[10,20,30]" {
			t.Fatalf("commit does not contain the whole batch: %s", c.Event)
		}
		observed = true
		return nil
	}
	creates := s.creates
	snap, err := lg.AppendTo(ctx, base, value...)
	if err != nil || !observed || s.creates != creates+1 {
		t.Fatalf("batch publication: %v", err)
	}
	head, err := lg.LoadHead(ctx)
	if err != nil || head.Revision() != 2 || !reflect.DeepEqual(head.Record(), snap.Record()) {
		t.Fatalf("published batch: %v", err)
	}
	s.before = nil
	if _, err := lg.AppendTo(ctx, base, 40, 50); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale batch: %v", err)
	}
	if _, err := lg.LoadRevision(ctx, 3); !errors.Is(err, ErrNotFound) {
		t.Fatalf("conflicting batch appended a prefix: %v", err)
	}
}

func TestBatchLimits(t *testing.T) {
	s := newStore()
	lg, err := Open[int](Config{Store: s, MaxEventBytes: 5})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, appendBatch := range []func() error{
		func() error { _, err := lg.Append(ctx); return err },
		func() error { _, err := lg.AppendTo(ctx, nil); return err },
	} {
		if err := appendBatch(); !errors.Is(err, ErrEmptyBatch) {
			t.Fatalf("empty batch: %v", err)
		}
	}
	if s.creates+s.gets+s.lists != 0 {
		t.Fatal("empty batch performed store I/O")
	}
	snap, err := lg.AppendTo(ctx, nil, 1, 2) // [1,2] is exactly five bytes.
	if err != nil {
		t.Fatal(err)
	}
	before := s.creates + s.gets + s.lists
	if _, err := lg.Append(ctx, 1, 23); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized batch: %v", err)
	}
	if _, err := lg.AppendTo(ctx, snap, 1, 23); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized conditional batch: %v", err)
	}
	if s.creates+s.gets+s.lists != before {
		t.Fatal("oversized batch performed store I/O")
	}
}

type batchCodecValue int

func (v *batchCodecValue) UnmarshalJSON(b []byte) error {
	if string(b) == "2" {
		return privateCodecFailure
	}
	return json.Unmarshal(b, (*int)(v))
}

func TestBatchDecodeFailure(t *testing.T) {
	s := newStore()
	lg := testLog[batchCodecValue](t, s)
	if _, err := lg.AppendTo(context.Background(), nil, 1, 2); !errors.Is(err, privateCodecFailure) {
		t.Fatalf("batch decode failure: %v", err)
	}
	if s.creates+s.gets+s.lists != 0 {
		t.Fatal("partly decoded batch performed store I/O")
	}
}

func TestBatchWireArray(t *testing.T) {
	ctx := context.Background()
	for _, value := range [][]byte{{255}, {1, 2, 255}} {
		lg := testLog[byte](t, newStore())
		snap, err := lg.AppendTo(ctx, nil, value...)
		if err != nil {
			t.Fatal(err)
		}
		var stored []int
		if err := json.Unmarshal(snap.commit.Event, &stored); err != nil || len(stored) != len(value) {
			t.Fatalf("byte batch must be a JSON array: %s, %v", snap.commit.Event, err)
		}
		for i, item := range value {
			if stored[i] != int(item) {
				t.Fatalf("byte %d: %d", i, stored[i])
			}
		}
		if !reflect.DeepEqual(snap.Record().Value, value) {
			t.Fatal("byte batch did not round trip")
		}
	}
}

func TestBatchWireValidation(t *testing.T) {
	lg := testLog[any](t, newStore())
	snap, err := lg.AppendTo(context.Background(), nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range []string{"null", "[]", "[ ]", "1", "{}", `"AQI="`} {
		c := *snap.commit
		c.Event = json.RawMessage(event)
		c.RecordHash = recordHash(1, c.CommitID, zeroHash, c.Event)
		if _, err := lg.validateProjection(c.project()); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("accepted non-batch event %q: %v", event, err)
		}
	}
	// v1 arrays had different batch semantics; v2 used reference-only upper levels.
	for _, format := range []string{"lokv/commit/v1", "lokv/commit/v2"} {
		c := *snap.commit
		c.Format = format
		body, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := lg.decodeCommit(lg.logKey(1), body); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("accepted %s: %v", format, err)
		}
	}
}
