// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"context"
	"testing"
)

func BenchmarkAppend(b *testing.B) {
	s := newStore()
	lg := testLog[int](b, s)
	var snap *Snapshot[int]
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		snap, err = lg.AppendTo(ctx, snap, i)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(s.creates)/float64(b.N), "creates/op")
	b.ReportMetric(float64(s.gets)/float64(b.N), "gets/op")
	b.ReportMetric(float64(s.creates-b.N)/float64(b.N), "carry-levels/op")
}

func BenchmarkScan(b *testing.B) {
	s := newStore()
	lg := testLog[int](b, s)
	snap := build(b, lg, 4097)
	s.gets = 0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := lg.Scan(context.Background(), snap, Range{1, 4097}, func(Record[int]) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(s.gets)/float64(b.N), "gets/op")
	b.ReportMetric(4097, "records/op")
}
