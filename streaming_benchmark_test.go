// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"runtime"
	"runtime/metrics"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

// fileStore keeps benchmark payloads off the Go heap as well as production
// scratch data. All files are owned by the benchmark and closed at cleanup.
type fileStore struct {
	mu                    sync.Mutex
	objects               map[string]*tempFile
	gets, creates         int64
	readBytes, writeBytes int64
}

func (s *fileStore) List(context.Context, string, int) ([]string, error) {
	panic("benchmark does not list")
}

func (s *fileStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	s.gets++
	f := s.objects[key]
	s.mu.Unlock()
	if f == nil {
		return nil, ErrNotFound
	}
	return &fileStoreBody{Reader: contextReader{ctx, io.NewSectionReader(f, 0, f.Size())}, store: s}, nil
}

type fileStoreBody struct {
	io.Reader
	store *fileStore
}

func (b *fileStoreBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.store.mu.Lock()
	b.store.readBytes += int64(n)
	b.store.mu.Unlock()
	return n, err
}

func (*fileStoreBody) Close() error { return nil }

func (s *fileStore) Create(ctx context.Context, key string, body SizeReaderAt) error {
	f, err := newTempFile()
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, contextReader{ctx, io.NewSectionReader(body, 0, body.Size())}); err != nil {
		f.Close()
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.creates++
	s.writeBytes += body.Size()
	if s.objects[key] != nil {
		f.Close()
		return ErrExists
	}
	s.objects[key] = f
	return nil
}

func BenchmarkStreamingCompaction(b *testing.B) {
	for _, level := range []uint8{2, 4} {
		for _, entropy := range []bool{false, true} {
			for _, writers := range []int{1, 2} {
				b.Run(fmt.Sprintf("level%d/random=%v/writers=%d", level, entropy, writers), func(b *testing.B) {
					s := &fileStore{objects: make(map[string]*tempFile)}
					b.Cleanup(func() {
						for _, f := range s.objects {
							f.Close()
						}
					})
					lg := testLog[string](b, s)
					ctx := context.Background()
					childSize := int64(1) << (4 * (level - 1))
					// Equal batch sizes isolate the effect of increasing the range size.
					event := json.RawMessage(`["` + strings.Repeat("x", 1024) + `"]`)
					rng := rand.New(rand.NewPCG(1, 2))
					randomBatch := func() json.RawMessage {
						if !entropy {
							return event
						}
						data := make([]byte, 768)
						for i := range data {
							data[i] = byte(rng.Uint32())
						}
						return json.RawMessage(`["` + base64.StdEncoding.EncodeToString(data) + `"]`)
					}
					chain := zeroHash
					var children []objectRef
					for child := int64(0); child < 16; child++ {
						start, end := child*childSize+1, (child+1)*childSize
						ref := objectRef{Level: level - 1, Start: hexRevision(start), End: hexRevision(end), FirstPrevHash: chain}
						f, err := newTempFile()
						if err != nil {
							b.Fatal(err)
						}
						enc, err := zstd.NewWriter(f, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(1<<20))
						if err != nil {
							f.Close()
							b.Fatal(err)
						}
						hash := sha256.New()
						stream := segmentStream{ref, func(yield func(projection) error) error {
							for revision := start; revision <= end; revision++ {
								event := randomBatch()
								id := fmt.Sprintf("%032x", revision)
								nextHash := recordHash(revision, id, chain, event)
								p := projection{hexRevision(revision), id, chain, nextHash, event}
								chain = nextHash
								if err := yield(p); err != nil {
									return err
								}
							}
							return nil
						}}
						if err := jsonv2.MarshalWrite(io.MultiWriter(hash, enc), stream); err != nil {
							enc.Close()
							f.Close()
							b.Fatal(err)
						}
						if err := enc.Close(); err != nil {
							f.Close()
							b.Fatal(err)
						}
						ref.SHA256, ref.LastRecordHash = hex.EncodeToString(hash.Sum(nil)), chain
						ref.Key = lg.treeKey(ref)
						s.objects[ref.Key] = f
						children = append(children, ref)
					}
					scratch := &scratchStats{}
					original := lg.newTemp
					lg.newTemp = func() (*tempFile, error) { return scratch.create(original) }
					runtime.GC()
					var stats runtime.MemStats
					runtime.ReadMemStats(&stats)
					baseline, peak := stats.HeapAlloc, stats.HeapAlloc
					done, stopped := make(chan struct{}), make(chan struct{})
					go func() {
						defer close(stopped)
						ticker := time.NewTicker(5 * time.Millisecond)
						defer ticker.Stop()
						for {
							select {
							case <-done:
								return
							case <-ticker.C:
								runtime.ReadMemStats(&stats)
								peak = max(peak, stats.HeapAlloc)
							}
						}
					}()
					cpuBefore := processCPU()
					b.ResetTimer()
					for range b.N {
						var wg sync.WaitGroup
						refs := make([]objectRef, writers)
						errs := make([]error, writers)
						for i := range writers {
							wg.Go(func() { refs[i], errs[i] = lg.createSegment(ctx, children, nil) })
						}
						wg.Wait()
						if err := errors.Join(errs...); err != nil {
							close(done)
							<-stopped
							b.Fatal(err)
						}
						// Discard only benchmark output between iterations, so every
						// iteration measures a fresh compaction with writers racing to
						// publish the same immutable range. Production never deletes it.
						b.StopTimer()
						output := s.objects[refs[0].Key]
						delete(s.objects, refs[0].Key)
						output.Close()
						b.StartTimer()
					}
					b.StopTimer()
					close(done)
					<-stopped
					b.ReportMetric(float64(peak-baseline), "peak-extra-heap-B")
					b.ReportMetric(float64(childSize*16*int64(len(event))), "payload-B")
					b.ReportMetric(float64(s.gets)/float64(b.N), "gets/op")
					b.ReportMetric(float64(s.creates)/float64(b.N), "creates/op")
					b.ReportMetric(float64(s.readBytes)/float64(b.N), "read-B/op")
					b.ReportMetric(float64(s.writeBytes)/float64(b.N), "write-B/op")
					b.ReportMetric(float64(scratch.peak), "peak-scratch-B")
					b.ReportMetric(float64(scratch.written)/float64(b.N), "scratch-write-B/op")
					b.ReportMetric((processCPU()-cpuBefore)/float64(b.N), "cpu-s/op")
					if scratch.live != 0 {
						b.Fatalf("leaked %d scratch bytes", scratch.live)
					}
				})
			}
		}
	}
}

// processCPU uses the runtime's approximate CPU accounting for application,
// garbage collection, and scavenging work. Setup is excluded from the delta.
func processCPU() float64 {
	samples := []metrics.Sample{
		{Name: "/cpu/classes/user:cpu-seconds"},
		{Name: "/cpu/classes/gc/total:cpu-seconds"},
		{Name: "/cpu/classes/scavenge/total:cpu-seconds"},
	}
	metrics.Read(samples)
	var total float64
	for _, sample := range samples {
		total += sample.Value.Float64()
	}
	return total
}

type scratchStats struct {
	mu                  sync.Mutex
	live, peak, written int64
}

func (s *scratchStats) create(newTemp func() (*tempFile, error)) (*tempFile, error) {
	f, err := newTemp()
	if err != nil {
		return nil, err
	}
	f.file = &scratchHandle{tempHandle: f.file, stats: s}
	return f, nil
}

type scratchHandle struct {
	tempHandle
	stats *scratchStats
	size  int64
}

func (f *scratchHandle) Write(p []byte) (int, error) {
	n, err := f.tempHandle.Write(p)
	f.size += int64(n)
	f.stats.mu.Lock()
	f.stats.live += int64(n)
	f.stats.written += int64(n)
	f.stats.peak = max(f.stats.peak, f.stats.live)
	f.stats.mu.Unlock()
	return n, err
}

func (f *scratchHandle) Close() error {
	err := f.tempHandle.Close()
	f.stats.mu.Lock()
	f.stats.live -= f.size
	f.stats.mu.Unlock()
	return err
}

func BenchmarkCatchUp(b *testing.B) {
	for _, level := range []uint8{2, 4} {
		b.Run(fmt.Sprintf("level%d", level), func(b *testing.B) {
			store := &catchupStore{Store: newStore()}
			fixture := newRangeFixture(b, store, level)
			count := int64(1) << (4 * level)
			head := fixture.snapshot(count + 1)
			root := head.commit.Frontier[0].Refs[0]
			for _, behind := range []int64{1, 2, 3, 17, 257, count + 1} {
				if behind > count+1 || behind == 257 && level == 2 {
					continue
				}
				r := After(count + 1 - behind)
				fixture.prepare(root, r)
				for _, packed := range []bool{false, true} {
					b.Run(fmt.Sprintf("behind%d/packed=%v", behind, packed), func(b *testing.B) {
						store.gets, store.bytes = 0, 0
						b.ReportAllocs()
						b.ResetTimer()
						for range b.N {
							var err error
							if packed && r.First <= count {
								err = fixture.lg.readRecords(b.Context(), root, nil, func(p projection) error {
									revision, _ := parseRevision(p.Revision)
									if revision >= r.First {
										_, err := fixture.lg.record(p)
										return err
									}
									return nil
								})
							} else {
								err = fixture.lg.Scan(b.Context(), head, r, func(Record[int]) error { return nil })
							}
							if err != nil {
								b.Fatal(err)
							}
						}
						b.StopTimer()
						b.ReportMetric(float64(store.gets)/float64(b.N), "gets/op")
						b.ReportMetric(float64(store.bytes)/float64(b.N), "read-B/op")
					})
				}
			}
		})
	}
}
