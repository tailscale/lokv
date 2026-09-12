// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

// fileStore keeps benchmark payloads off the Go heap as well as production
// scratch data. All files are owned by the benchmark and closed at cleanup.
type fileStore struct {
	mu      sync.Mutex
	objects map[string]*tempFile
}

func (s *fileStore) List(context.Context, string, int) ([]string, error) {
	panic("benchmark does not list")
}

func (s *fileStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	f := s.objects[key]
	s.mu.Unlock()
	if f == nil {
		return nil, ErrNotFound
	}
	return io.NopCloser(contextReader{ctx, io.NewSectionReader(f, 0, f.Size())}), nil
}

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
	if s.objects[key] != nil {
		f.Close()
		return ErrExists
	}
	s.objects[key] = f
	return nil
}

func BenchmarkStreamingCompaction(b *testing.B) {
	for _, level := range []uint8{2, 4} {
		b.Run(fmt.Sprintf("level%d", level), func(b *testing.B) {
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
			b.ResetTimer()
			for range b.N {
				if _, err := lg.createSegment(ctx, children, nil); err != nil {
					close(done)
					<-stopped
					b.Fatal(err)
				}
			}
			b.StopTimer()
			close(done)
			<-stopped
			b.ReportMetric(float64(peak-baseline), "peak-extra-heap-B")
			b.ReportMetric(float64(childSize*16*int64(len(event))), "payload-B")
		})
	}
}
