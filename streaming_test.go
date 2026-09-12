// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
)

func useTempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, env := range []string{"TMPDIR", "TMP", "TEMP"} {
		t.Setenv(env, dir)
	}
	return dir
}

func checkTempCleanup(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("temporary files remain: %v, %v", entries, err)
	}
	if runtime.GOOS == "linux" {
		fds, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		for _, fd := range fds {
			name, _ := os.Readlink(filepath.Join("/proc/self/fd", fd.Name()))
			if strings.HasPrefix(name, dir+string(os.PathSeparator)) {
				t.Fatalf("temporary file descriptor leaked: %s", name)
			}
		}
	}
}

func TestTempFile(t *testing.T) {
	dir := useTempDir(t)
	f, err := newTempFile()
	if err != nil {
		t.Fatal(err)
	}
	name := f.file.Name()
	_, statErr := os.Stat(name)
	if runtime.GOOS == "windows" {
		if statErr != nil {
			t.Fatal(statErr)
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("temporary file was not unlinked: %v", statErr)
	}
	if _, err := io.WriteString(f, "temporary contents"); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		b, err := io.ReadAll(io.NewSectionReader(f, 0, f.Size()))
		if err != nil || string(b) != "temporary contents" {
			t.Fatalf("independent read: %q, %v", b, err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ReadAt(make([]byte, 1), 0); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("descriptor remains open: %v", err)
	}
	checkTempCleanup(t, dir)
}

func TestStreamingWireCompatibility(t *testing.T) {
	dir := useTempDir(t)
	s := newStore()
	lg := testLog[json.RawMessage](t, s)
	// These spellings must survive v2 streaming APIs unchanged, including raw
	// escapes, number spellings, object order, and duplicate names in payloads.
	value := json.RawMessage(`{"z":"\u0061\u003c\u2028","a":1e+00,"a":-0}`)
	var snap *Snapshot[json.RawMessage]
	var records []projection
	for range 17 {
		var err error
		snap, err = lg.AppendTo(context.Background(), snap, value)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, snap.commit.project())
	}
	ref := snap.commit.Frontier[0].Refs[0]
	want, err := json.Marshal(segment{segmentFormat, ref.Level, ref.Start, ref.End, records[:16]})
	if err != nil {
		t.Fatal(err)
	}
	got, err := lg.decompressBytes(s.objects[ref.Key])
	if err != nil || !bytes.Equal(got, want) || ref.SHA256 != digest(want) {
		t.Fatalf("streaming changed canonical segment bytes or hash: %v", err)
	}
	if err := lg.Scan(context.Background(), snap, All(), func(r Record[json.RawMessage]) error {
		if len(r.Value) != 1 || !bytes.Equal(r.Value[0], value) {
			t.Fatal("streaming changed payload bytes")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	checkTempCleanup(t, dir)
}

func TestStreamingValidationBeforeYield(t *testing.T) {
	dir := useTempDir(t)
	s := newStore()
	lg := testLog[int](t, s)
	snap := build(t, lg, 17)
	ref := snap.commit.Frontier[0].Refs[0]
	original := bytes.Clone(s.objects[ref.Key])
	raw, err := lg.decompressBytes(original)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"digest", "last-record", "checksum", "trailing-frame", "missing-field"} {
		t.Run(name, func(t *testing.T) {
			bad := ref
			body := bytes.Clone(original)
			switch name {
			case "digest":
				bad.SHA256 = zeroHash
			case "last-record":
				var seg segment
				if err := json.Unmarshal(raw, &seg); err != nil {
					t.Fatal(err)
				}
				seg.Records[15].RecordHash = zeroHash
				changed, _ := json.Marshal(seg)
				bad.SHA256 = digest(changed)
				body = compressTest(t, changed)
			case "checksum":
				body[len(body)-1] ^= 1
			case "trailing-frame":
				body = append(body, original...)
			case "missing-field":
				changed := bytes.Replace(raw, []byte(`"level":1,`), nil, 1)
				bad.SHA256 = digest(changed)
				body = compressTest(t, changed)
			}
			bad.Key = lg.treeKey(bad)
			s.objects[bad.Key] = body
			calls := 0
			if err := lg.readRecords(context.Background(), bad, nil, func(projection) error {
				calls++
				return nil
			}); !errors.Is(err, ErrCorrupt) || calls != 0 {
				t.Fatalf("corrupt segment yielded %d records: %v", calls, err)
			}
			checkTempCleanup(t, dir)
		})
	}
	s.objects[ref.Key] = original
	stop := errors.New("stop")
	if err := lg.Scan(context.Background(), snap, All(), func(Record[int]) error { return stop }); !errors.Is(err, stop) {
		t.Fatal(err)
	}
	checkTempCleanup(t, dir)
}

type faultBody struct {
	io.Reader
	readErr  error
	closeErr error
	closed   bool
	cancel   context.CancelFunc
}

func (b *faultBody) Read(p []byte) (int, error) {
	if b.cancel != nil {
		b.cancel()
	}
	if b.readErr != nil {
		return 0, b.readErr
	}
	return b.Reader.Read(p)
}

func (b *faultBody) Close() error { b.closed = true; return b.closeErr }

type bodyStore struct {
	Store
	body *faultBody
}

func (s bodyStore) Get(context.Context, string) (io.ReadCloser, error) { return s.body, nil }

func TestStreamingResponseCleanup(t *testing.T) {
	dir := useTempDir(t)
	failure := errors.New("response failure")
	for _, kind := range []string{"read", "close", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			body := &faultBody{Reader: strings.NewReader("contents")}
			want := failure
			switch kind {
			case "read":
				body.readErr = failure
			case "close":
				body.closeErr = failure
			case "cancel":
				body.cancel, want = cancel, context.Canceled
			}
			lg := testLog[int](t, bodyStore{newStore(), body})
			if _, err := lg.download(ctx, objectRef{Key: "object"}); !errors.Is(err, want) || !body.closed {
				t.Fatalf("download error %v, body closed %v", err, body.closed)
			}
			checkTempCleanup(t, dir)
		})
	}
}

func TestCompactionTempCleanup(t *testing.T) {
	dir := useTempDir(t)
	s := newStore()
	lg := testLog[int](t, s)
	base := build(t, lg, 256)
	checkTempCleanup(t, dir)
	failure := errors.New("upload failure")
	s.before = func(key string, _ []byte) error {
		if strings.Contains(key, "/tree/2/") {
			return failure
		}
		return nil
	}
	if _, err := lg.AppendTo(context.Background(), base, 256); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	checkTempCleanup(t, dir)
	s.before = nil
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.before = func(string, []byte) error { cancel(); return ctx.Err() }
	if _, err := lg.AppendTo(ctx, base, 256); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	checkTempCleanup(t, dir)
}

type faultyTemp struct {
	tempHandle
	remaining int
	writeErr  error
	closeErr  error
}

func (f *faultyTemp) Write(p []byte) (int, error) {
	if f.writeErr == nil {
		return f.tempHandle.Write(p)
	}
	n, err := f.tempHandle.Write(p[:min(len(p), f.remaining)])
	f.remaining -= n
	if err != nil {
		return n, err
	}
	if n < len(p) {
		return n, f.writeErr
	}
	return n, nil
}

func (f *faultyTemp) Close() error {
	return errors.Join(f.tempHandle.Close(), f.closeErr)
}

func TestCompactionDiskFailures(t *testing.T) {
	for _, kind := range []string{"create", "output write", "spool write", "output close", "spool close"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			store := newStore()
			lg, err := Open[int](Config{Store: store, TempDir: dir})
			if err != nil {
				t.Fatal(err)
			}
			base := build(t, lg, 16)
			failure := &os.PathError{Op: "write", Path: dir, Err: syscall.ENOSPC}
			original := lg.newTemp
			var created atomic.Int32
			lg.newTemp = func() (*tempFile, error) {
				n := created.Add(1)
				if kind == "create" {
					return nil, failure
				}
				f, err := original()
				if err != nil {
					return nil, err
				}
				// The output is created first; the following four are the
				// first group of validated child spools, in worker order.
				if strings.HasPrefix(kind, "output") && n == 1 || strings.HasPrefix(kind, "spool") && n == 2 {
					fault := &faultyTemp{tempHandle: f.file, remaining: 8}
					if strings.HasSuffix(kind, "write") {
						fault.writeErr = failure
					} else {
						fault.closeErr = failure
					}
					f.file = fault
				}
				return f, nil
			}
			if _, err := lg.AppendTo(t.Context(), base, 16); !errors.Is(err, failure) {
				t.Fatalf("append did not report disk failure: %v", err)
			}
			if _, ok := store.objects[lg.logKey(17)]; ok {
				t.Fatal("published commit after temporary storage failure")
			}
			checkTempCleanup(t, dir)
			lg.newTemp = original
			snap, err := lg.AppendTo(t.Context(), base, 16)
			if err != nil || snap.Revision() != 17 {
				t.Fatalf("retry: %v, %v", snap, err)
			}
			if err := lg.Verify(t.Context(), snap); err != nil {
				t.Fatal(err)
			}
			checkTempCleanup(t, dir)
		})
	}
}

func TestScanDiskFailures(t *testing.T) {
	for _, phase := range []string{"download write", "download close", "spool write", "spool close"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			store := newStore()
			lg, err := Open[int](Config{Store: store, TempDir: dir})
			if err != nil {
				t.Fatal(err)
			}
			snap := build(t, lg, 17)
			failure := errors.New("temporary disk failure")
			original := lg.newTemp
			created := 0
			lg.newTemp = func() (*tempFile, error) {
				created++
				f, err := original()
				if err != nil {
					return nil, err
				}
				if strings.HasPrefix(phase, "spool") && created == 1 || strings.HasPrefix(phase, "download") && created == 2 {
					fault := &faultyTemp{tempHandle: f.file, remaining: 8}
					if strings.HasSuffix(phase, "write") {
						fault.writeErr = failure
					} else {
						fault.closeErr = failure
					}
					f.file = fault
				}
				return f, nil
			}
			calls := 0
			err = lg.Scan(t.Context(), snap, All(), func(Record[int]) error { calls++; return nil })
			if !errors.Is(err, failure) || phase != "spool close" && calls != 0 {
				t.Fatalf("scan: calls=%d, err=%v", calls, err)
			}
			checkTempCleanup(t, dir)
		})
	}
}

func TestTempDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "not-created-yet")
	store := newStore()
	lg, err := Open[int](Config{Store: store, TempDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	base := build(t, lg, 16)
	if _, err := lg.AppendTo(t.Context(), base, 16); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing scratch directory: %v", err)
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := lg.AppendTo(t.Context(), base, 16); err != nil {
		t.Fatal(err)
	}
	checkTempCleanup(t, dir)
}

func TestCompactionScratchBound(t *testing.T) {
	store := newStore()
	fixture := newRangeFixture(t, store, 2)
	var children []objectRef
	for start := int64(1); start <= 256; start += 16 {
		children = append(children, fixture.ref(start, 1))
	}
	var expanded int64
	for _, p := range fixture.records[:256] {
		body, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		expanded += int64(len(body) + 1)
	}
	stats := &scratchStats{}
	original := fixture.lg.newTemp
	fixture.lg.newTemp = func() (*tempFile, error) { return stats.create(original) }
	if _, err := fixture.lg.createSegment(t.Context(), children, nil); err != nil {
		t.Fatal(err)
	}
	if stats.live != 0 || stats.peak >= expanded*3/4 {
		t.Fatalf("scratch live=%d peak=%d; expanded range=%d", stats.live, stats.peak, expanded)
	}
}
