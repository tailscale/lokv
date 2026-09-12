// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"encoding/json"
	"testing"
)

func FuzzKeyParsing(f *testing.F) {
	for _, s := range []string{"v1/log/7ffffffffffffffe.json", "v1/log/7fffffffffffffff.json", "v1/log/7fe0000000000000.json", "v1/log/7fdfffffffffffff.json", "v1/log/0000000000000000.json", "", "v1/log/FFFFFFFFFFFFFFFF.json"} {
		f.Add(s)
	}
	lg := testLog[int](f, newStore())
	f.Fuzz(func(t *testing.T, key string) {
		r, err := lg.parseLogKey(key)
		if err == nil && (r <= 0 || r > MaxRevision || lg.logKey(r) != key) {
			t.Fatal("out-of-range or noncanonical key accepted")
		}
	})
}

func FuzzCommitDecoding(f *testing.F) {
	lg := testLog[int](f, newStore())
	s := build(f, lg, 1)
	f.Add(s.body)
	f.Add([]byte("{}"))
	f.Add([]byte("null"))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 1<<20 {
			return
		}
		_, _ = lg.decodeCommit(lg.logKey(1), b)
	})
}

func FuzzFrontier(f *testing.F) {
	f.Add(int64(-1), []byte("[]"))
	f.Add(int64(0), []byte("[]"))
	f.Add(int64(1), []byte("[]"))
	f.Add(MaxRevision, []byte("[]"))
	f.Add(MaxRevision+1, []byte("[]"))
	f.Add(int64(17), []byte(`[ {"level":1,"refs":[]} ]`))
	lg := testLog[int](f, newStore())
	f.Fuzz(func(t *testing.T, revision int64, b []byte) {
		if len(b) > 1<<20 {
			return
		}
		var frontier []frontierLevel
		if json.Unmarshal(b, &frontier) == nil {
			_ = lg.validateFrontier(frontier, revision, zeroHash)
		}
	})
}

func FuzzIndexDecoding(f *testing.F) {
	f.Add([]byte(`{"format":"lokv/index/v1","level":2,"start":"0000000000000001","end":"0000000000000100","children":[]}`))
	lg := testLog[int](f, newStore())
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 1<<20 {
			return
		}
		ref := objectRef{2, hexRevision(1), hexRevision(256), "", digest(b), zeroHash, zeroHash}
		ref.Key = lg.treeKey(ref)
		_, _ = lg.decodeIndex(ref, b)
	})
}

func FuzzSegmentDecompression(f *testing.F) {
	f.Add(compressTest(f, []byte(`{}`)))
	f.Add([]byte(""))
	f.Add([]byte("not zstd"))
	lg, err := Open[int](Config{Store: newStore(), MaxEventBytes: 1 << 16, MaxObjectBytes: 1 << 20})
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 1<<20 {
			return
		}
		raw, err := lg.decompress(b)
		if err == nil && len(raw) > 1<<20 {
			t.Fatal("limit exceeded")
		}
	})
}
