// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package diskstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tailscale/lokv"
	"github.com/tailscale/lokv/storetest"
)

func openStore(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestConformance(t *testing.T) {
	dir := t.TempDir()
	// Route every operation through alternating, independent instances. This
	// exercises filesystem coordination without any shared in-memory lock.
	s := &alternatingStore{stores: [2]*Store{openStore(t, dir), openStore(t, dir)}}
	storetest.Test(t, s, "conformance/")
	checkNoTemps(t, dir)
}

type alternatingStore struct {
	stores [2]*Store
	next   atomic.Uint64
}

func (s *alternatingStore) pick() *Store { return s.stores[s.next.Add(1)%2] }

func (s *alternatingStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.pick().Get(ctx, key)
}

func (s *alternatingStore) List(ctx context.Context, prefix string, limit int) ([]string, error) {
	return s.pick().List(ctx, prefix, limit)
}

func (s *alternatingStore) Create(ctx context.Context, key string, value lokv.SizeReaderAt) error {
	return s.pick().Create(ctx, key, value)
}

func TestKeysAndReopen(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	s := openStore(t, dir)
	keys := []string{"", "/", "../escape", "a/b", "a\\b", "a", "A", ".", "..", "!", "nul", "CON", "日本語", "\xff\x00"}
	for _, n := range []int{63, 64, 65, 127, 128, 129, 1024} {
		key := strings.Repeat("x", n)
		keys = append(keys, key, key+"\x00", key+"y", key+"z")
	}
	for i := range 256 {
		keys = append(keys, "bytes/"+string([]byte{byte(i)}))
	}
	for i, key := range keys {
		if err := s.Create(ctx, key, strings.NewReader(fmt.Sprint(i))); err != nil {
			t.Fatalf("Create(%q): %v", key, err)
		}
	}
	s = openStore(t, dir)
	for i, key := range keys {
		body, err := s.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(body)
		if err := errors.Join(err, body.Close()); err != nil {
			t.Fatal(err)
		}
		if string(got) != fmt.Sprint(i) {
			t.Fatalf("Get(%q) = %q, want %d", key, got, i)
		}
	}
	slices.Sort(keys)
	prefixes := []string{"", "a", "a/", "../", "\xff", "missing", "bytes/", "bytes/\xff", "日"}
	for _, n := range []int{62, 63, 64, 65, 127, 128, 129, 130} {
		prefixes = append(prefixes, strings.Repeat("x", n))
	}
	for _, prefix := range prefixes {
		var matching []string
		for _, key := range keys {
			if strings.HasPrefix(key, prefix) {
				matching = append(matching, key)
			}
		}
		for _, limit := range []int{1, 2, 10, 1000} {
			got, err := s.List(ctx, prefix, limit)
			want := matching[:min(limit, len(matching))]
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("List(%q, %d) = %q, %v; want %q", prefix, limit, got, err, want)
			}
		}
	}
	checkNoTemps(t, dir)
}

func checkNoTemps(t *testing.T, dir string) {
	t.Helper()
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err == nil && strings.HasPrefix(entry.Name(), ".tmp-") {
			t.Errorf("temporary file left behind: %s", path)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPublicationAndCancellation(t *testing.T) {
	dir := t.TempDir()
	writer, observer := openStore(t, dir), openStore(t, dir)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started, resume := make(chan struct{}), make(chan struct{})
	defer close(resume)
	source := &blockedSource{started: started, resume: resume}
	result := make(chan error, 1)
	go func() { result <- writer.Create(ctx, "pending", source) }()
	<-started
	if body, err := observer.Get(ctx, "pending"); !errors.Is(err, lokv.ErrNotFound) {
		if body != nil {
			body.Close()
		}
		t.Fatalf("unpublished Get = %v", err)
	}
	if keys, err := observer.List(ctx, "", 10); err != nil || len(keys) != 0 {
		t.Fatalf("unpublished List = %q, %v", keys, err)
	}
	if err := observer.Create(ctx, "pending", strings.NewReader("winner")); err != nil {
		t.Fatal(err)
	}
	cancel()
	resume <- struct{}{}
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Create after cancellation = %v", err)
	}
	body, err := observer.Get(t.Context(), "pending")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(body)
	if err := errors.Join(err, body.Close()); err != nil || string(got) != "winner" {
		t.Fatalf("winner = %q, %v", got, err)
	}
	checkNoTemps(t, dir)
}

type blockedSource struct {
	started chan struct{}
	resume  chan struct{}
}

func (*blockedSource) Size() int64 { return 4 }

func (s *blockedSource) ReadAt(p []byte, off int64) (int, error) {
	close(s.started)
	<-s.resume
	return strings.NewReader("body").ReadAt(p, off)
}

func TestInvalidConfigAndInput(t *testing.T) {
	if _, err := New(""); err == nil {
		t.Fatal("accepted empty directory")
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(file); err == nil {
		t.Fatal("accepted file as directory")
	}
	s := openStore(t, t.TempDir())
	if _, err := s.List(t.Context(), "", 0); err == nil {
		t.Fatal("accepted zero limit")
	}
	if err := s.Create(t.Context(), "nil", nil); err == nil {
		t.Fatal("accepted nil body")
	}
}
