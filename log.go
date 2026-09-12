// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"
)

const safetyCeiling int64 = 256 << 20

// Config selects the immutable namespace and resource limits. Zero limits use
// defaults. Prefix is normalized by stripping leading and trailing slashes.
type Config struct {
	Prefix             string
	Store              Store
	MaxConflictRetries int   // Default 32; negative values are invalid. AppendTo never retries conflicts.
	MaxEventBytes      int64 // Default 1 MiB of JSON.
	MaxObjectBytes     int64 // Default 64 MiB, both stored and decompressed; at most 256 MiB.
}

// Log is a concurrency-safe handle to one namespace. Configuration is immutable.
type Log[T any] struct {
	store               Store
	prefix              string
	maxRetries          int
	maxEvent, maxObject int64
}

// Open validates configuration without I/O. The caller must ensure Store obeys
// the strong-consistency, ordering, immutability, and atomic-create contract.
func Open[T any](cfg Config) (*Log[T], error) {
	if cfg.Store == nil {
		return nil, errors.New("lokv: nil store")
	}
	v := reflect.ValueOf(cfg.Store)
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Interface, reflect.Chan:
		if v.IsNil() {
			return nil, errors.New("lokv: nil store")
		}
	}
	if cfg.MaxConflictRetries < 0 || cfg.MaxEventBytes < 0 || cfg.MaxObjectBytes < 0 {
		return nil, errors.New("lokv: negative configuration limit")
	}
	if cfg.MaxConflictRetries == 0 {
		cfg.MaxConflictRetries = 32
	}
	if cfg.MaxEventBytes == 0 {
		cfg.MaxEventBytes = 1 << 20
	}
	if cfg.MaxObjectBytes == 0 {
		cfg.MaxObjectBytes = 64 << 20
	}
	if cfg.MaxEventBytes > cfg.MaxObjectBytes || cfg.MaxObjectBytes > safetyCeiling {
		return nil, errors.New("lokv: invalid size limits")
	}
	p := strings.Trim(cfg.Prefix, "/")
	if !utf8.ValidString(p) || strings.ContainsAny(p, "\\") || strings.ContainsFunc(p, unicode.IsControl) {
		return nil, errors.New("lokv: invalid prefix")
	}
	if p != "" {
		for _, part := range strings.Split(p, "/") {
			if part == "" || part == "." || part == ".." {
				return nil, errors.New("lokv: invalid prefix component")
			}
		}
		p += "/"
	}
	return &Log[T]{cfg.Store, p, cfg.MaxConflictRetries, cfg.MaxEventBytes, cfg.MaxObjectBytes}, nil
}

// CommitID identifies an append invocation. It contains 16 cryptographically
// random bytes generated once per Append or AppendTo call and reused on retries.
type CommitID [16]byte

// RecordHash is a record's logical SHA-256 hash. It covers the record's revision,
// commit ID, predecessor's hash, and JSON-encoded event, linking it to its history.
type RecordHash [32]byte

// Record is a decoded log event and its identity.
type Record[T any] struct {
	// Revision is the record's sequence number, starting at 1. Values <= 0 are invalid.
	Revision   int64
	CommitID   CommitID
	Value      T
	RecordHash RecordHash
}

// Preserve errors.Is/As without exposing a custom codec's payload-bearing text.
type eventCodecError struct {
	op  string
	err error
}

func (e *eventCodecError) Error() string { return "lokv: cannot " + e.op + " event" }
func (e *eventCodecError) Unwrap() error { return e.err }

// Snapshot is a loaded historical root. A nil pointer represents an empty log.
// Its private wire bytes remain independent of mutations to values returned by
// Record. Snapshots may be used with the Log that loaded or appended them.
type Snapshot[T any] struct {
	owner  *Log[T]
	commit *commit
	body   []byte
	record Record[T]
}

// Record returns the root's event, or a zero record for an empty snapshot.
// As with ordinary Go values, maps, slices and pointers in Value are shallow
// copies. Mutating them cannot change the stored event or future appends/scans.
func (s *Snapshot[T]) Record() Record[T] {
	if s == nil {
		return Record[T]{}
	}
	return s.record
}

// Revision returns the root's positive revision, starting at 1. It returns zero
// for an empty snapshot; zero is never a valid record revision.
func (s *Snapshot[T]) Revision() int64 {
	if s == nil {
		return 0
	}
	return s.record.Revision
}

// Empty reports whether the snapshot represents an empty log.
func (s *Snapshot[T]) Empty() (empty bool) { return s == nil }

func (l *Log[T]) record(p projection) (Record[T], error) {
	var r Record[T]
	revision, err := l.validateProjection(p)
	if err != nil {
		return r, err
	}
	r.Revision = revision
	id, _ := hex.DecodeString(p.CommitID)
	copy(r.CommitID[:], id)
	h, _ := hex.DecodeString(p.RecordHash)
	copy(r.RecordHash[:], h)
	if err := json.Unmarshal(p.Event, &r.Value); err != nil {
		return Record[T]{}, fmt.Errorf("%w: %w", ErrCorrupt, &eventCodecError{"unmarshal", err})
	}
	return r, nil
}

func (l *Log[T]) snapshot(key string, body []byte) (*Snapshot[T], error) {
	c, err := l.decodeCommit(key, body)
	if err != nil {
		return nil, fmt.Errorf("load %s: %w", key, err)
	}
	r, err := l.record(c.project())
	if err != nil {
		return nil, fmt.Errorf("load %s: %w", key, err)
	}
	return &Snapshot[T]{l, c, body, r}, nil
}

func (l *Log[T]) checkSnapshot(s *Snapshot[T]) error {
	if s != nil && (s.owner != l || s.commit == nil) {
		return errors.New("lokv: snapshot belongs to a different log or is uninitialized")
	}
	if s != nil && s.Revision() <= 0 {
		return corrupt("snapshot revision must be positive")
	}
	return nil
}

func (l *Log[T]) get(ctx context.Context, key string, referenced bool) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b, err := l.store.Get(ctx, key)
	if err != nil {
		if referenced && (errors.Is(err, ErrNotFound) || errors.Is(err, ErrTooLarge)) {
			err = fmt.Errorf("%w: %w", ErrCorrupt, err)
		}
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	if int64(len(b)) > l.maxObject {
		return nil, fmt.Errorf("get %s: %w: %w", key, ErrCorrupt, ErrTooLarge)
	}
	return b, nil
}

// LoadHead discovers the root using exactly one List with limit 1 and one Get.
// An empty namespace returns nil. A malformed first key is never skipped.
func (l *Log[T]) LoadHead(ctx context.Context) (*Snapshot[T], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	keys, err := l.store.List(ctx, l.logPrefix(), 1)
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", l.logPrefix(), err)
	}
	if len(keys) == 0 {
		return nil, nil
	}
	if len(keys) != 1 {
		return nil, corrupt("store exceeded LIST limit")
	}
	if _, err := l.parseLogKey(keys[0]); err != nil {
		return nil, err
	}
	b, err := l.get(ctx, keys[0], true)
	if err != nil {
		return nil, err
	}
	return l.snapshot(keys[0], b)
}

// LoadRevision loads a historical root without listing. An absent requested
// revision returns ErrNotFound; missing dependencies encountered later are corrupt.
// Revisions start at 1; a revision <= 0 returns ErrRange without store I/O.
func (l *Log[T]) LoadRevision(ctx context.Context, revision int64) (*Snapshot[T], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if revision <= 0 {
		return nil, ErrRange
	}
	key := l.logKey(revision)
	b, err := l.get(ctx, key, false)
	if err != nil {
		return nil, err
	}
	return l.snapshot(key, b)
}

// Head returns the latest event. ok reports whether a record was returned.
// An empty log returns (zero, false, nil).
func (l *Log[T]) Head(ctx context.Context) (_ Record[T], ok bool, _ error) {
	s, err := l.LoadHead(ctx)
	if err != nil || s == nil {
		return Record[T]{}, false, err
	}
	return s.Record(), true, nil
}

func (l *Log[T]) prepare(value T) (json.RawMessage, string, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return nil, "", &eventCodecError{"marshal", err}
	}
	if int64(len(b)) > l.maxEvent {
		return nil, "", ErrTooLarge
	}
	var id CommitID
	if _, err := rand.Read(id[:]); err != nil {
		return nil, "", fmt.Errorf("lokv: generate commit ID: %w", err)
	}
	return b, hex.EncodeToString(id[:]), nil
}

// Append marshals once and retries optimistic conflicts with the same commit ID.
// The first record has revision 1. Appending after math.MaxInt64 returns ErrExhausted.
// Transport retries belong to the adapter. An unresolved transport error is
// returned, since the core cannot classify arbitrary backend errors as retryable.
func (l *Log[T]) Append(ctx context.Context, value T) (Record[T], error) {
	if err := ctx.Err(); err != nil {
		return Record[T]{}, err
	}
	event, id, err := l.prepare(value)
	if err != nil {
		return Record[T]{}, err
	}
	for attempt := 0; ; attempt++ {
		base, err := l.LoadHead(ctx)
		if err != nil {
			return Record[T]{}, err
		}
		s, err := l.appendPrepared(ctx, base, event, id)
		if err == nil {
			return s.Record(), nil
		}
		if !errors.Is(err, ErrConflict) || attempt >= l.maxRetries {
			return Record[T]{}, err
		}
	}
}

// AppendTo attempts exactly one successor of base. A nil base means the empty
// root, whose successor has revision 1. It does not list, and returns ErrConflict
// when another writer wins or ErrExhausted when base is already at math.MaxInt64.
//
// Use AppendTo when the value depends on a snapshot you read: a conflict lets you
// reload and recompute it. Append retries the same value automatically. Reusing
// a snapshot also saves the head lookup required by Append.
func (l *Log[T]) AppendTo(ctx context.Context, base *Snapshot[T], value T) (*Snapshot[T], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := l.checkSnapshot(base); err != nil {
		return nil, err
	}
	if base != nil && base.Revision() == math.MaxInt64 {
		return nil, ErrExhausted
	}
	event, id, err := l.prepare(value)
	if err != nil {
		return nil, err
	}
	return l.appendPrepared(ctx, base, event, id)
}

func (l *Log[T]) appendPrepared(ctx context.Context, base *Snapshot[T], event json.RawMessage, id string) (*Snapshot[T], error) {
	if err := l.checkSnapshot(base); err != nil {
		return nil, err
	}
	revision := int64(1)
	var prev *previous
	frontier := make([]frontierLevel, 0)
	if base != nil {
		if base.Revision() == math.MaxInt64 {
			return nil, ErrExhausted
		}
		revision = base.Revision() + 1
		prev = &previous{l.logKey(base.Revision()), base.commit.RecordHash}
		var err error
		frontier, err = l.carry(ctx, base)
		if err != nil {
			return nil, err
		}
	}
	prevHash := zeroHash
	if prev != nil {
		prevHash = prev.RecordHash
	}
	if err := l.validateFrontier(frontier, revision, prevHash); err != nil {
		return nil, err
	}
	c := commit{commitFormat, hexRevision(revision), id, prev, event, recordHash(revision, id, prevHash, event), frontier}
	body, err := json.Marshal(c)
	if err != nil {
		return nil, errors.New("lokv: encode commit")
	}
	if int64(len(body)) > l.maxObject {
		return nil, ErrTooLarge
	}
	key := l.logKey(revision)
	// Decode before publishing: a T with an incompatible UnmarshalJSON must not
	// cause an append to commit and only then report a decoding failure.
	s, err := l.snapshot(key, body)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	createErr := l.store.Create(ctx, key, body)
	if createErr == nil {
		return s, nil
	}
	// Both ErrExists and ambiguous errors can represent an earlier successful
	// adapter retry. Always inspect the attempted key before choosing a new one.
	actual, err := l.get(ctx, key, false)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			if errors.Is(createErr, ErrExists) {
				return nil, corrupt("existing commit is not visible")
			}
			return nil, fmt.Errorf("create %s: %w", key, createErr)
		}
		return nil, err
	}
	winner, err := l.snapshot(key, actual)
	if err != nil {
		return nil, err
	}
	if winner.commit.CommitID == id {
		if winner.commit.RecordHash != c.RecordHash {
			return nil, corrupt("commit ID reused with different record")
		}
		return winner, nil
	}
	return nil, fmt.Errorf("revision %d: %w", revision, ErrConflict)
}
