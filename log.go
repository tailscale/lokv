// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"
)

// MaxRevision is the maximum revision and maximum number of batch records in a log.
// It equals JavaScript's Number.MAX_SAFE_INTEGER (9007199254740991), so all valid
// revisions can pass through JavaScript Numbers without losing precision.
// Appending to a log at MaxRevision returns ErrExhausted.
const MaxRevision int64 = 1<<53 - 1

// Config selects the immutable namespace and resource limits. Zero limits use
// defaults. Prefix is normalized by stripping leading and trailing slashes.
type Config struct {
	Prefix             string
	Store              Store
	MaxConflictRetries int // Default 32; negative values are invalid. AppendTo never retries conflicts.

	// MaxEventBytes limits the complete JSON array of a new append batch.
	// Zero defaults to 1 MiB. It does not limit reads or compaction of stored data.
	MaxEventBytes int64
}

// Log is a concurrency-safe handle to one namespace. Configuration is immutable.
type Log[T any] struct {
	store      Store
	prefix     string
	maxRetries int
	maxEvent   int64
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
	if cfg.MaxConflictRetries < 0 || cfg.MaxEventBytes < 0 {
		return nil, errors.New("lokv: negative configuration limit")
	}
	if cfg.MaxConflictRetries == 0 {
		cfg.MaxConflictRetries = 32
	}
	if cfg.MaxEventBytes == 0 {
		cfg.MaxEventBytes = 1 << 20
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
	return &Log[T]{cfg.Store, p, cfg.MaxConflictRetries, cfg.MaxEventBytes}, nil
}

// CommitID identifies an append invocation. It contains 16 cryptographically
// random bytes generated once per Append or AppendTo call and reused on retries.
type CommitID [16]byte

// RecordHash is a record's logical SHA-256 hash. It covers the record's revision,
// commit ID, predecessor's hash, and JSON-encoded batch, linking it to its history.
type RecordHash [32]byte

// Record is an atomically appended batch and its identity. Each successful append
// creates one record and consumes one revision, regardless of the batch's size.
type Record[T any] struct {
	// Revision is the record's sequence number, from 1 through MaxRevision.
	// Values <= 0 or > MaxRevision are invalid.
	Revision   int64
	CommitID   CommitID
	Value      []T // Nonempty, in append argument order.
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

// Record returns the root's batch, or a zero record for an empty snapshot.
// Value and any maps, slices, or pointers within it are shallow copies. Mutating
// them cannot change the stored batch or future appends/scans.
func (s *Snapshot[T]) Record() Record[T] {
	if s == nil {
		return Record[T]{}
	}
	return s.record
}

// Revision returns the root's revision, from 1 through [MaxRevision]. It returns
// zero for an empty snapshot; zero is never a valid record revision.
func (s *Snapshot[T]) Revision() int64 {
	if s == nil {
		return 0
	}
	return s.record.Revision
}

// Empty reports whether the snapshot represents an empty log.
func (s *Snapshot[T]) Empty() (empty bool) { return s == nil }

func (lg *Log[T]) record(p projection) (Record[T], error) {
	var r Record[T]
	revision, err := lg.validateProjection(p)
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

func (lg *Log[T]) snapshot(key string, body []byte) (*Snapshot[T], error) {
	c, err := lg.decodeCommit(key, body)
	if err != nil {
		return nil, fmt.Errorf("load %s: %w", key, err)
	}
	r, err := lg.record(c.project())
	if err != nil {
		return nil, fmt.Errorf("load %s: %w", key, err)
	}
	return &Snapshot[T]{lg, c, body, r}, nil
}

func (lg *Log[T]) checkSnapshot(s *Snapshot[T]) error {
	if s != nil && (s.owner != lg || s.commit == nil) {
		return errors.New("lokv: snapshot belongs to a different log or is uninitialized")
	}
	if s != nil && (s.Revision() <= 0 || s.Revision() > MaxRevision) {
		return corrupt("snapshot revision out of range")
	}
	return nil
}

func (lg *Log[T]) openObject(ctx context.Context, key string, referenced bool) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b, err := lg.store.Get(ctx, key)
	if err != nil {
		if referenced && errors.Is(err, ErrNotFound) {
			err = fmt.Errorf("%w: %w", ErrCorrupt, err)
		}
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	return b, nil
}

// get buffers only a commit, which contains one batch and its frontier.
func (lg *Log[T]) get(ctx context.Context, key string, referenced bool) ([]byte, error) {
	body, err := lg.openObject(ctx, key, referenced)
	if err != nil {
		return nil, err
	}
	b, readErr := io.ReadAll(contextReader{ctx, body})
	if err := errors.Join(readErr, body.Close(), ctx.Err()); err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	return b, nil
}

// LoadHead discovers the root using one List with limit 1, followed by one Get
// for a nonempty namespace. An empty namespace returns nil without a Get.
// A malformed first key is never skipped.
//
// To build an in-memory index, scan the returned snapshot with [All]. On later
// polls, use [After] with the last successfully applied revision to scan through
// the new snapshot's revision. Skip the scan if no revisions were added. See the
// follow example for [Log.Scan].
//
// Every nonempty LoadHead call fetches the root, even if it has not changed.
// Log does not provide notifications; callers arrange polling or wakeups. A
// caller that already knows a committed revision can use [Log.LoadRevision]
// to load that root with one Get and no List.
func (lg *Log[T]) LoadHead(ctx context.Context) (*Snapshot[T], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	keys, err := lg.store.List(ctx, lg.logPrefix(), 1)
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", lg.logPrefix(), err)
	}
	if len(keys) == 0 {
		return nil, nil
	}
	if len(keys) != 1 {
		return nil, corrupt("store exceeded LIST limit")
	}
	if _, err := lg.parseLogKey(keys[0]); err != nil {
		return nil, err
	}
	b, err := lg.get(ctx, keys[0], true)
	if err != nil {
		return nil, err
	}
	return lg.snapshot(keys[0], b)
}

// LoadRevision loads a historical root without listing. An absent requested
// revision returns ErrNotFound; missing dependencies encountered later are corrupt.
// A revision outside [1, MaxRevision] returns ErrRange without store I/O.
//
// For frequent idle polling, probing the last applied revision plus 1 costs one
// Get. ErrNotFound means no successor was visible at that lookup. Check for
// [MaxRevision] before incrementing. If a successor exists, LoadHead can discover
// the latest root for a batch catch-up, at the cost of an extra probe Get on
// active polls. See the follow example for [Log.Scan].
func (lg *Log[T]) LoadRevision(ctx context.Context, revision int64) (*Snapshot[T], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if revision <= 0 || revision > MaxRevision {
		return nil, ErrRange
	}
	key := lg.logKey(revision)
	b, err := lg.get(ctx, key, false)
	if err != nil {
		return nil, err
	}
	return lg.snapshot(key, b)
}

// Head returns the latest batch. ok reports whether a record was returned.
// An empty log returns (zero, false, nil).
func (lg *Log[T]) Head(ctx context.Context) (_ Record[T], ok bool, _ error) {
	s, err := lg.LoadHead(ctx)
	if err != nil || s == nil {
		return Record[T]{}, false, err
	}
	return s.Record(), true, nil
}

func (lg *Log[T]) prepare(value []T) (json.RawMessage, string, error) {
	if len(value) == 0 {
		return nil, "", ErrEmptyBatch
	}
	// Marshal each item separately so even Log[byte] stores an array instead of
	// encoding/json's special base64 representation for []byte.
	b := []byte{'['}
	for i, item := range value {
		encoded, err := json.Marshal(item)
		if err != nil {
			return nil, "", &eventCodecError{"marshal", err}
		}
		if i != 0 {
			b = append(b, ',')
		}
		if int64(len(b))+int64(len(encoded))+1 > lg.maxEvent {
			return nil, "", ErrTooLarge
		}
		b = append(b, encoded...)
	}
	b = append(b, ']')
	var id CommitID
	if _, err := rand.Read(id[:]); err != nil {
		return nil, "", fmt.Errorf("lokv: generate commit ID: %w", err)
	}
	return b, hex.EncodeToString(id[:]), nil
}

// Append atomically appends value as one batch, preserving argument order. It
// marshals each item once and retries conflicts with the same batch and commit ID.
// An empty batch returns ErrEmptyBatch without store I/O. MaxEventBytes limits
// the complete JSON array. An ordinary append creates one object for the batch;
// a radix carry may create additional aggregate objects for preceding records.
// The first record has revision 1. Appending after [MaxRevision] returns ErrExhausted.
// Transport retries belong to the adapter. An unresolved transport error is
// returned, since the core cannot classify arbitrary backend errors as retryable.
func (lg *Log[T]) Append(ctx context.Context, value ...T) (Record[T], error) {
	if err := ctx.Err(); err != nil {
		return Record[T]{}, err
	}
	event, id, err := lg.prepare(value)
	if err != nil {
		return Record[T]{}, err
	}
	for attempt := 0; ; attempt++ {
		base, err := lg.LoadHead(ctx)
		if err != nil {
			return Record[T]{}, err
		}
		s, err := lg.appendPrepared(ctx, base, event, id)
		if err == nil {
			return s.Record(), nil
		}
		if !errors.Is(err, ErrConflict) || attempt >= lg.maxRetries {
			return Record[T]{}, err
		}
	}
}

// AppendTo atomically appends value as one batch only if base is still the current
// head when the commit is created. A nil base means the log must still be empty;
// its successor has revision 1. If another writer has advanced the log, AppendTo
// returns ErrConflict without appending any item. A base at [MaxRevision] returns
// ErrExhausted. An empty batch returns ErrEmptyBatch. As with [Log.Append], the
// batch occupies one revision, and MaxEventBytes limits its complete JSON array.
//
// Use AppendTo when choosing value depends on the log's state. For example,
// scan a snapshot into an index of reserved names, check that a name is free,
// then append its reservation against that same snapshot. On ErrConflict, catch
// up and check again: another writer may have reserved the name. [Log.Append]
// automatically retries the same value, so it cannot recheck that decision.
// See the follow example for [Log.Scan] for maintaining such an index.
//
// AppendTo returns the new snapshot on success. A sequential writer can reuse
// it as the next base. Reusing a loaded or returned snapshot avoids the List and
// head Get performed by Append; carries may still read historical objects. Both
// base and the returned snapshot remain immutable historical views.
func (lg *Log[T]) AppendTo(ctx context.Context, base *Snapshot[T], value ...T) (*Snapshot[T], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := lg.checkSnapshot(base); err != nil {
		return nil, err
	}
	if base != nil && base.Revision() == MaxRevision {
		return nil, ErrExhausted
	}
	event, id, err := lg.prepare(value)
	if err != nil {
		return nil, err
	}
	return lg.appendPrepared(ctx, base, event, id)
}

func (lg *Log[T]) appendPrepared(ctx context.Context, base *Snapshot[T], event json.RawMessage, id string) (*Snapshot[T], error) {
	if err := lg.checkSnapshot(base); err != nil {
		return nil, err
	}
	revision := int64(1)
	var prev *previous
	frontier := make([]frontierLevel, 0)
	if base != nil {
		if base.Revision() == MaxRevision {
			return nil, ErrExhausted
		}
		revision = base.Revision() + 1
		prev = &previous{lg.logKey(base.Revision()), base.commit.RecordHash}
		var err error
		frontier, err = lg.carry(ctx, base)
		if err != nil {
			return nil, err
		}
	}
	prevHash := zeroHash
	if prev != nil {
		prevHash = prev.RecordHash
	}
	if err := lg.validateFrontier(frontier, revision, prevHash); err != nil {
		return nil, err
	}
	c := commit{commitFormat, hexRevision(revision), id, prev, event, recordHash(revision, id, prevHash, event), frontier}
	body, err := json.Marshal(c)
	if err != nil {
		return nil, errors.New("lokv: encode commit")
	}
	key := lg.logKey(revision)
	// Decode before publishing: a T with an incompatible UnmarshalJSON must not
	// cause an append to commit and only then report a decoding failure.
	s, err := lg.snapshot(key, body)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	createErr := lg.store.Create(ctx, key, bytes.NewReader(body))
	if createErr == nil {
		return s, nil
	}
	// Both ErrExists and ambiguous errors can represent an earlier successful
	// adapter retry. Always inspect the attempted key before choosing a new one.
	actual, err := lg.get(ctx, key, false)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			if errors.Is(createErr, ErrExists) {
				return nil, corrupt("existing commit is not visible")
			}
			return nil, fmt.Errorf("create %s: %w", key, createErr)
		}
		return nil, err
	}
	winner, err := lg.snapshot(key, actual)
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
