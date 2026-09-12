// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"context"
	"errors"
	"io"
)

// SizeReaderAt is a fixed-size byte source. Size returns its nonnegative length.
// ReadAt must support concurrent calls, as required by io.ReaderAt.
type SizeReaderAt interface {
	Size() int64
	io.ReaderAt
}

// Store is a sorted, create-only key/value namespace. Implementations must be
// safe for concurrent use and honor context cancellation.
//
// Create must atomically publish a complete value only if the key is absent,
// with exactly one winner among concurrent creators. Successful creation must
// be immediately visible to both Get and List.
// The caller owns Get's reader and must close it. Create must not retain its
// input after returning; the caller keeps the source open and unchanged until then.
//
// Values are immutable. The storage authority must prohibit overwrites,
// deletion, and lifecycle expiration. Open cannot verify these operational
// preconditions. The s3store subpackage adapts general-purpose S3 buckets;
// directory buckets are unsupported because their listing is unordered.
type Store interface {
	// List returns the first at most limit keys beginning with prefix, in
	// ascending bytewise order. Prefix matching is literal; an empty prefix
	// matches all keys. Returned keys are complete, including the prefix.
	// limit must be positive. A successful Create is immediately visible to List.
	List(ctx context.Context, prefix string, limit int) ([]string, error)

	// Get opens an object, or returns an error matching ErrNotFound.
	// The caller must close the reader. ctx governs reads until it is closed.
	// Each call returns an independent stream, with errors reported by Read.
	Get(ctx context.Context, key string) (io.ReadCloser, error)

	// Create atomically publishes a complete value only if key is absent.
	// Exactly one concurrent creator wins; losers return ErrExists after the
	// winner is visible. Create must never overwrite an existing value.
	// It reads exactly value.Size() bytes starting at offset zero, and may reread
	// them for retries. It does not close value. A source read error must not
	// publish a partial value.
	Create(ctx context.Context, key string, value SizeReaderAt) error
}

var (
	ErrNotFound   = errors.New("lokv: object not found")
	ErrExists     = errors.New("lokv: object already exists")
	ErrConflict   = errors.New("lokv: append conflict")
	ErrCorrupt    = errors.New("lokv: corrupt log")
	ErrRange      = errors.New("lokv: invalid range")
	ErrTooLarge   = errors.New("lokv: append batch too large")
	ErrExhausted  = errors.New("lokv: revision space exhausted")
	ErrEmptyBatch = errors.New("lokv: empty batch")
)
