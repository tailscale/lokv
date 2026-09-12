// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"context"
	"errors"
)

// Store is a sorted, create-only key/value namespace. Implementations must be
// safe for concurrent use and honor context cancellation.
//
// Create must atomically publish a complete value only if the key is absent,
// with exactly one winner among concurrent creators. Successful creation must
// be immediately visible to both Get and List. List must return globally
// ascending bytewise keys for the requested prefix and honor its result limit.
// Returned buffers belong to the caller; Create must not retain a mutable input.
//
// Values are immutable. The storage authority must prohibit overwrites,
// deletion, and lifecycle expiration. Open cannot verify these operational
// preconditions. The s3store subpackage adapts general-purpose S3 buckets;
// directory buckets are unsupported because their listing is unordered.
type Store interface {
	// List returns at most limit matching keys in ascending bytewise order.
	// limit must be positive. A successful Create is immediately visible to List.
	List(ctx context.Context, prefix string, limit int) ([]string, error)

	// Get returns the complete object, or an error matching ErrNotFound.
	// Packed objects grow with their covered history and have no size limit.
	Get(ctx context.Context, key string) ([]byte, error)

	// Create atomically publishes a complete value only if key is absent.
	// Exactly one concurrent creator wins; losers return ErrExists after the
	// winner is visible. Create must never overwrite an existing value.
	Create(ctx context.Context, key string, value []byte) error
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
