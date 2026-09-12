// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"context"
	"errors"
)

// Store is an immutable flat namespace. Implementations must be safe for
// concurrent use and obey the consistency contract in the package documentation.
// Returned buffers belong to the caller; Create must not retain a mutable input.
type Store interface {
	// List returns at most limit matching keys in ascending bytewise order.
	// limit must be positive. A successful Create is immediately visible to List.
	List(ctx context.Context, prefix string, limit int) ([]string, error)
	// Get returns the complete object, or an error matching ErrNotFound.
	// Adapters must bound response bodies before allocating them in memory.
	Get(ctx context.Context, key string) ([]byte, error)
	// Create atomically publishes a complete value only if key is absent.
	// Exactly one concurrent creator wins; losers return ErrExists after the
	// winner is visible. Create must never overwrite an existing value.
	Create(ctx context.Context, key string, value []byte) error
}

var (
	ErrNotFound  = errors.New("lokv: object not found")
	ErrExists    = errors.New("lokv: object already exists")
	ErrConflict  = errors.New("lokv: append conflict")
	ErrCorrupt   = errors.New("lokv: corrupt log")
	ErrRange     = errors.New("lokv: invalid range")
	ErrTooLarge  = errors.New("lokv: object too large")
	ErrExhausted = errors.New("lokv: revision space exhausted")
)
