// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

// Package lokv implements an append-only log over a sorted, create-only
// key/value store (e.g. S3, if so configured). The name means Log over K/V.
// Each commit is a historical root; reverse revision keys discover
// the head with one limited LIST. A radix-16 frontier packs events once into zstd
// segments, then builds reference-only index nodes.
//
// Store implementations must provide atomic create-if-absent, immediate GET and
// LIST visibility, and ascending bytewise listing. The storage authority must
// prohibit overwrites, deletion, and lifecycle expiration. Open cannot verify
// these operational preconditions. The s3store subpackage adapts general-purpose
// S3 buckets; directory buckets are unsupported.
//
// Conditional commit creation serializes cooperating writers. Hashes detect
// corruption, not malicious authorized writers. Append retains an invocation ID
// across conflicts and ambiguous responses; applications requiring deduplication
// across process restarts must supply their own durable event identifiers.
//
// Snapshots are immutable historical roots. Loading validates the root locally;
// Scan validates the objects it traverses, and Verify traverses the entire root.
// Log methods may be called concurrently when the Store supports concurrent use.
package lokv
