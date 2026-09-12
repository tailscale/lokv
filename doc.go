// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

// Package lokv implements an append-only log over a sorted, create-only
// key/value store (e.g. S3, if so configured). The name means Log over K/V.
// Each commit is a historical root; reverse revision keys discover
// the head with one limited LIST. A radix-16 frontier packs events once into zstd
// segments, then builds reference-only index nodes.
//
// Revisions are int64 sequence numbers starting at 1. Nonpositive revisions are
// invalid. Empty snapshots report zero, and appending after the maximum int64
// revision returns ErrExhausted.
//
// Conditional commit creation serializes cooperating writers. Hashes detect
// corruption, not malicious authorized writers. Append retains an invocation ID
// across conflicts and ambiguous responses; applications requiring deduplication
// across process restarts must supply their own durable event identifiers.
//
// Snapshots are immutable historical roots. Loading validates the root locally;
// Scan validates the objects it traverses, and Verify traverses the entire root.
// Log methods may be called concurrently.
package lokv
