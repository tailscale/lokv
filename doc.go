// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

// Package lokv implements an append-only log over a sorted, create-only
// key/value store (e.g. S3, if so configured). The name means Log over K/V.
// Each commit is a historical root; reverse revision keys discover
// the head with one limited LIST. A radix-16 frontier packs events once into zstd
// segments, then builds reference-only index nodes.
//
// Revisions are int64 sequence numbers from 1 through [MaxRevision] (2^53 - 1,
// JavaScript's Number.MAX_SAFE_INTEGER). Values outside that range are invalid.
// Empty snapshots report zero, and appending after MaxRevision returns ErrExhausted.
//
// Conditional commit creation serializes cooperating writers. Hashes detect
// corruption, not malicious authorized writers. Append retains an invocation ID
// across conflicts and ambiguous responses; applications requiring deduplication
// across process restarts must supply their own durable event identifiers.
//
// Snapshots are immutable historical roots. Loading validates the root locally;
// Scan validates the objects it traverses, and Verify traverses the entire root.
// Log methods may be called concurrently.
//
// The follow example for [Log.Scan] builds an in-memory index, then catches up
// only new records after polling [Log.LoadHead]. Applications arrange their own
// polling or notifications. [Log.AppendTo] can make a write conditional on the
// snapshot used to build the index.
package lokv
