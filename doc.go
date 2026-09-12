// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

// Package lokv implements an append-only log over a sorted, create-only
// key/value store (e.g. S3, if so configured). The name means Log over K/V.
// Each commit is a historical root; reverse revision keys discover
// the head with one limited LIST. A radix-16 frontier packs complete record
// ranges into zstd segments at every level: 16, 256, 4096 records, and so on.
// Scans read those segments directly without fetching their constituent objects.
//
// Each Record contains one nonempty batch of values. Append and AppendTo accept
// variadic values, publish the entire batch atomically, and consume one revision
// per call. Stored events are always JSON arrays, including single-item batches.
// Scan yields a complete batch per callback. An empty append returns ErrEmptyBatch.
// MaxEventBytes limits new batches only. Compacted objects have no size limit.
// Reads and compaction stream records through temporary files, keeping memory
// proportional to individual batches and codec buffers. Files are unlinked on
// creation except on Windows, where they are removed after closing.
//
// Revisions are int64 sequence numbers from 1 through [MaxRevision] (2^53 - 1,
// JavaScript's Number.MAX_SAFE_INTEGER). Values outside that range are invalid.
// Empty snapshots report zero, and appending after MaxRevision returns ErrExhausted.
// The cap counts batch records, not the individual values within them.
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
// [LoadState] builds an application-defined value or index by applying the whole
// log one value at a time. [State.Sync] applies new batches and tracks the revision;
// [State.SyncTo] reuses an already-known snapshot. Applications arrange their
// own polling or notifications. The State example maintains unique username and
// user ID indexes, using [Log.AppendTo] to make registration conditional on the
// snapshot used to build the indexes. State access requires caller synchronization.
// An apply error permanently poisons State; store errors remain resumable.
package lokv
