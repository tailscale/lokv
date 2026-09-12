// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

// Package lokv implements an append-only log over a sorted, create-only
// key/value store (e.g. S3, if so configured). The name means Log over K/V.
// It supports event sourcing: the log serves as the source of truth, and
// application state is derived by replaying its events.
//
// Use [Open] to create a [Log] backed by a [Store]. [Log.Append] publishes events
// as atomic batches, each represented by a [Record]. [Log.LoadHead] returns a
// snapshot of the log, and [Log.Scan] reads events from that snapshot in order.
//
// [LoadState] builds an in-memory projection (materialized view) by applying the
// log to an application-defined value. Its apply callback is the event handler,
// sometimes called a reducer. [State.Sync] applies newly appended events and
// [State.Revision] tracks the projection's position. Applications arrange their
// own polling or notifications.
//
// [Log.AppendTo] provides optimistic concurrency control for decisions based on
// a snapshot: the append succeeds only if that snapshot is still current. The
// State example uses this to register unique usernames and allocate user IDs.
package lokv
