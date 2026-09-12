<!--
Copyright (c) Tailscale Inc & contributors
SPDX-License-Identifier: BSD-3-Clause
-->

> [!WARNING]
> **Experimental:** lokv is not ready for production use. APIs and the on-disk format may change without notice.

# lokv (Log over K/V)

[![Go Reference](https://pkg.go.dev/badge/github.com/tailscale/lokv.svg)](https://pkg.go.dev/github.com/tailscale/lokv)

lokv implements an append-only log over a sorted, create-only key/value store
(e.g. S3, if so configured). Values are JSON, and every commit is a historical
snapshot.

lokv supports **event sourcing**: use the log as the source of truth and replay
its events to build application state. `State[T, S]` maintains that state as an
in-memory **projection**, also called a **materialized view**.

Each record is an atomic batch: `Record[T].Value` is a nonempty `[]T`, and
`Append(ctx, a, b, c)` commits all three values in argument order at one revision.
`AppendTo(ctx, snapshot, values...)` conditionally commits a whole batch.

```go
import (
    "context"
    "fmt"

    "github.com/tailscale/lokv"
    "github.com/tailscale/lokv/memstore"
)

ctx := context.Background()
lg, err := lokv.Open[string](lokv.Config{
    Store: new(memstore.Store),
    Prefix: "audit",
})
if err != nil { return err }

record, err := lg.Append(ctx, "created", "enabled") // both at revision 1
if err != nil { return err }
fmt.Println(record.Revision)

snap, err := lg.LoadHead(ctx)
if err != nil { return err }
err = lg.Scan(ctx, snap, lokv.All(),
    func(r lokv.Record[string]) error {
        fmt.Println(r.Revision, r.Value)
        return nil
    })
if err != nil { return err }
```

`Head` returns `(record, ok, error)`. `LoadHead` returns a nil snapshot for an
empty log, and `LoadRevision` loads a historical snapshot. Use `Scan` with
`lokv.All()` to read the whole snapshot, `lokv.StartingAt(rev)` to start at a
revision, or `lokv.After(applied)` to read newer records. `Verify` checks the
integrity of the records and tree objects reachable from a snapshot.

## Event sourcing and projections

For readers familiar with event sourcing, the terminology maps to lokv as follows:

| Term | lokv API |
| --- | --- |
| Event log | `Log[T]`, used as the application's source of truth |
| Event | One `T` value; `Record[T]` contains an atomic batch of events |
| Projection / materialized view | The application value `S` maintained by `State[T, S]` |
| Event handler / reducer | The `func(*S, T) error` callback passed to `LoadState` |
| Projection position | `State.Revision()`, the last fully applied batch's revision |
| Optimistic concurrency control | `AppendTo` commits only if its base snapshot is still current |

In the [username registration example](example_state_test.go), a
`userRegistration` is an event, `userIndex` is the projection, and its `apply`
method is the event handler. `LoadState` builds the projection by replaying the
log; `Sync` applies newer events on later calls. `AppendTo` lets a registration
decision based on that projection commit only if another writer has not advanced
the log. On conflict, the client syncs and recomputes the decision.

## Following the log with an in-memory index

`LoadState` builds a `State[T, S]` by applying the whole log to your initial
application value. Supply a function that mutates `*S` for one `T` value at a
time. State handles iteration in revision order and append argument order within
each batch. For example, count each event in the string log above:

```go
state, err := lokv.LoadState(ctx, lg, make(map[string]int),
    func(counts *map[string]int, event string) error {
        (*counts)[event]++
        return nil
    })
if err != nil { return err }
fmt.Println(state.Revision(), state.Value())

// On a poll or wakeup, apply only newly appended batches.
if _, err := state.Sync(ctx); err != nil { return err }
fmt.Println(state.Revision(), state.Value())
```

`Sync` returns the snapshot matching the updated state. When a write depends
on the indexes, make the decision from `state.Value()` and pass that snapshot to
`AppendTo`. On `ErrConflict`, catch up and recompute the decision. After a
successful append, `state.SyncTo(ctx, newSnapshot)` applies the new batch with
no store I/O. It also accepts snapshots obtained with `LoadHead` or `LoadRevision`.
It rejects snapshots older than the applied revision and never rewinds state.

The executable [username registration example](example_state_test.go), also
available as the `State` example in Go documentation, maintains indexes in both
directions between usernames and allocated user IDs. Each client has its own
state. A competing writer may take the proposed name or ID, so a conflict causes
the client to recheck the name and choose the next ID before retrying. Every
writer must follow this protocol for the indexes to remain unique.

The initial value must represent the empty log. `State` owns and mutates it;
`Value` returns a shallow copy, with maps and pointers still referring to the
same data. State is not safe for concurrent use. Share it only with caller
synchronization covering catch-up, decisions, and reads of referenced data.

The applied revision advances only after every item in a batch succeeds. Any
apply error permanently poisons the State. `Err()`, `Sync`, and `SyncTo` return
the same sticky error, wrapping the callback error with its revision and item
number. Further syncs perform no I/O or callback calls. The application value
may contain a partially applied batch, including mutations from the failing
call, and must not be used. After addressing the cause, rebuild with `LoadState`
and a fresh initial value. There is no rollback or reset of a poisoned State.

Store and scan errors remain resumable: `LoadState` returns the partial State
alongside the error, and if `state.Err()` is nil, `Sync` can retry without
reapplying successful batches. Cancellation outside apply takes effect between
batches; a context error returned by apply itself poisons the State. A snapshot
is a fixed upper bound: writes arriving during sync are left for the next call.

The lower-level [following example](example_follow_test.go) tracks the index and
applied revision directly using `LoadHead`, `Scan`, and `After`. Details of
request counts and range reads are in the
[Scan documentation](https://pkg.go.dev/github.com/tailscale/lokv#Log.Scan).

`Log` and `Store` have no watch API. Call `state.Sync` from one
goroutine using a ticker, an application-provided wakeup channel, or both:

```go
ticker := time.NewTicker(time.Second)
defer ticker.Stop()
for {
    if _, err := state.Sync(ctx); err != nil { return err }
    select {
    case <-ctx.Done():
        return ctx.Err()
    case <-ticker.C:
    case _, ok := <-wake: // optional <-chan struct{}; nil disables wakeups
        if !ok { wake = nil }
    }
}
```

Treat external notifications, such as S3 notifications for committed log keys,
as hints to catch up. Duplicate or coalesced hints work; a periodic poll covers
missed hints. If other goroutines read the State, hold a mutex across sync and
index access, and check `Err()` before using the value. For applications managing
their own index with `Scan`, callbacks receive whole batches; stage each batch's
updates or provide rollback if apply failures need to be retried. When persisting
that index, commit the whole batch's updates and last-applied revision in the same
transaction. Log atomicity does not make application index updates atomic.

## Making decisions against an indexed snapshot

`AppendTo` provides optimistic concurrency control by making a write conditional
on the snapshot used for the decision. For example, after catching up a
name-reservation index, check that a name is free and call `AppendTo` with that
exact snapshot. On `ErrConflict`, catch up and check again: another writer may
have reserved the name in the meantime. `Append` retries the same batch
automatically and cannot recheck application-specific conditions.

The [AppendTo example](example_follow_test.go) demonstrates that race. Keep the
snapshot returned by a successful `AppendTo` for the next operation, and apply
its record to your index before using that index for another decision. When a
catch-up scan fails, finish catching up before making decisions against its head.

## Storage and concurrency

lokv packs historical events into compressed ranges so clients can load a log
without fetching each original record separately. Reads and compaction stream
through temporary files to keep memory use proportional to batches and codec
buffers; temporary disk use grows with the range being processed.

`Log` methods are safe for concurrent use. `Store` implementations must provide
atomic create-if-absent, consistent reads and sorted listing, and immutable
values. The [Store contract](https://pkg.go.dev/github.com/tailscale/lokv#Store)
documents the requirements; `storetest.Test` checks additional adapters.

An append error can leave the caller unsure whether the write committed.
Applications that need durable deduplication across retries or process restarts
should include their own request ID in each event. See the
[append documentation](https://pkg.go.dev/github.com/tailscale/lokv#Log.Append)
for retry behavior.

## S3

The core package imports no AWS SDK packages. `s3store` uses AWS SDK for Go v2:

```go
awsCfg, err := config.LoadDefaultConfig(ctx)
if err != nil { return err }
store, err := s3store.New(s3store.Config{
    Client: s3.NewFromConfig(awsCfg),
    Bucket: "my-general-purpose-bucket",
})
if err != nil { return err }
lg, err := lokv.Open[Event](lokv.Config{Store: store, Prefix: "audit"})
```

Use a general-purpose S3 bucket configured to prohibit overwrites, deletes,
and lifecycle expiration of log objects. The
[s3store package documentation](https://pkg.go.dev/github.com/tailscale/lokv/s3store)
explains the recommended IAM and bucket configuration and links to the
[example bucket policy](examples/immutable-bucket-policy.json).

Configuration and operation-specific rules are documented in the
[Go reference](https://pkg.go.dev/github.com/tailscale/lokv). The storage protocol,
key layout, compression, and hash definitions are in [DESIGN.md](DESIGN.md).

## Local disk caching

`cachestore` combines an authoritative store, such as the S3 store above, with
a cache store. `diskstore` provides a persistent local filesystem store:

```go
cache, err := diskstore.New("/var/cache/myapp/audit-bucket")
if err != nil { return err }
cached, err := cachestore.New(cachestore.Config{
    Origin: store,
    Cache: cache,
})
if err != nil { return err }
lg, err := lokv.Open[Event](lokv.Config{Store: cached, Prefix: "audit"})
```

Reads use cached immutable objects when available. Complete origin reads and
successful origin writes fill the cache; cache lookup or fill failures do not
fail successful origin operations. `List` always reaches the origin, so other
writers remain visible. A warm cache can eliminate origin GETs for repeated
scans and idle `State.Sync` calls, while each sync still makes an origin LIST.

Give each origin its own cache directory. There is no automatic eviction;
the cache is disposable and can be removed while clients are stopped. See the
[cachestore documentation](https://pkg.go.dev/github.com/tailscale/lokv/cachestore)
and its executable example for streaming, error handling, and ownership details.
