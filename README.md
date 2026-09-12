<!--
Copyright (c) Tailscale Inc & contributors
SPDX-License-Identifier: BSD-3-Clause
-->

> [!WARNING]
> **Experimental:** lokv is not ready for production use. APIs and the on-disk format may change without notice.

# lokv (Log over K/V)

lokv implements an append-only log over a sorted, create-only key/value store
(e.g. S3, if so configured). Values are JSON, revisions are gap-free `int64`s
starting at **1**, and every commit is a historical snapshot. Revisions are capped
at `lokv.MaxRevision` (**9,007,199,254,740,991**, or 2^53 - 1), JavaScript's
`Number.MAX_SAFE_INTEGER`, so they remain exact when passed through JavaScript
Numbers. Revisions outside `1..MaxRevision` are invalid.

Each record is an atomic batch: `Record[T].Value` is a nonempty `[]T`, and
`Append(ctx, a, b, c)` commits all three values in argument order at one revision.
A batch uses one commit object, with extra aggregate objects only on a radix
carry. `AppendTo(ctx, snapshot, values...)` conditionally commits a whole batch.
Single values are stored as one-element JSON arrays. Empty batches return
`ErrEmptyBatch`. Revision limits and scan ranges count batches, not their items.

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
empty log. An empty snapshot reports revision 0, which is not a valid record
revision. `LoadRevision` loads a historical snapshot without listing and rejects
revisions outside `1..MaxRevision` with `ErrRange`. Appending after `MaxRevision`
returns `ErrExhausted`.
`AppendTo(ctx, snapshot, values...)` avoids head discovery and attempts one successor;
a stale snapshot returns `ErrConflict`. Use it when choosing the next batch depends
on the history you just read, so you can reload and recompute on conflict.
`Append` retries the same batch automatically. Snapshots belong to the `Log` that
loaded them. Their private wire state is unaffected by mutations to returned values.

Ranges include both endpoints, which must be within `1..MaxRevision`, with
`First <= Last`; invalid bounds return `ErrRange`. The zero `Range` is empty.
`Scan` visits the intersection of the range and snapshot: `lokv.All()` scans
everything, and `lokv.StartingAt(rev)` scans from `rev` through the snapshot's end.
`lokv.After(applied)` excludes the already-applied revision: `After(0)` covers the
whole log, and `After(MaxRevision)` returns an empty range.

Empty ranges, empty snapshots, and ranges starting past the head yield nothing,
with no store I/O. Scans visit records in order and return callback errors
immediately. `Verify` scans all records and reachable tree objects; verifying an
empty snapshot succeeds. Loading validates the root locally, while scans validate
the objects they fetch. Verification does not inventory unreachable objects or
superseded raw commits.

## Following the log with an in-memory index

The executable [following example](example_follow_test.go) shows the complete
pattern, also available as the follow example for `Log.Scan` in Go documentation:

1. Start with an empty index and `applied = 0`.
2. Call `LoadHead`, then `Scan` with `lokv.All()` to build the index.
3. On each poll or wakeup, call `LoadHead` again. If its revision is greater than
   `applied`, scan with `lokv.After(applied)` to apply only new records.
4. Apply every item in `r.Value`, then advance `applied` to `r.Revision`. If a
   scan fails partway through, resume after the last fully applied batch.

`After` handles both an empty index and a checkpoint at `MaxRevision` without
caller-side arithmetic. A snapshot is a fixed upper bound: writes that arrive
during a scan are picked up on the next catch-up. Scan yields only the requested
records and performs no LIST operations.

Without transport retries or an application-provided cache, the costs are:

| Situation | Store operations |
| --- | --- |
| Poll an empty log | One LIST with limit 1 |
| Poll an unchanged nonempty log | One LIST and one head GET; skip Scan |
| Catch up exactly one new batch | One LIST and one head GET; Scan needs no further I/O |
| Catch up several new batches | The head lookup, plus one GET per intersecting frontier segment or raw tail commit |

The head contains its entire batch, so Scan never fetches it again. Disjoint
historical ranges are skipped. Every segment contains its complete range,
including at higher levels, so reading it needs no GETs for constituent objects.
A partially overlapping segment is fetched and validated in full, including any
older entries it contains; only the new entries are applied. The core does not
cache objects between calls.

`Log` and `Store` have no watch API. Run the example's `catchUp` closure from one
goroutine using a ticker, an application-provided wakeup channel, or both:

```go
ticker := time.NewTicker(time.Second)
defer ticker.Stop()
for {
    if err := catchUp(); err != nil { return err }
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
missed hints. If other goroutines read the index, hold a mutex across the entire
batch and its checkpoint update. If applying a batch can fail, stage its updates
and publish them together only on success, or make replay safe. When persisting
the index, commit the whole batch's updates and last-applied revision in the same
transaction. Log atomicity does not make application index updates atomic.

For mostly idle logs, probing `LoadRevision(applied+1)` uses one GET and no LIST:
`ErrNotFound` means no successor was visible at that lookup. Guard against
`lokv.MaxRevision` before incrementing. If a successor exists, load the latest head
to batch the catch-up; this trades an extra probe GET on active polls for cheaper
idle polls. If a notification already provides a committed revision, use
`LoadRevision` directly and scan up to that revision. A snapshot returned by a
local `AppendTo` needs no lookup at all. All three approaches reuse the same
last-applied revision and range-scan pattern.

## Making decisions against an indexed snapshot

`AppendTo` makes a write conditional on the snapshot used for the decision. For
example, after catching up a name-reservation index, check that a name is free
and call `AppendTo` with that exact snapshot. On `ErrConflict`, catch up and check
again: another writer may have reserved the name in the meantime. `Append` retries
the same batch automatically and cannot recheck application-specific conditions.

The [AppendTo example](example_follow_test.go) demonstrates that race. Keep the
snapshot returned by a successful `AppendTo` for the next operation, and apply
its record to your index before using that index for another decision. When a
catch-up scan fails, finish catching up before making decisions against its head.

## Storage and concurrency

There is no mutable HEAD. Reverse revision keys make one ascending `List` with
limit 1 find the newest commit. A head read uses that LIST and one GET.

A radix-16 frontier packs complete batch ranges into zstd segments at every
level: level 1 contains 16 batches, level 2 contains 256, level 3 contains 4,096,
and level 4 contains 65,536. Each segment contains the actual records in order.
The head references up to 15 objects per level, plus its own batch.

Appending revision 17 creates a level-1 segment and a commit; revision 257
creates level-1 and level-2 segments and a commit. An ordinary append creates
only its commit. For `N` records, successful single-writer creations total:

```text
N + floor((N-1)/16) + floor((N-1)/256) + ...
```

This approaches `16/15` creations per batch record. Each carry reads 16 child
ranges, combines their record projections, and writes one compressed object.
The prior head is reused when packing level 0. Aggregate dependencies are
created before the final conditional commit PUT, which publishes the batch.
Payloads are copied at each completed level, and older objects remain stored.
This spends more storage and compaction work to reduce client download requests.

For a new client, `LoadHead` followed by `Scan(..., lokv.All(), ...)` takes one
LIST plus `1 + sum(hex digits of N-1)` GETs for `N > 0`, excluding transport
retries. The empty log needs only the LIST. There are no recursive index reads:

| Batch records | LISTs | GETs including the head |
| ---: | ---: | ---: |
| 16 | 1 | 16 |
| 17 | 1 | 2 |
| 257 | 1 | 2 |
| 4,097 | 1 | 2 |
| 65,537 | 1 | 2 |
| 1,000,000 | 1 | 40 |

For example, 65,537 batches fit in one level-4 segment plus the head. Scans
currently fetch objects sequentially. A narrow range that intersects a large
segment still downloads and validates that entire segment.

Writers race to create the deterministic next-revision key. `Append` marshals
once and retains one random commit ID through conflict retries. On a failed PUT,
it reads the attempted key to resolve a possibly lost success response. Backend
transport retries belong to the adapter; an unclassified error with no visible
object is returned rather than retried automatically. Cancellation or a failed
ambiguity-resolution GET can leave the caller unsure whether a write committed.
A new invocation has a new ID; durable application deduplication needs an event
identifier in the value.

A conforming `Store` must provide:

- Atomic create-if-absent with exactly one winner and no partial visibility.
- Immediate visibility of successful creation to both GET and LIST.
- Globally ascending bytewise prefix listing with a positive result limit.
- Independent `io.ReadCloser` streams from `Get`, owned and closed by callers.
- `Create` inputs implementing `SizeReaderAt` (`Size() int64` and `io.ReaderAt`),
  kept open and unchanged until the call returns.
- Safe concurrent calls and context cancellation during transfers.
- Immutable values, with deletion unavailable to the library's authority.

`Open` performs no I/O and cannot diagnose these deployment properties. Hashes
catch corruption, not an authorized writer fabricating a new history. The library
never updates or deletes objects, and does not garbage-collect orphaned carries.
`storetest.Test` provides a reusable conformance suite for additional adapters.

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

The adapter always sends `If-None-Match: *`, maps HTTP 412 to `ErrExists`, retries
HTTP 409 with bounded jitter, and bounds GET response bodies. It uses only
`ListObjectsV2`, `GetObject`, and single-request `PutObject`. Directory buckets,
S3 Express directory buckets, and unordered or eventually consistent compatible
services are unsupported. Bucket names are required; ARNs and access-point
aliases are not supported.

Grant the writer only prefix-scoped `s3:ListBucket`, `s3:GetObject`, and
conditional `s3:PutObject`. Explicitly deny deletes and nonconditional creation
with the [example bucket policy](examples/immutable-bucket-policy.json), replacing
`BUCKET` and `PREFIX`. The example contains denies, not permission grants. Disable
lifecycle expiration on the prefix and prefer bucket-owner-enforced ownership.
Object Lock compliance mode can add protection against administrators, but does
not replace conditional creation. Versioning alone does not prevent replacement
of the current object.

## Limits and wire format

Defaults are 32 conflict retries and 1 MiB for a new batch's complete JSON
array. `MaxEventBytes` is checked before append I/O. It does not limit stored
records, so lowering it cannot prevent reading or compacting existing history.
Zero selects the default; negative limits are rejected. Use `AppendTo` for an
attempt without conflict retries.

Compacted objects have no configured size limit in either the core or S3
adapter. Compaction never rejects previously accepted data for exceeding a byte
limit. Store transfers, JSON record processing, and zstd compression are
streamed. A commit still holds one complete batch and its frontier in memory;
upper-level segments do not require whole-range byte slices or record slices.

`Store.Get` returns an `io.ReadCloser` that the caller closes. `Store.Create`
accepts a `SizeReaderAt`, so it knows the body length without seeking and can
read it again for retries. `bytes.Reader`, `strings.Reader`, and
`io.SectionReader` implement this interface. The S3 adapter creates a fresh
section reader for each upload attempt and sets the content length explicitly.

Temporary files use the operating system's temporary directory. On non-Windows
systems they are unlinked immediately and accessed through the open descriptor;
on Windows they are removed after closing. Normal returns, failures, and
cancellation close the descriptors and remove any remaining names.

A read first spools the compressed response to disk, validates its frame, then
decodes and hashes it incrementally while spooling validated record projections.
Only after the entire segment passes validation are its records replayed to
scan callbacks. This needs one S3 GET, with additional local disk I/O.
Compaction prepares children with at most four workers, then writes their
records in order through JSON and zstd into a temporary upload file. The
uncompressed hash determines its key, and its counted compressed length supplies
`Size()`. Input spools are released as they are consumed.

Memory use depends on individual batch sizes and codec buffers, not the whole
compacted range. Temporary disk use grows with the range being processed.
The `memstore` adapter necessarily retains its stored values in memory.

Commit and segment envelopes use `lokv/commit/v3` and `lokv/segment/v3`.
The streaming implementation preserves the v3 JSON bytes and hash definitions.
Earlier envelopes are rejected. Every nonzero level uses a `.json.zst` packed
segment; there are no reference-only index objects. Commit keys and the record
hash algorithm are unchanged.

Prefix normalization strips leading and trailing slashes; empty prefixes work.
Invalid UTF-8, control characters, backslashes, and empty or dot path components
are rejected. References must remain inside the normalized namespace.

Readers validate required fields, fixed-width lowercase hex, digests, aligned
ranges, frontier digits, record hashes, and chain boundaries. Unknown JSON fields
are accepted; duplicate envelope fields, trailing JSON, and concatenated zstd
frames are rejected. Segments use zstd's default compression level, one encoder
worker, a 1 MiB window, and checksums. The window bounds compression history,
not decompressed output. Packing uses at most four concurrent GETs, joined
before returning.

The complete protocol, key layout, and hash definition are in [DESIGN.md](DESIGN.md).

## Validation

```sh
go test ./...
go test -race ./...
go vet ./...
go test -run '^$' -bench . -benchmem
```

Tests cover carry boundaries through revision 4097, request counts, concurrent
writers, atomic batches, crash injection, ambiguous success, corruption, pruning,
and batch admission limits. A seeded level-4 packing test verifies that 65,537
batches download with two GETs. Under `-race`, large packing, boundary, and crash
workloads use two carry levels; normal runs retain the deeper coverage. CI runs
both modes. HTTP tests exercise AWS SDK headers, status mapping, and packed
full-scan request counts.
Tests also cover stream ownership, read failures, retry replay, canonical wire
compatibility, validation before callbacks, and temporary-file cleanup.
Benchmarks report store requests and average carry depth alongside allocations.
`BenchmarkStreamingCompaction` uses files for stored payloads and samples peak
heap growth while compacting ranges of different sizes; run it with
`go test -run '^$' -bench '^BenchmarkStreamingCompaction$' -benchtime=1x`.
Fuzz targets cover key parsing, commit and segment decoding, frontier validation,
and segment decompression; for example:

```sh
go test -run '^$' -fuzz '^FuzzSegmentDecompression$' -fuzztime 30s
```

The live S3 test is opt-in and **permanently writes objects** under a unique
`lokv-integration/` prefix. It never cleans them up. Supply a general-purpose
test bucket, suitable permissions, and standard AWS credentials/region:

```sh
LOKV_S3_INTEGRATION=1 LOKV_S3_BUCKET=my-test-bucket \
  go test ./s3store -run '^TestLiveS3$' -v
```
