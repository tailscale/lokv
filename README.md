<!--
Copyright (c) Tailscale Inc & contributors
SPDX-License-Identifier: BSD-3-Clause
-->

> [!WARNING]
> **Experimental:** lokv is not ready for production use. APIs and the on-disk format may change without notice.

# lokv (Log over K/V)

lokv implements an append-only log over a sorted, create-only key/value store
(e.g. S3, if so configured). Values are JSON, revisions are gap-free `int64`s
starting at **1**, and every commit is a historical snapshot. Nonpositive revisions
are invalid.

```go
import (
    "context"
    "fmt"

    "github.com/tailscale/lokv"
    "github.com/tailscale/lokv/memstore"
)

ctx := context.Background()
log, err := lokv.Open[string](lokv.Config{
    Store: new(memstore.Store),
    Prefix: "audit",
})
if err != nil { return err }

record, err := log.Append(ctx, "created") // revision 1
if err != nil { return err }
fmt.Println(record.Revision)

snap, err := log.LoadHead(ctx)
if err != nil { return err }
err = log.Scan(ctx, snap, lokv.Range{First: 1, Last: snap.Revision()},
    func(r lokv.Record[string]) error {
        fmt.Println(r.Revision, r.Value)
        return nil
    })
if err != nil { return err }
```

`Head` returns `(record, found, error)`. `LoadHead` returns a nil snapshot for an
empty log. An empty snapshot reports revision 0, which is not a valid record
revision. `LoadRevision` loads a historical snapshot without listing and rejects
revisions <= 0 with `ErrRange`. Appending after `math.MaxInt64` returns `ErrExhausted`.
`AppendTo(ctx, snapshot, value)` avoids head discovery and attempts one successor;
a stale snapshot returns `ErrConflict`. Use it when choosing the next event depends
on the history you just read, so you can reload and recompute on conflict.
`Append` retries the same value automatically. Snapshots belong to the `Log` that
loaded them. Their private wire state is unaffected by mutations to returned values.

Ranges include both endpoints, must be positive, and must fit inside a nonempty
snapshot; invalid ranges return `ErrRange`. `Scan`
visits records in order and returns callback errors immediately. `Verify` scans
all records and reachable tree objects; verifying an empty snapshot succeeds.
Loading validates the root locally, while scans validate the objects they fetch.
Verification does not inventory unreachable objects or superseded raw commits.

## Storage and concurrency

There is no mutable HEAD. Reverse revision keys make one ascending `List` with
limit 1 find the newest commit. A head read uses that LIST and one GET.

A radix-16 frontier packs every completed group of 16 preceding events into one
zstd segment. Higher levels contain references only. Appending revision 17
creates a segment and a commit; revision 257 creates a segment, an index, and a
commit. An ordinary append creates only its commit. For `N` records, successful
single-writer creations total:

```text
N + floor((N-1)/16) + floor((N-1)/256) + ...
```

This approaches `16/15` creations per record. Event data is stored in its raw
commit and, once its group closes, in one packed segment. Aggregate dependencies
are created before the final conditional commit PUT, which publishes the event.

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
- Caller-owned returned buffers and safe concurrent calls.
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
log, err := lokv.Open[Event](lokv.Config{Store: store, Prefix: "audit"})
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

Defaults are 32 conflict retries, 1 MiB of event JSON, and 64 MiB for each stored
or decompressed object. `MaxObjectBytes` may be increased up to 256 MiB, and
`MaxEventBytes` must not exceed it. Configure the S3 adapter's object limit at
least as high as the core limit. A segment's 16 events and metadata must fit the
object limit. A zero configuration limit selects its default; negative limits
are rejected. Use `AppendTo` for an attempt without conflict retries.

Prefix normalization strips leading and trailing slashes; empty prefixes work.
Invalid UTF-8, control characters, backslashes, and empty or dot path components
are rejected. References must remain inside the normalized namespace.

Readers validate required fields, fixed-width lowercase hex, digests, aligned
ranges, frontier digits, record hashes, and chain boundaries. Unknown JSON fields
are accepted; duplicate envelope fields, trailing JSON, concatenated zstd frames,
and oversized input or decompressed output are rejected. Segments use zstd's
default compression level, one encoder worker, a 1 MiB window, and checksums.
Tail packing uses at most four concurrent GETs, joined before returning.

The complete protocol, key layout, and hash definition are in [DESIGN.md](DESIGN.md).

## Validation

```sh
go test -race ./...
go vet ./...
go test -run '^$' -bench . -benchmem
```

Tests cover carry boundaries through revision 4097, request counts, concurrent
writers, crash injection, ambiguous success, corruption, pruning, and resource
limits. HTTP tests exercise the actual AWS SDK request headers and status mapping.
Benchmarks report store requests and average carry depth alongside allocations.
Fuzz targets cover key parsing, commit and index decoding, frontier validation,
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
