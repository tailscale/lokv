<!--
Copyright (c) Tailscale Inc & contributors
SPDX-License-Identifier: BSD-3-Clause
-->

# lokv: append-only log over a sorted, create-only key/value store

Status: implementation specification\
Target language: Go\
Working package name: `lokv`\
Key layout: `lokv/v1`; commit and segment envelopes: v3

## 1. Summary

lokv (Log over K/V) implements an append-only log over a sorted, create-only
key/value store (e.g. S3, if so configured). This generic Go library stores values
of type `T` in revision order. Each nonempty batch of `T` values is encoded as
a JSON array and embedded in one immutable commit object. Amazon S3 is the
first concrete adapter, not part of the core API.

The commit object for a record is also the root manifest for the complete log at
that revision. There is no mutable `HEAD` object. The current head is found with
one forward `Store.List` request over reverse-encoded, fixed-width revision keys:

```text
v1/log/7ffffffffffffffc.json   # revision 3
v1/log/7ffffffffffffffd.json   # revision 2
v1/log/7ffffffffffffffe.json   # revision 1
```

Ascending lexical order therefore returns the highest revision first.

History preceding each head is represented by an immutable radix-16 frontier:

- level 0 references an individual commit/event object and covers 1 record;
- every level `L >= 1` is a packed zstd segment containing all `16^L` records
  in its range (16, 256, 4096, 65536, and so on);
- a head has 0–15 references at each level. The reference count at level `L`
  equals hexadecimal digit `L` of the number of preceding records.

Appending usually writes only the new commit/head object. A hexadecimal carry
writes one aggregate object per carried level before publishing the commit.
The final commit `Store.Create` is the linearization point.

Best case: **1 object creation per record** (one PUT for small S3 values).\
Amortized: **`1 + 1/16 + 1/256 + ... = 16/15 ~= 1.0667 creations per record`**.

All objects are written with the store's atomic create-if-absent operation. For
S3, the adapter implements this using `If-None-Match: *`, and the bucket/prefix
must deny deletes and reject non-conditional object creation. No object is ever
overwritten or deleted by this library.

## 2. Goals

1. Atomically store nonempty batches of arbitrary Go values `T` for which
   `json.Marshal` and `json.Unmarshal` work.
2. Give committed records a gap-free `int64` revision starting at 1.
3. Find the latest committed revision with one `LIST` returning at most one key.
4. Use one object creation for an append that causes no radix carry.
5. Download complete history with one GET per frontier object, using larger
   packed ranges at successive levels.
6. Support safe optimistic concurrent writers using conditional creation of the
   deterministic next-revision key.
7. Make every historical revision a readable snapshot/root of the log as it
   existed then.
8. Fail closed on missing, malformed, inconsistent, or hash-invalid objects.
9. Permit full and range scans without listing the entire bucket.
10. Keep storage behind a narrow interface so the core package has no AWS SDK
    dependency and unit tests require no network.

## 3. Non-goals

- Mutating or deleting a committed event.
- Reclaiming orphaned or superseded objects. The storage policy forbids it.
- Exactly-once application semantics across unrelated process invocations.
- Enforcing the wire protocol against a principal that can issue arbitrary
  low-level writes. Backend permissions ensure physical immutability;
  protocol-conforming writers ensure logical validity.
- A materialized application-state checkpoint. Applications may build a
  separate checkpoint facility whose watermark is a log revision.
- Efficient lookup by event contents. This is a sequence, not a key/value index.
- Backends without the ordering, consistency, atomic-visibility, and
  create-if-absent contract in section 5. This excludes S3 directory buckets and
  S3 Express directory buckets because they do not provide ordered listing.

## 4. Terminology and invariants

`revision`
: `int64` sequence number from 1 through `MaxRevision` (9,007,199,254,740,991,
  or 2^53 - 1). Revision 1 is the first record; values outside this range are
  invalid. Zero is reserved for empty-snapshot accessors. The cap equals
  JavaScript's `Number.MAX_SAFE_INTEGER` so revisions remain exact when passed
  through JavaScript Numbers. Each revision identifies one entire batch, so
  `MaxRevision` limits batch records rather than individual values.

`record`
: One nonempty, ordered batch of values, its revision, commit ID, and hash.

`commit`
: The immutable `v1/log/...json` object containing one batch and the radix
  frontier for all records before it. A commit is also a historical root.

`head`
: The committed object with the greatest revision, found by reverse-key listing.

`frontier`
: A canonical partition of revisions `[1, head.revision)` into ordered blocks.

`reference`
: A key, content digest, level, and inclusive revision range naming a commit
  or a packed segment.

Required invariants:

1. The log is empty, or committed revision keys are exactly `1..HEAD`.
2. A commit at revision `R > 1` names revision `R-1` as its predecessor.
3. A commit's frontier covers exactly `[1,R)` with no gaps or overlap.
4. A level `L` reference covers exactly `16^L` consecutive records and its
   start revision minus 1 is aligned to `16^L`: `(start-1) % 16^L == 0`.
5. At most 15 references exist at any frontier level.
6. Frontier references are chronological within a level. When the whole
   frontier is read, levels are visited from highest to lowest.
7. `len(frontier[L])` equals nibble `L` of `R-1`, the number of preceding records.
8. Every referenced object exists and matches the key, range, level, and digest
   in its reference.
9. Aggregate objects are successfully created before a commit that references
   them is created.
10. The commit creation is conditional and is the append's linearization point.

With the `MaxRevision` cap there are 14 possible frontier levels, numbered 0
through 13. Level 13 has at most 1 reference, and the maximum frontier contains
195 references.
This bounds commit metadata even though old commit objects are retained forever.

## 5. Store interface and required behavior

The core package depends only on this public interface:

```go
package lokv

type SizeReaderAt interface {
    Size() int64
    io.ReaderAt
}

// Store is a flat key/value namespace. Values and successful creations are
// immutable. Implementations must satisfy the consistency contract below.
type Store interface {
    // List returns at most limit keys having prefix, ordered by ascending
    // bytewise lexicographical comparison. limit must be positive.
    List(ctx context.Context, prefix string, limit int) ([]string, error)

    // Get opens the value for key. The caller must close the reader.
    // ctx governs reads until closure. An absent key returns ErrNotFound.
    Get(ctx context.Context, key string) (io.ReadCloser, error)

    // Create atomically creates key with value if and only if key is absent.
    // It returns an error matching ErrExists if key already exists. It must
    // never overwrite an existing value and must never expose a partial value.
    // value supplies exactly Size() bytes starting at offset zero. The caller
    // keeps it open and unchanged until Create returns. Create does not close it.
    Create(ctx context.Context, key string, value SizeReaderAt) error
}

var (
    ErrNotFound = errors.New("lokv: object not found")
    ErrExists   = errors.New("lokv: object already exists")
)
```

Each successful Get returns an independent reader owned by the caller. Reading
may fail after Get succeeds, and the caller must close the stream on every path.
Create may use independent section readers over its SizeReaderAt for retries;
source read failures must never publish a partial object. Size is nonnegative
and stable, and reads starting at offset zero cover the complete value.
Keys are opaque UTF-8 strings to the store, but ordering must be exactly bytewise
so fixed-width lowercase hexadecimal keys behave identically on every backend.

The required semantic contract is:

- `Create` is atomic and exposes either the complete value or nothing;
- concurrent `Create` calls for one absent key have exactly one winner, with all
  losers returning an error matching `ErrExists` once the winner is visible;
- successful `Create` is immediately visible to `Get`;
- successful `Create` is immediately reflected by `List`, giving linearizable
  `Head` behavior;
- `List` returns globally ascending keys for the requested prefix and honors
  `limit` without requiring the caller to enumerate preceding keys;
- values cannot be overwritten through `Create`;
- the library's authority has no way to delete keys. Accordingly, `Store`
  intentionally has no update or delete method.

The store adapter owns backend-specific transport retries and maps native
not-found/already-exists results to the sentinel errors above. After any other
`Create` error, the core may call `Get(key)` to distinguish an ambiguously
successful creation from a definite failure, but it must not assume every error
is retryable.

A backend with stale `List` can eventually make progress because a creation of
the stale next revision will lose with `ErrExists`, but it cannot provide the
specified linearizable `Head` API. Therefore eventual-list consistency is not a
conforming implementation, even though a future relaxed adapter/API could make
that tradeoff explicit.

The first adapter should target a general-purpose Amazon S3 bucket:

- `List` uses `ListObjectsV2` and requests `MaxKeys=limit`;
- `Get` uses `GetObject` and returns its response body as an `io.ReadCloser`;
- `Create` uses `PutObject` for small values and multipart uploads for large
  values. Both publication operations use `If-None-Match: *` and map HTTP 412
  to `ErrExists`; HTTP 409 is retried as directed by S3. Each PUT or part gets a
  section reader and an explicit content length;
- S3 creation is atomic and subsequent GET/LIST operations are strongly
  consistent.

Keep that adapter in `lokv/s3store`, so the core `lokv` package does not
import AWS SDK types. Test stores must implement the full behavioral contract,
including sorted limited listing and conditional-create races.

Do not silently support a backend with eventually consistent LIST, unordered
LIST, partial writes, or unconditional-only PUT. `Open` should document these
preconditions. An optional adapter integration diagnostic may exercise them in a
disposable prefix, but must not be required for normal operation.

## 6. Keyspace

Normalize the caller's prefix by removing leading and trailing slashes. An empty
prefix is allowed. All package-owned objects live under `<prefix>/v1/`.

```text
<prefix>/v1/log/<reverse-revision-16hex>.json
<prefix>/v1/tree/<level-hex>/<start-16hex>-<end-16hex>-<sha256hex>.json.zst
```

Rules:

- `reverseRevision = math.MaxInt64 - revision`, formatted as exactly 16
  lowercase hexadecimal digits.
- `start` and `end` are inclusive revisions, also exactly 16 lowercase hex
  digits.
- Every nonzero level is a compressed data segment containing its full range.
  Revision bounds currently permit levels through hexadecimal `d` (13).
- Put no other object directly under `v1/log/`; otherwise it could shadow HEAD.
- A record key is deterministic per revision. A tree key is content-addressed.
- Stored values are opaque bytes; correctness must not depend on backend object
  metadata. An adapter may set content types from these suffixes, but the core
  library owns decompression.

HEAD discovery in the core is:

```go
keys, err := store.List(ctx, "<prefix>/v1/log/", 1)
```

The S3 adapter implements that call with `ListObjectsV2`, `MaxKeys=1`, and no
delimiter.

Valid commit revisions range from 1 through `MaxRevision`; a reverse suffix
encoding a revision outside that range is corrupt. The reverse encoding still
subtracts from `math.MaxInt64`, so valid suffixes range from `7fe0000000000000`
(revision `MaxRevision`) through `7ffffffffffffffe` (revision 1).
An empty result means an empty log. Otherwise the sole key must exactly match
the commit-key grammar. Decode and invert its suffix to obtain the candidate
revision, GET it, and validate that the envelope revision agrees with the key.
Malformed first results are corruption and must not be skipped.

## 7. Wire model

All envelope JSON is produced from private Go structs, not maps. Field order and
array order are part of the canonical representation. Unknown fields may be
accepted for forward compatibility, but all required fields must be present and
valid. Hex strings are lowercase and fixed width. Digests are 64 lowercase hex
characters.

### 7.1 Commit/head object

```json
{
  "format": "lokv/commit/v3",
  "revision": "0000000000000011",
  "commit_id": "66b7d24d9d8c4f519b8c126e486fa953",
  "previous": {
    "key": "v1/log/7fffffffffffffef.json",
    "record_hash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  },
  "event": [{"example": "the generic T appears here"}],
  "record_hash": "89abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234567",
  "frontier": [
    {
      "level": 1,
      "refs": [
        {
          "level": 1,
          "start": "0000000000000001",
          "end": "0000000000000010",
          "key": "v1/tree/1/0000000000000001-0000000000000010-<digest>.json.zst",
          "sha256": "<digest>",
          "first_prev_hash": "0000000000000000000000000000000000000000000000000000000000000000",
          "last_record_hash": "<record-hash-at-revision-16>"
        }
      ]
    }
  ]
}
```

For revision 1, `previous` is `null` and `frontier` is empty. The frontier covers
only records before this commit; the `event` in the commit is the final record
of this historical view. This avoids a self-reference.

The commit ID is 16 cryptographically random bytes rendered as 32 hex digits.
Generate it once per public `Append` call and reuse it through retries. It lets
the caller distinguish an ambiguously successful PUT from another writer's
winning object at the same revision.

`event` is a nonempty JSON array, embedded as `json.RawMessage`, even for a
single-item batch. Marshal each item with `json.Marshal`, then join the encoded
items with commas inside square brackets. This also gives `Log[byte]` an outer
array instead of `encoding/json`'s special base64 encoding of `[]byte`.
Marshal the whole batch before any S3 operation and reuse its bytes for all
retries. `MaxEventBytes` limits the complete array, including punctuation.

Commit and segment format markers are v3. Reject earlier envelopes: v1 did
not have batch semantics, and v2 commits could reference index-only upper
levels. Commit keys and the record hash algorithm remain unchanged. All
aggregate keys now end in `.json.zst`.

### 7.2 Logical record hash

The logical record hash is independent of frontier layout and JSON envelope
formatting:

```text
SHA-256(
  "lokv-record-v1\x00" ||
  int64_big_endian(revision) ||
  commit_id_16_bytes ||
  previous_record_hash_32_bytes ||
  uint64_big_endian(len(event_json)) ||
  event_json
)
```

For revision 1, `previous_record_hash` is 32 zero bytes. This hash chain detects
reordering, omission, and event corruption during scans. It is not a signature
and does not defend against a malicious authorized writer.

### 7.3 Reference

Every reference contains:

```go
type objectRef struct {
    Level           uint8  `json:"level"`
    Start           hexRev `json:"start"`
    End             hexRev `json:"end"`
    Key             string `json:"key"`
    SHA256          hexHash `json:"sha256"`
    FirstPrevHash   hexHash `json:"first_prev_hash"`
    LastRecordHash  hexHash `json:"last_record_hash"`
}
```

For a level-0 reference, `Key` names a commit object and `SHA256` is the SHA-256
of its exact stored bytes. For a tree reference, `SHA256` is the SHA-256 of the
canonical, uncompressed aggregate JSON. Range and boundary hashes allow readers
to validate adjacency without opening unrelated siblings.

### 7.4 Packed segment at every nonzero level

A level-L segment contains exactly `16^L` record projections in revision order.
For example, a level-1 segment contains 16:

```json
{
  "format": "lokv/segment/v3",
  "level": 1,
  "start": "0000000000000001",
  "end": "0000000000000010",
  "records": [
    {
      "revision": "0000000000000001",
      "commit_id": "<32 hex>",
      "previous_record_hash": "<64 hex>",
      "record_hash": "<64 hex>",
      "event": [{"example": 0}]
    }
  ]
}
```

Stream canonical JSON through a hasher and zstd encoder into a temporary file.
Use Go 1.27's json/v2 and jsontext APIs to process one record at a time. Preserve
v1 JSON semantics for record projections, including raw event escapes and
number spellings. The uncompressed digest determines the object key; the
compressed file's counted length supplies Size() for upload. Use one fixed zstd
configuration: default compression level, one encoder worker, a 1 MiB window,
and checksums. The decoder bounds the window but imposes no output-size limit.

Each higher-level segment contains all records from 16 preceding segments.
There are no child references inside it: a reader downloads one object for the
whole range, without reading the original commits or lower-level segments.
Every record retains its original event bytes, commit ID, and hash.

Compaction copies payloads at each completed level. Old objects remain stored
for historical snapshots. This increases retained bytes and compaction work
with the number of levels, while reducing full scans to the frontier objects.
Object sizes are unbounded. Transfers, record arrays, and zstd encoding/decoding
are streamed. Commits and individual batches remain buffered, but higher-level
segments never materialize whole-range byte arrays or projection slices.

Temporary files live in `Config.TempDir`, or `os.TempDir` when it is empty.
On non-Windows systems, unlink each file immediately after creation and use its
open descriptor. On Windows, close it before removing its name. All success,
failure, and cancellation paths clean up owned files and response streams.
File and response close errors are reported, including on otherwise successful
operations. Failed cleanup can leave named files on Windows; inspect and clean
the application's scratch directory after addressing the filesystem error.

Downloads spool compressed bytes to a file, check zstd framing using fixed-size
ReadAt calls, then decode and hash the stream while writing projections to a
private record spool. That spool uses JSON values separated by newlines; it is
scratch data, not the wire format. Validate the complete envelope, record chain,
and digest before returning the spool to consumers. Scans replay it one batch
at a time without another Store Get. Memory is proportional to batch sizes and
codec buffers; temporary disk usage is proportional to processed ranges.

## 8. Public Go API

The API below specifies behavior, not necessarily exact documentation wording.

```go
package lokv

type Config struct {
    Prefix string

    Store Store

    // Defaults: 32 retries, 1 MiB JSON array for new append batches.
    // Reads and compaction of stored data have no byte limit.
    MaxConflictRetries int
    MaxEventBytes      int64
}

type Log[T any] struct { /* private */ }

// MaxRevision is also the maximum record count, capped at JavaScript's
// Number.MAX_SAFE_INTEGER so revisions remain exact in JavaScript Numbers.
const MaxRevision int64 = 1<<53 - 1

func Open[T any](cfg Config) (*Log[T], error)

// CommitID identifies an append invocation and is reused on retries.
type CommitID [16]byte

// RecordHash is a record's logical SHA-256 hash, linking it to its history.
type RecordHash [32]byte

type Record[T any] struct {
    Revision   int64 // Valid from 1 through MaxRevision.
    CommitID   CommitID
    Value      []T // Nonempty batch, in append argument order.
    RecordHash RecordHash
}

// Head returns (zero, false, nil) for an empty log.
func (lg *Log[T]) Head(ctx context.Context) (_ Record[T], ok bool, _ error)

// Append atomically commits a batch at one revision, retrying conflicts.
func (lg *Log[T]) Append(ctx context.Context, value ...T) (Record[T], error)

// Snapshot is an opaque loaded historical root. A nil Snapshot means empty.
type Snapshot[T any] struct { /* exported accessors, private frontier */ }

func (lg *Log[T]) LoadHead(ctx context.Context) (*Snapshot[T], error)
func (lg *Log[T]) LoadRevision(ctx context.Context, revision int64) (*Snapshot[T], error)

// AppendTo attempts exactly one successor of base. It returns ErrConflict if
// another writer wins. This avoids an extra LIST/GET when a caller already owns
// a fresh snapshot.
func (lg *Log[T]) AppendTo(ctx context.Context, base *Snapshot[T], value ...T) (*Snapshot[T], error)

type Range struct {
    First int64 // inclusive, in [1, MaxRevision]
    Last  int64 // inclusive, in [1, MaxRevision]
}

func All() Range                      { return Range{1, MaxRevision} }
func StartingAt(revision int64) Range { return Range{revision, MaxRevision} }

// After excludes revision. After(0) is All(); After(MaxRevision) is empty.
func After(revision int64) Range

// Scan visits the intersection of r and snap in increasing revision order.
// It stops immediately on a callback error. Invalid bounds return ErrRange;
// the zero Range is empty. Valid ranges on an empty snapshot or starting past
// its head yield nothing.
func (lg *Log[T]) Scan(ctx context.Context, snap *Snapshot[T], r Range,
    yield func(Record[T]) error) error

// Verify performs a full structural, digest, range, and record-chain scan.
func (lg *Log[T]) Verify(ctx context.Context, snap *Snapshot[T]) error

var (
    ErrConflict   = errors.New("lokv: append conflict")
    ErrCorrupt    = errors.New("lokv: corrupt log")
    ErrRange      = errors.New("lokv: invalid range")
    ErrTooLarge   = errors.New("lokv: append batch too large")
    ErrExhausted  = errors.New("lokv: revision space exhausted")
    ErrEmptyBatch = errors.New("lokv: empty batch")
)
```

`Snapshot` should expose `Record() Record[T]`, `Revision() int64`, and perhaps
`Empty() (empty bool)`, but not mutable frontier slices. A nil snapshot reports revision 0.
`LoadRevision` rejects revisions outside `[1, MaxRevision]` with `ErrRange`.
Returning an opaque snapshot allows `AppendTo` to reuse the already fetched
commit body safely. `Scan` with `All()` visits every record in the snapshot;
`StartingAt(revision)` visits records from that revision through the snapshot's
end. `After(revision)` excludes that revision, for resuming after an application
checkpoint. `After(0)` covers the whole log and `After(MaxRevision)` returns the
empty zero `Range`. Invalid inputs produce invalid ranges, rejected by `Scan`.
None of these helpers waits for new records.

`Scan` yields one whole batch per callback. Applications apply every item before
advancing their checkpoint; if updates can fail or readers share the index, use
an application transaction or lock to publish the whole batch consistently.

`Open` rejects a normalized prefix longer than 906 UTF-8 bytes, reserving 118
bytes for the longest generated key suffix within S3's 1024-byte key limit.
This applies to every store and is checked before I/O, so aggregate keys cannot
first exceed the key limit during compaction.

`Open` rejects a nil store, malformed prefix, negative batch limits, and negative
retry counts before performing I/O. Backend-specific configuration such as S3
bucket, region, credentials, and SDK client belongs to the adapter constructor,
not `lokv.Config`.

## 9. Append algorithm

### 9.1 Empty log

1. Reject an empty batch with `ErrEmptyBatch`. Marshal the batch's items into an
   array; reject marshal errors and size violations before store I/O.
2. Generate one commit ID.
3. Build revision-1 commit with no predecessor and empty frontier.
4. `Store.Create(logKey(1), body)`.
5. Success commits revision 1. `ErrExists` means another writer won; `Append` loads
   the new head and retries while `AppendTo(nil, ...)` returns `ErrConflict`.
6. For an ambiguous transport error, GET `logKey(1)`. Matching commit ID means
   success; a different existing commit means conflict; absence means retry the
   same conditional creation subject to context and retry policy.

### 9.2 Non-empty log

The loaded head at revision `R` has a frontier covering `[1,R)`. The new commit
will be revision `R+1`; first insert the old head as a level-0 reference so the
new frontier covers `[1,R+1)`.

```text
carry = ref(oldHead)
frontier = deepCopy(oldHead.frontier)

for level = 0; ; level++ {
    frontier[level].append(carry)
    if len(frontier[level]) < 16 {
        break
    }

    children = frontier[level]       // exactly 16, chronological
    frontier[level] = empty

    carry = createPackedSegment(level+1, children)
}

validateCanonicalFrontier(frontier, R+1)
newCommit = commit(revision=R+1, previous=oldHead, frontier=frontier, event=batch)
store.Create(logKey(R+1), newCommit) // publish last
```

Creating a packed segment requires all records from 16 children. At level 1,
children are raw commits; the prior head body is already loaded and reused. At
higher levels, children are packed segments. Process four children at a time,
using at most four concurrent GETs and decoding each into a validated private
record spool. Replay the group in range order through a streaming JSON encoder,
hasher, and zstd encoder into the temporary upload file. Close these input
spools before fetching the next group. This bounds expanded scratch data to
four children, rather than all sixteen. Validate record count, adjacency, and
hash boundaries before publication; no whole-range projection slice or JSON
byte slice is built. A disk or cleanup error stops the carry before the new
commit is published, even if a completed aggregate has already been created.

Aggregate writes are also conditional creates. If `Create` returns `ErrExists`, GET
and validate it, then treat it as success. Concurrent writers based on the same
head construct the same carry aggregates, so they normally converge on identical
content-addressed keys. The final deterministic revision key selects one whole
batch.

On `ErrExists` for the final commit:

- `AppendTo` returns `ErrConflict`;
- `Append` checks whether the object at the attempted revision has its commit
  ID. If yes, the prior response was ambiguously successful and Append returns
  it. Otherwise it loads current HEAD and retries the same event bytes and
  commit ID at the next revision, up to `MaxConflictRetries`.

The store adapter owns backend-specific retry classification. In particular,
the S3 adapter retries HTTP 409 as AWS directs. The core must never fall back to
an unconditional write. Respect context cancellation and apply bounded jittered
backoff only to errors the adapter identifies as retryable.

### 9.3 Crash behavior

- Crash before any PUT: no effect.
- Crash after aggregate creations but before commit creation: no new revision is visible.
  Deterministic carry objects will normally be reused by the winning or retried
  append; otherwise they are harmless unreachable objects.
- Successful commit creation: the append is committed because every referenced
  aggregate was written first.
- Lost success response: commit-ID verification resolves the ambiguity.

Never publish a commit and then fill in its dependencies.

## 10. Read and scan algorithms

To read a snapshot in chronological order:

1. Validate the commit envelope and key.
2. Visit nonempty frontier levels from highest to lowest (at most 13 down to 0).
3. Within each level, visit references in stored order.
4. GET each nonzero-level segment and yield its complete records in order.
   Do not fetch any lower-level objects for that segment.
5. GET/yield any level-0 commit references.
6. Yield the snapshot commit's own record last.

For a range scan, compare the requested inclusive range with each reference's
`start..end` and skip disjoint ranges. A fully included segment uses one GET.
For an intersection that includes the segment's end, compare reading the packed
segment with loading the raw commit at its end. That historical commit's
frontier splits the range into fifteen objects at each lower level, followed
by the commit's own record.
Recursively choose cheaper representations for the intersecting smaller ranges.
An intersection ending before the segment's end uses the packed object: its
digest anchors the requested records without needing a separate chain proof.
This uses deterministic commit keys and requires no additional LIST operations.
Every historical root remains at or below the original snapshot's revision.

Choose the representation before I/O. The private cost estimate charges one
head-sized batch plus 64 bytes of compressed metadata per record, 384 bytes
per raw commit frontier reference, and a 64 KiB allowance per GET for request
overhead. Frontier reference counts follow directly from revision digits, so
planning needs no metadata requests. These estimates consider both transfer
and request costs but cannot know actual historical batch sizes, compression,
or network latency. They are a heuristic, not a minimum-byte guarantee.

Before using a historical root, validate its commit and canonical frontier,
check its record hash against the original segment's last-record hash, and
check that its smaller refs reproduce the segment's interval and first/last
hash boundaries. Raw refs retain their object-digest checks. The historical
frontier itself is not covered by its record's logical hash, so spool the
selected suffix and validate every record link through the original segment's
known last-record hash before exposing it to callbacks. The single end record
needs no spool because its logical hash is already the known anchor. Each
fetched packed segment is also validated in full before use. Only requested
revisions are emitted, in order. Callback failures close all owned spools.

Normal `Scan` validates object digests when a reference supplies them, plus
format, level, range, internal adjacency, record hashes, and the hash-chain
boundary between successive yielded records. Historical end commits are
anchored by their logical record hash. `Verify` always walks the original
packed snapshot and checks every canonical-frontier invariant. Adaptive scans do not verify
representations they did not read. A missing or corrupt object in the chosen
path fails the operation; never switch representations to hide the error.

Historical read is straightforward: `LoadRevision(R)` GETs the deterministic
commit key and uses its embedded frontier. It never consults current HEAD.

## 11. Concurrency semantics

The design supports optimistic multi-writer serialization without an external
sequencer:

1. Writers A and B load revision `R`.
2. Both create the same carry aggregates, if any.
3. Both conditionally PUT deterministic key `logKey(R+1)` with different events.
4. Exactly one creation succeeds. That PUT is the linearization point.
5. The loser either returns `ErrConflict` (`AppendTo`) or reloads and retries
   (`Append`).

This depends on every writer using deterministic next-revision keys and
conditional creation. A principal that directly writes revision `R+1000` can
make an invalid object sort as HEAD; readers must fail closed when validation
finds the gap. IAM cannot enforce revision arithmetic.

`Append` offers at-most-one committed record for one in-memory invocation,
including ambiguous network responses, because it retains one commit ID. If the
caller loses process state and invokes Append again, duplication is possible.
Applications needing durable exactly-once semantics must put a stable event ID
inside `T` and deduplicate while applying the log, or use an external transaction
system. Do not add an `event-id -> revision` S3 index in v1 because it would add
a common-case PUT and still would not provide a multi-key transaction.

## 12. Storage and request cost

For `N` appended batch records (regardless of the number of items per batch):

- commit creations: `N`;
- level-L segment creations: `floor((N-1)/16^L)`;
- total object creations approach `16N/15`;
- original event JSON is stored in its commit and copied into a segment at
  each completed level; retained payload and compaction work grow with levels;
- one current head/frontier is discoverable with one LIST plus one GET;
- a full scan fetches each frontier object exactly once, plus the head;
- for `N > 0`, the total GET count is `1 + sum(hex digits of N-1)`;
- at `N = 65537`, one level-4 segment plus the head needs two GETs;
- at `N = 1000000`, the frontier has 15 level-4, 4 level-3, 2 level-2,
  3 level-1, and 15 level-0 references: 40 GETs including the head.

These full-scan counts exclude retries. Empty-log discovery needs only one LIST.
Partial scans can instead read historical roots and smaller ranges. At revision
65,537, a follower at 65,535 needs only commit 65,536 plus the already loaded
head's batch, instead of downloading the complete level-4 segment.

The permanent metadata cost of copying a bounded frontier into each commit is
linear in `N`, not quadratic. Actual byte cost should be benchmarked because a
near-maximum 195-reference frontier can be tens of kilobytes per append.

### Local measurements and capacity planning

Reproduce the catch-up comparison and compaction resource measurements with:

```sh
go test . -run '^$' -bench '^BenchmarkCatchUp$' -benchtime=1x -benchmem
go test . -run '^$' -bench '^BenchmarkStreamingCompaction$' -benchtime=1x -benchmem
```

These September 12, 2026 measurements used Go 1.27 on Linux/amd64, an Intel
Xeon 6975P-C, and GOMAXPROCS=16. Each case ran once; timing and sampled peak heap
are illustrative, not deployment guarantees. Fixture generation is outside
the measured interval. Catch-up uses an in-memory Store and real scratch files;
compaction uses a file-backed Store, with both source and scratch data eligible
for the OS page cache. Neither benchmark includes S3 latency or throttling.

The catch-up fixture has 65,537 two-integer batch records. Costs below exclude
the head lookup. The packed baseline downloads and validates the whole level-4
object even when it only needs a suffix:

| New batches | Adaptive GETs | Adaptive bytes | Packed GETs | Packed bytes |
| --- | ---: | ---: | ---: | ---: |
| 1 | 0 | 0 | 0 | 0 |
| 2 | 1 | 25,321 | 1 | 3,280,023 |
| 3 | 2 | 50,294 | 1 | 3,280,023 |
| 17 | 16 | 363,355 | 1 | 3,280,023 |
| 257 | 31 | 377,906 | 1 | 3,280,023 |
| 65,537 (full load) | 1 | 3,280,023 | 1 | 3,280,023 |

Request savings are not the only goal: the 17- and 257-batch cases trade more
requests for fewer bytes and less decoding. Small packed ranges remain useful.
At a level-2 boundary with three new batches, the estimator keeps the single
12,661-byte packed GET instead of two raw commits totaling 23,982 bytes.

The level-4 compaction fixture contains 65,536 batches with 1,024-character
strings: 64.25 MiB of batch JSON in total. Repeated payloads contain only `x`;
random payloads encode deterministic random bytes as base64. Each writer reads
the same sixteen complete level-3 segments and creates the same level-4 object.
Two writers include the losing aggregate creation and its validation readback.
These numbers measure the large compaction step preceding commit publication,
excluding smaller carries, the final commit, and any conflict retry afterward.

| Payload | Writers | Elapsed seconds | Approx. CPU seconds | Store read MiB | Store write MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Repeated | 1 | 0.74 | 2.12 | 2.89 | 2.90 |
| Repeated | 2 | 1.72 | 5.93 | 8.68 | 5.80 |
| Random | 1 | 1.02 | 2.54 | 51.94 | 51.94 |
| Random | 2 | 2.11 | 6.87 | 155.82 | 103.88 |

| Payload | Writers | Peak scratch MiB | Scratch writes MiB | Sampled extra heap MiB |
| --- | ---: | ---: | ---: | ---: |
| Repeated | 1 | 22.54 | 86.23 | 25.54 |
| Repeated | 2 | 45.11 | 175.35 | 50.32 |
| Random | 1 | 71.36 | 184.32 | 27.43 |
| Random | 2 | 141.64 | 420.57 | 54.03 |

CPU values use Go runtime CPU accounting, including GC and scavenging work.
Scratch counters track bytes written and live file lengths, not filesystem
allocation blocks. Peak heap is sampled every 5 ms; it excludes the Store's
file contents and OS page cache. Total allocations remain substantial (about
700 MiB for one level-4 writer here) despite much smaller live heap. Repeated
writers duplicate decoding, compression, uploads, and scratch traffic.

Budget scratch for the growing compressed output plus four expanded child
spools and up to four compressed child downloads. With uneven child sizes,
use the largest four children, not one quarter of the whole segment. Multiply
that allowance by simultaneous appends. A scan needs its compressed download
and validated record spool; an adaptive suffix also needs a spool for its
selected records until their complete chain is checked. `Config.TempDir` can
place this work on a volume separate from the system temporary directory.

Compactions run synchronously before the next commit becomes visible. Every
additional carry level covers sixteen times as many batches; ordinary append
latency is therefore a poor basis for an append deadline. Measure the largest
expected carry on the deployment's payloads, disk, and S3 connection. Allow for
all carry levels, decoding and encoding, read and upload throughput, request
latency, SDK retries, and contention. For example, this random level-4 case
alone transfers about 104 MiB for one writer; a shared 10 MiB/s transfer budget
already implies about ten seconds, before other work and retries. Multipart
conflict retries can repeat an entire upload.

Use context deadlines with that measured margin and arrange for failed
operations to retry after the underlying resource issue is fixed. Disk-full,
temporary-file creation, and close failures return errors without publishing
the new commit; an already completed aggregate can remain for a retry to reuse.
There is no aggregate admission cap or background compactor to hide this work.
This fixture does not qualify multi-gigabyte carries or real S3 tail latency;
those remain deployment qualification work. Byte/request counters can be
collected with a Store wrapper, as these benchmarks demonstrate.

## 13. S3 adapter: IAM and bucket configuration

The S3 adapter must send `If-None-Match: *` when publishing an object, through
either `PutObject` or `CompleteMultipartUpload`. Multipart initiation and part
uploads do not publish objects and cannot use that condition. IAM is defense
in depth, not a substitute for correct adapter behavior.

Deployment requirements:

1. Use a general-purpose bucket.
2. Grant the writer only `s3:ListBucket` scoped by prefix plus `s3:GetObject` and
   conditional `s3:PutObject` on the package prefix. Grant
   `s3:AbortMultipartUpload` on the same objects for unfinished-upload cleanup.
3. Do not grant `s3:DeleteObject`, `s3:DeleteObjectVersion`, or overwrite paths.
4. Add an explicit bucket-policy deny for deletes on the prefix.
5. Add a bucket-policy deny for object creation missing `If-None-Match`, using
   `s3:ObjectCreationOperation` to exempt initiation and part uploads.
6. Do not configure lifecycle expiration on the prefix. Configure
   `AbortIncompleteMultipartUpload` to reclaim stranded uploads, with
   `DaysAfterInitiation` longer than any expected upload (for example, seven
   days). This does not expire completed objects.
7. Prefer bucket-owner-enforced object ownership and no ACL headers.
8. Optionally enable S3 Object Lock in compliance mode when immutability must
   survive credential or administrator compromise. Object Lock and conditional
   creation solve different problems; neither replaces the other.

Illustrative deny statements (replace placeholders and review in the target
account):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DenyDeletesFromS3Log",
      "Effect": "Deny",
      "Principal": "*",
      "Action": ["s3:DeleteObject", "s3:DeleteObjectVersion"],
      "Resource": "arn:aws:s3:::BUCKET/PREFIX/v1/*"
    },
    {
      "Sid": "DenyNonConditionalS3LogCreation",
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::BUCKET/PREFIX/v1/*",
      "Condition": {
        "Null": {"s3:if-none-match": "true"},
        "Bool": {"s3:ObjectCreationOperation": "true"}
      }
    }
  ]
}
```

The adapter uses single-request `PutObject` through 128 MiB and multipart above
that threshold. The threshold is private and lowered in tests. Four workers
stream parts directly from section readers over the `SizeReaderAt` source;
neither entire objects nor entire parts are buffered. Part sizes start at
64 MiB and grow to keep the upload within 10,000 parts. S3 permits parts from
5 MiB through 5 GiB, with no minimum for the final part. CRC32 part checksums
are carried into the ordered completion request as a composite checksum.
These transfer choices do not change format v3, object keys, compressed bytes,
or cold-read GET counts. Old readers can read objects uploaded with multipart.

The SDK handles transport retries using seekable part readers. A conditional
completion conflict (HTTP 409) requires aborting that upload and starting a new
one, including reuploading all parts. HTTP 412 maps to `ErrExists`. Other
completion errors, including a lost response followed by `NoSuchUpload`, are
returned for the core's existing commit-ID/hash or aggregate-digest readback
resolution. A completion error is never proof that the object was not published.

On failure or cancellation, all local part workers finish before cleanup.
Cleanup gets its own 30-second context, independent of caller cancellation.
Abort failures are joined with the original error and prevent another conflict
retry. A missing upload during abort is harmless: completion may already have
consumed it. No cleanup path deletes a completed object. Lifecycle cleanup is
still needed after crashes, lost initiation responses, and failed aborts.

Local tests use `github.com/johannesboyne/gofakes3` with the AWS SDK. Its v1.2.0
completion handler ignores `If-None-Match`, so a test HTTP shim supplies that
precondition atomically across publications. Fault injection covers part
replay, completion conflicts and embedded errors, cancellation, failed aborts,
and lost completion responses. These tests do not qualify AWS IAM enforcement
or replace the opt-in live S3 tests.

### Beyond S3's object capacity

Multipart removes the single-PUT ceiling but cannot exceed S3's physical
capacity of 10,000 parts of 5 GiB each (about 48.8 TiB). The current adapter
reports an error before starting an upload that cannot fit. This remains a
growth limit for sufficiently large compactions; multipart alone does not
complete the storage-layout work.

The proposed next layout splits a logical packed segment's byte stream into
large immutable chunks and publishes a small manifest only after every chunk
exists. The manifest records ordered chunk keys, lengths, and content hashes,
plus the logical segment's full digest and revision bounds. Readers concatenate
and validate the chunks while streaming the logical segment. Larger levels
still contain a full copy of their covered range, rather than references to
individual commits or lower levels. Cold scans need one manifest GET and one
GET per large chunk; they never expand into one GET per record.

Chunks should use content-addressed keys within the log namespace and be
created conditionally. Competing writers can reuse identical chunks. A failed
writer can strand complete chunks before manifest publication; reclaiming
those objects conflicts with the current no-delete policy, so automatic
garbage collection is not part of this proposal. Chunk naming must also fit
the existing key-length budget, or require a separately reviewed prefix change.

This proposal is not implemented and no chunk manifests are written today.
It requires a new wire version with an explicit manifest representation, not
reinterpretation of existing v3 zstd objects. New readers must retain v3 support;
old readers must reject the new version before following its references.
Finalize and test this compatibility contract before enabling chunked writes.
Batch admission limits must not be used to hide this remaining growth problem.

## 14. Limits and defensive decoding

- Allow revision `MaxRevision` (2^53 - 1), then reject further appends with
  `ErrExhausted`.
- Reject revisions outside `[1, MaxRevision]` in public lookups and range scans
  with `ErrRange`, before store I/O. The zero `Range` is an empty-range exception.
- Reject revisions outside `[1, MaxRevision]` in stored data with `ErrCorrupt`.
- Reject invalid UTF-8 only as `encoding/json` normally handles it; the exact
  marshaled bytes are authoritative.
- Reject empty append batches with `ErrEmptyBatch` before store I/O.
- Require every stored event to be a nonempty JSON array.
- Limit the complete batch's JSON array before any append I/O. The limit does
  not apply to stored data; lowering it cannot break reads or future compaction.
- Impose no compressed-object or decompressed-output byte limit.
- Reject trailing zstd streams or trailing non-whitespace JSON.
- Verify every parsed key remains under the configured normalized prefix; never
  follow absolute URLs or foreign-bucket references from stored data.
- Require exactly `16^L` ordered records in a level-L segment.
- Check all range arithmetic for overflow.
- Do not trust ETag as a content hash. Use the protocol's SHA-256 fields.
- Wrap errors with operation, bucket, key, and revision, but never include event
  contents in error strings or logs.

## 15. Observability

Do not require a logging framework. Optionally accept hooks or an interface for:

- store operations by method and result;
- append conflicts and retries;
- carry depth;
- bytes marshaled/compressed/uploaded/downloaded;
- scan records and objects read;
- validation failures.

Never log `T` or raw object bodies. Expose carry depth and request counts in
benchmarks even if production metrics hooks are deferred.

## 16. Implementation phases

### Phase 1: pure model and fake store

- key encoding/decoding;
- fixed-width hex and digest types;
- wire structs and strict validation;
- record hashing;
- in-memory `Store` fake with ordered `List` and atomic conditional `Create`;
- frontier insertion/carry planning without I/O.

### Phase 2: storage and append

- `lokv/s3store` AWS SDK v2 adapter;
- head and historical revision loading;
- empty and non-carry append;
- packed segment creation at every level;
- conflict, ambiguous-success, and retry handling.

### Phase 3: reads

- full scan;
- range pruning;
- full verification;
- bounded GET concurrency and append batch admission limits.

### Phase 4: production hardening

- fault-injection tests;
- live S3 integration test behind an environment flag;
- benchmarks and request-count assertions;
- package documentation with IAM example and backend contract.

## 17. Required tests

At minimum:

1. Reverse-key lexical ordering for revisions 1, 2, 15, 16, 17,
   `MaxRevision-1`, and `MaxRevision`; reject keys outside the allowed range.
2. Append/scan round trips for empty log and counts 1, 15, 16, 17, 255, 256,
   and 257.
3. `Create`-count assertions at all carry boundaries.
4. Frontier digits and exact range partition at those boundaries.
5. Generic JSON values: struct, map, string, number alias, slice, pointer, and
   a type implementing `json.Marshaler`/`json.Unmarshaler`.
6. Marshal failure performs zero store writes.
7. Two and at least 32 concurrent appenders yield a gap-free log containing
   every successful input exactly once.
8. `AppendTo` loses cleanly with `ErrConflict`.
9. Lost-success response is resolved by matching commit ID without duplication.
10. Fault injection after every aggregate creation leaves old HEAD valid and does not
    expose an incomplete new revision.
11. Missing object, malformed JSON, wrong key revision, bad digest, wrong level,
    non-adjacent child ranges, bad record hash, and broken hash chain all return
    errors matching `ErrCorrupt`.
12. Range scans at block edges return exactly the requested revisions and avoid
    GETs for disjoint subtrees.
13. Context cancellation stops retries and bounded concurrent GET work.
14. Reject oversized new batches before I/O; lowering the append limit must
    preserve old reads and compaction. Test large and unknown-size zstd frames.
    Verify streaming preserves canonical v3 bytes, corrupt segments never yield
    a prefix, and success/failure/cancellation paths close all temporary files.
15. A generic store conformance suite checks sorted limited listing, immediate
    create visibility, atomic values, exactly one winner, and sentinel errors.
16. A live, general-purpose S3 adapter test confirms that `LIST MaxKeys=1` sees
    a newly committed reverse key and that a second conditional PUT receives
    412/409 as appropriate.

Run unit tests with both `go test ./...` and `go test -race ./...`; race builds
reduce large packing, boundary, and crash workloads to two carry levels. A
seeded level-4 test validates packing and full-scan request counts without
building 65536 individual commits. Add fuzz targets for key parsing, commit and
segment decoding, frontier validation, and segment decompression.

## 18. Acceptance criteria

The implementation is ready to hand off when:

- package docs describe the storage contract and immutability limitations;
- `lokv/s3store` wraps the AWS SDK for Go v2 and satisfies `lokv.Store`;
- all required tests pass under the race detector;
- normal appends issue exactly one `Store.Create`;
- carries issue exactly `1 + carryDepth` object creations;
- the core package depends only on `Store` and has no AWS SDK dependency;
- no S3 adapter code path deletes or mutates completed objects or publishes an
  object unconditionally; aborts only discard unfinished multipart uploads;
- a successful commit never references an object that was not created first;
- head discovery uses one one-key forward lexical LIST;
- historical and range scans return events in strictly increasing revision order;
- corrupt or incomplete state is reported, never silently skipped;
- public APIs contain no unbounded background goroutines and honor context
  cancellation.

## 19. Design rationale and rejected alternatives

### Mutable `HEAD`

Rejected because it requires overwriting one key and violates the bucket's
create-only invariant. Reverse-revision commit keys make the commit log itself
the head index.

### Timestamp revisions

Rejected because clocks collide and move backward. A delayed writer could sort
before a previously published watermark. Revisions are protocol sequence
numbers; timestamps may live inside `T` if useful.

### Full snapshots every M records

Rejected because rewriting `[1..N]` after every fixed-size tail produces
quadratic aggregate history writes.

### Reference-only upper levels

Rejected because full scans still require approximately one data GET per 16
batches, plus index requests. Packing complete payload ranges at every level
lets clients read the frontier directly. This intentionally accepts greater
write amplification and retained storage in the create-only store.

### Index-only tree with no packed data

Rejected because a full scan would GET nearly every original commit object.

### Separate mutation and head-manifest objects

Rejected because it makes the common append two object creations. The commit
embeds both the event and prior-history frontier, so the common append is one
creation.

## 20. AWS references

- [ListObjectsV2 sorting order](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
- [PutObject conditional creation and atomic object behavior](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)
- [Enforcing conditional writes with bucket policies](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html)
- [Amazon S3 strong consistency](https://aws.amazon.com/s3/consistency/)
