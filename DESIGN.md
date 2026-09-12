<!--
Copyright (c) Tailscale Inc & contributors
SPDX-License-Identifier: BSD-3-Clause
-->

# lokv: append-only log over a sorted, create-only key/value store

Status: implementation specification\
Target language: Go\
Working package name: `lokv`\
Wire format: `lokv/v1`

## 1. Summary

lokv (Log over K/V) implements an append-only log over a sorted, create-only
key/value store (e.g. S3, if so configured). This generic Go library stores values
of type `T` in revision order. Each `T` is encoded with
`encoding/json` and embedded in an immutable commit object. Amazon S3 is the
first concrete adapter, not part of the core API.

The commit object for a record is also the root manifest for the complete log at
that revision. There is no mutable `HEAD` object. The current head is found with
one forward `Store.List` request over reverse-encoded, fixed-width revision keys:

```text
v1/log/fffffffffffffffd.json   # revision 2
v1/log/fffffffffffffffe.json   # revision 1
v1/log/ffffffffffffffff.json   # revision 0
```

Ascending lexical order therefore returns the highest revision first.

History preceding each head is represented by an immutable radix-16 frontier:

- level 0 references an individual commit/event object and covers 1 record;
- level 1 is a packed zstd segment containing 16 records;
- level `L >= 2` is a small index node containing 16 level `L-1` references and
  covers `16^L` records;
- a head has 0–15 references at each level. The reference count at level `L`
  equals hexadecimal digit `L` of the number of preceding records.

Appending usually writes only the new commit/head object. A hexadecimal carry
writes one aggregate object per carried level before publishing the commit.
The final commit `Store.Create` is the linearization point.

Best case: **1 object creation per record** (one PUT with the S3 adapter).\
Amortized: **`1 + 1/16 + 1/256 + ... = 16/15 ~= 1.0667 creations per record`**.

All objects are written with the store's atomic create-if-absent operation. For
S3, the adapter implements this using `If-None-Match: *`, and the bucket/prefix
must deny deletes and reject non-conditional object creation. No object is ever
overwritten or deleted by this library.

## 2. Goals

1. Store an arbitrary Go value `T` for which `json.Marshal` and `json.Unmarshal`
   work.
2. Give committed records a gap-free, zero-based `uint64` revision.
3. Find the latest committed revision with one `LIST` returning at most one key.
4. Use one object creation for an append that causes no radix carry.
5. Avoid repeatedly rewriting all historical event bytes.
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
: Zero-based sequence number. Revision 0 is the first record.

`commit`
: The immutable `v1/log/...json` object containing one event and the radix
  frontier for all records before it. A commit is also a historical root.

`head`
: The committed object with the greatest revision, found by reverse-key listing.

`frontier`
: A canonical partition of revisions `[0, head.revision)` into ordered blocks.

`reference`
: A key, content digest, level, and inclusive revision range naming a commit,
  segment, or index node.

Required invariants:

1. The log is empty, or committed revision keys are exactly `0..HEAD`.
2. A commit at revision `R > 0` names revision `R-1` as its predecessor.
3. A commit's frontier covers exactly `[0,R)` with no gaps or overlap.
4. A level `L` reference covers exactly `16^L` consecutive records and its
   start revision is aligned to `16^L`.
5. At most 15 references exist at any frontier level.
6. Frontier references are chronological within a level. When the whole
   frontier is read, levels are visited from highest to lowest.
7. `len(frontier[L])` equals nibble `L` of `R`.
8. Every referenced object exists and matches the key, range, level, and digest
   in its reference.
9. Aggregate objects are successfully created before a commit that references
   them is created.
10. The commit creation is conditional and is the append's linearization point.

For a `uint64` revision there are 16 frontier levels, numbered 0 through 15.
The maximum frontier contains 240 references. This bounds commit metadata even
though old commit objects are retained forever.

## 5. Store interface and required behavior

The core package depends only on this public interface:

```go
package lokv

// Store is a flat key/value namespace. Values and successful creations are
// immutable. Implementations must satisfy the consistency contract below.
type Store interface {
    // List returns at most limit keys having prefix, ordered by ascending
    // bytewise lexicographical comparison. limit must be positive.
    List(ctx context.Context, prefix string, limit int) ([]string, error)

    // Get returns the complete value for key. It returns an error matching
    // ErrNotFound when key does not exist.
    Get(ctx context.Context, key string) ([]byte, error)

    // Create atomically creates key with value if and only if key is absent.
    // It returns an error matching ErrExists if key already exists. It must
    // never overwrite an existing value and must never expose a partial value.
    Create(ctx context.Context, key string, value []byte) error
}

var (
    ErrNotFound = errors.New("lokv: object not found")
    ErrExists   = errors.New("lokv: object already exists")
)
```

All returned byte slices are owned by the caller. Implementations may copy on
return; the core must not retain or mutate a Store-owned buffer after the call.
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
- `Get` uses `GetObject` with a bounded reader;
- `Create` uses `PutObject` with `If-None-Match: *` and maps HTTP 412 to
  `ErrExists`; HTTP 409 is retried as directed by S3;
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
<prefix>/v1/tree/1/<start-16hex>-<end-16hex>-<sha256hex>.json.zst
<prefix>/v1/tree/<level-hex>/<start-16hex>-<end-16hex>-<sha256hex>.json
```

Rules:

- `reverseRevision = math.MaxUint64 - revision`, formatted as exactly 16
  lowercase hexadecimal digits.
- `start` and `end` are inclusive revisions, also exactly 16 lowercase hex
  digits.
- Level 1 is a compressed data segment. Levels 2 through `f` are uncompressed
  index nodes.
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
  "format": "lokv/commit/v1",
  "revision": "0000000000000010",
  "commit_id": "66b7d24d9d8c4f519b8c126e486fa953",
  "previous": {
    "key": "v1/log/fffffffffffffff0.json",
    "record_hash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  },
  "event": {"example": "the generic T appears here"},
  "record_hash": "89abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234567",
  "frontier": [
    {
      "level": 1,
      "refs": [
        {
          "level": 1,
          "start": "0000000000000000",
          "end": "000000000000000f",
          "key": "v1/tree/1/0000000000000000-000000000000000f-<digest>.json.zst",
          "sha256": "<digest>",
          "first_prev_hash": "0000000000000000000000000000000000000000000000000000000000000000",
          "last_record_hash": "<record-hash-at-revision-15>"
        }
      ]
    }
  ]
}
```

For revision 0, `previous` is `null` and `frontier` is empty. The frontier covers
only records before this commit; the `event` in the commit is the final record
of this historical view. This avoids a self-reference.

The commit ID is 16 cryptographically random bytes rendered as 32 hex digits.
Generate it once per public `Append` call and reuse it through retries. It lets
the caller distinguish an ambiguously successful PUT from another writer's
winning object at the same revision.

`event` is the exact output of `json.Marshal(value)`, embedded as
`json.RawMessage`. Marshal it once before any S3 operation and reuse those bytes
for all retries.

### 7.2 Logical record hash

The logical record hash is independent of frontier layout and JSON envelope
formatting:

```text
SHA-256(
  "lokv-record-v1\x00" ||
  uint64_big_endian(revision) ||
  commit_id_16_bytes ||
  previous_record_hash_32_bytes ||
  uint64_big_endian(len(event_json)) ||
  event_json
)
```

For revision 0, `previous_record_hash` is 32 zero bytes. This hash chain detects
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

### 7.4 Level-1 packed segment

A level-1 segment contains exactly 16 record projections in revision order:

```json
{
  "format": "lokv/segment/v1",
  "level": 1,
  "start": "0000000000000000",
  "end": "000000000000000f",
  "records": [
    {
      "revision": "0000000000000000",
      "commit_id": "<32 hex>",
      "previous_record_hash": "<64 hex>",
      "record_hash": "<64 hex>",
      "event": {"example": 0}
    }
  ]
}
```

Marshal the canonical JSON, hash the uncompressed bytes, then compress with
zstd. The object key uses the uncompressed digest. Readers decompress, hash the
result, compare it with the key/reference, then decode. Use one fixed zstd
configuration and impose both compressed and decompressed size limits to prevent
resource exhaustion.

Because original commit objects can never be deleted, packing duplicates each
event's bytes once. It does not rewrite event payload at higher levels. This is
intentional: historical scans need about one data GET per 16 records, while
permanent event-data write amplification stays about 2x rather than growing
with the number of levels.

### 7.5 Level-2 and higher index node

```json
{
  "format": "lokv/index/v1",
  "level": 2,
  "start": "0000000000000000",
  "end": "00000000000000ff",
  "children": [
    {"level": 1, "start": "...", "end": "...", "key": "...", "sha256": "...",
     "first_prev_hash": "...", "last_record_hash": "..."}
  ]
}
```

There are exactly 16 children, in revision order. Marshal canonical JSON and use
the SHA-256 of the exact bytes in the key. Do not compress these small nodes.

## 8. Public Go API

The API below specifies behavior, not necessarily exact documentation wording.

```go
package lokv

type Config struct {
    Prefix string

    Store Store

    // Defaults: 32 retries, 1 MiB event JSON, 64 MiB uncompressed segment,
    // 256 MiB compressed-input/decompression safety ceiling as appropriate.
    MaxConflictRetries int
    MaxEventBytes      int64
    MaxObjectBytes     int64
}

type Log[T any] struct { /* private */ }

func Open[T any](cfg Config) (*Log[T], error)

// CommitID identifies an append invocation and is reused on retries.
type CommitID [16]byte

// RecordHash is a record's logical SHA-256 hash, linking it to its history.
type RecordHash [32]byte

type Record[T any] struct {
    Revision   uint64
    CommitID   CommitID
    Value      T
    RecordHash RecordHash
}

// Head returns (zero, false, nil) for an empty log.
func (l *Log[T]) Head(ctx context.Context) (Record[T], bool, error)

// Append marshals value once, discovers HEAD, and retries optimistic conflicts.
func (l *Log[T]) Append(ctx context.Context, value T) (Record[T], error)

// Snapshot is an opaque loaded historical root. A nil Snapshot means empty.
type Snapshot[T any] struct { /* exported accessors, private frontier */ }

func (l *Log[T]) LoadHead(ctx context.Context) (*Snapshot[T], error)
func (l *Log[T]) LoadRevision(ctx context.Context, revision uint64) (*Snapshot[T], error)

// AppendTo attempts exactly one successor of base. It returns ErrConflict if
// another writer wins. This avoids an extra LIST/GET when a caller already owns
// a fresh snapshot.
func (l *Log[T]) AppendTo(ctx context.Context, base *Snapshot[T], value T) (*Snapshot[T], error)

type Range struct {
    First uint64 // inclusive
    Last  uint64 // inclusive
}

// Scan visits records in increasing revision order. It stops immediately on a
// callback error. Invalid/out-of-snapshot ranges return ErrRange.
func (l *Log[T]) Scan(ctx context.Context, snap *Snapshot[T], r Range,
    yield func(Record[T]) error) error

// Verify performs a full structural, digest, range, and record-chain scan.
func (l *Log[T]) Verify(ctx context.Context, snap *Snapshot[T]) error

var (
    ErrConflict   = errors.New("lokv: append conflict")
    ErrCorrupt    = errors.New("lokv: corrupt log")
    ErrRange      = errors.New("lokv: invalid range")
    ErrTooLarge   = errors.New("lokv: object too large")
    ErrExhausted  = errors.New("lokv: revision space exhausted")
)
```

`Snapshot` should expose `Record() Record[T]`, `Revision() uint64`, and perhaps
`Empty() bool`, but not mutable frontier slices. Returning an opaque snapshot
allows `AppendTo` to reuse the already fetched commit body safely.

`Open` rejects a nil store, malformed prefix, invalid size limits, and negative
retry counts before performing I/O. Backend-specific configuration such as S3
bucket, region, credentials, and SDK client belongs to the adapter constructor,
not `lokv.Config`.

## 9. Append algorithm

### 9.1 Empty log

1. Marshal `T`; reject marshal errors and size violations before store writes.
2. Generate one commit ID.
3. Build revision-0 commit with no predecessor and empty frontier.
4. `Store.Create(logKey(0), body)`.
5. Success commits revision 0. `ErrExists` means another writer won; `Append` loads
   the new head and retries while `AppendTo(nil, ...)` returns `ErrConflict`.
6. For an ambiguous transport error, GET `logKey(0)`. Matching commit ID means
   success; a different existing commit means conflict; absence means retry the
   same conditional creation subject to context and retry policy.

### 9.2 Non-empty log

The loaded head at revision `R` has a frontier covering `[0,R)`. The new commit
will be revision `R+1`; first insert the old head as a level-0 reference so the
new frontier covers `[0,R+1)`.

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

    if level == 0 {
        carry = createPackedSegment(children)
    } else {
        carry = createIndexNode(level+1, children)
    }
}

validateCanonicalFrontier(frontier, R+1)
newCommit = commit(revision=R+1, previous=oldHead, frontier=frontier, event=T)
store.Create(logKey(R+1), newCommit) // publish last
```

Creating a packed segment requires the 16 raw commit bodies. The prior head body
is already loaded; fetch any other needed level-0 children concurrently with a
bounded worker group. Decode and validate each before packing only its record
projection. A long-lived single writer may keep a small validated raw-tail cache,
but correctness must not depend on it.

Aggregate writes are also conditional creates. If `Create` returns `ErrExists`, GET
and validate it, then treat it as success. Concurrent writers based on the same
head construct the same carry aggregates, so they normally converge on identical
content-addressed keys. The final deterministic revision key selects one event.

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
2. Visit nonempty frontier levels from 15 down to 0.
3. Within each level, visit references in stored order.
4. Recursively expand index nodes in child order.
5. Decode level-1 segments and yield their records in order.
6. GET/yield any level-0 commit references.
7. Yield the snapshot commit's own record last.

For a range scan, compare the requested inclusive range with each reference's
`start..end` and skip disjoint subtrees. A partially intersecting level-1
segment is decompressed once and filtered by revision.

Normal `Scan` validates every fetched object's content digest, format, level,
range, internal adjacency, record hashes, and the hash-chain boundary between
successive yielded records. `Verify` additionally walks the entire snapshot and
checks every canonical-frontier invariant. Do not silently skip corrupt data or
fall back to raw objects unless a separately named recovery API is added later.

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

For `N` appended records:

- commit creations: `N`;
- level-1 segment creations: approximately `N/16`;
- level-2 node creations: approximately `N/256`;
- higher levels: `N/4096`, and so on;
- total object creations approach `16N/15`;
- original event JSON is stored once in its commit and once in a level-1 packed
  segment after the containing group closes;
- higher levels copy references only, not event bytes;
- one current head/frontier is discoverable with one LIST plus one GET;
- a full old-history data scan uses approximately `N/16` segment GETs plus a
  much smaller number of index-node GETs and up to 16 raw/head GETs.

The permanent metadata cost of copying a bounded frontier into each commit is
linear in `N`, not quadratic. Actual byte cost should be benchmarked because a
near-maximum 240-reference frontier can be tens of kilobytes per append.

## 13. S3 adapter: IAM and bucket configuration

The S3 adapter must always send `If-None-Match: *`; IAM is defense in depth, not
a substitute for correct adapter behavior.

Deployment requirements:

1. Use a general-purpose bucket.
2. Grant the writer only `s3:ListBucket` scoped by prefix plus `s3:GetObject` and
   conditional `s3:PutObject` on the package prefix.
3. Do not grant `s3:DeleteObject`, `s3:DeleteObjectVersion`, or overwrite paths.
4. Add an explicit bucket-policy deny for deletes on the prefix.
5. Add a bucket-policy deny for object-creation PUTs missing `If-None-Match`.
6. Do not configure lifecycle expiration on the prefix.
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

Keep objects below the single-`PutObject` limit and do not use multipart upload
in v1. This keeps conditional-create behavior and failure handling simple.

## 14. Limits and defensive decoding

- Reject append at revision `math.MaxUint64` with `ErrExhausted`.
- Reject invalid UTF-8 only as `encoding/json` normally handles it; the exact
  marshaled bytes are authoritative.
- Limit event JSON before upload.
- Limit commit/node response bodies while reading.
- Limit both compressed and decompressed segment sizes and reject trailing zstd
  streams or trailing non-whitespace JSON.
- Verify every parsed key remains under the configured normalized prefix; never
  follow absolute URLs or foreign-bucket references from stored data.
- Require exactly 16 children in aggregate objects.
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
- packed segment and higher index creation;
- conflict, ambiguous-success, and retry handling.

### Phase 3: reads

- full scan;
- range pruning;
- full verification;
- bounded concurrency and size limits.

### Phase 4: production hardening

- fault-injection tests;
- live S3 integration test behind an environment flag;
- benchmarks and request-count assertions;
- package documentation with IAM example and backend contract.

## 17. Required tests

At minimum:

1. Reverse-key lexical ordering for revisions 0, 1, 15, 16,
   `MaxUint64-1`, and `MaxUint64`.
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
14. Decoder limits reject oversized and decompression-bomb inputs.
15. A generic store conformance suite checks sorted limited listing, immediate
    create visibility, atomic values, exactly one winner, and sentinel errors.
16. A live, general-purpose S3 adapter test confirms that `LIST MaxKeys=1` sees
    a newly committed reverse key and that a second conditional PUT receives
    412/409 as appropriate.

Run unit tests with `go test -race ./...`. Add fuzz targets for key parsing,
commit/node decoding, frontier validation, and segment decompression.

## 18. Acceptance criteria

The implementation is ready to hand off when:

- package docs describe the storage contract and immutability limitations;
- `lokv/s3store` wraps the AWS SDK for Go v2 and satisfies `lokv.Store`;
- all required tests pass under the race detector;
- normal appends issue exactly one `Store.Create`;
- carries issue exactly `1 + carryDepth` object creations;
- the core package depends only on `Store` and has no AWS SDK dependency;
- no S3 adapter code path calls delete, copy, tagging mutation, or unconditional
  PUT;
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

Rejected because rewriting `[0..N]` after every fixed-size tail produces
quadratic aggregate history writes.

### Physical LSM compaction at every level

Rejected because the bucket never deletes old objects. Recopying event payload
at every level would permanently retain every duplicate. This design packs event
bytes only once at level 1 and uses reference-only nodes above it.

### Index-only tree with no packed leaf

Viable and even cheaper in bytes, but a full scan would still GET nearly every
original commit object. Packing groups of 16 gives a useful GET reduction for
one bounded extra copy of each event.

### Separate mutation and head-manifest objects

Rejected because it makes the common append two object creations. The commit
embeds both the event and prior-history frontier, so the common append is one
creation.

## 20. AWS references

- [ListObjectsV2 sorting order](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
- [PutObject conditional creation and atomic object behavior](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)
- [Enforcing conditional writes with bucket policies](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html)
- [Amazon S3 strong consistency](https://aws.amazon.com/s3/consistency/)
