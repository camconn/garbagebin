# Garbage Bin: Single-Node S3 Design

Garbage Bin (garbagebin, binary gb) is a single-process, S3-compatible object store that runs as one Docker container for local development and service integration tests. It is a stand-in for real S3, in the spirit of s3mock: small, fast, boring, and never more than one node.

## Goals

The service exists so that any service which talks to S3 can be integration-tested with `docker run` and nothing else. Every design choice follows one principle: **behave exactly like S3 on the wire, and be as simple as possible behind it.**

- **Wire-faithful.** Unmodified AWS SDKs (boto3, aws-sdk-go-v2, aws-sdk-js v3, aws-sdk-java, the AWS CLI) work against it with only an endpoint override. No custom client, no special flags.
- **Single node, forever.** One process, one port, one data directory. There is no cluster mode and there never will be.
- **Fast to start, small to ship.** Sub-second cold start, image under 20 MB, so it can be spun up per test class or per test.
- **Zero-config default.** `docker run -p 9000:9000 <image>` is a working S3 endpoint. Configuration is additive, never required.
- **Deterministic and disposable.** In-memory by default so every test run starts clean; a filesystem backend when state needs to survive a restart.

## In scope

The first release implements the operations real SDKs call during ordinary object storage work, and nothing else. The rule for adding an operation: an SDK smoke test fails without it.

| Area | Operations | Notes |
| --- | --- | --- |
| Buckets | CreateBucket, DeleteBucket, HeadBucket, ListBuckets | Names validated per S3 rules; delete refused on non-empty bucket |
| Objects | PutObject, GetObject, HeadObject, DeleteObject, DeleteObjects, CopyObject | `Range`, `If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since` on GET/HEAD |
| Listing | ListObjectsV2, ListObjects (v1) | prefix, delimiter, CommonPrefixes, start-after / marker, continuation tokens, max-keys, encoding-type=url |
| Multipart | CreateMultipartUpload, UploadPart, UploadPartCopy, CompleteMultipartUpload, AbortMultipartUpload, ListParts, ListMultipartUploads | SDKs switch to multipart automatically above \~8 MB, so this is non-optional |
| Metadata | Content-Type, Content-Encoding, Cache-Control, Content-Disposition, `x-amz-meta-*`, ETag, Last-Modified | Stored verbatim and returned on GET/HEAD; `x-amz-metadata-directive` on copy |
| Versioning | PutBucketVersioning, GetBucketVersioning, ListObjectVersions; `versionId` on GetObject, HeadObject, DeleteObject, DeleteObjects, the object tagging operations and copy sources | `Enabled` and `Suspended` with AWS's null-version semantics; delete markers; `x-amz-version-id`, `x-amz-delete-marker` and `x-amz-copy-source-version-id` headers. MFA delete is refused (`NotImplemented`) |
| Object Lock | PutObjectLockConfiguration, GetObjectLockConfiguration, PutObjectRetention, GetObjectRetention, PutObjectLegalHold, GetObjectLegalHold; `x-amz-bucket-object-lock-enabled` on CreateBucket; `x-amz-object-lock-mode`, `-retain-until-date` and `-legal-hold` on PutObject, CopyObject and CreateMultipartUpload, returned on GET/HEAD | GOVERNANCE and COMPLIANCE retention, legal hold, bucket default retention, `x-amz-bypass-governance-retention`. Enforced when a version is permanently deleted; delete markers and overwrites are always allowed. Retention is judged by a clock that `S3_NOW` can fix (see Container and configuration) |
| Tagging | PutObjectTagging, GetObjectTagging, DeleteObjectTagging, PutBucketTagging, GetBucketTagging, DeleteBucketTagging | A string map on the metadata row; x-amz-tagging (URL-query-encoded) on PUT and x-amz-tagging-directive on copy. Half a day of work, no interaction with anything else |
| Auth | SigV4 header and query (presigned URL) signing; path-style and virtual-host addressing | Lenient by default, strict verification behind a flag (see Key design decisions) |
| Uploads | `aws-chunked` streaming bodies, `Expect: 100-continue`, `Content-MD5`, `x-amz-checksum-*` (CRC32, CRC32C, CRC64NVME, SHA1, SHA256, MD5, SHA512, XXHASH64, XXHASH3, XXHASH128) | These are SDK defaults, not edge cases |
| Errors | S3 error XML with the correct HTTP status and `Code` for every implemented operation | `NoSuchBucket`, `NoSuchKey`, `BucketNotEmpty`, `InvalidPart`, `EntityTooSmall`, `PreconditionFailed`, ... |
| Backends | `memory` (default) and `fs` (`/data` volume) | Same behaviour, chosen at startup |
| Operability | `/health` endpoint, structured request log, `--init-buckets` to pre-create buckets | So a test harness needs no setup step |

## Out of scope

Two tiers. **Deferred** features are designed for but not built until a real consumer asks; the storage layout already makes them possible. **Never built** features are where object stores get complicated, and none is needed to test a service that reads and writes objects.

**Deferred to a later release**

Nothing at present; planned work is tracked in [TODO/](../TODO/), and known differences from AWS in [known-differences.md](known-differences.md).

**Never built**

| Never built | Why |
| --- | --- |
| Multi-node, clustering, replication, erasure coding | The whole point is one node. Durability is the test harness's problem, not ours. |
| Lifecycle rules, expiration, storage classes, Glacier restore | Time-based background behaviour has no place in a deterministic test fixture. `x-amz-storage-class` is accepted and echoed so SDK calls succeed; nothing is tiered. |
| IAM, bucket policies, ACLs, Object Ownership, block-public-access | A second product, not a feature: principals and session tokens (so STS), a JSON policy evaluator with deny-wins and condition keys, bucket policies reconciled with ACLs. A fake would be wrong in exactly the cases someone tries to test. Strict SigV4 with one key pair covers "does my service sign correctly"; if per-principal isolation is ever needed, the answer is a static key-pair-to-bucket allow-list in config, not IAM. |
| Server-side encryption (SSE-S3, SSE-KMS, SSE-C) | Headers are accepted and echoed so SDK calls succeed, and objects without them report SSE-S3 (`AES256`) as AWS's default encryption does; nothing is actually encrypted. |
| Event notifications (SNS, SQS, Lambda, EventBridge) | Would require emulating three other services. |
| Cross-region replication, Transfer Acceleration, Access Points, Multi-Region Access Points | Multi-node or multi-service by definition. |
| Static website hosting, CORS configuration, redirects | Different product surface; use a real web server. |
| Select, S3 Tables, Metadata tables, inventory, analytics, metrics | Query and reporting layers on top of storage. |
| Requester Pays, billing, quotas | No money changes hands. |
| Admin UI, web console | `aws s3 ls` is the UI. |
| Performance as a goal beyond "fast enough for tests" | Correctness and startup time are what matter. No benchmarks against MinIO. |

**Rejected alternatives to building this at all.** MinIO is a full distributed system with a large image and a licence that changed; LocalStack emulates all of AWS and starts slowly; s3mock is JVM-based and heavy for a sidecar; gofakes3 is unmaintained and misses multipart edge cases. A purpose-built single binary is smaller than any of them and has no mode we do not test.

## Key design decisions

Rust with the `s3s` crate for the protocol layer, a pluggable storage trait with two backends, blobs on disk addressed by content hash, metadata in an embedded KV store, and lenient auth by default.

| Decision | Choice | Rejected | Why |
| --- | --- | --- | --- |
| Language | Rust | Go | Not the runtime: both give a \~10 MB static binary in a `scratch` image. The deciding factor is `s3s`, a maintained crate that implements S3 routing, SigV4, XML (de)serialisation, aws-chunked decoding and multipart plumbing behind a trait. Go has no maintained equivalent; the choice there is hand-rolling the protocol or forking gofakes3. |
| Protocol layer | `s3s` + `hyper`/`tokio` | Hand-written HTTP + XML | Reimplementing S3's wire quirks is the single largest source of SDK incompatibility bugs. Let the crate own them. |
| Storage abstraction | One `Storage` trait, two impls: `memory`, `fs` | Single hard-coded backend | Tests want a clean slate per run (memory); some suites want data to survive a container restart (fs). The trait keeps both behaviourally identical. |
| Blob layout on disk | Content-addressed: `/data/blobs/ab/cd/<sha256>` | Mirror the key as a filesystem path | Keys can contain `/`, `..`, NUL, be 1024 bytes long, or differ only in case. Mapping them to paths is a bug farm. Hash-named blobs also give free de-dup and make CopyObject a metadata-only operation. |
| Metadata store | Embedded KV (`redb`), one table keyed `bucket\0key` | SQLite; JSON sidecar files | Needs ordered iteration for prefix listing and atomic multi-key writes. `redb` is pure Rust, zero-config, ACID, and ordered. SQLite works but adds C and schema migrations. Sidecar files cannot do ordered listing without a full scan. |
| Write atomicity | Temp file → fsync → rename → metadata commit | Write in place | A crashed PUT must leave either the old object or the new one, never a torn one. Rename is atomic on every filesystem we care about. |
| Concurrency | Per-key async mutex; reads lock-free via the KV snapshot | Global lock | A global lock would serialise a test suite's parallel uploads. Per-key is enough because S3 semantics are per-object anyway. |
| ETag | MD5 of body for single PUT; `md5(concat(part_md5s))-N` for multipart | Random UUID | SDKs and tools (rclone, s3cmd) compare ETags to local MD5s. Matching AWS's formula is what makes them pass. |
| Auth | Accept any credentials by default; `--strict-auth` verifies SigV4 against a configured key pair | Always strict | Most tests want to not think about credentials. Strict mode exists to catch signing bugs (clock skew, canonical request errors) when a team wants that coverage. |
| Addressing | Both path-style and virtual-host style | Path-style only | SDK defaults differ by version. Virtual-host needs a wildcard DNS or `--host bucket.localhost`; both handled. |
| Config surface | Env vars and flags only, no config file | YAML config | A container gets its config from its environment. One source of truth. |
| Multi-tenancy | None | Per-key-pair namespaces | One namespace. Isolation between tests is achieved by separate containers or separate buckets. |

## Architecture

A request passes through four layers in one process: the protocol crate turns HTTP into typed operations, the service core applies S3 semantics, and a storage trait hides which backend holds the bytes.

&#91;embedded content: request path · 4 layers, 2 backends\]

The protocol layer and the service core never know which backend is active; the memory and fs backends pass the same conformance suite, so a test that works against one works against the other.

**Error bodies.** One function renders every S3 error body, because s3s's serializer emits only `Code`, `Message` and `RequestId`. The service returns errors to s3s as usual and also leaves them in a request extension; the server renders that, adding `Key`/`BucketName` (from the request path, or from the error when it is about something else, like a copy source), `RequestId` and `HostId`. Errors s3s raises before the service runs (auth, routing, request parsing) are read back from s3s's body, since s3s offers no hook for them.

## Storage layout and write path

A PUT is durable and atomic at the point the metadata row commits; before that, a crash leaves the previous object intact and at most one orphan blob to sweep.

**On-disk layout (fs backend)**

```
/data/
  meta.redb                     # one redb file: object versions, buckets, multipart uploads
  blobs/
    ab/cd/abcd0123...<sha256>   # immutable, content-addressed, fanned out 2 levels
  tmp/
    <uuid>                      # in-flight uploads; swept on startup
  uploads/<upload-id>/<part-n>  # multipart parts until Complete or Abort
```

**PutObject, step by step**

1. Stream the body to `tmp/<uuid>`, computing MD5 and SHA-256 as it passes through. Decode `aws-chunked` framing and verify any `x-amz-checksum-*` trailer.
2. `fsync` the temp file.
3. Take the per-key lock for `bucket/key`. A conditional write (`If-Match`, `If-None-Match`) is checked against the current version here, so two racing writers cannot both pass; it is also checked before the body is read, to fail early.
4. `rename(tmp/<uuid>, blobs/ab/cd/<sha256>)`. If the blob already exists (same content), delete the temp file instead. Rename is the atomic step for the bytes; the blob's directory, and any directory created for it, is then fsynced so the rename survives a power loss.
5. Add the version row `{version_id, size, etag, sha256, content_type, user_meta, last_modified}` to the key's versions under `bucket\0key` in a single redb transaction, together with the blob refcounts. Commit is the atomic step for visibility.
6. If the write replaced a version (the null version, see below), decrement its blob's refcount; delete the blob at zero.
7. Release the lock, return `200` with the ETag.

**Why content-addressed blobs**

- Object keys never touch the filesystem, so `a/b/../c`, 1024-byte keys, and keys differing only in case are all safe.
- CopyObject is a metadata write; no bytes move.
- Two-level fan-out keeps any one directory small even at a million objects.
- Overwrites are naturally atomic: the new row points at the new blob; readers holding the old blob open keep reading it until they close.

**Versions**

Every key holds a list of versions, newest first; the first is the current one, so there is no separate "current" pointer. In the fs backend the whole list is the value of the key's one row in the `objects` table, so every write stays a single read-modify-write of one row in one transaction. That makes a write O(versions of that key), which is fine for a test fixture.

- A version row is the object's metadata plus a version id. A delete marker is a version with no blob.
- The service picks the id of each write from the bucket's versioning state: a fresh opaque id when `Enabled` (ids are unique and sort newest first), otherwise `null`. Writing a `null` version replaces any existing null version, so an unversioned or `Suspended` bucket overwrites in place while older non-null versions stay.
- DELETE without a version id removes the null version in an unversioned bucket and otherwise writes a delete marker (with a fresh id when `Enabled`, as the null version when `Suspended`). DELETE with a version id removes exactly that version; the next newest becomes current, which is how deleting a delete marker undeletes.
- Every version with a blob holds one reference on it, so old versions pin blobs and a copy of an old version shares its bytes. The startup sweep recounts references over every version of every key.
- Rows written before versioning (`{blob, meta}`, one per key) are read as a single null version; no migration step runs, and they are rewritten in the new format on their next write.

**Object Lock**

Lock state lives on rows that already exist: a bucket row carries its lock configuration (present means enabled, plus an optional default retention rule), and each version row carries its own retention (mode and retain-until instant) and legal hold. Rows without these fields load as unlocked.

- CreateBucket with lock enabled also enables versioning, and a lock-enabled bucket refuses `Suspended`, so every write adds a new version and never replaces a locked one. Lock can also be enabled later on a bucket whose versioning is `Enabled`; it cannot be turned off.
- A new version gets retention and legal hold from its request headers, else the bucket's default retention counted from its creation (for multipart, from Complete; a year is 365 days). A copy does not inherit the source's lock.
- Every permanent removal of a version passes one check inside the backend's delete, so it is atomic with it: refused (`AccessDenied`) under a legal hold or unexpired retention, unless the retention is GOVERNANCE and the request sets `x-amz-bypass-governance-retention`. Delete markers are ordinary writes and always allowed. Retention changes are checked inside the version row update: unexpired COMPLIANCE can only be extended; unexpired GOVERNANCE only extended unless bypassed.
- "Now" comes from one clock in the service: system time, or the instant `S3_NOW` fixes. The in-process test harness moves it to expire retention without sleeping.

**Multipart**

Parts land in `uploads/<id>/` as ordinary hashed temp files. Complete validates part numbers and sizes (every part but the last must be at least 5 MB), takes the per-key lock so no part can be replaced or aborted mid-copy, concatenates parts into a single blob with the AWS ETag formula `md5(concat(part_md5s))-N`, then follows steps 4-7. The object row keeps each part's size and checksum for `partNumber` reads. Abort deletes the directory. Uploads with no activity for 24 hours are swept on startup only; there is no background reaper.

**Memory backend**

The same trait over a `BTreeMap<(Bucket, Key), Vec<Version>>` behind an `RwLock`, with bodies as `Bytes`; the version-list rules above are shared code. The ordered map gives prefix listing for free. Nothing is written anywhere; the container's memory limit is the storage limit.

## Container and configuration

`docker run -p 9000:9000 <image>` is a complete S3 endpoint; everything else is an environment variable.

| Setting | Env var | Default | Notes |
| --- | --- | --- | --- |
| Listen address | `S3_LISTEN` | `0.0.0.0:9000` | One port, HTTP only. TLS is the job of a proxy in front, if a test needs it. |
| Backend | `S3_BACKEND` | `memory` | `memory` or `fs` |
| Data directory | `S3_DATA_DIR` | `/data` | Only read when backend is `fs`; declared as a `VOLUME` |
| Credentials | `S3_ACCESS_KEY`, `S3_SECRET_KEY` | `test` / `test` | Used only when strict auth is on |
| Strict auth | `S3_STRICT_AUTH` | `false` | `true` (or `1`, `yes`, `on`) rejects requests whose SigV4 signature does not verify |
| Region | `S3_REGION` | `us-east-1` | Echoed in responses and used in signature verification |
| Pre-created buckets | `S3_INIT_BUCKETS` | (none) | Comma-separated; created at startup so tests skip setup |
| Virtual-host domain | `S3_VHOST_DOMAIN` | (none) | e.g. `localhost` makes `mybucket.localhost` resolve to `mybucket` |
| Log level | `S3_LOG` | `info` | Structured JSON, one line per request, to stdout |
| Object Lock clock | `S3_NOW` | (system time) | A fixed RFC 3339 instant, e.g. `2030-01-01T00:00:00Z`, that Object Lock judges retention against (expiry, default retention, "retain until must be in the future") instead of the system clock, so retention tests are deterministic. Last-Modified, version ids and SigV4 still use system time |

**Image.** Multi-stage build: `rust:alpine` compiles a fully static `x86_64-unknown-linux-musl` binary (arm64 too); the runtime stage is `scratch` plus the binary and a CA bundle it does not need. Target: under 20 MB, cold start under 500 ms, resident memory under 30 MB idle.

**Health.** `GET /health` returns `200` once the backend is open and init buckets exist; `GET /ready` is an alias. Docker `HEALTHCHECK` is baked into the image. Only an unsigned `GET`/`HEAD` with no query (no `Authorization` header) is a health check; a signed request to the same path is HeadBucket or ListObjects, so buckets named `health` and `ready` behave as on AWS. Rejected: reserving the names (a real bucket name would fail) and answering health only while no such bucket exists (HeadBucket on a missing `health` bucket would get `200`).

**Test harness integration.**

- Testcontainers modules for Go, Python, Java and JS/TS that expose `Endpoint()`, `AccessKey()`, `SecretKey()` and wait on `/health`.
- A `docker-compose.yml` fragment in the README.
- The binary also runs bare (`gb --backend memory`) for CI runners without Docker.
- Shutdown on `SIGTERM` finishes in-flight requests, then exits; the fs backend needs no recovery step beyond the startup sweep of `tmp/`.

## Compatibility gotchas

Most S3 fakes fail not on the happy path but on the defaults SDKs quietly turn on. Each of these has a dedicated test.

| Behaviour | What breaks if wrong |
| --- | --- |
| `aws-chunked` bodies with a trailing checksum (`x-amz-content-sha256: STREAMING-...-TRAILER`) | Every upload from a current AWS SDK; the body arrives framed, not raw |
| `x-amz-checksum-crc32` / `-crc32c` / `-sha1` / `-sha256` request and response headers | SDKs since 2024 send CRC32 by default and validate the echo on GET |
| `Expect: 100-continue` | Java and Go SDKs wait for `100 Continue` before sending large bodies; a server that ignores it stalls |
| ETag quoting: `"d41d8cd9..."` including the quotes | rclone, s3cmd and the CLI's sync compare strings exactly |
| Multipart ETag `...-N` formula and `x-amz-mp-parts-count` on HEAD | Sync tools decide whether to re-upload based on this |
| `ListObjectsV2` with `encoding-type=url` and correct XML escaping of keys containing `&`, `<`, unicode | Any bucket with keys that are not plain ASCII |
| `KeyCount`, `IsTruncated`, opaque `NextContinuationToken` | Pagination loops in every SDK |
| Exact error XML: `<Error><Code>NoSuchKey</Code><Message>...</Message><Key>...</Key><RequestId>...</RequestId></Error>` with matching HTTP status | SDK retry classifiers and typed exceptions key on `Code` |
| `HEAD` responses carrying the same headers as `GET`, with no body, and `404` (not `403`) for a missing key | Existence checks |
| `Content-Type` defaulting to `binary/octet-stream`, not `application/octet-stream` | Some tests assert AWS's default |
| `Last-Modified` in RFC 1123, `Date` header on every response, `x-amz-request-id` and `x-amz-id-2` present | SDK clock-skew correction and logging |
| `Range: bytes=0-` returning `206` with `Content-Range`, and unsatisfiable ranges returning `416` | Streaming readers and resume |
| Virtual-host `Host: bucket.endpoint` vs path-style `/bucket/key`, and SigV4 canonical request differing between them | SDK default addressing style changed across major versions |
| SigV4 canonicalisation: double URL-encoding of keys, sorted headers, `UNSIGNED-PAYLOAD`, presigned `X-Amz-Expires` | Strict-auth mode; presigned upload and download links |
| `DeleteObjects` with `Quiet=true`, and `Content-MD5` on that request | Batch delete in the CLI and SDKs |
| Bucket names: 3-63 chars, lowercase, no `_`, not IP-shaped; keys up to 1024 bytes UTF-8 | Validation parity so a test that passes here passes on AWS |
| CopyObject with `x-amz-copy-source` URL-encoded, same-key copy requiring `REPLACE` directive | Metadata updates via self-copy, the idiom for changing Content-Type |

## Conformance and testing

The definition of "correct" is "real SDKs and existing conformance suites pass"; hand-written unit tests come second.

1. **Ceph `s3-tests`** (the de facto S3 conformance suite) run in CI against both backends, with a checked-in allowlist of tests that fail because they exercise out-of-scope features (ACLs, lifecycle). The allowlist can only shrink.
2. **MinIO `mint`** for a second opinion on multipart and listing edge cases.
3. **SDK smoke tests**, one small program per SDK, each doing: create bucket, put small, put 20 MB (forces multipart), list with prefix and delimiter, range get, copy, presigned get, batch delete, delete bucket. SDKs covered: boto3, aws-sdk-go-v2, aws-sdk-js v3, aws-sdk-java v2, the AWS CLI, rclone.
4. **Differential tests** that replay the same recorded request sequence against a real S3 bucket and this service and diff the responses, ignoring request ids and dates. Run by hand, not per commit.
5. **Crash tests** for the fs backend: kill the process at each step of the PUT sequence, restart, assert the object is either the old one or the new one and the sweep removed orphans.
6. **Property tests** over the listing code: random key sets, random prefix and delimiter, compared against a reference implementation in the test.
7. **Unit tests** for SigV4 canonicalisation using AWS's published test vectors.

A change that makes any SDK smoke test fail does not merge.

## Open questions

- [x] Should the memory backend enforce a configurable size cap and return `EntityTooLarge`, or let the container OOM? No cap; the container OOMs
- [x] Is there any need for a `sleep`/latency-injection flag for testing client timeouts, or does that belong in a proxy? No flag; use a proxy
- [x] Multipart orphan sweep on startup only, or also a `--sweep-interval` flag for long-running dev instances? Startup only
- [x] What triggers versioning + Object Lock leaving the deferred tier: a named internal consumer, or a failing SDK smoke test? Neither: moved in scope by decision. Both are built
