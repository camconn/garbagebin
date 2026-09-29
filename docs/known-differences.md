# Known differences from AWS

Behaviour that differs from S3 on purpose, or that has not been checked against real S3. Planned work
lives in [TODO/](../TODO/); features that will never be built are listed in [design.md](design.md).

## By design

- Lenient mode (the default) ignores `x-amz-content-sha256` and aws-chunked chunk signatures, since it
  accepts any credentials. Strict mode verifies both.
- `LocationConstraint` on CreateBucket is ignored: there is one region, and rejecting a mismatch would
  break lenient-mode clients whose configured region differs from `S3_REGION`.
- Error bodies add `Key`/`BucketName` only for `NoSuchKey`, `NoSuchBucket` and `BucketNotEmpty`; AWS
  also adds fields such as `UploadId` and `ActualObjectSize`. SDKs key on `Code` and `Message`.
- Storage classes are validated, stored and echoed only: a `GLACIER` or `DEEP_ARCHIVE` object reads like
  any other, where AWS answers `InvalidObjectState` until it is restored.
- Nothing is encrypted, though objects stored without SSE headers report
  `x-amz-server-side-encryption: AES256` as AWS's default bucket encryption does.
- Objects stored before gb computed a default CRC64NVME (see below) have no checksum. Neither do parts
  uploaded before then, so Complete cannot check a whole-object `x-amz-checksum-crc64nvme` against them
  and stores the object without one.

## Unverified against AWS

Each follows AWS's documentation or a conformance suite, but has not been compared with real S3.

- DeleteObject with `If-Match` on a missing key answers 412 (RFC 9110); a conditional PUT answers 404
  as AWS does.
- More than 10 object tags answer `InvalidTag` (as Ceph s3-tests expects); mint's aws-sdk-go-v2 suite
  expects `BadRequest`.
- Any unknown `versionId` answers `NoSuchVersion`; AWS answers 400 `InvalidArgument` for a malformed
  one. A `version-id-marker` whose version was deleted between pages resumes at the next key.
- Object Lock counts a default retention of N years as N × 365 days; AWS may use calendar years.
- PutObject and UploadPart without `Content-Length` or `Transfer-Encoding` answer 411
  `MissingContentLength`. So does an XML request body without `Content-Length`, even over HTTP/2.
- The operations AWS's model marks as needing a body digest (DeleteObjects, PutObjectTagging,
  PutBucketTagging, PutBucketVersioning, PutObjectLockConfiguration, PutObjectRetention,
  PutObjectLegalHold) answer `InvalidRequest` without `Content-MD5` or `x-amz-checksum-*`. A PutObject
  with Object Lock headers and no digest is accepted, though AWS documents a digest as required for
  one with a retention period: mint's versioning suite, whose SDK sends none, expects it to succeed.
- MD5, SHA512 and the XXHASH checksums are base64 of the big-endian digest, and multipart uploads using
  them are `COMPOSITE` only, like SHA1 and SHA256.
- An upload naming no checksum gets a `FULL_OBJECT` CRC64NVME. So does a multipart upload created
  without one: every part also gets a CRC64NVME, echoed in UploadPart, UploadPartCopy and ListParts
  responses, and Complete combines them.
- GET/HEAD with `partNumber` in checksum mode report that part's checksum with the object's checksum
  type, `FULL_OBJECT` objects included.

## fs backend

- Empty `blobs/ab/cd/` directories left by deletes are removed only by the startup sweep.
- All metadata writes serialise on one commit lock (redb serialises write transactions anyway).
- CompleteMultipartUpload holds the per-key lock while it concatenates parts, so parts cannot be
  replaced or aborted mid-copy; a concurrent PUT to the same key waits for it.
- The startup sweep reads every object row to recount blob references.
- Power loss: blobs, part files and every directory created to hold them are fsynced before the
  metadata commit, which redb fsyncs. Only process death is tested (crash tests); durability rests on
  the filesystem honouring fsync. A process killed between creating a directory and syncing it leaves
  it to the kernel's writeback.
- The `GB_CRASH_AT` fault-injection hook exists only in debug builds, so the crash tests are skipped
  under `cargo test --release`.
