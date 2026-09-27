//! S3 semantics over a [`Storage`] backend, exposed as an `s3s::S3` implementation.

mod bucket;
mod list;
mod lock;
mod multipart;
mod object;
mod tagging;
mod versioning;

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::TryStreamExt;
use http::HeaderMap;
use s3s::dto::{ETag, ETagCondition, Owner, StreamingBlob, Timestamp};
use s3s::stream::aws_chunked_stream::AwsChunkedStreamError;
use s3s::stream::upload_stream::UploadStreamError;
use s3s::{S3, S3Error, S3ErrorCode, S3Request, S3Response, S3Result};

use crate::clock::Clock;
use crate::errors::Captured;
use crate::lenient;
use crate::logic::checksum::{self, Algorithm, BodyError, Digests};
use crate::logic::validate;
use crate::storage::{
    BucketInfo, ByteStream, Checksums, NULL_VERSION, ObjectMeta, Storage, StorageError, Tags,
    Versioning, WriteCheck,
};

pub const DEFAULT_CONTENT_TYPE: &str = "binary/octet-stream";

pub struct GarbageBin<S> {
    storage: S,
    region: String,
    /// Object Lock's notion of now; other timestamps use system time.
    clock: Arc<Clock>,
}

impl<S: Storage> GarbageBin<S> {
    pub fn new(storage: S, region: impl Into<String>, clock: Arc<Clock>) -> Self {
        Self {
            storage,
            region: region.into(),
            clock,
        }
    }

    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Stages `body`, then checks it against Content-MD5 and any checksums
    /// sent as headers or aws-chunked trailers.
    async fn stage(
        &self,
        body: Option<StreamingBlob>,
        upload: Upload<'_>,
    ) -> S3Result<(S::Blob, Digests)> {
        check_checksum_support(upload.headers)?;
        let mut expected = upload.expected;
        let algorithms = checksum::requested(&expected, upload.sdk_algorithm, upload.headers);
        let (stream, handle) = checksum::hashing(body_stream(body), &algorithms);
        let blob = self.storage.write_blob(stream).await.map_err(storage_err)?;
        let digests = handle.finish();

        // From the raw header: s3s treats an empty Content-MD5 as absent.
        if let Some(md5) = upload.headers.get("content-md5") {
            checksum::check_content_md5(md5.to_str().unwrap_or_default(), &digests.md5)
                .map_err(|e| digest_error(BodyError::Md5(e)))?;
        }
        checksum::merge_trailers(
            &mut expected,
            upload.trailers.and_then(|t| t.take()).as_ref(),
        );
        if let Some(a) = checksum::mismatch(&expected, &digests.checksums) {
            return Err(digest_error(BodyError::Checksum(a)));
        }
        Ok((blob, digests))
    }

    async fn require_bucket(&self, bucket: &str) -> S3Result<BucketInfo> {
        self.storage.head_bucket(bucket).await.map_err(storage_err)
    }

    /// `x-amz-version-id` for a version read from `bucket`: its id, or `null`
    /// for a null version once the bucket has had versioning configured.
    async fn version_header(&self, bucket: &str, meta: &ObjectMeta) -> S3Result<Option<String>> {
        if meta.version_id != NULL_VERSION {
            return Ok(Some(meta.version_id.clone()));
        }
        let info = self.require_bucket(bucket).await?;
        Ok(info.versioning.map(|_| NULL_VERSION.to_owned()))
    }
}

/// The version id a write to a bucket in state `info` gets.
fn write_version(info: &BucketInfo) -> String {
    match info.versioning {
        Some(Versioning::Enabled) => new_version_id(),
        _ => NULL_VERSION.to_owned(),
    }
}

/// `x-amz-version-id` for a version just written: absent for a null version.
fn written_version(meta: &ObjectMeta) -> Option<String> {
    (meta.version_id != NULL_VERSION).then(|| meta.version_id.clone())
}

/// A fresh, unique version id. Ids sort newest first.
fn new_version_id() -> String {
    static LAST: AtomicU64 = AtomicU64::new(0);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    let prev = LAST
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |p| {
            Some(now.max(p + 1))
        })
        .unwrap_or_default();
    let seq = now.max(prev + 1);
    let salt = uuid::Uuid::new_v4().simple().to_string();
    format!("{:016x}{}", u64::MAX - seq, &salt[..16])
}

/// A `versionId` query parameter; the empty string is rejected as AWS does.
fn version_param(v: Option<&String>) -> S3Result<Option<&str>> {
    match v.map(String::as_str) {
        Some("") => Err(invalid_argument("Version id cannot be the empty string")),
        v => Ok(v),
    }
}

/// Headers AWS puts on a response about a delete marker.
fn delete_marker_headers(e: &mut S3Error, version_id: &str) {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-delete-marker",
        http::HeaderValue::from_static("true"),
    );
    if let Ok(v) = http::HeaderValue::from_str(version_id) {
        headers.insert("x-amz-version-id", v);
    }
    e.set_headers(headers);
}

/// Maps a storage error from reading a version; `explicit` if the request named it.
/// A named delete marker cannot be read (405); a current one hides the key (404).
fn read_err(e: StorageError, explicit: bool) -> S3Error {
    match e {
        StorageError::DeleteMarker(id) if explicit => {
            let mut err = error(
                S3ErrorCode::MethodNotAllowed,
                "The specified method is not allowed against this resource.",
            );
            delete_marker_headers(&mut err, &id);
            err
        }
        e => storage_err(e),
    }
}

/// Request details `stage` needs besides the body.
struct Upload<'a> {
    headers: &'a HeaderMap,
    trailers: Option<Trailers>,
    expected: Checksums,
    sdk_algorithm: Option<&'a str>,
}

/// Trailing headers from either s3s (strict) or the lenient decoder.
enum Trailers {
    Strict(s3s::TrailingHeaders),
    Lenient(lenient::Trailers),
}

impl Trailers {
    fn of<T>(req: &S3Request<T>) -> Option<Self> {
        if let Some(t) = &req.trailing_headers {
            return Some(Self::Strict(t.clone()));
        }
        req.extensions
            .get::<lenient::Trailers>()
            .cloned()
            .map(Self::Lenient)
    }

    fn take(&self) -> Option<HeaderMap> {
        match self {
            Self::Strict(t) => t.take(),
            Self::Lenient(t) => t.take(),
        }
    }
}

fn body_stream(body: Option<StreamingBlob>) -> ByteStream {
    match body {
        Some(b) => Box::pin(b.map_err(io::Error::other)),
        None => Box::pin(futures::stream::empty()),
    }
}

pub(crate) fn error(code: S3ErrorCode, msg: impl Into<String>) -> S3Error {
    S3Error::with_message(code, msg.into())
}

/// The S3 error for a body that does not match its `Content-MD5` or checksum header.
pub fn digest_error(e: BodyError) -> S3Error {
    match e {
        BodyError::Md5(checksum::Md5Error::Invalid) => error(
            S3ErrorCode::InvalidDigest,
            "The Content-MD5 you specified is not valid.",
        ),
        BodyError::Md5(checksum::Md5Error::Mismatch) => error(
            S3ErrorCode::BadDigest,
            "The Content-MD5 you specified did not match what we received.",
        ),
        BodyError::Checksum(a) => error(
            S3ErrorCode::BadDigest,
            format!(
                "The {} you specified did not match the calculated checksum.",
                a.header()
            ),
        ),
    }
}

pub(crate) fn storage_err(e: StorageError) -> S3Error {
    use S3ErrorCode as C;
    match e {
        StorageError::NoSuchBucket => error(C::NoSuchBucket, "The specified bucket does not exist"),
        StorageError::BucketAlreadyOwnedByYou => error(
            C::BucketAlreadyOwnedByYou,
            "Your previous request to create the named bucket succeeded and you already own it.",
        ),
        StorageError::BucketNotEmpty => error(
            C::BucketNotEmpty,
            "The bucket you tried to delete is not empty",
        ),
        StorageError::NoSuchKey => no_such_key(),
        StorageError::NoSuchVersion => {
            error(C::NoSuchVersion, "The specified version does not exist.")
        }
        StorageError::DeleteMarker(id) => {
            let mut e = no_such_key();
            delete_marker_headers(&mut e, &id);
            e
        }
        StorageError::NoSuchUpload => error(
            C::NoSuchUpload,
            "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.",
        ),
        StorageError::InvalidPart => invalid_part(),
        StorageError::PreconditionFailed => precondition_failed(),
        StorageError::Body(e) => body_err(e),
        StorageError::Refused(e) => match e.downcast::<S3Error>() {
            Ok(e) => *e,
            Err(e) => error(C::InternalError, e.to_string()),
        },
        StorageError::Io(e) => S3Error::with_source(C::InternalError, Box::new(e)),
        StorageError::Internal(msg) => error(C::InternalError, msg),
    }
}

fn no_such_key() -> S3Error {
    error(S3ErrorCode::NoSuchKey, "The specified key does not exist.")
}

fn body_err(e: io::Error) -> S3Error {
    let inner = e.get_ref();
    if let Some(chunked) = inner.and_then(|i| i.downcast_ref::<AwsChunkedStreamError>()) {
        return S3Error::with_message(chunked.to_s3_error_code(), chunked.to_string());
    }
    if inner.is_some_and(|i| i.is::<s3s::BodySizeLimitExceeded>()) {
        return error(
            S3ErrorCode::EntityTooLarge,
            "Your proposed upload exceeds the maximum allowed size",
        );
    }
    if inner.is_some_and(|i| i.is::<lenient::MalformedChunk>()) {
        return error(S3ErrorCode::IncompleteBody, e.to_string());
    }
    if let Some(UploadStreamError::Sha256Mismatch) =
        inner.and_then(|i| i.downcast_ref::<UploadStreamError>())
    {
        let mut err = S3Error::with_message(
            S3ErrorCode::Custom("XAmzContentSHA256Mismatch".into()),
            "The provided 'x-amz-content-sha256' header does not match what was computed.",
        );
        err.set_status_code(http::StatusCode::BAD_REQUEST);
        return err;
    }
    let msg = e.to_string();
    error(
        S3ErrorCode::IncompleteBody,
        format!("Error reading request body: {msg}"),
    )
}

fn invalid_part() -> S3Error {
    error(
        S3ErrorCode::InvalidPart,
        "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.",
    )
}

fn precondition_failed() -> S3Error {
    error(
        S3ErrorCode::PreconditionFailed,
        "At least one of the pre-conditions you specified did not hold",
    )
}

fn invalid_argument(msg: impl Into<String>) -> S3Error {
    error(S3ErrorCode::InvalidArgument, msg)
}

/// A validated `x-amz-storage-class`, `None` for `STANDARD`. Classes are only echoed back.
fn storage_class(c: Option<&s3s::dto::StorageClass>) -> S3Result<Option<String>> {
    use s3s::dto::StorageClass as C;
    const OTHERS: &[&str] = &[
        C::DEEP_ARCHIVE,
        C::EXPRESS_ONEZONE,
        C::GLACIER,
        C::GLACIER_IR,
        C::INTELLIGENT_TIERING,
        C::ONEZONE_IA,
        C::OUTPOSTS,
        C::REDUCED_REDUNDANCY,
        C::SNOW,
        C::STANDARD_IA,
    ];
    match c.map(C::as_str) {
        None | Some(C::STANDARD) => Ok(None),
        Some(c) if OTHERS.contains(&c) => Ok(Some(c.to_owned())),
        Some(_) => Err(error(
            S3ErrorCode::InvalidStorageClass,
            "The storage class you specified is not valid",
        )),
    }
}

fn check_checksum_support(headers: &HeaderMap) -> S3Result<()> {
    match checksum::unsupported(headers) {
        Some(name) => Err(invalid_argument(format!(
            "Unsupported checksum algorithm: {name}"
        ))),
        None => Ok(()),
    }
}

fn check_key(key: &str) -> S3Result<()> {
    if validate::key(key) {
        Ok(())
    } else {
        Err(error(S3ErrorCode::KeyTooLongError, "Your key is too long"))
    }
}

/// Current time at the second precision HTTP dates carry.
fn now() -> SystemTime {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn unix_secs(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

fn ts(t: SystemTime) -> Timestamp {
    Timestamp::from(t)
}

fn ts_secs(t: &Timestamp) -> i64 {
    time::OffsetDateTime::from(t.clone()).unix_timestamp()
}

fn etag(meta: &ObjectMeta) -> ETag {
    ETag::Strong(meta.etag.clone())
}

fn etag_matches(cond: &ETagCondition, meta: &ObjectMeta) -> bool {
    cond.is_any() || cond.as_etag().is_some_and(|e| e.value() == meta.etag)
}

/// Conditional GET/HEAD per RFC 9110 §13.2.2 as S3 applies it.
fn check_conditions(
    meta: &ObjectMeta,
    if_match: Option<&ETagCondition>,
    if_none_match: Option<&ETagCondition>,
    if_modified_since: Option<&Timestamp>,
    if_unmodified_since: Option<&Timestamp>,
) -> S3Result<()> {
    let modified = unix_secs(meta.last_modified);
    match if_match {
        Some(c) if !etag_matches(c, meta) => return Err(precondition_failed()),
        Some(_) => {}
        None => {
            if if_unmodified_since.is_some_and(|t| modified > ts_secs(t)) {
                return Err(precondition_failed());
            }
        }
    }
    let not_modified = match if_none_match {
        Some(c) => etag_matches(c, meta),
        None => if_modified_since.is_some_and(|t| modified <= ts_secs(t)),
    };
    if not_modified {
        let mut err = S3Error::new(S3ErrorCode::NotModified);
        let mut headers = HeaderMap::new();
        if let Ok(v) = format!("\"{}\"", meta.etag).parse() {
            headers.insert(http::header::ETAG, v);
        }
        if let Ok(v) = httpdate::fmt_http_date(meta.last_modified).parse() {
            headers.insert(http::header::LAST_MODIFIED, v);
        }
        err.set_headers(headers);
        return Err(err);
    }
    Ok(())
}

/// Conditional write (PutObject, CompleteMultipartUpload) against the key's current
/// version: `If-None-Match` fails on a matching ETag; `If-Match` requires one (404 if absent).
fn write_condition(
    existing: Option<&ObjectMeta>,
    if_match: Option<&ETagCondition>,
    if_none_match: Option<&ETagCondition>,
) -> S3Result<()> {
    let blocked = match (if_none_match, if_match, existing) {
        (Some(c), _, Some(m)) if etag_matches(c, m) => true,
        (_, Some(_), None) => return Err(storage_err(StorageError::NoSuchKey)),
        (_, Some(c), Some(m)) => !etag_matches(c, m),
        _ => false,
    };
    if blocked {
        return Err(precondition_failed());
    }
    Ok(())
}

/// Checks a conditional write now, so it fails before a body is read, and returns
/// the check for the backend to repeat atomically with the commit.
async fn check_write_conditions<S: Storage>(
    gb: &GarbageBin<S>,
    bucket: &str,
    key: &str,
    if_match: Option<ETagCondition>,
    if_none_match: Option<ETagCondition>,
) -> S3Result<WriteCheck> {
    if if_match.is_none() && if_none_match.is_none() {
        return Ok(Box::new(|_| Ok(())));
    }
    let existing = match gb.storage.head_object(bucket, key, None).await {
        Ok(m) => Some(m),
        Err(StorageError::NoSuchKey | StorageError::DeleteMarker(_)) => None,
        Err(e) => return Err(storage_err(e)),
    };
    write_condition(existing.as_ref(), if_match.as_ref(), if_none_match.as_ref())?;
    Ok(Box::new(move |m| {
        write_condition(m, if_match.as_ref(), if_none_match.as_ref()).map_err(lock::refused)
    }))
}

/// Parses an `x-amz-tagging` header (URL query encoded).
fn parse_tagging_header(header: Option<&str>) -> S3Result<Tags> {
    let tags: Tags = header
        .map(|h| form_urlencoded::parse(h.as_bytes()).into_owned().collect())
        .unwrap_or_default();
    validate_tags(&tags, TagTarget::Object)?;
    Ok(tags)
}

#[derive(Clone, Copy)]
enum TagTarget {
    Object,
    Bucket,
}

fn validate_tags(tags: &Tags, target: TagTarget) -> S3Result<()> {
    let too_many = match target {
        TagTarget::Object if tags.len() > 10 => Some("Object tags cannot be greater than 10"),
        TagTarget::Bucket if tags.len() > 50 => Some("Bucket tag count cannot be greater than 50"),
        _ => None,
    };
    if let Some(msg) = too_many {
        return Err(error(S3ErrorCode::InvalidTag, msg));
    }
    for (k, v) in tags {
        if k.is_empty() || k.chars().count() > 128 || v.chars().count() > 256 {
            return Err(error(
                S3ErrorCode::InvalidTag,
                "The TagKey or TagValue you have provided is invalid",
            ));
        }
    }
    Ok(())
}

/// Content-Encoding as stored: verbatim, minus the transport-level `aws-chunked` token.
fn content_encoding(v: Option<String>) -> Option<String> {
    let v = v?;
    let chunked = |t: &str| t.trim().eq_ignore_ascii_case("aws-chunked");
    if !v.split(',').any(chunked) {
        return Some(v);
    }
    let kept: Vec<&str> = v
        .split(',')
        .map(str::trim)
        .filter(|t| !t.is_empty() && !chunked(t))
        .collect();
    (!kept.is_empty()).then(|| kept.join(", "))
}

fn owner() -> Owner {
    Owner {
        display_name: Some("garbagebin".to_owned()),
        id: Some("02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4".to_owned()),
    }
}

/// `CRC32` etc. from an s3s checksum-algorithm value.
fn algorithm(a: Option<&s3s::dto::ChecksumAlgorithm>) -> Option<&str> {
    a.map(|a| a.as_str())
}

fn expected_checksums(
    crc32: Option<String>,
    crc32c: Option<String>,
    crc64nvme: Option<String>,
    sha1: Option<String>,
    sha256: Option<String>,
) -> Checksums {
    Checksums {
        crc32,
        crc32c,
        crc64nvme,
        sha1,
        sha256,
        checksum_type: None,
    }
}

/// Stored checksums of a fresh single-part upload.
fn full_object(mut c: Checksums) -> Checksums {
    let any = Algorithm::ALL.into_iter().any(|a| a.get(&c).is_some());
    c.checksum_type = any.then(|| "FULL_OBJECT".to_owned());
    c
}

macro_rules! delegate {
    ($($op:ident: $input:ident => $output:ident, via $module:ident;)*) => {
        #[async_trait::async_trait]
        impl<S: Storage> S3 for GarbageBin<S> {
            $(
                async fn $op(&self, req: S3Request<s3s::dto::$input>) -> S3Result<S3Response<s3s::dto::$output>> {
                    let captured = req.extensions.get::<Captured>().cloned();
                    let res = $module::$op(self, req).await;
                    if let (Err(e), Some(c)) = (&res, captured) {
                        c.set(e);
                    }
                    res
                }
            )*
        }
    };
}

delegate! {
    create_bucket: CreateBucketInput => CreateBucketOutput, via bucket;
    delete_bucket: DeleteBucketInput => DeleteBucketOutput, via bucket;
    head_bucket: HeadBucketInput => HeadBucketOutput, via bucket;
    list_buckets: ListBucketsInput => ListBucketsOutput, via bucket;
    get_bucket_location: GetBucketLocationInput => GetBucketLocationOutput, via bucket;
    put_object: PutObjectInput => PutObjectOutput, via object;
    get_object: GetObjectInput => GetObjectOutput, via object;
    head_object: HeadObjectInput => HeadObjectOutput, via object;
    delete_object: DeleteObjectInput => DeleteObjectOutput, via object;
    delete_objects: DeleteObjectsInput => DeleteObjectsOutput, via object;
    copy_object: CopyObjectInput => CopyObjectOutput, via object;
    list_objects: ListObjectsInput => ListObjectsOutput, via list;
    list_objects_v2: ListObjectsV2Input => ListObjectsV2Output, via list;
    create_multipart_upload: CreateMultipartUploadInput => CreateMultipartUploadOutput, via multipart;
    upload_part: UploadPartInput => UploadPartOutput, via multipart;
    upload_part_copy: UploadPartCopyInput => UploadPartCopyOutput, via multipart;
    complete_multipart_upload: CompleteMultipartUploadInput => CompleteMultipartUploadOutput, via multipart;
    abort_multipart_upload: AbortMultipartUploadInput => AbortMultipartUploadOutput, via multipart;
    list_parts: ListPartsInput => ListPartsOutput, via multipart;
    list_multipart_uploads: ListMultipartUploadsInput => ListMultipartUploadsOutput, via multipart;
    put_object_tagging: PutObjectTaggingInput => PutObjectTaggingOutput, via tagging;
    get_object_tagging: GetObjectTaggingInput => GetObjectTaggingOutput, via tagging;
    delete_object_tagging: DeleteObjectTaggingInput => DeleteObjectTaggingOutput, via tagging;
    put_bucket_tagging: PutBucketTaggingInput => PutBucketTaggingOutput, via tagging;
    get_bucket_tagging: GetBucketTaggingInput => GetBucketTaggingOutput, via tagging;
    delete_bucket_tagging: DeleteBucketTaggingInput => DeleteBucketTaggingOutput, via tagging;
    put_bucket_versioning: PutBucketVersioningInput => PutBucketVersioningOutput, via versioning;
    get_bucket_versioning: GetBucketVersioningInput => GetBucketVersioningOutput, via versioning;
    list_object_versions: ListObjectVersionsInput => ListObjectVersionsOutput, via versioning;
    put_object_lock_configuration: PutObjectLockConfigurationInput => PutObjectLockConfigurationOutput, via lock;
    get_object_lock_configuration: GetObjectLockConfigurationInput => GetObjectLockConfigurationOutput, via lock;
    put_object_retention: PutObjectRetentionInput => PutObjectRetentionOutput, via lock;
    get_object_retention: GetObjectRetentionInput => GetObjectRetentionOutput, via lock;
    put_object_legal_hold: PutObjectLegalHoldInput => PutObjectLegalHoldOutput, via lock;
    get_object_legal_hold: GetObjectLegalHoldInput => GetObjectLegalHoldOutput, via lock;
}
