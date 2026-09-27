use s3s::dto::{
    self, ChecksumMode, ChecksumType, CopyObjectInput, CopyObjectOutput, CopyObjectResult,
    CopySource, DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput, DeleteObjectsOutput,
    DeletedObject, ETagCondition, GetObjectInput, GetObjectOutput, HeadObjectInput,
    HeadObjectOutput, MetadataDirective, PutObjectInput, PutObjectOutput, ServerSideEncryption,
    StorageClass, StreamingBlob, TaggingDirective,
};
use s3s::{S3ErrorCode, S3Request, S3Response, S3Result};

use super::{
    DEFAULT_CONTENT_TYPE, GarbageBin, Trailers, Upload, algorithm, check_conditions, check_key,
    content_encoding, error, etag, etag_matches, expected_checksums, full_object, lock,
    new_version_id, now, parse_tagging_header, precondition_failed, read_err, storage_class,
    storage_err, ts, version_param, write_version, written_version,
};
use futures::TryStreamExt;

use crate::logic::checksum::{self, Algorithm};
use crate::logic::etag as etag_fmt;
use crate::storage::{
    BucketInfo, Checksums, DeleteCheck, NULL_VERSION, ObjectMeta, Sse, Storage, StorageError,
    Versioning, WriteCheck,
};

pub async fn put_object<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<PutObjectInput>,
) -> S3Result<S3Response<PutObjectOutput>> {
    let trailers = Trailers::of(&req);
    let input = req.input;
    check_key(&input.key)?;
    let tags = parse_tagging_header(input.tagging.as_deref())?;
    let storage_class = storage_class(input.storage_class.as_ref())?;
    // Fail fast before reading a body we cannot store.
    let info = gb.require_bucket(&input.bucket).await?;
    let lock_now = gb.clock.now();
    let (retention, legal_hold) = lock::from_headers(
        &info,
        input.object_lock_mode.as_ref(),
        input.object_lock_retain_until_date.as_ref(),
        input.object_lock_legal_hold_status.as_ref(),
        lock_now,
    )?;
    let check = super::check_write_conditions(
        gb,
        &input.bucket,
        &input.key,
        input.if_match,
        input.if_none_match,
    )
    .await?;

    let expected = expected_checksums(
        input.checksum_crc32,
        input.checksum_crc32c,
        input.checksum_crc64nvme,
        input.checksum_sha1,
        input.checksum_sha256,
    );
    let upload = Upload {
        headers: &req.headers,
        trailers,
        expected,
        sdk_algorithm: algorithm(input.checksum_algorithm.as_ref()),
    };
    let (blob, digests) = gb.stage(input.body, upload).await?;

    let sse = Sse {
        algorithm: input.server_side_encryption.map(|s| s.as_str().to_owned()),
        kms_key_id: input.ssekms_key_id,
        customer_algorithm: input.sse_customer_algorithm,
        customer_key_md5: input.sse_customer_key_md5,
        bucket_key_enabled: input.bucket_key_enabled,
    };
    let meta = ObjectMeta {
        version_id: write_version(&info),
        delete_marker: false,
        size: digests.size,
        etag: etag_fmt::single(&digests.md5),
        last_modified: now(),
        content_type: Some(
            input
                .content_type
                .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_owned()),
        ),
        content_encoding: content_encoding(input.content_encoding),
        content_language: input.content_language,
        cache_control: input.cache_control,
        content_disposition: input.content_disposition,
        expires: input.expires,
        user_metadata: input.metadata.unwrap_or_default().into_iter().collect(),
        tags,
        checksums: full_object(digests.checksums),
        part_sizes: Vec::new(),
        upload_id: None,
        sse,
        storage_class,
        retention: retention.or_else(|| lock::default_retention(&info, lock_now)),
        legal_hold,
    };
    gb.storage
        .put_object_if(&input.bucket, &input.key, blob, meta.clone(), check)
        .await
        .map_err(storage_err)?;

    let c = &meta.checksums;
    Ok(S3Response::new(PutObjectOutput {
        e_tag: Some(etag(&meta)),
        checksum_crc32: c.crc32.clone(),
        checksum_crc32c: c.crc32c.clone(),
        checksum_crc64nvme: c.crc64nvme.clone(),
        checksum_sha1: c.sha1.clone(),
        checksum_sha256: c.sha256.clone(),
        checksum_type: c.checksum_type.clone().map(ChecksumType::from),
        server_side_encryption: meta.sse.algorithm.clone().map(ServerSideEncryption::from),
        ssekms_key_id: meta.sse.kms_key_id.clone(),
        sse_customer_algorithm: meta.sse.customer_algorithm.clone(),
        sse_customer_key_md5: meta.sse.customer_key_md5.clone(),
        bucket_key_enabled: meta.sse.bucket_key_enabled,
        version_id: written_version(&meta),
        ..Default::default()
    }))
}

/// Fills the headers GET and HEAD share from the metadata row.
macro_rules! object_headers {
    ($out:ident, $meta:expr, $with_checksums:expr) => {{
        let m: &ObjectMeta = $meta;
        $out.accept_ranges = Some("bytes".to_owned());
        $out.e_tag = Some(etag(m));
        $out.last_modified = Some(ts(m.last_modified));
        $out.content_type = m.content_type.clone();
        $out.content_encoding = m.content_encoding.clone();
        $out.content_language = m.content_language.clone();
        $out.cache_control = m.cache_control.clone();
        $out.content_disposition = m.content_disposition.clone();
        $out.expires = m.expires.clone();
        $out.metadata =
            (!m.user_metadata.is_empty()).then(|| m.user_metadata.clone().into_iter().collect());
        $out.parts_count = (!m.part_sizes.is_empty()).then(|| m.part_sizes.len() as i32);
        $out.tag_count = (!m.tags.is_empty()).then(|| m.tags.len() as i32);
        $out.server_side_encryption = m.sse.algorithm.clone().map(ServerSideEncryption::from);
        $out.ssekms_key_id = m.sse.kms_key_id.clone();
        $out.sse_customer_algorithm = m.sse.customer_algorithm.clone();
        $out.sse_customer_key_md5 = m.sse.customer_key_md5.clone();
        $out.bucket_key_enabled = m.sse.bucket_key_enabled;
        $out.storage_class = m.storage_class.clone().map(StorageClass::from);
        (
            $out.object_lock_mode,
            $out.object_lock_retain_until_date,
            $out.object_lock_legal_hold_status,
        ) = lock::headers(m);
        if $with_checksums {
            let c = &m.checksums;
            $out.checksum_crc32 = c.crc32.clone();
            $out.checksum_crc32c = c.crc32c.clone();
            $out.checksum_crc64nvme = c.crc64nvme.clone();
            $out.checksum_sha1 = c.sha1.clone();
            $out.checksum_sha256 = c.sha256.clone();
            $out.checksum_type = c.checksum_type.clone().map(ChecksumType::from);
        }
    }};
}

fn checksum_mode(mode: Option<&ChecksumMode>) -> bool {
    mode.is_some_and(|m| m.as_str().eq_ignore_ascii_case(ChecksumMode::ENABLED))
}

fn content_range(r: &std::ops::Range<u64>, size: u64) -> String {
    format!("bytes {}-{}/{size}", r.start, r.end - 1)
}

/// The byte range a GET/HEAD asks for via `Range` or `partNumber`; `None` for the whole object.
fn read_span(
    meta: &ObjectMeta,
    range: Option<&dto::Range>,
    part_number: Option<i32>,
) -> S3Result<Option<std::ops::Range<u64>>> {
    let size = meta.size;
    match (range, part_number) {
        (Some(_), Some(_)) => Err(error(
            S3ErrorCode::InvalidRequest,
            "Cannot specify both Range header and partNumber query parameter",
        )),
        (Some(r), None) => r.check(size).map(Some).map_err(|_| {
            let mut e = error(
                S3ErrorCode::InvalidRange,
                "The requested range is not satisfiable",
            );
            let mut headers = http::HeaderMap::new();
            headers.insert(
                http::header::CONTENT_TYPE,
                http::HeaderValue::from_static("application/xml"),
            );
            if let Ok(v) = http::HeaderValue::from_str(&format!("bytes */{size}")) {
                headers.insert(http::header::CONTENT_RANGE, v);
            }
            e.set_headers(headers);
            e
        }),
        (None, Some(n)) => {
            let n = super::multipart::part_number(n)? as usize;
            let bad_part = || {
                let mut e = error(
                    S3ErrorCode::Custom("InvalidPartNumber".into()),
                    "The requested partnumber is not satisfiable",
                );
                e.set_status_code(http::StatusCode::RANGE_NOT_SATISFIABLE);
                e
            };
            let (start, len) = if meta.part_sizes.is_empty() {
                if n != 1 {
                    return Err(bad_part());
                }
                (0, size)
            } else {
                let len = *meta.part_sizes.get(n - 1).ok_or_else(bad_part)?;
                (meta.part_sizes[..n - 1].iter().sum(), len)
            };
            match len {
                _ if size == 0 => Ok(None),
                // Only a last part can be empty; there is no byte range to name.
                0 => Err(bad_part()),
                _ => Ok(Some(start..start + len)),
            }
        }
        (None, None) => Ok(None),
    }
}

pub async fn get_object<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<GetObjectInput>,
) -> S3Result<S3Response<GetObjectOutput>> {
    let i = req.input;
    let version = version_param(i.version_id.as_ref())?;
    let (meta, reader) = gb
        .storage
        .open_object(&i.bucket, &i.key, version)
        .await
        .map_err(|e| read_err(e, version.is_some()))?;
    check_conditions(
        &meta,
        i.if_match.as_ref(),
        i.if_none_match.as_ref(),
        i.if_modified_since.as_ref(),
        i.if_unmodified_since.as_ref(),
    )?;
    let range = read_span(&meta, i.range.as_ref(), i.part_number)?;
    let full = range.is_none();
    let span = range.clone().unwrap_or(0..meta.size);

    let mut out = GetObjectOutput::default();
    object_headers!(out, &meta, full && checksum_mode(i.checksum_mode.as_ref()));
    out.content_length = Some((span.end - span.start) as i64);
    out.content_range = range.as_ref().map(|r| content_range(r, meta.size));
    out.version_id = gb.version_header(&i.bucket, &meta).await?;
    out.body = Some(StreamingBlob::wrap(reader.read(span)));
    Ok(S3Response::new(out))
}

pub async fn head_object<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<HeadObjectInput>,
) -> S3Result<S3Response<HeadObjectOutput>> {
    let i = req.input;
    let version = version_param(i.version_id.as_ref())?;
    let meta = gb
        .storage
        .head_object(&i.bucket, &i.key, version)
        .await
        .map_err(|e| read_err(e, version.is_some()))?;
    check_conditions(
        &meta,
        i.if_match.as_ref(),
        i.if_none_match.as_ref(),
        i.if_modified_since.as_ref(),
        i.if_unmodified_since.as_ref(),
    )?;
    let range = read_span(&meta, i.range.as_ref(), i.part_number)?;
    let span = range.clone().unwrap_or(0..meta.size);

    let mut out = HeadObjectOutput::default();
    object_headers!(
        out,
        &meta,
        range.is_none() && checksum_mode(i.checksum_mode.as_ref())
    );
    out.content_length = Some((span.end - span.start) as i64);
    out.content_range = range.as_ref().map(|r| content_range(r, meta.size));
    out.version_id = gb.version_header(&i.bucket, &meta).await?;
    Ok(S3Response::new(out))
}

/// What one DELETE did.
struct Deletion {
    /// The version the request named.
    version_id: Option<String>,
    /// The delete marker created, or the named version if it was one.
    marker: Option<String>,
}

/// DELETE of `version`, or of the current version per the bucket's versioning state:
/// unversioned removes the null version, otherwise a delete marker is added.
/// Removing a version Object Lock protects is refused; `bypass` lifts GOVERNANCE retention.
/// `if_match` is checked atomically with the delete; unlike a conditional PUT (404),
/// a missing version fails it (RFC 9110).
async fn delete_one<S: Storage>(
    gb: &GarbageBin<S>,
    info: &BucketInfo,
    key: &str,
    version: Option<&str>,
    bypass: bool,
    if_match: Option<ETagCondition>,
) -> Result<Deletion, StorageError> {
    let bucket = &info.name;
    let lock_now = gb.clock.now();
    let holds = |c: &Option<ETagCondition>, m: &ObjectMeta| {
        c.as_ref()
            .is_none_or(|c| !m.delete_marker && etag_matches(c, m))
    };
    let target = match (version, info.versioning) {
        (Some(v), _) => Some(v),
        (None, None) => Some(NULL_VERSION),
        (None, Some(_)) => None,
    };
    if let Some(v) = target {
        let cond = if_match.clone();
        let check: DeleteCheck = Box::new(move |m| {
            if !holds(&cond, m) {
                return Err(StorageError::PreconditionFailed);
            }
            lock::check_delete(m, lock_now, bypass).map_err(lock::refused)
        });
        let removed = gb.storage.delete_version_if(bucket, key, v, check).await?;
        if if_match.is_some() && removed.is_none() {
            return Err(StorageError::PreconditionFailed);
        }
        let dm = version.is_some() && removed.is_some_and(|m| m.delete_marker);
        return Ok(Deletion {
            version_id: version.map(str::to_owned),
            marker: dm.then(|| v.to_owned()),
        });
    }
    let id = match info.versioning {
        Some(Versioning::Enabled) => new_version_id(),
        _ => NULL_VERSION.to_owned(),
    };
    let mut meta = ObjectMeta::delete_marker(id.clone());
    meta.last_modified = now();
    let check: WriteCheck = Box::new(move |m| match m {
        Some(m) if holds(&if_match, m) => Ok(()),
        None if if_match.is_none() => Ok(()),
        _ => Err(StorageError::PreconditionFailed),
    });
    gb.storage
        .put_delete_marker_if(bucket, key, meta, check)
        .await?;
    Ok(Deletion {
        version_id: None,
        marker: Some(id),
    })
}

pub async fn delete_object<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<DeleteObjectInput>,
) -> S3Result<S3Response<DeleteObjectOutput>> {
    let i = req.input;
    let version = version_param(i.version_id.as_ref())?;
    let info = gb.require_bucket(&i.bucket).await?;
    let bypass = i.bypass_governance_retention.unwrap_or(false);
    let d = delete_one(gb, &info, &i.key, version, bypass, i.if_match.clone())
        .await
        .map_err(storage_err)?;
    Ok(S3Response::new(DeleteObjectOutput {
        delete_marker: d.marker.is_some().then_some(true),
        version_id: d.version_id.or(d.marker),
        ..Default::default()
    }))
}

pub async fn delete_objects<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<DeleteObjectsInput>,
) -> S3Result<S3Response<DeleteObjectsOutput>> {
    let i = req.input;
    let info = gb.require_bucket(&i.bucket).await?;
    if i.delete.objects.len() > 1000 {
        return Err(error(
            S3ErrorCode::MalformedXML,
            "The XML you provided was not well-formed",
        ));
    }
    let quiet = i.delete.quiet.unwrap_or(false);
    let bypass = i.bypass_governance_retention.unwrap_or(false);
    let mut deleted = Vec::new();
    let mut errors = Vec::new();
    for obj in i.delete.objects {
        let res = match version_param(obj.version_id.as_ref()) {
            Ok(v) => delete_one(gb, &info, &obj.key, v, bypass, None)
                .await
                .map_err(storage_err),
            Err(e) => Err(e),
        };
        match res {
            Ok(_) if quiet => {}
            Ok(d) => deleted.push(DeletedObject {
                key: Some(obj.key),
                version_id: d.version_id,
                delete_marker: d.marker.is_some().then_some(true),
                delete_marker_version_id: d.marker,
            }),
            Err(e) => {
                errors.push(dto::Error {
                    code: Some(e.code().as_str().to_owned()),
                    message: e.message().map(str::to_owned),
                    key: Some(obj.key),
                    version_id: obj.version_id,
                });
            }
        }
    }
    Ok(S3Response::new(DeleteObjectsOutput {
        deleted: (!deleted.is_empty()).then_some(deleted),
        errors: (!errors.is_empty()).then_some(errors),
        ..Default::default()
    }))
}

/// Source bucket, key and version of an `x-amz-copy-source` (s3s decodes it).
pub(super) fn copy_source(src: &CopySource) -> S3Result<(&str, &str, Option<&str>)> {
    match src {
        CopySource::Bucket {
            bucket,
            key,
            version_id,
        } => {
            let version = match version_id.as_deref() {
                Some("") => return Err(super::invalid_argument("Invalid version id specified")),
                v => v,
            };
            Ok((bucket, key, version))
        }
        _ => Err(error(
            S3ErrorCode::NotImplemented,
            "Only bucket copy sources are supported",
        )),
    }
}

/// Makes `meta`, a copy of multipart `src`, a single-part object as AWS does: an MD5
/// ETag and whole-object checksums (the source's algorithms plus `requested`), computed
/// by reading the source.
async fn single_part<S: Storage>(
    gb: &GarbageBin<S>,
    bucket: &str,
    key: &str,
    src: &ObjectMeta,
    requested: Option<Algorithm>,
    meta: &mut ObjectMeta,
) -> S3Result<()> {
    let (opened, reader) = gb
        .storage
        .open_object(bucket, key, Some(&src.version_id))
        .await
        .map_err(|e| copy_source_err(e, bucket, key, true))?;
    if opened.etag != src.etag {
        return Err(precondition_failed());
    }
    let algorithms: Vec<Algorithm> = Algorithm::ALL
        .into_iter()
        .filter(|a| a.get(&src.checksums).is_some())
        .chain(requested)
        .collect();
    let (stream, handle) = checksum::hashing(reader.read(0..src.size), &algorithms);
    stream
        .try_for_each(|_| futures::future::ok(()))
        .await
        .map_err(|e| storage_err(StorageError::Io(e)))?;
    let digests = handle.finish();
    meta.etag = etag_fmt::single(&digests.md5);
    meta.part_sizes = Vec::new();
    meta.checksums = full_object(digests.checksums);
    Ok(())
}

/// Maps an error reading copy source `bucket`/`key`; naming a delete marker is a bad request.
pub(super) fn copy_source_err(
    e: StorageError,
    bucket: &str,
    key: &str,
    explicit: bool,
) -> s3s::S3Error {
    match e {
        StorageError::DeleteMarker(_) if explicit => error(
            S3ErrorCode::InvalidRequest,
            "The source of a copy request may not specifically refer to a delete marker by version id.",
        ),
        e => crate::errors::about(storage_err(e), bucket, key),
    }
}

pub async fn copy_object<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<CopyObjectInput>,
) -> S3Result<S3Response<CopyObjectOutput>> {
    let i = req.input;
    check_key(&i.key)?;
    let (src_bucket, src_key, src_version) = copy_source(&i.copy_source)?;
    let info = gb.require_bucket(&i.bucket).await?;
    let src = gb
        .storage
        .head_object(src_bucket, src_key, src_version)
        .await
        .map_err(|e| copy_source_err(e, src_bucket, src_key, src_version.is_some()))?;
    check_conditions(
        &src,
        i.copy_source_if_match.as_ref(),
        i.copy_source_if_none_match.as_ref(),
        i.copy_source_if_modified_since.as_ref(),
        i.copy_source_if_unmodified_since.as_ref(),
    )
    .map_err(|e| match e.code() {
        S3ErrorCode::NotModified => precondition_failed(),
        _ => e,
    })?;

    let replace_meta = i
        .metadata_directive
        .as_ref()
        .is_some_and(|d| d.as_str() == MetadataDirective::REPLACE);
    // Copying a named version onto its own key restores it.
    let same_key = src_bucket == i.bucket && src_key == i.key && src_version.is_none();
    let changes_something = replace_meta
        || i.server_side_encryption.is_some()
        || i.storage_class.is_some()
        || i.website_redirect_location.is_some();
    if same_key && !changes_something {
        return Err(error(
            S3ErrorCode::InvalidRequest,
            "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.",
        ));
    }

    // Lock settings are not copied: the destination's headers or default apply.
    let lock_now = gb.clock.now();
    let (retention, legal_hold) = lock::from_headers(
        &info,
        i.object_lock_mode.as_ref(),
        i.object_lock_retain_until_date.as_ref(),
        i.object_lock_legal_hold_status.as_ref(),
        lock_now,
    )?;
    let mut meta = src.clone();
    meta.version_id = write_version(&info);
    meta.retention = retention.or_else(|| lock::default_retention(&info, lock_now));
    meta.legal_hold = legal_hold;
    meta.storage_class = storage_class(i.storage_class.as_ref())?;
    meta.last_modified = now();
    meta.upload_id = None;
    if replace_meta {
        meta.content_type = Some(
            i.content_type
                .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_owned()),
        );
        meta.content_encoding = content_encoding(i.content_encoding);
        meta.content_language = i.content_language;
        meta.cache_control = i.cache_control;
        meta.content_disposition = i.content_disposition;
        meta.expires = i.expires;
        meta.user_metadata = i.metadata.unwrap_or_default().into_iter().collect();
    }
    if i.tagging_directive
        .as_ref()
        .is_some_and(|d| d.as_str() == TaggingDirective::REPLACE)
    {
        meta.tags = parse_tagging_header(i.tagging.as_deref())?;
    }
    meta.sse = Sse {
        algorithm: i.server_side_encryption.map(|s| s.as_str().to_owned()),
        kms_key_id: i.ssekms_key_id,
        customer_algorithm: i.sse_customer_algorithm,
        customer_key_md5: i.sse_customer_key_md5,
        bucket_key_enabled: i.bucket_key_enabled,
    };
    let requested = algorithm(i.checksum_algorithm.as_ref()).and_then(Algorithm::parse);
    if !src.part_sizes.is_empty() || requested.is_some_and(|a| a.get(&src.checksums).is_none()) {
        single_part(gb, src_bucket, src_key, &src, requested, &mut meta).await?;
    }
    gb.storage
        .copy_object(
            src_bucket,
            src_key,
            src_version,
            &src.etag,
            &i.bucket,
            &i.key,
            meta.clone(),
        )
        .await
        .map_err(storage_err)?;

    let c: &Checksums = &meta.checksums;
    let result = CopyObjectResult {
        e_tag: Some(etag(&meta)),
        last_modified: Some(ts(meta.last_modified)),
        checksum_crc32: c.crc32.clone(),
        checksum_crc32c: c.crc32c.clone(),
        checksum_crc64nvme: c.crc64nvme.clone(),
        checksum_sha1: c.sha1.clone(),
        checksum_sha256: c.sha256.clone(),
        checksum_type: c.checksum_type.clone().map(ChecksumType::from),
        ..Default::default()
    };
    Ok(S3Response::new(CopyObjectOutput {
        copy_object_result: Some(result),
        server_side_encryption: meta.sse.algorithm.clone().map(ServerSideEncryption::from),
        ssekms_key_id: meta.sse.kms_key_id.clone(),
        sse_customer_algorithm: meta.sse.customer_algorithm.clone(),
        sse_customer_key_md5: meta.sse.customer_key_md5.clone(),
        bucket_key_enabled: meta.sse.bucket_key_enabled,
        version_id: written_version(&meta),
        copy_source_version_id: gb.version_header(src_bucket, &src).await?,
        ..Default::default()
    }))
}
