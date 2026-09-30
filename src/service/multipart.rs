use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, ChecksumAlgorithm, ChecksumType,
    CommonPrefix, CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CompletedPart,
    CopyPartResult, CreateMultipartUploadInput, CreateMultipartUploadOutput, EncodingType,
    Initiator, ListMultipartUploadsInput, ListMultipartUploadsOutput, ListPartsInput,
    ListPartsOutput, MultipartUpload, Part, StorageClass, UploadPartCopyInput,
    UploadPartCopyOutput, UploadPartInput, UploadPartOutput,
};
use s3s::{S3ErrorCode, S3Request, S3Response, S3Result};

use super::object::{copy_source, copy_source_err};
use super::{
    DEFAULT_CONTENT_TYPE, GarbageBin, Trailers, Upload, algorithm, check_checksum_support,
    check_conditions, check_key, check_write_conditions, checksums_of, content_encoding,
    digest_error, error, invalid_argument, invalid_part, lock, now, owner, parse_tagging_header,
    precondition_failed, require_length, set_checksums, sse_header, storage_class, storage_err, ts,
    write_version, written_version,
};
use crate::logic::checksum::{self, Algorithm, BodyError};
use crate::logic::listing;
use crate::logic::validate::{self, CompleteError};
use crate::storage::{Checksums, ObjectMeta, PartInfo, Sse, Storage, StorageError, UploadInfo};

pub(super) fn part_number(n: i32) -> S3Result<u32> {
    if (1..=validate::MAX_PART_NUMBER).contains(&n) {
        Ok(n as u32)
    } else {
        Err(invalid_argument(
            "Part number must be an integer between 1 and 10000, inclusive",
        ))
    }
}

/// The checksum type an upload with `algorithm` uses; AWS defaults CRC64NVME to
/// `FULL_OBJECT` and the rest to `COMPOSITE`.
fn checksum_type(algorithm: Algorithm, requested: Option<&str>) -> S3Result<&'static str> {
    let full = match requested {
        None => algorithm == Algorithm::Crc64Nvme,
        Some(t) if t.eq_ignore_ascii_case(ChecksumType::FULL_OBJECT) => true,
        Some(t) if t.eq_ignore_ascii_case(ChecksumType::COMPOSITE) => false,
        Some(_) => return Err(invalid_argument("Invalid checksum type")),
    };
    let allowed = if full {
        algorithm.combinable()
    } else {
        algorithm != Algorithm::Crc64Nvme
    };
    if !allowed {
        let t = if full { "FULL_OBJECT" } else { "COMPOSITE" };
        return Err(error(
            S3ErrorCode::InvalidRequest,
            format!(
                "The {t} checksum type cannot be used with the {} checksum algorithm.",
                algorithm.name().to_ascii_lowercase()
            ),
        ));
    }
    Ok(if full {
        ChecksumType::FULL_OBJECT
    } else {
        ChecksumType::COMPOSITE
    })
}

pub async fn create_multipart_upload<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<CreateMultipartUploadInput>,
) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
    check_checksum_support(&req.headers)?;
    let i = req.input;
    check_key(&i.key)?;
    let info = gb.require_bucket(&i.bucket).await?;
    let mut meta = ObjectMeta::new(0, String::new());
    // Explicit settings only; the bucket default applies on Complete.
    (meta.retention, meta.legal_hold) = lock::from_headers(
        &info,
        i.object_lock_mode.as_ref(),
        i.object_lock_retain_until_date.as_ref(),
        i.object_lock_legal_hold_status.as_ref(),
        gb.clock.now(),
    )?;
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
    meta.tags = parse_tagging_header(i.tagging.as_deref())?;
    meta.storage_class = storage_class(i.storage_class.as_ref())?;
    meta.sse = Sse {
        algorithm: i.server_side_encryption.map(|s| s.as_str().to_owned()),
        kms_key_id: i.ssekms_key_id,
        customer_algorithm: i.sse_customer_algorithm,
        customer_key_md5: i.sse_customer_key_md5,
        bucket_key_enabled: i.bucket_key_enabled,
    };
    let checksum_algorithm = algorithm(i.checksum_algorithm.as_ref()).and_then(Algorithm::parse);
    let checksum_type = checksum_algorithm
        .map(|a| checksum_type(a, i.checksum_type.as_ref().map(|t| t.as_str())))
        .transpose()?;
    let upload = UploadInfo {
        upload_id: uuid::Uuid::new_v4().simple().to_string(),
        key: i.key.clone(),
        initiated: now(),
        checksum_algorithm: checksum_algorithm.map(|a| a.name().to_owned()),
        checksum_type: checksum_type.map(str::to_owned),
        meta,
    };
    gb.storage
        .create_upload(&i.bucket, upload.clone())
        .await
        .map_err(storage_err)?;
    Ok(S3Response::new(CreateMultipartUploadOutput {
        bucket: Some(i.bucket),
        key: Some(i.key),
        upload_id: Some(upload.upload_id),
        checksum_algorithm: upload.checksum_algorithm.map(ChecksumAlgorithm::from),
        checksum_type: upload.checksum_type.map(ChecksumType::from),
        server_side_encryption: sse_header(&upload.meta.sse),
        ssekms_key_id: upload.meta.sse.kms_key_id,
        sse_customer_algorithm: upload.meta.sse.customer_algorithm,
        sse_customer_key_md5: upload.meta.sse.customer_key_md5,
        bucket_key_enabled: upload.meta.sse.bucket_key_enabled,
        ..Default::default()
    }))
}

pub async fn upload_part<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<UploadPartInput>,
) -> S3Result<S3Response<UploadPartOutput>> {
    require_length(&req)?;
    let trailers = Trailers::of(&req);
    let i = req.input;
    let n = part_number(i.part_number)?;
    // Fail fast on a bad upload id before reading the body.
    let upload = gb
        .storage
        .get_upload(&i.bucket, &i.key, &i.upload_id)
        .await
        .map_err(storage_err)?;
    let expected = checksums_of!(i);
    let sdk_algorithm =
        algorithm(i.checksum_algorithm.as_ref()).or(upload.checksum_algorithm.as_deref());
    // A part must carry the upload's checksum; any other could not be combined on complete.
    if let Some(want) = upload
        .checksum_algorithm
        .as_deref()
        .and_then(Algorithm::parse)
    {
        let sent = checksum::requested(&expected, sdk_algorithm, &req.headers);
        if let Some(got) = sent.into_iter().find(|a| *a != want) {
            return Err(error(
                S3ErrorCode::InvalidRequest,
                format!(
                    "Checksum Type mismatch occurred, expected checksum Type: {}, actual checksum Type: {}",
                    want.name().to_ascii_lowercase(),
                    got.name().to_ascii_lowercase()
                ),
            ));
        }
    }
    let staged = Upload {
        headers: &req.headers,
        trailers,
        expected,
        sdk_algorithm,
        always_default: upload.checksum_algorithm.is_none(),
    };
    let (blob, digests) = gb.stage(i.body, staged).await?;
    let part = PartInfo {
        part_number: n,
        size: digests.size,
        etag: crate::logic::etag::single(&digests.md5),
        last_modified: now(),
        checksums: digests.checksums,
    };
    gb.storage
        .put_part(&i.bucket, &i.key, &i.upload_id, part.clone(), blob)
        .await
        .map_err(storage_err)?;
    let mut out = UploadPartOutput {
        e_tag: Some(s3s::dto::ETag::Strong(part.etag.clone())),
        server_side_encryption: sse_header(&upload.meta.sse),
        ..Default::default()
    };
    set_checksums!(out, &part.checksums);
    Ok(S3Response::new(out))
}

/// Parses `x-amz-copy-source-range: bytes=first-last` against the source size.
fn copy_range(header: Option<&str>, size: u64) -> S3Result<std::ops::Range<u64>> {
    let Some(h) = header else { return Ok(0..size) };
    let bad = || {
        invalid_argument("The x-amz-copy-source-range value must be of the form bytes=first-last")
    };
    let (first, last) = h
        .strip_prefix("bytes=")
        .and_then(|r| r.split_once('-'))
        .ok_or_else(bad)?;
    let first: u64 = first.parse().map_err(|_| bad())?;
    let last: u64 = last.parse().map_err(|_| bad())?;
    if first > last || last >= size {
        return Err(error(
            S3ErrorCode::InvalidRange,
            "The requested range is not satisfiable",
        ));
    }
    Ok(first..last + 1)
}

pub async fn upload_part_copy<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<UploadPartCopyInput>,
) -> S3Result<S3Response<UploadPartCopyOutput>> {
    let i = req.input;
    let n = part_number(i.part_number)?;
    let (src_bucket, src_key, src_version) = copy_source(&i.copy_source)?;
    let upload = gb
        .storage
        .get_upload(&i.bucket, &i.key, &i.upload_id)
        .await
        .map_err(storage_err)?;
    let (meta, reader) = gb
        .storage
        .open_object(src_bucket, src_key, src_version)
        .await
        .map_err(|e| copy_source_err(e, src_bucket, src_key, src_version.is_some()))?;
    check_conditions(
        &meta,
        i.copy_source_if_match.as_ref(),
        i.copy_source_if_none_match.as_ref(),
        i.copy_source_if_modified_since.as_ref(),
        i.copy_source_if_unmodified_since.as_ref(),
    )
    .map_err(|e| match e.code() {
        S3ErrorCode::NotModified => precondition_failed(),
        _ => e,
    })?;
    let range = copy_range(i.copy_source_range.as_deref(), meta.size)?;
    let algorithms = checksum::or_default(
        upload
            .checksum_algorithm
            .as_deref()
            .and_then(Algorithm::parse)
            .into_iter()
            .collect(),
    );
    let (stream, handle) = checksum::hashing(reader.read(range), &algorithms);
    let blob = gb.storage.write_blob(stream).await.map_err(storage_err)?;
    let digests = handle.finish();
    let part = PartInfo {
        part_number: n,
        size: digests.size,
        etag: crate::logic::etag::single(&digests.md5),
        last_modified: now(),
        checksums: digests.checksums,
    };
    gb.storage
        .put_part(&i.bucket, &i.key, &i.upload_id, part.clone(), blob)
        .await
        .map_err(storage_err)?;
    let mut result = CopyPartResult {
        e_tag: Some(s3s::dto::ETag::Strong(part.etag.clone())),
        last_modified: Some(ts(part.last_modified)),
        ..Default::default()
    };
    set_checksums!(result, &part.checksums);
    Ok(S3Response::new(UploadPartCopyOutput {
        copy_part_result: Some(result),
        copy_source_version_id: gb.version_header(src_bucket, &meta).await?,
        ..Default::default()
    }))
}

/// A checksum the client sent for a part in the Complete body must match the stored one.
fn part_checksums_match(sent: &CompletedPart, stored: &Checksums) -> bool {
    let sent = sent.clone();
    let sent = checksums_of!(sent);
    checksum::mismatch(&sent, stored).is_none()
}

pub async fn complete_multipart_upload<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<CompleteMultipartUploadInput>,
) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
    let i = req.input;
    let (upload, stored) = match gb.storage.list_parts(&i.bucket, &i.key, &i.upload_id).await {
        Ok(v) => v,
        // A retried Complete of an upload that already finished succeeds again.
        Err(StorageError::NoSuchUpload) => {
            return match gb.storage.head_object(&i.bucket, &i.key, None).await {
                Ok(m) if m.upload_id.as_deref() == Some(i.upload_id.as_str()) => {
                    Ok(S3Response::new(complete_output(i.bucket, i.key, &m)))
                }
                _ => Err(storage_err(StorageError::NoSuchUpload)),
            };
        }
        Err(e) => return Err(storage_err(e)),
    };
    let info = gb.require_bucket(&i.bucket).await?;
    let check = check_write_conditions(gb, &i.bucket, &i.key, i.if_match, i.if_none_match).await?;
    let mut sent: Vec<CompletedPart> = i.multipart_upload.and_then(|m| m.parts).unwrap_or_default();
    // A part number listed twice in a row names the same part; the later entry wins.
    sent.dedup_by(|later, earlier| {
        let same = later.part_number == earlier.part_number;
        if same {
            std::mem::swap(later, earlier);
        }
        same
    });
    let requested: Vec<(i32, &str)> = sent
        .iter()
        .map(|p| {
            let etag = p.e_tag.as_ref().map_or("", |e| e.value());
            (p.part_number.unwrap_or(0), etag)
        })
        .collect();
    let chosen = validate::complete(&requested, &stored).map_err(|e| match e {
        CompleteError::NoParts => error(
            S3ErrorCode::MalformedXML,
            "The XML you provided was not well-formed or did not validate against our published schema",
        ),
        CompleteError::InvalidPartOrder => error(
            S3ErrorCode::InvalidPartOrder,
            "The list of parts was not in ascending order. Parts must be ordered by part number.",
        ),
        CompleteError::InvalidPart(_) => invalid_part(),
        CompleteError::EntityTooSmall(_) => error(
            S3ErrorCode::EntityTooSmall,
            "Your proposed upload is smaller than the minimum allowed object size.",
        ),
    })?;
    if sent
        .iter()
        .zip(&chosen)
        .any(|(s, c)| !part_checksums_match(s, &c.checksums))
    {
        return Err(invalid_part());
    }

    let mut meta = upload.meta.clone();
    meta.version_id = write_version(&info);
    meta.size = chosen.iter().map(|p| p.size).sum();
    meta.etag = crate::logic::etag::multipart(chosen.iter().map(|p| p.etag.as_str()));
    meta.last_modified = now();
    meta.part_sizes = chosen.iter().map(|p| p.size).collect();
    meta.upload_id = Some(i.upload_id.clone());
    meta.retention = meta
        .retention
        .or_else(|| lock::default_retention(&info, gb.clock.now()));
    meta.checksums = Checksums::default();
    meta.part_checksums = Vec::new();
    let parts: Vec<(&Checksums, u64)> = chosen.iter().map(|p| (&p.checksums, p.size)).collect();
    // Created without a checksum, parts carry the default, combined as AWS does.
    let explicit = upload
        .checksum_algorithm
        .as_deref()
        .and_then(Algorithm::parse);
    let a = explicit.unwrap_or(checksum::DEFAULT);
    let full =
        explicit.is_none() || upload.checksum_type.as_deref() == Some(ChecksumType::FULL_OBJECT);
    let value = if full {
        checksum::full_object(a, &parts)
    } else {
        checksum::composite(a, &parts)
    };
    match value {
        // Every part carries an explicit algorithm, so a value is always computable.
        None if explicit.is_some() => {
            return Err(error(
                S3ErrorCode::InvalidRequest,
                format!("A part is missing its {} checksum.", a.header()),
            ));
        }
        // Parts stored before gb computed the default.
        None => {}
        Some(v) => {
            // Only the upload's own algorithm is checked; others cannot be computed from parts.
            let sent = checksums_of!(i);
            // minio-go sends a composite value without its `-N` suffix.
            let suffix = format!("-{}", chosen.len());
            let matches = |s: &String| *s == v || (!full && v.strip_suffix(&suffix) == Some(s));
            if a.get(&sent).is_some_and(|s| !matches(s)) {
                return Err(digest_error(BodyError::Checksum(a)));
            }
            a.set(&mut meta.checksums, Some(v));
            let t = if full {
                ChecksumType::FULL_OBJECT
            } else {
                ChecksumType::COMPOSITE
            };
            meta.checksums.checksum_type = Some(t.to_owned());
            meta.part_checksums = parts
                .iter()
                .filter_map(|(c, _)| a.get(c).cloned())
                .collect();
        }
    }
    gb.storage
        .complete_upload_if(
            &i.bucket,
            &i.key,
            &i.upload_id,
            &chosen,
            meta.clone(),
            check,
        )
        .await
        .map_err(storage_err)?;
    Ok(S3Response::new(complete_output(i.bucket, i.key, &meta)))
}

fn complete_output(
    bucket: String,
    key: String,
    meta: &ObjectMeta,
) -> CompleteMultipartUploadOutput {
    let mut out = CompleteMultipartUploadOutput {
        location: Some(format!("/{bucket}/{key}")),
        bucket: Some(bucket),
        key: Some(key),
        e_tag: Some(s3s::dto::ETag::Strong(meta.etag.clone())),
        checksum_type: meta.checksums.checksum_type.clone().map(ChecksumType::from),
        server_side_encryption: sse_header(&meta.sse),
        ssekms_key_id: meta.sse.kms_key_id.clone(),
        bucket_key_enabled: meta.sse.bucket_key_enabled,
        version_id: written_version(meta),
        ..Default::default()
    };
    set_checksums!(out, &meta.checksums);
    out
}

pub async fn abort_multipart_upload<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<AbortMultipartUploadInput>,
) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
    let i = req.input;
    gb.storage
        .abort_upload(&i.bucket, &i.key, &i.upload_id)
        .await
        .map_err(storage_err)?;
    Ok(S3Response::new(AbortMultipartUploadOutput::default()))
}

fn class_or_standard(m: &ObjectMeta) -> StorageClass {
    StorageClass::from(
        m.storage_class
            .clone()
            .unwrap_or_else(|| StorageClass::STANDARD.to_owned()),
    )
}

fn initiator() -> Initiator {
    let o = owner();
    Initiator {
        display_name: o.display_name,
        id: o.id,
    }
}

pub async fn list_parts<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<ListPartsInput>,
) -> S3Result<S3Response<ListPartsOutput>> {
    let i = req.input;
    let (upload, parts) = gb
        .storage
        .list_parts(&i.bucket, &i.key, &i.upload_id)
        .await
        .map_err(storage_err)?;
    let max = match i.max_parts {
        Some(n) if n < 0 => return Err(invalid_argument("max-parts must be non-negative")),
        Some(n) => n.min(1000) as usize,
        None => 1000,
    };
    let marker = i.part_number_marker.unwrap_or(0).max(0) as u32;
    let mut rest = parts
        .into_iter()
        .filter(|p| p.part_number > marker)
        .peekable();
    let page: Vec<PartInfo> = rest.by_ref().take(max).collect();
    let truncated = rest.peek().is_some();
    let next_marker = page.last().map(|p| p.part_number as i32);
    let parts = page
        .into_iter()
        .map(|p| {
            let mut part = Part {
                part_number: Some(p.part_number as i32),
                e_tag: Some(s3s::dto::ETag::Strong(p.etag)),
                size: Some(p.size as i64),
                last_modified: Some(ts(p.last_modified)),
                ..Default::default()
            };
            set_checksums!(part, &p.checksums);
            part
        })
        .collect();
    Ok(S3Response::new(ListPartsOutput {
        bucket: Some(i.bucket),
        key: Some(i.key),
        upload_id: Some(i.upload_id),
        part_number_marker: i.part_number_marker,
        next_part_number_marker: next_marker,
        max_parts: Some(max as i32),
        is_truncated: Some(truncated),
        parts: Some(parts),
        initiator: Some(initiator()),
        owner: Some(owner()),
        storage_class: Some(class_or_standard(&upload.meta)),
        checksum_algorithm: upload
            .checksum_algorithm
            .clone()
            .map(ChecksumAlgorithm::from),
        checksum_type: upload.checksum_type.map(ChecksumType::from),
        ..Default::default()
    }))
}

pub async fn list_multipart_uploads<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<ListMultipartUploadsInput>,
) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
    let i = req.input;
    let max = match i.max_uploads {
        Some(n) if n < 0 => return Err(invalid_argument("max-uploads must be non-negative")),
        Some(n) => n.min(1000) as usize,
        None => 1000,
    };
    let prefix = i.prefix.clone().unwrap_or_default();
    let delimiter = i.delimiter.clone().filter(|d| !d.is_empty());
    let key_marker = i.key_marker.clone().unwrap_or_default();
    let uploads = gb
        .storage
        .list_uploads(&i.bucket)
        .await
        .map_err(storage_err)?;

    // Skip up to and including the marker position.
    let mut uploads: Vec<UploadInfo> = uploads
        .into_iter()
        .filter(|u| u.key.starts_with(&prefix))
        .collect();
    if !key_marker.is_empty() {
        let pos = match &i.upload_id_marker {
            Some(id) => uploads
                .iter()
                .position(|u| u.key == key_marker && &u.upload_id == id)
                .map(|p| p + 1)
                .unwrap_or_else(|| uploads.partition_point(|u| u.key <= key_marker)),
            None => uploads.partition_point(|u| u.key <= key_marker),
        };
        uploads.drain(..pos);
    }

    let mut out_uploads = Vec::new();
    let mut prefixes: Vec<String> = Vec::new();
    let mut truncated = false;
    let mut last: Option<(String, Option<String>)> = None;
    for u in uploads {
        let cp = listing::common_prefix(&u.key, &prefix, delimiter.as_deref());
        // A prefix at or before the marker was returned on an earlier page.
        if let Some(cp) = cp
            && (cp <= key_marker.as_str() || prefixes.last().is_some_and(|p| p == cp))
        {
            continue;
        }
        if out_uploads.len() + prefixes.len() == max {
            truncated = true;
            break;
        }
        match cp {
            Some(cp) => {
                last = Some((cp.to_owned(), None));
                prefixes.push(cp.to_owned());
            }
            None => {
                last = Some((u.key.clone(), Some(u.upload_id.clone())));
                out_uploads.push(MultipartUpload {
                    key: Some(u.key),
                    upload_id: Some(u.upload_id),
                    initiated: Some(ts(u.initiated)),
                    initiator: Some(initiator()),
                    owner: Some(owner()),
                    storage_class: Some(class_or_standard(&u.meta)),
                    checksum_algorithm: u.checksum_algorithm.map(ChecksumAlgorithm::from),
                    ..Default::default()
                });
            }
        }
    }
    let url = i
        .encoding_type
        .as_ref()
        .is_some_and(|t| t.as_str() == EncodingType::URL);
    let enc = |s: String| if url { listing::url_encode(&s) } else { s };
    let (next_key, next_id) = match (truncated, last) {
        (true, Some((k, id))) => (Some(enc(k)), id),
        _ => (None, None),
    };
    for u in &mut out_uploads {
        u.key = u.key.take().map(enc);
    }
    Ok(S3Response::new(ListMultipartUploadsOutput {
        bucket: Some(i.bucket),
        prefix: Some(enc(prefix)),
        delimiter: delimiter.map(enc),
        key_marker: Some(enc(key_marker)),
        upload_id_marker: Some(i.upload_id_marker.unwrap_or_default()),
        next_key_marker: next_key,
        next_upload_id_marker: next_id,
        max_uploads: Some(max as i32),
        is_truncated: Some(truncated),
        uploads: (!out_uploads.is_empty()).then_some(out_uploads),
        common_prefixes: (!prefixes.is_empty()).then(|| {
            prefixes
                .into_iter()
                .map(|p| CommonPrefix {
                    prefix: Some(enc(p)),
                })
                .collect()
        }),
        encoding_type: i.encoding_type,
        ..Default::default()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_ranges() {
        assert_eq!(copy_range(None, 10).unwrap(), 0..10);
        assert_eq!(copy_range(Some("bytes=2-4"), 10).unwrap(), 2..5);
        assert!(copy_range(Some("bytes=2-10"), 10).is_err());
        assert!(copy_range(Some("bytes=5-2"), 10).is_err());
        assert!(copy_range(Some("2-4"), 10).is_err());
        assert!(copy_range(Some("bytes=-4"), 10).is_err());
    }
}
