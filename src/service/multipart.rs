use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, ChecksumAlgorithm, ChecksumType,
    CommonPrefix, CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CompletedPart,
    CopyPartResult, CreateMultipartUploadInput, CreateMultipartUploadOutput, EncodingType,
    Initiator, ListMultipartUploadsInput, ListMultipartUploadsOutput, ListPartsInput,
    ListPartsOutput, MultipartUpload, Part, ServerSideEncryption, StorageClass,
    UploadPartCopyInput, UploadPartCopyOutput, UploadPartInput, UploadPartOutput,
};
use s3s::{S3ErrorCode, S3Request, S3Response, S3Result};

use super::object::{copy_source, copy_source_err};
use super::{
    DEFAULT_CONTENT_TYPE, GarbageBin, Trailers, Upload, algorithm, check_checksum_support,
    check_conditions, check_key, check_write_conditions, content_encoding, digest_error, error,
    expected_checksums, invalid_argument, invalid_part, now, owner, parse_tagging_header,
    precondition_failed, storage_class, storage_err, ts, write_version, written_version,
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
    let mut meta = ObjectMeta::new(0, String::new());
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
        server_side_encryption: upload.meta.sse.algorithm.map(ServerSideEncryption::from),
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
    let trailers = Trailers::of(&req);
    let i = req.input;
    let n = part_number(i.part_number)?;
    // Fail fast on a bad upload id before reading the body.
    let (upload, _) = gb
        .storage
        .list_parts(&i.bucket, &i.key, &i.upload_id)
        .await
        .map_err(storage_err)?;
    let expected = expected_checksums(
        i.checksum_crc32,
        i.checksum_crc32c,
        i.checksum_crc64nvme,
        i.checksum_sha1,
        i.checksum_sha256,
    );
    let sdk_algorithm =
        algorithm(i.checksum_algorithm.as_ref()).or(upload.checksum_algorithm.as_deref());
    let staged = Upload {
        headers: &req.headers,
        trailers,
        expected,
        sdk_algorithm,
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
    let c = &part.checksums;
    Ok(S3Response::new(UploadPartOutput {
        e_tag: Some(s3s::dto::ETag::Strong(part.etag.clone())),
        checksum_crc32: c.crc32.clone(),
        checksum_crc32c: c.crc32c.clone(),
        checksum_crc64nvme: c.crc64nvme.clone(),
        checksum_sha1: c.sha1.clone(),
        checksum_sha256: c.sha256.clone(),
        server_side_encryption: upload.meta.sse.algorithm.map(ServerSideEncryption::from),
        ..Default::default()
    }))
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
    let (upload, _) = gb
        .storage
        .list_parts(&i.bucket, &i.key, &i.upload_id)
        .await
        .map_err(storage_err)?;
    let (meta, reader) = gb
        .storage
        .open_object(src_bucket, src_key, src_version)
        .await
        .map_err(|e| copy_source_err(e, src_version.is_some()))?;
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
    let algorithms: Vec<Algorithm> = upload
        .checksum_algorithm
        .as_deref()
        .and_then(Algorithm::parse)
        .into_iter()
        .collect();
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
    let c = &part.checksums;
    Ok(S3Response::new(UploadPartCopyOutput {
        copy_part_result: Some(CopyPartResult {
            e_tag: Some(s3s::dto::ETag::Strong(part.etag.clone())),
            last_modified: Some(ts(part.last_modified)),
            checksum_crc32: c.crc32.clone(),
            checksum_crc32c: c.crc32c.clone(),
            checksum_crc64nvme: c.crc64nvme.clone(),
            checksum_sha1: c.sha1.clone(),
            checksum_sha256: c.sha256.clone(),
            ..Default::default()
        }),
        copy_source_version_id: gb.version_header(src_bucket, &meta).await?,
        ..Default::default()
    }))
}

/// A checksum the client sent for a part in the Complete body must match the stored one.
fn part_checksums_match(sent: &CompletedPart, stored: &Checksums) -> bool {
    let sent = expected_checksums(
        sent.checksum_crc32.clone(),
        sent.checksum_crc32c.clone(),
        sent.checksum_crc64nvme.clone(),
        sent.checksum_sha1.clone(),
        sent.checksum_sha256.clone(),
    );
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
    check_write_conditions(
        gb,
        &i.bucket,
        &i.key,
        i.if_match.as_ref(),
        i.if_none_match.as_ref(),
    )
    .await?;
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
    meta.checksums = Checksums::default();
    if let Some(a) = upload
        .checksum_algorithm
        .as_deref()
        .and_then(Algorithm::parse)
    {
        let full = upload.checksum_type.as_deref() == Some(ChecksumType::FULL_OBJECT);
        let parts: Vec<(&Checksums, u64)> = chosen.iter().map(|p| (&p.checksums, p.size)).collect();
        let value = if full {
            checksum::full_object(a, &parts)
        } else {
            checksum::composite(a, &parts)
        };
        if let Some(v) = value {
            // Only the upload's own algorithm is checked; others cannot be computed from parts.
            let sent = expected_checksums(
                i.checksum_crc32,
                i.checksum_crc32c,
                i.checksum_crc64nvme,
                i.checksum_sha1,
                i.checksum_sha256,
            );
            // minio-go sends a composite value without its `-N` suffix.
            let suffix = format!("-{}", chosen.len());
            let matches = |s: &String| *s == v || (!full && v.strip_suffix(&suffix) == Some(s));
            if a.get(&sent).is_some_and(|s| !matches(s)) {
                return Err(digest_error(BodyError::Checksum(a)));
            }
            a.set(&mut meta.checksums, Some(v));
            meta.checksums.checksum_type = upload.checksum_type.clone();
        }
    }
    gb.storage
        .complete_upload(&i.bucket, &i.key, &i.upload_id, &chosen, meta.clone())
        .await
        .map_err(storage_err)?;
    Ok(S3Response::new(complete_output(i.bucket, i.key, &meta)))
}

fn complete_output(
    bucket: String,
    key: String,
    meta: &ObjectMeta,
) -> CompleteMultipartUploadOutput {
    let c = &meta.checksums;
    CompleteMultipartUploadOutput {
        location: Some(format!("/{bucket}/{key}")),
        bucket: Some(bucket),
        key: Some(key),
        e_tag: Some(s3s::dto::ETag::Strong(meta.etag.clone())),
        checksum_crc32: c.crc32.clone(),
        checksum_crc32c: c.crc32c.clone(),
        checksum_crc64nvme: c.crc64nvme.clone(),
        checksum_sha1: c.sha1.clone(),
        checksum_sha256: c.sha256.clone(),
        checksum_type: c.checksum_type.clone().map(ChecksumType::from),
        server_side_encryption: meta.sse.algorithm.clone().map(ServerSideEncryption::from),
        ssekms_key_id: meta.sse.kms_key_id.clone(),
        bucket_key_enabled: meta.sse.bucket_key_enabled,
        version_id: written_version(meta),
        ..Default::default()
    }
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
        .map(|p| Part {
            part_number: Some(p.part_number as i32),
            e_tag: Some(s3s::dto::ETag::Strong(p.etag)),
            size: Some(p.size as i64),
            last_modified: Some(ts(p.last_modified)),
            checksum_crc32: p.checksums.crc32,
            checksum_crc32c: p.checksums.crc32c,
            checksum_crc64nvme: p.checksums.crc64nvme,
            checksum_sha1: p.checksums.sha1,
            checksum_sha256: p.checksums.sha256,
            ..Default::default()
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
        if let Some(cp) = cp
            && prefixes.last().is_some_and(|p| p == cp)
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
