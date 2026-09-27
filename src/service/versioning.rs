use s3s::dto::{
    BucketVersioningStatus, CommonPrefix, DeleteMarkerEntry, GetBucketVersioningInput,
    GetBucketVersioningOutput, ListObjectVersionsInput, ListObjectVersionsOutput, MFADelete,
    ObjectVersion, ObjectVersionStorageClass, PutBucketVersioningInput, PutBucketVersioningOutput,
};
use s3s::{S3ErrorCode, S3Request, S3Response, S3Result};

use super::list::{Enc, max_keys};
use super::{GarbageBin, error, etag, invalid_argument, owner, storage_err, ts};
use crate::logic::listing::{self, VersionEntry};
use crate::storage::{BucketInfo, Storage, Versioning};

pub async fn put_bucket_versioning<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<PutBucketVersioningInput>,
) -> S3Result<S3Response<PutBucketVersioningOutput>> {
    let i = req.input;
    let cfg = i.versioning_configuration;
    if cfg
        .mfa_delete
        .as_ref()
        .is_some_and(|m| m.as_str() == MFADelete::ENABLED)
    {
        return Err(error(
            S3ErrorCode::NotImplemented,
            "MFA delete is not supported",
        ));
    }
    let status = match cfg.status.as_ref().map(|s| s.as_str()) {
        Some(BucketVersioningStatus::ENABLED) => Versioning::Enabled,
        Some(BucketVersioningStatus::SUSPENDED) => Versioning::Suspended,
        _ => {
            return Err(error(
                S3ErrorCode::IllegalVersioningConfigurationException,
                "The Versioning element must be specified",
            ));
        }
    };
    let f = Box::new(move |b: &mut BucketInfo| {
        b.versioning = Some(status);
        Ok(())
    });
    gb.storage
        .update_bucket(&i.bucket, f)
        .await
        .map_err(storage_err)?;
    Ok(S3Response::new(PutBucketVersioningOutput::default()))
}

pub async fn get_bucket_versioning<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<GetBucketVersioningInput>,
) -> S3Result<S3Response<GetBucketVersioningOutput>> {
    let info = gb.require_bucket(&req.input.bucket).await?;
    let status = info.versioning.map(|v| {
        BucketVersioningStatus::from_static(match v {
            Versioning::Enabled => BucketVersioningStatus::ENABLED,
            Versioning::Suspended => BucketVersioningStatus::SUSPENDED,
        })
    });
    Ok(S3Response::new(GetBucketVersioningOutput {
        status,
        mfa_delete: None,
    }))
}

pub async fn list_object_versions<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<ListObjectVersionsInput>,
) -> S3Result<S3Response<ListObjectVersionsOutput>> {
    let i = req.input;
    let max = max_keys(i.max_keys)?;
    let prefix = i.prefix.clone().unwrap_or_default();
    let delimiter = i.delimiter.clone().filter(|d| !d.is_empty());
    let key_marker = i.key_marker.clone().filter(|m| !m.is_empty());
    let version_marker = i.version_id_marker.clone().filter(|m| !m.is_empty());
    if version_marker.is_some() && key_marker.is_none() {
        return Err(invalid_argument(
            "A version-id marker cannot be specified without a key marker.",
        ));
    }
    let page = listing::list_versions(
        &gb.storage,
        &i.bucket,
        &prefix,
        delimiter.as_deref(),
        key_marker.as_deref(),
        version_marker.as_deref(),
        max,
    )
    .await
    .map_err(storage_err)?;

    let enc = Enc::new(i.encoding_type.as_ref());
    let mut versions = Vec::new();
    let mut markers = Vec::new();
    let mut prefixes = Vec::new();
    for e in page.entries {
        match e {
            VersionEntry::Version { key, meta, latest } if meta.delete_marker => {
                markers.push(DeleteMarkerEntry {
                    key: Some(enc.s(key)),
                    version_id: Some(meta.version_id),
                    is_latest: Some(latest),
                    last_modified: Some(ts(meta.last_modified)),
                    owner: Some(owner()),
                })
            }
            VersionEntry::Version { key, meta, latest } => versions.push(ObjectVersion {
                key: Some(enc.s(key)),
                version_id: Some(meta.version_id.clone()),
                is_latest: Some(latest),
                last_modified: Some(ts(meta.last_modified)),
                e_tag: Some(etag(&meta)),
                size: Some(meta.size as i64),
                storage_class: Some(ObjectVersionStorageClass::from(
                    meta.storage_class
                        .clone()
                        .unwrap_or_else(|| ObjectVersionStorageClass::STANDARD.to_owned()),
                )),
                owner: Some(owner()),
                ..Default::default()
            }),
            VersionEntry::Prefix(p) => prefixes.push(CommonPrefix {
                prefix: Some(enc.s(p)),
            }),
        }
    }
    Ok(S3Response::new(ListObjectVersionsOutput {
        name: Some(i.bucket),
        prefix: Some(enc.s(prefix)),
        delimiter: enc.opt(delimiter),
        key_marker: Some(enc.s(key_marker.unwrap_or_default())),
        version_id_marker: Some(version_marker.unwrap_or_default()),
        next_key_marker: enc.opt(page.next_key_marker),
        next_version_id_marker: page.next_version_marker,
        max_keys: Some(max as i32),
        is_truncated: Some(page.truncated),
        versions: (!versions.is_empty()).then_some(versions),
        delete_markers: (!markers.is_empty()).then_some(markers),
        common_prefixes: (!prefixes.is_empty()).then_some(prefixes),
        encoding_type: i.encoding_type,
        ..Default::default()
    }))
}
