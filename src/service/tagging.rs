use s3s::dto::{
    DeleteBucketTaggingInput, DeleteBucketTaggingOutput, DeleteObjectTaggingInput,
    DeleteObjectTaggingOutput, GetBucketTaggingInput, GetBucketTaggingOutput,
    GetObjectTaggingInput, GetObjectTaggingOutput, PutBucketTaggingInput, PutBucketTaggingOutput,
    PutObjectTaggingInput, PutObjectTaggingOutput, Tag, TagSet, Tagging,
};
use s3s::{S3ErrorCode, S3Request, S3Response, S3Result};

use super::{GarbageBin, error, read_err, storage_err, validate_tags, version_param};
use crate::storage::{ObjectMeta, Storage, Tags};

/// Replaces the tag set of the named or current version; returns its `x-amz-version-id`.
async fn set_tags<S: Storage>(
    gb: &GarbageBin<S>,
    bucket: &str,
    key: &str,
    version: Option<&String>,
    tags: Tags,
) -> S3Result<Option<String>> {
    let version = version_param(version)?;
    let f = Box::new(move |m: &mut ObjectMeta| {
        m.tags = tags;
        Ok(())
    });
    let meta = gb
        .storage
        .update_object(bucket, key, version, f)
        .await
        .map_err(|e| read_err(e, version.is_some()))?;
    gb.version_header(bucket, &meta).await
}

fn to_tags(t: Tagging) -> S3Result<Tags> {
    let mut tags = Tags::new();
    for Tag { key, value } in t.tag_set {
        let key = key.unwrap_or_default();
        if tags.insert(key, value.unwrap_or_default()).is_some() {
            return Err(error(
                S3ErrorCode::InvalidTag,
                "Cannot provide multiple Tags with the same key",
            ));
        }
    }
    validate_tags(tags.iter())?;
    Ok(tags)
}

async fn set_bucket_tags<S: Storage>(gb: &GarbageBin<S>, bucket: &str, tags: Tags) -> S3Result<()> {
    let f = Box::new(move |b: &mut crate::storage::BucketInfo| {
        b.tags = tags;
        Ok(())
    });
    gb.storage
        .update_bucket(bucket, f)
        .await
        .map_err(storage_err)
}

fn to_tag_set(tags: Tags) -> TagSet {
    tags.into_iter()
        .map(|(k, v)| Tag {
            key: Some(k),
            value: Some(v),
        })
        .collect()
}

pub async fn put_object_tagging<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<PutObjectTaggingInput>,
) -> S3Result<S3Response<PutObjectTaggingOutput>> {
    let i = req.input;
    let tags = to_tags(i.tagging)?;
    let version_id = set_tags(gb, &i.bucket, &i.key, i.version_id.as_ref(), tags).await?;
    Ok(S3Response::new(PutObjectTaggingOutput { version_id }))
}

pub async fn get_object_tagging<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<GetObjectTaggingInput>,
) -> S3Result<S3Response<GetObjectTaggingOutput>> {
    let i = req.input;
    let version = version_param(i.version_id.as_ref())?;
    let meta = gb
        .storage
        .head_object(&i.bucket, &i.key, version)
        .await
        .map_err(|e| read_err(e, version.is_some()))?;
    let version_id = gb.version_header(&i.bucket, &meta).await?;
    Ok(S3Response::new(GetObjectTaggingOutput {
        tag_set: to_tag_set(meta.tags),
        version_id,
    }))
}

pub async fn delete_object_tagging<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<DeleteObjectTaggingInput>,
) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
    let i = req.input;
    let version_id = set_tags(gb, &i.bucket, &i.key, i.version_id.as_ref(), Tags::new()).await?;
    Ok(S3Response::new(DeleteObjectTaggingOutput { version_id }))
}

pub async fn put_bucket_tagging<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<PutBucketTaggingInput>,
) -> S3Result<S3Response<PutBucketTaggingOutput>> {
    let i = req.input;
    let tags = to_tags(i.tagging)?;
    set_bucket_tags(gb, &i.bucket, tags).await?;
    Ok(S3Response::new(PutBucketTaggingOutput::default()))
}

pub async fn get_bucket_tagging<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<GetBucketTaggingInput>,
) -> S3Result<S3Response<GetBucketTaggingOutput>> {
    let info = gb
        .storage
        .head_bucket(&req.input.bucket)
        .await
        .map_err(storage_err)?;
    if info.tags.is_empty() {
        return Err(error(
            S3ErrorCode::NoSuchTagSet,
            "The TagSet does not exist",
        ));
    }
    Ok(S3Response::new(GetBucketTaggingOutput {
        tag_set: to_tag_set(info.tags),
    }))
}

pub async fn delete_bucket_tagging<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<DeleteBucketTaggingInput>,
) -> S3Result<S3Response<DeleteBucketTaggingOutput>> {
    set_bucket_tags(gb, &req.input.bucket, Tags::new()).await?;
    Ok(S3Response::new(DeleteBucketTaggingOutput::default()))
}
