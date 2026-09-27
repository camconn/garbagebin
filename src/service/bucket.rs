use s3s::dto::*;
use s3s::{S3ErrorCode, S3Request, S3Response, S3Result};

use super::{GarbageBin, error, invalid_argument, owner, storage_err, ts};
use crate::logic::listing;
use crate::logic::validate;
use crate::storage::{BucketInfo, ObjectLock, Storage, StorageError, Versioning};

const US_EAST_1: &str = "us-east-1";
const MAX_BUCKETS: i32 = 10_000;

pub async fn create_bucket<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<CreateBucketInput>,
) -> S3Result<S3Response<CreateBucketOutput>> {
    let lock = req.input.object_lock_enabled_for_bucket.unwrap_or(false);
    let bucket = req.input.bucket;
    if !validate::bucket_name(&bucket) {
        return Err(error(
            S3ErrorCode::InvalidBucketName,
            "The specified bucket is not valid.",
        ));
    }
    match gb.storage.create_bucket(&bucket).await {
        Ok(()) if lock => {
            let f = Box::new(|b: &mut BucketInfo| {
                b.versioning = Some(Versioning::Enabled);
                b.object_lock = Some(ObjectLock::default());
                Ok(())
            });
            gb.storage
                .update_bucket(&bucket, f)
                .await
                .map_err(storage_err)?;
        }
        Ok(()) => {}
        // us-east-1 answers a repeat create by the owner with 200.
        Err(StorageError::BucketAlreadyOwnedByYou) if gb.region == US_EAST_1 => {}
        Err(e) => return Err(storage_err(e)),
    }
    Ok(S3Response::new(CreateBucketOutput {
        location: Some(format!("/{bucket}")),
        ..Default::default()
    }))
}

pub async fn delete_bucket<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<DeleteBucketInput>,
) -> S3Result<S3Response<DeleteBucketOutput>> {
    gb.storage
        .delete_bucket(&req.input.bucket)
        .await
        .map_err(storage_err)?;
    Ok(S3Response::new(DeleteBucketOutput::default()))
}

pub async fn head_bucket<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<HeadBucketInput>,
) -> S3Result<S3Response<HeadBucketOutput>> {
    gb.require_bucket(&req.input.bucket).await?;
    Ok(S3Response::new(HeadBucketOutput {
        bucket_region: Some(gb.region.clone()),
        ..Default::default()
    }))
}

pub async fn list_buckets<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<ListBucketsInput>,
) -> S3Result<S3Response<ListBucketsOutput>> {
    let i = req.input;
    let prefix = i.prefix.unwrap_or_default();
    let max = match i.max_buckets {
        Some(n) if !(1..=MAX_BUCKETS).contains(&n) => {
            return Err(invalid_argument(
                "Argument max-buckets must be an integer between 1 and 10000",
            ));
        }
        Some(n) => n as usize,
        None => usize::MAX,
    };
    let after = match &i.continuation_token {
        Some(t) => Some(
            listing::decode_token(t)
                .ok_or_else(|| invalid_argument("The continuation token provided is incorrect"))?,
        ),
        None => None,
    };
    let other_region = i.bucket_region.is_some_and(|r| r != gb.region);
    let all = gb.storage.list_buckets().await.map_err(storage_err)?;
    let mut matching = all
        .into_iter()
        .filter(|b| !other_region && b.name.starts_with(&prefix))
        .filter(|b| after.as_ref().is_none_or(|a| b.name > *a));
    let page: Vec<_> = matching.by_ref().take(max).collect();
    let next = match (matching.next(), page.last()) {
        (Some(_), Some(last)) => Some(listing::encode_token(&last.name)),
        _ => None,
    };
    let buckets = page
        .into_iter()
        .map(|b| Bucket {
            name: Some(b.name),
            creation_date: Some(ts(b.created)),
            bucket_region: Some(gb.region.clone()),
            ..Default::default()
        })
        .collect();
    Ok(S3Response::new(ListBucketsOutput {
        buckets: Some(buckets),
        owner: Some(owner()),
        prefix: (!prefix.is_empty()).then_some(prefix),
        continuation_token: next,
    }))
}

pub async fn get_bucket_location<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<GetBucketLocationInput>,
) -> S3Result<S3Response<GetBucketLocationOutput>> {
    gb.require_bucket(&req.input.bucket).await?;
    // AWS reports us-east-1 as an empty constraint.
    let location_constraint =
        (gb.region != US_EAST_1).then(|| BucketLocationConstraint::from(gb.region.clone()));
    Ok(S3Response::new(GetBucketLocationOutput {
        location_constraint,
    }))
}
