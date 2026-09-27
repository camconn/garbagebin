use s3s::dto::{
    CommonPrefix, EncodingType, ListObjectsInput, ListObjectsOutput, ListObjectsV2Input,
    ListObjectsV2Output, Object, ObjectStorageClass,
};
use s3s::{S3Request, S3Response, S3Result};

use super::{GarbageBin, etag, invalid_argument, owner, storage_err, ts};
use crate::logic::listing::{self, Entry, Page};
use crate::storage::Storage;

const MAX_KEYS: i32 = 1000;

pub(super) fn max_keys(v: Option<i32>) -> S3Result<usize> {
    match v {
        Some(n) if n < 0 => Err(invalid_argument("max-keys must be non-negative")),
        Some(n) => Ok(n.min(MAX_KEYS) as usize),
        None => Ok(MAX_KEYS as usize),
    }
}

/// Applies `encoding-type=url` when requested.
pub(super) struct Enc(bool);

impl Enc {
    pub(super) fn new(t: Option<&EncodingType>) -> Self {
        Self(t.is_some_and(|t| t.as_str() == EncodingType::URL))
    }

    pub(super) fn s(&self, s: String) -> String {
        if self.0 { listing::url_encode(&s) } else { s }
    }

    pub(super) fn opt(&self, s: Option<String>) -> Option<String> {
        s.map(|s| self.s(s))
    }
}

fn split(page: Page, enc: &Enc, with_owner: bool) -> (Vec<Object>, Vec<CommonPrefix>) {
    let mut objects = Vec::new();
    let mut prefixes = Vec::new();
    for e in page.entries {
        match e {
            Entry::Object(key, meta) => objects.push(Object {
                key: Some(enc.s(key)),
                e_tag: Some(etag(&meta)),
                last_modified: Some(ts(meta.last_modified)),
                size: Some(meta.size as i64),
                storage_class: Some(ObjectStorageClass::from(
                    meta.storage_class
                        .clone()
                        .unwrap_or_else(|| ObjectStorageClass::STANDARD.to_owned()),
                )),
                owner: with_owner.then(owner),
                ..Default::default()
            }),
            Entry::Prefix(p) => prefixes.push(CommonPrefix {
                prefix: Some(enc.s(p)),
            }),
        }
    }
    (objects, prefixes)
}

pub async fn list_objects_v2<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<ListObjectsV2Input>,
) -> S3Result<S3Response<ListObjectsV2Output>> {
    let i = req.input;
    let max = max_keys(i.max_keys)?;
    let marker = match &i.continuation_token {
        Some(t) => Some(
            listing::decode_token(t)
                .ok_or_else(|| invalid_argument("The continuation token provided is incorrect"))?,
        ),
        None => i.start_after.clone(),
    };
    let prefix = i.prefix.clone().unwrap_or_default();
    let delimiter = i.delimiter.clone().filter(|d| !d.is_empty());
    let page = listing::list(
        &gb.storage,
        &i.bucket,
        &prefix,
        delimiter.as_deref(),
        marker.as_deref(),
        max,
    )
    .await
    .map_err(storage_err)?;

    let enc = Enc::new(i.encoding_type.as_ref());
    let truncated = page.truncated;
    let next = page.next_marker.as_deref().map(listing::encode_token);
    let key_count = page.entries.len() as i32;
    let (contents, prefixes) = split(page, &enc, i.fetch_owner.unwrap_or(false));
    Ok(S3Response::new(ListObjectsV2Output {
        name: Some(i.bucket),
        prefix: Some(enc.s(prefix)),
        delimiter: enc.opt(delimiter),
        max_keys: Some(max as i32),
        key_count: Some(key_count),
        is_truncated: Some(truncated),
        continuation_token: i.continuation_token,
        next_continuation_token: next,
        start_after: enc.opt(i.start_after),
        contents: (!contents.is_empty()).then_some(contents),
        common_prefixes: (!prefixes.is_empty()).then_some(prefixes),
        encoding_type: i.encoding_type,
        ..Default::default()
    }))
}

pub async fn list_objects<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<ListObjectsInput>,
) -> S3Result<S3Response<ListObjectsOutput>> {
    let i = req.input;
    let max = max_keys(i.max_keys)?;
    let prefix = i.prefix.clone().unwrap_or_default();
    let delimiter = i.delimiter.clone().filter(|d| !d.is_empty());
    let marker = i.marker.clone().filter(|m| !m.is_empty());
    let page = listing::list(
        &gb.storage,
        &i.bucket,
        &prefix,
        delimiter.as_deref(),
        marker.as_deref(),
        max,
    )
    .await
    .map_err(storage_err)?;

    let enc = Enc::new(i.encoding_type.as_ref());
    let truncated = page.truncated;
    let next_marker = if truncated {
        enc.opt(page.next_marker.clone())
    } else {
        None
    };
    let (contents, prefixes) = split(page, &enc, true);
    Ok(S3Response::new(ListObjectsOutput {
        name: Some(i.bucket),
        // Unlike V2, AWS returns the V1 Prefix unencoded (and botocore does not decode it).
        prefix: Some(prefix),
        delimiter: enc.opt(delimiter),
        marker: Some(enc.s(marker.unwrap_or_default())),
        next_marker,
        max_keys: Some(max as i32),
        is_truncated: Some(truncated),
        contents: (!contents.is_empty()).then_some(contents),
        common_prefixes: (!prefixes.is_empty()).then_some(prefixes),
        encoding_type: i.encoding_type,
        ..Default::default()
    }))
}
