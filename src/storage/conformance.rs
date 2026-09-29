//! Backend conformance suite. Every backend must pass it unchanged:
//!
//! ```ignore
//! #[cfg(test)]
//! mod tests {
//!     crate::storage_conformance!(super::MyStorage::new());
//! }
//! ```
//!
//! The expression is evaluated once per test and must yield a fresh, empty backend.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use futures::StreamExt;

use super::{
    BucketInfo, ByteStream, Checksums, LockMode, NULL_VERSION, ObjectLock, ObjectMeta, PartInfo,
    Retention, RetentionPeriod, Sse, Storage, StorageError, Tags, UploadInfo,
};

#[macro_export]
macro_rules! storage_conformance {
    ($make:expr) => {
        $crate::storage_conformance!(@tests $make;
            buckets, bucket_tags, missing_bucket, put_get_head, meta_round_trip, overwrite,
            ranges, delete, body_error, copy, object_tags, listing, multipart,
            multipart_errors, concurrent_writes, versions, delete_markers, version_copy_and_tags,
            version_listing, lock_fields, write_checks, get_upload);
    };
    (@tests $make:expr; $($name:ident),*) => {
        $(
            #[tokio::test]
            async fn $name() {
                $crate::storage::conformance::$name($make).await;
            }
        )*
    };
}

fn stream(chunks: &[&[u8]]) -> ByteStream {
    let chunks: Vec<io::Result<Bytes>> = chunks
        .iter()
        .map(|c| Ok(Bytes::copy_from_slice(c)))
        .collect();
    Box::pin(futures::stream::iter(chunks))
}

async fn put<S: Storage>(s: &S, bucket: &str, key: &str, body: &[u8]) -> ObjectMeta {
    let blob = s.write_blob(stream(&[body])).await.unwrap();
    let meta = ObjectMeta::new(body.len() as u64, format!("etag-{}", body.len()));
    s.put_object(bucket, key, blob, meta.clone()).await.unwrap();
    meta
}

async fn read<S: Storage>(
    s: &S,
    bucket: &str,
    key: &str,
    range: Option<std::ops::Range<u64>>,
) -> Vec<u8> {
    let (meta, reader) = s.open_object(bucket, key, None).await.unwrap();
    let mut body = reader.read(range.unwrap_or(0..meta.size));
    let mut out = Vec::new();
    while let Some(chunk) = body.next().await {
        out.extend_from_slice(&chunk.unwrap());
    }
    out
}

async fn keys<S: Storage>(
    s: &S,
    bucket: &str,
    prefix: &str,
    after: Option<&str>,
    limit: usize,
) -> Vec<String> {
    let list = s.list_objects(bucket, prefix, after, limit).await.unwrap();
    list.into_iter().map(|(k, _)| k).collect()
}

async fn set_bucket_tags<S: Storage>(s: &S, bucket: &str, tags: Tags) -> super::Result<()> {
    let f = Box::new(move |b: &mut super::BucketInfo| {
        b.tags = tags;
        Ok(())
    });
    s.update_bucket(bucket, f).await
}

async fn set_object_tags<S: Storage>(
    s: &S,
    bucket: &str,
    key: &str,
    tags: Tags,
) -> super::Result<ObjectMeta> {
    let f = Box::new(move |m: &mut ObjectMeta| {
        m.tags = tags;
        Ok(())
    });
    s.update_object(bucket, key, None, f).await
}

/// Writes version `id` of `b/key`.
async fn put_version<S: Storage>(s: &S, key: &str, id: &str, body: &[u8]) -> ObjectMeta {
    let blob = s.write_blob(stream(&[body])).await.unwrap();
    let mut meta = ObjectMeta::new(body.len() as u64, format!("etag-{id}"));
    meta.version_id = id.to_owned();
    s.put_object("b", key, blob, meta.clone()).await.unwrap();
    meta
}

async fn read_version<S: Storage>(s: &S, key: &str, version: Option<&str>) -> Vec<u8> {
    let (meta, reader) = s.open_object("b", key, version).await.unwrap();
    let mut body = reader.read(0..meta.size);
    let mut out = Vec::new();
    while let Some(chunk) = body.next().await {
        out.extend_from_slice(&chunk.unwrap());
    }
    out
}

/// Every key under `prefix` with its version ids, newest first.
async fn version_ids<S: Storage>(s: &S, prefix: &str) -> Vec<(String, Vec<String>)> {
    let list = s.list_versions("b", prefix, None, 100).await.unwrap();
    list.into_iter()
        .map(|(k, v)| (k, v.into_iter().map(|m| m.version_id).collect()))
        .collect()
}

fn vk(key: &str, ids: &[&str]) -> (String, Vec<String>) {
    (key.to_owned(), ids.iter().map(|s| s.to_string()).collect())
}

fn part(n: u32, body: &[u8]) -> PartInfo {
    PartInfo {
        part_number: n,
        size: body.len() as u64,
        etag: format!("p{n}-{}", body.len()),
        last_modified: SystemTime::now(),
        checksums: Checksums::default(),
    }
}

fn upload(id: &str, key: &str) -> UploadInfo {
    UploadInfo {
        upload_id: id.to_owned(),
        key: key.to_owned(),
        initiated: SystemTime::now(),
        checksum_algorithm: None,
        checksum_type: None,
        meta: ObjectMeta::new(0, String::new()),
    }
}

macro_rules! assert_err {
    ($e:expr, $pat:pat) => {
        match $e {
            Err($pat) => {}
            other => panic!("expected {}, got {:?}", stringify!($pat), other.map(|_| ())),
        }
    };
}

pub async fn buckets<S: Storage>(s: S) {
    assert!(s.list_buckets().await.unwrap().is_empty());
    s.create_bucket("b2").await.unwrap();
    s.create_bucket("b1").await.unwrap();
    assert_err!(
        s.create_bucket("b1").await,
        StorageError::BucketAlreadyOwnedByYou
    );
    let names: Vec<_> = s
        .list_buckets()
        .await
        .unwrap()
        .into_iter()
        .map(|b| b.name)
        .collect();
    assert_eq!(names, ["b1", "b2"]);
    assert_eq!(s.head_bucket("b1").await.unwrap().name, "b1");

    put(&s, "b1", "k", b"x").await;
    assert_err!(s.delete_bucket("b1").await, StorageError::BucketNotEmpty);
    s.delete_version("b1", "k", NULL_VERSION).await.unwrap();
    s.create_upload("b1", upload("u", "k")).await.unwrap();
    s.delete_bucket("b1").await.unwrap();
    assert_err!(s.head_bucket("b1").await, StorageError::NoSuchBucket);
    assert_err!(s.delete_bucket("b1").await, StorageError::NoSuchBucket);

    // Recreated bucket starts empty.
    s.create_bucket("b1").await.unwrap();
    assert!(s.list_uploads("b1").await.unwrap().is_empty());
}

pub async fn bucket_tags<S: Storage>(s: S) {
    s.create_bucket("b").await.unwrap();
    assert!(s.head_bucket("b").await.unwrap().tags.is_empty());
    let tags = Tags::from([("a".into(), "1".into()), ("b".into(), "".into())]);
    set_bucket_tags(&s, "b", tags.clone()).await.unwrap();
    assert_eq!(s.head_bucket("b").await.unwrap().tags, tags);
    set_bucket_tags(&s, "b", Tags::new()).await.unwrap();
    assert!(s.head_bucket("b").await.unwrap().tags.is_empty());
    assert_err!(
        set_bucket_tags(&s, "nope", Tags::new()).await,
        StorageError::NoSuchBucket
    );
}

pub async fn missing_bucket<S: Storage>(s: S) {
    let blob = s.write_blob(stream(&[b"x"])).await.unwrap();
    assert_err!(
        s.put_object("nope", "k", blob, ObjectMeta::new(1, "e".into()))
            .await,
        StorageError::NoSuchBucket
    );
    assert_err!(
        s.head_object("nope", "k", None).await,
        StorageError::NoSuchBucket
    );
    assert_err!(
        s.open_object("nope", "k", None).await,
        StorageError::NoSuchBucket
    );
    assert_err!(
        s.delete_version("nope", "k", NULL_VERSION).await,
        StorageError::NoSuchBucket
    );
    assert_err!(
        s.list_objects("nope", "", None, 10).await,
        StorageError::NoSuchBucket
    );
    assert_err!(s.list_uploads("nope").await, StorageError::NoSuchBucket);
    assert_err!(
        s.create_upload("nope", upload("u", "k")).await,
        StorageError::NoSuchBucket
    );
}

pub async fn put_get_head<S: Storage>(s: S) {
    s.create_bucket("b").await.unwrap();
    let blob = s
        .write_blob(stream(&[b"hello ", b"", b"world"]))
        .await
        .unwrap();
    s.put_object("b", "k", blob, ObjectMeta::new(11, "e1".into()))
        .await
        .unwrap();
    assert_eq!(read(&s, "b", "k", None).await, b"hello world");
    assert_eq!(s.head_object("b", "k", None).await.unwrap().etag, "e1");
    assert_err!(
        s.head_object("b", "other", None).await,
        StorageError::NoSuchKey
    );
    assert_err!(
        s.open_object("b", "other", None).await,
        StorageError::NoSuchKey
    );

    // Empty body; awkward keys never touch the filesystem.
    for key in [
        "empty",
        "a/../b",
        "/lead",
        "x//y",
        "ключ",
        "k\0nul",
        &"z".repeat(1024),
    ] {
        put(&s, "b", key, b"").await;
        assert_eq!(read(&s, "b", key, None).await, b"");
    }
    // Keys differing only in case are distinct.
    put(&s, "b", "Case", b"upper").await;
    put(&s, "b", "case", b"lower").await;
    assert_eq!(read(&s, "b", "Case", None).await, b"upper");
    assert_eq!(read(&s, "b", "case", None).await, b"lower");
}

pub async fn meta_round_trip<S: Storage>(s: S) {
    s.create_bucket("b").await.unwrap();
    let meta = ObjectMeta {
        version_id: "v1".into(),
        delete_marker: false,
        size: 3,
        etag: "abc-2".into(),
        last_modified: SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
        content_type: Some("text/plain; charset=utf-8".into()),
        content_encoding: Some("gzip".into()),
        content_language: Some("en".into()),
        cache_control: Some("no-cache".into()),
        content_disposition: Some("attachment; filename=\"a b.txt\"".into()),
        expires: Some("Thu, 01 Dec 2033 16:00:00 GMT".into()),
        user_metadata: BTreeMap::from([("foo".into(), "Bar Baz".into()), ("x".into(), "".into())]),
        tags: Tags::from([("t".into(), "v".into())]),
        checksums: Checksums {
            crc32: Some("AAAAAA==".into()),
            sha256: Some("47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=".into()),
            checksum_type: Some("FULL_OBJECT".into()),
            ..Default::default()
        },
        part_sizes: vec![2, 1],
        part_checksums: vec!["AAAAAA==".into(), "AAAAAB==".into()],
        upload_id: Some("u".into()),
        sse: Sse {
            algorithm: Some("aws:kms".into()),
            kms_key_id: Some("key".into()),
            customer_algorithm: None,
            customer_key_md5: None,
            bucket_key_enabled: Some(true),
        },
        storage_class: Some("STANDARD_IA".into()),
        retention: Some(Retention {
            mode: LockMode::Compliance,
            until: SystemTime::UNIX_EPOCH + Duration::from_secs(1_900_000_000),
        }),
        legal_hold: Some(false),
    };
    let blob = s.write_blob(stream(&[b"abc"])).await.unwrap();
    s.put_object("b", "k", blob, meta.clone()).await.unwrap();
    assert_eq!(s.head_object("b", "k", None).await.unwrap(), meta);
    assert_eq!(s.open_object("b", "k", None).await.unwrap().0, meta);
    assert_eq!(
        s.list_objects("b", "", None, 10).await.unwrap(),
        [("k".to_owned(), meta)]
    );
}

pub async fn overwrite<S: Storage>(s: S) {
    s.create_bucket("b").await.unwrap();
    put(&s, "b", "k", b"first").await;
    // A reader opened before the overwrite keeps the old bytes.
    let (old_meta, old_reader) = s.open_object("b", "k", None).await.unwrap();
    put(&s, "b", "k", b"second!").await;
    assert_eq!(read(&s, "b", "k", None).await, b"second!");
    assert_eq!(s.head_object("b", "k", None).await.unwrap().size, 7);
    let mut old = old_reader.read(0..old_meta.size);
    let mut out = Vec::new();
    while let Some(c) = old.next().await {
        out.extend_from_slice(&c.unwrap());
    }
    assert_eq!(out, b"first");
    // Same content under two keys, then one overwritten: the other survives.
    put(&s, "b", "dup1", b"same").await;
    put(&s, "b", "dup2", b"same").await;
    put(&s, "b", "dup1", b"diff").await;
    assert_eq!(read(&s, "b", "dup2", None).await, b"same");
}

pub async fn ranges<S: Storage>(s: S) {
    s.create_bucket("b").await.unwrap();
    let body: Vec<u8> = (0..=255u8).cycle().take(100_000).collect();
    put(&s, "b", "k", &body).await;
    assert_eq!(read(&s, "b", "k", Some(0..1)).await, &body[0..1]);
    assert_eq!(
        read(&s, "b", "k", Some(99_999..100_000)).await,
        &body[99_999..]
    );
    assert_eq!(
        read(&s, "b", "k", Some(12_345..67_890)).await,
        &body[12_345..67_890]
    );
    assert_eq!(read(&s, "b", "k", Some(5..5)).await, b"");
}

pub async fn delete<S: Storage>(s: S) {
    s.create_bucket("b").await.unwrap();
    put(&s, "b", "k", b"x").await;
    s.delete_version("b", "k", NULL_VERSION).await.unwrap();
    assert_err!(s.head_object("b", "k", None).await, StorageError::NoSuchKey);
    s.delete_version("b", "k", NULL_VERSION).await.unwrap();
    assert!(keys(&s, "b", "", None, 10).await.is_empty());
}

pub async fn body_error<S: Storage>(s: S) {
    s.create_bucket("b").await.unwrap();
    let chunks: Vec<io::Result<Bytes>> = vec![
        Ok(Bytes::from_static(b"partial")),
        Err(io::Error::other("boom")),
    ];
    let body: ByteStream = Box::pin(futures::stream::iter(chunks));
    assert_err!(s.write_blob(body).await, StorageError::Body(_));
    // A staged blob that is dropped never becomes visible.
    drop(s.write_blob(stream(&[b"dropped"])).await.unwrap());
    assert!(keys(&s, "b", "", None, 10).await.is_empty());
}

pub async fn copy<S: Storage>(s: S) {
    s.create_bucket("b").await.unwrap();
    s.create_bucket("c").await.unwrap();
    let src = put(&s, "b", "src", b"payload").await;
    let mut meta = src.clone();
    meta.content_type = Some("text/x".into());
    s.copy_object("b", "src", None, &src.etag, "c", "dst", meta.clone())
        .await
        .unwrap();
    assert_eq!(s.head_object("c", "dst", None).await.unwrap(), meta);
    assert_eq!(read(&s, "c", "dst", None).await, b"payload");

    // Self-copy replaces metadata only.
    s.copy_object("b", "src", None, &src.etag, "b", "src", meta.clone())
        .await
        .unwrap();
    assert_eq!(
        s.head_object("b", "src", None)
            .await
            .unwrap()
            .content_type
            .as_deref(),
        Some("text/x")
    );
    assert_eq!(read(&s, "b", "src", None).await, b"payload");

    // Overwriting or deleting the source leaves the copy intact.
    put(&s, "b", "src", b"changed!").await;
    assert_eq!(read(&s, "c", "dst", None).await, b"payload");
    assert_err!(
        s.copy_object("b", "src", None, &src.etag, "c", "dst2", meta.clone())
            .await,
        StorageError::PreconditionFailed
    );
    s.delete_version("b", "src", NULL_VERSION).await.unwrap();
    assert_eq!(read(&s, "c", "dst", None).await, b"payload");
    assert_err!(
        s.copy_object("b", "src", None, &src.etag, "c", "x", meta.clone())
            .await,
        StorageError::NoSuchKey
    );
    assert_err!(
        s.copy_object("b", "dst", None, &src.etag, "nope", "x", meta.clone())
            .await,
        StorageError::NoSuchKey
    );
    assert_err!(
        s.copy_object("c", "dst", None, &src.etag, "nope", "x", meta)
            .await,
        StorageError::NoSuchBucket
    );
}

pub async fn object_tags<S: Storage>(s: S) {
    s.create_bucket("b").await.unwrap();
    put(&s, "b", "k", b"x").await;
    let tags = Tags::from([("k1".into(), "v1".into())]);
    set_object_tags(&s, "b", "k", tags.clone()).await.unwrap();
    assert_eq!(s.head_object("b", "k", None).await.unwrap().tags, tags);
    assert_eq!(read(&s, "b", "k", None).await, b"x");
    set_object_tags(&s, "b", "k", Tags::new()).await.unwrap();
    assert!(s.head_object("b", "k", None).await.unwrap().tags.is_empty());
    assert_err!(
        set_object_tags(&s, "b", "nope", tags).await,
        StorageError::NoSuchKey
    );
}

pub async fn listing<S: Storage>(s: S) {
    s.create_bucket("b").await.unwrap();
    s.create_bucket("other").await.unwrap();
    put(&s, "other", "a", b"").await;
    let all = ["a", "a/b", "a/c", "ab", "b", "é", "中", "\u{10FFFF}"];
    for k in all.iter().rev() {
        put(&s, "b", k, b"").await;
    }
    // Byte order, which for UTF-8 is code point order.
    assert_eq!(keys(&s, "b", "", None, 100).await, all);
    assert_eq!(
        keys(&s, "b", "a", None, 100).await,
        ["a", "a/b", "a/c", "ab"]
    );
    assert_eq!(keys(&s, "b", "a/", None, 100).await, ["a/b", "a/c"]);
    assert_eq!(keys(&s, "b", "", Some("a/b"), 2).await, ["a/c", "ab"]);
    assert_eq!(keys(&s, "b", "a", Some("0"), 1).await, ["a"]);
    assert_eq!(
        keys(&s, "b", "a", Some("zzz"), 10).await,
        Vec::<String>::new()
    );
    assert_eq!(keys(&s, "b", "", Some("a/"), 1).await, ["a/b"]);
    assert_eq!(keys(&s, "b", "zz", None, 10).await, Vec::<String>::new());
    assert_eq!(keys(&s, "b", "", None, 0).await, Vec::<String>::new());
}

pub async fn multipart<S: Storage>(s: S) {
    s.create_bucket("b").await.unwrap();
    s.create_upload("b", upload("u2", "k")).await.unwrap();
    s.create_upload("b", upload("u1", "a")).await.unwrap();
    let ids: Vec<_> = s
        .list_uploads("b")
        .await
        .unwrap()
        .into_iter()
        .map(|u| u.upload_id)
        .collect();
    assert_eq!(ids, ["u1", "u2"]);

    for (n, body) in [(3, &b"ccc"[..]), (1, b"a"), (2, b"wrong")] {
        let blob = s.write_blob(stream(&[body])).await.unwrap();
        s.put_part("b", "k", "u2", part(n, body), blob)
            .await
            .unwrap();
    }
    // Re-uploading a part replaces it.
    let blob = s.write_blob(stream(&[b"bb"])).await.unwrap();
    s.put_part("b", "k", "u2", part(2, b"bb"), blob)
        .await
        .unwrap();

    let (info, parts) = s.list_parts("b", "k", "u2").await.unwrap();
    assert_eq!(info.key, "k");
    assert_eq!(
        parts.iter().map(|p| p.part_number).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(
        parts[1],
        part(2, b"bb").clone_with_time(parts[1].last_modified)
    );

    // Only the listed parts are used, in the given order.
    let chosen = [parts[0].clone(), parts[2].clone()];
    let mut meta = ObjectMeta::new(4, "x-2".into());
    meta.part_sizes = vec![1, 3];
    s.complete_upload("b", "k", "u2", &chosen, meta.clone())
        .await
        .unwrap();
    assert_eq!(read(&s, "b", "k", None).await, b"accc");
    assert_eq!(s.head_object("b", "k", None).await.unwrap(), meta);
    assert_err!(
        s.list_parts("b", "k", "u2").await,
        StorageError::NoSuchUpload
    );
    let ids: Vec<_> = s
        .list_uploads("b")
        .await
        .unwrap()
        .into_iter()
        .map(|u| u.upload_id)
        .collect();
    assert_eq!(ids, ["u1"]);

    s.abort_upload("b", "a", "u1").await.unwrap();
    assert!(s.list_uploads("b").await.unwrap().is_empty());
    assert_err!(
        s.abort_upload("b", "a", "u1").await,
        StorageError::NoSuchUpload
    );
}

pub async fn multipart_errors<S: Storage>(s: S) {
    s.create_bucket("b").await.unwrap();
    s.create_upload("b", upload("u", "k")).await.unwrap();
    let blob = s.write_blob(stream(&[b"x"])).await.unwrap();
    assert_err!(
        s.put_part("b", "other", "u", part(1, b"x"), blob).await,
        StorageError::NoSuchUpload
    );
    let blob = s.write_blob(stream(&[b"x"])).await.unwrap();
    assert_err!(
        s.put_part("b", "k", "nope", part(1, b"x"), blob).await,
        StorageError::NoSuchUpload
    );
    assert_err!(
        s.list_parts("b", "other", "u").await,
        StorageError::NoSuchUpload
    );
    assert_err!(
        s.abort_upload("b", "other", "u").await,
        StorageError::NoSuchUpload
    );

    let blob = s.write_blob(stream(&[b"x"])).await.unwrap();
    s.put_part("b", "k", "u", part(1, b"x"), blob)
        .await
        .unwrap();
    let meta = ObjectMeta::new(1, "e".into());
    // Missing part.
    assert_err!(
        s.complete_upload("b", "k", "u", &[part(2, b"x")], meta.clone())
            .await,
        StorageError::InvalidPart
    );
    // Changed ETag.
    let mut stale = part(1, b"x");
    stale.etag = "stale".into();
    assert_err!(
        s.complete_upload("b", "k", "u", &[stale], meta.clone())
            .await,
        StorageError::InvalidPart
    );
    // A failed complete leaves the upload intact and creates no object.
    assert_eq!(s.list_parts("b", "k", "u").await.unwrap().1.len(), 1);
    assert_err!(s.head_object("b", "k", None).await, StorageError::NoSuchKey);
    assert_err!(
        s.complete_upload("b", "other", "u", &[part(1, b"x")], meta)
            .await,
        StorageError::NoSuchUpload
    );
}

/// A check that records what it saw and refuses unless `allow`.
fn recording_check(
    seen: &Arc<std::sync::Mutex<Vec<Option<String>>>>,
    allow: bool,
) -> super::WriteCheck {
    let seen = Arc::clone(seen);
    Box::new(move |m| {
        seen.lock().unwrap().push(m.map(|m| m.etag.clone()));
        if allow {
            Ok(())
        } else {
            Err(StorageError::PreconditionFailed)
        }
    })
}

pub async fn write_checks<S: Storage>(s: S) {
    s.create_bucket("b").await.unwrap();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let meta = |etag: &str| ObjectMeta::new(1, etag.to_owned());

    // A refused put, delete marker or complete changes nothing.
    let blob = s.write_blob(stream(&[b"x"])).await.unwrap();
    assert_err!(
        s.put_object_if("b", "k", blob, meta("e1"), recording_check(&seen, false))
            .await,
        StorageError::PreconditionFailed
    );
    assert_err!(s.head_object("b", "k", None).await, StorageError::NoSuchKey);
    let blob = s.write_blob(stream(&[b"x"])).await.unwrap();
    s.put_object_if("b", "k", blob, meta("e1"), recording_check(&seen, true))
        .await
        .unwrap();
    let mut dm = ObjectMeta::delete_marker("dm".into());
    dm.last_modified = SystemTime::now();
    assert_err!(
        s.put_delete_marker_if("b", "k", dm.clone(), recording_check(&seen, false))
            .await,
        StorageError::PreconditionFailed
    );
    assert_eq!(s.head_object("b", "k", None).await.unwrap().etag, "e1");
    s.put_delete_marker_if("b", "k", dm, recording_check(&seen, true))
        .await
        .unwrap();

    s.create_upload("b", upload("u", "k")).await.unwrap();
    let blob = s.write_blob(stream(&[b"x"])).await.unwrap();
    s.put_part("b", "k", "u", part(1, b"x"), blob)
        .await
        .unwrap();
    let parts = [part(1, b"x")];
    assert_err!(
        s.complete_upload_if(
            "b",
            "k",
            "u",
            &parts,
            meta("e2"),
            recording_check(&seen, false)
        )
        .await,
        StorageError::PreconditionFailed
    );
    s.get_upload("b", "k", "u").await.unwrap();
    s.complete_upload_if(
        "b",
        "k",
        "u",
        &parts,
        meta("e2"),
        recording_check(&seen, true),
    )
    .await
    .unwrap();
    assert_eq!(s.head_object("b", "k", None).await.unwrap().etag, "e2");

    // Checks see the current version; a delete marker counts as none.
    let e1 = Some("e1".to_owned());
    assert_eq!(
        *seen.lock().unwrap(),
        [None, None, e1.clone(), e1, None, None]
    );
}

pub async fn get_upload<S: Storage>(s: S) {
    s.create_bucket("b").await.unwrap();
    s.create_upload("b", upload("u", "k")).await.unwrap();
    let blob = s.write_blob(stream(&[b"x"])).await.unwrap();
    s.put_part("b", "k", "u", part(1, b"x"), blob)
        .await
        .unwrap();
    assert_eq!(
        s.get_upload("b", "k", "u").await.unwrap(),
        upload_info(&s).await
    );
    assert_err!(
        s.get_upload("b", "other", "u").await,
        StorageError::NoSuchUpload
    );
    assert_err!(
        s.get_upload("b", "k", "nope").await,
        StorageError::NoSuchUpload
    );
    assert_err!(
        s.get_upload("nob", "k", "u").await,
        StorageError::NoSuchBucket
    );
    s.abort_upload("b", "k", "u").await.unwrap();
    assert_err!(
        s.get_upload("b", "k", "u").await,
        StorageError::NoSuchUpload
    );
}

async fn upload_info<S: Storage>(s: &S) -> UploadInfo {
    s.list_parts("b", "k", "u").await.unwrap().0
}

pub async fn concurrent_writes<S: Storage>(s: S) {
    let s = Arc::new(s);
    s.create_bucket("b").await.unwrap();
    let tasks: Vec<_> = (0..16u8)
        .map(|i| {
            let s = Arc::clone(&s);
            tokio::spawn(async move {
                let body = vec![i; 1000 + usize::from(i)];
                let blob = s.write_blob(stream(&[&body])).await.unwrap();
                let meta = ObjectMeta::new(body.len() as u64, format!("{i}"));
                s.put_object("b", "k", blob, meta).await.unwrap();
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    // Whichever write won, row and bytes agree.
    let meta = s.head_object("b", "k", None).await.unwrap();
    let i: u8 = meta.etag.parse().unwrap();
    assert_eq!(
        read(&*s, "b", "k", None).await,
        vec![i; 1000 + usize::from(i)]
    );
}

pub async fn versions<S: Storage>(s: S) {
    s.create_bucket("b").await.unwrap();
    put(&s, "b", "k", b"null-1").await;
    put_version(&s, "k", "v1", b"one").await;
    put_version(&s, "k", "v2", b"two").await;
    assert_eq!(read_version(&s, "k", None).await, b"two");
    assert_eq!(read_version(&s, "k", Some("v1")).await, b"one");
    assert_eq!(read_version(&s, "k", Some(NULL_VERSION)).await, b"null-1");
    assert_eq!(
        s.head_object("b", "k", None).await.unwrap().version_id,
        "v2"
    );
    assert_err!(
        s.head_object("b", "k", Some("nope")).await,
        StorageError::NoSuchVersion
    );
    assert_err!(
        s.open_object("b", "gone", Some("v1")).await,
        StorageError::NoSuchVersion
    );

    // A null write replaces the null version and becomes current.
    put(&s, "b", "k", b"null-2").await;
    assert_eq!(version_ids(&s, "").await, [vk("k", &["null", "v2", "v1"])]);
    assert_eq!(read_version(&s, "k", None).await, b"null-2");

    // Removing the current version exposes the next newest.
    let gone = s.delete_version("b", "k", NULL_VERSION).await.unwrap();
    assert_eq!(gone.unwrap().size, 6);
    assert_eq!(read_version(&s, "k", None).await, b"two");
    assert!(
        s.delete_version("b", "k", NULL_VERSION)
            .await
            .unwrap()
            .is_none()
    );
    s.delete_version("b", "k", "v2").await.unwrap();
    assert_eq!(read_version(&s, "k", None).await, b"one");
    s.delete_version("b", "k", "v1").await.unwrap();
    assert_err!(s.head_object("b", "k", None).await, StorageError::NoSuchKey);
    assert!(version_ids(&s, "").await.is_empty());
    s.delete_bucket("b").await.unwrap();
}

pub async fn delete_markers<S: Storage>(s: S) {
    s.create_bucket("b").await.unwrap();
    put_version(&s, "k", "v1", b"one").await;
    s.put_delete_marker("b", "k", ObjectMeta::delete_marker("dm1".into()))
        .await
        .unwrap();
    let is_dm1 = |r: Result<(), StorageError>| matches!(r, Err(StorageError::DeleteMarker(id)) if id == "dm1");
    assert!(is_dm1(s.head_object("b", "k", None).await.map(drop)));
    assert!(is_dm1(s.open_object("b", "k", None).await.map(drop)));
    assert!(is_dm1(s.head_object("b", "k", Some("dm1")).await.map(drop)));
    assert!(is_dm1(
        set_object_tags(&s, "b", "k", Tags::new()).await.map(drop)
    ));
    assert!(is_dm1(
        s.copy_object("b", "k", None, "", "b", "c", ObjectMeta::new(0, "".into()))
            .await
    ));
    assert_eq!(read_version(&s, "k", Some("v1")).await, b"one");
    assert!(keys(&s, "b", "", None, 10).await.is_empty());
    let list = s.list_versions("b", "", None, 10).await.unwrap();
    assert!(list[0].1[0].delete_marker && !list[0].1[1].delete_marker);
    assert_err!(s.delete_bucket("b").await, StorageError::BucketNotEmpty);

    // Removing the marker restores the object.
    let gone = s.delete_version("b", "k", "dm1").await.unwrap().unwrap();
    assert!(gone.delete_marker);
    assert_eq!(read_version(&s, "k", None).await, b"one");

    // A null marker replaces the null version; a key holding only markers still counts.
    s.delete_version("b", "k", "v1").await.unwrap();
    put(&s, "b", "n", b"x").await;
    s.put_delete_marker("b", "n", ObjectMeta::delete_marker(NULL_VERSION.into()))
        .await
        .unwrap();
    assert_eq!(version_ids(&s, "").await, [vk("n", &["null"])]);
    assert!(keys(&s, "b", "", None, 10).await.is_empty());
    assert_err!(s.delete_bucket("b").await, StorageError::BucketNotEmpty);
    s.delete_version("b", "n", NULL_VERSION).await.unwrap();
    s.delete_bucket("b").await.unwrap();
}

pub async fn version_copy_and_tags<S: Storage>(s: S) {
    s.create_bucket("b").await.unwrap();
    let v1 = put_version(&s, "k", "v1", b"one").await;
    put_version(&s, "k", "v2", b"two").await;
    let mut meta = v1.clone();
    meta.version_id = "c1".into();
    s.copy_object("b", "k", Some("v1"), &v1.etag, "b", "c", meta.clone())
        .await
        .unwrap();
    assert_eq!(read_version(&s, "c", None).await, b"one");
    assert_eq!(s.head_object("b", "c", None).await.unwrap(), meta);
    assert_err!(
        s.copy_object("b", "k", Some("v2"), &v1.etag, "b", "c", meta.clone())
            .await,
        StorageError::PreconditionFailed
    );
    assert_err!(
        s.copy_object("b", "k", Some("v9"), &v1.etag, "b", "c", meta)
            .await,
        StorageError::NoSuchVersion
    );

    // Tags live on one version.
    let tags = Tags::from([("t".into(), "1".into())]);
    let t = tags.clone();
    let f = Box::new(move |m: &mut ObjectMeta| {
        m.tags = t;
        Ok(())
    });
    let updated = s.update_object("b", "k", Some("v1"), f).await.unwrap();
    assert_eq!((updated.version_id.as_str(), &updated.tags), ("v1", &tags));
    assert_eq!(
        s.head_object("b", "k", Some("v1")).await.unwrap().tags,
        tags
    );
    assert!(s.head_object("b", "k", None).await.unwrap().tags.is_empty());
    let f = Box::new(|_: &mut ObjectMeta| Err(StorageError::Internal("refused".into())));
    assert_err!(
        s.update_object("b", "k", Some("v1"), f).await,
        StorageError::Internal(_)
    );
    assert_eq!(
        s.head_object("b", "k", Some("v1")).await.unwrap().tags,
        tags
    );

    // Old versions keep their bytes; so does a copy once its source is gone.
    put_version(&s, "k", "v3", b"one").await;
    s.delete_version("b", "k", "v1").await.unwrap();
    assert_eq!(read_version(&s, "k", Some("v2")).await, b"two");
    s.delete_version("b", "k", "v3").await.unwrap();
    s.delete_version("b", "k", "v2").await.unwrap();
    assert_eq!(read_version(&s, "c", None).await, b"one");
}

pub async fn version_listing<S: Storage>(s: S) {
    s.create_bucket("b").await.unwrap();
    put_version(&s, "b", "b1", b"").await;
    s.put_delete_marker("b", "b", ObjectMeta::delete_marker("b2".into()))
        .await
        .unwrap();
    put_version(&s, "a/b", "ab1", b"").await;
    put_version(&s, "a", "a1", b"").await;
    put(&s, "b", "a", b"").await;
    assert_eq!(
        version_ids(&s, "").await,
        [
            vk("a", &["null", "a1"]),
            vk("a/b", &["ab1"]),
            vk("b", &["b2", "b1"])
        ]
    );
    assert_eq!(version_ids(&s, "a/").await, [vk("a/b", &["ab1"])]);
    let after = s.list_versions("b", "", Some("a"), 1).await.unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].0, "a/b");
    assert_eq!(keys(&s, "b", "", None, 10).await, ["a", "a/b"]);
    // Skipped marker keys do not count against the limit.
    assert_eq!(keys(&s, "b", "a/", None, 1).await, ["a/b"]);
    assert_eq!(
        keys(&s, "b", "", Some("a/b"), 1).await,
        Vec::<String>::new()
    );
}

/// Lock config on the bucket row, and closures that refuse change nothing.
pub async fn lock_fields<S: Storage>(s: S) {
    let refuse = || StorageError::Refused("locked".into());
    s.create_bucket("b").await.unwrap();
    assert_eq!(s.head_bucket("b").await.unwrap().object_lock, None);
    let lock = ObjectLock {
        default_retention: Some((LockMode::Governance, RetentionPeriod::Years(2))),
    };
    let f = Box::new(move |b: &mut BucketInfo| {
        b.object_lock = Some(lock);
        Ok(())
    });
    s.update_bucket("b", f).await.unwrap();
    assert_eq!(s.head_bucket("b").await.unwrap().object_lock, Some(lock));
    let f = Box::new(move |b: &mut BucketInfo| {
        b.object_lock = None;
        Err(refuse())
    });
    assert_err!(s.update_bucket("b", f).await, StorageError::Refused(_));
    assert_eq!(s.head_bucket("b").await.unwrap().object_lock, Some(lock));

    let v1 = put_version(&s, "k", "v1", b"one").await;
    let f = Box::new(move |m: &mut ObjectMeta| {
        m.legal_hold = Some(true);
        Ok(())
    });
    let held = s.update_object("b", "k", Some("v1"), f).await.unwrap();
    assert_eq!(held.legal_hold, Some(true));
    let f = Box::new(move |m: &mut ObjectMeta| {
        m.legal_hold = None;
        Err(refuse())
    });
    assert_err!(
        s.update_object("b", "k", Some("v1"), f).await,
        StorageError::Refused(_)
    );
    assert_eq!(s.head_object("b", "k", None).await.unwrap(), held);

    // The check sees the version (delete markers included) and can veto its removal.
    s.put_delete_marker("b", "k", ObjectMeta::delete_marker("dm".into()))
        .await
        .unwrap();
    let held_only = || -> super::DeleteCheck {
        Box::new(|m| match m.legal_hold {
            Some(true) => Err(StorageError::Refused("held".into())),
            _ => Ok(()),
        })
    };
    assert_err!(
        s.delete_version_if("b", "k", "v1", held_only()).await,
        StorageError::Refused(_)
    );
    assert_eq!(read_version(&s, "k", Some("v1")).await, b"one");
    let gone = s.delete_version_if("b", "k", "dm", held_only()).await;
    assert!(gone.unwrap().unwrap().delete_marker);
    assert!(
        s.delete_version_if("b", "k", "nope", held_only())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(s.head_object("b", "k", None).await.unwrap().etag, v1.etag);
    s.delete_version("b", "k", "v1").await.unwrap();
    s.delete_bucket("b").await.unwrap();
}

impl PartInfo {
    fn clone_with_time(&self, t: SystemTime) -> Self {
        Self {
            last_modified: t,
            ..self.clone()
        }
    }
}
