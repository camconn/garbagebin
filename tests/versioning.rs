//! Bucket versioning end to end through the Rust AWS SDK, on both backends.

mod common;

use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, Delete, EncodingType,
    MfaDelete, ObjectIdentifier, Tag, Tagging, VersioningConfiguration,
};
use common::{Backend, code, start, status};

const MIB: usize = 1024 * 1024;

async fn set_versioning(c: &Client, bucket: &str, s: BucketVersioningStatus) {
    let cfg = VersioningConfiguration::builder().status(s).build();
    c.put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(cfg)
        .send()
        .await
        .unwrap();
}

/// A bucket with versioning enabled.
async fn versioned(c: &Client, bucket: &str) {
    c.create_bucket().bucket(bucket).send().await.unwrap();
    set_versioning(c, bucket, BucketVersioningStatus::Enabled).await;
}

/// PUT; returns the response's version id.
async fn put(c: &Client, bucket: &str, key: &str, body: &str) -> Option<String> {
    c.put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body.as_bytes().to_vec()))
        .send()
        .await
        .unwrap()
        .version_id
}

/// GET of `version` (or the current one): body and `x-amz-version-id`.
async fn get(
    c: &Client,
    bucket: &str,
    key: &str,
    version: Option<&str>,
) -> (String, Option<String>) {
    let out = c
        .get_object()
        .bucket(bucket)
        .key(key)
        .set_version_id(version.map(str::to_owned))
        .send()
        .await
        .unwrap();
    let v = out.version_id.clone();
    let body = out.body.collect().await.unwrap().into_bytes();
    (String::from_utf8(body.to_vec()).unwrap(), v)
}

/// `(key, version id, is delete marker, is latest)` for every entry, in listing order per kind.
async fn versions(c: &Client, bucket: &str) -> Vec<(String, String, bool, bool)> {
    let out = c
        .list_object_versions()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    let mut all: Vec<_> = out
        .versions()
        .iter()
        .map(|v| {
            let id = v.version_id().unwrap().to_owned();
            (
                v.key().unwrap().to_owned(),
                id,
                false,
                v.is_latest().unwrap(),
            )
        })
        .chain(out.delete_markers().iter().map(|d| {
            let id = d.version_id().unwrap().to_owned();
            (
                d.key().unwrap().to_owned(),
                id,
                true,
                d.is_latest().unwrap(),
            )
        }))
        .collect();
    all.sort_by_key(|(k, _, _, latest)| (k.clone(), !latest));
    all
}

fn header<E>(
    e: &SdkError<E, aws_sdk_s3::config::http::HttpResponse>,
    name: &str,
) -> Option<String> {
    e.raw_response()?.headers().get(name).map(str::to_owned)
}

async fn bucket_configuration(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    let out = c
        .get_bucket_versioning()
        .bucket("bkt")
        .send()
        .await
        .unwrap();
    assert_eq!(out.status(), None);
    assert_eq!(out.mfa_delete(), None);
    for st in [
        BucketVersioningStatus::Suspended,
        BucketVersioningStatus::Enabled,
        BucketVersioningStatus::Suspended,
    ] {
        set_versioning(&c, "bkt", st.clone()).await;
        let out = c
            .get_bucket_versioning()
            .bucket("bkt")
            .send()
            .await
            .unwrap();
        assert_eq!(out.status(), Some(&st));
    }

    // MFA delete is not supported; `Disabled` is accepted.
    let cfg = VersioningConfiguration::builder()
        .status(BucketVersioningStatus::Enabled)
        .mfa_delete(MfaDelete::Enabled)
        .build();
    let e = c
        .put_bucket_versioning()
        .bucket("bkt")
        .versioning_configuration(cfg)
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "NotImplemented");
    let cfg = VersioningConfiguration::builder()
        .status(BucketVersioningStatus::Enabled)
        .mfa_delete(MfaDelete::Disabled)
        .build();
    c.put_bucket_versioning()
        .bucket("bkt")
        .versioning_configuration(cfg)
        .send()
        .await
        .unwrap();

    // There is no way back to unversioned.
    let e = c
        .put_bucket_versioning()
        .bucket("bkt")
        .versioning_configuration(VersioningConfiguration::builder().build())
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "IllegalVersioningConfigurationException");

    let e = c
        .get_bucket_versioning()
        .bucket("nope")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "NoSuchBucket");
}

async fn enabled_puts_add_versions(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    versioned(&c, "bkt").await;
    let mut ids = Vec::new();
    for body in ["one", "two", "three"] {
        ids.push(put(&c, "bkt", "k", body).await.unwrap());
    }
    assert_ne!(ids[0], ids[1]);
    assert_ne!(ids[1], ids[2]);
    assert_eq!(
        get(&c, "bkt", "k", None).await,
        ("three".into(), Some(ids[2].clone()))
    );
    for (id, body) in ids.iter().zip(["one", "two", "three"]) {
        assert_eq!(
            get(&c, "bkt", "k", Some(id)).await,
            (body.into(), Some(id.clone()))
        );
        let head = c
            .head_object()
            .bucket("bkt")
            .key("k")
            .version_id(id)
            .send()
            .await
            .unwrap();
        assert_eq!(head.version_id(), Some(id.as_str()));
        assert_eq!(head.content_length(), Some(body.len() as i64));
    }
    let e = c
        .get_object()
        .bucket("bkt")
        .key("k")
        .version_id("nope")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e.into_service_error()), "NoSuchVersion");

    // Newest first; only the newest is latest.
    let out = c.list_object_versions().bucket("bkt").send().await.unwrap();
    let listed: Vec<_> = out
        .versions()
        .iter()
        .map(|v| {
            (
                v.version_id().unwrap(),
                v.is_latest().unwrap(),
                v.size().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        listed,
        [
            (ids[2].as_str(), true, 5),
            (ids[1].as_str(), false, 3),
            (ids[0].as_str(), false, 3)
        ]
    );
    assert!(out.delete_markers().is_empty());
    let keys = c.list_objects_v2().bucket("bkt").send().await.unwrap();
    assert_eq!(keys.key_count(), Some(1));

    // Removing the current version exposes the previous one.
    let out = c
        .delete_object()
        .bucket("bkt")
        .key("k")
        .version_id(&ids[2])
        .send()
        .await
        .unwrap();
    assert_eq!(out.version_id(), Some(ids[2].as_str()));
    assert_eq!(out.delete_marker(), None);
    assert_eq!(get(&c, "bkt", "k", None).await.0, "two");
}

async fn null_versions(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    assert_eq!(put(&c, "bkt", "k", "plain").await, None);
    assert_eq!(get(&c, "bkt", "k", None).await, ("plain".into(), None));
    assert_eq!(get(&c, "bkt", "k", Some("null")).await.0, "plain");

    // Once versioning is on, the pre-existing object is the null version.
    set_versioning(&c, "bkt", BucketVersioningStatus::Enabled).await;
    assert_eq!(get(&c, "bkt", "k", None).await.1.as_deref(), Some("null"));
    let v1 = put(&c, "bkt", "k", "v1").await.unwrap();
    assert_eq!(
        versions(&c, "bkt").await,
        [
            ("k".into(), v1.clone(), false, true),
            ("k".into(), "null".into(), false, false)
        ]
    );

    // Suspended: a PUT replaces the null version and returns no version id.
    set_versioning(&c, "bkt", BucketVersioningStatus::Suspended).await;
    assert_eq!(put(&c, "bkt", "k", "s1").await, None);
    assert_eq!(put(&c, "bkt", "k", "s2").await, None);
    assert_eq!(
        get(&c, "bkt", "k", None).await,
        ("s2".into(), Some("null".into()))
    );
    assert_eq!(get(&c, "bkt", "k", Some(&v1)).await.0, "v1");
    assert_eq!(
        versions(&c, "bkt").await,
        [
            ("k".into(), "null".into(), false, true),
            ("k".into(), v1.clone(), false, false)
        ]
    );

    // A suspended DELETE replaces the null version with a null delete marker.
    let out = c
        .delete_object()
        .bucket("bkt")
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(out.delete_marker(), Some(true));
    assert_eq!(out.version_id(), Some("null"));
    assert_eq!(
        versions(&c, "bkt").await,
        [
            ("k".into(), "null".into(), true, true),
            ("k".into(), v1.clone(), false, false)
        ]
    );
    c.delete_object()
        .bucket("bkt")
        .key("k")
        .version_id("null")
        .send()
        .await
        .unwrap();
    assert_eq!(get(&c, "bkt", "k", None).await.0, "v1");

    // Back to enabled: new versions stack on top.
    set_versioning(&c, "bkt", BucketVersioningStatus::Enabled).await;
    let v2 = put(&c, "bkt", "k", "v2").await.unwrap();
    assert_eq!(versions(&c, "bkt").await.len(), 2);
    assert_eq!(get(&c, "bkt", "k", None).await, ("v2".into(), Some(v2)));
}

async fn delete_markers(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    versioned(&c, "bkt").await;
    let v1 = put(&c, "bkt", "k", "one").await.unwrap();
    let out = c
        .delete_object()
        .bucket("bkt")
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(out.delete_marker(), Some(true));
    let dm = out.version_id().unwrap().to_owned();
    assert_ne!(dm, v1);

    // The key is hidden: 404 with the marker's headers.
    let e = c
        .get_object()
        .bucket("bkt")
        .key("k")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&e), 404);
    assert_eq!(header(&e, "x-amz-delete-marker").as_deref(), Some("true"));
    assert_eq!(header(&e, "x-amz-version-id"), Some(dm.clone()));
    assert_eq!(code(&e.into_service_error()), "NoSuchKey");
    let e = c
        .head_object()
        .bucket("bkt")
        .key("k")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&e), 404);
    assert_eq!(header(&e, "x-amz-delete-marker").as_deref(), Some("true"));

    // Addressing the marker itself is not allowed.
    let e = c
        .get_object()
        .bucket("bkt")
        .key("k")
        .version_id(&dm)
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&e), 405);
    assert_eq!(header(&e, "x-amz-delete-marker").as_deref(), Some("true"));
    let e = c
        .head_object()
        .bucket("bkt")
        .key("k")
        .version_id(&dm)
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&e), 405);
    let e = c
        .get_object_tagging()
        .bucket("bkt")
        .key("k")
        .version_id(&dm)
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&e), 405);
    assert_eq!(get(&c, "bkt", "k", Some(&v1)).await.0, "one");

    // Listings skip the key; version listings show the marker as latest.
    let out = c.list_objects_v2().bucket("bkt").send().await.unwrap();
    assert_eq!(out.key_count(), Some(0));
    let out = c.list_objects().bucket("bkt").send().await.unwrap();
    assert!(out.contents().is_empty());
    assert_eq!(
        versions(&c, "bkt").await,
        [
            ("k".into(), dm.clone(), true, true),
            ("k".into(), v1.clone(), false, false)
        ]
    );

    // Markers stack; deleting the markers undeletes.
    let dm2 = c
        .delete_object()
        .bucket("bkt")
        .key("k")
        .send()
        .await
        .unwrap()
        .version_id
        .unwrap();
    for id in [&dm2, &dm] {
        let out = c
            .delete_object()
            .bucket("bkt")
            .key("k")
            .version_id(id)
            .send()
            .await
            .unwrap();
        assert_eq!(out.delete_marker(), Some(true));
        assert_eq!(out.version_id(), Some(id.as_str()));
    }
    assert_eq!(get(&c, "bkt", "k", None).await, ("one".into(), Some(v1)));

    // Deleting a key that never existed still adds a marker.
    let out = c
        .delete_object()
        .bucket("bkt")
        .key("ghost")
        .send()
        .await
        .unwrap();
    assert_eq!(out.delete_marker(), Some(true));

    // Unversioned buckets delete outright.
    c.create_bucket().bucket("plain").send().await.unwrap();
    put(&c, "plain", "k", "x").await;
    let out = c
        .delete_object()
        .bucket("plain")
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(out.delete_marker(), None);
    assert_eq!(out.version_id(), None);
    let e = c
        .head_object()
        .bucket("plain")
        .key("k")
        .send()
        .await
        .unwrap_err();
    assert_eq!(header(&e, "x-amz-delete-marker"), None);
    assert!(versions(&c, "plain").await.is_empty());
}

async fn copy_versions(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    versioned(&c, "bkt").await;
    c.create_bucket().bucket("plain").send().await.unwrap();
    let v1 = put(&c, "bkt", "k", "one").await.unwrap();
    let v2 = put(&c, "bkt", "k", "two").await.unwrap();

    let out = c
        .copy_object()
        .bucket("bkt")
        .key("old")
        .copy_source(format!("bkt/k?versionId={v1}"))
        .send()
        .await
        .unwrap();
    assert_eq!(out.copy_source_version_id(), Some(v1.as_str()));
    let new = out.version_id().unwrap().to_owned();
    assert_eq!(get(&c, "bkt", "old", None).await, ("one".into(), Some(new)));

    // Into an unversioned bucket: no version id; the source's current version.
    let out = c
        .copy_object()
        .bucket("plain")
        .key("k")
        .copy_source("bkt/k")
        .send()
        .await
        .unwrap();
    assert_eq!(out.version_id(), None);
    assert_eq!(out.copy_source_version_id(), Some(v2.as_str()));
    assert_eq!(get(&c, "plain", "k", None).await.0, "two");

    // Restoring an old version by copying it onto its own key.
    let restored = c
        .copy_object()
        .bucket("bkt")
        .key("k")
        .copy_source(format!("bkt/k?versionId={v1}"))
        .send()
        .await
        .unwrap()
        .version_id
        .unwrap();
    assert_eq!(
        get(&c, "bkt", "k", None).await,
        ("one".into(), Some(restored))
    );
    assert_eq!(versions(&c, "bkt").await.len(), 4);

    // A delete marker cannot be a copy source.
    let dm = c
        .delete_object()
        .bucket("bkt")
        .key("k")
        .send()
        .await
        .unwrap()
        .version_id
        .unwrap();
    let e = c
        .copy_object()
        .bucket("bkt")
        .key("x")
        .copy_source(format!("bkt/k?versionId={dm}"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&e), 400);
    let e = c
        .copy_object()
        .bucket("bkt")
        .key("x")
        .copy_source("bkt/k")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&e), 404);
    let e = c
        .copy_object()
        .bucket("bkt")
        .key("x")
        .copy_source("bkt/k?versionId=nope")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e.into_service_error()), "NoSuchVersion");
}

async fn multipart_versions(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    versioned(&c, "bkt").await;
    let v1 = put(&c, "bkt", "src", "old source").await.unwrap();
    put(&c, "bkt", "src", "new source").await.unwrap();
    let before = put(&c, "bkt", "k", "before").await.unwrap();

    let id = c
        .create_multipart_upload()
        .bucket("bkt")
        .key("k")
        .send()
        .await
        .unwrap()
        .upload_id
        .unwrap();
    let p1 = c
        .upload_part()
        .bucket("bkt")
        .key("k")
        .upload_id(&id)
        .part_number(1)
        .body(ByteStream::from(vec![b'a'; 5 * MIB]))
        .send()
        .await
        .unwrap();
    let p2 = c
        .upload_part_copy()
        .bucket("bkt")
        .key("k")
        .upload_id(&id)
        .part_number(2)
        .copy_source(format!("bkt/src?versionId={v1}"))
        .send()
        .await
        .unwrap();
    assert_eq!(p2.copy_source_version_id(), Some(v1.as_str()));
    let parts = vec![
        CompletedPart::builder()
            .part_number(1)
            .e_tag(p1.e_tag.unwrap())
            .build(),
        CompletedPart::builder()
            .part_number(2)
            .e_tag(p2.copy_part_result.unwrap().e_tag.unwrap())
            .build(),
    ];
    let out = c
        .complete_multipart_upload()
        .bucket("bkt")
        .key("k")
        .upload_id(&id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(parts))
                .build(),
        )
        .send()
        .await
        .unwrap();
    let v = out.version_id().unwrap().to_owned();
    assert_ne!(v, before);
    let (body, got) = get(&c, "bkt", "k", None).await;
    assert_eq!(got, Some(v));
    assert!(body.ends_with("old source"));
    assert_eq!(body.len(), 5 * MIB + 10);
    assert_eq!(get(&c, "bkt", "k", Some(&before)).await.0, "before");
}

async fn delete_objects_versions(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    versioned(&c, "bkt").await;
    let v1 = put(&c, "bkt", "a", "1").await.unwrap();
    let v2 = put(&c, "bkt", "a", "2").await.unwrap();
    put(&c, "bkt", "b", "b").await.unwrap();
    let ids = |key: &str, v: Option<&str>| {
        ObjectIdentifier::builder()
            .key(key)
            .set_version_id(v.map(str::to_owned))
            .build()
            .unwrap()
    };
    let del = Delete::builder()
        .objects(ids("a", Some(&v1)))
        .objects(ids("b", None))
        .objects(ids("a", Some("missing")))
        .build()
        .unwrap();
    let out = c
        .delete_objects()
        .bucket("bkt")
        .delete(del)
        .send()
        .await
        .unwrap();
    assert!(out.errors().is_empty());
    let d = out.deleted();
    assert_eq!(d.len(), 3);
    assert_eq!(
        (d[0].key(), d[0].version_id()),
        (Some("a"), Some(v1.as_str()))
    );
    assert_eq!(d[0].delete_marker(), None);
    assert_eq!(d[1].key(), Some("b"));
    assert_eq!(d[1].version_id(), None);
    assert_eq!(d[1].delete_marker(), Some(true));
    let dm = d[1].delete_marker_version_id().unwrap().to_owned();
    assert_eq!(d[2].version_id(), Some("missing"));

    // Removing a marker through DeleteObjects reports it.
    let del = Delete::builder()
        .objects(ids("b", Some(&dm)))
        .build()
        .unwrap();
    let out = c
        .delete_objects()
        .bucket("bkt")
        .delete(del)
        .send()
        .await
        .unwrap();
    let d = &out.deleted()[0];
    assert_eq!(d.delete_marker(), Some(true));
    assert_eq!(d.delete_marker_version_id(), Some(dm.as_str()));
    assert_eq!(get(&c, "bkt", "b", None).await.0, "b");
    assert_eq!(get(&c, "bkt", "a", None).await, ("2".into(), Some(v2)));
}

async fn delete_bucket_needs_no_versions(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    versioned(&c, "bkt").await;
    let v = put(&c, "bkt", "k", "x").await.unwrap();
    let dm = c
        .delete_object()
        .bucket("bkt")
        .key("k")
        .send()
        .await
        .unwrap()
        .version_id
        .unwrap();
    // Nothing is listed, but the version and marker remain.
    let e = c.delete_bucket().bucket("bkt").send().await.unwrap_err();
    assert_eq!(code(&e), "BucketNotEmpty");
    for id in [&v, &dm] {
        c.delete_object()
            .bucket("bkt")
            .key("k")
            .version_id(id)
            .send()
            .await
            .unwrap();
        let e = c.delete_bucket().bucket("bkt").send().await;
        assert_eq!(e.is_ok(), id == &dm);
    }
}

async fn list_versions_pagination(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    versioned(&c, "bkt").await;
    for key in ["a", "b/1", "b/2", "c&d", "e"] {
        for n in 0..3 {
            put(&c, "bkt", key, &format!("{key}{n}")).await.unwrap();
        }
    }
    c.delete_object()
        .bucket("bkt")
        .key("e")
        .send()
        .await
        .unwrap();
    let full = versions(&c, "bkt").await;
    assert_eq!(full.len(), 16);

    for max in [1, 2, 3, 5, 16, 17] {
        let mut seen = Vec::new();
        let (mut km, mut vm) = (None, None);
        loop {
            let out = c
                .list_object_versions()
                .bucket("bkt")
                .max_keys(max)
                .set_key_marker(km.clone())
                .set_version_id_marker(vm.clone())
                .send()
                .await
                .unwrap();
            let n = out.versions().len() + out.delete_markers().len();
            assert!(n <= max as usize);
            assert_eq!(out.max_keys(), Some(max));
            for v in out.versions() {
                seen.push((
                    v.key().unwrap().to_owned(),
                    v.version_id().unwrap().to_owned(),
                ));
            }
            for d in out.delete_markers() {
                seen.push((
                    d.key().unwrap().to_owned(),
                    d.version_id().unwrap().to_owned(),
                ));
            }
            if !out.is_truncated().unwrap() {
                assert_eq!(out.next_key_marker(), None);
                break;
            }
            km = out.next_key_marker().map(str::to_owned);
            vm = out.next_version_id_marker().map(str::to_owned);
        }
        seen.sort();
        let mut want: Vec<_> = full
            .iter()
            .map(|(k, v, _, _)| (k.clone(), v.clone()))
            .collect();
        want.sort();
        assert_eq!(seen, want, "max-keys {max}");
    }

    // Delimiter, prefix and URL encoding.
    let out = c
        .list_object_versions()
        .bucket("bkt")
        .delimiter("/")
        .encoding_type(EncodingType::Url)
        .send()
        .await
        .unwrap();
    let prefixes: Vec<_> = out
        .common_prefixes()
        .iter()
        .map(|p| p.prefix().unwrap())
        .collect();
    assert_eq!(prefixes, ["b/"]);
    let keys: Vec<_> = out.versions().iter().map(|v| v.key().unwrap()).collect();
    assert_eq!(
        keys,
        ["a", "a", "a", "c%26d", "c%26d", "c%26d", "e", "e", "e"]
    );
    let out = c
        .list_object_versions()
        .bucket("bkt")
        .prefix("b/")
        .key_marker("b/1")
        .send()
        .await
        .unwrap();
    assert!(out.versions().iter().all(|v| v.key() == Some("b/2")));
    assert_eq!(out.versions().len(), 3);

    // A version marker needs a key marker.
    let e = c
        .list_object_versions()
        .bucket("bkt")
        .version_id_marker("x")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "InvalidArgument");
}

async fn tagging_versions(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    versioned(&c, "bkt").await;
    let v1 = put(&c, "bkt", "k", "one").await.unwrap();
    let v2 = put(&c, "bkt", "k", "two").await.unwrap();
    let tagging = Tagging::builder()
        .tag_set(Tag::builder().key("t").value("old").build().unwrap())
        .build()
        .unwrap();
    let out = c
        .put_object_tagging()
        .bucket("bkt")
        .key("k")
        .version_id(&v1)
        .tagging(tagging)
        .send()
        .await
        .unwrap();
    assert_eq!(out.version_id(), Some(v1.as_str()));
    let out = c
        .get_object_tagging()
        .bucket("bkt")
        .key("k")
        .version_id(&v1)
        .send()
        .await
        .unwrap();
    assert_eq!(out.tag_set().len(), 1);
    assert_eq!(out.version_id(), Some(v1.as_str()));
    let out = c
        .get_object_tagging()
        .bucket("bkt")
        .key("k")
        .send()
        .await
        .unwrap();
    assert!(out.tag_set().is_empty());
    assert_eq!(out.version_id(), Some(v2.as_str()));
    let out = c
        .delete_object_tagging()
        .bucket("bkt")
        .key("k")
        .version_id(&v1)
        .send()
        .await
        .unwrap();
    assert_eq!(out.version_id(), Some(v1.as_str()));
    let out = c
        .get_object_tagging()
        .bucket("bkt")
        .key("k")
        .version_id(&v1)
        .send()
        .await
        .unwrap();
    assert!(out.tag_set().is_empty());
}

backend_tests!(
    bucket_configuration,
    enabled_puts_add_versions,
    null_versions,
    delete_markers,
    copy_versions,
    multipart_versions,
    delete_objects_versions,
    delete_bucket_needs_no_versions,
    list_versions_pagination,
    tagging_versions,
);
