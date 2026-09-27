//! Object Lock end to end through the Rust AWS SDK, on both backends.
//! Servers run with a fixed clock (`--now`) that tests advance to expire retention.

mod common;

use std::time::{Duration, SystemTime};

use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::{ByteStream, DateTime};
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, DefaultRetention, Delete,
    ObjectIdentifier, ObjectLockConfiguration, ObjectLockEnabled, ObjectLockLegalHold,
    ObjectLockLegalHoldStatus, ObjectLockMode, ObjectLockRetention, ObjectLockRetentionMode,
    ObjectLockRule, VersioningConfiguration,
};
use common::{Backend, TestServer, code, start, start_with, status};

const DAY: u64 = 24 * 60 * 60;
/// The servers' clock at start: 2030-01-01T00:00:00Z.
const T0: u64 = 1_893_456_000;

fn at(secs: u64) -> DateTime {
    DateTime::from_secs(secs as i64)
}

async fn server(b: Backend) -> (TestServer, Client) {
    let s = start_with(b, |c| {
        c.now = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(T0))
    })
    .await;
    let c = s.client();
    (s, c)
}

async fn locked_bucket(c: &Client, bucket: &str) {
    c.create_bucket()
        .bucket(bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();
}

async fn put(c: &Client, bucket: &str, key: &str) -> String {
    c.put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap()
        .version_id
        .unwrap()
}

fn retention(mode: ObjectLockRetentionMode, until: u64) -> ObjectLockRetention {
    ObjectLockRetention::builder()
        .mode(mode)
        .retain_until_date(at(until))
        .build()
}

async fn set_retention(
    c: &Client,
    key: &str,
    version: Option<&str>,
    r: ObjectLockRetention,
    bypass: bool,
) -> Result<(), String> {
    c.put_object_retention()
        .bucket("bkt")
        .key(key)
        .set_version_id(version.map(str::to_owned))
        .retention(r)
        .bypass_governance_retention(bypass)
        .send()
        .await
        .map(drop)
        .map_err(|e| code(&e))
}

async fn get_retention(c: &Client, key: &str) -> (ObjectLockRetentionMode, DateTime) {
    let r = c
        .get_object_retention()
        .bucket("bkt")
        .key(key)
        .send()
        .await
        .unwrap()
        .retention
        .unwrap();
    (r.mode.unwrap(), r.retain_until_date.unwrap())
}

async fn set_hold(c: &Client, key: &str, on: bool) {
    let status = if on {
        ObjectLockLegalHoldStatus::On
    } else {
        ObjectLockLegalHoldStatus::Off
    };
    c.put_object_legal_hold()
        .bucket("bkt")
        .key(key)
        .legal_hold(ObjectLockLegalHold::builder().status(status).build())
        .send()
        .await
        .unwrap();
}

async fn delete_version(c: &Client, key: &str, v: &str, bypass: bool) -> Result<(), String> {
    c.delete_object()
        .bucket("bkt")
        .key(key)
        .version_id(v)
        .bypass_governance_retention(bypass)
        .send()
        .await
        .map(drop)
        .map_err(|e| code(&e))
}

fn lock_config(
    mode: ObjectLockRetentionMode,
    days: Option<i32>,
    years: Option<i32>,
) -> ObjectLockConfiguration {
    let d = DefaultRetention::builder()
        .mode(mode)
        .set_days(days)
        .set_years(years)
        .build();
    ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(ObjectLockRule::builder().default_retention(d).build())
        .build()
}

async fn put_config(c: &Client, bucket: &str, cfg: ObjectLockConfiguration) -> Result<(), String> {
    c.put_object_lock_configuration()
        .bucket(bucket)
        .object_lock_configuration(cfg)
        .send()
        .await
        .map(drop)
        .map_err(|e| code(&e))
}

async fn bucket_configuration(b: Backend) {
    let (_s, c) = server(b).await;
    locked_bucket(&c, "bkt").await;
    let v = c
        .get_bucket_versioning()
        .bucket("bkt")
        .send()
        .await
        .unwrap();
    assert_eq!(v.status(), Some(&BucketVersioningStatus::Enabled));
    let cfg = c
        .get_object_lock_configuration()
        .bucket("bkt")
        .send()
        .await
        .unwrap()
        .object_lock_configuration
        .unwrap();
    assert_eq!(cfg.object_lock_enabled(), Some(&ObjectLockEnabled::Enabled));
    assert!(cfg.rule().is_none());

    for cfg in [
        lock_config(ObjectLockRetentionMode::Governance, Some(1), None),
        lock_config(ObjectLockRetentionMode::Compliance, None, Some(2)),
    ] {
        put_config(&c, "bkt", cfg.clone()).await.unwrap();
        let got = c
            .get_object_lock_configuration()
            .bucket("bkt")
            .send()
            .await
            .unwrap()
            .object_lock_configuration
            .unwrap();
        assert_eq!(got, cfg);
    }
    let bad = [
        (
            lock_config(ObjectLockRetentionMode::Governance, Some(1), Some(1)),
            "MalformedXML",
        ),
        (
            lock_config(ObjectLockRetentionMode::Governance, None, None),
            "MalformedXML",
        ),
        (
            lock_config(ObjectLockRetentionMode::Governance, Some(0), None),
            "InvalidRetentionPeriod",
        ),
        (
            lock_config(ObjectLockRetentionMode::Governance, None, Some(-1)),
            "InvalidRetentionPeriod",
        ),
        (
            lock_config("governance".into(), Some(1), None),
            "MalformedXML",
        ),
        (
            ObjectLockConfiguration::builder()
                .object_lock_enabled("Disabled".into())
                .build(),
            "MalformedXML",
        ),
    ];
    for (cfg, want) in bad {
        assert_eq!(put_config(&c, "bkt", cfg).await.unwrap_err(), want);
    }

    // Versioning cannot be suspended once lock is enabled.
    let e = c
        .put_bucket_versioning()
        .bucket("bkt")
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Suspended)
                .build(),
        )
        .send()
        .await
        .unwrap_err();
    assert_eq!((code(&e), status(&e)), ("InvalidBucketState".into(), 409));

    // A plain bucket has none, and can enable lock only once versioning is Enabled.
    c.create_bucket().bucket("plain").send().await.unwrap();
    let e = c
        .get_object_lock_configuration()
        .bucket("plain")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        (code(&e), status(&e)),
        ("ObjectLockConfigurationNotFoundError".into(), 404)
    );
    let enable = ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .build();
    for st in [None, Some(BucketVersioningStatus::Suspended)] {
        if let Some(st) = st {
            c.put_bucket_versioning()
                .bucket("plain")
                .versioning_configuration(VersioningConfiguration::builder().status(st).build())
                .send()
                .await
                .unwrap();
        }
        assert_eq!(
            put_config(&c, "plain", enable.clone()).await.unwrap_err(),
            "InvalidBucketState"
        );
    }
    c.put_bucket_versioning()
        .bucket("plain")
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    put_config(&c, "plain", enable).await.unwrap();

    // A repeat create of an existing bucket does not enable lock on it.
    c.create_bucket().bucket("other").send().await.unwrap();
    locked_bucket(&c, "other").await;
    let e = c
        .get_object_lock_configuration()
        .bucket("other")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "ObjectLockConfigurationNotFoundError");
}

async fn not_lock_enabled(b: Backend) {
    let (_s, c) = server(b).await;
    c.create_bucket().bucket("bkt").send().await.unwrap();
    c.put_object()
        .bucket("bkt")
        .key("k")
        .body(ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();
    let r = retention(ObjectLockRetentionMode::Governance, T0 + DAY);
    assert_eq!(
        set_retention(&c, "k", None, r, false).await.unwrap_err(),
        "InvalidRequest"
    );
    let e = c
        .get_object_retention()
        .bucket("bkt")
        .key("k")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "InvalidRequest");
    let e = c
        .get_object_legal_hold()
        .bucket("bkt")
        .key("k")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "InvalidRequest");
    let e = c
        .put_object()
        .bucket("bkt")
        .key("k")
        .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "InvalidRequest");
}

async fn default_retention_applies(b: Backend) {
    let (_s, c) = server(b).await;
    locked_bucket(&c, "bkt").await;
    // Without a default rule, new versions have no retention.
    put(&c, "bkt", "none").await;
    let e = c
        .get_object_retention()
        .bucket("bkt")
        .key("none")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        (code(&e), status(&e)),
        ("NoSuchObjectLockConfiguration".into(), 404)
    );
    let e = c
        .get_object_legal_hold()
        .bucket("bkt")
        .key("none")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "NoSuchObjectLockConfiguration");

    put_config(
        &c,
        "bkt",
        lock_config(ObjectLockRetentionMode::Governance, Some(2), None),
    )
    .await
    .unwrap();
    put(&c, "bkt", "k").await;
    assert_eq!(
        get_retention(&c, "k").await,
        (ObjectLockRetentionMode::Governance, at(T0 + 2 * DAY))
    );
    // Explicit headers win over the default.
    c.put_object()
        .bucket("bkt")
        .key("explicit")
        .object_lock_mode(ObjectLockMode::Compliance)
        .object_lock_retain_until_date(at(T0 + 5))
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        get_retention(&c, "explicit").await,
        (ObjectLockRetentionMode::Compliance, at(T0 + 5))
    );

    // Multipart: the default applies on Complete.
    let up = c
        .create_multipart_upload()
        .bucket("bkt")
        .key("mp")
        .send()
        .await
        .unwrap();
    let id = up.upload_id().unwrap();
    let part = c
        .upload_part()
        .bucket("bkt")
        .key("mp")
        .upload_id(id)
        .part_number(1)
        .body(ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();
    let parts = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(part.e_tag().unwrap())
                .build(),
        )
        .build();
    c.complete_multipart_upload()
        .bucket("bkt")
        .key("mp")
        .upload_id(id)
        .multipart_upload(parts)
        .send()
        .await
        .unwrap();
    assert_eq!(
        get_retention(&c, "mp").await,
        (ObjectLockRetentionMode::Governance, at(T0 + 2 * DAY))
    );
}

async fn header_validation(b: Backend) {
    let (_s, c) = server(b).await;
    locked_bucket(&c, "bkt").await;
    let put = |mode: Option<ObjectLockMode>, until: Option<u64>| {
        c.put_object()
            .bucket("bkt")
            .key("k")
            .set_object_lock_mode(mode)
            .set_object_lock_retain_until_date(until.map(at))
            .body(ByteStream::from_static(b"x"))
            .send()
    };
    for (mode, until) in [
        (Some(ObjectLockMode::Governance), None),
        (None, Some(T0 + DAY)),
        (Some(ObjectLockMode::Governance), Some(T0)),
        (Some("governance".into()), Some(T0 + DAY)),
    ] {
        let e = put(mode, until).await.unwrap_err();
        assert_eq!(code(&e), "InvalidArgument");
    }
    let e = c
        .put_object()
        .bucket("bkt")
        .key("k")
        .object_lock_legal_hold_status("maybe".into())
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "InvalidArgument");

    // Retention bodies: bad mode, or a date not in the future.
    put(None, None).await.unwrap();
    for r in [
        retention("governance".into(), T0 + DAY),
        retention(ObjectLockRetentionMode::Governance, T0 - 1),
    ] {
        let want = if r.mode() == Some(&ObjectLockRetentionMode::Governance) {
            "InvalidArgument"
        } else {
            "MalformedXML"
        };
        assert_eq!(
            set_retention(&c, "k", None, r, false).await.unwrap_err(),
            want
        );
    }
    let e = c
        .put_object_legal_hold()
        .bucket("bkt")
        .key("k")
        .legal_hold(ObjectLockLegalHold::builder().status("abc".into()).build())
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "MalformedXML");
}

async fn get_and_head_headers(b: Backend) {
    let (_s, c) = server(b).await;
    locked_bucket(&c, "bkt").await;
    let v = c
        .put_object()
        .bucket("bkt")
        .key("k")
        .object_lock_mode(ObjectLockMode::Governance)
        .object_lock_retain_until_date(at(T0 + DAY))
        .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap()
        .version_id
        .unwrap();
    let head = c.head_object().bucket("bkt").key("k").send().await.unwrap();
    assert_eq!(head.object_lock_mode(), Some(&ObjectLockMode::Governance));
    assert_eq!(head.object_lock_retain_until_date(), Some(&at(T0 + DAY)));
    assert_eq!(
        head.object_lock_legal_hold_status(),
        Some(&ObjectLockLegalHoldStatus::On)
    );
    let get = c
        .get_object()
        .bucket("bkt")
        .key("k")
        .version_id(&v)
        .send()
        .await
        .unwrap();
    assert_eq!(get.object_lock_mode(), Some(&ObjectLockMode::Governance));
    assert_eq!(get.object_lock_retain_until_date(), Some(&at(T0 + DAY)));
    assert_eq!(
        get.object_lock_legal_hold_status(),
        Some(&ObjectLockLegalHoldStatus::On)
    );
    set_hold(&c, "k", false).await;
    let head = c.head_object().bucket("bkt").key("k").send().await.unwrap();
    assert_eq!(
        head.object_lock_legal_hold_status(),
        Some(&ObjectLockLegalHoldStatus::Off)
    );

    // An unlocked object carries none of the headers.
    put(&c, "bkt", "plain").await;
    let head = c
        .head_object()
        .bucket("bkt")
        .key("plain")
        .send()
        .await
        .unwrap();
    assert!(head.object_lock_mode().is_none());
    assert!(head.object_lock_retain_until_date().is_none());
    assert!(head.object_lock_legal_hold_status().is_none());
}

async fn governance_rules(b: Backend) {
    let (_s, c) = server(b).await;
    locked_bucket(&c, "bkt").await;
    let v = put(&c, "bkt", "k").await;
    let gov = |t| retention(ObjectLockRetentionMode::Governance, t);
    set_retention(&c, "k", None, gov(T0 + 2 * DAY), false)
        .await
        .unwrap();
    // Extending is always allowed; shortening, removal or a mode change need the bypass.
    set_retention(&c, "k", Some(&v), gov(T0 + 3 * DAY), false)
        .await
        .unwrap();
    assert_eq!(
        set_retention(&c, "k", None, gov(T0 + DAY), false)
            .await
            .unwrap_err(),
        "AccessDenied"
    );
    let comp = retention(ObjectLockRetentionMode::Compliance, T0 + 3 * DAY);
    assert_eq!(
        set_retention(&c, "k", None, comp, false).await.unwrap_err(),
        "AccessDenied"
    );
    set_retention(&c, "k", None, gov(T0 + DAY), true)
        .await
        .unwrap();
    assert_eq!(
        get_retention(&c, "k").await,
        (ObjectLockRetentionMode::Governance, at(T0 + DAY))
    );
    let empty = ObjectLockRetention::builder().build();
    assert_eq!(
        set_retention(&c, "k", None, empty.clone(), false)
            .await
            .unwrap_err(),
        "AccessDenied"
    );

    // Deleting the version needs the bypass; a delete marker never does.
    assert_eq!(
        delete_version(&c, "k", &v, false).await.unwrap_err(),
        "AccessDenied"
    );
    let dm = c
        .delete_object()
        .bucket("bkt")
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(dm.delete_marker(), Some(true));
    delete_version(&c, "k", dm.version_id().unwrap(), false)
        .await
        .unwrap();
    delete_version(&c, "k", &v, true).await.unwrap();

    // Removal with the bypass leaves the version unprotected.
    let v = put(&c, "bkt", "k2").await;
    set_retention(&c, "k2", None, gov(T0 + DAY), false)
        .await
        .unwrap();
    set_retention(&c, "k2", None, empty, true).await.unwrap();
    let e = c
        .get_object_retention()
        .bucket("bkt")
        .key("k2")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "NoSuchObjectLockConfiguration");
    delete_version(&c, "k2", &v, false).await.unwrap();
}

async fn compliance_rules(b: Backend) {
    let (s, c) = server(b).await;
    locked_bucket(&c, "bkt").await;
    let v = put(&c, "bkt", "k").await;
    let comp = |t| retention(ObjectLockRetentionMode::Compliance, t);
    set_retention(&c, "k", None, comp(T0 + DAY), false)
        .await
        .unwrap();
    set_retention(&c, "k", None, comp(T0 + 2 * DAY), false)
        .await
        .unwrap();
    for (r, bypass) in [
        (comp(T0 + DAY), false),
        (comp(T0 + DAY), true),
        (
            retention(ObjectLockRetentionMode::Governance, T0 + 3 * DAY),
            true,
        ),
        (ObjectLockRetention::builder().build(), true),
    ] {
        assert_eq!(
            set_retention(&c, "k", None, r, bypass).await.unwrap_err(),
            "AccessDenied"
        );
    }
    assert_eq!(
        delete_version(&c, "k", &v, true).await.unwrap_err(),
        "AccessDenied"
    );

    // Once the retention date passes, the version can be deleted and its retention changed.
    s.handle.clock.advance(Duration::from_secs(2 * DAY - 1));
    assert_eq!(
        delete_version(&c, "k", &v, false).await.unwrap_err(),
        "AccessDenied"
    );
    s.handle.clock.advance(Duration::from_secs(1));
    set_retention(
        &c,
        "k",
        None,
        retention(ObjectLockRetentionMode::Governance, T0 + 3 * DAY),
        false,
    )
    .await
    .unwrap();
    s.handle.clock.advance(Duration::from_secs(DAY));
    delete_version(&c, "k", &v, false).await.unwrap();
}

async fn legal_hold(b: Backend) {
    let (_s, c) = server(b).await;
    locked_bucket(&c, "bkt").await;
    let v = put(&c, "bkt", "k").await;
    set_hold(&c, "k", true).await;
    let got = c
        .get_object_legal_hold()
        .bucket("bkt")
        .key("k")
        .version_id(&v)
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.legal_hold().and_then(|h| h.status()),
        Some(&ObjectLockLegalHoldStatus::On)
    );
    // A hold blocks deletion even with the bypass and no retention.
    assert_eq!(
        delete_version(&c, "k", &v, true).await.unwrap_err(),
        "AccessDenied"
    );
    set_hold(&c, "k", false).await;
    let got = c
        .get_object_legal_hold()
        .bucket("bkt")
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.legal_hold().and_then(|h| h.status()),
        Some(&ObjectLockLegalHoldStatus::Off)
    );
    delete_version(&c, "k", &v, false).await.unwrap();
    // Deleting the object left the bucket empty.
    c.delete_bucket().bucket("bkt").send().await.unwrap();
}

async fn delete_objects_per_entry(b: Backend) {
    let (_s, c) = server(b).await;
    locked_bucket(&c, "bkt").await;
    let v1 = put(&c, "bkt", "k1").await;
    let v2 = put(&c, "bkt", "k2").await;
    let v3 = put(&c, "bkt", "k3").await;
    let gov = retention(ObjectLockRetentionMode::Governance, T0 + DAY);
    set_retention(&c, "k1", None, gov, false).await.unwrap();
    set_hold(&c, "k3", true).await;
    let ids = |pairs: &[(&str, &str)]| {
        let objects = pairs
            .iter()
            .map(|(k, v)| {
                ObjectIdentifier::builder()
                    .key(*k)
                    .version_id(*v)
                    .build()
                    .unwrap()
            })
            .collect();
        Delete::builder()
            .set_objects(Some(objects))
            .build()
            .unwrap()
    };
    let out = c
        .delete_objects()
        .bucket("bkt")
        .delete(ids(&[("k1", &v1), ("k2", &v2), ("k3", &v3)]))
        .send()
        .await
        .unwrap();
    let deleted: Vec<_> = out.deleted().iter().map(|d| d.key().unwrap()).collect();
    assert_eq!(deleted, ["k2"]);
    let mut errors: Vec<_> = out
        .errors()
        .iter()
        .map(|e| (e.key().unwrap(), e.code().unwrap(), e.version_id().unwrap()))
        .collect();
    errors.sort();
    assert_eq!(
        errors,
        [
            ("k1", "AccessDenied", v1.as_str()),
            ("k3", "AccessDenied", v3.as_str())
        ]
    );
    // The bypass lifts governance retention but not a legal hold.
    let out = c
        .delete_objects()
        .bucket("bkt")
        .delete(ids(&[("k1", &v1), ("k3", &v3)]))
        .bypass_governance_retention(true)
        .send()
        .await
        .unwrap();
    let deleted: Vec<_> = out.deleted().iter().map(|d| d.key().unwrap()).collect();
    assert_eq!(deleted, ["k1"]);
    assert_eq!(out.errors().len(), 1);

    // Keys without a version id get delete markers regardless of locks.
    let out = c
        .delete_objects()
        .bucket("bkt")
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key("k3").build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(out.deleted()[0].delete_marker(), Some(true));
}

async fn copy_and_multipart_headers(b: Backend) {
    let (_s, c) = server(b).await;
    locked_bucket(&c, "bkt").await;
    c.put_object()
        .bucket("bkt")
        .key("src")
        .object_lock_mode(ObjectLockMode::Compliance)
        .object_lock_retain_until_date(at(T0 + DAY))
        .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
        .body(ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();
    // A copy does not inherit the source's lock.
    c.copy_object()
        .bucket("bkt")
        .key("plain")
        .copy_source("bkt/src")
        .send()
        .await
        .unwrap();
    let head = c
        .head_object()
        .bucket("bkt")
        .key("plain")
        .send()
        .await
        .unwrap();
    assert!(head.object_lock_mode().is_none());
    assert!(head.object_lock_legal_hold_status().is_none());
    c.copy_object()
        .bucket("bkt")
        .key("copy")
        .copy_source("bkt/src")
        .object_lock_mode(ObjectLockMode::Governance)
        .object_lock_retain_until_date(at(T0 + 2 * DAY))
        .send()
        .await
        .unwrap();
    assert_eq!(
        get_retention(&c, "copy").await,
        (ObjectLockRetentionMode::Governance, at(T0 + 2 * DAY))
    );

    let up = c
        .create_multipart_upload()
        .bucket("bkt")
        .key("mp")
        .object_lock_mode(ObjectLockMode::Governance)
        .object_lock_retain_until_date(at(T0 + DAY))
        .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
        .send()
        .await
        .unwrap();
    let id = up.upload_id().unwrap();
    let part = c
        .upload_part()
        .bucket("bkt")
        .key("mp")
        .upload_id(id)
        .part_number(1)
        .body(ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();
    let done = c
        .complete_multipart_upload()
        .bucket("bkt")
        .key("mp")
        .upload_id(id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part.e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    let head = c
        .head_object()
        .bucket("bkt")
        .key("mp")
        .send()
        .await
        .unwrap();
    assert_eq!(head.object_lock_mode(), Some(&ObjectLockMode::Governance));
    assert_eq!(
        head.object_lock_legal_hold_status(),
        Some(&ObjectLockLegalHoldStatus::On)
    );
    let v = done.version_id().unwrap();
    assert_eq!(
        delete_version(&c, "mp", v, true).await.unwrap_err(),
        "AccessDenied"
    );

    // Lock headers on create need a lock-enabled bucket.
    c.create_bucket().bucket("plain").send().await.unwrap();
    let e = c
        .create_multipart_upload()
        .bucket("plain")
        .key("mp")
        .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "InvalidRequest");
}

/// Without `--now` the clock follows system time until a test moves it.
async fn system_clock(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    locked_bucket(&c, "bkt").await;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let v = put(&c, "bkt", "k").await;
    let r = retention(ObjectLockRetentionMode::Compliance, now + 60);
    set_retention(&c, "k", None, r, false).await.unwrap();
    assert_eq!(
        delete_version(&c, "k", &v, false).await.unwrap_err(),
        "AccessDenied"
    );
    s.handle.clock.advance(Duration::from_secs(61));
    delete_version(&c, "k", &v, false).await.unwrap();
}

backend_tests!(
    bucket_configuration,
    not_lock_enabled,
    default_retention_applies,
    header_validation,
    get_and_head_headers,
    governance_rules,
    compliance_rules,
    legal_hold,
    delete_objects_per_entry,
    copy_and_multipart_headers,
    system_clock,
);
