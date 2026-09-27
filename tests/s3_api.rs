//! End-to-end tests through the Rust AWS SDK against a real server.

mod common;

use std::time::{Duration, SystemTime};

use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::{ByteStream, DateTime};
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, CompletedMultipartUpload, CompletedPart, Delete, EncodingType,
    MetadataDirective, ObjectIdentifier, Tag, Tagging, TaggingDirective,
};
use bytes::Bytes;
use common::{Backend, code, md5_hex, start, status};
use http_body_util::Full;
use hyper::Request;

const MIB: usize = 1024 * 1024;

async fn body(out: aws_sdk_s3::operation::get_object::GetObjectOutput) -> Vec<u8> {
    out.body.collect().await.unwrap().into_bytes().to_vec()
}

async fn health_and_standard_headers(b: Backend) {
    let s = start(b).await;
    for path in ["/health", "/ready"] {
        let r = s.raw_get(path).await;
        assert_eq!(r.status, 200, "{path}");
    }
    let r = s.raw_get("/").await;
    assert_eq!(r.status, 200);
    assert!(r.header("date").is_some());
    assert!(r.header("x-amz-request-id").is_some());
    assert!(r.header("x-amz-id-2").is_some());
    assert!(r.text().contains("<ListAllMyBucketsResult"));
}

async fn buckets(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bk1").send().await.unwrap();
    c.create_bucket().bucket("bk2").send().await.unwrap();

    let names: Vec<_> = c
        .list_buckets()
        .send()
        .await
        .unwrap()
        .buckets()
        .iter()
        .map(|b| b.name().unwrap().to_owned())
        .collect();
    assert_eq!(names, ["bk1", "bk2"]);
    let head = c.head_bucket().bucket("bk1").send().await.unwrap();
    assert_eq!(head.bucket_region(), Some("us-east-1"));

    c.put_object()
        .bucket("bk1")
        .key("k")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();
    let e = c.delete_bucket().bucket("bk1").send().await.unwrap_err();
    assert_eq!(code(&e), "BucketNotEmpty");
    assert_eq!(status(&e), 409);
    c.delete_object()
        .bucket("bk1")
        .key("k")
        .send()
        .await
        .unwrap();
    c.delete_bucket().bucket("bk1").send().await.unwrap();

    let e = c.head_bucket().bucket("bk1").send().await.unwrap_err();
    assert_eq!(status(&e), 404);
    let e = c.list_objects_v2().bucket("bk1").send().await.unwrap_err();
    assert_eq!(code(&e), "NoSuchBucket");

    for bad in ["ab", "Upper", "under_score", "192.168.0.1", "-dash"] {
        let r = s
            .raw(
                Request::put(format!("/{bad}"))
                    .body(Full::default())
                    .unwrap(),
            )
            .await;
        assert_eq!(r.status, 400, "{bad}");
        assert!(
            r.text().contains("<Code>InvalidBucketName</Code>"),
            "{bad}: {}",
            r.text()
        );
    }
}

async fn init_buckets(b: Backend) {
    let s = common::start_with(b, |c| c.init_buckets = vec!["one".into(), "two".into()]).await;
    let out = s.client().list_buckets().send().await.unwrap();
    assert_eq!(out.buckets().len(), 2);
}

async fn objects_and_metadata(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();

    let put = c
        .put_object()
        .bucket("bkt")
        .key("dir/file.txt")
        .body(ByteStream::from_static(b"hello world"))
        .content_type("text/plain")
        .content_encoding("identity")
        .cache_control("max-age=60")
        .content_disposition("attachment; filename=\"f.txt\"")
        .content_language("en")
        .metadata("color", "Blue Green")
        .send()
        .await
        .unwrap();
    let etag = format!("\"{}\"", md5_hex(b"hello world"));
    assert_eq!(put.e_tag(), Some(etag.as_str()));

    let got = c
        .get_object()
        .bucket("bkt")
        .key("dir/file.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(got.e_tag(), Some(etag.as_str()));
    assert_eq!(got.content_type(), Some("text/plain"));
    assert_eq!(got.content_encoding(), Some("identity"));
    assert_eq!(got.cache_control(), Some("max-age=60"));
    assert_eq!(
        got.content_disposition(),
        Some("attachment; filename=\"f.txt\"")
    );
    assert_eq!(got.content_language(), Some("en"));
    assert_eq!(
        got.metadata().unwrap().get("color").map(String::as_str),
        Some("Blue Green")
    );
    assert_eq!(got.content_length(), Some(11));
    let lm = got.last_modified().unwrap().secs();
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    assert!((now - lm).abs() < 60);
    assert_eq!(body(got).await, b"hello world");

    let head = c
        .head_object()
        .bucket("bkt")
        .key("dir/file.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(head.e_tag(), Some(etag.as_str()));
    assert_eq!(head.content_length(), Some(11));
    assert_eq!(
        head.metadata().unwrap().get("color").map(String::as_str),
        Some("Blue Green")
    );

    // Raw: exact ETag quoting and RFC 1123 Last-Modified; HEAD has no body.
    let r = s
        .raw(
            Request::head("/bkt/dir/file.txt")
                .body(Full::default())
                .unwrap(),
        )
        .await;
    assert_eq!(r.status, 200);
    assert_eq!(r.header("etag"), Some(etag.as_str()));
    assert!(r.header("last-modified").unwrap().ends_with(" GMT"));
    assert_eq!(r.header("content-length"), Some("11"));
    assert!(r.body.is_empty());

    // Default content type is AWS's, not application/octet-stream. The SDK
    // always sends one, so use a raw request.
    let r = s
        .raw(Request::put("/bkt/plain").body(Full::default()).unwrap())
        .await;
    assert_eq!(r.status, 200);
    let head = c
        .head_object()
        .bucket("bkt")
        .key("plain")
        .send()
        .await
        .unwrap();
    assert_eq!(head.content_type(), Some("binary/octet-stream"));
    assert_eq!(head.e_tag(), Some("\"d41d8cd98f00b204e9800998ecf8427e\""));

    // Missing key: HEAD is 404 (not 403), GET is NoSuchKey with the key in the XML.
    let e = c
        .head_object()
        .bucket("bkt")
        .key("nope")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&e), 404);
    assert!(e.into_service_error().is_not_found());
    let e = c
        .get_object()
        .bucket("bkt")
        .key("nope")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "NoSuchKey");
    let r = s.raw_get("/bkt/no%20pe").await;
    assert_eq!(r.status, 404);
    let xml = r.text();
    assert!(xml.contains("<Code>NoSuchKey</Code>"), "{xml}");
    assert!(xml.contains("<Key>no pe</Key>"), "{xml}");
    assert!(xml.contains(&format!(
        "<RequestId>{}</RequestId>",
        r.header("x-amz-request-id").unwrap()
    )));
    let r = s.raw_get("/nobucket/k").await;
    assert!(
        r.text().contains("<BucketName>nobucket</BucketName>"),
        "{}",
        r.text()
    );

    // Keys: 1024 bytes ok, 1025 rejected; odd characters round-trip.
    let long = "k".repeat(1024);
    c.put_object()
        .bucket("bkt")
        .key(&long)
        .body(ByteStream::from_static(b"1"))
        .send()
        .await
        .unwrap();
    let e = c
        .put_object()
        .bucket("bkt")
        .key(format!("{long}x"))
        .body(ByteStream::from_static(b"1"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "KeyTooLongError");
    for key in [
        "a b+c",
        "ünï/cødé",
        "x&y<z>",
        "../dots",
        "q?x=1#h",
        "trailing/",
    ] {
        c.put_object()
            .bucket("bkt")
            .key(key)
            .body(ByteStream::from(key.as_bytes().to_vec()))
            .send()
            .await
            .unwrap();
        let got = c.get_object().bucket("bkt").key(key).send().await.unwrap();
        assert_eq!(body(got).await, key.as_bytes(), "{key}");
    }

    // Overwrite and delete (idempotent).
    c.put_object()
        .bucket("bkt")
        .key("plain")
        .body(ByteStream::from_static(b"v2"))
        .send()
        .await
        .unwrap();
    let got = c
        .get_object()
        .bucket("bkt")
        .key("plain")
        .send()
        .await
        .unwrap();
    assert_eq!(body(got).await, b"v2");
    c.delete_object()
        .bucket("bkt")
        .key("plain")
        .send()
        .await
        .unwrap();
    c.delete_object()
        .bucket("bkt")
        .key("plain")
        .send()
        .await
        .unwrap();
    let r = s
        .raw(Request::delete("/bkt/plain").body(Full::default()).unwrap())
        .await;
    assert_eq!(r.status, 204);
}

async fn ranges(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    c.put_object()
        .bucket("bkt")
        .key("k")
        .body(ByteStream::from_static(b"0123456789"))
        .send()
        .await
        .unwrap();

    for (range, want, content_range) in [
        ("bytes=0-", &b"0123456789"[..], "bytes 0-9/10"),
        ("bytes=2-4", b"234", "bytes 2-4/10"),
        ("bytes=-3", b"789", "bytes 7-9/10"),
        ("bytes=8-100", b"89", "bytes 8-9/10"),
    ] {
        let got = c
            .get_object()
            .bucket("bkt")
            .key("k")
            .range(range)
            .send()
            .await
            .unwrap();
        assert_eq!(got.content_range(), Some(content_range), "{range}");
        assert_eq!(got.content_length(), Some(want.len() as i64));
        assert_eq!(body(got).await, want, "{range}");
    }
    let r = s
        .raw(
            Request::get("/bkt/k")
                .header("range", "bytes=0-")
                .body(Full::default())
                .unwrap(),
        )
        .await;
    assert_eq!(r.status, 206);
    assert_eq!(r.header("content-range"), Some("bytes 0-9/10"));
    assert_eq!(r.header("accept-ranges"), Some("bytes"));

    let e = c
        .get_object()
        .bucket("bkt")
        .key("k")
        .range("bytes=10-")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&e), 416);
    assert_eq!(code(&e), "InvalidRange");

    let r = s
        .raw(
            Request::head("/bkt/k")
                .header("range", "bytes=1-2")
                .body(Full::default())
                .unwrap(),
        )
        .await;
    assert_eq!(r.status, 206);
    assert_eq!(r.header("content-length"), Some("2"));
}

async fn conditionals(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    let etag = c
        .put_object()
        .bucket("bkt")
        .key("k")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap()
        .e_tag
        .unwrap();
    let past = DateTime::from_secs(1_000_000_000);
    let future = DateTime::from(SystemTime::now() + Duration::from_secs(3600));

    c.get_object()
        .bucket("bkt")
        .key("k")
        .if_match(&etag)
        .send()
        .await
        .unwrap();
    c.get_object()
        .bucket("bkt")
        .key("k")
        .if_match("*")
        .send()
        .await
        .unwrap();
    c.get_object()
        .bucket("bkt")
        .key("k")
        .if_none_match("\"other\"")
        .send()
        .await
        .unwrap();
    c.get_object()
        .bucket("bkt")
        .key("k")
        .if_modified_since(past)
        .send()
        .await
        .unwrap();
    c.get_object()
        .bucket("bkt")
        .key("k")
        .if_unmodified_since(future)
        .send()
        .await
        .unwrap();

    let e = c
        .get_object()
        .bucket("bkt")
        .key("k")
        .if_match("\"other\"")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&e), 412);
    assert_eq!(code(&e), "PreconditionFailed");
    let e = c
        .get_object()
        .bucket("bkt")
        .key("k")
        .if_unmodified_since(past)
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&e), 412);
    let e = c
        .get_object()
        .bucket("bkt")
        .key("k")
        .if_none_match(&etag)
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&e), 304);
    let e = c
        .get_object()
        .bucket("bkt")
        .key("k")
        .if_modified_since(future)
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&e), 304);
    let e = c
        .head_object()
        .bucket("bkt")
        .key("k")
        .if_match("\"other\"")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&e), 412);
    // If-Match wins over a failing If-Unmodified-Since.
    c.get_object()
        .bucket("bkt")
        .key("k")
        .if_match(&etag)
        .if_unmodified_since(past)
        .send()
        .await
        .unwrap();

    let r = s
        .raw(
            Request::get("/bkt/k")
                .header("if-none-match", etag.as_str())
                .body(Full::default())
                .unwrap(),
        )
        .await;
    assert_eq!(r.status, 304);
    assert_eq!(r.header("etag"), Some(etag.as_str()));
    assert!(r.body.is_empty());
}

async fn copy_object(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    c.create_bucket().bucket("cbk").send().await.unwrap();
    let key = "src dir/ünï+x.txt";
    c.put_object()
        .bucket("bkt")
        .key(key)
        .body(ByteStream::from_static(b"payload"))
        .content_type("text/plain")
        .metadata("m", "1")
        .tagging("t=1")
        .send()
        .await
        .unwrap();
    let src = "bkt/src%20dir/%C3%BCn%C3%AF%2Bx.txt";

    let out = c
        .copy_object()
        .bucket("cbk")
        .key("dst")
        .copy_source(src)
        .send()
        .await
        .unwrap();
    let etag = format!("\"{}\"", md5_hex(b"payload"));
    assert_eq!(
        out.copy_object_result().unwrap().e_tag(),
        Some(etag.as_str())
    );
    let got = c
        .get_object()
        .bucket("cbk")
        .key("dst")
        .send()
        .await
        .unwrap();
    assert_eq!(got.content_type(), Some("text/plain"));
    assert_eq!(
        got.metadata().unwrap().get("m").map(String::as_str),
        Some("1")
    );
    assert_eq!(got.tag_count(), Some(1));
    assert_eq!(body(got).await, b"payload");

    // Same-key copy needs REPLACE.
    let e = c
        .copy_object()
        .bucket("bkt")
        .key(key)
        .copy_source(src)
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "InvalidRequest");
    c.copy_object()
        .bucket("bkt")
        .key(key)
        .copy_source(src)
        .metadata_directive(MetadataDirective::Replace)
        .content_type("application/json")
        .tagging_directive(TaggingDirective::Replace)
        .tagging("a=b&c=d")
        .send()
        .await
        .unwrap();
    let head = c.head_object().bucket("bkt").key(key).send().await.unwrap();
    assert_eq!(head.content_type(), Some("application/json"));
    assert!(head.metadata().is_none_or(|m| m.is_empty()));
    let tags = c
        .get_object_tagging()
        .bucket("bkt")
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tag_set().len(), 2);

    // REPLACE without a content type falls back to the default.
    c.copy_object()
        .bucket("cbk")
        .key("dst2")
        .copy_source(src)
        .metadata_directive(MetadataDirective::Replace)
        .send()
        .await
        .unwrap();
    let head = c
        .head_object()
        .bucket("cbk")
        .key("dst2")
        .send()
        .await
        .unwrap();
    assert_eq!(head.content_type(), Some("binary/octet-stream"));

    let e = c
        .copy_object()
        .bucket("cbk")
        .key("x")
        .copy_source("bkt/missing")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "NoSuchKey");
    let e = c
        .copy_object()
        .bucket("cbk")
        .key("x")
        .copy_source(src)
        .copy_source_if_match("\"nope\"")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "PreconditionFailed");
}

async fn delete_objects(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    for k in ["a", "b", "c"] {
        c.put_object()
            .bucket("bkt")
            .key(k)
            .body(ByteStream::from_static(b"x"))
            .send()
            .await
            .unwrap();
    }
    let ids = |keys: &[&str]| {
        let objects = keys
            .iter()
            .map(|k| ObjectIdentifier::builder().key(*k).build().unwrap())
            .collect();
        Delete::builder().set_objects(Some(objects))
    };
    let out = c
        .delete_objects()
        .bucket("bkt")
        .delete(ids(&["a", "missing"]).build().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(out.deleted().len(), 2);
    assert!(out.errors().is_empty());
    let out = c
        .delete_objects()
        .bucket("bkt")
        .delete(ids(&["b", "c"]).quiet(true).build().unwrap())
        .send()
        .await
        .unwrap();
    assert!(out.deleted().is_empty());
    assert_eq!(
        c.list_objects_v2()
            .bucket("bkt")
            .send()
            .await
            .unwrap()
            .key_count(),
        Some(0)
    );
}

async fn listing(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    let keys = ["a/1", "a/2", "a/b/3", "b", "c&d<e>", "z z/1", "ünï"];
    for k in keys {
        c.put_object()
            .bucket("bkt")
            .key(k)
            .body(ByteStream::from_static(b"x"))
            .send()
            .await
            .unwrap();
    }

    let out = c
        .list_objects_v2()
        .bucket("bkt")
        .delimiter("/")
        .send()
        .await
        .unwrap();
    let got: Vec<_> = out.contents().iter().map(|o| o.key().unwrap()).collect();
    assert_eq!(got, ["b", "c&d<e>", "ünï"]);
    let prefixes: Vec<_> = out
        .common_prefixes()
        .iter()
        .map(|p| p.prefix().unwrap())
        .collect();
    assert_eq!(prefixes, ["a/", "z z/"]);
    assert_eq!(out.key_count(), Some(5));
    assert_eq!(out.is_truncated(), Some(false));
    let o = &out.contents()[0];
    assert_eq!(o.e_tag(), Some(format!("\"{}\"", md5_hex(b"x")).as_str()));
    assert_eq!(o.size(), Some(1));

    let out = c
        .list_objects_v2()
        .bucket("bkt")
        .prefix("a/")
        .delimiter("/")
        .send()
        .await
        .unwrap();
    let got: Vec<_> = out.contents().iter().map(|o| o.key().unwrap()).collect();
    assert_eq!(got, ["a/1", "a/2"]);
    assert_eq!(out.common_prefixes()[0].prefix(), Some("a/b/"));

    let out = c
        .list_objects_v2()
        .bucket("bkt")
        .start_after("b")
        .send()
        .await
        .unwrap();
    assert_eq!(out.contents()[0].key(), Some("c&d<e>"));

    // Pagination: every key exactly once, opaque tokens.
    let mut all = Vec::new();
    let mut token = None;
    loop {
        let out = c
            .list_objects_v2()
            .bucket("bkt")
            .max_keys(2)
            .set_continuation_token(token)
            .send()
            .await
            .unwrap();
        assert!(out.key_count().unwrap() <= 2);
        all.extend(out.contents().iter().map(|o| o.key().unwrap().to_owned()));
        if out.is_truncated() != Some(true) {
            break;
        }
        token = out.next_continuation_token().map(str::to_owned);
        assert!(!token.as_deref().unwrap().contains('/'));
    }
    assert_eq!(all, keys);

    // Paginator with delimiter across pages.
    let pages: Vec<_> = c
        .list_objects_v2()
        .bucket("bkt")
        .delimiter("/")
        .max_keys(1)
        .into_paginator()
        .send()
        .collect::<Vec<_>>()
        .await;
    assert_eq!(pages.len(), 5);

    // encoding-type=url: the SDK decodes it back.
    let out = c
        .list_objects_v2()
        .bucket("bkt")
        .encoding_type(EncodingType::Url)
        .send()
        .await
        .unwrap();
    let got: Vec<_> = out.contents().iter().map(|o| o.key().unwrap()).collect();
    assert!(
        got.contains(&"c%26d%3Ce%3E") || got.contains(&"c&d<e>"),
        "{got:?}"
    );
    let r = s
        .raw_get("/bkt?list-type=2&encoding-type=url&prefix=c")
        .await;
    assert!(r.text().contains("<Key>c%26d%3Ce%3E</Key>"), "{}", r.text());
    let r = s.raw_get("/bkt?list-type=2&prefix=c").await;
    assert!(
        r.text().contains("<Key>c&amp;d&lt;e&gt;</Key>"),
        "{}",
        r.text()
    );

    // v1 with marker.
    let out = c
        .list_objects()
        .bucket("bkt")
        .delimiter("/")
        .max_keys(2)
        .send()
        .await
        .unwrap();
    assert_eq!(out.is_truncated(), Some(true));
    let marker = out.next_marker().unwrap().to_owned();
    assert_eq!(marker, "b");
    let out = c
        .list_objects()
        .bucket("bkt")
        .delimiter("/")
        .marker(&marker)
        .send()
        .await
        .unwrap();
    let got: Vec<_> = out.contents().iter().map(|o| o.key().unwrap()).collect();
    assert_eq!(got, ["c&d<e>", "ünï"]);
    assert_eq!(out.common_prefixes()[0].prefix(), Some("z z/"));
}

async fn put_part(
    c: &aws_sdk_s3::Client,
    key: &str,
    id: &str,
    n: i32,
    data: Vec<u8>,
) -> CompletedPart {
    let out = c
        .upload_part()
        .bucket("bkt")
        .key(key)
        .upload_id(id)
        .part_number(n)
        .body(ByteStream::from(data))
        .send()
        .await
        .unwrap();
    CompletedPart::builder()
        .part_number(n)
        .e_tag(out.e_tag().unwrap())
        .build()
}

async fn multipart(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    let create = c
        .create_multipart_upload()
        .bucket("bkt")
        .key("big")
        .content_type("video/mp4")
        .metadata("m", "v")
        .send()
        .await
        .unwrap();
    let id = create.upload_id().unwrap().to_owned();

    let p1 = vec![1u8; 5 * MIB];
    let p2 = vec![2u8; 5 * MIB];
    let p3 = vec![3u8; 1024];
    let parts = vec![
        put_part(&c, "big", &id, 1, p1.clone()).await,
        put_part(&c, "big", &id, 2, p2.clone()).await,
        put_part(&c, "big", &id, 3, p3.clone()).await,
    ];
    assert_eq!(
        parts[0].e_tag(),
        Some(format!("\"{}\"", md5_hex(&p1)).as_str())
    );

    let listed = c
        .list_parts()
        .bucket("bkt")
        .key("big")
        .upload_id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(listed.parts().len(), 3);
    assert_eq!(listed.parts()[2].size(), Some(1024));
    let listed = c
        .list_parts()
        .bucket("bkt")
        .key("big")
        .upload_id(&id)
        .max_parts(2)
        .send()
        .await
        .unwrap();
    assert_eq!(listed.is_truncated(), Some(true));
    assert_eq!(listed.next_part_number_marker(), Some("2"));
    let uploads = c
        .list_multipart_uploads()
        .bucket("bkt")
        .send()
        .await
        .unwrap();
    assert_eq!(uploads.uploads()[0].upload_id(), Some(id.as_str()));

    let done = c
        .complete_multipart_upload()
        .bucket("bkt")
        .key("big")
        .upload_id(&id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(parts))
                .build(),
        )
        .send()
        .await
        .unwrap();
    let mut concat = Vec::new();
    for p in [&p1, &p2, &p3] {
        use md5::{Digest, Md5};
        concat.extend_from_slice(&Md5::digest(p));
    }
    let want = format!("\"{}-3\"", md5_hex(&concat));
    assert_eq!(done.e_tag(), Some(want.as_str()));

    let head = c
        .head_object()
        .bucket("bkt")
        .key("big")
        .send()
        .await
        .unwrap();
    assert_eq!(head.e_tag(), Some(want.as_str()));
    assert_eq!(head.content_length(), Some((10 * MIB + 1024) as i64));
    assert_eq!(head.content_type(), Some("video/mp4"));
    let r = s
        .raw(Request::head("/bkt/big").body(Full::default()).unwrap())
        .await;
    assert_eq!(r.header("x-amz-mp-parts-count"), Some("3"));
    let got = body(
        c.get_object()
            .bucket("bkt")
            .key("big")
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(got.len(), 10 * MIB + 1024);
    assert!(got[..5 * MIB].iter().all(|b| *b == 1) && got[10 * MIB..].iter().all(|b| *b == 3));
    assert!(
        c.list_multipart_uploads()
            .bucket("bkt")
            .send()
            .await
            .unwrap()
            .uploads()
            .is_empty()
    );

    let e = c
        .list_parts()
        .bucket("bkt")
        .key("big")
        .upload_id(&id)
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "NoSuchUpload");
    assert_eq!(status(&e), 404);
}

async fn multipart_errors_and_copy(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    c.put_object()
        .bucket("bkt")
        .key("src")
        .body(ByteStream::from(vec![7u8; 6 * MIB]))
        .send()
        .await
        .unwrap();
    let id = c
        .create_multipart_upload()
        .bucket("bkt")
        .key("k")
        .send()
        .await
        .unwrap()
        .upload_id
        .unwrap();
    let small = put_part(&c, "k", &id, 1, vec![0u8; 100]).await;
    let last = put_part(&c, "k", &id, 2, vec![0u8; 100]).await;
    let complete = |parts: Vec<CompletedPart>| {
        c.complete_multipart_upload()
            .bucket("bkt")
            .key("k")
            .upload_id(&id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
    };
    let e = complete(vec![small.clone(), last.clone()])
        .await
        .unwrap_err();
    assert_eq!(code(&e), "EntityTooSmall");
    let e = complete(vec![last.clone(), small.clone()])
        .await
        .unwrap_err();
    assert_eq!(code(&e), "InvalidPartOrder");
    let bogus = CompletedPart::builder()
        .part_number(1)
        .e_tag("\"0000\"")
        .build();
    let e = complete(vec![bogus]).await.unwrap_err();
    assert_eq!(code(&e), "InvalidPart");
    let missing = CompletedPart::builder()
        .part_number(9)
        .e_tag(small.e_tag().unwrap())
        .build();
    let e = complete(vec![missing]).await.unwrap_err();
    assert_eq!(code(&e), "InvalidPart");

    // UploadPartCopy replaces part 1 with a 5 MiB slice of src.
    let out = c
        .upload_part_copy()
        .bucket("bkt")
        .key("k")
        .upload_id(&id)
        .part_number(1)
        .copy_source("bkt/src")
        .copy_source_range(format!("bytes=0-{}", 5 * MIB - 1))
        .send()
        .await
        .unwrap();
    let copied = CompletedPart::builder()
        .part_number(1)
        .e_tag(out.copy_part_result().unwrap().e_tag().unwrap())
        .build();
    assert_eq!(
        copied.e_tag(),
        Some(format!("\"{}\"", md5_hex(&vec![7u8; 5 * MIB])).as_str())
    );
    complete(vec![copied, last]).await.unwrap();
    let head = c.head_object().bucket("bkt").key("k").send().await.unwrap();
    assert_eq!(head.content_length(), Some((5 * MIB + 100) as i64));

    let id = c
        .create_multipart_upload()
        .bucket("bkt")
        .key("k2")
        .send()
        .await
        .unwrap()
        .upload_id
        .unwrap();
    c.abort_multipart_upload()
        .bucket("bkt")
        .key("k2")
        .upload_id(&id)
        .send()
        .await
        .unwrap();
    let e = c
        .upload_part()
        .bucket("bkt")
        .key("k2")
        .upload_id(&id)
        .part_number(1)
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "NoSuchUpload");
    let e = c
        .abort_multipart_upload()
        .bucket("bkt")
        .key("k2")
        .upload_id("nope")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "NoSuchUpload");
}

async fn tagging(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    c.put_object()
        .bucket("bkt")
        .key("k")
        .body(ByteStream::from_static(b"x"))
        .tagging("a=1&b=two%20words")
        .send()
        .await
        .unwrap();
    let tags = c
        .get_object_tagging()
        .bucket("bkt")
        .key("k")
        .send()
        .await
        .unwrap();
    let got: Vec<_> = tags
        .tag_set()
        .iter()
        .map(|t| (t.key(), t.value()))
        .collect();
    assert_eq!(got, [("a", "1"), ("b", "two words")]);

    let set = Tagging::builder()
        .tag_set(Tag::builder().key("x").value("y").build().unwrap())
        .build()
        .unwrap();
    c.put_object_tagging()
        .bucket("bkt")
        .key("k")
        .tagging(set.clone())
        .send()
        .await
        .unwrap();
    let tags = c
        .get_object_tagging()
        .bucket("bkt")
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tag_set().len(), 1);
    c.delete_object_tagging()
        .bucket("bkt")
        .key("k")
        .send()
        .await
        .unwrap();
    assert!(
        c.get_object_tagging()
            .bucket("bkt")
            .key("k")
            .send()
            .await
            .unwrap()
            .tag_set()
            .is_empty()
    );
    let e = c
        .get_object_tagging()
        .bucket("bkt")
        .key("nope")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "NoSuchKey");

    let e = c
        .get_bucket_tagging()
        .bucket("bkt")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "NoSuchTagSet");
    c.put_bucket_tagging()
        .bucket("bkt")
        .tagging(set)
        .send()
        .await
        .unwrap();
    let tags = c.get_bucket_tagging().bucket("bkt").send().await.unwrap();
    assert_eq!(tags.tag_set()[0].key(), "x");
    c.delete_bucket_tagging()
        .bucket("bkt")
        .send()
        .await
        .unwrap();
    assert!(c.get_bucket_tagging().bucket("bkt").send().await.is_err());
}

async fn checksums(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();

    // SDK default: CRC32 in an aws-chunked trailer, validated again on GET.
    let modes = common::PayloadModes::default();
    let traced = aws_sdk_s3::Client::from_conf(
        s.sdk_config("test", "test")
            .force_path_style(true)
            .interceptor(modes.clone())
            .build(),
    );
    let data = vec![42u8; 100_000];
    let (stream, _file) = common::streaming_body(&data).await;
    let put = traced
        .put_object()
        .bucket("bkt")
        .key("k")
        .body(stream)
        .send()
        .await
        .unwrap();
    assert!(modes.last().starts_with("STREAMING-"), "{}", modes.last());
    assert!(put.checksum_crc32().is_some());
    let got = c
        .get_object()
        .bucket("bkt")
        .key("k")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    assert_eq!(got.checksum_crc32(), put.checksum_crc32());
    assert_eq!(body(got).await, data);

    for alg in [
        ChecksumAlgorithm::Crc32C,
        ChecksumAlgorithm::Sha1,
        ChecksumAlgorithm::Sha256,
    ] {
        let key = alg.as_str();
        c.put_object()
            .bucket("bkt")
            .key(key)
            .body(ByteStream::from_static(b"hello world"))
            .checksum_algorithm(alg.clone())
            .send()
            .await
            .unwrap();
        let head = c
            .head_object()
            .bucket("bkt")
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        let got = head
            .checksum_crc32_c()
            .or(head.checksum_sha1())
            .or(head.checksum_sha256());
        let want = match alg {
            ChecksumAlgorithm::Crc32C => "yZRlqg==",
            ChecksumAlgorithm::Sha1 => "Kq5sNclPz7QV2+lfQIuc6R7oRu0=",
            _ => "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=",
        };
        assert_eq!(got, Some(want), "{key}");
    }

    // Header checksum and Content-MD5 verification.
    let put = |h: &str, v: &str| {
        Request::put("/bkt/raw")
            .header(h, v)
            .header("content-length", "11")
            .body(Full::new(Bytes::from_static(b"hello world")))
            .unwrap()
    };
    let r = s.raw(put("x-amz-checksum-crc32", "DUoRhQ==")).await;
    assert_eq!(r.status, 200);
    assert_eq!(r.header("x-amz-checksum-crc32"), Some("DUoRhQ=="));
    let r = s.raw(put("x-amz-checksum-crc32", "AAAAAA==")).await;
    assert_eq!(r.status, 400);
    assert!(r.text().contains("<Code>BadDigest</Code>"), "{}", r.text());
    let r = s.raw(put("content-md5", "XrY7u+Ae7tCTyyK7j1rNww==")).await;
    assert_eq!(r.status, 200);
    let r = s.raw(put("content-md5", "AAAAAAAAAAAAAAAAAAAAAA==")).await;
    assert!(r.text().contains("<Code>BadDigest</Code>"), "{}", r.text());
    let r = s.raw(put("content-md5", "not-base64")).await;
    assert!(
        r.text().contains("<Code>InvalidDigest</Code>"),
        "{}",
        r.text()
    );
    // A failed upload leaves the previous object in place.
    let got = c
        .get_object()
        .bucket("bkt")
        .key("raw")
        .send()
        .await
        .unwrap();
    assert_eq!(body(got).await, b"hello world");

    // Multipart with CRC32 per part: composite checksum on complete.
    let id = c
        .create_multipart_upload()
        .bucket("bkt")
        .key("mp")
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .send()
        .await
        .unwrap()
        .upload_id
        .unwrap();
    let out = c
        .upload_part()
        .bucket("bkt")
        .key("mp")
        .upload_id(&id)
        .part_number(1)
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .body(ByteStream::from_static(b"hello world"))
        .send()
        .await
        .unwrap();
    assert_eq!(out.checksum_crc32(), Some("DUoRhQ=="));
    let part = CompletedPart::builder()
        .part_number(1)
        .e_tag(out.e_tag().unwrap())
        .checksum_crc32("DUoRhQ==")
        .build();
    let done = c
        .complete_multipart_upload()
        .bucket("bkt")
        .key("mp")
        .upload_id(&id)
        .multipart_upload(CompletedMultipartUpload::builder().parts(part).build())
        .send()
        .await
        .unwrap();
    assert!(done.checksum_crc32().unwrap().ends_with("-1"));
    let got = c
        .get_object()
        .bucket("bkt")
        .key("mp")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    assert_eq!(body(got).await, b"hello world");
}

async fn sse_headers_echoed(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    let put = c
        .put_object()
        .bucket("bkt")
        .key("k")
        .body(ByteStream::from_static(b"x"))
        .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::AwsKms)
        .ssekms_key_id("my-key")
        .send()
        .await
        .unwrap();
    assert_eq!(put.ssekms_key_id(), Some("my-key"));
    let head = c.head_object().bucket("bkt").key("k").send().await.unwrap();
    assert_eq!(
        head.server_side_encryption().map(|s| s.as_str()),
        Some("aws:kms")
    );
    assert_eq!(head.ssekms_key_id(), Some("my-key"));
}

async fn presigned_urls(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    c.put_object()
        .bucket("bkt")
        .key("a b")
        .body(ByteStream::from_static(b"secret"))
        .send()
        .await
        .unwrap();
    let cfg = PresigningConfig::expires_in(Duration::from_secs(300)).unwrap();
    let req = c
        .get_object()
        .bucket("bkt")
        .key("a b")
        .presigned(cfg.clone())
        .await
        .unwrap();
    let r = s
        .raw_get(req.uri().strip_prefix(&s.endpoint).unwrap())
        .await;
    assert_eq!(r.status, 200);
    assert_eq!(&r.body[..], b"secret");

    let req = c
        .put_object()
        .bucket("bkt")
        .key("up")
        .presigned(cfg)
        .await
        .unwrap();
    let r = s
        .raw(
            Request::put(req.uri().strip_prefix(&s.endpoint).unwrap())
                .body(Full::new(Bytes::from_static(b"up")))
                .unwrap(),
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    let got = c.get_object().bucket("bkt").key("up").send().await.unwrap();
    assert_eq!(body(got).await, b"up");
}

async fn lenient_accepts_any_credentials(b: Backend) {
    let s = start(b).await;
    let c = s.client_with("SOMEONE", "else");
    c.create_bucket().bucket("bkt").send().await.unwrap();
    c.put_object()
        .bucket("bkt")
        .key("k")
        .body(ByteStream::from(vec![1u8; 70_000]))
        .send()
        .await
        .unwrap();
    // Anonymous.
    let r = s.raw_get("/bkt/k").await;
    assert_eq!(r.status, 200);
    assert_eq!(r.body.len(), 70_000);
}

/// A signed-chunk (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`) body in lenient mode.
async fn lenient_signed_chunks(b: Backend) {
    let s = start(b).await;
    s.client()
        .create_bucket()
        .bucket("bkt")
        .send()
        .await
        .unwrap();
    let body = b"6;chunk-signature=aa\r\nhello \r\n5;chunk-signature=bb\r\nworld\r\n0;chunk-signature=cc\r\nx-amz-checksum-crc32:DUoRhQ==\r\nx-amz-trailer-signature:dd\r\n\r\n";
    let req = Request::put("/bkt/chunked")
        .header("authorization", "AWS4-HMAC-SHA256 Credential=x/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00")
        .header("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER")
        .header("x-amz-decoded-content-length", "11")
        .header("x-amz-trailer", "x-amz-checksum-crc32")
        .header("content-encoding", "aws-chunked")
        .header("content-length", body.len())
        .body(Full::new(Bytes::from_static(body)))
        .unwrap();
    let r = s.raw(req).await;
    assert_eq!(r.status, 200, "{}", r.text());
    let got = s
        .client()
        .get_object()
        .bucket("bkt")
        .key("chunked")
        .send()
        .await
        .unwrap();
    assert_eq!(got.content_encoding(), None);
    assert_eq!(body_of(got).await, b"hello world");

    // Wrong trailer checksum.
    let bad = b"b\r\nhello world\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n";
    let req = Request::put("/bkt/chunked")
        .header("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
        .header("x-amz-decoded-content-length", "11")
        .header("x-amz-trailer", "x-amz-checksum-crc32")
        .header("content-length", bad.len())
        .body(Full::new(Bytes::from_static(bad)))
        .unwrap();
    let r = s.raw(req).await;
    assert_eq!(r.status, 400);
    assert!(r.text().contains("BadDigest"), "{}", r.text());
}

async fn body_of(out: aws_sdk_s3::operation::get_object::GetObjectOutput) -> Vec<u8> {
    body(out).await
}

async fn virtual_host_addressing(b: Backend) {
    let s = common::start_with(b, |c| c.vhost_domain = Some("localhost".into())).await;
    let c = s.client();
    c.create_bucket().bucket("vhb").send().await.unwrap();
    c.put_object()
        .bucket("vhb")
        .key("dir/k")
        .body(ByteStream::from_static(b"v"))
        .send()
        .await
        .unwrap();
    let host = format!("vhb.localhost:{}", s.addr.port());
    let r = s
        .raw(
            Request::get("/dir/k")
                .header("host", &host)
                .body(Full::default())
                .unwrap(),
        )
        .await;
    assert_eq!(r.status, 200);
    assert_eq!(&r.body[..], b"v");
    let r = s
        .raw(
            Request::get("/?list-type=2")
                .header("host", &host)
                .body(Full::default())
                .unwrap(),
        )
        .await;
    assert!(r.text().contains("<Key>dir/k</Key>"), "{}", r.text());
    // Path style still works on the bare domain and on IPs.
    let r = s
        .raw(
            Request::get("/vhb/dir/k")
                .header("host", format!("localhost:{}", s.addr.port()))
                .body(Full::default())
                .unwrap(),
        )
        .await;
    assert_eq!(r.status, 200);
    // /health on a bucket host is an object key, not the health check.
    let r = s
        .raw(
            Request::get("/health")
                .header("host", &host)
                .body(Full::default())
                .unwrap(),
        )
        .await;
    assert_eq!(r.status, 404);
}

async fn expect_100_continue(b: Backend) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let s = start(b).await;
    s.client()
        .create_bucket()
        .bucket("bkt")
        .send()
        .await
        .unwrap();
    let mut conn = tokio::net::TcpStream::connect(s.addr).await.unwrap();
    let head = format!(
        "PUT /bkt/k HTTP/1.1\r\nHost: {}\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n",
        s.addr
    );
    conn.write_all(head.as_bytes()).await.unwrap();
    let mut buf = [0u8; 256];
    let n = tokio::time::timeout(Duration::from_secs(5), conn.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let interim = String::from_utf8_lossy(&buf[..n]);
    assert!(interim.starts_with("HTTP/1.1 100 Continue"), "{interim}");
    conn.write_all(b"hello").await.unwrap();
    let n = tokio::time::timeout(Duration::from_secs(5), conn.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let resp = String::from_utf8_lossy(&buf[..n]);
    assert!(resp.contains("200 OK"), "{resp}");
}

async fn graceful_shutdown_finishes_in_flight(b: Backend) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let s = start(b).await;
    s.client()
        .create_bucket()
        .bucket("bkt")
        .send()
        .await
        .unwrap();
    let mut conn = tokio::net::TcpStream::connect(s.addr).await.unwrap();
    let head = format!(
        "PUT /bkt/k HTTP/1.1\r\nHost: {}\r\nContent-Length: 10\r\n\r\nhello",
        s.addr
    );
    conn.write_all(head.as_bytes()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let addr = s.addr;
    let shutdown = tokio::spawn(s.handle.shutdown());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!shutdown.is_finished());
    // New connections are refused while the old request completes.
    assert!(tokio::net::TcpStream::connect(addr).await.is_err());
    conn.write_all(b"world").await.unwrap();
    let mut resp = Vec::new();
    conn.read_to_end(&mut resp).await.unwrap();
    assert!(String::from_utf8_lossy(&resp).starts_with("HTTP/1.1 200"));
    tokio::time::timeout(Duration::from_secs(5), shutdown)
        .await
        .unwrap()
        .unwrap();
}

async fn create_owned_bucket(b: Backend) {
    // us-east-1 answers a repeat create by the owner with 200 (legacy behaviour).
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    c.put_object()
        .bucket("bkt")
        .key("k")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    // The bucket is untouched.
    c.head_object().bucket("bkt").key("k").send().await.unwrap();

    // Every other region: 409 BucketAlreadyOwnedByYou.
    let s = common::start_with(b, |c| c.region = "eu-west-1".into()).await;
    let c = s.client();
    let cfg = || {
        aws_sdk_s3::types::CreateBucketConfiguration::builder()
            .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::EuWest1)
            .build()
    };
    c.create_bucket()
        .bucket("bkt")
        .create_bucket_configuration(cfg())
        .send()
        .await
        .unwrap();
    let e = c
        .create_bucket()
        .bucket("bkt")
        .create_bucket_configuration(cfg())
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "BucketAlreadyOwnedByYou");
    assert_eq!(status(&e), 409);
    let r = s
        .raw(Request::put("/bkt").body(Full::default()).unwrap())
        .await;
    assert!(
        r.text().contains("<Code>BucketAlreadyOwnedByYou</Code>"),
        "{}",
        r.text()
    );
}

async fn bucket_location(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    let out = c.get_bucket_location().bucket("bkt").send().await.unwrap();
    // AWS sends an empty element for us-east-1.
    assert!(
        out.location_constraint()
            .is_none_or(|l| l.as_str().is_empty())
    );
    let r = s.raw_get("/bkt?location").await;
    assert_eq!(r.status, 200);
    assert!(r.text().contains("LocationConstraint"), "{}", r.text());
    let e = c
        .get_bucket_location()
        .bucket("nope")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "NoSuchBucket");

    let s = common::start_with(b, |c| {
        c.region = "eu-central-1".into();
        c.init_buckets = vec!["bkt".into()];
    })
    .await;
    let out = s
        .client()
        .get_bucket_location()
        .bucket("bkt")
        .send()
        .await
        .unwrap();
    assert_eq!(
        out.location_constraint().map(|l| l.as_str()),
        Some("eu-central-1")
    );
}

async fn list_buckets_pages(b: Backend) {
    let names = ["bk1", "bk2", "bk3", "bk4", "bk5"]
        .map(String::from)
        .to_vec();
    let s = common::start_with(b, |c| c.init_buckets = names.clone()).await;
    let c = s.client();
    let out = c.list_buckets().max_buckets(2).send().await.unwrap();
    assert_eq!(out.buckets().len(), 2);
    let token = out.continuation_token().unwrap().to_owned();
    assert_ne!(token, "bk2");
    let mut seen = Vec::new();
    let mut pages = c.list_buckets().max_buckets(2).into_paginator().send();
    while let Some(page) = pages.next().await {
        let page = page.unwrap();
        assert!(page.buckets().len() <= 2);
        seen.extend(page.buckets().iter().map(|b| b.name().unwrap().to_owned()));
    }
    assert_eq!(seen, names);
    // Without max-buckets: everything, no token.
    let out = c.list_buckets().send().await.unwrap();
    assert_eq!(out.buckets().len(), 5);
    assert_eq!(out.continuation_token(), None);
    let out = c.list_buckets().prefix("bk4").send().await.unwrap();
    assert_eq!(out.buckets().len(), 1);
    let e = c.list_buckets().max_buckets(0).send().await.unwrap_err();
    assert_eq!(code(&e), "InvalidArgument");
}

async fn part_number_reads(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    let id = c
        .create_multipart_upload()
        .bucket("bkt")
        .key("mp")
        .send()
        .await
        .unwrap()
        .upload_id
        .unwrap();
    let p1 = vec![1u8; 5 * MIB];
    let p2 = vec![2u8; 5 * MIB + 7];
    let p3 = vec![3u8; 100];
    let parts = vec![
        put_part(&c, "mp", &id, 1, p1.clone()).await,
        put_part(&c, "mp", &id, 2, p2.clone()).await,
        put_part(&c, "mp", &id, 3, p3.clone()).await,
    ];
    let done = c
        .complete_multipart_upload()
        .bucket("bkt")
        .key("mp")
        .upload_id(&id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(parts))
                .build(),
        )
        .send()
        .await
        .unwrap();
    let total = p1.len() + p2.len() + p3.len();

    let mut start = 0;
    for (n, want) in [(1, &p1), (2, &p2), (3, &p3)] {
        let got = c
            .get_object()
            .bucket("bkt")
            .key("mp")
            .part_number(n)
            .send()
            .await
            .unwrap();
        assert_eq!(got.parts_count(), Some(3));
        assert_eq!(got.e_tag(), done.e_tag());
        assert_eq!(got.content_length(), Some(want.len() as i64));
        let range = format!("bytes {start}-{}/{total}", start + want.len() - 1);
        assert_eq!(got.content_range(), Some(range.as_str()));
        assert_eq!(&body(got).await, want, "part {n}");
        let head = c
            .head_object()
            .bucket("bkt")
            .key("mp")
            .part_number(n)
            .send()
            .await
            .unwrap();
        assert_eq!(head.content_length(), Some(want.len() as i64));
        assert_eq!(head.parts_count(), Some(3));
        start += want.len();
    }
    let r = s
        .raw(
            Request::get("/bkt/mp?partNumber=3")
                .body(Full::default())
                .unwrap(),
        )
        .await;
    assert_eq!(r.status, 206);
    assert_eq!(r.header("x-amz-mp-parts-count"), Some("3"));
    let r = s
        .raw(
            Request::head("/bkt/mp?partNumber=2")
                .body(Full::default())
                .unwrap(),
        )
        .await;
    assert_eq!(r.status, 206);

    let e = c
        .get_object()
        .bucket("bkt")
        .key("mp")
        .part_number(4)
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&e), 416);
    assert_eq!(code(&e), "InvalidPartNumber");
    let e = c
        .get_object()
        .bucket("bkt")
        .key("mp")
        .part_number(1)
        .range("bytes=0-1")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "InvalidRequest");
    let r = s.raw_get("/bkt/mp?partNumber=0").await;
    assert!(
        r.text().contains("<Code>InvalidArgument</Code>"),
        "{}",
        r.text()
    );

    // A single-part object is part 1 of 1.
    c.put_object()
        .bucket("bkt")
        .key("single")
        .body(ByteStream::from_static(b"hello"))
        .send()
        .await
        .unwrap();
    let got = c
        .get_object()
        .bucket("bkt")
        .key("single")
        .part_number(1)
        .send()
        .await
        .unwrap();
    assert_eq!(got.parts_count(), None);
    assert_eq!(got.content_range(), Some("bytes 0-4/5"));
    assert_eq!(body(got).await, b"hello");
    let e = c
        .head_object()
        .bucket("bkt")
        .key("single")
        .part_number(2)
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&e), 416);

    // Survives a copy.
    c.copy_object()
        .bucket("bkt")
        .key("mp-copy")
        .copy_source("bkt/mp")
        .send()
        .await
        .unwrap();
    let head = c
        .head_object()
        .bucket("bkt")
        .key("mp-copy")
        .part_number(3)
        .send()
        .await
        .unwrap();
    assert_eq!(head.content_length(), Some(100));
}

/// Rewrites a checksum header after signing, as a corrupting proxy would.
#[derive(Debug)]
struct CorruptChecksum;

impl aws_sdk_s3::config::Intercept for CorruptChecksum {
    fn name(&self) -> &'static str {
        "CorruptChecksum"
    }

    fn modify_before_transmit(
        &self,
        ctx: &mut aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextMut<'_>,
        _rc: &aws_sdk_s3::config::RuntimeComponents,
        _cfg: &mut aws_sdk_s3::config::ConfigBag,
    ) -> Result<(), aws_sdk_s3::error::BoxError> {
        let headers = ctx.request_mut().headers_mut();
        let names: Vec<String> = headers
            .iter()
            .map(|(k, _)| k.to_owned())
            .filter(|k| k.starts_with("x-amz-checksum-") || k == "content-md5")
            .collect();
        assert!(!names.is_empty(), "SDK sent no body checksum");
        for n in names {
            headers.insert(n, "AAAAAA==");
        }
        Ok(())
    }
}

async fn xml_body_digests(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    c.put_object()
        .bucket("bkt")
        .key("k")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();
    let corrupt = aws_sdk_s3::Client::from_conf(
        s.sdk_config("test", "test")
            .force_path_style(true)
            .interceptor(CorruptChecksum)
            .build(),
    );
    let ids = Delete::builder()
        .objects(ObjectIdentifier::builder().key("k").build().unwrap())
        .build()
        .unwrap();
    let e = corrupt
        .delete_objects()
        .bucket("bkt")
        .delete(ids)
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "BadDigest");
    let tags = Tagging::builder()
        .tag_set(Tag::builder().key("a").value("b").build().unwrap())
        .build()
        .unwrap();
    let e = corrupt
        .put_object_tagging()
        .bucket("bkt")
        .key("k")
        .tagging(tags.clone())
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "BadDigest");
    let e = corrupt
        .put_bucket_tagging()
        .bucket("bkt")
        .tagging(tags)
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "BadDigest");
    // Nothing was applied.
    c.head_object().bucket("bkt").key("k").send().await.unwrap();
    let t = c
        .get_object_tagging()
        .bucket("bkt")
        .key("k")
        .send()
        .await
        .unwrap();
    assert!(t.tag_set().is_empty());

    // Raw Content-MD5 on PutObjectTagging.
    let xml = "<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet></Tagging>";
    let tagging = |md5: &str| {
        Request::put("/bkt/k?tagging")
            .header("content-md5", md5)
            .body(Full::new(Bytes::from_static(xml.as_bytes())))
            .unwrap()
    };
    let r = s.raw(tagging("AAAAAAAAAAAAAAAAAAAAAA==")).await;
    assert_eq!(r.status, 400);
    assert!(r.text().contains("<Code>BadDigest</Code>"), "{}", r.text());
    use base64::Engine as _;
    use md5::{Digest, Md5};
    let good = base64::engine::general_purpose::STANDARD.encode(Md5::digest(xml));
    let r = s.raw(tagging(&good)).await;
    assert_eq!(r.status, 200, "{}", r.text());
}

async fn complete(
    c: &aws_sdk_s3::Client,
    key: &str,
    id: &str,
    parts: Vec<CompletedPart>,
) -> Result<
    aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput,
    aws_sdk_s3::error::SdkError<
        aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError,
    >,
> {
    c.complete_multipart_upload()
        .bucket("bkt")
        .key(key)
        .upload_id(id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(parts))
                .build(),
        )
        .send()
        .await
}

async fn new_upload(c: &aws_sdk_s3::Client, key: &str) -> String {
    let out = c
        .create_multipart_upload()
        .bucket("bkt")
        .key(key)
        .send()
        .await
        .unwrap();
    out.upload_id().unwrap().to_owned()
}

async fn multipart_complete_retry_and_duplicates(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();

    // A retried Complete answers like the first one.
    let id = new_upload(&c, "k").await;
    let parts = vec![put_part(&c, "k", &id, 1, b"abc".to_vec()).await];
    let first = complete(&c, "k", &id, parts.clone()).await.unwrap();
    let again = complete(&c, "k", &id, parts.clone()).await.unwrap();
    assert_eq!(first.e_tag(), again.e_tag());
    let err = complete(&c, "k", "nope", parts.clone()).await.unwrap_err();
    assert_eq!(code(err.as_service_error().unwrap()), "NoSuchUpload");
    // Not once the key is overwritten by something else.
    c.put_object().bucket("bkt").key("k").send().await.unwrap();
    let err = complete(&c, "k", &id, parts).await.unwrap_err();
    assert_eq!(code(err.as_service_error().unwrap()), "NoSuchUpload");

    // A part number listed twice in a row: the later entry wins.
    let id = new_upload(&c, "dup").await;
    let stale = put_part(&c, "dup", &id, 1, b"old".to_vec()).await;
    let fresh = put_part(&c, "dup", &id, 1, b"new".to_vec()).await;
    let err = complete(&c, "dup", &id, vec![fresh.clone(), stale.clone()])
        .await
        .unwrap_err();
    assert_eq!(code(err.as_service_error().unwrap()), "InvalidPart");
    complete(&c, "dup", &id, vec![stale, fresh]).await.unwrap();
    let got = c
        .get_object()
        .bucket("bkt")
        .key("dup")
        .send()
        .await
        .unwrap();
    assert_eq!(body(got).await, b"new");

    // Still strictly ascending otherwise.
    let id = new_upload(&c, "order").await;
    let p1 = put_part(&c, "order", &id, 1, vec![1; 5 * MIB]).await;
    let p2 = put_part(&c, "order", &id, 2, b"x".to_vec()).await;
    let err = complete(&c, "order", &id, vec![p2, p1]).await.unwrap_err();
    assert_eq!(code(err.as_service_error().unwrap()), "InvalidPartOrder");
}

async fn multipart_conditional_complete(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    let attempt = |if_match: Option<&str>, if_none_match: Option<&str>| {
        let c = c.clone();
        let (m, n) = (
            if_match.map(str::to_owned),
            if_none_match.map(str::to_owned),
        );
        async move {
            let id = new_upload(&c, "k").await;
            let part = put_part(&c, "k", &id, 1, b"abc".to_vec()).await;
            c.complete_multipart_upload()
                .bucket("bkt")
                .key("k")
                .upload_id(id)
                .set_if_match(m)
                .set_if_none_match(n)
                .multipart_upload(CompletedMultipartUpload::builder().parts(part).build())
                .send()
                .await
        }
    };
    let err = attempt(Some("\"nope\""), None).await.unwrap_err();
    assert_eq!(code(err.as_service_error().unwrap()), "NoSuchKey");
    let etag = attempt(None, Some("*"))
        .await
        .unwrap()
        .e_tag()
        .unwrap()
        .to_owned();
    for (m, n) in [
        (None, Some("*")),
        (None, Some(etag.as_str())),
        (Some("\"nope\""), None),
    ] {
        let err = attempt(m, n).await.unwrap_err();
        assert_eq!(status(&err), 412);
        assert_eq!(code(err.as_service_error().unwrap()), "PreconditionFailed");
    }
    attempt(Some(&etag), None).await.unwrap();
    attempt(None, Some("\"other\"")).await.unwrap();
}

async fn multipart_checksum_types(b: Backend) {
    use aws_sdk_s3::types::ChecksumType;
    use garbagebin::logic::checksum::Algorithm;
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    let data: Vec<u8> = (0..5 * MIB + 100).map(|i| (i % 251) as u8).collect();
    let (a, rest) = data.split_at(5 * MIB);

    let create = |alg: ChecksumAlgorithm, t: Option<ChecksumType>| {
        c.create_multipart_upload()
            .bucket("bkt")
            .key("k")
            .checksum_algorithm(alg)
            .set_checksum_type(t)
            .send()
    };
    for (alg, full) in [
        (ChecksumAlgorithm::Crc32, Algorithm::Crc32),
        (ChecksumAlgorithm::Crc32C, Algorithm::Crc32c),
    ] {
        let up = create(alg.clone(), Some(ChecksumType::FullObject))
            .await
            .unwrap();
        assert_eq!(up.checksum_type(), Some(&ChecksumType::FullObject));
        let id = up.upload_id().unwrap();
        let mut parts = Vec::new();
        for (n, chunk) in [(1, a), (2, rest)] {
            let out = c
                .upload_part()
                .bucket("bkt")
                .key("k")
                .upload_id(id)
                .part_number(n)
                .checksum_algorithm(alg.clone())
                .body(ByteStream::from(chunk.to_vec()))
                .send()
                .await
                .unwrap();
            parts.push(
                CompletedPart::builder()
                    .part_number(n)
                    .e_tag(out.e_tag().unwrap())
                    .set_checksum_crc32(out.checksum_crc32().map(str::to_owned))
                    .set_checksum_crc32_c(out.checksum_crc32_c().map(str::to_owned))
                    .build(),
            );
        }
        let whole = full.digest(&data);
        let done = complete(&c, "k", id, parts).await.unwrap();
        assert_eq!(done.checksum_type(), Some(&ChecksumType::FullObject));
        let got = done.checksum_crc32().or(done.checksum_crc32_c());
        assert_eq!(got, Some(whole.as_str()), "{alg:?}");
        let head = c
            .head_object()
            .bucket("bkt")
            .key("k")
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));
    }

    // Default for SHA256 is COMPOSITE; a wrong whole-object checksum on Complete is BadDigest.
    let up = create(ChecksumAlgorithm::Sha256, None).await.unwrap();
    assert_eq!(up.checksum_type(), Some(&ChecksumType::Composite));
    let id = up.upload_id().unwrap();
    let out = c
        .upload_part()
        .bucket("bkt")
        .key("k")
        .upload_id(id)
        .part_number(1)
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .body(ByteStream::from_static(b"abc"))
        .send()
        .await
        .unwrap();
    let part = CompletedPart::builder()
        .part_number(1)
        .e_tag(out.e_tag().unwrap())
        .checksum_sha256(out.checksum_sha256().unwrap())
        .build();
    let with_sum = |sum: &str| {
        c.complete_multipart_upload()
            .bucket("bkt")
            .key("k")
            .upload_id(id)
            .checksum_sha256(sum)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(part.clone())
                    .build(),
            )
            .send()
    };
    let err = with_sum("bad").await.unwrap_err();
    assert_eq!(code(err.as_service_error().unwrap()), "BadDigest");
    let raw = Algorithm::Sha256.digest(b"abc");
    let composite = format!("{}-1", Algorithm::Sha256.digest(&base64_decode(&raw)));
    let done = with_sum(&composite).await.unwrap();
    assert_eq!(done.checksum_sha256(), Some(composite.as_str()));

    let err = create(ChecksumAlgorithm::Sha256, Some(ChecksumType::FullObject))
        .await
        .unwrap_err();
    assert_eq!(code(err.as_service_error().unwrap()), "InvalidRequest");
}

fn base64_decode(s: &str) -> Vec<u8> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.decode(s).unwrap()
}

async fn stored_headers_verbatim(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    let expires = DateTime::from_secs(2_000_000_000);

    c.put_object()
        .bucket("bkt")
        .key("k")
        .expires(expires)
        .content_encoding("deflate, gzip")
        .send()
        .await
        .unwrap();
    let head = c.head_object().bucket("bkt").key("k").send().await.unwrap();
    #[allow(deprecated)]
    let got = head.expires();
    assert_eq!(got, Some(&expires));
    assert_eq!(head.content_encoding(), Some("deflate, gzip"));
    let get = s.raw_get("/bkt/k").await;
    assert_eq!(get.header("expires"), Some("Wed, 18 May 2033 03:33:20 GMT"));

    for (sent, stored) in [
        ("aws-chunked, gzip", Some("gzip")),
        ("gzip,aws-chunked", Some("gzip")),
        ("aws-chunked", None),
    ] {
        c.put_object()
            .bucket("bkt")
            .key("k")
            .content_encoding(sent)
            .send()
            .await
            .unwrap();
        let head = c.head_object().bucket("bkt").key("k").send().await.unwrap();
        assert_eq!(head.content_encoding(), stored, "{sent}");
    }

    // Multipart and copy with REPLACE keep Expires too.
    let id = c
        .create_multipart_upload()
        .bucket("bkt")
        .key("mp")
        .expires(expires)
        .send()
        .await
        .unwrap()
        .upload_id()
        .unwrap()
        .to_owned();
    let part = put_part(&c, "mp", &id, 1, b"x".to_vec()).await;
    complete(&c, "mp", &id, vec![part]).await.unwrap();
    c.copy_object()
        .bucket("bkt")
        .key("copy")
        .copy_source("bkt/k")
        .metadata_directive(MetadataDirective::Replace)
        .expires(expires)
        .send()
        .await
        .unwrap();
    for key in ["mp", "copy"] {
        let raw = s.raw_get(&format!("/bkt/{key}")).await;
        assert_eq!(
            raw.header("expires"),
            Some("Wed, 18 May 2033 03:33:20 GMT"),
            "{key}"
        );
    }
}

async fn empty_content_md5(b: Backend) {
    let s = start(b).await;
    s.client()
        .create_bucket()
        .bucket("bkt")
        .send()
        .await
        .unwrap();
    let put = |headers: &[(&str, &str)]| {
        let mut r = Request::put("/bkt/k");
        for (k, v) in headers {
            r = r.header(*k, *v);
        }
        r.body(Full::new(Bytes::from_static(b"bar"))).unwrap()
    };
    let resp = s
        .raw(put(&[("content-length", "3"), ("content-md5", "")]))
        .await;
    assert_eq!(resp.status, 400);
    assert!(
        resp.text().contains("<Code>InvalidDigest</Code>"),
        "{}",
        resp.text()
    );
}

async fn list_v1_prefix_unencoded(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    c.put_object()
        .bucket("bkt")
        .key("a b/c")
        .send()
        .await
        .unwrap();
    let v1 = s
        .raw_get("/bkt?prefix=a%20b&encoding-type=url")
        .await
        .text();
    assert!(v1.contains("<Prefix>a b</Prefix>"), "{v1}");
    assert!(v1.contains("<Key>a%20b/c</Key>"), "{v1}");
    let v2 = s
        .raw_get("/bkt?list-type=2&prefix=a%20b&encoding-type=url")
        .await
        .text();
    assert!(v2.contains("<Prefix>a%20b</Prefix>"), "{v2}");
}

/// Storage classes are validated, stored and echoed; nothing else changes.
async fn storage_class_echoed(b: Backend) {
    use aws_sdk_s3::types::{ObjectStorageClass, StorageClass};
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    let put = |key: &'static str, class: StorageClass| {
        c.put_object()
            .bucket("bkt")
            .key(key)
            .storage_class(class)
            .body(ByteStream::from_static(b"x"))
            .send()
    };
    put("ia", StorageClass::StandardIa).await.unwrap();
    put("std", StorageClass::Standard).await.unwrap();
    let head = |key: &'static str| c.head_object().bucket("bkt").key(key).send();
    assert_eq!(
        head("ia").await.unwrap().storage_class(),
        Some(&StorageClass::StandardIa)
    );
    // AWS omits the header for STANDARD.
    assert_eq!(head("std").await.unwrap().storage_class(), None);
    let list = c.list_objects_v2().bucket("bkt").send().await.unwrap();
    let classes: Vec<_> = list
        .contents()
        .iter()
        .map(|o| o.storage_class().cloned())
        .collect();
    assert_eq!(
        classes,
        [
            Some(ObjectStorageClass::StandardIa),
            Some(ObjectStorageClass::Standard)
        ]
    );

    // Copy takes the requested class (STANDARD when absent), including onto itself.
    c.copy_object()
        .bucket("bkt")
        .key("ia")
        .copy_source("bkt/ia")
        .storage_class(StorageClass::ReducedRedundancy)
        .send()
        .await
        .unwrap();
    assert_eq!(
        head("ia").await.unwrap().storage_class(),
        Some(&StorageClass::ReducedRedundancy)
    );
    c.copy_object()
        .bucket("bkt")
        .key("copy")
        .copy_source("bkt/ia")
        .send()
        .await
        .unwrap();
    assert_eq!(head("copy").await.unwrap().storage_class(), None);

    let up = c
        .create_multipart_upload()
        .bucket("bkt")
        .key("mp")
        .storage_class(StorageClass::Glacier)
        .send()
        .await
        .unwrap();
    let uploads = c
        .list_multipart_uploads()
        .bucket("bkt")
        .send()
        .await
        .unwrap();
    assert_eq!(
        uploads.uploads()[0].storage_class(),
        Some(&StorageClass::Glacier)
    );
    let id = up.upload_id().unwrap();
    let part = c
        .upload_part()
        .bucket("bkt")
        .key("mp")
        .upload_id(id)
        .part_number(1)
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();
    let parts = vec![
        CompletedPart::builder()
            .part_number(1)
            .e_tag(part.e_tag().unwrap())
            .build(),
    ];
    complete(&c, "mp", id, parts).await.unwrap();
    assert_eq!(
        head("mp").await.unwrap().storage_class(),
        Some(&StorageClass::Glacier)
    );

    let r = s
        .raw(
            Request::put("/bkt/bad")
                .header("x-amz-storage-class", "BOGUS")
                .body(Full::default())
                .unwrap(),
        )
        .await;
    assert_eq!(r.status, 400);
    assert!(
        r.text().contains("<Code>InvalidStorageClass</Code>"),
        "{}",
        r.text()
    );
}

/// A checksum the server cannot compute is refused, not stored unverified.
async fn unsupported_checksum_rejected(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    for (name, value) in [
        ("x-amz-checksum-md5", "1B2M2Y8AsgTpgAmY7PhCfg=="),
        ("x-amz-sdk-checksum-algorithm", "SHA512"),
    ] {
        let r = s
            .raw(
                Request::put("/bkt/k")
                    .header(name, value)
                    .body(Full::new(Bytes::from_static(b"data")))
                    .unwrap(),
            )
            .await;
        assert_eq!(r.status, 400, "{name}");
        assert!(
            r.text().contains("<Code>InvalidArgument</Code>"),
            "{}",
            r.text()
        );
    }
    assert_eq!(
        status(
            &c.head_object()
                .bucket("bkt")
                .key("k")
                .send()
                .await
                .unwrap_err()
        ),
        404
    );
    let r = s
        .raw(
            Request::post("/bkt/k?uploads")
                .header("x-amz-checksum-algorithm", "MD5")
                .body(Full::default())
                .unwrap(),
        )
        .await;
    assert_eq!(r.status, 400);
    assert!(
        r.text().contains("<Code>InvalidArgument</Code>"),
        "{}",
        r.text()
    );
}

async fn conditional_delete(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    let put = c
        .put_object()
        .bucket("bkt")
        .key("k")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();
    let delete = |m: &str| c.delete_object().bucket("bkt").key("k").if_match(m).send();

    let e = delete("\"wrong\"").await.unwrap_err();
    assert_eq!(status(&e), 412);
    assert_eq!(code(e.as_service_error().unwrap()), "PreconditionFailed");
    delete(put.e_tag().unwrap()).await.unwrap();
    assert_eq!(status(&delete("*").await.unwrap_err()), 412);

    c.put_object()
        .bucket("bkt")
        .key("k")
        .body(ByteStream::from_static(b"y"))
        .send()
        .await
        .unwrap();
    delete("*").await.unwrap();
    assert_eq!(
        status(
            &c.head_object()
                .bucket("bkt")
                .key("k")
                .send()
                .await
                .unwrap_err()
        ),
        404
    );
}

/// minio-go sends a COMPOSITE checksum on Complete without the `-N` part count.
async fn composite_checksum_without_part_count(b: Backend) {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use garbagebin::logic::checksum::Algorithm;
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    let data: Vec<u8> = (0..5 * MIB + 100).map(|i| (i % 251) as u8).collect();
    let (a, rest) = data.split_at(5 * MIB);

    for (sent_ok, key) in [(true, "ok"), (false, "bad")] {
        let up = c
            .create_multipart_upload()
            .bucket("bkt")
            .key(key)
            .checksum_algorithm(ChecksumAlgorithm::Crc32C)
            .send()
            .await
            .unwrap();
        let id = up.upload_id().unwrap();
        let mut parts = Vec::new();
        let mut raw = Vec::new();
        for (n, chunk) in [(1, a), (2, rest)] {
            let out = c
                .upload_part()
                .bucket("bkt")
                .key(key)
                .upload_id(id)
                .part_number(n)
                .checksum_algorithm(ChecksumAlgorithm::Crc32C)
                .body(ByteStream::from(chunk.to_vec()))
                .send()
                .await
                .unwrap();
            raw.extend(STANDARD.decode(out.checksum_crc32_c().unwrap()).unwrap());
            parts.push(
                CompletedPart::builder()
                    .part_number(n)
                    .e_tag(out.e_tag().unwrap())
                    .set_checksum_crc32_c(out.checksum_crc32_c().map(str::to_owned))
                    .build(),
            );
        }
        let composite = Algorithm::Crc32c.digest(&raw);
        let sent = if sent_ok {
            composite.clone()
        } else {
            Algorithm::Crc32c.digest(b"x")
        };
        let done = c
            .complete_multipart_upload()
            .bucket("bkt")
            .key(key)
            .upload_id(id)
            .checksum_crc32_c(sent)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
            .await;
        if sent_ok {
            assert_eq!(
                done.unwrap().checksum_crc32_c(),
                Some(format!("{composite}-2").as_str())
            );
        } else {
            assert_eq!(
                code(done.unwrap_err().as_service_error().unwrap()),
                "BadDigest"
            );
        }
    }
}

backend_tests!(
    create_owned_bucket,
    bucket_location,
    list_buckets_pages,
    part_number_reads,
    xml_body_digests,
    health_and_standard_headers,
    buckets,
    init_buckets,
    objects_and_metadata,
    ranges,
    conditionals,
    copy_object,
    delete_objects,
    listing,
    multipart,
    multipart_errors_and_copy,
    tagging,
    checksums,
    sse_headers_echoed,
    presigned_urls,
    lenient_accepts_any_credentials,
    lenient_signed_chunks,
    virtual_host_addressing,
    expect_100_continue,
    graceful_shutdown_finishes_in_flight,
    multipart_complete_retry_and_duplicates,
    multipart_conditional_complete,
    multipart_checksum_types,
    stored_headers_verbatim,
    empty_content_md5,
    list_v1_prefix_unencoded,
    storage_class_echoed,
    unsupported_checksum_rejected,
    conditional_delete,
    composite_checksum_without_part_count,
);
