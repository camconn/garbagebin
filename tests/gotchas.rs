//! One test per row of the "Compatibility gotchas" table in docs/design.md.
//! Strict-auth rows (SigV4 canonicalisation, virtual-host signing, presigned
//! expiry) live in `tests/auth.rs`; `Expect: 100-continue` in `tests/s3_api.rs`.

mod common;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, CompletedMultipartUpload, CompletedPart, EncodingType,
    MetadataDirective,
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use common::{Backend, TestServer, code, md5_hex, start, status};
use garbagebin::logic::checksum::Algorithm;
use http_body_util::Full;
use hyper::Request;

const MIB: usize = 1024 * 1024;

async fn with_bucket(b: Backend) -> (TestServer, aws_sdk_s3::Client) {
    let s = start(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    (s, c)
}

async fn put(c: &aws_sdk_s3::Client, key: &str, data: &'static [u8]) {
    c.put_object()
        .bucket("bkt")
        .key(key)
        .body(ByteStream::from_static(data))
        .send()
        .await
        .unwrap();
}

fn get(path: &str) -> Request<Full<Bytes>> {
    Request::get(path).body(Full::default()).unwrap()
}

fn md5_b64(data: &[u8]) -> String {
    use md5::{Digest, Md5};
    STANDARD.encode(Md5::digest(data))
}

async fn aws_chunked_trailing_checksum(b: Backend) {
    let (s, c) = with_bucket(b).await;
    // What current SDKs send for a streamed body.
    let modes = common::PayloadModes::default();
    let traced = aws_sdk_s3::Client::from_conf(
        s.sdk_config("test", "test")
            .force_path_style(true)
            .interceptor(modes.clone())
            .build(),
    );
    let data = vec![5u8; 300_000];
    let (stream, _file) = common::streaming_body(&data).await;
    traced
        .put_object()
        .bucket("bkt")
        .key("sdk")
        .body(stream)
        .send()
        .await
        .unwrap();
    assert!(modes.last().ends_with("-TRAILER"), "{}", modes.last());
    let got = c
        .get_object()
        .bucket("bkt")
        .key("sdk")
        .send()
        .await
        .unwrap();
    assert_eq!(got.content_length(), Some(data.len() as i64));
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), data);

    // The framing is stripped: the stored body is the decoded payload.
    let framed = b"5\r\nhello\r\n6\r\n world\r\n0\r\nx-amz-checksum-crc32:DUoRhQ==\r\n\r\n";
    let req = |body: &'static [u8]| {
        Request::put("/bkt/raw")
            .header("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
            .header("x-amz-decoded-content-length", "11")
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .header("content-encoding", "aws-chunked")
            .header("content-length", body.len())
            .body(Full::new(Bytes::from_static(body)))
            .unwrap()
    };
    let r = s.raw(req(framed)).await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.header("x-amz-checksum-crc32"), Some("DUoRhQ=="));
    let r = s.raw_get("/bkt/raw").await;
    assert_eq!(&r.body[..], b"hello world");
    assert_eq!(r.header("content-encoding"), None);

    let bad = b"b\r\nhello world\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n";
    let r = s.raw(req(bad)).await;
    assert_eq!(r.status, 400);
    assert!(r.text().contains("<Code>BadDigest</Code>"), "{}", r.text());
}

async fn checksum_headers(b: Backend) {
    let (s, c) = with_bucket(b).await;
    let data = b"hello world";
    for alg in [
        ChecksumAlgorithm::Crc32,
        ChecksumAlgorithm::Crc32C,
        ChecksumAlgorithm::Sha1,
        ChecksumAlgorithm::Sha256,
    ] {
        let key = alg.as_str();
        let a = Algorithm::parse(key).unwrap();
        let want = a.digest(data);
        let put = c
            .put_object()
            .bucket("bkt")
            .key(key)
            .body(ByteStream::from_static(data))
            .checksum_algorithm(alg.clone())
            .send()
            .await
            .unwrap();
        let echoed = put
            .checksum_crc32()
            .or(put.checksum_crc32_c())
            .or(put.checksum_sha1())
            .or(put.checksum_sha256());
        assert_eq!(echoed, Some(want.as_str()), "{key}");
        // Echoed on GET when asked; the SDK validates it.
        let got = c
            .get_object()
            .bucket("bkt")
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert_eq!(got.body.collect().await.unwrap().into_bytes(), &data[..]);
        let r = s
            .raw(
                Request::head(format!("/bkt/{key}"))
                    .header("x-amz-checksum-mode", "ENABLED")
                    .body(Full::default())
                    .unwrap(),
            )
            .await;
        assert_eq!(r.header(&a.header()), Some(want.as_str()), "{key}");

        // A wrong header value is refused and nothing is stored.
        let r = s
            .raw(
                Request::put(format!("/bkt/bad-{key}"))
                    .header(a.header(), a.digest(b"other"))
                    .body(Full::new(Bytes::from_static(data)))
                    .unwrap(),
            )
            .await;
        assert_eq!(r.status, 400, "{key}");
        assert!(r.text().contains("<Code>BadDigest</Code>"), "{}", r.text());
        let e = c
            .head_object()
            .bucket("bkt")
            .key(format!("bad-{key}"))
            .send()
            .await
            .unwrap_err();
        assert_eq!(status(&e), 404);
    }
}

async fn etag_quoting(b: Backend) {
    let (s, c) = with_bucket(b).await;
    let want = format!("\"{}\"", md5_hex(b"hello"));
    let r = s
        .raw(
            Request::put("/bkt/k")
                .body(Full::new(Bytes::from_static(b"hello")))
                .unwrap(),
        )
        .await;
    assert_eq!(r.header("etag"), Some(want.as_str()));
    assert_eq!(
        s.raw_get("/bkt/k").await.header("etag"),
        Some(want.as_str())
    );
    let head = s
        .raw(Request::head("/bkt/k").body(Full::default()).unwrap())
        .await;
    assert_eq!(head.header("etag"), Some(want.as_str()));
    // Empty body: the well-known MD5.
    let r = s
        .raw(
            Request::put("/bkt/empty")
                .header("content-length", "0")
                .body(Full::default())
                .unwrap(),
        )
        .await;
    assert_eq!(
        r.header("etag"),
        Some("\"d41d8cd98f00b204e9800998ecf8427e\"")
    );
    // Listing and copy carry the same quoted string.
    let out = c.list_objects_v2().bucket("bkt").send().await.unwrap();
    let listed = out
        .contents()
        .iter()
        .find(|o| o.key() == Some("k"))
        .unwrap();
    assert_eq!(listed.e_tag(), Some(want.as_str()));
    let copy = c
        .copy_object()
        .bucket("bkt")
        .key("k2")
        .copy_source("bkt/k")
        .send()
        .await
        .unwrap();
    assert_eq!(
        copy.copy_object_result().unwrap().e_tag(),
        Some(want.as_str())
    );
}

async fn multipart_etag_and_parts_count(b: Backend) {
    let (s, c) = with_bucket(b).await;
    let id = c
        .create_multipart_upload()
        .bucket("bkt")
        .key("mp")
        .send()
        .await
        .unwrap()
        .upload_id
        .unwrap();
    let parts = [vec![1u8; 5 * MIB], vec![2u8; 10]];
    let mut done = Vec::new();
    let mut md5s = Vec::new();
    for (i, p) in parts.iter().enumerate() {
        use md5::{Digest, Md5};
        md5s.extend_from_slice(&Md5::digest(p));
        let out = c
            .upload_part()
            .bucket("bkt")
            .key("mp")
            .upload_id(&id)
            .part_number(i as i32 + 1)
            .body(ByteStream::from(p.clone()))
            .send()
            .await
            .unwrap();
        done.push(
            CompletedPart::builder()
                .part_number(i as i32 + 1)
                .e_tag(out.e_tag().unwrap())
                .build(),
        );
    }
    let out = c
        .complete_multipart_upload()
        .bucket("bkt")
        .key("mp")
        .upload_id(&id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(done))
                .build(),
        )
        .send()
        .await
        .unwrap();
    let want = format!("\"{}-2\"", md5_hex(&md5s));
    assert_eq!(out.e_tag(), Some(want.as_str()));
    let r = s
        .raw(Request::head("/bkt/mp").body(Full::default()).unwrap())
        .await;
    assert_eq!(r.header("etag"), Some(want.as_str()));
    assert_eq!(r.header("x-amz-mp-parts-count"), Some("2"));
    // A single PUT has neither.
    put(&c, "single", b"x").await;
    let r = s
        .raw(Request::head("/bkt/single").body(Full::default()).unwrap())
        .await;
    assert!(!r.header("etag").unwrap().contains('-'));
    assert_eq!(r.header("x-amz-mp-parts-count"), None);
}

async fn list_encoding_and_escaping(b: Backend) {
    let (s, c) = with_bucket(b).await;
    let keys = ["a&b", "c<d>", "ünï cødé", "sp ace+plus"];
    for k in keys {
        c.put_object()
            .bucket("bkt")
            .key(k)
            .body(ByteStream::from_static(b"x"))
            .send()
            .await
            .unwrap();
    }
    // Without encoding: XML-escaped, raw UTF-8.
    let r = s.raw_get("/bkt?list-type=2").await;
    let xml = r.text();
    for want in [
        "<Key>a&amp;b</Key>",
        "<Key>c&lt;d&gt;</Key>",
        "<Key>ünï cødé</Key>",
    ] {
        assert!(xml.contains(want), "{want}: {xml}");
    }
    // encoding-type=url: percent-encoded keys, prefixes and delimiter.
    let r = s
        .raw_get("/bkt?list-type=2&encoding-type=url&delimiter=%20")
        .await;
    let xml = r.text();
    assert!(xml.contains("<EncodingType>url</EncodingType>"), "{xml}");
    for want in [
        "<Key>a%26b</Key>",
        "<Key>c%3Cd%3E</Key>",
        "<Prefix>%C3%BCn%C3%AF%20</Prefix>",
        "<Prefix>sp%20</Prefix>",
    ] {
        assert!(xml.contains(want), "{want}: {xml}");
    }
    // The SDK round-trips every key through either mode.
    for enc in [None, Some(EncodingType::Url)] {
        let out = c
            .list_objects_v2()
            .bucket("bkt")
            .set_encoding_type(enc.clone())
            .send()
            .await
            .unwrap();
        let mut got: Vec<String> = out
            .contents()
            .iter()
            .map(|o| o.key().unwrap().to_owned())
            .collect();
        if enc.is_some() {
            got = got
                .iter()
                .map(|k| {
                    let k = k.replace('+', " ");
                    form_urlencoded::parse(format!("k={k}").as_bytes())
                        .next()
                        .unwrap()
                        .1
                        .into_owned()
                })
                .collect();
        }
        got.sort();
        let mut want: Vec<_> = keys.iter().map(|k| k.to_string()).collect();
        want.sort();
        assert_eq!(got, want, "{enc:?}");
    }
}

async fn pagination_fields(b: Backend) {
    let (s, c) = with_bucket(b).await;
    for k in ["a", "b", "c", "d/1", "d/2"] {
        put(&c, k, b"x").await;
    }
    let r = s.raw_get("/bkt?list-type=2&max-keys=2").await;
    let xml = r.text();
    assert!(xml.contains("<KeyCount>2</KeyCount>"), "{xml}");
    assert!(xml.contains("<IsTruncated>true</IsTruncated>"), "{xml}");
    assert!(xml.contains("<MaxKeys>2</MaxKeys>"), "{xml}");
    let token = xml
        .split("<NextContinuationToken>")
        .nth(1)
        .and_then(|t| t.split("</NextContinuationToken>").next())
        .unwrap()
        .to_owned();
    // Opaque: not the last key in the clear.
    assert_ne!(token, "b");

    // Common prefixes count toward KeyCount and truncation.
    let r = s.raw_get("/bkt?list-type=2&delimiter=/&max-keys=4").await;
    let xml = r.text();
    assert!(xml.contains("<KeyCount>4</KeyCount>"), "{xml}");
    assert!(xml.contains("<IsTruncated>false</IsTruncated>"), "{xml}");
    assert!(!xml.contains("<NextContinuationToken>"), "{xml}");

    // The SDK paginator visits each key once.
    let mut seen = Vec::new();
    let mut pages = c
        .list_objects_v2()
        .bucket("bkt")
        .max_keys(1)
        .into_paginator()
        .send();
    while let Some(page) = pages.next().await {
        let page = page.unwrap();
        assert_eq!(page.key_count(), Some(1));
        seen.extend(page.contents().iter().map(|o| o.key().unwrap().to_owned()));
    }
    assert_eq!(seen, ["a", "b", "c", "d/1", "d/2"]);

    // A token that is not ours is rejected.
    let e = c
        .list_objects_v2()
        .bucket("bkt")
        .continuation_token("not a token")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "InvalidArgument");
}

async fn exact_error_xml(b: Backend) {
    let (s, _c) = with_bucket(b).await;
    let r = s.raw_get("/bkt/missing%20key").await;
    assert_eq!(r.status, 404);
    assert_eq!(r.header("content-type"), Some("application/xml"));
    let xml = r.text();
    let id = r.header("x-amz-request-id").unwrap();
    let start = xml.find("<Error>").unwrap();
    let body = &xml[start..];
    assert!(
        body.starts_with("<Error><Code>NoSuchKey</Code><Message>"),
        "{xml}"
    );
    let key = body.find("<Key>missing key</Key>").unwrap();
    let rid = body.find(&format!("<RequestId>{id}</RequestId>")).unwrap();
    assert!(body.find("</Message>").unwrap() < key && key < rid, "{xml}");
    assert!(body.ends_with("</Error>"), "{xml}");

    for (req, status, code) in [
        (get("/nobucket?list-type=2"), 404, "NoSuchBucket"),
        (
            Request::put("/UPPER").body(Full::default()).unwrap(),
            400,
            "InvalidBucketName",
        ),
        (
            Request::get("/bkt/k?uploadId=nope")
                .body(Full::default())
                .unwrap(),
            404,
            "NoSuchUpload",
        ),
    ] {
        let r = s.raw(req).await;
        assert_eq!(r.status, status, "{code}: {}", r.text());
        assert!(
            r.text().contains(&format!("<Code>{code}</Code>")),
            "{}",
            r.text()
        );
        assert!(r.text().contains("<RequestId>"), "{}", r.text());
    }
}

async fn head_matches_get(b: Backend) {
    let (s, c) = with_bucket(b).await;
    c.put_object()
        .bucket("bkt")
        .key("k")
        .body(ByteStream::from_static(b"body"))
        .content_type("text/plain")
        .cache_control("no-cache")
        .metadata("a", "1")
        .tagging("t=v")
        .send()
        .await
        .unwrap();
    let g = s.raw_get("/bkt/k").await;
    let h = s
        .raw(Request::head("/bkt/k").body(Full::default()).unwrap())
        .await;
    assert_eq!(g.status, 200);
    assert_eq!(h.status, 200);
    assert!(h.body.is_empty());
    for name in [
        "etag",
        "last-modified",
        "content-type",
        "content-length",
        "cache-control",
        "accept-ranges",
        "x-amz-meta-a",
        "x-amz-tagging-count",
    ] {
        assert!(g.header(name).is_some(), "GET {name}");
        assert_eq!(g.header(name), h.header(name), "{name}");
    }
    assert_eq!(h.header("content-length"), Some("4"));

    let h = s
        .raw(Request::head("/bkt/missing").body(Full::default()).unwrap())
        .await;
    assert_eq!(h.status, 404);
    assert!(h.body.is_empty());
    let h = s
        .raw(
            Request::head("/nobucket/missing")
                .body(Full::default())
                .unwrap(),
        )
        .await;
    assert_eq!(h.status, 404);
    let e = c
        .head_object()
        .bucket("bkt")
        .key("missing")
        .send()
        .await
        .unwrap_err();
    assert!(e.into_service_error().is_not_found());
}

async fn default_content_type(b: Backend) {
    let (s, c) = with_bucket(b).await;
    // The SDK always sends a Content-Type, so use raw requests.
    let r = s
        .raw(
            Request::put("/bkt/k")
                .body(Full::new(Bytes::from_static(b"x")))
                .unwrap(),
        )
        .await;
    assert_eq!(r.status, 200);
    assert_eq!(
        s.raw_get("/bkt/k").await.header("content-type"),
        Some("binary/octet-stream")
    );
    let r = s
        .raw(
            Request::post("/bkt/mp?uploads")
                .body(Full::default())
                .unwrap(),
        )
        .await;
    assert_eq!(r.status, 200);
    let id = r
        .text()
        .split("<UploadId>")
        .nth(1)
        .and_then(|t| t.split("</UploadId>").next())
        .unwrap()
        .to_owned();
    let out = c
        .upload_part()
        .bucket("bkt")
        .key("mp")
        .upload_id(&id)
        .part_number(1)
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();
    c.complete_multipart_upload()
        .bucket("bkt")
        .key("mp")
        .upload_id(&id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(out.e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        s.raw_get("/bkt/mp").await.header("content-type"),
        Some("binary/octet-stream")
    );
}

async fn dates_and_request_ids(b: Backend) {
    let (s, c) = with_bucket(b).await;
    put(&c, "k", b"x").await;
    let rfc1123 = |v: &str| {
        let parsed = httpdate::parse_http_date(v).unwrap();
        assert_eq!(httpdate::fmt_http_date(parsed), v, "not RFC 1123: {v}");
    };
    let r = s.raw_get("/bkt/k").await;
    rfc1123(r.header("last-modified").unwrap());
    let reqs = [
        get("/bkt/k"),
        Request::head("/bkt/k").body(Full::default()).unwrap(),
        get("/bkt/missing"),
        get("/bkt?list-type=2"),
        Request::put("/bkt/k2").body(Full::default()).unwrap(),
        Request::delete("/bkt/k2").body(Full::default()).unwrap(),
        get("/"),
    ];
    let mut ids = Vec::new();
    for req in reqs {
        let path = req.uri().to_string();
        let r = s.raw(req).await;
        rfc1123(r.header("date").unwrap_or_else(|| panic!("date: {path}")));
        let id = r.header("x-amz-request-id").expect("x-amz-request-id");
        assert!(r.header("x-amz-id-2").is_some(), "{path}");
        ids.push(id.to_owned());
    }
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 7, "request ids are unique");
}

async fn ranges_206_and_416(b: Backend) {
    let (s, c) = with_bucket(b).await;
    put(&c, "k", b"0123456789").await;
    let ranged = |r: &str| {
        Request::get("/bkt/k")
            .header("range", r)
            .body(Full::default())
            .unwrap()
    };
    let r = s.raw(ranged("bytes=0-")).await;
    assert_eq!(r.status, 206);
    assert_eq!(r.header("content-range"), Some("bytes 0-9/10"));
    assert_eq!(r.header("content-length"), Some("10"));
    assert_eq!(&r.body[..], b"0123456789");
    let r = s.raw(ranged("bytes=3-5")).await;
    assert_eq!(r.status, 206);
    assert_eq!(&r.body[..], b"345");

    for bad in ["bytes=10-", "bytes=20-30"] {
        let r = s.raw(ranged(bad)).await;
        assert_eq!(r.status, 416, "{bad}");
        assert!(
            r.text().contains("<Code>InvalidRange</Code>"),
            "{}",
            r.text()
        );
        assert_eq!(r.header("content-range"), Some("bytes */10"), "{bad}");
    }
    let e = c
        .get_object()
        .bucket("bkt")
        .key("k")
        .range("bytes=10-")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "InvalidRange");
}

async fn delete_objects_quiet_and_md5(b: Backend) {
    let (s, c) = with_bucket(b).await;
    for k in ["a", "b", "c", "d"] {
        put(&c, k, b"x").await;
    }
    let body = |quiet: bool, keys: &[&str]| {
        let objects: String = keys
            .iter()
            .map(|k| format!("<Object><Key>{k}</Key></Object>"))
            .collect();
        format!("<Delete><Quiet>{quiet}</Quiet>{objects}</Delete>")
    };
    let delete = |xml: String, headers: &[(&str, String)]| {
        let mut req = Request::post("/bkt?delete");
        for (k, v) in headers {
            req = req.header(*k, v);
        }
        req.body(Full::new(Bytes::from(xml))).unwrap()
    };

    // Wrong or malformed digests are refused before anything is deleted.
    let xml = body(false, &["a"]);
    let r = s
        .raw(delete(xml.clone(), &[("content-md5", md5_b64(b"other"))]))
        .await;
    assert_eq!(r.status, 400);
    assert!(r.text().contains("<Code>BadDigest</Code>"), "{}", r.text());
    assert!(r.text().contains("<RequestId>"), "{}", r.text());
    let r = s
        .raw(delete(xml.clone(), &[("content-md5", "nope".into())]))
        .await;
    assert!(
        r.text().contains("<Code>InvalidDigest</Code>"),
        "{}",
        r.text()
    );
    let r = s
        .raw(delete(
            xml.clone(),
            &[("x-amz-checksum-crc32", Algorithm::Crc32.digest(b"other"))],
        ))
        .await;
    assert_eq!(r.status, 400);
    assert!(r.text().contains("<Code>BadDigest</Code>"), "{}", r.text());
    c.head_object().bucket("bkt").key("a").send().await.unwrap();

    // Correct Content-MD5, verbose: each key reported.
    let r = s
        .raw(delete(
            xml.clone(),
            &[("content-md5", md5_b64(xml.as_bytes()))],
        ))
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert!(r.text().contains("<Deleted><Key>a</Key>"), "{}", r.text());

    // Quiet with a correct CRC32: an empty result.
    let xml = body(true, &["b", "missing"]);
    let crc = Algorithm::Crc32.digest(xml.as_bytes());
    let r = s.raw(delete(xml, &[("x-amz-checksum-crc32", crc)])).await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert!(!r.text().contains("<Deleted>"), "{}", r.text());
    assert!(!r.text().contains("<Error>"), "{}", r.text());

    // Through the SDK, which sends its own checksum header.
    let ids = ["c", "d"]
        .map(|k| {
            aws_sdk_s3::types::ObjectIdentifier::builder()
                .key(k)
                .build()
                .unwrap()
        })
        .to_vec();
    let out = c
        .delete_objects()
        .bucket("bkt")
        .delete(
            aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(ids))
                .quiet(true)
                .build()
                .unwrap(),
        )
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

async fn bucket_and_key_names(b: Backend) {
    let s = start(b).await;
    let c = s.client();
    let ok = [
        "abc",
        "a-b.c",
        "1bucket",
        "my.bucket.name",
        &"a".repeat(63),
        "192.168.5",
    ];
    for name in ok {
        c.create_bucket().bucket(name).send().await.unwrap();
    }
    let bad = [
        "ab",
        &"a".repeat(64),
        "Upper",
        "under_score",
        "-lead",
        "trail-",
        ".dot",
        "a..b",
        "192.168.5.4",
        "10.0.0.1",
        "xn--abc",
        "sthree-abc",
        "abc-s3alias",
        "abc--ol-s3",
    ];
    for name in bad {
        let r = s
            .raw(
                Request::put(format!("/{name}"))
                    .body(Full::default())
                    .unwrap(),
            )
            .await;
        assert_eq!(r.status, 400, "{name}");
        assert!(
            r.text().contains("<Code>InvalidBucketName</Code>"),
            "{name}: {}",
            r.text()
        );
    }

    // Keys: 1024 bytes of UTF-8 (not characters).
    let k1024 = "é".repeat(512);
    let k1025 = format!("{k1024}x");
    c.put_object()
        .bucket("abc")
        .key(&k1024)
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();
    let e = c
        .put_object()
        .bucket("abc")
        .key(&k1025)
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "KeyTooLongError");
    assert_eq!(status(&e), 400);
    let e = c
        .copy_object()
        .bucket("abc")
        .key(&k1025)
        .copy_source(format!("abc/{}", urlencode(&k1024)))
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "KeyTooLongError");
    let e = c
        .create_multipart_upload()
        .bucket("abc")
        .key(&k1025)
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "KeyTooLongError");
    let out = c.list_objects_v2().bucket("abc").send().await.unwrap();
    assert_eq!(out.contents()[0].key(), Some(k1024.as_str()));
}

fn urlencode(s: &str) -> String {
    form_urlencoded::byte_serialize(s.as_bytes())
        .collect::<String>()
        .replace('+', "%20")
}

async fn copy_source_encoding_and_self_copy(b: Backend) {
    let (s, c) = with_bucket(b).await;
    let key = "dir/a b+c&d=ü?.txt";
    c.put_object()
        .bucket("bkt")
        .key(key)
        .body(ByteStream::from_static(b"payload"))
        .content_type("text/plain")
        .send()
        .await
        .unwrap();
    let src = format!("bkt/{}", urlencode(key));
    // URL-encoded source, with and without a leading slash.
    for (dst, source) in [("c1", src.clone()), ("c2", format!("/{src}"))] {
        c.copy_object()
            .bucket("bkt")
            .key(dst)
            .copy_source(&source)
            .send()
            .await
            .unwrap();
        let got = c.get_object().bucket("bkt").key(dst).send().await.unwrap();
        assert_eq!(got.content_type(), Some("text/plain"));
        assert_eq!(
            got.body.collect().await.unwrap().into_bytes(),
            &b"payload"[..]
        );
    }
    let r = s
        .raw(
            Request::put("/bkt/c3")
                .header("x-amz-copy-source", format!("/{src}"))
                .body(Full::default())
                .unwrap(),
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert!(r.text().contains("<CopyObjectResult"), "{}", r.text());

    // Self-copy: refused without REPLACE, the Content-Type idiom with it.
    let e = c
        .copy_object()
        .bucket("bkt")
        .key(key)
        .copy_source(&src)
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "InvalidRequest");
    assert_eq!(status(&e), 400);
    let e = c
        .copy_object()
        .bucket("bkt")
        .key(key)
        .copy_source(&src)
        .metadata_directive(MetadataDirective::Copy)
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "InvalidRequest");
    c.copy_object()
        .bucket("bkt")
        .key(key)
        .copy_source(&src)
        .metadata_directive(MetadataDirective::Replace)
        .content_type("application/json")
        .metadata("new", "yes")
        .send()
        .await
        .unwrap();
    let head = c.head_object().bucket("bkt").key(key).send().await.unwrap();
    assert_eq!(head.content_type(), Some("application/json"));
    assert_eq!(
        head.metadata().unwrap().get("new").map(String::as_str),
        Some("yes")
    );
    assert_eq!(
        head.e_tag(),
        Some(format!("\"{}\"", md5_hex(b"payload")).as_str())
    );
}

backend_tests!(
    aws_chunked_trailing_checksum,
    checksum_headers,
    etag_quoting,
    multipart_etag_and_parts_count,
    list_encoding_and_escaping,
    pagination_fields,
    exact_error_xml,
    head_matches_get,
    default_content_type,
    dates_and_request_ids,
    ranges_206_and_416,
    delete_objects_quiet_and_md5,
    bucket_and_key_names,
    copy_source_encoding_and_self_copy,
);
