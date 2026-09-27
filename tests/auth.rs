//! Strict SigV4 mode. s3s owns canonicalisation; these check it is wired up
//! with the configured key pair and region.

mod common;

use std::time::Duration;

use aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextMut;
use aws_sdk_s3::config::{ConfigBag, Intercept, RuntimeComponents};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use bytes::Bytes;
use common::{Backend, code, start_with, status};
use http_body_util::Full;
use hyper::Request;

async fn strict(b: Backend) -> common::TestServer {
    start_with(b, |c| {
        c.strict_auth = true;
        c.access_key = "AKIDSTRICT".into();
        c.secret_key = "s3cr3t/with+chars".into();
    })
    .await
}

async fn accepts_good_signatures(b: Backend) {
    let s = strict(b).await;
    let modes = common::PayloadModes::default();
    let conf = s
        .sdk_config("AKIDSTRICT", "s3cr3t/with+chars")
        .force_path_style(true)
        .interceptor(modes.clone());
    let c = aws_sdk_s3::Client::from_conf(conf.build());
    c.create_bucket().bucket("bkt").send().await.unwrap();
    // Chunked upload with the SDK's default trailing checksum.
    let (stream, _file) = common::streaming_body(&[9u8; 200_000]).await;
    c.put_object()
        .bucket("bkt")
        .key("dir/a b+ü")
        .body(stream)
        .send()
        .await
        .unwrap();
    assert!(modes.last().starts_with("STREAMING-"), "{}", modes.last());
    let out = c
        .list_objects_v2()
        .bucket("bkt")
        .prefix("dir/")
        .send()
        .await
        .unwrap();
    assert_eq!(out.key_count(), Some(1));
    let got = c
        .get_object()
        .bucket("bkt")
        .key("dir/a b+ü")
        .send()
        .await
        .unwrap();
    assert_eq!(got.content_length(), Some(200_000));
    let id = c
        .create_multipart_upload()
        .bucket("bkt")
        .key("m")
        .send()
        .await
        .unwrap()
        .upload_id
        .unwrap();
    c.upload_part()
        .bucket("bkt")
        .key("m")
        .upload_id(&id)
        .part_number(1)
        .body(ByteStream::from_static(b"part"))
        .send()
        .await
        .unwrap();
}

async fn rejects_bad_credentials(b: Backend) {
    let s = strict(b).await;
    s.client()
        .create_bucket()
        .bucket("bkt")
        .send()
        .await
        .unwrap();

    let e = s
        .client_with("AKIDSTRICT", "wrong")
        .list_buckets()
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "SignatureDoesNotMatch");
    assert_eq!(status(&e), 403);

    let e = s
        .client_with("UNKNOWN", "x")
        .list_buckets()
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&e), 403);

    let e = s
        .client_with("AKIDSTRICT", "wrong")
        .put_object()
        .bucket("bkt")
        .key("k")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&e), "SignatureDoesNotMatch");

    // Anonymous.
    let r = s.raw_get("/bkt").await;
    assert_eq!(r.status, 403);
    assert!(
        r.text().contains("<Code>AccessDenied</Code>"),
        "{}",
        r.text()
    );
    // Health needs no auth.
    assert_eq!(s.raw_get("/health").await.status, 200);
}

async fn wrong_region_rejected(b: Backend) {
    let s = strict(b).await;
    let mut conf = s
        .sdk_config("AKIDSTRICT", "s3cr3t/with+chars")
        .force_path_style(true);
    conf.set_region(Some(aws_sdk_s3::config::Region::new("eu-west-1")));
    let e = aws_sdk_s3::Client::from_conf(conf.build())
        .list_buckets()
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&e), 400);
    assert_eq!(code(&e), "AuthorizationHeaderMalformed");
}

async fn presigned(b: Backend) {
    let s = strict(b).await;
    let c = s.client();
    c.create_bucket().bucket("bkt").send().await.unwrap();
    c.put_object()
        .bucket("bkt")
        .key("k k")
        .body(ByteStream::from_static(b"data"))
        .send()
        .await
        .unwrap();

    let cfg = PresigningConfig::expires_in(Duration::from_secs(60)).unwrap();
    let req = c
        .get_object()
        .bucket("bkt")
        .key("k k")
        .presigned(cfg.clone())
        .await
        .unwrap();
    let path = req.uri().strip_prefix(&s.endpoint).unwrap().to_owned();
    let r = s.raw_get(&path).await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(&r.body[..], b"data");

    // Tampered signature.
    let i = path.find("X-Amz-Signature=").unwrap() + "X-Amz-Signature=".len();
    let flipped = if &path[i..=i] == "0" { "1" } else { "0" };
    let bad = format!("{}{flipped}{}", &path[..i], &path[i + 1..]);
    assert_eq!(s.raw_get(&bad).await.status, 403);

    // Signed with the wrong secret.
    let other = s.client_with("AKIDSTRICT", "nope");
    let req = other
        .get_object()
        .bucket("bkt")
        .key("k k")
        .presigned(cfg)
        .await
        .unwrap();
    let r = s
        .raw_get(req.uri().strip_prefix(&s.endpoint).unwrap())
        .await;
    assert_eq!(r.status, 403);

    // Expired.
    let cfg = PresigningConfig::builder()
        .start_time(std::time::SystemTime::now() - Duration::from_secs(3600))
        .expires_in(Duration::from_secs(60))
        .build()
        .unwrap();
    let req = c
        .get_object()
        .bucket("bkt")
        .key("k k")
        .presigned(cfg)
        .await
        .unwrap();
    let r = s
        .raw_get(req.uri().strip_prefix(&s.endpoint).unwrap())
        .await;
    assert_eq!(r.status, 403);
    assert!(r.text().contains("expired"), "{}", r.text());

    // Presigned PUT.
    let cfg = PresigningConfig::expires_in(Duration::from_secs(60)).unwrap();
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
}

/// Canonicalisation cases s3s must agree with the SDK on: keys needing
/// double URL-encoding, many sorted signed headers and query parameters,
/// `UNSIGNED-PAYLOAD`, and a checksummed XML body.
async fn sigv4_canonicalisation(b: Backend) {
    let s = strict(b).await;
    let modes = common::PayloadModes::default();
    let c = aws_sdk_s3::Client::from_conf(
        s.sdk_config("AKIDSTRICT", "s3cr3t/with+chars")
            .force_path_style(true)
            .interceptor(modes.clone())
            .build(),
    );
    c.create_bucket().bucket("bkt").send().await.unwrap();
    let keys = [
        "a b/c+d=e&f",
        "pct%2Fliteral",
        "ünï/cødé~*'()!$,;:@",
        "dots/../x",
        "tilde~and_star*",
    ];
    for key in keys {
        c.put_object()
            .bucket("bkt")
            .key(key)
            .content_type("text/plain")
            .cache_control("no-cache")
            .metadata("zeta", "1")
            .metadata("alpha", "2")
            .tagging("k=v w")
            .body(ByteStream::from_static(b"x"))
            .send()
            .await
            .unwrap_or_else(|e| panic!("{key}: {e:?}"));
        c.head_object().bucket("bkt").key(key).send().await.unwrap();
    }

    // UNSIGNED-PAYLOAD.
    c.put_object()
        .bucket("bkt")
        .key("unsigned")
        .body(ByteStream::from_static(b"u"))
        .customize()
        .disable_payload_signing()
        .send()
        .await
        .unwrap();
    assert_eq!(modes.last(), "UNSIGNED-PAYLOAD");

    // Query parameters that must be sorted and encoded.
    let out = c
        .list_objects_v2()
        .bucket("bkt")
        .prefix("ünï/")
        .delimiter("/")
        .start_after("a")
        .max_keys(5)
        .send()
        .await
        .unwrap();
    assert_eq!(out.key_count(), Some(1));

    // DeleteObjects: signed XML body plus checksum header.
    let objects = keys
        .iter()
        .map(|k| ObjectIdentifier::builder().key(*k).build().unwrap())
        .collect();
    let out = c
        .delete_objects()
        .bucket("bkt")
        .delete(
            Delete::builder()
                .set_objects(Some(objects))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(out.deleted().len(), keys.len());
}

/// Signs for `bucket.localhost:PORT` but connects to 127.0.0.1, so the
/// test does not depend on `*.localhost` resolving.
#[derive(Debug)]
struct ConnectToLoopback(std::net::SocketAddr);

impl Intercept for ConnectToLoopback {
    fn name(&self) -> &'static str {
        "ConnectToLoopback"
    }

    fn modify_before_transmit(
        &self,
        ctx: &mut BeforeTransmitInterceptorContextMut<'_>,
        _rc: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), aws_sdk_s3::error::BoxError> {
        let req = ctx.request_mut();
        let uri: hyper::Uri = req.uri().parse()?;
        let host = uri.authority().unwrap().to_string();
        let path = uri.path_and_query().map_or("/", |p| p.as_str()).to_owned();
        req.headers_mut().insert("host", host);
        req.set_uri(format!("http://{}{path}", self.0))?;
        Ok(())
    }
}

async fn virtual_host_signatures(b: Backend) {
    let s = start_with(b, |c| {
        c.strict_auth = true;
        c.vhost_domain = Some("localhost".into());
    })
    .await;
    let conf = s
        .sdk_config("test", "test")
        .endpoint_url(format!("http://localhost:{}", s.addr.port()))
        .force_path_style(false)
        .interceptor(ConnectToLoopback(s.addr))
        .build();
    let c = aws_sdk_s3::Client::from_conf(conf);
    c.create_bucket().bucket("vhb").send().await.unwrap();
    c.put_object()
        .bucket("vhb")
        .key("some key/ü")
        .body(ByteStream::from_static(b"vh"))
        .send()
        .await
        .unwrap();
    let got = c
        .get_object()
        .bucket("vhb")
        .key("some key/ü")
        .send()
        .await
        .unwrap();
    assert_eq!(&got.body.collect().await.unwrap().into_bytes()[..], b"vh");
    let out = c.list_objects_v2().bucket("vhb").send().await.unwrap();
    assert_eq!(out.contents()[0].key(), Some("some key/ü"));
    // Same server, path-style client.
    let out = s
        .client()
        .list_objects_v2()
        .bucket("vhb")
        .send()
        .await
        .unwrap();
    assert_eq!(out.key_count(), Some(1));
}

backend_tests!(
    accepts_good_signatures,
    rejects_bad_credentials,
    wrong_region_rejected,
    presigned,
    sigv4_canonicalisation,
    virtual_host_signatures,
);
