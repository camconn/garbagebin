//! Upload digests: MD5 for the ETag plus any requested `x-amz-checksum-*`,
//! computed while the body streams to the backend.

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use futures::Stream;
use http::HeaderMap;
use md5::{Digest, Md5};
use s3s::crypto::{self, Checksum as _};

use crate::storage::{ByteStream, Checksums};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Algorithm {
    Crc32,
    Crc32c,
    Crc64Nvme,
    Sha1,
    Sha256,
}

impl Algorithm {
    pub const ALL: [Algorithm; 5] = [
        Self::Crc32,
        Self::Crc32c,
        Self::Crc64Nvme,
        Self::Sha1,
        Self::Sha256,
    ];

    /// From `x-amz-sdk-checksum-algorithm` style names (`CRC32`) or
    /// header names (`x-amz-checksum-crc32`).
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim().to_ascii_lowercase();
        let s = s.strip_prefix("x-amz-checksum-").unwrap_or(&s);
        Self::ALL
            .into_iter()
            .find(|a| a.name().eq_ignore_ascii_case(s))
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Crc32 => "CRC32",
            Self::Crc32c => "CRC32C",
            Self::Crc64Nvme => "CRC64NVME",
            Self::Sha1 => "SHA1",
            Self::Sha256 => "SHA256",
        }
    }

    pub fn header(self) -> String {
        format!("x-amz-checksum-{}", self.name().to_ascii_lowercase())
    }

    pub fn get(self, c: &Checksums) -> Option<&String> {
        self.slot_ref(c).as_ref()
    }

    pub fn set(self, c: &mut Checksums, v: Option<String>) {
        *self.slot(c) = v;
    }

    fn slot(self, c: &mut Checksums) -> &mut Option<String> {
        match self {
            Self::Crc32 => &mut c.crc32,
            Self::Crc32c => &mut c.crc32c,
            Self::Crc64Nvme => &mut c.crc64nvme,
            Self::Sha1 => &mut c.sha1,
            Self::Sha256 => &mut c.sha256,
        }
    }

    fn slot_ref(self, c: &Checksums) -> &Option<String> {
        match self {
            Self::Crc32 => &c.crc32,
            Self::Crc32c => &c.crc32c,
            Self::Crc64Nvme => &c.crc64nvme,
            Self::Sha1 => &c.sha1,
            Self::Sha256 => &c.sha256,
        }
    }

    /// CRCs can be combined across parts into a `FULL_OBJECT` checksum.
    pub fn combinable(self) -> bool {
        self.crc().is_some()
    }

    /// The crc-fast algorithm and byte width of a CRC.
    fn crc(self) -> Option<(crc_fast::CrcAlgorithm, usize)> {
        use crc_fast::CrcAlgorithm as C;
        match self {
            Self::Crc32 => Some((C::Crc32IsoHdlc, 4)),
            Self::Crc32c => Some((C::Crc32Iscsi, 4)),
            Self::Crc64Nvme => Some((C::Crc64Nvme, 8)),
            Self::Sha1 | Self::Sha256 => None,
        }
    }

    /// One-shot digest, base64-encoded.
    pub fn digest(self, data: &[u8]) -> String {
        let mut h = Hasher::new(self);
        h.update(data);
        h.finish()
    }
}

enum Hasher {
    Crc32(crypto::Crc32),
    Crc32c(crypto::Crc32c),
    Crc64Nvme(crypto::Crc64Nvme),
    Sha1(crypto::Sha1),
    Sha256(crypto::Sha256),
}

impl Hasher {
    fn new(a: Algorithm) -> Self {
        match a {
            Algorithm::Crc32 => Self::Crc32(crypto::Crc32::new()),
            Algorithm::Crc32c => Self::Crc32c(crypto::Crc32c::new()),
            Algorithm::Crc64Nvme => Self::Crc64Nvme(crypto::Crc64Nvme::new()),
            Algorithm::Sha1 => Self::Sha1(crypto::Sha1::new()),
            Algorithm::Sha256 => Self::Sha256(crypto::Sha256::new()),
        }
    }

    fn update(&mut self, data: &[u8]) {
        match self {
            Self::Crc32(h) => h.update(data),
            Self::Crc32c(h) => h.update(data),
            Self::Crc64Nvme(h) => h.update(data),
            Self::Sha1(h) => h.update(data),
            Self::Sha256(h) => h.update(data),
        }
    }

    fn finish(self) -> String {
        match self {
            Self::Crc32(h) => STANDARD.encode(h.finalize()),
            Self::Crc32c(h) => STANDARD.encode(h.finalize()),
            Self::Crc64Nvme(h) => STANDARD.encode(h.finalize()),
            Self::Sha1(h) => STANDARD.encode(h.finalize()),
            Self::Sha256(h) => STANDARD.encode(h.finalize()),
        }
    }
}

/// What a finished body hashed to.
#[derive(Debug, Clone)]
pub struct Digests {
    pub size: u64,
    pub md5: [u8; 16],
    pub checksums: Checksums,
}

struct State {
    size: u64,
    md5: Md5,
    hashers: Vec<(Algorithm, Hasher)>,
}

/// Handle for reading the digests once the wrapped stream is consumed.
#[derive(Clone)]
pub struct DigestHandle(Arc<Mutex<Option<State>>>);

impl DigestHandle {
    /// Panics if called twice.
    pub fn finish(&self) -> Digests {
        let state = self.0.lock().unwrap_or_else(PoisonError::into_inner).take();
        let state = state.expect("digests already taken");
        let mut checksums = Checksums::default();
        for (a, h) in state.hashers {
            a.set(&mut checksums, Some(h.finish()));
        }
        Digests {
            size: state.size,
            md5: state.md5.finalize().into(),
            checksums,
        }
    }
}

struct Hashing {
    inner: ByteStream,
    state: Arc<Mutex<Option<State>>>,
}

impl Stream for Hashing {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = self.inner.as_mut().poll_next(cx);
        if let Poll::Ready(Some(Ok(chunk))) = &item {
            let mut guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(st) = guard.as_mut() {
                st.size += chunk.len() as u64;
                st.md5.update(chunk);
                for (_, h) in &mut st.hashers {
                    h.update(chunk);
                }
            }
        }
        item
    }
}

/// Wraps `body` so it computes MD5 and `algorithms` as it is read.
pub fn hashing(body: ByteStream, algorithms: &[Algorithm]) -> (ByteStream, DigestHandle) {
    let mut hashers: Vec<(Algorithm, Hasher)> = Vec::new();
    for &a in algorithms {
        if !hashers.iter().any(|(b, _)| *b == a) {
            hashers.push((a, Hasher::new(a)));
        }
    }
    let state = Arc::new(Mutex::new(Some(State {
        size: 0,
        md5: Md5::new(),
        hashers,
    })));
    let stream = Hashing {
        inner: body,
        state: Arc::clone(&state),
    };
    (Box::pin(stream), DigestHandle(state))
}

/// Algorithms a request asks for: explicit checksum values, the SDK
/// algorithm hint, and checksum names announced in `x-amz-trailer`.
pub fn requested(
    expected: &Checksums,
    sdk_algorithm: Option<&str>,
    headers: &HeaderMap,
) -> Vec<Algorithm> {
    let mut out: Vec<Algorithm> = Algorithm::ALL
        .into_iter()
        .filter(|a| a.get(expected).is_some())
        .collect();
    out.extend(sdk_algorithm.and_then(Algorithm::parse));
    out.extend(declared_trailers(headers));
    out
}

/// Checksums a request names in `x-amz-trailer`.
fn declared_trailers(headers: &HeaderMap) -> impl Iterator<Item = Algorithm> + '_ {
    headers
        .get_all("x-amz-trailer")
        .iter()
        .flat_map(|v| v.to_str().unwrap_or_default().split(','))
        .filter_map(Algorithm::parse)
}

/// The first checksum declared in `x-amz-trailer` that did not arrive as a trailer.
pub fn missing_trailer(headers: &HeaderMap, trailers: Option<&HeaderMap>) -> Option<Algorithm> {
    declared_trailers(headers).find(|a| trailers.is_none_or(|t| !t.contains_key(a.header())))
}

/// The first checksum a request names (value header, algorithm header or `x-amz-trailer`)
/// that this server cannot compute. Such uploads are refused rather than stored unverified.
pub fn unsupported(headers: &HeaderMap) -> Option<String> {
    let known = |name: &str| Algorithm::parse(name).is_some();
    for (k, v) in headers {
        let v = v.to_str().unwrap_or_default();
        let bad = match k.as_str() {
            "x-amz-sdk-checksum-algorithm" | "x-amz-checksum-algorithm" => (!known(v)).then_some(v),
            "x-amz-checksum-type" | "x-amz-checksum-mode" => None,
            "x-amz-trailer" => v
                .split(',')
                .map(str::trim)
                .find(|n| n.to_ascii_lowercase().starts_with("x-amz-checksum-") && !known(n)),
            name if name.starts_with("x-amz-checksum-") && !known(name) => Some(name),
            _ => None,
        };
        if let Some(b) = bad {
            return Some(b.to_owned());
        }
    }
    None
}

/// Adds checksum values delivered as aws-chunked trailers to `expected`.
pub fn merge_trailers(expected: &mut Checksums, trailers: Option<&HeaderMap>) {
    let Some(trailers) = trailers else { return };
    for a in Algorithm::ALL {
        if let Some(v) = trailers.get(a.header()).and_then(|v| v.to_str().ok()) {
            a.set(expected, Some(v.to_owned()));
        }
    }
}

/// First algorithm whose expected value differs from what was computed.
pub fn mismatch(expected: &Checksums, computed: &Checksums) -> Option<Algorithm> {
    Algorithm::ALL
        .into_iter()
        .find(|a| a.get(expected).is_some_and(|e| a.get(computed) != Some(e)))
}

#[derive(Debug, PartialEq, Eq)]
pub enum Md5Error {
    /// Not base64 of 16 bytes: `InvalidDigest`.
    Invalid,
    /// Well-formed but wrong: `BadDigest`.
    Mismatch,
}

/// Verifies a `Content-MD5` header value against the body's MD5.
pub fn check_content_md5(header: &str, md5: &[u8; 16]) -> Result<(), Md5Error> {
    let decoded = STANDARD
        .decode(header.trim())
        .map_err(|_| Md5Error::Invalid)?;
    if decoded.len() != 16 {
        return Err(Md5Error::Invalid);
    }
    if decoded != md5 {
        return Err(Md5Error::Mismatch);
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
pub enum BodyError {
    Md5(Md5Error),
    Checksum(Algorithm),
}

/// Verifies `Content-MD5` and `x-amz-checksum-*` headers against a buffered body.
pub fn verify_body(headers: &HeaderMap, body: &[u8]) -> Result<(), BodyError> {
    if let Some(v) = headers.get("content-md5") {
        let md5: [u8; 16] = Md5::digest(body).into();
        check_content_md5(v.to_str().unwrap_or_default(), &md5).map_err(BodyError::Md5)?;
    }
    for a in Algorithm::ALL {
        if let Some(v) = headers.get(a.header())
            && v.to_str().ok().map(str::trim) != Some(a.digest(body).as_str())
        {
            return Err(BodyError::Checksum(a));
        }
    }
    Ok(())
}

/// Composite multipart checksum: checksum of the concatenated raw part
/// checksums, suffixed with `-N`. `None` if any part lacks one.
pub fn composite(algorithm: Algorithm, parts: &[(&Checksums, u64)]) -> Option<String> {
    let mut raw = Vec::new();
    for (p, _) in parts {
        raw.extend(STANDARD.decode(algorithm.get(p)?).ok()?);
    }
    Some(format!("{}-{}", algorithm.digest(&raw), parts.len()))
}

/// `FULL_OBJECT` multipart checksum: the CRC of the whole object, combined from
/// `(part checksums, part size)`. `None` for a non-CRC algorithm or a part without one.
pub fn full_object(algorithm: Algorithm, parts: &[(&Checksums, u64)]) -> Option<String> {
    let (crc, width) = algorithm.crc()?;
    let mut acc: Option<u64> = None;
    for (p, size) in parts {
        let raw = STANDARD.decode(algorithm.get(p)?).ok()?;
        if raw.len() != width {
            return None;
        }
        let mut buf = [0u8; 8];
        buf[8 - width..].copy_from_slice(&raw);
        let v = u64::from_be_bytes(buf);
        acc = Some(match acc {
            None => v,
            Some(a) => crc_fast::checksum_combine(crc, a, v, *size),
        });
    }
    let bytes = acc?.to_be_bytes();
    Some(STANDARD.encode(&bytes[8 - width..]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[test]
    fn known_values() {
        // Values as AWS reports them for the body "hello world".
        let data = b"hello world";
        assert_eq!(Algorithm::Crc32.digest(data), "DUoRhQ==");
        assert_eq!(Algorithm::Crc32c.digest(data), "yZRlqg==");
        assert_eq!(Algorithm::Sha1.digest(data), "Kq5sNclPz7QV2+lfQIuc6R7oRu0=");
        assert_eq!(
            Algorithm::Sha256.digest(data),
            "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek="
        );
    }

    #[test]
    fn parsing() {
        assert_eq!(Algorithm::parse("CRC32"), Some(Algorithm::Crc32));
        assert_eq!(
            Algorithm::parse("x-amz-checksum-crc32c"),
            Some(Algorithm::Crc32c)
        );
        assert_eq!(Algorithm::parse(" sha256"), Some(Algorithm::Sha256));
        assert_eq!(Algorithm::parse("md5"), None);
    }

    #[test]
    fn unsupported_algorithms() {
        let h = |pairs: &[(&'static str, &'static str)]| {
            let mut m = HeaderMap::new();
            for (k, v) in pairs {
                m.append(*k, v.parse().unwrap());
            }
            unsupported(&m)
        };
        assert_eq!(h(&[]), None);
        assert_eq!(
            h(&[
                ("x-amz-checksum-crc32", "AAAAAA=="),
                ("x-amz-sdk-checksum-algorithm", "CRC32"),
                ("x-amz-checksum-type", "FULL_OBJECT"),
                ("x-amz-trailer", "x-amz-checksum-crc32c"),
            ]),
            None
        );
        assert_eq!(
            h(&[("x-amz-checksum-md5", "x")]).as_deref(),
            Some("x-amz-checksum-md5")
        );
        assert_eq!(
            h(&[("x-amz-checksum-algorithm", "MD5")]).as_deref(),
            Some("MD5")
        );
        assert_eq!(
            h(&[("x-amz-trailer", "x-amz-checksum-sha512")]).as_deref(),
            Some("x-amz-checksum-sha512")
        );
    }

    #[tokio::test]
    async fn stream_hashing() {
        let chunks: Vec<io::Result<Bytes>> = vec![
            Ok(Bytes::from_static(b"hello ")),
            Ok(Bytes::from_static(b"world")),
        ];
        let (mut s, handle) = hashing(Box::pin(futures::stream::iter(chunks)), &[Algorithm::Crc32]);
        while s.next().await.is_some() {}
        let d = handle.finish();
        assert_eq!(d.size, 11);
        assert_eq!(
            crate::logic::etag::single(&d.md5),
            "5eb63bbbe01eeed093cb22bb8f5acdc3"
        );
        assert_eq!(d.checksums.crc32.as_deref(), Some("DUoRhQ=="));
        assert_eq!(d.checksums.sha256, None);
    }

    #[test]
    fn content_md5() {
        let md5: [u8; 16] = Md5::digest(b"x").into();
        let good = STANDARD.encode(md5);
        assert_eq!(check_content_md5(&good, &md5), Ok(()));
        assert_eq!(check_content_md5("nope", &md5), Err(Md5Error::Invalid));
        assert_eq!(
            check_content_md5(&STANDARD.encode([0u8; 16]), &md5),
            Err(Md5Error::Mismatch)
        );
    }

    #[test]
    fn buffered_body() {
        let mut h = HeaderMap::new();
        assert_eq!(verify_body(&h, b"hello world"), Ok(()));
        h.insert("content-md5", "XrY7u+Ae7tCTyyK7j1rNww==".parse().unwrap());
        h.insert("x-amz-checksum-crc32", "DUoRhQ==".parse().unwrap());
        assert_eq!(verify_body(&h, b"hello world"), Ok(()));
        assert_eq!(
            verify_body(&h, b"hello"),
            Err(BodyError::Md5(Md5Error::Mismatch))
        );
        h.remove("content-md5");
        assert_eq!(
            verify_body(&h, b"hello"),
            Err(BodyError::Checksum(Algorithm::Crc32))
        );
    }

    #[test]
    fn mismatches_and_composite() {
        let mut expected = Checksums::default();
        let computed = Checksums {
            crc32: Some("a".into()),
            ..Default::default()
        };
        assert_eq!(mismatch(&expected, &computed), None);
        expected.crc32 = Some("b".into());
        assert_eq!(mismatch(&expected, &computed), Some(Algorithm::Crc32));
        expected.sha1 = Some("c".into());
        expected.crc32 = Some("a".into());
        assert_eq!(mismatch(&expected, &computed), Some(Algorithm::Sha1));

        let p1 = Checksums {
            crc32: Some(Algorithm::Crc32.digest(b"a")),
            ..Default::default()
        };
        let p2 = Checksums {
            crc32: Some(Algorithm::Crc32.digest(b"b")),
            ..Default::default()
        };
        let got = composite(Algorithm::Crc32, &[(&p1, 1), (&p2, 1)]).unwrap();
        assert!(got.ends_with("-2"));
        assert_eq!(composite(Algorithm::Sha1, &[(&p1, 1)]), None);
    }

    #[test]
    fn full_object_combines_parts() {
        let body = b"hello brave new world";
        let (a, b) = body.split_at(6);
        for alg in [Algorithm::Crc32, Algorithm::Crc32c, Algorithm::Crc64Nvme] {
            let mut p1 = Checksums::default();
            let mut p2 = Checksums::default();
            alg.set(&mut p1, Some(alg.digest(a)));
            alg.set(&mut p2, Some(alg.digest(b)));
            let got = full_object(alg, &[(&p1, a.len() as u64), (&p2, b.len() as u64)]);
            assert_eq!(got, Some(alg.digest(body)), "{alg:?}");
            assert_eq!(
                full_object(alg, &[(&p1, a.len() as u64)]),
                Some(alg.digest(a))
            );
        }
        let mut p = Checksums::default();
        Algorithm::Sha256.set(&mut p, Some(Algorithm::Sha256.digest(a)));
        assert_eq!(full_object(Algorithm::Sha256, &[(&p, 6)]), None);
    }
}
