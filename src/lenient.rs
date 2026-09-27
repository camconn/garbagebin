//! Lenient auth: accept any credentials. s3s verifies every signature it
//! sees, so lenient mode strips signatures before s3s and decodes
//! `aws-chunked` bodies itself, ignoring chunk signatures.

use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, ready};

use bytes::{Buf, Bytes, BytesMut};
use futures::Stream;
use http::header::{AUTHORIZATION, CONTENT_LENGTH};
use http::{HeaderMap, HeaderName, HeaderValue, Request, Uri};
use s3s::stream::{ByteStream, RemainingLength};
use s3s::{Body, StdError};

/// Trailing headers of a lenient aws-chunked body; in request extensions.
#[derive(Clone, Default)]
pub struct Trailers(Arc<Mutex<Option<HeaderMap>>>);

impl Trailers {
    pub fn take(&self) -> Option<HeaderMap> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).take()
    }
}

const AUTH_PARAMS: &[&str] = &[
    "X-Amz-Algorithm",
    "X-Amz-Credential",
    "X-Amz-Date",
    "X-Amz-Expires",
    "X-Amz-SignedHeaders",
    "X-Amz-Signature",
    "X-Amz-Security-Token",
    "AWSAccessKeyId",
    "Signature",
    "Expires",
];

/// Turns a signed request (header or presigned) into an anonymous one.
pub fn strip_auth(req: &mut Request<Body>) {
    if let Some(uri) = strip_query_auth(req.uri()) {
        *req.uri_mut() = uri;
    }
    let headers = req.headers_mut();
    headers.remove(AUTHORIZATION);

    let streaming = headers
        .get("x-amz-content-sha256")
        .is_some_and(|v| v.as_bytes().starts_with(b"STREAMING-"));
    if !streaming {
        return;
    }
    headers.remove("x-amz-content-sha256");
    let decoded_len = headers
        .remove("x-amz-decoded-content-length")
        .and_then(|v| v.to_str().ok()?.parse::<usize>().ok());
    match decoded_len {
        Some(n) => headers.insert(CONTENT_LENGTH, HeaderValue::from(n)),
        None => headers.remove(CONTENT_LENGTH),
    };
    let trailers = Trailers::default();
    let body = std::mem::take(req.body_mut());
    let decoder = ChunkedDecoder {
        inner: body,
        buf: BytesMut::new(),
        state: State::Header,
        trailers: trailers.clone(),
        remaining: decoded_len,
    };
    *req.body_mut() = Body::from(Box::pin(decoder) as s3s::stream::DynByteStream);
    req.extensions_mut().insert(trailers);
}

fn strip_query_auth(uri: &Uri) -> Option<Uri> {
    let query = uri.query()?;
    let is_auth = |pair: &&str| {
        let name = pair.split('=').next().unwrap_or_default();
        AUTH_PARAMS.iter().any(|p| p.eq_ignore_ascii_case(name))
    };
    if !query.split('&').any(|p| is_auth(&p)) {
        return None;
    }
    let kept: Vec<&str> = query
        .split('&')
        .filter(|p| !p.is_empty() && !is_auth(p))
        .collect();
    let mut pq = uri.path().to_owned();
    if !kept.is_empty() {
        pq.push('?');
        pq.push_str(&kept.join("&"));
    }
    let mut parts = uri.clone().into_parts();
    parts.path_and_query = Some(pq.parse().ok()?);
    Uri::from_parts(parts).ok()
}

#[derive(Debug, thiserror::Error)]
#[error("malformed aws-chunked body")]
pub struct MalformedChunk;

enum State {
    Header,
    Data(usize),
    DataEnd,
    Trailers,
    Done,
}

/// Decodes `size[;chunk-signature=..]\r\n data \r\n ... 0\r\n trailers \r\n`.
struct ChunkedDecoder {
    inner: Body,
    buf: BytesMut,
    state: State,
    trailers: Trailers,
    remaining: Option<usize>,
}

const MAX_LINE: usize = 4096;

impl ChunkedDecoder {
    fn line(&mut self) -> Option<Bytes> {
        let pos = self.buf.windows(2).position(|w| w == b"\r\n")?;
        let line = self.buf.split_to(pos).freeze();
        self.buf.advance(2);
        Some(line)
    }

    fn parse_trailers(&mut self) -> Result<(), StdError> {
        let mut map = HeaderMap::new();
        for line in self.buf[..].split(|&b| b == b'\n') {
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            if line.is_empty() {
                continue;
            }
            let colon = line.iter().position(|&b| b == b':').ok_or(MalformedChunk)?;
            let name = HeaderName::from_bytes(line[..colon].trim_ascii())?;
            let value = HeaderValue::from_bytes(line[colon + 1..].trim_ascii())?;
            map.append(name, value);
        }
        *self
            .trailers
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(map);
        Ok(())
    }
}

impl Stream for ChunkedDecoder {
    type Item = Result<Bytes, StdError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.state {
                State::Done => return Poll::Ready(None),
                State::Header => {
                    if let Some(line) = self.line() {
                        let size = line.split(|&b| b == b';').next().unwrap_or_default();
                        let size = std::str::from_utf8(size.trim_ascii())
                            .ok()
                            .and_then(|s| usize::from_str_radix(s, 16).ok());
                        self.state = match size {
                            Some(0) => State::Trailers,
                            Some(n) => State::Data(n),
                            None => return Poll::Ready(Some(Err(MalformedChunk.into()))),
                        };
                        continue;
                    }
                    if self.buf.len() > MAX_LINE {
                        return Poll::Ready(Some(Err(MalformedChunk.into())));
                    }
                }
                State::Data(n) if !self.buf.is_empty() => {
                    let take = n.min(self.buf.len());
                    let data = self.buf.split_to(take).freeze();
                    self.state = if take == n {
                        State::DataEnd
                    } else {
                        State::Data(n - take)
                    };
                    if let Some(r) = &mut self.remaining {
                        *r = r.saturating_sub(take);
                    }
                    return Poll::Ready(Some(Ok(data)));
                }
                State::Data(_) => {}
                State::DataEnd if self.buf.len() >= 2 => {
                    if &self.buf[..2] != b"\r\n" {
                        return Poll::Ready(Some(Err(MalformedChunk.into())));
                    }
                    self.buf.advance(2);
                    self.state = State::Header;
                    continue;
                }
                State::DataEnd | State::Trailers => {}
            }
            // Need more input.
            match ready!(Pin::new(&mut self.inner).poll_next(cx)) {
                Some(Ok(bytes)) => {
                    if matches!(self.state, State::Trailers)
                        && self.buf.len() + bytes.len() > 16 * 1024
                    {
                        return Poll::Ready(Some(Err(MalformedChunk.into())));
                    }
                    self.buf.extend_from_slice(&bytes);
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None if matches!(self.state, State::Trailers) => {
                    self.state = State::Done;
                    if let Err(e) = self.parse_trailers() {
                        return Poll::Ready(Some(Err(e)));
                    }
                    return Poll::Ready(None);
                }
                None => return Poll::Ready(Some(Err(MalformedChunk.into()))),
            }
        }
    }
}

impl ByteStream for ChunkedDecoder {
    fn remaining_length(&self) -> RemainingLength {
        match self.remaining {
            Some(n) => RemainingLength::new_exact(n),
            None => RemainingLength::unknown(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    fn request(uri: &str, body: &'static [u8], streaming: bool) -> Request<Body> {
        let mut b = Request::builder()
            .uri(uri)
            .header(AUTHORIZATION, "AWS4-HMAC-SHA256 whatever");
        if streaming {
            b = b
                .header("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
                .header("x-amz-decoded-content-length", "11")
                .header(CONTENT_LENGTH, body.len());
        }
        b.body(Body::from(Bytes::from_static(body))).unwrap()
    }

    async fn collect(body: &mut Body) -> Result<Vec<u8>, StdError> {
        let mut out = Vec::new();
        while let Some(c) = body.next().await {
            out.extend_from_slice(&c?);
        }
        Ok(out)
    }

    #[test]
    fn strips_presigned_params() {
        let mut req = request(
            "/b/k?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=a%2Fb&x-id=GetObject&X-Amz-Signature=ff",
            b"",
            false,
        );
        strip_auth(&mut req);
        assert_eq!(req.uri(), "/b/k?x-id=GetObject");
        assert!(req.headers().get(AUTHORIZATION).is_none());

        let mut req = request("/b/k?AWSAccessKeyId=a&Signature=s&Expires=1", b"", false);
        strip_auth(&mut req);
        assert_eq!(req.uri(), "/b/k");

        let mut req = request("/b?list-type=2&prefix=a", b"", false);
        strip_auth(&mut req);
        assert_eq!(req.uri(), "/b?list-type=2&prefix=a");
    }

    #[tokio::test]
    async fn decodes_signed_chunks_with_trailers() {
        let body = b"6;chunk-signature=abc\r\nhello \r\n5;chunk-signature=def\r\nworld\r\n0;chunk-signature=0\r\nx-amz-checksum-crc32:DUoRhQ==\r\nx-amz-trailer-signature:1\r\n\r\n";
        let mut req = request("/b/k", body, true);
        strip_auth(&mut req);
        assert_eq!(req.headers()[CONTENT_LENGTH], "11");
        let trailers = req.extensions().get::<Trailers>().unwrap().clone();
        assert_eq!(collect(req.body_mut()).await.unwrap(), b"hello world");
        let t = trailers.take().unwrap();
        assert_eq!(t["x-amz-checksum-crc32"], "DUoRhQ==");
    }

    #[tokio::test]
    async fn decodes_unsigned_chunks_split_anywhere() {
        let raw: &[u8] = b"b\r\nhello world\r\n0\r\n\r\n";
        for split in 0..raw.len() {
            let chunks: Vec<Result<Bytes, StdError>> = vec![
                Ok(Bytes::copy_from_slice(&raw[..split])),
                Ok(Bytes::copy_from_slice(&raw[split..])),
            ];
            let inner: s3s::stream::DynByteStream = Box::pin(Chunks(chunks.into()));
            let mut dec = ChunkedDecoder {
                inner: Body::from(inner),
                buf: BytesMut::new(),
                state: State::Header,
                trailers: Trailers::default(),
                remaining: None,
            };
            let mut out = Vec::new();
            while let Some(c) = dec.next().await {
                out.extend_from_slice(&c.unwrap());
            }
            assert_eq!(out, b"hello world", "split at {split}");
        }
    }

    #[tokio::test]
    async fn rejects_truncated_body() {
        let mut req = request("/b/k", b"b\r\nhello", true);
        strip_auth(&mut req);
        assert!(collect(req.body_mut()).await.is_err());
    }

    struct Chunks(std::collections::VecDeque<Result<Bytes, StdError>>);

    impl Stream for Chunks {
        type Item = Result<Bytes, StdError>;
        fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(self.0.pop_front())
        }
    }

    impl ByteStream for Chunks {}
}
