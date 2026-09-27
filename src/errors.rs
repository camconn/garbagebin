//! S3 error bodies. `render` is the only writer of error XML; s3s's own
//! serializer omits `Key`, `BucketName` and `HostId`.

use std::sync::{Arc, Mutex, PoisonError};

use s3s::xml::{Deserializer, Serializer};
use s3s::{S3Error, S3ErrorCode};

/// Bucket and key an error body names.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Resource {
    pub bucket: Option<String>,
    pub key: Option<String>,
}

/// Attached to an error about something other than what the request addresses
/// (a copy source).
#[derive(Debug, thiserror::Error)]
#[error("error about {0:?}")]
struct About(Resource);

/// Marks `e` as being about `bucket`/`key` rather than the request's own target.
/// An error that already has a source (an internal one) keeps it.
pub fn about(mut e: S3Error, bucket: &str, key: &str) -> S3Error {
    if e.source().is_none() {
        e.set_source(Box::new(About(Resource {
            bucket: Some(bucket.to_owned()),
            key: Some(key.to_owned()),
        })));
    }
    e
}

/// What an error body says, besides the request ids.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorBody {
    pub code: S3ErrorCode,
    pub message: Option<String>,
    /// `None`: what the request addresses.
    pub resource: Option<Resource>,
}

impl From<&S3Error> for ErrorBody {
    fn from(e: &S3Error) -> Self {
        let about = e.source().and_then(|s| s.downcast_ref::<About>());
        Self {
            code: e.code().clone(),
            message: e.message().map(str::to_owned),
            resource: about.map(|a| a.0.clone()),
        }
    }
}

impl ErrorBody {
    /// Reads Code and Message back from an error s3s rendered before any of our
    /// code saw it (auth, routing, request parsing); every other error is captured.
    pub fn decode(xml: &[u8]) -> Option<Self> {
        let mut d = Deserializer::new(xml);
        let (code, message) = d
            .named_element("Error", |d| {
                let (mut code, mut message) = (None, None);
                d.for_each_element(|d, name| {
                    match name {
                        b"Code" => code = Some(d.content::<String>()?),
                        b"Message" => message = Some(d.content::<String>()?),
                        _ => d.text(|_| Ok(()))?,
                    }
                    Ok(())
                })?;
                Ok((code, message))
            })
            .ok()?;
        Some(Self {
            code: S3ErrorCode::from_bytes(code?.as_bytes())?,
            message,
            resource: None,
        })
    }
}

/// Where the service leaves the error it returned to s3s, for the server to render.
#[derive(Clone, Default)]
pub struct Captured(Arc<Mutex<Option<ErrorBody>>>);

impl Captured {
    pub fn set(&self, e: &S3Error) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(e.into());
    }

    pub fn take(&self) -> Option<ErrorBody> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).take()
    }
}

/// The error XML: Code, Message, then Key or BucketName where AWS sends one,
/// then RequestId and HostId. `target` is what the request addresses.
pub fn render(e: &ErrorBody, target: &Resource, request_id: &str) -> Vec<u8> {
    let r = e.resource.as_ref().unwrap_or(target);
    let key = match e.code {
        S3ErrorCode::NoSuchKey => r.key.as_deref(),
        _ => None,
    };
    let bucket = match e.code {
        S3ErrorCode::NoSuchBucket | S3ErrorCode::BucketNotEmpty => r.bucket.as_deref(),
        _ => None,
    };
    let mut buf = Vec::new();
    let mut s = Serializer::new(&mut buf);
    s.decl()
        .and_then(|()| {
            s.element("Error", |s| {
                s.content("Code", e.code.as_str())?;
                if let Some(m) = &e.message {
                    s.content("Message", m.as_str())?;
                }
                if let Some(k) = key {
                    s.content("Key", k)?;
                }
                if let Some(b) = bucket {
                    s.content("BucketName", b)?;
                }
                s.content("RequestId", request_id)?;
                s.content("HostId", request_id)
            })
        })
        .expect("writing XML to a Vec");
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_and_decodes() {
        let e = S3Error::with_message(S3ErrorCode::NoSuchKey, "m");
        let target = Resource {
            bucket: Some("b".into()),
            key: Some("a&b".into()),
        };
        let xml = render(&ErrorBody::from(&e), &target, "RID");
        assert_eq!(
            String::from_utf8(xml.clone()).unwrap(),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchKey</Code>\
             <Message>m</Message><Key>a&amp;b</Key><RequestId>RID</RequestId>\
             <HostId>RID</HostId></Error>"
        );
        let back = ErrorBody::decode(&xml).unwrap();
        assert_eq!(
            (back.code, back.message.as_deref()),
            (S3ErrorCode::NoSuchKey, Some("m"))
        );

        let e = about(S3Error::new(S3ErrorCode::NoSuchBucket), "src", "k");
        let xml = String::from_utf8(render(&ErrorBody::from(&e), &target, "R")).unwrap();
        assert!(xml.contains("<BucketName>src</BucketName>"), "{xml}");
        assert!(!xml.contains("<Key>"), "{xml}");
    }
}
