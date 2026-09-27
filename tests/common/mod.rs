//! Test harness: runs the real server on an ephemeral port.

#![allow(dead_code)]

use std::net::SocketAddr;

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use bytes::Bytes;
pub use garbagebin::config::Backend;
use garbagebin::config::Config;
use garbagebin::server::{self, ServerHandle};
use http_body_util::{BodyExt, Full};
use hyper::{HeaderMap, Request, StatusCode};
use tokio::net::TcpStream;

pub struct TestServer {
    pub handle: ServerHandle,
    pub addr: SocketAddr,
    pub endpoint: String,
    pub config: Config,
    /// Data directory of an fs-backed server, deleted on drop.
    pub dir: Option<tempfile::TempDir>,
}

/// Runs each named `async fn(Backend)` as a test against every backend,
/// as `memory::<name>` and `fs::<name>`.
#[macro_export]
macro_rules! backend_tests {
    ($($name:ident),* $(,)?) => {
        mod memory {
            $(
                #[tokio::test]
                async fn $name() {
                    super::$name($crate::common::Backend::Memory).await;
                }
            )*
        }
        mod fs {
            $(
                #[tokio::test]
                async fn $name() {
                    super::$name($crate::common::Backend::Fs).await;
                }
            )*
        }
    };
}

/// Starts a server on 127.0.0.1 after applying `f` to the default config.
/// An fs backend gets a fresh temporary data directory.
pub async fn start_with(backend: Backend, f: impl FnOnce(&mut Config)) -> TestServer {
    let dir = (backend == Backend::Fs).then(|| tempfile::tempdir().unwrap());
    let mut config = Config {
        listen: "127.0.0.1:0".parse().unwrap(),
        backend,
        ..Default::default()
    };
    if let Some(dir) = &dir {
        config.data_dir = dir.path().to_owned();
    }
    f(&mut config);
    let handle = server::start(&config).await.expect("server start");
    let addr = handle.local_addr;
    TestServer {
        endpoint: format!("http://{addr}"),
        addr,
        handle,
        config,
        dir,
    }
}

pub async fn start(backend: Backend) -> TestServer {
    start_with(backend, |_| {}).await
}

impl TestServer {
    /// Path-style client with the configured credentials.
    pub fn client(&self) -> Client {
        self.client_with(&self.config.access_key, &self.config.secret_key)
    }

    pub fn client_with(&self, access_key: &str, secret_key: &str) -> Client {
        Client::from_conf(
            self.sdk_config(access_key, secret_key)
                .force_path_style(true)
                .build(),
        )
    }

    pub fn sdk_config(&self, access_key: &str, secret_key: &str) -> aws_sdk_s3::config::Builder {
        sdk_config(&self.endpoint, &self.config.region, access_key, secret_key)
    }

    /// Sends one raw HTTP/1.1 request. `Host` defaults to the server address.
    pub async fn raw(&self, req: Request<Full<Bytes>>) -> RawResponse {
        let mut req = req;
        if !req.headers().contains_key("host") {
            req.headers_mut()
                .insert("host", self.addr.to_string().parse().unwrap());
        }
        let stream = TcpStream::connect(self.addr).await.unwrap();
        let io = hyper_util::rt::TokioIo::new(stream);
        let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
        tokio::spawn(conn);
        let resp = sender.send_request(req).await.unwrap();
        let status = resp.status();
        let headers = resp.headers().clone();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        RawResponse {
            status,
            headers,
            body,
        }
    }

    pub async fn raw_get(&self, path: &str) -> RawResponse {
        self.raw(Request::get(path).body(Full::default()).unwrap())
            .await
    }
}

pub fn sdk_config(
    endpoint: &str,
    region: &str,
    access_key: &str,
    secret_key: &str,
) -> aws_sdk_s3::config::Builder {
    let creds = Credentials::new(access_key, secret_key, None, None, "test");
    aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(region.to_owned()))
        .endpoint_url(endpoint)
        .credentials_provider(creds)
        .http_client(aws_smithy_http_client::Builder::new().build_http())
}

/// Path-style client for a server at `endpoint` with default region and credentials.
pub fn client_for(endpoint: &str) -> Client {
    Client::from_conf(
        sdk_config(endpoint, "us-east-1", "test", "test")
            .force_path_style(true)
            .build(),
    )
}

pub struct RawResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl RawResponse {
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }
}

/// S3 error code of an SDK error, e.g. `NoSuchKey`.
pub fn code<E: aws_sdk_s3::error::ProvideErrorMetadata>(e: &E) -> String {
    e.code().unwrap_or("<none>").to_owned()
}

pub fn status<E>(
    e: &aws_sdk_s3::error::SdkError<E, aws_sdk_s3::config::http::HttpResponse>,
) -> u16 {
    e.raw_response().map_or(0, |r| r.status().as_u16())
}

pub fn md5_hex(data: &[u8]) -> String {
    use md5::{Digest, Md5};
    Md5::digest(data)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Records `x-amz-content-sha256` of every request the SDK sends.
#[derive(Debug, Clone, Default)]
pub struct PayloadModes(pub std::sync::Arc<std::sync::Mutex<Vec<String>>>);

impl aws_sdk_s3::config::Intercept for PayloadModes {
    fn name(&self) -> &'static str {
        "PayloadModes"
    }

    fn read_before_transmit(
        &self,
        ctx: &aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextRef<'_>,
        _rc: &aws_sdk_s3::config::RuntimeComponents,
        _cfg: &mut aws_sdk_s3::config::ConfigBag,
    ) -> Result<(), aws_sdk_s3::error::BoxError> {
        let mode = ctx
            .request()
            .headers()
            .get("x-amz-content-sha256")
            .unwrap_or("")
            .to_owned();
        self.0.lock().unwrap().push(mode);
        Ok(())
    }
}

impl PayloadModes {
    pub fn last(&self) -> String {
        self.0.lock().unwrap().last().cloned().unwrap_or_default()
    }
}

/// A file-backed body: the SDK streams these with aws-chunked framing and a
/// trailing checksum, unlike in-memory bodies it can hash up front. Keep the
/// guard alive until the request is sent.
pub async fn streaming_body(data: &[u8]) -> (aws_sdk_s3::primitives::ByteStream, TempFile) {
    let path = std::env::temp_dir().join(format!("gb-test-{}", uuid::Uuid::new_v4()));
    std::fs::write(&path, data).unwrap();
    let body = aws_sdk_s3::primitives::ByteStream::from_path(&path)
        .await
        .unwrap();
    (body, TempFile(path))
}

pub struct TempFile(std::path::PathBuf);

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
