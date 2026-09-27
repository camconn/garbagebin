//! HTTP server: health endpoints, request ids, request log, auth mode, graceful shutdown.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::Context as _;
use bytes::Bytes;
use http::header::{CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, DATE, HOST};
use http::{HeaderValue, Method, Request, Response, StatusCode};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use s3s::auth::SimpleAuth;
use s3s::config::{S3Config, StaticConfigProvider};
use s3s::host::SingleDomain;
use s3s::region::Region;
use s3s::service::{S3Service, S3ServiceBuilder};
use s3s::{Body, S3Error, S3ErrorCode};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::config::{Backend, Config};
use crate::lenient;
use crate::logic::{checksum, validate};
use crate::service::{self, GarbageBin};
use crate::storage::{FsStorage, MemoryStorage, Storage, StorageError};

/// How long shutdown waits for in-flight requests.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

/// A running server. Dropping it leaves the server running; call `shutdown`.
pub struct ServerHandle {
    pub local_addr: SocketAddr,
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

impl ServerHandle {
    /// Stops accepting connections and waits for in-flight requests.
    pub async fn shutdown(self) {
        let _ = self.stop.send(());
        let _ = self.task.await;
    }
}

/// Opens the backend, creates init buckets, binds, and serves in the background.
/// Once this returns, `/health` answers 200.
pub async fn start(config: &Config) -> anyhow::Result<ServerHandle> {
    let s3 = match config.backend {
        Backend::Memory => build(MemoryStorage::new(), config).await?,
        Backend::Fs => build(FsStorage::open(&config.data_dir)?, config).await?,
    };
    let app = Arc::new(App {
        s3,
        lenient: !config.strict_auth,
        vhost_domain: config.vhost_domain.clone(),
    });
    let listener = TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("binding {}", config.listen))?;
    let local_addr = listener.local_addr()?;
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(serve(listener, app, stopped));
    info!(
        addr = %local_addr,
        backend = ?config.backend,
        strict_auth = config.strict_auth,
        "listening"
    );
    Ok(ServerHandle {
        local_addr,
        stop,
        task,
    })
}

/// Runs until SIGTERM or SIGINT, then shuts down gracefully.
pub async fn run(config: Config) -> anyhow::Result<()> {
    let handle = start(&config).await?;
    shutdown_signal().await;
    info!("shutting down");
    handle.shutdown().await;
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

async fn build<S: Storage>(storage: S, config: &Config) -> anyhow::Result<S3Service> {
    for bucket in config
        .init_buckets
        .iter()
        .map(|b| b.trim())
        .filter(|b| !b.is_empty())
    {
        anyhow::ensure!(
            validate::bucket_name(bucket),
            "invalid init bucket name {bucket:?}"
        );
        match storage.create_bucket(bucket).await {
            Ok(()) | Err(StorageError::BucketAlreadyOwnedByYou) => {}
            Err(e) => return Err(e).with_context(|| format!("creating init bucket {bucket}")),
        }
    }

    let mut builder = S3ServiceBuilder::new(GarbageBin::new(storage, config.region.clone()));
    if let Some(domain) = &config.vhost_domain {
        let host = SingleDomain::new(domain)
            .with_context(|| format!("invalid vhost domain {domain:?}"))?
            .with_cname_fallback(false);
        builder.set_host(host);
    }
    let mut s3_config = S3Config::default();
    if config.strict_auth {
        builder.set_auth(SimpleAuth::from_single(
            config.access_key.clone(),
            config.secret_key.clone(),
        ));
        let region = Region::new(config.region.clone().into())
            .map_err(|e| anyhow::anyhow!("invalid region {:?}: {e}", config.region))?;
        s3_config.expected_region = Some(region);
    }
    builder.set_config(Arc::new(StaticConfigProvider::new(Arc::new(s3_config))));
    Ok(builder.build())
}

struct App {
    s3: S3Service,
    lenient: bool,
    vhost_domain: Option<String>,
}

async fn serve(listener: TcpListener, app: Arc<App>, mut stopped: oneshot::Receiver<()>) {
    let graceful = GracefulShutdown::new();
    let builder = auto::Builder::new(TokioExecutor::new());
    loop {
        let stream = tokio::select! {
            res = listener.accept() => match res {
                Ok((stream, _)) => stream,
                Err(e) => {
                    warn!(error = %e, "accept failed");
                    continue;
                }
            },
            _ = &mut stopped => break,
        };
        let app = Arc::clone(&app);
        let svc = service_fn(move |req| handle(Arc::clone(&app), req));
        let conn = builder.serve_connection_with_upgrades(TokioIo::new(stream), svc);
        let conn = graceful.watch(conn.into_owned());
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::debug!(error = %e, "connection error");
            }
        });
    }
    drop(listener);
    if tokio::time::timeout(SHUTDOWN_GRACE, graceful.shutdown())
        .await
        .is_err()
    {
        warn!("in-flight requests did not finish in time");
    }
}

async fn handle(app: Arc<App>, req: Request<Incoming>) -> Result<Response<Body>, Infallible> {
    let start = Instant::now();
    let request_id = new_request_id();
    let method = req.method().clone();
    let path = req.uri().path().to_owned();
    let host = req
        .headers()
        .get(HOST)
        .and_then(|h| h.to_str().ok())
        .map(str::to_owned);

    let mut resp = if is_health(&req, &app) {
        text(StatusCode::OK, "ok\n")
    } else {
        let mut req = req.map(Body::from);
        if app.lenient {
            lenient::strip_auth(&mut req);
        }
        match verify_xml_body(&mut req).await {
            Err(e) => s3_error(&e),
            Ok(()) => match app.s3.call(req).await {
                Ok(resp) => resp,
                Err(e) => {
                    warn!(error = %e, "internal error");
                    text(StatusCode::INTERNAL_SERVER_ERROR, "internal error\n")
                }
            },
        }
    };

    // s3s drops the status of a HEAD response; a ranged HEAD is still a 206.
    if method == Method::HEAD
        && resp.status() == StatusCode::OK
        && resp.headers().contains_key(CONTENT_RANGE)
    {
        *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
    }
    let status = resp.status();
    if status.is_client_error() || status.is_server_error() {
        let target = target(host.as_deref(), &path, app.vhost_domain.as_deref());
        enrich_error(&mut resp, &request_id, target);
    }
    let headers = resp.headers_mut();
    if let Ok(v) = HeaderValue::from_str(&request_id) {
        headers.insert("x-amz-request-id", v.clone());
        headers.insert("x-amz-id-2", v);
    }
    if !headers.contains_key(DATE)
        && let Ok(v) = HeaderValue::from_str(&httpdate::fmt_http_date(SystemTime::now()))
    {
        headers.insert(DATE, v);
    }

    info!(
        target: "gb::request",
        request_id = %request_id,
        method = %method,
        path = %path,
        host = host.as_deref().unwrap_or(""),
        status = status.as_u16(),
        duration_ms = start.elapsed().as_secs_f64() * 1000.0,
    );
    Ok(resp)
}

/// Subresources whose XML request bodies carry `Content-MD5` or `x-amz-checksum-*`.
/// s3s parses these bodies before the service sees them, so they are checked here.
const CHECKSUMMED_XML: &[&str] = &["delete", "tagging"];

async fn verify_xml_body(req: &mut Request<Body>) -> Result<(), S3Error> {
    let has_digest = |h: &http::HeaderMap| {
        h.contains_key("content-md5") || h.keys().any(|k| k.as_str().starts_with("x-amz-checksum-"))
    };
    let xml = req.uri().query().is_some_and(|q| {
        q.split('&')
            .any(|p| CHECKSUMMED_XML.contains(&p.split('=').next().unwrap_or_default()))
    });
    if !(req.method() == Method::POST || req.method() == Method::PUT)
        || !xml
        || !has_digest(req.headers())
    {
        return Ok(());
    }
    let limit = S3Config::default().xml_max_body_size;
    let body = req.body_mut().store_all_limited(limit).await.map_err(|e| {
        if e.is::<s3s::BodySizeLimitExceeded>() {
            S3Error::with_message(
                S3ErrorCode::MaxMessageLengthExceeded,
                "Your request was too big.",
            )
        } else {
            S3Error::with_message(S3ErrorCode::IncompleteBody, e.to_string())
        }
    })?;
    checksum::verify_body(req.headers(), &body).map_err(service::digest_error)
}

/// An S3 error response built outside s3s; `enrich_error` adds the ids.
fn s3_error(e: &S3Error) -> Response<Body> {
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{}</Code><Message>{}</Message></Error>",
        xml_escape(e.code().as_str()),
        xml_escape(e.message().unwrap_or_default()),
    );
    let mut resp = Response::new(Body::from(xml));
    *resp.status_mut() = e.status_code().unwrap_or(StatusCode::BAD_REQUEST);
    resp.headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    resp
}

fn new_request_id() -> String {
    let id = uuid::Uuid::new_v4()
        .simple()
        .to_string()
        .to_ascii_uppercase();
    id[..16].to_owned()
}

/// `/health` and `/ready` on the bare endpoint (not a virtual-hosted bucket).
fn is_health<B>(req: &Request<B>, app: &App) -> bool {
    let path = req.uri().path();
    (req.method() == Method::GET || req.method() == Method::HEAD)
        && (path == "/health" || path == "/ready")
        && req.uri().query().is_none()
        && vhost_bucket(
            req.headers().get(HOST).and_then(|h| h.to_str().ok()),
            app.vhost_domain.as_deref(),
        )
        .is_none()
}

fn text(status: StatusCode, body: &'static str) -> Response<Body> {
    let mut resp = Response::new(Body::from(Bytes::from_static(body.as_bytes())));
    *resp.status_mut() = status;
    resp.headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
    resp
}

fn vhost_bucket<'a>(host: Option<&'a str>, domain: Option<&str>) -> Option<&'a str> {
    let host = host?;
    let host = host.rsplit_once(':').map_or(
        host,
        |(h, p)| if p.parse::<u16>().is_ok() { h } else { host },
    );
    host.strip_suffix(domain?)?
        .strip_suffix('.')
        .filter(|b| !b.is_empty())
}

/// Bucket and key a request addresses, for error bodies.
fn target(
    host: Option<&str>,
    path: &str,
    domain: Option<&str>,
) -> (Option<String>, Option<String>) {
    let path = percent_decode(path.strip_prefix('/').unwrap_or(path));
    let (bucket, key) = match vhost_bucket(host, domain) {
        Some(b) => (b.to_owned(), path),
        None => match path.split_once('/') {
            Some((b, k)) => (b.to_owned(), k.to_owned()),
            None => (path, String::new()),
        },
    };
    let some = |s: String| (!s.is_empty()).then_some(s);
    (some(bucket), some(key))
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let hex = b
            .get(i + 1..i + 3)
            .and_then(|h| u8::from_str_radix(std::str::from_utf8(h).ok()?, 16).ok());
        match (b[i], hex) {
            (b'%', Some(v)) => {
                out.push(v);
                i += 3;
            }
            (c, _) => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Adds `Key`/`BucketName`, `RequestId` and `HostId` to an S3 error body.
fn enrich_error(
    resp: &mut Response<Body>,
    request_id: &str,
    (bucket, key): (Option<String>, Option<String>),
) {
    let Some(body) = resp.body().bytes() else {
        return;
    };
    let Ok(xml) = std::str::from_utf8(&body) else {
        return;
    };
    let Some(end) = xml.rfind("</Error>") else {
        return;
    };
    let mut extra = String::new();
    if xml.contains("<Code>NoSuchKey</Code>")
        && let Some(k) = key
    {
        extra += &format!("<Key>{}</Key>", xml_escape(&k));
    }
    if (xml.contains("<Code>NoSuchBucket</Code>") || xml.contains("<Code>BucketNotEmpty</Code>"))
        && let Some(b) = bucket
    {
        extra += &format!("<BucketName>{}</BucketName>", xml_escape(&b));
    }
    if !xml.contains("<RequestId>") {
        extra += &format!("<RequestId>{request_id}</RequestId>");
    }
    extra += &format!("<HostId>{request_id}</HostId>");
    let new = format!("{}{extra}{}", &xml[..end], &xml[end..]);
    resp.headers_mut()
        .insert(CONTENT_LENGTH, HeaderValue::from(new.len()));
    *resp.body_mut() = Body::from(new);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vhost_parsing() {
        assert_eq!(
            vhost_bucket(Some("b.localhost:9000"), Some("localhost")),
            Some("b")
        );
        assert_eq!(
            vhost_bucket(Some("b.localhost"), Some("localhost")),
            Some("b")
        );
        assert_eq!(
            vhost_bucket(Some("localhost:9000"), Some("localhost")),
            None
        );
        assert_eq!(vhost_bucket(Some("b.localhost"), None), None);
        assert_eq!(vhost_bucket(Some("xlocalhost"), Some("localhost")), None);
    }

    #[test]
    fn targets() {
        let t = |h, p, d| target(h, p, d);
        assert_eq!(
            t(None, "/b/a%20b/c", None),
            (Some("b".into()), Some("a b/c".into()))
        );
        assert_eq!(t(None, "/b", None), (Some("b".into()), None));
        assert_eq!(
            t(Some("b.lh:1"), "/k", Some("lh")),
            (Some("b".into()), Some("k".into()))
        );
        assert_eq!(t(None, "/", None), (None, None));
    }

    #[test]
    fn error_enrichment() {
        let xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchKey</Code><Message>m</Message></Error>";
        let mut resp = Response::new(Body::from(xml.to_owned()));
        enrich_error(&mut resp, "RID", (Some("b".into()), Some("a&b".into())));
        let body = String::from_utf8(resp.body().bytes().unwrap().to_vec()).unwrap();
        assert!(body.ends_with(
            "<Message>m</Message><Key>a&amp;b</Key><RequestId>RID</RequestId><HostId>RID</HostId></Error>"
        ));
    }
}
