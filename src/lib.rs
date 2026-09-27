//! Garbage Bin: a single-node S3-compatible object store. See docs/design.md.

pub mod config;
pub mod healthcheck;
pub mod lenient;
pub mod logic;
pub mod server;
pub mod service;
pub mod storage;

/// JSON logs to stdout. s3s's own logging is off unless `filter` names it,
/// since the request log already records every error response.
pub fn init_logging(filter: &str) {
    use tracing_subscriber::EnvFilter;
    let filter = if filter.contains("s3s") {
        filter.to_owned()
    } else {
        format!("{filter},s3s=off")
    };
    let filter = EnvFilter::try_new(&filter).unwrap_or_else(|_| EnvFilter::new("info,s3s=off"));
    tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(false)
        .with_env_filter(filter)
        .with_writer(std::io::stdout)
        .init();
}
