//! Configuration from flags and environment variables. No config file.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, ValueEnum};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Backend {
    Memory,
    Fs,
}

#[derive(Clone, Debug, Parser)]
#[command(
    name = "gb",
    version,
    about = "Garbage Bin: a single-node S3-compatible object store"
)]
pub struct Config {
    /// Address to listen on.
    #[arg(long, env = "S3_LISTEN", default_value = "0.0.0.0:9000")]
    pub listen: SocketAddr,

    /// Storage backend.
    #[arg(long, env = "S3_BACKEND", value_enum, default_value = "memory")]
    pub backend: Backend,

    /// Data directory for the fs backend.
    #[arg(long, env = "S3_DATA_DIR", default_value = "/data")]
    pub data_dir: PathBuf,

    /// Access key checked in strict auth mode.
    #[arg(long, env = "S3_ACCESS_KEY", default_value = "test")]
    pub access_key: String,

    /// Secret key checked in strict auth mode.
    #[arg(
        long,
        env = "S3_SECRET_KEY",
        default_value = "test",
        hide_env_values = true
    )]
    pub secret_key: String,

    /// Reject requests whose SigV4 signature does not verify.
    #[arg(long, env = "S3_STRICT_AUTH", default_value_t = false, action = clap::ArgAction::Set,
          num_args = 0..=1, default_missing_value = "true")]
    pub strict_auth: bool,

    /// Region echoed in responses and used for signature verification.
    #[arg(long, env = "S3_REGION", default_value = "us-east-1")]
    pub region: String,

    /// Buckets to create at startup, comma-separated.
    #[arg(long, env = "S3_INIT_BUCKETS", value_delimiter = ',')]
    pub init_buckets: Vec<String>,

    /// Base domain for virtual-host addressing, e.g. `localhost`.
    #[arg(long, env = "S3_VHOST_DOMAIN")]
    pub vhost_domain: Option<String>,

    /// Log filter (tracing `EnvFilter` syntax).
    #[arg(long, env = "S3_LOG", default_value = "info")]
    pub log: String,
}

impl Default for Config {
    fn default() -> Self {
        Self::parse_from(["gb"])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let c = Config::try_parse_from(["gb"]).unwrap();
        assert_eq!(c.listen.to_string(), "0.0.0.0:9000");
        assert_eq!(c.backend, Backend::Memory);
        assert_eq!(c.data_dir, PathBuf::from("/data"));
        assert_eq!(
            (c.access_key.as_str(), c.secret_key.as_str()),
            ("test", "test")
        );
        assert!(!c.strict_auth);
        assert_eq!(c.region, "us-east-1");
        assert!(c.init_buckets.is_empty());
        assert_eq!(c.vhost_domain, None);
        assert_eq!(c.log, "info");
    }

    #[test]
    fn flags() {
        let c = Config::try_parse_from([
            "gb",
            "--backend",
            "fs",
            "--strict-auth",
            "--init-buckets",
            "a,b",
            "--vhost-domain",
            "localhost",
        ])
        .unwrap();
        assert_eq!(c.backend, Backend::Fs);
        assert!(c.strict_auth);
        assert_eq!(c.init_buckets, ["a", "b"]);
        assert_eq!(c.vhost_domain.as_deref(), Some("localhost"));
        let c = Config::try_parse_from(["gb", "--strict-auth", "false"]).unwrap();
        assert!(!c.strict_auth);
    }
}
