//! Configuration from flags and environment variables. No config file.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::SystemTime;

use clap::{Arg, CommandFactory, FromArgMatches, Parser, ValueEnum};
use s3s::dto::{Timestamp, TimestampFormat};

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
          num_args = 0..=1, default_missing_value = "true",
          value_parser = clap::builder::BoolishValueParser::new())]
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

    /// Fixed RFC 3339 instant Object Lock retention is judged against, instead of system time.
    #[arg(long, env = "S3_NOW", value_parser = parse_rfc3339)]
    pub now: Option<SystemTime>,
}

fn parse_rfc3339(s: &str) -> Result<SystemTime, String> {
    Timestamp::parse(TimestampFormat::DateTime, s)
        .map(|t| time::OffsetDateTime::from(t).into())
        .map_err(|e| format!("not an RFC 3339 timestamp: {e}"))
}

impl Config {
    /// The command line parser with environment variables ignored.
    fn command_without_env() -> clap::Command {
        Self::command().mut_args(|a: Arg| a.env(None))
    }
}

impl Default for Config {
    /// Built-in defaults; unlike parsing, ignores the environment.
    fn default() -> Self {
        let matches = Self::command_without_env().get_matches_from(["gb"]);
        Self::from_arg_matches(&matches).expect("defaults parse")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        assert!(
            Config::command_without_env()
                .get_arguments()
                .all(|a| a.get_env().is_none())
        );
        let c = Config::default();
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
        assert_eq!(c.now, None);
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
        let c = Config::try_parse_from(["gb", "--now", "2030-01-01T00:00:10+00:00"]).unwrap();
        let secs = |t: SystemTime| t.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        assert_eq!(c.now.map(secs), Some(1_893_456_010));
        assert!(Config::try_parse_from(["gb", "--now", "tomorrow"]).is_err());
    }

    #[test]
    fn boolish_flags() {
        for (v, want) in [
            ("1", true),
            ("0", false),
            ("true", true),
            ("false", false),
            ("yes", true),
            ("no", false),
            ("on", true),
            ("off", false),
        ] {
            let c = Config::try_parse_from(["gb", "--strict-auth", v]).unwrap();
            assert_eq!(c.strict_auth, want, "{v}");
        }
        assert!(Config::try_parse_from(["gb", "--strict-auth", "maybe"]).is_err());
    }
}
