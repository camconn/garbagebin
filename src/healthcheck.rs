//! `gb healthcheck`: probes `/health` on the configured listen address. For Docker's
//! `HEALTHCHECK` on a `scratch` image, where there is no curl.

use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream};
use std::time::Duration;

use anyhow::Context as _;

const TIMEOUT: Duration = Duration::from_secs(2);

/// Loopback when the server listens on a wildcard address.
pub fn probe_addr(listen: SocketAddr) -> SocketAddr {
    let ip = match listen.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        ip => ip,
    };
    SocketAddr::new(ip, listen.port())
}

/// `GET /health`; Ok only on a 200.
pub fn check(listen: SocketAddr) -> anyhow::Result<()> {
    let addr = probe_addr(listen);
    let mut stream =
        TcpStream::connect_timeout(&addr, TIMEOUT).with_context(|| format!("connecting {addr}"))?;
    stream.set_read_timeout(Some(TIMEOUT))?;
    stream.set_write_timeout(Some(TIMEOUT))?;
    write!(
        stream,
        "GET /health HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    )?;
    let mut buf = [0u8; 64];
    let mut n = 0;
    while n < buf.len() {
        match stream.read(&mut buf[n..])? {
            0 => break,
            m => n += m,
        }
        if buf[..n].contains(&b'\n') {
            break;
        }
    }
    let status_line = String::from_utf8_lossy(&buf[..n]);
    let status = status_line.split_whitespace().nth(1);
    anyhow::ensure!(
        status == Some("200"),
        "unhealthy: {:?}",
        status_line.lines().next().unwrap_or("")
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn wildcard_maps_to_loopback() {
        let p = |s: &str| probe_addr(s.parse().unwrap()).to_string();
        assert_eq!(p("0.0.0.0:9000"), "127.0.0.1:9000");
        assert_eq!(p("[::]:9000"), "[::1]:9000");
        assert_eq!(p("10.1.2.3:80"), "10.1.2.3:80");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn probes_running_server() {
        let config = Config {
            listen: "127.0.0.1:0".parse().unwrap(),
            ..Default::default()
        };
        let handle = crate::server::start(&config).await.unwrap();
        let addr = handle.local_addr;
        tokio::task::spawn_blocking(move || check(addr))
            .await
            .unwrap()
            .unwrap();
        handle.shutdown().await;
        let err = tokio::task::spawn_blocking(move || check(addr))
            .await
            .unwrap();
        assert!(err.is_err());
    }
}
