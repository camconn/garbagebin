//! Name and multipart validation, matching AWS.

use std::net::Ipv4Addr;

use crate::logic::etag;
use crate::storage::PartInfo;

pub const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;
pub const MAX_PART_NUMBER: i32 = 10_000;
pub const MAX_KEY_LEN: usize = 1024;

const RESERVED_BUCKET_PREFIXES: &[&str] = &["xn--", "sthree-", "amzn-s3-demo-"];
const RESERVED_BUCKET_SUFFIXES: &[&str] = &["-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"];

/// 3–63 chars of lowercase letters, digits, `.` and `-`; starts and ends
/// alphanumeric; no `..`; not shaped like an IPv4 address; no reserved prefix or suffix.
pub fn bucket_name(name: &str) -> bool {
    let b = name.as_bytes();
    (3..=63).contains(&b.len())
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'.' || *c == b'-')
        && b[0].is_ascii_alphanumeric()
        && b[b.len() - 1].is_ascii_alphanumeric()
        && !name.contains("..")
        && name.parse::<Ipv4Addr>().is_err()
        && !RESERVED_BUCKET_PREFIXES.iter().any(|p| name.starts_with(p))
        && !RESERVED_BUCKET_SUFFIXES.iter().any(|p| name.ends_with(p))
}

/// Non-empty and at most 1024 bytes of UTF-8.
pub fn key(key: &str) -> bool {
    !key.is_empty() && key.len() <= MAX_KEY_LEN
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum CompleteError {
    #[error("no parts given")]
    NoParts,
    #[error("parts not in ascending order")]
    InvalidPartOrder,
    #[error("part {0} not found or ETag mismatch")]
    InvalidPart(i32),
    #[error("part {0} is smaller than the minimum part size")]
    EntityTooSmall(i32),
}

/// Checks a CompleteMultipartUpload request (`(part number, ETag)` pairs as
/// sent) against the stored parts and returns the chosen parts in order.
pub fn complete(
    requested: &[(i32, &str)],
    stored: &[PartInfo],
) -> Result<Vec<PartInfo>, CompleteError> {
    if requested.is_empty() {
        return Err(CompleteError::NoParts);
    }
    if requested.windows(2).any(|w| w[0].0 >= w[1].0) {
        return Err(CompleteError::InvalidPartOrder);
    }
    let mut chosen = Vec::with_capacity(requested.len());
    for &(n, want) in requested {
        let part = u32::try_from(n)
            .ok()
            .and_then(|n| stored.iter().find(|p| p.part_number == n))
            .filter(|p| p.etag == etag::unquote(want))
            .ok_or(CompleteError::InvalidPart(n))?;
        chosen.push(part.clone());
    }
    let last = chosen.len() - 1;
    if let Some(p) = chosen[..last].iter().find(|p| p.size < MIN_PART_SIZE) {
        return Err(CompleteError::EntityTooSmall(p.part_number as i32));
    }
    Ok(chosen)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Checksums;
    use std::time::SystemTime;

    #[test]
    fn bucket_names() {
        for ok in ["abc", "a-b.c", "123", &"a".repeat(63), "my.bucket.1"] {
            assert!(bucket_name(ok), "{ok}");
        }
        let long = "a".repeat(64);
        for bad in [
            "ab",
            &long,
            "Abc",
            "a_b",
            "-ab",
            "ab-",
            ".ab",
            "a..b",
            "192.168.1.1",
            "xn--ab",
            "sthree-ab",
            "ab-s3alias",
            "ab--ol-s3",
            "a b",
        ] {
            assert!(!bucket_name(bad), "{bad}");
        }
    }

    #[test]
    fn keys() {
        assert!(key("a"));
        assert!(key(&"é".repeat(512)));
        assert!(!key(&"é".repeat(513)));
        assert!(!key(""));
    }

    fn part(n: u32, size: u64) -> PartInfo {
        PartInfo {
            part_number: n,
            size,
            etag: format!("e{n}"),
            last_modified: SystemTime::now(),
            checksums: Checksums::default(),
        }
    }

    #[test]
    fn complete_rules() {
        let stored = [
            part(1, MIN_PART_SIZE),
            part(2, MIN_PART_SIZE),
            part(3, 1),
            part(4, 1),
        ];
        let ok = complete(&[(1, "\"e1\""), (2, "e2"), (3, "e3")], &stored).unwrap();
        assert_eq!(
            ok.iter().map(|p| p.part_number).collect::<Vec<_>>(),
            [1, 2, 3]
        );
        // Gaps are fine.
        assert!(complete(&[(1, "e1"), (4, "e4")], &stored).is_ok());
        assert_eq!(complete(&[], &stored), Err(CompleteError::NoParts));
        assert_eq!(
            complete(&[(2, "e2"), (1, "e1")], &stored),
            Err(CompleteError::InvalidPartOrder)
        );
        assert_eq!(
            complete(&[(1, "e1"), (1, "e1")], &stored),
            Err(CompleteError::InvalidPartOrder)
        );
        assert_eq!(
            complete(&[(1, "e1"), (5, "e5")], &stored),
            Err(CompleteError::InvalidPart(5))
        );
        assert_eq!(
            complete(&[(1, "bad")], &stored),
            Err(CompleteError::InvalidPart(1))
        );
        assert_eq!(
            complete(&[(-1, "e1")], &stored),
            Err(CompleteError::InvalidPart(-1))
        );
        assert_eq!(
            complete(&[(3, "e3"), (4, "e4")], &stored),
            Err(CompleteError::EntityTooSmall(3))
        );
        // A single small part is fine.
        assert!(complete(&[(3, "e3")], &stored).is_ok());
    }
}
