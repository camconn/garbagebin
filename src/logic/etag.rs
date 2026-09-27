//! AWS ETag formulas.

use md5::{Digest, Md5};

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// ETag of a single-PUT object: hex MD5 of the body.
pub fn single(md5: &[u8; 16]) -> String {
    hex(md5)
}

/// ETag of a multipart object: `md5(concat(part_md5s))-N`, from the parts' hex ETags.
pub fn multipart<'a>(part_etags: impl IntoIterator<Item = &'a str>) -> String {
    let mut h = Md5::new();
    let mut n = 0;
    for etag in part_etags {
        h.update(unhex(etag).unwrap_or_default());
        n += 1;
    }
    format!("{}-{n}", hex(&h.finalize()))
}

/// Strips the quotes (and weak prefix) a client may send around an ETag.
pub fn unquote(etag: &str) -> &str {
    let etag = etag.strip_prefix("W/").unwrap_or(etag);
    etag.strip_prefix('"')
        .and_then(|e| e.strip_suffix('"'))
        .unwrap_or(etag)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_body() {
        let md5: [u8; 16] = Md5::digest(b"").into();
        assert_eq!(single(&md5), "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[test]
    fn multipart_formula() {
        let a = hex(&Md5::digest(b"a"));
        let b = hex(&Md5::digest(b"b"));
        let mut concat = Md5::digest(b"a").to_vec();
        concat.extend_from_slice(&Md5::digest(b"b"));
        let want = format!("{}-2", hex(&Md5::digest(&concat)));
        assert_eq!(multipart([a.as_str(), b.as_str()]), want);
    }

    #[test]
    fn unquoting() {
        assert_eq!(unquote("\"abc\""), "abc");
        assert_eq!(unquote("W/\"abc\""), "abc");
        assert_eq!(unquote("abc"), "abc");
    }
}
