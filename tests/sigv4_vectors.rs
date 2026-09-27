//! AWS SigV4 test suite vectors run through s3s-sigv4's canonicalisation and signing.
//!
//! Vectors in `tests/sigv4/` are copied from botocore's `tests/unit/auth/aws4_testsuite`
//! (Apache-2.0; LICENSE and NOTICE alongside). Each `<name>.req` is parsed into the inputs s3s
//! takes and checked against `<name>.creq` (canonical request), `.sts` (string to sign) and
//! `.authz` (signature), with the suite's credentials, region `us-east-1` and service `service`.
//!
//! Not vendored:
//! - `normalize-path/*`: S3 neither normalises dot segments/slashes nor double-encodes the path,
//!   unlike the generic suite; the vendored vectors have no percent-encoded paths, so both agree.
//! - `get-vanilla-with-session-token`, `post-sts-token/*`: the token is injected by the signer, not
//!   present in the `.req`, so they only test the signer.

use s3s_sigv4::{
    AmzDate, Payload, calculate_signature, create_canonical_request, create_string_to_sign,
};
use sha2::{Digest, Sha256};

const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";
const SERVICE: &str = "service";

/// s3s sorts duplicate query keys by name only (stable), not by value as SigV4 specifies.
/// Harmless for S3, whose query parameters never repeat.
const KNOWN_DIVERGENT: &[&str] = &[
    "get-vanilla-query-order-key",
    "get-vanilla-query-order-value",
];

struct Request {
    method: String,
    path: String,
    query: Vec<(String, String)>,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

fn parse(req: &str) -> Request {
    let (head, body) = req.split_once("\n\n").unwrap_or((req, ""));
    let mut lines = head.lines();
    let mut start = lines.next().unwrap().split(' ');
    let method = start.next().unwrap().to_owned();
    let target = start.next().unwrap();
    let (path, qs) = target.split_once('?').unwrap_or((target, ""));
    // S3 encodes the path once; none of the vendored vectors has an already-encoded path.
    assert!(!path.contains('%'), "percent-encoded path: {path}");
    let query = form_urlencoded::parse(qs.as_bytes()).into_owned().collect();

    let mut headers: Vec<(String, String)> = Vec::new();
    for line in lines {
        if line.starts_with([' ', '\t']) {
            // Folded continuation line.
            let last = headers.last_mut().unwrap();
            last.1.push(' ');
            last.1.push_str(line);
        } else {
            let (name, value) = line.split_once(':').unwrap();
            headers.push((name.to_ascii_lowercase(), value.to_owned()));
        }
    }
    headers.sort_by(|a, b| a.0.cmp(&b.0));

    Request {
        method,
        path: path.to_owned(),
        query,
        headers,
        body: body.as_bytes().to_vec(),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn check(name: &str) -> Result<(), String> {
    let read = |ext: &str| std::fs::read_to_string(format!("tests/sigv4/{name}.{ext}")).unwrap();
    let req = parse(&read("req"));
    let headers: Vec<(&str, &str)> = req
        .headers
        .iter()
        .map(|(n, v)| (n.as_str(), v.as_str()))
        .collect();
    let date = headers.iter().find(|(n, _)| *n == "x-amz-date").unwrap().1;
    let date = AmzDate::parse(date).unwrap();
    let payload_hash = hex(&Sha256::digest(&req.body));

    let creq = create_canonical_request(
        &req.method,
        &req.path,
        &req.query,
        &headers,
        Payload::SingleChunk(&payload_hash),
    );
    if creq != read("creq") {
        return Err(format!(
            "canonical request:\n{creq}\n--- expected:\n{}",
            read("creq")
        ));
    }

    let sts = create_string_to_sign(&creq, &date, REGION, SERVICE);
    if sts != read("sts") {
        return Err(format!(
            "string to sign:\n{sts}\n--- expected:\n{}",
            read("sts")
        ));
    }

    let sig = calculate_signature(&sts, SECRET, &date, REGION, SERVICE);
    let authz = read("authz");
    let want = authz.rsplit("Signature=").next().unwrap().trim();
    if sig != want {
        return Err(format!("signature {sig}, expected {want}"));
    }
    Ok(())
}

#[test]
fn aws_sigv4_test_suite() {
    let mut names: Vec<String> = std::fs::read_dir("tests/sigv4")
        .unwrap()
        .filter_map(|e| {
            e.unwrap()
                .file_name()
                .into_string()
                .unwrap()
                .strip_suffix(".req")
                .map(str::to_owned)
        })
        .collect();
    names.sort();
    assert!(!names.is_empty());

    let mut failures = Vec::new();
    for name in &names {
        let result = check(name);
        let divergent = KNOWN_DIVERGENT.contains(&name.as_str());
        match (result, divergent) {
            (Ok(()), false) | (Err(_), true) => {}
            (Ok(()), true) => failures.push(format!(
                "{name}: passes now; remove it from KNOWN_DIVERGENT"
            )),
            (Err(e), false) => failures.push(format!("{name}: {e}")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}
