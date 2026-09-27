//! The fs backend across real process restarts: persistence, and crash
//! recovery at each step of the write path (via the debug-only `GB_CRASH_AT` hook).

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use aws_sdk_s3::Client;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, VersioningConfiguration,
};

const MIB: usize = 1024 * 1024;

struct Gb {
    child: Child,
    client: Client,
}

impl Gb {
    /// Starts `gb` on `dir` and waits until it answers.
    async fn start(dir: &Path, crash_at: Option<&str>) -> Gb {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let listen = format!("127.0.0.1:{port}");
        let mut cmd = Command::new("sh");
        // No core files from the deliberate aborts.
        cmd.args(["-c", "ulimit -c 0; exec \"$0\" \"$@\""])
            .arg(env!("CARGO_BIN_EXE_gb"))
            .args(["--backend", "fs", "--listen", &listen, "--log", "warn"])
            .arg("--data-dir")
            .arg(dir)
            .env_remove("GB_CRASH_AT")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some(step) = crash_at {
            cmd.env("GB_CRASH_AT", step);
        }
        let mut child = cmd.spawn().unwrap();
        let conf = common::sdk_config(&format!("http://{listen}"), "us-east-1", "test", "test")
            .force_path_style(true)
            .retry_config(RetryConfig::disabled())
            .build();
        let client = Client::from_conf(conf);
        let deadline = Instant::now() + Duration::from_secs(10);
        while client.list_buckets().send().await.is_err() {
            assert!(child.try_wait().unwrap().is_none(), "gb exited on startup");
            assert!(Instant::now() < deadline, "gb did not start");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Gb { child, client }
    }

    /// SIGKILL.
    fn kill(self) {}

    /// SIGTERM, then waits for a clean exit.
    fn terminate(mut self) {
        let pid = self.child.id().to_string();
        let kill = Command::new("kill").args(["-TERM", &pid]).status();
        assert!(kill.unwrap().success());
        assert!(self.child.wait().unwrap().success());
    }

    /// Waits for the crash the last request triggered.
    fn crashed(mut self) {
        let status = self.child.wait().unwrap();
        assert!(!status.success(), "gb should have aborted");
    }

    async fn put(&self, key: &str, body: &[u8]) -> bool {
        self.client
            .put_object()
            .bucket("bkt")
            .key(key)
            .body(ByteStream::from(body.to_vec()))
            .send()
            .await
            .is_ok()
    }

    async fn set_versioning(&self, s: BucketVersioningStatus) {
        let cfg = VersioningConfiguration::builder().status(s).build();
        self.client
            .put_bucket_versioning()
            .bucket("bkt")
            .versioning_configuration(cfg)
            .send()
            .await
            .unwrap();
    }

    /// Version ids of `k`, newest first, marking delete markers with a `dm:` prefix.
    async fn versions(&self) -> Vec<String> {
        let out = self
            .client
            .list_object_versions()
            .bucket("bkt")
            .send()
            .await
            .unwrap();
        let mut all: Vec<(bool, String)> = out
            .versions()
            .iter()
            .map(|v| (v.is_latest().unwrap(), v.version_id().unwrap().to_owned()))
            .chain(out.delete_markers().iter().map(|d| {
                let id = format!("dm:{}", d.version_id().unwrap());
                (d.is_latest().unwrap(), id)
            }))
            .collect();
        all.sort_by_key(|(latest, _)| !latest);
        all.into_iter().map(|(_, id)| id).collect()
    }

    async fn get(&self, key: &str) -> Vec<u8> {
        let out = self
            .client
            .get_object()
            .bucket("bkt")
            .key(key)
            .send()
            .await
            .unwrap();
        out.body.collect().await.unwrap().into_bytes().to_vec()
    }
}

impl Drop for Gb {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(files(&path));
        } else {
            out.push(path);
        }
    }
    out
}

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(data)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// After recovery: nothing staged, and exactly the blob of `body` on disk.
fn assert_clean(dir: &Path, body: &[u8]) {
    assert_blobs(dir, &[body]);
}

/// After recovery: nothing staged, and exactly the blobs of `bodies` on disk.
fn assert_blobs(dir: &Path, bodies: &[&[u8]]) {
    assert!(files(&dir.join("tmp")).is_empty());
    let mut blobs: Vec<String> = files(&dir.join("blobs"))
        .iter()
        .map(|p| p.file_name().unwrap().to_str().unwrap().to_owned())
        .collect();
    let mut want: Vec<String> = bodies.iter().map(|b| sha256_hex(b)).collect();
    blobs.sort();
    want.sort();
    assert_eq!(blobs, want);
}

#[tokio::test]
async fn persists_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let gb = Gb::start(dir.path(), None).await;
    gb.client
        .create_bucket()
        .bucket("bkt")
        .send()
        .await
        .unwrap();
    assert!(gb.put("k", b"kept").await);
    gb.client
        .copy_object()
        .bucket("bkt")
        .key("copy")
        .copy_source("bkt/k")
        .send()
        .await
        .unwrap();
    gb.terminate();

    let gb = Gb::start(dir.path(), None).await;
    assert_eq!(gb.get("k").await, b"kept");
    assert_eq!(gb.get("copy").await, b"kept");
    gb.kill();
    assert_clean(dir.path(), b"kept");
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn crash_during_put() {
    // Steps in write order; the first three happen before the metadata commit.
    let steps = ["staged", "locked", "renamed", "committed", "unlinked"];
    for (i, step) in steps.into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let gb = Gb::start(dir.path(), None).await;
        gb.client
            .create_bucket()
            .bucket("bkt")
            .send()
            .await
            .unwrap();
        assert!(gb.put("k", b"old").await);
        gb.kill();

        let gb = Gb::start(dir.path(), Some(step)).await;
        assert!(!gb.put("k", b"new!").await, "{step}");
        gb.crashed();

        let gb = Gb::start(dir.path(), None).await;
        let want: &[u8] = if i < 3 { b"old" } else { b"new!" };
        assert_eq!(gb.get("k").await, want, "{step}");
        gb.kill();
        assert_clean(dir.path(), want);
    }
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn crash_during_complete() {
    let part1 = vec![1u8; 5 * MIB];
    let part2 = b"tail".to_vec();
    let whole = [part1.as_slice(), &part2].concat();
    let steps = ["concatenated", "renamed", "committed", "unlinked"];
    for (i, step) in steps.into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let gb = Gb::start(dir.path(), None).await;
        gb.client
            .create_bucket()
            .bucket("bkt")
            .send()
            .await
            .unwrap();
        assert!(gb.put("k", b"old").await);
        let id = gb
            .client
            .create_multipart_upload()
            .bucket("bkt")
            .key("k")
            .send()
            .await
            .unwrap()
            .upload_id
            .unwrap();
        let mut parts = Vec::new();
        for (n, body) in [(1, &part1), (2, &part2)] {
            let out = gb
                .client
                .upload_part()
                .bucket("bkt")
                .key("k")
                .upload_id(&id)
                .part_number(n)
                .body(ByteStream::from(body.clone()))
                .send()
                .await
                .unwrap();
            parts.push(
                CompletedPart::builder()
                    .part_number(n)
                    .e_tag(out.e_tag.unwrap())
                    .build(),
            );
        }
        let upload = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        gb.kill();

        let gb = Gb::start(dir.path(), Some(step)).await;
        let complete = gb
            .client
            .complete_multipart_upload()
            .bucket("bkt")
            .key("k")
            .upload_id(&id)
            .multipart_upload(upload.clone())
            .send()
            .await;
        assert!(complete.is_err(), "{step}");
        gb.crashed();

        let gb = Gb::start(dir.path(), None).await;
        let uploads = gb
            .client
            .list_multipart_uploads()
            .bucket("bkt")
            .send()
            .await
            .unwrap()
            .uploads
            .unwrap_or_default();
        if i < 2 {
            // Not committed: the old object and the whole upload survive.
            assert_eq!(gb.get("k").await, b"old", "{step}");
            assert_eq!(uploads.len(), 1, "{step}");
            assert_eq!(files(&dir.path().join("uploads")).len(), 2);
            assert_clean(dir.path(), b"old");
            gb.client
                .complete_multipart_upload()
                .bucket("bkt")
                .key("k")
                .upload_id(&id)
                .multipart_upload(upload)
                .send()
                .await
                .unwrap();
        } else {
            assert!(uploads.is_empty(), "{step}");
        }
        assert_eq!(gb.get("k").await, whole, "{step}");
        gb.kill();
        assert_clean(dir.path(), &whole);
        assert!(files(&dir.path().join("uploads")).is_empty());
    }
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn crash_during_versioned_put() {
    // The old version pins its blob, so nothing is unlinked.
    let steps = ["staged", "locked", "renamed", "committed"];
    for (i, step) in steps.into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let gb = Gb::start(dir.path(), None).await;
        gb.client
            .create_bucket()
            .bucket("bkt")
            .send()
            .await
            .unwrap();
        gb.set_versioning(BucketVersioningStatus::Enabled).await;
        assert!(gb.put("k", b"old").await);
        let old = gb.versions().await;
        gb.kill();

        let gb = Gb::start(dir.path(), Some(step)).await;
        assert!(!gb.put("k", b"new!").await, "{step}");
        gb.crashed();

        let gb = Gb::start(dir.path(), None).await;
        let versions = gb.versions().await;
        if i < 3 {
            assert_eq!(versions, old, "{step}");
            assert_eq!(gb.get("k").await, b"old", "{step}");
            gb.kill();
            assert_blobs(dir.path(), &[b"old"]);
        } else {
            assert_eq!(versions.len(), 2, "{step}");
            assert_eq!(versions[1], old[0], "{step}");
            assert_eq!(gb.get("k").await, b"new!", "{step}");
            gb.kill();
            assert_blobs(dir.path(), &[b"old", b"new!"]);
        }
    }
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn crash_during_delete_marker() {
    // Enabled adds a marker; Suspended replaces the null version with one, unlinking its blob.
    let cases = [
        (
            BucketVersioningStatus::Enabled,
            &["locked", "committed"][..],
        ),
        (
            BucketVersioningStatus::Suspended,
            &["locked", "committed", "unlinked"],
        ),
    ];
    for (mode, steps) in cases {
        for (i, step) in steps.iter().enumerate() {
            let dir = tempfile::tempdir().unwrap();
            let gb = Gb::start(dir.path(), None).await;
            gb.client
                .create_bucket()
                .bucket("bkt")
                .send()
                .await
                .unwrap();
            assert!(gb.put("k", b"old").await);
            gb.set_versioning(mode.clone()).await;
            gb.kill();

            let gb = Gb::start(dir.path(), Some(step)).await;
            let del = gb
                .client
                .delete_object()
                .bucket("bkt")
                .key("k")
                .send()
                .await;
            assert!(del.is_err(), "{mode:?} {step}");
            gb.crashed();

            let gb = Gb::start(dir.path(), None).await;
            let versions = gb.versions().await;
            if i == 0 {
                assert_eq!(versions, ["null"], "{mode:?} {step}");
                assert_eq!(gb.get("k").await, b"old");
                gb.kill();
                assert_blobs(dir.path(), &[b"old"]);
                continue;
            }
            let get = gb.client.get_object().bucket("bkt").key("k").send().await;
            assert!(get.is_err(), "{mode:?} {step}");
            if mode == BucketVersioningStatus::Enabled {
                assert_eq!(versions.len(), 2, "{step}");
                assert!(versions[0].starts_with("dm:"), "{step}");
                assert_eq!(versions[1], "null", "{step}");
                gb.kill();
                assert_blobs(dir.path(), &[b"old"]);
            } else {
                assert_eq!(versions, ["dm:null"], "{step}");
                gb.kill();
                assert_blobs(dir.path(), &[]);
            }
        }
    }
}
