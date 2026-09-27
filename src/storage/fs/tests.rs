use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use futures::StreamExt;
use redb::ReadableTable;

use super::sweep::UPLOAD_TTL;
use super::{FsStorage, OBJECTS, REFS, object_key};
use crate::storage::{
    ByteStream, Checksums, NULL_VERSION, ObjectMeta, PartInfo, Storage, UploadInfo,
};

crate::storage_conformance!(super::FsStorage::temp());

fn stream(body: &[u8]) -> ByteStream {
    let chunk = Bytes::copy_from_slice(body);
    Box::pin(futures::stream::once(async move { Ok(chunk) }))
}

async fn put(s: &FsStorage, key: &str, body: &[u8]) {
    let blob = s.write_blob(stream(body)).await.unwrap();
    let meta = ObjectMeta::new(body.len() as u64, format!("{}", body.len()));
    s.put_object("b", key, blob, meta).await.unwrap();
}

async fn get(s: &FsStorage, key: &str) -> Vec<u8> {
    let (meta, reader) = s.open_object("b", key, None).await.unwrap();
    let mut body = reader.read(0..meta.size);
    let mut out = Vec::new();
    while let Some(c) = body.next().await {
        out.extend_from_slice(&c.unwrap());
    }
    out
}

/// Every regular file under `dir`, recursively.
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

fn upload(id: &str, initiated: SystemTime) -> UploadInfo {
    UploadInfo {
        upload_id: id.into(),
        key: "k".into(),
        initiated,
        checksum_algorithm: None,
        checksum_type: None,
        meta: ObjectMeta::new(0, String::new()),
    }
}

fn part(n: u32, body: &[u8], at: SystemTime) -> PartInfo {
    PartInfo {
        part_number: n,
        size: body.len() as u64,
        etag: format!("p{n}"),
        last_modified: at,
        checksums: Checksums::default(),
    }
}

async fn put_part(s: &FsStorage, id: &str, n: u32, body: &[u8], at: SystemTime) {
    let blob = s.write_blob(stream(body)).await.unwrap();
    s.put_part("b", "k", id, part(n, body, at), blob)
        .await
        .unwrap();
}

#[tokio::test]
async fn layout_and_dedup() {
    let dir = tempfile::tempdir().unwrap();
    let blobs = dir.path().join("blobs");
    let s = FsStorage::open(dir.path()).unwrap();
    s.create_bucket("b").await.unwrap();
    put(&s, "a", b"same").await;
    put(&s, "b", b"same").await;
    let only = files(&blobs);
    assert_eq!(only.len(), 1);
    let name = only[0].file_name().unwrap().to_str().unwrap().to_owned();
    assert_eq!(
        only[0],
        blobs.join(&name[..2]).join(&name[2..4]).join(&name)
    );

    // Copy moves no bytes.
    s.copy_object(
        "b",
        "a",
        None,
        "4",
        "b",
        "c",
        ObjectMeta::new(4, "4".into()),
    )
    .await
    .unwrap();
    assert_eq!(files(&blobs).len(), 1);

    // The blob goes once its last reference does.
    put(&s, "a", b"other").await;
    s.delete_version("b", "b", NULL_VERSION).await.unwrap();
    assert_eq!(files(&blobs).len(), 2);
    s.delete_version("b", "c", NULL_VERSION).await.unwrap();
    assert_eq!(files(&blobs).len(), 1);
    assert_eq!(get(&s, "a").await, b"other");

    // Staged blobs that fail to commit leave nothing behind.
    let blob = s.write_blob(stream(b"orphan")).await.unwrap();
    assert!(
        s.put_object("nope", "k", blob, ObjectMeta::new(6, "6".into()))
            .await
            .is_err()
    );
    drop(s.write_blob(stream(b"dropped")).await.unwrap());
    assert_eq!(files(&blobs).len(), 1);
    assert!(files(&dir.path().join("tmp")).is_empty());
    assert_eq!(s.locks.len(), 0);
}

#[tokio::test]
async fn multipart_files() {
    let dir = tempfile::tempdir().unwrap();
    let uploads = dir.path().join("uploads");
    let s = FsStorage::open(dir.path()).unwrap();
    s.create_bucket("b").await.unwrap();
    let now = SystemTime::now();
    s.create_upload("b", upload("u1", now)).await.unwrap();
    put_part(&s, "u1", 1, b"one", now).await;
    put_part(&s, "u1", 1, b"uno", now).await;
    put_part(&s, "u1", 2, b"two", now).await;
    assert_eq!(files(&uploads).len(), 2);
    let parts = [part(1, b"uno", now), part(2, b"two", now)];
    s.complete_upload("b", "k", "u1", &parts, ObjectMeta::new(6, "x-2".into()))
        .await
        .unwrap();
    assert_eq!(get(&s, "k").await, b"unotwo");
    assert!(std::fs::read_dir(&uploads).unwrap().next().is_none());

    s.create_upload("b", upload("u2", now)).await.unwrap();
    put_part(&s, "u2", 1, b"x", now).await;
    s.abort_upload("b", "k", "u2").await.unwrap();
    assert!(std::fs::read_dir(&uploads).unwrap().next().is_none());

    s.create_upload("b", upload("u3", now)).await.unwrap();
    put_part(&s, "u3", 1, b"x", now).await;
    s.delete_version("b", "k", NULL_VERSION).await.unwrap();
    s.delete_bucket("b").await.unwrap();
    assert!(std::fs::read_dir(&uploads).unwrap().next().is_none());
    assert!(files(&dir.path().join("blobs")).is_empty());
}

#[tokio::test]
async fn persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let now = SystemTime::now();
    {
        let s = FsStorage::open(dir.path()).unwrap();
        s.create_bucket("b").await.unwrap();
        put(&s, "k", b"kept").await;
        s.create_upload("b", upload("u", now)).await.unwrap();
        put_part(&s, "u", 1, b"part", now).await;
    }
    let s = FsStorage::open(dir.path()).unwrap();
    assert_eq!(get(&s, "k").await, b"kept");
    let (_, parts) = s.list_parts("b", "k", "u").await.unwrap();
    assert_eq!(parts.len(), 1);
    s.complete_upload("b", "k", "u", &parts, ObjectMeta::new(4, "e".into()))
        .await
        .unwrap();
    assert_eq!(get(&s, "k").await, b"part");
}

#[tokio::test]
async fn sweep_removes_orphans() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let now = SystemTime::now();
    let live_upload;
    {
        let s = FsStorage::open(root).unwrap();
        s.create_bucket("b").await.unwrap();
        put(&s, "k", b"kept").await;
        s.create_upload("b", upload("u", now)).await.unwrap();
        put_part(&s, "u", 1, b"part", now).await;
        live_upload = files(&root.join("uploads"))[0].parent().unwrap().to_owned();
    }
    let orphan_blob = root.join("blobs/zz/zz/zzzz0000");
    std::fs::create_dir_all(orphan_blob.parent().unwrap()).unwrap();
    std::fs::write(&orphan_blob, b"x").unwrap();
    std::fs::write(root.join("tmp/partial"), b"x").unwrap();
    std::fs::create_dir_all(root.join("uploads/gone")).unwrap();
    std::fs::write(root.join("uploads/gone/1-x"), b"x").unwrap();
    std::fs::write(live_upload.join("1-stale"), b"x").unwrap();

    let s = FsStorage::open(root).unwrap();
    assert!(!orphan_blob.exists());
    assert!(!root.join("blobs/zz").exists());
    assert!(files(&root.join("tmp")).is_empty());
    assert!(!root.join("uploads/gone").exists());
    assert_eq!(files(&live_upload).len(), 1);
    assert_eq!(files(&root.join("blobs")).len(), 1);
    assert_eq!(get(&s, "k").await, b"kept");
    assert_eq!(s.list_parts("b", "k", "u").await.unwrap().1.len(), 1);
}

#[tokio::test]
async fn sweep_expires_idle_uploads() {
    let dir = tempfile::tempdir().unwrap();
    let now = SystemTime::now();
    let old = now - UPLOAD_TTL - Duration::from_secs(60);
    {
        let s = FsStorage::open(dir.path()).unwrap();
        s.create_bucket("b").await.unwrap();
        s.create_upload("b", upload("idle", old)).await.unwrap();
        s.create_upload("b", upload("idle-parts", old))
            .await
            .unwrap();
        put_part(&s, "idle-parts", 1, b"x", old).await;
        s.create_upload("b", upload("active", old)).await.unwrap();
        put_part(&s, "active", 1, b"x", now).await;
        s.create_upload("b", upload("fresh", now)).await.unwrap();
    }
    let s = FsStorage::open(dir.path()).unwrap();
    let ids: Vec<_> = s
        .list_uploads("b")
        .await
        .unwrap()
        .into_iter()
        .map(|u| u.upload_id)
        .collect();
    assert_eq!(ids, ["active", "fresh"]);
    assert_eq!(files(&dir.path().join("uploads")).len(), 1);
}

#[tokio::test]
async fn sweep_rebuilds_refcounts() {
    let dir = tempfile::tempdir().unwrap();
    {
        let s = FsStorage::open(dir.path()).unwrap();
        s.create_bucket("b").await.unwrap();
        put(&s, "a", b"same").await;
        put(&s, "b", b"same").await;
        // Corrupt the counts: one too few, plus one for a blob that does not exist.
        let txn = s.inner.db.begin_write().unwrap();
        {
            let mut refs = txn.open_table(REFS).unwrap();
            let shas: Vec<String> = refs
                .iter()
                .unwrap()
                .map(|e| e.unwrap().0.value().to_owned())
                .collect();
            refs.insert(shas[0].as_str(), 1).unwrap();
            refs.insert("f".repeat(64).as_str(), 3).unwrap();
        }
        txn.commit().unwrap();
    }
    let s = FsStorage::open(dir.path()).unwrap();
    s.delete_version("b", "a", NULL_VERSION).await.unwrap();
    assert_eq!(get(&s, "b").await, b"same");
    s.delete_version("b", "b", NULL_VERSION).await.unwrap();
    assert!(files(&dir.path().join("blobs")).is_empty());
}

#[tokio::test]
async fn versions_pin_blobs() {
    let dir = tempfile::tempdir().unwrap();
    let blobs = dir.path().join("blobs");
    {
        let s = FsStorage::open(dir.path()).unwrap();
        s.create_bucket("b").await.unwrap();
        for (id, body) in [("v1", &b"one"[..]), ("v2", b"two"), ("v3", b"one")] {
            let blob = s.write_blob(stream(body)).await.unwrap();
            let mut meta = ObjectMeta::new(3, id.into());
            meta.version_id = id.into();
            s.put_object("b", "k", blob, meta).await.unwrap();
        }
        let dm = ObjectMeta::delete_marker("dm".into());
        s.put_delete_marker("b", "k", dm).await.unwrap();
        assert_eq!(files(&blobs).len(), 2);
    }
    // The sweep counts every version, so reopening keeps both blobs.
    let s = FsStorage::open(dir.path()).unwrap();
    assert_eq!(files(&blobs).len(), 2);
    s.delete_version("b", "k", "v1").await.unwrap();
    assert_eq!(files(&blobs).len(), 2);
    s.delete_version("b", "k", "v3").await.unwrap();
    assert_eq!(files(&blobs).len(), 1);
    s.delete_version("b", "k", "dm").await.unwrap();
    assert_eq!(get(&s, "k").await, b"two");
    s.delete_version("b", "k", "v2").await.unwrap();
    assert!(files(&blobs).is_empty());
    s.delete_bucket("b").await.unwrap();
}

#[tokio::test]
async fn loads_rows_from_before_versioning() {
    let dir = tempfile::tempdir().unwrap();
    {
        let s = FsStorage::open(dir.path()).unwrap();
        s.create_bucket("b").await.unwrap();
        put(&s, "k", b"old").await;
        // Rewrite the row in the pre-versioning format: `{blob, meta}` without version fields.
        let txn = s.inner.db.begin_write().unwrap();
        {
            let mut t = txn.open_table(OBJECTS).unwrap();
            let ok = object_key("b", "k");
            let row: serde_json::Value =
                serde_json::from_slice(t.get(ok.as_slice()).unwrap().unwrap().value()).unwrap();
            let mut v = row[0].clone();
            let meta = v["meta"].as_object_mut().unwrap();
            meta.remove("version_id");
            meta.remove("delete_marker");
            let legacy = serde_json::to_vec(&v).unwrap();
            t.insert(ok.as_slice(), legacy.as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }
    let s = FsStorage::open(dir.path()).unwrap();
    assert_eq!(get(&s, "k").await, b"old");
    let meta = s.head_object("b", "k", None).await.unwrap();
    assert_eq!(meta.version_id, NULL_VERSION);
    let blob = s.write_blob(stream(b"new")).await.unwrap();
    let mut meta = ObjectMeta::new(3, "3".into());
    meta.version_id = "v1".into();
    s.put_object("b", "k", blob, meta).await.unwrap();
    assert_eq!(get(&s, "k").await, b"new");
    s.delete_version("b", "k", "v1").await.unwrap();
    assert_eq!(get(&s, "k").await, b"old");
    s.delete_version("b", "k", NULL_VERSION).await.unwrap();
    assert!(files(&dir.path().join("blobs")).is_empty());
}
