//! Filesystem backend: content-addressed blobs plus redb metadata.
//! See "Storage layout and write path" in docs/design.md.
//!
//! Every metadata write runs in one redb transaction under `Inner::commit`,
//! together with the rename that installs its bytes, so refcounts and blob
//! files cannot race. Writers to the same key also serialise on a per-key lock.

mod locks;
mod sweep;

use std::fs::File;
use std::io::{self, Read, SeekFrom, Write};
use std::ops::{Bound, Range};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::Context as _;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use redb::{
    Database, ReadTransaction, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use super::{
    BlobReader, BucketInfo, BucketUpdate, ByteStream, MetaUpdate, ObjectMeta, PartInfo, Result,
    Storage, StorageError, Stored, UploadInfo, versions,
};
use locks::KeyLocks;

/// Bucket name → `BucketInfo`.
const BUCKETS: TableDefinition<&str, &[u8]> = TableDefinition::new("buckets");
/// `bucket\0key` → the key's versions, newest first (`Vec<Version>`; `LegacyRow` before versioning).
const OBJECTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("objects");
/// `bucket\0upload_id` → `UploadRow`.
const UPLOADS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("uploads");
/// `bucket\0upload_id\0<part number, big-endian u32>` → `PartRow`.
const PARTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("parts");
/// Blob sha256 → number of versions pointing at it. Absent means zero.
const REFS: TableDefinition<&str, u64> = TableDefinition::new("refs");

const BLOBS_DIR: &str = "blobs";
const TMP_DIR: &str = "tmp";
const UPLOADS_DIR: &str = "uploads";
const CHUNK: usize = 64 * 1024;

/// Test-only fault injection: debug builds abort at the named step.
const CRASH_ENV: &str = "GB_CRASH_AT";

pub struct FsStorage {
    inner: Arc<Inner>,
    locks: KeyLocks,
    #[cfg(test)]
    _dir: Option<tempfile::TempDir>,
}

struct Inner {
    root: PathBuf,
    db: Database,
    /// Held across blob install, metadata commit and blob unlink.
    commit: Mutex<()>,
    crash_at: Option<String>,
}

/// A version and the sha256 of its blob.
type Version = Stored<String>;

/// Object row written before versioning: a key's single (null) version.
#[derive(Deserialize)]
struct LegacyRow {
    blob: String,
    meta: ObjectMeta,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum KeyRow {
    Versions(Vec<Version>),
    Legacy(Box<LegacyRow>),
}

fn decode_versions(v: &[u8]) -> Result<Vec<Version>> {
    Ok(match decode(v)? {
        KeyRow::Versions(list) => list,
        KeyRow::Legacy(r) => vec![Stored {
            meta: r.meta,
            blob: Some(r.blob),
        }],
    })
}

#[derive(Serialize, Deserialize)]
struct UploadRow {
    /// Directory under `uploads/`; our own id, so client input never names a path.
    dir: String,
    info: UploadInfo,
}

#[derive(Serialize, Deserialize)]
struct PartRow {
    /// File name within the upload directory.
    file: String,
    info: PartInfo,
}

/// A body staged in `tmp/`, not yet visible. Dropping it deletes the file.
pub struct FsBlob {
    tmp: PathBuf,
    sha256: String,
    armed: bool,
}

impl Drop for FsBlob {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_file(&self.tmp);
        }
    }
}

macro_rules! internal_from {
    ($($t:ty),*) => {$(
        impl From<$t> for StorageError {
            fn from(e: $t) -> Self {
                StorageError::Internal(format!("metadata: {e}"))
            }
        }
    )*};
}

internal_from!(
    redb::Error,
    redb::DatabaseError,
    redb::TransactionError,
    redb::TableError,
    redb::StorageError,
    redb::CommitError,
    serde_json::Error
);

fn object_key(bucket: &str, key: &str) -> Vec<u8> {
    [bucket.as_bytes(), key.as_bytes()].join(&0)
}

fn upload_key(bucket: &str, upload_id: &str) -> Vec<u8> {
    [bucket.as_bytes(), upload_id.as_bytes()].join(&0)
}

fn part_key(bucket: &str, upload_id: &str, n: u32) -> Vec<u8> {
    [bucket.as_bytes(), upload_id.as_bytes(), &n.to_be_bytes()].join(&0)
}

/// Prefix shared by every row of `bucket`, or of the parts of one upload.
fn child_prefix(parent: &[u8]) -> Vec<u8> {
    [parent, &[0]].concat()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn decode<'a, T: Deserialize<'a>>(v: &'a [u8]) -> Result<T> {
    Ok(serde_json::from_slice(v)?)
}

fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(v)?)
}

trait BytesTable: ReadableTable<&'static [u8], &'static [u8]> {}
impl<T: ReadableTable<&'static [u8], &'static [u8]>> BytesTable for T {}

/// All rows whose key starts with `prefix`, in key order.
fn scan(t: &impl BytesTable, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut out = Vec::new();
    for entry in t.range::<&[u8]>(prefix..)? {
        let (k, v) = entry?;
        if !k.value().starts_with(prefix) {
            break;
        }
        out.push((k.value().to_vec(), v.value().to_vec()));
    }
    Ok(out)
}

/// Whether any row starts with `prefix`.
fn any_with_prefix(t: &impl BytesTable, prefix: &[u8]) -> Result<bool> {
    match t.range::<&[u8]>(prefix..)?.next() {
        Some(entry) => Ok(entry?.0.value().starts_with(prefix)),
        None => Ok(false),
    }
}

fn get<T: for<'a> Deserialize<'a>>(t: &impl BytesTable, key: &[u8]) -> Result<Option<T>> {
    t.get(key)?.map(|v| decode(v.value())).transpose()
}

fn bucket_info(
    t: &impl ReadableTable<&'static str, &'static [u8]>,
    bucket: &str,
) -> Result<BucketInfo> {
    match t.get(bucket)? {
        Some(v) => decode(v.value()),
        None => Err(StorageError::NoSuchBucket),
    }
}

/// The versions of one key, from a read snapshot.
fn read_versions(txn: &ReadTransaction, bucket: &str, key: &[u8]) -> Result<Vec<Version>> {
    bucket_info(&txn.open_table(BUCKETS)?, bucket)?;
    let t = txn.open_table(OBJECTS)?;
    let v = t.get(key)?;
    v.map_or(Ok(Vec::new()), |v| decode_versions(v.value()))
}

fn upload_row(t: &impl BytesTable, bucket: &str, key: &str, upload_id: &str) -> Result<UploadRow> {
    get::<UploadRow>(t, &upload_key(bucket, upload_id))?
        .filter(|u| u.info.key == key)
        .ok_or(StorageError::NoSuchUpload)
}

fn remove_path(path: &Path) {
    let res = if path.is_dir() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    };
    if let Err(e) = res
        && e.kind() != io::ErrorKind::NotFound
    {
        tracing::warn!(path = %path.display(), error = %e, "removing file");
    }
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

impl Inner {
    fn blob_path(&self, sha: &str) -> PathBuf {
        self.root
            .join(BLOBS_DIR)
            .join(&sha[..2])
            .join(&sha[2..4])
            .join(sha)
    }

    fn tmp_path(&self) -> PathBuf {
        self.root
            .join(TMP_DIR)
            .join(uuid::Uuid::new_v4().simple().to_string())
    }

    fn upload_dir(&self, dir: &str) -> PathBuf {
        self.root.join(UPLOADS_DIR).join(dir)
    }

    fn crash(&self, step: &str) {
        if self.crash_at.as_deref() == Some(step) {
            eprintln!("{CRASH_ENV}={step}: aborting");
            std::process::abort();
        }
    }

    fn read<T>(&self, f: impl FnOnce(&ReadTransaction) -> Result<T>) -> Result<T> {
        f(&self.db.begin_read()?)
    }

    /// Runs `f` in one write transaction and commits it. Afterwards deletes blobs
    /// whose refcount reached zero and paths queued in `Tx::remove`; on failure,
    /// undoes whatever `f` installed.
    fn write<T>(&self, f: impl FnOnce(&mut Tx) -> Result<T>) -> Result<T> {
        let _commit = self.commit.lock().unwrap_or_else(|e| e.into_inner());
        let txn = self.db.begin_write()?;
        let mut tx = Tx {
            inner: self,
            txn: &txn,
            dead: Vec::new(),
            remove: Vec::new(),
            installed: None,
        };
        let res = f(&mut tx).and_then(|v| {
            tx.keep_live_blobs()?;
            Ok(v)
        });
        let Tx {
            dead,
            remove,
            installed,
            ..
        } = tx;
        let res = res.and_then(|v| {
            txn.commit()?;
            Ok(v)
        });
        match res {
            Ok(v) => {
                self.crash("committed");
                for sha in &dead {
                    remove_path(&self.blob_path(sha));
                }
                if !dead.is_empty() {
                    self.crash("unlinked");
                }
                for path in &remove {
                    remove_path(path);
                }
                Ok(v)
            }
            Err(e) => {
                match installed {
                    Some(Installed::Blob(sha)) => {
                        let refs = self.db.begin_read()?.open_table(REFS)?;
                        if refs.get(sha.as_str())?.is_none() {
                            remove_path(&self.blob_path(&sha));
                        }
                    }
                    Some(Installed::File(path)) => remove_path(&path),
                    None => {}
                }
                Err(e)
            }
        }
    }

    /// Streams `files` into one staged blob.
    fn concat(&self, files: &[PathBuf]) -> Result<FsBlob> {
        let mut blob = FsBlob {
            tmp: self.tmp_path(),
            sha256: String::new(),
            armed: true,
        };
        let mut out = File::create(&blob.tmp).map_err(StorageError::Io)?;
        let mut hash = Sha256::new();
        let mut buf = vec![0; CHUNK];
        for path in files {
            let mut src = File::open(path).map_err(StorageError::Io)?;
            loop {
                let n = src.read(&mut buf).map_err(StorageError::Io)?;
                if n == 0 {
                    break;
                }
                hash.update(&buf[..n]);
                out.write_all(&buf[..n]).map_err(StorageError::Io)?;
            }
        }
        out.sync_all().map_err(StorageError::Io)?;
        blob.sha256 = hex(&hash.finalize());
        Ok(blob)
    }
}

enum Installed {
    Blob(String),
    File(PathBuf),
}

/// One metadata write in progress.
struct Tx<'a> {
    inner: &'a Inner,
    txn: &'a WriteTransaction,
    /// Blobs whose refcount may have reached zero.
    dead: Vec<String>,
    /// Deleted once the transaction commits.
    remove: Vec<PathBuf>,
    /// Undone if the transaction does not commit.
    installed: Option<Installed>,
}

impl Tx<'_> {
    fn require_bucket(&self, bucket: &str) -> Result<()> {
        bucket_info(&self.txn.open_table(BUCKETS)?, bucket).map(|_| ())
    }

    fn incref(&mut self, sha: &str) -> Result<()> {
        let mut refs = self.txn.open_table(REFS)?;
        let n = refs.get(sha)?.map_or(0, |v| v.value());
        refs.insert(sha, n + 1)?;
        Ok(())
    }

    fn decref(&mut self, sha: &str) -> Result<()> {
        let mut refs = self.txn.open_table(REFS)?;
        let n = refs.get(sha)?.map_or(0, |v| v.value());
        if n > 1 {
            refs.insert(sha, n - 1)?;
        } else {
            refs.remove(sha)?;
            self.dead.push(sha.to_owned());
        }
        Ok(())
    }

    /// Drops from `dead` any blob that was referenced again later in the transaction.
    fn keep_live_blobs(&mut self) -> Result<()> {
        let refs = self.txn.open_table(REFS)?;
        let mut dead = std::mem::take(&mut self.dead);
        dead.sort();
        dead.dedup();
        for sha in dead {
            if refs.get(sha.as_str())?.is_none() {
                self.dead.push(sha);
            }
        }
        Ok(())
    }

    /// Renames the staged blob into `blobs/`. Same content already there is
    /// replaced by identical bytes, which open readers never notice.
    fn install_blob(&mut self, blob: &mut FsBlob) -> Result<()> {
        let dst = self.inner.blob_path(&blob.sha256);
        self.install(blob, &dst)?;
        self.installed = Some(Installed::Blob(blob.sha256.clone()));
        self.inner.crash("renamed");
        Ok(())
    }

    fn install_file(&mut self, blob: &mut FsBlob, dst: PathBuf) -> Result<()> {
        self.install(blob, &dst)?;
        self.installed = Some(Installed::File(dst));
        Ok(())
    }

    fn install(&self, blob: &mut FsBlob, dst: &Path) -> Result<()> {
        let dir = dst.parent().expect("blob path has a parent");
        std::fs::create_dir_all(dir).map_err(StorageError::Io)?;
        std::fs::rename(&blob.tmp, dst).map_err(StorageError::Io)?;
        blob.armed = false;
        sync_dir(dir).map_err(StorageError::Io)
    }

    fn versions(&self, key: &[u8]) -> Result<Vec<Version>> {
        let t = self.txn.open_table(OBJECTS)?;
        let v = t.get(key)?;
        v.map_or(Ok(Vec::new()), |v| decode_versions(v.value()))
    }

    fn save_versions(&self, key: &[u8], list: &[Version]) -> Result<()> {
        let mut t = self.txn.open_table(OBJECTS)?;
        if list.is_empty() {
            t.remove(key)?;
        } else {
            t.insert(key, encode(&list)?.as_slice())?;
        }
        Ok(())
    }

    /// Makes `v` current, moving blob references for it and any null version it replaces.
    fn push_version(&mut self, key: &[u8], v: Version) -> Result<()> {
        let mut list = self.versions(key)?;
        if let Some(sha) = &v.blob {
            self.incref(sha)?;
        }
        if let Some(sha) = versions::push(&mut list, v).and_then(|old| old.blob) {
            self.decref(&sha)?;
        }
        self.save_versions(key, &list)
    }

    fn upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<UploadRow> {
        upload_row(&self.txn.open_table(UPLOADS)?, bucket, key, upload_id)
    }

    /// Removes the upload's rows and queues its directory for deletion.
    fn drop_upload(&mut self, bucket: &str, upload_id: &str, dir: &str) -> Result<()> {
        let uk = upload_key(bucket, upload_id);
        self.txn.open_table(UPLOADS)?.remove(uk.as_slice())?;
        let mut parts = self.txn.open_table(PARTS)?;
        for (k, _) in scan(&parts, &child_prefix(&uk))? {
            parts.remove(k.as_slice())?;
        }
        self.remove.push(self.inner.upload_dir(dir));
        Ok(())
    }
}

impl FsStorage {
    /// Opens (creating if needed) the data directory and runs the startup sweep.
    pub fn open(data_dir: &Path) -> anyhow::Result<Self> {
        for d in [BLOBS_DIR, TMP_DIR, UPLOADS_DIR] {
            let path = data_dir.join(d);
            std::fs::create_dir_all(&path)
                .with_context(|| format!("creating {}", path.display()))?;
        }
        let meta = data_dir.join("meta.redb");
        let db = Database::create(&meta).with_context(|| format!("opening {}", meta.display()))?;
        let crash_at = if cfg!(debug_assertions) {
            std::env::var(CRASH_ENV).ok()
        } else {
            None
        };
        let inner = Inner {
            root: data_dir.to_owned(),
            db,
            commit: Mutex::default(),
            crash_at,
        };
        sweep::run(&inner).context("startup sweep")?;
        Ok(Self {
            inner: Arc::new(inner),
            locks: KeyLocks::default(),
            #[cfg(test)]
            _dir: None,
        })
    }

    /// A backend in a fresh temporary directory, deleted on drop.
    #[cfg(test)]
    pub(crate) fn temp() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Self::open(dir.path()).unwrap();
        s._dir = Some(dir);
        s
    }

    /// Runs `f` on the blocking pool; redb and std::fs calls block.
    async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Inner) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || f(&inner))
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
    }

    /// Up to `limit` keys under `prefix` after `after`, each mapped by `f`; `None` skips a key.
    async fn scan_keys<T: Send + 'static>(
        &self,
        bucket: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
        f: impl Fn(Vec<Version>) -> Option<T> + Send + 'static,
    ) -> Result<Vec<(String, T)>> {
        let bucket = bucket.to_owned();
        let full_prefix = object_key(&bucket, prefix);
        let start = match after {
            Some(a) if a >= prefix => Bound::Excluded(object_key(&bucket, a)),
            _ => Bound::Included(full_prefix.clone()),
        };
        self.run(move |i| {
            i.read(|txn| {
                bucket_info(&txn.open_table(BUCKETS)?, &bucket)?;
                let t = txn.open_table(OBJECTS)?;
                let skip = bucket.len() + 1;
                let mut out = Vec::new();
                let range = (start.as_ref().map(Vec::as_slice), Bound::Unbounded);
                for entry in t.range::<&[u8]>(range)? {
                    if out.len() >= limit {
                        break;
                    }
                    let (k, v) = entry?;
                    let k = k.value();
                    if !k.starts_with(&full_prefix) {
                        break;
                    }
                    let Some(item) = f(decode_versions(v.value())?) else {
                        continue;
                    };
                    let key = String::from_utf8(k[skip..].to_vec())
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    out.push((key, item));
                }
                Ok(out)
            })
        })
        .await
    }

    async fn lock(&self, bucket: &str, key: &str) -> locks::KeyGuard<'_> {
        let guard = self.locks.lock(object_key(bucket, key)).await;
        self.inner.crash("locked");
        guard
    }
}

struct FsReader(File);

impl BlobReader for FsReader {
    fn read(self: Box<Self>, range: Range<u64>) -> ByteStream {
        let file = tokio::fs::File::from_std(self.0);
        let state = (file, range.start, range.end, false);
        Box::pin(futures::stream::try_unfold(
            state,
            |(mut file, pos, end, seeked)| async move {
                if pos >= end {
                    return Ok(None);
                }
                if !seeked {
                    file.seek(SeekFrom::Start(pos)).await?;
                }
                let mut buf = vec![0; (end - pos).min(CHUNK as u64) as usize];
                let n = file.read(&mut buf).await?;
                if n == 0 {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
                buf.truncate(n);
                Ok(Some((Bytes::from(buf), (file, pos + n as u64, end, true))))
            },
        ))
    }
}

#[async_trait]
impl Storage for FsStorage {
    type Blob = FsBlob;

    async fn create_bucket(&self, bucket: &str) -> Result<()> {
        let bucket = bucket.to_owned();
        self.run(move |i| {
            i.write(|tx| {
                let mut t = tx.txn.open_table(BUCKETS)?;
                if t.get(bucket.as_str())?.is_some() {
                    return Err(StorageError::BucketAlreadyOwnedByYou);
                }
                let info = BucketInfo::new(&bucket);
                t.insert(bucket.as_str(), encode(&info)?.as_slice())?;
                Ok(())
            })
        })
        .await
    }

    async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        let bucket = bucket.to_owned();
        self.run(move |i| {
            i.write(|tx| {
                tx.require_bucket(&bucket)?;
                let prefix = child_prefix(bucket.as_bytes());
                if any_with_prefix(&tx.txn.open_table(OBJECTS)?, &prefix)? {
                    return Err(StorageError::BucketNotEmpty);
                }
                tx.txn.open_table(BUCKETS)?.remove(bucket.as_str())?;
                let uploads = scan(&tx.txn.open_table(UPLOADS)?, &prefix)?;
                for (_, v) in uploads {
                    let row: UploadRow = decode(&v)?;
                    tx.drop_upload(&bucket, &row.info.upload_id, &row.dir)?;
                }
                Ok(())
            })
        })
        .await
    }

    async fn head_bucket(&self, bucket: &str) -> Result<BucketInfo> {
        let bucket = bucket.to_owned();
        self.run(move |i| i.read(|txn| bucket_info(&txn.open_table(BUCKETS)?, &bucket)))
            .await
    }

    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        self.run(|i| {
            i.read(|txn| {
                let t = txn.open_table(BUCKETS)?;
                let mut out = Vec::new();
                for entry in t.iter()? {
                    out.push(decode(entry?.1.value())?);
                }
                Ok(out)
            })
        })
        .await
    }

    async fn update_bucket(&self, bucket: &str, f: BucketUpdate) -> Result<()> {
        let bucket = bucket.to_owned();
        self.run(move |i| {
            i.write(|tx| {
                let mut t = tx.txn.open_table(BUCKETS)?;
                let mut info = bucket_info(&t, &bucket)?;
                f(&mut info)?;
                t.insert(bucket.as_str(), encode(&info)?.as_slice())?;
                Ok(())
            })
        })
        .await
    }

    async fn write_blob(&self, mut body: ByteStream) -> Result<FsBlob> {
        let mut blob = FsBlob {
            tmp: self.inner.tmp_path(),
            sha256: String::new(),
            armed: true,
        };
        let mut file = tokio::fs::File::create(&blob.tmp)
            .await
            .map_err(StorageError::Io)?;
        let mut hash = Sha256::new();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(StorageError::Body)?;
            hash.update(&chunk);
            file.write_all(&chunk).await.map_err(StorageError::Io)?;
        }
        file.flush().await.map_err(StorageError::Io)?;
        file.sync_all().await.map_err(StorageError::Io)?;
        blob.sha256 = hex(&hash.finalize());
        self.inner.crash("staged");
        Ok(blob)
    }

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        mut blob: FsBlob,
        meta: ObjectMeta,
    ) -> Result<()> {
        let _lock = self.lock(bucket, key).await;
        let bucket = bucket.to_owned();
        let ok = object_key(&bucket, key);
        self.run(move |i| {
            i.write(|tx| {
                tx.require_bucket(&bucket)?;
                tx.install_blob(&mut blob)?;
                let blob = Some(blob.sha256.clone());
                tx.push_version(&ok, Stored { meta, blob })
            })
        })
        .await
    }

    async fn put_delete_marker(&self, bucket: &str, key: &str, meta: ObjectMeta) -> Result<()> {
        let _lock = self.lock(bucket, key).await;
        let (bucket, ok) = (bucket.to_owned(), object_key(bucket, key));
        self.run(move |i| {
            i.write(|tx| {
                tx.require_bucket(&bucket)?;
                let blob = None;
                tx.push_version(&ok, Stored { meta, blob })
            })
        })
        .await
    }

    async fn head_object(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&str>,
    ) -> Result<ObjectMeta> {
        let (bucket, ok) = (bucket.to_owned(), object_key(bucket, key));
        let version = version.map(str::to_owned);
        self.run(move |i| {
            i.read(|txn| {
                let list = read_versions(txn, &bucket, &ok)?;
                Ok(versions::find(&list, version.as_deref())?.meta.clone())
            })
        })
        .await
    }

    async fn open_object(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&str>,
    ) -> Result<(ObjectMeta, Box<dyn BlobReader>)> {
        let (bucket, ok) = (bucket.to_owned(), object_key(bucket, key));
        let version = version.map(str::to_owned);
        self.run(move |i| {
            // An overwrite can unlink the blob between reading the row and
            // opening the file; the next snapshot has the new row.
            for _ in 0..3 {
                let v = i.read(|txn| {
                    let list = read_versions(txn, &bucket, &ok)?;
                    Ok(versions::find(&list, version.as_deref())?.clone())
                })?;
                let sha = v.blob.as_deref().expect("a live version has a blob");
                match File::open(i.blob_path(sha)) {
                    Ok(f) => return Ok((v.meta, Box::new(FsReader(f)) as Box<dyn BlobReader>)),
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(StorageError::Io(e)),
                }
            }
            Err(StorageError::Internal("blob file missing".into()))
        })
        .await
    }

    async fn delete_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<ObjectMeta>> {
        let _lock = self.lock(bucket, key).await;
        let (bucket, ok) = (bucket.to_owned(), object_key(bucket, key));
        let version_id = version_id.to_owned();
        self.run(move |i| {
            i.write(|tx| {
                tx.require_bucket(&bucket)?;
                let mut list = tx.versions(&ok)?;
                let Some(old) = versions::remove(&mut list, &version_id) else {
                    return Ok(None);
                };
                if let Some(sha) = &old.blob {
                    tx.decref(sha)?;
                }
                tx.save_versions(&ok, &list)?;
                Ok(Some(old.meta))
            })
        })
        .await
    }

    async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        src_version: Option<&str>,
        src_etag: &str,
        dst_bucket: &str,
        dst_key: &str,
        meta: ObjectMeta,
    ) -> Result<()> {
        let _lock = self.lock(dst_bucket, dst_key).await;
        let (src_bucket, src) = (src_bucket.to_owned(), object_key(src_bucket, src_key));
        let (dst_bucket, dst) = (dst_bucket.to_owned(), object_key(dst_bucket, dst_key));
        let (src_version, src_etag) = (src_version.map(str::to_owned), src_etag.to_owned());
        self.run(move |i| {
            i.write(|tx| {
                tx.require_bucket(&src_bucket)?;
                let list = tx.versions(&src)?;
                let v = versions::find(&list, src_version.as_deref())?;
                if v.meta.etag != src_etag {
                    return Err(StorageError::PreconditionFailed);
                }
                tx.require_bucket(&dst_bucket)?;
                let blob = v.blob.clone();
                tx.push_version(&dst, Stored { meta, blob })
            })
        })
        .await
    }

    async fn update_object(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        f: MetaUpdate,
    ) -> Result<ObjectMeta> {
        let _lock = self.lock(bucket, key).await;
        let (bucket, ok) = (bucket.to_owned(), object_key(bucket, key));
        let version = version.map(str::to_owned);
        self.run(move |i| {
            i.write(|tx| {
                tx.require_bucket(&bucket)?;
                let mut list = tx.versions(&ok)?;
                let v = versions::find_mut(&mut list, version.as_deref())?;
                f(&mut v.meta)?;
                let meta = v.meta.clone();
                tx.save_versions(&ok, &list)?;
                Ok(meta)
            })
        })
        .await
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, ObjectMeta)>> {
        self.scan_keys(bucket, prefix, after, limit, |list| {
            versions::live(&list).map(|v| v.meta.clone())
        })
        .await
    }

    async fn list_versions(
        &self,
        bucket: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, Vec<ObjectMeta>)>> {
        self.scan_keys(bucket, prefix, after, limit, |list| {
            Some(list.into_iter().map(|v| v.meta).collect())
        })
        .await
    }

    async fn create_upload(&self, bucket: &str, upload: UploadInfo) -> Result<()> {
        let bucket = bucket.to_owned();
        self.run(move |i| {
            i.write(|tx| {
                tx.require_bucket(&bucket)?;
                let uk = upload_key(&bucket, &upload.upload_id);
                let row = UploadRow {
                    dir: uuid::Uuid::new_v4().simple().to_string(),
                    info: upload,
                };
                tx.txn
                    .open_table(UPLOADS)?
                    .insert(uk.as_slice(), encode(&row)?.as_slice())?;
                Ok(())
            })
        })
        .await
    }

    async fn put_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part: PartInfo,
        mut blob: FsBlob,
    ) -> Result<()> {
        let _lock = self.lock(bucket, key).await;
        let (bucket, key, upload_id) = (bucket.to_owned(), key.to_owned(), upload_id.to_owned());
        self.run(move |i| {
            i.write(|tx| {
                tx.require_bucket(&bucket)?;
                let up = tx.upload(&bucket, &key, &upload_id)?;
                let dir = i.upload_dir(&up.dir);
                // Unique per write, so a replaced part never overwrites the file its row names.
                let staged = blob.tmp.file_name().expect("tmp file name");
                let file = format!("{}-{}", part.part_number, staged.to_string_lossy());
                tx.install_file(&mut blob, dir.join(&file))?;
                let pk = part_key(&bucket, &upload_id, part.part_number);
                let row = PartRow { file, info: part };
                let old = {
                    let mut t = tx.txn.open_table(PARTS)?;
                    let old = t.insert(pk.as_slice(), encode(&row)?.as_slice())?;
                    old.map(|v| decode::<PartRow>(v.value())).transpose()?
                };
                if let Some(old) = old {
                    tx.remove.push(dir.join(old.file));
                }
                Ok(())
            })
        })
        .await
    }

    async fn list_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(UploadInfo, Vec<PartInfo>)> {
        let (bucket, key, upload_id) = (bucket.to_owned(), key.to_owned(), upload_id.to_owned());
        self.run(move |i| {
            i.read(|txn| {
                bucket_info(&txn.open_table(BUCKETS)?, &bucket)?;
                let up = upload_row(&txn.open_table(UPLOADS)?, &bucket, &key, &upload_id)?;
                let prefix = child_prefix(&upload_key(&bucket, &upload_id));
                let parts = scan(&txn.open_table(PARTS)?, &prefix)?
                    .into_iter()
                    .map(|(_, v)| decode::<PartRow>(&v).map(|p| p.info))
                    .collect::<Result<_>>()?;
                Ok((up.info, parts))
            })
        })
        .await
    }

    async fn list_uploads(&self, bucket: &str) -> Result<Vec<UploadInfo>> {
        let bucket = bucket.to_owned();
        self.run(move |i| {
            i.read(|txn| {
                bucket_info(&txn.open_table(BUCKETS)?, &bucket)?;
                let prefix = child_prefix(bucket.as_bytes());
                let mut uploads = scan(&txn.open_table(UPLOADS)?, &prefix)?
                    .into_iter()
                    .map(|(_, v)| decode::<UploadRow>(&v).map(|u| u.info))
                    .collect::<Result<Vec<_>>>()?;
                uploads.sort_by(|a, b| (&a.key, a.initiated).cmp(&(&b.key, b.initiated)));
                Ok(uploads)
            })
        })
        .await
    }

    async fn complete_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[PartInfo],
        meta: ObjectMeta,
    ) -> Result<()> {
        // Held throughout, so parts cannot be replaced or aborted mid-concatenation.
        let _lock = self.lock(bucket, key).await;
        let (bucket, key, upload_id) = (bucket.to_owned(), key.to_owned(), upload_id.to_owned());
        let wanted: Vec<(u32, String)> = parts
            .iter()
            .map(|p| (p.part_number, p.etag.clone()))
            .collect();
        self.run(move |i| {
            let files = i.read(|txn| {
                bucket_info(&txn.open_table(BUCKETS)?, &bucket)?;
                let up = upload_row(&txn.open_table(UPLOADS)?, &bucket, &key, &upload_id)?;
                let dir = i.upload_dir(&up.dir);
                let t = txn.open_table(PARTS)?;
                wanted
                    .iter()
                    .map(|(n, etag)| {
                        match get::<PartRow>(&t, &part_key(&bucket, &upload_id, *n))? {
                            Some(p) if p.info.etag == *etag => Ok(dir.join(p.file)),
                            _ => Err(StorageError::InvalidPart),
                        }
                    })
                    .collect::<Result<Vec<_>>>()
            })?;
            let mut blob = i.concat(&files)?;
            i.crash("concatenated");
            i.write(|tx| {
                tx.require_bucket(&bucket)?;
                let up = tx.upload(&bucket, &key, &upload_id)?;
                tx.install_blob(&mut blob)?;
                let blob = Some(blob.sha256.clone());
                tx.push_version(&object_key(&bucket, &key), Stored { meta, blob })?;
                tx.drop_upload(&bucket, &upload_id, &up.dir)
            })
        })
        .await
    }

    async fn abort_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<()> {
        let _lock = self.lock(bucket, key).await;
        let (bucket, key, upload_id) = (bucket.to_owned(), key.to_owned(), upload_id.to_owned());
        self.run(move |i| {
            i.write(|tx| {
                tx.require_bucket(&bucket)?;
                let up = tx.upload(&bucket, &key, &upload_id)?;
                tx.drop_upload(&bucket, &upload_id, &up.dir)
            })
        })
        .await
    }
}

#[cfg(test)]
mod tests;
