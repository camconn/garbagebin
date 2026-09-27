//! Startup sweep: clears `tmp/`, rebuilds blob refcounts from every version,
//! expires idle multipart uploads, and deletes files no row references.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime};

use redb::{ReadableTable, ReadableTableMetadata as _};
use tracing::{info, warn};

use super::{
    BLOBS_DIR, BUCKETS, Inner, OBJECTS, PARTS, PartRow, REFS, TMP_DIR, UPLOADS, UPLOADS_DIR,
    UploadRow, child_prefix, decode, decode_versions, remove_path, scan,
};

/// Uploads with no activity for this long are discarded on startup.
pub const UPLOAD_TTL: Duration = Duration::from_secs(24 * 60 * 60);

pub fn run(inner: &Inner) -> anyhow::Result<()> {
    let tmp = inner.root.join(TMP_DIR);
    std::fs::remove_dir_all(&tmp)?;
    std::fs::create_dir(&tmp)?;

    let cutoff = SystemTime::now() - UPLOAD_TTL;
    let txn = inner.db.begin_write()?;
    txn.open_table(BUCKETS)?;

    // Refcounts are always written with their rows; recounting guards against drift anyway.
    let mut counts: HashMap<String, u64> = HashMap::new();
    for entry in txn.open_table(OBJECTS)?.iter()? {
        for v in decode_versions(entry?.1.value())? {
            if let Some(sha) = v.blob {
                *counts.entry(sha).or_default() += 1;
            }
        }
    }
    {
        let mut refs = txn.open_table(REFS)?;
        let mut stored = HashMap::new();
        for entry in refs.iter()? {
            let (k, v) = entry?;
            stored.insert(k.value().to_owned(), v.value());
        }
        if stored != counts {
            warn!(
                stored = refs.len()?,
                actual = counts.len(),
                "blob refcounts drifted; rebuilt"
            );
            refs.retain(|_, _| false)?;
            for (sha, n) in &counts {
                refs.insert(sha.as_str(), *n)?;
            }
        }
    }

    // Last activity and part files per upload, keyed by upload row key.
    let mut activity: HashMap<Vec<u8>, SystemTime> = HashMap::new();
    let mut part_files: HashMap<Vec<u8>, HashSet<String>> = HashMap::new();
    for entry in txn.open_table(PARTS)?.iter()? {
        let (k, v) = entry?;
        let k = k.value();
        let upload = k[..k.len() - 5].to_vec();
        let row: PartRow = decode(v.value())?;
        let t = activity
            .entry(upload.clone())
            .or_insert(row.info.last_modified);
        *t = (*t).max(row.info.last_modified);
        part_files.entry(upload).or_default().insert(row.file);
    }
    let mut live: HashMap<String, HashSet<String>> = HashMap::new();
    let mut expired = 0;
    {
        let mut uploads = txn.open_table(UPLOADS)?;
        let mut parts = txn.open_table(PARTS)?;
        for (k, v) in scan(&uploads, b"")? {
            let row: UploadRow = decode(&v)?;
            let last = activity
                .get(&k)
                .map_or(row.info.initiated, |t| (*t).max(row.info.initiated));
            if last < cutoff {
                uploads.remove(k.as_slice())?;
                for (pk, _) in scan(&parts, &child_prefix(&k))? {
                    parts.remove(pk.as_slice())?;
                }
                expired += 1;
            } else {
                live.insert(row.dir, part_files.remove(&k).unwrap_or_default());
            }
        }
    }
    txn.commit()?;

    let mut orphans = 0;
    for level1 in std::fs::read_dir(inner.root.join(BLOBS_DIR))? {
        let level1 = level1?.path();
        if !level1.is_dir() {
            remove_path(&level1);
            continue;
        }
        for level2 in std::fs::read_dir(&level1)? {
            let level2 = level2?.path();
            if !level2.is_dir() {
                remove_path(&level2);
                continue;
            }
            for blob in std::fs::read_dir(&level2)? {
                let blob = blob?;
                if !counts.contains_key(&*blob.file_name().to_string_lossy()) {
                    remove_path(&blob.path());
                    orphans += 1;
                }
            }
            let _ = std::fs::remove_dir(&level2);
        }
        let _ = std::fs::remove_dir(&level1);
    }
    for sha in counts.keys() {
        if !inner.blob_path(sha).exists() {
            warn!(sha, "blob referenced by metadata is missing");
        }
    }

    for dir in std::fs::read_dir(inner.root.join(UPLOADS_DIR))? {
        let dir = dir?;
        if !dir.path().is_dir() {
            remove_path(&dir.path());
            continue;
        }
        match live.get(&*dir.file_name().to_string_lossy()) {
            None => remove_path(&dir.path()),
            Some(files) => {
                for file in std::fs::read_dir(dir.path())? {
                    let file = file?;
                    if !files.contains(&*file.file_name().to_string_lossy()) {
                        remove_path(&file.path());
                    }
                }
            }
        }
    }

    info!(
        orphan_blobs = orphans,
        expired_uploads = expired,
        "startup sweep"
    );
    Ok(())
}
