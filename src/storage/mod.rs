//! Storage abstraction. Backends are dumb: they store blobs and metadata rows.
//! S3 semantics (ETags, validation, conditionals, listing groups) live in the service.

use std::collections::BTreeMap;
use std::io;
use std::ops::Range;
use std::pin::Pin;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use serde::{Deserialize, Serialize};

pub mod fs;
pub mod memory;

#[cfg(test)]
pub mod conformance;

pub use fs::FsStorage;
pub use memory::MemoryStorage;

pub type ByteStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send + Sync>>;
pub type Tags = BTreeMap<String, String>;
pub type Result<T, E = StorageError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("bucket does not exist")]
    NoSuchBucket,
    #[error("bucket already exists")]
    BucketAlreadyOwnedByYou,
    #[error("bucket is not empty")]
    BucketNotEmpty,
    #[error("key does not exist")]
    NoSuchKey,
    #[error("version does not exist")]
    NoSuchVersion,
    /// The addressed version (the current one, if none was named) is a delete marker.
    #[error("version {0} is a delete marker")]
    DeleteMarker(String),
    #[error("upload does not exist")]
    NoSuchUpload,
    /// A part named in Complete is missing or changed since it was listed.
    #[error("part is missing or changed")]
    InvalidPart,
    /// The copy source changed between the service reading it and the backend linking it.
    #[error("source changed during copy")]
    PreconditionFailed,
    /// Reading the request body failed; the source is the stream's error.
    #[error("request body: {0}")]
    Body(io::Error),
    /// An update or delete check declined the change; nothing was written.
    #[error("refused: {0}")]
    Refused(Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    Io(io::Error),
    #[error("{0}")]
    Internal(String),
}

/// Checksums as base64 strings, exactly as sent on the wire.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checksums {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crc32: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crc32c: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crc64nvme: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha1: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub md5: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha512: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub xxhash64: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub xxhash3: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub xxhash128: Option<String>,
    /// `FULL_OBJECT` or `COMPOSITE`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum_type: Option<String>,
}

/// SSE headers, stored only to be echoed back.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sse {
    pub algorithm: Option<String>,
    pub kms_key_id: Option<String>,
    pub customer_algorithm: Option<String>,
    pub customer_key_md5: Option<String>,
    pub bucket_key_enabled: Option<bool>,
}

/// Version id of the null version: objects written while versioning is off or suspended.
pub const NULL_VERSION: &str = "null";

fn null_version() -> String {
    NULL_VERSION.to_owned()
}

/// One version row. Backends store and return it verbatim.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMeta {
    /// Opaque, or [`NULL_VERSION`]. Writing a null version replaces the key's existing one.
    #[serde(default = "null_version")]
    pub version_id: String,
    /// A delete marker has no body; only `version_id` and `last_modified` are meaningful.
    #[serde(default)]
    pub delete_marker: bool,
    pub size: u64,
    /// Unquoted: hex MD5, or `hex-N` for multipart.
    pub etag: String,
    pub last_modified: SystemTime,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    /// `Expires` header, verbatim.
    #[serde(default)]
    pub expires: Option<String>,
    /// `x-amz-meta-*` without the prefix.
    pub user_metadata: BTreeMap<String, String>,
    pub tags: Tags,
    pub checksums: Checksums,
    /// Part sizes in order for objects created by CompleteMultipartUpload; empty otherwise.
    #[serde(default)]
    pub part_sizes: Vec<u64>,
    /// Each part's checksum in the object's algorithm; empty without one.
    #[serde(default)]
    pub part_checksums: Vec<String>,
    /// The upload a CompleteMultipartUpload created this object from, so a retried Complete succeeds.
    #[serde(default)]
    pub upload_id: Option<String>,
    pub sse: Sse,
    /// `x-amz-storage-class` other than `STANDARD`, stored only to be echoed back.
    #[serde(default)]
    pub storage_class: Option<String>,
    #[serde(default)]
    pub retention: Option<Retention>,
    /// `None` if never set; AWS tells that apart from `OFF`.
    #[serde(default)]
    pub legal_hold: Option<bool>,
}

impl ObjectMeta {
    pub fn new(size: u64, etag: String) -> Self {
        Self {
            version_id: null_version(),
            delete_marker: false,
            size,
            etag,
            last_modified: SystemTime::now(),
            content_type: None,
            content_encoding: None,
            content_language: None,
            cache_control: None,
            content_disposition: None,
            expires: None,
            user_metadata: BTreeMap::new(),
            tags: Tags::new(),
            checksums: Checksums::default(),
            part_sizes: Vec::new(),
            part_checksums: Vec::new(),
            upload_id: None,
            sse: Sse::default(),
            storage_class: None,
            retention: None,
            legal_hold: None,
        }
    }

    pub fn delete_marker(version_id: String) -> Self {
        Self {
            version_id,
            delete_marker: true,
            ..Self::new(0, String::new())
        }
    }
}

/// Bucket versioning state; a bucket that never had it configured has none.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Versioning {
    Enabled,
    Suspended,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LockMode {
    Governance,
    Compliance,
}

/// Object Lock retention of one version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Retention {
    pub mode: LockMode,
    pub until: SystemTime,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetentionPeriod {
    Days(u32),
    Years(u32),
}

/// A bucket's Object Lock configuration; having one means lock is enabled.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectLock {
    /// Applied to new versions written without explicit retention.
    pub default_retention: Option<(LockMode, RetentionPeriod)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketInfo {
    pub name: String,
    pub created: SystemTime,
    pub tags: Tags,
    #[serde(default)]
    pub versioning: Option<Versioning>,
    #[serde(default)]
    pub object_lock: Option<ObjectLock>,
}

impl BucketInfo {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            created: SystemTime::now(),
            tags: Tags::new(),
            versioning: None,
            object_lock: None,
        }
    }
}

/// An in-place edit of a bucket row, applied atomically; an error aborts it.
pub type BucketUpdate = Box<dyn FnOnce(&mut BucketInfo) -> Result<()> + Send>;
/// An in-place edit of a version row (tags, lock fields), applied atomically.
pub type MetaUpdate = Box<dyn FnOnce(&mut ObjectMeta) -> Result<()> + Send>;
/// Decides, atomically with the delete, whether a version may be removed.
pub type DeleteCheck = Box<dyn FnOnce(&ObjectMeta) -> Result<()> + Send>;
/// Decides, atomically with a write, whether it may go ahead given the key's
/// current version (`None` if there is none or it is a delete marker).
pub type WriteCheck = Box<dyn FnOnce(Option<&ObjectMeta>) -> Result<()> + Send>;

pub(crate) fn no_check() -> WriteCheck {
    Box::new(|_| Ok(()))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadInfo {
    pub upload_id: String,
    pub key: String,
    pub initiated: SystemTime,
    /// Checksum algorithm declared on create (e.g. `CRC32`).
    pub checksum_algorithm: Option<String>,
    /// `FULL_OBJECT` or `COMPOSITE`; set with `checksum_algorithm`.
    #[serde(default)]
    pub checksum_type: Option<String>,
    /// Metadata applied to the object on Complete; `size`/`etag` are placeholders.
    pub meta: ObjectMeta,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartInfo {
    pub part_number: u32,
    pub size: u64,
    /// Unquoted hex MD5 of the part.
    pub etag: String,
    pub last_modified: SystemTime,
    pub checksums: Checksums,
}

/// Read side of a stored blob, opened together with its metadata row so
/// later overwrites of the key cannot affect it.
pub trait BlobReader: Send + Sync {
    /// Streams `range`, which the caller has already checked against the object size.
    fn read(self: Box<Self>, range: Range<u64>) -> ByteStream;
}

/// A stored version: its row, and its body unless it is a delete marker.
/// Every key holds its versions newest first; the first is the current one.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Stored<B> {
    pub meta: ObjectMeta,
    pub blob: Option<B>,
}

/// Version-list operations both backends share.
pub(crate) mod versions {
    use super::{NULL_VERSION, Result, StorageError, Stored};

    /// Makes `v` current. A null version replaces the existing one, which is returned.
    pub fn push<B>(list: &mut Vec<Stored<B>>, v: Stored<B>) -> Option<Stored<B>> {
        let old = if v.meta.version_id == NULL_VERSION {
            remove(list, NULL_VERSION)
        } else {
            None
        };
        list.insert(0, v);
        old
    }

    pub fn remove<B>(list: &mut Vec<Stored<B>>, id: &str) -> Option<Stored<B>> {
        let i = list.iter().position(|v| v.meta.version_id == id)?;
        Some(list.remove(i))
    }

    /// `remove`, unless `check` refuses the version.
    pub fn remove_if<B>(
        list: &mut Vec<Stored<B>>,
        id: &str,
        check: super::DeleteCheck,
    ) -> Result<Option<Stored<B>>> {
        let Some(i) = list.iter().position(|v| v.meta.version_id == id) else {
            return Ok(None);
        };
        check(&list[i].meta)?;
        Ok(Some(list.remove(i)))
    }

    fn position<B>(list: &[Stored<B>], id: Option<&str>) -> Result<usize> {
        let i = match id {
            None if list.is_empty() => return Err(StorageError::NoSuchKey),
            None => 0,
            Some(id) => list
                .iter()
                .position(|v| v.meta.version_id == id)
                .ok_or(StorageError::NoSuchVersion)?,
        };
        if list[i].meta.delete_marker {
            return Err(StorageError::DeleteMarker(list[i].meta.version_id.clone()));
        }
        Ok(i)
    }

    /// The version `id` names, else the current one. Never a delete marker.
    pub fn find<'a, B>(list: &'a [Stored<B>], id: Option<&str>) -> Result<&'a Stored<B>> {
        Ok(&list[position(list, id)?])
    }

    pub fn find_mut<'a, B>(
        list: &'a mut [Stored<B>],
        id: Option<&str>,
    ) -> Result<&'a mut Stored<B>> {
        let i = position(list, id)?;
        Ok(&mut list[i])
    }

    /// The current version, unless there is none or it is a delete marker.
    pub fn live<B>(list: &[Stored<B>]) -> Option<&Stored<B>> {
        list.first().filter(|v| !v.meta.delete_marker)
    }
}

/// The storage contract. The service calls `write_blob` to stage a body
/// (the fs backend streams to `tmp/` and fsyncs), verifies digests, then
/// commits it with `put_object` or `put_part`. Dropping an uncommitted blob
/// must discard it.
///
/// Every method taking a bucket returns `NoSuchBucket` if it is missing; every
/// method taking an upload id returns `NoSuchUpload` if it is missing or
/// belongs to a different key.
///
/// Each key holds versions, newest first. Writes take the version id from the
/// row: a new id adds a version, [`NULL_VERSION`] replaces the null version;
/// either way the written version becomes current. Reads taking
/// `version: Option<&str>` address that version, or the current one if `None`:
/// `NoSuchKey` if the key has no versions, `NoSuchVersion` if the id is not
/// among them, `DeleteMarker` if it names a delete marker. Versioning policy
/// (which id to write) is the service's.
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    type Blob: Send + 'static;

    // Buckets.

    /// `BucketAlreadyOwnedByYou` if it exists.
    async fn create_bucket(&self, bucket: &str) -> Result<()>;
    /// `BucketNotEmpty` if it holds any version. Pending uploads are discarded.
    async fn delete_bucket(&self, bucket: &str) -> Result<()>;
    async fn head_bucket(&self, bucket: &str) -> Result<BucketInfo>;
    /// Ordered by name.
    async fn list_buckets(&self) -> Result<Vec<BucketInfo>>;
    async fn update_bucket(&self, bucket: &str, f: BucketUpdate) -> Result<()>;

    // Objects.

    /// Consumes `body` into a staged blob. A stream error aborts with `Body`.
    async fn write_blob(&self, body: ByteStream) -> Result<Self::Blob>;
    /// Atomically adds version `meta` with body `blob` as the current version of `key`.
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        blob: Self::Blob,
        meta: ObjectMeta,
    ) -> Result<()> {
        self.put_object_if(bucket, key, blob, meta, no_check())
            .await
    }
    /// `put_object`, unless `check` returns an error; that error is returned and nothing changes.
    async fn put_object_if(
        &self,
        bucket: &str,
        key: &str,
        blob: Self::Blob,
        meta: ObjectMeta,
        check: WriteCheck,
    ) -> Result<()>;
    /// Adds delete marker `meta` as the current version of `key`.
    async fn put_delete_marker(&self, bucket: &str, key: &str, meta: ObjectMeta) -> Result<()> {
        self.put_delete_marker_if(bucket, key, meta, no_check())
            .await
    }
    /// `put_delete_marker`, unless `check` returns an error.
    async fn put_delete_marker_if(
        &self,
        bucket: &str,
        key: &str,
        meta: ObjectMeta,
        check: WriteCheck,
    ) -> Result<()>;
    async fn head_object(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&str>,
    ) -> Result<ObjectMeta>;
    async fn open_object(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&str>,
    ) -> Result<(ObjectMeta, Box<dyn BlobReader>)>;
    /// Permanently removes one version (possibly a delete marker) and returns it;
    /// `None` if it was already absent. The next newest version becomes current.
    async fn delete_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<ObjectMeta>> {
        self.delete_version_if(bucket, key, version_id, Box::new(|_| Ok(())))
            .await
    }
    /// `delete_version`, unless `check` returns an error for the version; that error
    /// is returned and nothing changes.
    async fn delete_version_if(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        check: DeleteCheck,
    ) -> Result<Option<ObjectMeta>>;
    /// Adds version `meta` of `dst` sharing the blob of `src`'s version `src_version`;
    /// no bytes move. `PreconditionFailed` if its ETag is no longer `src_etag`.
    #[allow(clippy::too_many_arguments)]
    async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        src_version: Option<&str>,
        src_etag: &str,
        dst_bucket: &str,
        dst_key: &str,
        meta: ObjectMeta,
    ) -> Result<()>;
    /// Edits one version row in place and returns the result.
    async fn update_object(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        f: MetaUpdate,
    ) -> Result<ObjectMeta>;
    /// Up to `limit` keys starting with `prefix` and sorting strictly after
    /// `after`, in byte order, with their current version. Keys whose current
    /// version is a delete marker are skipped.
    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, ObjectMeta)>>;
    /// Like `list_objects`, but every key with any version, and all its versions, newest first.
    async fn list_versions(
        &self,
        bucket: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, Vec<ObjectMeta>)>>;

    // Multipart.

    async fn create_upload(&self, bucket: &str, upload: UploadInfo) -> Result<()>;
    /// The upload, without its parts.
    async fn get_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<UploadInfo>;
    /// Stores or replaces part `part.part_number`.
    async fn put_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part: PartInfo,
        blob: Self::Blob,
    ) -> Result<()>;
    /// The upload and its parts ordered by part number.
    async fn list_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(UploadInfo, Vec<PartInfo>)>;
    /// Ordered by key, then initiation time.
    async fn list_uploads(&self, bucket: &str) -> Result<Vec<UploadInfo>>;
    /// Concatenates `parts` in order into version `meta` of `key` and
    /// removes the upload. `InvalidPart` if a part is missing or its ETag changed.
    async fn complete_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[PartInfo],
        meta: ObjectMeta,
    ) -> Result<()> {
        self.complete_upload_if(bucket, key, upload_id, parts, meta, no_check())
            .await
    }
    /// `complete_upload`, unless `check` returns an error; the upload is then kept.
    #[allow(clippy::too_many_arguments)]
    async fn complete_upload_if(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[PartInfo],
        meta: ObjectMeta,
        check: WriteCheck,
    ) -> Result<()>;
    async fn abort_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<()>;
}
