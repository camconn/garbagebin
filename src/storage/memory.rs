//! In-memory backend: ordered maps behind one lock, bodies as `Bytes`.
//! Each key maps to its versions, newest first.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;

use super::{
    BlobReader, BucketInfo, BucketUpdate, ByteStream, MetaUpdate, ObjectMeta, PartInfo, Result,
    Storage, StorageError, Stored, UploadInfo, versions,
};

#[derive(Default)]
pub struct MemoryStorage {
    buckets: RwLock<BTreeMap<String, Bucket>>,
}

struct Bucket {
    info: BucketInfo,
    objects: BTreeMap<String, Vec<Version>>,
    uploads: BTreeMap<String, Upload>,
}

type Version = Stored<Bytes>;

struct Upload {
    info: UploadInfo,
    parts: BTreeMap<u32, (PartInfo, Bytes)>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }

    fn read(&self) -> RwLockReadGuard<'_, BTreeMap<String, Bucket>> {
        self.buckets.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write(&self) -> RwLockWriteGuard<'_, BTreeMap<String, Bucket>> {
        self.buckets.write().unwrap_or_else(|e| e.into_inner())
    }
}

fn bucket<'a>(map: &'a BTreeMap<String, Bucket>, name: &str) -> Result<&'a Bucket> {
    map.get(name).ok_or(StorageError::NoSuchBucket)
}

fn bucket_mut<'a>(map: &'a mut BTreeMap<String, Bucket>, name: &str) -> Result<&'a mut Bucket> {
    map.get_mut(name).ok_or(StorageError::NoSuchBucket)
}

impl Bucket {
    fn versions(&self, key: &str) -> &[Version] {
        self.objects.get(key).map_or(&[], Vec::as_slice)
    }

    fn push(&mut self, key: &str, v: Version) {
        versions::push(self.objects.entry(key.to_owned()).or_default(), v);
    }

    fn range<'a>(
        &'a self,
        prefix: &'a str,
        after: Option<&'a str>,
    ) -> impl Iterator<Item = (&'a String, &'a Vec<Version>)> {
        use std::ops::Bound;
        let lower = match after {
            Some(a) if a >= prefix => Bound::Excluded(a),
            _ => Bound::Included(prefix),
        };
        self.objects
            .range::<str, _>((lower, Bound::Unbounded))
            .take_while(move |(k, _)| k.starts_with(prefix))
    }

    fn upload(&self, key: &str, upload_id: &str) -> Result<&Upload> {
        self.uploads
            .get(upload_id)
            .filter(|u| u.info.key == key)
            .ok_or(StorageError::NoSuchUpload)
    }

    fn upload_mut(&mut self, key: &str, upload_id: &str) -> Result<&mut Upload> {
        self.uploads
            .get_mut(upload_id)
            .filter(|u| u.info.key == key)
            .ok_or(StorageError::NoSuchUpload)
    }
}

struct MemoryReader(Bytes);

impl BlobReader for MemoryReader {
    fn read(self: Box<Self>, range: Range<u64>) -> ByteStream {
        let body = self.0.slice(range.start as usize..range.end as usize);
        Box::pin(futures::stream::once(async move { Ok(body) }))
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    type Blob = Bytes;

    async fn create_bucket(&self, name: &str) -> Result<()> {
        let mut map = self.write();
        if map.contains_key(name) {
            return Err(StorageError::BucketAlreadyOwnedByYou);
        }
        let b = Bucket {
            info: BucketInfo::new(name),
            objects: BTreeMap::new(),
            uploads: BTreeMap::new(),
        };
        map.insert(name.to_owned(), b);
        Ok(())
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        let mut map = self.write();
        if !bucket(&map, name)?.objects.is_empty() {
            return Err(StorageError::BucketNotEmpty);
        }
        map.remove(name);
        Ok(())
    }

    async fn head_bucket(&self, name: &str) -> Result<BucketInfo> {
        Ok(bucket(&self.read(), name)?.info.clone())
    }

    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        Ok(self.read().values().map(|b| b.info.clone()).collect())
    }

    async fn update_bucket(&self, name: &str, f: BucketUpdate) -> Result<()> {
        let mut map = self.write();
        let b = bucket_mut(&mut map, name)?;
        let mut info = b.info.clone();
        f(&mut info)?;
        b.info = info;
        Ok(())
    }

    async fn write_blob(&self, mut body: ByteStream) -> Result<Bytes> {
        let mut buf = BytesMut::new();
        while let Some(chunk) = body.next().await {
            buf.extend_from_slice(&chunk.map_err(StorageError::Body)?);
        }
        Ok(buf.freeze())
    }

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        meta: ObjectMeta,
    ) -> Result<()> {
        let mut map = self.write();
        let blob = Some(body);
        bucket_mut(&mut map, bucket)?.push(key, Stored { meta, blob });
        Ok(())
    }

    async fn put_delete_marker(&self, bucket: &str, key: &str, meta: ObjectMeta) -> Result<()> {
        let mut map = self.write();
        let blob = None;
        bucket_mut(&mut map, bucket)?.push(key, Stored { meta, blob });
        Ok(())
    }

    async fn head_object(
        &self,
        bucket_name: &str,
        key: &str,
        version: Option<&str>,
    ) -> Result<ObjectMeta> {
        let map = self.read();
        let versions = bucket(&map, bucket_name)?.versions(key);
        Ok(versions::find(versions, version)?.meta.clone())
    }

    async fn open_object(
        &self,
        bucket_name: &str,
        key: &str,
        version: Option<&str>,
    ) -> Result<(ObjectMeta, Box<dyn BlobReader>)> {
        let map = self.read();
        let versions = bucket(&map, bucket_name)?.versions(key);
        let v = versions::find(versions, version)?;
        let body = v.blob.clone().unwrap_or_default();
        Ok((v.meta.clone(), Box::new(MemoryReader(body))))
    }

    async fn delete_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<ObjectMeta>> {
        let mut map = self.write();
        let b = bucket_mut(&mut map, bucket)?;
        let Some(list) = b.objects.get_mut(key) else {
            return Ok(None);
        };
        let removed = versions::remove(list, version_id);
        if list.is_empty() {
            b.objects.remove(key);
        }
        Ok(removed.map(|v| v.meta))
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
        let mut map = self.write();
        let src = versions::find(bucket(&map, src_bucket)?.versions(src_key), src_version)?;
        if src.meta.etag != src_etag {
            return Err(StorageError::PreconditionFailed);
        }
        let blob = src.blob.clone();
        bucket_mut(&mut map, dst_bucket)?.push(dst_key, Stored { meta, blob });
        Ok(())
    }

    async fn update_object(
        &self,
        bucket: &str,
        key: &str,
        version: Option<&str>,
        f: MetaUpdate,
    ) -> Result<ObjectMeta> {
        let mut map = self.write();
        let list = bucket_mut(&mut map, bucket)?.objects.get_mut(key);
        let v = versions::find_mut(list.map_or(&mut [][..], |l| l.as_mut_slice()), version)?;
        let mut meta = v.meta.clone();
        f(&mut meta)?;
        v.meta = meta.clone();
        Ok(meta)
    }

    async fn list_objects(
        &self,
        bucket_name: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, ObjectMeta)>> {
        let map = self.read();
        Ok(bucket(&map, bucket_name)?
            .range(prefix, after)
            .filter_map(|(k, list)| Some((k.clone(), versions::live(list)?.meta.clone())))
            .take(limit)
            .collect())
    }

    async fn list_versions(
        &self,
        bucket_name: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, Vec<ObjectMeta>)>> {
        let map = self.read();
        Ok(bucket(&map, bucket_name)?
            .range(prefix, after)
            .take(limit)
            .map(|(k, list)| (k.clone(), list.iter().map(|v| v.meta.clone()).collect()))
            .collect())
    }

    async fn create_upload(&self, bucket: &str, upload: UploadInfo) -> Result<()> {
        let mut map = self.write();
        let b = bucket_mut(&mut map, bucket)?;
        let upload = Upload {
            info: upload,
            parts: BTreeMap::new(),
        };
        b.uploads.insert(upload.info.upload_id.clone(), upload);
        Ok(())
    }

    async fn put_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part: PartInfo,
        body: Bytes,
    ) -> Result<()> {
        let mut map = self.write();
        let upload = bucket_mut(&mut map, bucket)?.upload_mut(key, upload_id)?;
        upload.parts.insert(part.part_number, (part, body));
        Ok(())
    }

    async fn list_parts(
        &self,
        bucket_name: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(UploadInfo, Vec<PartInfo>)> {
        let map = self.read();
        let upload = bucket(&map, bucket_name)?.upload(key, upload_id)?;
        let parts = upload.parts.values().map(|(p, _)| p.clone()).collect();
        Ok((upload.info.clone(), parts))
    }

    async fn list_uploads(&self, bucket_name: &str) -> Result<Vec<UploadInfo>> {
        let map = self.read();
        let mut uploads: Vec<_> = bucket(&map, bucket_name)?
            .uploads
            .values()
            .map(|u| u.info.clone())
            .collect();
        uploads.sort_by(|a, b| (&a.key, a.initiated).cmp(&(&b.key, b.initiated)));
        Ok(uploads)
    }

    async fn complete_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[PartInfo],
        meta: ObjectMeta,
    ) -> Result<()> {
        let mut map = self.write();
        let b = bucket_mut(&mut map, bucket)?;
        let upload = b.upload(key, upload_id)?;
        let mut body = BytesMut::new();
        for want in parts {
            match upload.parts.get(&want.part_number) {
                Some((have, data)) if have.etag == want.etag => body.extend_from_slice(data),
                _ => return Err(StorageError::InvalidPart),
            }
        }
        b.uploads.remove(upload_id);
        let blob = Some(body.freeze());
        b.push(key, Stored { meta, blob });
        Ok(())
    }

    async fn abort_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<()> {
        let mut map = self.write();
        let b = bucket_mut(&mut map, bucket)?;
        b.upload(key, upload_id)?;
        b.uploads.remove(upload_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    crate::storage_conformance!(super::MemoryStorage::new());
}
