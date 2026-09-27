//! ListObjects and ListObjectVersions grouping (prefix, delimiter,
//! CommonPrefixes, markers, max-keys) over the backend's ordered key scan.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

use crate::storage::{ObjectMeta, Result, Storage};

const BATCH: usize = 1000;

/// The ordered scans listing needs from a backend.
#[async_trait::async_trait]
trait Scan: Sync {
    async fn objects(
        &self,
        bucket: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, ObjectMeta)>>;
    async fn versions(
        &self,
        bucket: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, Vec<ObjectMeta>)>>;
}

#[async_trait::async_trait]
impl<S: Storage> Scan for S {
    async fn objects(
        &self,
        bucket: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, ObjectMeta)>> {
        self.list_objects(bucket, prefix, after, limit).await
    }

    async fn versions(
        &self,
        bucket: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, Vec<ObjectMeta>)>> {
        self.list_versions(bucket, prefix, after, limit).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    Object(String, Box<ObjectMeta>),
    Prefix(String),
}

impl Entry {
    /// The key or common prefix; what markers are compared against.
    pub fn name(&self) -> &str {
        match self {
            Entry::Object(k, _) | Entry::Prefix(k) => k,
        }
    }
}

#[derive(Debug, Default)]
pub struct Page {
    pub entries: Vec<Entry>,
    pub truncated: bool,
    /// Resume point when truncated: the last entry's name.
    pub next_marker: Option<String>,
}

/// The common prefix `key` rolls up into, if any.
pub fn common_prefix<'a>(key: &'a str, prefix: &str, delimiter: Option<&str>) -> Option<&'a str> {
    let d = delimiter.filter(|d| !d.is_empty())?;
    let rest = key.strip_prefix(prefix)?;
    rest.find(d).map(|i| &key[..prefix.len() + i + d.len()])
}

/// Lists up to `max_keys` entries (objects plus common prefixes) sorting after `marker`.
pub async fn list<S: Storage>(
    s: &S,
    bucket: &str,
    prefix: &str,
    delimiter: Option<&str>,
    marker: Option<&str>,
    max_keys: usize,
) -> Result<Page> {
    list_batched(s, bucket, prefix, delimiter, marker, max_keys, BATCH).await
}

async fn list_batched(
    s: &impl Scan,
    bucket: &str,
    prefix: &str,
    delimiter: Option<&str>,
    marker: Option<&str>,
    max_keys: usize,
    batch: usize,
) -> Result<Page> {
    let mut page = Page::default();
    let mut after = marker.map(|m| skip_group(m, prefix, delimiter));
    loop {
        let objects = s.objects(bucket, prefix, after.as_deref(), batch).await?;
        let exhausted = objects.len() < batch;
        for (key, meta) in objects {
            after = Some(key.clone());
            let entry = match common_prefix(&key, prefix, delimiter) {
                Some(cp) => Entry::Prefix(cp.to_owned()),
                None => Entry::Object(key, Box::new(meta)),
            };
            if marker.is_some_and(|m| entry.name() <= m) || page.entries.last() == Some(&entry) {
                continue;
            }
            if page.entries.len() == max_keys {
                page.truncated = max_keys > 0;
                page.next_marker = page.entries.last().map(|e| e.name().to_owned());
                return Ok(page);
            }
            page.entries.push(entry);
        }
        if exhausted {
            return Ok(page);
        }
        if let Some(Entry::Prefix(cp)) = page.entries.last() {
            seek_past(&mut after, cp, prefix, delimiter);
        }
    }
}

/// Moves a scan position inside group `cp` past the rest of the group.
fn seek_past(after: &mut Option<String>, cp: &str, prefix: &str, delimiter: Option<&str>) {
    if let Some(a) = after.as_deref()
        && a.starts_with(cp)
    {
        let jump = skip_group(cp, prefix, delimiter);
        if jump.as_str() > a {
            *after = Some(jump);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionEntry {
    /// A version or delete marker; `latest` if it is the key's current version.
    Version {
        key: String,
        meta: Box<ObjectMeta>,
        latest: bool,
    },
    Prefix(String),
}

#[derive(Debug, Default)]
pub struct VersionPage {
    pub entries: Vec<VersionEntry>,
    pub truncated: bool,
    /// Resume point when truncated: the last entry's key (or prefix) and version id.
    pub next_key_marker: Option<String>,
    pub next_version_marker: Option<String>,
}

impl VersionPage {
    /// Adds `e` unless the page is full, in which case it marks the page truncated.
    fn push(&mut self, e: VersionEntry, max: usize) -> bool {
        if self.entries.len() == max {
            self.truncated = max > 0;
            match self.entries.last() {
                Some(VersionEntry::Version { key, meta, .. }) => {
                    self.next_key_marker = Some(key.clone());
                    self.next_version_marker = Some(meta.version_id.clone());
                }
                Some(VersionEntry::Prefix(p)) => self.next_key_marker = Some(p.clone()),
                None => {}
            }
            return false;
        }
        self.entries.push(e);
        true
    }
}

/// Lists up to `max_keys` versions, delete markers and common prefixes, by key
/// then newest first, after `key_marker` (or after `version_marker` within it).
pub async fn list_versions<S: Storage>(
    s: &S,
    bucket: &str,
    prefix: &str,
    delimiter: Option<&str>,
    key_marker: Option<&str>,
    version_marker: Option<&str>,
    max_keys: usize,
) -> Result<VersionPage> {
    list_versions_batched(
        s,
        bucket,
        prefix,
        delimiter,
        key_marker,
        version_marker,
        max_keys,
        BATCH,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn list_versions_batched(
    s: &impl Scan,
    bucket: &str,
    prefix: &str,
    delimiter: Option<&str>,
    key_marker: Option<&str>,
    version_marker: Option<&str>,
    max_keys: usize,
    batch: usize,
) -> Result<VersionPage> {
    let mut page = VersionPage::default();
    // Resuming inside a key: its versions older than the marker come first.
    if let (Some(km), Some(vm)) = (key_marker, version_marker)
        && km.starts_with(prefix)
        && common_prefix(km, prefix, delimiter).is_none()
    {
        // The smallest key starting with `km` is `km` itself.
        let first = s.versions(bucket, km, None, 1).await?;
        if let Some((key, versions)) = first.into_iter().find(|(k, _)| k == km)
            && let Some(pos) = versions.iter().position(|m| m.version_id == vm)
        {
            for (i, meta) in versions.into_iter().enumerate().skip(pos + 1) {
                let e = VersionEntry::Version {
                    key: key.clone(),
                    meta: Box::new(meta),
                    latest: i == 0,
                };
                if !page.push(e, max_keys) {
                    return Ok(page);
                }
            }
        }
    }
    let mut after = key_marker.map(|m| skip_group(m, prefix, delimiter));
    loop {
        let keys = s.versions(bucket, prefix, after.as_deref(), batch).await?;
        let exhausted = keys.len() < batch;
        for (key, versions) in keys {
            after = Some(key.clone());
            if let Some(cp) = common_prefix(&key, prefix, delimiter) {
                if key_marker.is_some_and(|m| cp <= m)
                    || matches!(page.entries.last(), Some(VersionEntry::Prefix(p)) if p == cp)
                {
                    continue;
                }
                if !page.push(VersionEntry::Prefix(cp.to_owned()), max_keys) {
                    return Ok(page);
                }
                continue;
            }
            for (i, meta) in versions.into_iter().enumerate() {
                let e = VersionEntry::Version {
                    key: key.clone(),
                    meta: Box::new(meta),
                    latest: i == 0,
                };
                if !page.push(e, max_keys) {
                    return Ok(page);
                }
            }
        }
        if exhausted {
            return Ok(page);
        }
        if let Some(VersionEntry::Prefix(cp)) = page.entries.last() {
            seek_past(&mut after, cp, prefix, delimiter);
        }
    }
}

/// Scan position just past every key under `name` if it is a common prefix,
/// else `name` itself. Keys under a group all sort below group + U+10FFFF,
/// except ones continuing with U+10FFFF, which are deduplicated instead.
fn skip_group(name: &str, prefix: &str, delimiter: Option<&str>) -> String {
    if common_prefix(name, prefix, delimiter) == Some(name) {
        format!("{name}\u{10FFFF}")
    } else {
        name.to_owned()
    }
}

/// Opaque ListObjectsV2 continuation token.
pub fn encode_token(marker: &str) -> String {
    URL_SAFE_NO_PAD.encode(marker)
}

pub fn decode_token(token: &str) -> Option<String> {
    String::from_utf8(URL_SAFE_NO_PAD.decode(token).ok()?).ok()
}

/// `encoding-type=url`: percent-encode everything but unreserved characters and `/`.
pub fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~/".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{MemoryStorage, ObjectMeta};
    use bytes::Bytes;
    use proptest::prelude::*;

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
    }

    async fn store(keys: &[String]) -> MemoryStorage {
        let s = MemoryStorage::new();
        s.create_bucket("b").await.unwrap();
        for k in keys {
            let blob = Bytes::new();
            s.put_object("b", k, blob, ObjectMeta::new(0, String::new()))
                .await
                .unwrap();
        }
        s
    }

    /// Naive reference: group every key, dedupe, filter by marker, truncate.
    fn reference(
        keys: &[String],
        prefix: &str,
        delim: Option<&str>,
        marker: Option<&str>,
    ) -> Vec<String> {
        let mut sorted = keys.to_vec();
        sorted.sort();
        sorted.dedup();
        let mut out: Vec<String> = Vec::new();
        for k in sorted.iter().filter(|k| k.starts_with(prefix)) {
            let name = common_prefix(k, prefix, delim).unwrap_or(k).to_owned();
            if marker.is_some_and(|m| name.as_str() <= m) || out.last() == Some(&name) {
                continue;
            }
            out.push(name);
        }
        out
    }

    async fn paged(
        s: &MemoryStorage,
        prefix: &str,
        delim: Option<&str>,
        max: usize,
        batch: usize,
    ) -> Vec<String> {
        let mut all = Vec::new();
        let mut marker: Option<String> = None;
        loop {
            let page = list_batched(s, "b", prefix, delim, marker.as_deref(), max, batch)
                .await
                .unwrap();
            assert!(page.entries.len() <= max);
            all.extend(page.entries.iter().map(|e| e.name().to_owned()));
            if !page.truncated {
                return all;
            }
            assert!(!page.entries.is_empty());
            marker = page.next_marker;
        }
    }

    fn key_strategy() -> impl Strategy<Value = String> {
        proptest::string::string_regex("[ab/é\u{10FFFF}.]{0,6}").unwrap()
    }

    proptest! {
        #[test]
        fn matches_reference(
            keys in proptest::collection::vec(key_strategy(), 0..40),
            prefix in "[ab/]{0,2}",
            delim in proptest::option::of(prop_oneof!["/", "b", "a/", "\u{10FFFF}"]),
            marker in proptest::option::of(key_strategy()),
            max in 1usize..6,
            batch in 1usize..5,
        ) {
            let keys: Vec<String> = keys.into_iter().filter(|k| !k.is_empty()).collect();
            rt().block_on(async {
                let s = store(&keys).await;
                let delim = delim.as_deref();
                let want = reference(&keys, &prefix, delim, marker.as_deref());

                let page = list_batched(&s, "b", &prefix, delim, marker.as_deref(), max, batch).await.unwrap();
                let got: Vec<_> = page.entries.iter().map(|e| e.name().to_owned()).collect();
                prop_assert_eq!(&got[..], &want[..want.len().min(max)]);
                prop_assert_eq!(page.truncated, want.len() > max);

                // Paging through with next_marker yields everything exactly once.
                let want_all = reference(&keys, &prefix, delim, None);
                prop_assert_eq!(paged(&s, &prefix, delim, max, batch).await, want_all);
                Ok(())
            })?;
        }
    }

    /// Keys with `n` versions each, ids `v<i>`; odd ids are delete markers.
    async fn versioned_store(keys: &[(String, u8)]) -> MemoryStorage {
        let s = MemoryStorage::new();
        s.create_bucket("b").await.unwrap();
        let mut id = 0;
        for (k, n) in keys {
            for _ in 0..*n {
                id += 1;
                let meta = if id % 2 == 1 {
                    ObjectMeta::delete_marker(format!("v{id}"))
                } else {
                    let mut m = ObjectMeta::new(0, String::new());
                    m.version_id = format!("v{id}");
                    m
                };
                if meta.delete_marker {
                    s.put_delete_marker("b", k, meta).await.unwrap();
                } else {
                    s.put_object("b", k, Bytes::new(), meta).await.unwrap();
                }
            }
        }
        s
    }

    /// Entries as `key@version` or a bare common prefix.
    fn names(entries: &[VersionEntry]) -> Vec<String> {
        entries
            .iter()
            .map(|e| match e {
                VersionEntry::Version { key, meta, .. } => format!("{key}@{}", meta.version_id),
                VersionEntry::Prefix(p) => p.clone(),
            })
            .collect()
    }

    /// Naive reference: every version by key then newest first, grouped and deduped.
    async fn version_reference(
        s: &MemoryStorage,
        prefix: &str,
        delim: Option<&str>,
    ) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for (k, versions) in s
            .list_versions("b", prefix, None, usize::MAX)
            .await
            .unwrap()
        {
            match common_prefix(&k, prefix, delim) {
                Some(cp) if out.last().map(String::as_str) == Some(cp) => {}
                Some(cp) => out.push(cp.to_owned()),
                None => out.extend(versions.iter().map(|m| format!("{k}@{}", m.version_id))),
            }
        }
        out
    }

    proptest! {
        #[test]
        fn versions_match_reference(
            keys in proptest::collection::vec((key_strategy(), 1u8..4), 0..20),
            prefix in "[ab/]{0,2}",
            delim in proptest::option::of(prop_oneof!["/", "b", "\u{10FFFF}"]),
            max in 1usize..6,
            batch in 1usize..5,
        ) {
            let keys: Vec<(String, u8)> = keys.into_iter().filter(|(k, _)| !k.is_empty()).collect();
            rt().block_on(async {
                let s = versioned_store(&keys).await;
                let delim = delim.as_deref();
                let want = version_reference(&s, &prefix, delim).await;

                let page = list_versions_batched(&s, "b", &prefix, delim, None, None, max, batch).await.unwrap();
                prop_assert_eq!(&names(&page.entries)[..], &want[..want.len().min(max)]);
                prop_assert_eq!(page.truncated, want.len() > max);

                // Paging with the next markers yields everything exactly once, with
                // `latest` set on each key's first version only.
                let (mut all, mut km, mut vm) = (Vec::new(), None::<String>, None::<String>);
                let mut seen = std::collections::HashSet::new();
                loop {
                    let page = list_versions_batched(&s, "b", &prefix, delim, km.as_deref(), vm.as_deref(), max, batch).await.unwrap();
                    prop_assert!(page.entries.len() <= max);
                    for e in &page.entries {
                        if let VersionEntry::Version { key, latest, .. } = e {
                            prop_assert_eq!(*latest, seen.insert(key.clone()));
                        }
                    }
                    all.extend(names(&page.entries));
                    if !page.truncated {
                        break;
                    }
                    km = page.next_key_marker;
                    vm = page.next_version_marker;
                }
                prop_assert_eq!(all, want);
                Ok(())
            })?;
        }
    }

    /// Counts backend scans.
    struct Counting(MemoryStorage, std::sync::atomic::AtomicUsize);

    #[async_trait::async_trait]
    impl Scan for Counting {
        async fn objects(
            &self,
            bucket: &str,
            prefix: &str,
            after: Option<&str>,
            limit: usize,
        ) -> Result<Vec<(String, ObjectMeta)>> {
            self.1.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.0.objects(bucket, prefix, after, limit).await
        }

        async fn versions(
            &self,
            bucket: &str,
            prefix: &str,
            after: Option<&str>,
            limit: usize,
        ) -> Result<Vec<(String, Vec<ObjectMeta>)>> {
            self.1.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.0.versions(bucket, prefix, after, limit).await
        }
    }

    #[test]
    fn one_scan_per_batch() {
        rt().block_on(async {
            let keys: Vec<String> = ["a/1", "a/2", "b", "c/1", "c/2", "d"]
                .map(String::from)
                .to_vec();
            let s = Counting(store(&keys).await, Default::default());
            let page = list_batched(&s, "b", "", Some("/"), None, 1000, 100)
                .await
                .unwrap();
            let names: Vec<_> = page.entries.iter().map(|e| e.name()).collect();
            assert_eq!(names, ["a/", "b", "c/", "d"]);
            assert_eq!(s.1.swap(0, std::sync::atomic::Ordering::Relaxed), 1);
            let page = list_versions_batched(&s, "b", "", Some("/"), None, None, 1000, 100)
                .await
                .unwrap();
            assert_eq!(page.entries.len(), 4);
            assert_eq!(s.1.load(std::sync::atomic::Ordering::Relaxed), 1);
        });
    }

    #[test]
    fn grouping_examples() {
        rt().block_on(async {
            let keys: Vec<String> = ["a/1", "a/2", "b", "c/d/e", "c/x"]
                .map(String::from)
                .to_vec();
            let s = store(&keys).await;
            let page = list(&s, "b", "", Some("/"), None, 1000).await.unwrap();
            let names: Vec<_> = page.entries.iter().map(|e| e.name()).collect();
            assert_eq!(names, ["a/", "b", "c/"]);
            let page = list(&s, "b", "c/", Some("/"), None, 1000).await.unwrap();
            let names: Vec<_> = page.entries.iter().map(|e| e.name()).collect();
            assert_eq!(names, ["c/d/", "c/x"]);
            let page = list(&s, "b", "", Some("/"), None, 1).await.unwrap();
            assert!(page.truncated);
            assert_eq!(page.next_marker.as_deref(), Some("a/"));
            let page = list(&s, "b", "", Some("/"), Some("a/"), 1).await.unwrap();
            assert_eq!(page.entries[0].name(), "b");
            let page = list(&s, "b", "", None, None, 0).await.unwrap();
            assert!(page.entries.is_empty() && !page.truncated);
        });
    }

    #[test]
    fn tokens_and_encoding() {
        assert_eq!(decode_token(&encode_token("a/é")).as_deref(), Some("a/é"));
        assert_eq!(decode_token("!!"), None);
        assert_eq!(url_encode("a b&<é/~"), "a%20b%26%3C%C3%A9/~");
    }
}
