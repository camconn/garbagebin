//! Per-key async mutexes. Entries are dropped once nobody holds or waits on them.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

#[derive(Default)]
pub struct KeyLocks {
    map: Mutex<HashMap<Vec<u8>, Arc<AsyncMutex<()>>>>,
}

pub struct KeyGuard<'a> {
    locks: &'a KeyLocks,
    key: Vec<u8>,
    guard: Option<OwnedMutexGuard<()>>,
}

impl KeyLocks {
    pub async fn lock(&self, key: Vec<u8>) -> KeyGuard<'_> {
        let m = {
            let mut map = self.map.lock().unwrap_or_else(|e| e.into_inner());
            Arc::clone(map.entry(key.clone()).or_default())
        };
        let guard = Some(m.lock_owned().await);
        KeyGuard {
            locks: self,
            key,
            guard,
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.map.lock().unwrap().len()
    }
}

impl Drop for KeyGuard<'_> {
    fn drop(&mut self) {
        self.guard.take();
        let mut map = self.locks.map.lock().unwrap_or_else(|e| e.into_inner());
        if map
            .get(&self.key)
            .is_some_and(|m| Arc::strong_count(m) == 1)
        {
            map.remove(&self.key);
        }
    }
}
