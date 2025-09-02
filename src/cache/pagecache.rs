use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::cache::memcache::MemCache;
use crate::pages::PageId;
use crate::storage::StorageId;
use crate::storage::{StorageBackend, StorageError};

use super::memcache::{MemCacheError, PageRef, PageRefMut};
use parking_lot::RwLock;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PageCacheError {
    #[error("storage")]
    Storage(#[from] StorageError),
    #[error("memcache")]
    MemCache(#[from] MemCacheError),
}

/// A cache that manages pages in memory and interacts with the on-disk storage.
///
/// The `PageCache` is responsible for:
/// - Fetching pages from the disk and loading them into memory.
/// - Evicting pages from memory when the cache is full.
/// - Writing dirty pages back to the disk.
pub struct PageCache<S: StorageBackend> {
    next_storage_id: AtomicU32,
    storage_backends: RwLock<HashMap<StorageId, S>>,
    mem_cache: MemCache,
}

impl<S: StorageBackend> PageCache<S> {
    /// Creates a new `PageCache`.
    pub fn try_new() -> Result<Self, PageCacheError> {
        Ok(Self {
            next_storage_id: AtomicU32::new(0),
            storage_backends: RwLock::new(HashMap::new()),
            mem_cache: MemCache::try_new().map_err(PageCacheError::MemCache)?,
        })
    }

    /// Adds a n
    pub fn cache_storage<'pagecache>(
        &'pagecache self,
        storage: S,
    ) -> StoragePageCache<'pagecache, S> {
        let storage_id = StorageId(self.next_storage_id.fetch_add(1, Ordering::Relaxed));
        self.storage_backends.write().insert(storage_id, storage);
        StoragePageCache {
            pagecache: self,
            storage_id,
        }
    }

    /// Creates a new page, both in the cache and on disk.
    ///
    /// If the cache is full, it will try to evict a page to make space.
    ///
    /// Returns a mutable reference to the new page.
    pub fn new_page(&self, storage_id: StorageId) -> Result<PageRefMut<'_>, PageCacheError> {
        let guard = self.storage_backends.read();
        let storage = guard.get(&storage_id).unwrap();
        let page_id = storage.allocate_page();

        // try evict a page if the memory cache is full
        // FIXME: race condition
        if let Some((storage_id, page_id)) = self.mem_cache.evict() {
            if let Ok(page) = self.mem_cache.get_page(storage_id, page_id) {
                storage.write_page(&page, page_id)?;
                storage.fsync();
            };

            self.mem_cache.remove_page(storage_id, page_id)?;
        }

        self.mem_cache
            .new_page_mut(storage_id, page_id)
            .map_err(PageCacheError::MemCache)
    }

    /// Retrieves a a read-only reference to a page from the cache.
    ///
    /// If the page is not in the cache, it will be fetched from the disk.
    pub fn get_page(
        &self,
        storage_id: StorageId,
        page_id: PageId,
    ) -> Result<PageRef<'_>, PageCacheError> {
        if let Ok(page) = self.mem_cache.get_page(storage_id, page_id) {
            Ok(page)
        } else {
            let mut new_page_ref = self
                .mem_cache
                .new_page_mut(storage_id, page_id)
                .map_err(PageCacheError::MemCache)?;

            {
                let guard = self.storage_backends.read();
                let storage = guard.get(&storage_id).unwrap();
                storage
                    .read_page(page_id, new_page_ref.page_mut())
                    .map_err(PageCacheError::Storage)?;
            }

            Ok(new_page_ref.downgrade())
        }
    }

    /// Retrieves a mutable reference to a page from the cache.
    ///
    /// If the page is not in the cache, it will be fetched from the disk.
    pub fn get_page_mut(
        &self,
        storage_id: StorageId,
        page_id: PageId,
    ) -> Result<PageRefMut<'_>, PageCacheError> {
        if let Ok(page) = self.mem_cache.get_page_mut(storage_id, page_id) {
            Ok(page)
        } else {
            let mut new_page_ref = self
                .mem_cache
                .new_page_mut(storage_id, page_id)
                .map_err(PageCacheError::MemCache)?;

            let guard = self.storage_backends.read();
            let storage = guard.get(&storage_id).unwrap();
            storage
                .read_page(page_id, new_page_ref.page_mut())
                .map_err(PageCacheError::Storage)?;

            Ok(new_page_ref)
        }
    }

    /// Retrieves the last page id from the storage backend.
    pub fn last_page_id(&self, storage_id: StorageId) -> PageId {
        let guard = self.storage_backends.read();
        let storage = guard.get(&storage_id).unwrap();
        storage.last_page_id()
    }
}

/// A page cache for a `StorageBackend` (a file for example) backed by a global `PageCache`.
///
/// Created with `PageCache::cache_storage`.
pub struct StoragePageCache<'pagecache, S: StorageBackend> {
    pagecache: &'pagecache PageCache<S>,
    storage_id: StorageId,
}

impl<'pagecache, S: StorageBackend> StoragePageCache<'pagecache, S> {
    pub fn new_page(&self) -> Result<PageRefMut<'_>, PageCacheError> {
        self.pagecache.new_page(self.storage_id)
    }

    pub fn get_page(&self, page_id: PageId) -> Result<PageRef<'_>, PageCacheError> {
        self.pagecache.get_page(self.storage_id, page_id)
    }

    pub fn get_page_mut(&self, page_id: PageId) -> Result<PageRefMut<'_>, PageCacheError> {
        self.pagecache.get_page_mut(self.storage_id, page_id)
    }

    pub fn last_page_id(&self) -> PageId {
        self.pagecache.last_page_id(self.storage_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::cache::DEFAULT_PAGE_CACHE_SIZE;
    use crate::pages::PAGE_RESERVED;
    use crate::storage::FileStorage;

    use tempfile::NamedTempFile;

    #[test]
    fn evict_page_lru() {
        let storage_path = NamedTempFile::new().unwrap();
        let storage = FileStorage::create(storage_path).unwrap();
        let page_cache = PageCache::try_new().unwrap();
        let file_cache = page_cache.cache_storage(storage);

        // Page 0 is reserved and not allocatable via new_page().
        for _ in 1..DEFAULT_PAGE_CACHE_SIZE {
            file_cache.new_page().unwrap();
        }

        let page0 = file_cache.get_page(PAGE_RESERVED).unwrap();
        let page1 = file_cache.get_page(PageId::new(1)).unwrap();

        // Page 2 should be evicted since it's the oldest non used page.
        assert_eq!(
            page_cache.mem_cache.evict(),
            Some((StorageId(0), PageId::new(2)))
        );
        drop(page0);
        drop(page1);
    }
}
