use crate::cache::memcache::MemCache;
use crate::pages::PageId;
use crate::storage::{Storage, StorageError};

use parking_lot::Mutex;

use super::memcache::{MemCacheError, PageRef, PageRefMut};
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
pub struct PageCache {
    storage: Mutex<Storage>,
    mem_cache: MemCache,
}

impl PageCache {
    /// Creates a new `PageCache` with the given storage backend.
    pub fn try_new(storage: Storage) -> Result<Self, PageCacheError> {
        Ok(Self {
            storage: Mutex::new(storage),
            mem_cache: MemCache::try_new().map_err(PageCacheError::MemCache)?,
        })
    }

    /// Creates a new page, both in the cache and on disk.
    ///
    /// If the cache is full, it will try to evict a page to make space.
    ///
    /// Returns a mutable reference to the new page.
    pub fn new_page(&self) -> Result<PageRefMut<'_>, PageCacheError> {
        let mut storage = self.storage.lock();
        let page_id = storage.allocate_page();

        // try evict a page if the memory cache is full
        // FIXME: race condition
        if let Some(page_id) = self.mem_cache.evict() {
            if let Ok(page) = self.mem_cache.get_page(page_id) {
                storage.write_page(&page, page_id)?;
                storage.flush();
            };

            self.mem_cache.remove_page(page_id)?;
        }

        self.mem_cache
            .new_page_mut(page_id)
            .map_err(PageCacheError::MemCache)
    }

    /// Retrieves a a read-only reference to a page from the cache.
    ///
    /// If the page is not in the cache, it will be fetched from the disk.
    pub fn get_page(&self, page_id: PageId) -> Result<PageRef<'_>, PageCacheError> {
        if let Ok(page) = self.mem_cache.get_page(page_id) {
            Ok(page)
        } else {
            let mut new_page_ref = self
                .mem_cache
                .new_page_mut(page_id)
                .map_err(PageCacheError::MemCache)?;

            {
                let mut storage = self.storage.lock();
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
    pub fn get_page_mut(&self, page_id: PageId) -> Result<PageRefMut<'_>, PageCacheError> {
        if let Ok(page) = self.mem_cache.get_page_mut(page_id) {
            Ok(page)
        } else {
            let mut new_page_ref = self
                .mem_cache
                .new_page_mut(page_id)
                .map_err(PageCacheError::MemCache)?;

            let mut storage = self.storage.lock();
            storage
                .read_page(page_id, new_page_ref.page_mut())
                .map_err(PageCacheError::Storage)?;
            drop(storage);

            Ok(new_page_ref)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::cache::DEFAULT_PAGE_CACHE_SIZE;
    use crate::pages::PAGE_RESERVED;

    use tempfile::NamedTempFile;

    #[test]
    fn evict_page_lru() {
        let storage_path = NamedTempFile::new().unwrap();
        let storage = Storage::open(storage_path).unwrap();
        let page_cache = PageCache::try_new(storage).unwrap();

        // Page 0 is reserved and not allocatable via new_page().
        for _ in 1..DEFAULT_PAGE_CACHE_SIZE {
            page_cache.new_page().unwrap();
        }

        let page0 = page_cache.get_page(PAGE_RESERVED).unwrap();
        let page1 = page_cache.get_page(1).unwrap();

        // Page 2 should be evicted since it's the oldest non used page.
        assert_eq!(page_cache.mem_cache.evict(), Some(2));
        drop(page0);
        drop(page1);
    }
}
