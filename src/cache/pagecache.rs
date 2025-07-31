use crate::cache::memcache::MemCache;
use crate::page::{Page, PageId};
use crate::storage::{Storage, StorageError};

use std::sync::Mutex;

use super::memcache::{MemCacheError, PageRef, PageRefMut};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PageCacheError {
    #[error("storage")]
    Storage(#[from] StorageError),
    #[error("memcache")]
    MemCache(#[from] MemCacheError),
}

pub struct PageCache {
    storage: Mutex<Storage>,
    mem_cache: MemCache,
}

impl PageCache {
    pub fn new(storage: Storage) -> Self {
        Self {
            storage: Mutex::new(storage),
            mem_cache: MemCache::new(),
        }
    }

    pub fn new_page(&self) -> Result<PageRefMut, PageCacheError> {
        let mut storage = self.storage.lock().unwrap();
        let page_id = storage.last_page_id();
        let page = Page::new();
        // FIXME: needed for self.storage.last_page_id()
        storage.write_page(&page, page_id)?;

        // try evict a page if the memory cache is full
        while let Some(page_id) = self.mem_cache.pick_page_to_evict() {
            let Ok(page) = self.mem_cache.get_page(page_id) else {
                continue;
            };
            storage.write_page(&page, page_id)?;
            storage.flush();
            drop(page);

            if self.mem_cache.remove_page(page_id).is_err() {
                continue;
            }
            break;
        }
        drop(storage);

        self.mem_cache
            .new_page(page_id)
            .map_err(PageCacheError::MemCache)
    }

    pub fn get_page(&self, page_id: PageId) -> Result<PageRef, PageCacheError> {
        if let Ok(page) = self.mem_cache.get_page(page_id) {
            Ok(page)
        } else {
            let page = self
                .storage
                .lock()
                .unwrap()
                .read_page(page_id)
                .map_err(PageCacheError::Storage)?;

            let page_id = self
                .mem_cache
                .add_page(&page, page_id)
                .map_err(PageCacheError::MemCache)?;
            self.get_page(page_id)
        }
    }

    pub fn get_page_mut(&self, page_id: PageId) -> Result<PageRefMut, PageCacheError> {
        if let Ok(page) = self.mem_cache.get_page_mut(page_id) {
            Ok(page)
        } else {
            let page = self
                .storage
                .lock()
                .unwrap()
                .read_page(page_id)
                .map_err(PageCacheError::Storage)?;

            let _ = self
                .mem_cache
                .add_page(&page, page_id)
                .map_err(PageCacheError::MemCache);
            self.mem_cache
                .get_page_mut(page_id)
                .map_err(PageCacheError::MemCache)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::DEFAULT_PAGE_CACHE_SIZE;

    use std::path::PathBuf;

    fn test_path() -> PathBuf {
        [
            "/tmp/",
            "joujoudb_",
            uuid::Uuid::new_v4().to_string().as_str(),
        ]
        .into_iter()
        .collect::<String>()
        .into()
    }

    #[test]
    fn test_eviction() {
        let storage = Storage::open(test_path()).unwrap();
        let page_cache = PageCache::new(storage);

        // fill cache
        for _ in 0..DEFAULT_PAGE_CACHE_SIZE {
            page_cache.new_page().unwrap();
        }

        let _ = page_cache.get_page(0).unwrap();
        let _ = page_cache.get_page(1).unwrap();

        // page 2 should be evicted since it's the oldest non used page
        assert_eq!(page_cache.mem_cache.pick_page_to_evict(), Some(2));
        let _ = page_cache.new_page().unwrap();
    }
}
