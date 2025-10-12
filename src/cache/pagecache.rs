use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock};
use std::thread::JoinHandle;

use crate::cache::memcache::MemCache;
use crate::config::CONFIG;
use crate::pages::{PageId, PageMetadata};
use crate::storage::{FileStorage, StorageBackend, StorageError, StorageId};

use super::memcache::{MemCacheError, PageRef, PageRefMut};
use parking_lot::{Mutex, RwLock};
use thiserror::Error;

pub static GLOBAL_PAGE_CACHE: LazyLock<PageCache<FileStorage>> =
    LazyLock::new(|| PageCache::try_new().expect("Could not initialize global page cache"));

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
pub struct PageCache<S: StorageBackend + 'static> {
    inner: Arc<PageCacheInner<S>>,
}

impl<S: StorageBackend + 'static> PageCache<S> {
    /// Creates a new `PageCache`.
    pub fn try_new() -> Result<Self, PageCacheError> {
        let pagecache = Self {
            inner: Arc::new(PageCacheInner {
                next_storage_id: AtomicU32::new(0),
                storage_backends: RwLock::new(HashMap::new()),
                mem_cache: MemCache::try_new().map_err(PageCacheError::MemCache)?,
                dirty_pages: Mutex::new(None),
                writeback_jh: Mutex::new(None),
            }),
        };
        let jh = Self::writeback_thread(&pagecache);
        *pagecache.writeback_jh.lock() = Some(jh);

        Ok(pagecache)
    }

    /// Adds a storage backend to the shared page cache.
    ///
    /// Returns a page cache for the storage given.
    pub fn cache_storage(&self, storage: S) -> StoragePageCache<S> {
        let storage_id = StorageId(self.next_storage_id.fetch_add(1, Ordering::Relaxed));
        self.storage_backends.write().insert(storage_id, storage);
        StoragePageCache {
            pagecache: PageCache {
                inner: Arc::clone(&self.inner),
            },
            storage_id,
        }
    }

    /// Runs a background thread to write dirty pages to storage.
    ///
    /// Thread stops when `Arc::strong_count(&pagecache) == 0`.
    fn writeback_thread(&self) -> JoinHandle<()> {
        let weak = Arc::downgrade(&self.inner);
        std::thread::spawn(move || {
            while let Some(pagecache) = weak.upgrade() {
                pagecache.writeback_dirty_pages();
                drop(pagecache);
                std::thread::sleep(CONFIG.WRITEBACK_INTERVAL_MS);
            }
        })
    }
}

impl<S: StorageBackend + 'static> std::ops::Deref for PageCache<S> {
    type Target = PageCacheInner<S>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct PageCacheInner<S: StorageBackend + 'static> {
    next_storage_id: AtomicU32,
    storage_backends: RwLock<HashMap<StorageId, S>>,
    mem_cache: MemCache,
    dirty_pages: Mutex<Option<HashMap<StorageId, BTreeSet<PageId>>>>,
    writeback_jh: Mutex<Option<JoinHandle<()>>>,
}

impl<S: StorageBackend + 'static> Drop for PageCacheInner<S> {
    fn drop(&mut self) {
        if let Some(jh) = self.writeback_jh.lock().take() {
            let _ = jh.join();
        }
        self.writeback_dirty_pages();
    }
}

impl<S: StorageBackend + 'static> PageCacheInner<S> {
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

    pub fn set_page_dirty(&self, storage_id: StorageId, metadata: &PageMetadata) {
        metadata.set_dirty();
        self.dirty_pages
            .lock()
            .get_or_insert_default()
            .entry(storage_id)
            .and_modify(|h| {
                h.insert(metadata.page_id);
            })
            .or_insert(BTreeSet::from([metadata.page_id]));
    }

    fn writeback_dirty_pages(&self) {
        // Storage io can block: get dirty pages and release the lock.
        let dirty_pages = self.dirty_pages.lock().take();
        if let Some(dirty_pages) = dirty_pages {
            for (storage_id, page_ids) in dirty_pages {
                let guard = self.storage_backends.read();
                let storage = guard.get(&storage_id).unwrap();

                for page_id in page_ids {
                    let page_ref = self
                        .get_page(storage_id, page_id)
                        .expect("writeback failed");
                    if page_ref.metadata().is_dirty() {
                        storage
                            .write_page(page_ref.page(), page_id)
                            .expect("write_page failed");
                        page_ref.metadata().clear_dirty();
                    }
                }
                storage.fsync();
            }
        }
    }

    /// Retrives the first page from the storage backend.
    pub fn first_page_id(&self, storage_id: StorageId) -> PageId {
        let guard = self.storage_backends.read();
        let storage = guard.get(&storage_id).unwrap();
        storage.first_page_id()
    }

    /// Retrieves the last page id from the storage backend.
    pub fn last_page_id(&self, storage_id: StorageId) -> PageId {
        let guard = self.storage_backends.read();
        let storage = guard.get(&storage_id).unwrap();
        storage.last_page_id()
    }
}

impl<S: StorageBackend> Clone for PageCache<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// A page cache for a `StorageBackend` (a file for example) backed by a global `PageCache`.
///
/// Created with `PageCache::cache_storage`.
pub struct StoragePageCache<S: StorageBackend + 'static> {
    pagecache: PageCache<S>,
    storage_id: StorageId,
}

impl<S: StorageBackend> Clone for StoragePageCache<S> {
    fn clone(&self) -> Self {
        Self {
            pagecache: self.pagecache.clone(),
            storage_id: self.storage_id,
        }
    }
}

impl<S: StorageBackend + 'static> StoragePageCache<S> {
    pub fn new_page(&self) -> Result<PageRefMut<'_>, PageCacheError> {
        self.pagecache.new_page(self.storage_id)
    }

    pub fn get_page(&self, page_id: PageId) -> Result<PageRef<'_>, PageCacheError> {
        self.pagecache.get_page(self.storage_id, page_id)
    }

    pub fn set_page_dirty(&self, metadata: &PageMetadata) {
        self.pagecache.set_page_dirty(self.storage_id, metadata);
    }

    pub fn get_page_mut(&self, page_id: PageId) -> Result<PageRefMut<'_>, PageCacheError> {
        self.pagecache.get_page_mut(self.storage_id, page_id)
    }
    pub fn first_page_id(&self) -> PageId {
        self.pagecache.first_page_id(self.storage_id)
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
