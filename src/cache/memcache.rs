use crate::cache::DEFAULT_PAGE_CACHE_SIZE;
use crate::cache::EvictionPolicy;
use crate::cache::lru::LRU;
use crate::page::{Page, PageId};

use std::cell::UnsafeCell;
use std::collections::{HashMap, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use thiserror::Error;

// SAFETY:
// Shared and exclusive access are handled with a separate RwLock stored
// in PageMetadata, so it is safe to mark UnsafePage as Sync.
// In the future, consider looking at: https://github.com/rust-lang/rust/issues/95439
unsafe impl Sync for UnsafePage {}

// UnsafeCell<Page> has the same in-memory layout as Page.
// We could use RwLock<Page> but RwLock metadata would be stored
// next to Page data. This way we make sure pages are contiguous
// in-memory and no RwLock metadata is prepended or appended.
struct UnsafePage(UnsafeCell<Page>);

impl Default for UnsafePage {
    fn default() -> Self {
        // FIXME: create an invalid page
        Self(UnsafeCell::new(Page::new(0)))
    }
}

struct PageMetadata {
    latch: RwLock<()>,
    counter: AtomicUsize,
}

impl Default for PageMetadata {
    fn default() -> Self {
        Self {
            latch: RwLock::new(()),
            counter: AtomicUsize::new(0),
        }
    }
}

// SAFETY: see UnsafePage
unsafe impl Sync for UnsafePageMetadata {}
struct UnsafePageMetadata(UnsafeCell<PageMetadata>);

impl Default for UnsafePageMetadata {
    fn default() -> Self {
        Self(UnsafeCell::new(PageMetadata::default()))
    }
}

struct PageTable {
    map: HashMap<PageId, usize>,
    free_list: VecDeque<usize>,
}

impl Default for PageTable {
    fn default() -> Self {
        Self {
            map: HashMap::new(),
            free_list: VecDeque::from_iter(0..DEFAULT_PAGE_CACHE_SIZE),
        }
    }
}

pub struct MemCache {
    pages: Box<[UnsafePage]>,
    pages_metadata: Box<[UnsafePageMetadata]>,
    page_table: Mutex<PageTable>,
    eviction_policy: Box<dyn EvictionPolicy>,
}

#[derive(Error, Debug)]
pub enum MemCacheError {
    #[error("cache is full")]
    Full,
    #[error("page not found")]
    PageNotFound,
}

impl MemCache {
    pub fn new() -> Self {
        let pages = std::iter::repeat_with(UnsafePage::default).take(DEFAULT_PAGE_CACHE_SIZE);
        let pages_metadata =
            std::iter::repeat_with(UnsafePageMetadata::default).take(DEFAULT_PAGE_CACHE_SIZE);
        Self {
            pages: Box::from_iter(pages),
            pages_metadata: Box::from_iter(pages_metadata),
            page_table: Mutex::new(PageTable::default()),
            eviction_policy: Box::new(LRU::new()),
        }
    }

    unsafe fn get_page_ref(&self, idx: usize) -> &Page {
        unsafe { &*(self.pages[idx].0.get()) }
    }

    #[allow(clippy::mut_from_ref)]
    unsafe fn get_page_ref_mut(&self, idx: usize) -> &mut Page {
        unsafe { &mut *(self.pages[idx].0.get()) }
    }

    unsafe fn get_metadata_ref(&self, idx: usize) -> &PageMetadata {
        unsafe { &*(self.pages_metadata[idx].0.get()) }
    }

    #[allow(clippy::mut_from_ref)]
    unsafe fn get_metadata_ref_mut(&self, idx: usize) -> &mut PageMetadata {
        unsafe { &mut *(self.pages_metadata[idx].0.get()) }
    }

    pub fn get_page(&self, page_id: PageId) -> Result<PageRef<'_>, MemCacheError> {
        let page_table = self.page_table.lock().unwrap();
        let idx = *page_table
            .map
            .get(&page_id)
            .ok_or(MemCacheError::PageNotFound)?;

        let page = unsafe { self.get_page_ref(idx) };
        let metadata = unsafe { self.get_metadata_ref(idx) };
        let _guard = metadata.latch.read().unwrap();
        let counter = &metadata.counter;
        counter.fetch_add(1, Ordering::Relaxed);
        drop(page_table);

        self.eviction_policy.record_access(page_id);

        Ok(PageRef {
            _guard,
            counter,
            page,
        })
    }

    pub fn get_page_mut(&self, page_id: usize) -> Result<PageRefMut<'_>, MemCacheError> {
        let page_table = self.page_table.lock().unwrap();
        let idx = *page_table
            .map
            .get(&page_id)
            .ok_or(MemCacheError::PageNotFound)?;

        let page = unsafe { self.get_page_ref_mut(idx) };
        let metadata = unsafe { self.get_metadata_ref(idx) };
        let _guard = metadata.latch.write().unwrap();
        let counter = &metadata.counter;
        counter.fetch_add(1, Ordering::Relaxed);
        drop(page_table);

        self.eviction_policy.record_access(page_id);

        Ok(PageRefMut {
            _guard,
            counter,
            page,
        })
    }

    pub fn add_page(&self, page: &Page) -> Result<PageId, MemCacheError> {
        let mut page_table = self.page_table.lock().unwrap();
        let page_id = page.page_id();

        #[cfg(not(test))]
        assert!(page_table.map.contains_key(&page_id));

        let idx = page_table
            .free_list
            .pop_front()
            .ok_or(MemCacheError::Full)?;
        page_table.map.insert(page_id, idx);

        *unsafe { self.get_page_ref_mut(idx) } = Page::new(page_id);
        *unsafe { self.get_metadata_ref_mut(idx) } = PageMetadata::default();
        drop(page_table);

        self.eviction_policy.record_access(page_id);

        Ok(page_id)
    }

    pub fn remove_page(&self, page_id: PageId) -> Result<(), MemCacheError> {
        let mut page_table = self.page_table.lock().unwrap();
        let idx = page_table
            .map
            .get(&page_id)
            .copied()
            .ok_or(MemCacheError::PageNotFound)?;

        let metadata = unsafe { self.get_metadata_ref(idx) };
        let _guard = metadata.latch.write().unwrap();
        assert_eq!(metadata.counter.load(Ordering::Relaxed), 0);
        page_table.map.remove(&page_id);
        page_table.free_list.push_back(idx);

        Ok(())
    }

    pub fn pick_page_to_evict(&self) -> Option<PageId> {
        let page_table = self.page_table.lock().unwrap();

        if page_table.free_list.is_empty() {
            let page_id = self.eviction_policy.should_evict()?;
            let idx = *page_table.map.get(&page_id).unwrap();
            let metadata = unsafe { self.get_metadata_ref(idx) };

            if metadata.counter.load(Ordering::Relaxed) > 0 {
                return None;
            }
            drop(page_table);

            self.eviction_policy.evict();

            Some(page_id)
        } else {
            None
        }
    }
}

pub struct PageRef<'page> {
    _guard: RwLockReadGuard<'page, ()>,
    counter: &'page AtomicUsize,
    page: &'page Page,
}

pub struct PageRefMut<'page> {
    _guard: RwLockWriteGuard<'page, ()>,
    counter: &'page AtomicUsize,
    page: &'page mut Page,
}

impl Deref for PageRef<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        self.page
    }
}

impl Deref for PageRefMut<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        self.page
    }
}

impl DerefMut for PageRefMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.page
    }
}

impl Drop for PageRef<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drop for PageRefMut<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    #[test]
    fn test_concurrent_cache_access() {
        let cache = Arc::new(MemCache::new());

        let cache1 = cache.clone();
        let cache2 = cache.clone();
        let t1 = std::thread::spawn(move || {
            for _ in 1..100000 {
                let page = Page::new(0);
                let _ = cache1.add_page(&page);
            }
        });
        let t2 = std::thread::spawn(move || {
            for _ in 1..100000 {
                let _ = cache2.get_page(0);
            }
        });
        let t3 = std::thread::spawn(move || {
            for _ in 1..100000 {
                let _ = cache.remove_page(0);
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();
        t3.join().unwrap();
    }
}
