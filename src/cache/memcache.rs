use crate::cache::{DEFAULT_PAGE_CACHE_SIZE, EvictionPolicy, lru::LRU};
use crate::pages::{BTreeInnerPage, BTreeLeafPage, BTreeSuperBlock};
use crate::pages::{HeapPage, Page, PageId, PageMetadata};

use std::cell::UnsafeCell;
use std::collections::{HashMap, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use thiserror::Error;

// UnsafeCell<Page> has the same in-memory layout as Page.
// We could use RwLock<Page> but RwLock metadata would be stored
// next to Page data. This way we make sure pages are contiguous
// in-memory and no RwLock metadata is prepended or appended.
struct UnsafePage(UnsafeCell<Page>);
// SAFETY:
// Shared and exclusive access are handled with a separate RwLock stored
// in PageLatch, so it is safe to mark UnsafePage as Sync.
// In the future, consider looking at: https://github.com/rust-lang/rust/issues/95439
unsafe impl Sync for UnsafePage {}

impl Default for UnsafePage {
    fn default() -> Self {
        Self(UnsafeCell::new(Page::new()))
    }
}

struct UnsafePageMetadata(UnsafeCell<PageMetadata>);
// SAFETY: see UnsafePage
unsafe impl Sync for UnsafePageMetadata {}

impl UnsafePageMetadata {
    fn new(page_id: PageId) -> Self {
        Self(UnsafeCell::new(PageMetadata::new(page_id)))
    }
}

struct PageLatch {
    latch: RwLock<()>,
}

// struct UnsafePageLock(UnsafeCell<PageMetadata>);
// SAFETY: see UnsafePage
// unsafe impl Sync for UnsafePageMetadata {}

impl Default for PageLatch {
    fn default() -> Self {
        Self {
            latch: RwLock::new(()),
        }
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

pub struct PageRef<'page> {
    _guard: RwLockReadGuard<'page, ()>,
    page: &'page Page,
    metadata: &'page PageMetadata,
    eviction_policy: &'page Mutex<dyn EvictionPolicy>,
}

impl PageRef<'_> {
    pub fn page(&self) -> &Page {
        self.page
    }

    pub fn metadata(&self) -> &PageMetadata {
        self.metadata
    }

    pub fn heap_page(&self) -> &HeapPage {
        self.page().into()
    }

    pub fn btree_superblock(&self) -> &BTreeSuperBlock {
        self.page().into()
    }

    pub fn btree_inner_page(&self) -> &BTreeInnerPage {
        self.page().into()
    }

    pub fn btree_leaf_page(&self) -> &BTreeLeafPage {
        self.page().into()
    }
}

pub struct PageRefMut<'page> {
    _guard: RwLockWriteGuard<'page, ()>,
    page: &'page mut Page,
    metadata: &'page mut PageMetadata,
    eviction_policy: &'page Mutex<dyn EvictionPolicy>,
}

impl PageRefMut<'_> {
    pub fn page(&self) -> &Page {
        self.page
    }

    pub fn page_mut(&mut self) -> &mut Page {
        self.page
    }

    pub fn metadata(&self) -> &PageMetadata {
        self.metadata
    }

    pub fn metadata_mut(&mut self) -> &mut PageMetadata {
        self.metadata
    }

    pub fn heap_page(&self) -> &HeapPage {
        self.page().into()
    }

    pub fn heap_page_mut(&mut self) -> &mut HeapPage {
        self.page_mut().into()
    }

    pub fn btree_superblock(&self) -> &BTreeSuperBlock {
        self.page().into()
    }

    pub fn btree_superblock_mut(&mut self) -> &mut BTreeSuperBlock {
        self.page_mut().into()
    }

    pub fn btree_inner_page(&self) -> &BTreeInnerPage {
        self.page().into()
    }

    pub fn btree_inner_page_mut(&mut self) -> &mut BTreeInnerPage {
        self.page_mut().into()
    }

    pub fn btree_leaf_page(&self) -> &BTreeLeafPage {
        self.page().into()
    }

    pub fn btree_leaf_page_mut(&mut self) -> &mut BTreeLeafPage {
        self.page_mut().into()
    }
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
        self.metadata.unpin();
        if self.metadata.get_pin_counter() == 0 {
            self.eviction_policy
                .lock()
                .unwrap()
                .set_evictable(self.metadata.page_id)
        }
    }
}

impl Drop for PageRefMut<'_> {
    fn drop(&mut self) {
        let old_counter = self.metadata.unpin();
        assert_eq!(old_counter, 1);
        if self.metadata.get_pin_counter() == 0 {
            self.eviction_policy
                .lock()
                .unwrap()
                .set_evictable(self.metadata.page_id);
        }
    }
}

pub struct MemCache {
    pages: Box<[UnsafePage]>,
    pages_metadata: Box<[UnsafePageMetadata]>,
    pages_latch: Box<[PageLatch]>,
    page_table: Mutex<PageTable>,
    eviction_policy: Box<Mutex<dyn EvictionPolicy>>,
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
            // FIXME: create an invalid page ?
            std::iter::repeat_with(|| UnsafePageMetadata::new(0)).take(DEFAULT_PAGE_CACHE_SIZE);
        let pages_lock = std::iter::repeat_with(PageLatch::default).take(DEFAULT_PAGE_CACHE_SIZE);
        Self {
            pages: Box::from_iter(pages),
            pages_metadata: Box::from_iter(pages_metadata),
            pages_latch: Box::from_iter(pages_lock),
            page_table: Mutex::new(PageTable::default()),
            eviction_policy: Box::new(Mutex::new(LRU::new())),
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
        let idx = {
            let page_table = self.page_table.lock().unwrap();
            page_table
                .map
                .get(&page_id)
                .copied()
                .ok_or(MemCacheError::PageNotFound)?
        };

        let latch = &self.pages_latch[idx].latch;
        let _guard = latch.read().unwrap();
        let page = unsafe { self.get_page_ref(idx) };
        let metadata = unsafe { self.get_metadata_ref(idx) };
        metadata.pin();

        {
            let mut eviction_policy = self.eviction_policy.lock().unwrap();
            eviction_policy.record_access(page_id);
            eviction_policy.set_unevictable(page_id);
        }

        Ok(PageRef {
            _guard,
            page,
            metadata,
            eviction_policy: &self.eviction_policy,
        })
    }

    pub fn get_page_mut(&self, page_id: PageId) -> Result<PageRefMut<'_>, MemCacheError> {
        let idx = {
            let page_table = self.page_table.lock().unwrap();
            page_table
                .map
                .get(&page_id)
                .copied()
                .ok_or(MemCacheError::PageNotFound)?
        };

        let latch = &self.pages_latch[idx].latch;
        let _guard = latch.write().unwrap();
        let page = unsafe { self.get_page_ref_mut(idx) };
        let metadata = unsafe { self.get_metadata_ref_mut(idx) };
        let old_counter = metadata.pin();
        assert_eq!(old_counter, 0);

        {
            let mut eviction_policy = self.eviction_policy.lock().unwrap();
            eviction_policy.record_access(page_id);
            eviction_policy.set_unevictable(page_id);
        }

        Ok(PageRefMut {
            _guard,
            page,
            metadata,
            eviction_policy: &self.eviction_policy,
        })
    }

    pub fn new_page_mut(&self, page_id: PageId) -> Result<PageRefMut<'_>, MemCacheError> {
        let idx = {
            let mut page_table = self.page_table.lock().unwrap();
            page_table
                .free_list
                .pop_front()
                .ok_or(MemCacheError::Full)?
        };

        let latch = &self.pages_latch[idx].latch;
        let _guard = latch.write().unwrap();
        let page = unsafe { self.get_page_ref_mut(idx) };
        let metadata = unsafe { self.get_metadata_ref_mut(idx) };
        *metadata = PageMetadata::new(page_id);
        let old_counter = metadata.pin();
        assert_eq!(old_counter, 0);

        {
            let mut page_table = self.page_table.lock().unwrap();
            assert!(!page_table.map.contains_key(&page_id));
            page_table.map.insert(page_id, idx);
        }

        {
            let mut eviction_policy = self.eviction_policy.lock().unwrap();
            eviction_policy.record_access(page_id);
            eviction_policy.set_unevictable(page_id);
        }

        Ok(PageRefMut {
            _guard,
            page,
            metadata,
            eviction_policy: &self.eviction_policy,
        })
    }

    pub fn remove_page(&self, page_id: PageId) -> Result<(), MemCacheError> {
        let idx = {
            let mut page_table = self.page_table.lock().unwrap();
            page_table
                .map
                .remove(&page_id)
                .ok_or(MemCacheError::PageNotFound)?
        };

        let latch = &self.pages_latch[idx].latch;
        let _guard = latch.write().unwrap();
        let metadata = unsafe { self.get_metadata_ref(idx) };
        assert_eq!(metadata.get_pin_counter(), 0);

        self.eviction_policy.lock().unwrap().remove(page_id);
        {
            let mut page_table = self.page_table.lock().unwrap();
            page_table.free_list.push_back(idx);
        }

        Ok(())
    }

    pub fn evict(&self) -> Option<PageId> {
        let page_table = self.page_table.lock().unwrap();

        if page_table.free_list.is_empty() {
            self.eviction_policy.lock().unwrap().evict()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    #[test]
    fn high_contention_scenario() {
        let cache = Arc::new(MemCache::new());

        let mut handles = vec![];

        for thread_id in 0..16 {
            let cache = cache.clone();
            let handle = std::thread::spawn(move || {
                for j in 0..DEFAULT_PAGE_CACHE_SIZE / 2 {
                    let page_id = j as PageId;
                    match thread_id {
                        0 => {
                            let _ = cache.new_page_mut(page_id);
                        }
                        1 => {
                            let _ =
                                cache.new_page_mut(DEFAULT_PAGE_CACHE_SIZE as u32 / 2 + page_id);
                        }
                        2..6 => {
                            let _ = cache.get_page_mut(page_id);
                        }
                        6..8 => {
                            let _ = cache.remove_page(page_id);
                        }
                        _ => {
                            let _ = cache.get_page(page_id);
                        }
                    }
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
