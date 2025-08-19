use crate::cache::{EvictionPolicy, lru::LRU};
use crate::config::CONFIG;
use crate::pages::{BTreeInnerPage, BTreeLeafPage, BTreeSuperBlock, PAGE_INVALID, PAGE_SIZE};
use crate::pages::{HeapPage, Page, PageId, PageMetadata};

use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::cell::UnsafeCell;
use std::collections::{HashMap, VecDeque};
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::slice;

use memmap2::MmapMut;
use thiserror::Error;

// SAFETY:
// 1. Pages are stored in a memory-mapped region and accessed through raw pointers.
// 2. Shared and exclusive access to pages and pages metadata are handled with a separate RwLock stored
//    in PageLatch.
// 3. Memory mapping is managed by memmap2 which ensures the memory is valid for the lifetime
//    of the MmapMut object.
// 4. Page references are only created with proper synchronization through the page latch.

// In the future, consider looking at: https://github.com/rust-lang/rust/issues/95439
struct UnsafePageMetadata(UnsafeCell<PageMetadata>);
unsafe impl Sync for UnsafePageMetadata {}

impl UnsafePageMetadata {
    fn new(page_id: PageId) -> Self {
        Self(UnsafeCell::new(PageMetadata::new(page_id)))
    }
}

struct PageLatch {
    latch: RwLock<()>,
}

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
            free_list: VecDeque::from_iter(0..CONFIG.PAGE_CACHE_SIZE),
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

impl<'page> PageRefMut<'page> {
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

    pub fn downgrade(self) -> PageRef<'page> {
        let this = ManuallyDrop::new(self);

        // SAFETY: The references are valid for the lifetime 'page because we still hold the lock.
        // Don't drop `this` with ManuallyDrop::drop(this), the Drop implementation of PageRef
        // would call metadata.unpin() and drop the guard.
        let _guard = RwLockWriteGuard::downgrade(unsafe { std::ptr::read(&this._guard) });
        let page = unsafe { &*(this.page as *const Page) };
        let metadata = unsafe { &*(this.metadata as *const PageMetadata) };
        let eviction_policy =
            unsafe { &*(this.eviction_policy as *const Mutex<dyn EvictionPolicy>) };

        PageRef {
            _guard,
            page,
            metadata,
            eviction_policy,
        }
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
                .set_evictable(self.metadata.page_id);
        }
    }
}

pub struct MemCache {
    pages: MmapMut,
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
    #[error("mmap failed")]
    MmapFailed(#[from] std::io::Error),
}

impl MemCache {
    pub fn try_new() -> Result<Self, MemCacheError> {
        let pages = MmapMut::map_anon(CONFIG.PAGE_CACHE_SIZE * PAGE_SIZE)
            .map_err(MemCacheError::MmapFailed)?;
        let pages_metadata = std::iter::repeat_with(|| UnsafePageMetadata::new(PAGE_INVALID))
            .take(CONFIG.PAGE_CACHE_SIZE);
        let pages_lock = std::iter::repeat_with(PageLatch::default).take(CONFIG.PAGE_CACHE_SIZE);

        Ok(Self {
            pages,
            pages_metadata: Box::from_iter(pages_metadata),
            pages_latch: Box::from_iter(pages_lock),
            page_table: Mutex::new(PageTable::default()),
            eviction_policy: Box::new(Mutex::new(LRU::new())),
        })
    }

    fn get_page_ref(&self, idx: usize) -> &Page {
        let pages = unsafe {
            std::slice::from_raw_parts(self.pages.as_ptr() as *const Page, CONFIG.PAGE_CACHE_SIZE)
        };

        debug_assert!(idx < CONFIG.PAGE_CACHE_SIZE);
        unsafe { pages.get_unchecked(idx) }
    }

    #[allow(clippy::mut_from_ref)]
    fn get_page_ref_mut(&self, idx: usize) -> &mut Page {
        let pages: &mut [Page] = unsafe {
            slice::from_raw_parts_mut(self.pages.as_ptr() as *mut Page, CONFIG.PAGE_CACHE_SIZE)
        };

        debug_assert!(idx < CONFIG.PAGE_CACHE_SIZE);
        unsafe { pages.get_unchecked_mut(idx) }
    }

    fn get_metadata_ref(&self, idx: usize) -> &PageMetadata {
        unsafe { &*(self.pages_metadata[idx].0.get()) }
    }

    #[allow(clippy::mut_from_ref)]
    fn get_metadata_ref_mut(&self, idx: usize) -> &mut PageMetadata {
        unsafe { &mut *(self.pages_metadata[idx].0.get()) }
    }

    pub fn get_page(&self, page_id: PageId) -> Result<PageRef<'_>, MemCacheError> {
        let idx = {
            let page_table = self.page_table.lock();
            page_table
                .map
                .get(&page_id)
                .copied()
                .ok_or(MemCacheError::PageNotFound)?
        };

        let latch = &self.pages_latch[idx].latch;
        let _guard = latch.read();
        let page = self.get_page_ref(idx);
        let metadata = self.get_metadata_ref(idx);
        metadata.pin();

        {
            let mut eviction_policy = self.eviction_policy.lock();
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
            let page_table = self.page_table.lock();
            page_table
                .map
                .get(&page_id)
                .copied()
                .ok_or(MemCacheError::PageNotFound)?
        };

        let latch = &self.pages_latch[idx].latch;
        let _guard = latch.write();
        let page = self.get_page_ref_mut(idx);
        let metadata = self.get_metadata_ref_mut(idx);
        let old_counter = metadata.pin();
        assert_eq!(old_counter, 0);

        {
            let mut eviction_policy = self.eviction_policy.lock();
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
            let mut page_table = self.page_table.lock();
            page_table
                .free_list
                .pop_front()
                .ok_or(MemCacheError::Full)?
        };

        let latch = &self.pages_latch[idx].latch;
        let _guard = latch.write();
        let page = self.get_page_ref_mut(idx);
        let metadata = self.get_metadata_ref_mut(idx);
        *metadata = PageMetadata::new(page_id);
        let old_counter = metadata.pin();
        assert_eq!(old_counter, 0);

        {
            let mut page_table = self.page_table.lock();
            assert!(!page_table.map.contains_key(&page_id));
            page_table.map.insert(page_id, idx);
        }

        {
            let mut eviction_policy = self.eviction_policy.lock();
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
            let mut page_table = self.page_table.lock();
            page_table
                .map
                .remove(&page_id)
                .ok_or(MemCacheError::PageNotFound)?
        };

        let latch = &self.pages_latch[idx].latch;
        let _guard = latch.write();
        let metadata = self.get_metadata_ref(idx);
        assert_eq!(metadata.get_pin_counter(), 0);

        self.eviction_policy.lock().remove(page_id);
        {
            let mut page_table = self.page_table.lock();
            page_table.free_list.push_back(idx);
        }

        Ok(())
    }

    pub fn evict(&self) -> Option<PageId> {
        let page_table = self.page_table.lock();

        if page_table.free_list.is_empty() {
            self.eviction_policy.lock().evict()
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
        let cache = Arc::new(MemCache::try_new().unwrap());

        let mut handles = vec![];

        for thread_id in 0..16 {
            let cache = cache.clone();
            let handle = std::thread::spawn(move || {
                for j in 0..CONFIG.PAGE_CACHE_SIZE / 2 {
                    let page_id = j as PageId;
                    match thread_id {
                        0 => {
                            let _ = cache.new_page_mut(page_id);
                        }
                        1 => {
                            let _ = cache.new_page_mut(CONFIG.PAGE_CACHE_SIZE as u32 / 2 + page_id);
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
