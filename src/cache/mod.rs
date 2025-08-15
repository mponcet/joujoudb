mod lru;
// mod lruk;
mod memcache;
mod pagecache;

use crate::pages::PageId;

pub const DEFAULT_PAGE_CACHE_SIZE: usize = 20000;

pub trait EvictionPolicy: Send + Sync {
    fn record_access(&mut self, page_id: PageId);
    fn evict(&mut self) -> Option<PageId>;
    fn set_evictable(&mut self, page_id: PageId);
    fn set_unevictable(&mut self, page_id: PageId);
    fn remove(&mut self, page_id: PageId);
}

pub use memcache::{PageRef, PageRefMut};
pub use pagecache::{PageCache, PageCacheError};
