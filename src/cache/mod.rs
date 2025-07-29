mod lru;
mod memcache;
mod pagecache;

use crate::page::PageId;

pub const DEFAULT_PAGE_CACHE_SIZE: usize = 1_000;

pub trait EvictionPolicy: Send + Sync {
    fn record_access(&self, page_id: PageId);
    fn evict(&self);
    fn should_evict(&self) -> Option<PageId>;
}

pub use pagecache::PageCache;
