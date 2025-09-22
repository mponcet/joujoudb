mod lru;
// mod lruk;
mod memcache;
mod pagecache;

use crate::pages::PageId;
use crate::storage::StorageId;

pub const DEFAULT_PAGE_CACHE_SIZE: usize = 20000;

pub trait EvictionPolicy: Send + Sync {
    fn record_access(&mut self, storage_id: StorageId, page_id: PageId);
    fn evict(&mut self) -> Option<(StorageId, PageId)>;
    fn set_evictable(&mut self, storage_id: StorageId, page_id: PageId);
    fn set_unevictable(&mut self, storage_id: StorageId, page_id: PageId);
    fn remove(&mut self, storage_id: StorageId, page_id: PageId);
}

pub use memcache::{PageRef, PageRefMut};
pub use pagecache::{GLOBAL_PAGE_CACHE, PageCache, PageCacheError, StoragePageCache};
