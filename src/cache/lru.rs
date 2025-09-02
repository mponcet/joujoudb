use crate::cache::EvictionPolicy;
use crate::pages::PageId;
use crate::storage::StorageId;

use std::collections::HashMap;

use priority_queue::PriorityQueue;

#[allow(clippy::upper_case_acronyms)]
pub struct LRU {
    queue: PriorityQueue<(StorageId, PageId), i64>,
    // when a page is set unevictable and removed
    // from the priority queue, keep track of the
    // last access in a hashmap
    last_access: HashMap<(StorageId, PageId), i64>,
}

impl LRU {
    pub fn new() -> Self {
        Self {
            queue: PriorityQueue::new(),
            last_access: HashMap::new(),
        }
    }
}

impl EvictionPolicy for LRU {
    fn record_access(&mut self, storage_id: StorageId, page_id: PageId) {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
        self.last_access.insert((storage_id, page_id), now);
        self.queue.push((storage_id, page_id), -now);
    }

    fn evict(&mut self) -> Option<(StorageId, PageId)> {
        self.queue.pop().map(|(ids, _)| ids)
    }

    fn set_evictable(&mut self, storage_id: StorageId, page_id: PageId) {
        if let Some(&timestamp) = self.last_access.get(&(storage_id, page_id)) {
            self.queue.push((storage_id, page_id), -timestamp);
        }
    }

    fn set_unevictable(&mut self, storage_id: StorageId, page_id: PageId) {
        self.queue.remove(&(storage_id, page_id));
    }

    fn remove(&mut self, storage_id: StorageId, page_id: PageId) {
        self.queue.remove(&(storage_id, page_id));
        self.last_access.remove(&(storage_id, page_id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_eviction_policy() {
        let mut lru = LRU::new();
        lru.record_access(StorageId(0), PageId::new(0));
        lru.set_evictable(StorageId(0), PageId::new(0));
        lru.record_access(StorageId(0), PageId::new(1));
        lru.set_evictable(StorageId(0), PageId::new(1));
        lru.record_access(StorageId(0), PageId::new(2));
        lru.set_evictable(StorageId(0), PageId::new(2));
        assert_eq!(lru.evict(), Some((StorageId(0), PageId::new(0))));
        lru.set_unevictable(StorageId(0), PageId::new(1));
        assert_eq!(lru.evict(), Some((StorageId(0), PageId::new(2))));
    }
}
