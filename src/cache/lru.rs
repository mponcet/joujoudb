use crate::cache::EvictionPolicy;
use crate::page::PageId;

use std::collections::HashMap;

use priority_queue::PriorityQueue;

#[allow(clippy::upper_case_acronyms)]
pub struct LRU {
    queue: PriorityQueue<PageId, i64>,
    // when a page is set unevictable and removed
    // from the priority queue, keep track of the
    // last access in a hashmap
    last_access: HashMap<PageId, i64>,
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
    fn record_access(&mut self, page_id: PageId) {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
        self.last_access.insert(page_id, now);
        self.queue.push(page_id, -now);
    }

    fn evict(&mut self) -> Option<PageId> {
        self.queue.pop().map(|(page_id, _)| page_id)
    }

    fn set_evictable(&mut self, page_id: PageId) {
        if let Some(&timestamp) = self.last_access.get(&page_id) {
            self.queue.push(page_id, -timestamp);
        }
    }

    fn set_unevictable(&mut self, page_id: PageId) {
        self.queue.remove(&page_id);
    }

    fn remove(&mut self, page_id: PageId) {
        self.queue.remove(&page_id);
        self.last_access.remove(&page_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_eviction_policy() {
        let mut lru = LRU::new();
        lru.record_access(0);
        lru.set_evictable(0);
        lru.record_access(1);
        lru.set_evictable(1);
        lru.record_access(2);
        lru.set_evictable(2);
        assert_eq!(lru.evict(), Some(0));
        lru.set_unevictable(1);
        assert_eq!(lru.evict(), Some(2));
    }
}
