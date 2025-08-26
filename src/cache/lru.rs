use crate::cache::EvictionPolicy;
use crate::pages::PageId;

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
    fn lru_eviction_policy() {
        let mut lru = LRU::new();
        lru.record_access(PageId::new(0));
        lru.set_evictable(PageId::new(0));
        lru.record_access(PageId::new(1));
        lru.set_evictable(PageId::new(1));
        lru.record_access(PageId::new(2));
        lru.set_evictable(PageId::new(2));
        assert_eq!(lru.evict(), Some(PageId::new(0)));
        lru.set_unevictable(PageId::new(1));
        assert_eq!(lru.evict(), Some(PageId::new(2)));
    }
}
