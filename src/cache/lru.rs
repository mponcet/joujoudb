use crate::cache::EvictionPolicy;
use crate::page::PageId;

use std::collections::VecDeque;
use std::sync::Mutex;

// poor implementation, but needed for testing
#[allow(clippy::upper_case_acronyms)]
pub struct LRU {
    lru: Mutex<VecDeque<PageId>>,
}

impl LRU {
    pub fn new() -> Self {
        Self {
            lru: Mutex::new(VecDeque::new()),
        }
    }
}

impl EvictionPolicy for LRU {
    fn record_access(&self, page_id: PageId) {
        let mut lru = self.lru.lock().unwrap();

        lru.iter()
            .enumerate()
            .find_map(|(idx, &id)| if id == page_id { Some(idx) } else { None })
            .and_then(|idx| lru.remove(idx));
        lru.push_front(page_id)
    }

    fn evict(&self) {
        self.lru.lock().unwrap().pop_back();
    }

    fn should_evict(&self) -> Option<PageId> {
        self.lru.lock().unwrap().back().copied()
    }
}
