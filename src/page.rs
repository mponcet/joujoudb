use std::sync::atomic::{AtomicUsize, Ordering};

pub const PAGE_SIZE: usize = 4096;

pub type PageId = usize;

/// the actual data read from/written to disk
#[derive(Copy, Clone)]
pub struct Page {
    pub data: [u8; PAGE_SIZE],
}

impl Default for Page {
    fn default() -> Self {
        Self {
            data: [0; PAGE_SIZE],
        }
    }
}

impl Page {
    pub fn new() -> Self {
        Self::default()
    }
}

pub struct PageMetadata {
    pub page_id: PageId,
    dirty: bool,
    counter: AtomicUsize,
}

impl PageMetadata {
    pub fn new(page_id: PageId) -> Self {
        Self {
            page_id,
            dirty: false,
            counter: AtomicUsize::new(0),
        }
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub fn set_dirty(&mut self) {
        self.dirty = true;
    }

    pub fn clear_dirty(&mut self) {
        self.dirty = false;
    }

    pub fn get_pin_counter(&self) -> usize {
        self.counter.load(Ordering::Relaxed)
    }

    pub fn pin(&self) -> usize {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }

    pub fn unpin(&self) -> usize {
        self.counter.fetch_sub(1, Ordering::Relaxed)
    }
}
