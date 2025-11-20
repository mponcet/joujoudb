use crate::storage::StorageId;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use zerocopy::little_endian::U32;
use zerocopy_derive::*;

pub const PAGE_SIZE: usize = 4096;
pub const PAGE_INVALID: PageId = PageId(U32::new(0));
/// The page id reserved for the superblock
pub const PAGE_RESERVED: PageId = PageId(U32::new(0));

#[derive(
    Clone,
    Copy,
    Debug,
    Hash,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
    FromBytes,
    IntoBytes,
    KnownLayout,
    Immutable,
)]
pub struct PageId(U32);

impl PageId {
    pub fn new(page_id: u32) -> Self {
        Self(U32::new(page_id))
    }

    pub fn get(&self) -> u32 {
        self.0.get()
    }

    pub fn set(&mut self, page_id: u32) {
        self.0.set(page_id);
    }

    pub fn next(&mut self) {
        let page_id = self.0.get();
        debug_assert!(page_id < u32::MAX);
        self.0.set(page_id + 1);
    }
}

/// the actual data read from/written to disk
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
    pub storage_id: StorageId,
    dirty: AtomicBool,
    counter: AtomicUsize,
}

impl PageMetadata {
    pub fn new(storage_id: StorageId, page_id: PageId) -> Self {
        Self {
            storage_id,
            page_id,
            dirty: AtomicBool::new(false),
            counter: AtomicUsize::new(0),
        }
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Relaxed)
    }

    pub fn set_dirty(&self) {
        self.dirty.store(true, Ordering::Relaxed);
    }

    pub fn clear_dirty(&self) {
        self.dirty.store(false, Ordering::Relaxed);
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
