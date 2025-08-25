use crate::pages::{PAGE_SIZE, Page, PageId};
use crate::serialize::Serialize;
use crate::tuple::{Tuple, TupleRef};

use thiserror::Error;
use zerocopy::*;
use zerocopy_derive::*;

/// The identifier for a slot in a heap page.
pub type HeapPageSlotId = u16;

// The identifier for a unique entry in a table
#[derive(Copy, Clone, FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct RecordId {
    pub page_id: PageId,
    pub slot_id: HeapPageSlotId,
}

impl RecordId {
    pub fn new(page_id: PageId, slot_id: HeapPageSlotId) -> Self {
        Self { page_id, slot_id }
    }
}

/// The header of a heap page, containing metadata about the page.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
struct HeapPageHeader {
    num_slots: HeapPageSlotId,
}

/// A slotted page that stores tuples.
///
/// The `HeapPage` is organized as follows:
/// - A header that contains metadata about the page.
/// - A slot array that grows from the beginning of the page.
/// - A tuple section that grows from the end of the page.
///
/// This layout allows for efficient use of space and easy access to tuples.
///
/// ```text
/// +-------------------------------------------------+
/// | Page Header (number of slots)                   |
/// +-------------------------------------------------+
/// | Slot Array (offsets and lengths of tuples)      |
/// |  - Slot 0: (offset, length)                     |
/// |  - Slot 1: (offset, length)                     |
/// |  - ...                                          |
/// +-------------------------------------------------+
/// |                                                 |
/// |                  Free Space                     |
/// |                                                 |
/// +-------------------------------------------------+
/// | Tuple Data (grows from the end of the page)     |
/// |  - Tuple 2: [ ... data ... ]                    |
/// |  - Tuple 1: [ ... data ... ]                    |
/// |  - Tuple 0: [ ... data ... ]                    |
/// +-------------------------------------------------+
/// ```
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct HeapPage {
    header: HeapPageHeader,
    data: [u8; Self::DATA_SIZE],
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
struct HeapPageSlot {
    offset: u16,
    len: u16,
}

impl HeapPageSlot {
    fn new(offset: u16, len: u16) -> Self {
        assert!(len > 0);
        Self { offset, len }
    }

    fn mark_deleted(&mut self) {
        self.len = 0;
    }

    pub fn is_deleted(&self) -> bool {
        self.len == 0
    }
}

#[derive(Error, Debug, PartialEq)]
pub enum HeapPageError {
    #[error("page is full")]
    NoFreeSpace,
    #[error("slot not found")]
    SlotNotFound,
    #[error("slot has been deleted")]
    SlotDeleted,
}

impl Default for HeapPage {
    fn default() -> Self {
        Self {
            header: HeapPageHeader { num_slots: 0 },
            data: [0; Self::DATA_SIZE],
        }
    }
}

impl HeapPage {
    const HEADER_SIZE: usize = std::mem::size_of::<HeapPageHeader>();
    const SLOT_SIZE: usize = std::mem::size_of::<HeapPageSlot>();
    const DATA_SIZE: usize = PAGE_SIZE - Self::HEADER_SIZE;

    /// The maximum size of a tuple that can be stored in a heap page.
    pub const MAX_TUPLE_SIZE: usize = Self::DATA_SIZE - Self::SLOT_SIZE;

    /// Creates a new, empty `HeapPage`.
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    fn last_slot_id(&self) -> Option<HeapPageSlotId> {
        if self.header.num_slots == 0 {
            return None;
        }

        Some(self.header.num_slots - 1)
    }

    #[inline]
    fn last_tuple_offset(&self) -> Option<usize> {
        self.last_slot_id()
            .map(|slot_id| self.get_slot(slot_id).unwrap().offset as usize)
    }

    fn get_slot(&self, slot_id: HeapPageSlotId) -> Option<&HeapPageSlot> {
        if slot_id >= self.header.num_slots {
            return None;
        }

        let idx = slot_id as usize * Self::SLOT_SIZE;
        let bytes = &self.data[idx..idx + Self::SLOT_SIZE];
        HeapPageSlot::ref_from_bytes(bytes).ok()
    }

    fn get_slot_mut(&mut self, slot_id: HeapPageSlotId) -> Option<&mut HeapPageSlot> {
        if slot_id >= self.header.num_slots {
            return None;
        }

        let idx = slot_id as usize * Self::SLOT_SIZE;
        let bytes = &mut self.data[idx..idx + Self::SLOT_SIZE];
        HeapPageSlot::mut_from_bytes(bytes).ok()
    }

    // free space for both the slot and the tuple
    fn has_free_space(&self, tuple: &Tuple) -> bool {
        let free_space = self.last_tuple_offset().unwrap_or(Self::DATA_SIZE)
            - self.header.num_slots as usize * Self::SLOT_SIZE;

        free_space >= (Self::SLOT_SIZE + tuple.len())
    }

    /// Inserts a tuple into the heap page.
    ///
    /// Returns a `Result` containing the `HeapPageSlotId` of the new tuple, or a `HeapPageError` if there is not enough free space.
    pub fn insert_tuple(&mut self, tuple: &Tuple) -> Result<HeapPageSlotId, HeapPageError> {
        if self.has_free_space(tuple) {
            // insert tuple

            let tuple_len = tuple.len();
            let offset = self
                .last_tuple_offset()
                .unwrap_or(Self::DATA_SIZE - tuple_len);
            tuple.write_bytes_to(&mut self.data[offset..]);

            // insert slot
            let slot = HeapPageSlot::new(offset as u16, tuple_len as u16);
            let idx = self.header.num_slots as usize * Self::SLOT_SIZE;
            self.data[idx..idx + Self::SLOT_SIZE].copy_from_slice(slot.as_bytes());
            self.header.num_slots += 1;

            Ok(self.header.num_slots - 1)
        } else {
            Err(HeapPageError::NoFreeSpace)
        }
    }

    /// Deletes a tuple from the heap page.
    ///
    /// Returns an empty `Result` if successful, or a `HeapPageError` if the slot is not found.
    pub fn delete_tuple(&mut self, slot_id: HeapPageSlotId) -> Result<(), HeapPageError> {
        let slot = self
            .get_slot_mut(slot_id)
            .ok_or(HeapPageError::SlotNotFound)?;
        slot.mark_deleted();

        Ok(())
    }

    /// Retrieves a tuple from the heap page.
    ///
    /// Returns a `Result` containing a `Tuple` reference, or a `HeapPageError` if the slot is not found or has been deleted.
    pub fn get_tuple(&self, slot_id: HeapPageSlotId) -> Result<&TupleRef, HeapPageError> {
        let slot = self.get_slot(slot_id).ok_or(HeapPageError::SlotNotFound)?;
        let (idx, len) = (slot.offset as usize, slot.len as usize);

        if slot.is_deleted() {
            Err(HeapPageError::SlotDeleted)
        } else {
            Ok(TupleRef::ref_from_bytes(&self.data[idx..idx + len]).unwrap())
        }
    }
}

impl<'a> From<&'a Page> for &'a HeapPage {
    fn from(page: &'a Page) -> &'a HeapPage {
        unsafe { &*(page.data.as_ptr() as *const HeapPage) }
    }
}

impl<'a> From<&'a mut Page> for &'a mut HeapPage {
    fn from(page: &mut Page) -> &mut HeapPage {
        unsafe { &mut *(page.data.as_mut_ptr() as *mut HeapPage) }
    }
}

#[cfg(test)]
mod tests {
    // use super::*;

    // #[test]
    // fn test() {
    //     let cache = crate::cache::MemCache::new();
    //     let _ = cache.new_page(0);
    //     let page = cache.get_page_mut(0).unwrap();
    //     let mut heappage: HeapPageRefMut = page.into(); // = page_ref_mut.into();
    //     let tuple_w = crate::tuple::Tuple::try_new(vec![1, 2, 3].into_boxed_slice()).unwrap();
    //     let slot_id = heappage.insert_tuple(&tuple_w).unwrap();
    //     let tuple_r = heappage.get_tuple(slot_id).unwrap();
    //     assert_eq!(tuple_w.values(), tuple_r.values());
    // }

    // #[test]
    // fn page_should_not_overflow() {
    //     let mut page = HeapPage::new();
    //     let data = vec![0, 1, 2, 3, 4, 5, 6, 7].into_boxed_slice();
    //     let tuple = Tuple::try_new(data).unwrap();
    //
    //     for _ in 0..PAGE_SIZE {
    //         let _ = page.insert_tuple(&tuple);
    //     }
    //
    //     let result = page.insert_tuple(&tuple);
    //     assert_eq!(result.err().unwrap(), HeapPageError::NoFreeSpace)
    // }
    //
    // #[test]
    // fn test_get_after_insert_delete() {
    //     let mut page = HeapPage::new();
    //     let data = vec![0, 1, 2, 3, 4, 5, 6, 7].into_boxed_slice();
    //     let tuple = Tuple::try_new(data).unwrap();
    //
    //     let slot_id = page.insert_tuple(&tuple).expect("cannot insert tuple");
    //     let tuple_ref = page.get_tuple(slot_id).expect("cannot get tuple");
    //     assert_eq!(tuple.values(), tuple_ref.values());
    //     page.delete_tuple(slot_id).expect("cannot delete tuple");
    //     let tuple_ref = page.get_tuple(slot_id);
    //     assert_eq!(tuple_ref.err().unwrap(), HeapPageError::SlotDeleted);
    // }
}
