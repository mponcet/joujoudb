use crate::page::{PAGE_SIZE, Page};
use crate::tuple::{Tuple, TupleRef};

use thiserror::Error;
use zerocopy::*;
use zerocopy_derive::*;

type HeapPageSlotId = u16;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
struct HeapPageHeader {
    num_slots: HeapPageSlotId,
}

/// A slotted page structure.
///
/// Slot array grows from start to end.
/// Tuple section grows from end to start.
///
///+-------------------------------+
///|           Header              |
///|-------------------------------|
///| Num Records: 3                |
///|-------------------------------|
///|           Slot Array          |
///|-------------------------------| <- start
///| Slot 0: Tuple 0 offset        |
///| Slot 1: Tuple 1 offset        |
///| Slot 2: Tuple 2 offset        |
///|-------------------------------|
///|           Free Space          |
///|-------------------------------|
///| [Unused Space]                |
///|-------------------------------|
///|          Tuple Section        |
///|-------------------------------|
///| Tuple 2: [Data]               |
///| Tuple 1: [Data]               |
///| Tuple 0: [Data]               |
///+-------------------------------+ <- end

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

impl HeapPage {
    const HEADER_SIZE: usize = std::mem::size_of::<HeapPageHeader>();
    const SLOT_SIZE: usize = std::mem::size_of::<HeapPageSlot>();
    const DATA_SIZE: usize = PAGE_SIZE - Self::HEADER_SIZE;

    // max tuple size: space for header and values
    pub const MAX_TUPLE_SIZE: usize = Self::DATA_SIZE - Self::SLOT_SIZE;

    pub fn new() -> Self {
        Self {
            header: HeapPageHeader { num_slots: 0 },
            data: [0; Self::DATA_SIZE],
        }
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

    pub fn insert_tuple(&mut self, tuple: &Tuple) -> Result<HeapPageSlotId, HeapPageError> {
        if self.has_free_space(tuple) {
            // insert tuple
            let tuple_len = tuple.len();
            let idx = self
                .last_tuple_offset()
                .unwrap_or(Self::DATA_SIZE - tuple_len);
            self.data[idx..idx + Tuple::HEADER_SIZE].copy_from_slice(tuple.header().as_bytes());
            self.data[idx + Tuple::HEADER_SIZE..idx + Tuple::HEADER_SIZE + tuple.values().len()]
                .copy_from_slice(tuple.values());

            // insert slot
            let slot = HeapPageSlot::new(idx as u16, tuple_len as u16);
            let idx = self.header.num_slots as usize * Self::SLOT_SIZE;
            self.data[idx..idx + Self::SLOT_SIZE].copy_from_slice(slot.as_bytes());
            self.header.num_slots += 1;

            Ok(self.header.num_slots - 1)
        } else {
            Err(HeapPageError::NoFreeSpace)
        }
    }

    pub fn delete_tuple(&mut self, slot_id: HeapPageSlotId) -> Result<(), HeapPageError> {
        let slot = self
            .get_slot_mut(slot_id)
            .ok_or(HeapPageError::SlotNotFound)?;
        slot.mark_deleted();

        Ok(())
    }

    pub fn get_tuple(&self, slot_id: HeapPageSlotId) -> Result<Tuple, HeapPageError> {
        let slot = self.get_slot(slot_id).ok_or(HeapPageError::SlotNotFound)?;
        let (idx, len) = (slot.offset as usize, slot.len as usize);

        if slot.is_deleted() {
            Err(HeapPageError::SlotDeleted)
        } else {
            Ok(Tuple::Ref(
                TupleRef::ref_from_bytes(&self.data[idx..idx + len]).unwrap(),
            ))
        }
    }
}

impl<'a> From<&'a mut Page> for &'a mut HeapPage {
    fn from(page: &'a mut Page) -> &'a mut HeapPage {
        unsafe { &mut *(page.data.as_mut_ptr() as *mut HeapPage) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_struct_size_equals_page_size_const() {
        assert_eq!(std::mem::size_of::<HeapPage>(), PAGE_SIZE)
    }

    #[test]
    fn page_should_not_overflow() {
        let mut page = HeapPage::new();
        let data = vec![0, 1, 2, 3, 4, 5, 6, 7].into_boxed_slice();
        let tuple = Tuple::try_new(data).unwrap();

        for _ in 0..PAGE_SIZE {
            let _ = page.insert_tuple(&tuple);
        }

        let result = page.insert_tuple(&tuple);
        assert_eq!(result.err().unwrap(), HeapPageError::NoFreeSpace)
    }

    #[test]
    fn test_get_after_insert_delete() {
        let mut page = HeapPage::new();
        let data = vec![0, 1, 2, 3, 4, 5, 6, 7].into_boxed_slice();
        let tuple = Tuple::try_new(data).unwrap();

        let slot_id = page.insert_tuple(&tuple).expect("cannot insert tuple");
        let tuple_ref = page.get_tuple(slot_id).expect("cannot get tuple");
        assert_eq!(tuple.values(), tuple_ref.values());
        page.delete_tuple(slot_id).expect("cannot delete tuple");
        let tuple_ref = page.get_tuple(slot_id);
        assert_eq!(tuple_ref.err().unwrap(), HeapPageError::SlotDeleted);
    }
}
