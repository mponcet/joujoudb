use crate::tuple::Tuple;
use crate::zerocopy::{FromBytes, IntoBytes, RefFromBytes, RefMutFromBytes};
use thiserror::Error;

pub const PAGE_SIZE: usize = 4096;

type SlotId = u16;

#[repr(C)]
struct PageHeader {
    num_slots: SlotId,
    dirty: bool,
}

const PAGE_HEADER_SIZE: usize = std::mem::size_of::<PageHeader>();
const PAGE_DATA_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;

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
#[repr(C)]
pub struct Page {
    header: PageHeader,
    data: [u8; PAGE_DATA_SIZE],
}

#[repr(C)]
struct PageSlot {
    offset: u16,
    len: u16,
}

pub const PAGE_SLOT_SIZE: usize = std::mem::size_of::<PageSlot>();

impl PageSlot {
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

impl RefFromBytes<'_> for PageSlot {
    fn ref_from_bytes(bytes: &'_ [u8]) -> &Self {
        assert_eq!(bytes.len(), PAGE_SLOT_SIZE);
        unsafe { &*(bytes.as_ptr() as *const PageSlot) }
    }
}

impl RefMutFromBytes<'_> for PageSlot {
    fn ref_mut_from_bytes(bytes: &'_ mut [u8]) -> &mut Self {
        assert_eq!(bytes.len(), PAGE_SLOT_SIZE);
        unsafe { &mut *(bytes.as_mut_ptr() as *mut PageSlot) }
    }
}

impl IntoBytes for PageSlot {
    fn as_bytes(&self) -> &[u8] {
        let len = std::mem::size_of::<Self>();
        let slf = self as *const Self;
        unsafe { std::slice::from_raw_parts(slf.cast::<u8>(), len) }
    }
}

#[derive(Error, Debug, PartialEq)]
pub enum PageError {
    #[error("page is full")]
    NoFreeSpace,
    #[error("slot not found")]
    SlotNotFound,
    #[error("slot has been deleted")]
    SlotDeleted,
}

impl Page {
    // max tuple size: space for header and values
    pub const MAX_TUPLE_SIZE: usize = PAGE_DATA_SIZE - PAGE_SLOT_SIZE;

    pub fn new() -> Self {
        Self {
            header: PageHeader {
                num_slots: 0,
                dirty: false,
            },
            data: [0; PAGE_DATA_SIZE],
        }
    }

    #[inline]
    fn last_slot_id(&self) -> Option<SlotId> {
        if self.header.num_slots == 0 {
            return None;
        }

        Some(self.header.num_slots - 1)
    }

    #[inline]
    fn last_tuple_offset(&self) -> Option<usize> {
        self.last_slot_id()
            .map(|slotid| self.get_slot(slotid).unwrap().offset as usize)
    }

    fn get_slot(&self, slotid: SlotId) -> Option<&PageSlot> {
        if slotid >= self.header.num_slots {
            return None;
        }

        let idx = slotid as usize * PAGE_SLOT_SIZE;
        let bytes = &self.data[idx..idx + PAGE_SLOT_SIZE];
        Some(PageSlot::ref_from_bytes(bytes))
    }

    fn get_slot_mut(&mut self, slotid: SlotId) -> Option<&mut PageSlot> {
        if slotid >= self.header.num_slots {
            return None;
        }

        let idx = slotid as usize * PAGE_SLOT_SIZE;
        let bytes = &mut self.data[idx..idx + PAGE_SLOT_SIZE];
        Some(PageSlot::ref_mut_from_bytes(bytes))
    }

    // free space for both the slot and the tuple
    fn has_free_space(&self, tuple: &Tuple) -> bool {
        let free_space = self.last_tuple_offset().unwrap_or(PAGE_DATA_SIZE)
            - self.header.num_slots as usize * PAGE_SLOT_SIZE;

        free_space >= (PAGE_SLOT_SIZE + tuple.len())
    }

    fn mark_dirty(&mut self) {
        self.header.dirty = true;
    }

    pub fn insert_tuple(&mut self, tuple: &Tuple) -> Result<SlotId, PageError> {
        if self.has_free_space(tuple) {
            // insert tuple
            let tuple_len = tuple.len();
            let idx = self
                .last_tuple_offset()
                .unwrap_or(PAGE_DATA_SIZE - tuple_len);
            self.data[idx..idx + tuple_len].copy_from_slice(tuple.as_bytes());

            // insert slot
            let slot = PageSlot::new(idx as u16, tuple_len as u16);
            let idx = self.header.num_slots as usize * PAGE_SLOT_SIZE;
            self.data[idx..idx + PAGE_SLOT_SIZE].copy_from_slice(slot.as_bytes());
            self.header.num_slots += 1;

            Ok(self.header.num_slots - 1)
        } else {
            Err(PageError::NoFreeSpace)
        }
    }

    pub fn delete_tuple(&mut self, slotid: SlotId) -> Result<(), PageError> {
        self.mark_dirty();
        let slot = self.get_slot_mut(slotid).ok_or(PageError::SlotNotFound)?;
        slot.mark_deleted();

        Ok(())
    }

    pub fn get_tuple(&self, slotid: SlotId) -> Result<Tuple, PageError> {
        let slot = self.get_slot(slotid).ok_or(PageError::SlotNotFound)?;
        let (idx, len) = (slot.offset as usize, slot.len as usize);

        if slot.is_deleted() {
            Err(PageError::SlotDeleted)
        } else {
            Ok(Tuple::from_bytes(&self.data[idx..idx + len]))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_struct_size_equals_page_size_const() {
        assert_eq!(std::mem::size_of::<Page>(), PAGE_SIZE)
    }

    #[test]
    fn page_should_not_overflow() {
        let mut page = Page::new();
        let data = vec![0, 1, 2, 3, 4, 5, 6, 7];
        let tuple = Tuple::new(data).unwrap();

        for _ in 0..PAGE_SIZE {
            let _ = page.insert_tuple(&tuple);
        }

        let result = page.insert_tuple(&tuple);
        assert_eq!(result.err().unwrap(), PageError::NoFreeSpace)
    }

    #[test]
    fn test_get_after_insert_delete() {
        let mut page = Page::new();
        let data = vec![0, 1, 2, 3, 4, 5, 6, 7];
        let tuple = Tuple::new(data).unwrap();

        let slotid = page.insert_tuple(&tuple).expect("cannot insert tuple");
        let tuple_ref = page.get_tuple(slotid).expect("cannot get tuple");
        assert_eq!(tuple.value(), tuple_ref.value());
        page.delete_tuple(slotid).expect("cannot delete tuple");
        let tuple_ref = page.get_tuple(slotid);
        assert_eq!(tuple_ref.err().unwrap(), PageError::SlotDeleted);
    }
}
