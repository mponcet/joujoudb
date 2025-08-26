use crate::pages::{PAGE_SIZE, Page, PageId};
use crate::serialize::Serialize;
use crate::tuple::{Tuple, TupleRef};

use thiserror::Error;
use zerocopy::{little_endian::U16, *};
use zerocopy_derive::*;

/// The identifier for a slot in a heap page.
#[derive(
    Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, FromBytes, IntoBytes, KnownLayout, Immutable,
)]
pub struct HeapPageSlotId(U16);

impl HeapPageSlotId {
    pub fn new(slot_id: u16) -> Self {
        Self(U16::new(slot_id))
    }

    pub fn get(&self) -> u16 {
        self.0.get()
    }

    pub fn set(&mut self, slot_id: u16) {
        self.0.set(slot_id);
    }
}

// The identifier for a unique entry in a table
#[derive(Copy, Clone, FromBytes, IntoBytes, KnownLayout, Immutable)]
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
#[derive(
    Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, FromBytes, IntoBytes, KnownLayout, Immutable,
)]
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
    offset: U16,
    len: U16,
}

impl HeapPageSlot {
    fn new(offset: usize, len: usize) -> Self {
        assert!(len > 0 && len as u16 <= u16::MAX);
        Self {
            offset: U16::new(offset as u16),
            len: U16::new(len as u16),
        }
    }

    fn offset(&self) -> usize {
        self.offset.get() as usize
    }

    fn len(&self) -> usize {
        self.len.get() as usize
    }

    fn mark_deleted(&mut self) {
        self.len.set(0)
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

#[cfg(test)]
impl Default for HeapPage {
    fn default() -> Self {
        Self {
            header: HeapPageHeader {
                num_slots: HeapPageSlotId::new(0),
            },
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

    #[cfg(test)]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    fn last_slot_id(&self) -> Option<HeapPageSlotId> {
        if self.header.num_slots.get() == 0 {
            return None;
        }

        Some(HeapPageSlotId::new(self.header.num_slots.get() - 1))
    }

    #[inline]
    fn last_slot_offset(&self) -> Option<usize> {
        self.last_slot_id()
            .map(|slot_id| slot_id.get() as usize * Self::SLOT_SIZE)
    }

    #[inline]
    fn last_tuple_offset(&self) -> Option<usize> {
        self.last_slot_id()
            .map(|slot_id| self.get_slot(slot_id).unwrap().offset())
    }

    fn get_slot(&self, slot_id: HeapPageSlotId) -> Option<&HeapPageSlot> {
        if slot_id >= self.header.num_slots {
            return None;
        }

        let idx = slot_id.get() as usize * Self::SLOT_SIZE;
        let bytes = &self.data[idx..idx + Self::SLOT_SIZE];
        HeapPageSlot::ref_from_bytes(bytes).ok()
    }

    fn get_slot_mut(&mut self, slot_id: HeapPageSlotId) -> Option<&mut HeapPageSlot> {
        if slot_id >= self.header.num_slots {
            return None;
        }

        let idx = slot_id.get() as usize * Self::SLOT_SIZE;
        let bytes = &mut self.data[idx..idx + Self::SLOT_SIZE];
        HeapPageSlot::mut_from_bytes(bytes).ok()
    }

    #[inline]
    fn free_space(&self) -> usize {
        self.last_tuple_offset().unwrap_or(Self::DATA_SIZE)
            - self.header.num_slots.get() as usize * Self::SLOT_SIZE
    }

    // free space for both the slot and the tuple
    #[inline]
    fn has_free_space(&self, tuple: &Tuple) -> bool {
        self.free_space() >= (Self::SLOT_SIZE + tuple.len())
    }

    /// Inserts a tuple into the heap page.
    ///
    /// Returns a `Result` containing the `HeapPageSlotId` of the new tuple, or a `HeapPageError` if there is not enough free space.
    pub fn insert_tuple(&mut self, tuple: &Tuple) -> Result<HeapPageSlotId, HeapPageError> {
        if self.has_free_space(tuple) {
            // insert tuple
            let tuple_len = tuple.len();
            let offset = self.last_tuple_offset().unwrap_or(Self::DATA_SIZE) - tuple_len;
            tuple.write_bytes_to(&mut self.data[offset..]);

            // insert slot
            self.header.num_slots.set(self.header.num_slots.get() + 1);
            let slot = HeapPageSlot::new(offset, tuple_len);
            let idx = self.last_slot_offset().unwrap();
            slot.write_to(&mut self.data[idx..idx + Self::SLOT_SIZE])
                .unwrap();

            Ok(HeapPageSlotId::new(self.header.num_slots.get() - 1))
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
        let (idx, len) = (slot.offset(), slot.len());

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
    use crate::sql::schema::{Column, ColumnType, Constraints, Schema};
    use crate::sql::types::{BigInt, Char, Value, VarChar};

    use super::*;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Column::new(ColumnType::BigInt, Constraints::default()),
            Column::new(ColumnType::VarChar, Constraints::default()),
            Column::new(ColumnType::Char(32), Constraints::default()),
        ])
    }

    fn test_values(varchar_len: usize, char_len: usize) -> Vec<Value> {
        let varchar = String::from_iter(std::iter::repeat_n('v', varchar_len));
        let char = String::from_iter(std::iter::repeat_n('c', char_len));
        vec![
            Value::BigInt(BigInt::new(42)),
            Value::VarChar(VarChar::new(varchar)),
            Value::Char(Char::new(char, Some(32))),
        ]
    }

    #[test]
    fn page_should_not_overflow() {
        let mut page = HeapPage::new();
        let values = test_values(128, 32);
        let tuple = Tuple::try_new(values).unwrap();

        for _ in 0..40 {
            let _ = page.insert_tuple(&tuple);
        }

        let result = page.insert_tuple(&tuple);
        assert_eq!(result.err().unwrap(), HeapPageError::NoFreeSpace)
    }

    #[test]
    fn fill_page() {
        let mut page = HeapPage::new();

        assert_eq!(page.free_space(), HeapPage::DATA_SIZE);
        let values = vec![Value::Char(Char::new("cc".to_string(), Some(2)))];
        let tuple = Tuple::try_new(values).unwrap();
        // slot and tuple (with header) size: 16
        for _ in 0..HeapPage::DATA_SIZE / 16 {
            let _ = page.insert_tuple(&tuple);
        }

        assert_eq!(page.free_space(), HeapPage::DATA_SIZE % 16);
    }

    #[test]
    fn get_after_insert_delete() {
        let mut page = HeapPage::new();

        let schema = test_schema();
        let values = test_values(128, 32);
        let values2 = test_values(64, 16);
        let values_clone = values.clone();
        let values2_clone = values2.clone();
        let tuple = Tuple::try_new(values).unwrap();
        let tuple2 = Tuple::try_new(values2).unwrap();

        let slot_id = page.insert_tuple(&tuple).unwrap();
        let slot_id2 = page.insert_tuple(&tuple2).unwrap();
        let tuple = page.get_tuple(slot_id).unwrap().to_owned(&schema);
        for (lhs, rhs) in tuple.values().iter().zip(values_clone.iter()) {
            assert_eq!(lhs, rhs);
        }
        let tuple2 = page.get_tuple(slot_id2).unwrap().to_owned(&schema);
        for (lhs, rhs) in tuple2.values().iter().zip(values2_clone.iter()) {
            assert_eq!(lhs, rhs);
        }

        page.delete_tuple(slot_id).unwrap();
        let tuple_ref = page.get_tuple(slot_id);
        assert_eq!(tuple_ref.err().unwrap(), HeapPageError::SlotDeleted);

        let tuple2 = page.get_tuple(slot_id2).unwrap().to_owned(&schema);
        assert_eq!(tuple2.values(), values2_clone);
    }
}
