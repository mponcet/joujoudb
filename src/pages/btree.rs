use crate::pages::HeapPageSlotId;
use crate::pages::{PAGE_SIZE, PageId};

use zerocopy_derive::*;

const BTREE_BRANCHING_FACTOR: usize = 340;
const BTREE_NUM_KEYS: usize = BTREE_BRANCHING_FACTOR - 1;

enum BTreePageType {
    Root,
    Internal,
    Leaf,
}

#[derive(Default, FromBytes, KnownLayout, Immutable)]
#[repr(C)]
struct BTreePageHeader {
    // should be a BTreePageType but zerocopy
    // FromBytes trait doesn't support enum
    page_type: u8,
    num_keys: u16,
}

impl BTreePageHeader {
    fn get_type(&self) -> BTreePageType {
        match self.page_type {
            0 => BTreePageType::Root,
            1 => BTreePageType::Internal,
            2 => BTreePageType::Leaf,
            _ => unreachable!(),
        }
    }
}

type Key = u32;

#[derive(Copy, Clone, Default, FromBytes, KnownLayout, Immutable)]
#[repr(C)]
struct RecordId {
    page_id: PageId,
    slot_id: HeapPageSlotId,
}

#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
struct BTreePage {
    header: BTreePageHeader,
    keys: [Key; BTREE_NUM_KEYS],
    values: [RecordId; BTREE_BRANCHING_FACTOR],
    prev: PageId,
    next: PageId,
    _padding: [u8; 8],
}

const _: () = assert!(std::mem::size_of::<BTreePage>() == PAGE_SIZE);

impl Default for BTreePage {
    fn default() -> Self {
        Self {
            header: BTreePageHeader::default(),
            keys: [Key::default(); BTREE_NUM_KEYS],
            values: [RecordId::default(); BTREE_BRANCHING_FACTOR],
            prev: Default::default(),
            next: Default::default(),
            _padding: [0; 8],
        }
    }
}

use thiserror::Error;
#[derive(Error, Debug)]
enum BTreePageError {
    #[error("key not found")]
    KeyNotFound,
    #[error("page is full, a split is needed")]
    SplitNeeded,
}

impl BTreePage {
    #[inline]
    fn keys(&self) -> &[Key] {
        let num_keys = self.header.num_keys as usize;
        &self.keys[..num_keys]
    }

    fn search(&self, key: Key) -> Result<RecordId, BTreePageError> {
        let idx = self
            .keys()
            .binary_search(&key)
            .map_err(|_| BTreePageError::KeyNotFound)?;

        Ok(self.values[idx])
    }

    fn insert(&mut self, key: Key, value: RecordId) -> Result<(), BTreePageError> {
        match self.keys().binary_search(&key) {
            Ok(idx) => {
                // FIXME: key exists, replace value ?
                self.values[idx] = value;
                Ok(())
            }
            Err(idx) => {
                let num_keys = self.header.num_keys as usize;
                if num_keys <= BTREE_NUM_KEYS {
                    self.keys.copy_within(idx..num_keys, idx + 1);
                    self.keys[idx] = key;
                    self.values.copy_within(idx..num_keys, idx + 1);
                    self.values[idx] = value;
                    self.header.num_keys += 1;
                    Ok(())
                } else {
                    Err(BTreePageError::SplitNeeded)
                }
            }
        }
    }

    fn delete(&mut self, key: Key) -> Result<(), BTreePageError> {
        let num_keys = self.header.num_keys as usize;
        let idx = self
            .keys()
            .binary_search(&key)
            .map_err(|_| BTreePageError::KeyNotFound)?;

        self.keys.copy_within(idx + 1..num_keys, idx);
        self.values.copy_within(idx + 1..num_keys, idx);
        self.header.num_keys -= 1;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut leaf = BTreePage::default();
        for key in 0..BTREE_NUM_KEYS {
            let _ = leaf.insert(key as u32, RecordId::default());
        }
        assert_eq!(leaf.header.num_keys, BTREE_NUM_KEYS as u16);
        assert!(leaf.keys().is_sorted());

        let _ = leaf.delete((BTREE_NUM_KEYS / 2) as u32);
        assert_eq!(leaf.header.num_keys, (BTREE_NUM_KEYS - 1) as u16);
        assert_eq!(
            leaf.keys[BTREE_NUM_KEYS / 2],
            (BTREE_NUM_KEYS / 2 + 1) as u32
        );
        assert!(leaf.keys().is_sorted());

        assert!(leaf.search(0).is_ok());
        let _ = leaf.delete(0);
        assert!(leaf.search(0).is_err());
    }

    #[test]
    fn test_insert_no_order() {
        let mut leaf = BTreePage::default();
        for key in 0..BTREE_NUM_KEYS {
            let key = (if key % 2 == 0 { key } else { key * 2 }) as u32;
            let _ = leaf.insert(key, RecordId::default());
        }

        assert!(leaf.keys().is_sorted());
    }
}
