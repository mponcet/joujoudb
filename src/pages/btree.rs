use crate::pages::HeapPageSlotId;
use crate::pages::{PAGE_SIZE, PageId};

use zerocopy_derive::*;

const BTREE_BRANCHING_FACTOR: usize = 341;
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

pub type Key = u32;

#[derive(Copy, Clone, FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct RecordId {
    page_id: PageId,
    slot_id: HeapPageSlotId,
}

#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct BTreeLeafPage {
    header: BTreePageHeader,
    keys: [Key; BTREE_NUM_KEYS],
    values: [RecordId; BTREE_NUM_KEYS],
    prev: PageId,
    next: PageId,
    _padding: [u8; 4],
}

const _: () = assert!(std::mem::size_of::<BTreeLeafPage>() == PAGE_SIZE);

#[cfg(test)]
impl Default for BTreeLeafPage {
    fn default() -> Self {
        Self {
            header: BTreePageHeader::default(),
            keys: [Key::default(); BTREE_NUM_KEYS],
            values: [RecordId {
                page_id: 0,
                slot_id: 0,
            }; BTREE_NUM_KEYS],
            prev: Default::default(),
            next: Default::default(),
            _padding: [0; 4],
        }
    }
}

use thiserror::Error;
#[derive(Error, Debug)]
pub enum BTreePageError {
    #[error("key not found")]
    KeyNotFound,
}

pub struct SplitLeaf<'page> {
    lhs: &'page mut BTreeLeafPage,
}

impl SplitLeaf<'_> {
    pub fn split(&mut self, rhs: &mut BTreeLeafPage, key: Key, value: RecordId) {
        let lhs_num_keys = self.lhs.header.num_keys as usize;
        let split_at = lhs_num_keys.div_ceil(2);
        let rhs_num_keys = lhs_num_keys - split_at;
        let median_key = self.lhs.keys[split_at];
        rhs.keys[..rhs_num_keys].copy_from_slice(&self.lhs.keys[split_at..]);
        rhs.values[..rhs_num_keys].copy_from_slice(&self.lhs.values[split_at..]);

        self.lhs.header.num_keys = split_at as u16;
        rhs.header.num_keys = rhs_num_keys as u16;

        if key < median_key {
            self.lhs.insert(key, value);
        } else {
            rhs.insert(key, value);
        }
    }
}

impl BTreeLeafPage {
    #[inline]
    fn keys(&self) -> &[Key] {
        let num_keys = self.header.num_keys as usize;
        &self.keys[..num_keys]
    }

    #[inline]
    fn values(&self) -> &[RecordId] {
        let num_keys = self.header.num_keys as usize;
        &self.values[..num_keys]
    }

    pub fn search(&self, key: Key) -> Option<RecordId> {
        let pos = self.keys().binary_search(&key).ok()?;
        Some(self.values[pos])
    }

    pub fn insert(&mut self, key: Key, value: RecordId) -> Option<SplitLeaf<'_>> {
        match self.keys().binary_search(&key) {
            Ok(pos) => {
                // FIXME: key exists, replace value ?
                self.values[pos] = value;
                None
            }
            Err(pos) => {
                let num_keys = self.header.num_keys as usize;
                if num_keys < BTREE_NUM_KEYS {
                    self.keys.copy_within(pos..num_keys, pos + 1);
                    self.keys[pos] = key;
                    self.values.copy_within(pos..num_keys, pos + 1);
                    self.values[pos] = value;
                    self.header.num_keys += 1;
                    None
                } else {
                    Some(SplitLeaf { lhs: self })
                }
            }
        }
    }

    pub fn delete(&mut self, key: Key) -> Result<(), BTreePageError> {
        let num_keys = self.header.num_keys as usize;
        let pos = self
            .keys()
            .binary_search(&key)
            .map_err(|_| BTreePageError::KeyNotFound)?;

        self.keys.copy_within(pos + 1..num_keys, pos);
        self.values.copy_within(pos + 1..num_keys, pos);
        self.header.num_keys -= 1;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_record(key: Key) -> RecordId {
        RecordId {
            page_id: key as PageId,
            slot_id: key as HeapPageSlotId,
        }
    }

    #[test]
    fn test_leaf_page_basic() {
        let mut leaf = BTreeLeafPage::default();
        for key in 0..BTREE_NUM_KEYS {
            let _ = leaf.insert(key as Key, make_record(key as Key));
        }
        assert_eq!(leaf.keys().len(), BTREE_NUM_KEYS);
        assert!(leaf.keys().is_sorted());

        let key = (BTREE_NUM_KEYS / 2) as Key;
        assert!(leaf.search(key).is_some());
        let _ = leaf.delete(key);
        assert!(leaf.search(key).is_none());
        assert!(leaf.keys().is_sorted());
    }

    #[test]
    fn test_insert_leaf_page_not_monotonic() {
        let mut leaf = BTreeLeafPage::default();
        for key in 0..BTREE_NUM_KEYS {
            let key = (if key % 2 == 0 { key } else { key * 2 }) as Key;
            let _ = leaf.insert(key as Key, make_record(key as Key));
        }

        assert!(leaf.keys().is_sorted());
    }

    #[test]
    fn test_split_leaf_page() {
        let mut lhs = BTreeLeafPage::default();
        let mut rhs = BTreeLeafPage::default();

        // fill lhs
        for key in 0..BTREE_NUM_KEYS {
            let key = key * 2;
            lhs.insert(key as Key, make_record(key as Key));
        }

        // lhs is full, split needed
        let key = BTREE_NUM_KEYS - BTREE_NUM_KEYS % 2 + 1;
        let (key, value) = (key as Key, make_record(key as Key));
        let split = lhs.insert(key, value);
        assert!(split.is_some());
        split.unwrap().split(&mut rhs, key, value);

        assert!(lhs.keys().is_sorted());
        assert!(rhs.keys().is_sorted());
        assert_eq!(lhs.keys().len() + rhs.keys().len(), BTREE_NUM_KEYS + 1);
    }
}
