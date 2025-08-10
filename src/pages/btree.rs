use crate::pages::{HeapPageSlotId, PAGE_INVALID, PAGE_SIZE, Page, PageId};

use thiserror::Error;
use zerocopy::FromBytes;
use zerocopy_derive::*;

const BTREE_BRANCHING_FACTOR: usize = 341;
const BTREE_NUM_KEYS: usize = BTREE_BRANCHING_FACTOR - 1;

pub enum BTreePageType {
    Inner,
    Leaf,
}

#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
struct BTreePageHeader {
    // should be a BTreePageType but zerocopy
    // FromBytes trait doesn't support enum
    page_type: u8,
    num_keys: u16,
}

pub fn btree_get_page_type(page: &Page) -> BTreePageType {
    let (header, _) = BTreePageHeader::ref_from_prefix(&page.data).unwrap();

    match header.page_type {
        0 => BTreePageType::Inner,
        1 => BTreePageType::Leaf,
        _ => unreachable!(),
    }
}

pub type Key = u32;

#[derive(Copy, Clone, FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct RecordId {
    page_id: PageId,
    slot_id: HeapPageSlotId,
}

impl RecordId {
    pub fn new(page_id: PageId, slot_id: HeapPageSlotId) -> Self {
        Self { page_id, slot_id }
    }
}

#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct BTreeLeafPage {
    header: BTreePageHeader,
    keys: [Key; BTREE_NUM_KEYS],
    values: [RecordId; BTREE_NUM_KEYS],
    next: PageId,
}

const _: () = assert!(std::mem::size_of::<BTreeLeafPage>() <= PAGE_SIZE);

#[derive(Error, Debug)]
pub enum BTreePageError {
    #[error("key not found")]
    KeyNotFound,
}

pub struct SplitLeaf<'page> {
    lhs: &'page mut BTreeLeafPage,
}

impl SplitLeaf<'_> {
    pub fn split(&mut self, rhs: &mut BTreeLeafPage, key: Key, value: RecordId) -> Key {
        let lhs_num_keys = self.lhs.header.num_keys as usize;
        let split_at = lhs_num_keys.div_ceil(2);
        let rhs_num_keys = lhs_num_keys - split_at;
        let median_key = self.lhs.keys[split_at];
        // FIXME: optimize: insert key before copying
        rhs.keys[..rhs_num_keys].copy_from_slice(&self.lhs.keys[split_at..]);
        rhs.values[..rhs_num_keys].copy_from_slice(&self.lhs.values[split_at..]);

        self.lhs.header.num_keys = split_at as u16;
        rhs.header.num_keys = rhs_num_keys as u16;

        if key < median_key {
            self.lhs.insert(key, value);
        } else if key > median_key {
            rhs.insert(key, value);
        } else {
            unreachable!();
        }

        rhs.keys().first().copied().unwrap()
    }
}

impl BTreeLeafPage {
    #[inline]
    pub fn keys(&self) -> &[Key] {
        let num_keys = self.header.num_keys as usize;
        &self.keys[..num_keys]
    }

    #[inline]
    pub fn next_page_id(&self) -> PageId {
        self.next
    }

    #[inline]
    pub fn set_next_page_id(&mut self, page_id: PageId) {
        self.next = page_id;
    }

    pub fn search(&self, key: Key) -> Option<RecordId> {
        let pos = self.keys().binary_search(&key).ok()?;
        Some(self.values[pos])
    }

    pub fn init(&mut self) {
        self.header = BTreePageHeader {
            page_type: 1,
            num_keys: 0,
        };
        self.next = PAGE_INVALID;
    }

    pub fn insert(&mut self, key: Key, value: RecordId) -> Option<SplitLeaf<'_>> {
        match self.keys().binary_search(&key) {
            Ok(_) => {
                unimplemented!("duplicate keys");
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

impl From<&Page> for &BTreeLeafPage {
    fn from(page: &Page) -> Self {
        unsafe { &*(page.data.as_ptr() as *const BTreeLeafPage) }
    }
}

impl From<&mut Page> for &mut BTreeLeafPage {
    fn from(page: &mut Page) -> Self {
        unsafe { &mut *(page.data.as_mut_ptr() as *mut BTreeLeafPage) }
    }
}

#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct BTreeInnerPage {
    header: BTreePageHeader,
    keys: [Key; BTREE_NUM_KEYS],
    pointers: [PageId; BTREE_BRANCHING_FACTOR],
}

const _: () = assert!(std::mem::size_of::<BTreeInnerPage>() <= PAGE_SIZE);

pub struct SplitInner<'page> {
    lhs: &'page mut BTreeInnerPage,
}

impl SplitInner<'_> {
    pub fn split(&mut self, rhs: &mut BTreeInnerPage, key: Key, right_pointer: PageId) -> Key {
        let lhs_num_keys = self.lhs.header.num_keys as usize;
        let split_at = lhs_num_keys.div_ceil(2) - 1;
        let rhs_num_keys = lhs_num_keys - split_at;
        // FIXME: optimize: insert key before copying
        rhs.keys[..rhs_num_keys - 1].copy_from_slice(&self.lhs.keys[split_at + 1..]);
        rhs.pointers[..rhs_num_keys].copy_from_slice(&self.lhs.pointers[split_at + 1..]);

        self.lhs.header.num_keys = split_at as u16;
        rhs.header.num_keys = (rhs_num_keys - 1) as u16;

        let split_key = self.lhs.keys[split_at];
        if key > split_key {
            rhs.insert(key, right_pointer);
        } else if key < split_key {
            self.lhs.insert(key, right_pointer);
        } else {
            unreachable!();
        }

        split_key
    }
}

impl BTreeInnerPage {
    #[inline]
    pub fn keys(&self) -> &[Key] {
        let num_keys = self.header.num_keys as usize;
        &self.keys[..num_keys]
    }

    #[inline]
    pub fn pointers(&self) -> &[Key] {
        let num_keys = self.header.num_keys as usize;
        &self.pointers[..num_keys + 1]
    }

    pub fn search(&self, key: Key) -> PageId {
        match self.keys().binary_search(&key) {
            Ok(pos) => self.pointers[pos + 1],
            Err(pos) => self.pointers[pos],
        }
    }

    pub fn init(&mut self, key: Key, left_pointer: PageId, right_pointer: PageId) {
        self.header = BTreePageHeader {
            page_type: 0,
            num_keys: 1,
        };

        self.keys[0] = key;
        self.pointers[0] = left_pointer;
        self.pointers[1] = right_pointer;
    }

    pub fn init_header(&mut self) {
        self.header = BTreePageHeader {
            page_type: 0,
            num_keys: 0,
        };
    }

    pub fn insert(&mut self, key: Key, right_pointer: PageId) -> Option<SplitInner<'_>> {
        match self.keys().binary_search(&key) {
            Ok(_) => {
                unimplemented!("duplicate keys");
            }
            Err(pos) => {
                let num_keys = self.header.num_keys as usize;
                if num_keys < BTREE_NUM_KEYS {
                    self.keys.copy_within(pos..num_keys, pos + 1);
                    self.keys[pos] = key;
                    self.pointers.copy_within(pos + 1..num_keys + 1, pos + 2);
                    self.pointers[pos + 1] = right_pointer;
                    self.header.num_keys += 1;
                    None
                } else {
                    Some(SplitInner { lhs: self })
                }
            }
        }
    }

    pub fn delete(&mut self, key: Key) -> Result<(), BTreePageError> {
        todo!()
    }
}

impl From<&Page> for &BTreeInnerPage {
    fn from(page: &Page) -> Self {
        unsafe { &*(page.data.as_ptr() as *const BTreeInnerPage) }
    }
}

impl From<&mut Page> for &mut BTreeInnerPage {
    fn from(page: &mut Page) -> Self {
        unsafe { &mut *(page.data.as_mut_ptr() as *mut BTreeInnerPage) }
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

    #[cfg(test)]
    impl Default for BTreeLeafPage {
        fn default() -> Self {
            Self {
                header: BTreePageHeader {
                    page_type: BTreePageType::Leaf as u8,
                    num_keys: 0,
                },
                keys: [Key::default(); BTREE_NUM_KEYS],
                values: [make_record(0); BTREE_NUM_KEYS],
                next: Default::default(),
            }
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
            let key = (if key % 2 == 0 { key } else { key * 1000 }) as Key;
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

        assert!(lhs.keys().iter().chain(rhs.keys().iter()).is_sorted());
        assert_eq!(lhs.keys().len() + rhs.keys().len(), BTREE_NUM_KEYS + 1);
    }

    #[cfg(test)]
    impl Default for BTreeInnerPage {
        fn default() -> Self {
            Self {
                header: BTreePageHeader {
                    page_type: BTreePageType::Inner as u8,
                    num_keys: 0,
                },
                keys: [Key::default(); BTREE_NUM_KEYS],
                pointers: [PageId::default(); BTREE_BRANCHING_FACTOR],
            }
        }
    }

    #[test]
    fn test_inner_page_basic() {
        let mut inner = BTreeInnerPage::default();

        inner.init(0, 1, 2);
        for key in 1..BTREE_NUM_KEYS {
            inner.insert(key as Key, key as PageId);
        }
    }
}
