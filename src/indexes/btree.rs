use crate::cache::{PageCache, PageCacheError, PageRef, PageRefMut};
use crate::pages::{
    BTreePageError, BTreePageType, Key, PAGE_INVALID, PAGE_RESERVED, PageId, RecordId,
};
use crate::storage::Storage;

use crate::pages::btree_get_page_type;

use thiserror::Error;

/// A B+ tree implementation for indexing and storing key-value pairs.
///
/// The `BTree` struct provides a high-level interface for creating, searching, inserting,
/// and deleting records. It abstracts away the underlying page management by using a `PageCache`.
///
/// Key characteristics:
/// - It is a B+ tree, meaning all records are stored in the leaf pages.
/// - Leaf pages are linked together to allow for efficient range scans.
/// - Deletion does not trigger merging or redistribution of nodes. This simplifies the
///   implementation and can improve delete performance by avoiding complex rebalancing
///   operations. However, it may lead to lower storage utilization over time if the
///   workload has many deletions.
///
/// B+ Tree Structure:
/// ```text
///                    +--------------------+
///                    |    Root Page       |  (Inner page with separator keys and pointers)
///                    |--------------------|
///                    | Keys:  [20, 40]    |
///                    | Ptrs:  [1, 2, 3]   |
///                    +--------------------+
///                   /          |          \
///                  /           |           \
///                 v            v            v
///        +--------------+  +--------------+  +--------------+
///        |  Inner Page  |  |  Inner Page  |  |  Inner Page  |  (Inner pages)
///        |--------------|  |--------------|  |--------------|
///        | Keys: [10]   |  | Keys: [30]   |  | Keys: [50]   |
///        | Ptrs: [4,5]  |  | Ptrs: [6,7]  |  | Ptrs: [8,9]  |
///        +--------------+  +--------------+  +--------------+
///             /     \           /     \           /     \
///            /       \         /       \         /       \
///           v         v       v         v       v         v
///    +-------------+  +-------------+  +-------------+  +-------------+
///    |  Leaf Page  |  |  Leaf Page  |  |  Leaf Page  |  |  Leaf Page  |  (Leaf pages)
///    |-------------|  |-------------|  |-------------|  |-------------|
///    | Keys: 1..19 |  | Keys:20..39 |  | Keys:40..59 |  | Keys:60..79 |
///    | Values: Rids|  | Values: Rids|  | Values: Rids|  | Values: Rids|
///    | Next: -----+|  | Next: -----+|  | Next: -----+|  | Next:  NULL |
///    +-------------+  +-------------+  +-------------+  +-------------+
///           |                |                |                |
///           +----------------+----------------+----------------+  (Linked list)
/// ```
pub struct BTree {
    page_cache: PageCache,
}

#[derive(Error, Debug)]
pub enum BTreeError {
    #[error("error accessing a btree page")]
    Page(#[from] BTreePageError),
    #[error("page cache error")]
    PageCache(#[from] PageCacheError),
}

impl BTree {
    /// Creates a new B-tree.
    ///
    /// Returns a `Result` containing the new `BTree` instance, or a `BTreeError` on failure.
    pub fn try_new(storage: Storage) -> Result<Self, BTreeError> {
        let page_cache = PageCache::new(storage);
        let mut superblock_ref = page_cache.get_page_mut(PAGE_RESERVED)?;
        let superblock = superblock_ref.btree_superblock_mut();
        let mut root_page_ref = page_cache.new_page().map_err(BTreeError::PageCache)?;

        let root_page_id = root_page_ref.metadata().page_id;
        let root_page = root_page_ref.btree_leaf_page_mut();
        root_page.init();
        superblock.init(root_page_id);
        drop(root_page_ref);
        drop(superblock_ref);

        Ok(Self { page_cache })
    }

    /// Finds the leaf page that should contain the given key.
    ///
    /// Returns a `Result` containing a read-only reference to the leaf page, or a `BTreeError` on failure.
    fn find_leaf_page(&self, key: Key) -> Result<PageRef<'_>, BTreeError> {
        let mut page_ref = {
            let superblock_ref = self.page_cache.get_page(PAGE_RESERVED)?;
            let superblock = superblock_ref.btree_superblock();
            self.page_cache
                .get_page(superblock.root_page_id)
                .map_err(BTreeError::PageCache)?
        };

        loop {
            match btree_get_page_type(page_ref.page()) {
                BTreePageType::Inner => {
                    let inner_page = page_ref.btree_inner_page();
                    let page_id = inner_page.get(key);
                    page_ref = self
                        .page_cache
                        .get_page(page_id)
                        .map_err(BTreeError::PageCache)?;
                }
                BTreePageType::Leaf => {
                    return Ok(page_ref);
                }
            }
        }
    }

    /// Finds the leaf page that should contain the given key.
    ///
    /// Returns a `Result` containing a mutable reference to the leaf page, or a `BTreeError` on failure.
    fn find_leaf_page_mut(&self, key: Key) -> Result<PageRefMut<'_>, BTreeError> {
        let mut parent_page_ref = {
            let superblock_ref = self.page_cache.get_page(PAGE_RESERVED)?;
            let superblock = superblock_ref.btree_superblock();
            let page_ref = self
                .page_cache
                .get_page(superblock.root_page_id)
                .map_err(BTreeError::PageCache)?;

            if btree_get_page_type(page_ref.page()).is_leaf() {
                drop(page_ref);
                return self
                    .page_cache
                    .get_page_mut(superblock.root_page_id)
                    .map_err(BTreeError::PageCache);
            }

            page_ref
        };

        loop {
            let inner_page = parent_page_ref.btree_inner_page();
            let child_page_id = inner_page.get(key);
            let child_page_ref = self
                .page_cache
                .get_page(child_page_id)
                .map_err(BTreeError::PageCache)?;

            match btree_get_page_type(child_page_ref.page()) {
                BTreePageType::Inner => parent_page_ref = child_page_ref,
                BTreePageType::Leaf => {
                    let child_page_id = child_page_ref.metadata().page_id;
                    drop(child_page_ref);
                    return self
                        .page_cache
                        .get_page_mut(child_page_id)
                        .map_err(BTreeError::PageCache);
                }
            }
        }
    }

    /// Searches for a record by its key.
    ///
    /// Returns an `Option` containing the `RecordId` if the key is found, or `None` otherwise.
    pub fn search(&self, key: Key) -> Option<RecordId> {
        // For convinience we return an Option.
        // We should log errors instead of unwraping.
        let page_ref = self.find_leaf_page(key).unwrap();
        let leaf_page = page_ref.btree_leaf_page();
        leaf_page.get(key)
    }

    fn insert_inner_r(
        &self,
        inner_page_ref: &mut PageRefMut<'_>,
        key: Key,
        value: RecordId,
    ) -> Result<Option<(Key, PageId)>, BTreeError> {
        let inner_page = inner_page_ref.btree_inner_page_mut();

        let child_page_id = inner_page.get(key);
        let mut child_page_ref = self
            .page_cache
            .get_page_mut(child_page_id)
            .map_err(BTreeError::PageCache)?;

        let result = match btree_get_page_type(child_page_ref.page()) {
            BTreePageType::Inner => self.insert_inner_r(&mut child_page_ref, key, value)?,
            BTreePageType::Leaf => self.insert_leaf(&mut child_page_ref, key, value)?,
        };

        if let Some((split_key, rhs_page_id)) = result
            && let Some(mut split) = inner_page.insert(split_key, rhs_page_id)
        {
            let mut rhs_inner_page_ref =
                self.page_cache.new_page().map_err(BTreeError::PageCache)?;
            let rhs_inner_page_id = rhs_inner_page_ref.metadata().page_id;
            let rhs_inner_page = rhs_inner_page_ref.btree_inner_page_mut();
            rhs_inner_page.init_header();
            let split_key = split.split(rhs_inner_page, split_key, rhs_page_id);

            inner_page_ref.metadata_mut().set_dirty();
            rhs_inner_page_ref.metadata_mut().set_dirty();

            Ok(Some((split_key, rhs_inner_page_id)))
        } else {
            Ok(None)
        }
    }

    fn insert_leaf(
        &self,
        lhs_page_ref: &mut PageRefMut<'_>,
        key: Key,
        value: RecordId,
    ) -> Result<Option<(Key, PageId)>, BTreeError> {
        let lhs = lhs_page_ref.btree_leaf_page_mut();
        if let Some(mut split) = lhs.insert(key, value) {
            let mut rhs_page_ref = self.page_cache.new_page().map_err(BTreeError::PageCache)?;
            let rhs = rhs_page_ref.btree_leaf_page_mut();
            rhs.init();
            let split_key = split.split(rhs, key, value);
            let rhs_page_id = rhs_page_ref.metadata().page_id;
            lhs.set_next_page_id(rhs_page_id);

            lhs_page_ref.metadata_mut().set_dirty();
            rhs_page_ref.metadata_mut().set_dirty();

            Ok(Some((split_key, rhs_page_id)))
        } else {
            Ok(None)
        }
    }

    /// Inserts a new key-value pair into the B-tree.
    ///
    /// Returns an empty `Result` if successful, or a `BTreeError` on failure.
    pub fn insert(&self, key: Key, record_id: RecordId) -> Result<(), BTreeError> {
        // Fast path: get an exclusive lock on the leaf, every parent has its lock released.
        // This optimization is useful for mixed workload. For write-heavy applications
        // the performance decreases slightly : if a split occurs in the leaf we need to insert
        // the key via the slow path.
        let mut leaf_page_ref = self.find_leaf_page_mut(key)?;
        let leaf_page = leaf_page_ref.btree_leaf_page_mut();
        if leaf_page.insert(key, record_id).is_some() {
            drop(leaf_page_ref);
            self.insert_slow_path(key, record_id)
        } else {
            Ok(())
        }
    }

    pub fn insert_slow_path(&self, key: Key, record_id: RecordId) -> Result<(), BTreeError> {
        // Slow path: we descend in the tree, getting an exclusive lock at every step.
        let mut superblock_ref = self.page_cache.get_page_mut(PAGE_RESERVED)?;
        let superblock = superblock_ref.btree_superblock_mut();
        let root_page_id = superblock.root_page_id;

        let mut root_page_ref = self
            .page_cache
            .get_page_mut(root_page_id)
            .map_err(BTreeError::PageCache)?;

        let result = match btree_get_page_type(root_page_ref.page()) {
            BTreePageType::Inner => self.insert_inner_r(&mut root_page_ref, key, record_id)?,
            BTreePageType::Leaf => self.insert_leaf(&mut root_page_ref, key, record_id)?,
        };

        if let Some((split_key, rhs_page_id)) = result {
            let mut new_root_page_ref =
                self.page_cache.new_page().map_err(BTreeError::PageCache)?;
            let new_root_page_id = new_root_page_ref.metadata().page_id;
            let new_root_page = new_root_page_ref.btree_inner_page_mut();
            new_root_page.init(split_key, root_page_id, rhs_page_id);
            new_root_page_ref.metadata_mut().set_dirty();
            superblock.root_page_id = new_root_page_id;
        }

        Ok(())
    }

    /// Deletes a key-value pair from the B-tree.
    ///
    /// Returns an empty `Result` if successful, or a `BTreeError` if the key is not found.
    pub fn delete(&self, key: Key) -> Result<(), BTreeError> {
        let mut leaf_page_ref = self.find_leaf_page_mut(key)?;
        let leaf_page = leaf_page_ref.btree_leaf_page_mut();
        leaf_page.delete(key).map_err(BTreeError::Page)
    }

    /// Creates an iterator over a range of keys.
    ///
    /// Returns a `Result` containing the `BTreeRangeIterator`, or a `BTreeError` on failure.
    pub fn iter(&self, start: Key) -> Result<BTreeRangeIterator<'_>, BTreeError> {
        let page_ref = self.find_leaf_page(start)?;
        let leaf_page = page_ref.btree_leaf_page();
        // FIXME: what if the key doesn't exist ?
        let pos = match leaf_page.keys().binary_search(&start) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };

        Ok(BTreeRangeIterator {
            pos,
            btree: self,
            page_ref,
        })
    }
}

pub struct BTreeRangeIterator<'btree> {
    pos: usize,
    btree: &'btree BTree,
    page_ref: PageRef<'btree>,
}

impl<'btree> Iterator for BTreeRangeIterator<'btree> {
    type Item = (Key, RecordId);

    fn next(&mut self) -> Option<Self::Item> {
        let leaf_page = self.page_ref.btree_leaf_page();

        if self.pos >= leaf_page.len() {
            if leaf_page.next_page_id() == PAGE_INVALID {
                return None;
            }

            self.page_ref = self
                .btree
                .page_cache
                .get_page(leaf_page.next_page_id())
                .map_err(|_| todo!("log errors"))
                .ok()?;

            self.pos = 0;
        }

        let leaf_page = self.page_ref.btree_leaf_page();
        let (key, record_id) = (leaf_page.key_at(self.pos), leaf_page.value_at(self.pos));
        self.pos += 1;

        Some((key, record_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{collections::VecDeque, sync::Arc};

    use tempfile::NamedTempFile;

    const NR_KEYS: Key = 1000;

    fn create_btree() -> BTree {
        let storage_path = NamedTempFile::new().unwrap();
        let storage = Storage::open(storage_path).unwrap();
        BTree::try_new(storage).unwrap()
    }

    #[allow(dead_code)]
    fn print_btree(btree: &BTree) {
        let root_page_id = {
            let superblock_ref = btree.page_cache.get_page(PAGE_RESERVED).unwrap();
            let superblock = superblock_ref.btree_superblock();
            superblock.root_page_id
        };
        let mut queue = VecDeque::from([vec![root_page_id]]);

        while let Some(page_ids) = queue.pop_front()
            && !page_ids.is_empty()
        {
            let mut new_page_ids = vec![];
            for page_id in page_ids {
                let page_ref = btree.page_cache.get_page(page_id).unwrap();
                let page_id = page_ref.metadata().page_id;
                match btree_get_page_type(page_ref.page()) {
                    BTreePageType::Inner => {
                        let inner_page = page_ref.btree_inner_page();
                        print!(
                            " Inner({page_id}): keys={:?} pointers={:?} |",
                            inner_page.keys(),
                            inner_page.pointers()
                        );
                        new_page_ids.extend(inner_page.pointers());
                    }
                    BTreePageType::Leaf => {
                        let leaf_page = page_ref.btree_leaf_page();
                        print!(
                            " Leaf({})=>({}): keys={:?} |",
                            page_id,
                            leaf_page.next_page_id(),
                            leaf_page.keys()
                        );
                    }
                }
            }
            queue.push_back(new_page_ids);
            println!();
        }
    }

    #[test]
    fn btree_new() {
        let _ = create_btree();
    }

    #[test]
    fn insert_multiple_records_increasing() {
        let btree = create_btree();

        for key in 0..NR_KEYS {
            btree.insert(key, RecordId::new(0, 0)).unwrap();
        }

        for key in 0..NR_KEYS {
            assert!(btree.search(key).is_some());
        }
    }

    #[test]
    fn insert_multiple_records_decreasing() {
        let btree = create_btree();

        for key in (0..NR_KEYS).rev() {
            btree.insert(key, RecordId::new(0, 0)).unwrap();
        }

        for key in (0..NR_KEYS).rev() {
            assert!(btree.search(key).is_some());
        }
    }

    #[test]
    fn insert_multiple_records_non_monotonic() {
        let btree = create_btree();

        for key in 0..NR_KEYS {
            let key = (if key % 2 == 0 { key } else { key * 1000 }) as Key;
            btree.insert(key, RecordId::new(0, 0)).unwrap();
        }
        for key in 0..NR_KEYS {
            let key = (if key % 2 == 0 { key } else { key * 1000 }) as Key;
            assert!(btree.search(key).is_some());
        }
    }

    #[test]
    #[should_panic]
    fn insert_duplicate_key() {
        let btree = create_btree();
        btree.insert(10, RecordId::new(0, 0)).unwrap();
        btree.insert(10, RecordId::new(0, 0)).unwrap();
    }

    #[test]
    fn search() {
        let btree = create_btree();

        for key in 0..NR_KEYS {
            btree.insert(key * 2, RecordId::new(0, 0)).unwrap();
        }
        assert!(btree.search(10).is_some());
        assert!(btree.search(9).is_none());
        assert!(btree.search(11).is_none());
    }

    #[test]
    fn search_empty_tree() {
        let btree = create_btree();
        assert!(btree.search(42).is_none());
    }

    #[test]
    fn search_nonexistent_key() {
        let btree = create_btree();
        btree.insert(10, RecordId::new(0, 0)).unwrap();
        btree.insert(20, RecordId::new(0, 0)).unwrap();

        // Search for keys that don't exist
        assert!(btree.search(1).is_none());
        assert!(btree.search(15).is_none());
        assert!(btree.search(25).is_none());
    }

    #[test]
    fn delete_existing_key() {
        let btree = create_btree();
        btree.insert(10, RecordId::new(0, 0)).unwrap();
        btree.insert(20, RecordId::new(0, 0)).unwrap();
        btree.insert(30, RecordId::new(0, 0)).unwrap();

        let _ = btree.delete(20);

        assert!(btree.search(20).is_none());
        assert!(btree.search(10).is_some());
        assert!(btree.search(30).is_some());
    }

    #[test]
    fn delete_nonexistent_key() {
        let btree = create_btree();
        btree.insert(10, RecordId::new(0, 0)).unwrap();

        assert!(matches!(
            btree.delete(20),
            Err(BTreeError::Page(BTreePageError::KeyNotFound))
        ));
        assert!(btree.search(10).is_some());
    }

    #[test]
    fn delete_from_empty_tree() {
        let btree = create_btree();

        assert!(matches!(
            btree.delete(20),
            Err(BTreeError::Page(BTreePageError::KeyNotFound))
        ));
    }

    #[test]
    fn delete_all_records() {
        let btree = create_btree();

        for key in 0..1000 {
            btree.insert(key, RecordId::new(0, 0)).unwrap();
        }

        for key in 0..1000 {
            let _ = btree.delete(key);
        }

        for key in 0..1000 {
            assert!(btree.search(key).is_none());
        }
    }

    #[test]
    fn iterator() {
        let btree = create_btree();

        for key in 0..1000 {
            btree.insert(key, RecordId::new(0, 0)).unwrap();
        }
        assert!(btree.search(0).is_some());
        assert!(btree.search(999).is_some());
        assert_eq!(btree.iter(0).unwrap().count(), 1000);
        let keys = btree.iter(0).unwrap().map(|(key, _)| key);
        assert!(keys.eq(0..1000));
    }

    #[test]
    fn concurrent_insert() {
        const NUM_THREADS: usize = 8;
        const KEYS_PER_THREAD: usize = 10000;
        let btree = Arc::new(create_btree());
        let mut handles = Vec::new();

        for i in 0..NUM_THREADS {
            let btree = btree.clone();
            let handle = std::thread::spawn(move || {
                for key in 0..KEYS_PER_THREAD {
                    let key = i * KEYS_PER_THREAD + key;
                    btree.insert(key as Key, RecordId::new(0, 0)).unwrap();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for key in 0..NUM_THREADS * KEYS_PER_THREAD {
            assert!(btree.search(key as Key).is_some());
        }
    }

    #[test]
    fn concurrent_insert_search_delete() {
        fn create_ranges(count: usize, step: usize) -> Vec<core::ops::Range<usize>> {
            (0..count).map(|i| (i * step)..((i + 1) * step)).collect()
        }

        const NUM_RANGES: usize = 3;
        const NUM_THREADS: usize = NUM_RANGES * 4;
        let ranges = Arc::new(create_ranges(NUM_RANGES, 10000));
        let btree = Arc::new(create_btree());
        let mut handles = Vec::new();

        for i in 0..NUM_THREADS {
            let btree = btree.clone();
            let ranges = ranges.clone();
            let handle = std::thread::spawn(move || match i {
                0..NUM_RANGES => {
                    let range = ranges[i % NUM_RANGES].clone();
                    for key in range {
                        btree.insert(key as Key, RecordId::new(0, 0)).unwrap();
                    }
                }
                NUM_RANGES.. if i % 2 == 0 => {
                    let range = ranges[i % NUM_RANGES].clone();
                    for key in range {
                        let _ = btree.search(key as Key);
                    }
                }
                NUM_RANGES.. => {
                    if i % 2 == 1 {
                        let range = ranges[i % NUM_RANGES].clone();
                        for key in range {
                            let _ = btree.delete(key as Key);
                        }
                    }
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
