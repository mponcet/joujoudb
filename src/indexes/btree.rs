use crate::cache::{PageCache, PageCacheError, PageRefMut};
use crate::pages::{BTreePageError, BTreePageType, Key, PageId, RecordId};
use crate::storage::Storage;

use crate::pages::btree_get_page_type;

use thiserror::Error;

struct BTree {
    root_page_id: PageId,
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
    fn try_new(storage: Storage) -> Result<Self, BTreeError> {
        let page_cache = PageCache::new(storage);
        let mut root_page_ref = page_cache.new_page().map_err(BTreeError::PageCache)?;
        let root_page_id = root_page_ref.metadata().page_id;
        let root_page = root_page_ref.btree_leaf_page_mut();
        root_page.init();
        drop(root_page_ref);

        Ok(Self {
            root_page_id,
            page_cache,
        })
    }

    fn search(&self, key: Key) -> Option<RecordId> {
        let mut page_id = self.root_page_id;

        loop {
            let page_ref = self
                .page_cache
                .get_page(page_id)
                .expect("btree dead pointer");

            match btree_get_page_type(page_ref.page()) {
                BTreePageType::Inner => {
                    let inner_page = page_ref.btree_inner_page();
                    page_id = inner_page.search(key);
                }
                BTreePageType::Leaf => {
                    let leaf_page = page_ref.btree_leaf_page();
                    return leaf_page.search(key);
                }
            }
        }
    }

    fn insert_inner_r(
        &self,
        mut inner_page_ref: PageRefMut<'_>,
        key: Key,
        value: RecordId,
    ) -> Result<Option<(Key, PageId)>, BTreeError> {
        let inner_page = inner_page_ref.btree_inner_page_mut();

        let children_page_id = inner_page.search(key);
        let children_page_ref = self
            .page_cache
            .get_page_mut(children_page_id)
            .map_err(BTreeError::PageCache)?;

        let result = match btree_get_page_type(children_page_ref.page()) {
            BTreePageType::Inner => self.insert_inner_r(children_page_ref, key, value)?,
            BTreePageType::Leaf => self.insert_leaf(children_page_ref, key, value)?,
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
        mut lhs_page_ref: PageRefMut<'_>,
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

    pub fn insert(&mut self, key: Key, record_id: RecordId) -> Result<(), BTreeError> {
        let root_page_ref = self
            .page_cache
            .get_page_mut(self.root_page_id)
            .map_err(BTreeError::PageCache)?;

        let result = match btree_get_page_type(root_page_ref.page()) {
            BTreePageType::Inner => self.insert_inner_r(root_page_ref, key, record_id)?,
            BTreePageType::Leaf => self.insert_leaf(root_page_ref, key, record_id)?,
        };

        if let Some((split_key, rhs_page_id)) = result {
            let mut new_root_page_ref =
                self.page_cache.new_page().map_err(BTreeError::PageCache)?;
            let new_root_page = new_root_page_ref.btree_inner_page_mut();
            new_root_page.init(split_key, self.root_page_id, rhs_page_id);
            new_root_page_ref.metadata_mut().set_dirty();
            self.root_page_id = new_root_page_ref.metadata().page_id;
        }

        Ok(())
    }

    fn delete_r(&self, page_id: PageId, key: Key) -> Result<(), BTreeError> {
        let page_ref = self
            .page_cache
            .get_page(page_id)
            .map_err(BTreeError::PageCache)?;

        match btree_get_page_type(page_ref.page()) {
            BTreePageType::Inner => {
                let inner_page = page_ref.btree_inner_page();
                let children_page_id = inner_page.search(key);
                self.delete_r(children_page_id, key)
            }
            BTreePageType::Leaf => {
                drop(page_ref);
                // reacquire a page lock, but mutable
                let mut page_ref = self
                    .page_cache
                    .get_page_mut(page_id)
                    .map_err(BTreeError::PageCache)?;
                let leaf_page = page_ref.btree_leaf_page_mut();
                let result = leaf_page.delete(key).map_err(BTreeError::Page);
                page_ref.metadata_mut().set_dirty();
                result
            }
        }
    }

    pub fn delete(&mut self, key: Key) -> Result<(), BTreeError> {
        self.delete_r(self.root_page_id, key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::VecDeque;

    const NR_KEYS: Key = 1000;

    fn test_path() -> std::path::PathBuf {
        [
            "/tmp/",
            "joujoudb_",
            uuid::Uuid::new_v4().to_string().as_str(),
        ]
        .into_iter()
        .collect::<String>()
        .into()
    }

    fn create_btree() -> BTree {
        let storage = Storage::open(test_path()).unwrap();
        BTree::try_new(storage).unwrap()
    }

    fn print_btree(btree: &BTree) {
        let mut queue = VecDeque::from([vec![btree.root_page_id]]);

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
        let btree = create_btree();
        assert!(btree.root_page_id > 0);
    }

    #[test]
    fn insert_multiple_records_increasing() {
        let mut btree = create_btree();

        for key in 0..NR_KEYS {
            btree.insert(key, RecordId::new(0, 0)).unwrap();
        }

        for key in 0..NR_KEYS {
            assert!(btree.search(key).is_some());
        }
    }

    #[test]
    fn insert_multiple_records_decreasing() {
        let mut btree = create_btree();

        for key in (0..NR_KEYS).rev() {
            btree.insert(key, RecordId::new(0, 0)).unwrap();
        }

        for key in (0..NR_KEYS).rev() {
            assert!(btree.search(key).is_some());
        }
    }

    #[test]
    fn insert_multiple_records_non_monotonic() {
        let mut btree = create_btree();

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
        let mut btree = create_btree();
        btree.insert(10, RecordId::new(0, 0)).unwrap();
        btree.insert(10, RecordId::new(0, 0)).unwrap();
    }

    #[test]
    fn search() {
        let mut btree = create_btree();

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
        let mut btree = create_btree();
        btree.insert(10, RecordId::new(0, 0)).unwrap();
        btree.insert(20, RecordId::new(0, 0)).unwrap();

        // Search for keys that don't exist
        assert!(btree.search(1).is_none());
        assert!(btree.search(15).is_none());
        assert!(btree.search(25).is_none());
    }

    #[test]
    fn delete_existing_key() {
        let mut btree = create_btree();
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
        let mut btree = create_btree();
        btree.insert(10, RecordId::new(0, 0)).unwrap();

        assert!(matches!(
            btree.delete(20),
            Err(BTreeError::Page(BTreePageError::KeyNotFound))
        ));
        assert!(btree.search(10).is_some());
    }

    #[test]
    fn delete_from_empty_tree() {
        let mut btree = create_btree();

        assert!(matches!(
            btree.delete(20),
            Err(BTreeError::Page(BTreePageError::KeyNotFound))
        ));
    }

    #[test]
    fn delete_all_records() {
        let mut btree = create_btree();

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
}
