use crate::cache::{PageCacheError, StoragePageCache};
use crate::pages::{HeapPageError, HeapPageSlotId, PageId, RecordId};
use crate::sql::schema::Schema;
use crate::storage::StorageBackend;
use crate::tuple::{Tuple, TupleError};

use thiserror::Error;

pub struct Table<S: StorageBackend + 'static> {
    pub name: String,
    pub schema: Schema,
    cache: StoragePageCache<S>,
}

#[derive(Debug, Error)]
pub enum TableError {
    #[error("heappage error")]
    HeapPage(#[from] HeapPageError),
    #[error("page cache error")]
    PageCache(#[from] PageCacheError),
    #[error("tuple error")]
    Tuple(#[from] TupleError),
}

impl<S: StorageBackend + 'static> Table<S> {
    pub fn try_new(
        name: &str,
        schema: &Schema,
        cache: StoragePageCache<S>,
    ) -> Result<Self, TableError> {
        Ok(Self {
            name: name.to_string(),
            schema: schema.clone(),
            cache,
        })
    }

    pub fn get(&self, record_id: RecordId) -> Result<Tuple, TableError> {
        let page_ref = self
            .cache
            .get_page(record_id.page_id)
            .map_err(TableError::PageCache)?;
        let heappage = page_ref.heap_page();

        Ok(heappage
            .get_tuple(record_id.slot_id)
            .map_err(TableError::HeapPage)?
            .to_owned(&self.schema))
    }

    pub fn insert(&self, tuple: &Tuple) -> Result<RecordId, TableError> {
        self.validate_tuple(tuple)?;

        let mut page_ref = self
            .cache
            .get_page_mut(self.cache.last_page_id())
            .map_err(TableError::PageCache)?;
        let heappage = page_ref.heap_page_mut();
        match heappage.insert_tuple(tuple) {
            Ok(slot_id) => {
                let metadata = page_ref.metadata();
                self.cache.set_page_dirty(metadata);
                Ok(RecordId::new(metadata.page_id(), slot_id))
            }
            Err(HeapPageError::NoFreeSpace) => {
                let mut page_ref = self.cache.new_page().map_err(TableError::PageCache)?;
                let heappage = page_ref.heap_page_mut();
                let slot_id = heappage.insert_tuple(tuple).map_err(TableError::HeapPage)?;
                let metadata = page_ref.metadata();
                self.cache.set_page_dirty(metadata);
                Ok(RecordId::new(metadata.page_id(), slot_id))
            }
            Err(e) => Err(TableError::from(e)),
        }
    }

    pub fn delete(&self, record_id: RecordId) -> Result<(), TableError> {
        let mut page_ref = self
            .cache
            .get_page_mut(record_id.page_id)
            .map_err(TableError::PageCache)?;
        let heappage = page_ref.heap_page_mut();

        heappage
            .delete_tuple(record_id.slot_id)
            .map_err(TableError::HeapPage)?;

        Ok(())
    }

    fn validate_tuple(&self, tuple: &Tuple) -> Result<(), TableError> {
        // check data types and nullable constraints
        tuple
            .validate_with_schema(&self.schema)
            .map_err(TableError::Tuple)

        // TODO: check for column uniqueness
    }

    pub fn iter(&self) -> TableIterator<'_, S> {
        TableIterator::new(self)
    }
}

pub struct TableIterator<'table, S: StorageBackend + 'static> {
    table: &'table Table<S>,
    page_id: PageId,
    slot_id: HeapPageSlotId,
}

impl<'table, S: StorageBackend + 'static> TableIterator<'table, S> {
    pub fn new(table: &'table Table<S>) -> Self {
        Self {
            table,
            page_id: table.cache.first_page_id(),
            slot_id: HeapPageSlotId::new(0),
        }
    }
}

impl<'table, S: StorageBackend + 'static> Iterator for TableIterator<'table, S> {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        let mut page_ref = self.table.cache.get_page(self.page_id).ok()?;

        loop {
            let heappage = page_ref.heap_page();
            match heappage.get_tuple(self.slot_id) {
                Ok(tuple) => {
                    self.slot_id.next();
                    return Some(tuple.to_owned(&self.table.schema));
                }
                Err(HeapPageError::SlotDeleted) => {
                    self.slot_id.next();
                }
                Err(HeapPageError::SlotNotFound) => {
                    self.page_id.next();
                    page_ref = self.table.cache.get_page(self.page_id).ok()?;
                    self.slot_id = HeapPageSlotId::new(0);
                }
                Err(_) => unreachable!(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use crate::cache::GLOBAL_PAGE_CACHE;
    use crate::pages::{HeapPageSlotId, PageId, RecordId};
    use crate::sql::schema::{Column, ConstraintsBuilder, DataType, Schema};
    use crate::sql::types::Value;
    use crate::storage::FileStorage;
    use crate::table::Table;
    use crate::tuple::Tuple;

    const NR_ROWS: usize = 10000;

    fn test_table(fill: bool) -> Table<FileStorage> {
        let storage_path = NamedTempFile::new().unwrap();
        let storage = FileStorage::create(storage_path).unwrap();
        let cache = GLOBAL_PAGE_CACHE.cache_storage(storage);
        let schema = Schema::try_new(vec![Column::new(
            "id".into(),
            DataType::Integer,
            ConstraintsBuilder::new().build(),
        )])
        .unwrap();

        let table = Table::try_new("test_tbl", &schema, cache).unwrap();
        if fill {
            for id in 0..NR_ROWS {
                let tuple = Tuple::try_new(vec![Value::Integer(id as i64)]).unwrap();
                table.insert(&tuple).unwrap();
            }
        }
        table
    }

    #[test]
    fn insert_and_get() {
        let table = test_table(false);

        let tuple = Tuple::try_new(vec![Value::Integer(42)]).unwrap();
        let record_id = table.insert(&tuple).unwrap();

        let retrieved_tuple = table.get(record_id).unwrap();
        assert_eq!(retrieved_tuple.values()[0], Value::Integer(42));
    }

    #[test]
    fn insert_multiple_columns() {
        let storage_path = NamedTempFile::new().unwrap();
        let storage = FileStorage::create(storage_path).unwrap();
        let cache = GLOBAL_PAGE_CACHE.cache_storage(storage);
        let schema = Schema::try_new(vec![
            Column::new(
                "id".into(),
                DataType::Integer,
                ConstraintsBuilder::new().build(),
            ),
            Column::new(
                "name".into(),
                DataType::VarChar,
                ConstraintsBuilder::new().build(),
            ),
        ])
        .unwrap();
        let table = Table::try_new("test_tbl", &schema, cache).unwrap();

        let tuple =
            Tuple::try_new(vec![Value::Integer(42), Value::VarChar("test".to_string())]).unwrap();
        let record_id = table.insert(&tuple).unwrap();

        let retrieved_tuple = table.get(record_id).unwrap();
        assert_eq!(retrieved_tuple.values()[0], Value::Integer(42));
        assert_eq!(
            retrieved_tuple.values()[1],
            Value::VarChar("test".to_string())
        );
    }

    #[test]
    fn insert_invalid_tuple() {
        let table = test_table(false);

        // Try to insert a tuple with wrong schema (too many values)
        let tuple = Tuple::try_new(vec![Value::Integer(42), Value::Integer(43)]).unwrap();
        let result = table.insert(&tuple);
        assert!(result.is_err());
    }

    #[test]
    fn delete() {
        let table = test_table(false);

        let tuple = Tuple::try_new(vec![Value::Integer(42)]).unwrap();
        let record_id = table.insert(&tuple).unwrap();

        table.delete(record_id).unwrap();

        let result = table.get(record_id);
        assert!(result.is_err());
    }

    #[test]
    fn delete_nonexistent() {
        let table = test_table(false);

        let record_id = RecordId::new(PageId::new(0), HeapPageSlotId::new(0));
        let result = table.delete(record_id);
        assert!(result.is_err());
    }

    #[test]
    fn contraint_nullable() {
        let table = test_table(false);

        let tuple = Tuple::try_new(vec![Value::Null]).unwrap();
        let result = table.insert(&tuple);
        assert!(result.is_err());
    }

    #[test]
    fn constraint_unique() {
        // TODO: this test would require a unique constraint implementation
        let table = test_table(false);

        let tuple = Tuple::try_new(vec![Value::Integer(42)]).unwrap();
        table.insert(&tuple).unwrap();

        let _result = table.insert(&tuple);
        // assert!(result.is_err());
    }

    #[test]
    fn iterator() {
        let table = test_table(true);
        assert_eq!(table.iter().count(), NR_ROWS);
        assert!(
            table
                .iter()
                .enumerate()
                .all(|(id, tuple)| { tuple.values()[0] == Value::Integer(id as i64) })
        );
    }

    #[test]
    fn iterator_empty_table() {
        let table = test_table(false);
        assert_eq!(table.iter().count(), 0);
    }

    #[test]
    fn iterator_with_deleted_slots() {
        let table = test_table(false);

        let record_ids: Vec<_> = (0..5i64)
            .map(|i| {
                let tuple = Tuple::try_new(vec![Value::Integer(i)]).unwrap();
                table.insert(&tuple).unwrap()
            })
            .collect();

        table.delete(record_ids[1]).unwrap();
        table.delete(record_ids[3]).unwrap();

        let values: Vec<i64> = table
            .iter()
            .map(|tuple| {
                if let Value::Integer(integer) = tuple.values()[0] {
                    integer
                } else {
                    panic!("Expected Integer value")
                }
            })
            .collect();

        assert_eq!(values, vec![0, 2, 4]);
    }
}
