use crate::cache::{PageCacheError, PageRef, StoragePageCache};
use crate::pages::{HeapPageError, HeapPageSlotId, PageId, RecordId};
use crate::sql::schema::Schema;
use crate::sql::types::Value;
use crate::storage::StorageBackend;
use crate::tuple::{Tuple, TupleRef};

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
}

pub struct ResultSet {
    values: Vec<Value>,
    schema: Schema,
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

    pub fn get_tuple(&self, record_id: RecordId) -> Result<Tuple, TableError> {
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

    pub fn insert_tuple(&self, tuple: &Tuple) -> Result<(), TableError> {
        let mut page_ref = self
            .cache
            .get_page_mut(self.cache.last_page_id())
            .map_err(TableError::PageCache)?;
        let heappage = page_ref.heap_page_mut();

        heappage.insert_tuple(tuple).map_err(TableError::HeapPage)?;
        let metadata = page_ref.metadata();
        let page_id = metadata.page_id;
        // metadata.set_dirty();
        drop(page_ref);
        println!("writeback");
        // self.cache.writeback(page_id);
        println!("writeback finished");

        Ok(())
    }

    pub fn delete_tuple(&self, record_id: RecordId) -> Result<(), TableError> {
        let mut page_ref = self
            .cache
            .get_page_mut(self.cache.last_page_id())
            .map_err(TableError::PageCache)?;
        let heappage = page_ref.heap_page_mut();

        heappage
            .delete_tuple(record_id.slot_id)
            .map_err(TableError::HeapPage)?;

        Ok(())
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
        let page_ref = self.table.cache.get_page(self.page_id).ok()?;
        let heappage = page_ref.heap_page();
        match heappage.get_tuple(self.slot_id) {
            Ok(tuple) => {
                self.slot_id = HeapPageSlotId::new(self.slot_id.get() + 1);
                Some(tuple.to_owned(&self.table.schema))
            }
            Err(HeapPageError::SlotDeleted) => {
                self.slot_id = HeapPageSlotId::new(self.slot_id.get() + 1);
                self.next()
            }
            Err(HeapPageError::SlotNotFound) => {
                // println!("deleted");
                self.page_id = PageId::new(self.page_id.get() + 1);
                self.next()
            }
            Err(_) => unreachable!(),
        }
    }
}
