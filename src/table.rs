use crate::cache::{PageCache, PageCacheError};
use crate::pages::{HeapPageError, RecordId};
use crate::sql::schema::Schema;
use crate::storage::Storage;
use crate::tuple::Tuple;

use thiserror::Error;

pub struct Table {
    pub name: String,
    pub schema: Schema,
    page_cache: PageCache,
}

#[derive(Debug, Error)]
pub enum TableError {
    #[error("heappage error")]
    HeapPage(#[from] HeapPageError),
    #[error("page cache error")]
    PageCache(#[from] PageCacheError),
}

impl Table {
    pub fn try_new(name: &str, storage: Storage, schema: Schema) -> Result<Self, TableError> {
        let page_cache = PageCache::try_new(storage).map_err(TableError::PageCache)?;
        Ok(Self {
            name: name.to_string(),
            schema,
            page_cache,
        })
    }

    pub fn get_tuple(&self, record_id: RecordId) -> Result<Tuple, TableError> {
        let page_ref = self
            .page_cache
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
            .page_cache
            .get_page_mut(self.page_cache.last_page_id())
            .map_err(TableError::PageCache)?;
        let heappage = page_ref.heap_page_mut();

        heappage.insert_tuple(tuple).map_err(TableError::HeapPage)?;

        Ok(())
    }

    pub fn delete_tuple(&self, record_id: RecordId) -> Result<(), TableError> {
        let mut page_ref = self
            .page_cache
            .get_page_mut(self.page_cache.last_page_id())
            .map_err(TableError::PageCache)?;
        let heappage = page_ref.heap_page_mut();

        heappage
            .delete_tuple(record_id.slot_id)
            .map_err(TableError::HeapPage)?;

        Ok(())
    }
}
