use crate::cache::{PageCacheError, StoragePageCache};
use crate::pages::{HeapPageError, RecordId};
use crate::sql::schema::Schema;
use crate::storage::StorageBackend;
use crate::tuple::Tuple;

use thiserror::Error;

pub struct Table<'pagecache, S: StorageBackend> {
    pub name: String,
    pub schema: Schema,
    file_cache: StoragePageCache<'pagecache, S>,
}

#[derive(Debug, Error)]
pub enum TableError {
    #[error("heappage error")]
    HeapPage(#[from] HeapPageError),
    #[error("page cache error")]
    PageCache(#[from] PageCacheError),
}

impl<'pagecache, S: StorageBackend> Table<'pagecache, S> {
    pub fn try_new(
        name: &str,
        schema: Schema,
        file_cache: StoragePageCache<'pagecache, S>,
    ) -> Result<Self, TableError> {
        Ok(Self {
            name: name.to_string(),
            schema,
            file_cache,
        })
    }

    pub fn get_tuple(&self, record_id: RecordId) -> Result<Tuple, TableError> {
        let page_ref = self
            .file_cache
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
            .file_cache
            .get_page_mut(self.file_cache.last_page_id())
            .map_err(TableError::PageCache)?;
        let heappage = page_ref.heap_page_mut();

        heappage.insert_tuple(tuple).map_err(TableError::HeapPage)?;

        Ok(())
    }

    pub fn delete_tuple(&self, record_id: RecordId) -> Result<(), TableError> {
        let mut page_ref = self
            .file_cache
            .get_page_mut(self.file_cache.last_page_id())
            .map_err(TableError::PageCache)?;
        let heappage = page_ref.heap_page_mut();

        heappage
            .delete_tuple(record_id.slot_id)
            .map_err(TableError::HeapPage)?;

        Ok(())
    }
}
