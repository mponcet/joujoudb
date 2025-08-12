use crate::pages::{PAGE_SIZE, Page, PageId};

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("io error")]
    Io(#[from] std::io::Error),
}

pub struct Storage {
    file: File,
}

impl Storage {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .custom_flags(libc::O_DIRECT)
            .open(path)
            .map_err(StorageError::Io)?;

        Ok(Self { file })
    }

    pub fn read_page(&mut self, page_id: PageId, page: &mut Page) -> Result<(), StorageError> {
        let offset = page_id as u64 * PAGE_SIZE as u64;

        self.file
            .read_exact_at(page.data.as_mut_slice(), offset)
            .map_err(StorageError::Io)?;

        Ok(())
    }

    pub fn write_page(&mut self, page: &Page, page_id: PageId) -> Result<(), StorageError> {
        let offset = page_id as u64 * PAGE_SIZE as u64;

        self.file
            .write_all_at(page.data.as_slice(), offset)
            .map_err(StorageError::Io)?;

        Ok(())
    }

    pub fn flush(&mut self) {
        let result = self.file.flush();
        if result.is_err() {
            // if fsync fails, we can't make sure data is flushed to disk
            // ref: https://wiki.postgresql.org/wiki/Fsync_Errors
            panic!("flush (fsync) failed");
        }
    }

    // this information will be later stored in a metadata page
    // at the beginning of the file.
    // page 0 is used as in invalid page id, start at page id 1
    pub fn last_page_id(&mut self) -> PageId {
        (self.file.metadata().unwrap().len() / PAGE_SIZE as u64) as u32 + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pages::HeapPage;
    use crate::tuple::Tuple;

    use std::path::PathBuf;

    fn test_path() -> PathBuf {
        PathBuf::from("/tmp/test_data")
    }

    #[test]
    fn storage_read_after_write_page() {
        let mut storage = Storage::open(test_path()).unwrap();
        let page = &mut Page::new();

        // write
        let values = vec![0, 1, 2, 3].into_boxed_slice();
        let tuple_w = Tuple::try_new(values).unwrap();
        let heappage: &mut HeapPage = page.into();
        heappage.insert_tuple(&tuple_w).unwrap();
        storage.write_page(page, 0).unwrap();
        storage.flush();

        // read back
        let page = &mut Page::new();
        storage.read_page(0, page).unwrap();
        // assert_eq!(page.page_id(), 0);
        let heappage: &mut HeapPage = page.into();
        let tuple_r = heappage.get_tuple(0).unwrap();

        assert_eq!(tuple_w.values(), tuple_r.values());
    }
}
