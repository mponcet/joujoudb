use crate::page::{PAGE_SIZE, Page, PageId, PageMetadata};

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;

use thiserror::Error;

#[derive(Error, Debug)]
enum StorageError {
    #[error("io error")]
    Io(#[from] std::io::Error),
}

struct Storage {
    file: File,
}

impl Storage {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(StorageError::Io)?;

        Ok(Self { file })
    }

    pub fn read_page(&mut self, page_id: PageId) -> Result<Page, StorageError> {
        let offset = (page_id * PAGE_SIZE) as u64;

        let mut data = Box::new([0u8; PAGE_SIZE]);
        self.file
            .read_exact_at(data.as_mut_slice(), offset)
            .map_err(StorageError::Io)?;

        Ok(Page {
            metadata: PageMetadata {
                page_id,
                // dirty: false,
            },
            data,
        })
    }

    pub fn write_page(&mut self, page: &Page) -> Result<(), StorageError> {
        let offset = (page.metadata.page_id * PAGE_SIZE) as u64;

        self.file
            .write_all_at(page.data.as_slice(), offset)
            .map_err(StorageError::Io)?;

        Ok(())
    }

    fn flush(&mut self) -> Result<(), StorageError> {
        self.file.flush().map_err(StorageError::Io)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heappage::HeapPage;
    use crate::tuple::Tuple;

    use std::path::PathBuf;

    fn test_path() -> PathBuf {
        PathBuf::from("/tmp/test_data")
    }

    #[test]
    fn storage_read_after_write_page() {
        let mut heapfile = Storage::open(test_path()).unwrap();
        let page = &mut Page::new(0);

        // write
        let values = vec![0, 1, 2, 3].into_boxed_slice();
        let tuple_w = Tuple::try_new(values).unwrap();
        let heappage: &mut HeapPage = page.into();
        heappage.insert_tuple(&tuple_w).unwrap();
        heapfile.write_page(page).unwrap();
        heapfile.flush().unwrap();

        // read back
        let page = &mut heapfile.read_page(0).unwrap();
        assert_eq!(page.metadata.page_id, 0);
        let heappage: &mut HeapPage = page.into();
        let tuple_r = heappage.get_tuple(0).unwrap();

        assert_eq!(tuple_w.values(), tuple_r.values());
    }
}
