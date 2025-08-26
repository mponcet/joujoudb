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

/// Manages the on-disk storage of table pages.
///
/// The `Storage` struct is responsible for reading from and writing to the database file.
/// It uses direct I/O to bypass the operating system's buffer cache, ensuring that data
/// is written directly to the disk.
pub struct Storage {
    file: File,
}

impl Storage {
    /// Creates a new storage file.
    ///
    /// Returns a `Result` containing the `Storage` instance if successful, or a `StorageError` on failure.
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)
            .map_err(StorageError::Io)?;

        if file.metadata().unwrap().len() == 0 {
            // Create reserved page
            file.write_all(&[0; PAGE_SIZE]).unwrap();
        }

        Ok(Self { file })
    }

    /// Opens a new storage file.
    ///
    /// Returns a `Result` containing the `Storage` instance if successful, or a `StorageError` on failure.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .truncate(false)
            .custom_flags(libc::O_DIRECT)
            .open(path)
            .map_err(StorageError::Io)?;

        Ok(Self { file })
    }

    /// Reads a page from the database file.
    ///
    /// Returns an empty `Result` if successful, or a `StorageError` on failure.
    pub fn read_page(&mut self, page_id: PageId, page: &mut Page) -> Result<(), StorageError> {
        let offset = page_id.get() as u64 * PAGE_SIZE as u64;

        self.file
            .read_exact_at(page.data.as_mut_slice(), offset)
            .map_err(StorageError::Io)?;

        Ok(())
    }

    /// Writes a page to the database file.
    ///
    /// Returns an empty `Result` if successful, or a `StorageError` on failure.
    pub fn write_page(&mut self, page: &Page, page_id: PageId) -> Result<(), StorageError> {
        let offset = page_id.get() as u64 * PAGE_SIZE as u64;

        self.file
            .write_all_at(page.data.as_slice(), offset)
            .map_err(StorageError::Io)?;

        Ok(())
    }

    /// Flushes any buffered data to the disk.
    ///
    /// This function ensures that all data is written to the underlying storage device.
    ///
    /// # Panics
    ///
    /// Panics if the underlying `fsync` operation fails.
    pub fn flush(&mut self) {
        let result = self.file.flush();
        if result.is_err() {
            // if fsync fails, we can't make sure data is flushed to disk
            // ref: https://wiki.postgresql.org/wiki/Fsync_Errors
            panic!("flush (fsync) failed");
        }
    }

    /// Allocates a new page and returns the ID of the last page in the database file.
    pub fn allocate_page(&mut self) -> PageId {
        let offset = self.file.metadata().unwrap().len();
        self.file.write_all_at(&[0; PAGE_SIZE], offset).unwrap();
        PageId::new((offset / PAGE_SIZE as u64) as u32)
    }

    /// Retreives the last allocated page id.
    ///
    /// TODO: implement a free space map for more efficent storage.
    pub fn last_page_id(&self) -> PageId {
        let offset = self.file.metadata().unwrap().len();
        PageId::new(((offset / PAGE_SIZE as u64) - 1) as u32)
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     use crate::pages::HeapPage;
//     use crate::tuple::Tuple;
//
//     use tempfile::NamedTempFile;
//
//     #[test]
//     fn storage_read_after_write_page() {
//         let storage_path = NamedTempFile::new().unwrap();
//         let mut storage = Storage::open(storage_path).unwrap();
//         let page = &mut Page::new();
//
//         // write
//         let values = vec![0, 1, 2, 3].into_boxed_slice();
//         let tuple_w = Tuple::try_new(values).unwrap();
//         let heappage: &mut HeapPage = page.into();
//         heappage.insert_tuple(&tuple_w).unwrap();
//         storage.write_page(page, 0).unwrap();
//         storage.flush();
//
//         // read back
//         let page = &mut Page::new();
//         storage.read_page(0, page).unwrap();
//         // assert_eq!(page.page_id(), 0);
//         let heappage: &mut HeapPage = page.into();
//         let tuple_r = heappage.get_tuple(0).unwrap();
//
//         assert_eq!(tuple_w.values(), tuple_r.values());
//     }
// }
