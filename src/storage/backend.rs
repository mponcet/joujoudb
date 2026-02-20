use crate::pages::{PAGE_SIZE, Page, PageId};

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;

use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StorageId(pub u32);

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("io error")]
    Io(#[from] std::io::Error),
}

pub trait StorageBackend: Sync + Send {
    fn read_page(&self, page_id: PageId, page: &mut Page) -> Result<(), StorageError>;
    fn write_page(&self, page: &Page, page_id: PageId) -> Result<(), StorageError>;
    fn fsync(&self);
    fn allocate_page(&self) -> PageId;
    fn first_page_id(&self) -> PageId;
    fn last_page_id(&self) -> PageId;
}

/// Manages the on-disk storage of table pages.
///
/// The `Storage` struct is responsible for reading from and writing to the database file.
/// It uses direct I/O to bypass the operating system's buffer cache, ensuring that data
/// is written directly to the disk.
pub struct FileStorage {
    file: File,
}

impl FileStorage {
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

        if file.metadata()?.len() == 0 {
            // Create reserved page
            file.write_all(&[0; PAGE_SIZE])?;
            file.sync_all().expect("fsync failed");
        }

        Ok(Self { file })
    }

    /// Opens a new storage file.
    ///
    /// Returns a `Result` containing the `Storage` instance if successful, or a `StorageError` on failure.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .truncate(false)
            .custom_flags(libc::O_DIRECT)
            .open(path)
            .map_err(StorageError::Io)?;

        if file.metadata()?.len() == 0 {
            // Create reserved page
            file.write_all(&[0; PAGE_SIZE])?;
            file.sync_all().expect("fsync failed");
        }

        Ok(Self { file })
    }
}

impl StorageBackend for FileStorage {
    /// Reads a page from the database file.
    ///
    /// Returns an empty `Result` if successful, or a `StorageError` on failure.
    fn read_page(&self, page_id: PageId, page: &mut Page) -> Result<(), StorageError> {
        let offset = page_id.get() as u64 * PAGE_SIZE as u64;

        self.file
            .read_exact_at(page.data.as_mut_slice(), offset)
            .map_err(StorageError::Io)?;

        Ok(())
    }

    /// Writes a page to the database file.
    ///
    /// Returns an empty `Result` if successful, or a `StorageError` on failure.
    fn write_page(&self, page: &Page, page_id: PageId) -> Result<(), StorageError> {
        let offset = page_id.get() as u64 * PAGE_SIZE as u64;

        self.file
            .write_all_at(page.data.as_slice(), offset)
            .map_err(StorageError::Io)?;

        Ok(())
    }

    /// Attempts to sync file data and metadata to the disk.
    ///
    /// This function ensures that all data is written to the underlying storage device.
    ///
    /// # Panics
    ///
    /// Panics if the underlying `File::sync_all` operation fails.
    fn fsync(&self) {
        let result = self.file.sync_all();
        if result.is_err() {
            // if fsync fails, we can't make sure data is flushed to disk
            // ref: https://wiki.postgresql.org/wiki/Fsync_Errors
            panic!("flush (fsync) failed");
        }
    }

    /// Allocates a new page and returns the ID of the last page in the database file.
    fn allocate_page(&self) -> PageId {
        let offset = self.file.metadata().unwrap().len();
        self.file.write_all_at(&[0; PAGE_SIZE], offset).unwrap();
        PageId::new((offset / PAGE_SIZE as u64) as u32)
    }

    fn first_page_id(&self) -> PageId {
        PageId::new(0)
    }

    /// Retreives the last allocated page id.
    ///
    /// TODO: implement a free space map for more efficent storage.
    fn last_page_id(&self) -> PageId {
        let offset = self.file.metadata().unwrap().len();
        PageId::new(((offset / PAGE_SIZE as u64) - 1) as u32)
    }
}
