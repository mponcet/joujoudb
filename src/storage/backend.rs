use crate::pages::{PAGE_SIZE, Page, PageId};

use std::fs::{File, OpenOptions};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};

use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StorageId(pub u32);

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("file corrupted")]
    FileCorrupted,
}

pub trait StorageBackend: Sync + Send {
    fn read_page(&self, page_id: PageId, page: &mut Page) -> Result<(), StorageError>;
    fn write_page(&self, page: &Page, page_id: PageId) -> Result<(), StorageError>;
    fn fsync(&self);
    fn allocate_page(&self) -> Result<PageId, StorageError>;
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
    last_page_id: AtomicU32,
}

impl FileStorage {
    /// Creates a new storage file.
    ///
    /// Returns a `Result` containing the `Storage` instance if successful, or a `StorageError` on failure.
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)
            .map_err(StorageError::Io)?;

        let file = Self {
            file,
            last_page_id: AtomicU32::new(0),
        };

        if file.file.metadata()?.len() == 0 {
            // Create reserved page
            let reserved_page = Page::new();
            let reserved_page_id = PageId::new(0);
            file.write_page(&reserved_page, reserved_page_id)?;
            file.fsync();
        }

        Ok(file)
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

        let len = file.metadata()?.len() as usize;
        if len == 0 || !len.is_multiple_of(PAGE_SIZE) {
            return Err(StorageError::FileCorrupted);
        }

        let last_page_id = (len / PAGE_SIZE) as u32 - 1;
        let file = Self {
            file,
            last_page_id: AtomicU32::new(last_page_id),
        };

        Ok(file)
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
            .map_err(StorageError::Io)
    }

    /// Writes a page to the database file.
    ///
    /// Returns an empty `Result` if successful, or a `StorageError` on failure.
    fn write_page(&self, page: &Page, page_id: PageId) -> Result<(), StorageError> {
        let offset = page_id.get() as u64 * PAGE_SIZE as u64;

        self.file
            .write_all_at(page.data.as_slice(), offset)
            .map_err(StorageError::Io)
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

    /// Allocates a new page and returns its id.
    fn allocate_page(&self) -> Result<PageId, StorageError> {
        let last_page_id = self.last_page_id.fetch_add(1, Ordering::Relaxed) + 1;
        let new_page_id = PageId::new(last_page_id);
        let new_page = Page::new();
        // TODO: could use posix_fallocate.
        self.write_page(&new_page, new_page_id)?;
        Ok(new_page_id)
    }

    fn first_page_id(&self) -> PageId {
        PageId::new(0)
    }

    /// Retreives the last allocated page id.
    ///
    /// TODO: implement a free space map for more efficent storage.
    fn last_page_id(&self) -> PageId {
        PageId::new(self.last_page_id.load(Ordering::Relaxed))
    }
}
