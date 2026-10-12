mod backend;
mod fs;

pub use backend::{FileStorage, StorageError, StorageId};
pub use fs::{DatabaseName, DatabaseRootDirectory, TableName};
