mod backend;

use crate::config::CONFIG;

use std::{io, path::Path};

pub use backend::{Storage, StorageError};

pub fn create_database(name: &str) -> Result<(), io::Error> {
    let path = format!("{}/{}", CONFIG.ROOT_DIRECTORY, name);
    let path = Path::new(&path);

    if path.exists() {
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "database path already exists",
        ))
    } else if path.is_dir() {
        std::fs::create_dir(path)
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotADirectory,
            "database path should be a directory",
        ))
    }
}
