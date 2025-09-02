use std::collections::HashMap;
use std::fs;
use std::io::{Error, ErrorKind, Result};
use std::path::{Path, PathBuf};

use regex::Regex;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DatabaseName(String);

impl TryFrom<&str> for DatabaseName {
    type Error = &'static str;

    fn try_from(name: &str) -> std::result::Result<Self, Self::Error> {
        let regex = Regex::new(r"^[\p{L}\p{N}_]{1,64}$").unwrap();
        if regex.is_match(name) {
            Ok(Self(name.to_string()))
        } else {
            Err("DatabaseName contains invalid characters")
        }
    }
}

impl DatabaseName {
    fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TableName(String);

impl TryFrom<&str> for TableName {
    type Error = &'static str;

    fn try_from(name: &str) -> std::result::Result<Self, Self::Error> {
        let regex = Regex::new(r"^[\p{L}\p{N}_]{1,64}$").unwrap();
        if regex.is_match(name) {
            Ok(Self(name.to_string()))
        } else {
            Err("TableName contains invalid characters")
        }
    }
}

impl TableName {
    fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug)]
pub struct TableFile {
    name: TableName,
    path: PathBuf,
    // primary_index: TableName
    // primary_index_path: PathBuf
}

impl TableFile {
    fn new(db: &DatabaseDirectory, table_name: &TableName) -> Result<Self> {
        let path = db
            .path
            .as_path()
            .join(format!("{}.tbl", table_name.as_str()));
        fs::File::create_new(path.as_path())?;
        Self::from_path(path)
    }

    fn from_path<P: AsRef<Path>>(table_path: P) -> Result<Self> {
        let table_path = table_path.as_ref();

        if table_path.is_file() {
            let name = TableName::try_from(
                table_path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .strip_suffix(".tbl")
                    .unwrap(),
            )
            .map_err(|_| Error::from(ErrorKind::InvalidFilename))?;

            Ok(Self {
                name,
                path: table_path.to_path_buf(),
            })
        } else {
            Err(Error::from(ErrorKind::NotFound))
        }
    }

    pub fn path(&self) -> &Path {
        self.path.as_path()
    }
}

#[derive(Debug)]
pub struct DatabaseDirectory {
    name: DatabaseName,
    path: PathBuf,
    tables: HashMap<TableName, TableFile>,
}

impl DatabaseDirectory {
    fn new(root: &DatabaseRootDirectory, db_name: &DatabaseName) -> Result<Self> {
        let db_dir = root.root_dir.as_path().join(db_name.as_str());
        fs::create_dir(db_dir.as_path())?;
        Self::from_path(db_dir)
    }

    fn from_path<P: AsRef<Path>>(db_dir: P) -> Result<Self> {
        let db_dir = db_dir.as_ref();

        if db_dir.is_dir() {
            let mut tables = HashMap::new();
            for entry in fs::read_dir(db_dir)? {
                if let Ok(entry) = entry
                    && entry.path().is_file()
                    && let Ok(table) = TableFile::from_path(entry.path())
                {
                    tables.insert(table.name.clone(), table);
                }
            }

            let name = DatabaseName::try_from(db_dir.file_name().unwrap().to_str().unwrap())
                .map_err(|_| Error::from(ErrorKind::InvalidFilename))?;

            Ok(Self {
                name,
                path: db_dir.to_path_buf(),
                tables,
            })
        } else {
            Err(Error::from(ErrorKind::NotADirectory))
        }
    }

    fn create_table(&mut self, table_name: &TableName) -> Result<&TableFile> {
        if !self.tables.contains_key(table_name) {
            let table = TableFile::new(self, table_name)?;
            Ok(self.tables.entry(table_name.clone()).or_insert(table))
        } else {
            Err(Error::from(ErrorKind::AlreadyExists))
        }
    }

    fn drop_table(&mut self, table_name: &TableName) -> Result<()> {
        if self.tables.remove(table_name).is_some() {
            let path = self
                .path
                .as_path()
                .join(format!("{}.tbl", table_name.as_str()));
            fs::remove_file(path)
        } else {
            Err(Error::from(ErrorKind::NotFound))
        }
    }
}

#[derive(Debug)]
pub struct DatabaseRootDirectory {
    root_dir: PathBuf,
    databases: HashMap<DatabaseName, DatabaseDirectory>,
}

impl DatabaseRootDirectory {
    pub fn from_path<P: AsRef<Path>>(root_dir: P) -> Result<Self> {
        let root_dir = root_dir.as_ref();
        if root_dir.is_dir() {
            let mut databases = HashMap::new();
            for entry in fs::read_dir(root_dir)? {
                if let Ok(entry) = entry
                    && entry.path().is_dir()
                    && let Ok(db) = DatabaseDirectory::from_path(entry.path())
                {
                    databases.insert(db.name.clone(), db);
                }
            }
            Ok(Self {
                root_dir: root_dir.to_path_buf(),
                databases,
            })
        } else {
            Err(Error::from(ErrorKind::NotADirectory))
        }
    }

    pub fn get_database_mut(&mut self, db_name: &DatabaseName) -> Result<&mut DatabaseDirectory> {
        self.databases
            .get_mut(db_name)
            .ok_or(Error::from(ErrorKind::NotFound))
    }

    pub fn create_database(&mut self, db_name: &DatabaseName) -> Result<()> {
        if !self.databases.contains_key(db_name) {
            let db = DatabaseDirectory::new(self, db_name)?;
            self.databases.insert(db_name.clone(), db);
            Ok(())
        } else {
            Err(Error::from(ErrorKind::InvalidFilename))
        }
    }

    pub fn drop_database(&mut self, db_name: &DatabaseName) -> Result<()> {
        if self.databases.remove(db_name).is_some() {
            let _db_dir = self.root_dir.join(db_name.as_str());
            // TODO: add a marker file in the root directory
            // std::fs::remove_dir_all(_db_dir)
            Ok(())
        } else {
            Err(Error::from(ErrorKind::NotFound))
        }
    }

    pub fn create_table(
        &mut self,
        db_name: &DatabaseName,
        table_name: &TableName,
    ) -> Result<&TableFile> {
        let db = self
            .databases
            .get_mut(db_name)
            .ok_or(Error::from(ErrorKind::NotFound))?;
        let table = db.create_table(table_name)?;

        Ok(table)
    }

    pub fn drop_table(&mut self, db_name: &DatabaseName, table_name: &TableName) -> Result<()> {
        let db = self
            .databases
            .get_mut(db_name)
            .ok_or(Error::from(ErrorKind::NotFound))?;
        db.drop_table(table_name)?;

        Ok(())
    }

    pub fn table_path(&self, db_name: &DatabaseName, table_name: &TableName) -> Option<&Path> {
        let db = self.databases.get(db_name)?;
        let table = db.tables.get(table_name)?;
        Some(table.path())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::TempDir;

    #[test]
    fn create_open_delete_database() {
        let dir = TempDir::new().unwrap();
        let mut dbs = DatabaseRootDirectory::from_path(dir.path()).unwrap();
        let db_name = DatabaseName::try_from("my_db").unwrap();
        let table_name = TableName::try_from("my_table").unwrap();
        dbs.create_database(&db_name).unwrap();
        dbs.create_table(&db_name, &table_name).unwrap();
        dbs.drop_table(&db_name, &table_name).unwrap();
    }
}
