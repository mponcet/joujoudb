use crate::cache::GLOBAL_PAGE_CACHE;
use crate::config::CONFIG;
use crate::sql::schema::{Column, Constraints, DataType, Schema};
use crate::sql::types::Value;
use crate::storage::{DatabaseName, DatabaseRootDirectory, FileStorage, StorageBackend, TableName};
use crate::table::Table;
use crate::tuple::Tuple;

use std::path::Path;
use std::sync::LazyLock;

use thiserror::Error;

struct Catalog<S: StorageBackend + 'static> {
    db_root: DatabaseRootDirectory,
    information_schema_tables: Table<S>,
    information_schema_columns: Table<S>,
}

#[derive(Debug, Error)]
enum CatalogError {
    #[error("Database already exists")]
    CreateDatabase,
    #[error("table creation failed")]
    CreateTable,
}

static INFORMATION_SCHEMA_TABLES: LazyLock<Schema> = LazyLock::new(|| {
    Schema::try_new(vec![
        // TABLE_SCHEMA: the name of the database to which the table belongs to.
        Column {
            column_name: "TABLE_SCHEMA".into(),
            data_type: DataType::VarChar,
            constraints: Constraints::new(false, false),
        },
        // TABLE_TYPE: table or index.
        Column {
            column_name: "TABLE_TYPE".into(),
            data_type: DataType::VarChar,
            constraints: Constraints::new(false, false),
        },
        // TABLE_NAME: the name of the table.
        Column {
            column_name: "TABLE_NAME".into(),
            data_type: DataType::VarChar,
            constraints: Constraints::new(false, true),
        },
        // TABLE_ROWS: the number of rows.
        Column {
            column_name: "TABLE_ROWS".into(),
            data_type: DataType::Integer,
            constraints: Constraints::new(false, false),
        },
    ])
    .unwrap()
});

static INFORMATION_SCHEMA_COLUMNS: LazyLock<Schema> = LazyLock::new(|| {
    Schema::try_new(vec![
        // TABLE_SCHEMA: the name of the database to which the column belongs.
        Column {
            column_name: "TABLE_SCHEMA".into(),
            data_type: DataType::VarChar,
            constraints: Constraints::new(false, false),
        },
        // TABLE_NAME: the name of the table.
        Column {
            column_name: "TABLE_NAME".into(),
            data_type: DataType::VarChar,
            constraints: Constraints::new(false, false),
        },
        // COLUMN_NAME: the name of the column.
        Column {
            column_name: "COLUMN_NAME".into(),
            data_type: DataType::VarChar,
            constraints: Constraints::new(false, true),
        },
        // ORDINAL_POSITION: the position of the column within the table.
        Column {
            column_name: "ORDINAL_POSITION".into(),
            data_type: DataType::Integer,
            constraints: Constraints::new(false, false),
        },
        // COLUMN_DEFAULT: the default value of the column.
        // Column {
        //     column_name: "COLUMN_DEFAULT".into(),
        //     data_type: DataType::VarChar,
        //     constraints: Constraints::new(false, false),
        // },
        // IS_NULLABLE: the column nullability.
        Column {
            column_name: "IS_NULLABLE".into(),
            data_type: DataType::VarChar,
            constraints: Constraints::new(false, false),
        },
        // DATA_TYPE: the data type.
        Column {
            column_name: "DATA_TYPE".into(),
            data_type: DataType::VarChar,
            constraints: Constraints::new(false, false),
        },
    ])
    .unwrap()
});

impl Catalog<FileStorage> {
    const INFORMATION_SCHEMA_DB: &str = "INFORMATION_SCHEMA";
    const INFORMATION_SCHEMA_TABLES_TABLE: &str = "TABLES";
    const INFORMATION_SCHEMA_COLUMNS_TABLE: &str = "COLUMNS";

    pub fn new() -> Self {
        Self::with_root_path(CONFIG.ROOT_DIRECTORY.as_str())
    }

    pub fn with_root_path<P: AsRef<Path>>(path: P) -> Self {
        let path = path.as_ref();
        let mut db_root = DatabaseRootDirectory::from_path(path)
            .unwrap_or_else(|e| panic!("{} (path: {})", e, path.display()));
        let db = DatabaseName::try_from(Self::INFORMATION_SCHEMA_DB).unwrap();
        let tables = TableName::try_from(Self::INFORMATION_SCHEMA_TABLES_TABLE).unwrap();
        let columns = TableName::try_from(Self::INFORMATION_SCHEMA_COLUMNS_TABLE).unwrap();

        if db_root.get_database_mut(&db).is_err() {
            db_root.create_database(&db).unwrap();
            db_root.create_table(&db, &tables).unwrap();
            db_root.create_table(&db, &columns).unwrap();
        }

        let tables_path = db_root.table_path(&db, &tables).unwrap();
        let tables_storage = FileStorage::open(tables_path).unwrap();
        let tables_table = Table::try_new(
            Self::INFORMATION_SCHEMA_TABLES_TABLE,
            &INFORMATION_SCHEMA_TABLES,
            GLOBAL_PAGE_CACHE.cache_storage(tables_storage),
        )
        .unwrap_or_else(|e| {
            panic!(
                "Failed to open table {}: {}",
                Self::INFORMATION_SCHEMA_TABLES_TABLE,
                e
            )
        });

        let columns_path = db_root.table_path(&db, &columns).unwrap();
        let columns_storage = FileStorage::open(columns_path).unwrap();
        let columns_table = Table::try_new(
            Self::INFORMATION_SCHEMA_COLUMNS_TABLE,
            &INFORMATION_SCHEMA_COLUMNS,
            GLOBAL_PAGE_CACHE.cache_storage(columns_storage),
        )
        .unwrap_or_else(|e| {
            panic!(
                "Failed to open table {}: {}",
                Self::INFORMATION_SCHEMA_COLUMNS_TABLE,
                e
            )
        });

        Self {
            db_root,
            information_schema_tables: tables_table,
            information_schema_columns: columns_table,
        }
    }
}

impl<S: StorageBackend + 'static> Catalog<S> {
    fn create_database(&mut self, db_name: &DatabaseName) -> Result<(), CatalogError> {
        self.db_root
            .create_database(db_name)
            .map_err(|_| CatalogError::CreateDatabase)
    }

    fn create_table(
        &mut self,
        db_name: &DatabaseName,
        table_name: &TableName,
        schema: &Schema,
    ) -> Result<(), CatalogError> {
        self.db_root
            .create_table(db_name, table_name)
            .map_err(|_| CatalogError::CreateTable)?;

        let tuple = Tuple::try_new(vec![
            Value::VarChar(db_name.as_str().to_string()),
            Value::VarChar("table".to_string()),
            Value::VarChar(table_name.as_str().to_string()),
            Value::Integer(schema.num_columns() as i64),
        ])
        .map_err(|_| CatalogError::CreateTable)?;

        self.information_schema_tables
            .insert(&tuple)
            .map_err(|_| CatalogError::CreateTable)?;

        for (ordinal_position, column) in schema.columns().iter().enumerate() {
            let is_nullable = if column.constraints.is_nullable() {
                "YES"
            } else {
                "NO"
            };
            let tuple = Tuple::try_new(vec![
                Value::VarChar(db_name.as_str().to_string()),
                Value::VarChar(table_name.as_str().to_string()),
                Value::VarChar(column.column_name.clone()),
                Value::Integer(ordinal_position as i64),
                Value::VarChar(is_nullable.to_string()),
                Value::VarChar(format!("{}", column.data_type)),
            ])
            .map_err(|_| CatalogError::CreateTable)?;

            self.information_schema_columns
                .insert(&tuple)
                .map_err(|_| CatalogError::CreateTable)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_scan_catalog() {
        let root_path = tempfile::TempDir::new()
            .unwrap()
            .keep()
            .to_string_lossy()
            .into_owned();
        let mut catalog = Catalog::with_root_path(&root_path);
        let db_name = DatabaseName::try_from("test_db").unwrap();
        let _ = catalog.create_database(&db_name);

        let table_name = TableName::try_from("test_tbl").unwrap();
        let schema = Schema::try_new(vec![
            Column::new(
                "id".into(),
                DataType::Integer,
                Constraints::new(false, false),
            ),
            Column::new(
                "name".into(),
                DataType::VarChar,
                Constraints::new(false, false),
            ),
        ])
        .unwrap();

        catalog
            .create_table(&db_name, &table_name, &schema)
            .unwrap();

        assert_eq!(catalog.information_schema_tables.iter().count(), 1);
        assert_eq!(catalog.information_schema_columns.iter().count(), 2);

        // test catalog persistence
        drop(catalog);
        let catalog = Catalog::with_root_path(root_path);
        assert_eq!(catalog.information_schema_tables.iter().count(), 1);
        assert_eq!(catalog.information_schema_columns.iter().count(), 2);
    }
}
