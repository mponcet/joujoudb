use crate::cache::GLOBAL_PAGE_CACHE;
use crate::config::CONFIG;
use crate::sql::schema::{Column, ColumnType, Constraints, Schema};
use crate::sql::types::{BigInt, Char, Value, VarChar};
use crate::storage::{DatabaseName, DatabaseRootDirectory, FileStorage, StorageBackend, TableName};
use crate::table::Table;
use crate::tuple::Tuple;

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
    DatabaseExists,
    #[error("Table already exists")]
    TableExists,
}

static INFORMATION_SCHEMA_TABLES: LazyLock<Schema> = LazyLock::new(|| {
    Schema::try_new(vec![
        // TABLE_SCHEMA: the name of the database to which the table belongs to.
        Column {
            column_name: "TABLE_SCHEMA".into(),
            column_type: ColumnType::VarChar,
            constraints: Constraints::new(false, false),
        },
        // TABLE_TYPE: table or index.
        Column {
            column_name: "TABLE_TYPE".into(),
            column_type: ColumnType::Char(5),
            constraints: Constraints::new(false, false),
        },
        // TABLE_NAME: the name of the table.
        Column {
            column_name: "TABLE_NAME".into(),
            column_type: ColumnType::VarChar,
            constraints: Constraints::new(false, true),
        },
        // TABLE_ROWS: the number of rows.
        Column {
            column_name: "TABLE_ROWS".into(),
            column_type: ColumnType::BigInt,
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
            column_type: ColumnType::VarChar,
            constraints: Constraints::new(false, false),
        },
        // TABLE_NAME: the name of the table.
        Column {
            column_name: "TABLE_NAME".into(),
            column_type: ColumnType::VarChar,
            constraints: Constraints::new(false, false),
        },
        // COLUMN_NAME: the name of the column.
        Column {
            column_name: "COLUMN_NAME".into(),
            column_type: ColumnType::VarChar,
            constraints: Constraints::new(false, true),
        },
        // ORDINAL_POSITION: the position of the column within the table.
        Column {
            column_name: "ORDINAL_POSITION".into(),
            column_type: ColumnType::BigInt,
            constraints: Constraints::new(false, false),
        },
        // COLUMN_DEFAULT: the default value of the column.
        // Column {
        //     column_name: "COLUMN_DEFAULT".into(),
        //     column_type: ColumnType::VarChar,
        //     constraints: Constraints::new(false, false),
        // },
        // IS_NULLABLE: the column nullability.
        Column {
            column_name: "IS_NULLABLE".into(),
            column_type: ColumnType::Char(3),
            constraints: Constraints::new(false, false),
        },
        // DATA_TYPE: the data type.
        Column {
            column_name: "DATA_TYPE".into(),
            column_type: ColumnType::VarChar,
            constraints: Constraints::new(false, false),
        },
    ])
    .unwrap()
});

impl Catalog<FileStorage> {
    const INFORMATION_SCHEMA_DB: &str = "INFORMATION_SCHEMA";
    const INFORMATION_SCHEMA_TABLES_TABLE: &str = "TABLES";
    const INFORMATION_SCHEMA_COLUMMS_TABLE: &str = "COLUMNS";

    pub fn new() -> Self {
        let mut db_root =
            DatabaseRootDirectory::from_path(CONFIG.ROOT_DIRECTORY.as_str()).expect("TODO");
        let db = DatabaseName::try_from(Self::INFORMATION_SCHEMA_DB).unwrap();
        let tables = TableName::try_from(Self::INFORMATION_SCHEMA_TABLES_TABLE).unwrap();
        let columns = TableName::try_from(Self::INFORMATION_SCHEMA_COLUMMS_TABLE).unwrap();

        // TODO: database exists ?
        if !db_root.get_database_mut(&db).is_ok() {
            db_root
                .create_database(&db)
                .expect("Could not create database");
            db_root
                .create_table(&db, &tables)
                .expect("Could not create table");
            db_root
                .create_table(&db, &columns)
                .expect("Could not create table");
        }

        let tables_path = db_root.table_path(&db, &tables).unwrap();
        let tables_table = Table::try_new(
            "TABLES",
            &INFORMATION_SCHEMA_TABLES,
            GLOBAL_PAGE_CACHE.cache_storage(FileStorage::open(tables_path).expect("TODO")),
        )
        .expect("TODO");

        let columns_path = db_root.table_path(&db, &columns).unwrap();
        let columns_table = Table::try_new(
            "COLUMNS",
            &INFORMATION_SCHEMA_COLUMNS,
            GLOBAL_PAGE_CACHE.cache_storage(FileStorage::open(columns_path).expect("TODO")),
        )
        .expect("TODO");

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
            .map_err(|_| CatalogError::DatabaseExists)
    }

    fn create_table(
        &mut self,
        db_name: &DatabaseName,
        table_name: &TableName,
        schema: &Schema,
    ) -> Result<(), CatalogError> {
        // add table_name to table list
        self.db_root
            .create_table(db_name, table_name)
            .map_err(|_| CatalogError::TableExists)?;

        let tuple = Tuple::try_new(vec![
            Value::VarChar(VarChar::new(db_name.as_str().to_string())),
            Value::Char(Char::new("table".to_string(), Some(5))),
            Value::VarChar(VarChar::new(table_name.as_str().to_string())),
            Value::BigInt(BigInt::new(schema.num_columns() as i64)),
        ])
        .expect("TODO");
        self.information_schema_tables
            .insert_tuple(&tuple)
            .expect("TODO");

        for (ordinal_position, column) in schema.columns().iter().enumerate() {
            println!("insert column {ordinal_position}");
            let is_nullable = if column.constraints.is_nullable() {
                "YES"
            } else {
                "NO"
            };
            let tuple = Tuple::try_new(vec![
                Value::VarChar(VarChar::new(db_name.as_str().to_string())),
                Value::VarChar(VarChar::new(table_name.as_str().to_string())),
                Value::VarChar(VarChar::new(column.column_name.clone())),
                Value::BigInt(BigInt::new(ordinal_position as i64)),
                Value::Char(Char::new(is_nullable.to_string(), Some(3))),
                Value::VarChar(VarChar::new(column.column_type.into())),
            ])
            .expect("TODO");
            self.information_schema_columns
                .insert_tuple(&tuple)
                .expect("TODO");
            // TODO: add schema columns to columns table
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_scan_catalog() {
        let mut catalog = Catalog::new();
        let db_name = DatabaseName::try_from("test_db").unwrap();
        let _ = catalog.create_database(&db_name);

        let table_name = TableName::try_from("test_tbl").unwrap();
        let schema = Schema::try_new(vec![
            Column::new(
                "id".into(),
                ColumnType::BigInt,
                Constraints::new(false, false),
            ),
            Column::new(
                "name".into(),
                ColumnType::VarChar,
                Constraints::new(false, false),
            ),
        ])
        .unwrap();
        let _ = catalog.create_table(&db_name, &table_name, &schema);

        println!("tuples:");
        for tuple in catalog.information_schema_tables.iter() {
            println!("tuple={:?}", tuple);
        }
        for tuple in catalog.information_schema_columns.iter() {
            println!("tuple={:?}", tuple);
        }
    }
}
