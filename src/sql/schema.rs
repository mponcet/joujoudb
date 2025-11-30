use std::collections::HashSet;

use thiserror::Error;

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum DataType {
    Boolean,
    Integer,
    VarChar,
}

impl From<DataType> for String {
    fn from(data_type: DataType) -> Self {
        match data_type {
            DataType::Boolean => "BOOLEAN".to_string(),
            DataType::Integer => "INTEGER".to_string(),
            DataType::VarChar => "VARCHAR".to_string(),
        }
    }
}

#[derive(Copy, Clone, Default)]
#[repr(C)]
pub struct Constraints(u8);

impl Constraints {
    pub fn new(nullable: bool, unique: bool) -> Self {
        let mut constraints = Self::default();

        if nullable {
            constraints.set_nullable();
        }

        if unique {
            constraints.set_unique();
        }

        constraints
    }

    fn set_nullable(&mut self) {
        self.0 |= 0b1
    }

    fn set_unique(&mut self) {
        self.0 |= 0b10
    }

    pub fn is_nullable(&self) -> bool {
        self.0 & 0b1 == 0b1
    }

    pub fn is_unique(&self) -> bool {
        self.0 & 0b10 == 0b10
    }
}

#[derive(Clone)]
pub struct Column {
    pub column_name: String,
    pub data_type: DataType,
    pub constraints: Constraints,
}

impl Column {
    pub fn new(column_name: String, data_type: DataType, constraints: Constraints) -> Self {
        Self {
            column_name,
            data_type,
            constraints,
        }
    }
}

#[derive(Clone)]
pub struct Schema {
    columns: Vec<Column>,
}

impl Schema {
    pub fn try_new(columns: Vec<Column>) -> Result<Self, SchemaError> {
        let mut uniq = HashSet::new();
        if columns.iter().all(|c| uniq.insert(c.column_name.as_str())) {
            Ok(Self { columns })
        } else {
            Err(SchemaError::UniqueName)
        }
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn columns(&self) -> &[Column] {
        self.columns.as_slice()
    }
}

#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("maximum number of columns reached")]
    TooManyColumns,
    #[error("columns must have unique names")]
    UniqueName,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> Schema {
        let columns = vec![
            Column {
                column_name: "a".into(),
                data_type: DataType::Integer,
                constraints: Constraints::new(true, false),
            },
            Column {
                column_name: "b".into(),
                data_type: DataType::VarChar,
                constraints: Constraints::new(false, false),
            },
        ];

        Schema::try_new(columns).unwrap()
    }
}
