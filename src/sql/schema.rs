use std::collections::HashSet;

use thiserror::Error;

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    VarChar,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            DataType::Boolean => "BOOLEAN",
            DataType::Integer => "INTEGER",
            DataType::Float => "FLOAT",
            DataType::VarChar => "VARCHAR",
        };
        write!(f, "{s}")
    }
}

pub struct ConstraintsBuilder(u8);

impl ConstraintsBuilder {
    pub fn new() -> Self {
        Self(0)
    }

    pub fn nullable(mut self) -> Self {
        self.0 |= 0b1;
        self
    }

    pub fn unique(mut self) -> Self {
        self.0 |= 0b10;
        self
    }

    pub fn build(self) -> Constraints {
        Constraints(self.0)
    }
}

impl Default for ConstraintsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Copy, Clone)]
pub struct Constraints(u8);

impl Constraints {
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
                constraints: ConstraintsBuilder::new().unique().build(),
            },
            Column {
                column_name: "b".into(),
                data_type: DataType::VarChar,
                constraints: ConstraintsBuilder::new().build(),
            },
        ];

        Schema::try_new(columns).unwrap()
    }
}
