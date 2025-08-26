use thiserror::Error;
use zerocopy_derive::*;

#[derive(Copy, Clone)]
pub enum ColumnType {
    Char(usize),
    VarChar,
    BigInt,
}

#[derive(Copy, Clone, Default, TryFromBytes, IntoBytes, KnownLayout, Immutable)]
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

pub struct Column {
    pub column_type: ColumnType,
    pub constraints: Constraints,
}

impl Column {
    pub fn new(column_type: ColumnType, constraints: Constraints) -> Self {
        Self {
            column_type,
            constraints,
        }
    }
}

pub struct Schema {
    columns: Vec<Column>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
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
}

impl From<Vec<Column>> for Schema {
    fn from(columns: Vec<Column>) -> Self {
        Self { columns }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> Schema {
        let columns = vec![
            Column {
                column_type: ColumnType::BigInt,
                constraints: Constraints::new(true, false),
            },
            Column {
                column_type: ColumnType::Char(32),
                constraints: Constraints::new(false, false),
            },
        ];

        Schema::from(columns)
    }
}
