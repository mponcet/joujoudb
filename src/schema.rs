use thiserror::Error;
use zerocopy_derive::*;

// Must be the same size as the null bitmap in tuples
const SCHEMA_MAX_COLUMNS: usize = 64;

#[derive(Copy, Clone, Default, TryFromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub enum DataType {
    #[default]
    Unknown,
    Char,
    Varchar,
    Integer,
    Date,
    Time,
    Timestamp,
}

#[derive(Copy, Clone, Default, TryFromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct Constraints(u8);

impl Constraints {
    fn new(nullable: bool, unique: bool) -> Self {
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

    fn is_nullable(&self) -> bool {
        self.0 & 0b1 == 0b1
    }

    fn is_unique(&self) -> bool {
        self.0 & 0b10 == 0b10
    }
}

pub struct Column {
    data_type: DataType,
    constraints: Constraints,
}

pub struct Schema {
    columns: Vec<Column>,
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
                data_type: DataType::Integer,
                constraints: Constraints::new(true, false),
            },
            Column {
                data_type: DataType::Char,
                constraints: Constraints::new(false, false),
            },
        ];

        Schema::from(columns)
    }
}
