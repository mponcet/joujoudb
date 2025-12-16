use crate::sql::schema::Schema;
use crate::sql::types::Value;
use crate::{pages::HeapPage, serialize::Serialize};

use thiserror::Error;
use zerocopy::{
    byteorder::little_endian::{U16, U64},
    *,
};
use zerocopy_derive::*;

#[derive(Clone, Copy, Debug, Default, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub struct NullBitmap(U64);

impl NullBitmap {
    pub fn is_null(&self, column: usize) -> bool {
        (self.0.get() >> column) & 1 == 1
    }

    pub fn set_null(&mut self, column: usize) {
        self.0.set(self.0.get() | (1 << column));
    }
}

#[derive(Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub struct TupleHeader {
    len: U16,
    null_bitmap: NullBitmap,
}

impl TupleHeader {
    fn new(len: usize, null_bitmap: NullBitmap) -> Self {
        assert!(len <= u16::MAX as usize);
        Self {
            len: U16::new(len as u16),
            null_bitmap,
        }
    }
}

/// A reference to a tuple stored in a page.
///
/// `TupleRef` provides a way to access tuple data without copying it.
#[derive(FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub struct TupleRef {
    header: TupleHeader,
    values: [u8],
}

/// A newly created tuple that owns its data.
#[derive(Debug)]
pub struct Tuple {
    values: Vec<Value>,
}

impl TupleRef {
    pub fn to_owned(&self, schema: &Schema) -> Tuple {
        let mut values = Vec::with_capacity(schema.num_columns());

        let mut offset = 0;
        for (i, column) in schema.columns().iter().enumerate() {
            if self.header.null_bitmap.is_null(i) {
                values.push(Value::Null);
            } else {
                let value = Value::from_bytes(&self.values[offset..], column.data_type);
                offset += value.header_size();
                offset += value.data_size();
                values.push(value);
            }
        }

        Tuple { values }
    }
}

#[derive(Error, Debug)]
pub enum TupleError {
    #[error("tuple size cannot exceed {}", HeapPage::MAX_TUPLE_SIZE)]
    SizeExceeded,
    #[error("tuple cannot have more than {} columns", Tuple::MAX_COLUMNS)]
    TooManyColumns,
    #[error("tuple values and table schema mismatch")]
    SchemaMismatch,
}

impl Tuple {
    pub const MAX_COLUMNS: usize = 64;

    /// The size of the tuple header in bytes.
    const HEADER_SIZE: usize = std::mem::size_of::<TupleHeader>();

    /// Creates a new tuple with the given values.
    ///
    /// Returns a `Result` containing the new `Tuple`, or a `TupleError` if the tuple size exceeds the maximum allowed.
    pub fn try_new(values: Vec<Value>) -> Result<Self, TupleError> {
        if values.len() > Self::MAX_COLUMNS {
            return Err(TupleError::TooManyColumns);
        }

        let values_size = values
            .iter()
            .map(|v| v.header_size() + v.data_size())
            .sum::<usize>();

        if Self::HEADER_SIZE + values_size <= HeapPage::MAX_TUPLE_SIZE {
            Ok(Tuple { values })
        } else {
            Err(TupleError::SizeExceeded)
        }
    }

    /// Returns the total size of the tuple in bytes, including the header.
    #[inline]
    pub fn size(&self) -> usize {
        Self::HEADER_SIZE
            + self
                .values
                .iter()
                .map(|v| v.header_size() + v.data_size())
                .sum::<usize>()
    }

    /// Validates that this tuple conforms to the given schema.
    ///
    /// Returns `Ok(())` if the tuple is valid, or a `TupleError` if it is not.
    pub fn validate_with_schema(&self, schema: &Schema) -> Result<(), TupleError> {
        if self.values.len() != schema.num_columns() {
            return Err(TupleError::TooManyColumns);
        }

        let values_match_schema =
            self.values
                .iter()
                .zip(schema.columns())
                .all(|(value, column)| match value {
                    Value::Null => column.constraints.is_nullable(),
                    value => value.data_type().is_some_and(|v| v == column.data_type),
                });

        if !values_match_schema {
            Err(TupleError::SchemaMismatch)
        } else {
            Ok(())
        }
    }

    #[cfg(test)]
    fn as_bytes(&self) -> &[u8] {
        let zero = std::iter::repeat_n(0, self.size());
        let mut v = Vec::from_iter(zero);
        self.write_bytes_to(v.as_mut_slice());

        Box::leak(v.into_boxed_slice())
    }

    #[cfg(test)]
    pub fn values(&self) -> &[Value] {
        self.values.as_slice()
    }
}

impl Serialize for Tuple {
    fn write_bytes_to(&self, dst: &mut [u8]) {
        let (header_len, null_bitmap) = self.values.iter().enumerate().fold(
            (0, NullBitmap::default()),
            |(mut header_len, mut bitmap), (i, value)| {
                if value.is_null() {
                    bitmap.set_null(i)
                }
                header_len += value.header_size() + value.data_size();
                (header_len, bitmap)
            },
        );
        let header = TupleHeader::new(header_len, null_bitmap);
        let mut offset = Self::HEADER_SIZE;
        header.write_to(&mut dst[..offset]).unwrap();

        for value in self.values.iter() {
            if !value.is_null() {
                value.write_bytes_to(&mut dst[offset..]);
                offset += value.header_size() + value.data_size();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::schema::{Column, ConstraintsBuilder, DataType};
    use crate::sql::types::Value;

    use super::*;

    #[test]
    fn read_after_write() {
        let schema = Schema::try_new(vec![
            Column::new(
                "a".into(),
                DataType::Integer,
                ConstraintsBuilder::new().build(),
            ),
            Column::new(
                "b".into(),
                DataType::VarChar,
                ConstraintsBuilder::new().build(),
            ),
            Column::new(
                "c".into(),
                DataType::Boolean,
                ConstraintsBuilder::new().build(),
            ),
            Column::new(
                "d".into(),
                DataType::VarChar,
                ConstraintsBuilder::new().unique().build(),
            ),
        ])
        .unwrap();
        let values = vec![
            Value::Integer(42),
            Value::VarChar("bbbbb".to_string()),
            Value::Boolean(true),
            Value::Null,
        ];
        let values_clone = values.clone();
        let tuple = Tuple::try_new(values).unwrap();

        let bytes = tuple.as_bytes();
        let tuple = TupleRef::ref_from_bytes(bytes).unwrap();
        let tuple = tuple.to_owned(&schema);

        for (lhs, rhs) in tuple.values.iter().zip(values_clone.iter()) {
            assert_eq!(lhs, rhs);
        }
    }

    #[test]
    fn validate_tuple_ok() {
        let schema = Schema::try_new(vec![
            Column::new(
                "a".into(),
                DataType::Integer,
                ConstraintsBuilder::new().build(),
            ),
            Column::new(
                "b".into(),
                DataType::VarChar,
                ConstraintsBuilder::new().build(),
            ),
            Column::new(
                "d".into(),
                DataType::VarChar,
                ConstraintsBuilder::new().build(),
            ),
            Column::new(
                "e".into(),
                DataType::VarChar,
                ConstraintsBuilder::new().nullable().build(),
            ),
        ])
        .unwrap();
        let values = vec![
            Value::Integer(42),
            Value::VarChar("aaaa".to_string()),
            Value::VarChar("bbbbb".to_string()),
            Value::Null,
        ];
        let tuple = Tuple::try_new(values).unwrap();
        assert!(tuple.validate_with_schema(&schema).is_ok());
    }

    #[test]
    fn validate_tuple_nullable() {
        let schema = Schema::try_new(vec![
            Column::new(
                "a".into(),
                DataType::Integer,
                ConstraintsBuilder::new().nullable().build(),
            ),
            Column::new(
                "b".into(),
                DataType::VarChar,
                ConstraintsBuilder::new().nullable().build(),
            ),
            Column::new(
                "c".into(),
                DataType::VarChar,
                ConstraintsBuilder::new().nullable().build(),
            ),
            Column::new(
                "d".into(),
                DataType::VarChar,
                ConstraintsBuilder::new().nullable().build(),
            ),
        ])
        .unwrap();
        let values = vec![Value::Null, Value::Null, Value::Null, Value::Null];
        let tuple = Tuple::try_new(values).unwrap();
        assert!(tuple.validate_with_schema(&schema).is_ok());
    }
}
