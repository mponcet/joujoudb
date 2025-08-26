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
                let value = Value::from_bytes(&self.values[offset..], column.column_type);
                offset += value.header_len();
                offset += value.data_len();
                values.push(value);
            }
        }

        Tuple { values }
    }
}

#[derive(Error, Debug)]
pub enum TupleError {
    #[error("tuple size cannot exceed {}", HeapPage::MAX_TUPLE_SIZE)]
    Size,
    #[error("tuple cannot have more than {} columns", Tuple::MAX_COLUMNS)]
    TooManyColumns,
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

        let values_len = values
            .iter()
            .map(|v| v.header_len() + v.data_len())
            .sum::<usize>();

        if Self::HEADER_SIZE + values_len <= HeapPage::MAX_TUPLE_SIZE {
            Ok(Tuple { values })
        } else {
            Err(TupleError::Size)
        }
    }

    /// Returns the total length of the tuple in bytes, including the header.
    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        Self::HEADER_SIZE
            + self
                .values
                .iter()
                .map(|v| v.header_len() + v.data_len())
                .sum::<usize>()
    }

    #[cfg(test)]
    fn as_bytes(&self) -> &[u8] {
        let zero = std::iter::repeat_n(0, self.len());
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
        let (len, null_bitmap) = self.values.iter().enumerate().fold(
            (0, NullBitmap::default()),
            |(mut len, mut bitmap), (i, value)| {
                if value.is_null() {
                    bitmap.set_null(i)
                }
                len += value.header_len() + value.data_len();
                (len, bitmap)
            },
        );
        let header = TupleHeader::new(len, null_bitmap);
        let mut offset = Self::HEADER_SIZE;
        header.write_to(&mut dst[..offset]).unwrap();

        for value in self.values.iter() {
            if !value.is_null() {
                value.write_bytes_to(&mut dst[offset..]);
                offset += value.header_len() + value.data_len();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::schema::{Column, ColumnType, Constraints};
    use crate::sql::types::{BigInt, Char, Value, VarChar};

    use super::*;

    #[test]
    fn read_after_write() {
        let schema = Schema::new(vec![
            Column::new(ColumnType::BigInt, Constraints::default()),
            Column::new(ColumnType::VarChar, Constraints::default()),
            Column::new(ColumnType::Char(32), Constraints::default()),
            Column::new(ColumnType::VarChar, Constraints::default()),
            Column::new(ColumnType::VarChar, Constraints::new(true, false)),
        ]);
        let values = vec![
            Value::BigInt(BigInt::new(42)),
            Value::VarChar(VarChar::new("aaaa".to_string())),
            Value::Char(Char::new("AAAA".to_string(), Some(32))),
            Value::VarChar(VarChar::new("bbbbb".to_string())),
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
}
