use crate::pages::HeapPage;

use thiserror::Error;
use zerocopy_derive::*;

#[derive(Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C, packed)]
pub struct TupleHeader {
    len: u16,
}

impl TupleHeader {
    fn new(len: u16) -> Self {
        Self { len }
    }
}

/// A reference to a tuple stored in a page.
///
/// `TupleRef` provides a way to access tuple data without copying it.
#[derive(FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C, packed)]
pub struct TupleRef {
    header: TupleHeader,
    values: [u8],
}

/// A newly created tuple that owns its data.
pub struct Tuple {
    header: TupleHeader,
    values: Box<[u8]>,
}

impl TupleRef {
    pub fn header(&self) -> &TupleHeader {
        &self.header
    }

    pub fn values(&self) -> &[u8] {
        &self.values
    }

    pub fn to_owned(&self) -> Tuple {
        Tuple {
            header: self.header,
            values: Box::from(&self.values),
        }
    }
}

#[derive(Error, Debug)]
pub enum TupleError {
    #[error("tuple size cannot exceed {}", HeapPage::MAX_TUPLE_SIZE)]
    Size,
}

impl Tuple {
    /// The size of the tuple header in bytes.
    pub const HEADER_SIZE: usize = std::mem::size_of::<TupleHeader>();

    /// Creates a new tuple with the given values.
    ///
    /// Returns a `Result` containing the new `Tuple`, or a `TupleError` if the tuple size exceeds the maximum allowed.
    pub fn try_new(values: Box<[u8]>) -> Result<Self, TupleError> {
        let values_len = values.len();
        if Self::HEADER_SIZE + values_len > HeapPage::MAX_TUPLE_SIZE {
            return Err(TupleError::Size);
        }

        Ok(Tuple {
            header: TupleHeader::new(values_len as u16),
            values,
        })
    }

    /// Returns a reference to the tuple's header.
    pub fn header(&self) -> &TupleHeader {
        &self.header
    }

    /// Returns a slice containing the tuple's values.
    pub fn values(&self) -> &[u8] {
        &self.values
    }

    /// Returns the total length of the tuple in bytes, including the header.
    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        Self::HEADER_SIZE + self.values().len()
    }
}
