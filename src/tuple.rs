use crate::pages::HeapPage;

use thiserror::Error;
use zerocopy_derive::*;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
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
pub struct TupleNew {
    header: TupleHeader,
    values: Box<[u8]>,
}

/// Represents a tuple, which can be either a new tuple or a reference to an existing one.
///
/// This enum allows for flexible handling of tuples, whether they are being created
/// or read from storage.
pub enum Tuple<'a> {
    New(TupleNew),
    Ref(&'a TupleRef),
}

#[derive(Error, Debug)]
pub enum TupleError {
    #[error("tuple size cannot exceed {}", HeapPage::MAX_TUPLE_SIZE)]
    Size,
}

impl<'a> Tuple<'a> {
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

        Ok(Self::New(TupleNew {
            header: TupleHeader::new(values_len as u16),
            values,
        }))
    }

    /// Returns a reference to the tuple's header.
    pub fn header(&self) -> &TupleHeader {
        match self {
            Tuple::New(tuple_new) => &tuple_new.header,
            Tuple::Ref(tuple_ref) => &tuple_ref.header,
        }
    }

    /// Returns a slice containing the tuple's values.
    pub fn values(&self) -> &[u8] {
        match self {
            Tuple::New(tuple_new) => &tuple_new.values,
            Tuple::Ref(tuple_ref) => &tuple_ref.values,
        }
    }

    /// Returns the total length of the tuple in bytes, including the header.
    pub fn len(&self) -> usize {
        Self::HEADER_SIZE + self.values().len()
    }
}
