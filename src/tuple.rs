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

/// A tuple with a header and values
///
/// # Examples
/// You can create a new tuple from values with:
///
/// ```
/// let tuple = Tuple::new(values);
/// ```
///
/// You can also create a tuple referencing a tuple inside a `Page`:
///
/// ```
/// let tuple = Page::get_tuple(slot_id);
/// ```
#[derive(FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C, packed)]
pub struct TupleRef {
    header: TupleHeader,
    values: [u8],
}

pub struct TupleNew {
    header: TupleHeader,
    values: Box<[u8]>,
}

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
    pub const HEADER_SIZE: usize = std::mem::size_of::<TupleHeader>();

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

    pub fn header(&self) -> &TupleHeader {
        match self {
            Tuple::New(tuple_new) => &tuple_new.header,
            Tuple::Ref(tuple_ref) => &tuple_ref.header,
        }
    }

    pub fn values(&self) -> &[u8] {
        match self {
            Tuple::New(tuple_new) => &tuple_new.values,
            Tuple::Ref(tuple_ref) => &tuple_ref.values,
        }
    }

    pub fn len(&self) -> usize {
        Self::HEADER_SIZE + self.values().len()
    }
}
