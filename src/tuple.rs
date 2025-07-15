use std::borrow::Cow;

use crate::zerocopy::{FromBytes, IntoBytes};
use thiserror::Error;

use crate::page::Page;

#[repr(C)]
struct TupleHeader {
    len: u16,
}

const TUPLE_HEADER_SIZE: usize = std::mem::size_of::<TupleHeader>();

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
/// let tuple = Tuple::from_bytes(&page_slice);
/// ```
pub struct Tuple<'page> {
    data: Cow<'page, [u8]>,
}

#[derive(Error, Debug)]
pub enum TupleError {
    #[error("tuple size cannot exceed {}", Page::MAX_TUPLE_SIZE)]
    Size,
}

impl<'a> Tuple<'a> {
    pub fn new(value: Vec<u8>) -> Result<Self, TupleError> {
        let value_len = value.len();
        if TUPLE_HEADER_SIZE + value_len > Page::MAX_TUPLE_SIZE {
            return Err(TupleError::Size);
        }

        let header = TupleHeader::new(value_len as u16);
        let mut data = Vec::with_capacity(TUPLE_HEADER_SIZE + value_len);
        data.extend_from_slice(header.as_bytes());
        data.extend(value);

        Ok(Self {
            data: Cow::from(data),
        })
    }

    // fn header(&self) -> &TupleHeader {
    //     TupleHeader::try_ref_from_bytes(self.data.as_ref()).unwrap()
    // }

    pub fn value(&self) -> &[u8] {
        &self.data[TUPLE_HEADER_SIZE..]
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}

impl<'page> FromBytes<'page> for Tuple<'page> {
    fn from_bytes(bytes: &'page [u8]) -> Tuple<'page> {
        Self {
            data: Cow::from(bytes),
        }
    }
}

impl<'a> IntoBytes for Tuple<'a> {
    fn as_bytes(&self) -> &[u8] {
        self.data.as_ref()
    }
}

// impl TryRefFromBytes for TupleHeader {
//     fn try_ref_from_bytes(bytes: &[u8]) -> Result<&Self, ZeroCopyError> {
//         if bytes.len() < std::mem::size_of::<Self>() {
//             return Err(ZeroCopyError::Size);
//         }
//
//         Ok(unsafe { &*(bytes.as_ptr() as *const Self) })
//     }
// }

impl IntoBytes for TupleHeader {
    fn as_bytes(&self) -> &[u8] {
        let len = std::mem::size_of::<Self>();
        let slf = self as *const Self;
        unsafe { std::slice::from_raw_parts(slf.cast::<u8>(), len) }
    }
}
