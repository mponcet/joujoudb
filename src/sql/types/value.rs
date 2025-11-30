use zerocopy::{
    byteorder::little_endian::{I64, U16},
    *,
};
use zerocopy_derive::*;

use crate::serialize::Serialize;
use crate::sql::schema::DataType;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub struct ValueHeader {
    pub len: U16,
}

impl ValueHeader {
    const SIZE: usize = std::mem::size_of::<ValueHeader>();

    pub fn new(len: usize) -> Self {
        assert!(len <= u16::MAX as usize);
        Self {
            len: U16::new(len as u16),
        }
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u16 {
        self.len.get()
    }
}

#[derive(SplitAt, FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub struct VarCharRef {
    header: ValueHeader,
    data: [u8],
}

impl VarCharRef {
    fn to_owned(&self) -> String {
        String::from_utf8(self.data.to_vec()).unwrap()
    }
}

#[derive(Clone, Debug)]
pub enum Value {
    Integer(i64),
    VarChar(String),
    Boolean(bool),
    Null,
}

impl Value {
    pub fn from_bytes(bytes: &[u8], data_type: DataType) -> Self {
        match data_type {
            DataType::Boolean => {
                let b = bytes[0] == 0x01;
                Self::Boolean(b)
            }
            DataType::Integer => {
                let i = I64::ref_from_bytes(&bytes[0..8]).unwrap().get();
                Self::Integer(i)
            }
            DataType::VarChar => {
                let varchar = VarCharRef::ref_from_bytes(bytes).unwrap();
                let split = varchar.split_at(varchar.header.len() as usize).unwrap();
                let (varchar, _) = split.via_immutable();
                let varchar = varchar.to_owned();
                Self::VarChar(varchar)
            }
        }
    }

    pub fn header_size(&self) -> usize {
        match self {
            Value::Boolean(_) => 0,
            Value::Integer(_) => 0,
            Value::VarChar(_) => ValueHeader::SIZE,
            Value::Null => 0,
        }
    }

    pub fn data_size(&self) -> usize {
        match self {
            Value::Boolean(_) => std::mem::size_of::<u8>(),
            Value::Integer(_) => std::mem::size_of::<i64>(),
            Value::VarChar(varchar) => varchar.len(),
            Value::Null => 0,
        }
    }

    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Value::Boolean(_) => Some(DataType::Boolean),
            Value::Integer(_) => Some(DataType::Integer),
            Value::VarChar(_) => Some(DataType::VarChar),
            Value::Null => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl Serialize for Value {
    fn write_bytes_to(&self, dst: &mut [u8]) {
        match self {
            Value::Boolean(b) => {
                // An object with the boolean type has a size and alignment of 1 each.
                // The value false has the bit pattern 0x00 and the value true has the bit pattern 0x01.
                // https://doc.rust-lang.org/reference/types/boolean.html
                dst[0] = *b as u8;
            }
            Value::Integer(i) => {
                let i = I64::new(*i);
                i.write_to(&mut dst[0..8]).unwrap();
            }
            Value::VarChar(s) => {
                let header = ValueHeader::new(s.len());
                let offset = ValueHeader::SIZE;
                header.write_to(&mut dst[..offset]).unwrap();
                let src = s.as_bytes();
                src.write_to(&mut dst[offset..offset + src.len()]).unwrap();
            }
            Value::Null => unreachable!(),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Boolean(lhs), Self::Boolean(rhs)) => lhs.eq(rhs),
            (Self::Integer(lhs), Self::Integer(rhs)) => lhs.eq(rhs),
            (Self::VarChar(lhs), Self::VarChar(rhs)) => lhs.eq(rhs),
            (Self::Null, Self::Null) => true,
            _ => false,
        }
    }
}
