use zerocopy::{
    byteorder::little_endian::{I64, U16},
    *,
};
use zerocopy_derive::*;

use crate::serialize::{Deserialize, Serialize};
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

#[derive(Clone, Copy, Debug, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub struct BigInt(I64);

impl BigInt {
    pub fn new(i: i64) -> Self {
        Self(I64::new(i))
    }

    pub fn get(&self) -> i64 {
        self.0.get()
    }
}

impl Serialize for BigInt {
    fn write_bytes_to(&self, dst: &mut [u8]) {
        zerocopy::IntoBytes::write_to(self, &mut dst[..8]).unwrap();
    }
}

impl Deserialize for BigInt {
    fn from_bytes(source: &[u8]) -> Self {
        *BigInt::ref_from_bytes(&source[0..8]).unwrap()
    }
}

// Char is a fixed-length string, padded with spaces.
#[derive(Clone, Debug)]
pub struct Char(String);

impl Char {
    pub fn new(mut s: String, n: Option<usize>) -> Self {
        if let Some(n) = n {
            let num_spaces = n - s.len();
            let spaces = std::iter::repeat_n(' ', num_spaces);
            s.extend(spaces);
        }
        Self(s)
    }

    pub fn get(&self) -> &str {
        self.0.as_str().trim_end_matches(' ')
    }
}
impl Serialize for Char {
    fn write_bytes_to(&self, dst: &mut [u8]) {
        let src = self.0.as_bytes();
        src.write_to(&mut dst[..src.len()]).unwrap()
    }
}

impl Deserialize for Char {
    fn from_bytes(source: &[u8]) -> Self {
        let char = str::from_utf8(source).unwrap().to_string();
        Char::new(char, None)
    }
}

#[derive(Clone, Debug)]
pub struct VarChar(String);

impl VarChar {
    pub fn new(s: String) -> Self {
        Self(s)
    }

    pub fn get(&self) -> &str {
        self.0.as_str()
    }
}

impl Serialize for VarChar {
    fn write_bytes_to(&self, dst: &mut [u8]) {
        let header = ValueHeader::new(self.0.len());
        let offset = ValueHeader::SIZE;
        header.write_to(&mut dst[..offset]).unwrap();
        let src = self.0.as_bytes();
        src.write_to(&mut dst[offset..offset + src.len()]).unwrap();
    }
}

impl Deserialize for VarChar {
    fn from_bytes(source: &[u8]) -> Self {
        let varchar = VarCharRef::ref_from_bytes(source).unwrap();
        let split = varchar.split_at(varchar.header.len() as usize).unwrap();
        let (varchar, _) = split.via_immutable();
        varchar.to_owned()
    }
}

#[derive(SplitAt, FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub struct VarCharRef {
    header: ValueHeader,
    data: [u8],
}

impl VarCharRef {
    fn to_owned(&self) -> VarChar {
        VarChar(String::from_utf8(self.data.to_vec()).unwrap())
    }
}

#[derive(Clone, Debug)]
pub enum Value {
    BigInt(BigInt),
    Char(Char),
    VarChar(VarChar),
    Null,
}

impl Value {
    pub fn from_bytes(bytes: &[u8], data_type: DataType) -> Self {
        match data_type {
            DataType::Char(n) => Self::Char(Deserialize::from_bytes(&bytes[..n])),
            DataType::VarChar => Self::VarChar(Deserialize::from_bytes(bytes)),
            DataType::BigInt => Self::BigInt(Deserialize::from_bytes(bytes)),
        }
    }

    pub fn header_size(&self) -> usize {
        match self {
            Value::BigInt(_) => 0,
            Value::Char(_) => 0,
            Value::VarChar(_) => ValueHeader::SIZE,
            Value::Null => 0,
        }
    }

    pub fn data_size(&self) -> usize {
        match self {
            Value::BigInt(_) => std::mem::size_of::<BigInt>(),
            Value::Char(char) => char.0.len(),
            Value::VarChar(varchar) => varchar.0.len(),
            Value::Null => 0,
        }
    }

    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Value::BigInt(_) => Some(DataType::BigInt),
            Value::Char(s) => Some(DataType::Char(s.0.len())),
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
            Value::BigInt(bigint) => bigint.write_bytes_to(dst),
            Value::Char(char) => char.write_bytes_to(dst),
            Value::VarChar(varchar) => varchar.write_bytes_to(dst),
            Value::Null => unreachable!(),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::BigInt(lhs), Self::BigInt(rhs)) => lhs.get() == rhs.get(),
            (Self::Char(lhs), Self::Char(rhs)) => lhs.get() == rhs.get(),
            (Self::VarChar(lhs), Self::VarChar(rhs)) => lhs.get() == rhs.get(),
            (Self::Null, Self::Null) => true,
            _ => false,
        }
    }
}
