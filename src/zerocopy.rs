// use thiserror::Error;

// #[derive(Error, Debug)]
// pub enum ZeroCopyError {
//     #[error("wrong size")]
//     Size,
// }

// pub trait TryRefFromBytes: Sized {
//     fn try_ref_from_bytes(bytes: &[u8]) -> Result<&Self, ZeroCopyError>;
// }

pub trait FromBytes<'a>: Sized {
    fn from_bytes(bytes: &'a [u8]) -> Self;
}

pub trait RefFromBytes<'a>: Sized {
    fn ref_from_bytes(bytes: &'a [u8]) -> &'a Self;
}

pub trait RefMutFromBytes<'a>: Sized {
    fn ref_mut_from_bytes(bytes: &'a mut [u8]) -> &'a mut Self;
}

pub trait IntoBytes: Sized {
    fn as_bytes(&self) -> &[u8];
}
