pub trait Serialize {
    fn write_bytes_to(&self, dst: &mut [u8]);
}

pub trait Deserialize {
    fn from_bytes(source: &[u8]) -> Self;
}
