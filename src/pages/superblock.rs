use zerocopy_derive::*;

#[repr(C)]
struct SuperBlock {
    root_page_id: PageId,
    _checksum: u32,
}

