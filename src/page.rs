pub const PAGE_SIZE: usize = 4096;

pub type PageId = usize;

pub struct PageMetadata {
    pub page_id: PageId,
    // pub dirty: bool,
}

pub struct Page {
    pub metadata: PageMetadata,
    // the actual data read/written from/to disk
    pub data: Box<[u8; PAGE_SIZE]>,
}

impl Page {
    pub fn new(page_id: PageId) -> Self {
        Self {
            metadata: PageMetadata {
                page_id,
                // dirty: false,
            },
            data: Box::new([0; PAGE_SIZE]),
        }
    }

    // pub fn mark_dirty(&mut self) {
    //     self.metadata.dirty = true;
    // }
}
