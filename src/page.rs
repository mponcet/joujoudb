pub const PAGE_SIZE: usize = 4096;

pub type PageId = usize;

pub struct PageMetadata {
    page_id: PageId,
    dirty: bool,
}

// pub struct PageData(Box

pub struct Page {
    pub metadata: PageMetadata,
    // the actual data read from/written to disk
    pub data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new(page_id: PageId) -> Self {
        Self {
            metadata: PageMetadata {
                page_id,
                dirty: false,
            },
            data: [0; PAGE_SIZE],
        }
    }

    pub fn page_id(&self) -> PageId {
        self.metadata.page_id
    }

    pub fn is_dirty(&self) -> bool {
        self.metadata.dirty
    }

    pub fn set_dirty(&mut self) {
        self.metadata.dirty = true;
    }

    pub fn clear_dirty(&mut self) {
        self.metadata.dirty = false;
    }
}
