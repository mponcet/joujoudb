mod btree;
mod heappage;
mod page;

pub use btree::{BTreeInnerPage, BTreeLeafPage, Key, RecordId};
pub use heappage::{HeapPage, HeapPageSlotId};
pub use page::{PAGE_SIZE, Page, PageId, PageMetadata};

pub use btree::{BTreePageType, btree_get_page_type};
