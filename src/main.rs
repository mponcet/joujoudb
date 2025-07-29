mod cache;
mod heappage;
mod page;
mod storage;
mod tuple;

use cache::PageCache;
use heappage::HeapPage;
use tuple::Tuple;

use storage::Storage;

fn test_path() -> std::path::PathBuf {
    [
        "/tmp/",
        "joujoudb_",
        uuid::Uuid::new_v4().to_string().as_str(),
    ]
    .into_iter()
    .collect::<String>()
    .into()
}

fn main() {
    let mut page = HeapPage::new();
    let values = vec![0, 1, 2, 3].into_boxed_slice();
    let tuple = Tuple::try_new(values).unwrap();
    page.insert_tuple(&tuple).expect("cannot insert");
    let tuple2 = page.get_tuple(0).expect("cannot get tuple");
    assert_eq!(tuple.values(), tuple2.values());
    page.delete_tuple(0).expect("cannot delete tuple");

    let storage = Storage::open(test_path()).unwrap();
    let page_cache = PageCache::new(storage);
    let _ = page_cache.new_page();
    let _ = page_cache.get_page(0);
    let _ = page_cache.get_page_mut(0);
}
