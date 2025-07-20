mod heappage;
mod page;
mod storage;
mod tuple;

use heappage::HeapPage;
use tuple::Tuple;

fn main() {
    let mut page = HeapPage::new();
    let values = vec![0, 1, 2, 3].into_boxed_slice();
    let tuple = Tuple::try_new(values).unwrap();
    page.insert_tuple(&tuple).expect("cannot insert");
    let tuple2 = page.get_tuple(0).expect("cannot get tuple");
    assert_eq!(tuple.values(), tuple2.values());
    page.delete_tuple(0).expect("cannot delete tuple");
}
