mod page;
mod tuple;
mod zerocopy;

use page::Page;
use tuple::Tuple;

fn main() {
    let mut page = Page::new();
    let tuple = Tuple::new(vec![0, 1, 2, 3]).unwrap();
    page.insert_tuple(&tuple).expect("cannot insert");
    let tuple2 = page.get_tuple(0).expect("cannot get tuple");
    assert_eq!(tuple.value(), tuple2.value());
    page.delete_tuple(0).expect("cannot delete tuple");
}
