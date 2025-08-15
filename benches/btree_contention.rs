use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

fn btree_contention_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree insert contention benchmark");
    group.bench_function("2 threads", |b| {
        b.iter(|| btree_lock_benchmark_call(black_box(2)));
    });
    group.bench_function("8 threads", |b| {
        b.iter(|| btree_lock_benchmark_call(black_box(8)));
    });
    group.bench_function("16 threads", |b| {
        b.iter(|| btree_lock_benchmark_call(black_box(16)));
    });
    group.bench_function("32 threads", |b| {
        b.iter(|| btree_lock_benchmark_call(black_box(32)));
    });
    group.finish();
}

extern crate joujoudb;
use joujoudb::indexes::BTree;
use joujoudb::pages::{Key, RecordId};
use joujoudb::storage::Storage;
use std::sync::Arc;
use std::thread;

fn btree_lock_benchmark_call(num_threads: usize) {
    let storage_path = format!("/tmp/btree_contention_test_{}.db", uuid::Uuid::new_v4());
    let storage = Storage::open(&storage_path).unwrap();

    let btree = Arc::new(BTree::try_new(storage).unwrap());

    let keys_per_threads = 16000 / num_threads;
    const KEY_STRIDE: usize = 6400000;

    let mut threads = Vec::new();

    for i in 0..num_threads {
        let btree_clone = Arc::clone(&btree);
        let start_key = i * KEY_STRIDE;
        let end_key = start_key + keys_per_threads;

        let handle = thread::spawn(move || {
            for key in start_key..end_key {
                let record_id = RecordId::new(0, 0);
                btree_clone.insert(key as Key, record_id).unwrap();
            }
        });

        threads.push(handle);
    }

    for handle in threads {
        handle.join().unwrap();
    }
}

criterion_group!(benches, btree_contention_benchmark);
criterion_main!(benches);
