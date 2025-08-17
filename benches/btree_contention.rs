use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

fn btree_contention_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree mixed contention benchmark - slow path");
    group.sample_size(10);
    group.bench_function("9 readers , 1 writer", |b| {
        b.iter(|| btree_mixed_benchmark_call::<false>(black_box(9)));
    });
    group.finish();

    let mut group = c.benchmark_group("btree mixed contention benchmark - fast path");
    group.sample_size(10);
    group.bench_function("9 readers , 1 writer", |b| {
        b.iter(|| btree_mixed_benchmark_call::<true>(black_box(9)));
    });
    group.finish();

    let mut group = c.benchmark_group("btree insert contention benchmark - slow path");
    group.sample_size(10);
    group.bench_function("8 threads", |b| {
        b.iter(|| btree_write_benchmark_call::<false>(black_box(8)));
    });
    group.bench_function("16 threads", |b| {
        b.iter(|| btree_write_benchmark_call::<false>(black_box(16)));
    });
    group.finish();

    let mut group = c.benchmark_group("btree insert contention benchmark - fast path");
    group.sample_size(10);
    group.bench_function("8 threads", |b| {
        b.iter(|| btree_write_benchmark_call::<true>(black_box(8)));
    });
    group.bench_function("16 threads", |b| {
        b.iter(|| btree_write_benchmark_call::<true>(black_box(16)));
    });
    group.finish();
}

extern crate joujoudb;
use joujoudb::indexes::BTree;
use joujoudb::pages::{Key, RecordId};
use joujoudb::storage::Storage;

use std::sync::Arc;
use std::thread;

use tempfile::NamedTempFile;

fn btree_mixed_benchmark_call<const FAST_PATH: bool>(num_read_threads: usize) {
    let storage_path = NamedTempFile::new().unwrap();
    let storage = Storage::open(storage_path).unwrap();

    let btree = Arc::new(BTree::try_new(storage).unwrap());
    let mut threads = Vec::new();
    let btree_clone = Arc::clone(&btree);
    let start_key = 0;
    let end_key = 16000;

    for _ in 0..num_read_threads {
        let btree_clone = Arc::clone(&btree);

        let handle = thread::spawn(move || {
            for key in start_key..end_key {
                let _ = btree_clone.search(key as Key);
            }
        });

        threads.push(handle);
    }

    let handle = thread::spawn(move || {
        // HACK: stop when reader threads stop
        while Arc::strong_count(&btree_clone) > 2 {
            for key in start_key..end_key {
                let record_id = RecordId::new(0, 0);

                if FAST_PATH {
                    btree_clone.insert(key as Key, record_id).unwrap();
                } else {
                    btree_clone.insert_slow_path(key as Key, record_id).unwrap();
                }
            }
            for key in start_key..end_key {
                btree_clone.delete(key as Key).unwrap();
            }
        }
    });
    threads.push(handle);

    for handle in threads {
        handle.join().unwrap();
    }
}

fn btree_write_benchmark_call<const FAST_PATH: bool>(num_threads: usize) {
    let storage_path = NamedTempFile::new().unwrap();
    let storage = Storage::open(storage_path).unwrap();

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
                if FAST_PATH {
                    btree_clone.insert(key as Key, record_id).unwrap();
                } else {
                    btree_clone.insert_slow_path(key as Key, record_id).unwrap();
                }
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
