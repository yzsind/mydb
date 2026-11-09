use super::*;

#[test]
fn point_lookup_consumes_the_routed_leaf_without_repinning() {
    let (_dir, index) = index_fixture(
        (0..32)
            .map(|v| IndexKey::from(vec![KeyPart::Int(v)]))
            .collect(),
    );
    let key = IndexKey::from(vec![KeyPart::Int(12)]);
    let expected = RowId {
        page_id: 0,
        slot_id: 12,
    };
    let leaf = index
        .buffer_pool
        .lookup_frame(&index.page_key(index.root_page.load(Ordering::Acquire)))
        .unwrap()
        .unwrap();
    for operation in 0..5 {
        leaf.usage_count.store(0, Ordering::Relaxed);
        let before = index.buffer_pool.buffer_stats().hits;
        match operation {
            0 => assert_eq!(index.get_unique_one(&key).unwrap(), Some(expected)),
            1 => assert_eq!(index.get(&key, true).unwrap(), vec![expected]),
            2 => assert_eq!(index.prefix(&key, true).unwrap(), vec![expected]),
            3 => assert_eq!(
                index.get_entries(&key, true).unwrap(),
                vec![(key.clone(), expected)]
            ),
            _ => assert_eq!(
                index.prefix_entries(&key, true).unwrap(),
                vec![(key.clone(), expected)]
            ),
        }
        assert_eq!(
            index.buffer_pool.buffer_stats().hits - before,
            1,
            "a one-page tree should need only one buffer acquisition per lookup"
        );
        assert_eq!(
            leaf.usage_count.load(Ordering::Relaxed),
            2,
            "pin reuse must preserve the routing and consumption CLOCK accesses"
        );
        assert_eq!(index.buffer_pool.snapshot().pinned_pages, 0);
    }
}

fn index_fixture(keys: Vec<IndexKey>) -> (tempfile::TempDir, IndexFile) {
    index_fixture_with_durability(keys, DurabilityMode::Memory)
}

fn index_fixture_with_durability(
    keys: Vec<IndexKey>,
    durability: DurabilityMode,
) -> (tempfile::TempDir, IndexFile) {
    let dir = tempfile::tempdir().unwrap();
    let log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(dir.path()).unwrap(),
    ));
    let pool = BufferPool::new(
        1024,
        log.clone(),
        durability,
        Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
    );
    let index =
        IndexFile::open_with_durability(dir.path().join("lookup.idx"), log, durability, pool)
            .unwrap();
    let mut entries: Vec<_> = keys
        .into_iter()
        .enumerate()
        .map(|(i, key)| IndexEntry {
            key,
            row_id: RowId {
                page_id: (i / 32) as u32,
                slot_id: (i % 32) as u16,
            },
        })
        .collect();
    entries.sort();
    index.bulk_load_sorted(1, entries).unwrap();
    (dir, index)
}

// Process-level A/B: run this exact ignored test with old/new binaries, and
// with MYDB_INLJ_ENABLE_ROUTED_BATCH_LOOKUP=0 to isolate the environment cache.
// Setup, key construction, warmup and thread creation are outside timing.
#[test]
#[ignore = "release microbenchmark; run explicitly with --nocapture"]
fn small_batch_lookup_benchmark() {
    use std::hint::black_box;
    for rows in [32, 20_000] {
        for kind in [0, 1] {
            let make = |n: i64| {
                IndexKey::from(if kind == 0 {
                    vec![KeyPart::Int(n)]
                } else {
                    vec![
                        KeyPart::Int(n / 1000),
                        KeyPart::String(format!("key-{n:08}")),
                    ]
                })
            };
            let (_dir, index) = index_fixture((0..rows).map(make).collect());
            for scattered in [false, true] {
                for count in [1, 2, 5, 15, 63, 64, 200] {
                    let probes: Vec<Vec<_>> = (0..16)
                        .map(|offset| {
                            let mut keys: Vec<_> = (0..count)
                                .map(|i| {
                                    let n = if scattered {
                                        (i * 7919 + offset * 101) % (rows + 10)
                                    } else {
                                        (rows / 3 + offset + i) % (rows + 10)
                                    };
                                    make(n)
                                })
                                .collect();
                            keys.sort();
                            keys.dedup();
                            keys
                        })
                        .collect();
                    // Small fixtures intentionally include repeated and missing keys.
                    for keys in &probes {
                        black_box(index.lookup_entries_many_sorted(keys, true, true).unwrap());
                    }
                    for threads in [1, 24] {
                        if threads > 1
                            && (rows == 32
                                || kind != 0
                                || !scattered
                                || ![1, 2, 15, 63].contains(&count))
                        {
                            continue;
                        }
                        let iterations = (2000 / count).max(40) as usize;
                        let mut samples = Vec::new();
                        for _ in 0..5 {
                            let barrier = std::sync::Barrier::new(threads + 1);
                            let elapsed = std::thread::scope(|scope| {
                                for thread in 0..threads {
                                    let (index, probes, barrier) = (&index, &probes, &barrier);
                                    scope.spawn(move || {
                                        barrier.wait();
                                        for i in 0..iterations {
                                            black_box(
                                                index
                                                    .lookup_entries_many_sorted(
                                                        black_box(
                                                            &probes[(i + thread) % probes.len()],
                                                        ),
                                                        true,
                                                        true,
                                                    )
                                                    .unwrap(),
                                            );
                                        }
                                        barrier.wait();
                                    });
                                }
                                let start = Instant::now();
                                barrier.wait();
                                barrier.wait();
                                start.elapsed()
                            });
                            samples.push(elapsed.as_nanos() as f64 / (threads * iterations) as f64);
                        }
                        samples.sort_by(f64::total_cmp);
                        println!("BATCH rows={rows} kind={kind} scattered={scattered} requested={count} actual={} threads={threads} ns={:.1}", probes[0].len(), samples[2]);
                    }
                }
            }
        }
    }
}

fn compare_paths(index: &IndexFile, keys: &[IndexKey], exact: bool, dedup: bool) {
    let _structure = index.structure_lock.read_recursive();
    let fallback = index
        .lookup_entries_many_sorted_fallback(keys, exact, dedup)
        .unwrap();
    let routed = index
        .lookup_entries_many_sorted_routed(keys, exact, dedup, &|_, key, row| (key, row))
        .unwrap();
    let mut scalar = Vec::new();
    let mut seen = dedup.then(crate::util::fast_hash::ah_hash_set);
    for key in keys {
        let found = if exact {
            index.get_entries(key, false)
        } else {
            index.prefix_entries(key, false)
        }
        .unwrap();
        let mut expected_rows = Vec::new();
        let mut row_seen = dedup.then(crate::util::fast_hash::ah_hash_set);
        for (_, row) in &found {
            push_row_id(&mut expected_rows, &mut row_seen, *row);
        }
        let rows = if exact {
            assert_eq!(
                index.get_unique_one(key).unwrap(),
                expected_rows.first().copied()
            );
            index.get(key, dedup).unwrap()
        } else {
            index.prefix(key, dedup).unwrap()
        };
        assert_eq!(rows, expected_rows);
        assert_eq!(index.buffer_pool.snapshot().pinned_pages, 0);
        for (key, row) in found {
            push_index_entry_tuple(&mut scalar, &mut seen, key, row);
        }
    }
    assert_eq!(
        routed, scalar,
        "routed: exact={exact}, dedup={dedup}, keys={keys:?}"
    );
    assert_eq!(fallback, scalar, "fallback: exact={exact}, dedup={dedup}");
    assert_eq!(
        index
            .lookup_entries_many_sorted(keys, exact, dedup)
            .unwrap(),
        scalar
    );
}

fn lookup_key_makers() -> Vec<Box<dyn Fn(i64) -> Vec<KeyPart>>> {
    vec![
        Box::new(|n| vec![KeyPart::Int(n - 100)]),
        Box::new(|n| vec![KeyPart::Int(n * 100_000)]),
        Box::new(|n| vec![KeyPart::UInt((1 << 63) + n as u64)]),
        Box::new(|n| vec![KeyPart::UInt(n as u64 * 100_000)]),
        Box::new(|n| vec![KeyPart::Int(n / 7), KeyPart::Int(n % 7)]),
        Box::new(|n| vec![KeyPart::Date(18_000 + n as i32)]),
        Box::new(|n| vec![KeyPart::DateTime(n * 1_000_000)]),
        Box::new(|n| vec![KeyPart::Time(n * 1_000_000)]),
        Box::new(|n| vec![KeyPart::Year((n % 100) as u8)]),
        Box::new(|n| {
            vec![KeyPart::Decimal {
                value: n as i128 - 100,
                scale: 2,
            }]
        }),
        Box::new(|n| {
            vec![KeyPart::Decimal {
                value: n as i128 * 1_000_000_000,
                scale: 4,
            }]
        }),
        Box::new(|n| {
            vec![KeyPart::DecimalWide {
                value: num_bigint::BigInt::from(n) << 160,
                scale: 20,
            }]
        }),
        Box::new(|n| vec![KeyPart::String(format!("键-{n:08}"))]),
        Box::new(|n| vec![KeyPart::Bytes(n.to_be_bytes().to_vec())]),
        Box::new(|n| {
            vec![KeyPart::CollatedString {
                collation_id: 45,
                value: format!("Été-{n:08}"),
            }]
        }),
        Box::new(|n| vec![KeyPart::Float32(f32_to_order_u32((n - 100) as f32 / 3.0))]),
        Box::new(|n| vec![KeyPart::Float64(f64_to_order_u64((n - 100) as f64 / 7.0))]),
        Box::new(|n| vec![KeyPart::Null, KeyPart::Int(n)]),
        Box::new(|n| vec![KeyPart::Int(n / 7), KeyPart::String(format!("v-{n:08}"))]),
        // More than four parts exercises heap-backed IndexKey storage too.
        Box::new(|n| vec![KeyPart::Int(n); 5]),
    ]
}

#[test]
fn small_batch_lookup_matches_scalar_and_fallback_across_key_types() {
    let makers = lookup_key_makers();
    let mut layouts = std::collections::BTreeSet::new();
    for make in makers {
        let (_dir, index) = index_fixture(
            (0..2400)
                .step_by(2)
                .map(|n| IndexKey::from(make(n)))
                .collect(),
        );
        let page = index.read_page(index.leftmost_leaf().unwrap()).unwrap();
        let IndexPage::Leaf(leaf) = page.as_ref() else {
            unreachable!()
        };
        layouts.insert(leaf.key_layout);
        for count in [0, 1, 2, 3, 5, 15, 31, 63, 64, 65, 200] {
            let mut keys: Vec<_> = (0..count)
                .map(|i| IndexKey::from(make((i * 97) % 2600)))
                .collect();
            keys.sort();
            keys.dedup(); // Match the sorted, unique probes supplied by database callers.
            for dedup in [false, true] {
                compare_paths(&index, &keys, true, dedup);
            }
        }
    }
    for layout in 0..=9 {
        assert!(layouts.contains(&layout), "missing layout {layout}");
    }
}

#[test]
fn small_batch_lookup_spanning_prefixes_duplicates_and_empty_tree() {
    let (_dir, empty) = index_fixture(Vec::new());
    compare_paths(&empty, &[], true, true);
    compare_paths(
        &empty,
        &[
            IndexKey::from(vec![KeyPart::Int(1)]),
            IndexKey::from(vec![KeyPart::Int(2)]),
        ],
        true,
        false,
    );
    let (_dir, mut index) = index_fixture(
        (0..9000)
            .map(|n| IndexKey::from(vec![KeyPart::Int(n / 3000), KeyPart::Int(n % 1500)]))
            .collect(),
    );
    for count in [1, 2, 3, 5, 15, 63, 64, 200] {
        for exact in [false, true] {
            let mut keys: Vec<_> = (0..count)
                .map(|i| {
                    IndexKey::from(if exact {
                        vec![KeyPart::Int(i % 5 - 1), KeyPart::Int(i * 137 % 1500)]
                    } else {
                        vec![KeyPart::Int(i % 5 - 1)]
                    })
                })
                .collect();
            keys.sort();
            keys.dedup();
            for dedup in [false, true] {
                compare_paths(&index, &keys, exact, dedup);
            }
        }
    }
    // Mutated pages, deleted records, and missing boundary keys.
    for n in (0..9000).step_by(13) {
        index
            .delete(
                IndexKey::from(vec![KeyPart::Int(n / 3000), KeyPart::Int(n % 1500)]),
                RowId {
                    page_id: (n / 32) as u32,
                    slot_id: (n % 32) as u16,
                },
            )
            .unwrap();
    }
    compare_paths(
        &index,
        &[
            IndexKey::from(vec![KeyPart::Int(0)]),
            IndexKey::from(vec![KeyPart::Int(1)]),
        ],
        false,
        true,
    );
}

#[test]
fn small_batch_lookup_routes_through_multiple_internal_levels() {
    let make = |n| IndexKey::from(vec![KeyPart::String(format!("{n:08}-{}", "x".repeat(200)))]);
    let (_dir, index) = index_fixture((0..20_000).map(make).collect());
    let root = index
        .read_page(index.root_page.load(Ordering::Acquire))
        .unwrap();
    let IndexPage::Internal(root) = root.as_ref() else {
        panic!("expected internal root")
    };
    assert!(matches!(
        index.read_page(root.children[0]).unwrap().as_ref(),
        IndexPage::Internal(_)
    ));
    for count in [2, 5, 15, 63, 64, 200] {
        let mut keys: Vec<_> = (0..count).map(|i| make(i * 7919 % 21_000)).collect();
        keys.sort();
        compare_paths(&index, &keys, true, true);
    }
}

#[test]
fn small_batch_lookup_generic_prefixes_match_scalar() {
    let (_dir, index) = index_fixture(
        (0..9000)
            .map(|n| {
                IndexKey::from(vec![
                    KeyPart::String(format!("prefix-{}", n / 3000)),
                    KeyPart::UInt(n % 1500),
                ])
            })
            .collect(),
    );
    for keys in [
        vec![IndexKey::from(vec![])],
        vec![
            IndexKey::from(vec![KeyPart::String("prefix-0".into())]),
            IndexKey::from(vec![KeyPart::String("prefix-1".into())]),
        ],
        vec![
            IndexKey::from(vec![KeyPart::String("prefix-2".into())]),
            IndexKey::from(vec![KeyPart::String("prefix-3".into())]),
        ],
    ] {
        for dedup in [false, true] {
            compare_paths(&index, &keys, false, dedup);
        }
    }
}

#[test]
fn small_batch_lookup_dense_prefixes_and_shared_rowids() {
    for strings in [false, true] {
        let part = |n: i64| {
            if strings {
                KeyPart::String(format!("prefix-{n:02}"))
            } else {
                KeyPart::Int(n)
            }
        };
        let (_dir, index) = index_fixture(
            (0..32)
                .map(|n| IndexKey::from(vec![part(n / 2), KeyPart::Int(n % 2)]))
                .collect(),
        );
        // Two index keys can refer to the same row, e.g. across versions.
        index
            .insert_with_txn(
                2,
                IndexKey::from(vec![part(7), KeyPart::Int(9)]),
                RowId {
                    page_id: 0,
                    slot_id: 0,
                },
            )
            .unwrap();
        for count in [2, 5, 15, 16, 32, 63] {
            let keys: Vec<_> = (0..count).map(|n| IndexKey::from(vec![part(n)])).collect();
            for dedup in [false, true] {
                compare_paths(&index, &keys, false, dedup);
            }
        }
        // A page-local repeated probe must retain the previous lower bound.
        let keys = vec![
            IndexKey::from(vec![part(0)]),
            IndexKey::from(vec![part(0)]),
            IndexKey::from(vec![part(1)]),
        ];
        for dedup in [false, true] {
            compare_paths(&index, &keys, false, dedup);
        }
    }
}

#[test]
fn root_routing_cache_retires_payload_on_eviction_and_raw_mutation() {
    let (_dir, index) = index_fixture_with_durability(
        (0..6000)
            .map(|n| IndexKey::from(vec![KeyPart::Int(n)]))
            .collect(),
        DurabilityMode::Relaxed,
    );
    let key = IndexKey::from(vec![KeyPart::Int(1234)]);
    let expected = warm_root_routing(&index, &key);
    assert!(expected.is_some());
    let token = index.root_routing.get().unwrap().load_full().unwrap();
    let payload = Arc::downgrade(token.image.load().as_ref().unwrap());
    let page = index.page_key(token.page_id);
    let root = index.buffer_pool.lookup_frame(&page).unwrap().unwrap();
    assert_eq!(root.pin_count.load(Ordering::Acquire), 0);
    root.ref_bit.store(false, Ordering::Release);
    root.usage_count.store(0, Ordering::Release);
    for _ in 0..20 {
        assert_eq!(index.get_unique_one(&key).unwrap(), expected);
    }
    root.refresh_root_routing_reference();
    assert!(root.ref_bit.load(Ordering::Acquire));
    assert_eq!(
        root.usage_count.load(Ordering::Acquire),
        Frame::MAX_NORMAL_USAGE_COUNT
    );
    // A failed/no-op writer must invalidate before receiving mutable bytes.
    {
        let _bytes = root.write_bytes().unwrap();
        assert!(token.image.load().is_none());
    }
    assert!(payload.upgrade().is_none());
    assert_eq!(index.get_unique_one(&key).unwrap(), expected);
    let token = index.root_routing.get().unwrap().load_full().unwrap();
    let payload = Arc::downgrade(token.image.load().as_ref().unwrap());
    index.buffer_pool.flush_and_evict_all().unwrap();
    assert!(token.image.load().is_none());
    assert!(payload.upgrade().is_none());
    assert_eq!(index.get_unique_one(&key).unwrap(), expected);
    let root = index.buffer_pool.lookup_frame(&page).unwrap().unwrap();
    drop(root.write_bytes().unwrap());
    crate::engine::executor::mem_budget::with_test_effective_memory_limit(1, || {
        assert_eq!(index.get_unique_one(&key).unwrap(), expected);
        assert!(index.root_routing.get().unwrap().load().is_none());
    });
    assert_eq!(index.get_unique_one(&key).unwrap(), expected);
    index.buffer_pool.discard_file(&index.file_key).unwrap();
    assert!(index
        .root_routing
        .get()
        .unwrap()
        .load()
        .as_ref()
        .unwrap()
        .image
        .load()
        .is_none());
}

#[test]
fn root_routing_cache_follows_root_replacement_and_small_tree_collapse() {
    let (_dir, mut index) = index_fixture(
        (0..6000)
            .map(|n| IndexKey::from(vec![KeyPart::Int(n)]))
            .collect(),
    );
    let key = IndexKey::from(vec![KeyPart::Int(12)]);
    assert!(warm_root_routing(&index, &key).is_some());
    let old = index.root_routing.get().unwrap().load_full().unwrap();
    index
        .bulk_load_sorted(
            2,
            vec![IndexEntry {
                key: key.clone(),
                row_id: RowId {
                    page_id: 99,
                    slot_id: 1,
                },
            }],
        )
        .unwrap();
    assert_eq!(
        index.get_unique_one(&key).unwrap(),
        Some(RowId {
            page_id: 99,
            slot_id: 1
        })
    );
    assert!(old.image.load().is_none());
    index
        .delete(
            key.clone(),
            RowId {
                page_id: 99,
                slot_id: 1,
            },
        )
        .unwrap();
    assert_eq!(index.get_unique_one(&key).unwrap(), None);
    index
        .insert(
            key.clone(),
            RowId {
                page_id: 100,
                slot_id: 2,
            },
            true,
        )
        .unwrap();
    assert_eq!(
        index.get_unique_one(&key).unwrap(),
        Some(RowId {
            page_id: 100,
            slot_id: 2
        })
    );
}

#[test]
fn root_routing_cache_matches_oracle_across_key_layouts() {
    for make in lookup_key_makers() {
        let keys: Vec<_> = (0..6000).map(|n| IndexKey::from(make(n))).collect();
        let (_dir, index) = index_fixture(keys.clone());
        for n in [0, 17, 999, 3000, 5999] {
            let key = &keys[n];
            let expected: Vec<_> = keys
                .iter()
                .enumerate()
                .filter_map(|(i, k)| {
                    (k == key).then_some(RowId {
                        page_id: (i / 32) as u32,
                        slot_id: (i % 32) as u16,
                    })
                })
                .collect();
            for _ in 0..2 {
                assert_eq!(index.get(key, false).unwrap(), expected);
            }
        }
        assert!(index.root_routing.get().unwrap().load().is_some());
        assert_eq!(index.buffer_pool.snapshot().pinned_pages, 0);
    }
}

#[test]
fn root_routing_cache_readers_race_republication_without_retaining_pins() {
    let (_dir, index) = index_fixture(
        (0..6000)
            .map(|n| IndexKey::from(vec![KeyPart::Int(n)]))
            .collect(),
    );
    let key = IndexKey::from(vec![KeyPart::Int(42)]);
    let expected = warm_root_routing(&index, &key);
    let page = index.page_key(index.root_page.load(Ordering::Acquire));
    let frame = index.buffer_pool.lookup_frame(&page).unwrap().unwrap();
    let index = Arc::new(index);
    let barrier = Arc::new(std::sync::Barrier::new(5));
    let workers: Vec<_> = (0..4)
        .map(|_| {
            let index = Arc::clone(&index);
            let barrier = Arc::clone(&barrier);
            let key = key.clone();
            std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..200 {
                    assert_eq!(index.get_unique_one(&key).unwrap(), expected);
                }
            })
        })
        .collect();
    barrier.wait();
    for _ in 0..200 {
        drop(frame.write_bytes().unwrap());
    }
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(index.buffer_pool.snapshot().pinned_pages, 0);
    index.buffer_pool.discard_file(&index.file_key).unwrap();
    assert!(index
        .root_routing
        .get()
        .unwrap()
        .load()
        .as_ref()
        .unwrap()
        .image
        .load()
        .is_none());
}

fn warm_root_routing(index: &IndexFile, key: &IndexKey) -> Option<RowId> {
    let mut result = None;
    for _ in 0..=ROOT_ROUTING_MIN_READS {
        result = index.get_unique_one(key).unwrap();
    }
    result
}

#[test]
fn root_routing_cache_avoids_copying_single_use_handles() {
    let (_dir, index) = index_fixture(
        (0..6000)
            .map(|n| IndexKey::from(vec![KeyPart::Int(n)]))
            .collect(),
    );
    assert!(index
        .get_unique_one(&IndexKey::from(vec![KeyPart::Int(20)]))
        .unwrap()
        .is_some());
    assert!(index.root_routing.get().is_none());
}

#[test]
fn root_routing_cache_rejects_another_files_same_page_number() {
    let (_a, index) = index_fixture(
        (0..6000)
            .map(|n| IndexKey::from(vec![KeyPart::Int(n)]))
            .collect(),
    );
    let (_b, other) = index_fixture(
        (10000..16000)
            .map(|n| IndexKey::from(vec![KeyPart::Int(n)]))
            .collect(),
    );
    let key = IndexKey::from(vec![KeyPart::Int(1234)]);
    let expected = warm_root_routing(&index, &key);
    let old = index.root_routing.get().unwrap().load_full().unwrap();
    let page = index.root_page.load(Ordering::Acquire);
    assert_eq!(other.root_page.load(Ordering::Acquire), page);
    let unrelated = other.pin_tree_page(page).unwrap();
    index.remember_root_routing(&unrelated).unwrap();
    assert!(Arc::ptr_eq(
        &old,
        &index.root_routing.get().unwrap().load_full().unwrap()
    ));
    assert_eq!(index.get_unique_one(&key).unwrap(), expected);
}
