fn insert_lock_test_dir() -> tempfile::TempDir {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    tempfile::tempdir_in("/tmp/mydb").unwrap()
}

#[test]
#[ignore]
fn insert_lock_concurrent_benchmark() {
    for kind in ["heap", "btree"] {
        let dir = insert_lock_test_dir();
        let mut config = test_storage_config(DurabilityMode::Strict, true);
        config.buffer_pool_pages = 512;
        let db = Arc::new(Database::open_with_config(dir.path(), config.clone()).unwrap());
        let mut payload = concurrent_index_test_column("payload", kind == "btree");
        payload.data_type = DataType::VarChar(Some(512));
        db.create_table(
            "lock_bench",
            vec![concurrent_index_test_column("id", false), payload],
            if kind == "btree" {
                vec!["payload".into()]
            } else {
                vec![]
            },
            vec![],
        )
        .unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(4));
        let start = Instant::now();
        let threads: Vec<_> = (0..4i64)
            .map(|worker| {
                let db = db.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    for batch in 0..64 {
                        let rows = (0..512)
                            .map(|row| {
                                let id = worker + 4 * (batch * 512 + row);
                                vec![
                                    Value::Int(id),
                                    Value::String(format!(
                                        "{:08}-{}",
                                        (id * 7919) % 131072,
                                        "x".repeat(240)
                                    )),
                                ]
                            })
                            .collect();
                        let txn = db.begin_txn().unwrap();
                        db.insert_rows(txn, "test.lock_bench", rows).unwrap();
                        db.commit_txn(txn.txn_id).unwrap();
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        let elapsed = start.elapsed().as_secs_f64();
        eprintln!("insert_lock_benchmark kind={kind} rows=131072 seconds={elapsed:.3} rows_per_second={:.1}",131072.0/elapsed);
        db.check_consistency().unwrap();
        drop(db);
        let db = Database::open_with_config(dir.path(), config).unwrap();
        let txn = db.begin_txn().unwrap();
        let rows = db.scan_rows("test.lock_bench", txn).unwrap();
        assert_eq!(rows.len(), 131072);
        let ids: std::collections::HashSet<_> = rows
            .iter()
            .map(|r| match r.values[0] {
                Value::Int(id) => id,
                _ => panic!("integer id"),
            })
            .collect();
        assert_eq!(ids.len(), 131072);
        db.commit_txn(txn.txn_id).unwrap();
        db.check_consistency().unwrap();
    }
}

#[test]
fn heap_private_batch_publishes_without_extension_lock_and_protects_tail() {
    let dir = insert_lock_test_dir();
    let db = Arc::new(open_relaxed_test_db(dir.path()));
    let mut payload = concurrent_index_test_column("payload", false);
    payload.data_type = DataType::VarChar(Some(512));
    db.create_table(
        "private_batch",
        vec![concurrent_index_test_column("id", false), payload],
        vec![],
        vec![],
    )
    .unwrap();
    let table_id = db.table("test.private_batch").unwrap().id;
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let worker_db = db.clone();
    let worker = std::thread::spawn(move || {
        let txn = worker_db.begin_txn().unwrap();
        HEAP_BATCH_PUBLISH_TEST_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                entered_tx.send(()).unwrap();
                resume_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            }))
        });
        worker_db
            .insert_rows(
                txn,
                "test.private_batch",
                (0..512)
                    .map(|id| vec![Value::Int(id), Value::String("x".repeat(300))])
                    .collect(),
            )
            .unwrap();
        worker_db.commit_txn(txn.txn_id).unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let handle = db.heap_handle(table_id).unwrap();
    {
        let heap = handle.read();
        assert!(heap.extend_mutex.try_lock().is_ok());
        assert!(heap
            .page_latch(heap.page_count().unwrap() - 1)
            .try_lock()
            .is_err());
    }
    // Another large batch must finish while the first one is paused before
    // WAL/page publication. It owns disjoint pages, not the unfinished tail.
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.private_batch",
        (512..1024)
            .map(|id| vec![Value::Int(id), Value::String("y".repeat(300))])
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    resume_tx.send(()).unwrap();
    worker.join().unwrap();
    let txn = db.begin_txn().unwrap();
    assert_eq!(db.scan_rows("test.private_batch", txn).unwrap().len(), 1024);
    db.commit_txn(txn.txn_id).unwrap();
}

#[test]
fn btree_prepared_split_keeps_page_eviction_outside_structure_write_lock() {
    let dir = insert_lock_test_dir();
    let log = test_log(dir.path());
    let pool = BufferPool::new(
        64,
        log.clone(),
        DurabilityMode::Relaxed,
        Duration::from_secs(2),
    );
    let index = IndexFile::open_with_durability(
        t_index_path(dir.path(), "split", 1, "PRIMARY", "idx"),
        log.clone(),
        DurabilityMode::Relaxed,
        pool.clone(),
    )
    .unwrap();
    let entry = |id: u32| IndexEntry {
        key: IndexKey::from(vec![KeyPart::String(format!(
            "{id:08}{}",
            "x".repeat(1100)
        ))]),
        row_id: RowId {
            page_id: id,
            slot_id: 0,
        },
    };
    for id in 0..14 {
        let e = entry(id);
        index.insert_with_txn(0, e.key, e.row_id).unwrap();
    }
    let pressure = Arc::new(BufferPool::heap_file_key(
        dir.path().join("pressure.heap"),
        999,
    ));
    let dirty_lsn = log.lock().last_lsn();
    for id in 0..64 {
        pool.store_page(
            PageKey {
                file: pressure.clone(),
                page_id: id,
            },
            vec![0; PAGE_SIZE],
            dirty_lsn,
        )
        .unwrap();
    }
    let incoming = entry(14);
    let before = pool.evictions.load(Ordering::Relaxed);
    let prepared = index
        .prepare_insert_structure(&incoming)
        .unwrap()
        .expect("pool fits preflight");
    assert!(pool.evictions.load(Ordering::Relaxed) > before);
    assert!(index.structure_lock.try_write().is_some());
    let before = pool.evictions.load(Ordering::Relaxed);
    let pages = index.next_page_id();
    {
        let _structure = index.structure_lock.write();
        index
            .insert_internal_unlocked(0, incoming.clone(), false)
            .unwrap();
        assert_eq!(pool.evictions.load(Ordering::Relaxed), before);
    }
    assert!(index.next_page_id() > pages, "fixture must split a leaf");
    drop(prepared);
    assert_eq!(
        index.get(&incoming.key, true).unwrap(),
        vec![incoming.row_id]
    );
    index.flush().unwrap();
    IndexFile::validate(index.path.clone()).unwrap();
}

#[test]
fn heap_private_batches_mixed_inserts_rollback_and_crash_recover() {
    let dir = insert_lock_test_dir();
    let mut config = test_storage_config(DurabilityMode::Strict, true);
    config.buffer_pool_pages = 256;
    let db = Arc::new(Database::open_with_config(dir.path(), config.clone()).unwrap());
    let mut payload = concurrent_index_test_column("payload", false);
    payload.data_type = DataType::VarChar(Some(512));
    db.create_table(
        "mixed_private",
        vec![concurrent_index_test_column("id", true), payload],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(4));
    let threads: Vec<_> = (0..4i64)
        .map(|worker| {
            let db = db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                if worker == 3 {
                    for id in 30000..30064 {
                        let txn = db.begin_txn().unwrap();
                        db.insert_rows(
                            txn,
                            "test.mixed_private",
                            vec![vec![Value::Int(id), Value::String("small".into())]],
                        )
                        .unwrap();
                        db.commit_txn(txn.txn_id).unwrap();
                    }
                } else {
                    for batch in 0..4 {
                        let txn = db.begin_txn().unwrap();
                        db.insert_rows(
                            txn,
                            "test.mixed_private",
                            (0..512)
                                .map(|n| {
                                    vec![
                                        Value::Int(worker * 10000 + batch * 512 + n),
                                        Value::String("z".repeat(300)),
                                    ]
                                })
                                .collect(),
                        )
                        .unwrap();
                        if batch % 2 == 0 {
                            db.commit_txn(txn.txn_id).unwrap();
                        } else {
                            db.rollback_txn(txn.txn_id).unwrap();
                        }
                    }
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    db.check_consistency().unwrap();
    let db = Arc::try_unwrap(db).ok().unwrap();
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db); // Recover from WAL without an orderly checkpoint.
    let recovered = Database::open_with_config(dir.path(), config).unwrap();
    let txn = recovered.begin_txn().unwrap();
    let rows = recovered.scan_rows("test.mixed_private", txn).unwrap();
    let actual: std::collections::HashSet<_> = rows
        .iter()
        .map(|row| match row.values[0] {
            Value::Int(id) => id,
            _ => panic!("id"),
        })
        .collect();
    let expected: std::collections::HashSet<_> = (0..3)
        .flat_map(|w| {
            [0, 2]
                .into_iter()
                .flat_map(move |b| (0..512).map(move |n| w * 10000 + b * 512 + n))
        })
        .chain(30000..30064)
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(rows.len(), expected.len());
    recovered.commit_txn(txn.txn_id).unwrap();
    recovered.check_consistency().unwrap();
}

#[test]
fn heap_private_range_overflow_fails_before_file_extension() {
    let dir = insert_lock_test_dir();
    let db = open_memory_test_db(dir.path());
    create_concurrent_index_update_table(&db, 0);
    let table = db.table("test.idx_concurrent").unwrap();
    let handle = db.heap_handle(table.id).unwrap();
    let heap = handle.read();
    let original_count = heap.page_count.load(Ordering::Acquire);
    let original_len = heap.file.metadata().unwrap().len();
    heap.page_count
        .store(u64::from(u32::MAX), Ordering::Release);
    let result = heap.insert_mvcc_batch_pre_encoded(1, vec![vec![0; 8000]; 8]);
    heap.page_count.store(original_count, Ordering::Release);
    assert!(result
        .unwrap_err()
        .is(crate::error::catalog::ER_TOO_BIG_ROWSIZE));
    assert_eq!(heap.file.metadata().unwrap().len(), original_len);
}

#[test]
fn btree_prepared_split_revalidates_leaf_after_concurrent_delete() {
    let dir = insert_lock_test_dir();
    let log = test_log(dir.path());
    let pool = BufferPool::new(
        128,
        log.clone(),
        DurabilityMode::Relaxed,
        Duration::from_secs(2),
    );
    let index = Arc::new(
        IndexFile::open_with_durability(
            t_index_path(dir.path(), "split", 1, "PRIMARY", "idx"),
            log,
            DurabilityMode::Relaxed,
            pool,
        )
        .unwrap(),
    );
    let entry = |id: u32| IndexEntry {
        key: IndexKey::from(vec![KeyPart::String(format!(
            "{id:08}{}",
            "x".repeat(1100)
        ))]),
        row_id: RowId {
            page_id: id,
            slot_id: 0,
        },
    };
    for id in 0..14 {
        let e = entry(id);
        index.insert_with_txn(0, e.key, e.row_id).unwrap();
    }
    let removed = entry(0);
    let peer = index.clone();
    let ran = Arc::new(AtomicBool::new(false));
    let observed = ran.clone();
    INDEX_STRUCTURE_PREPARED_TEST_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            let e = entry(0);
            peer.delete_with_txn(0, e.key, e.row_id).unwrap();
            observed.store(true, Ordering::Release);
        }))
    });
    let incoming = entry(14);
    index
        .insert_with_txn(0, incoming.key.clone(), incoming.row_id)
        .unwrap();
    assert!(
        ran.load(Ordering::Acquire),
        "fixture must enter structural preparation"
    );
    assert!(
        index.get(&removed.key, true).unwrap().is_empty(),
        "stale preparation must not resurrect a deleted row"
    );
    assert_eq!(
        index.get(&incoming.key, true).unwrap(),
        vec![incoming.row_id]
    );
    index.flush().unwrap();
    IndexFile::validate(index.path.clone()).unwrap();
}

// Isolate the changed storage paths from SQL/constraints/zone-map overhead.
// MYDB_TEST_SERIAL_INSERT_LOCKS selects the old locking paths in test builds.
#[test]
#[ignore]
fn insert_lock_storage_benchmark() {
    let dir = insert_lock_test_dir();
    let mut config = test_storage_config(DurabilityMode::Strict, true);
    config.buffer_pool_pages = 256;
    let db = Arc::new(Database::open_with_config(dir.path(), config).unwrap());
    let mut payload = concurrent_index_test_column("payload", false);
    payload.data_type = DataType::VarChar(Some(512));
    db.create_table("storage_bench", vec![payload], vec![], vec![])
        .unwrap();
    let table = db.table("test.storage_bench").unwrap();
    let heap = db.heap_handle(table.id).unwrap();
    let input = Arc::new(vec![vec![Value::String("h".repeat(384))]; 512]);
    let barrier = Arc::new(std::sync::Barrier::new(5));
    let workers: Vec<_> = (0..4)
        .map(|worker| {
            let heap = heap.clone();
            let input = input.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let headers = vec![RowHeader::visible(worker + 1); 512];
                barrier.wait();
                for _ in 0..256 {
                    heap.read()
                        .insert_mvcc_batch(worker + 1, &headers, &input, &[])
                        .unwrap();
                }
            })
        })
        .collect();
    let start = Instant::now();
    barrier.wait();
    for w in workers {
        w.join().unwrap();
    }
    let elapsed = start.elapsed().as_secs_f64();
    eprintln!(
        "storage_lock_bench kind=heap rows=524288 seconds={elapsed:.4} rows_per_second={:.1}",
        524288.0 / elapsed
    );
    assert_eq!(heap.read().scan_mvcc().unwrap().len(), 524288);
    drop(heap);
    drop(db);

    let dir = insert_lock_test_dir();
    let log = test_log(dir.path());
    let pool = BufferPool::new(
        256,
        log.clone(),
        DurabilityMode::Strict,
        Duration::from_secs(5),
    );
    let index = Arc::new(
        IndexFile::open_with_durability(
            t_index_path(dir.path(), "bench", 1, "PRIMARY", "idx"),
            log,
            DurabilityMode::Strict,
            pool,
        )
        .unwrap(),
    );
    let sentinel = IndexKey::from(vec![KeyPart::String("-sentinel".into())]);
    index
        .insert_with_txn(
            0,
            sentinel.clone(),
            RowId {
                page_id: 0,
                slot_id: 0,
            },
        )
        .unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(5));
    let remaining = Arc::new(AtomicUsize::new(4));
    let workers: Vec<_> = (0..4u32)
        .map(|worker| {
            let index = index.clone();
            let barrier = barrier.clone();
            let remaining = remaining.clone();
            std::thread::spawn(move || {
                let batches: Vec<Vec<_>> = (0..32)
                    .map(|batch| {
                        (0..256)
                            .map(|n| {
                                let id = worker + 4 * (batch * 256 + n);
                                (
                                    IndexKey::from(vec![KeyPart::String(format!(
                                        "{:08}{}",
                                        (id * 7919) % 32768,
                                        "x".repeat(240)
                                    ))]),
                                    RowId {
                                        page_id: id + 1,
                                        slot_id: 0,
                                    },
                                )
                            })
                            .collect()
                    })
                    .collect();
                barrier.wait();
                for batch in batches {
                    index.insert_batch_deferred(0, batch, false).unwrap();
                }
                remaining.fetch_sub(1, Ordering::Release);
            })
        })
        .collect();
    let start = Instant::now();
    barrier.wait();
    let mut latency = Vec::new();
    while remaining.load(Ordering::Acquire) != 0 {
        let before = Instant::now();
        assert_eq!(index.get(&sentinel, true).unwrap().len(), 1);
        latency.push(before.elapsed().as_nanos() as u64);
        std::thread::sleep(Duration::from_micros(100));
    }
    for w in workers {
        w.join().unwrap();
    }
    let elapsed = start.elapsed().as_secs_f64();
    latency.sort_unstable();
    eprintln!("storage_lock_bench kind=btree rows=32768 seconds={elapsed:.4} rows_per_second={:.1} read_p99_us={:.1}",32768.0/elapsed,latency[latency.len()*99/100] as f64/1000.0);
    assert_eq!(index.entries().unwrap().len(), 32769);
    index.flush().unwrap();
    IndexFile::validate(index.path.clone()).unwrap();
}
