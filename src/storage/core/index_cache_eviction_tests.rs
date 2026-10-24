fn index_eviction_test_dir() -> tempfile::TempDir {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    tempfile::tempdir_in("/tmp/mydb").unwrap()
}

// Keep all but one cached handle pinned to force a deterministic eviction.
fn index_eviction_test_cache(db: &Database) -> (u64, u64, IndexHandle) {
    create_concurrent_index_update_table(db, 32);
    db.create_table(
        "eviction_cold",
        vec![concurrent_index_test_column("id", true)],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let table_id = db.table("test.idx_concurrent").unwrap().id;
    let cold_id = db.table("test.eviction_cold").unwrap().id;
    let hot = db.index_handle(table_id, "k_1").unwrap();
    let victim = db.index_handle(table_id, "PRIMARY").unwrap();
    let old = {
        let mut cache = db.index_cache.write();
        let old = std::mem::take(&mut *cache);
        cache.insert((table_id, "primary".into()), victim);
        cache.insert((table_id, "k_1".into()), hot.clone());
        for n in 0..object_handle_cache_max() - 2 {
            cache.insert((u64::MAX - n as u64, "pinned".into()), hot.clone());
        }
        old
    };
    drop(old);
    (table_id, cold_id, hot)
}

#[test]
fn index_cache_eviction_flush_does_not_hold_global_lock() {
    let dir = index_eviction_test_dir();
    let db = Arc::new(open_relaxed_test_db(dir.path()));
    let (_, cold_id, _hot) = index_eviction_test_cache(&db);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let worker_db = db.clone();
    let worker = std::thread::spawn(move || {
        INDEX_FLUSH_TEST_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                entered_tx.send(()).unwrap();
                resume_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            }));
        });
        worker_db.index_handle(cold_id, "PRIMARY").unwrap()
    });
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let unlocked = db.index_cache.try_read().is_some();
    resume_tx.send(()).unwrap();
    worker.join().unwrap();
    assert!(
        unlocked,
        "index eviction holds the global cache lock during flush"
    );
}

// Run separately with MYDB_OBJECT_HANDLE_CACHE_MAX=32 and --ignored --nocapture.
// Real strict-durability inserts churn more index handles than fit in cache;
// a pinned unrelated index records latency while this happens.
#[test]
#[ignore]
fn index_cache_eviction_concurrent_benchmark() {
    assert_eq!(object_handle_cache_max(), 32);
    let dir = index_eviction_test_dir();
    let mut config = test_storage_config(DurabilityMode::Strict, true);
    config.buffer_pool_pages = 8192;
    let db = Arc::new(Database::open_with_config(dir.path(), config.clone()).unwrap());
    for n in 0..49 {
        db.create_table(
            &format!("evict_{n}"),
            vec![concurrent_index_test_column("id", true)],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
    }
    let hot_id = db.table("test.evict_48").unwrap().id;
    let hot = db.index_handle(hot_id, "PRIMARY").unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(5));
    let remaining = Arc::new(AtomicUsize::new(4));
    let start = std::time::Instant::now();
    let workers: Vec<_> = (0..4)
        .map(|worker| {
            let db = db.clone();
            let barrier = barrier.clone();
            let remaining = remaining.clone();
            std::thread::spawn(move || {
                barrier.wait();
                for round in 0..8 {
                    for n in 0..12 {
                        let txn = db.begin_txn().unwrap();
                        db.insert_rows(
                            txn,
                            &format!("test.evict_{}", worker * 12 + n),
                            (0..32)
                                .map(|row| vec![Value::Int(round * 32 + row)])
                                .collect(),
                        )
                        .unwrap();
                        db.commit_txn(txn.txn_id).unwrap();
                    }
                }
                remaining.fetch_sub(1, Ordering::Release);
            })
        })
        .collect();
    barrier.wait();
    let mut latency = Vec::new();
    while remaining.load(Ordering::Acquire) != 0 {
        let before = std::time::Instant::now();
        let current = db.index_handle(hot_id, "PRIMARY").unwrap();
        assert!(Arc::ptr_eq(&current, &hot));
        latency.push(before.elapsed().as_nanos() as u64);
        // Keep the observer bounded; don't benchmark a hot spin loop.
        std::thread::sleep(Duration::from_micros(100));
    }
    for worker in workers {
        worker.join().unwrap();
    }
    let seconds = start.elapsed().as_secs_f64();
    latency.sort_unstable();
    eprintln!("eviction_benchmark rows=12288 seconds={seconds:.3} rows_per_second={:.1} samples={} hit_p50_us={:.1} hit_p99_us={:.1} hit_max_us={:.1}",
        12288.0 / seconds, latency.len(), latency[latency.len()/2] as f64 / 1000.0,
        latency[latency.len()*99/100] as f64 / 1000.0, latency.last().unwrap().to_owned() as f64 / 1000.0);
    drop(hot);
    drop(db);
    let recovered = Database::open_with_config(dir.path(), config).unwrap();
    for n in 0..48 {
        let table = recovered.table(&format!("test.evict_{n}")).unwrap();
        let index = table.indexes.iter().find(|index| index.primary).unwrap();
        for id in 0..256 {
            assert_eq!(
                recovered
                    .lookup_index(&table, index, IndexKey::from(vec![KeyPart::Int(id)]))
                    .unwrap()
                    .len(),
                1
            );
        }
    }
}

#[test]
fn index_cache_eviction_reuses_handle_acquired_during_flush() {
    let dir = index_eviction_test_dir();
    let db = Arc::new(open_relaxed_test_db(dir.path()));
    let (table_id, cold_id, _hot) = index_eviction_test_cache(&db);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let worker_db = db.clone();
    let worker = std::thread::spawn(move || {
        INDEX_FLUSH_TEST_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                entered_tx.send(()).unwrap();
                resume_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            }));
        });
        worker_db.index_handle(cold_id, "PRIMARY").unwrap()
    });
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    // This acquisition must not reopen the file or wait on the global lock.
    let during = db.index_handle(table_id, "PRIMARY").unwrap();
    assert!(
        during.try_read().is_none(),
        "the old handle is still flushing"
    );
    resume_tx.send(()).unwrap();
    worker.join().unwrap();
    let after = db.index_handle(table_id, "PRIMARY").unwrap();
    assert!(Arc::ptr_eq(&during, &after));
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.idx_concurrent",
        (33..=2048)
            .map(|id| vec![Value::Int(id), Value::Int(id)])
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    drop(during);
    drop(after);
    // Now it can be safely retired and reopened, including its new pages.
    let victim = db.index_handle(table_id, "PRIMARY").unwrap();
    db.evict_idle_index_handle((table_id, "primary".into()), victim)
        .unwrap();
    assert!(!db
        .index_cache
        .read()
        .contains_key(&(table_id, "primary".into())));
    let reopened = db.index_handle(table_id, "PRIMARY").unwrap();
    for id in 1..=2048 {
        assert_eq!(
            reopened
                .read()
                .get(&IndexKey::from(vec![KeyPart::Int(id)]), true)
                .unwrap()
                .len(),
            1
        );
    }
}

#[test]
fn index_cache_eviction_failed_flush_keeps_binding() {
    let dir = index_eviction_test_dir();
    let db = open_relaxed_test_db(dir.path());
    let (table_id, _, _hot) = index_eviction_test_cache(&db);
    let victim = db.index_handle(table_id, "PRIMARY").unwrap();
    // Inject an actual open error into flush, without damaging the real file.
    let original = {
        let mut index = victim.write();
        index.file.lock().take();
        let original = index.path.clone();
        index.path = dir.path().to_path_buf(); // opening a directory for write fails
        index.dirty.store(true, Ordering::Release);
        original
    };
    assert!(db
        .evict_idle_index_handle((table_id, "primary".into()), victim)
        .is_err());
    assert!(db
        .index_cache
        .read()
        .contains_key(&(table_id, "primary".into())));
    let restored = db.index_handle(table_id, "PRIMARY").unwrap();
    {
        let mut index = restored.write();
        assert!(index.flush_on_drop);
        index.path = original;
    }
    db.evict_idle_index_handle((table_id, "primary".into()), restored)
        .unwrap();
    let reopened = db.index_handle(table_id, "PRIMARY").unwrap();
    assert_eq!(
        reopened
            .read()
            .get(&IndexKey::from(vec![KeyPart::Int(32)]), true)
            .unwrap()
            .len(),
        1
    );
}
