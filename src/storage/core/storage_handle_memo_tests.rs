#[test]
fn borrowed_page_keys_preserve_identity_on_hash_collision_and_reload() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let first = dir.path().join("first.heap");
    let second = dir.path().join("second.heap");
    std::fs::write(&first, vec![11; PAGE_SIZE]).unwrap();
    std::fs::write(&second, vec![22; PAGE_SIZE]).unwrap();
    let pool = BufferPool::new_with_options(
        4,
        test_log(dir.path()),
        DurabilityMode::Memory,
        Duration::from_secs(1),
        1,
        0,
    );
    let a = Arc::new(BufferPool::heap_file_key(first.clone(), 1));
    let mut b = BufferPool::heap_file_key(second, 2);
    b.hash = a.hash;
    let b = Arc::new(b);
    let a_key = PageKeyRef {
        file: &a,
        page_id: 0,
    };
    let b_key = PageKeyRef {
        file: &b,
        page_id: 0,
    };
    let hint = AtomicFrameHint::empty();
    let old = pool.pin_frame_with_hint(a_key, &hint).unwrap();
    assert_eq!(old.bytes.read().unwrap()[0], 11);
    old.unpin();
    let other = pool.pin_frame_with_hint(b_key, &hint).unwrap();
    assert_eq!(other.bytes.read().unwrap()[0], 22);
    assert!(!Arc::ptr_eq(&old, &other));
    other.unpin();
    let same_value = Arc::new(a.as_ref().clone());
    let hit = pool
        .pin_page_ref(PageKeyRef {
            file: &same_value,
            page_id: 0,
        })
        .unwrap();
    assert!(Arc::ptr_eq(&old, &hit.frame));
    drop(hit);
    pool.discard_file(&a).unwrap();
    std::fs::write(first, vec![33; PAGE_SIZE]).unwrap();
    let fresh = pool.pin_frame_with_hint(a_key, &hint).unwrap();
    assert!(!Arc::ptr_eq(&old, &fresh));
    assert_eq!(fresh.bytes.read().unwrap()[0], 33);
    fresh.unpin();
}

#[test]
fn borrowed_page_keys_release_pins_on_error_and_unwind() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let path = dir.path().join("borrowed-key.heap");
    std::fs::write(&path, vec![0; PAGE_SIZE * 2]).unwrap();
    let pool = BufferPool::new_with_options(
        1,
        test_log(dir.path()),
        DurabilityMode::Memory,
        Duration::from_millis(20),
        1,
        0,
    );
    let file = Arc::new(BufferPool::heap_file_key(path, 1));
    let key = PageKeyRef {
        file: &file,
        page_id: 0,
    };
    let pinned = pool.pin_page_borrowed_ref(key).unwrap();
    assert!(pool
        .pin_page_borrowed_ref(PageKeyRef {
            file: &file,
            page_id: 1
        })
        .is_err());
    drop(pinned);
    let failed: Result<Option<()>, QueryError> = pool.with_cached_heap_view_ref(key, None, |_| {
        Err(error(crate::error::catalog::ER_GET_ERRNO, "visitor failed"))
    });
    assert!(failed.is_err());
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _pinned = pool.pin_page_borrowed_ref(key).unwrap();
        panic!("borrowed key unwind");
    }));
    assert!(unwound.is_err());
    let next = pool
        .pin_page_borrowed_ref(PageKeyRef {
            file: &file,
            page_id: 1,
        })
        .unwrap();
    assert_eq!(next.frame.bytes.read().unwrap()[0], 0);
}

#[test]
fn atomic_frame_hint_concurrent_replacement_preserves_identity_and_pin_release() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let path = dir.path().join("concurrent-hint.heap");
    let mut bytes = vec![11; PAGE_SIZE];
    bytes.extend(vec![22; PAGE_SIZE]);
    std::fs::write(&path, bytes).unwrap();
    let pool = BufferPool::new_with_options(
        2,
        test_log(dir.path()),
        DurabilityMode::Memory,
        Duration::from_secs(1),
        1,
        0,
    );
    let file = Arc::new(BufferPool::heap_file_key(path, 1));
    let hint = Arc::new(AtomicFrameHint::empty());
    let workers = (0..4)
        .map(|worker| {
            let pool = Arc::clone(&pool);
            let file = Arc::clone(&file);
            let hint = Arc::clone(&hint);
            std::thread::spawn(move || {
                for _ in 0..200 {
                    let page_id = worker % 2;
                    let pinned = pool
                        .pin_page_with_hint(
                            PageKey {
                                file: Arc::clone(&file),
                                page_id,
                            },
                            &hint,
                        )
                        .unwrap();
                    assert_eq!(
                        pinned.frame.bytes.read().unwrap()[0],
                        if page_id == 0 { 11 } else { 22 }
                    );
                }
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().unwrap();
    }
    for page_id in [0, 1] {
        assert_eq!(
            pool.lookup_frame(&PageKey {
                file: Arc::clone(&file),
                page_id
            })
            .unwrap()
            .unwrap()
            .pin_count
            .load(Ordering::Acquire),
            0
        );
    }
    pool.discard_file(&file).unwrap();
    assert!(hint
        .load()
        .as_ref()
        .and_then(|hint| hint.frame.upgrade())
        .is_none());
}

#[test]
fn storage_handle_memos_do_not_keep_database_files_alive() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let db = open_relaxed_test_db(dir.path());
    create_checkpoint_test_table(&db);
    let table = db.table("test.t").unwrap();
    let heap = db.heap_handle(table.id).unwrap();
    let index = db.index_handle(table.id, "PRIMARY").unwrap();
    let heap_weak = Arc::downgrade(&heap);
    let index_weak = Arc::downgrade(&index);
    drop((heap, index));
    drop(db);
    assert!(heap_weak.upgrade().is_none());
    assert!(index_weak.upgrade().is_none());
}

#[test]
fn storage_handle_epoch_sweeps_preserve_live_tokens_and_bound_retired_history() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let db = open_relaxed_test_db(dir.path());
    HEAP_HANDLE_MEMOS.with(|cache| *cache.borrow_mut() = StorageHandleMemos::new());
    INDEX_HANDLE_MEMOS.with(|cache| *cache.borrow_mut() = StorageHandleMemos::new());
    let live = (1000..1256)
        .map(|id| db.storage_handle_open_epoch(id))
        .collect::<Vec<_>>();
    for id in 2000..4048 {
        drop(db.storage_handle_open_epoch(id));
    }
    for (id, token) in (1000..1256).zip(&live) {
        assert!(Arc::ptr_eq(token, &db.storage_handle_open_epoch(id)));
    }
    assert!(db.storage_handle_open_epochs.lock().unwrap().epochs.len() <= live.len() + 128);
    drop(live);
    for id in 5000..5256 {
        drop(db.storage_handle_open_epoch(id));
    }
    assert!(db.storage_handle_open_epochs.lock().unwrap().epochs.len() <= 128);
}

#[test]
fn storage_handle_memos_isolate_database_instances_and_truncate() {
    let a_dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let b_dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let a = open_relaxed_test_db(a_dir.path());
    let b = open_relaxed_test_db(b_dir.path());
    for (db, value) in [(&a, 11), (&b, 22)] {
        create_checkpoint_test_table(db);
        insert_checkpoint_test_row(db, value);
        let table = db.table("test.t").unwrap();
        db.heap_handle(table.id).unwrap();
        db.index_handle(table.id, "PRIMARY").unwrap();
    }
    for (db, value) in [(&a, 11), (&b, 22), (&a, 11)] {
        let rows = db
            .scan_rows(
                "test.t",
                db.read_only_statement_view(IsolationLevel::ReadCommitted, None),
            )
            .unwrap();
        assert_eq!(rows[0].values, vec![Value::Int(value)]);
    }
    let old = a.table("test.t").unwrap();
    let old_heap = a.heap_handle(old.id).unwrap();
    let old_index = a.index_handle(old.id, "PRIMARY").unwrap();
    a.truncate_table("test.t").unwrap();
    insert_checkpoint_test_row(&a, 33);
    let table = a.table("test.t").unwrap();
    assert!(!Arc::ptr_eq(&old_heap, &a.heap_handle(table.id).unwrap()));
    assert!(!Arc::ptr_eq(
        &old_index,
        &a.index_handle(table.id, "PRIMARY").unwrap()
    ));
    let rows = a
        .scan_rows(
            "test.t",
            a.read_only_statement_view(IsolationLevel::ReadCommitted, None),
        )
        .unwrap();
    assert_eq!(rows[0].values, vec![Value::Int(33)]);
}

#[test]
fn storage_handle_memos_observe_publication_during_weak_upgrade() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let db = Arc::new(open_relaxed_test_db(dir.path()));
    create_checkpoint_test_table(&db);
    let id = db.table("test.t").unwrap().id;
    // Bootstrap can fill the tiny cache. Make admission deterministic so the
    // hook exercises an upgrade rather than the authoritative fallback.
    INDEX_HANDLE_MEMOS.with(|cache| *cache.borrow_mut() = StorageHandleMemos::new());
    let old = db.index_handle(id, "PRIMARY").unwrap();
    assert!(INDEX_HANDLE_MEMOS.with(|cache| {
        cache
            .borrow()
            .get(db.visibility_cache_id, id, "primary")
            .is_some()
    }));
    let new = IndexFile::open_with_durability(
        dir.path().join("replacement.idx"),
        db.log.clone(),
        DurabilityMode::Memory,
        db.buffer_pool.clone(),
    )
    .unwrap();
    let row = RowId {
        page_id: 7,
        slot_id: 3,
    };
    new.bulk_load_sorted(
        7,
        vec![IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(777)]),
            row_id: row,
        }],
    )
    .unwrap();
    let new = Arc::new(parking_lot::RwLock::new(new));
    let owner = db.clone();
    let published = new.clone();
    STORAGE_HANDLE_MEMO_UPGRADE_TEST_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            owner.publish_index_handle(id, "PRIMARY", published)
        }));
    });
    let selected = db.index_handle(id, "primary").unwrap();
    assert!(!Arc::ptr_eq(&old, &selected));
    assert!(Arc::ptr_eq(&new, &selected));
    assert_eq!(
        selected
            .read()
            .get_unique_one(&IndexKey::from(vec![KeyPart::Int(777)]))
            .unwrap(),
        Some(row)
    );
}

#[test]
fn storage_handle_memos_follow_index_rename_and_name_reuse() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let db = open_relaxed_test_db(dir.path());
    create_concurrent_index_update_table(&db, 32);
    let table = db.table("test.idx_concurrent").unwrap();
    let old = db.index_handle(table.id, "k_1").unwrap();
    db.rename_index("test.idx_concurrent", "k_1", "renamed")
        .unwrap();
    assert!(Arc::ptr_eq(
        &old,
        &db.index_handle(table.id, "RENAMED").unwrap()
    ));
    let mut replacement = table
        .indexes
        .iter()
        .find(|index| index.name == "k_1")
        .unwrap()
        .clone();
    replacement.index_id = 0;
    replacement.columns = vec!["id".into()];
    db.add_index("test.idx_concurrent", replacement).unwrap();
    let current = db.index_handle(table.id, "k_1").unwrap();
    assert!(!Arc::ptr_eq(&old, &current));
    assert_eq!(current.read().entries().unwrap().len(), 32);
}

#[test]
fn storage_handle_handoff_drains_every_shard_before_publication() {
    use std::sync::mpsc;
    let gate = Arc::new(StorageHandleHandoff::default());
    let held = gate.try_read_slot(31).unwrap();
    let (acquired, wait) = mpsc::channel();
    let (release, finish) = mpsc::channel();
    let writer_gate = Arc::clone(&gate);
    let writer = std::thread::spawn(move || {
        let _all = writer_gate.write();
        acquired.send(()).unwrap();
        finish.recv().unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while gate.active.load(Ordering::Acquire) & StorageHandleHandoff::CLOSED == 0 {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert!(wait.try_recv().is_err());
    drop(held);
    wait.recv_timeout(Duration::from_secs(5)).unwrap();
    for slot in 0..32 {
        assert!(gate.try_read_slot(slot).is_none());
    }
    release.send(()).unwrap();
    writer.join().unwrap();
    assert!(gate.try_read().is_some());
}

#[test]
fn storage_handle_handoff_rejects_registration_across_retirement() {
    let gate = StorageHandleHandoff::default();
    assert!(gate.register_slot(31));
    drop(gate.write());
    assert!(gate.read_registered_slot(31).is_none());
    assert!(gate.try_read_slot(31).is_some());
}

#[test]
fn storage_handle_handoff_only_allocates_shards_for_reused_epochs() {
    let gate = StorageHandleHandoff::default();
    for _ in 0..64 {
        assert!(gate.try_read().is_some());
    }
    assert!(gate.extra.get().is_none());
    assert!(gate.try_read().is_some());
    assert!(gate.extra.get().is_some());
    drop(gate.write());
    assert_eq!(gate.reads.load(Ordering::Relaxed), 0);
}
