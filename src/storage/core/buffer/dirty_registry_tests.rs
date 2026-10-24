//! Deterministic interleavings at the clean-observation/registry-removal
//! boundary. Hooks execute outside production latches, in this test thread
//! only, so the normal writer or invalidation path can run to completion.

use super::*;
thread_local! {
    static HOOK: std::cell::RefCell<Option<Box<dyn FnOnce(&PageKey)>>> =
        std::cell::RefCell::new(None);
}
pub(super) fn fire(key: &PageKey) {
    let hook = HOOK.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook(key);
    }
}

#[test]
fn redirty_during_flush_cleanup_is_persisted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("redirty.heap");
    File::create(&path).unwrap();
    let log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(dir.path()).unwrap(),
    ));
    let pool =
        BufferPool::new_with_options(8, log, DurabilityMode::Strict, Duration::from_secs(1), 1, 0);
    let key = PageKey {
        file: Arc::new(BufferPool::heap_file_key(path.clone(), 1)),
        page_id: 0,
    };
    // Keep the page owned by this controlled interleaving. The ordinary
    // background cleaner remains configured, but cannot consume the redirty
    // before this test observes its registry entry.
    let pinned = pool.pin_page(key.clone()).unwrap();
    pool.store_page(key.clone(), vec![1; PAGE_SIZE], 0).unwrap();
    let frame = pool.lookup_frame(&key).unwrap().unwrap();
    let writer = Arc::clone(&pool);
    HOOK.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move |key| {
            // Schedule a writer after the flusher releases the byte latch,
            // before it removes the previous dirty-set registration.
            writer
                .store_page(key.clone(), vec![2; PAGE_SIZE], 0)
                .unwrap();
        }))
    });
    assert!(pool.flush_frame(&frame).unwrap());
    assert!(frame.dirty.load(Ordering::Acquire));
    assert_eq!(pool.checkpoint_dirty_snapshot().unwrap().1, 1);
    drop(pinned);
    pool.flush_all().unwrap();
    let disk = std::fs::read(&path).unwrap();
    assert_eq!(pool.dirty_pages.load(Ordering::Relaxed), 0);
    assert_eq!(disk[0], 2, "checkpoint must persist the newly dirtied page");
}
#[test]
fn redirty_heap_preserves_unique_rows_after_checkpoint() {
    check_sql_restart(false);
}

#[test]
fn redirty_primary_preserves_prepared_delete_insert_after_checkpoint() {
    check_sql_restart(true);
}

fn check_sql_restart(index_page: bool) {
    use crate::engine::{QueryEngine, StorageQueryEngine};
    let dir = tempfile::tempdir().unwrap();
    let restart_dir = tempfile::tempdir().unwrap();
    fn copy_disk_tree(from: &Path, to: &Path) {
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let target = to.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                std::fs::create_dir_all(&target).unwrap();
                copy_disk_tree(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }
    let config = StorageConfig {
        durability: DurabilityMode::Strict,
        undo_retention: Duration::from_secs(3600),
        buffer_pool_pages: 2048,
        buffer_pool_io_threads: 1,
        startup_validate_indexes: false,
        startup_validation: StartupValidationMode::Off,
        ..StorageConfig::from_env()
    };
    {
        let engine = Arc::new(
            StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                config.clone(),
            )
            .unwrap(),
        );
        engine.db_handle().buffer_pool.shutdown_flusher();
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(200))")
            .unwrap();
        let values = (1..=512)
            .map(|i| format!("({i}, '{}')", "x".repeat(120)))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO t VALUES {values}"))
            .unwrap();
        let db = engine.db_handle();
        db.checkpoint().unwrap();
        let table = db.table("test.t").unwrap();
        let rows = db
            .scan_rows(
                "test.t",
                db.read_only_statement_view(IsolationLevel::ReadCommitted, None),
            )
            .unwrap();
        let row = rows.iter().find(|r| r.values[0] == Value::Int(1)).unwrap();
        let key = if index_page {
            db.with_index_read(table.id, "PRIMARY", |index| {
                let leaf =
                    index.find_leaf_for_key_lower_bound(&IndexKey::from(vec![KeyPart::Int(1)]))?;
                Ok(index.page_key(leaf))
            })
            .unwrap()
        } else {
            db.cached_heap_handle(table.id)
                .unwrap()
                .read()
                .page_key(u64::from(row.row_id.page_id))
        };
        engine
            .execute("UPDATE t SET v='baseline' WHERE id=1")
            .unwrap();
        let frame = db.buffer_pool.lookup_frame(&key).unwrap().unwrap();
        if index_page {
            let bytes = frame.bytes.read().unwrap().clone();
            db.buffer_pool.store_page(key.clone(), bytes, 0).unwrap();
        }
        let writer = Arc::clone(&engine);
        HOOK.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move |_| {
                let mut session = crate::engine::SessionContext::default();
                writer.execute_with_session(&mut session, "BEGIN").unwrap();
                writer
                    .execute_with_session(&mut session, "DELETE FROM t WHERE id=1")
                    .unwrap();
                writer
                    .execute_with_session(&mut session, "INSERT INTO t VALUES (1, 'replacement')")
                    .unwrap();
                writer.execute_with_session(&mut session, "COMMIT").unwrap();
            }))
        });
        assert!(db.buffer_pool.flush_frame(&frame).unwrap());
        let rows = db
            .scan_rows(
                "test.t",
                db.read_only_statement_view(IsolationLevel::ReadCommitted, None),
            )
            .unwrap();
        assert_eq!(rows.len(), 512);
        assert!(frame.dirty.load(Ordering::Acquire));
        assert!(db
            .buffer_pool
            .collect_dirty_frames()
            .unwrap()
            .iter()
            .any(|f| f.key == key));
        db.checkpoint().unwrap();
        // Capture exactly the checkpoint's durable files before any handle
        // destructor can opportunistically flush an unregistered page.
        copy_disk_tree(dir.path(), restart_dir.path());
    }
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        restart_dir.path(),
        config,
    )
    .unwrap();
    let db = engine.db_handle();
    let rows = db
        .scan_rows(
            "test.t",
            db.read_only_statement_view(IsolationLevel::ReadCommitted, None),
        )
        .unwrap();
    assert_eq!(
        rows.len(),
        512,
        "checkpoint must preserve exactly one row per primary key"
    );
    let replacements = rows
        .iter()
        .filter(|r| r.values[0] == Value::Int(1))
        .collect::<Vec<_>>();
    assert_eq!(replacements.len(), 1);
    assert_eq!(replacements[0].values[1].to_display_string(), "replacement");
    let mut session = crate::engine::SessionContext::default();
    let delete_sql = "DELETE FROM t WHERE id=?";
    let insert_sql = "INSERT INTO t VALUES (?, ?)";
    let delete = engine.prepare_cached(&mut session, delete_sql, 1).unwrap();
    let insert = engine.prepare_cached(&mut session, insert_sql, 2).unwrap();
    let mut delete_state = crate::engine::api::PreparedDmlState::default();
    let mut insert_state = crate::engine::api::PreparedDmlState::default();
    for _ in 0..2 {
        engine.execute_with_session(&mut session, "BEGIN").unwrap();
        let deleted = engine
            .execute_prepared_dml(
                &mut session,
                &mut delete_state,
                &delete,
                delete_sql,
                &[Value::Int(1)],
            )
            .unwrap()
            .unwrap();
        let inserted = engine
            .execute_prepared_dml(
                &mut session,
                &mut insert_state,
                &insert,
                insert_sql,
                &[Value::Int(1), Value::String("next".into())],
            )
            .unwrap();
        assert!(matches!(
            deleted,
            crate::engine::QueryResponse::OkAffected { affected_rows: 1 }
        ));
        assert!(matches!(
            inserted,
            Ok(crate::engine::QueryResponse::OkAffected { affected_rows: 1 })
        ));
        engine
            .execute_with_session(
                &mut session,
                if inserted.is_ok() {
                    "COMMIT"
                } else {
                    "ROLLBACK"
                },
            )
            .unwrap();
        assert!(
            inserted.is_ok(),
            "prepared DELETE + INSERT must not hit 1062 after clean restart"
        );
    }
    assert_eq!(
        rows.len(),
        512,
        "clean restart must not resurrect the deleted row"
    );
}

#[test]
fn redirty_during_contiguous_flush_cleanup_is_persisted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("batch.heap");
    File::create(&path).unwrap();
    let pool = BufferPool::new_with_options(
        8,
        Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        )),
        DurabilityMode::Strict,
        Duration::from_secs(1),
        1,
        0,
    );
    // This test controls the flush/redirty interleaving with HOOK. A real
    // cleaner can otherwise persist the second image before the assertions.
    pool.shutdown_flusher();
    let file = Arc::new(BufferPool::heap_file_key(path.clone(), 1));
    let mut frames = Vec::new();
    for page_id in 0..2 {
        let key = PageKey {
            file: Arc::clone(&file),
            page_id,
        };
        pool.store_page(key.clone(), vec![1; PAGE_SIZE], 0).unwrap();
        frames.push(pool.lookup_frame(&key).unwrap().unwrap());
    }
    let writer = Arc::clone(&pool);
    HOOK.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move |key| {
            writer
                .store_page(key.clone(), vec![2; PAGE_SIZE], 0)
                .unwrap();
        }))
    });
    assert_eq!(
        pool.flush_contiguous_frame_run(&frames, 0).unwrap(),
        Some(2)
    );
    assert_eq!(pool.checkpoint_dirty_snapshot().unwrap().1, 1);
    assert_eq!(pool.dirty_pages.load(Ordering::Relaxed), 1);
    pool.flush_all().unwrap();
    let disk = std::fs::read(path).unwrap();
    assert!(disk[..PAGE_SIZE].iter().all(|&byte| byte == 2));
    assert!(disk[PAGE_SIZE..].iter().all(|&byte| byte == 1));
    assert_eq!(pool.dirty_pages.load(Ordering::Relaxed), 0);
}

#[test]
fn stale_dirty_collection_preserves_redirtied_page() {
    check_stale_collection(false);
}

#[test]
fn stale_dirty_collection_preserves_replacement_frame() {
    check_stale_collection(true);
}

fn check_stale_collection(replace: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stale.heap");
    File::create(&path).unwrap();
    let pool = BufferPool::new_with_options(
        8,
        Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        )),
        DurabilityMode::Strict,
        Duration::from_secs(1),
        1,
        0,
    );
    let key = PageKey {
        file: Arc::new(BufferPool::heap_file_key(path.clone(), 1)),
        page_id: 0,
    };
    pool.store_page(key.clone(), vec![1; PAGE_SIZE], 0).unwrap();
    pool.flush_all().unwrap();
    // The collector may observe the flusher's clean bit while its old
    // dirty-set registration is still present.
    pool.note_dirty(&key, 0);
    let retained_pin = std::rc::Rc::new(std::cell::RefCell::new(None));
    let writer_pin = std::rc::Rc::clone(&retained_pin);
    let writer = Arc::clone(&pool);
    HOOK.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move |key| {
            if replace {
                writer.discard_page(key).unwrap();
            }
            *writer_pin.borrow_mut() = Some(writer.pin_page(key.clone()).unwrap());
            writer
                .store_page(key.clone(), vec![2; PAGE_SIZE], 0)
                .unwrap();
        }))
    });
    // Supply the collector's stale observation directly: the background
    // cleaner may independently prune the old clean registration before
    // this thread enumerates it, but must not affect this interleaving.
    pool.prune_clean_dirty_keys(pool.shard_index(&key), std::slice::from_ref(&key))
        .unwrap();
    assert_eq!(pool.checkpoint_dirty_snapshot().unwrap().1, 1);
    drop(retained_pin.borrow_mut().take());
    pool.flush_all().unwrap();
    assert!(std::fs::read(path).unwrap().iter().all(|&byte| byte == 2));
    assert_eq!(pool.dirty_pages.load(Ordering::Relaxed), 0);
}
