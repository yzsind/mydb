use super::*;
use crate::engine::StorageQueryEngine;

fn fixture() -> (tempfile::TempDir, StorageQueryEngine) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        StorageConfig {
            durability: DurabilityMode::Memory,
            buffer_pool_pages: 512,
            undo_retention: Duration::ZERO,
            ..StorageConfig::from_env()
        },
    )
    .unwrap();
    (dir, engine)
}

#[test]
fn audit1109_zone_merge_failure_compensates_heap_and_lobs() {
    let (_dir, e) = fixture();
    e.execute("CREATE TABLE t(id INT PRIMARY KEY,b LONGBLOB)")
        .unwrap();
    let db = e.database();
    let table = db.table("test.t").unwrap();
    let txn = db.begin_txn().unwrap();
    TEST_INSERT_ZONE_FAILURE.set(Some(table.id));
    let result = db.insert_rows(
        txn,
        "test.t",
        vec![vec![Value::Int(1), Value::Bytes(vec![42; 20000])]],
    );
    TEST_INSERT_ZONE_FAILURE.set(None);
    assert!(result.is_err());
    db.commit_txn(txn.txn_id).unwrap();
    let view = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    assert!(db.scan_rows("test.t", view).unwrap().is_empty());
    let (pages, free) = db.lob_space_stats(table.id).unwrap();
    assert_eq!(free, pages - 1);
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t", vec![vec![Value::Int(1), Value::Null]])
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
}

#[test]
fn audit1109_rolled_back_insert_slots_wait_for_readers_then_reuse() {
    let (_dir, e) = fixture();
    e.execute("CREATE TABLE t(id INT PRIMARY KEY,v VARCHAR(200))")
        .unwrap();
    let db = e.database();
    let table = db.table("test.t").unwrap();
    let snapshot = db.read_only_statement_view(IsolationLevel::RepeatableRead, None);
    let guard = db.register_statement_snapshot(snapshot.snapshot_scn);
    let txn = db.begin_txn().unwrap();
    let ids = db
        .insert_rows(
            txn,
            "test.t",
            (0..100)
                .map(|i| vec![Value::Int(i), Value::String("x".repeat(200))])
                .collect(),
        )
        .unwrap();
    db.rollback_txn(txn.txn_id).unwrap();
    std::thread::sleep(Duration::from_millis(5));
    db.purge_obsolete_undo();
    assert_ne!(
        db.slot_flags_for_test(table.id, ids[0]).unwrap(),
        Some(ROW_FLAG_FREE)
    );
    drop(guard);
    db.purge_obsolete_undo();
    assert_eq!(
        db.slot_flags_for_test(table.id, ids[0]).unwrap(),
        Some(ROW_FLAG_FREE)
    );
    let before = db.heap_page_count(table.id);
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        (0..100)
            .map(|i| vec![Value::Int(i), Value::String("x".repeat(200))])
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    assert_eq!(db.heap_page_count(table.id), before);
}

#[test]
fn audit1109_fast_parent_insert_retains_fk_wait_lock() {
    let (_dir, e) = fixture();
    e.execute("CREATE TABLE p(id INT AUTO_INCREMENT PRIMARY KEY)")
        .unwrap();
    e.execute("CREATE TABLE c(id INT PRIMARY KEY,pid INT,FOREIGN KEY(pid) REFERENCES p(id))")
        .unwrap();
    let db = e.database();
    let p = db.table("test.p").unwrap();
    let txn = db.begin_txn().unwrap();
    let inserted = db
        .insert_rows_autoinc_fast_detailed(txn, "test.p", vec![vec![Value::Null]])
        .unwrap();
    assert!(db.txn_holds_row_lock(txn.txn_id, p.id, inserted.row_ids[0]));
    db.commit_txn(txn.txn_id).unwrap();
    e.execute("INSERT INTO c VALUES(1,1)").unwrap();
}

#[test]
fn audit1109_repeated_rollback_does_not_quarantine_database() {
    let (_dir, e) = fixture();
    let db = e.database();
    let a = db.begin_txn().unwrap();
    db.rollback_txn(a.txn_id).unwrap();
    db.rollback_txn(a.txn_id).unwrap();
    let b = db.begin_txn().unwrap();
    db.commit_txn(b.txn_id).unwrap();
    db.rollback_txn(b.txn_id).unwrap();
    assert!(db.rollback_txn(u64::MAX).is_err());
    e.execute("CREATE TABLE t(a INT)").unwrap();
    e.execute("INSERT INTO t VALUES(1)").unwrap();
}

#[test]
fn audit1109_partial_heap_write_keeps_published_lob_chains_alive() {
    let (_dir, e) = fixture();
    e.execute("CREATE TABLE t(id INT PRIMARY KEY,b LONGBLOB)")
        .unwrap();
    let db = e.database();
    let table = db.table("test.t").unwrap();
    let txn = db.begin_txn().unwrap();
    let ids = db
        .insert_rows(
            txn,
            "test.t",
            (1..=3)
                .map(|i| vec![Value::Int(i), Value::Bytes(vec![41; 20000])])
                .collect(),
        )
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let txn = db.begin_txn().unwrap();
    TEST_HEAP_WRITE_FAILURE.set(Some(1));
    let result = db.update_rows(
        txn,
        "test.t",
        None,
        |_| Ok(true),
        |old| Ok(vec![old[0].clone(), Value::Bytes(vec![42; 20000])]),
    );
    TEST_HEAP_WRITE_FAILURE.set(None);
    assert!(result.is_err());
    // Reuse every free chain before reading the published row. Freed LOB
    // pages retain bytes until reused, so a read alone cannot detect this bug.
    let mut masks = Vec::new();
    for _ in 0..3 {
        masks.push(
            db.externalize_row_lobs(
                table.id,
                &table.row_codec,
                &[Value::Int(4), Value::Bytes(vec![43; 20000])],
            )
            .unwrap(),
        );
    }
    assert_eq!(
        db.read_mvcc_row(table.id, ids[0]).unwrap().unwrap().values[1],
        Value::Bytes(vec![42; 20000])
    );
    db.free_external_masks(table.id, &masks);
    db.rollback_txn(txn.txn_id).unwrap();
    let view = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    for row in db.scan_rows("test.t", view).unwrap() {
        assert_eq!(row.values[1], Value::Bytes(vec![41; 20000]));
    }
}

#[test]
fn audit1109_missing_update_releases_only_new_point_lock() {
    let (_dir, e) = fixture();
    e.execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
        .unwrap();
    let db = e.database();
    let table = db.table("test.t").unwrap();
    let row = RowId::new(0, 0);
    let txn = db.begin_txn().unwrap();
    assert!(!db
        .update_row_by_id(txn, &table, row, |_| Ok(vec![Value::Int(1), Value::Int(2)]))
        .unwrap());
    assert!(!db.txn_holds_row_lock(txn.txn_id, table.id, row));
    db.lock_rows_nonblocking(txn.txn_id, table.id, [row])
        .unwrap();
    assert!(!db
        .update_row_by_id(txn, &table, row, |_| Ok(vec![Value::Int(1), Value::Int(2)]))
        .unwrap());
    assert!(db.txn_holds_row_lock(txn.txn_id, table.id, row));
    db.rollback_txn(txn.txn_id).unwrap();
}

#[test]
fn audit1109_savepoint_restore_uses_compacted_undo_head() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let config = StorageConfig {
        durability: DurabilityMode::Relaxed,
        buffer_pool_pages: 512,
        undo_retention: Duration::from_secs(3600),
        ..StorageConfig::from_env()
    };
    let e =
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap();
    e.execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
        .unwrap();
    e.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    let db = e.database();
    db.set_undo_segment_bytes(512).unwrap();
    let writer = db.begin_txn().unwrap();
    db.update_rows(
        writer,
        "test.t",
        None,
        |r| Ok(r[0] == Value::Int(1)),
        |_| Ok(vec![Value::Int(1), Value::Int(11)]),
    )
    .unwrap();
    let savepoint = db.statement_savepoint(writer.txn_id).unwrap();
    let old = savepoint.undo_head.unwrap();
    for v in 30..100 {
        let churn = db.begin_txn().unwrap();
        db.update_rows(
            churn,
            "test.t",
            None,
            |r| Ok(r[0] == Value::Int(2)),
            |_| Ok(vec![Value::Int(2), Value::Int(v)]),
        )
        .unwrap();
        db.commit_txn(churn.txn_id).unwrap();
    }
    {
        let mut undo = db.undo.write();
        let ids = undo
            .records
            .keys()
            .copied()
            .filter(|id| *id != old)
            .collect::<Vec<_>>();
        for id in ids {
            undo.remove_record(id).unwrap();
        }
    }
    db.compact_undo_segments(90, 64).unwrap();
    let canonical = db.undo.read().canonical_ptr(old);
    assert_ne!(canonical, old);
    db.rollback_to_statement_savepoint(writer.txn_id, savepoint)
        .unwrap();
    assert_eq!(
        db.txns_read().unwrap().undo_head(writer.txn_id).unwrap(),
        Some(canonical)
    );
    db.rollback_txn(writer.txn_id).unwrap();
}

#[test]
fn audit1109_recovery_reclaims_insert_without_logical_undo() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let config = StorageConfig {
        durability: DurabilityMode::Relaxed,
        buffer_pool_pages: 512,
        ..StorageConfig::from_env()
    };
    let (table_id, row_id) = {
        let e = StorageQueryEngine::open_with_storage_config_without_background_workers(
            dir.path(),
            config.clone(),
        )
        .unwrap();
        e.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
        let db = e.database();
        let table = db.table("test.t").unwrap();
        let txn = db.begin_txn().unwrap();
        let inserted = db
            .insert_mvcc_batch_externalized(
                txn.txn_id,
                table.id,
                &table.row_codec,
                &[RowHeader::visible(txn.txn_id)],
                &[vec![Value::Int(1)]],
            )
            .unwrap();
        db.flush_txn_wal_buffer(txn.txn_id, None).unwrap();
        db.flush_dirty_data().unwrap();
        (table.id, inserted[0])
    };
    let e =
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap();
    let db = e.database();
    assert_eq!(
        db.slot_flags_for_test(table_id, row_id).unwrap(),
        Some(ROW_FLAG_FREE)
    );
    let txn = db.begin_txn().unwrap();
    let ids = db
        .insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
        .unwrap();
    assert_eq!(ids[0], row_id);
    db.commit_txn(txn.txn_id).unwrap();
}

#[test]
fn audit1109_rls_filtered_delete_releases_only_new_point_lock() {
    let (_dir, e) = fixture();
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY)",
        "INSERT INTO t VALUES(1)",
        "CREATE USER alice",
        "GRANT SELECT,DELETE ON test.t TO alice",
        "ALTER TABLE t ENABLE ROW LEVEL SECURITY",
    ] {
        e.execute(sql).unwrap();
    }
    let db = e.database();
    let table = db.table("test.t").unwrap();
    let view = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let row_id = db.scan_rows("test.t", view).unwrap()[0].row_id;
    let session = crate::engine::SessionContext {
        user: "alice".into(),
        login_user: "alice".into(),
        authenticated_as: crate::engine::auth::AccountKey::new("alice", "%"),
        ..Default::default()
    };
    let txn = db.begin_txn().unwrap();
    let deleted = crate::engine::expr::with_session_expr_info(&session, || {
        db.delete_row_by_id_at_if(txn, &table, table.id, row_id, |_| Ok(true))
    })
    .unwrap();
    assert!(!deleted);
    assert!(!db.txn_holds_row_lock(txn.txn_id, table.id, row_id));
    db.lock_rows_nonblocking(txn.txn_id, table.id, [row_id])
        .unwrap();
    let deleted = crate::engine::expr::with_session_expr_info(&session, || {
        db.delete_row_by_id_at_if(txn, &table, table.id, row_id, |_| Ok(true))
    })
    .unwrap();
    assert!(!deleted);
    assert!(db.txn_holds_row_lock(txn.txn_id, table.id, row_id));
    db.rollback_txn(txn.txn_id).unwrap();
}

#[test]
fn audit1109_concurrent_rollback_retries_serialize_compensation() {
    let (_dir, e) = fixture();
    e.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    let db = e.database().clone();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
        .unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(3));
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..2 {
            let db = db.clone();
            let barrier = barrier.clone();
            handles.push(scope.spawn(move || {
                barrier.wait();
                db.rollback_txn(txn.txn_id)
            }));
        }
        barrier.wait();
        for handle in handles {
            handle.join().unwrap().unwrap();
        }
    });
    e.execute("INSERT INTO t VALUES(1)").unwrap();
}
