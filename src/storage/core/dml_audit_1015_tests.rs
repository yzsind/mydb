use super::*;

#[test]
fn unique_conflict_probe_waits_for_xa_prepared_writer_without_unique_key_lock() {
    let dir = tempfile::tempdir().unwrap();
    let engine =
        crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
            dir.path(),
            audit_config(),
        )
        .unwrap();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    let db = engine.database();
    let table = db.table("test.t").unwrap();
    let index = &table.indexes[0];
    let writer = db.begin_dml_txn().unwrap();
    let ids = db
        .insert_rows(writer, "test.t", vec![vec![Value::Int(1)]])
        .unwrap();
    let xid = crate::xa::Xid::new(1, b"dml-audit-prepared".to_vec(), vec![]).unwrap();
    db.prepare_xa(writer.txn_id, 0, xid.clone()).unwrap();
    let reader = db.begin_txn().unwrap();
    let key = table.index_key(index, &[Value::Int(1)]).unwrap();
    // Exercise the constraint probe directly, as shadow/index-build paths do.
    let err = db
        .check_unique_conflict_at_row(&table, reader, None, index, &key, table.id, ids[0])
        .unwrap_err();
    assert!(is_lock_wait_pending_error(&err), "{err:?}");
    db.rollback_txn(reader.txn_id).unwrap();
    db.commit_prepared(&xid).unwrap();
    let reader = db.begin_txn().unwrap();
    assert_eq!(
        db.check_unique_conflict_at_row(&table, reader, None, index, &key, table.id, ids[0])
            .unwrap_err()
            .code(),
        1062
    );
    db.rollback_txn(reader.txn_id).unwrap();
}

#[test]
fn fk_and_unique_probes_accept_gc_commits_and_recovered_checkpoint_rows() {
    let dir = tempfile::tempdir().unwrap();
    let config = audit_config();
    let engine =
        crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
            dir.path(),
            config.clone(),
        )
        .unwrap();
    for sql in [
        "CREATE TABLE p (id INT PRIMARY KEY)",
        "CREATE TABLE c (id INT PRIMARY KEY,pid INT,FOREIGN KEY(pid) REFERENCES p(id))",
        "INSERT INTO p VALUES (1)",
    ] {
        engine.execute(sql).unwrap();
    }
    let db = engine.database();
    let table = db.table("test.p").unwrap();
    let row = db
        .scan_rows(
            "test.p",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap()
        .remove(0);
    let writer_id = db
        .read_mvcc_row(table.id, row.row_id)
        .unwrap()
        .unwrap()
        .header
        .last_txn_id;
    db.advance_min_retained_txn_id();
    assert_eq!(
        db.txns.read().status(writer_id),
        None,
        "fixture must actually GC the committed slot"
    );
    engine.execute("INSERT INTO c VALUES (1,1)").unwrap();
    assert_eq!(
        engine
            .execute("INSERT INTO p VALUES (1)")
            .unwrap_err()
            .code(),
        1062
    );
    assert_eq!(
        engine
            .execute("INSERT INTO c VALUES (2,2)")
            .unwrap_err()
            .code(),
        1452
    );
    db.checkpoint().unwrap();
    drop(engine);
    let engine =
        crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
            dir.path(),
            config,
        )
        .unwrap();
    engine.execute("INSERT INTO c VALUES (2,1)").unwrap();
    assert_eq!(
        engine
            .execute("INSERT INTO p VALUES (1)")
            .unwrap_err()
            .code(),
        1062
    );
    assert_eq!(engine.execute("DELETE FROM p").unwrap_err().code(), 1451);
}

fn audit_config() -> StorageConfig {
    StorageConfig {
        durability: DurabilityMode::Strict,
        checkpoint_interval: Duration::from_secs(60),
        checkpoint_wal_bytes: 64 * 1024 * 1024,
        lock_wait: Duration::from_secs(1),
        group_commit_window: Duration::ZERO,
        wal_segment_bytes: 64 * 1024 * 1024,
        buffer_pool_pages: 512,
        buffer_pool_io_threads: 1,
        buffer_pool_prefetch_pages: 0,
        undo_retention: Duration::from_secs(60),
        heap_update_reserve_bytes: 2048,
        startup_validation: StartupValidationMode::Full,
        startup_validate_indexes: true,
    }
}
