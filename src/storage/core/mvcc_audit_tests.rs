// Targeted audit probes. Assertions describe required behavior.
use super::*;

fn audit_int_db() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open_with_config(dir.path(), test_storage_config(DurabilityMode::Strict, true)).unwrap());
    db.create_table("t", vec![lob_col("id", DataType::Int, true), lob_col("v", DataType::Int, false)], vec!["id".into()], vec![]).unwrap();
    let t = db.begin_txn().unwrap();
    db.insert_rows(t, "test.t", vec![vec![Value::Int(1), Value::Int(10)], vec![Value::Int(2), Value::Int(20)]]).unwrap();
    db.commit_txn(t.txn_id).unwrap();
    (dir, db)
}

#[test]
fn unrelated_unique_key_repair_probe_does_not_wait_for_transaction_directory_writer() {
    let (_dir, db) = audit_int_db();
    let table = db.table("test.t").unwrap();
    let index = table
        .indexes
        .iter()
        .find(|index| index.name.eq_ignore_ascii_case("primary"))
        .unwrap()
        .clone();
    let writer = db.begin_dml_txn().unwrap();
    db.register_index_repair_entries(
        writer.txn_id,
        [(
            IndexRepairKey {
                table_id: table.id,
                index_name: "primary".into(),
                key: IndexKey::from(vec![KeyPart::Int(1)]),
            },
            RowId::new(1, 0),
            true,
        )],
    )
    .unwrap();

    let other = Arc::clone(&db);
    let (go_tx, go_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        go_rx.recv().unwrap();
        let result = other.probe_unique_index_candidate(
            &table,
            &index,
            &IndexKey::from(vec![KeyPart::Int(2)]),
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
            || Ok(None),
        );
        done_tx.send(result).unwrap();
    });
    let directory = db.txns.write();
    go_tx.send(()).unwrap();
    let result = done_rx.recv_timeout(std::time::Duration::from_secs(3));
    drop(directory);
    worker.join().unwrap();
    db.rollback_txn(writer.txn_id).unwrap();
    let (candidate, repairs, recheck) = result
        .expect("an unrelated retired key needs no transaction metadata")
        .unwrap();
    assert!(candidate.is_none());
    assert!(repairs.is_empty());
    assert!(
        recheck,
        "repairs on the index still require physical-key rechecking"
    );
}

#[test]
fn empty_unique_key_repair_snapshot_retries_after_registration_during_probe() {
    let (_dir, db) = audit_int_db();
    let table = db.table("test.t").unwrap();
    let index = table
        .indexes
        .iter()
        .find(|index| index.name.eq_ignore_ascii_case("primary"))
        .unwrap();
    let writer = db.begin_dml_txn().unwrap();
    let retired = |value| IndexRepairKey {
        table_id: table.id,
        index_name: "primary".into(),
        key: IndexKey::from(vec![KeyPart::Int(value)]),
    };
    db.register_index_repair_entries(writer.txn_id, [(retired(1), RowId::new(1, 0), true)])
        .unwrap();
    let mut probes = 0;
    let row_id = RowId::new(1, 1);
    let (candidate, repairs, recheck) = db
        .probe_unique_index_candidate(
            &table,
            index,
            &IndexKey::from(vec![KeyPart::Int(2)]),
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
            || {
                probes += 1;
                if probes == 1 {
                    db.register_index_repair_entries(writer.txn_id, [(retired(2), row_id, true)])?;
                }
                Ok(None)
            },
        )
        .unwrap();
    db.rollback_txn(writer.txn_id).unwrap();
    assert!(candidate.is_none());
    assert!(recheck);
    assert_eq!(probes, 2, "registration invalidates the empty-key snapshot");
    assert_eq!(
        repairs,
        vec![(IndexKey::from(vec![KeyPart::Int(2)]), row_id)]
    );
}

#[test]
fn index_repair_registration_does_not_wait_for_transaction_directory_writer() {
    let (_dir, db) = audit_int_db();
    let table = db.table("test.t").unwrap();
    let key = IndexRepairKey {
        table_id: table.id,
        index_name: "primary".into(),
        key: IndexKey::from(vec![KeyPart::Int(1)]),
    };
    let other = Arc::clone(&db);
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (go_tx, go_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let txn = other.begin_dml_txn().unwrap();
        ready_tx.send(txn.txn_id).unwrap();
        go_rx.recv().unwrap();
        let result = other.register_index_repair_entries(txn.txn_id, [(key, RowId::new(1, 0), true)]);
        done_tx.send(result).unwrap();
    });
    let txn_id = ready_rx.recv().unwrap();
    let directory = db.txns.write();
    go_tx.send(()).unwrap();
    let result = done_rx.recv_timeout(std::time::Duration::from_secs(3));
    drop(directory);
    worker.join().unwrap();
    result
        .expect("repair registration must use its independent directory")
        .unwrap();
    assert!(db.has_pending_index_repairs());
    db.rollback_txn(txn_id).unwrap();
    assert!(!db.has_pending_index_repairs());
}

#[test]
fn dml_cached_empty_undo_head_revalidates_after_cross_thread_savepoint_restore() {
    let (_dir, db) = audit_int_db();
    let txn = db.begin_dml_txn().unwrap();
    db.create_named_savepoint(txn.txn_id, "empty".into())
        .unwrap();
    assert_eq!(db.txns.read().active_write_count, 1);

    let other = Arc::clone(&db);
    std::thread::spawn(move || {
        other
            .rollback_to_named_savepoint(txn.txn_id, "empty")
            .unwrap();
    })
    .join()
    .unwrap();
    assert_eq!(db.txns.read().active_write_count, 0);

    db.update_rows(
        txn,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(1)),
        |_| Ok(vec![Value::Int(1), Value::Int(11)]),
    )
    .unwrap();
    assert_eq!(
        db.txns.read().active_write_count,
        1,
        "a restored empty head must register the writer before appending undo"
    );
    db.commit_txn(txn.txn_id).unwrap();
    assert!(
        db.undo_head_for_append(txn.txn_id).is_err(),
        "finished transactions cannot reuse a cached append handle"
    );
}

#[test]
fn dml_cached_handle_does_not_cross_database_identity() {
    let (_left_dir, left) = audit_int_db();
    let (_right_dir, right) = audit_int_db();
    let a = left.begin_dml_txn().unwrap();
    let b = right.begin_dml_txn().unwrap();
    assert_eq!(a.txn_id, b.txn_id);

    left.update_rows(
        a,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(1)),
        |_| Ok(vec![Value::Int(1), Value::Int(31)]),
    )
    .unwrap();
    right
        .update_rows(
            b,
            "test.t",
            None,
            |row| Ok(row[0] == Value::Int(1)),
            |_| Ok(vec![Value::Int(1), Value::Int(41)]),
        )
        .unwrap();
    left.rollback_txn(a.txn_id).unwrap();
    right.commit_txn(b.txn_id).unwrap();
    let view = TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted);
    let left_rows = left.scan_rows("test.t", view).unwrap();
    let right_rows = right.scan_rows("test.t", view).unwrap();
    assert_eq!(
        left_rows
            .iter()
            .find(|row| row.values[0] == Value::Int(1))
            .unwrap()
            .values[1],
        Value::Int(10)
    );
    assert_eq!(
        right_rows
            .iter()
            .find(|row| row.values[0] == Value::Int(1))
            .unwrap()
            .values[1],
        Value::Int(41)
    );
}

#[test]
fn audit_mvcc_compaction_savepoint_boundary() {
    std::env::set_var("MYDB_UNDO_WRITE_SHARDS", "1");
    let (_dir, db) = audit_int_db();
    db.set_undo_segment_bytes(512).unwrap();
    let t = db.begin_txn().unwrap();
    db.update_rows(t, "test.t", None, |r| Ok(r[0] == Value::Int(1)), |_| Ok(vec![Value::Int(1), Value::Int(11)])).unwrap();
    db.create_named_savepoint(t.txn_id, "s".into()).unwrap();
    let old_head = db.txns.read().undo_head(t.txn_id).unwrap().unwrap();
    for v in 21..70 {
        let churn = db.begin_txn().unwrap();
        db.update_rows(churn, "test.t", None, |r| Ok(r[0] == Value::Int(2)), |_| Ok(vec![Value::Int(2), Value::Int(v)])).unwrap();
        db.commit_txn(churn.txn_id).unwrap();
    }
    {
        let mut undo = db.undo.write();
        let ids = undo.records.keys().copied().filter(|id| *id != old_head).collect::<Vec<_>>();
        for id in ids { undo.remove_record(id).unwrap(); }
    }
    db.compact_undo_segments(100, 64).unwrap();
    assert_ne!(db.txns.read().undo_head(t.txn_id).unwrap(), Some(old_head), "fixture must relocate head");
    db.rollback_to_named_savepoint(t.txn_id, "s").unwrap();
    let rows = db.scan_rows("test.t", TxnView { snapshot_scn: u64::MAX, ..t }).unwrap();
    assert_eq!(rows.iter().find(|r| r.values[0] == Value::Int(1)).unwrap().values[1], Value::Int(11), "rolling back to unchanged savepoint must do nothing");
}

#[test]
fn audit_mvcc_savepoint_wal_buffer_order() {
    let (dir, db) = audit_int_db();
    db.flush_dirty_data().unwrap();
    let t = db.begin_dml_txn().unwrap();
    db.create_named_savepoint(t.txn_id, "s".into()).unwrap();
    db.update_rows(t, "test.t", None, |r| Ok(r[0] == Value::Int(1)), |_| Ok(vec![Value::Int(1), Value::Int(99)])).unwrap();
    assert!(txn_wal_buffer_len(&db.log, t.txn_id) > 0, "fixture must buffer forward WAL");
    db.rollback_to_named_savepoint(t.txn_id, "s").unwrap();
    eprintln!("buffered forward records after compensation: {}", txn_wal_buffer_len(&db.log, t.txn_id));
    db.commit_txn(t.txn_id).unwrap();
    let live = db.scan_rows("test.t", TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted)).unwrap();
    assert_eq!(live.iter().find(|r| r.values[0] == Value::Int(1)).unwrap().values[1], Value::Int(10));
    fn copy_tree(src: &Path, dst: &Path) {
        std::fs::create_dir_all(dst).unwrap();
        for entry in std::fs::read_dir(src).unwrap() {
            let entry = entry.unwrap();
            let to = dst.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() { copy_tree(&entry.path(), &to); }
            else { std::fs::copy(entry.path(), to).unwrap(); }
        }
    }
    let crash = tempfile::tempdir().unwrap();
    copy_tree(dir.path(), crash.path());
    let recovered = Database::open_with_config(crash.path(), test_storage_config(DurabilityMode::Strict, true)).unwrap();
    let rows = recovered.scan_rows("test.t", TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted)).unwrap();
    assert_eq!(rows.iter().find(|r| r.values[0] == Value::Int(1)).unwrap().values[1], Value::Int(10));
}

fn audit_db() -> (tempfile::TempDir, Arc<Database>) {
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    let dir = tempfile::tempdir().unwrap();
    let db = open_lob_table(dir.path(), &test_storage_config(DurabilityMode::Memory, false));
    (dir, db)
}

fn audit_seed(db: &Database) {
    let t = db.begin_txn().unwrap();
    db.insert_rows(t, "test.t", vec![vec![Value::Int(1), Value::String("A".repeat(12000))]]).unwrap();
    db.commit_txn(t.txn_id).unwrap();
}

#[test]
fn audit_mvcc_savepoint_purge_live_lob() {
    let (_dir, db) = audit_db();
    audit_seed(&db);
    let t = db.begin_txn().unwrap();
    db.create_named_savepoint(t.txn_id, "s".into()).unwrap();
    db.update_rows(t, "test.t", None, |_| Ok(true), |_| Ok(vec![Value::Int(1), Value::String("B".repeat(12000))])).unwrap();
    db.rollback_to_named_savepoint(t.txn_id, "s").unwrap();
    assert_eq!(scan_body(&db, 1), Some(Value::String("A".repeat(12000))));
    db.commit_txn(t.txn_id).unwrap();
    for _ in 0..4 { db.purge_obsolete_undo(); }
    let t = db.begin_txn().unwrap();
    db.insert_rows(t, "test.t", vec![vec![Value::Int(2), Value::String("Z".repeat(24000))]]).unwrap();
    db.commit_txn(t.txn_id).unwrap();
    assert_eq!(scan_body(&db, 1), Some(Value::String("A".repeat(12000))));
}

#[test]
fn audit_mvcc_index_repairs_commit_gap() {
    let (_dir, db) = audit_db();
    audit_seed(&db);
    let before = db.visible_scn();
    let unpublished_scn = db.next_scn_atomic.fetch_add(1, Ordering::AcqRel);
    let t = db.begin_txn().unwrap();
    db.update_rows(t, "test.t", None, |_| Ok(true), |r| Ok(vec![Value::Int(2), r[1].clone()])).unwrap();
    let mut deferred = db.commit_txn_deferred(t.txn_id).unwrap();
    assert_eq!(db.visible_scn(), before);
    assert!(db.has_pending_index_repairs());
    db.purge_obsolete_index_repairs();
    assert!(db.has_pending_index_repairs(), "commit above frontier still needs old index key");
    db.complete_commit_frontier(unpublished_scn);
    db.finish_deferred_commits(&mut [&mut deferred]).unwrap();
}

#[test]
fn audit_mvcc_allocator_hole_watermark() {
    let (_dir, db) = audit_db();
    let reserved = db.next_txn_id_atomic.fetch_add(1, Ordering::AcqRel);
    let later = db.begin_txn().unwrap();
    db.commit_txn(later.txn_id).unwrap();
    db.advance_min_retained_txn_id();
    assert!(db.min_retained_txn_id.load(Ordering::Acquire) < reserved,
        "GC must not classify allocated but not yet registered transaction as terminal");
}

#[test]
fn audit_mvcc_serializable_s_to_ix() {
    let locks = ShardedTableLocks::new();
    assert!(matches!(locks.try_acquire(1, 7, TableLockMode::Shared), TableAcquireOutcome::Acquired { .. }));
    assert!(matches!(locks.try_acquire(2, 7, TableLockMode::Shared), TableAcquireOutcome::Acquired { .. }));
    assert!(matches!(locks.try_acquire(1, 7, TableLockMode::IntentExclusive), TableAcquireOutcome::Conflict { .. }),
        "S + IX must request SIX and conflict with another reader");
}

#[test]
fn audit_mvcc_release_preserves_later_savepoint() {
    let (_dir, db) = audit_db();
    let t = db.begin_txn().unwrap();
    db.create_named_savepoint(t.txn_id, "a".into()).unwrap();
    db.create_named_savepoint(t.txn_id, "b".into()).unwrap();
    db.release_named_savepoint(t.txn_id, "a").unwrap();
    db.rollback_to_named_savepoint(t.txn_id, "b").unwrap();
}

#[test]
fn audit_mvcc_lob_index_rollback() {
    let (_dir, db) = audit_db();
    db.add_index("test.t", IndexSchema {
        readable_from_scn: 0,
        index_id: 0, name: "body_idx".into(), columns: vec!["body".into()],
        column_prefix_lengths: vec![Some(8)], column_directions: Vec::new(),
        key_expressions: Vec::new(), unique: false, primary: false,
        included_columns: Vec::new(), state: IndexState::Public,
        index_type: "BTREE".into(), visible: true, comment: String::new(), fulltext: None,
    }).unwrap();
    audit_seed(&db);
    let t = db.begin_txn().unwrap();
    db.delete_rows(t, "test.t", None, |_| Ok(true)).unwrap();
    db.rollback_txn(t.txn_id).unwrap();
    let table = db.table("test.t").unwrap();
    let index = table.indexes.iter().find(|i| i.name == "body_idx").unwrap();
    let keys = table.index_keys(index, &[Value::Int(1), Value::String("A".repeat(12000))]).unwrap();
    let ids = db.lookup_index(&table, index, keys[0].clone()).unwrap();
    assert_eq!(ids.len(), 1, "restored LOB must restore its real prefix key");
}

fn audit_poison_row_page(db: &Database, table_id: u64, row_id: RowId) {
    let heap = db.heap_handle(table_id).unwrap();
    let heap = heap.read();
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = heap.page_latch(row_id.page_id as u64).lock().unwrap();
        panic!("audit: inject page-latch failure during compensation");
    }));
    assert!(caught.is_err());
}

#[test]
fn audit_mvcc_insert_rollback_propagates_heap_failure() {
    let (_dir, db) = audit_int_db();
    let t = db.begin_txn().unwrap();
    db.insert_rows(t, "test.t", vec![vec![Value::Int(3), Value::Int(30)]]).unwrap();
    let table = db.table("test.t").unwrap();
    let rows = db.scan_rows("test.t", t).unwrap();
    let rid = rows.iter().find(|r| r.values[0] == Value::Int(3)).unwrap().row_id;
    audit_poison_row_page(&db, table.id, rid);
    let result = db.rollback_txn(t.txn_id);
    assert!(result.is_err(), "heap deletion failure must not report successful rollback");
}

#[test]
fn audit_mvcc_statement_rollback_failure_quarantines() {
    let (_dir, db) = audit_int_db();
    let t = db.begin_txn().unwrap();
    let sp = db.statement_savepoint(t.txn_id).unwrap();
    db.update_rows(t, "test.t", None, |r| Ok(r[0] == Value::Int(1)), |_| Ok(vec![Value::Int(1), Value::Int(99)])).unwrap();
    let table = db.table("test.t").unwrap();
    let rows = db.scan_rows("test.t", t).unwrap();
    let rid = rows.iter().find(|r| r.values[0] == Value::Int(1)).unwrap().row_id;
    audit_poison_row_page(&db, table.id, rid);
    assert!(db.rollback_to_statement_savepoint(t.txn_id, sp).is_err());
    assert!(db.mvcc_quarantined.load(Ordering::Acquire), "partial compensation failure must fail-stop, like full rollback");
}

#[test]
fn session_audit_rollback_failure_still_releases_session_resources() {
    use crate::engine::api::SessionContext;
    use crate::engine::executor::StorageQueryEngine;
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    let mut session = SessionContext::default();
    session.connection_id = 937521;
    for sql in [
        "CREATE TABLE session_cleanup(id INT PRIMARY KEY, v INT)",
        "INSERT INTO session_cleanup VALUES(1,1)",
        "CREATE TEMPORARY TABLE cleanup_tmp(a INT)",
        "SELECT GET_LOCK('session_audit_cleanup',0)",
        "BEGIN",
        "UPDATE session_cleanup SET v=2 WHERE id=1",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    let db = engine.db_handle();
    let undo_head = db
        .txns
        .read()
        .undo_head(session.current_txn.unwrap())
        .unwrap()
        .unwrap();
    db.undo
        .write()
        .records
        .get_mut(&undo_head)
        .unwrap()
        .before_values = None;
    let err = engine.reset_session(&mut session).unwrap_err();
    assert!(err.message().contains("before-image"), "{err:?}");
    assert!(session.temp_tables.is_empty());
    assert!(session.explicit_table_locks.is_empty());
    assert!(db
        .metadata_lock_snapshot()
        .iter()
        .all(|row| row.owner_thread_id != Some(937521)));
    assert_eq!(crate::engine::expr::release_all_user_locks(937521), 0);
}

#[test]
fn cancelled_bulk_dml_after_candidate_evaluation_never_commits() {
    use crate::engine::cancel::{enter, QueryCancelToken};
    for delete in [false, true] {
        let (_dir, db) = audit_int_db();
        let txn = db.begin_txn().unwrap();
        let token = QueryCancelToken::noop();
        let _scope = enter(token.clone());
        let mut visits = 0;
        let result = if delete {
            db.delete_rows(txn, "test.t", None, |_| {
                visits += 1;
                // Each row is checked before and after acquiring its lock.
                if visits == 4 {
                    token.cancel_local();
                }
                Ok(true)
            })
        } else {
            db.update_rows_with_changed_columns(
                txn,
                "test.t",
                None,
                Some(&[1]),
                |_| Ok(true),
                |row| {
                    visits += 1;
                    if visits == 2 {
                        token.cancel_local();
                    }
                    Ok(vec![row[0].clone(), Value::Int(99)])
                },
            )
        };
        assert_eq!(result.unwrap_err().code(), 1317);
        // Rollback must ignore the statement cancellation and finish undo.
        db.rollback_txn(txn.txn_id).unwrap();
        drop(_scope);
        let rows = db
            .scan_rows(
                "test.t",
                TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
            )
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows.iter()
                .map(|r| match r.values[1] {
                    Value::Int(v) => v,
                    _ => panic!(),
                })
                .sum::<i64>(),
            30
        );
        let next = db.begin_txn().unwrap();
        assert_eq!(
            db.update_rows(
                next,
                "test.t",
                None,
                |_| Ok(true),
                |r| Ok(vec![r[0].clone(), Value::Int(7)])
            )
            .unwrap(),
            2
        );
        db.commit_txn(next.txn_id).unwrap();
    }
}

#[test]
fn cancelled_bulk_dml_after_first_write_rolls_back_and_reopens() {
    use crate::engine::cancel::{enter, QueryCancelToken};
    for mode in ["delta", "grow", "lob", "delete"] {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(
            Database::open_with_config(
                dir.path(),
                test_storage_config(DurabilityMode::Strict, true),
            )
            .unwrap(),
        );
        let data_type = if mode == "delta" {
            DataType::Int
        } else {
            DataType::Text(65535)
        };
        db.create_table(
            "cancel_bulk",
            vec![
                lob_col("id", DataType::Int, true),
                lob_col("v", data_type, false),
            ],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let before = if mode == "delta" {
            Value::Int(10)
        } else {
            Value::String("original".into())
        };
        let after = match mode {
            "delta" => Value::Int(99),
            "lob" => Value::String("新值".repeat(2000)),
            _ => Value::String("a longer row after update".into()),
        };
        let insert = db.begin_txn().unwrap();
        db.insert_rows(
            insert,
            "test.cancel_bulk",
            (0..4096)
                .map(|i| vec![Value::Int(i), before.clone()])
                .collect(),
        )
        .unwrap();
        db.commit_txn(insert.txn_id).unwrap();
        let txn = db.begin_txn().unwrap();
        let token = QueryCancelToken::noop();
        let observer_db = db.clone();
        let observer_token = token.clone();
        let observer = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            // Observe actual undo publication, not a sleep guessed from speed.
            while observer_db
                .txns
                .read()
                .undo_head(txn.txn_id)
                .unwrap()
                .is_none()
            {
                assert!(std::time::Instant::now() < deadline, "no write observed");
                std::thread::yield_now();
            }
            observer_token.cancel_local();
        });
        let scope = enter(token);
        let result = if mode == "delete" {
            db.delete_rows(txn, "test.cancel_bulk", None, |_| Ok(true))
        } else {
            db.update_rows_with_changed_columns(
                txn,
                "test.cancel_bulk",
                None,
                Some(&[1]),
                |_| Ok(true),
                |r| Ok(vec![r[0].clone(), after.clone()]),
            )
        };
        observer.join().unwrap();
        assert_eq!(result.unwrap_err().code(), 1317, "mode={mode}");
        db.rollback_txn(txn.txn_id).unwrap();
        drop(scope);
        let read = TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted);
        let rows = db.scan_rows("test.cancel_bulk", read).unwrap();
        assert_eq!(rows.len(), 4096);
        assert!(rows.iter().all(|r| r.values[1] == before), "mode={mode}");
        db.flush_dirty_data().unwrap();
        drop(db);
        let reopened = Database::open_with_config(
            dir.path(),
            test_storage_config(DurabilityMode::Strict, true),
        )
        .unwrap();
        let rows = reopened.scan_rows("test.cancel_bulk", read).unwrap();
        assert_eq!(rows.len(), 4096);
        assert!(rows.iter().all(|r| r.values[1] == before));
    }
}
