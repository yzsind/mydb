// Recovery tests use a child process that exits without destructors or clean
// shutdown. The parent reopens the durable files in a separate database.
#[test]
fn fresh_audit_promotion_rejects_uncovered_current_row_before_rollback() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let config = test_storage_config(DurabilityMode::Strict, true);
    let db = open_lob_table(dir.path(), &config);
    let seed = db.begin_txn().unwrap();
    db.insert_rows(seed,"test.t",vec![vec![Value::Int(1),Value::String("A".into())]]).unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let writer = db.begin_dml_txn().unwrap();
    db.update_rows(writer,"test.t",None,|_|Ok(true),|_|Ok(vec![Value::Int(1),Value::String("B".into())])).unwrap();
    let head = db.txns.read().undo_head(writer.txn_id).unwrap();
    db.txns.write().slots.get_mut(&writer.txn_id).unwrap().undo_head.set(None);
    let err = db.rollback_all_active_replication_txns().unwrap_err();
    assert!(err.is(crate::error::catalog::MYDB_ER_ROLE_CHANGE_BLOCKED));
    assert_eq!(db.txns.read().status(writer.txn_id),Some(TxnStatus::Active));
    db.txns.write().slots.get_mut(&writer.txn_id).unwrap().undo_head.set(head);
    db.rollback_all_active_replication_txns().unwrap();
    assert_eq!(scan_body(&db,1),Some(Value::String("A".into())));
}

#[test]
fn mvcc_fix_crash_child() {
    let Ok(path) = std::env::var("MYDB_MVCC_FIX_CHILD_DIR") else {
        return;
    };
    let scenario = std::env::var("MYDB_MVCC_FIX_SCENARIO").unwrap();
    let config = test_storage_config(DurabilityMode::Strict, true);
    if scenario.starts_with("lob") {
        let db = open_lob_table(Path::new(&path), &config);
        let seed = db.begin_txn().unwrap();
        db.insert_rows(
            seed,
            "test.t",
            vec![vec![Value::Int(1), Value::String("A".repeat(12000))]],
        )
        .unwrap();
        db.commit_txn(seed.txn_id).unwrap();
        db.checkpoint().unwrap();
        let writer = db.begin_dml_txn().unwrap();
        db.create_named_savepoint(writer.txn_id, "s".into())
            .unwrap();
        for value in ["B", "C"] {
            db.update_rows(
                writer,
                "test.t",
                None,
                |_| Ok(true),
                |_| Ok(vec![Value::Int(1), Value::String(value.repeat(12000))]),
            )
            .unwrap();
            db.rollback_to_named_savepoint(writer.txn_id, "s").unwrap();
        }
        if scenario.contains("xa") {
            let xid = crate::xa::Xid::new(1, b"compensated".to_vec(), vec![]).unwrap();
            db.prepare_xa(writer.txn_id, 0, xid).unwrap();
        } else {
            db.commit_txn(writer.txn_id).unwrap();
        }
        if scenario == "lob-checkpoint" {
            db.checkpoint().unwrap();
        }
    } else {
        let db = Arc::new(Database::open_with_config(&path, config).unwrap());
        db.create_table(
            "t",
            vec![
                lob_col("id", DataType::Int, true),
                lob_col("v", DataType::Int, false),
            ],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let seed = db.begin_txn().unwrap();
        db.insert_rows(
            seed,
            "test.t",
            vec![
                vec![Value::Int(1), Value::Int(10)],
                vec![Value::Int(2), Value::Int(20)],
            ],
        )
        .unwrap();
        db.commit_txn(seed.txn_id).unwrap();
        db.checkpoint().unwrap();
        let writer = if scenario == "unbuffered" {
            db.begin_txn().unwrap()
        } else {
            db.begin_dml_txn().unwrap()
        };
        if scenario == "mixed" {
            set_update_delta_enabled_for_test(true);
            db.update_rows_with_changed_columns(
                writer,
                "test.t",
                None,
                Some(&[1]),
                |r| Ok(r[0] == Value::Int(2)),
                |_| Ok(vec![Value::Int(2), Value::Int(22)]),
            )
            .unwrap();
            let key = txn_wal_buffer_key(&db.log, writer.txn_id);
            assert!(
                txn_wal_buffer_shard(key)
                    .lock()
                    .get(&key)
                    .unwrap()
                    .entries
                    .iter()
                    .any(|(payload, _)| matches!(payload, WalPayload::HeapUpdateLogical { .. })),
                "fixture must buffer a physical prefix mutation"
            );
            set_update_delta_enabled_for_test(false);
        } else {
            edge_update(&db, writer, 2, 22);
        }
        if scenario.starts_with("foreign-watermark") {
            let foreign = db.begin_txn().unwrap();
            edge_update(&db, foreign, 1, 15);
            db.commit_txn(foreign.txn_id).unwrap();
            assert!(
                db.log_state_atomics.next_lsn() - 1 > db.log.lock().last_txn_lsn(writer.txn_id)
            );
        }
        let sp = db.statement_savepoint(writer.txn_id).unwrap();
        if scenario == "insert" {
            db.insert_rows(writer, "test.t", vec![vec![Value::Int(3), Value::Int(99)]])
                .unwrap();
        } else {
            edge_update(&db, writer, 1, 99);
        }
        if scenario == "midflush" {
            db.flush_txn_wal_buffer(writer.txn_id, None).unwrap();
            edge_update(&db, writer, 1, 88);
        }
        db.rollback_to_statement_savepoint(writer.txn_id, sp.clone())
            .unwrap();
        if scenario == "repeated" || scenario == "foreign-watermark-repeated" {
            edge_update(&db, writer, 1, 88);
            db.rollback_to_statement_savepoint(writer.txn_id, sp)
                .unwrap();
        }
        let other = db.begin_txn().unwrap();
        if scenario == "insert" {
            db.insert_rows(other, "test.t", vec![vec![Value::Int(3), Value::Int(77)]])
                .unwrap();
        } else {
            edge_update(&db, other, 1, 77);
        }
        db.commit_txn(other.txn_id).unwrap();
        if scenario == "commit" {
            db.commit_txn(writer.txn_id).unwrap();
        }
        if scenario == "checkpoint" {
            db.checkpoint().unwrap();
        }
        // Otherwise writer remains an active crash loser. Its original row 1
        // WAL must be skipped so recovery cannot overwrite other's value 77.
    }
    std::process::exit(0);
}

#[test]
fn fresh_audit_crash_undo_preserves_mixed_deferred_update_and_eager_delete_order() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let config = test_storage_config(DurabilityMode::Strict, true);
    let db = Arc::new(Database::open_with_config(dir.path(),config.clone()).unwrap());
    db.create_table("t",vec![lob_col("id",DataType::Int,true),lob_col("v",DataType::Int,false)],vec!["id".into()],vec![]).unwrap();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(seed,"test.t",vec![vec![Value::Int(1),Value::Int(10)]]).unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let loser = db.begin_dml_txn().unwrap();
    db.update_rows(loser,"test.t",None,|row|Ok(row[0]==Value::Int(1)),|_|Ok(vec![Value::Int(1),Value::Int(999)])).unwrap();
    db.delete_rows(loser,"test.t",None,|row|Ok(row[0]==Value::Int(1))).unwrap();
    for id in 100..400 {
        db.insert_rows(loser,"test.t",vec![vec![Value::Int(id),Value::Int(id)]]).unwrap();
    }
    db.checkpoint().unwrap();
    std::mem::forget(db);
    let db = Database::open_with_config(dir.path(),config).unwrap();
    let rows = db.scan_rows("test.t",TxnView::read_only(u64::MAX,IsolationLevel::ReadCommitted)).unwrap();
    assert_eq!(rows.len(),1);
    assert_eq!(rows[0].values,vec![Value::Int(1),Value::Int(10)]);
}

fn fix_crash_reopen(scenario: &str) {
    let dir = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "storage::core::tests::mvcc_edge_tests::mvcc_fix_crash_child",
            "--nocapture",
        ])
        .env("MYDB_MVCC_FIX_CHILD_DIR", dir.path())
        .env("MYDB_MVCC_FIX_SCENARIO", scenario)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let db = Arc::new(
        Database::open_with_config(
            dir.path(),
            test_storage_config(DurabilityMode::Strict, true),
        )
        .unwrap(),
    );
    if scenario.starts_with("lob") {
        if scenario.contains("xa") {
            let xid = crate::xa::Xid::new(1, b"compensated".to_vec(), vec![]).unwrap();
            assert_eq!(db.list_prepared_xa().len(), 1);
            if scenario.ends_with("rollback") {
                db.rollback_prepared(&xid).unwrap();
            } else {
                db.commit_prepared(&xid).unwrap();
            }
        }
        for _ in 0..3 {
            db.purge_obsolete_undo();
        }
        let writer = db.begin_txn().unwrap();
        db.insert_rows(
            writer,
            "test.t",
            vec![vec![Value::Int(2), Value::String("Z".repeat(24000))]],
        )
        .unwrap();
        db.commit_txn(writer.txn_id).unwrap();
        assert_eq!(scan_body(&db, 1), Some(Value::String("A".repeat(12000))));
    } else {
        let expected = if scenario == "insert" {
            vec![(1, 10), (2, 20), (3, 77)]
        } else {
            vec![(1, 77), (2, if scenario == "commit" { 22 } else { 20 })]
        };
        assert_eq!(edge_current(&db), expected);
    }
    db.checkpoint().unwrap();
    drop(db);
    let db = Arc::new(
        Database::open_with_config(
            dir.path(),
            test_storage_config(DurabilityMode::Strict, true),
        )
        .unwrap(),
    );
    if scenario.starts_with("lob") {
        assert_eq!(scan_body(&db, 1), Some(Value::String("A".repeat(12000))));
    } else {
        let expected = if scenario == "insert" {
            vec![(1, 10), (2, 20), (3, 77)]
        } else {
            vec![(1, 77), (2, if scenario == "commit" { 22 } else { 20 })]
        };
        assert_eq!(edge_current(&db), expected);
    }
}

#[test]
fn mvcc_fix_crash_buffered_statement() {
    fix_crash_reopen("buffered");
}
#[test]
fn mvcc_fix_crash_unbuffered_statement() {
    fix_crash_reopen("unbuffered");
}
#[test]
fn mvcc_fix_crash_after_buffer_midflush() {
    fix_crash_reopen("midflush");
}
#[test]
fn mvcc_fix_crash_after_statement_commit() {
    fix_crash_reopen("commit");
}
#[test]
fn mvcc_fix_crash_active_checkpoint() {
    fix_crash_reopen("checkpoint");
}
#[test]
fn mvcc_fix_crash_lob_compensation() {
    fix_crash_reopen("lob");
}
#[test]
fn mvcc_fix_crash_lob_checkpoint() {
    fix_crash_reopen("lob-checkpoint");
}
#[test]
fn mvcc_fix_crash_lob_prepared() {
    fix_crash_reopen("lob-xa");
}

#[test]
fn mvcc_fix_table_mode_lattice_and_all_holder_pairs() {
    use TableLockMode::*;
    let modes = [
        IntentShared,
        IntentExclusive,
        Shared,
        ShareIntentExclusive,
        Exclusive,
    ];
    let expected = [
        [
            IntentShared,
            IntentExclusive,
            Shared,
            ShareIntentExclusive,
            Exclusive,
        ],
        [
            IntentExclusive,
            IntentExclusive,
            ShareIntentExclusive,
            ShareIntentExclusive,
            Exclusive,
        ],
        [
            Shared,
            ShareIntentExclusive,
            Shared,
            ShareIntentExclusive,
            Exclusive,
        ],
        [
            ShareIntentExclusive,
            ShareIntentExclusive,
            ShareIntentExclusive,
            ShareIntentExclusive,
            Exclusive,
        ],
        [Exclusive; 5],
    ];
    for (a, &held) in modes.iter().enumerate() {
        for (b, &requested) in modes.iter().enumerate() {
            assert_eq!(held.combine(requested), expected[a][b]);
            for &other in &modes {
                if !TableLockMode::is_compatible(held, other) {
                    continue;
                }
                let locks = ShardedTableLocks::new();
                assert!(matches!(
                    locks.try_acquire(1, 7, held),
                    TableAcquireOutcome::Acquired { .. }
                ));
                assert!(matches!(
                    locks.try_acquire(2, 7, other),
                    TableAcquireOutcome::Acquired { .. }
                ));
                let result = locks.try_acquire(1, 7, requested);
                let compatible = TableLockMode::is_compatible(expected[a][b], other);
                assert_eq!(
                    !matches!(result, TableAcquireOutcome::Conflict { .. }),
                    compatible,
                    "{held:?} + {requested:?}, other {other:?}"
                );
                locks.release_txn(2);
                assert!(!matches!(
                    locks.try_acquire(1, 7, requested),
                    TableAcquireOutcome::Conflict { .. }
                ));
            }
        }
    }
}

#[test]
fn mvcc_fix_invalid_savepoint_does_not_quarantine() {
    let (_dir, db) = edge_db();
    let writer = db.begin_txn().unwrap();
    assert!(db
        .rollback_to_named_savepoint(writer.txn_id, "missing")
        .is_err());
    assert!(!db.mvcc_quarantined.load(Ordering::Acquire));
    edge_update(&db, writer, 1, 11);
    db.commit_txn(writer.txn_id).unwrap();
    assert_eq!(edge_current(&db), vec![(1, 11), (2, 20)]);
}

#[test]
fn mvcc_fix_lob_update_prefix_keys_and_repeated_purge() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_lob_table(
        dir.path(),
        &test_storage_config(DurabilityMode::Strict, true),
    );
    db.add_index(
        "test.t",
        IndexSchema {
            readable_from_scn: 0,
            index_id: 0,
            name: "body_idx".into(),
            columns: vec!["body".into()],
            column_prefix_lengths: vec![Some(8)],
            column_directions: vec![],
            key_expressions: vec![],
            unique: false,
            primary: false,
            included_columns: vec![],
            state: IndexState::Public,
            index_type: "BTREE".into(),
            visible: true,
            comment: String::new(),
            fulltext: None,
        },
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.t",
        vec![vec![Value::Int(1), Value::String("A".repeat(12000))]],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let table = db.table("test.t").unwrap();
    let index = table.indexes.iter().find(|i| i.name == "body_idx").unwrap();
    for named in [false, true] {
        let writer = db.begin_dml_txn().unwrap();
        db.create_named_savepoint(writer.txn_id, "s".into())
            .unwrap();
        db.update_rows(
            writer,
            "test.t",
            None,
            |_| Ok(true),
            |_| Ok(vec![Value::Int(1), Value::String("B".repeat(12000))]),
        )
        .unwrap();
        if named {
            db.rollback_to_named_savepoint(writer.txn_id, "s").unwrap();
            db.commit_txn(writer.txn_id).unwrap();
        } else {
            db.rollback_txn(writer.txn_id).unwrap();
        }
        for _ in 0..3 {
            db.purge_obsolete_undo();
        }
        for (letter, expected) in [("A", 1), ("B", 0)] {
            let key = table
                .index_keys(index, &[Value::Int(1), Value::String(letter.repeat(12000))])
                .unwrap()
                .remove(0);
            assert_eq!(db.lookup_index(&table, index, key).unwrap().len(), expected);
        }
        assert_eq!(scan_body(&db, 1), Some(Value::String("A".repeat(12000))));
    }
}

#[test]
fn mvcc_fix_statement_boundary_survives_two_compactions() {
    std::env::set_var("MYDB_UNDO_WRITE_SHARDS", "1");
    let (_dir, db) = edge_db();
    db.set_undo_segment_bytes(512).unwrap();
    let writer = db.begin_dml_txn().unwrap();
    edge_update(&db, writer, 1, 11);
    let saved = db.statement_savepoint(writer.txn_id).unwrap();
    let mut previous = db.txns.read().undo_head(writer.txn_id).unwrap();
    for pass in 0..2 {
        for value in 0..50 {
            let churn = db.begin_txn().unwrap();
            edge_update(&db, churn, 2, pass * 100 + value);
            db.commit_txn(churn.txn_id).unwrap();
        }
        db.purge_obsolete_undo();
        db.compact_undo_segments(100, 64).unwrap();
        let relocated = db.txns.read().undo_head(writer.txn_id).unwrap();
        assert_ne!(relocated, previous, "fixture must relocate on pass {pass}");
        previous = relocated;
        edge_update(&db, writer, 1, 99);
        db.rollback_to_statement_savepoint(writer.txn_id, saved.clone())
            .unwrap();
        assert_eq!(
            edge_rows(
                &db,
                TxnView {
                    snapshot_scn: db.visible_scn(),
                    ..writer
                }
            )[0],
            (1, 11)
        );
    }
    db.commit_txn(writer.txn_id).unwrap();
    assert_eq!(edge_current(&db)[0], (1, 11));
}

#[test]
fn mvcc_fix_late_registration_cache_miss_does_not_expose_writer() {
    let (_dir, db) = edge_db();
    // Split exactly the allocation/registration steps used by begin_txn.
    let reserved = db.next_txn_id_atomic.fetch_add(1, Ordering::AcqRel);
    let later = db.begin_txn().unwrap();
    db.commit_txn(later.txn_id).unwrap();
    db.advance_min_retained_txn_id();
    assert!(db.min_retained_txn_id.load(Ordering::Acquire) < reserved);
    let writer = db
        .txns
        .write()
        .begin_with_id(reserved, db.visible_scn(), false, true, 0)
        .unwrap();
    db.append_wal(
        reserved,
        WalPayload::BeginTxn {
            start_scn: writer.snapshot_scn,
        },
    )
    .unwrap();
    db.slot_status_cache.insert_active(reserved);
    edge_update(&db, writer, 1, 99);
    db.slot_status_cache.remove(reserved);
    assert_eq!(edge_current(&db), vec![(1, 10), (2, 20)]);
    db.commit_txn(reserved).unwrap();
    assert_eq!(edge_current(&db), vec![(1, 99), (2, 20)]);
}

#[test]
fn mvcc_fix_compensated_lob_survives_compaction_and_reopen() {
    std::env::set_var("MYDB_UNDO_WRITE_SHARDS", "1");
    let dir = tempfile::tempdir().unwrap();
    let config = test_storage_config(DurabilityMode::Strict, true);
    let db = open_lob_table(dir.path(), &config);
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.t",
        vec![
            vec![Value::Int(1), Value::String("A".repeat(12000))],
            vec![Value::Int(2), Value::String("churn".into())],
        ],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    db.set_undo_segment_bytes(512).unwrap();
    let writer = db.begin_txn().unwrap();
    db.create_named_savepoint(writer.txn_id, "s".into())
        .unwrap();
    db.update_rows(
        writer,
        "test.t",
        None,
        |r| Ok(r[0] == Value::Int(1)),
        |_| Ok(vec![Value::Int(1), Value::String("B".repeat(12000))]),
    )
    .unwrap();
    let original = db.txns.read().undo_head(writer.txn_id).unwrap().unwrap();
    db.rollback_to_named_savepoint(writer.txn_id, "s").unwrap();
    for v in 0..50 {
        let churn = db.begin_txn().unwrap();
        db.update_rows(
            churn,
            "test.t",
            None,
            |r| Ok(r[0] == Value::Int(2)),
            |_| Ok(vec![Value::Int(2), Value::String(format!("churn-{v}"))]),
        )
        .unwrap();
        db.commit_txn(churn.txn_id).unwrap();
    }
    db.purge_obsolete_undo();
    db.compact_undo_segments(100, 64).unwrap();
    let relocated = db.undo.read().canonical_ptr(original);
    assert_ne!(relocated, original);
    assert!(db.undo.read().compensated.contains(&relocated));
    assert_eq!(db.undo.read().wal_identity(relocated), original);
    db.commit_txn(writer.txn_id).unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let db = Arc::new(Database::open_with_config(dir.path(), config).unwrap());
    for _ in 0..3 {
        db.purge_obsolete_undo();
    }
    assert_eq!(scan_body(&db, 1), Some(Value::String("A".repeat(12000))));
}

#[test]
fn mvcc_fix_crash_mixed_eager_buffered_prefix() {
    fix_crash_reopen("mixed");
}
#[test]
fn mvcc_fix_crash_savepoint_foreign_allocation_watermark() {
    fix_crash_reopen("foreign-watermark");
}
#[test]
fn mvcc_fix_crash_repeated_savepoint_foreign_allocation_watermark() {
    fix_crash_reopen("foreign-watermark-repeated");
}
#[test]
fn mvcc_fix_crash_repeated_statement_rollback() {
    fix_crash_reopen("repeated");
}
#[test]
fn mvcc_fix_crash_reused_insert_after_statement_rollback() {
    fix_crash_reopen("insert");
}
#[test]
fn mvcc_fix_crash_lob_prepared_rollback() {
    fix_crash_reopen("lob-xa-rollback");
}

#[test]
fn mvcc_fix_noop_savepoint_keeps_wal_buffer_parked() {
    let (_dir, db) = edge_db();
    let writer = db.begin_dml_txn().unwrap();
    edge_update(&db, writer, 1, 11);
    let sp = db.statement_savepoint(writer.txn_id).unwrap();
    let before = txn_wal_buffer_len(&db.log, writer.txn_id);
    assert!(before > 0);
    db.rollback_to_statement_savepoint(writer.txn_id, sp)
        .unwrap();
    assert_eq!(txn_wal_buffer_len(&db.log, writer.txn_id), before);
    db.commit_txn(writer.txn_id).unwrap();
    assert_eq!(edge_current(&db), vec![(1, 11), (2, 20)]);
}

#[test]
fn buffered_statement_savepoint_does_not_wait_for_wal_mutex() {
    let (_dir, db) = edge_db();
    let writer = db.begin_dml_txn().unwrap();
    edge_update(&db, writer, 1, 11);
    assert!(txn_wal_buffering_active(&db.log, writer.txn_id));
    let guard = db.log.lock();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker_db = Arc::clone(&db);
    let worker = std::thread::spawn(move || {
        tx.send(worker_db.statement_savepoint(writer.txn_id)).unwrap();
    });
    let completed = rx.recv_timeout(std::time::Duration::from_secs(1));
    // Release the latch before joining even on failure, so the regression
    // reports an assertion instead of leaving a blocked test worker behind.
    drop(guard);
    worker.join().unwrap();
    assert!(
        completed.is_ok(),
        "buffered savepoint waited for the global WAL mutex"
    );
    completed.unwrap().unwrap();
    db.rollback_txn(writer.txn_id).unwrap();
}

#[test]
fn buffered_savepoint_foreign_watermark_preserves_transaction_backlink() {
    let (_dir, db) = edge_db();
    let writer = db.begin_dml_txn().unwrap();
    edge_update(&db, writer, 2, 22);
    db.flush_txn_wal_buffer(writer.txn_id, None).unwrap();
    let own_lsn = db.log.lock().last_txn_lsn(writer.txn_id);
    let foreign = db.begin_txn().unwrap();
    edge_update(&db, foreign, 1, 15);
    db.commit_txn(foreign.txn_id).unwrap();
    let sp = db.statement_savepoint(writer.txn_id).unwrap();
    let boundary = sp.wal_boundary.as_ref().unwrap();
    assert!(boundary.eager_lsn > own_lsn);
    // An empty buffer still resolves saved cursors. The skip interval must
    // remain ordered even though this txn has no record at the global bound.
    db.flush_txn_wal_buffer(writer.txn_id, None).unwrap();
    assert_eq!(boundary.last_lsn.load(Ordering::Acquire), own_lsn);
    assert_eq!(
        boundary.prefix_lsn.load(Ordering::Acquire),
        boundary.eager_lsn
    );
    edge_update(&db, writer, 2, 99);
    db.rollback_to_statement_savepoint(writer.txn_id, sp).unwrap();
    db.commit_txn(writer.txn_id).unwrap();
    assert_eq!(edge_current(&db), vec![(1, 15), (2, 22)]);
}

#[test]
fn mvcc_fix_compaction_preserves_pending_vector_identity() {
    std::env::set_var("MYDB_UNDO_WRITE_SHARDS", "1");
    let (_dir, db) = edge_db();
    db.set_undo_segment_bytes(512).unwrap();
    let writer = db.begin_txn().unwrap();
    edge_update(&db, writer, 1, 11);
    let original = db.txns.read().undo_head(writer.txn_id).unwrap().unwrap();
    let table = db.table("test.t").unwrap();
    let row_id = db
        .scan_rows("test.t", writer)
        .unwrap()
        .into_iter()
        .find(|row| row.values[0] == Value::Int(1))
        .unwrap()
        .row_id;
    // White-box fixture isolates the identity used by both ANN and spatial
    // deferred maintenance. No graph access is needed to verify queue pruning.
    db.pending_vector_deletes.lock().unwrap().insert(
        writer.txn_id,
        vec![PendingVectorMutation {
            undo_id: original,
            table_id: table.id,
            storage_id: table.id,
            row_id,
            new_values: Some(vec![Value::Int(1), Value::Int(11)]),
            old_values: None,
        }],
    );
    for v in 0..50 {
        let churn = db.begin_txn().unwrap();
        edge_update(&db, churn, 2, v);
        db.commit_txn(churn.txn_id).unwrap();
    }
    db.purge_obsolete_undo();
    db.compact_undo_segments(100, 64).unwrap();
    assert_ne!(
        db.txns.read().undo_head(writer.txn_id).unwrap(),
        Some(original)
    );
    db.create_named_savepoint(writer.txn_id, "after_compaction".into())
        .unwrap();
    db.rollback_to_named_savepoint(writer.txn_id, "after_compaction")
        .unwrap();
    assert_eq!(
        db.pending_vector_deletes
            .lock()
            .unwrap()
            .get(&writer.txn_id)
            .map(Vec::len),
        Some(1)
    );
    db.rollback_txn(writer.txn_id).unwrap();
}

#[test]
fn mvcc_fix_checkpoint_does_not_certify_allocated_registration_holes() {
    let (_dir, db) = edge_db();
    let reserved = db.next_txn_id_atomic.fetch_add(1, Ordering::AcqRel);
    let later = db.begin_txn().unwrap();
    db.commit_txn(later.txn_id).unwrap();
    db.checkpoint().unwrap();
    let records = LogManager::scan(&db.data_dir).unwrap().records;
    let proof = records
        .iter()
        .rev()
        .find_map(|record| match record.payload {
            WalPayload::CheckpointEnd {
                gc_safe_txn_frontier,
                ..
            } => Some(gc_safe_txn_frontier),
            _ => None,
        })
        .unwrap();
    assert!(proof < reserved);
}

#[test]
fn mvcc_fix_checkpoint_excludes_registered_but_unlogged_begin() {
    let (_dir, db) = edge_db();
    let id = db.next_txn_id_atomic.fetch_add(1, Ordering::AcqRel);
    db.txns
        .write()
        .begin_with_id(id, db.visible_scn(), false, false, 0)
        .unwrap();
    let later = db.begin_txn().unwrap();
    db.commit_txn(later.txn_id).unwrap();
    db.advance_min_retained_txn_id();
    db.checkpoint().unwrap();
    let records = LogManager::scan(&db.data_dir).unwrap().records;
    let proof = records
        .iter()
        .rev()
        .find_map(|record| match record.payload {
            WalPayload::CheckpointEnd {
                gc_safe_txn_frontier,
                ..
            } => Some(gc_safe_txn_frontier),
            _ => None,
        })
        .unwrap();
    assert!(
        proof < id,
        "replica must not cross a source BEGIN still pending WAL publication"
    );
    db.rollback_txn(id).unwrap();
}
