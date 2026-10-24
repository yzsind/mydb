// MVCC boundary tests with pinned snapshots and deterministic interleavings.
// Expected values come from a separate row-state oracle, never from undo.
#[cfg(test)]
use super::*;

fn edge_db() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Database::open_with_config(
            dir.path(),
            test_storage_config(DurabilityMode::Strict, true),
        )
        .unwrap(),
    );
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
    (dir, db)
}

fn edge_rows(db: &Database, view: TxnView) -> Vec<(i64, i64)> {
    let mut rows = db
        .scan_rows("test.t", view)
        .unwrap()
        .into_iter()
        .map(|r| match r.values.as_slice() {
            [Value::Int(id), Value::Int(v)] => (*id, *v),
            other => panic!("unexpected row: {other:?}"),
        })
        .collect::<Vec<_>>();
    rows.sort_unstable();
    rows
}

fn edge_update(db: &Database, txn: TxnView, id: i64, value: i64) {
    assert_eq!(
        db.update_rows(
            txn,
            "test.t",
            None,
            |r| Ok(r[0] == Value::Int(id)),
            |_| Ok(vec![Value::Int(id), Value::Int(value)]),
        )
        .unwrap(),
        1
    );
}

fn edge_current(db: &Arc<Database>) -> Vec<(i64, i64)> {
    let (view, _guard) =
        db.read_only_statement_view_with_guard(IsolationLevel::ReadCommitted, None);
    edge_rows(db, view)
}

fn edge_mixed_dml(commit: bool) {
    let (_dir, db) = edge_db();
    let (old, _old_guard) =
        db.read_only_statement_view_with_guard(IsolationLevel::RepeatableRead, None);
    let writer = db.begin_txn().unwrap();
    edge_update(&db, writer, 1, 11);
    db.delete_rows(writer, "test.t", None, |r| Ok(r[0] == Value::Int(2)))
        .unwrap();
    db.insert_rows(writer, "test.t", vec![vec![Value::Int(3), Value::Int(30)]])
        .unwrap();
    let changed = vec![(1, 11), (3, 30)];
    assert_eq!(
        edge_rows(&db, writer),
        changed,
        "read own insert/update/delete"
    );
    assert_eq!(edge_rows(&db, old), vec![(1, 10), (2, 20)]);
    assert_eq!(
        edge_current(&db),
        vec![(1, 10), (2, 20)],
        "no dirty RC reads"
    );
    let dirty = TxnView::read_only(db.visible_scn(), IsolationLevel::ReadUncommitted);
    assert_eq!(edge_rows(&db, dirty), changed);
    if commit {
        db.commit_txn(writer.txn_id).unwrap();
    } else {
        db.rollback_txn(writer.txn_id).unwrap();
    }
    assert_eq!(
        edge_rows(&db, old),
        vec![(1, 10), (2, 20)],
        "fixed snapshot survives resolution"
    );
    let expected = if commit {
        changed
    } else {
        vec![(1, 10), (2, 20)]
    };
    assert_eq!(edge_current(&db), expected);
    assert_eq!(
        edge_rows(&db, dirty),
        expected,
        "RU must not cache the active version"
    );
}

#[test]
fn mvcc_edge_mixed_dml_commit_isolation() {
    edge_mixed_dml(true);
}
#[test]
fn mvcc_edge_mixed_dml_rollback_isolation() {
    edge_mixed_dml(false);
}

#[test]
fn mvcc_edge_reverse_begin_commit_order_and_slot_gc() {
    let (_dir, db) = edge_db();
    let first = db.begin_txn().unwrap();
    let second = db.begin_txn().unwrap();
    edge_update(&db, first, 1, 11);
    edge_update(&db, second, 2, 22);
    db.commit_txn(second.txn_id).unwrap();
    let (middle, _guard) =
        db.read_only_statement_view_with_guard(IsolationLevel::RepeatableRead, None);
    assert_eq!(edge_rows(&db, middle), vec![(1, 10), (2, 22)]);
    db.commit_txn(first.txn_id).unwrap();
    for _ in 0..3 {
        db.purge_obsolete_undo();
        db.advance_min_retained_txn_id();
    }
    assert_eq!(
        edge_rows(&db, middle),
        vec![(1, 10), (2, 22)],
        "txn id is not commit order"
    );
    assert_eq!(edge_current(&db), vec![(1, 11), (2, 22)]);
}

#[test]
fn mvcc_edge_generation_oracle_with_partial_reader_release() {
    let (_dir, db) = edge_db();
    let mut expected = vec![(1, 10), (2, 20)];
    let mut readers = Vec::new();
    // Exercise many versions, alternating writers, repeated own writes,
    // committed and aborted transactions, and incremental snapshot release.
    for generation in 0..36i64 {
        let (view, guard) =
            db.read_only_statement_view_with_guard(IsolationLevel::RepeatableRead, None);
        readers.push((view, guard, expected.clone()));
        let writer = db.begin_txn().unwrap();
        let id = 1 + generation % 2;
        edge_update(&db, writer, id, 100 + generation);
        edge_update(&db, writer, id, 200 + generation);
        if generation % 4 == 0 {
            db.rollback_txn(writer.txn_id).unwrap();
        } else {
            db.commit_txn(writer.txn_id).unwrap();
            expected[(id - 1) as usize].1 = 200 + generation;
        }
        if readers.len() > 8 {
            readers.remove(0);
        }
        db.purge_obsolete_undo();
        db.advance_min_retained_txn_id();
        for (view, _, oracle) in &readers {
            assert_eq!(
                &edge_rows(&db, *view),
                oracle,
                "generation {generation}, SCN {}",
                view.snapshot_scn
            );
        }
        assert_eq!(edge_current(&db), expected);
    }
    drop(readers);
    for _ in 0..3 {
        db.purge_obsolete_undo();
        db.advance_min_retained_txn_id();
    }
    assert_eq!(edge_current(&db), expected);
}

#[test]
fn mvcc_edge_delete_reinsert_same_key_retains_three_generations() {
    let (_dir, db) = edge_db();
    let (before, _g0) =
        db.read_only_statement_view_with_guard(IsolationLevel::RepeatableRead, None);
    let deleted = db.begin_txn().unwrap();
    db.delete_rows(deleted, "test.t", None, |r| Ok(r[0] == Value::Int(1)))
        .unwrap();
    db.commit_txn(deleted.txn_id).unwrap();
    let (absent, _g1) =
        db.read_only_statement_view_with_guard(IsolationLevel::RepeatableRead, None);
    let inserted = db.begin_txn().unwrap();
    db.insert_rows(
        inserted,
        "test.t",
        vec![vec![Value::Int(1), Value::Int(99)]],
    )
    .unwrap();
    db.commit_txn(inserted.txn_id).unwrap();
    let (after, _g2) = db.read_only_statement_view_with_guard(IsolationLevel::RepeatableRead, None);
    db.purge_obsolete_undo();
    db.purge_obsolete_index_repairs();
    let table = db.table("test.t").unwrap();
    let primary = table.indexes.iter().find(|i| i.primary).unwrap();
    for (view, expected) in [(before, Some(10)), (absent, None), (after, Some(99))] {
        let ids = db
            .lookup_index_visible(&table, primary, IndexKey::from(vec![KeyPart::Int(1)]), view)
            .unwrap();
        let mut values = Vec::new();
        for id in ids {
            if let Some(row) = db.read_row(&table, id, view).unwrap() {
                if row.values[0] == Value::Int(1) {
                    values.push(row.values[1].clone());
                }
            }
        }
        assert_eq!(
            values,
            expected.map(Value::Int).into_iter().collect::<Vec<_>>(),
            "index path at SCN {}",
            view.snapshot_scn
        );
        assert_eq!(
            edge_rows(&db, view)
                .into_iter()
                .find(|r| r.0 == 1)
                .map(|r| r.1),
            expected
        );
    }
}

#[test]
fn mvcc_edge_batch_insert_update_delete_savepoint_and_reuse() {
    let (_dir, db) = edge_db();
    let (old, _guard) =
        db.read_only_statement_view_with_guard(IsolationLevel::RepeatableRead, None);
    let writer = db.begin_txn().unwrap();
    db.create_named_savepoint(writer.txn_id, "empty".into())
        .unwrap();
    db.insert_rows(
        writer,
        "test.t",
        (3..35)
            .map(|id| vec![Value::Int(id), Value::Int(id * 10)])
            .collect(),
    )
    .unwrap();
    edge_update(&db, writer, 3, 333);
    db.delete_rows(writer, "test.t", None, |r| Ok(r[0] == Value::Int(4)))
        .unwrap();
    db.rollback_to_named_savepoint(writer.txn_id, "empty")
        .unwrap();
    assert_eq!(edge_rows(&db, writer), vec![(1, 10), (2, 20)]);
    db.insert_rows(
        writer,
        "test.t",
        vec![vec![Value::Int(3), Value::Int(3000)]],
    )
    .unwrap();
    db.commit_txn(writer.txn_id).unwrap();
    db.purge_obsolete_undo();
    assert_eq!(edge_rows(&db, old), vec![(1, 10), (2, 20)]);
    assert_eq!(edge_current(&db), vec![(1, 10), (2, 20), (3, 3000)]);
}

#[test]
fn mvcc_edge_repeated_named_savepoint_rollback_then_full_abort() {
    let (_dir, db) = edge_db();
    let writer = db.begin_txn().unwrap();
    edge_update(&db, writer, 1, 11);
    db.create_named_savepoint(writer.txn_id, "s".into())
        .unwrap();
    for value in 12..16 {
        edge_update(&db, writer, 1, value);
        db.rollback_to_named_savepoint(writer.txn_id, "s").unwrap();
        assert_eq!(edge_rows(&db, writer), vec![(1, 11), (2, 20)]);
        assert_eq!(edge_current(&db), vec![(1, 10), (2, 20)]);
    }
    db.rollback_txn(writer.txn_id).unwrap();
    db.purge_obsolete_undo();
    assert_eq!(edge_current(&db), vec![(1, 10), (2, 20)]);
}

#[test]
fn mvcc_edge_savepoint_replacement_and_later_invalidation() {
    let (_dir, db) = edge_db();
    let writer = db.begin_txn().unwrap();
    db.create_named_savepoint(writer.txn_id, "s".into())
        .unwrap();
    edge_update(&db, writer, 1, 11);
    db.create_named_savepoint(writer.txn_id, "s".into())
        .unwrap();
    edge_update(&db, writer, 1, 12);
    db.create_named_savepoint(writer.txn_id, "later".into())
        .unwrap();
    edge_update(&db, writer, 1, 13);
    db.rollback_to_named_savepoint(writer.txn_id, "s").unwrap();
    assert_eq!(edge_rows(&db, writer), vec![(1, 11), (2, 20)]);
    assert_eq!(
        db.rollback_to_named_savepoint(writer.txn_id, "later")
            .unwrap_err()
            .code(),
        1305
    );
    db.commit_txn(writer.txn_id).unwrap();
    assert_eq!(edge_current(&db), vec![(1, 11), (2, 20)]);
}

fn edge_prepared(commit: bool) {
    let (_dir, db) = edge_db();
    let writer = db.begin_txn().unwrap();
    edge_update(&db, writer, 1, 99);
    let xid = crate::xa::Xid::new(1, b"mvcc-edge".to_vec(), vec![]).unwrap();
    db.prepare_xa(writer.txn_id, 0, xid.clone()).unwrap();
    let (prepared_view, _guard) =
        db.read_only_statement_view_with_guard(IsolationLevel::RepeatableRead, None);
    for _ in 0..3 {
        db.purge_obsolete_undo();
        db.advance_min_retained_txn_id();
        assert_eq!(edge_rows(&db, prepared_view), vec![(1, 10), (2, 20)]);
    }
    assert!(db.is_txn_active(writer.txn_id));
    let dirty = TxnView::read_only(db.visible_scn(), IsolationLevel::ReadUncommitted);
    assert_eq!(edge_rows(&db, dirty), vec![(1, 99), (2, 20)]);
    if commit {
        db.commit_prepared(&xid).unwrap();
    } else {
        db.rollback_prepared(&xid).unwrap();
    }
    assert_eq!(edge_rows(&db, prepared_view), vec![(1, 10), (2, 20)]);
    assert_eq!(
        edge_current(&db),
        vec![(1, if commit { 99 } else { 10 }), (2, 20)]
    );
    assert!(!db.is_txn_active(writer.txn_id));
}

#[test]
fn mvcc_edge_prepared_commit_after_snapshot_and_purge() {
    edge_prepared(true);
}
#[test]
fn mvcc_edge_prepared_rollback_after_snapshot_and_purge() {
    edge_prepared(false);
}

#[test]
fn mvcc_edge_scan_cache_collision_and_commit_boundary() {
    let (_dir, db) = edge_db();
    let first = db.begin_txn().unwrap();
    let mut others = Vec::new();
    for _ in 0..32 {
        others.push(db.begin_txn().unwrap());
    }
    let last = *others.last().unwrap();
    assert_eq!(
        first.txn_id % 32,
        last.txn_id % 32,
        "force direct-map collision"
    );
    let commit_scn = db.commit_txn(first.txn_id).unwrap();
    let view = TxnView::read_only(commit_scn, IsolationLevel::ReadCommitted);
    let mut cache = ScanVisibilityCache::new();
    for _ in 0..4 {
        assert!(db
            .row_version_visible_cached(&mut cache, RowHeader::visible(first.txn_id), view)
            .unwrap());
        assert!(!db
            .row_version_visible_cached(&mut cache, RowHeader::visible(last.txn_id), view)
            .unwrap());
    }
    let before = TxnView::read_only(commit_scn - 1, IsolationLevel::ReadCommitted);
    assert!(!db
        .row_version_visible(RowHeader::visible(first.txn_id), before)
        .unwrap());
    // The same fixed scan cannot begin seeing a writer that commits later.
    db.commit_txn(last.txn_id).unwrap();
    assert!(!db
        .row_version_visible_cached(&mut cache, RowHeader::visible(last.txn_id), view)
        .unwrap());
    assert!(!db
        .row_version_visible(RowHeader::visible(last.txn_id), view)
        .unwrap());
    for txn in others.into_iter().take(31) {
        db.rollback_txn(txn.txn_id).unwrap();
    }
}

#[test]
fn mvcc_edge_rc_refresh_and_rr_pinned_read_own_writes() {
    let (_dir, db) = edge_db();
    let reader = db.begin_txn().unwrap();
    let (rr, _rr_guard) = db.session_statement_view_with_guard(
        reader.txn_id,
        IsolationLevel::RepeatableRead,
        None,
        false,
    );
    let writer = db.begin_txn().unwrap();
    edge_update(&db, writer, 1, 11);
    db.commit_txn(writer.txn_id).unwrap();
    let (rc, _rc_guard) = db.session_statement_view_with_guard(
        reader.txn_id,
        IsolationLevel::ReadCommitted,
        None,
        false,
    );
    assert_eq!(edge_rows(&db, rr), vec![(1, 10), (2, 20)]);
    assert_eq!(edge_rows(&db, rc), vec![(1, 11), (2, 20)]);
    edge_update(&db, rr, 2, 22);
    assert_eq!(edge_rows(&db, rr), vec![(1, 10), (2, 22)]);
    db.rollback_txn(reader.txn_id).unwrap();
    assert_eq!(edge_current(&db), vec![(1, 11), (2, 20)]);
}

fn edge_corrupt_version(mutate: impl FnOnce(&mut UndoRecord), expected_message: &str) {
    let (_dir, db) = edge_db();
    let (old, _guard) =
        db.read_only_statement_view_with_guard(IsolationLevel::RepeatableRead, None);
    let writer = db.begin_txn().unwrap();
    edge_update(&db, writer, 1, 99);
    let head = db.txns.read().undo_head(writer.txn_id).unwrap().unwrap();
    let original = {
        let mut undo = db.undo.write();
        let record = undo.records.get_mut(&head).unwrap();
        assert!(
            record.resident,
            "fault injection targets a resident undo payload"
        );
        let original = record.clone();
        mutate(record);
        original
    };
    let result = db.scan_rows("test.t", old);
    // Restore the fixture before rollback; corruption must never be left to
    // accidentally test a second, unrelated cleanup path.
    db.undo.write().records.insert(head, original);
    db.rollback_txn(writer.txn_id).unwrap();
    let err = result.expect_err("corrupt undo must not silently return a row or no rows");
    assert_eq!(err.code(), crate::error::catalog::ER_GET_ERRNO.code().get());
    assert!(
        err.message().contains(expected_message),
        "{}",
        err.message()
    );
    assert_eq!(edge_current(&db), vec![(1, 10), (2, 20)]);
}

#[test]
fn mvcc_edge_corrupt_undo_missing_header_is_error() {
    edge_corrupt_version(|r| r.before_header = None, "missing before_header");
}

#[test]
fn mvcc_edge_corrupt_delta_cardinality_is_error() {
    edge_corrupt_version(
        |r| {
            r.kind = UndoKind::UpdateDelta {
                before_cols: vec![1, 0],
            };
            r.before_values = Some(vec![Value::Int(10)]);
        },
        "delta column/value length mismatch",
    );
}

#[test]
fn mvcc_edge_corrupt_delta_column_is_error() {
    edge_corrupt_version(
        |r| {
            r.kind = UndoKind::UpdateDelta {
                before_cols: vec![99],
            };
            r.before_values = Some(vec![Value::Int(10)]);
        },
        "exceeds row width",
    );
}

#[test]
fn mvcc_edge_corrupt_version_cycle_is_error() {
    edge_corrupt_version(
        |r| {
            r.kind = UndoKind::Update;
            r.before_values = Some(vec![Value::Int(1), Value::Int(10)]);
            r.before_header = Some(RowHeader {
                flags: 0,
                last_txn_id: r.txn_id,
                undo_ptr: Some(r.id),
            });
        },
        "cycle detected",
    );
}

#[test]
fn mvcc_edge_concurrent_partial_writer_then_atomic_commit() {
    use std::sync::mpsc::sync_channel;
    let (_dir, db) = edge_db();
    let (old, _guard) =
        db.read_only_statement_view_with_guard(IsolationLevel::RepeatableRead, None);
    let (ready_tx, ready_rx) = sync_channel(1);
    let (resume_tx, resume_rx) = sync_channel(1);
    let writer_db = Arc::clone(&db);
    let writer = std::thread::spawn(move || {
        let txn = writer_db.begin_txn().unwrap();
        edge_update(&writer_db, txn, 1, 11);
        ready_tx.send(()).unwrap();
        resume_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        edge_update(&writer_db, txn, 2, 22);
        writer_db.commit_txn(txn.txn_id).unwrap();
    });
    ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let while_partial = edge_current(&db);
    let (during, _during_guard) =
        db.read_only_statement_view_with_guard(IsolationLevel::RepeatableRead, None);
    resume_tx.send(()).unwrap();
    writer.join().unwrap();
    assert_eq!(while_partial, vec![(1, 10), (2, 20)]);
    assert_eq!(edge_rows(&db, old), vec![(1, 10), (2, 20)]);
    assert_eq!(edge_rows(&db, during), vec![(1, 10), (2, 20)]);
    assert_eq!(edge_current(&db), vec![(1, 11), (2, 22)]);
}

#[test]
fn mvcc_edge_mixed_lob_null_unicode_generations() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Database::open_with_config(
            dir.path(),
            test_storage_config(DurabilityMode::Strict, true),
        )
        .unwrap(),
    );
    let mut body = lob_col("body", DataType::Text(65_535), false);
    body.nullable = true;
    db.create_table(
        "t",
        vec![lob_col("id", DataType::Int, true), body],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let versions = [
        Value::String("初始🦀".repeat(3000)),
        Value::Null,
        Value::String(String::new()),
        Value::String("短值".into()),
        Value::String("终态é".repeat(4000)),
    ];
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.t",
        vec![vec![Value::Int(1), versions[0].clone()]],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let mut readers = Vec::new();
    for (generation, value) in versions.iter().enumerate() {
        if generation != 0 {
            let txn = db.begin_txn().unwrap();
            db.update_rows_with_changed_columns(
                txn,
                "test.t",
                None,
                Some(&[1]),
                |_| Ok(true),
                |_| Ok(vec![Value::Int(1), value.clone()]),
            )
            .unwrap();
            db.commit_txn(txn.txn_id).unwrap();
        }
        let (view, guard) =
            db.read_only_statement_view_with_guard(IsolationLevel::RepeatableRead, None);
        readers.push((view, guard, value.clone()));
        db.purge_obsolete_undo();
        for (view, _, expected) in &readers {
            let rows = db.scan_rows("test.t", *view).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(&rows[0].values[1], expected, "generation {generation}");
        }
    }
    let loser = db.begin_txn().unwrap();
    db.delete_rows(loser, "test.t", None, |_| Ok(true)).unwrap();
    db.rollback_txn(loser.txn_id).unwrap();
    drop(readers);
    db.purge_obsolete_undo();
    let (view, _guard) =
        db.read_only_statement_view_with_guard(IsolationLevel::ReadCommitted, None);
    assert_eq!(
        db.scan_rows("test.t", view).unwrap()[0].values[1],
        versions[4]
    );
}

#[test]
fn mvcc_edge_commit_gap_index_matches_heap_after_repair_purge() {
    let (_dir, db) = edge_db();
    // Model a committer paused immediately after allocating an SCN, before
    // publication. No reader exists yet: the frontier itself must pin history
    // required by the NEXT reader, including the old index key.
    let gap = db.next_scn_atomic.fetch_add(1, Ordering::AcqRel);
    let writer = db.begin_txn().unwrap();
    db.update_rows(
        writer,
        "test.t",
        None,
        |r| Ok(r[0] == Value::Int(1)),
        |_| Ok(vec![Value::Int(3), Value::Int(30)]),
    )
    .unwrap();
    let mut deferred = db.commit_txn_deferred(writer.txn_id).unwrap();
    assert!(deferred.commit_scn > db.visible_scn());
    db.purge_obsolete_index_repairs();
    let (view, _guard) =
        db.read_only_statement_view_with_guard(IsolationLevel::ReadCommitted, None);
    let table = db.table("test.t").unwrap();
    let primary = table.indexes.iter().find(|i| i.primary).unwrap();
    let candidates =
        db.lookup_index_visible(&table, primary, IndexKey::from(vec![KeyPart::Int(1)]), view);
    let heap_rows = edge_rows(&db, view);
    // Close the artificial allocation gap before asserting observable results.
    db.complete_commit_frontier(gap);
    db.finish_deferred_commits(&mut [&mut deferred]).unwrap();
    let mut indexed_rows = Vec::new();
    for id in candidates.unwrap() {
        if let Some(row) = db.read_row(&table, id, view).unwrap() {
            if row.values[0] == Value::Int(1) {
                indexed_rows.push(row.values);
            }
        }
    }
    assert_eq!(heap_rows, vec![(1, 10), (2, 20)]);
    assert_eq!(
        indexed_rows,
        vec![vec![Value::Int(1), Value::Int(10)]],
        "an index lookup must return the same old row as heap MVCC during a commit gap"
    );
}

include!("mvcc_fix_tests.rs");


#[test]
fn jepsen_commit_ack_waits_for_snapshot_visibility() {
    let (_dir, db) = edge_db();
    let gap = db.next_scn_atomic.fetch_add(1, Ordering::AcqRel);
    let writer = db.begin_txn().unwrap();
    edge_update(&db, writer, 1, 99);
    let mut deferred = db.commit_txn_deferred(writer.txn_id).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker_db = Arc::clone(&db);
    let worker = std::thread::spawn(move || {
        worker_db
            .finish_deferred_commits(&mut [&mut deferred])
            .unwrap();
        tx.send(edge_current(&worker_db)).unwrap();
    });
    let early = rx.recv_timeout(Duration::from_millis(100));
    db.complete_commit_frontier(gap);
    let observed = early
        .as_ref()
        .ok()
        .cloned()
        .unwrap_or_else(|| rx.recv_timeout(Duration::from_secs(5)).unwrap());
    worker.join().unwrap();
    assert_eq!(
        observed,
        vec![(1, 99), (2, 20)],
        "a fresh SELECT after COMMIT success must see the commit"
    );
    assert!(
        early.is_err(),
        "COMMIT must not acknowledge above the visible frontier"
    );
}

#[test]
fn jepsen_unique_point_lookup_survives_repair_and_key_reuse() {
    let (_dir, db) = edge_db();
    let table = db.table("test.t").unwrap();
    let primary = table.indexes.iter().find(|i| i.primary).unwrap();
    let (view, _guard) =
        db.read_only_statement_view_with_guard(IsolationLevel::ReadCommitted, None);
    assert!(!db.has_pending_index_repairs_for(table.id, &primary.name));
    // Pause the reader after the caller's fast-path check. A writer moves
    // the key and reuses it before the reader performs the actual probe.
    let writer = db.begin_txn().unwrap();
    db.update_rows(
        writer,
        "test.t",
        None,
        |r| Ok(r[0] == Value::Int(1)),
        |_| Ok(vec![Value::Int(3), Value::Int(30)]),
    )
    .unwrap();
    db.insert_rows(writer, "test.t", vec![vec![Value::Int(1), Value::Int(90)]])
        .unwrap();
    let id = db
        .lookup_unique_index_candidate_one(
            &table,
            primary,
            IndexKey::from(vec![KeyPart::Int(1)]),
            view,
        )
        .unwrap()
        .expect("old key must remain visible");
    assert_eq!(
        db.read_row(&table, id, view).unwrap().unwrap().values,
        vec![Value::Int(1), Value::Int(10)]
    );
    assert!(
        db.lookup_unique_index_candidate_one(
            &table,
            primary,
            IndexKey::from(vec![KeyPart::Int(3)]),
            view
        )
        .unwrap()
        .is_none(),
        "the reconstructed old row must not match the new physical key"
    );
    let own = db
        .lookup_unique_index_candidate_one(
            &table,
            primary,
            IndexKey::from(vec![KeyPart::Int(1)]),
            writer,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        db.read_row(&table, own, writer).unwrap().unwrap().values[1],
        Value::Int(90)
    );
    db.commit_txn(writer.txn_id).unwrap();
    let old = db
        .lookup_unique_index_candidate_one(
            &table,
            primary,
            IndexKey::from(vec![KeyPart::Int(1)]),
            view,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        db.read_row(&table, old, view).unwrap().unwrap().values[1],
        Value::Int(10)
    );
    let (fresh, _fresh_guard) =
        db.read_only_statement_view_with_guard(IsolationLevel::ReadCommitted, None);
    let new = db
        .lookup_unique_index_candidate_one(
            &table,
            primary,
            IndexKey::from(vec![KeyPart::Int(1)]),
            fresh,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        db.read_row(&table, new, fresh).unwrap().unwrap().values[1],
        Value::Int(90)
    );
}

#[test]
fn transaction_audit_acknowledged_commit_is_durable() {
    let (dir, db) = edge_db();
    let txn = db.begin_txn().unwrap();
    db.update_rows(txn, "test.t", None, |r| Ok(r[0] == Value::Int(1)),
        |_| Ok(vec![Value::Int(1), Value::Int(99)])).unwrap();
    let mut deferred = db.commit_txn_deferred(txn.txn_id).unwrap();
    let commit_lsn = deferred.commit_lsn.unwrap();
    // Visibility may precede durability; successful COMMIT may not.
    db.finish_deferred_commits(&mut [&mut deferred]).unwrap();
    assert!(db.log.lock().durable_lsn() >= commit_lsn);
    let scan = crate::wal::LogManager::scan(dir.path()).unwrap();
    assert!(scan.records.iter().any(|r| r.txn_id == txn.txn_id
        && matches!(r.payload, crate::wal::WalPayload::CommitTxn { .. })));
}

#[test]
fn transaction_audit_empty_prepared_rollback_is_durable() {
    for read_only in [false, true] {
        let (_dir, db) = edge_db();
        let txn = if read_only {
            db.begin_read_only_txn().unwrap()
        } else {
            db.begin_txn().unwrap()
        };
        let xid = crate::xa::Xid::new(1, b"empty_rollback".to_vec(), vec![]).unwrap();
        db.prepare_xa(txn.txn_id, 0, xid.clone()).unwrap();
        db.checkpoint().unwrap();
        db.rollback_prepared(&xid).unwrap();
        let log = db.log.lock();
        assert!(
            log.durable_lsn() >= log.last_lsn(),
            "acknowledged XA ROLLBACK must persist its terminal record"
        );
    }
}
