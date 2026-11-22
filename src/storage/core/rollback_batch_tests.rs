use crate::engine::StorageQueryEngine;
use crate::storage::txn::index_repair_cleanup_test_hook;

// Semantic and crash matrices cross real batch boundaries with smaller data.
// A separate narrow-row test verifies the exact production record limit.
const CRASH_BATCH_RECORDS: usize = 512;

#[test]
fn bulk_delete_drains_deferred_wal_and_recovers_uncommitted_prefix() {
    let dir = test_dir();
    let expected: Vec<_> = (0..1024)
        .map(|id| {
            vec![
                Value::Int(id),
                Value::Int(id * 7),
                if id % 3 == 0 {
                    Value::Null
                } else {
                    Value::String(format!("row-{id}"))
                },
            ]
        })
        .collect();
    {
        let engine = open_engine(dir.path(), DurabilityMode::Strict);
        engine
            .execute("CREATE TABLE bulk_prefix(id BIGINT, v BIGINT, payload VARCHAR(32))")
            .unwrap();
        let db = engine.database();
        let seed = db.begin_txn().unwrap();
        db.insert_rows(seed, "test.bulk_prefix", expected.clone())
            .unwrap();
        db.commit_txn(seed.txn_id).unwrap();
        let writer = db.begin_txn().unwrap();
        assert_eq!(
            db.delete_rows(writer, "test.bulk_prefix", None, |_| Ok(true))
                .unwrap(),
            expected.len()
        );
        assert!(
            txn_wal_buffer_len(&db.log, writer.txn_id) < TXN_WAL_BUFFER_FLUSH_THRESHOLD,
            "large DELETE must release deferred page reservations between chunks"
        );
        db.log.lock().flush().unwrap();
        // Drop without committing or checkpointing. Recovery must undo the
        // published prefix, including the header patches of every full chunk.
    }
    let engine = open_engine(dir.path(), DurabilityMode::Strict);
    let db = engine.database();
    let mut recovered = db
        .scan_rows(
            "test.bulk_prefix",
            db.read_only_statement_view(IsolationLevel::ReadCommitted, None),
        )
        .unwrap();
    recovered.sort_by_key(|row| match row.values[0] {
        Value::Int(id) => id,
        _ => unreachable!(),
    });
    assert_eq!(
        recovered
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<_>>(),
        expected
    );
    db.check_consistency().unwrap();
}

struct BatchRecordLimit(usize);

#[test]
fn bulk_delete_forwarded_roots_preserve_values_and_rollback() {
    let dir = test_dir();
    let engine = open_engine(dir.path(), DurabilityMode::Strict);
    engine.execute("CREATE TABLE layout_delete(id INT PRIMARY KEY, k INT, payload VARCHAR(4000), KEY ki(k))").unwrap();
    let db = engine.database();
    let load = db.begin_txn().unwrap();
    db.insert_rows(
        load,
        "test.layout_delete",
        (0..600)
            .map(|id| {
                vec![
                    Value::Int(id),
                    Value::Int(id % 7),
                    Value::String("x".into()),
                ]
            })
            .collect(),
    )
    .unwrap();
    db.commit_txn(load.txn_id).unwrap();
    engine
        .execute("UPDATE layout_delete SET payload=REPEAT('y',3000) WHERE MOD(id,3)=0")
        .unwrap();
    let table = db.table("test.layout_delete").unwrap();
    let heap = db.heap_handle(table.id).unwrap();
    let snapshot = db
        .scan_rows(
            "test.layout_delete",
            db.read_only_statement_view(IsolationLevel::ReadCommitted, None),
        )
        .unwrap();
    let mut forwarded = 0;
    for row in &snapshot {
        forwarded += usize::from(
            heap.read()
                .with_page(row.row_id.page_id as u64, |page| {
                    Ok(decode_heap_forwarding_stub(
                        page.payload(row.row_id.slot_id as usize)?.unwrap(),
                    )?
                    .is_some())
                })
                .unwrap(),
        );
    }
    assert!(forwarded > 0, "test must exercise forwarding roots");
    let writer = db.begin_txn().unwrap();
    assert_eq!(
        db.delete_rows(
            writer,
            "test.layout_delete",
            Some(snapshot.iter().map(|row| row.row_id).collect()),
            |_| Ok(true)
        )
        .unwrap(),
        600
    );
    db.rollback_txn(writer.txn_id).unwrap();
    let restored = db
        .scan_rows(
            "test.layout_delete",
            db.read_only_statement_view(IsolationLevel::ReadCommitted, None),
        )
        .unwrap();
    assert_eq!(
        restored
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<_>>(),
        snapshot
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<_>>()
    );
    db.check_consistency().unwrap();
}

#[test]
fn range_delete_cached_heap_recovers_wide_keys_lobs_and_instant_columns() {
    for commit in [false, true] {
        let dir = test_dir();
        let mut expected = Vec::new();
        {
            let engine = open_engine(dir.path(), DurabilityMode::Strict);
            engine.execute("CREATE TABLE wide_delete(id VARCHAR(768) PRIMARY KEY,k INT,payload LONGBLOB,KEY ki(k))").unwrap();
            let db = engine.database();
            for start in (0..600).step_by(256) {
                let txn = db.begin_txn().unwrap();
                let batch: Vec<_> = (start..(start + 256).min(600))
                    .map(|id| {
                        vec![
                            Value::String(format!("{id:05}{}", "x".repeat(700))),
                            if id % 3 == 0 {
                                Value::Null
                            } else {
                                Value::Int(id)
                            },
                            if id % 257 == 0 {
                                Value::Bytes(vec![42; 20_000])
                            } else {
                                Value::Null
                            },
                        ]
                    })
                    .collect();
                expected.extend(batch.iter().cloned().map(|mut row| {
                    row.push(Value::Int(7));
                    row
                }));
                db.insert_rows(txn, "test.wide_delete", batch).unwrap();
                db.commit_txn(txn.txn_id).unwrap();
            }
            engine
                .execute(
                    "ALTER TABLE wide_delete ADD COLUMN extra INT DEFAULT 7, ALGORITHM=INSTANT",
                )
                .unwrap();
            let table = db.table("test.wide_delete").unwrap();
            db.with_index_dml(table.id, "PRIMARY", |file| {
                let root = file.read_internal(file.root_page.load(Ordering::Acquire))?;
                assert!(matches!(
                    file.read_page(root.children[0])?.as_ref(),
                    IndexPage::Internal(_)
                ));
                Ok(())
            })
            .unwrap();
            db.checkpoint().unwrap();
            let txn = db.begin_txn().unwrap();
            let candidates = db
                .scan_rows("test.wide_delete", txn)
                .unwrap()
                .into_iter()
                .map(|row| row.row_id)
                .collect();
            let mut calls = 0;
            assert_eq!(
                db.delete_rows(txn, "test.wide_delete", Some(candidates), |values| {
                    assert_eq!(values[3], Value::Int(7));
                    if let Value::Bytes(bytes) = &values[2] {
                        assert_eq!(bytes, &vec![42; 20_000]);
                    }
                    calls += 1;
                    Ok(true)
                })
                .unwrap(),
                600
            );
            assert_eq!(
                calls, 1200,
                "preserve pre-lock and post-lock predicate calls"
            );
            if commit {
                db.commit_txn(txn.txn_id).unwrap();
            }
            db.log.lock().flush().unwrap();
        }
        let engine = open_engine(dir.path(), DurabilityMode::Strict);
        let db = engine.database();
        let mut actual: Vec<_> = db
            .scan_rows(
                "test.wide_delete",
                db.read_only_statement_view(IsolationLevel::ReadCommitted, None),
            )
            .unwrap()
            .into_iter()
            .map(|row| row.values)
            .collect();
        actual.sort_by(|left, right| match (&left[0], &right[0]) {
            (Value::String(left), Value::String(right)) => left.cmp(right),
            _ => unreachable!(),
        });
        assert_eq!(actual, if commit { Vec::new() } else { expected });
        db.check_consistency().unwrap();
    }
}

#[test]
fn partial_numeric_range_delete_recovers_synchronized_packed_leaves() {
    for commit in [false, true] {
        let dir = test_dir();
        let mut expected;
        {
            let engine = open_engine(dir.path(), DurabilityMode::Strict);
            engine
                .execute("CREATE TABLE numeric_delete(id INT PRIMARY KEY,k INT,KEY ki(k))")
                .unwrap();
            engine.execute("INSERT INTO numeric_delete SELECT n,MOD(n,17) FROM generate_series(0,3999) AS gs(n)").unwrap();
            let db = engine.database();
            db.checkpoint().unwrap();
            let txn = db.begin_txn().unwrap();
            let initial = db.scan_rows("test.numeric_delete", txn).unwrap();
            expected = initial
                .iter()
                .filter(|row| !commit || matches!(row.values[0], Value::Int(id) if id%5!=0))
                .map(|row| row.values.clone())
                .collect::<Vec<_>>();
            let candidates = initial
                .into_iter()
                .filter(|row| matches!(row.values[0], Value::Int(id) if id%5==0))
                .map(|row| row.row_id)
                .collect();
            assert_eq!(
                db.delete_rows(txn, "test.numeric_delete", Some(candidates), |_| Ok(true))
                    .unwrap(),
                800
            );
            if commit {
                db.commit_txn(txn.txn_id).unwrap();
            }
            db.log.lock().flush().unwrap();
        }
        let engine = open_engine(dir.path(), DurabilityMode::Strict);
        let db = engine.database();
        let mut actual = db
            .scan_rows(
                "test.numeric_delete",
                db.read_only_statement_view(IsolationLevel::ReadCommitted, None),
            )
            .unwrap()
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<_>>();
        let id = |row: &Vec<Value>| match row[0] {
            Value::Int(id) => id,
            _ => unreachable!(),
        };
        expected.sort_by_key(&id);
        actual.sort_by_key(&id);
        assert_eq!(actual, expected);
        db.check_consistency().unwrap();
    }
}

impl BatchRecordLimit {
    fn scoped(limit: usize) -> Self {
        Self(ROLLBACK_BATCH_RECORD_LIMIT.with(|current| current.replace(limit)))
    }
}

impl Drop for BatchRecordLimit {
    fn drop(&mut self) {
        ROLLBACK_BATCH_RECORD_LIMIT.with(|current| current.set(self.0));
    }
}

fn test_dir() -> tempfile::TempDir {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    tempfile::tempdir_in("/tmp/mydb").unwrap()
}

fn open_engine(path: &Path, durability: DurabilityMode) -> StorageQueryEngine {
    StorageQueryEngine::open_with_storage_config_without_background_workers(
        path,
        StorageConfig {
            durability,
            buffer_pool_pages: 2048,
            buffer_pool_io_threads: 1,
            group_commit_window: Duration::ZERO,
            ..StorageConfig::from_env()
        },
    )
    .unwrap()
}

fn seed(engine: &StorageQueryEngine, count: i64) {
    engine.execute("CREATE TABLE rb (id INT PRIMARY KEY, v BIGINT NOT NULL, payload LONGBLOB, amount DECIMAL(20,4), day DATE, UNIQUE KEY uv(v))").unwrap();
    let db = engine.database();
    for start in (0..count).step_by(256) {
        let txn = db.begin_txn().unwrap();
        db.insert_rows(
            txn,
            "test.rb",
            (start..(start + 256).min(count))
                .map(|id| {
                    vec![
                        Value::Int(id),
                        Value::Int(id),
                        if id % 257 == 0 {
                            Value::Bytes(vec![42; 20_000])
                        } else {
                            Value::Null
                        },
                        Value::DecimalCompact(id * 10001, 4),
                        Value::Date(18000 + id as i32),
                    ]
                })
                .collect(),
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
    }
}

fn rows(db: &Database) -> Vec<Vec<Value>> {
    let mut rows = db
        .scan_rows(
            "test.rb",
            db.read_only_statement_view(IsolationLevel::ReadCommitted, None),
        )
        .unwrap();
    rows.sort_by_key(|row| match row.values[0] {
        Value::Int(id) => id,
        _ => panic!("integer id"),
    });
    rows.into_iter().map(|row| row.values).collect()
}

fn assert_physical_versions_match_visible(db: &Database) {
    let table = db.table("test.rb").unwrap();
    let view = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    for row in db.scan_rows("test.rb", view).unwrap() {
        let physical = db.read_mvcc_row(table.id, row.row_id).unwrap().unwrap();
        assert_eq!(
            physical.values, row.values,
            "visibility through undo must not hide an unfinished physical rollback"
        );
    }
}

fn update(db: &Database, txn: TxnView, delta: i64) {
    db.update_rows(
        txn,
        "test.rb",
        None,
        |_| Ok(true),
        |row| {
            let mut values = row.to_vec();
            let Value::Int(v) = row[1] else {
                panic!("integer value")
            };
            values[1] = Value::Int(v + delta);
            if values[2] != Value::Null {
                values[2] = Value::Bytes(vec![delta as u8; 24_000]);
            }
            Ok(values)
        },
    )
    .unwrap();
}

#[test]
fn deferred_secondary_delete_marks_follow_views_savepoints_and_key_reuse() {
    let dir = test_dir();
    let engine = open_engine(dir.path(), DurabilityMode::Strict);
    engine.execute(
        "CREATE TABLE marked(id BIGINT PRIMARY KEY, u BIGINT NOT NULL, k INT NOT NULL,          payload VARCHAR(16) NOT NULL, UNIQUE KEY uv(u), KEY kv(k))",
    ).unwrap();
    let db = engine.database();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.marked",
        (1..=2304)
            .map(|n| {
                vec![
                    Value::Int(n),
                    Value::Int(n),
                    Value::Int(n % 16),
                    Value::String("old".into()),
                ]
            })
            .collect(),
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let table = db.table("test.marked").unwrap();
    let key = IndexKey::from(vec![KeyPart::Int(10)]);
    let physical = || {
        db.with_index_read(table.id, "kv", |index| index.get(&key, false))
            .unwrap()
    };
    let original = physical();
    assert_eq!(original.len(), 144);
    let original_42 = db
        .with_index_read(table.id, "PRIMARY", |file| {
            file.get_unique_one(&IndexKey::from(vec![KeyPart::Int(42)]))
        })
        .unwrap()
        .unwrap();

    let mut old = crate::engine::SessionContext::default();
    let mut writer = crate::engine::SessionContext::default();
    engine
        .execute_with_session(&mut old, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .unwrap();
    engine
        .execute_with_session(&mut old, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "SAVEPOINT before_delete")
        .unwrap();

    let current_count = |session: &mut crate::engine::SessionContext| {
        let sql = "SELECT COUNT(*) FROM marked FORCE INDEX (kv) WHERE k=10";
        match engine.execute_with_session(session, sql).unwrap() {
            crate::engine::QueryResponse::ResultSet(result) => result.rows[0][0].clone(),
            _ => panic!("expected count result"),
        }
    };
    let replace_deleted = |session: &mut crate::engine::SessionContext| {
        engine
            .execute_with_session(session, "DELETE FROM marked WHERE id BETWEEN 1 AND 1300")
            .unwrap();
        assert_eq!(physical(), original);
        assert!(!db
            .with_heap_mut(table.id, |heap| heap.reclaim_slot_logged(original_42))
            .unwrap());
        engine
            .execute_with_session(session, "INSERT INTO marked VALUES(42,42,10,'new')")
            .unwrap();
        assert_eq!(physical().len(), 145);
    };

    replace_deleted(&mut writer);
    assert_eq!(current_count(&mut old), Value::Int(144));
    assert_eq!(current_count(&mut writer), Value::Int(64));
    engine
        .execute_with_session(&mut writer, "ROLLBACK TO SAVEPOINT before_delete")
        .unwrap();
    assert_eq!(current_count(&mut writer), Value::Int(144));
    assert_eq!(physical(), original);

    replace_deleted(&mut writer);
    engine.execute_with_session(&mut writer, "COMMIT").unwrap();
    let mut fresh = crate::engine::SessionContext::default();
    assert_eq!(current_count(&mut fresh), Value::Int(64));
    assert_eq!(current_count(&mut old), Value::Int(144));
    db.check_consistency().unwrap();
    db.purge_obsolete_undo();
    assert_eq!(
        physical().len(),
        145,
        "old snapshot pins the original index entries"
    );
    engine.execute_with_session(&mut old, "COMMIT").unwrap();
    for _ in 0..8 {
        db.purge_obsolete_undo();
    }
    assert_eq!(physical().len(), 64);
    assert_eq!(db.deferred_index_purge_metrics().0, 1300);
    assert_eq!(current_count(&mut fresh), Value::Int(64));
    db.check_consistency().unwrap();
}

#[test]
fn rollback_chain_walk_accepts_millions_and_detects_long_cycles() {
    let mut walk = UndoChainWalk::default();
    for ptr in (1..=1_000_100).rev() {
        walk.visit(ptr).unwrap();
    }
    for cycle in [1, 2, 65, 257, 4097] {
        let mut walk = UndoChainWalk::default();
        for ptr in 1..100 {
            walk.visit(ptr).unwrap();
        }
        let mut detected = false;
        for hop in 0..cycle * 5 + 256 {
            if walk.visit(100 + (hop % cycle) as u64).is_err() {
                detected = true;
                break;
            }
        }
        assert!(detected, "cycle of length {cycle}");
    }
}

#[test]
fn rollback_delete_batches_preserve_snapshot_and_other_writers_repairs() {
    let _limit = BatchRecordLimit::scoped(64);
    let dir = test_dir();
    let engine = open_engine(dir.path(), DurabilityMode::Strict);
    engine
        .execute("CREATE TABLE rb(id BIGINT PRIMARY KEY, u BIGINT NOT NULL, k INT, UNIQUE KEY uv(u), KEY kv(k))")
        .unwrap();
    let db = engine.database();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.rb",
        (0..300)
            .map(|id| vec![Value::Int(id), Value::Int(id), Value::Int(id % 3)])
            .collect(),
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let initial = rows(db);
    let snapshot = TxnView {
        isolation: IsolationLevel::RepeatableRead,
        ..db.begin_txn().unwrap()
    };
    let writer = db.begin_txn().unwrap();
    db.delete_rows(writer, "test.rb", None, |row| Ok(row[0] == Value::Int(0)))
        .unwrap();
    let savepoint = db.statement_savepoint(writer.txn_id).unwrap();
    db.delete_rows(writer, "test.rb", None, |row| {
        Ok(matches!(row[0], Value::Int(id) if (1..240).contains(&id)))
    })
    .unwrap();
    db.insert_rows(
        writer,
        "test.rb",
        vec![vec![Value::Int(400), Value::Int(1), Value::Int(0)]],
    )
    .unwrap();
    db.delete_rows(writer, "test.rb", None, |row| {
        Ok(matches!(row[0], Value::Int(id) if (240..270).contains(&id)))
    })
    .unwrap();
    let other = db.begin_txn().unwrap();
    db.delete_rows(other, "test.rb", None, |row| {
        Ok(matches!(row[0], Value::Int(id) if (270..300).contains(&id)))
    })
    .unwrap();
    let other_repairs = db.index_repair_state.writer_entries(other.txn_id);
    assert_eq!(other_repairs.len(), 60);
    assert!(other_repairs.iter().all(|entry| entry.0.index_name != "kv"));
    let observed = Arc::new(AtomicBool::new(false));
    let observed_by_reader = observed.clone();
    let reader_db = db.clone();
    ROLLBACK_DELETE_INDEX_TEST_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            if observed_by_reader.swap(true, Ordering::Relaxed) {
                return;
            }
            let table = reader_db.table("test.rb").unwrap();
            let index = table
                .indexes
                .iter()
                .find(|index| index.name == "kv")
                .unwrap();
            let key = IndexKey::from(vec![crate::storage::schema::KeyPart::Int(0)]);
            let visible = reader_db
                .lookup_index_visible(&table, index, key, snapshot)
                .unwrap()
                .into_iter()
                .filter_map(|id| reader_db.read_row(&table, id, snapshot).unwrap())
                .filter(|row| row.values[2] == Value::Int(0))
                .count();
            assert_eq!(visible, 100);
        }));
    });
    db.rollback_to_statement_savepoint(writer.txn_id, savepoint)
        .unwrap();
    ROLLBACK_DELETE_INDEX_TEST_HOOK.with(|hook| *hook.borrow_mut() = None);
    assert!(observed.load(Ordering::Relaxed));
    assert_eq!(
        db.index_repair_state.writer_entries(other.txn_id),
        other_repairs
    );
    let writer_rows = db.scan_rows("test.rb", writer).unwrap();
    assert_eq!(writer_rows.len(), 299);
    assert!(writer_rows
        .iter()
        .all(|row| row.values[0] != Value::Int(0) && row.values[0] != Value::Int(400)));
    db.rollback_txn(writer.txn_id).unwrap();
    db.rollback_txn(other.txn_id).unwrap();
    db.rollback_txn(snapshot.txn_id).unwrap();
    assert_eq!(rows(db), initial);
    assert_physical_versions_match_visible(db);
    db.check_consistency().unwrap();
}

#[test]
fn index_repair_batch_clear_releases_directory_and_retries_old_point_probes() {
    let dir = test_dir();
    let engine = open_engine(dir.path(), DurabilityMode::Strict);
    let count = 800;
    seed(&engine, count);
    let db = engine.database();
    let table = db.table("test.rb").unwrap();
    let primary = table.indexes.iter().find(|index| index.primary).unwrap();
    let key = IndexKey::from(vec![crate::storage::schema::KeyPart::Int(17)]);
    let snapshot = TxnView {
        isolation: IsolationLevel::RepeatableRead,
        ..db.begin_txn().unwrap()
    };
    let writer = db.begin_txn().unwrap();
    db.delete_rows(writer, "test.rb", None, |_| Ok(true))
        .unwrap();
    assert!(db
        .lookup_index(&table, primary, key.clone())
        .unwrap()
        .is_empty());
    let primary_epoch = db.index_repair_rollback_epoch(table.id, &primary.name);
    let unique_epoch = db.index_repair_rollback_epoch(table.id, "uv");
    let checked = Arc::new(AtomicBool::new(false));
    let hook_checked = checked.clone();
    let hook_db = db.clone();
    let hook_table = table.clone();
    index_repair_cleanup_test_hook::AFTER_BATCH.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |txn_id| {
            if hook_checked.swap(true, Ordering::Relaxed) {
                return;
            }
            assert_eq!(txn_id, writer.txn_id);
            assert!(hook_db.txns.try_write().is_some());
            assert_eq!(hook_db.txns.read().status(txn_id), Some(TxnStatus::Active));
            assert_eq!(
                hook_db
                    .slot_status_cache
                    .lookup(txn_id)
                    .map(|(status, _)| status),
                Some(TxnStatus::Active),
            );
            let repairs = &hook_db.index_repair_state;
            let num_primary = repairs
                .candidates(hook_table.id, "primary", None, None, |_| true)
                .len();
            let num_unique = repairs
                .candidates(hook_table.id, "uv", None, None, |_| true)
                .len();
            assert!((1..(2 * count) as usize).contains(&(num_primary + num_unique)));
            if num_primary < count as usize {
                assert_ne!(
                    hook_db.index_repair_rollback_epoch(hook_table.id, "primary"),
                    primary_epoch
                );
            }
            if num_unique < count as usize {
                assert_ne!(
                    hook_db.index_repair_rollback_epoch(hook_table.id, "uv"),
                    unique_epoch
                );
            }
            let unrelated = hook_db.begin_txn().unwrap();
            assert_eq!(
                hook_db.scan_rows("test.rb", unrelated).unwrap().len(),
                count as usize
            );
            hook_db.commit_txn(unrelated.txn_id).unwrap();
        }));
    });
    let hook_db = db.clone();
    index_repair_cleanup_test_hook::AFTER_POINT_PROBE.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            hook_db.rollback_txn(writer.txn_id).unwrap();
        }));
    });
    let ids = db
        .lookup_index_visible(&table, primary, key, snapshot)
        .unwrap();
    index_repair_cleanup_test_hook::AFTER_BATCH.with(|hook| *hook.borrow_mut() = None);
    assert!(checked.load(Ordering::Relaxed));
    assert_eq!(
        ids.len(),
        1,
        "the physical read preceding the rollback must retry"
    );
    assert_eq!(
        db.read_row(&table, ids[0], snapshot)
            .unwrap()
            .unwrap()
            .values[0],
        Value::Int(17)
    );
    assert!(!db.index_repair_state.has_writers());
    assert!(!db.has_pending_index_repairs());
    db.rollback_txn(snapshot.txn_id).unwrap();
    db.check_consistency().unwrap();
}

#[test]
fn rollback_batches_preserve_savepoint_mixed_dml_and_lob_ownership() {
    let _limit = BatchRecordLimit::scoped(CRASH_BATCH_RECORDS);
    for durability in [DurabilityMode::Memory, DurabilityMode::Strict] {
        let dir = test_dir();
        let engine = open_engine(dir.path(), durability);
        seed(&engine, CRASH_BATCH_RECORDS as i64 + 19);
        let db = engine.database();
        let initial = rows(db);
        let txn = db.begin_txn().unwrap();
        update(db, txn, 100_000);
        let before_savepoint = db.scan_rows("test.rb", txn).unwrap();
        let savepoint = db.statement_savepoint(txn.txn_id).unwrap();
        update(db, txn, 200_000);
        update(db, txn, 400_000);
        db.delete_rows(txn, "test.rb", None, |row| {
            Ok(matches!(row[0], Value::Int(id) if id % 17 == 0))
        })
        .unwrap();
        db.insert_rows(
            txn,
            "test.rb",
            (20_000..20_000 + CRASH_BATCH_RECORDS as i64 + 3)
                .map(|id| {
                    vec![
                        Value::Int(id),
                        Value::Int(id),
                        Value::Null,
                        Value::Null,
                        Value::Null,
                    ]
                })
                .collect(),
        )
        .unwrap();
        let progress = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = progress.clone();
        let reader_db = db.clone();
        ROLLBACK_PROGRESS_TEST_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |txn_id, head, intermediate| {
                let mut observed = observed.lock().unwrap();
                if intermediate {
                    let log = reader_db.log.lock();
                    if durability != DurabilityMode::Memory {
                        assert!(log.durable_lsn() >= log.last_txn_lsn(txn_id));
                    }
                    drop(log);
                    assert!(head.is_some());
                    assert_eq!(
                        reader_db.txns.read().status(txn_id),
                        Some(TxnStatus::Active)
                    );
                }
                // A concurrent reader must still see the original committed
                // versions while only a prefix of the rollback is complete.
                if observed.is_empty() && intermediate {
                    assert_eq!(rows(&reader_db), initial);
                }
                observed.push((head, intermediate));
            }));
        });
        db.rollback_to_statement_savepoint(txn.txn_id, savepoint)
            .unwrap();
        ROLLBACK_PROGRESS_TEST_HOOK.with(|hook| *hook.borrow_mut() = None);
        assert!(
            progress
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, mid)| *mid)
                .count()
                >= 2
        );
        let restored = db.scan_rows("test.rb", txn).unwrap();
        assert_eq!(restored.len(), before_savepoint.len());
        let mut expected = before_savepoint
            .into_iter()
            .map(|r| (r.row_id, r.values))
            .collect::<Vec<_>>();
        let mut actual = restored
            .into_iter()
            .map(|r| (r.row_id, r.values))
            .collect::<Vec<_>>();
        expected.sort_by_key(|r| r.0);
        actual.sort_by_key(|r| r.0);
        assert_eq!(actual, expected);
        db.commit_txn(txn.txn_id).unwrap();
        db.check_consistency().unwrap();
        let committed = rows(db);
        if durability != DurabilityMode::Memory {
            drop(engine);
            let reopened = open_engine(dir.path(), durability);
            assert_eq!(rows(reopened.database()), committed);
            reopened.database().check_consistency().unwrap();
        }
    }
}

#[test]
fn rollback_batch_default_record_limit_is_exact() {
    let dir = test_dir();
    let engine = open_engine(dir.path(), DurabilityMode::Memory);
    engine
        .execute("CREATE TABLE rb(id INT PRIMARY KEY, v INT)")
        .unwrap();
    let db = engine.database();
    let count = ROLLBACK_BATCH_RECORDS + 1;
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.rb",
        (0..count)
            .map(|id| vec![Value::Int(id as i64), Value::Int(0)])
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let txn = db.begin_txn().unwrap();
    db.update_rows(
        txn,
        "test.rb",
        None,
        |_| Ok(true),
        |row| Ok(vec![row[0].clone(), Value::Int(1)]),
    )
    .unwrap();
    let records = Arc::new(AtomicUsize::new(0));
    let observed_records = records.clone();
    ROLLBACK_RECORD_TEST_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            observed_records.fetch_add(1, Ordering::Relaxed);
        }));
    });
    let progress = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed_progress = progress.clone();
    ROLLBACK_PROGRESS_TEST_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |_, _, intermediate| {
            observed_progress
                .lock()
                .unwrap()
                .push((records.load(Ordering::Relaxed), intermediate));
        }));
    });
    db.rollback_txn(txn.txn_id).unwrap();
    ROLLBACK_RECORD_TEST_HOOK.with(|hook| *hook.borrow_mut() = None);
    ROLLBACK_PROGRESS_TEST_HOOK.with(|hook| *hook.borrow_mut() = None);
    assert_eq!(
        *progress.lock().unwrap(),
        vec![(ROLLBACK_BATCH_RECORDS, true), (count, false),]
    );
    let restored = rows(db);
    assert_eq!(restored.len(), count);
    assert!(restored.iter().all(|row| row[1] == Value::Int(0)));
}

#[test]
fn rollback_batch_byte_budget_cuts_wide_rows_before_record_limit() {
    let dir = test_dir();
    let engine = open_engine(dir.path(), DurabilityMode::Memory);
    engine
        .execute("CREATE TABLE rb (id INT PRIMARY KEY, v VARCHAR(2000))")
        .unwrap();
    let db = engine.database();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.rb",
        (0..3000)
            .map(|id| vec![Value::Int(id), Value::String("a".repeat(1800))])
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let txn = db.begin_txn().unwrap();
    db.update_rows(
        txn,
        "test.rb",
        None,
        |_| Ok(true),
        |row| Ok(vec![row[0].clone(), Value::String("b".repeat(1800))]),
    )
    .unwrap();
    let intermediate = Arc::new(AtomicUsize::new(0));
    let observed = intermediate.clone();
    ROLLBACK_PROGRESS_TEST_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |_, _, mid| {
            observed.fetch_add(usize::from(mid), Ordering::Relaxed);
        }));
    });
    db.rollback_txn(txn.txn_id).unwrap();
    ROLLBACK_PROGRESS_TEST_HOOK.with(|hook| *hook.borrow_mut() = None);
    assert!(intermediate.load(Ordering::Relaxed) > 0);
    assert!(rows(db)
        .iter()
        .all(|r| r[1] == Value::String("a".repeat(1800))));
}

#[test]
fn rollback_batch_corrupt_long_cycle_fails_closed() {
    let dir = test_dir();
    let engine = open_engine(dir.path(), DurabilityMode::Memory);
    seed(&engine, 100);
    let db = engine.database();
    let txn = db.begin_txn().unwrap();
    update(db, txn, 1000);
    let head = db.txns.read().undo_head(txn.txn_id).unwrap().unwrap();
    let mut ptr = head;
    loop {
        let prev = db.undo.read().records[&ptr].prev_undo_ptr;
        match prev {
            Some(prev) => ptr = prev,
            None => break,
        }
    }
    db.undo.write().records.get_mut(&ptr).unwrap().prev_undo_ptr = Some(head);
    let err = db.rollback_txn(txn.txn_id).unwrap_err();
    assert!(err.message().contains("cycle"), "{err:?}");
    assert!(db.mvcc_quarantined.load(Ordering::Acquire));
    assert_eq!(db.txns.read().status(txn.txn_id), Some(TxnStatus::Active));
}

// This subprocess deliberately exits without running destructors. The parent
// reopens the durable prefix, including compensation WAL whose data pages may
// never have been written, and repeats recovery once more.
#[test]
fn rollback_batch_crash_worker() {
    let Some(path) = std::env::var_os("MYDB_ROLLBACK_CRASH_DIR") else {
        return;
    };
    let mode = std::env::var("MYDB_ROLLBACK_CRASH_MODE").unwrap();
    ROLLBACK_BATCH_RECORD_LIMIT.with(|limit| limit.set(CRASH_BATCH_RECORDS));
    let engine = open_engine(Path::new(&path), DurabilityMode::Strict);
    if mode == "prepared_logical_partial" {
        engine
            .execute("CREATE TABLE rb(id BIGINT PRIMARY KEY,v BIGINT,KEY kv(v))")
            .unwrap();
        for id in 0..37 {
            engine
                .execute(&format!("INSERT INTO rb VALUES({id},{id})"))
                .unwrap();
        }
        let db = engine.database();
        db.checkpoint().unwrap();
        let mut session = crate::engine::SessionContext::default();
        for sql in [
            "XA START 'logical'",
            "UPDATE rb SET v=v+1000000",
            "XA END 'logical'",
            "XA PREPARE 'logical'",
        ] {
            engine.execute_with_session(&mut session, sql).unwrap();
        }
        let db = db.clone();
        ROLLBACK_RECORD_TEST_HOOK.with(|hook| {
            let mut count = 0;
            *hook.borrow_mut() = Some(Box::new(move || {
                count += 1;
                if count == 13 {
                    db.log.lock().flush().unwrap();
                    db.flush_dirty_data().unwrap();
                    std::process::exit(91);
                }
            }));
        });
        engine
            .execute_with_session(&mut session, "XA ROLLBACK 'logical'")
            .unwrap();
        panic!("partial logical rollback did not crash");
    }
    let db = engine.database();
    if mode.starts_with("delete_mark_") {
        engine
            .execute("CREATE TABLE rb(id BIGINT PRIMARY KEY, v BIGINT, KEY dv(v))")
            .unwrap();
        let count = 2048 + 23;
        let load = db.begin_txn().unwrap();
        db.insert_rows(
            load,
            "test.rb",
            (0..count)
                .map(|n| vec![Value::Int(n), Value::Int(n % 16)])
                .collect(),
        )
        .unwrap();
        db.commit_txn(load.txn_id).unwrap();
        db.checkpoint().unwrap();
        let txn = db.begin_txn().unwrap();
        let table = db.table("test.rb").unwrap();
        assert!(table.indexes.iter().any(index_supports_delete_mark));
        let sample = db.scan_rows("test.rb", txn).unwrap();
        assert_eq!(sample.len(), count as usize);
        assert!(db
            .with_heap_read(table.id, |heap| heap
                .has_reserved_undo_slot(sample[0].row_id))
            .unwrap());
        assert_eq!(
            db.delete_rows(txn, "test.rb", None, |_| Ok(true)).unwrap(),
            count as usize
        );
        assert_ne!(db.retained_index_deletes.load(Ordering::Acquire), 0);
        if mode != "delete_mark_loser" {
            db.commit_txn(txn.txn_id).unwrap();
            if mode == "delete_mark_purge_wal" {
                AFTER_DEFERRED_INDEX_PURGE_WAL.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(|| std::process::exit(91)));
                });
                db.purge_obsolete_undo();
                panic!("deferred index purge did not reach its durability boundary");
            }
            if mode == "delete_mark_reclaim_wal" {
                AFTER_RETAINED_SLOT_RECLAIM_WAL.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(|| std::process::exit(91)));
                });
                db.purge_obsolete_undo();
                panic!("retained slot reclaim did not reach its durability boundary");
            }
        } else {
            db.checkpoint().unwrap();
        }
        db.log.lock().flush().unwrap();
        std::process::exit(91);
    }
    seed(&engine, CRASH_BATCH_RECORDS as i64 + 23);
    db.checkpoint().unwrap();
    let txn = db.begin_txn().unwrap();
    if matches!(
        mode.as_str(),
        "delete_index_partial"
            | "delete_header_partial"
            | "delete_progress"
            | "delete_repair_partial"
    ) {
        db.delete_rows(txn, "test.rb", None, |_| Ok(true)).unwrap();
        if mode == "delete_index_partial" {
            let db = db.clone();
            ROLLBACK_DELETE_INDEX_TEST_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    db.log.lock().flush().unwrap();
                    db.flush_dirty_data().unwrap();
                    std::process::exit(91);
                }));
            });
        } else if mode == "delete_header_partial" {
            let db = db.clone();
            ROLLBACK_DELETE_HEADER_TEST_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    db.log.lock().flush().unwrap();
                    std::process::exit(91);
                }));
            });
        } else if mode == "delete_repair_partial" {
            index_repair_cleanup_test_hook::AFTER_BATCH.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(|_| std::process::exit(91)));
            });
        } else {
            ROLLBACK_PROGRESS_TEST_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(|_, _, intermediate| {
                    if intermediate {
                        std::process::exit(91);
                    }
                }));
            });
        }
        db.rollback_txn(txn.txn_id).unwrap();
        panic!("delete rollback did not reach the requested crash boundary");
    }
    update(db, txn, 100_000);
    let savepoint = db.statement_savepoint(txn.txn_id).unwrap();
    update(db, txn, 200_000);
    update(db, txn, 400_000);
    let target = if mode == "second" { 2 } else { 1 };
    let leave_committed = mode == "commit";
    let finish_savepoint = leave_committed || mode == "complete";
    let crash_partial = mode == "partial";
    if crash_partial {
        let db = db.clone();
        ROLLBACK_RECORD_TEST_HOOK.with(|hook| {
            let mut records = 0;
            *hook.borrow_mut() = Some(Box::new(move || {
                records += 1;
                if records == CRASH_BATCH_RECORDS + 13 {
                    db.log.lock().flush().unwrap();
                    db.flush_dirty_data().unwrap();
                    std::process::exit(91);
                }
            }));
        });
    }
    ROLLBACK_PROGRESS_TEST_HOOK.with(|hook| {
        let mut batches = 0;
        *hook.borrow_mut() = Some(Box::new(move |_, _, intermediate| {
            if intermediate {
                batches += 1;
                if !finish_savepoint && !crash_partial && batches == target {
                    std::process::exit(91);
                }
            }
        }));
    });
    if mode == "savepoint" || finish_savepoint {
        db.rollback_to_statement_savepoint(txn.txn_id, savepoint)
            .unwrap();
    } else if mode == "prepared" {
        let xid = crate::xa::Xid::new(1, b"rollback-batches".to_vec(), Vec::new()).unwrap();
        db.prepare_xa(txn.txn_id, 0, xid.clone()).unwrap();
        db.rollback_prepared(&xid).unwrap();
    } else {
        db.rollback_txn(txn.txn_id).unwrap();
    }
    ROLLBACK_PROGRESS_TEST_HOOK.with(|hook| *hook.borrow_mut() = None);
    if leave_committed {
        db.commit_txn(txn.txn_id).unwrap();
        std::process::exit(91);
    }
    if finish_savepoint {
        db.log.lock().flush().unwrap();
        std::process::exit(91);
    }
    panic!("rollback did not reach the requested crash boundary");
}

// Each crash boundary is independent. Separate nextest cases keep the seven
// full crash/recovery cycles from forming a long serial tail of the suite.
mod rollback_batches_survive_process_crash_and_repeated_recovery {
    use super::*;

    macro_rules! crash_case {
        ($($mode:ident),+ $(,)?) => {
            $(
                #[test]
                fn $mode() {
                    assert_crash_recovery(stringify!($mode));
                }
            )+
        };
    }

    crash_case!(
        first,
        second,
        partial,
        savepoint,
        complete,
        commit,
        prepared,
        delete_index_partial,
        delete_header_partial,
        delete_mark_commit,
        delete_mark_loser,
        delete_mark_purge_wal,
        delete_mark_reclaim_wal,
        delete_progress,
        delete_repair_partial
    );
}

fn assert_crash_recovery(mode: &str) {
    let dir = test_dir();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "storage::core::rollback_batch_tests::rollback_batch_crash_worker",
            "--nocapture",
        ])
        .env("MYDB_ROLLBACK_CRASH_DIR", dir.path())
        .env("MYDB_ROLLBACK_CRASH_MODE", mode)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(91), "{mode}");
    for _ in 0..2 {
        let engine = open_engine(dir.path(), DurabilityMode::Strict);
        let db = engine.database();
        let restored = rows(db);
        if mode == "delete_mark_loser" {
            assert_eq!(restored.len(), 2048 + 23);
            for (id, row) in restored.iter().enumerate() {
                assert_eq!(
                    *row,
                    vec![Value::Int(id as i64), Value::Int((id % 16) as i64)]
                );
            }
            assert_physical_versions_match_visible(db);
            db.check_consistency().unwrap();
            db.checkpoint().unwrap();
            continue;
        }
        if matches!(
            mode,
            "delete_mark_commit" | "delete_mark_purge_wal" | "delete_mark_reclaim_wal"
        ) {
            assert!(restored.is_empty());
            db.check_consistency().unwrap();
            for _ in 0..8 {
                db.purge_obsolete_undo();
            }
            let table = db.table("test.rb").unwrap();
            assert!(db
                .with_index_read(table.id, "dv", |file| file.entries())
                .unwrap()
                .is_empty());
            db.checkpoint().unwrap();
            continue;
        }
        assert_eq!(restored.len(), CRASH_BATCH_RECORDS + 23, "{mode}");
        for row in restored {
            let Value::Int(id) = row[0] else {
                panic!("integer id")
            };
            let delta = if mode == "commit" { 100_000 } else { 0 };
            assert_eq!(row[1], Value::Int(id + delta), "{mode} row {id}");
            if id % 257 == 0 {
                let bytes = if mode == "commit" {
                    vec![100_000i64 as u8; 24_000]
                } else {
                    vec![42; 20_000]
                };
                assert_eq!(row[2], Value::Bytes(bytes), "{mode} row {id}");
            }
        }
        assert_physical_versions_match_visible(db);
        db.check_consistency().unwrap();
        db.purge_obsolete_undo();
        db.checkpoint().unwrap();
    }
}

#[test]
fn rollback_batch_replica_promotion_finishes_an_interrupted_abort() {
    let _limit = BatchRecordLimit::scoped(CRASH_BATCH_RECORDS);
    fn copy_tree(source: &Path, target: &Path) {
        std::fs::create_dir_all(target).unwrap();
        for entry in std::fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let to = target.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_tree(&entry.path(), &to);
            } else {
                std::fs::copy(entry.path(), to).unwrap();
            }
        }
    }
    let source_dir = test_dir();
    let baseline = test_dir();
    let source = open_engine(source_dir.path(), DurabilityMode::Strict);
    seed(&source, CRASH_BATCH_RECORDS as i64 + 23);
    let db = source.database();
    let expected = rows(db);
    db.checkpoint().unwrap();
    copy_tree(source_dir.path(), baseline.path());
    let start = db.wal_log().lock().next_lsn();
    let txn = db.begin_txn().unwrap();
    update(db, txn, 100_000);
    update(db, txn, 200_000);
    let first_progress = Arc::new(AtomicU64::new(0));
    let observed = first_progress.clone();
    let log = db.wal_log();
    ROLLBACK_PROGRESS_TEST_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |txn_id, _, mid| {
            if mid && observed.load(Ordering::Relaxed) == 0 {
                observed.store(log.lock().last_txn_lsn(txn_id), Ordering::Relaxed);
            }
        }));
    });
    db.rollback_txn(txn.txn_id).unwrap();
    ROLLBACK_PROGRESS_TEST_HOOK.with(|hook| *hook.borrow_mut() = None);
    let cutoff = first_progress.load(Ordering::Relaxed);
    assert!(cutoff >= start);
    for restart in [false, true] {
        let replica_dir = test_dir();
        copy_tree(baseline.path(), replica_dir.path());
        let mut role = crate::repl::role::RoleState::new_standalone();
        role.role = crate::repl::role::Role::Replica;
        crate::repl::role::save(replica_dir.path(), &role).unwrap();
        let mut replica = open_engine(replica_dir.path(), DurabilityMode::Strict);
        {
            let target = replica.database().clone();
            let mut applier = crate::repl::replica::applier::Applier::with_database(
                replica_dir.path(),
                target.wal_log(),
                target,
                crate::repl::replica::progress::ReplicaProgress::default(),
            );
            let mut reader = crate::wal::WalReader::open(&db.wal_log(), start).unwrap();
            while let Some(chunk) = reader.next_chunk(cutoff, 1 << 20).unwrap() {
                applier.apply_chunk(&chunk, &role.node_uuid, 1).unwrap();
            }
        }
        if restart {
            drop(replica);
            replica = open_engine(replica_dir.path(), DurabilityMode::Strict);
        }
        let target = replica.database();
        assert_eq!(
            target.rollback_all_active_replication_txns().unwrap(),
            1,
            "AbortTxn is a decision, not completion; restart={restart}"
        );
        assert_eq!(rows(target), expected);
        assert_physical_versions_match_visible(target);
        target.check_consistency().unwrap();
    }
}

#[test]
fn replica_promotion_restores_retained_prepared_xa_runtime() {
    fn copy_tree(source: &Path, target: &Path) {
        std::fs::create_dir_all(target).unwrap();
        for entry in std::fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let to = target.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_tree(&entry.path(), &to);
            } else {
                std::fs::copy(entry.path(), to).unwrap();
            }
        }
    }
    for commit in [false, true] {
        for restart in [false, true] {
            let source_dir = test_dir();
            let replica_dir = test_dir();
            let source = open_engine(source_dir.path(), DurabilityMode::Strict);
            seed(&source, 32);
            let db = source.database();
            let expected = rows(db);
            db.checkpoint().unwrap();
            copy_tree(source_dir.path(), replica_dir.path());
            let start = db.wal_log().lock().next_lsn();
            let txn = db.begin_txn().unwrap();
            update(db, txn, 100_000);
            let xid = crate::xa::Xid::new(1, b"retained-on-promotion".to_vec(), Vec::new()).unwrap();
            let prepare_scn = db.prepare_xa(txn.txn_id, 0, xid.clone()).unwrap();
            let cutoff = db.wal_log().lock().durable_lsn();
            let mut role = crate::repl::role::RoleState::new_standalone();
            role.role = crate::repl::role::Role::Replica;
            crate::repl::role::save(replica_dir.path(), &role).unwrap();
            let mut replica = open_engine(replica_dir.path(), DurabilityMode::Strict);
            {
                let target = replica.database().clone();
                let mut applier = crate::repl::replica::applier::Applier::with_database(
                    replica_dir.path(),
                    target.wal_log(),
                    target,
                    crate::repl::replica::progress::ReplicaProgress::default(),
                );
                let mut reader = crate::wal::WalReader::open(&db.wal_log(), start).unwrap();
                while let Some(chunk) = reader.next_chunk(cutoff, 1 << 20).unwrap() {
                    applier.apply_chunk(&chunk, &role.node_uuid, 1).unwrap();
                }
            }
            if restart {
                drop(replica);
                replica = open_engine(replica_dir.path(), DurabilityMode::Strict);
            }
            let target = replica.database();
            assert_eq!(rows(target), expected);
            assert_eq!(target.rollback_all_active_replication_txns().unwrap(), 0);
            target.restore_replicated_xa_for_promotion().unwrap();
            let first = target.list_prepared_xa();
            assert_eq!(first.len(), 1);
            assert_eq!(first[0].xid, xid);
            assert_eq!(first[0].prepare_scn, prepare_scn);
            let locks = collect_prepared_locks(&target.lock_manager(), txn.txn_id).unwrap().len();
            assert!(locks > 0, "unique locks must survive live promotion");
            target.restore_replicated_xa_for_promotion().unwrap();
            assert_eq!(target.xa_prepared_active.load(Ordering::Relaxed), 1);
            assert_eq!(collect_prepared_locks(&target.lock_manager(), txn.txn_id).unwrap().len(), locks);
            target.set_physical_replica_mode(false).unwrap();
            if commit {
                target.commit_prepared(&xid).unwrap();
            } else {
                target.rollback_prepared(&xid).unwrap();
            }
            assert!(target.list_prepared_xa().is_empty());
            assert_eq!(target.xa_prepared_active.load(Ordering::Relaxed), 0);
            assert!(collect_prepared_locks(&target.lock_manager(), txn.txn_id).unwrap().is_empty());
            let restored = rows(target);
            for (actual, before) in restored.iter().zip(&expected) {
                assert_eq!(actual[0], before[0]);
                let Value::Int(id) = before[0] else { panic!("integer id") };
                assert_eq!(actual[1], Value::Int(id + if commit { 100_000 } else { 0 }));
            }
            assert_physical_versions_match_visible(target);
            target.checkpoint().unwrap();
            drop(replica);
            let reopened = open_engine(replica_dir.path(), DurabilityMode::Strict);
            assert!(reopened.database().list_prepared_xa().is_empty());
            assert_eq!(rows(reopened.database()), restored);
        }
    }
}

#[test]
fn replica_promotion_refuses_prepared_xa_without_wal_snapshot() {
    let dir = test_dir();
    let replica = open_engine(dir.path(), DurabilityMode::Strict);
    let db = replica.database();
    let xid = crate::xa::Xid::new(1, b"missing-prepare".to_vec(), Vec::new()).unwrap();
    db.set_physical_replica_mode(true).unwrap();
    db.apply_prepare_txn(500, xid, 10, 1);
    let error = db.restore_replicated_xa_for_promotion().unwrap_err();
    assert!(error.message().contains("missing its PREPARE WAL snapshot"));
    assert!(db.physical_replica_mode());
    assert!(db.list_prepared_xa().is_empty());
}

#[test]
fn audit1022_prepared_logical_rollback_survives_crash_before_progress_marker() {
    let dir = test_dir();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "storage::core::rollback_batch_tests::rollback_batch_crash_worker",
            "--nocapture",
        ])
        .env("MYDB_ROLLBACK_CRASH_DIR", dir.path())
        .env("MYDB_ROLLBACK_CRASH_MODE", "prepared_logical_partial")
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(91));
    for _ in 0..2 {
        let engine = open_engine(dir.path(), DurabilityMode::Strict);
        let db = engine.database();
        let restored = rows(db);
        assert_eq!(restored.len(), 37);
        for (id, row) in restored.iter().enumerate() {
            assert_eq!(row, &vec![Value::Int(id as i64), Value::Int(id as i64)]);
        }
        assert_physical_versions_match_visible(db);
        db.check_consistency().unwrap();
        db.checkpoint().unwrap();
    }
}
